#include "Scheduler.hpp"

#ifdef FLUX_DEBUG_DUPES
#include <iostream>
#endif

inline int Scheduler::GetHardwareThreadsCount() const
{
    return std::thread::hardware_concurrency();
}


/// Add task to fallback queue (called when workers not started or saturated).
/// PRECONDITION: caller must already hold m_mutex (std::mutex is
/// non-recursive; locking here too would deadlock).
uint64_t Scheduler::EnqueuFallback(Task task, uint64_t id)
{
    task.task_id = id;
    task.status = TaskStatus::Queued;
    task.start_time = std::chrono::steady_clock::now();

    m_tasks_to_dispatch.push_back(std::move(task));
    // Kept in sync with the deque under the same lock the caller already
    // holds; see m_fallback_pending's doc comment in Scheduler.hpp.
    m_fallback_pending.fetch_add(1, std::memory_order_relaxed);
    return id;
}

/// Attempt to move queued fallback tasks into worker queues.
void Scheduler::DrainFallbackQueue()
{
    // Wait-free fast path: this runs on EVERY task completion (see
    // ExecuteTask), so in the common case -- fallback queue empty, which is
    // effectively always once Run() is up and workers aren't saturated --
    // we must not pay for m_mutex at all. Relaxed is enough: this is a hint,
    // not a correctness gate. If we race a concurrent EnqueuFallback and see
    // stale 0, we just miss draining this cycle; the next completion (or the
    // enqueue's own AddTask path) retries.
    if (m_fallback_pending.load(std::memory_order_relaxed) == 0)
        return;

    std::lock_guard<std::mutex> lock(m_mutex);
    if (!m_workers) return; // Moved inside the lock: m_workers is written
    // (destructor) and read (here, AddTask,
    // GetTasksStolen) from different threads, so
    // every access must go through m_mutex.

    while (!m_tasks_to_dispatch.empty())
    {
        Task& front = m_tasks_to_dispatch.front();
        if (!m_workers->SubmitTask(front, front.priority))
        {
            break; // Still saturated: stop, retry on next completion.
        }
        m_tasks_to_dispatch.pop_front();
        m_fallback_pending.fetch_sub(1, std::memory_order_relaxed);
    }
}

/// Submit a task for execution.
uint64_t Scheduler::AddTask(Task task)
{
    // No work to do
    if (!task.payload)
        return 0;

    if (task.status != TaskStatus::Idle)
        return 0; // Already submitted

    // Assign unique monotonic ID
    uint64_t id = m_next_task_id++;
    task.task_id = id;
    task.status = TaskStatus::Queued;
    task.start_time = std::chrono::steady_clock::now();

    // Register metadata for tracking (GetTaskSnapshot, Delete, Abort).
    // Lock-free hot path -- see TaskRegistry.hpp; does not copy task.payload.
    m_registry.Register(id, task.priority, task.start_time);

    // m_workers is read here and written by the destructor from a different
    // thread, so this whole check-and-submit must happen under m_mutex (see
    // DrainFallbackQueue's comment above for the full rationale). The
    // critical section stays short: a pointer check plus one already
    // lock-free SubmitTask() call, not the heavy path this used to guard
    // before TaskRegistry.
    std::lock_guard<std::mutex> lock(m_mutex);

    if (m_workers)
    {
        TaskPriority priority = task.priority;
        if (m_workers->SubmitTask(task, priority))
            return id;

        // Worker injection queue full: fall back to internal queue.
        return EnqueuFallback(std::move(task), id);
    }

    // Workers not started yet: queue internally
    return EnqueuFallback(std::move(task), id);
}

#ifdef FLUX_DEBUG_DUPES
/// DEBUG: lock-free — one relaxed fetch_add on a pre-sized array, no mutex, no map.
/// task_id must stay under kDebugSeenCapacity (bump it if you run more tasks).
void Scheduler::CheckDupeCompletion(uint64_t task_id)
{
    if (task_id >= kDebugSeenCapacity)
        return; // out of range, skip rather than crash

    uint8_t prev = m_debug_seen[task_id].fetch_add(1, std::memory_order_relaxed);
    if (prev >= 1)
    {
        std::cerr << "[Scheduler] DUPLICATE COMPLETION: task_id=" << task_id
            << " completed " << (prev + 1) << " times, thread="
            << std::this_thread::get_id() << "\n";
    }
}
#endif

/// Execute task payload with statistics tracking.
///
/// NOTE on locking: 'task' is passed BY VALUE, so it's a copy exclusively
/// owned by this call on this thread -- nothing else can observe or race on
/// task.status / task.payload here. m_tasks_in_progress/completed/failed are
/// already std::atomic<int>. None of that needs m_mutex; the lock here used
/// to buy nothing but cache-line contention between every worker thread and
/// the submitter on every single task. m_mutex is now taken ONLY inside
/// DrainFallbackQueue(), and only past its own fast-path check.
void Scheduler::ExecuteTask(Task task)
{
    task.status = TaskStatus::InProgress;
    m_tasks_in_progress.fetch_add(1, std::memory_order_relaxed);
    std::function<void()> work = std::move(task.payload);

    try
    {
        work(); // Execute user code (no scheduler locks held)
    }
    catch (...)
    {
        m_tasks_in_progress.fetch_sub(1, std::memory_order_relaxed);
        m_tasks_failed.fetch_add(1, std::memory_order_relaxed);

        // Update registry to Failed (lock-free, see TaskRegistry.hpp).
        m_registry.SetStatus(task.task_id, TaskStatus::Failed);

        DrainFallbackQueue();

        throw; // Propagate to worker (caught there, logged)
    }

    m_tasks_in_progress.fetch_sub(1, std::memory_order_relaxed);
    m_tasks_completed.fetch_add(1, std::memory_order_relaxed);

#ifdef FLUX_DEBUG_DUPES
    CheckDupeCompletion(task.task_id);
#endif

    // Update registry to Completed (lock-free, see TaskRegistry.hpp).
    m_registry.SetStatus(task.task_id, TaskStatus::Completed);

    DrainFallbackQueue(); // A worker just freed capacity: give fallback tasks a chance.
}

/// Start worker threads (idempotent).
void Scheduler::Run()
{
    std::lock_guard<std::mutex> lock(m_mutex);
    if (m_workers)
        return; // Already running

    auto executor = [this](Task&& task) { ExecuteTask(std::move(task)); };
    m_workers = std::make_unique<Workers>(GetHardwareThreadsCount(), std::move(executor));
    // Safe to construct Workers (spawns threads) while holding m_mutex: a
    // freshly-started worker finds every queue empty and never calls back
    // into Scheduler (DrainFallbackQueue only runs after a task actually
    // executes) until real work exists, so there's no re-entrancy risk here.

    while (!m_tasks_to_dispatch.empty())
    {
        Task task = std::move(m_tasks_to_dispatch.front());
        m_tasks_to_dispatch.pop_front();
        if (!m_workers->SubmitTask(task, task.priority))
        {
            m_tasks_to_dispatch.push_front(std::move(task));
            break;
        }
        m_fallback_pending.fetch_sub(1, std::memory_order_relaxed);
    }
}

int64_t Scheduler::GetTasksStolen() const
{
    std::lock_guard<std::mutex> lock(m_mutex);
    if (!m_workers) return 0;
    return m_workers->GetStealCount();
}

Scheduler::~Scheduler()
{
    // Move m_workers out under the lock: this makes the "is m_workers null"
    // state change atomically visible to any thread checking it in AddTask/
    // DrainFallbackQueue/GetTasksStolen. The actual object -- and its
    // blocking join() of every worker thread -- is destroyed just below,
    // OUTSIDE the lock. That ordering matters: Workers::~Workers() joins
    // worker threads, and a worker thread might itself be blocked trying to
    // acquire m_mutex (e.g. inside DrainFallbackQueue) at that very moment.
    // If we joined while still holding m_mutex, that worker could never
    // reach the check that would let it return and finish -- deadlock. By
    // releasing the lock first, that worker sees m_workers == nullptr as
    // soon as it acquires m_mutex, returns immediately, and the join below
    // can proceed without ever contending on m_mutex again.
    std::unique_ptr<Workers> workers_to_join;
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        workers_to_join = std::move(m_workers);
    }
    // workers_to_join destroyed here, lock already released.
}

Task Scheduler::GetTaskSnapshot(uint64_t task_id)
{
    Task snapshot;
    TaskStatus status;
    TaskPriority priority;
    std::chrono::steady_clock::time_point start_time;

    if (m_registry.Get(task_id, status, priority, start_time))
    {
        snapshot.task_id = task_id;
        snapshot.status = status;
        snapshot.priority = priority;
        snapshot.start_time = start_time;
        // payload intentionally left empty: TaskRegistry never retains it,
        // see TaskRegistry.hpp class comment.
    }

    return snapshot;
}

bool Scheduler::DeleteTaskFromQueue(uint64_t task_id)
{
    std::lock_guard<std::mutex> lock(m_mutex);

    for (auto it = m_tasks_to_dispatch.begin(); it != m_tasks_to_dispatch.end(); ++it)
    {
        if (it->task_id == task_id)
        {
            m_tasks_to_dispatch.erase(it);
            m_fallback_pending.fetch_sub(1, std::memory_order_relaxed);
            // Registry entry is intentionally left in place -- it's a durable
            // post-mortem record, not deleted on cancel. See TaskRegistry.hpp.
            return true;
        }
    }
    return false;
}

bool Scheduler::AbortTaskInProgress(uint64_t task_id)
{
    return m_registry.TryAbort(task_id);
}

Task Scheduler::GetTaskById(uint64_t task_id)
{
    // TaskRegistry no longer retains the payload, so this is now identical
    // to GetTaskSnapshot(). Kept as a separate call for existing call sites.
    return GetTaskSnapshot(task_id);
}