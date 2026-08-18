/*
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * Copyright (C) 2026 Cyril "Parad0x141" Bouvier
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 */

#include "Scheduler.hpp"

#ifdef FLUX_DEBUG_DUPES
#include <iostream>
#endif

using namespace flux;

PriorityStatsSnapshot Scheduler::GetPriorityStats(TaskPriority p) const
{
    const auto& stats = m_priority_stats[static_cast<size_t>(p)];
    PriorityStatsSnapshot out;
    out.completed_count = stats.completed_count.load(std::memory_order_relaxed);
    int64_t total_ns = stats.total_latency_ns.load(std::memory_order_relaxed);
    out.avg_latency_us = out.completed_count > 0
        ? (static_cast<double>(total_ns) / out.completed_count) / 1000.0
        : 0.0;
    out.max_latency_us = stats.max_latency_ns.load(std::memory_order_relaxed) / 1000.0;
    return out;
}

void Scheduler::ResetPriorityStats()
{
    // Relaxed is consistent with how these fields are read/written
    // everywhere else (ExecuteTask, GetPriorityStats): they're a racy
    // diagnostic hint, not a correctness gate. Caller is responsible for
    // ensuring no task from a previous run is still in flight -> see the
    // doc comment in Scheduler.hpp.
    for (auto& stats : m_priority_stats)
    {
        stats.completed_count.store(0, std::memory_order_relaxed);
        stats.total_latency_ns.store(0, std::memory_order_relaxed);
        stats.max_latency_ns.store(0, std::memory_order_relaxed);
    }
}

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
    // ExecuteTask), so in the common case, fallback queue empty, which is
    // effectively always once Run() is up and workers aren't saturated
    // we must not pay for m_mutex at all. Relaxed is enough: this is a hint,
    // not a correctness gate. If we race a concurrent EnqueuFallback and see
    // stale 0, we just miss draining this cycle; the next completion (or the
    // enqueue's own AddTask path) retries.
    if (m_fallback_pending.load(std::memory_order_relaxed) == 0)
        return;

    std::lock_guard<std::mutex> lock(m_mutex);
    // Load the canonical workers reference. The shared_ptr copy keeps Workers
    // alive for the duration of this drain attempt, preventing use-after-free
    // if the destructor races with this call.
    auto workers = m_workers_atomic.load(std::memory_order_acquire);
    if (!workers) return; // Workers not started or destroyed.

    while (!m_tasks_to_dispatch.empty())
    {
        Task& front = m_tasks_to_dispatch.front();
        if (!workers->SubmitTask(front, front.priority))
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
    if (!task.payload || task.status != TaskStatus::Idle)
        return 0;

    uint64_t id = m_next_task_id++;
    task.task_id = id;
    task.status = TaskStatus::Queued;
    task.start_time = std::chrono::steady_clock::now();

    m_registry.Register(id, task.priority, task.start_time);

    // Fast path: wait-free load of workers. If present, try direct submission.
    // The shared_ptr copy keeps Workers alive for this call even if destructor races.
    auto workers = m_workers_atomic.load(std::memory_order_acquire);
    if (workers)
    {
        TaskPriority priority = task.priority;
        if (workers->SubmitTask(task, priority))
            return id;
        // Fall through to fallback (lock required for m_tasks_to_dispatch).
    }

    // Slow path: either workers not started, or submission failed (saturation).
    // Re-check under lock to avoid race with concurrent Run().
    std::lock_guard<std::mutex> lock(m_mutex);
    workers = m_workers_atomic.load(std::memory_order_acquire);
    if (workers)
    {
        TaskPriority priority = task.priority;
        if (workers->SubmitTask(task, priority))
            return id;
    }
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
    auto start_time = task.start_time;
    TaskPriority priority = task.priority;

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

    // Per-priority latency tracking (queue time: submission -> completion).
    // Diagnostic only, same "racy hint" spirit as the rest of FluX -- lets
    // GetPriorityStats() expose starvation empirically (e.g. Low's
    // max_latency_us ballooning under sustained High load) instead of
    // guessing from throughput numbers alone.
    {
        auto now = std::chrono::steady_clock::now();
        int64_t latency_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
            now - start_time).count();

        auto& stats = m_priority_stats[static_cast<size_t>(priority)];
        stats.completed_count.fetch_add(1, std::memory_order_relaxed);
        stats.total_latency_ns.fetch_add(latency_ns, std::memory_order_relaxed);

        int64_t prev_max = stats.max_latency_ns.load(std::memory_order_relaxed);
        while (latency_ns > prev_max &&
            !stats.max_latency_ns.compare_exchange_weak(prev_max, latency_ns, std::memory_order_relaxed)) {
        }
    }

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
    // Idempotency check: already running or destroyed.
    if (m_workers_atomic.load(std::memory_order_acquire))
        return;

    auto executor = [this](Task&& task) { ExecuteTask(std::move(task)); };

    // Safe to construct Workers (spawns threads) while holding m_mutex: a
    // freshly-started worker finds every queue empty and never calls back
    // into Scheduler (DrainFallbackQueue only runs after a task actually
    // executes) until real work exists, so there's no re-entrancy risk here.
    auto workers = std::make_shared<Workers>(GetHardwareThreadsCount(), std::move(executor));

    // Atomically publish the shared_ptr. The release store synchronizes-with
    // the acquire load in getters, ensuring they see the fully-constructed
    // Workers object. The shared_ptr reference count guarantees the object
    // stays alive as long as any getter holds a copy, preventing use-after-free
    // when a getter races with the destructor.
    m_workers_atomic.store(workers, std::memory_order_release);

    // Drain any tasks that were queued before Run() was called.
    while (!m_tasks_to_dispatch.empty())
    {
        Task task = std::move(m_tasks_to_dispatch.front());
        m_tasks_to_dispatch.pop_front();
        if (!workers->SubmitTask(task, task.priority))
        {
            m_tasks_to_dispatch.push_front(std::move(task));
            break;
        }
        m_fallback_pending.fetch_sub(1, std::memory_order_relaxed);
    }
}

int64_t Scheduler::GetTasksStolen() const
{
    // Acquire-load the shared_ptr. The reference count increment ensures the
    // Workers object stays alive for the duration of this call, even if the
    // destructor runs concurrently and releases its shared_ptr.
    auto workers = m_workers_atomic.load(std::memory_order_acquire);
    return workers ? workers->GetStealCount() : 0;
}



Scheduler::~Scheduler()
{
    // Atomically release the Workers pointer. Any getter that already loaded
    // a shared_ptr copy keeps the Workers object alive until it finishes.
    // The actual Workers destruction (including the blocking join() of all
    // worker threads) happens when this local 'to_destroy' is destroyed
    // at the end of this function, OUTSIDE any lock. This avoids deadlocks
    // where a worker blocked on m_mutex in DrainFallbackQueue() could never
    // release the lock if we joined while holding it.
    auto to_destroy = m_workers_atomic.exchange(nullptr, std::memory_order_acq_rel);
    // No lock is held here. m_mutex only guards the fallback queue, which is
    // irrelevant during destruction as no new tasks can be added.
    // to_destroy goes out of scope, joining all worker threads.
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


RequeueStallStats Scheduler::GetRequeueStallStats() const
{
    // Acquire-load the shared_ptr. The reference count increment ensures the
    // Workers object stays alive for the duration of this call, even if the
    // destructor runs concurrently and releases its shared_ptr.
    auto workers = m_workers_atomic.load(std::memory_order_acquire);
    return workers ? workers->GetRequeueStallStats() : RequeueStallStats{};
}

size_t Scheduler::GetWorkerCount() const
{
    // Acquire-load the shared_ptr. The reference count increment ensures the
    // Workers object stays alive for the duration of this call, even if the
    // destructor runs concurrently and releases its shared_ptr.
    auto workers = m_workers_atomic.load(std::memory_order_acquire);
    return workers ? workers->GetWorkerCount() : 0;
}

uint64_t Scheduler::GetWorkerBusyNs(size_t worker_index) const
{
    // Acquire-load the shared_ptr. The reference count increment ensures the
    // Workers object stays alive for the duration of this call, even if the
    // destructor runs concurrently and releases its shared_ptr.
    auto workers = m_workers_atomic.load(std::memory_order_acquire);
    return workers ? workers->GetBusyNs(worker_index) : 0;
}