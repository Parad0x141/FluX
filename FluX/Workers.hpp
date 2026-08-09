#pragma once

#include <thread>
#include <vector>
#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include "Types.hpp"
#include "ChaseLevDeque.hpp"
#include "MPMCQueue.hpp"
#include "TaskPool.hpp"

/// Per-worker state: owns a Chase-Lev deque, injection queue, and task pool.
/// 
/// - queue: Owner-only work-stealing deque (LIFO push/pop, FIFO steal).
/// - inject_queue: MPMC queue for external task submissions (Scheduler -> Worker).
/// - pool: Object pool for Task allocation (avoids heap allocation on hot path).
/// - mtx: Mutex for future extensibility (currently unused in hot path).
/// - shutdown: Atomic flag for graceful worker termination.
struct Worker
{
#ifdef FLUX_STRESS_STEAL_WINDOW
    Worker() : queue(4096) {}   // small capacity -> frequent wraps under stress test
#else
    Worker() : queue(65536) {}
#endif
    Worker(const Worker&) = delete;
    Worker& operator=(const Worker&) = delete;
    Worker(Worker&&) = delete;
    Worker& operator=(Worker&&) = delete;

    std::thread::id id;                              ///< Thread ID (set on thread start).
    std::thread thread;                              ///< Worker thread handle.
    ChaseLevDeque<Task> queue;                       ///< Work-stealing deque (owner LIFO, thief FIFO).
    // MPMCQueue<Task*, 131072> inject_queue;        ///< External submissions (lock-free MPMC).
    // TaskPool<Task, 131072> pool;                  ///< Task object pool (zero-allocation).
    MPMCQueue<Task*, 4096> inject_queue;
    TaskPool<Task, 4096> pool;
    mutable std::mutex mtx;                          ///< Reserved for future synchronization.
    std::atomic<bool> shutdown{false};               ///< Termination flag (checked in WorkerLoop).
};

/// Manages a fixed set of worker threads with work-stealing.
/// 
/// External threads submit tasks via SubmitTask() (round-robin to workers).
/// Workers execute local tasks (LIFO), drain injection queue, then steal (FIFO).
/// 
/// Thread-safe: SubmitTask/TrySteal called concurrently from multiple threads.
/// WorkerLoop runs on dedicated thread per worker.
class Workers
{
public:
    using Executor = std::function<void(Task&&)>;  ///< Task execution callback (Scheduler::ExecuteTask).

    /// Create and start worker threads.
    /// @param hardware_threads_count Number of workers (typically hardware_concurrency).
    /// @param executor Callback to execute tasks (captures Scheduler for stats).
    explicit Workers(int hardware_threads_count, Executor executor);
    ~Workers();

    /// Submit a task from external thread (e.g., Scheduler::AddTask).
    /// Round-robin selects target worker, acquires slot from its pool, pushes to inject_queue.
    /// @param priority RESERVED for future priority-aware worker/queue selection
    /// (e.g. routing High-priority tasks to a dedicated queue or bypassing
    /// round-robin). Currently unused routing is plain round-robin
    /// regardless of task.priority.
    /// @return true if enqueued, false if worker's pool/inject_queue full.
    bool SubmitTask(Task& task, TaskPriority priority);
    /// Attempt to steal a task from another worker (called by thief worker).
    /// @param thief_index Index of stealing worker.
    /// @param out_task Receives stolen task on success.
    /// @return true if steal succeeded.
    bool TrySteal(size_t thief_index, Task& out_task);

    /// Total queued tasks for a worker (local deque + injection queue).
    size_t GetQueueSize(size_t worker_index) const;

    /// Check if worker has no pending work (both queues empty).
    bool IsIdle(size_t worker_index) const;

    /// Total successful steals across all workers.
    uint64_t GetStealCount() const noexcept { return steal_count.load(std::memory_order_relaxed); }

private:
    /// Main worker loop: execute local tasks, drain injections, steal, yield.
    void WorkerLoop(size_t index);

    /// Move tasks from inject_queue to local Chase-Lev deque.
    void DrainInjectQueue(size_t index);

    /// Select next worker for submission (round-robin).
    size_t SelectWorker();

    /// Try steal from specific victim worker.
    /// Was meant for debugging, dead code finally not impl.
    bool TryStealFromVictim(size_t thief_index, size_t victim_index, Task& out_task);

    int                                  m_hardware_threads_count; ///< Number of worker threads.
    std::vector<std::unique_ptr<Worker>> m_workers;                ///< Worker states (owned).
    alignas(64) std::atomic<size_t>      m_round_robin_index{0};   ///< Round-robin counter (cache-line aligned).
    alignas(64) std::atomic<size_t>      m_steal_start{0};         ///< Steal victim start index (cache-line aligned).
    alignas(64) std::atomic<uint64_t>    steal_count{ 0 };         ///< Total successful steals.
    Executor m_executor;                                           ///< Task execution callback.

};