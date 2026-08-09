#pragma once

#include <vector>
#include <thread>
#include <condition_variable>
#include <mutex>
#include <atomic>
#include <cstdint>
#include <chrono>
#include <deque>
#include <functional>
#include <memory>

#include "Types.hpp"
#include "Workers.hpp"
#include "TaskRegistry.hpp"

/// High-level task scheduler with work-stealing thread pool.
/// 
/// Public API for task submission, tracking, and control.
/// Internally manages Workers (thread pool) and a fallback queue
/// for tasks submitted before Run() or when workers are saturated.
/// 
/// Thread-safe: all public methods can be called concurrently.
class Scheduler
{
public:
    Scheduler(const Scheduler&) = delete;
    Scheduler& operator=(const Scheduler&) = delete;
    Scheduler(Scheduler&&) = delete;
    Scheduler& operator=(Scheduler&&) = delete;

    Scheduler() = default;
    ~Scheduler();

    /// Submit a task for execution.
    /// @param task Task with payload and priority. Must have valid payload and Idle status.
    /// @return Unique task_id (>0) on success, 0 on failure (invalid task or OOM).
    uint64_t AddTask(Task task);

    /// Get immutable snapshot of task metadata (no payload -- see TaskRegistry
    /// class comment: the registry never retains the callable, only status/
    /// priority/timing, so payload is always empty on the returned Task).
    /// @return Task copy with current status, or default Task if never registered.
    Task GetTaskSnapshot(uint64_t task_id);

    /// Cancel a queued task (not yet started).
    /// @return true if task was found and removed from queue. Note: this only
    /// removes the pending Task from the dispatch deque; the TaskRegistry
    /// entry (if any) is left in place as a durable record, it is not erased.
    bool DeleteTaskFromQueue(uint64_t task_id);

    /// Request abort of a running task (marks as Failed).
    /// @return true if task was InProgress/Claimed and marked Failed.
    bool AbortTaskInProgress(uint64_t task_id);

    /// Start worker threads (hardware_concurrency). Idempotent.
    void Run();

    /// Debug: get task metadata by id. NOTE: unlike before, this can no
    /// longer return the payload (TaskRegistry doesn't retain it) -- it's
    /// now equivalent to GetTaskSnapshot(). Kept as a separate method so call
    /// sites don't need to change; may be removed/merged later.
    Task GetTaskById(uint64_t task_id);

    /// Wait-free statistics (atomic loads)
    int GetTasksInProgress() const { return m_tasks_in_progress.load(); }
    int GetTasksCompleted() const { return m_tasks_completed.load(); }
    int GetTasksFailed() const { return m_tasks_failed.load(); }
    /// Wait-free (relaxed atomic load): no longer takes m_mutex. See
    /// m_fallback_pending below -- kept in sync with m_tasks_to_dispatch by
    /// every mutating call site (EnqueuFallback / DrainFallbackQueue /
    /// DeleteTaskFromQueue / Run), so it never needs the lock to read.
    size_t GetTasksQueued() const { return m_fallback_pending.load(std::memory_order_relaxed); }
    uint64_t GetTasksStolen() const;

private:
    int GetHardwareThreadsCount() const;

    /// Execute task payload with stats tracking (called by Workers executor).
    void ExecuteTask(Task task);

    uint64_t EnqueuFallback(Task task, uint64_t id);
    void DrainFallbackQueue(); // A worker slot just freed up (task failed): give fallback tasks a chance

#ifdef FLUX_DEBUG_DUPES
    /// DEBUG: lock-free duplicate-completion detector. A mutex+map here was heavy
    /// enough to perturb the very race we're hunting (original bug only showed up
    /// at ~980k tasks/sec / 22ms exec time for 1M tasks, nanosecond-scale window).
    /// Flat pre-sized atomic array instead: one relaxed fetch_add, no lock, no hash.
    void CheckDupeCompletion(uint64_t task_id);
    static constexpr size_t kDebugSeenCapacity = 20'000'000;
    // NOTE: must use "= vector<...>(count)" here, NOT "{ count }" with braces,
    // a single arithmetic arg makes the compiler prefer the initializer_list<T>
    // constructor over vector(size_type), which tries to copy-construct T (here
    // std::atomic<uint8_t>, non-copyable) and fails with a construct_at error.
    std::vector<std::atomic<uint8_t>> m_debug_seen = std::vector<std::atomic<uint8_t>>(kDebugSeenCapacity);
#endif

    std::deque<Task>   m_tasks_to_dispatch;              ///< Fallback queue (pre-Run or saturated).
    /// Mirrors m_tasks_to_dispatch.size(), updated under m_mutex at every
    /// mutation site. Lets DrainFallbackQueue() and GetTasksQueued() take a
    /// wait-free fast path (relaxed load, no lock) in the overwhelmingly
    /// common case where the fallback queue is empty -- which after Run()
    /// starts is effectively always, since AddTask only falls back when
    /// Workers::SubmitTask() itself reports saturation.
    std::atomic<size_t> m_fallback_pending{ 0 };

    TaskRegistry<> m_registry;                            ///< Task metadata by ID (lock-free hot path, see TaskRegistry.hpp).

    std::atomic<int>      m_tasks_in_progress{ 0 };        ///< Tasks currently executing.
    std::atomic<int>      m_tasks_completed{ 0 };          ///< Tasks finished successfully.
    std::atomic<int>      m_tasks_failed{ 0 };             ///< Tasks failed/aborted.

    std::atomic<uint64_t> m_next_task_id{ 1 };             ///< Monotonic task ID generator.

    std::unique_ptr<Workers> m_workers;                  ///< Worker thread pool (created on Run).
    mutable std::mutex m_mutex;                          ///< Protects m_tasks_to_dispatch, m_workers pointer swaps.
};