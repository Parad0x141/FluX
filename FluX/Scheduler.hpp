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

#pragma once

#include <vector>
#include <thread>
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
    int64_t GetTasksInProgress() const { return m_tasks_in_progress.load(); }
    int64_t GetTasksCompleted() const { return m_tasks_completed.load(); }
    int64_t GetTasksFailed() const { return m_tasks_failed.load(); }
    /// Wait-free (relaxed atomic load): no longer takes m_mutex. See
    /// m_fallback_pending below -- kept in sync with m_tasks_to_dispatch by
    /// every mutating call site (EnqueuFallback / DrainFallbackQueue /
    /// DeleteTaskFromQueue / Run), so it never needs the lock to read.
    int64_t GetTasksQueued() const { return m_fallback_pending.load(std::memory_order_relaxed); }
    int64_t GetTasksStolen() const;


    /// STATS: see Workers::RequeueStallStats. Returns {0,0} if workers not started.
    RequeueStallStats GetRequeueStallStats() const;

    /// Per-priority completion stats (avg/max latency in microseconds).
    /// Use this to verify actual scheduling behavior under mixed-priority load
    ///  e.g. confirm High tasks aren't starved behind a Normal backlog.
    PriorityStatsSnapshot GetPriorityStats(TaskPriority p) const;

    /// Zero out per-priority stats (completed_count / total_latency_ns /
    /// max_latency_ns) for all priorities. Unlike GetTasksCompleted/Failed/
    /// Stolen, these have no baseline-snapshot escape hatch: total_latency_ns
    /// and max_latency_ns are cumulative sums/maxima, and max in particular
    /// cannot be recovered per-run via subtraction once mixed with a prior
    /// run's data (a later run with a lower max would still report the
    /// earlier run's higher one). Callers that need isolated per-run
    /// avg/max latency (e.g. FTXUIDashboard, between benchmark runs in the
    /// same process) must call this immediately before submitting that
    /// run's tasks.
    /// NOT safe to call while any previously-submitted task is still queued
    /// or in-flight (GetTasksInProgress() != 0 or fallback queue non-empty)
    /// callers must guarantee no concurrent writers, e.g. by only calling
    /// this between runs once the prior run's completion has been observed.
    void ResetPriorityStats();

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
    static constexpr size_t kDebugSeenCapacity = 120'000'000; //< 100M + margin
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
    std::atomic<int64_t> m_fallback_pending{ 0 };

    TaskRegistry<> m_registry;                            ///< Task metadata by ID (lock-free hot path, see TaskRegistry.hpp).

    std::atomic<int64_t>      m_tasks_in_progress{ 0 };        ///< Tasks currently executing.
    std::atomic<int64_t>      m_tasks_completed{ 0 };          ///< Tasks finished successfully.
    std::atomic<int64_t>      m_tasks_failed{ 0 };             ///< Tasks failed/aborted.

    /// Per-priority completion tracking (index = static_cast<uint8_t>(TaskPriority)).
    /// completed_count: how many tasks of that priority finished.
    /// total_latency_ns: sum of (completion_time - start_time) in ns, for averaging.
    /// max_latency_ns: worst-case latency seen for that priority -- the number
    /// that actually exposes starvation, since averages hide outliers.


    std::array<PriorityStats, 4> m_priority_stats; ///< Indexed by TaskPriority.


    std::atomic<uint64_t> m_next_task_id{ 1 };             ///< Monotonic task ID generator.

    std::unique_ptr<Workers> m_workers;                  ///< Worker thread pool (created on Run).
    mutable std::mutex m_mutex;                          ///< Protects m_tasks_to_dispatch, m_workers pointer swaps.
};