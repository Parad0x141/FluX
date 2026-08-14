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

#include <chrono>
#include <cstdint>
#include <functional>
#include <thread>

namespace flux {


/// Scheduling policy for the task queue (not currently used, reserved for future).
enum class SchedulerMode : uint8_t
{
    FIFO = 0,  ///< First-in-first-out processing order.
    LIFO = 1   ///< Last-in-first-out processing order (cache-friendly for recursive tasks).
};

/// Task priority levels. Higher values indicate higher priority.
enum class TaskPriority : uint8_t
{
    Low = 0,           ///< Background / best-effort work.
    Normal = 1,        ///< Default priority for most tasks.
    AboveNormal = 2,   ///< Elevated priority, processed before Normal.
    High = 3           ///< Critical / latency-sensitive work.
};

// reserved
struct PriorityStats
{
    std::atomic<int64_t> completed_count{ 0 };
    std::atomic<int64_t> total_latency_ns{ 0 };
    std::atomic<int64_t> max_latency_ns{ 0 };
};

// Reserved
struct PriorityStatsSnapshot
{
    int64_t completed_count = 0;
    double avg_latency_us = 0.0;
    double max_latency_us = 0.0;
};

/// Lifecycle state of a task. Transitions are monotonic:
/// Idle -> Queued -> InProgress -> Completed | Failed
/// (Claimed is reserved but not currently reached by any code path; the
/// scheduler jumps straight from Queued to InProgress in ExecuteTask.)
enum class TaskStatus : uint8_t
{
    Idle = 0,        ///< Task constructed but not yet submitted.
    Queued = 1,      ///< Task enqueued, waiting for a worker.
    Claimed = 2,     ///< Worker has popped the task but not yet started execution.
    InProgress = 3,  ///< Task payload currently executing.
    Completed = 4,   ///< Task finished successfully.
    Failed = 5       ///< Task threw an exception or was aborted.
};

/// Unit of work submitted to the thread pool. 
/// All fields are public for zero-overhead access; invariants are enforced by the scheduler.
/// Copyable and movable. Copy exists because Scheduler::AddTask stores a full
/// copy in m_task_registry (including payload for debugging purpose) for later snapshot/lookup, while
/// the original is moved into the worker pool. Note this means std::function's
/// own copy cost (possible heap allocation if the capture exceeds its SBO)
/// applies on every AddTask() call, not just at submission-adjacent moves.
struct Task
{
    std::thread::id thread_id{};          ///< Reserved for the worker thread executing this task.
                                          ///< NOT YET WIRED UP: never assigned anywhere currently.

    uint64_t task_id{ 0 };                ///< Unique identifier assigned by Scheduler on submission.
    TaskPriority priority{ TaskPriority::Normal }; ///< Execution priority hint.
    std::chrono::steady_clock::time_point start_time{}; ///< Timestamp when task was queued.

    TaskStatus status{ TaskStatus::Idle };  ///< Current lifecycle state.

    bool is_stealable{ true };             ///< Affinity hint (NOT YET ENFORCED: StealTop() does not
                                          ///< currently check this flag; reserved for future use).

    /// Internal bookkeeping for TaskPool: index of the slot this Task currently
    /// occupies while pooled. Set by TaskPool::Acquire()/Workers::SubmitTask, read
    /// by TaskPool::Release(). Exists so Release() can find its owning Slot by plain
    /// array indexing instead of recovering it from a T* via offsetof() -- Task
    /// contains std::function, which is essentially never a standard-layout type,
    /// which makes offsetof() on the containing Slot conditionally-supported (UB in
    /// the general case) rather than standard-guaranteed. Meaningless outside the
    /// pool; do not read or rely on this elsewhere.
    size_t pool_slot_index{ static_cast<size_t>(-1) };

    /// User-provided callable. Must be movable and nothrow-destructible.
    /// Empty payload is treated as a no-op (skipped with warning).
    std::function<void()> payload;
};

} // namespace flux