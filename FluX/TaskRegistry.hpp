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

#include <atomic>
#include <array>
#include <memory>
#include <cstdint>
#include <chrono>

#include "Types.hpp"

namespace flux {

/// Metadata-only registry of tasks, indexed by task_id, built for
/// observability (status/timing/priority queries, post-mortem debugging)
/// WITHOUT a shared lock on the hot path.
///
/// Design:
/// - Segmented storage: task_id space is split into fixed-size Chunks.
///   `chunk_index = task_id / ChunkSize`, `slot_index = task_id % ChunkSize`.
///   A Chunk, once allocated, is NEVER moved or freed until the registry
///   itself is destroyed -- so a raw Entry* handed out here stays valid for
///   the registry's whole lifetime. That's what lets readers/writers touch a
///   slot with plain atomics instead of a mutex.
/// - Chunk *creation* is the only operation that can race: it's resolved
///   with a lock-free CAS on `std::atomic<Chunk*>` (double-checked, no
///   mutex). This only happens once every ChunkSize task_ids, not once per
///   task, and losing the race just means freeing a redundant allocation.
/// - `status` is `std::atomic<TaskStatus>`: workers update it with a
///   release store (SetStatus) or a CAS (TryAbort), zero locks.
/// - Deliberately does NOT retain the task's `payload` (std::function).
///   Registering used to copy the *entire* Task (including the callable) on
///   every AddTask() -- this registry only stores what's needed for
///   introspection, which also removes that copy from the submission path.
///   GetTaskById()/GetTaskSnapshot() on the Scheduler side will therefore
///   return a Task with an empty payload -- they were never meant to be
///   re-executed anyway, only inspected.
/// - No true deletion: entries are a durable historical record (that's the
///   point -- post-mortem data survives). DeleteTaskFromQueue() still
///   removes the pending Task from the dispatch deque; it just no longer
///   erases the registry entry.
///
/// task_id values are expected to stay within `MaxChunks * ChunkSize`
/// (default ~4096 * 65536 ~= 268M). IDs beyond that range are silently
/// ignored by Register()/SetStatus()/TryAbort() (no-op) and Get() returns
/// false -- by design, this should never happen in practice since
/// Scheduler's m_next_task_id is a monotonic uint64_t counter and this cap
/// is intentionally generous; bump MaxChunks if you ever need more.
template <size_t ChunkSize = 65536, size_t MaxChunks = 4096>
class TaskRegistry
{
public:
    struct Entry
    {
        std::atomic<TaskStatus> status{ TaskStatus::Idle };
        std::atomic<bool> in_use{ false };   ///< True once Register() has stamped this slot.
        TaskPriority priority{ TaskPriority::Normal };
        std::chrono::steady_clock::time_point start_time{};
    };

    TaskRegistry()
    {
        for (auto& p : m_chunks)
            p.store(nullptr, std::memory_order_relaxed);
    }

    ~TaskRegistry()
    {
        for (auto& p : m_chunks)
            delete p.load(std::memory_order_relaxed);
    }

    TaskRegistry(const TaskRegistry&) = delete;
    TaskRegistry& operator=(const TaskRegistry&) = delete;

    /// Stamp metadata for a freshly-submitted task. Call once per task_id,
    /// before any SetStatus()/Get()/TryAbort() on that id.
    void Register(uint64_t task_id, TaskPriority priority,
        std::chrono::steady_clock::time_point start_time)
    {
        Entry* e = GetOrCreateSlot(task_id);
        if (!e) return; // task_id outside configured range, see class comment.

        e->priority = priority;
        e->start_time = start_time;
        e->status.store(TaskStatus::Queued, std::memory_order_release);
        e->in_use.store(true, std::memory_order_release);
    }

    /// Hot path: called by workers on task completion/failure. No lock.
    void SetStatus(uint64_t task_id, TaskStatus status)
    {
        Entry* e = TryGetSlot(task_id);
        if (e) e->status.store(status, std::memory_order_release);
    }

    /// Snapshot metadata for task_id. Returns false if never registered
    /// (unknown id, or id beyond the configured range).
    bool Get(uint64_t task_id, TaskStatus& out_status, TaskPriority& out_priority,
        std::chrono::steady_clock::time_point& out_start_time) const
    {
        const Entry* e = TryGetSlot(task_id);
        if (!e || !e->in_use.load(std::memory_order_acquire))
            return false;

        out_status = e->status.load(std::memory_order_acquire);
        out_priority = e->priority;
        out_start_time = e->start_time;
        return true;
    }

    /// CAS-based abort: only transitions InProgress/Claimed -> Failed.
    /// Returns true iff this call performed that transition.
    bool TryAbort(uint64_t task_id)
    {
        Entry* e = TryGetSlot(task_id);
        if (!e) return false;

        TaskStatus expected = e->status.load(std::memory_order_acquire);
        for (;;)
        {
            if (expected != TaskStatus::InProgress && expected != TaskStatus::Claimed)
                return false;

            if (e->status.compare_exchange_weak(expected, TaskStatus::Failed,
                std::memory_order_acq_rel, std::memory_order_acquire))
                return true;
            // CAS failure refreshes 'expected' with the current value; loop rechecks it.
        }
    }

private:
    using Chunk = std::array<Entry, ChunkSize>;

    Entry* GetOrCreateSlot(uint64_t task_id)
    {
        size_t chunk_idx = static_cast<size_t>(task_id / ChunkSize);
        size_t slot_idx = static_cast<size_t>(task_id % ChunkSize);
        if (chunk_idx >= MaxChunks)
            return nullptr;

        Chunk* chunk = m_chunks[chunk_idx].load(std::memory_order_acquire);
        if (!chunk)
        {
            // Speculatively allocate; lose the race gracefully if another
            // thread wins the CAS below (only matters the first time this
            // particular chunk is ever touched).
            std::unique_ptr<Chunk> fresh = std::make_unique<Chunk>();
            Chunk* expected = nullptr;
            if (m_chunks[chunk_idx].compare_exchange_strong(expected, fresh.get(),
                std::memory_order_acq_rel, std::memory_order_acquire))
            {
                chunk = fresh.release(); // Ownership now lives in m_chunks.
            }
            else
            {
                chunk = expected; // Someone else won; use theirs, ours is freed here.
            }
        }
        return &(*chunk)[slot_idx];
    }

    Entry* TryGetSlot(uint64_t task_id) const
    {
        size_t chunk_idx = static_cast<size_t>(task_id / ChunkSize);
        size_t slot_idx = static_cast<size_t>(task_id % ChunkSize);
        if (chunk_idx >= MaxChunks)
            return nullptr;

        Chunk* chunk = m_chunks[chunk_idx].load(std::memory_order_acquire);
        if (!chunk)
            return nullptr;

        return &(*chunk)[slot_idx];
    }

    std::array<std::atomic<Chunk*>, MaxChunks> m_chunks;
};

} // namespace flux