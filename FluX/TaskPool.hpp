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
#include <cstddef>
#include <cstdint>
#include <memory>

#ifdef FLUX_DEBUG_DUPES
#include <thread>
#include <iostream>
#endif


/// Lock-free object pool for fixed-size objects (e.g., Task).
/// 
/// Uses a sequence-based protocol similar to MPMCQueue. Acquire() is safe for
/// concurrent producers (multiple external threads may target the same worker's
/// pool via Workers::SubmitTask). Release() is also safe for concurrent callers:
/// the owning worker releases after local execution, and any thief that steals
/// a task also releases directly into the victim's pool from its own thread.
///
/// Key design: stores slot index inside each Slot to enable O(1) Release without
/// pointer arithmetic (avoids UB from pointer-to-index conversion).
///
/// Memory ordering:
/// - Acquire: acquire-load sequence, relaxed CAS on m_enqueue_pos, release-store sequence
/// - Release: acquire-load sequence, then a CAS on sequence with release-on-success 
///   acquire-on-failure (the acquire-on-failure matters: on CAS failure the reloaded
///   'seq' feeds directly back into the loop's in_use check, so it must itself be a
///   valid acquire-load, not merely relaxed) -> see TOCTOU note below.
/// Requires: T is default-constructible and exposes a public `size_t pool_slot_index`
/// member (see Types.hpp::Task).
///
/// DEBUG: compile with -DFLUX_DEBUG_DUPES to enable an in_use flag per slot that
/// catches Acquire()-while-still-in-use and Release()-while-not-in-use, printing
/// the offending thread ids to stderr. Zero overhead when the macro is undefined.
template <typename T, size_t Capacity = 4096>
class TaskPool
{
    static size_t RoundUpPowerOf2(size_t v)
    {
        v--;
        v |= v >> 1;
        v |= v >> 2;
        v |= v >> 4;
        v |= v >> 8;
        v |= v >> 16;
        v |= v >> 32;
        return v + 1;
    }

    struct Slot
    {
        std::atomic<uint64_t> sequence{ 0 }; ///< Sequence for acquire/release synchronization.
        T value;                            ///< Stored object.
        size_t index;                       ///< Slot index (set at construction) for O(1) Release.

#ifdef FLUX_DEBUG_DUPES
        std::atomic<bool> in_use{ false };         ///< True between a successful Acquire() and its Release().
        std::atomic<uint64_t> acquire_count{ 0 };  ///< Total Acquire() calls that returned this slot.
        std::atomic<uint64_t> release_count{ 0 };  ///< Total Release() calls that accepted this slot.
        std::atomic<std::thread::id> last_acquire_thread{};
        std::atomic<std::thread::id> last_release_thread{};
#endif
    };

public:
    /// Create pool with fixed capacity (rounded up to power of 2).
    explicit TaskPool()
    {
        m_capacity = RoundUpPowerOf2(Capacity);
        m_mask = m_capacity - 1;
        m_slots = std::make_unique<Slot[]>(m_capacity);
        for (size_t i = 0; i < m_capacity; ++i) {
            m_slots[i].sequence.store(i, std::memory_order_relaxed);
            m_slots[i].index = i; // Store index for safe Release
        }
    }

    ~TaskPool() = default;

    TaskPool(const TaskPool&) = delete;
    TaskPool& operator=(const TaskPool&) = delete;

    /// Acquire a slot for writing. Returns nullptr if pool exhausted.
    /// Caller must construct the object in-place (placement new or assignment).
    /// Safe for concurrent producers (matches actual usage: Workers::SubmitTask
    /// can be invoked by multiple external threads targeting the same worker's pool).
    /// @param out_index If non-null, receives the slot index this T* was acquired
    ///        from. Callers that later overwrite the whole object (e.g. via
    ///        move-assignment) MUST restore T::pool_slot_index = *out_index
    ///        afterwards, since that assignment clobbers whatever pool_slot_index
    ///        the incoming object happened to carry. See Workers::SubmitTask.
    T* Acquire(size_t* out_index = nullptr)
    {
        Slot* slot;
        size_t pos = m_enqueue_pos.load(std::memory_order_relaxed);

        for (;;) {
            slot = &m_slots[pos & m_mask];
            uint64_t seq = slot->sequence.load(std::memory_order_acquire);
            int64_t diff = static_cast<int64_t>(seq) - static_cast<int64_t>(pos);
            if (diff == 0) {
                if (m_enqueue_pos.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed))
                    break; // Claimed 'pos'; slot is ours.
            }
            else if (diff < 0) {
                return nullptr; // Pool genuinely full. No ticket was consumed: nothing to roll back.
            }
            else {
                pos = m_enqueue_pos.load(std::memory_order_relaxed);
            }
        }

        size_t index = pos & m_mask;

#ifdef FLUX_DEBUG_DUPES
        bool was_in_use = slot->in_use.exchange(true, std::memory_order_acq_rel);
        if (was_in_use) {
            std::cerr << "[TaskPool] CORRUPTION: Acquire() returned slot " << index
                << " (pos=" << pos << ") that is still in_use!"
                << " thread=" << std::this_thread::get_id()
                << " last_acquire_thread=" << slot->last_acquire_thread.load()
                << " last_release_thread=" << slot->last_release_thread.load()
                << " acquire_count=" << slot->acquire_count.load()
                << " release_count=" << slot->release_count.load()
                << "\n";
        }
        slot->acquire_count.fetch_add(1, std::memory_order_relaxed);
        slot->last_acquire_thread.store(std::this_thread::get_id(), std::memory_order_relaxed);
#endif

        // Default-construct object in slot, then stamp its pool_slot_index so
        // Release() can find this Slot again without offsetof. This is a first
        // line of defense -- if the caller overwrites the whole object afterwards
        // (Workers::SubmitTask does via move-assignment), IT must restore this
        // field using out_index; see the note on the parameter above.
        slot->value = T{};
        slot->value.pool_slot_index = index;
        if (out_index) *out_index = index;
        slot->sequence.store(pos + 1, std::memory_order_release);
        return &slot->value;
    }

    /// Release a previously acquired slot back to the pool.
    /// Finds its Slot by plain array indexing via T::pool_slot_index (no offsetof,
    /// no pointer arithmetic, no UB). Validates the pointer matches that slot before
    /// touching anything.
    /// Idempotent: safe to call twice on same slot (CAS-based guard below);
    /// FLUX_DEBUG_DUPES adds a second, independent in_use guard for diagnosis.
    void Release(T* ptr)
    {
        if (!ptr) return;

        size_t index = ptr->pool_slot_index;
        if (index >= m_capacity) return; // never validly acquired from this pool

        Slot* slot = &m_slots[index];
        if (&slot->value != ptr) 
            return; // sanity: ptr must actually be this slot's value

#ifdef FLUX_DEBUG_DUPES
        // Checked BEFORE the sequence guard below so a second Release() on the same
        // slot always logs full context, even though the sequence guard will also
        // (correctly) no-op the actual pool state change.
        bool was_in_use = slot->in_use.exchange(false, std::memory_order_acq_rel);
        if (!was_in_use) {
            std::cerr << "[TaskPool] CORRUPTION: Release() on slot " << index
                << " that was NOT marked in_use (this is call #"
                << (slot->release_count.load() + 1) << " to Release() on this slot)!"
                << " thread=" << std::this_thread::get_id()
                << " last_acquire_thread=" << slot->last_acquire_thread.load()
                << " last_release_thread=" << slot->last_release_thread.load()
                << " acquire_count=" << slot->acquire_count.load()
                << " release_count=" << slot->release_count.load()
                << "\n";
        }
        slot->release_count.fetch_add(1, std::memory_order_relaxed);
        slot->last_release_thread.store(std::this_thread::get_id(), std::memory_order_relaxed);
#endif

        // Transition slot from "in use" to "free for next cycle" via CAS, not a plain
        // load-then-store. A slot is "in use" iff its sequence is of the form
        // k*capacity + index + 1 for some cycle k >= 0; releasing sets it to
        // k*capacity + index + capacity, i.e. seq + (capacity - 1). Using CAS closes
        // the TOCTOU window a separate load+store had: if two Release() calls raced
        // on the SAME slot, the loser used to re-read a value already advanced by the
        // winner and add capacity-1 a second time, corrupting the sequence into a
        // value that could falsely match a future Acquire()'s expected pos (premature
        // reuse -- a second, independent source of corruption, unrelated to the
        // ChaseLevDeque fence bug).
        uint64_t seq = slot->sequence.load(std::memory_order_acquire);
        for (;;)
        {
            bool in_use = ((seq - index - 1) % m_capacity) == 0;
            if (!in_use) {
#ifdef FLUX_DEBUG_DUPES
                std::cerr << "[TaskPool] Release() on already-released slot " << index
                    << " (CAS guard caught it, pool state unaffected) thread="
                    << std::this_thread::get_id() << "\n";
#endif
                return; // Already released (or never validly acquired at this seq), ignore
            }
            if (slot->sequence.compare_exchange_weak(seq, seq + m_capacity - 1,
                std::memory_order_release, std::memory_order_acquire))
            {
                m_dequeue_pos.fetch_add(1, std::memory_order_relaxed);
                return; // We won the transition; slot is now free for its next cycle.
            }
            // CAS failed: 'seq' was refreshed to the current value by compare_exchange_weak.
            // Loop and recheck -- either we retry the transition, or the in_use check
            // above now finds it's already released and bails cleanly.
        }
    }

    /// Check if pool is exhausted (acquire loads for consistency).
    bool IsFull() const
    {
        size_t enq = m_enqueue_pos.load(std::memory_order_acquire);
        size_t deq = m_dequeue_pos.load(std::memory_order_acquire);

        return (enq - deq) >= m_capacity;
    }

    /// Number of slots currently in use (acquire loads).
    size_t Size() const
    {
        size_t enq = m_enqueue_pos.load(std::memory_order_acquire);
        size_t deq = m_dequeue_pos.load(std::memory_order_acquire);

        return (enq >= deq) ? (enq - deq) : 0;
    }

    size_t GetCapacity() const { return m_capacity; }

private:
    size_t m_capacity;                   ///< Power-of-2 capacity.
    size_t m_mask;                       ///< Bitmask for modulo (capacity - 1).
    std::unique_ptr<Slot[]> m_slots;     ///< Slot array.
    alignas(64) std::atomic<size_t> m_enqueue_pos{ 0 };  ///< Acquire position (cache-line aligned).
    alignas(64) std::atomic<size_t> m_dequeue_pos{ 0 };  ///< Release position (cache-line aligned).
};