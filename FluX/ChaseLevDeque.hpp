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
#include <vector>

#include "Helpers.hpp"

namespace flux {

/// Custom Lock-free Chase-Lev work-stealing deque (C. Chase & Y. Lev, 2005).
/// 
/// Single-producer (owner), multiple-consumer (thieves).
/// Owner operates on bottom (LIFO - cache-friendly), thieves steal from top (FIFO).
/// 
/// Memory model: SC-atomics variant (no standalone fences).
/// Rather than pairing a release-store + a bare seq_cst fence (the classic
/// formulation from Lê et al., PPoPP 2013), every access to m_top/m_bottom that
/// participates in the owner/thief handoff uses memory_order_seq_cst directly.
/// Two seq_cst operations on DIFFERENT atomics are still totally ordered with
/// respect to each other (that's what seq_cst guarantees across the whole
/// program), so this gives the exact same cross-thread visibility as the
/// fence-based version but as ordinary atomic loads/stores/CAS, which every
/// tool (ThreadSanitizer included) instruments and reasons about natively.
/// GCC's -fsanitize=thread does NOT model bare std::atomic_thread_fence, which
/// made the previous formulation invisible to TSan; this rewrite exists so we
/// can actually get a verdict out of TSan instead of auditing by hand forever.
/// 
/// Capacity is rounded up to power of 2 for fast modulo via bitmask.
/// 
/// Slots are nulled out (relaxed store) immediately after a successful pop/steal.
/// This is purely a debugging aid. a stale non-null pointer sitting in a slot
/// makes future corruption much harder to diagnose than an honest nullptr. It has
/// absolutly no bearing on correctness: exclusivity is provided entirely by the CAS on
/// m_top (and the owner's bottom-publish-then-check protocol), not by this store.
template <typename T>
class ChaseLevDeque
{
    

public:
    /// Create deque with given capacity (rounded up to next power of 2).
    explicit ChaseLevDeque(size_t capacity = 1024)
        : m_capacity(RoundUpPowerOf2(capacity))
        , m_mask(m_capacity - 1)
        , m_slots(m_capacity)
        , m_top(0)
        , m_bottom(0)
    {
        for (size_t i = 0; i < m_capacity; ++i)
        {
            m_slots[i].store(nullptr, std::memory_order_relaxed);
        }
    }

    ~ChaseLevDeque() = default;

    // Non-copyable, non-movable (contains atomics and unique ownership semantics)
    ChaseLevDeque(const ChaseLevDeque&) = delete;
    ChaseLevDeque& operator=(const ChaseLevDeque&) = delete;
    ChaseLevDeque(ChaseLevDeque&&) = delete;
    ChaseLevDeque& operator=(ChaseLevDeque&&) = delete;

    /// Owner-only: push task to bottom of deque (LIFO - freshest task processed first).
    /// Returns false if deque is full (capacity - 1 slots used to distinguish full/empty).
    bool PushBottom(T* task)
    {
        size_t b = m_bottom.load(std::memory_order_relaxed); // owner-only var, own last write
        size_t t = m_top.load(std::memory_order_seq_cst);    // must see latest thief steals

        // Signed comparison avoids unsigned wraparound when bottom wraps.
        // long size = static_cast<long>(b) - static_cast<long>(t); //< Intentionnaly kept as a reminder :)...
        int64_t size = static_cast<int64_t>(b) - static_cast<int64_t>(t);
        if (size >= static_cast<int64_t>(m_capacity - 1))
        {
            return false; // Full
        }

        // Store task pointer (relaxed: ordering enforced by bottom seq_cst store below)
        m_slots[b & m_mask].store(task, std::memory_order_relaxed);
        // seq_cst publish: totally ordered against thieves' seq_cst top/bottom accesses.
        m_bottom.store(b + 1, std::memory_order_seq_cst);

        return true;
    }

    /// Owner-only: pop task from bottom of deque (LIFO).
    /// Returns false if deque appears empty. On race with steal, may return false
    /// even if an element was present (steal won the race).
    bool PopBottom(T*& out_task)
    {
        size_t b = m_bottom.load(std::memory_order_relaxed);
        if (b == 0) 
            return false; // Underflow (empty)

        b--; // Speculative decrement
        // seq_cst store, no separate fence: this and the seq_cst load of m_top just
        // below are totally ordered against StealTop's seq_cst top/bottom accesses.
        m_bottom.store(b, std::memory_order_seq_cst);

        size_t t = m_top.load(std::memory_order_seq_cst);
        int64_t size = static_cast<int64_t>(b) - static_cast<int64_t>(t);

        if (size < 0)
        {
            // Deque became empty due to concurrent steal: rollback bottom and fail.
            m_bottom.store(t, std::memory_order_relaxed);

            return false;
        }

        if (size > 0)
        {
            // Multiple elements remain (1+): pop succeeded, no race with steal.
            out_task = m_slots[b & m_mask].load(std::memory_order_relaxed);
            m_slots[b & m_mask].store(nullptr, std::memory_order_relaxed);

            return true;
        }

        // size == 0: last element, race with steal possible.
        // CAS top to claim the last element. Strong CAS avoids spurious failure on LL/SC.
        if (!m_top.compare_exchange_strong(t, t + 1,
            std::memory_order_seq_cst, std::memory_order_seq_cst))
        {
            // Steal won the race: task was taken by thief.
            out_task = nullptr;
            m_bottom.store(t + 1, std::memory_order_relaxed);

            return false;
        }

#ifdef FLUX_STRESS_STEAL_WINDOW
        // Artificially widen the CAS-win -> load window (same rationale as StealTop).
        for (volatile int i = 0; i < 2000; ++i) { /* spin */ }
#endif
        // CAS succeeded: we own the last element. Load it now.
        out_task = m_slots[b & m_mask].load(std::memory_order_relaxed);
        m_slots[b & m_mask].store(nullptr, std::memory_order_relaxed);

        // Restore bottom to consistent state (t+1 == new top)
        m_bottom.store(t + 1, std::memory_order_relaxed);

        return out_task != nullptr;
    }

    /// Thief: steal task from top of deque (FIFO - oldest task stolen first).
    /// Returns false if deque empty or steal lost to another thief/owner.
    bool StealTop(T*& out_task)
    {
        size_t t = m_top.load(std::memory_order_seq_cst);
        // No separate fence: this seq_cst load and the seq_cst load of m_bottom just
        // below are totally ordered against the owner's seq_cst bottom store in
        // PopBottom, giving the same visibility the fence-based version relied on.
        size_t b = m_bottom.load(std::memory_order_seq_cst);

        int64_t size = static_cast<int64_t>(b) - static_cast<int64_t>(t);
        if (size <= 0)
            return false; // Empty

        // CAS top FIRST to claim the element (Chase-Lev protocol).
        // Only load the slot if CAS succeeds.
        if (m_top.compare_exchange_strong(t, t + 1,
            std::memory_order_seq_cst, std::memory_order_seq_cst))
        {
#ifdef FLUX_STRESS_STEAL_WINDOW
            // Artificially widen the CAS-win -> load window to flush out races
            // that depend on the owner wrapping the buffer in between.
            for (volatile int i = 0; i < 2000; ++i) { /* spin */ }
#endif
            // CAS succeeded: we own this slot. Load it now.
            out_task = m_slots[t & m_mask].load(std::memory_order_relaxed);
            m_slots[t & m_mask].store(nullptr, std::memory_order_relaxed);
            return out_task != nullptr;
        }

        return false; // Lost race (another thief or owner popped)
    }

    /// Check if deque is empty (seq_cst loads for consistency with the rest).
    bool IsEmpty() const
    {
        size_t t = m_top.load(std::memory_order_seq_cst);
        size_t b = m_bottom.load(std::memory_order_seq_cst);

        return t >= b;
    }

    /// Current number of elements (seq_cst loads).
    size_t Size() const
    {
        size_t t = m_top.load(std::memory_order_seq_cst);
        size_t b = m_bottom.load(std::memory_order_seq_cst);

        return (b > t) ? (b - t) : 0;
    }

    size_t GetCapacity() const { return m_capacity; }

    // Debug/introspection helpers. Test-only, not used on the hot path.
    size_t Top() const { return m_top.load(std::memory_order_relaxed); }
    size_t Bottom() const { return m_bottom.load(std::memory_order_relaxed); }
    T* GetSlot(size_t idx) const { return m_slots[idx & m_mask].load(std::memory_order_relaxed); }
    void SetSlot(size_t idx, T* val) { m_slots[idx & m_mask].store(val, std::memory_order_relaxed); }
    // seq_cst: must stay consistent with the real protocol if used
    // to seed test state.
    void SetTop(size_t val) { m_top.store(val, std::memory_order_seq_cst); }
    void SetBottom(size_t val) { m_bottom.store(val, std::memory_order_relaxed); }

private:
    const size_t m_capacity;                    ///< Power-of-2 capacity.
    const size_t m_mask;                        ///< Bitmask for modulo (capacity - 1).
    std::vector<std::atomic<T*>> m_slots;       ///< Circular buffer of task pointers.

    // alignas(64) pads each atomic to its own cache line to prevent false sharing
    // between m_top (thief CAS) and m_bottom (owner store). MSVC C4324 warning
    // (structure padded due to alignment) is expected and desired here.
    #pragma warning(push)
    #pragma warning(disable: 4324)
    alignas(64) std::atomic<size_t> m_top;      ///< Index of oldest element (thief reads, owner CASes).
    alignas(64) std::atomic<size_t> m_bottom;   ///< Index one past newest element (owner reads/writes).
    #pragma warning(pop)
};

} // namespace flux