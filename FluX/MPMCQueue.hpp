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
#include <utility>


/// Lock-free MPMC (Multiple-Producer Multiple-Consumer) bounded queue.
/// 
/// Based on Dmitry Vyukov's bounded MPMC queue algorithm.
/// Uses sequence numbers per slot to detect full/empty and avoid ABA.
/// 
/// Memory ordering:
/// - Enqueue: acquire-load sequence, release-store sequence (publish value)
/// - Dequeue: acquire-load sequence, release-store sequence (retire slot)
/// - Positions: relaxed RMW (CAS), acquire-load for size/empty checks
/// 
/// Capacity rounded to power of 2 for fast modulo via bitmask.
template <typename T, size_t Capacity = 8192>
class MPMCQueue
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
		std::atomic<uint64_t> sequence{ 0 }; ///< Monotonically increasing sequence number.
		T value{};                            ///< Stored element.
	};

public:
	/// Create queue with fixed capacity (rounded up to power of 2).
	explicit MPMCQueue()
	{
		m_capacity = RoundUpPowerOf2(Capacity);
		m_mask = m_capacity - 1;
		m_slots = std::make_unique<Slot[]>(m_capacity);
		for (size_t i = 0; i < m_capacity; ++i)
		{
			// Initialize sequence to slot index: enables O(1) full/empty detection
			m_slots[i].sequence.store(i, std::memory_order_relaxed);
		}
	}

	~MPMCQueue() = default;

	MPMCQueue(const MPMCQueue&) = delete;
	MPMCQueue& operator=(const MPMCQueue&) = delete;

	/// Try to push a value. Returns false if queue is full.
	/// Multiple producers may call concurrently.
	bool TryPush(T value)
	{
		Slot* slot;
		size_t pos = m_enqueue_pos.load(std::memory_order_relaxed);

		for (;;)
		{
			slot = &m_slots[pos & m_mask];
			uint64_t seq = slot->sequence.load(std::memory_order_acquire);
			// diff == 0  -> slot is free for writing (sequence matches enqueue position)
			// diff < 0   -> slot not yet released by consumer (queue full)
			// diff > 0   -> stale position, retry with latest enqueue_pos
			int64_t diff = static_cast<int64_t>(seq) - static_cast<int64_t>(pos);
			if (diff == 0)
			{
				// Claim this slot by advancing enqueue position
				if (m_enqueue_pos.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed))
					break;
			}
			else if (diff < 0)
			{
				return false; // Queue full
			}
			else
			{
				pos = m_enqueue_pos.load(std::memory_order_relaxed);
			}
		}
		slot->value = std::move(value);

		// Publish (release): as far as THIS QUEUE's contract is concerned, this
		// store is what makes slot->value visible to the consumer. the plain
		// assignment above carries no cross-thread visibility guarantee of its own
		// under the queue's protocol. Note this is about T's *ordinary* data
		// members: if T itself contains atomics, T's move-assignment may perform
		// its own atomic operations with their own ordering, entirely orthogonal
		// to (and not a substitute for) the release-store below.
		slot->sequence.store(pos + 1, std::memory_order_release);

		return true;
	}

	/// Try to pop a value into out_value. Returns false if queue is empty.
	/// Multiple consumers may call concurrently.
	bool TryPop(T& out_value)
	{
		Slot* slot;
		size_t pos = m_dequeue_pos.load(std::memory_order_relaxed);
		for (;;)
		{
			slot = &m_slots[pos & m_mask];
			uint64_t seq = slot->sequence.load(std::memory_order_acquire);
			// diff == 0  -> slot has value ready (sequence == dequeue_pos + 1)
			// diff < 0   -> slot not yet produced (queue empty)
			// diff > 0   -> stale position, retry with latest dequeue_pos
			int64_t diff = static_cast<int64_t>(seq) - static_cast<int64_t>(pos + 1);
			if (diff == 0)
			{
				// Claim this slot by advancing dequeue position
				if (m_dequeue_pos.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed))
					break;
			}
			else if (diff < 0)
			{
				return false; // Queue empty
			}
			else
			{
				pos = m_dequeue_pos.load(std::memory_order_relaxed);
			}
		}

		// Move value out. Safe to read non-atomically here because the acquire-load
		// of `sequence` earlier in this loop already synchronized-with the producer's
		// release-store, establishing happens-before for slot->value.
		out_value = std::move(slot->value);
		// Retire slot: sequence = pos + capacity marks it free for next cycle
		slot->sequence.store(pos + m_capacity, std::memory_order_release);

		return true;
	}

	/// Check if queue is empty (acquire loads for consistency).
	bool IsEmpty() const
	{
		size_t enq = m_enqueue_pos.load(std::memory_order_acquire);
		size_t deq = m_dequeue_pos.load(std::memory_order_acquire);

		return enq == deq;
	}

	/// Current number of elements (acquire loads).
	size_t Size() const
	{
		size_t enq = m_enqueue_pos.load(std::memory_order_acquire);
		size_t deq = m_dequeue_pos.load(std::memory_order_acquire);

		return (enq >= deq) ? (enq - deq) : 0;
	}

private:
	size_t m_capacity;                   ///< Power-of-2 capacity.
	size_t m_mask;                       ///< Bitmask for modulo (capacity - 1).
	std::unique_ptr<Slot[]> m_slots;     ///< Ring buffer of slots.
	alignas(64) std::atomic<size_t> m_enqueue_pos{ 0 };  ///< Producer position (cache-line aligned).
	alignas(64) std::atomic<size_t> m_dequeue_pos{ 0 };  ///< Consumer position (cache-line aligned).
};