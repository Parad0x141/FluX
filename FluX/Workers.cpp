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

#include "Workers.hpp"
#include <iostream>


Workers::Workers(int hardware_threads_count, Executor executor)
    : m_hardware_threads_count(hardware_threads_count),
    m_executor(std::move(executor))
{
    m_workers.reserve(hardware_threads_count);

    for (int i = 0; i < hardware_threads_count; ++i)
    {
        m_workers.emplace_back(std::make_unique<Worker>());
    }

    for (int i = 0; i < hardware_threads_count; ++i)
    {
        m_workers[i]->thread = std::thread(
            [this, i]
            {
                WorkerLoop(static_cast<size_t>(i));
            });
    }
}

Workers::~Workers()
{
    for (auto& w : m_workers)
    {
        w->shutdown.store(true, std::memory_order_release);
        if (w->thread.joinable()) w->thread.join();
    }
}

void Workers::WorkerLoop(size_t index)
{
    std::cout << "Worker start " << index << "\n";

    Worker& self = *m_workers[index];
    self.id = std::this_thread::get_id();

    int idle_spins = 0;

    while (true)
    {
        Task* task_ptr = nullptr;
        bool has_task = false;

        // 1. Pop local : high_queue puis normal_queue
        has_task = self.high_queue.PopBottom(task_ptr);
        if (!has_task) has_task = self.normal_queue.PopBottom(task_ptr);

        // 2. Drain inject
        if (!has_task)
        {
            DrainInjectQueue(index);
            has_task = self.high_queue.PopBottom(task_ptr);
            if (!has_task) has_task = self.normal_queue.PopBottom(task_ptr);
        }

        // 3. Steal
        if (!has_task)
        {
            Task stolen_task;
            if (TrySteal(index, stolen_task))
            {
                idle_spins = 0;
                try {
                    if (m_executor)
                    {
                        m_executor(std::move(stolen_task));
                    }
                    else
                    {
                        stolen_task.payload();
                    }
                }
                catch (const std::exception& e) {
                    std::cerr << "Worker " << index << ": stolen task threw: " << e.what() << "\n";
                }
                catch (...)
                {
                    std::cerr << "Worker " << index << ": stolen task threw unknown exception\n";
                }
                continue;
            }
        }

        if (!has_task)
        {
            if (self.shutdown.load(std::memory_order_acquire)) return;

            ++idle_spins;
            if (idle_spins < 1000)
            {
                std::this_thread::yield();
            }
            else
            {
                std::this_thread::sleep_for(std::chrono::microseconds(200));
            }
            continue;
        }

        idle_spins = 0;

        if (task_ptr)
        {
            if (!task_ptr->payload)
            {
#ifdef FLUX_DEBUG_DUPES
                std::cerr << "[Workers] RELEASE site=local-empty-skip worker=" << index
                    << " task_id=" << task_ptr->task_id
                    << " ptr=" << static_cast<void*>(task_ptr)
                    << " thread=" << std::this_thread::get_id() << "\n";
#endif
                std::cerr << "Warning: empty payload in local task (task_id=" << task_ptr->task_id << "), skipping\n";
                self.pool.Release(task_ptr);
                std::this_thread::yield();
                continue;
            }

            Task task = std::move(*task_ptr);
            self.pool.Release(task_ptr);

            if (m_executor)
            {
                m_executor(std::move(task));
            }
            else
            {
                task.payload();
            }
        }
    }
}

/// Move tasks from injection queues to the local Chase-Lev deques.
/// Drains queues in priority order (High -> AboveNormal -> Normal -> Low).
/// Tasks with empty payload are released immediately (same as original).
/// If the target deque is full, attempts to overflow high-priority tasks
/// into normal_queue if it has spare capacity (threshold = 1024).
/// If still full, re-injects the task into the same injection queue
/// and stops draining that priority.
void Workers::DrainInjectQueue(size_t index)
{
    Worker& self = *m_workers[index];

    // Iterate from lowest priority (0) up to highest (3). This is the
    // OPPOSITE of what you'd naively want, but it's correct given how
    // ChaseLevDeque's two ends behave differently:
    //   - Owner pops from the bottom (PopBottom), LIFO: the MOST RECENTLY
    //     pushed item comes out first.
    //   - Thieves steal from the top (StealTop), FIFO: the OLDEST pushed
    //     item goes first.
    // high_queue is shared by AboveNormal and High; normal_queue is shared
    // by Low and Normal. Draining highest-first (the old order) pushed the
    // higher priority in EARLIER, so it ended up UNDER the lower priority
    // pushed right after it, the owner's next PopBottom would then return
    // the lower priority, not the higher one. That's backwards, and it's
    // exactly what made High/AboveNormal statistically indistinguishable in
    // the benchmark (their local completion order was essentially random
    // w.r.t. label) and let Normal beat Low locally.
    // Draining lowest-first instead means the higher priority within each
    // shared queue is always the LAST one pushed on a given drain pass, so
    // it sits at the bottom and is the next thing PopBottom returns... which
    // giving it real preference over its queue-mate for local execution,
    // which is by far the dominant path (>98% of tasks here never get
    // stolen at all). The trade-off: thieves now steal the LOWER of the two
    // priorities first (it's older, sits at top), acceptable, since it
    // keeps the higher priority local rather than shipping it off to
    // another worker's queue.
    for (int p = 0; p <= 3; ++p)
    {
        Task* task_ptr = nullptr;
        while (self.inject_queues[p].TryPop(task_ptr))
        {
            // Sentinel
            if (!task_ptr || !task_ptr->payload)
            {
                if (task_ptr)
                {
#ifdef FLUX_DEBUG_DUPES
                    std::cerr << "[Workers] RELEASE site=drain-invalid worker=" << index
                        << " task_id=" << task_ptr->task_id
                        << " ptr=" << static_cast<void*>(task_ptr)
                        << " thread=" << std::this_thread::get_id() << "\n";
#endif
                    std::cerr << "Warning: invalid task in injection queue, releasing\n";
                    self.pool.Release(task_ptr);
                }
                continue; // Skip to next task in same queue
            }

            bool pushed = false;

            if (p >= 2) // AboveNormal (2) or High (3)
            {
                pushed = self.high_queue.PushBottom(task_ptr);
                // Overflow into normal_queue if high is full and normal has room
                if (!pushed && self.normal_queue.Size() < 1024)
                {
                    pushed = self.normal_queue.PushBottom(task_ptr);
                    // (Optional: log overflow for debugging)
                }
            }
            else // Low (0) or Normal (1)
            {
                pushed = self.normal_queue.PushBottom(task_ptr);
                // Never overflow low/normal into high_queue
            }

            if (!pushed)
            {
                self.requeue_stall_hits.fetch_add(1, std::memory_order_relaxed);
                while (!self.inject_queues[p].TryPush(task_ptr))
                {
                    self.requeue_stall_spins.fetch_add(1, std::memory_order_relaxed);
                    std::this_thread::yield();
                }
                break;
            }
        }
    }
}

/// Submit a task from external thread (e.g., Scheduler::AddTask).
/// Round-robin selects target worker, acquires slot from its pool, pushes to
/// the injection queue corresponding to task.priority.
/// @param task Reference to the task to be submitted (moved into pool).
/// @param priority Task priority (used to select the injection queue).
/// @return true if enqueued, false if worker's pool or injection queue full.
bool Workers::SubmitTask(Task& task, TaskPriority priority)
{
    size_t idx = SelectWorker();
    Worker& w = *m_workers[idx];

    size_t slot_index = 0;
    Task* task_ptr = w.pool.Acquire(&slot_index);
    if (!task_ptr) return false;

    *task_ptr = std::move(task);
    task_ptr->pool_slot_index = slot_index;

    // La seule modification : utiliser inject_queues avec l'index de priorité
    bool injected = w.inject_queues[static_cast<int>(priority)].TryPush(task_ptr);
    if (!injected)
    {
        task = std::move(*task_ptr);
#ifdef FLUX_DEBUG_DUPES
        std::cerr << "[Workers] RELEASE site=submit-rollback worker=" << idx
            << " task_id=" << task_ptr->task_id
            << " ptr=" << static_cast<void*>(task_ptr)
            << " thread=" << std::this_thread::get_id() << "\n";
#endif
        w.pool.Release(task_ptr);
        return false;
    }
    return true;
}

/// Attempt to steal a task from another worker (called by thief worker).
/// @param thief_index Index of stealing worker.
/// @param out_task Receives stolen task on success.
/// @return true if steal succeeded.
/// Steals from high_queue first, then normal_queue of each victim.
bool Workers::TrySteal(size_t thief_index, Task& out_task)
{
    Task* task_ptr = nullptr;
    size_t start = m_steal_start.fetch_add(1, std::memory_order_relaxed) % m_workers.size();
    size_t victim_index = 0;

    // Pass 1: scan every victim's high_queue first. The previous version
    // exhausted a single victim (its high_queue, THEN its normal_queue)
    // before moving to the next victim, so a thief could steal a
    // Low/Normal task from an early victim while a High/AboveNormal task
    // was sitting unstolen on a later victim, just because of scan order.
    // That let a starved High task's worst-case latency stay just as bad as
    // Low/Normal's even after local pop order was fixed. Two full passes
    // costs more in the worst case (steal only succeeds on pass 2), but
    // stealing is already the rare path here (~1-2% of tasks), so it's a
    // fine trade for actually honoring priority across workers.
    for (size_t offset = 0; offset < m_workers.size(); ++offset)
    {
        size_t i = (start + offset) % m_workers.size();
        if (i == thief_index) continue;

        if (m_workers[i]->high_queue.StealTop(task_ptr))
        {
            victim_index = i;
            goto stolen;
        }
    }

    // Pass 2: no High/AboveNormal work anywhere to steal, fall back to
    // normal_queue across the same victim order.
    for (size_t offset = 0; offset < m_workers.size(); ++offset)
    {
        size_t i = (start + offset) % m_workers.size();
        if (i == thief_index) continue;

        if (m_workers[i]->normal_queue.StealTop(task_ptr))
        {
            victim_index = i;
            goto stolen;
        }
    }
    return false;

stolen:
    if (!task_ptr || !task_ptr->payload)
    {
        if (task_ptr)
        {
#ifdef FLUX_DEBUG_DUPES
            std::cerr << "[Workers] RELEASE site=steal-empty thief=" << thief_index
                << " victim=" << victim_index
                << " task_id=" << task_ptr->task_id
                << " ptr=" << static_cast<void*>(task_ptr)
                << " thread=" << std::this_thread::get_id() << "\n";
#endif
            std::cerr << "Warning: stolen task with empty payload, releasing\n";
            m_workers[victim_index]->pool.Release(task_ptr);
        }
        return false;
    }

    out_task = std::move(*task_ptr);
    m_workers[victim_index]->pool.Release(task_ptr);
    steal_count.fetch_add(1, std::memory_order_relaxed);
    return true;
}

size_t Workers::SelectWorker()
{
    size_t count = m_workers.size();
    if (count == 0)
        return 0;

    size_t idx = m_round_robin_index.fetch_add(1, std::memory_order_relaxed);
    return idx % count;
}

size_t Workers::GetQueueSize(size_t worker_index) const
{
    if (worker_index >= m_workers.size()) return 0;
    const Worker& w = *m_workers[worker_index];
    size_t total = w.high_queue.Size() + w.normal_queue.Size();
    for (const auto& q : w.inject_queues)
    {
        total += q.Size();
    }
    return total;
}

bool Workers::IsIdle(size_t worker_index) const
{
    if (worker_index >= m_workers.size()) return true;
    const Worker& w = *m_workers[worker_index];
    if (!w.high_queue.IsEmpty() || !w.normal_queue.IsEmpty()) return false;
    for (const auto& q : w.inject_queues) 
    {
        if (!q.IsEmpty()) return false;
    }
    return true;
}


RequeueStallStats Workers::GetRequeueStallStats() const
{
    RequeueStallStats out;
    for (const auto& w : m_workers)
    {
        out.hits += w->requeue_stall_hits.load(std::memory_order_relaxed);
        out.spins += w->requeue_stall_spins.load(std::memory_order_relaxed);
    }
    return out;
}