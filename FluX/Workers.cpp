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
    for (auto& w : m_workers) {
        w->shutdown.store(true, std::memory_order_release);
        if (w->thread.joinable()) w->thread.join();
    }
}

void Workers::WorkerLoop(size_t index)
{
    std::cout << "Worker start " << index << "\n";

    Worker& self = *m_workers[index];
    self.id = std::this_thread::get_id();

    // Idle backoff counter. Pure yield()-spinning below used to mean an idle
    // worker burns ~100% of a core indefinitely: yield() only gives up the
    // current timeslice, it gets rescheduled again almost immediately if
    // nothing else wants the core, so a tight yield()-loop is still a busy
    // spin in practice. Fine for a benchmark that submits work immediately
    // and exits, but the FTXUI dashboard can sit idle at "Pret" for minutes,
    // so idle workers now spin-yield for a short burst (near-zero latency
    // for bursty workloads) and fall back to short sleeps once it's clear
    // there's genuinely nothing to do.
    int idle_spins = 0;

    while (true)
    {
        Task* task_ptr = nullptr;
        bool has_task = false;

        // 1. Try local Chase-Lev deque first (LIFO - freshest tasks first).
        has_task = self.queue.PopBottom(task_ptr);

        // 2. If local queue empty, drain external injection queue.
        if (!has_task)
        {
            DrainInjectQueue(index);
            has_task = self.queue.PopBottom(task_ptr);
        }

        // 3. If still no work, attempt to steal from other workers (FIFO).
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
                catch (const std::exception& e) 
                {
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

            // First ~1000 idle iterations: pure yield(), 
            // keeps latency near-zero for workloads where new tasks show up
            // every few microseconds. Past that, this worker has genuinely
            // been idle for a while: sleep briefly instead of hammering the
            // core. 200us caps idle CPU use hard while staying well under
            // human-perceptible latency for the next task to start.
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

            // Move task out of pool slot, then release slot back to pool
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

void Workers::DrainInjectQueue(size_t index)
{
    Worker& self = *m_workers[index];
    Task* task_ptr;
    int drained = 0; // TODO : Atomic stats, actually never read anywhere.

    while (self.inject_queue.TryPop(task_ptr))
    {
        if (task_ptr && task_ptr->payload)
        {
            bool pushed = self.queue.PushBottom(task_ptr);
            if (!pushed)
            {
                while (!self.inject_queue.TryPush(task_ptr))
                {
                    std::this_thread::yield();
                }
                break;
            }
            ++drained;
        }
        else
        {
            if (task_ptr)
            {
#ifdef FLUX_DEBUG_DUPES
                std::cerr << "[Workers] RELEASE site=inject-invalid worker=" << index
                    << " task_id=" << task_ptr->task_id
                    << " ptr=" << static_cast<void*>(task_ptr)
                    << " thread=" << std::this_thread::get_id() << "\n";
#endif
                std::cerr << "Warning: invalid task in inject queue, releasing\n";
                self.pool.Release(task_ptr);
            }
        }
    }
}

bool Workers::SubmitTask(Task& task, TaskPriority priority)
{
    size_t idx = SelectWorker();
    Worker& w = *m_workers[idx];

    size_t slot_index = 0;
    Task* task_ptr = w.pool.Acquire(&slot_index);
    if (!task_ptr)
    {
        return false;
    }

    *task_ptr = std::move(task);
    // The whole-object move-assignment above just overwrote task_ptr->pool_slot_index
    // with whatever value the caller's 'task' happened to carry (garbage/default for
    // a freshly-constructed Task). Restore it so TaskPool::Release() can find this
    // slot again without offsetof.
    task_ptr->pool_slot_index = slot_index;

    bool injected = w.inject_queue.TryPush(task_ptr);
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

bool Workers::TrySteal(size_t thief_index, Task& out_task)
{
    Task* task_ptr = nullptr;
    size_t start = m_steal_start.fetch_add(1, std::memory_order_relaxed) % m_workers.size();

    for (size_t offset = 0; offset < m_workers.size(); ++offset)
    {
        size_t i = (start + offset) % m_workers.size();
        if (i == thief_index) continue;

        if (m_workers[i]->queue.StealTop(task_ptr))
        {
            if (!task_ptr || !task_ptr->payload)
            {
                if (task_ptr)
                {
#ifdef FLUX_DEBUG_DUPES
                    std::cerr << "[Workers] RELEASE site=steal-empty thief=" << thief_index
                        << " victim=" << i
                        << " task_id=" << task_ptr->task_id
                        << " ptr=" << static_cast<void*>(task_ptr)
                        << " thread=" << std::this_thread::get_id() << "\n";
#endif
                    std::cerr << "Warning: stolen task with empty payload, releasing\n";
                    m_workers[i]->pool.Release(task_ptr);
                }
                continue;
            }
            out_task = std::move(*task_ptr);
            m_workers[i]->pool.Release(task_ptr);
            steal_count.fetch_add(1, std::memory_order_relaxed);

            return true;
        }
    }

    return false;
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
    if (worker_index >= m_workers.size())
        return 0;

    return m_workers[worker_index]->queue.Size() + m_workers[worker_index]->inject_queue.Size();
}

bool Workers::IsIdle(size_t worker_index) const
{
    if (worker_index >= m_workers.size())
        return true;

    return m_workers[worker_index]->queue.IsEmpty() && m_workers[worker_index]->inject_queue.IsEmpty();
}