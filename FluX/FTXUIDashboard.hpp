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
#include <cstdint>
#include <string>
#include <thread>
#include <atomic>

namespace flux {

class Scheduler;
/// Quick and dirty Interactive terminal dashboard (FTXUI) for FluX.
///
/// Lets the user type a task count, launch a benchmark run against the
/// already-running Scheduler, and watch live stats (completed/failed/
/// in-progress/steals/throughput) update while it runs.
///
/// IMPORTANT: Scheduler's stat counters (GetTasksCompleted/Failed/InProgress)
/// are cumulative for the Scheduler's whole lifetime, not per-run. Running
/// the benchmark more than once from the dashboard without restarting the
/// process means task_ids keep climbing and the counters keep accumulating.
/// RunBenchmarkThread() below snapshots the counters before submitting so
/// each run's displayed stats are a DELTA against that baseline, not a
/// reset, there is no way to reset the Scheduler's own atomics short of
/// destroying and recreating it (which would also tear down and respawn
/// every worker thread).
class FTXUIDashboard
{
public:
    /// @param scheduler Must already have had Run() called (workers started)
    /// before Run() on this dashboard is invoked.
    explicit FTXUIDashboard(Scheduler& scheduler);
    ~FTXUIDashboard();

    FTXUIDashboard(const FTXUIDashboard&) = delete;
    FTXUIDashboard& operator=(const FTXUIDashboard&) = delete;

    /// Blocks until the user quits (the "Quitter" button, or 'q').
    /// If a benchmark is still running when the user quits, this waits for
    /// its submission loop to finish (the background thread is joined)
    /// before returning, it does NOT wait for already-submitted tasks
    /// still in-flight on the Scheduler's workers, since those keep running
    /// independently of the dashboard.
    void Run();

private:
    /// Runs on a background thread so the UI stays responsive while
    /// submitting. Submits num_tasks fire-and-forget tasks, records
    /// submit/exec/total timings, and flips m_benchmark_running back to
    /// false when the Scheduler reports every submitted task has completed
    /// or failed.
    void RunBenchmarkThread(int64_t num_tasks);

    Scheduler& m_scheduler;

    std::string m_input_value = "10000";
    std::string m_status_line;

    std::atomic<bool> m_benchmark_running{ false };
    std::atomic<bool> m_quit_requested{ false };

    // Snapshot of Scheduler counters taken right before submission starts,
    // so the dashboard can show THIS run's numbers instead of the
    // Scheduler's lifetime totals. See class comment above.
    std::atomic<int64_t>  m_baseline_completed{ 0 };
    std::atomic<int64_t>  m_baseline_failed{ 0 };
    std::atomic<int64_t> m_baseline_stolen{ 0 };
    // NOTE: no per-priority baseline array here (there used to be one).
    // Per-priority stats are isolated per-run via Scheduler::ResetPriorityStats(),
    // called at the top of RunBenchmarkThread() -- see the comment there for
    // why baseline-subtraction doesn't work for a cumulative max.

    std::atomic<int64_t>  m_last_num_tasks{ 0 };
    std::atomic<int64_t>  m_last_submit_ms{ 0 };
    std::atomic<int64_t>  m_last_exec_ms{ 0 };
    std::atomic<int64_t>  m_last_total_ms{ 0 };
    std::atomic<double>   m_last_throughput{ 0.0 };

    std::thread m_benchmark_thread; ///< Joined in Run()/dtor before returning / on next launch.
};

} // namespace flux