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
#include <chrono>
#include <cstdint>
#include <deque>
#include <string>
#include <thread>
#include <vector>
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

        /// One-shot end-of-run report writer. Called exactly once at the tail
        /// of RunBenchmarkThread(), after every stat for this run has reached
        /// its final value -- deliberately NOT called during the polling loop,
        /// so the benchmark's own timing never absorbs file I/O jitter (see
        /// call site comment). Appends to flux_stats.txt in the working
        /// directory; failure to open is silently ignored (a benchmark run
        /// that otherwise succeeded shouldn't be reported as failed over a
        /// missing/read-only log file).
        void WriteStatsReport(
            int64_t num_tasks,
            int64_t baseline_completed,
            int64_t baseline_failed,
            int64_t baseline_stolen,
            const std::vector<uint64_t>& baseline_worker_busy_ns,
            std::chrono::high_resolution_clock::time_point start,
            std::chrono::high_resolution_clock::time_point end,
            std::chrono::high_resolution_clock::time_point submit_end) const;

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

        // --- Per-worker utilization tracking -----------------------------
        // Sampled once per render tick (renderer runs on the UI thread only,
        // driven by screen.Loop() plus the ~80ms ticker's PostEvent -- see
        // Run() -- so no synchronization needed on these members). Utilization
        // itself is derived, not stored by the Scheduler: each tick we diff
        // Scheduler::GetWorkerBusyNs() against the previous sample and divide
        // by elapsed wall time. See Workers::busy_ns for why this is safe to
        // sample from any thread (single relaxed atomic, monotonic).

        /// Previous tick's cumulative busy_ns per worker. Resized lazily once
        /// GetWorkerCount() is known (first render after Scheduler::Run()).
        std::vector<uint64_t> m_prev_busy_ns;
        std::chrono::steady_clock::time_point m_prev_sample_time;
        bool m_utilization_initialized = false;

        /// Latest instantaneous per-worker utilization, 0-100. Drives the
        /// per-worker gauge rows.
        std::vector<int> m_worker_utilization;

        /// Rolling history of the CLUSTER-AVERAGE utilization (0-100, one
        /// sample per render tick), feeds the ftxui graph(). Capped so a long
        /// dashboard session doesn't grow this unbounded; only the most recent
        /// window is ever shown anyway.
        std::deque<int> m_utilization_history;
        static constexpr size_t kUtilizationHistoryCap = 300;
    };

} // namespace flux