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

#define WIN32_LEAN_AND_MEAN
#define NOMINMAX
#include <windows.h>

#include "FTXUIDashboard.hpp"

#include <algorithm>
#include <chrono>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <numeric>
#include <sstream>

#include <ftxui/component/component.hpp>
#include <ftxui/component/component_options.hpp>
#include <ftxui/component/event.hpp>
#include <ftxui/component/screen_interactive.hpp>
#include <ftxui/dom/elements.hpp>

#include "Scheduler.hpp"
#include "Types.hpp"

using namespace flux;

namespace {

    /// Directory containing the running executable, e.g. for locating
    /// flux_stats.txt next to the .exe regardless of the process's current
    /// working directory (which differs when launched from an IDE debugger,
    /// a shortcut with a different "Start in" folder, etc.). Computed once
    /// and cached: GetModuleFileNameA(nullptr, ...) queries the CURRENT
    /// process's own module, which never changes for the lifetime of the
    /// process, so there's no reason to re-query it on every write.
    const std::filesystem::path& GetExecutableDir()
    {
        static const std::filesystem::path dir = [] {
            char buf[MAX_PATH];
            DWORD len = GetModuleFileNameA(nullptr, buf, MAX_PATH);
            if (len == 0 || len == MAX_PATH)
                return std::filesystem::path(); // Failed/truncated: fall back to relative (cwd).
            return std::filesystem::path(buf).parent_path();
            }();
        return dir;
    }

} // namespace

FTXUIDashboard::FTXUIDashboard(Scheduler& scheduler)
    : m_scheduler(scheduler)
{
}

FTXUIDashboard::~FTXUIDashboard()
{
    m_quit_requested.store(true, std::memory_order_relaxed);
    if (m_benchmark_thread.joinable())
        m_benchmark_thread.join();
}

void FTXUIDashboard::RunBenchmarkThread(int64_t num_tasks)
{
    // Snapshot BEFORE submitting anything: Scheduler's own counters are
    // cumulative for its whole process lifetime (see class comment in the
    // header), so every number this dashboard shows for "this run" is a
    // delta against this baseline, not the Scheduler's raw totals.
    int64_t  baseline_completed = m_scheduler.GetTasksCompleted();
    int64_t  baseline_failed = m_scheduler.GetTasksFailed();
    int64_t  baseline_stolen = m_scheduler.GetTasksStolen();

    m_baseline_completed.store(baseline_completed, std::memory_order_relaxed);
    m_baseline_failed.store(baseline_failed, std::memory_order_relaxed);
    m_baseline_stolen.store(baseline_stolen, std::memory_order_relaxed);
    m_last_num_tasks.store(num_tasks, std::memory_order_relaxed);

    // Baseline busy_ns per worker, same delta-against-snapshot idea as
    // completed/failed/stolen above, but for the end-of-run text report
    // (see WriteStatsReport): lets us report each worker's AVERAGE
    // utilization over just this run instead of since process start.
    // Local to this function/thread only -- no atomics needed, nothing else
    // touches this vector concurrently (see class comment on why a second
    // benchmark run can't start until this thread has joined).
    size_t worker_count = m_scheduler.GetWorkerCount();
    std::vector<uint64_t> baseline_worker_busy_ns(worker_count, 0);
    for (size_t i = 0; i < worker_count; ++i)
        baseline_worker_busy_ns[i] = m_scheduler.GetWorkerBusyNs(i);

    // Per-priority stats (completed_count/avg/max latency) are cumulative
    // for the process lifetime (see Scheduler::GetPriorityStats). Unlike
    // completed/failed/stolen above, they can't be fixed up with a baseline
    // snapshot + subtraction after the fact: max_latency_ns in particular is
    // not reducible that way (a lower max this run wouldn't overwrite a
    // higher max left over from the previous run). Reset them here instead,
    // right before submitting safe because we only reach this point once
    // the previous run's thread has fully joined (see the launch_button
    // handler in Run()), so nothing is in flight to race with the reset.
    m_scheduler.ResetPriorityStats();

    auto start = std::chrono::high_resolution_clock::now();

    // Mixed priority distribution: skewed toward Normal (dominant in
    // practice) but enough High/AboveNormal/Low traffic to actually
    // exercise high_queue routing, overflow, and scan order -- previously
    // every task here was TaskPriority::Normal, which validated nothing
    // about priority handling.
    for (int64_t i = 0; i < num_tasks; ++i)
    {
        Task task;
        task.payload = [i]
            {
                // Same minimal synthetic workload as the console benchmark in
                // FluX.cpp, keeps the two modes comparable.
                volatile int x = 0;
                for (int j = 0; j < 100; ++j) x += j;
                (void)x;
            };

        int roll = static_cast<int>(i % 100);
        if (roll < 5)        task.priority = TaskPriority::Low;
        else if (roll < 75)  task.priority = TaskPriority::Normal;
        else if (roll < 90)  task.priority = TaskPriority::AboveNormal;
        else                 task.priority = TaskPriority::High;

        m_scheduler.AddTask(std::move(task));
    }

    auto submit_end = std::chrono::high_resolution_clock::now();
    m_last_submit_ms.store(
        std::chrono::duration_cast<std::chrono::milliseconds>(submit_end - start).count(),
        std::memory_order_relaxed);

    // Poll until every task submitted THIS run has completed or failed.
    // Same completion condition as the console benchmark, just expressed as
    // a delta against the baseline snapshotted above.
    while (!m_quit_requested.load(std::memory_order_relaxed))
    {
        int64_t completed = m_scheduler.GetTasksCompleted() - baseline_completed;
        int64_t failed = m_scheduler.GetTasksFailed() - baseline_failed;
        int64_t in_progress = m_scheduler.GetTasksInProgress();

        if (completed + failed >= num_tasks && in_progress == 0)
            break;

        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    auto end = std::chrono::high_resolution_clock::now();
    int64_t total_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    m_last_total_ms.store(total_ms, std::memory_order_relaxed);
    m_last_exec_ms.store(
        std::chrono::duration_cast<std::chrono::milliseconds>(end - submit_end).count(),
        std::memory_order_relaxed);
    m_last_throughput.store(total_ms > 0 ? (num_tasks * 1000.0 / static_cast<double>(total_ms)) : 0.0,
        std::memory_order_relaxed);

    // Single write, right here, AFTER the run is fully done and every stat
    // below is its final value for this run deliberately not written
    // incrementally during polling above, so the benchmark's own timing
    // (submit/exec/total_ms) never includes file I/O jitter.
    WriteStatsReport(num_tasks, baseline_completed, baseline_failed, baseline_stolen,
        baseline_worker_busy_ns, start, end, submit_end);

    m_benchmark_running.store(false, std::memory_order_release);
}

void FTXUIDashboard::WriteStatsReport(
    int64_t num_tasks,
    int64_t baseline_completed,
    int64_t baseline_failed,
    int64_t baseline_stolen,
    const std::vector<uint64_t>& baseline_worker_busy_ns,
    std::chrono::high_resolution_clock::time_point start,
    std::chrono::high_resolution_clock::time_point end,
    std::chrono::high_resolution_clock::time_point submit_end) const
{
    int64_t completed = m_scheduler.GetTasksCompleted() - baseline_completed;
    int64_t failed = m_scheduler.GetTasksFailed() - baseline_failed;
    int64_t stolen = m_scheduler.GetTasksStolen() - baseline_stolen;
    int64_t submit_ms = std::chrono::duration_cast<std::chrono::milliseconds>(submit_end - start).count();
    int64_t exec_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - submit_end).count();
    int64_t total_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    double throughput = total_ms > 0 ? (num_tasks * 1000.0 / static_cast<double>(total_ms)) : 0.0;
    auto stall = m_scheduler.GetRequeueStallStats();

    // Run-duration window used for per-worker average utilization below.
    // Same [start, end] window as total_ms, just kept as ns for precision.
    int64_t run_duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();

    std::ostringstream out;

    auto now_c = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    std::tm tm_buf{};
    localtime_s(&tm_buf, &now_c);
    out << "==================== FluX benchmark run ====================\n";
    out << "Timestamp        : " << std::put_time(&tm_buf, "%Y-%m-%d %H:%M:%S") << "\n";
    out << "Tasks submitted  : " << num_tasks << "\n";
    out << "Completed        : " << completed << "\n";
    out << "Failed           : " << failed << "\n";
    out << "Stolen           : " << stolen << "\n";
    out << "Submit time (ms) : " << submit_ms << "\n";
    out << "Exec time (ms)   : " << exec_ms << "\n";
    out << "Total time (ms)  : " << total_ms << "\n";
    out << std::fixed << std::setprecision(3);
    out << "Throughput       : " << (throughput / 1'000'000.0) << " million tasks/sec\n";
    out << "Requeue stalls   : hits=" << stall.hits << " spins=" << stall.spins << "\n";

    out << "\n--- Per-priority latency ---\n";
    static constexpr const char* kPrioNames[] = { "Low", "Normal", "AboveNormal", "High" };
    for (int p = 0; p < 4; ++p)
    {
        auto s = m_scheduler.GetPriorityStats(static_cast<TaskPriority>(p));

        // Build the "123.45us"-style strings via ostringstream with
        // fixed/setprecision applied BEFORE the value is formatted --
        // std::to_string(double) has no way to honor precision (it ignores
        // stream state entirely, always ~6 decimals), which is what
        // produced "211.409054us" instead of "211.41us" here.
        std::ostringstream avg, max;
        avg << std::fixed << std::setprecision(2) << s.avg_latency_us << "us";
        max << std::fixed << std::setprecision(2) << s.max_latency_us << "us";

        // setw() only pads when the field is SHORTER than the width; a
        // value long enough to already exceed setw(10) (e.g. a 5-digit
        // max_latency_us) would collide directly into "max=" with no
        // separating space, same bug as above but on width instead of
        // precision. Explicit padding via std::left | std::setw on each
        // completed field, then a literal space before every label,
        // guarantees a separator regardless of value width.
        out << std::left << std::setw(14) << kPrioNames[p]
            << "completed=" << std::left << std::setw(12) << s.completed_count
            << "avg=" << std::left << std::setw(12) << avg.str()
            << "max=" << max.str() << "\n";
    }

    out << "\n--- Per-worker average utilization (this run) ---\n";
    double total_util = 0.0;
    for (size_t i = 0; i < baseline_worker_busy_ns.size(); ++i)
    {
        uint64_t busy_delta = m_scheduler.GetWorkerBusyNs(i) - baseline_worker_busy_ns[i];
        double util = run_duration_ns > 0
            ? std::min(1.0, static_cast<double>(busy_delta) / static_cast<double>(run_duration_ns))
            : 0.0;
        total_util += util;
        out << "  Worker " << std::setw(3) << i << " : "
            << std::fixed << std::setprecision(1) << (util * 100.0) << "%\n";
    }
    if (!baseline_worker_busy_ns.empty())
    {
        out << "  Average across " << baseline_worker_busy_ns.size() << " workers : "
            << std::fixed << std::setprecision(1)
            << (total_util / baseline_worker_busy_ns.size() * 100.0) << "%\n";
    }
    out << "==============================================================\n\n";

    // Append mode: keeps a running history across consecutive benchmark
    // runs in the same dashboard session rather than clobbering the
    // previous run's report. One ofstream open/write/close per run -- this
    // is the ONLY file I/O this class does, and only reached after the
    // benchmark's own timing has already been captured above.
    // Written next to the .exe (GetExecutableDir()), not the process's
    // current working directory -- those differ when launched from an IDE
    // debugger or a shortcut with a different "Start in" folder.
    std::ofstream file(GetExecutableDir() / "flux_stats.txt", std::ios::app);
    if (file)
        file << out.str();
    // Silent no-op on failure (e.g. read-only install directory): a missing
    // stats file shouldn't crash a benchmark run that otherwise completed fine.
}

void FTXUIDashboard::Run()
{
    using namespace ftxui;

    auto screen = ScreenInteractive::Fullscreen();

    auto input_component = Input(&m_input_value, "number of tasks");

    auto launch_button = Button("Start Benchmark", [this] {
        if (m_benchmark_running.load(std::memory_order_acquire))
            return; // Already running: ignore extra clicks.

        int64_t num_tasks = 0;
        try
        {
            num_tasks = std::stoll(m_input_value);
        }
        catch (...)
        {
            m_status_line = "Invalid input: please enter a positive whole number.";
            return;
        }

        if (num_tasks <= 0)
        {
            m_status_line = "The number of tasks must be positive.";
            return;
        }

        // Previous run's thread is guaranteed finished here: we only reach
        // this branch when m_benchmark_running just read false, and that
        // flag is the last thing RunBenchmarkThread does before returning.
        if (m_benchmark_thread.joinable())
            m_benchmark_thread.join();

        m_status_line.clear();
        m_benchmark_running.store(true, std::memory_order_release);
        m_benchmark_thread = std::thread(&FTXUIDashboard::RunBenchmarkThread, this, num_tasks);
        });

    auto exit_closure = screen.ExitLoopClosure();
    auto quit_button = Button("Quit", [this, exit_closure] {
        m_quit_requested.store(true, std::memory_order_relaxed);
        exit_closure();
        });

    auto layout = Container::Vertical({
        input_component,
        launch_button,
        quit_button,
        });

    auto renderer = Renderer(layout, [&] {
        bool running = m_benchmark_running.load(std::memory_order_acquire);

        int64_t completed = m_scheduler.GetTasksCompleted() - m_baseline_completed.load(std::memory_order_relaxed);
        int64_t failed = m_scheduler.GetTasksFailed() - m_baseline_failed.load(std::memory_order_relaxed);
        int64_t in_progress = m_scheduler.GetTasksInProgress();
        int64_t stolen = m_scheduler.GetTasksStolen() - m_baseline_stolen.load(std::memory_order_relaxed);
        size_t queued = m_scheduler.GetTasksQueued();

        // --- Per-worker utilization sample --------------------------
        // Diff cumulative busy_ns against the previous tick's sample, over
        // the wall time elapsed between ticks. Runs unconditionally
        // (not gated on `running`) so the graph/gauges reflect the workers
        // at rest too, not just during a benchmark.
        size_t worker_count = m_scheduler.GetWorkerCount();
        auto sample_time = std::chrono::steady_clock::now();

        if (worker_count > 0)
        {
            if (!m_utilization_initialized || m_prev_busy_ns.size() != worker_count)
            {
                // First sample (or worker count changed, which shouldn't
                // happen post-Run() but keeps this robust): just baseline,
                // no delta to compute yet.
                m_prev_busy_ns.assign(worker_count, 0);
                m_worker_utilization.assign(worker_count, 0);
                for (size_t i = 0; i < worker_count; ++i)
                    m_prev_busy_ns[i] = m_scheduler.GetWorkerBusyNs(i);
                m_prev_sample_time = sample_time;
                m_utilization_initialized = true;
            }
            else
            {
                double elapsed_ns = static_cast<double>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(
                        sample_time - m_prev_sample_time).count());

                if (elapsed_ns > 0.0)
                {
                    uint64_t total_busy_delta = 0;
                    for (size_t i = 0; i < worker_count; ++i)
                    {
                        uint64_t busy = m_scheduler.GetWorkerBusyNs(i);
                        uint64_t delta = busy - m_prev_busy_ns[i]; // monotonic counter, no underflow
                        double util = std::min(1.0, static_cast<double>(delta) / elapsed_ns);
                        m_worker_utilization[i] = static_cast<int>(util * 100.0);
                        total_busy_delta += delta;
                        m_prev_busy_ns[i] = busy;
                    }
                    m_prev_sample_time = sample_time;

                    double avg_util = std::min(1.0,
                        (static_cast<double>(total_busy_delta) / worker_count) / elapsed_ns);
                    m_utilization_history.push_back(static_cast<int>(avg_util * 100.0));
                    if (m_utilization_history.size() > kUtilizationHistoryCap)
                        m_utilization_history.pop_front();
                }
            }
        }

        int64_t num_tasks = m_last_num_tasks.load(std::memory_order_relaxed);
        float progress = (num_tasks > 0)
            ? std::min(1.0f, static_cast<float>(completed + failed) / static_cast<float>(num_tasks))
            : 0.0f;

        Element status = running
            ? (text("Running...") | color(Color::Yellow) | bold)
            : (text("Ready") | color(Color::Green) | bold);

        Elements stat_rows = {
            hbox({ text("Status              : "), status }),
            hbox({ text("Progress            : "), gauge(progress) | flex,
                   text(" " + std::to_string(static_cast<int64_t>(progress * 100)) + "%") }),
            separator(),
            hbox({ text("Completed                      : "), text(std::to_string(completed)) }),
            hbox({ text("Currently Running              : "), text(std::to_string(in_progress)) }),
            hbox({ text("Failed                         : "), text(std::to_string(failed)) }),
            hbox({ text("Stolen                         : "), text(std::to_string(stolen)) }),
            hbox({ text("Waiting                        : "), text(std::to_string(queued)) }),
        };

        if (worker_count > 0)
        {
            stat_rows.push_back(separator());
            stat_rows.push_back(text("Worker Utilization (" + std::to_string(worker_count) + " workers):") | bold);

            // Cluster-average history as a scrolling graph: most recent
            // sample on the right edge, oldest scrolls off the left, same
            // convention as e.g. a system monitor's CPU graph.
            auto utilization_graph = graph([this](int width, int height) {
                std::vector<int> out(static_cast<size_t>(std::max(width, 0)), 0);
                int hist_size = static_cast<int>(m_utilization_history.size());
                for (int x = 0; x < width; ++x)
                {
                    int idx = hist_size - width + x;
                    if (idx < 0 || idx >= hist_size) continue;
                    int pct = m_utilization_history[static_cast<size_t>(idx)];
                    out[static_cast<size_t>(x)] = std::clamp(pct * height / 100, 0, height > 0 ? height - 1 : 0);
                }
                return out;
                });

            stat_rows.push_back(utilization_graph | color(Color::Cyan) | size(HEIGHT, EQUAL, 8) | border);

            // Instantaneous per-worker breakdown: one gauge row each, so
            // e.g. starvation/imbalance across workers is visible at a
            // glance rather than hidden inside the cluster average above.
            for (size_t i = 0; i < m_worker_utilization.size(); ++i)
            {
                float u = static_cast<float>(m_worker_utilization[i]) / 100.0f;
                stat_rows.push_back(hbox({
                    text("  W" + std::to_string(i)) | size(WIDTH, EQUAL, 5),
                    gauge(u) | flex,
                    text(" " + std::to_string(m_worker_utilization[i]) + "%") | size(WIDTH, EQUAL, 5),
                    }));
            }
        }

        // Raw "Last Run" timings and "Per-Priority Latency" text tables
        // used to live here -- they're now written once to flux_stats.txt
        // at the end of each run instead (see WriteStatsReport()), so the
        // live view stays the graph/gauges + the running counters above
        // rather than a wall of numbers competing with them for space.
        if (!running && m_last_total_ms.load(std::memory_order_relaxed) > 0)
        {
            stat_rows.push_back(separator());
            stat_rows.push_back(text("Last run's full stats -> flux_stats.txt (next to the .exe)") | dim);
        }

        if (!m_status_line.empty())
            stat_rows.push_back(text(m_status_line) | color(Color::Red));

        return vbox({
            text("FluX -- Dashboard") | bold | color(Color::Cyan),
            separator(),
            hbox({ text("Number of Tasks : "), input_component->Render() | flex }),
            hbox({ launch_button->Render(), text("  "), quit_button->Render() }),
            separator(),
            vbox(stat_rows),
            }) | border;
        });

    // FTXUI only redraws in response to a posted Task/Event. without this,
    // the screen would freeze between keypresses/clicks while a benchmark
    // runs on RunBenchmarkThread in the background. PostEvent is the
    // documented thread-safe way to nudge the loop from another thread.
    std::thread ticker([&] {
        while (!m_quit_requested.load(std::memory_order_relaxed))
        {
            screen.PostEvent(Event::Custom);
            std::this_thread::sleep_for(std::chrono::milliseconds(80));
        }
        });

    screen.Loop(renderer);

    m_quit_requested.store(true, std::memory_order_relaxed);
    if (ticker.joinable())
        ticker.join();

    if (m_benchmark_thread.joinable())
        m_benchmark_thread.join();
}