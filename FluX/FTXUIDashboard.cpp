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

#include "FTXUIDashboard.hpp"

#include <algorithm>
#include <chrono>
#include <iomanip>
#include <sstream>

#include <ftxui/component/component.hpp>
#include <ftxui/component/component_options.hpp>
#include <ftxui/component/event.hpp>
#include <ftxui/component/screen_interactive.hpp>
#include <ftxui/dom/elements.hpp>

#include "Scheduler.hpp"
#include "Types.hpp"

using namespace flux;

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

    m_benchmark_running.store(false, std::memory_order_release);
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

        auto stall = m_scheduler.GetRequeueStallStats();


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

        if (!running && m_last_total_ms.load(std::memory_order_relaxed) > 0)
        {
            std::ostringstream tp;
            tp << std::fixed << std::setprecision(2)
                << (m_last_throughput.load(std::memory_order_relaxed) / 1'000'000.0);

            stat_rows.push_back(separator());
            stat_rows.push_back(text("Last Run:") | bold);
            stat_rows.push_back(hbox({ text("  Submit Time     : "), text(std::to_string(m_last_submit_ms.load(std::memory_order_relaxed)) + " milliseconds") }));
            stat_rows.push_back(hbox({ text("  Execution Time  : "), text(std::to_string(m_last_exec_ms.load(std::memory_order_relaxed)) + " milliseconds") }));
            stat_rows.push_back(hbox({ text("  Total Time      : "), text(std::to_string(m_last_total_ms.load(std::memory_order_relaxed)) + " milliseconds") }));
            stat_rows.push_back(hbox({ text("  Throughput      : "), text(tp.str() + " million tasks per second") }));
            stat_rows.push_back(hbox({ text("  Requeue stall hits    : "), text(std::to_string(stall.hits)) }));
            stat_rows.push_back(hbox({ text("  Total spins    : "), text(std::to_string(stall.spins)) }));

            stat_rows.push_back(separator());
            stat_rows.push_back(text("Per-Priority Latency (Fresh every run):") | bold);

            static constexpr const char* kPrioNames[] = { "Low", "Normal", "AboveNormal", "High" };
            for (int p = 0; p < 4; ++p)
            {
                auto s = m_scheduler.GetPriorityStats(static_cast<TaskPriority>(p));
                std::ostringstream avg, max;
                avg << std::fixed << std::setprecision(2) << s.avg_latency_us;
                max << std::fixed << std::setprecision(2) << s.max_latency_us;

                stat_rows.push_back(hbox({
                    text(std::string("  ") + kPrioNames[p]) | size(WIDTH, EQUAL, 14),
                    text("completed: " + std::to_string(s.completed_count)) | size(WIDTH, EQUAL, 20),
                    text("avg: " + avg.str() + " us") | size(WIDTH, EQUAL, 18),
                    text("max: " + max.str() + " us"),
                    }));
            }

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