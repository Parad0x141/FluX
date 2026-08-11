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

#include <iostream>
#include <iomanip>
#include <chrono>
#include <thread>
#include <string>

#include "Scheduler.hpp"
#include "Helpers.hpp"
#include "FTXUIDashboard.hpp"


/// Console benchmark: submit and execute NUM_TASKS tasks, measure throughput.
/// Unchanged behavior from before -- this is the USE_FTXUI_DASHBOARD == false
/// path in main() below.
static void RunConsoleBenchmark(Scheduler& scheduler)
{
    std::cout << "Scheduler running, adding tasks...\n" << std::flush;

    const int64_t NUM_TASKS = 10000;
    auto start = std::chrono::high_resolution_clock::now();

    // Submit tasks
    for (int64_t i = 0; i < NUM_TASKS; ++i)
    {
        Task task;

        task.payload = [i]
            {
                // Minimal work: prevent compiler from optimizing away
                volatile int x = 0;
                for (int j = 0; j < 100; ++j) x += j;
                (void)x;
            };

        /*
        task.payload = [i]
        {
            std::this_thread::sleep_for(std::chrono::microseconds(500));
        };*/


        task.priority = TaskPriority::Normal;
        scheduler.AddTask(std::move(task));
    }

    auto submit_end = std::chrono::high_resolution_clock::now();

    // Wait for all tasks to complete (polling with 10ms sleep)
    while (true)
    {
        int64_t completed = scheduler.GetTasksCompleted();
        int64_t in_progress = scheduler.GetTasksInProgress();
        int64_t failed = scheduler.GetTasksFailed();
        if (completed + failed >= NUM_TASKS && in_progress == 0)
            break;

        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    auto end = std::chrono::high_resolution_clock::now();

    auto submit_ms = std::chrono::duration_cast<std::chrono::milliseconds>(submit_end - start).count();
    auto total_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    auto exec_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - submit_end).count();

    double throughput = NUM_TASKS * 1000.0 / total_ms;

    std::cout << "\n=== BENCHMARK RESULTS ===\n";
    std::cout << "Tasks: " << FormatWithCommas(NUM_TASKS) << "\n";
    std::cout << "Submit time: " << FormatWithCommas(submit_ms) << " ms\n";
    std::cout << "Execution time: " << FormatWithCommas(exec_ms) << " ms\n";
    std::cout << "Total time: " << FormatWithCommas(total_ms) << " ms\n";
    // std::fixed + setprecision(0): plain integer count, no scientific
    // notation. Default cout formatting has only 6 significant digits of
    // precision before it silently switches to "1.23e+06"-style output --
    // that's what was happening above 999,999 tasks/sec.
    std::cout << "Throughput: " << FormatWithCommas(static_cast<uint64_t>(throughput))
        << " tasks/sec (" << std::fixed << std::setprecision(2)
        << (throughput / 1'000'000.0) << "M/sec)\n";
    std::cout << "Completed: " << FormatWithCommas(scheduler.GetTasksCompleted()) << "\n";
    std::cout << "Failed: " << FormatWithCommas(scheduler.GetTasksFailed()) << "\n";
    std::cout << "Steals: " << FormatWithCommas(scheduler.GetTasksStolen()) << "\n";
}

int main()
{
    // Flip this to switch between the interactive FTXUI dashboard (type the
    // task count, relaunch runs live, see stats update in place) and the
    // original hardcoded console benchmark. Both paths run against the same
    // Scheduler/Workers.
    const bool USE_FTXUI_DASHBOARD = true;

    std::cout << "Starting scheduler...\n" << std::flush;
    Scheduler scheduler;
    scheduler.Run();

    if (USE_FTXUI_DASHBOARD)
    {
        FTXUIDashboard dashboard(scheduler);
        dashboard.Run(); // Blocks until the user quits from the dashboard.
    }
    else
    {
        RunConsoleBenchmark(scheduler);
    }

    return EXIT_SUCCESS;
}