/// FluX: a lock-free, work-stealing thread pool for C++20.
/// Designed for low-latency parallel task execution with minimal 
/// synchronization overhead and zero-allocation hot paths.

/// Code by Cyril "Parad0x141" Bouvier, 2026.
/// Last update : 09/08/2026

#include <iostream>
#include <iomanip>
#include <chrono>
#include <thread>
#include <string>

#include "Scheduler.hpp"
#include "Helpers.hpp"


/// Benchmark: submit and execute NUM_TASKS tasks, measure throughput.
/// 
/// Each task performs a small computation loop (100 iterations)
/// to simulate real work while keeping execution time measurable.
/// 
/// This does not obviously replace my lazyness about making some proper test units :)...
int main()
{
    std::cout << "Starting scheduler...\n" << std::flush;
    Scheduler scheduler;
    scheduler.Run();
    std::cout << "Scheduler running, adding tasks...\n" << std::flush;

    const int NUM_TASKS = 100000000;
    auto start = std::chrono::high_resolution_clock::now();

    // Submit tasks
    for (int i = 0; i < NUM_TASKS; ++i)
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
        int completed = scheduler.GetTasksCompleted();
        int in_progress = scheduler.GetTasksInProgress();
        int failed = scheduler.GetTasksFailed();
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

    return EXIT_SUCCESS;
}