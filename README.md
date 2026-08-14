# FluX

[![License: AGPL v3](https://img.shields.io/badge/License-AGPL--3.0-blue.svg)](LICENSE)
[![C++20](https://img.shields.io/badge/C%2B%2B-20-blue.svg)](https://en.cppreference.com/w/cpp/20)
[![Platform](https://img.shields.io/badge/platform-Windows%20%7C%20Linux%20%7C%20macOS-lightgrey.svg)]()
[![64-bit](https://img.shields.io/badge/arch-x86__64-blue.svg)]()
[![TSan](https://img.shields.io/badge/TSan-tested-success.svg)]()
[![ASan](https://img.shields.io/badge/ASan-tested-success.svg)]()
[![UBSan](https://img.shields.io/badge/UBSan-tested-success.svg)]()
[![Benchmark](https://img.shields.io/badge/benchmark-3.83M%20tasks%2Fs-orange.svg)]()
[![Stress test](https://img.shields.io/badge/stress-3B%20tasks-purple.svg)]()

> **FluX** — A low-overhead C++20 work-stealing task scheduler built around lock-free worker queues, lock-free task metadata tracking, and zero-allocation task recycling on steady-state hot paths.

*This README is subject to a lot of changes along the development.*

---

## Architecture

```text
┌─────────────────────────────────────────────────────────────────────┐
│                            Scheduler                                │
│  ┌──────────────┐  ┌──────────────────────┐  ┌─────────────────┐    │
│  │ AddTask()    │  │ Task Registry        │  │ Fallback Queue  │    │
│  │ GetSnapshot()│  │ Lock-free metadata   │  │ (deque + μ)     │    │
│  │ Delete/Abort │  │ tracking             │  │ Pre-Run buffer  │    │
│  └──────┬───────┘  └──────────────────────┘  └────────┬────────┘    │
│         │                                             │             │
│         ▼                                             ▼             │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │                        Workers                              │    │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐     ┌─────────┐      │    │
│  │  │Worker 0 │  │Worker 1 │  │Worker 2 │ ... │Worker N │      │    │
│  │  │ ┌─────┐ │  │ ┌─────┐ │  │ ┌─────┐ │     │ ┌─────┐ │      │    │
│  │  │ │ Deq │ │  │ │ Deq │ │  │ │ Deq │ │     │ │ Deq │ │      │    │
│  │  │ │ Inj │ │  │ │ Inj │ │  │ │ Inj │ │     │ │ Inj │ │      │    │
│  │  │ │Pool │ │  │ │Pool │ │  │ │Pool │ │     │ │Pool │ │      │    │
│  │  │ └─────┘ │  │ └─────┘ │  │ └─────┘ │     │ └─────┘ │      │    │
│  │  └────┬────┘  └────┬────┘  └────┬────┘     └────┬────┘      │    │
│  │       │            │            │               │           │    │
│  │       └────────────┴────Steal──┴───────────────┘            │    │
│  │                    (Chase-Lev FIFO steal)                   │    │
│  └─────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────┘
```

### Core Components

| Component         | Algorithm                                          | Role                                                      |
| ----------------- | -------------------------------------------------- | --------------------------------------------------------- |
| **MPMCQueue**     | Vyukov bounded MPMC                                | External task injection (multi-producer; algorithmically MPMC, but used as MPSC in FluX — see note below) |
| **ChaseLevDeque** | Chase-Lev work-stealing                            | Per-worker local queue (owner LIFO, thieves FIFO)         |
| **TaskPool**      | Sequence/CAS-based concurrent object pool           | Lock-free task object recycling with cross-thread release |
| **Workers**       | Round-robin submission + rotating victim selection | Thread pool orchestration                                 |
| **TaskRegistry**  | Segmented atomic registry                          | Lock-free task metadata, status tracking and cancellation |
| **Scheduler**     | Public API + fallback queue + registry             | High-level task submission, tracking and control          |

> **Note on `MPMCQueue` usage:** the class itself implements Vyukov's full MPMC algorithm and is safe for concurrent producers *and* concurrent consumers. In FluX, each worker's `inject_queue` is written by potentially many external threads (`Scheduler::AddTask` callers, `Workers::SubmitTask`) — genuinely multi-producer — but only ever *read* by that worker's own thread, inside `DrainInjectQueue()`. No other thread calls `TryPop` on it. So in this codebase the queue runs as MPSC in practice, not MPMC; the extra consumer-side concurrency the algorithm supports simply isn't exercised here.

---

## Features

* **Lock-free worker hot paths** — Worker-local execution, stealing, injection and task recycling avoid scheduler registry locks.
* **Lock-free task registry** — Segmented metadata storage with atomic status transitions and CAS-based abort state transitions. **Lock-free task execution accounting** — `ExecuteTask` updates in-progress/completed/failed counters via direct atomic RMW; no mutex is taken on the per-task execution path.
* **Work-stealing** — Chase-Lev deque with owner LIFO for locality and thief FIFO for fairness.
* **Zero-allocation task recycling** — `TaskPool` recycles task objects after pool initialization.
* **Amortized registry allocation** — Metadata storage is allocated in fixed-size chunks rather than once per task.
* **Priority support** — `TaskPriority` routing infrastructure is present; priority ordering is not yet enforced.
* **Task tracking** — Metadata snapshots, queued-task removal, and cooperative `InProgress`/`Claimed` → `Failed` abort transitions. **TSan-tested** — Stress-tested with ThreadSanitizer and dedicated race-window instrumentation.
* **Debug instrumentation** — `FLUX_DEBUG_DUPES` detects duplicate task completion, pool acquire/release, and slot lifetime violations. **Atomic statistics** — In-progress, completed, failed and stolen-task counters.
* **Concurrency stress instrumentation** — `FLUX_STRESS_STEAL_WINDOW` widens selected Chase-Lev race windows to exercise difficult owner/thief interleavings.
* **Lifecycle-safe shutdown** — Worker ownership is moved out under the scheduler mutex before blocking worker joins.

> **Important:** FluX is not globally mutex-free. The scheduler still uses a small mutex-protected fallback queue and lifecycle state (`m_workers`). The lock-free design applies to the worker queues, task pool, task registry, and — as of the latest pass — the per-task execution accounting inside `ExecuteTask`.

---

## Quick Start

```cpp
#include "Scheduler.hpp"
#include <iostream>
#include <chrono>
#include <thread>
#include <atomic>
#include <cstdint>

int main()
{
    std::cout << "=== FluX Basic Example ===\n\n";

    Scheduler scheduler;
    scheduler.Run();  // Start worker threads (hardware_concurrency)

    const int64_t NUM_TASKS = 10000;
    std::atomic<int64_t> counter{0};

    // Submit tasks
    for (int64_t i = 0; i < NUM_TASKS; ++i)
    {
        Task task;
        task.payload = [&counter]
        {
            counter.fetch_add(1, std::memory_order_relaxed);
        };
        task.priority = TaskPriority::Normal;
        scheduler.AddTask(std::move(task));
    }

    // Wait for completion – note that GetTasksCompleted/Failed now return int64_t
    while (scheduler.GetTasksCompleted() + scheduler.GetTasksFailed() < NUM_TASKS)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    std::cout << "Tasks submitted: " << NUM_TASKS << "\n";
    std::cout << "Counter value:   " << counter.load() << "\n";
    std::cout << "Completed:       " << scheduler.GetTasksCompleted() << "\n";
    std::cout << "Failed:          " << scheduler.GetTasksFailed() << "\n";
    std::cout << "Steals:          " << scheduler.GetTasksStolen() << "\n";

    return 0;
}
```

### Build

**Visual Studio (recommended)** — Open the `.sln` in the repo root.

Optional preprocessor definitions:

* `FLUX_DEBUG_DUPES=1` — Enable duplicate completion/acquire/release detection.
* `FLUX_STRESS_STEAL_WINDOW=1` — Widen selected Chase-Lev race windows for stress testing.

Command-line compile (FluX is not header-only: `Scheduler.cpp` and `Workers.cpp`
must be compiled and linked alongside `FluX.cpp`, or you'll get `undefined
reference` errors at link time):

```bash
g++ -std=c++20 -O3 -I FluX FluX/FluX.cpp FluX/Scheduler.cpp FluX/Workers.cpp -o flux_benchmark
./flux_benchmark
```

---

## API Reference

### `Scheduler`

| Method                    | Description                                                                            |
| ------------------------- | -------------------------------------------------------------------------------------- |
| `Run()`                   | Start worker threads (idempotent).                                                     |
| `AddTask(Task)`           | Submit a task and return a unique `task_id` (`0` on failure).                          |
| `GetTaskSnapshot(id)`     | Return immutable task metadata without retaining/copying the payload.                  |
| `GetTaskById(id)`         | Metadata lookup retained for API compatibility; payload is not stored by the registry. |
| `DeleteTaskFromQueue(id)` | Cancel a task still waiting in the scheduler fallback queue.                           |
| `AbortTaskInProgress(id)` | Atomically mark an `InProgress`/`Claimed` task as `Failed`; does not interrupt the payload. |
| `GetTasksInProgress()`    | Atomic count of currently executing tasks.                                             |
| `GetTasksCompleted()`     | Atomic count of successfully completed tasks.                                          |
| `GetTasksFailed()`        | Atomic count of failed/aborted tasks.                                                  |
| `GetTasksStolen()`        | Number of successful work-steal operations.                                            |
| `GetTasksQueued()`        | Wait-free relaxed-atomic read of the scheduler fallback queue size (no mutex).         |

### `Task`

```cpp
struct Task
{
    std::thread::id thread_id{};        // Reserved (not yet wired)
    uint64_t task_id{0};                // Assigned by Scheduler
    TaskPriority priority{Normal};      // Low / Normal / AboveNormal / High
    std::chrono::steady_clock::time_point start_time{};
    TaskStatus status{Idle};            // Idle → Queued → InProgress → Completed/Failed
    bool is_stealable{true};            // Affinity hint (not yet enforced)
    size_t pool_slot_index{...};        // Internal (TaskPool)
    std::function<void()> payload;      // User callable
};
```

**Note on initialization:** In C++20, designated initializers must follow declaration order:

```cpp
Task task
{
    .priority = TaskPriority::High,
    .payload = []{ /* work */ }
}; // OK — priority is declared before payload in Types.hpp
```

### `Workers` (advanced / internal)

```cpp
// Custom executor injection
Workers workers(thread_count, [](Task&& t)
{
    /* custom execution */
});

workers.SubmitTask(task, priority);

// Work stealing happens internally in WorkerLoop.
workers.GetStealCount();
```

---

## Task Registry

FluX uses a dedicated metadata-only `TaskRegistry` instead of storing complete `Task` objects in a mutex-protected `std::unordered_map`.

### Design

The registry uses fixed-size segmented storage:

```text
task_id
   │
   ├── chunk_index = task_id / ChunkSize
   │
   └── slot_index  = task_id % ChunkSize
                         │
                         ▼
              ┌─────────────────────┐
              │      Chunk          │
              │ ┌─────┬─────┬─────┐ │
              │ │Entry│Entry│ ... │ │
              │ └─────┴─────┴─────┘ │
              └─────────────────────┘
```

Each entry stores only:

* `TaskStatus`
* `TaskPriority`
* `start_time`
* `in_use`

The status is atomic and acts as the publication mechanism for the associated metadata.

### Why metadata-only?

The previous registry stored complete `Task` objects, including their
`std::function<void()>` payloads.

That unnecessarily duplicated execution data in the tracking structure and added
callable copy/move overhead to task registration. Depending on the callable and
`std::function`'s SBO (small-buffer optimization), this could also introduce additional
dynamic allocation activity.

FluX now separates:

```text
Execution data
    ↓
Worker queues / TaskPool
    ↓
Task payload

Tracking data
    ↓
TaskRegistry
    ↓
Status / priority / timestamp
```

This removes the registry's payload copy from the submission path and substantially reduces contention and memory traffic.

### Concurrency properties

Normal registry operations do not acquire a mutex:

* `Register()` — publishes metadata through release ordering
* `SetStatus()` — release store
* `Get()` — acquire-based snapshot
* `TryAbort()` — atomic CAS state transition

Chunk creation occurs only when a new task-ID range is reached and is resolved
using an atomic pointer CAS. Once published, a chunk remains at a stable address
for the lifetime of the registry.

The registry is intentionally append-only: entries are not reclaimed or reused
during the lifetime of the registry. This gives readers stable `Entry` addresses
and avoids a separate reclamation scheme.

---

## Scheduler Execution Path

`Scheduler::ExecuteTask` runs once per task, on whichever worker thread claimed
it, so it sits directly on the execution hot path. It intentionally takes no
scheduler mutex.

* `Task` is passed by value into `ExecuteTask`, giving the executing thread
  exclusive ownership of that task instance. Its non-atomic execution fields
  therefore require no additional synchronization.
* `m_tasks_in_progress`, `m_tasks_completed` and `m_tasks_failed` are atomic
  counters updated directly with `fetch_add`/`fetch_sub`. No scheduler mutex is
  taken for per-task execution accounting.
* Task status publication is handled independently by `TaskRegistry`, whose
  status transitions use atomic release/acquire operations.

`DrainFallbackQueue()` is also reached from the task execution path. The
fallback deque itself remains mutex-protected, but an atomic
`m_fallback_pending` counter provides a lock-elision fast path: when the counter
indicates that the fallback queue is empty, the worker returns without acquiring
the mutex.

When fallback work is actually pending, `DrainFallbackQueue()` acquires the
scheduler mutex before accessing the protected deque. The atomic counter does
not replace the mutex; it only avoids entering the critical section when there
is nothing to drain.

`GetTasksQueued()` reads the same atomic counter and therefore does not need to
lock the fallback queue merely to report its approximate/current queued count.

---

## Benchmarks

### Primary benchmark

**Hardware:** Intel Core i7-4790K — 4C/8T (Rawr ! I'm a dinosaur ! 🦖)  
**Workload:** 100,000,000 extremely fine-grained tasks  
**Priority distribution:** 5% Low / 70% Normal / 15% AboveNormal / 10% High

| Metric | Result |
|---|---:|
| Tasks | **100,000,000** |
| Failed | **0** |
| Successful steals | **18,379,555** |
| Submit time | **26,084 ms** |
| Execution time | **10 ms** |
| Total time | **26,095 ms** |
| **Measured throughput** | **3.83M tasks/sec** |

The workload intentionally uses extremely small tasks and a mixed-priority
submission pattern. The measured throughput therefore primarily reflects task
submission, synchronization, queueing and scheduling overhead rather than
application-level computation.

The benchmark completed all 100 million tasks without a failed task while
performing more than 18 million successful steal operations.

### Long-duration stress test

A separate long-duration run processed:

| Metric | Result |
|---|---:|
| Tasks | **3,000,000,000** |
| Failed | **0** |
| Successful steals | **397,120,105** |
| Submit time | **781,119 ms** |
| Execution time | **12 ms** |
| Total time | **781,132 ms** |
| **Measured throughput** | **3.84M tasks/sec** |

This run is primarily intended as a sustained concurrency and task-lifetime
stress test rather than as a representative application workload.

Both benchmarks were performed on the same Intel Core i7-4790K system. Results
vary with CPU architecture, worker count, contention, compiler configuration,
task payload, submission pattern and enabled instrumentation.

### Per-priority queue latency

The benchmark also records queue latency independently for each priority level.

A representative 100M-task run produced:

| Priority | Tasks | Average queue time | Maximum queue time |
|---|---:|---:|---:|
| Low | 5,000,000 | 43.48 µs | 64,696.80 µs |
| Normal | 70,000,000 | 39.78 µs | 64,868.80 µs |
| AboveNormal | 15,000,000 | 33.73 µs | 64,533.60 µs |
| High | 10,000,000 | 31.70 µs | 66,760.80 µs |

Priority routing infrastructure is exercised by this workload, but priority
ordering is not currently a strict scheduling guarantee.

---

## Debug & Stress Instrumentation

FluX includes dedicated instrumentation for investigating concurrency bugs that
are difficult to reproduce under normal execution.

### `FLUX_DEBUG_DUPES`

Enables additional runtime checks for task lifetime and pool-slot ownership.

The instrumentation tracks task completion and `TaskPool` acquire/release activity,
including cross-thread releases. It reports duplicate or inconsistent lifetime
events together with diagnostic information such as the slot index, thread ID,
and acquire/release counters.

This instrumentation is disabled in normal builds because the additional atomic
operations, logging and synchronization can significantly perturb the timing of
the workload being investigated.

### `FLUX_STRESS_STEAL_WINDOW`

Widens selected race windows in the Chase-Lev deque and enables additional
steal-path instrumentation.

It is intended specifically for exercising difficult owner/thief interleavings
that may otherwise be extremely rare during normal execution.

The combination of widened race windows, `FLUX_DEBUG_DUPES`, ThreadSanitizer and
large task-count stress runs has been used to investigate task duplication,
premature reuse and concurrent queue/deque lifetime errors.

### Sanitizer testing

Example:

```bash
clang++ -std=c++20 \
    -fsanitize=thread \
    -O1 -g \
    -I FluX \
    FluX/FluX.cpp FluX/Scheduler.cpp FluX/Workers.cpp \
    -o flux_tsan

./flux_tsan
```
Additional validation has also been performed with AddressSanitizer and
UndefinedBehaviorSanitizer.

Sanitizer runs and stress tests provide dynamic evidence over the executions
they cover; they are not a formal proof that every possible concurrent
execution is correct.

FluX has been stress-tested under ThreadSanitizer with millions of task operations and dedicated race-window instrumentation.

> ThreadSanitizer testing provides strong dynamic coverage, but it is not a formal proof that every possible execution is race-free.

---

| Operation | Ordering | Purpose |
|---|---|---|
| `MPMCQueue::TryPush` | release | Publishes a completed slot to consumers |
| `MPMCQueue::TryPop` | acquire | Observes producer publication |
| `ChaseLevDeque::PushBottom` | seq_cst | Conservative synchronization for owner-side deque operations |
| `ChaseLevDeque::StealTop` | seq_cst CAS | Coordinates concurrent thief/owner access |
| `TaskPool::Acquire` | acquire / relaxed CAS / release | Coordinates slot acquisition and publication |
| `TaskPool::Release` | release CAS / acquire failure | Atomically transitions a slot back to the reusable state |
| `TaskRegistry::Register` | release | Publishes task metadata |
| `TaskRegistry::SetStatus` | release | Publishes status transitions |
| `TaskRegistry::Get` | acquire | Observes published metadata |
| `TaskRegistry::TryAbort` | acq_rel CAS | Performs an atomic status transition |
| `Scheduler::ExecuteTask` counters | relaxed RMW | Statistics only; no inter-counter ordering required |
| `Scheduler::m_fallback_pending` | relaxed | Fast-path hint for avoiding an unnecessary fallback-queue mutex acquisition |

Cross-thread data publication uses explicit release/acquire synchronization or
sequentially consistent ordering where required by the individual algorithm.

The Chase-Lev implementation deliberately uses `seq_cst` ordering throughout
its critical synchronization protocol. This is a conservative design choice
that favors a simpler memory-ordering model and easier validation over
minimizing the strength of individual atomic operations.

---

## Current Synchronization Model

FluX is **not globally mutex-free**.

The architecture deliberately separates the performance-sensitive worker
mechanisms from scheduler-level coordination and lifecycle management.

### Lock-free / atomic hot paths

* Per-worker Chase-Lev deques
* Per-worker injection queues
* TaskPool object recycling
* TaskRegistry metadata operations
* Work stealing
* Worker-side task acquisition and release
* Per-task execution accounting
* Fallback-queue pending-state check

These paths do not require the scheduler mutex for their normal operation.

### Mutex-protected state

* Scheduler fallback queue contents (`m_tasks_to_dispatch`)
* Scheduler worker lifecycle state (`m_workers`)

The fallback queue remains intentionally mutex-protected. Its
`m_fallback_pending` atomic counter provides a fast empty-state check so workers
do not acquire the mutex when there is no fallback work to drain.

The scheduler mutex therefore remains part of the control/coordination path, but
is not required for normal task execution, work stealing, task recycling or task
metadata updates.

This distinction is intentional: FluX does not attempt to eliminate every
mutex from the system at the cost of making the entire design unnecessarily
complex. Instead, synchronization is kept out of the high-frequency worker
paths wherever the underlying ownership model permits it.

---

## Interactive Dashboard

FluX includes an optional interactive terminal dashboard built with
[FTXUI](https://github.com/ArthurSonzogni/FTXUI).

The dashboard provides a live view of scheduler activity and allows benchmark
runs to be launched interactively without restarting the application.

It displays:

* Benchmark progress
* Completed / failed tasks
* Currently executing tasks
* Successful steals
* Waiting tasks
* Submission, execution and total benchmark time
* Measured throughput
* Requeue-stall statistics
* Per-priority queue latency

Benchmark statistics are reset or baselined between runs so that each displayed
result represents the current benchmark rather than the scheduler's cumulative
process lifetime counters.

FTXUI is used exclusively by the demonstration/benchmark frontend; it is not a
dependency of the scheduler's core concurrency mechanisms.

---

## Tools

**Compiler & Runtime**  
- Std: C++20 (Latest)  
- Microsoft C++ Build Tools v145  
- Microsoft Visual Studio Insiders 2026  
- Windows Subsystem for Linux (WSL)  
- G++ / ThreadSanitizer / AddressSanitizer  

**Profiling**  
- Windows Performance Analyzer (WPA)

---

## Visual Studio Extensions
- Rainbow Braces — Mads Kristensen  
- ClaudiaIDE — Buchizo


## Roadmap

### Scheduling & task control

* [ ] Priority-aware scheduling with strict priority ordering
* [ ] Cooperative cancellation with `CancellationToken` (Design not finalized; the cancellation mechanism may change as the architecture evolves... moon phase permitting :))) )
* [ ] Enforce `is_stealable` task affinity hints
* [ ] `SubmitBlocking` / `WaitForCapacity` backpressure API

### Performance & scalability

* [ ] Further investigate scheduler mutex contention under sustained fallback-queue pressure
* [ ] Evaluate sharded fallback queues
* [ ] Benchmark alternative callable representations such as `unique_function`
* [ ] Expand the benchmark suite with different task sizes, worker counts and submission patterns

### Tooling & distribution

* [ ] Unit test suite (GoogleTest + dedicated concurrency stress harness)
* [ ] CMake `install()` + pkg-config / FetchContent support
* [ ] Broader comparative benchmarks against established task schedulers

### Task registry

* [ ] Bounded retention / eviction for historical registry entries
* [ ] Evaluate external or persistent post-mortem storage for evicted metadata

---

## Known Limitations

FluX is still evolving, and several parts of the architecture remain
experimental.

* Priority routing is implemented and exercised by the benchmark, but the scheduler does not currently provide a strict global priority guarantee.
* `AbortTaskInProgress()` only updates the task's registry status; it does not interrupt a running payload.
* `is_stealable` is currently an affinity hint and is not enforced.
* `thread_id` is reserved for future use.
* `std::function<void()>` remains the callable representation and may allocate depending on the captured object.
* The scheduler fallback queue remains mutex-protected by design.
* The `TaskRegistry` is append-only for the lifetime of the `Scheduler`; historical metadata is not currently reclaimed or evicted.
* The registry has a configured maximum task-ID range. IDs beyond that range are intentionally ignored by registry operations.
* The public API is experimental and may change before a stable release.
---

## Documentation Notice

This documentation was initially generated with the assistance of Claude AI and has been reviewed, corrected, and maintained by the author.

While every effort has been made to keep the documentation accurate, it may become outdated as the project evolves (I'm lazy, and this is just some "toy code" to keep me busy anyway). Refactoring, implementation changes, and architectural changes may introduce documentation drift.

The source code remains the authoritative reference for the current implementation. Every effort is made to keep the README up to date, but ultimately, I'm only human.
---

## License

Copyright (C) 2026 Cyril "Parad0x141" Bouvier

See `LICENSE` for details.

---


## Acknowledgments

* **Dmitry Vyukov** — Bounded MPMC queue algorithm
* **C. Chase & Y. Lev** — Work-stealing deque
* **Lê et al.** — Work-stealing deque memory-model analysis and fence-equivalence work
* **Arthur Sonzogni** — FTXUI Linux/Win, used for the interactive terminal dashboard
* **ThreadSanitizer team** — Dynamic race detection and stress testing
