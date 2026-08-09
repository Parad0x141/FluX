# FluX

[![C++20](https://img.shields.io/badge/C%2B%2B-20-blue.svg)](https://en.cppreference.com/w/cpp/20)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Platform](https://img.shields.io/badge/platform-Windows-lightgrey.svg)]()
[![ThreadSanitizer](https://img.shields.io/badge/TSan-tested-success.svg)]()
[![Throughput](https://img.shields.io/badge/benchmark-1.64M%20tasks%2Fs-orange.svg)]()
[![Tasks](https://img.shields.io/badge/stress%20test-100M%20tasks-purple.svg)]()

> **FluX** — A low-latency C++20 work-stealing task scheduler with lock-free worker queues, lock-free task metadata tracking, and zero-allocation task recycling on steady-state hot paths.

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
| **MPMCQueue**     | Vyukov bounded MPMC                                | External task injection (multi-producer / multi-consumer) |
| **ChaseLevDeque** | Chase-Lev work-stealing                            | Per-worker local queue (owner LIFO, thieves FIFO)         |
| **TaskPool**      | Sequence/CAS-based pool                            | Lock-free task object recycling                           |
| **Workers**       | Round-robin submission + rotating victim selection | Thread pool orchestration                                 |
| **TaskRegistry**  | Segmented atomic registry                          | Lock-free task metadata, status tracking and cancellation |
| **Scheduler**     | Public API + fallback queue + registry             | High-level task submission, tracking and control          |

---

## Features

* **Lock-free worker hot paths** — Worker-local execution, stealing, injection and task recycling avoid scheduler registry locks.
* **Lock-free task registry** — Segmented metadata storage with atomic status transitions and CAS-based cancellation.
* **Work-stealing** — Chase-Lev deque with owner LIFO for locality and thief FIFO for fairness.
* **Zero-allocation task recycling** — `TaskPool` recycles task objects after pool initialization.
* **Amortized registry allocation** — Metadata storage is allocated in fixed-size chunks rather than once per task.
* **Priority support** — `TaskPriority` routing infrastructure is present; priority ordering is not yet enforced.
* **Task tracking** — Metadata snapshots, queued-task cancellation and cooperative abort state transitions.
* **TSan-tested** — Stress-tested with ThreadSanitizer and dedicated race-window instrumentation.
* **Debug instrumentation** — `FLUX_DEBUG_DUPES` detects duplicate task completion/acquire/release events.
* **Atomic statistics** — In-progress, completed, failed and stolen-task counters.
* **Lifecycle-safe shutdown** — Worker ownership is moved out under the scheduler mutex before blocking worker joins.

> **Important:** FluX is not globally mutex-free. The scheduler still uses a small mutex-protected fallback queue and lifecycle state (`m_workers`). The lock-free design applies to the worker queues, task pool and task registry.

---

## Quick Start

```cpp
#include "Scheduler.hpp"
#include <iostream>
#include <chrono>
#include <thread>

int main() {
    std::cout << "=== FluX Basic Example ===\n\n";

    Scheduler scheduler;
    scheduler.Run();  // Start worker threads (hardware_concurrency)

    const int NUM_TASKS = 10000;
    std::atomic<int> counter{0};

    // Submit tasks
    for (int i = 0; i < NUM_TASKS; ++i) {
        Task task;
        task.payload = [&counter] {
            counter.fetch_add(1, std::memory_order_relaxed);
        };
        task.priority = TaskPriority::Normal;
        scheduler.AddTask(std::move(task));
    }

    // Wait for completion
    while (scheduler.GetTasksCompleted() + scheduler.GetTasksFailed() < NUM_TASKS) {
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

Single-header compile:

```bash
g++ -std=c++20 -O3 -I FluX FluX/FluX.cpp -o flux_benchmark
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
| `AbortTaskInProgress(id)` | Atomically transition an `InProgress`/`Claimed` task to `Failed`.                      |
| `GetTasksInProgress()`    | Atomic count of currently executing tasks.                                             |
| `GetTasksCompleted()`     | Atomic count of successfully completed tasks.                                          |
| `GetTasksFailed()`        | Atomic count of failed/aborted tasks.                                                  |
| `GetTasksStolen()`        | Number of successful work-steal operations.                                            |
| `GetTasksQueued()`        | Number of tasks currently held by the scheduler fallback queue.                        |

### `Task`

```cpp
struct Task {
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
Task task{
    .priority = TaskPriority::High,
    .payload = []{ /* work */ }
}; // OK — priority is declared before payload in Types.hpp
```

### `Workers` (advanced / internal)

```cpp
// Custom executor injection
Workers workers(thread_count, [](Task&& t) {
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

The previous registry stored complete `Task` objects, including their `std::function<void()>` payloads.

That meant task submission could involve copying/storing the callable in the tracking structure.

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

* `Register()` — atomic publication
* `SetStatus()` — release store
* `Get()` — acquire loads
* `TryAbort()` — CAS-based state transition

Chunk creation occurs only when a new task-ID range is reached and is resolved using an atomic pointer CAS. Once published, a chunk remains at a stable address for the lifetime of the registry.

---

## Benchmarks

### Latest stress benchmark

**Hardware:** Intel Core i7-4790K — 4C/8T
**Tasks:** 100,000,000
**Result:** 0 failed tasks

| Metric                  |               Result |
| ----------------------- | -------------------: |
| Tasks                   |      **100,000,000** |
| Submit time             |         **60.726 s** |
| Execution time          |            **22 ms** |
| Total time              |         **60.748 s** |
| **Measured throughput** | **1.646M tasks/sec** |
| Completed               |      **100,000,000** |
| Failed                  |                **0** |
| Successful steals       |        **2,150,303** |

The benchmark is intentionally dominated by extremely small task submissions. The measured throughput therefore primarily reflects scheduler/submission overhead rather than the computational throughput of the payload itself.

The very small execution time relative to submission time also demonstrates that the worker side can drain the generated workload rapidly once tasks are distributed.

### Previous 5M-task stress run

| Metric         |           Result |
| -------------- | ---------------: |
| Tasks          |        5,000,000 |
| Submit time    |           ~6.3 s |
| Execution time |        ~10–14 ms |
| Throughput     | ~0.78M tasks/sec |
| Completed      |        5,000,000 |
| Failed         |                0 |
| Steals         |     ~0.58M–0.66M |

Results vary depending on whether additional steal-window stress instrumentation is enabled.

### Benchmark interpretation

These numbers should **not** be interpreted as a universal comparison against production runtimes.

Task size, CPU architecture, worker count, contention, submission pattern, allocator behavior and synchronization strategy all have a significant impact on task throughput.

The benchmark is primarily intended to measure FluX's own synchronization and scheduling overhead under extremely fine-grained workloads.

---

## Debug & Stress Instrumentation

### `FLUX_DEBUG_DUPES`

Enables additional runtime checks for task lifetime and duplicate completion/acquire/release bugs.

The instrumentation is deliberately kept separate from normal builds because debugging synchronization bugs can significantly perturb the timing of the race being investigated.

### `FLUX_STRESS_STEAL_WINDOW`

Widens selected race windows in the Chase-Lev implementation and enables additional steal instrumentation.

This was specifically used to stress the difficult CAS/load windows involved in concurrent stealing and owner operations.

Example:

```bash
clang++ -std=c++20 \
    -fsanitize=thread \
    -O1 -g \
    -I FluX \
    FluX/FluX.cpp \
    -o flux_tsan

./flux_tsan
```

FluX has been stress-tested under ThreadSanitizer with millions of task operations and dedicated race-window instrumentation.

> ThreadSanitizer testing provides strong dynamic coverage, but it is not a formal proof that every possible execution is race-free.

---

## Memory Model

FluX relies on explicit atomic memory ordering rather than mutexes for its lock-free components.

| Operation                   | Ordering                           | Synchronization                               |
| --------------------------- | ---------------------------------- | --------------------------------------------- |
| `MPMCQueue::TryPush`        | `release`                          | Publishes slot to consumer                    |
| `MPMCQueue::TryPop`         | `acquire`                          | Observes producer publication                 |
| `ChaseLevDeque::PushBottom` | `seq_cst`                          | Synchronizes with concurrent deque operations |
| `ChaseLevDeque::StealTop`   | `seq_cst` CAS                      | Coordinates thief ownership                   |
| `TaskPool::Acquire`         | `release` / acquire protocol       | Coordinates slot reuse                        |
| `TaskPool::Release`         | CAS with release/acquire semantics | Publishes slot availability                   |
| `TaskRegistry::SetStatus`   | `release`                          | Publishes task metadata                       |
| `TaskRegistry::Get`         | `acquire`                          | Observes published metadata                   |
| `TaskRegistry::TryAbort`    | `acq_rel` CAS                      | Atomic state transition                       |

Cross-thread data publication uses explicit release/acquire or sequentially consistent synchronization where required by the algorithm.

The Chase-Lev implementation intentionally uses stronger `seq_cst` ordering to make the concurrency protocol easier to reason about and to improve dynamic race detection under ThreadSanitizer.

---

## Current Synchronization Model

FluX is **not globally mutex-free**.

The architecture intentionally separates the performance-critical worker mechanisms from the higher-level scheduler control path.

### Lock-free

* Per-worker Chase-Lev deques
* MPMC injection queues
* TaskPool recycling
* TaskRegistry operations
* Work stealing
* Worker-side task acquisition/release

### Mutex-protected

* Scheduler fallback queue
* Scheduler worker lifecycle state
* Fallback queue draining
* Scheduler execution counters

The scheduler mutex protects a deliberately small coordination region. The previous registry mutex has been removed from the task tracking hot path.

---

## Roadmap

* [ ] Priority-aware submission (dedicated high-priority queue)
* [ ] Cooperative cancellation (`CancellationToken` in payload)
* [ ] `unique_function` move-only callable wrapper
* [ ] `SubmitBlocking` / `WaitForCapacity` backpressure API
* [ ] Further reduction of scheduler mutex contention
* [ ] Sharded fallback queue
* [ ] CMake `install()` + pkg-config / FetchContent support
* [ ] Unit test suite (GoogleTest + stress harness)
* [ ] Broader benchmark suite against other task schedulers

---

## Known Limitations

FluX is intentionally still evolving.

* Priority routing infrastructure exists, but priority ordering is not currently enforced.
* `is_stealable` is currently an affinity hint and is not enforced.
* `thread_id` is reserved for future use.
* `std::function<void()>` remains the callable representation and may allocate depending on the captured object.
* The fallback queue remains mutex-protected.
* The registry uses bounded segmented storage and is intentionally configured with a generous maximum task-ID range.
* No formal benchmark suite against external schedulers is currently included.
* The current API is experimental and may change before a stable release.

---

## License

MIT © 2026 Cyril "Parad0x141" Bouvier

See `LICENSE` for details.

---

## Acknowledgments

* **Dmitry Vyukov** — Bounded MPMC queue algorithm
* **C. Chase & Y. Lev** — Work-stealing deque
* **Lê et al.** — Work-stealing deque memory-model analysis and fence-equivalence work
* **ThreadSanitizer team** — Dynamic race detection and stress testing
