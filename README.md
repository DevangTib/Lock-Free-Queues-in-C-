# Lock-free queues in C++

Implemented: header-only SPSC, NCQ, MPSC, and SPMC, as described in [DESIGN.md](DESIGN.md).

| Class | Callers | Header |
| --- | --- | --- |
| `lfq::SpscRing<T, N>` | One producer, one consumer | `lfq/spsc_ring.hpp` |
| `lfq::MpscRing<T, N>` | Multiple producers, one consumer | `lfq/mpsc_ring.hpp` |
| `lfq::SpmcRing<T, N>` | One producer, multiple consumers | `lfq/spmc_ring.hpp` |
| `lfq::detail::NcqIndexRing<N>` | Internal MPMC index engine | `lfq/ncq_index_ring.hpp` |

```cpp
#include <lfq/spsc_ring.hpp>

struct Message { char bytes[500]; };
lfq::SpscRing<Message, 1024> queue;
Message input{}, output{};

// Producer thread:
const auto pushed = queue.try_push(input);
// Consumer thread:
const auto popped = queue.try_pop(output);
```

Choose the class matching your thread topology; all message queues expose the same push/pop API. SPMC distributes each message to one consumer, not every consumer. All capacity slots are usable. A failed push returns `PushResult::no_capacity`; a failed pop returns `PopResult::empty` and preserves output. Retry/drop decisions belong to the caller. No queue allocates or waits for new data/storage, although NCQ CAS loops may retry under contention. Storage is std::array<T, Capacity>, so T must be default-constructible as well as a complete, non-cv, trivially copyable object type. Copies use ordinary & addresses, so T must not overload unary operator&; caller arguments must be live complete objects without conflicting concurrent caller access. Embedded pointer lifetimes remain the caller's responsibility. Destroy queues only after all callers stop.

The classes own their storage and cannot be copied or moved because their atomic members prohibit it. No explicit deleted constructors or `final` declarations are needed. The optional third template parameter configures cursor alignment (64 bytes by default). The target must support always-lock-free 64-bit atomics; tests check this requirement. SPSC uses acquire/release ownership transfers; the NCQ engine initially uses seq_cst operations.

## NCQ and large messages

MPSC/SPMC contain one private [MessageRing](include/lfq/message_ring.hpp) engine, with a fixed message array, a free-index ring, and a ready-index ring. Producers acquire free blocks, copy into them, and publish their indices. Consumers claim ready indices before copying out, then return storage. Ordinary message copies need no whole-message atomics.

```cpp
#include <lfq/mpsc_ring.hpp>
#include <lfq/spmc_ring.hpp>

lfq::MpscRing<Message, 1024> incoming; // Producers merge messages.
lfq::SpmcRing<Message, 1024> jobs;     // Consumers distribute work.
```

For these queues require N >= the total participant count, with one outstanding call per participant. Capacity includes messages being copied: `no_capacity` means no free block, not necessarily N ready messages. A permanently abandoned call can retain a block; the queue does not provide crash recovery or redelivery.

NCQ is adapted from Nikolaev's reference algorithm; see [source and license notes](THIRD_PARTY_NOTICES.md). Its internal `enqueue_assuming_space(index)` is **not a general try_push**: callers must own a valid index and guarantee space. The message pool enforces this by conserving exactly N indices. Do not insert arbitrary values or enqueue into a full NCQ ring. Standalone tests obey the same precondition.

There is no `exhausted` result. The supported NCQ lifetime assumes fewer than 2^63 cursor advances per ring including initialization, with no full uint64_t counter wrap. Reconstruct only after draining and stopping every participant before reaching that lifetime bound. Physical ring wrap and crossing 32-bit positions are supported. This is a very long finite-counter assumption, not a proof of indefinite reuse under arbitrary wrap.

## Build and test

Requires a C++20 compiler and CMake 3.20 or later:

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --config Release
ctest --test-dir build -C Release --output-on-failure
```

Or compile the dependency-free test executables directly:

```sh
g++ -std=c++20 -O2 -Wall -Wextra -Wpedantic -pthread -Iinclude tests/spsc_ring_tests.cpp -o spsc_ring_tests
g++ -std=c++20 -O2 -Wall -Wextra -Wpedantic -pthread -Iinclude tests/concurrent_ring_tests.cpp -o concurrent_ring_tests
```

Tests cover full/empty, FIFO, all slots usable, physical and uint64_t counter wrap, payload edge values, over-aligned/default-constructible/non-assignable types, and 200,000 concurrent 500-byte transfers. Checks remain enabled in Release builds. A private friend test accessor seeds a quiescent queue near counter wrap; there is no public reset API.

The NCQ/MPSC/SPMC suite covers empty/full initialization, physical wrap, crossing 32-bit positions, tail helping, stale head claims, 80,000 index circulations through four concurrent NCQ workers, and ownership conservation. It also checks large-message FIFO and exactly-once delivery, rejection accounting, and simulated suspended producers/consumers holding a block while other blocks circulate. Full uint64_t wrap testing applies only to SPSC. Each suite has a timeout; do not use sanitizer builds for benchmarks.

For supported Linux GCC/Clang environments, use separate sanitizer builds:

```sh
cmake -S . -B build-tsan -DCMAKE_BUILD_TYPE=RelWithDebInfo -DLFQ_ENABLE_TSAN=ON
cmake --build build-tsan
ctest --test-dir build-tsan --output-on-failure

cmake -S . -B build-asan -DCMAKE_BUILD_TYPE=RelWithDebInfo -DLFQ_ENABLE_ASAN_UBSAN=ON
cmake --build build-asan
ctest --test-dir build-asan --output-on-failure
```

Sanitizers and stress tests do not prove correctness under every schedule. Controlled NCQ/pool states exercise suspension boundaries without production callbacks. Exhaustive history/model checking, deterministic pauses inside SPSC copies, and weakly ordered hardware validation remain additional checks.

Validation performed: Windows x86-64 GCC 13.1 direct build with C++20, `-O2 -DNDEBUG`, and warnings treated as errors; all tests passed. CMake configuration and sanitizer runs have not been exercised in this environment (CMake is unavailable).
