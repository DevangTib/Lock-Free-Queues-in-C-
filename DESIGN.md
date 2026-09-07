`# Fixed-capacity concurrent queues in C++

Status: proposed design, before implementation and concurrency validation.

## 1. Objective and scope

Build three fixed-capacity FIFO queues as an infrastructure interview project:

| Queue | Producers | Consumers | Example |
| --- | --- | --- | --- |
| SPSC | Exactly one | Exactly one | A feed handler passing events to a strategy thread |
| MPSC | One or more | Exactly one | Several workers passing results to an aggregator |
| SPMC | Exactly one | One or more | A dispatcher distributing work among workers |

SPMC is a work queue: each accepted item goes to **one** consumer. It is not a broadcast buffer in which every consumer receives every item.

Requirements:

- Compile-time capacity and an array allocated as part of the queue object.
- No allocation, mutex, condition variable, or operating-system wait inside queue operations.
- Reject the incoming item when full. Never overwrite an unread item.
- Explicit atomic operations and documented memory ordering.
- FIFO ordering, no duplicate delivery, and no loss of successfully accepted items.
- Lock-free progress on supported targets, subject to the documented payload and counter constraints.

Start with C++20. Exclude resizing, blocking wrappers, overwriting, batching, iterators, references into slots, and concurrent destruction from version 1.

## 2. The important design choice

**Using atomics does not, by itself, make a queue lock-free.**

- **Wait-free:** each operation finishes in a bounded number of its own steps.
- **Lock-free:** some operation makes progress despite other threads being suspended; an individual caller may starve.
- **Mutex-free:** no mutex is used, but a suspended thread may still prevent useful progress.

A common bounded queue reserves a position, writes a non-atomic payload, and then publishes a per-slot sequence number. That is attractive for general C++ objects, but a producer suspended between reservation and publication can leave a hole at the front. The original description of a widely used sequence-number queue explicitly disclaims formal lock-freedom. [Vyukov's bounded queue](https://www.1024cores.net/home/lock-free-algorithms/queues/bounded-mpmc-queue)

This project therefore uses:

| Queue | Selected baseline | Payload | Progress |
| --- | --- | --- | --- |
| SPSC | Separate producer and consumer cursors | Small trivially copyable value | Bounded queue-level steps |
| MPSC | Atomically publish payload and generation together; help the producer cursor | Compact integer value/token | Lock-free CAS loop |
| SPMC | Atomically consume payload and reclaim its slot together; help the consumer cursor | Compact integer value/token | Lock-free CAS loop |

The MPSC/SPMC algorithms below are a proposed, deliberately restricted design, not a claim to have reproduced a published, production-validated algorithm. Their proof sketches must be checked against the implementation and adversarial schedules.

**Tradeoff:** a compact atomic record is simpler to reason about than helping with arbitrary C++ object construction, but limits payload size and needs an explicit generation-exhaustion policy. Do not present it as an unrestricted `Queue<T>`.

If unrestricted payloads and indefinite operation are required, treat that as a separate design milestone. SCQ is a published bounded, lock-free FIFO design worth studying; its cancellation and generation protocol should be adopted with its proof, rather than approximated by adding a CAS to the simpler reservation scheme. [Nikolaev's paper](https://arxiv.org/abs/1908.04511), [author's implementation](https://github.com/rusnikola/lfqueue)

## 3. Public API and semantics

Illustrative API; not an implementation:

```cpp
enum class PushResult { success, full, exhausted };
enum class PopResult  { success, empty };

template <std::size_t Capacity>
class MpscRing {
public:
    using value_type = std::uint32_t;

    [[nodiscard]] PushResult try_push(value_type value) noexcept;
    [[nodiscard]] PopResult try_pop(value_type& out) noexcept;
    static constexpr std::size_t capacity() noexcept { return Capacity; }

    MpscRing();
    MpscRing(const MpscRing&) = delete;
    MpscRing& operator=(const MpscRing&) = delete;
};
```

Use the same operational interface for the other topologies. SPSC does not need the compact-record exhaustion policy described below and never returns `exhausted`.

| Operation | Contract |
| --- | --- |
| Successful `try_push` | Queue accepts a copy of the value exactly once |
| `full` | Attempt does not enqueue anything; caller retains its value |
| `exhausted` | Compact-record generation budget is exhausted; no enqueue occurs |
| Successful `try_pop` | Removes exactly one item and writes it to `out` |
| `empty` | Removes nothing and leaves `out` unchanged |
| `capacity()` | Returns the number of usable slots, exactly `Capacity` |

“Drop on full” means the **caller may discard a rejected incoming value**. The queue reports rejection; it must not silently pretend to accept it. Callers may count drops, retry, or apply another policy outside the queue.

For the sequentially consistent MPSC/SPMC baseline, full and empty decisions must correspond to a valid point during the call. For acquire/release SPSC, allow conservative failure when the other cursor's latest advancement has not been observed. A failed attempt is not a promise that the queue remains full or empty after return.

`try_` means there is no wait for capacity or data. It does **not** promise bounded execution time for a contended CAS loop.

No exact concurrent `size()`, `empty()`, or `full()` observer in version 1: two cursor reads need not form a consistent snapshot, and helped cursors can lag completed slot transitions. Use the operation result directly. Any later `size_approx()` is diagnostic only.

The queue must be initialized before publication to workers. Destruction, reset, and thread-role reassignment require external synchronization and no outstanding operations. The object is neither copyable nor movable. No consumer fairness guarantee is made.

## 4. Storage and target assumptions

- `N = Capacity` is a power of two, with `N >= 2`.
- Slot index is `position & (N - 1)`.
- Logical positions distinguish different visits to the same physical slot.
- All `N` slots are usable; no sentinel slot is sacrificed.
- Queue operations do not allocate, log, invoke callbacks, or manipulate reference-counted objects.
- Require `std::atomic<std::uint64_t>::is_always_lock_free` on the supported build. Fail clearly if this is false; a hidden library lock violates the project contract.
- Record the compiler, architecture, build flags, and atomic lock-free checks in benchmark results. Larger `std::atomic<struct>` values are not automatically lock-free. [C++ atomic lock-free properties](https://eel.is/c++draft/atomics.lockfree)

Start with `uint32_t` values for all queues to make comparisons meaningful. An integer token may identify an external object, but that object's ownership, lifetime, and allocation are outside this queue's guarantee.

Place independently written cursors on separate, configurable cache-line boundaries. Start with compact slots; benchmark per-slot padding before adding it. Padding every slot can increase the working set substantially.

## 5. SPSC design

### State

```text
slots[N]                 // ordinary, non-atomic values
atomic<uint64_t> write   // producer's next publication position
atomic<uint64_t> read    // consumer's next consumption position
```

Initially both cursors are zero. Only the producer writes `write`; only the consumer writes `read`.

### Enqueue

```text
w = write.load(relaxed)
r = read.load(acquire)
if unsigned_distance(w, r) == N:
    return full

slots[w & mask] = value
write.store(w + 1, release)
return success
```

### Dequeue

```text
r = read.load(relaxed)
w = write.load(acquire)
if r == w:
    return empty

out = slots[r & mask]
read.store(r + 1, release)
return success
```

### Why this works

The producer writes a slot before publishing `write`. A consumer that acquires that publication can safely read the ordinary payload. The consumer finishes its read before releasing `read`; a producer that acquires that advancement can safely reuse the slot.

```text
payload write
    -> write.store(release)
    -> write.load(acquire), observing publication
    -> payload read
    -> read.store(release)
    -> read.load(acquire), observing reclamation
    -> next payload write into that slot
```

These two ownership transfers prevent both reading an unfinished item and overwriting an item still being read. Each cursor has one writer, so no CAS is needed. The successful enqueue takes effect at the write-cursor store; successful dequeue takes effect at the read-cursor store.

Release/acquire synchronization depends on observing the appropriate publication, not merely putting those labels on unrelated accesses. [C++ memory-order specification](https://eel.is/c++draft/atomics.order)

With bounded value copies and bounded atomic primitives, each call has bounded work and no retry loop. Say “wait-free at the algorithm level”; C++ lock-free atomic availability alone is not a universal hardware latency guarantee.

Use unsigned modular arithmetic for the cursors, keep capacity below half the counter range, and preserve the occupancy invariant `0 <= distance(write, read) <= N`. There is no delayed CAS observer in SPSC; the other endpoint cannot lap a paused endpoint indefinitely. Physical index wrap and unsigned counter wrap should both be tested.

## 6. Compact atomic records for MPSC and SPMC

### Representation

Use a single `atomic<uint64_t>` per slot:

```text
upper 32 bits: generation/state sequence
lower 32 bits: payload value
```

Pack and unpack with unsigned shifts and masks. Do not use implementation-dependent C++ bit-field layout. All 32-bit payload values are legal; no value is reserved to mean empty.

For logical position `p`:

```text
EMPTY(p) = sequence 2*p
READY(p, value) = sequence 2*p + 1 plus payload
```

Initialize slot `i` to `EMPTY(i)`. After consuming position `p`, the slot becomes `EMPTY(p + N)`.

```text
EMPTY(p) -- publish payload atomically --> READY(p, value)
READY(p, value) -- consume atomically --> EMPTY(p + N)
```

There is no publicly visible “reserved but not yet published” slot state. Consumers take a local copy of the complete atomic record before attempting to consume it. There are no concurrent ordinary accesses to the slot payload.

### Memory ordering

Use **`memory_order_seq_cst` for every shared load, store, and CAS in the initial MPSC/SPMC implementation**, including CAS failure ordering. Use `compare_exchange_strong` to make the progress argument independent of spurious failures. Every retry reloads the cursor and record.

This conservative baseline makes the proof use one total order of shared atomic operations. Do not copy SPSC's relaxed cursor operations into these algorithms. Here, helpers and atomic slot transitions interact with cursor advancement. Weakening these orders needs a separate argument, especially if payload tokens publish access to external memory.

`seq_cst` is a correctness baseline, not a claim that it fixes flawed ownership protocols.

### Counter exhaustion and ABA

Do **not** let the compact sequence field silently wrap. A thread suspended with an old CAS expectation could otherwise succeed against a different generation of the same slot: the ABA problem.

For a 32-bit sequence field, define:

```text
L = 2^31 - N
valid enqueue positions: 0 <= p < L
```

At position `L`, enqueues return `exhausted`; existing items can still drain. The final reclamation sequence fits in 32 bits. Cursors use `uint64_t` and never wrap within this lifetime. Require `N < 2^31`.

Only after all operations have stopped and the queue is drained may the owner reinitialize it. This is an externally coordinated lifecycle operation, not a concurrent hot-path reset.

This limitation is material: roughly two billion publications is only about 3.6 minutes at ten million enqueues per second. A 16-bit payload / 48-bit sequence configuration extends the budget to roughly `2^47` publications, about 163 days at that rate, but still requires an exhaustion policy. These are operation-budget illustrations, not performance predictions.

For an interview prototype, the 32/32 layout is easy to inspect and exhaustion is easy to test. For a long-running infrastructure queue, resolving this limitation is a prerequisite: use a validated wrap-safe algorithm, or a wider record only on a target where its atomic operations are actually lock-free.

## 7. MPSC design

### State and ownership

```text
atomic<uint64_t> slots[N]
atomic<uint64_t> tail_hint = 0   // producers advance/help this cursor
uint64_t consumer_pos = 0       // private to the only consumer
```

The cursor is a hint to the earliest publication position not yet acknowledged by producers. A successful slot CAS publishes an item; cursor advancement can be helped afterward.

### Producer algorithm

All shared operations below are sequentially consistent.

```text
loop:
    p = tail_hint.load()
    if p == L:
        return exhausted

    record = slots[p & mask].load()
    s = sequence(record)

    if s < 2*p:
        return full

    if s > 2*p:
        tail_hint.CAS(p, p + 1)    // another producer already published p
        continue

    if slots[p & mask].CAS(record, READY(p, value)):
        tail_hint.CAS(p, p + 1)    // one attempt; somebody else may have helped
        return success

    // CAS lost to another producer; restart with fresh state
```

CAS the **entire record**, including its observed payload bits. The old payload is irrelevant logically but still part of the atomic expected value.

### Consumer algorithm

```text
p = consumer_pos
record = slots[p & mask].load()
if sequence(record) != 2*p + 1:
    return empty

value = payload(record)
slots[p & mask].store(EMPTY(p + N))
consumer_pos = p + 1
out = value
return success
```

No consumer CAS is needed: there is only one consumer, and producers cannot modify a ready slot. The consumer can pop a published record before its producer has advanced `tail_hint`.

### Why this works

1. **Exclusive publication:** only one producer can change a particular `EMPTY(p)` record to `READY(p, value)`.
2. **No publication hole:** the payload and readiness appear in the same atomic transition. A producer paused before its CAS owns nothing.
3. **Helping:** a record sequence greater than `2*p` proves position `p` was already published, possibly consumed too. Any producer can advance a stale hint. A producer paused after publication holds no required private state.
4. **FIFO:** producers cannot proceed to `p + 1` until `p` has been published. The consumer advances in position order.
5. **Safe reuse:** the consumer copies the value before storing the next empty generation.
6. **Full detection:** an older generation at `p` means the slot from the preceding lap has not been reclaimed. With a sole FIFO consumer, that establishes a full ring during the attempt.

Successful push linearizes at the slot CAS; successful pop at the empty-record store. Concurrent producer invocations are ordered by successful publication, not by thread ID or wall-clock invocation order.

CAS failures and failed helper attempts imply competing state advancement. Suspending a producer before or after publication does not prevent the others from continuing. A suspended sole consumer can cause the queue to fill; returning full then is the correct capacity behavior.

## 8. SPMC design

### State and ownership

```text
atomic<uint64_t> slots[N]
uint64_t producer_pos = 0       // private to the only producer
atomic<uint64_t> head_hint = 0  // consumers advance/help this cursor
```

### Producer algorithm

```text
p = producer_pos
if p == L:
    return exhausted

record = slots[p & mask].load()
if sequence(record) != 2*p:
    return full

slots[p & mask].store(READY(p, value))
producer_pos = p + 1
return success
```

The producer needs no CAS because it is the only writer allowed to publish into an empty slot of that generation.

### Consumer algorithm

```text
loop:
    p = head_hint.load()
    record = slots[p & mask].load()
    s = sequence(record)

    if s < 2*p + 1:
        return empty

    if s > 2*p + 1:
        head_hint.CAS(p, p + 1)    // another consumer already consumed p
        continue

    value = payload(record)       // private copy from the atomic load
    if slots[p & mask].CAS(record, EMPTY(p + N)):
        head_hint.CAS(p, p + 1)    // one attempt; helper may already have done it
        out = value
        return success

    // Discard the private copy and retry; another consumer won
```

### Why this works

1. **Exclusive consumption:** only one consumer can CAS a particular ready generation to the next empty generation.
2. **No unsafe payload race:** all contenders obtain the payload through an atomic record load. Losing consumers discard their local copies.
3. **Immediate reclamation:** the winning CAS both consumes the item and frees the slot. The producer may reuse it even if the winning consumer is suspended before returning.
4. **Helping:** a later sequence proves position `p` was consumed. Another consumer advances the stale head hint.
5. **FIFO:** head advancement follows successful consumption of the previous position. Successful pops are ordered by slot CAS.
6. **No dependence on a losing reader:** a consumer suspended before CAS cannot prevent another consumer from winning or the producer from eventually reusing the slot. Its old CAS fails against the new generation.

Successful push linearizes at the ready-record store; successful pop at the empty-record CAS. Different consumers may **return or finish processing** in a different order from dequeue linearization. FIFO queue removal does not enforce FIFO job completion.

## 9. Why the usual non-atomic payload variants are not selected

These alternatives are useful to explain during the interview, but do not meet this document's strict progress target without more machinery.

### MPSC: reserve, write, publish

```text
P0 reserves position k and is suspended before publishing.
P1 publishes k + 1 and returns successfully.
C reaches k and cannot read its unfinished payload.
```

Waiting for P0 creates a progress dependency. Reporting ordinary empty after P1 has completed can violate a strict FIFO queue specification. Reporting a distinct `not_ready` result is possible, but changes the API and does not remove the publication hole.

### SPMC: claim, copy, release

```text
C0 claims position k and is suspended before copying its ordinary payload.
Other consumers proceed.
P eventually wraps to k's physical slot.
```

The producer cannot overwrite that slot while C0 may still read it. A later slot being free does not make this slot safe. Similarly, reading an ordinary payload before winning a head CAS is unsafe: another consumer can win and allow concurrent reuse.

Increasing memory ordering to `seq_cst` does not solve either ownership problem. The selected compact-record algorithms remove the vulnerable interval by making the relevant payload/state transition atomic.

## 10. Validation plan

Passing a stress test does not prove lock-freedom or correctness under all C++ executions. Use tests to challenge the invariants and retain a written argument for each shared access.

### Functional and accounting tests

- Empty queue; one item; exactly `N` accepted items; next push rejected.
- Verify rejected pushes do not modify existing items and failed pops preserve `out`.
- Repeated physical wrap with capacities 2, 4, 8, and larger sizes.
- Payload edge cases: zero and all bits set.
- After workers stop and the queue drains: accepted count equals popped count.
- Unique IDs: no missing accepted IDs, duplicate IDs, or fabricated IDs.
- FIFO for sequential operations and per-producer order for MPSC.
- For SPMC, check removal order with controlled instrumentation, not the order consumer threads append to a results vector.

### Adversarial schedules

Add test-only pause hooks around shared transitions:

| Pause point | Required observation |
| --- | --- |
| MPSC before publication CAS | Other producers can publish; paused producer owns no slot |
| MPSC after publication, before tail help | Consumer can pop; other producers help and continue |
| SPMC after record load, before consumption CAS | Other consumer wins; stale CAS fails after reuse |
| SPMC after consumption CAS, before head help | Other consumers help; producer can reuse the freed slot |
| SPSC before release publication | Consumer cannot read the unfinished value |
| SPSC before release reclamation | Producer cannot reuse the slot |

Use reduced-width sequence fields in a test configuration to reach exhaustion quickly. Confirm rejection at the boundary, successful draining, and reinitialization only after every old operation has finished. Separately test SPSC unsigned cursor wrap.

Record small concurrent invocation/response histories and check for a legal FIFO sequential ordering that respects real-time precedence. Do not infer cross-thread enqueue order from timestamps taken before the actual publication.

### Tooling and review

- Use a race detector on a supported compiler/platform; add a Linux/Clang validation build if the Windows toolchain lacks the required support.
- Use address and undefined-behavior sanitizers where supported.
- Exercise a weakly ordered architecture when available, especially for SPSC acquire/release.
- Model small capacities and pause schedules; review the compact-record algorithm independently before weakening atomics.
- Check lock-free atomic support in each target build.

## 11. Benchmark plan

Measure release builds after correctness validation:

| Dimension | Cases |
| --- | --- |
| Topology | 1P/1C, 2P/1C, 4P/1C, 1P/2C, 1P/4C |
| Capacity | 64, 1,024, 65,536 |
| Traffic | Balanced, producer faster, consumer faster, bursts |
| Placement | Same core complex, separate cores, cross-NUMA if available |
| Baseline | Equivalent fixed-capacity queue protected by a mutex |

Report successful transfers per second, failed attempts/drop rate, and operation latency distributions including tail latency. Separate attempted pushes from accepted items. Distinguish time spent inside a call from enqueue-to-dequeue residence time.

Use thread-local counters, warm-up, repeated runs, and recorded CPU/compiler details. Keep timing and logging overhead out of the primary throughput loop. Do not claim that lock-free must outperform a mutex, or that throughput demonstrates a progress guarantee.

Run below the compact generation budget or reset between fully quiescent benchmark runs. Never silently reset counters to extend a run.

## 12. Implementation milestones

1. Implement SPSC with the shared value API, acquire/release ownership transfers, and boundary tests.
2. Implement/test packing, generation comparisons, exhaustion, and target atomic requirements.
3. Implement MPSC with atomic publication and producer helping; validate forced pauses.
4. Implement SPMC with atomic consumption/reclamation and consumer helping; validate stale readers.
5. Check small concurrent histories, run sanitizers, and review invariants against the actual code.
6. Benchmark against a mutex baseline; optimize one measured bottleneck at a time.
7. If long-running service use or larger payloads is needed, complete a separate wrap-safe/wider-payload design before claiming production suitability.

Completion means the implemented API matches these contracts, ordering choices have an explanation, pause tests confirm the intended helping behavior, and limitations are visible in the README. A useful interview discussion should explain both why a slot is safe to access and why a paused participant cannot strand a required transition.
