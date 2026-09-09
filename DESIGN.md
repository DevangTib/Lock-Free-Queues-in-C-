# Fixed-capacity concurrent message queues in C++

Status: SPSC, NCQ, SPMC, and MPSC are implemented as header-only classes under [include/lfq](include/lfq). Tests are in [spsc_ring_tests.cpp](tests/spsc_ring_tests.cpp) and [concurrent_ring_tests.cpp](tests/concurrent_ring_tests.cpp). See [README.md](README.md) for build commands and validation limits. Learning order: **SPSC -> NCQ -> SPMC -> MPSC**.

## 1. Scope and supported messages

Implement SPSC and NCQ-based SPMC/MPSC queues for general trivially copyable messages, including 500-byte structs, with fixed storage and drop-on-full behavior. Message storage is non-atomic and queue metadata is atomic. The direct atomic-payload MPSC/SPMC queues are not part of the implementation plan. SPSC overwrite using a seqlock-style protocol remains in [possible_extensions.md](possible_extensions.md).

```cpp
struct Message {
    std::byte bytes[500];
};
static_assert(std::is_trivially_copyable_v<Message>);
```

There is no small-message correctness restriction. Copying costs grow with `sizeof(T)`, which is fixed per queue instantiation. Variable-length messages can use a fixed maximum-size array plus a length field. `std::string` and `std::vector` are outside this value-copy interface.

| Queue | Producers | Consumers | Delivery |
| --- | --- | --- | --- |
| SPSC | One | One | Each item goes to the sole consumer |
| MPSC | Multiple | One | Producers merge into one FIFO |
| SPMC | One | Multiple | Consumers compete; each item goes to one consumer |

SPMC here is a work queue, not broadcast. Every consumer receiving every message requires a different design.

Requirements:

- Compile-time capacity; all storage belongs to the queue object.
- No allocation, mutex, condition variable, callback, or operating-system wait in push/pop.
- Copy in on successful push; copy out on successful pop.
- Reject incoming messages when storage is unavailable; never overwrite unread or actively accessed data.
- Explain ownership, memory ordering, FIFO behavior, and progress separately.
- Require lock-free metadata atomics on the target.

Exclude resizing, blocking wrappers, overwrite mode, zero-copy references, concurrent reset/destruction, and exact concurrent size queries.

## 2. Selected design and research basis

SPSC retains the direct array with separate read/write counters.

MPSC/SPMC use **a fixed message array and two bounded index queues**. Indices refer to internal storage; callers still push/pop complete T values and do not manage a pool.

The research basis is Ruslan Nikolaev's *A Scalable, Portable, and Memory-Efficient Lock-Free FIFO Queue* (DISC 2019). Section 3 describes two-queue indirection for arbitrary-size data. Section 4 presents NCQ (Naive Circular Queue); Section 6 gives its lock-free argument. Choose NCQ initially because its CAS/helping protocol is smaller to study than SCQ's FAA, invalidation, and threshold protocol. SCQ is a later performance option, not a prerequisite for large messages. The source assumes a bounded participant count no greater than pool capacity. [Paper](https://arxiv.org/html/1908.04511)

Use the author's [NCQ reference implementation, lfring_naive.h](https://github.com/rusnikola/lfqueue/blob/master/lfring_naive.h). The [repository](https://github.com/rusnikola/lfqueue) distinguishes NCQ from SCQ and provides both.

This is an adaptation of an existing algorithm. A published argument supports that algorithm; it does not automatically validate our C++ port. Pin the reference commit when implementation begins, retain applicable attribution/license notices if porting code, and review deviations explicitly.

| Component | Initial implementation |
| --- | --- |
| SPSC | Direct message array, acquire/release cursors |
| MPSC | Message pool + free-index NCQ + ready-index NCQ |
| SPMC | Same pool/NCQ engine, restricted public thread topology |
| Internal NCQ | Full MPMC implementation initially; no topology-specific weakening |

MPSC/SPMC use three shared fixed arrays: message blocks and the two index rings. The message array is storage, not a per-producer queue. Blocks have no permanent producer assignment.

NCQ supports MPMC operations internally. Expose and test SPMC and MPSC message queues first; advertising a general MPMC wrapper would additionally require simultaneous multi-producer/multi-consumer validation. A published algorithm and benchmarks do not replace tests of this port.

## 3. API and precise capacity contract

Illustrative API:

```cpp
enum class PushResult { success, no_capacity };
enum class PopResult  { success, empty };

template <class T, std::size_t Capacity>
class MpscRing {
public:
    using value_type = T;
    [[nodiscard]] PushResult try_push(const T& value) noexcept;
    [[nodiscard]] PopResult try_pop(T& out) noexcept;
    static constexpr std::size_t capacity() noexcept { return Capacity; }

    MpscRing();
    MpscRing(const MpscRing&) = delete;
    MpscRing& operator=(const MpscRing&) = delete;
    MpscRing(MpscRing&&) = delete;
    MpscRing& operator=(MpscRing&&) = delete;
};
```

Use the same operational interface for SPSC and SPMC.

| Result | Meaning |
| --- | --- |
| Push success | A copy of the message has been published |
| Push no_capacity | Nothing accepted; input unchanged |
| Pop success | Exactly one message removed and copied into out |
| Pop empty | Nothing removed; out unchanged |

### Capacity includes messages being copied

For MPSC/SPMC, Capacity = N is the number of message blocks. At abstract ownership boundaries:

```text
free blocks + producer-owned blocks + ready blocks + consumer-owned blocks = N
```

A push returns `no_capacity` when it cannot obtain a free block. A block held by an in-flight copy still consumes capacity. Filling an empty queue with sequential pushes and no pops accepts exactly N messages and rejects the next.

For example, a consumer may have removed an index but still be copying its message. That block cannot yet be reused. A push may therefore fail while fewer than N published messages remain in the ready queue. **Rejection is not proof that N published messages exist.** This is a resource-bounded API, not an exact occupancy-only full predicate.

Drop-on-full means the caller may discard a rejected incoming value. The queue reports rejection and never silently loses accepted messages. A failed attempt says nothing about capacity becoming available immediately afterward.

There is no wait for new data or capacity, but internal CAS retries are allowed. `try_` does not imply bounded own-step completion under contention.

For SPSC, an acquire load may conservatively observe an older opposite cursor and reject an attempt. For the initial sequentially consistent NCQ engine, failure follows the corresponding internal dequeue's empty decision. The wrapper has no separate size/full check.

### Message and object lifetime

- Require a complete, non-cv, trivially copyable object type T. Wrap raw arrays in a struct or `std::array` for the public type.
- Arguments are live complete T objects; out is writable. Callers must not access those objects concurrently in a conflicting way.
- Copy representations with `std::memcpy` and ordinary `&` addresses; T must not overload unary operator&.
- Internal blocks are `std::array<T, Capacity>`. T must be default-constructible; all slots are constructed with the queue, before concurrent access.
- Copies use existing live T objects and do not require a copy-assignment operator. Push/pop do not construct new objects.
- Pointer members are copied as pointers. Pointee lifetime remains the application's responsibility.

Byte copying for trivially copyable objects is supported by the [C++ object-representation rules](https://eel.is/c++draft/basic.types.trivial).

Initialize before publishing the queue to threads. Reset, destruction, and thread-role reassignment require external synchronization and no outstanding calls. Never reclaim a suspended thread's block if it might resume.

## 4. Fixed storage and target assumptions

```text
N = Capacity, a power of two

SPSC:
    T blocks[N]
    atomic<uint64_t> read, write

MPSC/SPMC:
    T blocks[N]
    NcqIndexRing<N> free_indices
    NcqIndexRing<N> ready_indices

Each NcqIndexRing:
    atomic<uint64_t> entries[N]  // generation plus internal block index
    atomic<uint64_t> head, tail
```

All N message blocks are usable. Approximate MPSC/SPMC storage before padding is `N * sizeof(T) + 2 * N * 8` bytes plus cursors. There is no atomic 500-byte object or external allocator.

Require `std::atomic<uint64_t>::is_always_lock_free`. Reject unsupported builds rather than silently using library locks. [C++ atomic lock-free properties](https://eel.is/c++draft/atomics.lockfree)

Separate independently written cursors onto configurable cache-line boundaries. Start without per-message padding. Reference-code cache-index remapping is a later optimization; it must be applied consistently to access and initialization.

Document maximum participant count K. Initially require N >= K for MPSC/SPMC and at most one outstanding call per participant. K includes the sole endpoint and every participant on the multi-threaded side. Use N >= 2 for SPSC.

## 5. SPSC: direct array with ownership transfers

Initially read = write = 0. Only the producer writes write; only the consumer writes read.

```text
try_push(value):
    w = write.load(relaxed)
    r = read.load(acquire)
    if unsigned_distance(w, r) == N:
        return no_capacity
    copy sizeof(T) bytes from value into blocks[w & (N - 1)]
    write.store(w + 1, release)
    return success

try_pop(out):
    r = read.load(relaxed)
    w = write.load(acquire)
    if r == w:
        return empty
    copy sizeof(T) bytes from blocks[r & (N - 1)] into out
    read.store(r + 1, release)
    return success
```

Empty is read == write; full is unsigned write - read == N.

The producer completes its entire copy before releasing write. The consumer acquires publication before reading, finishes its copy, and releases read. The producer acquires reclamation before overwriting that block.

## 6. Shared MPSC/SPMC ownership protocol

Initialize free_indices with every index 0..N-1 exactly once and ready_indices empty.

```text
FREE
  -> producer acquires index
PRODUCER-OWNED
  -> producer finishes copy and publishes index
READY
  -> consumer removes index
CONSUMER-OWNED
  -> consumer finishes copy and returns index
FREE
```

These are proof states, not an additional per-block atomic state machine.

```text
try_push(value):
    i = free_indices.try_dequeue()
    if no index:
        return no_capacity
    copy sizeof(T) bytes from value into blocks[i]
    ready_indices.enqueue_assuming_space(i)
    return success

try_pop(out):
    i = ready_indices.try_dequeue()
    if no index:
        return empty
    copy sizeof(T) bytes from blocks[i] into out
    free_indices.enqueue_assuming_space(i)
    return success
```

Acquiring a storage block does not reserve a position in the message FIFO. The ready index is published only after the copy is complete.

Do not access blocks[i] before successfully dequeuing its index. Losing consumers must never speculatively copy ordinary message bytes.

### Why internal enqueue has space

Every index is in one queue or owned by one operation. A caller publishing/returning an index owns a block currently absent from the destination queue. Across all callers, there are only N indices. Consequently, the destination can accommodate the enqueue without waiting for a future dequeue.

This conservation invariant is a precondition of NCQ enqueue. Keep `enqueue_assuming_space` private; it cannot accept arbitrary or duplicate indices. Contention may cause retries, but it does not poll for capacity.

There is no cancellation/failure path after taking a block: the copy cannot invoke throwing user code, and the index must be published or returned exactly once.

## 7. NCQ index engine

The pseudocode below adapts the author's [NCQ implementation](https://github.com/rusnikola/lfqueue/blob/master/lfring_naive.h). It uses identity slot mapping, seq_cst operations, strong CAS, and fresh loads on each retry. The reference retains some local observations across retries and uses weaker memory orders; those optimizations are deferred.

### Representation and initialization

For N = 2^b, an atomic entry uses its low b bits for a block index and the remaining bits for the cycle. Message size consumes no generation bits.

```text
physical slot = position & (N - 1)
cycle base   = position & ~(N - 1)   // uint64_t arithmetic
entry        = cycle base | block_index
```

The physical index-ring slot and the stored message-block index are different concepts.

With identity mapping, an empty ring starts with head = tail = N and entries at cycle zero. A full free ring starts with head = 0, tail = N, and entries containing the distinct block indices at cycle zero.

### Enqueue behavior

Read tail and its entry. If the entry is already published for tail's cycle, help advance tail. Otherwise, an immediately preceding-cycle entry is eligible under the capacity precondition. Publish current cycle plus block index with a CAS on the complete entry, then attempt tail advancement.

Refresh stale observations on other generation mismatches or conflicting updates. Do not increment tail to create a private reservation before publication. Publication linearizes at the successful entry CAS.

### Dequeue behavior

Read head and atomically copy its entry. A matching cycle supplies a candidate index. CAS head forward to claim that candidate; only the winner returns it. On failure discard the candidate and retry. An immediately preceding-cycle entry indicates empty; other mismatches require refreshing state.

Dequeue does not clear the entry. Its candidate was copied atomically before claiming head, so later entry replacement cannot tear that local copy. Message bytes are accessed only after ownership is obtained.

### Implementation pseudocode

All positions, entries, and masks below are uint64_t. N is a power of two, N >= 2. The counter-lifetime precondition below applies; this pseudocode does not establish safety through full counter wrap.

```text
class NcqIndexRing<N>:
    MASK = uint64_t(N - 1)
    CYCLE_MASK = bitwise_not_64(MASK)

    atomic<uint64_t> entries[N]
    atomic<uint64_t> head
    atomic<uint64_t> tail

    cycle(x):
        return x & CYCLE_MASK

    slot(position):
        return position & MASK

    block_index(entry):
        return entry & MASK
```

In this pseudocode, `CAS(atom, old, next)` creates a local `expected = old` and calls `compare_exchange_strong(expected, next, seq_cst, seq_cst)`. It returns only success/failure; it does not modify the caller's `old`. Thus a failed CAS cannot silently change the logical position used by later expressions. Every shared load is seq_cst too.

**Initialization:** perform before threads can access the object, not as concurrent reset operations.

```text
init_empty():                       // For ready_indices
    for i in 0 .. N-1:
        entries[i].initialize(0)    // Cycle zero; low bits are not a live index
    head.initialize(N)              // First dequeue expects cycle N
    tail.initialize(N)              // First publication uses cycle N

init_full():                        // For free_indices
    for i in 0 .. N-1:
        entries[i].initialize(i)    // Cycle zero, distinct live block index i
    head.initialize(0)
    tail.initialize(N)
```

`initialize` denotes construction or initialization before publication, not a special concurrent atomic operation. The initial zero in an empty entry is not an empty-value sentinel: block index zero is valid. The entry's cycle relative to head distinguishes absence from a live index.

**Enqueue:** private operation; caller owns a valid index and the two-queue conservation invariant guarantees capacity for this insertion.

```text
enqueue_assuming_space(index):
    assert 0 <= index < N

    loop:
        t = tail.load(seq_cst)
        j = slot(t)
        observed = entries[j].load(seq_cst)
        tc = cycle(t)
        ec = cycle(observed)

        if ec == tc:
            // Someone published t but may not have advanced tail yet.
            CAS(tail, t, t + 1)
            continue

        if ec + N != tc:
            // Snapshot belongs to a different lap. Refresh both observations.
            continue

        // Entry from the preceding cycle is eligible under our capacity invariant.
        desired = tc | index
        if CAS(entries[j], observed, desired):
            // Publication has succeeded, regardless of who advances tail.
            CAS(tail, t, t + 1)
            return

        // Another publisher changed the entry. Restart with fresh observations.
```

This function has no `full` result. It must not be called arbitrarily on an already full ring. The message wrapper detects no_capacity by failing to dequeue a free index before copying a message. Internal enqueue is safe only with the ownership accounting described in section 6.

**Dequeue:** returns an optional index, not a payload. All values 0..N-1 are valid indices, so represent absence separately.

```text
try_dequeue() -> optional<uint64_t>:
    loop:
        h = head.load(seq_cst)
        j = slot(h)
        observed = entries[j].load(seq_cst)
        hc = cycle(h)
        ec = cycle(observed)

        if ec != hc:
            if ec + N == hc:
                return none          // Next position is not published.
            continue                 // Stale snapshot; reload head and entry.

        candidate = block_index(observed)
        if CAS(head, h, h + 1):
            return some(candidate)   // This caller exclusively owns that index.

        // Another consumer claimed the position. Discard candidate and retry.
```

Do not clear entries[j] after the successful head CAS. A producer may already have reused that physical entry. The returned index is a local copy from the atomic load, and the caller now owns its separate message block.

**Small example, N = 4:** an empty ring starts at head = tail = 4 with cycle-zero entries. Publishing block index 2 into position 4 changes entries[0] to `4 | 2 = 6`; tail advances to 5. Dequeue sees cycle(6) = cycle(4) = 4, claims head 4 -> 5, and returns `6 & 3 = 2`. It leaves entries[0] unchanged. At position 8, that entry's cycle 4 is the preceding cycle and can be replaced under the capacity invariant.

### NCQ pseudocode attribution

Adapted from lfring_naive.h, Copyright (c) 2019 Ruslan Nikolaev. The source is dual-licensed under BSD-2-Clause/MIT; the MIT notice for this adaptation is retained here:

```text
Copyright (c) 2019 Ruslan Nikolaev

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
```

### Memory ordering

Initially use **seq_cst for every shared NCQ metadata load and CAS, including CAS failure order**, and strong CAS. The reference uses acquire loads and acquire/release CAS in many places; retain stronger ordering in the first port until reviewed.

| Transfer | Required relationship |
| --- | --- |
| Copy message, then publish ready index | Publication releases the completed write |
| Obtain ready index, then copy message | Dequeue acquires publication |
| Finish reading, then return free index | Return releases completion of the read |
| Obtain free index, then overwrite | Dequeue acquires reclamation |

```text
message write
  -> ready-index release publication
  -> acquiring ready-index dequeue
  -> message read
  -> free-index release publication
  -> acquiring free-index dequeue
  -> next message write
```

The slot publication CAS and observing entry load establish the payload synchronization. Preserve ordering on NCQ cursor/helping operations too; they are not SPSC private cursors.

Sequentially consistent operations include the acquire/release properties needed here. No separate fences, atomic message fields, or volatile are needed. Order reductions require an argument for both ownership transfers and the index protocol. [C++ memory ordering](https://eel.is/c++draft/atomics.order)

### Counter lifetime and ABA

Use 64-bit positions and the source's cycle representation. An entry stores an index, not a full message, so the low log2(N) bits encode that index and the remaining bits encode a cycle. The cycle advances once per N positions. For N = 1024, 54 cycle bits and 10 index bits represent 2^64 logical positions before repetition. This eliminates the short 32-bit-sequence lifetime, but fixed-width generations still do not provide an unlimited ABA guarantee.

The initial correctness scope assumes no full uint64_t counter wrap during the object's operational lifetime. Set a supported budget below 2^63 head/tail advances per internal ring, including initialization offsets. This is a lifetime precondition, not an `exhausted` hot-path result. Drain, join all participants, and reconstruct well before that bound; never reset a live queue.

Physical array wrap is supported and frequent. Full counter wrap across an arbitrarily suspended CAS observer is a separate problem. Reduced-width tests exercise stale generations and the lifecycle boundary without claiming arbitrary-wrap correctness.

## 8. MPSC behavior

| Internal ring | Enqueue callers | Dequeue callers |
| --- | --- | --- |
| ready_indices | Message producers | Sole consumer |
| free_indices | Sole consumer | Message producers |

A producer paused during copying owns a block but has not entered the ready FIFO. Another producer may publish first:

```text
P0 acquires block 7 and pauses mid-copy.
P1 acquires block 3, finishes B, and publishes index 3.
C receives B without waiting for P0.
P0 later finishes A and publishes index 7.
```

Overlapping pushes may be ordered this way. FIFO follows ready-index publication, not free-block allocation or invocation time. Sequential pushes from one producer preserve its order. A completed push precedes another push that begins afterward.

Push linearizes at ready-index publication; pop at the successful ready-ring head CAS. Returning a block affects resource availability, not removal order.

A producer paused after publication but before tail advancement can be helped by another producer. The consumer can see the entry before its publisher returns. The publisher must never touch message bytes after publication, even while finishing metadata bookkeeping.

## 9. SPMC behavior

| Internal ring | Enqueue callers | Dequeue callers |
| --- | --- | --- |
| ready_indices | Sole producer | Message consumers |
| free_indices | Message consumers | Sole producer |

A consumer first wins an index dequeue and then copies its block. A consumer paused during copying keeps its block out of free_indices:

```text
C0 claims index 7 and pauses during its copy.
C1 claims index 3, finishes, and returns index 3.
P can reuse block 3 for another message.
P cannot reuse block 7 until C0 returns it.
```

The message array is not reused in a forced physical order. Other blocks can circulate around a suspended consumer.

Several consumers may inspect an atomic index entry, but only the head-CAS winner reads its message. A losing consumer discards its stale candidate without dereferencing it.

Pushes follow the sole producer's order. Pops linearize at ready-ring head CAS operations. Consumer return order and processing completion order can differ from FIFO removal order. No fairness or broadcast guarantee is made.

## 10. Correctness and progress obligations

The key safety invariants are:

1. Every block index has exactly one abstract owner.
2. Only the owner accesses ordinary message bytes.
3. Ready publication follows the complete producer copy.
4. Free publication follows the complete consumer copy.
5. An index is published/returned once, never duplicated.
6. Every internal enqueue satisfies its capacity precondition.

Together with the index FIFO, these establish no torn messages, no premature reuse, exactly-once successful removal, and FIFO publication/removal.

Lock-free means system-wide operation completion; an individual loop may starve. Wait-free means bounded own-step completion. Absence of mutexes establishes neither.

NCQ's competing CAS failures demonstrate another operation's advancement; helpers complete a published tail transition. The wrapper composes two index operations and a finite copy while preserving the enqueue precondition.

A suspended participant can retain one block. Others use the remaining pool instead of waiting for that specific block. With N >= K and one call per participant, K - 1 suspended participants cannot privately retain every block. Ready messages can occupy the remainder, legitimately causing capacity rejection. A stopped sole producer or consumer naturally leads the opposite side toward empty or no-capacity results.

This does not guarantee unchanged effective capacity, successful push on every call, individual fairness, or hard real-time latency. Finite copies and lock-free metadata primitives are algorithmic assumptions; scheduling and page faults are outside that guarantee.

**Validate against the resource-capacity contract.** Do not claim linearizability against a different specification that permits rejection only when N published messages exist.

## 11. Alternatives not selected

| Alternative | Reason |
| --- | --- |
| Atomic message plus generation | Arbitrary-size messages cannot rely on portable lock-free whole-message atomics |
| Direct reserve/write/publish ring | A paused producer can leave a FIFO publication hole |
| Direct claim/copy/release ring | A paused reader can prevent reuse of a required physical slot |
| Per-producer SPSC queues | Changes global FIFO and partitions capacity |
| SCQ index engine | Possible later optimization; more protocol complexity initially |

Vyukov's bounded sequence-slot queue explicitly disclaims formal lock-freedom. It is useful to study, but should not be relabeled to meet this requirement. [Author's description](https://sites.google.com/site/1024cores/home/lock-free-algorithms/queues/bounded-mpmc-queue)

## 12. Validation plan

### Functional tests

- Test 4-byte, 64-byte, 500-byte, and 4-KiB messages.
- Include embedded arrays, over-aligned types, and default-constructible trivially copyable types with deleted copy assignment.
- Check empty, one item, N sequential pushes, rejection, and complete drain.
- Failed push preserves input; failed pop preserves output.
- After quiescence and drain, accepted count equals popped count and every block index is free exactly once.
- Use unique IDs, producer IDs, lengths, and checksums to detect duplicates, loss, torn copies, and premature reuse.
- Compare initialized fields or explicitly filled arrays, not arbitrary struct padding.
- Exercise repeated physical wraps. Multi-party tests obey N >= K; N = 4 supports minimal 2P/1C and 1P/2C cases.

### Forced schedules

| Pause location | Required observation |
| --- | --- |
| Producer owns block, before/during copy | Other producer can publish another block |
| Ready publication, before tail help | Helpers and consumer continue; message is complete |
| Consumer sees index, before head CAS | Winner proceeds; loser never reads message bytes |
| Consumer owns block, during copy | Other blocks circulate; owned block is never reused |
| Consumer finishes copy, before free publication | Block remains unavailable until returned |
| Free publication, before tail help | Returning consumers can help; allocator can obtain the published index |

Instrument ownership states in tests without adding production synchronization. Include a schedule exhausting free storage with ready and in-flight messages and verify no_capacity without asserting that the ready queue is full.

Check small concurrent histories against the declared resource-bounded contract. Check FIFO at publication/removal, not by post-return result-vector append order. Model NCQ separately with small capacities and stale observations.

Use race, address, and undefined-behavior sanitizers on supported builds; add a Linux/Clang validation build if needed. Exercise a weakly ordered architecture when available. Tests support the proof review; passing stress tests alone does not establish correctness or lock-freedom.

## 13. Benchmarks and implementation sequence

Benchmark release builds with balanced traffic, bursts, and overload on either endpoint. Use 1P/1C, 2P/1C, 4P/1C, 1P/2C, and 1P/4C; capacities 64, 1,024, and 65,536; and the message sizes above.

Report successful transfers, rejected pushes, latency distributions, and CPU/compiler details. Separate copying cost from metadata cost. Compare an equivalent fixed-storage mutex queue. Keep logging and shared measurement counters out of the hot path; lock-free does not imply faster.

Implementation order:

1. **SPSC:** implement the generic typed-storage queue; validate boundaries, FIFO, and acquire/release ownership transfers.
2. **NCQ:** pin the reference revision and port the complete MPMC index engine with seq_cst atomics and identity mapping. Test initialization, cycling, competing enqueues/dequeues, helping, and the private enqueue capacity precondition. Test indices independently before adding message copies.
3. **SPMC:** implement the internal message pool and shared wrapper, then expose the SPMC class. Validate competing consumers, large copies, index reclamation, and consumers paused during copying.
4. **MPSC:** reuse the validated pool/engine for the MPSC class. Validate competing producers and publication ordering when a producer pauses during copying.
5. **Integration:** run 500-byte message checks, conservation/FIFO tests, forced schedules, and separate TSan and ASan/UBSan builds. A supported Linux/Clang build can provide TSan coverage when developing on Windows.
6. **Benchmarks:** compare with a fixed-storage mutex queue; consider weaker orders or cache remapping only after correctness review and measurement.

Large-message support is part of the initial deliverable. Callers do not supply an allocator or handle-lifetime protocol. The public push results are success and no_capacity; there is no exhausted result. The counter-lifetime assumption in section 7 still applies.

## 14. References

1. **Ruslan Nikolaev. A Scalable, Portable, and Memory-Efficient Lock-Free FIFO Queue. DISC 2019.** [Published paper](https://doi.org/10.4230/LIPIcs.DISC.2019.28), [readable full text](https://arxiv.org/html/1908.04511). Read section 3 for the two-index-queue design and assumptions, section 4 for NCQ, section 6 for its lock-free argument, and section 7 for concurrent evaluation. The paper's main contribution is SCQ; this project initially implements its simpler NCQ baseline.
2. **Author's reference code:** [lfqueue repository](https://github.com/rusnikola/lfqueue), [NCQ: lfring_naive.h](https://github.com/rusnikola/lfqueue/blob/master/lfring_naive.h). Pin a commit when porting and retain the applicable license notices.
3. **C++ specification:** [memory ordering](https://eel.is/c++draft/atomics.order), [atomic lock-free properties](https://eel.is/c++draft/atomics.lockfree), [trivially copyable objects](https://eel.is/c++draft/basic.types.trivial).
4. **Validation tools:** [ThreadSanitizer](https://clang.llvm.org/docs/ThreadSanitizer.html), [AddressSanitizer](https://clang.llvm.org/docs/AddressSanitizer.html), [UndefinedBehaviorSanitizer](https://clang.llvm.org/docs/UndefinedBehaviorSanitizer.html).
