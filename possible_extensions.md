# Possible extensions

The implementation plan in [DESIGN.md](DESIGN.md) is **SPSC -> NCQ -> SPMC -> MPSC**, with large-message support and drop-on-full behavior. This document covers the later SPSC overwrite/seqlock extension. Complete baseline validation before starting it.

## 1. SPSC overwrite: purpose and changed contract

Allow the sole producer to replace old unread messages when the consumer falls behind. This intentionally changes drop-on-full into loss of older messages. Suitable uses favor recent samples over guaranteed delivery of every accepted event.

Retained messages should be delivered in increasing logical position order, without duplicate successful deliveries.

## 2. What the sequence counter does

A sequence-counter reader samples a version, copies the data, and validates the version again. Odd versions indicate an update; an unchanged even version indicates a candidate stable snapshot. Linux seqlock_t also includes writer locking; its sequence-counter concept is the relevant starting point here because SPSC already has one writer.

A ring needs more than an odd/even flag: a logical generation identifies which item occupies the physical slot. The reader must validate both snapshot consistency and the expected item's identity.

The NCQ generation counters in the main design distinguish reused index-ring entries. A seqlock-style version instead validates a read that may overlap replacement. These serve different purposes.

## 3. Portable C++ storage and memory ordering

**Do not copy ordinary shared payload bytes concurrently with overwrite and then validate a sequence number.** Retrying afterward does not repair a C++ data race. This includes memcpy of a non-atomic 500-byte payload. [C++ data-race rules](https://eel.is/c++draft/intro.races)

A conservative prototype direction is an array of lock-free atomic words for each payload representation, with an atomic per-slot version:

1. Writer marks the slot version odd.
2. Writer stores the encoded payload words and logical item identity.
3. Writer publishes the next even version.
4. Reader loads the version; odd means retry.
5. Reader loads atomic words into private scratch storage.
6. Reader reloads the version and validates unchanged/even plus logical identity.
7. Only a valid snapshot is decoded/copied into the caller's output.

Initially use seq_cst for all version, identity, payload-word, and publication-cursor accesses. Under the no-version-wrap assumption, matching even versions in that total order exclude an intervening writer update during the sampled words. This is a snapshot argument, not yet a complete overwrite-ring proof.

All concurrent accesses to payload words must be atomic, including the writer's. Do not memcpy across atomic objects or reinterpret an ordinary struct as an atomic-word array. Encode/decode through local byte storage, handling partial final words and padding explicitly.

This avoids a whole-message atomic requirement but adds atomic operations proportional to message size. Require lock-free support for the word types. Weakening memory ordering needs a separate C++ proof; release/acquire on the sequence alone is not a sufficient recipe.

## 4. Progress and validation

An indefinitely retrying reader can starve under continuous writes, or spin on an odd version if the writer is suspended. Do not label that reader wait-free or inherit the baseline queue's progress claim. A bounded-attempt retry API lets calls finish without promising successful snapshot delivery.

Add tests for:

- No-overrun operation preserving normal order.
- Consumer falling behind by one, N, and several N positions.
- Writer paused after marking odd and during payload-word updates.
- Reader paused between its two version reads while multiple overwrites occur.
- 500-byte checksummed messages: never accept a mixed-generation snapshot.
- Increasing delivered positions, no duplicates, and correct loss accounting.
- Output unchanged on retry, empty, and overrun-only results.
- Reduced-width version limits and quiescent reset.
- ThreadSanitizer plus ASan/UBSan, alongside deterministic schedule tests.

Keep this extension separate from the large-message drop-on-full queues: overwrite semantics and snapshot validation are an additional project, not a necessary step toward large-message support.