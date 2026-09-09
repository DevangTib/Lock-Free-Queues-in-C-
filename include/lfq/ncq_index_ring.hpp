#pragma once

#include <array>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <optional>

namespace lfq::detail {
struct NcqTestAccess;

// Internal MPMC index queue, not a general bounded value queue.
// enqueue_assuming_space requires an owned index and guaranteed destination capacity.
// Require lock-free uint64_t atomics and no full counter wrap during this object's life.
template <std::size_t Capacity, std::size_t CacheLineBytes = 64>
class NcqIndexRing {
    static_assert(Capacity >= 2 && (Capacity & (Capacity - 1)) == 0,
                  "Capacity must be a power of two, at least two");

public:
    enum class InitialState { empty, full };

    explicit NcqIndexRing(InitialState state = InitialState::empty) noexcept {
        const bool full = state == InitialState::full;
        for (std::size_t i = 0; i < Capacity; ++i) {
            entries_[i].store(full ? i : 0, std::memory_order_relaxed);
        }
        // Constructor runs before any concurrent access.
        head_.position.store(full ? 0 : Capacity, std::memory_order_relaxed);
        tail_.position.store(Capacity, std::memory_order_relaxed);
    }

    static constexpr std::size_t capacity() noexcept { return Capacity; }

    void enqueue_assuming_space(std::uint64_t index) noexcept {
        assert(index < Capacity);
        for (;;) {
            const auto tail = tail_.position.load(std::memory_order_seq_cst);
            auto& slot = entries_[tail & mask];
            auto observed = slot.load(std::memory_order_seq_cst);
            const auto tail_cycle = cycle_base(tail);
            const auto entry_cycle = cycle_base(observed);

            if (entry_cycle == tail_cycle) {
                // Publication already happened; help its producer advance tail.
                auto expected_tail = tail;
                tail_.position.compare_exchange_strong(expected_tail, tail + 1,
                                                       std::memory_order_seq_cst);
                continue;
            }
            if (entry_cycle + Capacity != tail_cycle) {
                continue; // Stale observations from a different lap.
            }

            if (slot.compare_exchange_strong(observed, tail_cycle | index,
                                             std::memory_order_seq_cst)) {
                // The entry CAS publishes the index; a helper may finish the cursor.
                auto expected_tail = tail;
                tail_.position.compare_exchange_strong(expected_tail, tail + 1,
                                                       std::memory_order_seq_cst);
                return;
            }
        }
    }

    std::optional<std::uint64_t> try_dequeue() noexcept {
        for (;;) {
            const auto head = head_.position.load(std::memory_order_seq_cst);
            const auto observed = entries_[head & mask].load(std::memory_order_seq_cst);
            const auto head_cycle = cycle_base(head);
            const auto entry_cycle = cycle_base(observed);

            if (entry_cycle != head_cycle) {
                if (entry_cycle + Capacity == head_cycle) {
                    return std::nullopt; // Preceding lap: expected entry not published.
                }
                continue;
            }

            auto expected_head = head;
            if (head_.position.compare_exchange_strong(expected_head, head + 1,
                                                       std::memory_order_seq_cst)) {
                // Never clear this entry: it may already be reused by a producer.
                return observed & mask;
            }
            // Lost the claim. Do not use the candidate index; reload and retry.
        }
    }

private:
    static constexpr std::uint64_t mask = Capacity - 1;
    static constexpr std::uint64_t cycle_base(std::uint64_t value) noexcept {
        return value & ~mask;
    }
    struct alignas(CacheLineBytes) Cursor {
        std::atomic<std::uint64_t> position{0};
    };
    Cursor head_;
    Cursor tail_;
    std::array<std::atomic<std::uint64_t>, Capacity> entries_;
    friend struct NcqTestAccess;
};
} // namespace lfq::detail
