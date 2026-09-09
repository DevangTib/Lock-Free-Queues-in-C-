#pragma once

#include "lfq/queue_result.hpp"

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <type_traits>

namespace lfq {
namespace detail {
struct SpscRingTestAccess;
}

// Exactly one producer calls try_push and one consumer calls try_pop.
// Lock-free progress requires lock-free uint64_t atomics on the target
template <class T, std::size_t Capacity, std::size_t CacheLineBytes = 64>
class SpscRing {
    static_assert(std::is_trivially_copyable_v<T>, "T must be trivially copyable");
    static_assert(Capacity >= 2 && (Capacity & (Capacity - 1)) == 0,
                  "Capacity must be a power of two, at least two");

public:
    using value_type = T;
    using size_type = std::size_t;

    SpscRing() = default;
    ~SpscRing() = default;

    static constexpr size_type capacity() noexcept { return Capacity; }

    PushResult try_push(const T& value) noexcept {
        const auto write = write_.position.load(std::memory_order_relaxed);
        // Acquire the consumer's completed read before reusing its slot.
        const auto read = read_.position.load(std::memory_order_acquire);
        if (write - read == Capacity) {
            return PushResult::no_capacity;
        }

        std::memcpy(static_cast<void*>(&blocks_[index(write)]), &value, sizeof(T));
        // Publish only after the entire payload has been copied.
        write_.position.store(write + 1, std::memory_order_release);
        return PushResult::success;
    }

    PopResult try_pop(T& out) noexcept {
        const auto read = read_.position.load(std::memory_order_relaxed);
        // Acquire publication before touching ordinary message bytes.
        const auto write = write_.position.load(std::memory_order_acquire);
        if (read == write) {
            return PopResult::empty;
        }

        std::memcpy(static_cast<void*>(&out), &blocks_[index(read)], sizeof(T));
        // Release storage only after the consumer's copy is complete.
        read_.position.store(read + 1, std::memory_order_release);
        return PopResult::success;
    }

private:
    // Padding the cursor types also keeps message storage off their cache lines.
    struct alignas(CacheLineBytes) Cursor {
        std::atomic<std::uint64_t> position{0};
    };

    static constexpr size_type index(std::uint64_t position) noexcept {
        return static_cast<size_type>(position & (Capacity - 1));
    }

    Cursor write_;
    Cursor read_;
    // Live, correctly aligned objects; T must be default-constructible.
    std::array<T, Capacity> blocks_;

    // Tests seed a quiescent empty queue near uint64_t wrap without a public reset API.
    friend struct detail::SpscRingTestAccess;
};

} // namespace lfq
