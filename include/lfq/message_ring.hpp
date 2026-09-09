#pragma once

#include "lfq/ncq_index_ring.hpp"
#include "lfq/queue_result.hpp"

#include <array>
#include <cstddef>
#include <cstring>
#include <type_traits>

namespace lfq::detail {
struct MessageRingTestAccess;

// Shared engine for MPSC and SPMC. Each block has exactly one owner.
// Capacity includes blocks being copied; a stopped operation retains its block.
template <class T, std::size_t Capacity, std::size_t CacheLineBytes = 64>
class MessageRing {
    static_assert(std::is_trivially_copyable_v<T>, "T must be trivially copyable");
    using IndexRing = NcqIndexRing<Capacity, CacheLineBytes>;

public:
    PushResult try_push(const T& value) noexcept {
        const auto index = free_indices_.try_dequeue();
        if (!index) {
            return PushResult::no_capacity;
        }

        std::memcpy(static_cast<void*>(&blocks_[*index]), &value, sizeof(T));
        // Publishing releases the complete copy. Do not touch this block afterward.
        ready_indices_.enqueue_assuming_space(*index);
        return PushResult::success;
    }

    PopResult try_pop(T& out) noexcept {
        const auto index = ready_indices_.try_dequeue();
        if (!index) {
            return PopResult::empty;
        }

        // Head was already advanced. This consumer now exclusively owns the block.
        std::memcpy(static_cast<void*>(&out), &blocks_[*index], sizeof(T));
        // Return storage only after the read finishes; other blocks can circulate.
        free_indices_.enqueue_assuming_space(*index);
        return PopResult::success;
    }

private:
    IndexRing free_indices_{IndexRing::InitialState::full};
    IndexRing ready_indices_;
    // T objects are constructed with the queue, before concurrent access.
    std::array<T, Capacity> blocks_;
    friend struct MessageRingTestAccess;
};
} // namespace lfq::detail
