#pragma once

#include "lfq/message_ring.hpp"

namespace lfq {

// Multiple producers, exactly one consumer. At most Capacity participants,
// one outstanding operation per participant. Stop all callers before destruction.
template <class T, std::size_t Capacity, std::size_t CacheLineBytes = 64>
class MpscRing {
public:
    using value_type = T;
    using size_type = std::size_t;

    static constexpr size_type capacity() noexcept { return Capacity; }
    PushResult try_push(const T& value) noexcept { return queue_.try_push(value); }
    PopResult try_pop(T& out) noexcept { return queue_.try_pop(out); }

private:
    detail::MessageRing<T, Capacity, CacheLineBytes> queue_;
};
} // namespace lfq
