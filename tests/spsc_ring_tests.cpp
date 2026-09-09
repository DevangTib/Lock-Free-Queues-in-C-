#include "lfq/spsc_ring.hpp"

#include <chrono>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <thread>

namespace lfq::detail {
struct SpscRingTestAccess {
    template <class T, std::size_t N, std::size_t Alignment>
    static void seed_empty(SpscRing<T, N, Alignment>& queue, std::uint64_t position) {
        queue.write_.position.store(position, std::memory_order_relaxed);
        queue.read_.position.store(position, std::memory_order_relaxed);
    }
};
} // namespace lfq::detail

namespace {
using lfq::PopResult;
using lfq::PushResult;

void require(bool condition, const char* message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

void boundaries_and_wrap() {
    lfq::SpscRing<std::uint64_t, 4> queue;
    static_assert(decltype(queue)::capacity() == 4);
    static_assert(!std::is_copy_constructible_v<decltype(queue)>);
    static_assert(!std::is_move_constructible_v<decltype(queue)>);
    static_assert(noexcept(queue.try_push(std::uint64_t{})));
    std::uint64_t out = 987;
    require(queue.try_pop(out) == PopResult::empty && out == 987, "empty must preserve output");

    // Run the same capacity/FIFO checks across both physical and counter wrap.
    for (const auto start : {std::uint64_t{0}, std::numeric_limits<std::uint64_t>::max() - 1}) {
        lfq::detail::SpscRingTestAccess::seed_empty(queue, start);
        for (std::uint64_t lap = 0; lap < 1000; ++lap) {
            for (std::uint64_t i = 0; i < 4; ++i) {
                require(queue.try_push(lap * 4 + i) == PushResult::success, "all slots must be usable");
            }
            const std::uint64_t rejected = 12345;
            require(queue.try_push(rejected) == PushResult::no_capacity, "full must reject");
            require(rejected == 12345, "push must preserve input");
            for (std::uint64_t i = 0; i < 4; ++i) {
                require(queue.try_pop(out) == PopResult::success && out == lap * 4 + i,
                        "FIFO or wrap failure");
            }
            const auto previous = out;
            require(queue.try_pop(out) == PopResult::empty && out == previous, "drain must preserve output");
        }
    }
}

struct alignas(128) NonAssignable {
    explicit NonAssignable(int v) : value(v) {}
    NonAssignable() : value(0) {}
    NonAssignable(const NonAssignable&) = default;
    NonAssignable& operator=(const NonAssignable&) = delete;
    int value;
};
static_assert(std::is_trivially_copyable_v<NonAssignable>);

void general_payload() {
    lfq::SpscRing<NonAssignable, 2> queue;
    NonAssignable input(42), output(-1);
    require(queue.try_push(input) == PushResult::success, "non-assignable push");
    require(queue.try_pop(output) == PopResult::success && output.value == 42,
            "over-aligned/non-assignable copy");
    lfq::SpscRing<std::uint64_t, 2> words;
    for (const auto value : {std::uint64_t{0}, std::numeric_limits<std::uint64_t>::max()}) {
        std::uint64_t result = 0;
        require(words.try_push(value) == PushResult::success, "edge payload push");
        require(words.try_pop(result) == PopResult::success && result == value, "edge payload copy");
    }
}

using Message = std::array<std::byte, 500>;
static_assert(sizeof(Message) == 500);

Message message(std::uint64_t sequence) {
    Message result{};
    for (std::size_t i = 0; i < result.size(); ++i) {
        result[i] = static_cast<std::byte>((sequence * 17 + i * 31 + (sequence >> 8)) & 255);
    }
    // Preserve the full sequence too, so pattern repetition cannot hide loss/reordering.
    std::memcpy(result.data(), &sequence, sizeof(sequence));
    return result;
}

template <std::size_t N>
void concurrent_transfer() {
    lfq::SpscRing<Message, N> queue;
    constexpr std::uint64_t count = 100000;
    std::atomic<bool> start{false};
    std::atomic<bool> failed{false};
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
    auto timed_out = [&] {
        if (std::chrono::steady_clock::now() > deadline) {
            failed.store(true);
        }
        return failed.load();
    };

    std::thread producer([&] {
        while (!start.load(std::memory_order_acquire)) { std::this_thread::yield(); }
        for (std::uint64_t i = 0; i < count && !failed.load(); ++i) {
            const auto value = message(i);
            while (queue.try_push(value) != PushResult::success) {
                if (timed_out()) { return; }
                std::this_thread::yield();
            }
            if ((i & 1023) == 0) { std::this_thread::yield(); }
        }
    });
    std::thread consumer([&] {
        while (!start.load(std::memory_order_acquire)) { std::this_thread::yield(); }
        for (std::uint64_t i = 0; i < count && !failed.load(); ++i) {
            Message value{};
            while (queue.try_pop(value) != PopResult::success) {
                if (timed_out()) { return; }
                std::this_thread::yield();
            }
            if (value != message(i)) { failed.store(true); return; }
            if ((i & 255) == 0) { std::this_thread::yield(); }
        }
    });
    start.store(true, std::memory_order_release);
    producer.join();
    consumer.join();
    require(!failed.load(), "concurrent transfer corrupted, reordered, lost data, or timed out");
    Message out{};
    require(queue.try_pop(out) == PopResult::empty, "concurrent drain left extra messages");
}
} // namespace

int main() {
    try {
        require(std::atomic<std::uint64_t>::is_always_lock_free,
                "target must support always-lock-free uint64_t atomics");
        boundaries_and_wrap();
        general_payload();
        concurrent_transfer<2>();
        concurrent_transfer<64>();
        std::cout << "All SPSC tests passed (including 200000 concurrent 500-byte transfers).\n";
    } catch (const std::exception& error) {
        std::cerr << "FAIL: " << error.what() << '\n';
        return 1;
    }
}
