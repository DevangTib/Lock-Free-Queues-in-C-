#include "lfq/mpsc_ring.hpp"
#include "lfq/spmc_ring.hpp"

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <thread>
#include <vector>

namespace {
void check(bool ok, const char* message) {
    if (!ok) {
        std::cerr << "FAIL: " << message << '\n';
        std::exit(1); // Also ends workers when a concurrency check fails.
    }
}
using lfq::PopResult;
using lfq::PushResult;
using Packet = std::array<std::byte, 500>;

Packet packet(std::uint64_t id) {
    Packet p{};
    for (std::size_t i = 0; i < p.size(); ++i) {
        p[i] = static_cast<std::byte>((id * 17 + i * 31 + (id >> 8)) & 255);
    }
    std::memcpy(p.data(), &id, sizeof(id));
    return p;
}
std::uint64_t packet_id(const Packet& p) {
    std::uint64_t id;
    std::memcpy(&id, p.data(), sizeof(id));
    check(p == packet(id), "torn or corrupted 500-byte message");
    return id;
}
} // namespace

namespace lfq::detail {
// Controlled ownership/metadata transitions simulate participants suspended at
// these boundaries, without adding callbacks or pause flags to production code.
struct NcqTestAccess {
    template <std::size_t N>
    static void publish_without_tail(NcqIndexRing<N>& q, std::uint64_t index) {
        const auto t = q.tail_.position.load();
        auto expected = q.entries_[t & (N - 1)].load();
        check(q.entries_[t & (N - 1)].compare_exchange_strong(expected, q.cycle_base(t) | index),
              "test publication failed");
    }
    template <std::size_t N>
    static bool claim_old_head(NcqIndexRing<N>& q, std::uint64_t old) {
        const auto next = old + 1;
        return q.head_.position.compare_exchange_strong(old, next);
    }
    template <std::size_t N>
    static void seed_empty(NcqIndexRing<N>& q, std::uint64_t position) {
        check(position % N == 0, "test seed must be cycle aligned");
        for (auto& entry : q.entries_) { entry.store(position - N); }
        q.head_.position.store(position);
        q.tail_.position.store(position);
    }
};
struct MessageRingTestAccess {
    template <class T, std::size_t N>
    static auto take_free(MessageRing<T, N>& q) { return q.free_indices_.try_dequeue(); }
    template <class T, std::size_t N>
    static auto take_ready(MessageRing<T, N>& q) { return q.ready_indices_.try_dequeue(); }
    template <class T, std::size_t N>
    static void finish_push(MessageRing<T, N>& q, std::uint64_t i, const T& value) {
        std::memcpy(static_cast<void*>(&q.blocks_[i]), &value, sizeof(T));
        q.ready_indices_.enqueue_assuming_space(i);
    }
    template <class T, std::size_t N>
    static void finish_pop(MessageRing<T, N>& q, std::uint64_t i, T& out) {
        std::memcpy(static_cast<void*>(&out), &q.blocks_[i], sizeof(T));
        q.free_indices_.enqueue_assuming_space(i);
    }
};
} // namespace lfq::detail

namespace {
void ncq_boundaries_and_helping() {
    using Ring = lfq::detail::NcqIndexRing<4>;
    Ring empty;
    check(!empty.try_dequeue(), "NCQ starts empty");
    for (int lap = 0; lap < 1000; ++lap) {
        for (std::uint64_t i = 0; i < 4; ++i) { empty.enqueue_assuming_space(i); }
        for (std::uint64_t i = 0; i < 4; ++i) {
            check(empty.try_dequeue() == i, "NCQ sequential FIFO");
        }
        check(!empty.try_dequeue(), "NCQ drain");
    }

    Ring full(Ring::InitialState::full);
    // A consumer's saved head=0 becomes stale while other consumers cycle the ring.
    for (std::uint64_t i = 0; i < 4; ++i) {
        check(full.try_dequeue() == i, "full initialization");
        full.enqueue_assuming_space(i);
    }
    check(!lfq::detail::NcqTestAccess::claim_old_head(full, 0), "stale consumer must lose CAS");

    Ring helped;
    lfq::detail::NcqTestAccess::publish_without_tail(helped, 2);
    check(helped.try_dequeue() == 2, "consumer must not wait for publisher's tail update");
    helped.enqueue_assuming_space(3); // Must help tail past the consumed publication.
    check(helped.try_dequeue() == 3, "producer tail helping");

    lfq::detail::NcqTestAccess::seed_empty(empty, (std::uint64_t{1} << 32) - 4);
    for (int lap = 0; lap < 8; ++lap) {
        for (std::uint64_t i = 0; i < 4; ++i) { empty.enqueue_assuming_space(i); }
        for (std::uint64_t i = 0; i < 4; ++i) {
            check(empty.try_dequeue() == i, "NCQ must work past 32-bit positions");
        }
    }
}

void ncq_mpmc() {
    using Ring = lfq::detail::NcqIndexRing<64>;
    Ring ring(Ring::InitialState::full);
    std::array<std::atomic<bool>, 64> owned{};
    std::array<std::uint64_t, 64> visits{};
    std::vector<std::thread> workers;
    for (int t = 0; t < 4; ++t) {
        workers.emplace_back([&] {
            for (int k = 0; k < 20000; ++k) {
                auto index = ring.try_dequeue();
                while (!index) { std::this_thread::yield(); index = ring.try_dequeue(); }
                check(*index < 64 && !owned[*index].exchange(true), "duplicate NCQ ownership");
                ++visits[*index]; // Ownership plus queue publication protects ordinary storage.
                owned[*index].store(false);
                ring.enqueue_assuming_space(*index);
            }
        });
    }
    for (auto& worker : workers) { worker.join(); }
    std::array<bool, 64> seen{};
    std::uint64_t total = 0;
    for (int i = 0; i < 64; ++i) {
        const auto index = ring.try_dequeue();
        check(index && *index < 64 && !seen[*index], "NCQ lost or duplicated index");
        seen[*index] = true;
        total += visits[*index];
    }
    check(total == 80000 && !ring.try_dequeue(), "NCQ conservation");
}

template <class Queue>
void boundaries() {
    Queue queue;
    Packet out = packet(999);
    check(queue.try_pop(out) == PopResult::empty && out == packet(999), "empty preserves output");
    for (int lap = 0; lap < 100; ++lap) {
        for (std::size_t i = 0; i < Queue::capacity(); ++i) {
            check(queue.try_push(packet(i)) == PushResult::success, "all message blocks usable");
        }
        const auto rejected = packet(987);
        check(queue.try_push(rejected) == PushResult::no_capacity && rejected == packet(987),
              "full rejects without changing input");
        for (std::size_t i = 0; i < Queue::capacity(); ++i) {
            check(queue.try_pop(out) == PopResult::success && packet_id(out) == i, "message FIFO");
        }
        const auto previous = out;
        check(queue.try_pop(out) == PopResult::empty && out == previous, "drained output preserved");
    }
}

void held_message_blocks() {
    using Access = lfq::detail::MessageRingTestAccess;
    lfq::detail::MessageRing<Packet, 4> queue;
    const auto held_producer = Access::take_free(queue);
    check(held_producer.has_value(), "reserve producer block");
    // Simulate a producer suspended during copying: only three blocks remain free.
    for (int i = 0; i < 3; ++i) { check(queue.try_push(packet(i)) == PushResult::success, "remaining capacity"); }
    check(queue.try_push(packet(3)) == PushResult::no_capacity, "in-flight copy consumes capacity");
    Packet out{};
    for (int i = 0; i < 3; ++i) {
        check(queue.try_pop(out) == PopResult::success && packet_id(out) == static_cast<unsigned>(i),
              "unpublished producer must not create FIFO hole");
    }
    Access::finish_push(queue, *held_producer, packet(99));
    const auto held_consumer = Access::take_ready(queue);
    check(held_consumer.has_value(), "claim consumer block before copying");
    std::thread worker([&] {
        for (int i = 0; i < 1000; ++i) {
            Packet received{};
            check(queue.try_push(packet(i)) == PushResult::success, "other blocks can circulate");
            check(queue.try_pop(received) == PopResult::success && packet_id(received) == static_cast<unsigned>(i),
                  "held consumer blocks only its own storage");
        }
    });
    worker.join();
    Access::finish_pop(queue, *held_consumer, out);
    check(packet_id(out) == 99, "held payload must never be overwritten");
    // Every block should be available again.
    for (int i = 0; i < 4; ++i) { check(queue.try_push(packet(i)) == PushResult::success, "pool restored"); }
    check(queue.try_push(packet(4)) == PushResult::no_capacity, "pool must not duplicate blocks");
}

template <std::size_t N>
void mpsc_transfer(bool drop_on_full) {
    lfq::MpscRing<Packet, N> queue;
    constexpr int producers = 3;
    constexpr std::uint64_t per_producer = 12000;
    std::array<std::vector<std::uint64_t>, producers> accepted;
    std::array<std::vector<std::uint64_t>, producers> received;
    std::atomic<int> finished{0};
    std::thread consumer([&] {
        // Recheck the queue after acquiring the final producer completion.
        for (;;) {
            const bool done = finished.load() == producers;
            Packet p{};
            if (queue.try_pop(p) == PopResult::success) {
                const auto id = packet_id(p);
                check(id < producers * per_producer, "MPSC invalid id");
                received[id / per_producer].push_back(id);
            } else if (done) {
                return;
            } else { std::this_thread::yield(); }
        }
    });
    std::vector<std::thread> workers;
    for (int t = 0; t < producers; ++t) {
        workers.emplace_back([&, t] {
            for (std::uint64_t k = 0; k < per_producer; ++k) {
                const auto id = t * per_producer + k;
                const auto p = packet(id);
                auto result = queue.try_push(p);
                while (!drop_on_full && result == PushResult::no_capacity) {
                    std::this_thread::yield(); result = queue.try_push(p);
                }
                if (result == PushResult::success) { accepted[t].push_back(id); }
            }
            finished.fetch_add(1);
        });
    }
    for (auto& worker : workers) { worker.join(); }
    consumer.join();
    for (int t = 0; t < producers; ++t) {
        check(accepted[t] == received[t], "MPSC accepted IDs and per-producer FIFO must match");
    }
}

template <std::size_t N>
void spmc_transfer() {
    lfq::SpmcRing<Packet, N> queue;
    constexpr std::uint64_t count = 36000;
    std::array<std::vector<std::uint64_t>, 3> received;
    std::atomic<bool> finished{false};
    std::vector<std::thread> consumers;
    for (int t = 0; t < 3; ++t) {
        consumers.emplace_back([&, t] {
            for (;;) {
                const bool done = finished.load();
                Packet p{};
                if (queue.try_pop(p) == PopResult::success) {
                    received[t].push_back(packet_id(p));
                } else if (done) { return; }
                else { std::this_thread::yield(); }
            }
        });
    }
    std::thread producer([&] {
        for (std::uint64_t id = 0; id < count; ++id) {
            const auto p = packet(id);
            while (queue.try_push(p) != PushResult::success) { std::this_thread::yield(); }
        }
        finished.store(true);
    });
    producer.join();
    for (auto& worker : consumers) { worker.join(); }
    std::vector<bool> seen(count, false);
    std::uint64_t total = 0;
    for (const auto& items : received) {
        for (std::size_t i = 0; i < items.size(); ++i) {
            const auto id = items[i];
            check(id < count && !seen[id], "SPMC exactly-once delivery");
            if (i) { check(items[i - 1] < id, "SPMC per-consumer removal order"); }
            seen[id] = true;
            ++total;
        }
    }
    check(total == count, "SPMC lost accepted messages");
}

struct alignas(128) Value {
    explicit Value(int v) : number(v) {}
    Value() : number(0) {}
    Value(const Value&) = default;
    Value& operator=(const Value&) = delete;
    int number;
};
template <class Queue>
void unusual_type() {
    Queue q;
    Value in(42), out(-1);
    check(q.try_push(in) == PushResult::success, "non-assignable payload");
    check(q.try_pop(out) == PopResult::success && out.number == 42, "non-assignable over-aligned payload");
}

template <class Queue>
void large_payload() {
    Queue queue;
    typename Queue::value_type in{}, out{};
    for (std::size_t i = 0; i < in.size(); ++i) { in[i] = static_cast<std::byte>(i & 255); }
    check(queue.try_push(in) == PushResult::success, "4-KiB message push");
    check(queue.try_pop(out) == PopResult::success && out == in, "4-KiB message copy");
}
} // namespace

int main() {
    // Guard internal retry loops as well as the outer test loops.
    std::jthread watchdog([](std::stop_token stop) {
        const auto end = std::chrono::steady_clock::now() + std::chrono::seconds(45);
        while (!stop.stop_requested()) {
            check(std::chrono::steady_clock::now() < end, "concurrency test timed out");
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    });
    check(std::atomic<std::uint64_t>::is_always_lock_free, "target needs lock-free uint64 atomics");
    ncq_boundaries_and_helping();
    ncq_mpmc();
    boundaries<lfq::MpscRing<Packet, 4>>();
    boundaries<lfq::SpmcRing<Packet, 4>>();
    unusual_type<lfq::MpscRing<Value, 4>>();
    unusual_type<lfq::SpmcRing<Value, 4>>();
    large_payload<lfq::MpscRing<std::array<std::byte, 4096>, 4>>();
    large_payload<lfq::SpmcRing<std::array<std::byte, 4096>, 4>>();
    held_message_blocks();
    mpsc_transfer<4>(false);
    mpsc_transfer<64>(false);
    mpsc_transfer<4>(true);
    spmc_transfer<4>();
    spmc_transfer<64>();
    std::cout << "All NCQ/MPSC/SPMC tests passed (MPMC indices, held blocks, large messages, FIFO, drops).\n";
}
