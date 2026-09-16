#pragma once

#include <cstdint>
#include <queue>
#include <vector>

namespace tickpipe {

// Deterministic event ordering.
//
// Tie-break rule (documented contract):
//   1. timestamp_ns   — ascending
//   2. sequence       — ascending
//   3. rank           — ascending (book delta = 0, order activation = 1, trade = 2)
//   4. insertion counter — ascending; monotonic, assigned at push time.
//
// The first three keys are semantic; the insertion counter removes the last
// remaining ambiguity when two events share the same (timestamp, sequence,
// rank), so the total order is unique and reproducible across runs. The
// counter is internal to the queue and never exposed through pop().

struct Event {
    std::int64_t timestamp_ns{0};
    std::int64_t sequence{0};
    std::int32_t rank{0};
    std::uint64_t payload{0};
    std::uint64_t counter{0};
};

struct EventCompare {
    bool operator()(const Event& lhs, const Event& rhs) const noexcept {
        if (lhs.timestamp_ns != rhs.timestamp_ns) return lhs.timestamp_ns > rhs.timestamp_ns;
        if (lhs.sequence != rhs.sequence) return lhs.sequence > rhs.sequence;
        if (lhs.rank != rhs.rank) return lhs.rank > rhs.rank;
        return lhs.counter > rhs.counter;
    }
};

class EventQueue {
public:
    void push(Event event) {
        event.counter = next_counter_++;
        heap_.push(event);
    }

    bool empty() const noexcept { return heap_.empty(); }
    std::size_t size() const noexcept { return heap_.size(); }

    const Event& top() const { return heap_.top(); }
    void pop() { heap_.pop(); }

private:
    using Heap = std::priority_queue<Event, std::vector<Event>, EventCompare>;
    Heap heap_;
    std::uint64_t next_counter_{0};
};

}  // namespace tickpipe
