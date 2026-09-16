// C++17 event-driven replay engine for tickpipe backtests.
//
// The engine consumes a contiguous numpy structured array of tick records
// (zero-copy through the buffer protocol via py::array_t; no per-tick Python
// objects are allocated during replay) and steps an event-driven clock
// forward. Events are ordered by (timestamp_ns, sequence, rank, insertion
// counter) — see event_queue.hpp for the documented tie-break rule.
//
// speed: 1.0 replays at wall-clock rate, 0.0 replays as fast as possible.
// When no Python callbacks are registered the GIL is released for the whole
// run; when callbacks (trade/book/fill) are registered the run keeps the GIL
// (Python strategies need it), which also means wall-clock pacing sleeps
// while holding the GIL in that mode.

#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "event_queue.hpp"
#include "execution.hpp"

namespace py = pybind11;

namespace tickpipe {

enum class TickKind : std::int8_t { kBookDelta = 0, kTrade = 1 };

// Layout must stay in sync with TICK_RECORD_DTYPE in tickpipe/backtest/engine.py.
struct TickRecord {
    std::int8_t kind;
    std::int8_t side;  // 0 = none, 1 = bid, 2 = ask
    std::int64_t exchange_ts_ns;
    std::int64_t ingest_ts_ns;
    std::int64_t sequence;
    std::int64_t price_ticks;
    std::int64_t size_ticks;
    std::int32_t symbol_id;
    std::int64_t trade_id;
};
static_assert(sizeof(TickRecord) == 64,
              "TickRecord layout must stay in sync with TICK_RECORD_DTYPE");

constexpr std::int32_t kRankBookDelta = 0;
constexpr std::int32_t kRankOrderActivation = 1;
constexpr std::int32_t kRankTrade = 2;
constexpr std::int8_t kSideBuy = 1;
constexpr std::int8_t kSideSell = 2;

class ReplayEngine {
public:
    ReplayEngine(double speed, std::int64_t tick_scale, std::int64_t order_entry_latency_ns,
                 std::int64_t slippage_fixed_ticks, std::int64_t slippage_per_share_ticks)
        : speed_(speed),
          tick_scale_(tick_scale),
          latency_ns_(order_entry_latency_ns),
          slippage_{slippage_fixed_ticks, slippage_per_share_ticks},
          symbols_(py::none()),
          trade_ids_(py::none()),
          trade_cb_(py::none()),
          book_cb_(py::none()),
          fill_cb_(py::none()),
          bid_str_(py::str("bid")),
          ask_str_(py::str("ask")) {
        if (tick_scale <= 0) {
            throw std::invalid_argument("tick_scale must be positive");
        }
    }

    void set_ticks(py::array array) {
        py::buffer_info info = array.request();
        if (info.ndim != 1) {
            throw std::invalid_argument("ticks must be a 1-D array");
        }
        if (info.itemsize != sizeof(TickRecord)) {
            throw std::invalid_argument(
                "ticks dtype itemsize mismatch: expected " + std::to_string(sizeof(TickRecord)) +
                " bytes, got " + std::to_string(info.itemsize));
        }
        ticks_ = static_cast<const TickRecord*>(info.ptr);
        n_ticks_ = static_cast<std::size_t>(info.size);
        ticks_array_ = std::move(array);
    }

    void set_symbols(py::object symbols) { symbols_ = std::move(symbols); }
    void set_trade_ids(py::object trade_ids) { trade_ids_ = std::move(trade_ids); }
    void set_trade_callback(py::object callback) { trade_cb_ = std::move(callback); }
    void set_book_delta_callback(py::object callback) { book_cb_ = std::move(callback); }
    void set_fill_callback(py::object callback) { fill_cb_ = std::move(callback); }

    std::uint64_t submit_order(std::int32_t symbol_id, std::int8_t side,
                               std::int64_t size_ticks) {
        if (side != kSideBuy && side != kSideSell) {
            throw std::invalid_argument("side must be 1 (buy) or 2 (sell)");
        }
        if (size_ticks <= 0) {
            throw std::invalid_argument("size_ticks must be positive");
        }
        Order order;
        order.id = next_order_id_++;
        order.symbol_id = symbol_id;
        order.side = side;
        order.size_ticks = size_ticks;
        order.remaining_ticks = size_ticks;
        order.submitted_ts_ns = current_ts_ns_;
        order.executable_ts_ns = current_ts_ns_ + latency_ns_;
        orders_.push_back(order);
        Event activation;
        activation.timestamp_ns = order.executable_ts_ns;
        activation.sequence = static_cast<std::int64_t>(order.id);
        activation.rank = kRankOrderActivation;
        activation.payload = orders_.size() - 1;
        queue_.push(activation);
        return order.id;
    }

    std::vector<Fill> pop_fills() {
        std::vector<Fill> result = std::move(fills_);
        fills_.clear();
        return result;
    }

    std::int64_t current_ts_ns() const { return current_ts_ns_; }

    void run() {
        if (!has_callbacks()) {
            py::gil_scoped_release release;
            run_impl();
        } else {
            run_impl();
        }
    }

private:
    using Clock = std::chrono::steady_clock;

    bool has_callbacks() const {
        return !trade_cb_.is_none() || !book_cb_.is_none() || !fill_cb_.is_none();
    }

    void run_impl() {
        queue_ = EventQueue{};
        for (std::size_t i = 0; i < n_ticks_; ++i) {
            const TickRecord& tick = ticks_[i];
            Event event;
            event.timestamp_ns = tick.exchange_ts_ns;
            event.sequence = tick.sequence;
            event.rank =
                (tick.kind == static_cast<std::int8_t>(TickKind::kTrade)) ? kRankTrade
                                                                          : kRankBookDelta;
            event.payload = i;
            queue_.push(event);
        }
        if (!queue_.empty()) {
            first_ts_ns_ = queue_.top().timestamp_ns;
        }
        start_wall_ = Clock::now();
        while (!queue_.empty()) {
            Event event = queue_.top();
            queue_.pop();
            current_ts_ns_ = event.timestamp_ns;
            pace(event.timestamp_ns);
            if (event.rank == kRankOrderActivation) {
                continue;  // eligibility is derived from executable_ts_ns
            }
            const TickRecord& tick = ticks_[event.payload];
            if (tick.kind == static_cast<std::int8_t>(TickKind::kTrade)) {
                dispatch_trade(tick);
            } else {
                dispatch_book_delta(tick);
            }
        }
    }

    void pace(std::int64_t ts_ns) {
        if (speed_ <= 0.0 || first_ts_ns_ < 0) {
            return;
        }
        const double scaled = static_cast<double>(ts_ns - first_ts_ns_) / speed_;
        const auto target =
            start_wall_ + std::chrono::nanoseconds(static_cast<std::int64_t>(scaled));
        if (Clock::now() < target) {
            std::this_thread::sleep_until(target);
        }
    }

    void dispatch_trade(const TickRecord& tick) {
        if (!trade_cb_.is_none()) {
            trade_cb_(symbols_[py::int_(tick.symbol_id)], tick.exchange_ts_ns, tick.price_ticks,
                      tick.size_ticks, trade_ids_[py::int_(tick.trade_id - 1)]);
        }
        match_orders(tick);
    }

    void dispatch_book_delta(const TickRecord& tick) {
        if (!book_cb_.is_none()) {
            book_cb_(symbols_[py::int_(tick.symbol_id)], tick.exchange_ts_ns,
                     side_name(tick.side), tick.price_ticks, tick.size_ticks);
        }
    }

    void match_orders(const TickRecord& tick) {
        std::int64_t tick_remaining = tick.size_ticks;
        for (auto& order : orders_) {
            if (tick_remaining <= 0) {
                break;
            }
            if (order.remaining_ticks <= 0 || order.symbol_id != tick.symbol_id ||
                order.executable_ts_ns > tick.exchange_ts_ns) {
                continue;
            }
            const std::int64_t quantity = std::min(order.remaining_ticks, tick_remaining);
            const std::int64_t slippage = compute_slippage_ticks(quantity, slippage_, tick_scale_);
            Fill fill;
            fill.order_id = order.id;
            fill.symbol_id = order.symbol_id;
            fill.side = order.side;
            fill.size_ticks = quantity;
            fill.tick_price_ticks = tick.price_ticks;
            fill.fill_price_ticks = (order.side == kSideSell) ? tick.price_ticks - slippage
                                                              : tick.price_ticks + slippage;
            fill.slippage_ticks = slippage;
            fill.trade_ts_ns = tick.exchange_ts_ns;
            fills_.push_back(fill);
            order.remaining_ticks -= quantity;
            tick_remaining -= quantity;
            if (!fill_cb_.is_none()) {
                fill_cb_(fill.order_id, symbols_[py::int_(fill.symbol_id)], fill.side,
                         fill.size_ticks, fill.tick_price_ticks, fill.fill_price_ticks,
                         fill.slippage_ticks, fill.trade_ts_ns);
            }
        }
    }

    py::object side_name(std::int8_t side) const {
        if (side == 1) {
            return bid_str_;
        }
        if (side == 2) {
            return ask_str_;
        }
        return py::str("");
    }

    double speed_;
    std::int64_t tick_scale_;
    std::int64_t latency_ns_;
    SlippageConfig slippage_;
    EventQueue queue_;
    const TickRecord* ticks_{nullptr};
    std::size_t n_ticks_{0};
    py::array ticks_array_;
    py::object symbols_;
    py::object trade_ids_;
    py::object trade_cb_;
    py::object book_cb_;
    py::object fill_cb_;
    py::str bid_str_;
    py::str ask_str_;
    std::vector<Order> orders_;
    std::vector<Fill> fills_;
    std::uint64_t next_order_id_{1};
    std::int64_t current_ts_ns_{0};
    std::int64_t first_ts_ns_{-1};
    Clock::time_point start_wall_{};
};

}  // namespace tickpipe

PYBIND11_MODULE(_replay, m) {
    using namespace tickpipe;

    m.doc() = "C++17 event-driven replay engine (tickpipe backtest core).";
    m.attr("TICK_RECORD_ITEMSIZE") = static_cast<std::int64_t>(sizeof(TickRecord));

    py::class_<EventQueue>(m, "EventQueue")
        .def(py::init<>())
        .def(
            "push",
            [](EventQueue& queue, std::int64_t timestamp_ns, std::int64_t sequence,
               std::int32_t rank) { queue.push({timestamp_ns, sequence, rank, 0, 0}); },
            py::arg("timestamp_ns"), py::arg("sequence"), py::arg("rank"))
        .def("pop", [](EventQueue& queue) {
            if (queue.empty()) {
                throw std::out_of_range("pop from empty EventQueue");
            }
            const Event& event = queue.top();
            py::object result = py::make_tuple(event.timestamp_ns, event.sequence, event.rank);
            queue.pop();
            return result;
        })
        .def("empty", &EventQueue::empty)
        .def("__len__", &EventQueue::size);

    py::class_<Fill>(m, "Fill")
        .def_readonly("order_id", &Fill::order_id)
        .def_readonly("symbol_id", &Fill::symbol_id)
        .def_readonly("side", &Fill::side)
        .def_readonly("size_ticks", &Fill::size_ticks)
        .def_readonly("tick_price_ticks", &Fill::tick_price_ticks)
        .def_readonly("fill_price_ticks", &Fill::fill_price_ticks)
        .def_readonly("slippage_ticks", &Fill::slippage_ticks)
        .def_readonly("trade_ts_ns", &Fill::trade_ts_ns);

    py::class_<ReplayEngine>(m, "ReplayEngine")
        .def(py::init<double, std::int64_t, std::int64_t, std::int64_t, std::int64_t>(),
             py::arg("speed") = 0.0, py::arg("tick_scale") = 1'000'000'000,
             py::arg("order_entry_latency_ns") = 0, py::arg("slippage_fixed_ticks") = 0,
             py::arg("slippage_per_share_ticks") = 0)
        .def("set_ticks", &ReplayEngine::set_ticks, py::keep_alive<1, 2>())
        .def("set_symbols", &ReplayEngine::set_symbols)
        .def("set_trade_ids", &ReplayEngine::set_trade_ids)
        .def("set_trade_callback", &ReplayEngine::set_trade_callback)
        .def("set_book_delta_callback", &ReplayEngine::set_book_delta_callback)
        .def("set_fill_callback", &ReplayEngine::set_fill_callback)
        .def("submit_order", &ReplayEngine::submit_order, py::arg("symbol_id"),
             py::arg("side"), py::arg("size_ticks"))
        .def("pop_fills", &ReplayEngine::pop_fills)
        .def("current_ts_ns", &ReplayEngine::current_ts_ns)
        .def("run", &ReplayEngine::run);
}
