#pragma once

#include <cstdint>

namespace tickpipe {

// Simulated execution model.
//
// Slippage: a fixed tick penalty per fill plus a per-share penalty.
//   slippage_ticks = fixed_ticks + (per_share_ticks * size_ticks) / tick_scale
// All arithmetic is integer; division truncates toward zero. Slippage always
// works against the order: buys pay trade_price + slippage, sells receive
// trade_price - slippage.
//
// Entry latency: an order submitted at time t is only eligible for trades
// with exchange_ts_ns >= t + order_entry_latency_ns.
//
// Partial fills: a fill is capped by the remaining traded size of the tick;
// an order may be filled across multiple ticks.

struct SlippageConfig {
    std::int64_t fixed_ticks{0};
    std::int64_t per_share_ticks{0};
};

struct Order {
    std::uint64_t id{0};
    std::int32_t symbol_id{0};
    std::int8_t side{1};  // 1 = buy, 2 = sell
    std::int64_t size_ticks{0};
    std::int64_t remaining_ticks{0};
    std::int64_t submitted_ts_ns{0};
    std::int64_t executable_ts_ns{0};
};

struct Fill {
    std::uint64_t order_id{0};
    std::int32_t symbol_id{0};
    std::int8_t side{1};
    std::int64_t size_ticks{0};
    std::int64_t tick_price_ticks{0};   // pre-slippage trade price
    std::int64_t fill_price_ticks{0};
    std::int64_t slippage_ticks{0};     // always non-negative magnitude
    std::int64_t trade_ts_ns{0};
};

inline std::int64_t compute_slippage_ticks(std::int64_t size_ticks,
                                           const SlippageConfig& config,
                                           std::int64_t tick_scale) {
    return config.fixed_ticks + (config.per_share_ticks * size_ticks) / tick_scale;
}

}  // namespace tickpipe
