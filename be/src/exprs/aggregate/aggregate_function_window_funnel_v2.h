// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <algorithm>
#include <iterator>
#include <utility>
#include <vector>

#include "common/cast_set.h"
#include "common/exception.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/column/column_string.h"
#include "core/data_type/data_type_number.h"
#include "core/types.h"
#include "core/value/vdatetime_value.h"
#include "exprs/aggregate/aggregate_function.h"
#include "exprs/aggregate/aggregate_function_window_funnel.h" // for WindowFunnelMode, string_to_window_funnel_mode
#include "util/var_int.h"

namespace doris {
#include "common/compile_check_begin.h"
class Arena;
class BufferReadable;
class BufferWritable;
class IColumn;
} // namespace doris

namespace doris {

/// Merge two event lists, utilizing sorted flags to optimize.
/// After merge, all events are in `events_list` and it is sorted.
template <typename T>
void merge_events_list(T& events_list, size_t prefix_size, bool prefix_sorted, bool suffix_sorted) {
    if (!prefix_sorted && !suffix_sorted) {
        std::stable_sort(std::begin(events_list), std::end(events_list));
    } else {
        const auto begin = std::begin(events_list);
        const auto middle = std::next(begin, prefix_size);
        const auto end = std::end(events_list);

        if (!prefix_sorted) {
            std::stable_sort(begin, middle);
        }
        if (!suffix_sorted) {
            std::stable_sort(middle, end);
        }
        std::inplace_merge(begin, middle, end);
    }
}

/// V2 state: stores only matched events as (timestamp, event_index) pairs.
/// Compared to V1 which stores all rows with N boolean columns, V2 only stores
/// events that actually match at least one condition, dramatically reducing memory.
struct WindowFunnelStateV2 {
    /// (timestamp_int_val, 1-based event_index)
    /// event_index 0 is unused in normal modes.
    using TimestampEvent = std::pair<UInt64, UInt8>;

    int event_count = 0;
    int64_t window = 0;
    WindowFunnelMode window_funnel_mode = WindowFunnelMode::INVALID;
    bool sorted = true;
    std::vector<TimestampEvent> events_list;

    WindowFunnelStateV2() = default;
    WindowFunnelStateV2(int arg_event_count) : event_count(arg_event_count) {}

    void reset() {
        events_list.clear();
        sorted = true;
    }

    void add(const IColumn** arg_columns, ssize_t row_num, int64_t win, WindowFunnelMode mode) {
        window = win;
        window_funnel_mode = mode;

        // get_data() returns DateV2Value<DateTimeV2ValueType>; convert to packed UInt64
        auto timestamp = assert_cast<const ColumnVector<TYPE_DATETIMEV2>&>(*arg_columns[2])
                                 .get_data()[row_num]
                                 .to_date_int_val();

        // Iterate from last event to first (reverse order).
        // This ensures that after stable_sort, events with the same timestamp
        // appear in descending event_index order, which is important for correct
        // matching when one row satisfies multiple conditions.
        for (int i = event_count - 1; i >= 0; --i) {
            auto event_val =
                    assert_cast<const ColumnUInt8&>(*arg_columns[3 + i]).get_data()[row_num];
            if (event_val) {
                TimestampEvent new_event {timestamp, cast_set<UInt8>(i + 1)};
                if (sorted && !events_list.empty()) {
                    sorted = events_list.back() <= new_event;
                }
                events_list.emplace_back(new_event);
            }
        }
    }

    void sort() {
        if (!sorted) {
            std::stable_sort(std::begin(events_list), std::end(events_list));
            sorted = true;
        }
    }

    void merge(const WindowFunnelStateV2& other) {
        if (other.events_list.empty()) {
            return;
        }

        if (events_list.empty()) {
            events_list = other.events_list;
            sorted = other.sorted;
        } else {
            const auto prefix_size = events_list.size();
            events_list.insert(std::end(events_list), std::begin(other.events_list),
                               std::end(other.events_list));
            merge_events_list(events_list, prefix_size, sorted, other.sorted);
            sorted = true;
        }

        event_count = event_count > 0 ? event_count : other.event_count;
        window = window > 0 ? window : other.window;
        window_funnel_mode = window_funnel_mode == WindowFunnelMode::INVALID
                                     ? other.window_funnel_mode
                                     : window_funnel_mode;
    }

    void write(BufferWritable& out) const {
        write_var_int(event_count, out);
        write_var_int(window, out);
        write_var_int(static_cast<std::underlying_type_t<WindowFunnelMode>>(window_funnel_mode),
                      out);
        write_var_int(sorted ? 1 : 0, out);
        write_var_int(cast_set<Int64>(events_list.size()), out);
        for (const auto& [ts, idx] : events_list) {
            // Use fixed-size binary write for timestamp (8 bytes) and event_idx (1 byte).
            // This is more efficient than VarInt for these fixed-width values.
            out.write(reinterpret_cast<const char*>(&ts), sizeof(ts));
            out.write(reinterpret_cast<const char*>(&idx), sizeof(idx));
        }
    }

    void read(BufferReadable& in) {
        Int64 tmp = 0;
        read_var_int(tmp, in);
        event_count = cast_set<int>(tmp);

        read_var_int(window, in);

        read_var_int(tmp, in);
        window_funnel_mode = static_cast<WindowFunnelMode>(tmp);

        read_var_int(tmp, in);
        sorted = (tmp != 0);

        Int64 size = 0;
        read_var_int(size, in);
        events_list.clear();
        events_list.resize(size);
        for (Int64 i = 0; i < size; ++i) {
            in.read(reinterpret_cast<char*>(&events_list[i].first), sizeof(events_list[i].first));
            in.read(reinterpret_cast<char*>(&events_list[i].second), sizeof(events_list[i].second));
        }
    }

    using DateValueType = DateV2Value<DateTimeV2ValueType>;

    /// Reconstruct DateV2Value from packed UInt64.
    static DateValueType _ts_from_int(UInt64 packed) { return DateValueType(packed); }

    /// Check if `current_ts` is within `window` seconds of `base_ts`.
    /// Both are packed UInt64 from DateV2Value::to_date_int_val().
    bool _within_window(UInt64 base_ts, UInt64 current_ts) const {
        DateValueType end_ts = _ts_from_int(base_ts);
        TimeInterval interval(SECOND, window, false);
        end_ts.template date_add_interval<SECOND>(interval);
        return current_ts <= end_ts.to_date_int_val();
    }

    /// Track (first_timestamp, last_timestamp) for each event level in a chain.
    /// Uses packed UInt64 values; 0 means unset.
    struct TimestampPair {
        UInt64 first_ts = 0;
        UInt64 last_ts = 0;
        bool has_value() const { return first_ts != 0; }
        void reset() {
            first_ts = 0;
            last_ts = 0;
        }
    };

    int get() const {
        if (event_count == 0 || events_list.empty()) {
            return 0;
        }
        if (window < 0) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "the sliding time window must be a positive integer, but got: {}",
                            window);
        }

        switch (window_funnel_mode) {
        case WindowFunnelMode::DEFAULT:
            return _get_default(false);
        case WindowFunnelMode::INCREASE:
            return _get_default(true);
        case WindowFunnelMode::DEDUPLICATION:
            return _get_deduplication();
        case WindowFunnelMode::FIXED:
            return _get_fixed();
        default:
            throw Exception(ErrorCode::INTERNAL_ERROR, "Invalid window_funnel mode");
        }
    }

private:
    /// DEFAULT and INCREASE mode: O(N) single-pass algorithm.
    /// Uses events_timestamp array to track the (first, last) timestamps for each level.
    /// For each event in sorted order:
    ///   - If it's event 0, start a new potential chain
    ///   - If its predecessor level has been matched and within time window, extend the chain
    int _get_default(bool increase_mode) const {
        std::vector<TimestampPair> events_timestamp(event_count);

        for (const auto& [ts, raw_idx] : events_list) {
            int event_idx = raw_idx - 1;

            if (event_idx == 0) {
                events_timestamp[0] = {ts, ts};
            } else if (events_timestamp[event_idx - 1].has_value()) {
                bool matched = _within_window(events_timestamp[event_idx - 1].first_ts, ts);
                if (increase_mode) {
                    matched = matched && events_timestamp[event_idx - 1].last_ts < ts;
                }
                if (matched) {
                    events_timestamp[event_idx] = {events_timestamp[event_idx - 1].first_ts, ts};
                    if (event_idx + 1 == event_count) {
                        return event_count;
                    }
                }
            }
        }

        for (int event = event_count; event > 0; --event) {
            if (events_timestamp[event - 1].has_value()) {
                return event;
            }
        }
        return 0;
    }

    /// DEDUPLICATION mode: if a previously matched event level appears again,
    /// the current chain is terminated and max_level is updated.
    /// This preserves V1 semantics where duplicate events break the chain.
    int _get_deduplication() const {
        std::vector<TimestampPair> events_timestamp(event_count);
        int max_level = -1;
        int curr_level = -1;

        for (const auto& [ts, raw_idx] : events_list) {
            int event_idx = raw_idx - 1;

            if (event_idx == 0) {
                // Duplicate of event 0: terminate current chain first
                if (events_timestamp[0].has_value()) {
                    if (curr_level > max_level) {
                        max_level = curr_level;
                    }
                    _eliminate_chain(curr_level, events_timestamp);
                }
                // Start a new chain
                events_timestamp[0] = {ts, ts};
                curr_level = 0;
            } else if (events_timestamp[event_idx].has_value()) {
                // Duplicate event detected: this level was already matched
                if (curr_level > max_level) {
                    max_level = curr_level;
                }
                // Eliminate current chain
                _eliminate_chain(curr_level, events_timestamp);
            } else if (events_timestamp[event_idx - 1].has_value()) {
                if (_promote_level(events_timestamp, ts, event_idx, curr_level, false)) {
                    return event_count;
                }
            }
        }

        if (curr_level > max_level) {
            return curr_level + 1;
        }
        return max_level + 1;
    }

    /// FIXED mode (StarRocks-style semantics): if a matched event appears whose
    /// predecessor level has NOT been matched, the chain is broken (event level jumped).
    /// Note: V2 semantics differ from V1. V1 checks physical row adjacency;
    /// V2 checks event level continuity (unmatched rows don't break the chain).
    int _get_fixed() const {
        std::vector<TimestampPair> events_timestamp(event_count);
        int max_level = -1;
        int curr_level = -1;
        bool first_event = false;

        for (const auto& [ts, raw_idx] : events_list) {
            int event_idx = raw_idx - 1;

            if (event_idx == 0) {
                // Save current chain before starting a new one
                if (events_timestamp[0].has_value()) {
                    if (curr_level > max_level) {
                        max_level = curr_level;
                    }
                    _eliminate_chain(curr_level, events_timestamp);
                }
                events_timestamp[0] = {ts, ts};
                curr_level = 0;
                first_event = true;
            } else if (first_event && !events_timestamp[event_idx - 1].has_value()) {
                // Event level jumped: predecessor was not matched
                if (curr_level >= 0) {
                    if (curr_level > max_level) {
                        max_level = curr_level;
                    }
                    _eliminate_chain(curr_level, events_timestamp);
                }
            } else if (events_timestamp[event_idx - 1].has_value()) {
                if (_promote_level(events_timestamp, ts, event_idx, curr_level, false)) {
                    return event_count;
                }
            }
        }

        if (curr_level > max_level) {
            return curr_level + 1;
        }
        return max_level + 1;
    }

    /// Clear the current event chain back to the beginning.
    static void _eliminate_chain(int& curr_level, std::vector<TimestampPair>& events_timestamp) {
        for (; curr_level >= 0; --curr_level) {
            events_timestamp[curr_level].reset();
        }
    }

    /// Try to promote the chain to the next level.
    /// Returns true if we've matched all events (early termination).
    bool _promote_level(std::vector<TimestampPair>& events_timestamp, UInt64 ts, int event_idx,
                        int& curr_level, bool increase_mode) const {
        bool matched = _within_window(events_timestamp[event_idx - 1].first_ts, ts);
        if (increase_mode) {
            matched = matched && events_timestamp[event_idx - 1].last_ts < ts;
        }
        if (matched) {
            events_timestamp[event_idx] = {events_timestamp[event_idx - 1].first_ts, ts};
            if (event_idx > curr_level) {
                curr_level = event_idx;
            }
            if (event_idx + 1 == event_count) {
                return true;
            }
        }
        return false;
    }
};

class AggregateFunctionWindowFunnelV2
        : public IAggregateFunctionDataHelper<WindowFunnelStateV2, AggregateFunctionWindowFunnelV2>,
          MultiExpression,
          NullableAggregateFunction {
public:
    AggregateFunctionWindowFunnelV2(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<WindowFunnelStateV2, AggregateFunctionWindowFunnelV2>(
                      argument_types_) {}

    void create(AggregateDataPtr __restrict place) const override {
        new (place) WindowFunnelStateV2(
                cast_set<int>(IAggregateFunction::get_argument_types().size() - 3));
    }

    String get_name() const override { return "window_funnel_v2"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeInt32>(); }

    void reset(AggregateDataPtr __restrict place) const override { this->data(place).reset(); }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena&) const override {
        const auto& win = assert_cast<const ColumnInt64&>(*columns[0]).get_data()[row_num];
        StringRef mode = columns[1]->get_data_at(row_num);
        this->data(place).add(columns, row_num, win,
                              string_to_window_funnel_mode(mode.to_string()));
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena&) const override {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena&) const override {
        this->data(place).read(buf);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        this->data(const_cast<AggregateDataPtr>(place)).sort();
        assert_cast<ColumnInt32&>(to).get_data().push_back(
                IAggregateFunctionDataHelper<WindowFunnelStateV2,
                                             AggregateFunctionWindowFunnelV2>::data(place)
                        .get());
    }

protected:
    using IAggregateFunction::version;
};

} // namespace doris

#include "common/compile_check_end.h"
