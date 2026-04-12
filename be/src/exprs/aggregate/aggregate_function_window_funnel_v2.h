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
///
/// To prevent same-row multi-condition chain advancement (where a single row
/// matching multiple conditions could incorrectly advance the funnel through
/// multiple levels), we use the high bit of event_idx as a "same-row continuation"
/// flag. When a row matches multiple conditions, the first event stored for that
/// row has bit 7 = 0, and subsequent events from the same row have bit 7 = 1.
/// The algorithm uses this to ensure each funnel step comes from a different row.
///
/// This approach adds ZERO storage overhead — each event remains 9 bytes (UInt64 + UInt8).
struct WindowFunnelStateV2 {
    /// (timestamp_int_val, 1-based event_index with continuation flag in bit 7)
    ///
    /// Bit layout of event_idx:
    ///   bit 7 (0x80): continuation flag — 1 means this event is from the same row
    ///                  as the preceding event in events_list
    ///   bits 0-6:     actual 1-based event index (supports up to 127 conditions)
    ///
    /// Sorted by timestamp only (via operator<). stable_sort preserves insertion order
    /// for equal timestamps, so same-row events remain consecutive after sorting.
    struct TimestampEvent {
        UInt64 timestamp;
        UInt8 event_idx; // includes continuation flag in bit 7

        /// Sort by timestamp only. For same timestamp, stable_sort preserves insertion
        /// order, keeping same-row events consecutive.
        bool operator<(const TimestampEvent& other) const { return timestamp < other.timestamp; }
        bool operator<=(const TimestampEvent& other) const { return timestamp <= other.timestamp; }
    };

    static constexpr UInt8 CONTINUATION_FLAG = 0x80;
    static constexpr UInt8 EVENT_IDX_MASK = 0x7F;
    static constexpr int MAX_EVENT_CONDITIONS = EVENT_IDX_MASK;

    static void validate_event_count(int count) {
        if (count < 0 || count > MAX_EVENT_CONDITIONS) {
            throw Exception(
                    ErrorCode::INVALID_ARGUMENT,
                    "window_funnel_v2 supports at most {} conditions because event indexes are "
                    "encoded in 7 bits, but got {}",
                    MAX_EVENT_CONDITIONS, count);
        }
    }

    /// Extract the actual 1-based event index (stripping continuation flag).
    static int get_event_idx(UInt8 raw) { return (raw & EVENT_IDX_MASK); }
    /// Check if this event is a continuation of the same row as the previous event.
    static bool is_continuation(UInt8 raw) { return (raw & CONTINUATION_FLAG) != 0; }

    static constexpr int64_t WINDOW_UNSET = -1;

    int event_count = 0;
    int64_t window = WINDOW_UNSET;
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
        //
        // The first event stored for this row has continuation=0;
        // subsequent events from the same row have continuation=1 (bit 7 set).
        bool first_match = true;
        for (int i = event_count - 1; i >= 0; --i) {
            auto event_val =
                    assert_cast<const ColumnUInt8&>(*arg_columns[3 + i]).get_data()[row_num];
            if (event_val) {
                UInt8 packed_idx = cast_set<UInt8>(i + 1);
                if (!first_match) {
                    packed_idx |= CONTINUATION_FLAG;
                }
                first_match = false;
                TimestampEvent new_event {timestamp, packed_idx};
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
            // Both stable_sort and inplace_merge preserve relative order of equal elements.
            // Since same-row events have the same timestamp (and thus compare equal in
            // the primary sort key), they remain consecutive after merge — preserving
            // the validity of continuation flags.
            merge_events_list(events_list, prefix_size, sorted, other.sorted);
            sorted = true;
        }

        event_count = event_count > 0 ? event_count : other.event_count;
        window = window != WINDOW_UNSET ? window : other.window;
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
        for (const auto& evt : events_list) {
            // Use fixed-size binary write for timestamp (8 bytes) and event_idx (1 byte).
            // The event_idx byte includes the continuation flag in bit 7.
            out.write(reinterpret_cast<const char*>(&evt.timestamp), sizeof(evt.timestamp));
            out.write(reinterpret_cast<const char*>(&evt.event_idx), sizeof(evt.event_idx));
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
            in.read(reinterpret_cast<char*>(&events_list[i].timestamp),
                    sizeof(events_list[i].timestamp));
            in.read(reinterpret_cast<char*>(&events_list[i].event_idx),
                    sizeof(events_list[i].event_idx));
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

    /// Track (first_timestamp, last_timestamp, last_event_list_idx) for each event level.
    /// Uses packed UInt64 values; 0 means unset for first_ts.
    /// last_list_idx tracks the position in events_list of the event that set this level,
    /// used to check continuation flag on subsequent events to detect same-row advancement.
    struct TimestampPair {
        UInt64 first_ts = 0;
        UInt64 last_ts = 0;
        size_t last_list_idx = 0;
        bool has_value() const { return first_ts != 0; }
        void reset() {
            first_ts = 0;
            last_ts = 0;
            last_list_idx = 0;
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
            return _get_default();
        case WindowFunnelMode::INCREASE:
            return _get_increase();
        case WindowFunnelMode::DEDUPLICATION:
            return _get_deduplication();
        case WindowFunnelMode::FIXED:
            return _get_fixed();
        default:
            throw Exception(ErrorCode::INTERNAL_ERROR, "Invalid window_funnel mode");
        }
    }

private:
    /// DEFAULT mode: O(N) single-pass algorithm.
    /// Uses events_timestamp array to track the (first, last) timestamps for each level.
    /// For each event in sorted order:
    ///   - If it's event 0, start a new potential chain
    ///   - If its predecessor level has been matched, within time window, AND from a
    ///     different row (checked via continuation flag), extend the chain
    ///
    /// In DEFAULT mode, unconditionally overwriting events_timestamp[0] when a new event-0
    /// appears is safe: timestamps are monotonically non-decreasing, higher levels retain
    /// the old chain's first_ts, and the <= window check still succeeds.
    int _get_default() const {
        std::vector<TimestampPair> events_timestamp(event_count);

        for (size_t i = 0; i < events_list.size(); ++i) {
            const auto& evt = events_list[i];
            int event_idx = get_event_idx(evt.event_idx) - 1;

            if (event_idx == 0) {
                events_timestamp[0] = {evt.timestamp, evt.timestamp, i};
            } else if (events_timestamp[event_idx - 1].has_value() &&
                       !_is_same_row(events_timestamp[event_idx - 1].last_list_idx, i)) {
                // Must be from a DIFFERENT row than the predecessor level
                if (_within_window(events_timestamp[event_idx - 1].first_ts, evt.timestamp)) {
                    events_timestamp[event_idx] = {events_timestamp[event_idx - 1].first_ts,
                                                   evt.timestamp, i};
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

    /// INCREASE mode: multi-pass algorithm matching V1 semantics.
    ///
    /// A single-pass approach cannot correctly handle INCREASE mode because when a new
    /// event-0 appears, the old chain and the new chain have different trade-offs:
    /// - The old chain has an earlier last_ts (better for the strict-increase check)
    /// - The new chain has a later first_ts (better for the time-window check)
    /// Neither dominates the other, so both must be tried independently.
    ///
    /// This method iterates over each event-0 occurrence as a potential chain start,
    /// then scans forward to build the longest matching chain from that start.
    /// The maximum chain length across all starts is returned.
    ///
    /// Complexity: O(M_event0 × N_matched) worst-case, where M_event0 is the count of
    /// event-0 occurrences and N_matched is the total matched-event count. In practice
    /// N_matched is much smaller than total rows (V2 only stores matched events), and
    /// the remaining-events pruning eliminates start points that cannot improve max_level,
    /// so the typical case is significantly better than worst-case.
    int _get_increase() const {
        int max_level = 0;
        const size_t list_size = events_list.size();

        for (size_t start = 0; start < list_size; ++start) {
            // Remaining-events pruning: from this start point, at most
            // (list_size - start) events remain. If that can't beat max_level, stop.
            // This also prunes all subsequent start points since they have even fewer.
            if (static_cast<int>(list_size - start) <= max_level) {
                break;
            }

            int start_event = get_event_idx(events_list[start].event_idx) - 1;
            if (start_event != 0) {
                continue;
            }

            // Try building a chain from this event-0
            std::vector<TimestampPair> events_timestamp(event_count);
            events_timestamp[0] = {events_list[start].timestamp, events_list[start].timestamp,
                                   start};
            int curr_level = 0;

            for (size_t i = start + 1; i < list_size; ++i) {
                const auto& evt = events_list[i];
                int event_idx = get_event_idx(evt.event_idx) - 1;

                if (event_idx == 0) {
                    // Another event-0: this chain's event-0 phase is done; skip
                    continue;
                }

                if (events_timestamp[event_idx - 1].has_value() &&
                    !_is_same_row(events_timestamp[event_idx - 1].last_list_idx, i)) {
                    bool matched =
                            _within_window(events_timestamp[event_idx - 1].first_ts, evt.timestamp);
                    matched = matched && events_timestamp[event_idx - 1].last_ts < evt.timestamp;
                    if (matched) {
                        events_timestamp[event_idx] = {events_timestamp[event_idx - 1].first_ts,
                                                       evt.timestamp, i};
                        curr_level = std::max(event_idx, curr_level);
                        if (event_idx + 1 == event_count) {
                            return event_count;
                        }
                    }
                }
            }

            max_level = std::max(curr_level + 1, max_level);
        }
        return max_level;
    }

    /// DEDUPLICATION mode (multi-pass, matching V1 semantics):
    ///
    /// V1 tries each E0-matching row as a chain starting point and searches for
    /// conditions one at a time. Before accepting a match for condition K, V1 checks
    /// the "gap" (rows between the previous match and the current match) for any
    /// re-occurrence of already-matched conditions 0..K-1. If found, the chain breaks.
    ///
    /// A single-pass approach cannot correctly implement this because V2's greedy
    /// multi-condition-per-row advancement creates premature higher-level matches
    /// that then conflict with later events. The multi-pass approach iterates over
    /// each event-0 occurrence, building chains one level at a time with gap-based
    /// duplicate detection.
    int _get_deduplication() const {
        int max_level = 0;
        const size_t list_size = events_list.size();

        for (size_t start = 0; start < list_size; ++start) {
            // Remaining-events pruning: if remaining events can't beat max_level, stop.
            if (static_cast<int>(list_size - start) <= max_level) {
                break;
            }

            int start_event = get_event_idx(events_list[start].event_idx) - 1;
            if (start_event != 0) {
                continue;
            }

            // Build a chain from this event-0
            UInt64 first_ts = events_list[start].timestamp;
            int curr_level = 0;
            size_t last_advance_idx = start;

            for (size_t i = start + 1; i < list_size; ++i) {
                int event_idx = get_event_idx(events_list[i].event_idx) - 1;

                // Only search for the next expected level (matches V1's sequential scan)
                if (event_idx != curr_level + 1) {
                    continue;
                }

                // Must be from a different row than the last chain event
                if (_is_same_row(last_advance_idx, i)) {
                    continue;
                }

                // Window check
                if (!_within_window(first_ts, events_list[i].timestamp)) {
                    break;
                }

                // Gap-based dedup check (V1 semantics):
                // Check if any event at a level below the target (0..event_idx-1)
                // fired in the gap between the last chain event's row and the
                // current event's row.
                size_t gap_start = _next_row_start(last_advance_idx + 1);
                size_t gap_end = _row_start(i);

                bool dup = false;
                if (gap_start < gap_end) {
                    for (size_t j = gap_start; j < gap_end; ++j) {
                        int j_level = get_event_idx(events_list[j].event_idx) - 1;
                        if (j_level < event_idx) {
                            dup = true;
                            break;
                        }
                    }
                }

                if (dup) {
                    break;
                }

                // Accept this match and advance the chain
                curr_level = event_idx;
                last_advance_idx = i;

                if (curr_level + 1 == event_count) {
                    return event_count;
                }
            }

            max_level = std::max(curr_level + 1, max_level);
        }
        return max_level;
    }

    /// FIXED mode (multi-pass, row-based).
    ///
    /// Row-by-row semantics similar to V1: after matching c_K at a row, the NEXT
    /// event-producing row must match c_{K+1} or the chain breaks.
    ///
    /// Difference from V1: rows matching no condition ("truly irrelevant") are
    /// absent from events_list and implicitly skipped. V1 treats them as
    /// non-matching and breaks the chain. This is the documented 4.1 behavior
    /// change: irrelevant events no longer break the chain.
    ///
    /// Like V1, tries every c1 event as a potential chain starting point and
    /// returns the maximum level reached across all attempts.
    ///
    /// Within each row, events are stored in descending condition-index order:
    /// [c_max(cont=0), ..., c_min(cont=N)]. The first (non-continuation) event
    /// carries the row's highest matched condition index. c1 (index 1 after add,
    /// 0 after -1) is always the last event of its row.
    int _get_fixed() const {
        int max_level = -1;

        for (size_t start_i = 0; start_i < events_list.size(); ++start_i) {
            // Only start from c1 events (condition 0 in 0-based)
            if (get_event_idx(events_list[start_i].event_idx) != 1) continue;

            UInt64 chain_start_ts = events_list[start_i].timestamp;
            int curr_level = 0;

            // c1 is always the last event of its row, so start_i+1 is
            // the first event of the next row (or end of list).
            size_t i = start_i + 1;

            while (i < events_list.size() && curr_level + 1 < event_count) {
                // Skip continuation events to reach the first event of the next row.
                i = _next_row_start(i);
                if (i >= events_list.size()) break;

                // Window check against the row's timestamp.
                if (!_within_window(chain_start_ts, events_list[i].timestamp)) break;

                int expected = curr_level + 1;
                int highest = get_event_idx(events_list[i].event_idx) - 1;

                if (highest < expected) {
                    // Row's highest matched condition is below expected → break.
                    break;
                }

                if (!_row_contains_level(i, expected)) {
                    // Row matches higher conditions but not the expected one
                    // (level jump) → break.
                    break;
                }

                curr_level = expected;

                // Advance past this row to the start of the next one.
                i = _next_row_start(i + 1);
            }

            if (curr_level > max_level) {
                max_level = curr_level;
                if (max_level + 1 == event_count) return event_count;
            }

            // Prune: remaining events cannot beat current best.
            if (static_cast<int>(events_list.size() - start_i) <= max_level) break;
        }

        return max_level + 1;
    }

    /// Advance past continuation events starting at `i`, returning the index of the
    /// next non-continuation event (i.e., the first event of the next row), or
    /// events_list.size() if no more rows remain.
    size_t _next_row_start(size_t i) const {
        while (i < events_list.size() && is_continuation(events_list[i].event_idx)) {
            ++i;
        }
        return i;
    }

    /// Walk backward from `i` to find the first event of the row containing `i`.
    /// The first event of a row has no continuation flag set.
    size_t _row_start(size_t i) const {
        while (i > 0 && is_continuation(events_list[i].event_idx)) {
            --i;
        }
        return i;
    }

    /// Check if the row whose first event is at `row_first` contains an event
    /// matching the given 0-based `target_level`. The first event of a row has the
    /// highest matched condition index; subsequent continuation events have lower indices.
    bool _row_contains_level(size_t row_first, int target_level) const {
        if (get_event_idx(events_list[row_first].event_idx) - 1 == target_level) {
            return true;
        }
        for (size_t j = row_first + 1;
             j < events_list.size() && is_continuation(events_list[j].event_idx); ++j) {
            if (get_event_idx(events_list[j].event_idx) - 1 == target_level) {
                return true;
            }
        }
        return false;
    }

    /// Check if the event at `current_idx` in events_list is from the same original row
    /// as the event at `prev_idx`. Same-row events are consecutive in the sorted list
    /// with continuation flags set. We walk backwards from current_idx checking if every
    /// event between prev_idx+1 and current_idx has the continuation flag set.
    bool _is_same_row(size_t prev_idx, size_t current_idx) const {
        if (current_idx <= prev_idx) {
            return false;
        }
        // Check that all events from prev_idx+1 to current_idx have the continuation flag.
        // If any of them doesn't, there's a row boundary in between.
        for (size_t j = prev_idx + 1; j <= current_idx; ++j) {
            if (!is_continuation(events_list[j].event_idx)) {
                return false;
            }
        }
        return true;
    }
};

class AggregateFunctionWindowFunnelV2
        : public IAggregateFunctionDataHelper<WindowFunnelStateV2, AggregateFunctionWindowFunnelV2>,
          MultiExpression,
          NullableAggregateFunction {
public:
    AggregateFunctionWindowFunnelV2(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<WindowFunnelStateV2, AggregateFunctionWindowFunnelV2>(
                      argument_types_) {
        WindowFunnelStateV2::validate_event_count(
                cast_set<int>(IAggregateFunction::get_argument_types().size() - 3));
    }

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
