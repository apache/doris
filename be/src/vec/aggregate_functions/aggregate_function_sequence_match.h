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

// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/AggregateFunctionSequenceMatch.h
// and modified by Doris

#pragma once

#include <string.h>

#include <algorithm>
#include <bitset>
#include <boost/iterator/iterator_facade.hpp>
#include <cstdint>
#include <functional>
#include <iterator>
#include <memory>
#include <ostream>
#include <stack>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "common/logging.h"
#include "util/binary_cast.hpp"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/common/pod_array_fwd.h"
#include "vec/common/string_ref.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_number.h"
#include "vec/io/io_helper.h"

namespace doris {
namespace vectorized {
class Arena;
class BufferReadable;
class BufferWritable;
class IColumn;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

template <template <typename> class Comparator>
struct ComparePairFirst final {
    template <typename T1, typename T2>
    bool operator()(const std::pair<T1, T2>& lhs, const std::pair<T1, T2>& rhs) const {
        return Comparator<T1> {}(lhs.first, rhs.first);
    }
};

static constexpr size_t MAX_EVENTS = 32;

/// Max number of iterations to match the pattern against a sequence, exception thrown when exceeded
constexpr auto sequence_match_max_iterations = 1000000l;

template <typename DateValueType, typename NativeType, typename Derived>
struct AggregateFunctionSequenceMatchData final {
    using Timestamp = DateValueType;
    using Events = std::bitset<MAX_EVENTS>;
    using TimestampEvents = std::pair<Timestamp, Events>;
    using Comparator = ComparePairFirst<std::less>;

    AggregateFunctionSequenceMatchData() { reset(); }

public:
    const std::string get_pattern() const { return pattern; }

    size_t get_arg_count() const { return arg_count; }

    void init(const std::string pattern, size_t arg_count) {
        if (!init_flag) {
            this->pattern = pattern;
            this->arg_count = arg_count;
            parse_pattern();
            init_flag = true;
        }
    }

    void reset() {
        sorted = true;
        init_flag = false;
        pattern_has_time = false;
        pattern = "";
        arg_count = 0;
        conditions_met.reset();
        conditions_in_pattern.reset();

        events_list.clear();
        actions.clear();
        dfa_states.clear();
    }

    void add(const Timestamp& timestamp, const Events& events) {
        /// store information exclusively for rows with at least one event
        if (events.any()) {
            events_list.emplace_back(timestamp, events);
            sorted = false;
            conditions_met |= events;
        }
    }

    void merge(const AggregateFunctionSequenceMatchData& other) {
        if (other.events_list.empty()) return;

        events_list.insert(std::begin(other.events_list), std::end(other.events_list));
        sorted = false;
        conditions_met |= other.conditions_met;
    }

    void sort() {
        if (sorted) return;

        std::sort(std::begin(events_list), std::end(events_list), Comparator {});
        sorted = true;
    }

    void write(BufferWritable& buf) const {
        write_binary(sorted, buf);
        write_binary(events_list.size(), buf);

        for (const auto& events : events_list) {
            write_binary(events.first, buf);
            write_binary(events.second.to_ulong(), buf);
        }

        UInt32 conditions_met_value = conditions_met.to_ulong();
        write_binary(conditions_met_value, buf);

        write_binary(pattern, buf);
        write_binary(arg_count, buf);
    }

    void read(BufferReadable& buf) {
        read_binary(sorted, buf);

        size_t events_list_size;
        read_binary(events_list_size, buf);

        events_list.clear();
        events_list.reserve(events_list_size);

        for (size_t i = 0; i < events_list_size; ++i) {
            Timestamp timestamp;
            read_binary(timestamp, buf);

            UInt64 events;
            read_binary(events, buf);

            events_list.emplace_back(timestamp, Events {events});
        }

        UInt32 conditions_met_value;
        read_binary(conditions_met_value, buf);
        conditions_met = conditions_met_value;

        read_binary(pattern, buf);
        read_binary(arg_count, buf);
    }

private:
    enum class PatternActionType {
        SpecificEvent,
        AnyEvent,
        KleeneStar,
        TimeLessOrEqual,
        TimeLess,
        TimeGreaterOrEqual,
        TimeGreater,
        TimeEqual
    };

    struct PatternAction final {
        PatternActionType type;
        std::uint64_t extra;

        PatternAction() = default;
        explicit PatternAction(const PatternActionType type_, const std::uint64_t extra_ = 0)
                : type {type_}, extra {extra_} {}
    };

    using PatternActions = PODArrayWithStackMemory<PatternAction, 64>;

    Derived& derived() { return assert_cast<Derived&, TypeCheckOnRelease::DISABLE>(*this); }

    void parse_pattern() {
        actions.clear();
        actions.emplace_back(PatternActionType::KleeneStar);

        dfa_states.clear();
        dfa_states.emplace_back(true);

        pattern_has_time = false;

        const char* pos = pattern.data();
        const char* begin = pos;
        const char* end = pos + pattern.size();

        // Pattern is checked in fe, so pattern should be valid here, we check it and if pattern is invalid, we return.
        auto throw_exception = [&](const std::string& msg) {
            LOG(WARNING) << msg + " '" + std::string(pos, end) + "' at position " +
                                    std::to_string(pos - begin);
        };

        auto match = [&pos, end](const char* str) mutable {
            size_t length = strlen(str);
            if (pos + length <= end && 0 == memcmp(pos, str, length)) {
                pos += length;
                return true;
            }
            return false;
        };

        while (pos < end) {
            if (match("(?")) {
                if (match("t")) {
                    PatternActionType type;

                    if (match("<="))
                        type = PatternActionType::TimeLessOrEqual;
                    else if (match("<"))
                        type = PatternActionType::TimeLess;
                    else if (match(">="))
                        type = PatternActionType::TimeGreaterOrEqual;
                    else if (match(">"))
                        type = PatternActionType::TimeGreater;
                    else if (match("=="))
                        type = PatternActionType::TimeEqual;
                    else {
                        throw_exception("Unknown time condition");
                        return;
                    }

                    NativeType duration = 0;
                    const auto* prev_pos = pos;
                    pos = try_read_first_int_text(duration, pos, end);
                    if (pos == prev_pos) {
                        throw_exception("Could not parse number");
                        return;
                    }

                    if (actions.back().type != PatternActionType::SpecificEvent &&
                        actions.back().type != PatternActionType::AnyEvent &&
                        actions.back().type != PatternActionType::KleeneStar) {
                        throw_exception(
                                "Temporal condition should be preceded by an event condition");
                        return;
                    }

                    pattern_has_time = true;
                    actions.emplace_back(type, duration);
                } else {
                    UInt64 event_number = 0;
                    const auto* prev_pos = pos;
                    pos = try_read_first_int_text(event_number, pos, end);
                    if (pos == prev_pos) throw_exception("Could not parse number");

                    if (event_number > arg_count - 1) {
                        throw_exception("Event number " + std::to_string(event_number) +
                                        " is out of range");
                        return;
                    }

                    actions.emplace_back(PatternActionType::SpecificEvent, event_number - 1);
                    dfa_states.back().transition = DFATransition::SpecificEvent;
                    dfa_states.back().event = static_cast<uint32_t>(event_number - 1);
                    dfa_states.emplace_back();
                    conditions_in_pattern.set(event_number - 1);
                }

                if (!match(")")) {
                    throw_exception("Expected closing parenthesis, found");
                    return;
                }

            } else if (match(".*")) {
                actions.emplace_back(PatternActionType::KleeneStar);
                dfa_states.back().has_kleene = true;
            } else if (match(".")) {
                actions.emplace_back(PatternActionType::AnyEvent);
                dfa_states.back().transition = DFATransition::AnyEvent;
                dfa_states.emplace_back();
            } else {
                throw_exception("Could not parse pattern, unexpected starting symbol");
                return;
            }
        }
    }

public:
    /// Uses a DFA based approach in order to better handle patterns without
    /// time assertions.
    ///
    /// NOTE: This implementation relies on the assumption that the pattern is *small*.
    ///
    /// This algorithm performs in O(mn) (with m the number of DFA states and N the number
    /// of events) with a memory consumption and memory allocations in O(m). It means that
    /// if n >>> m (which is expected to be the case), this algorithm can be considered linear.
    template <typename EventEntry>
    bool dfa_match(EventEntry& events_it, const EventEntry events_end) const {
        using ActiveStates = std::vector<bool>;
        /// Those two vectors keep track of which states should be considered for the current
        /// event as well as the states which should be considered for the next event.
        ActiveStates active_states(dfa_states.size(), false);
        ActiveStates next_active_states(dfa_states.size(), false);
        active_states[0] = true;

        /// Keeps track of dead-ends in order not to iterate over all the events to realize that
        /// the match failed.
        size_t n_active = 1;

        for (/* empty */; events_it != events_end && n_active > 0 && !active_states.back();
             ++events_it) {
            n_active = 0;
            next_active_states.assign(dfa_states.size(), false);

            for (size_t state = 0; state < dfa_states.size(); ++state) {
                if (!active_states[state]) {
                    continue;
                }

                switch (dfa_states[state].transition) {
                case DFATransition::None:
                    break;
                case DFATransition::AnyEvent:
                    next_active_states[state + 1] = true;
                    ++n_active;
                    break;
                case DFATransition::SpecificEvent:
                    if (events_it->second.test(dfa_states[state].event)) {
                        next_active_states[state + 1] = true;
                        ++n_active;
                    }
                    break;
                }

                if (dfa_states[state].has_kleene) {
                    next_active_states[state] = true;
                    ++n_active;
                }
            }
            swap(active_states, next_active_states);
        }

        return active_states.back();
    }

    template <typename EventEntry>
    bool backtracking_match(EventEntry& events_it, const EventEntry events_end) const {
        const auto action_begin = std::begin(actions);
        const auto action_end = std::end(actions);
        auto action_it = action_begin;

        const auto events_begin = events_it;
        auto base_it = events_it;

        /// an iterator to action plus an iterator to row in events list plus timestamp at the start of sequence
        using backtrack_info = std::tuple<decltype(action_it), EventEntry, EventEntry>;
        std::stack<backtrack_info> back_stack;

        /// backtrack if possible
        const auto do_backtrack = [&] {
            while (!back_stack.empty()) {
                auto& top = back_stack.top();

                action_it = std::get<0>(top);
                events_it = std::next(std::get<1>(top));
                base_it = std::get<2>(top);

                back_stack.pop();

                if (events_it != events_end) return true;
            }

            return false;
        };

        size_t i = 0;
        while (action_it != action_end && events_it != events_end) {
            if (action_it->type == PatternActionType::SpecificEvent) {
                if (events_it->second.test(action_it->extra)) {
                    /// move to the next action and events
                    base_it = events_it;
                    ++action_it, ++events_it;
                } else if (!do_backtrack())
                    /// backtracking failed, bail out
                    break;
            } else if (action_it->type == PatternActionType::AnyEvent) {
                base_it = events_it;
                ++action_it, ++events_it;
            } else if (action_it->type == PatternActionType::KleeneStar) {
                back_stack.emplace(action_it, events_it, base_it);
                base_it = events_it;
                ++action_it;
            } else if (action_it->type == PatternActionType::TimeLessOrEqual) {
                if (events_it->first.second_diff(base_it->first) <= action_it->extra) {
                    /// condition satisfied, move onto next action
                    back_stack.emplace(action_it, events_it, base_it);
                    base_it = events_it;
                    ++action_it;
                } else if (!do_backtrack())
                    break;
            } else if (action_it->type == PatternActionType::TimeLess) {
                if (events_it->first.second_diff(base_it->first) < action_it->extra) {
                    back_stack.emplace(action_it, events_it, base_it);
                    base_it = events_it;
                    ++action_it;
                } else if (!do_backtrack())
                    break;
            } else if (action_it->type == PatternActionType::TimeGreaterOrEqual) {
                if (events_it->first.second_diff(base_it->first) >= action_it->extra) {
                    back_stack.emplace(action_it, events_it, base_it);
                    base_it = events_it;
                    ++action_it;
                } else if (++events_it == events_end && !do_backtrack())
                    break;
            } else if (action_it->type == PatternActionType::TimeGreater) {
                if (events_it->first.second_diff(base_it->first) > action_it->extra) {
                    back_stack.emplace(action_it, events_it, base_it);
                    base_it = events_it;
                    ++action_it;
                } else if (++events_it == events_end && !do_backtrack())
                    break;
            } else if (action_it->type == PatternActionType::TimeEqual) {
                if (events_it->first.second_diff(base_it->first) == action_it->extra) {
                    back_stack.emplace(action_it, events_it, base_it);
                    base_it = events_it;
                    ++action_it;
                } else if (++events_it == events_end && !do_backtrack())
                    break;
            } else {
                LOG(WARNING) << "Unknown PatternActionType";
                return false;
            }

            if (++i > sequence_match_max_iterations) {
                LOG(WARNING)
                        << "Pattern application proves too difficult, exceeding max iterations (" +
                                   std::to_string(sequence_match_max_iterations) + ")";
                return false;
            }
        }

        /// if there are some actions remaining
        if (action_it != action_end) {
            /// match multiple empty strings at end
            while (action_it->type == PatternActionType::KleeneStar ||
                   action_it->type == PatternActionType::TimeLessOrEqual ||
                   action_it->type == PatternActionType::TimeLess ||
                   (action_it->type == PatternActionType::TimeGreaterOrEqual &&
                    action_it->extra == 0))
                ++action_it;
        }

        if (events_it == events_begin) ++events_it;

        return action_it == action_end;
    }

    /// Splits the pattern into deterministic parts separated by non-deterministic fragments
    /// (time constraints and Kleene stars), and tries to match the deterministic parts in their specified order,
    /// ignoring the non-deterministic fragments.
    /// This function can quickly check that a full match is not possible if some deterministic fragment is missing.
    template <typename EventEntry>
    bool could_match_deterministic_parts(const EventEntry events_begin, const EventEntry events_end,
                                         bool limit_iterations = true) const {
        size_t events_processed = 0;
        auto events_it = events_begin;

        const auto actions_end = std::end(actions);
        auto actions_it = std::begin(actions);
        auto det_part_begin = actions_it;

        auto match_deterministic_part = [&events_it, events_end, &events_processed, det_part_begin,
                                         actions_it, limit_iterations]() {
            auto events_it_init = events_it;
            auto det_part_it = det_part_begin;

            while (det_part_it != actions_it && events_it != events_end) {
                /// matching any event
                if (det_part_it->type == PatternActionType::AnyEvent) ++events_it, ++det_part_it;

                /// matching specific event
                else {
                    if (events_it->second.test(det_part_it->extra)) ++events_it, ++det_part_it;

                    /// abandon current matching, try to match the deterministic fragment further in the list
                    else {
                        events_it = ++events_it_init;
                        det_part_it = det_part_begin;
                    }
                }

                if (limit_iterations && ++events_processed > sequence_match_max_iterations) {
                    LOG(WARNING) << "Pattern application proves too difficult, exceeding max "
                                    "iterations are " +
                                            std::to_string(sequence_match_max_iterations);
                    return false;
                }
            }

            return det_part_it == actions_it;
        };

        for (; actions_it != actions_end; ++actions_it)
            if (actions_it->type != PatternActionType::SpecificEvent &&
                actions_it->type != PatternActionType::AnyEvent) {
                if (!match_deterministic_part()) return false;
                det_part_begin = std::next(actions_it);
            }

        return match_deterministic_part();
    }

private:
    enum class DFATransition : char {
        ///   .-------.
        ///   |       |
        ///   `-------'
        None,
        ///   .-------.  (?[0-9])
        ///   |       | ----------
        ///   `-------'
        SpecificEvent,
        ///   .-------.      .
        ///   |       | ----------
        ///   `-------'
        AnyEvent,
    };

    struct DFAState {
        explicit DFAState(bool has_kleene_ = false)
                : has_kleene {has_kleene_}, event {0}, transition {DFATransition::None} {}

        ///   .-------.
        ///   |       | - - -
        ///   `-------'
        ///     |_^
        bool has_kleene;
        /// In the case of a state transitions with a `SpecificEvent`,
        /// `event` contains the value of the event.
        uint32_t event;
        /// The kind of transition out of this state.
        DFATransition transition;
    };

    using DFAStates = std::vector<DFAState>;

public:
    bool sorted = true;
    PODArrayWithStackMemory<TimestampEvents, 64> events_list;
    // sequenceMatch conditions met at least once in events_list
    std::bitset<MAX_EVENTS> conditions_met;
    // sequenceMatch conditions met at least once in the pattern
    std::bitset<MAX_EVENTS> conditions_in_pattern;
    // `True` if the parsed pattern contains time assertions (?t...), `false` otherwise.
    bool pattern_has_time;

private:
    std::string pattern;
    size_t arg_count;
    bool init_flag = false;

    PatternActions actions;
    DFAStates dfa_states;
};

template <typename DateValueType, typename NativeType, typename Derived>
class AggregateFunctionSequenceBase
        : public IAggregateFunctionDataHelper<
                  AggregateFunctionSequenceMatchData<DateValueType, NativeType, Derived>, Derived> {
public:
    AggregateFunctionSequenceBase(const DataTypes& arguments)
            : IAggregateFunctionDataHelper<
                      AggregateFunctionSequenceMatchData<DateValueType, NativeType, Derived>,
                      Derived>(arguments) {
        arg_count = arguments.size();
    }

    void reset(AggregateDataPtr __restrict place) const override { this->data(place).reset(); }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, const ssize_t row_num,
             Arena*) const override {
        std::string pattern =
                assert_cast<const ColumnString*, TypeCheckOnRelease::DISABLE>(columns[0])
                        ->get_data_at(0)
                        .to_string();
        this->data(place).init(pattern, arg_count);

        const auto& timestamp =
                assert_cast<const ColumnVector<NativeType>&, TypeCheckOnRelease::DISABLE>(
                        *columns[1])
                        .get_data()[row_num];
        typename AggregateFunctionSequenceMatchData<DateValueType, NativeType, Derived>::Events
                events;

        for (auto i = 2; i < arg_count; i++) {
            const auto event =
                    assert_cast<const ColumnUInt8*, TypeCheckOnRelease::DISABLE>(columns[i])
                            ->get_data()[row_num];
            events.set(i - 2, event);
        }

        this->data(place).add(binary_cast<NativeType, DateValueType>(timestamp), events);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena*) const override {
        const std::string pattern = this->data(rhs).get_pattern();
        size_t arg_count = this->data(rhs).get_arg_count();
        this->data(place).init(pattern, arg_count);
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena*) const override {
        this->data(place).read(buf);
        const std::string pattern = this->data(place).get_pattern();
        size_t arg_count = this->data(place).get_arg_count();
        this->data(place).init(pattern, arg_count);
    }

private:
    size_t arg_count;
};

template <typename DateValueType, typename NativeType>
class AggregateFunctionSequenceMatch final
        : public AggregateFunctionSequenceBase<
                  DateValueType, NativeType,
                  AggregateFunctionSequenceMatch<DateValueType, NativeType>> {
public:
    AggregateFunctionSequenceMatch(const DataTypes& arguments, const String& pattern_)
            : AggregateFunctionSequenceBase<
                      DateValueType, NativeType,
                      AggregateFunctionSequenceMatch<DateValueType, NativeType>>(arguments,
                                                                                 pattern_) {}

    using AggregateFunctionSequenceBase<DateValueType, NativeType,
                                        AggregateFunctionSequenceMatch<DateValueType, NativeType>>::
            AggregateFunctionSequenceBase;

    String get_name() const override { return "sequence_match"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeUInt8>(); }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        auto& output = assert_cast<ColumnUInt8&>(to).get_data();
        if (!this->data(place).conditions_in_pattern.any()) {
            output.push_back(false);
            return;
        }

        if ((this->data(place).conditions_in_pattern & this->data(place).conditions_met) !=
            this->data(place).conditions_in_pattern) {
            output.push_back(false);
            return;
        }
        this->data(const_cast<AggregateDataPtr>(place)).sort();

        const auto& data_ref = this->data(place);

        const auto events_begin = std::begin(data_ref.events_list);
        const auto events_end = std::end(data_ref.events_list);
        auto events_it = events_begin;

        bool match = (this->data(place).pattern_has_time
                              ? (this->data(place).could_match_deterministic_parts(events_begin,
                                                                                   events_end) &&
                                 this->data(place).backtracking_match(events_it, events_end))
                              : this->data(place).dfa_match(events_it, events_end));
        output.push_back(match);
    }
};

template <typename DateValueType, typename NativeType>
class AggregateFunctionSequenceCount final
        : public AggregateFunctionSequenceBase<
                  DateValueType, NativeType,
                  AggregateFunctionSequenceCount<DateValueType, NativeType>> {
public:
    AggregateFunctionSequenceCount(const DataTypes& arguments, const String& pattern_)
            : AggregateFunctionSequenceBase<
                      DateValueType, NativeType,
                      AggregateFunctionSequenceCount<DateValueType, NativeType>>(arguments,
                                                                                 pattern_) {}

    using AggregateFunctionSequenceBase<DateValueType, NativeType,
                                        AggregateFunctionSequenceCount<DateValueType, NativeType>>::
            AggregateFunctionSequenceBase;

    String get_name() const override { return "sequence_count"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeInt64>(); }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        auto& output = assert_cast<ColumnInt64&>(to).get_data();
        if (!this->data(place).conditions_in_pattern.any()) {
            output.push_back(0);
            return;
        }

        if ((this->data(place).conditions_in_pattern & this->data(place).conditions_met) !=
            this->data(place).conditions_in_pattern) {
            output.push_back(0);
            return;
        }
        this->data(const_cast<AggregateDataPtr>(place)).sort();
        output.push_back(count(place));
    }

private:
    UInt64 count(ConstAggregateDataPtr __restrict place) const {
        const auto& data_ref = this->data(place);

        const auto events_begin = std::begin(data_ref.events_list);
        const auto events_end = std::end(data_ref.events_list);
        auto events_it = events_begin;

        size_t count = 0;
        // check if there is a chance of matching the sequence at least once
        if (this->data(place).could_match_deterministic_parts(events_begin, events_end)) {
            while (events_it != events_end &&
                   this->data(place).backtracking_match(events_it, events_end))
                ++count;
        }

        return count;
    }
};

} // namespace doris::vectorized
