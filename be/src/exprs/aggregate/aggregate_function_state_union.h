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
#include <utility>

#include "core/column/column_nullable.h"
#include "core/custom_allocator.h"
#include "core/data_type/data_type_agg_state.h"
#include "exprs/aggregate/aggregate_function.h"

namespace doris {
const static std::string AGG_UNION_SUFFIX = "_union";

class AggregateStateUnion : public AggregateFunctionNonFinalBase,
                            public IAggregateFunctionHelper<AggregateStateUnion> {
public:
    AggregateStateUnion(AggregateFunctionPtr function, const DataTypes& argument_types_,
                        DataTypePtr return_type)
            : IAggregateFunctionHelper(argument_types_),
              _function(std::move(function)),
              _return_type(std::move(return_type)),
              _input_is_sql_nullable(argument_types_[0]->is_nullable() &&
                                     argument_types_[0]->get_primitive_type() == TYPE_AGG_STATE) {}
    ~AggregateStateUnion() override = default;

    static AggregateFunctionPtr create(AggregateFunctionPtr function,
                                       const DataTypes& argument_types_,
                                       const DataTypePtr& return_type) {
        CHECK(argument_types_.size() == 1);
        if (function == nullptr) {
            return nullptr;
        }
        return std::make_shared<AggregateStateUnion>(function, argument_types_, return_type);
    }

    void set_version(const int version_) override {
        IAggregateFunctionHelper::set_version(version_);
        _function->set_version(version_);
    }

    void create(AggregateDataPtr __restrict place) const override { _function->create(place); }

    String get_name() const override { return _function->get_name() + AGG_UNION_SUFFIX; }

    DataTypePtr get_return_type() const override { return _return_type; }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena& arena) const override {
        const IColumn* column = columns[0];
        if (_input_is_sql_nullable) {
            const auto* nullable = assert_cast<const ColumnNullable*>(column);
            // SQL NULL is not an empty aggregate state and has no bytes to decode.
            if (nullable->is_null_at(row_num)) {
                return;
            }
            column = &nullable->get_nested_column();
        }
        //the range is [begin, end]
        _function->deserialize_and_merge_from_column_range(place, *column, row_num, row_num, arena);
    }

    void add_batch_single_place(size_t batch_size, AggregateDataPtr place, const IColumn** columns,
                                Arena& arena) const override {
        if (batch_size == 0) {
            return;
        }
        if (_input_is_sql_nullable) {
            for (size_t row = 0; row < batch_size; ++row) {
                add(place, columns, row, arena);
            }
        } else {
            _function->deserialize_and_merge_from_column_range(place, *columns[0], 0,
                                                               batch_size - 1, arena);
        }
    }

    void add_batch(size_t batch_size, AggregateDataPtr* places, size_t place_offset,
                   const IColumn** columns, Arena& arena, bool agg_many) const override {
        add_batch_impl<false>(batch_size, places, place_offset, columns, arena);
    }

    void add_batch_selected(size_t batch_size, AggregateDataPtr* places, size_t place_offset,
                            const IColumn** columns, Arena& arena) const override {
        add_batch_impl<true>(batch_size, places, place_offset, columns, arena);
    }
    void reset(AggregateDataPtr place) const override { _function->reset(place); }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena& arena) const override {
        _function->merge(place, rhs, arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        _function->serialize(place, buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena& arena) const override {
        _function->deserialize(place, buf, arena);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        _function->serialize_without_key_to_column(place, to);
    }

    void destroy(AggregateDataPtr __restrict place) const noexcept override {
        _function->destroy(place);
    }

    bool is_trivial() const override { return false; }

    size_t size_of_data() const override { return _function->size_of_data(); }

    size_t align_of_data() const override { return _function->align_of_data(); }

private:
    template <bool selected>
    void add_batch_impl(size_t batch_size, AggregateDataPtr* places, size_t place_offset,
                        const IColumn** columns, Arena& arena) const {
        // Some batch implementations read the first serialized row before entering their loop.
        if (batch_size == 0) {
            return;
        }
        const IColumn* column = columns[0];
        DorisVector<AggregateDataPtr> non_null_places;
        if (_input_is_sql_nullable) {
            const auto* nullable = assert_cast<const ColumnNullable*>(column);
            column = &nullable->get_nested_column();
            non_null_places.assign(places, places + batch_size);
            for (size_t row = 0; row < batch_size; ++row) {
                if (nullable->is_null_at(row)) {
                    non_null_places[row] = nullptr;
                }
            }
        }
        if (selected || !non_null_places.empty()) {
            const auto* selected_places = non_null_places.empty() ? places : non_null_places.data();
            if (std::none_of(selected_places, selected_places + batch_size,
                             [](auto place) { return place != nullptr; })) {
                return;
            }
        }
        // Delegate the whole batch so native state columns (e.g. SUM) can merge directly,
        // instead of dispatching through the range interface for every input row.
        // Generic implementations need aligned scratch states and manage their construction,
        // destruction and exception cleanup. Release only this storage after the batch;
        // keep passing the caller's arena for allocations that merged states may retain.
        // The function can be shared by evaluators, so scratch must not be a mutable member.
        Arena scratch_arena;
        AggregateDataPtr scratch = nullptr;
        if (_function->needs_deserialize_and_merge_scratch()) {
            scratch = scratch_arena.aligned_alloc(batch_size * _function->size_of_data(),
                                                  _function->align_of_data());
        }
        if (!non_null_places.empty()) {
            _function->deserialize_and_merge_vec_selected(non_null_places.data(), place_offset,
                                                          scratch, column, arena, batch_size);
        } else if constexpr (selected) {
            _function->deserialize_and_merge_vec_selected(places, place_offset, scratch, column,
                                                          arena, batch_size);
        } else {
            _function->deserialize_and_merge_vec(places, place_offset, scratch, column, arena,
                                                 batch_size);
        }
    }

protected:
    AggregateFunctionPtr _function;
    DataTypePtr _return_type;
    // The logical input type distinguishes outer SQL NULL from a native nullable partial's
    // internal null map. Argument columns are materialized by AggFnEvaluator before add.
    bool _input_is_sql_nullable;
};

} // namespace doris
