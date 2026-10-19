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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>

#include "exprs/aggregate/aggregate_function.h"

namespace doris {

template <typename T>
struct FunctionStableTransfer;

// Published flag-plus-state String columns retain a separate codec, while every
// calculation delegates to V2. Keep the concrete function type so generic batch
// readers can inline create/decode/merge/destroy instead of making virtual calls
// for each state. Native readers retain their separate scratch capability.
template <typename Function>
class AggregateFunctionLegacyNullableState final
        : public IAggregateFunctionHelper<AggregateFunctionLegacyNullableState<Function>> {
    using Base = IAggregateFunctionHelper<AggregateFunctionLegacyNullableState<Function>>;

public:
    explicit AggregateFunctionLegacyNullableState(std::shared_ptr<Function> function)
            : Base(function->get_argument_types()), _function(std::move(function)) {}

    String get_name() const override { return _function->get_name(); }
    IAggregateFunction* transmit_to_stable() override {
        if (auto* stable = _function->transmit_to_stable()) {
            using StableFunction = typename FunctionStableTransfer<Function>::FunctionStable;
            return new AggregateFunctionLegacyNullableState<StableFunction>(
                    std::shared_ptr<StableFunction>(assert_cast<StableFunction*>(stable)));
        }
        return nullptr;
    }
    DataTypePtr get_return_type() const override { return _function->get_return_type(); }
    void set_version(int version_) override {
        Base::set_version(version_);
        _function->set_version(version_);
    }
    void set_query_context(QueryContext* ctx) override { _function->set_query_context(ctx); }
    bool is_blockable() const override { return _function->is_blockable(); }
    bool supported_incremental_mode() const override {
        return _function->supported_incremental_mode();
    }
    bool result_column_could_resize() const override {
        return _function->result_column_could_resize();
    }
    void execute_function_with_incremental(int64_t partition_start, int64_t partition_end,
                                           int64_t frame_start, int64_t frame_end,
                                           AggregateDataPtr place, const IColumn** columns,
                                           Arena& arena, bool previous_is_nul, bool end_is_nul,
                                           bool has_null, UInt8* use_null_result,
                                           UInt8* could_use_previous_result) const override {
        _function->execute_function_with_incremental(
                partition_start, partition_end, frame_start, frame_end, place, columns, arena,
                previous_is_nul, end_is_nul, has_null, use_null_result, could_use_previous_result);
    }
    void insert_result_into_range(ConstAggregateDataPtr place, IColumn& column, size_t start,
                                  size_t end) const override {
        _function->insert_result_into_range(place, column, start, end);
    }
    // Do not forward the native reader's scratch capability: this adapter reads String
    // columns through generic readers even when the nested function reads native fixed states.
    void create(AggregateDataPtr place) const override { _function->create(place); }
    void destroy(AggregateDataPtr place) const noexcept override { _function->destroy(place); }
    void reset(AggregateDataPtr place) const override { _function->reset(place); }
    bool is_trivial() const override { return _function->is_trivial(); }
    size_t size_of_data() const override { return _function->size_of_data(); }
    size_t align_of_data() const override { return _function->align_of_data(); }
    void check_input_columns_type(const IColumn** columns) const override {
        _function->check_input_columns_type(columns);
    }
    void check_result_column_type(const IColumn& column) const override {
        _function->check_result_column_type(column);
    }
    void add(AggregateDataPtr place, const IColumn** columns, ssize_t row,
             Arena& arena) const override {
        _function->add(place, columns, row, arena);
    }
    void add_batch(size_t rows, AggregateDataPtr* places, size_t offset, const IColumn** columns,
                   Arena& arena, bool agg_many) const override {
        _function->add_batch(rows, places, offset, columns, arena, agg_many);
    }
    void add_batch_selected(size_t rows, AggregateDataPtr* places, size_t offset,
                            const IColumn** columns, Arena& arena) const override {
        _function->add_batch_selected(rows, places, offset, columns, arena);
    }
    void add_batch_single_place(size_t rows, AggregateDataPtr place, const IColumn** columns,
                                Arena& arena) const override {
        _function->add_batch_single_place(rows, place, columns, arena);
    }
    void add_batch_range(size_t begin, size_t end, AggregateDataPtr place, const IColumn** columns,
                         Arena& arena, bool has_null) override {
        _function->add_batch_range(begin, end, place, columns, arena, has_null);
    }
    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena& arena) const override {
        _function->merge(place, rhs, arena);
    }
    void serialize(ConstAggregateDataPtr place, BufferWritable& buf) const override {
        _function->serialize(place, buf);
    }
    void deserialize(AggregateDataPtr place, BufferReadable& buf, Arena& arena) const override {
        _function->deserialize(place, buf, arena);
    }
    void deserialize_and_merge(AggregateDataPtr place, AggregateDataPtr rhs, BufferReadable& buf,
                               Arena& arena) const override {
        _function->deserialize(rhs, buf, arena);
        _function->merge(place, rhs, arena);
    }
    void insert_result_into(ConstAggregateDataPtr place, IColumn& column) const override {
        _function->insert_result_into(place, column);
    }

private:
    std::shared_ptr<Function> _function;
};

} // namespace doris
