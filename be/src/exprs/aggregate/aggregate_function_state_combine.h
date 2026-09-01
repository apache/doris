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

#include <utility>

#include "exprs/aggregate/aggregate_function.h"

namespace doris {
const static std::string AGG_COMBINE_SUFFIX = "_combine";

class AggregateStateCombine final : public IAggregateFunctionHelper<AggregateStateCombine> {
public:
    AggregateStateCombine(AggregateFunctionPtr function, const DataTypes& argument_types_,
                          DataTypePtr return_type)
            : IAggregateFunctionHelper(argument_types_),
              _function(std::move(function)),
              _return_type(std::move(return_type)) {}

    static AggregateFunctionPtr create(AggregateFunctionPtr function,
                                       const DataTypes& argument_types_,
                                       const DataTypePtr& return_type) {
        if (function == nullptr) {
            return nullptr;
        }
        return std::make_shared<AggregateStateCombine>(function, argument_types_, return_type);
    }

    void set_version(const int version_) override {
        IAggregateFunctionHelper::set_version(version_);
        _function->set_version(version_);
    }

    void create(AggregateDataPtr __restrict place) const override { _function->create(place); }

    void destroy_vec(AggregateDataPtr __restrict place,
                     const size_t num_rows) const noexcept override {
        _function->destroy_vec(place, num_rows);
    }

    String get_name() const override { return _function->get_name() + AGG_COMBINE_SUFFIX; }

    DataTypePtr get_return_type() const override { return _return_type; }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena& arena) const override {
        _function->add(place, columns, row_num, arena);
    }

    void add_batch(size_t batch_size, AggregateDataPtr* places, size_t place_offset,
                   const IColumn** columns, Arena& arena, bool agg_many) const override {
        _function->add_batch(batch_size, places, place_offset, columns, arena, agg_many);
    }

    void add_batch_selected(size_t batch_size, AggregateDataPtr* places, size_t place_offset,
                            const IColumn** columns, Arena& arena) const override {
        _function->add_batch_selected(batch_size, places, place_offset, columns, arena);
    }

    void add_batch_single_place(size_t batch_size, AggregateDataPtr place, const IColumn** columns,
                                Arena& arena) const override {
        _function->add_batch_single_place(batch_size, place, columns, arena);
    }

    void add_batch_range(size_t batch_begin, size_t batch_end, AggregateDataPtr place,
                         const IColumn** columns, Arena& arena, bool has_null) override {
        _function->add_batch_range(batch_begin, batch_end, place, columns, arena, has_null);
    }

    void add_range_single_place(int64_t partition_start, int64_t partition_end, int64_t frame_start,
                                int64_t frame_end, AggregateDataPtr place, const IColumn** columns,
                                Arena& arena, UInt8* use_null_result,
                                UInt8* could_use_previous_result) const override {
        _function->add_range_single_place(partition_start, partition_end, frame_start, frame_end,
                                          place, columns, arena, use_null_result,
                                          could_use_previous_result);
    }

    void reset(AggregateDataPtr place) const override { _function->reset(place); }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena& arena) const override {
        _function->merge(place, rhs, arena);
    }

    void merge_vec(const AggregateDataPtr __restrict* __restrict places, size_t offset,
                   ConstAggregateDataPtr __restrict rhs, Arena& arena,
                   const size_t num_rows) const override {
        _function->merge_vec(places, offset, rhs, arena, num_rows);
    }

    void merge_vec_selected(const AggregateDataPtr __restrict* __restrict places, size_t offset,
                            ConstAggregateDataPtr __restrict rhs, Arena& arena,
                            const size_t num_rows) const override {
        _function->merge_vec_selected(places, offset, rhs, arena, num_rows);
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        _function->serialize(place, buf);
    }

    void serialize_vec(const std::vector<AggregateDataPtr>& places, size_t offset,
                       BufferWritable& buf, const size_t num_rows) const override {
        _function->serialize_vec(places, offset, buf, num_rows);
    }

    void serialize_to_column(const std::vector<AggregateDataPtr>& places, size_t offset,
                             MutableColumnPtr& dst, const size_t num_rows) const override {
        _function->serialize_to_column(places, offset, dst, num_rows);
    }

    void serialize_without_key_to_column(ConstAggregateDataPtr __restrict place,
                                         IColumn& to) const override {
        _function->serialize_without_key_to_column(place, to);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena& arena) const override {
        _function->deserialize(place, buf, arena);
    }

    void deserialize_vec(AggregateDataPtr places, const ColumnString* column, Arena& arena,
                         size_t num_rows) const override {
        _function->deserialize_vec(places, column, arena, num_rows);
    }

    void deserialize_and_merge_vec(const AggregateDataPtr* places, size_t offset,
                                   AggregateDataPtr rhs, const IColumn* column, Arena& arena,
                                   const size_t num_rows) const override {
        _function->deserialize_and_merge_vec(places, offset, rhs, column, arena, num_rows);
    }

    void deserialize_and_merge_vec_selected(const AggregateDataPtr* places, size_t offset,
                                            AggregateDataPtr rhs, const IColumn* column,
                                            Arena& arena, const size_t num_rows) const override {
        _function->deserialize_and_merge_vec_selected(places, offset, rhs, column, arena, num_rows);
    }

    void deserialize_and_merge(AggregateDataPtr __restrict place, AggregateDataPtr __restrict rhs,
                               BufferReadable& buf, Arena& arena) const override {
        _function->deserialize_and_merge(place, rhs, buf, arena);
    }

    void deserialize_and_merge_from_column_range(AggregateDataPtr __restrict place,
                                                 const IColumn& column, size_t begin, size_t end,
                                                 Arena& arena) const override {
        _function->deserialize_and_merge_from_column_range(place, column, begin, end, arena);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        if (to.empty()) {
            _function->serialize_without_key_to_column(place, to);
            DORIS_CHECK_EQ(to.size(), 1);
            return;
        }

        auto serialized_column = _function->create_serialize_column();
        _function->serialize_without_key_to_column(place, *serialized_column);
        DORIS_CHECK_EQ(serialized_column->size(), 1);
        to.insert_from(*serialized_column, 0);
    }

    void insert_result_into_vec(const std::vector<AggregateDataPtr>& places, const size_t offset,
                                IColumn& to, const size_t num_rows) const override {
        if (to.empty()) {
            auto mutable_to = to.assert_mutable();
            _function->serialize_to_column(places, offset, mutable_to, num_rows);
            DORIS_CHECK_EQ(to.size(), num_rows);
            return;
        }

        auto serialized_column = _function->create_serialize_column();
        _function->serialize_to_column(places, offset, serialized_column, num_rows);
        DORIS_CHECK_EQ(serialized_column->size(), num_rows);
        to.insert_range_from(*serialized_column, 0, num_rows);
    }

    void streaming_agg_serialize_to_column(const IColumn** columns, MutableColumnPtr& dst,
                                           const size_t num_rows, Arena& arena) const override {
        _function->streaming_agg_serialize_to_column(columns, dst, num_rows, arena);
    }

    void destroy(AggregateDataPtr __restrict place) const noexcept override {
        _function->destroy(place);
    }

    bool is_trivial() const override { return _function->is_trivial(); }

    size_t size_of_data() const override { return _function->size_of_data(); }

    size_t align_of_data() const override { return _function->align_of_data(); }

    void check_input_columns_type(const IColumn** columns) const override {
        _function->check_input_columns_type(columns);
    }

    MutableColumnPtr create_serialize_column() const override {
        return _function->create_serialize_column();
    }

    DataTypePtr get_serialized_type() const override { return _function->get_serialized_type(); }

    bool supported_incremental_mode() const override {
        return _function->supported_incremental_mode();
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

    void set_query_context(QueryContext* context) override {
        _function->set_query_context(context);
    }

    bool is_blockable() const override { return _function->is_blockable(); }

private:
    AggregateFunctionPtr _function;
    DataTypePtr _return_type;
};

} // namespace doris
