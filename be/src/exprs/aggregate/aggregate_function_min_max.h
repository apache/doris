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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/AggregateFunctions/AggregateFunctionMinMaxAny.h
// and modified by Doris

#pragma once

#include <fmt/format.h>
#include <glog/logging.h>
#include <string.h>

#include <memory>
#include <string>
#include <type_traits>
#include <vector>

#include "common/cast_set.h"
#include "common/compare.h"
#include "common/logging.h"
#include "core/assert_cast.h"
#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/column/column_decimal.h"
#include "core/column/column_fixed_length_object.h"
#include "core/column/column_string.h"
#include "core/custom_allocator.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_fixed_length_object.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/primitive_type.h"
#include "core/string_buffer.hpp"
#include "core/string_ref.h"
#include "core/type_limit.h"
#include "core/types.h"
#include "exprs/aggregate/aggregate_function.h"
#include "exprs/aggregate/single_value_data.h"

namespace doris {
class Arena;
template <PrimitiveType T>
class ColumnDecimal;
template <PrimitiveType T>
class ColumnVector;
} // namespace doris

namespace doris {

template <typename Data>
struct AggregateFunctionMaxData : public Data {
    using Self = AggregateFunctionMaxData;
    using Data::UsesFixedLengthStateSerialization;
    constexpr static bool IS_ANY = false;

    AggregateFunctionMaxData() { reset(); }

    void change_if_better(const IColumn& column, size_t row_num, Arena& arena) {
        this->set_if_greater(column, row_num, arena);
    }

    void change_if_better(const Self& to, Arena& arena) { this->set_if_greater(to, arena); }

    void reset() {
        if constexpr (Data::UsesFixedLengthStateSerialization) {
            this->set_value_to_min();
        }
        Data::reset();
    }

    static const char* name() { return "max"; }
};

template <typename Data>
struct AggregateFunctionMinData : Data {
    using Self = AggregateFunctionMinData;
    using Data::UsesFixedLengthStateSerialization;
    constexpr static bool IS_ANY = false;

    AggregateFunctionMinData() { reset(); }

    void change_if_better(const IColumn& column, size_t row_num, Arena& arena) {
        this->set_if_smaller(column, row_num, arena);
    }
    void change_if_better(const Self& to, Arena& arena) { this->set_if_smaller(to, arena); }

    void reset() {
        if constexpr (Data::UsesFixedLengthStateSerialization) {
            this->set_value_to_max();
        }
        Data::reset();
    }

    static const char* name() { return "min"; }
};

// this is used for plain type about any_value function
template <typename Data>
struct AggregateFunctionAnyData : Data {
    using Self = AggregateFunctionAnyData;
    using Data::UsesFixedLengthStateSerialization;
    static const char* name() { return "any"; }
    constexpr static bool IS_ANY = true;

    AggregateFunctionAnyData() {};

    void change_if_better(const IColumn& column, size_t row_num, Arena& arena) {
        if (UNLIKELY(!this->has())) {
            this->set(column, row_num, arena);
        }
    }

    void change_if_better(const Self& to, Arena& arena) {
        if (UNLIKELY(!this->has() && to.has())) {
            this->set(to, arena);
        }
    }
};

template <typename Data>
class AggregateFunctionsSingleValue final
        : public IAggregateFunctionDataHelper<Data, AggregateFunctionsSingleValue<Data>> {
private:
    const DataTypePtr& _data_type;
    using Base = IAggregateFunctionDataHelper<Data, AggregateFunctionsSingleValue<Data>>;
    using IAggregateFunction::argument_types;

public:
    AggregateFunctionsSingleValue(const DataTypes& arguments)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionsSingleValue<Data>>(arguments),
              _data_type(this->argument_types[0]) {}

    void create(AggregateDataPtr __restrict place) const override { new (place) Data; }

    String get_name() const override { return Data::name(); }

    DataTypePtr get_return_type() const override { return _data_type; }

    bool is_trivial() const override {
        return Data::UsesFixedLengthStateSerialization && Data::IS_ANY;
    }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena& arena) const override {
        this->data(place).change_if_better(*columns[0], row_num, arena);
    }

    void add_batch_single_place(size_t batch_size, AggregateDataPtr place, const IColumn** columns,
                                Arena& arena) const override {
        if constexpr (Data::IS_ANY) {
            DCHECK_GT(batch_size, 0);
            this->data(place).change_if_better(*columns[0], 0, arena);
        } else {
            Base::add_batch_single_place(batch_size, place, columns, arena);
        }
    }

    void reset(AggregateDataPtr place) const override { this->data(place).reset(); }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena& arena) const override {
        this->data(place).change_if_better(this->data(rhs), arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        this->data(place).write(buf, _data_type, IAggregateFunction::version);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena& arena) const override {
        this->data(place).read(buf, _data_type, IAggregateFunction::version, arena);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        this->data(place).insert_result_into(to);
    }

    void check_input_columns_type(const IColumn** columns) const override {
        IAggregateFunction::check_input_columns_type(columns);
        if constexpr (Data::NeedCheckColumnType) {
            this->template check_argument_column_type<typename Data::ColVecType>(columns[0]);
        }
    }

    void check_result_column_type(const IColumn& to) const override {
        IAggregateFunction::check_result_column_type(to);
        if constexpr (Data::NeedCheckColumnType) {
            this->template check_result_column_type_as<typename Data::ColVecType>(to);
        }
    }

    void insert_result_into_repeat(ConstAggregateDataPtr place, uint64_t, IColumn& to,
                                   Arena&) const override {
        this->data(place).insert_result_into(to);
    }

    void insert_result_into_repeat_vec(const std::vector<AggregateDataPtr>& places,
                                       const size_t offset, const std::vector<uint64_t>&,
                                       IColumn& to, const size_t num_rows, Arena&) const override {
        for (size_t i = 0; i != num_rows; ++i) {
            this->data(places[i] + offset).insert_result_into(to);
        }
    }

    void serialize_to_column(const std::vector<AggregateDataPtr>& places, size_t offset,
                             MutableColumnPtr& dst, const size_t num_rows) const override {
        if constexpr (Data::UsesFixedLengthStateSerialization) {
            auto& dst_column = assert_cast<ColumnFixedLengthObject&>(*dst);
            dst_column.resize(num_rows);
            auto* dst_data = reinterpret_cast<Data*>(dst_column.get_data().data());
            for (size_t i = 0; i != num_rows; ++i) {
                dst_data[i] = this->data(places[i] + offset);
            }
        } else {
            Base::serialize_to_column(places, offset, dst, num_rows);
        }
    }

    void streaming_agg_serialize_to_column(const IColumn** columns, MutableColumnPtr& dst,
                                           const size_t num_rows, Arena& arena) const override {
        if constexpr (Data::UsesFixedLengthStateSerialization) {
            auto& dst_column = assert_cast<ColumnFixedLengthObject&>(*dst);
            dst_column.resize(num_rows);
            auto* dst_data = reinterpret_cast<Data*>(dst_column.get_data().data());
            for (size_t i = 0; i != num_rows; ++i) {
                dst_data[i].set(*columns[0], i, arena);
            }
        } else {
            Base::streaming_agg_serialize_to_column(columns, dst, num_rows, arena);
        }
    }

    void deserialize_and_merge_from_column_range(AggregateDataPtr __restrict place,
                                                 const IColumn& column, size_t begin, size_t end,
                                                 Arena& arena) const override {
        if constexpr (Data::UsesFixedLengthStateSerialization) {
            DCHECK(end <= column.size() && begin <= end) << ", begin:" << begin << ", end:" << end
                                                         << ", column.size():" << column.size();
            auto& col = assert_cast<const ColumnFixedLengthObject&>(column);
            auto* data = reinterpret_cast<const Data*>(col.get_data().data());
            for (size_t i = begin; i <= end; ++i) {
                this->data(place).change_if_better(data[i], arena);
            }
        } else {
            Base::deserialize_and_merge_from_column_range(place, column, begin, end, arena);
        }
    }

    void deserialize_and_merge_vec(const AggregateDataPtr* places, size_t offset,
                                   AggregateDataPtr rhs, const IColumn* column, Arena& arena,
                                   const size_t num_rows) const override {
        if constexpr (Data::UsesFixedLengthStateSerialization) {
            const auto& col = assert_cast<const ColumnFixedLengthObject&>(*column);
            const auto* data = col.get_data().data();
            this->merge_vec(places, offset, AggregateDataPtr(data), arena, num_rows);
        } else {
            this->deserialize_vec(rhs, assert_cast<const ColumnString*>(column), arena, num_rows);
            DEFER({ this->destroy_vec(rhs, num_rows); });
            this->merge_vec(places, offset, rhs, arena, num_rows);
        }
    }

    void deserialize_and_merge_vec_selected(const AggregateDataPtr* places, size_t offset,
                                            AggregateDataPtr rhs, const IColumn* column,
                                            Arena& arena, const size_t num_rows) const override {
        if constexpr (Data::UsesFixedLengthStateSerialization) {
            const auto& col = assert_cast<const ColumnFixedLengthObject&>(*column);
            const auto* data = col.get_data().data();
            this->merge_vec_selected(places, offset, AggregateDataPtr(data), arena, num_rows);
        } else {
            this->deserialize_vec(rhs, assert_cast<const ColumnString*>(column), arena, num_rows);
            DEFER({ this->destroy_vec(rhs, num_rows); });
            this->merge_vec_selected(places, offset, rhs, arena, num_rows);
        }
    }

    void serialize_without_key_to_column(ConstAggregateDataPtr __restrict place,
                                         IColumn& to) const override {
        if constexpr (Data::UsesFixedLengthStateSerialization) {
            auto& col = assert_cast<ColumnFixedLengthObject&>(to);
            size_t old_size = col.size();
            col.resize(old_size + 1);
            *(reinterpret_cast<Data*>(col.get_data().data()) + old_size) = this->data(place);
        } else {
            Base::serialize_without_key_to_column(place, to);
        }
    }

    MutableColumnPtr create_serialize_column() const override {
        if constexpr (Data::UsesFixedLengthStateSerialization) {
            return ColumnFixedLengthObject::create(sizeof(Data));
        } else {
            return ColumnString::create();
        }
    }

    DataTypePtr get_serialized_type() const override {
        if constexpr (Data::UsesFixedLengthStateSerialization) {
            return std::make_shared<DataTypeFixedLengthObject>();
        } else {
            return std::make_shared<DataTypeString>();
        }
    }

    bool supported_incremental_mode() const override {
        if constexpr (Data::IS_ANY) {
            return false;
        }
        switch (_data_type->get_primitive_type()) {
        case TYPE_BITMAP:
        case TYPE_HLL:
        case TYPE_QUANTILE_STATE:
        case TYPE_AGG_STATE:
            return false;
        default:
            return true;
        }
    }

    void execute_function_with_incremental(int64_t partition_start, int64_t partition_end,
                                           int64_t frame_start, int64_t frame_end,
                                           AggregateDataPtr place, const IColumn** columns,
                                           Arena& arena, bool previous_is_nul, bool end_is_nul,
                                           bool has_null, UInt8* use_null_result,
                                           UInt8* could_use_previous_result) const override {
        int64_t current_frame_start = std::max<int64_t>(frame_start, partition_start);
        int64_t current_frame_end = std::min<int64_t>(frame_end, partition_end);
        if (current_frame_start >= current_frame_end) {
            *use_null_result = true;
            return;
        }
        if (*could_use_previous_result) {
            auto outcoming_pos = frame_start - 1;
            auto incoming_pos = frame_end - 1;
            if (!previous_is_nul && outcoming_pos >= partition_start &&
                outcoming_pos < partition_end) {
                if (this->data(place).is_equal_to(*columns[0], outcoming_pos)) {
                    this->data(place).reset();
                    if (has_null) {
                        const auto& null_map_data =
                                assert_cast<const ColumnUInt8*, TypeCheckOnRelease::DISABLE>(
                                        columns[1])
                                        ->get_data();
                        for (size_t i = current_frame_start; i < current_frame_end; ++i) {
                            if (null_map_data[i] == 0) {
                                this->data(place).change_if_better(*columns[0], i, arena);
                            }
                        }
                    } else {
                        this->add_range_single_place(partition_start, partition_end,
                                                     current_frame_start, current_frame_end, place,
                                                     columns, arena, use_null_result,
                                                     could_use_previous_result);
                    }
                    return;
                }
            }
            if (!end_is_nul && incoming_pos >= partition_start && incoming_pos < partition_end) {
                this->data(place).change_if_better(*columns[0], incoming_pos, arena);
            }

        } else {
            this->add_range_single_place(partition_start, partition_end, frame_start, frame_end,
                                         place, columns, arena, use_null_result,
                                         could_use_previous_result);
        }
    }

    void add_range_single_place(int64_t partition_start, int64_t partition_end, int64_t frame_start,
                                int64_t frame_end, AggregateDataPtr place, const IColumn** columns,
                                Arena& arena, UInt8* use_null_result,
                                UInt8* could_use_previous_result) const override {
        auto current_frame_start = std::max<int64_t>(frame_start, partition_start);
        auto current_frame_end = std::min<int64_t>(frame_end, partition_end);

        if (current_frame_start >= current_frame_end) {
            if (!*could_use_previous_result) {
                *use_null_result = true;
            }
        } else {
            for (size_t row_num = current_frame_start; row_num < current_frame_end; ++row_num) {
                this->data(place).change_if_better(*columns[0], row_num, arena);
            }
            *use_null_result = false;
            *could_use_previous_result = true;
        }
    }
};

template <template <typename> class Data>
AggregateFunctionPtr create_aggregate_function_single_value(const String& name,
                                                            const DataTypes& argument_types,
                                                            const DataTypePtr& result_type,
                                                            const bool result_is_nullable,
                                                            const AggregateFunctionAttr& attr = {});

template <template <typename> class Data>
AggregateFunctionPtr create_aggregate_function_single_value_any_value_function(
        const String& name, const DataTypes& argument_types, const DataTypePtr& result_type,
        const bool result_is_nullable, const AggregateFunctionAttr& attr = {});
} // namespace doris
