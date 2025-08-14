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

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_buffer.hpp"
#include "vec/common/string_ref.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"
class Arena;

template <PrimitiveType T>
struct AggregateFunctionArrayAggData {
    static constexpr PrimitiveType PType = T;
    using ElementType = typename PrimitiveTypeTraits<T>::ColumnItemType;
    using ColVecType = typename PrimitiveTypeTraits<T>::ColumnType;
    using Self = AggregateFunctionArrayAggData<T>;
    MutableColumnPtr column_data;
    ColVecType* nested_column = nullptr;
    NullMap* null_map = nullptr;

    AggregateFunctionArrayAggData(const DataTypes& argument_types) {
        DataTypePtr column_type = make_nullable(argument_types[0]);
        column_data = column_type->create_column();
        null_map = &(assert_cast<ColumnNullable&>(*column_data).get_null_map_data());
        nested_column = assert_cast<ColVecType*>(
                assert_cast<ColumnNullable&>(*column_data).get_nested_column_ptr().get());
    }

    void add(const IColumn& column, size_t row_num) {
        const auto& col = assert_cast<const ColumnNullable&, TypeCheckOnRelease::DISABLE>(column);
        const auto& vec =
                assert_cast<const ColVecType&, TypeCheckOnRelease::DISABLE>(col.get_nested_column())
                        .get_data();
        null_map->push_back(col.get_null_map_data()[row_num]);
        nested_column->get_data().push_back(vec[row_num]);
        DCHECK(null_map->size() == nested_column->size());
    }

    void deserialize_and_merge(const IColumn& column, size_t row_num) {
        const auto& to_arr = assert_cast<const ColumnArray&>(column);
        const auto& to_nested_col = to_arr.get_data();
        const auto* col_null = assert_cast<const ColumnNullable*>(&to_nested_col);
        const auto& vec = assert_cast<const ColVecType&>(col_null->get_nested_column()).get_data();
        auto start = to_arr.get_offsets()[row_num - 1];
        auto end = start + to_arr.get_offsets()[row_num] - to_arr.get_offsets()[row_num - 1];
        for (auto i = start; i < end; ++i) {
            null_map->push_back(col_null->get_null_map_data()[i]);
            nested_column->get_data().push_back(vec[i]);
        }
    }

    void reset() {
        null_map->clear();
        nested_column->clear();
    }

    void insert_result_into(IColumn& to) const {
        auto& to_arr = assert_cast<ColumnArray&>(to);
        auto& to_nested_col = to_arr.get_data();
        auto* col_null = assert_cast<ColumnNullable*>(&to_nested_col);
        auto& vec = assert_cast<ColVecType&>(col_null->get_nested_column()).get_data();
        size_t num_rows = null_map->size();
        auto& nested_column_data = nested_column->get_data();
        for (size_t i = 0; i < num_rows; ++i) {
            col_null->get_null_map_data().push_back((*null_map)[i]);
            vec.push_back(nested_column_data[i]);
        }
        to_arr.get_offsets().push_back(to_nested_col.size());
    }

    void write(BufferWritable& buf) const {
        const size_t size = null_map->size();
        buf.write_binary(size);

        for (size_t i = 0; i < size; i++) {
            buf.write_binary(null_map->data()[i]);
        }

        for (size_t i = 0; i < size; i++) {
            buf.write_binary(nested_column->get_data()[i]);
        }
    }

    void read(BufferReadable& buf) {
        DCHECK(null_map);
        DCHECK(null_map->empty());
        size_t size = 0;
        buf.read_binary(size);
        null_map->resize(size);
        nested_column->reserve(size);
        for (size_t i = 0; i < size; i++) {
            buf.read_binary(null_map->data()[i]);
        }

        ElementType data_value;
        for (size_t i = 0; i < size; i++) {
            buf.read_binary(data_value);
            nested_column->get_data().push_back(data_value);
        }
    }

    void merge(const Self& rhs) {
        const auto size = rhs.null_map->size();
        null_map->resize(size);
        nested_column->reserve(size);
        for (size_t i = 0; i < size; i++) {
            const auto null_value = rhs.null_map->data()[i];
            const auto data_value = rhs.nested_column->get_data()[i];
            null_map->data()[i] = null_value;
            nested_column->get_data().push_back(data_value);
        }
    }
};

template <PrimitiveType T>
    requires(is_string_type(T))
struct AggregateFunctionArrayAggData<T> {
    static constexpr PrimitiveType PType = T;
    using ElementType = StringRef;
    using ColVecType = ColumnString;
    using Self = AggregateFunctionArrayAggData<T>;
    MutableColumnPtr column_data;
    ColVecType* nested_column = nullptr;
    NullMap* null_map = nullptr;

    AggregateFunctionArrayAggData(const DataTypes& argument_types) {
        DataTypePtr column_type = make_nullable(argument_types[0]);
        column_data = column_type->create_column();
        null_map = &(assert_cast<ColumnNullable&>(*column_data).get_null_map_data());
        nested_column = assert_cast<ColVecType*>(
                assert_cast<ColumnNullable&>(*column_data).get_nested_column_ptr().get());
    }

    void add(const IColumn& column, size_t row_num) {
        const auto& col = assert_cast<const ColumnNullable&, TypeCheckOnRelease::DISABLE>(column);
        const auto& vec = assert_cast<const ColVecType&, TypeCheckOnRelease::DISABLE>(
                col.get_nested_column());
        null_map->push_back(col.get_null_map_data()[row_num]);
        nested_column->insert_from(vec, row_num);
        DCHECK(null_map->size() == nested_column->size());
    }

    void deserialize_and_merge(const IColumn& column, size_t row_num) {
        auto& to_arr = assert_cast<const ColumnArray&>(column);
        auto& to_nested_col = to_arr.get_data();
        auto col_null = assert_cast<const ColumnNullable*>(&to_nested_col);
        const auto& vec = assert_cast<const ColVecType&>(col_null->get_nested_column());
        auto start = to_arr.get_offsets()[row_num - 1];
        auto end = start + to_arr.get_offsets()[row_num] - to_arr.get_offsets()[row_num - 1];
        for (auto i = start; i < end; ++i) {
            null_map->push_back(col_null->get_null_map_data()[i]);
            nested_column->insert_from(vec, i);
        }
    }

    void reset() {
        null_map->clear();
        nested_column->clear();
    }

    void insert_result_into(IColumn& to) const {
        auto& to_arr = assert_cast<ColumnArray&>(to);
        auto& to_nested_col = to_arr.get_data();
        auto* col_null = assert_cast<ColumnNullable*>(&to_nested_col);
        auto& vec = assert_cast<ColVecType&>(col_null->get_nested_column());
        size_t num_rows = null_map->size();
        for (size_t i = 0; i < num_rows; ++i) {
            col_null->get_null_map_data().push_back((*null_map)[i]);
            vec.insert_from(*nested_column, i);
        }
        to_arr.get_offsets().push_back(to_nested_col.size());
    }

    void write(BufferWritable& buf) const {
        const size_t size = null_map->size();
        buf.write_binary(size);
        for (size_t i = 0; i < size; i++) {
            buf.write_binary(null_map->data()[i]);
        }
        for (size_t i = 0; i < size; i++) {
            buf.write_binary(nested_column->get_data_at(i));
        }
    }

    void read(BufferReadable& buf) {
        DCHECK(null_map);
        DCHECK(null_map->empty());
        size_t size = 0;
        buf.read_binary(size);
        null_map->resize(size);
        nested_column->reserve(size);
        for (size_t i = 0; i < size; i++) {
            buf.read_binary(null_map->data()[i]);
        }

        StringRef s;
        for (size_t i = 0; i < size; i++) {
            buf.read_binary(s);
            nested_column->insert_data(s.data, s.size);
        }
    }

    void merge(const Self& rhs) {
        const auto size = rhs.null_map->size();
        null_map->resize(size);
        nested_column->reserve(size);
        for (size_t i = 0; i < size; i++) {
            const auto null_value = rhs.null_map->data()[i];
            auto s = rhs.nested_column->get_data_at(i);
            null_map->data()[i] = null_value;
            nested_column->insert_data(s.data, s.size);
        }
    }
};

template <PrimitiveType T>
    requires(!is_string_type(T) && !is_int_or_bool(T) && !is_float_or_double(T) && !is_decimal(T) &&
             !is_date_type(T) && !is_ip(T))
struct AggregateFunctionArrayAggData<T> {
    static constexpr PrimitiveType PType = T;
    using ElementType = StringRef;
    using Self = AggregateFunctionArrayAggData<T>;
    MutableColumnPtr column_data;

    AggregateFunctionArrayAggData(const DataTypes& argument_types) {
        DataTypePtr column_type = argument_types[0];
        column_data = column_type->create_column();
    }

    void add(const IColumn& column, size_t row_num) { column_data->insert_from(column, row_num); }

    void deserialize_and_merge(const IColumn& column, size_t row_num) {
        const auto& to_arr = assert_cast<const ColumnArray&>(column);
        const auto& to_nested_col = to_arr.get_data();
        auto start = to_arr.get_offsets()[row_num - 1];
        auto end = start + to_arr.get_offsets()[row_num] - to_arr.get_offsets()[row_num - 1];
        for (auto i = start; i < end; ++i) {
            column_data->insert_from(to_nested_col, i);
        }
    }

    void reset() { column_data->clear(); }

    void insert_result_into(IColumn& to) const {
        auto& to_arr = assert_cast<ColumnArray&>(to);
        auto& to_nested_col = to_arr.get_data();
        size_t num_rows = column_data->size();
        for (size_t i = 0; i < num_rows; ++i) {
            to_nested_col.insert_from(*column_data, i);
        }
        to_arr.get_offsets().push_back(to_nested_col.size());
    }

    void write(BufferWritable& buf) const {
        throw Exception(ErrorCode::NOT_IMPLEMENTED_ERROR, "array_agg not support write");
    }

    void read(BufferReadable& buf) {
        throw Exception(ErrorCode::NOT_IMPLEMENTED_ERROR, "array_agg not support read");
    }

    void merge(const Self& rhs) {
        throw Exception(ErrorCode::NOT_IMPLEMENTED_ERROR, "array_agg not support merge");
    }
};

//ShowNull is just used to support array_agg because array_agg needs to display NULL
//todo: Supports order by sorting for array_agg
template <typename Data>
class AggregateFunctionArrayAgg
        : public IAggregateFunctionDataHelper<Data, AggregateFunctionArrayAgg<Data>, true>,
          UnaryExpression,
          NotNullableAggregateFunction {
public:
    AggregateFunctionArrayAgg(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionArrayAgg<Data>, true>(
                      {argument_types_}),
              return_type(std::make_shared<DataTypeArray>(make_nullable(argument_types_[0]))) {}

    std::string get_name() const override { return "array_agg"; }

    DataTypePtr get_return_type() const override { return return_type; }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena& arena) const override {
        this->data(place).add(*columns[0], row_num);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena& arena) const override {
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
        auto& to_arr = assert_cast<ColumnArray&>(to);
        auto& to_nested_col = to_arr.get_data();
        DCHECK(to_nested_col.is_nullable());
        this->data(place).insert_result_into(to);
    }

    void serialize_without_key_to_column(ConstAggregateDataPtr __restrict place,
                                         IColumn& to) const override {
        this->data(place).insert_result_into(to);
    }

    void deserialize_and_merge_from_column(AggregateDataPtr __restrict place, const IColumn& column,
                                           Arena& arena) const override {
        const size_t num_rows = column.size();
        for (size_t i = 0; i != num_rows; ++i) {
            this->data(place).deserialize_and_merge(column, i);
        }
    }

    void deserialize_and_merge_vec(const AggregateDataPtr* places, size_t offset,
                                   AggregateDataPtr rhs, const IColumn* column, Arena& arena,
                                   const size_t num_rows) const override {
        for (size_t i = 0; i != num_rows; ++i) {
            this->data(places[i] + offset).deserialize_and_merge(*column, i);
        }
    }

    void deserialize_from_column(AggregateDataPtr places, const IColumn& column, Arena& arena,
                                 size_t num_rows) const override {
        for (size_t i = 0; i != num_rows; ++i) {
            this->data(places).deserialize_and_merge(column, i);
        }
    }

    void deserialize_and_merge_from_column_range(AggregateDataPtr __restrict place,
                                                 const IColumn& column, size_t begin, size_t end,
                                                 Arena& arena) const override {
        DCHECK(end <= column.size() && begin <= end)
                << ", begin:" << begin << ", end:" << end << ", column.size():" << column.size();
        for (size_t i = begin; i <= end; ++i) {
            this->data(place).deserialize_and_merge(column, i);
        }
    }

    void deserialize_and_merge_vec_selected(const AggregateDataPtr* places, size_t offset,
                                            AggregateDataPtr rhs, const IColumn* column,
                                            Arena& arena, const size_t num_rows) const override {
        for (size_t i = 0; i != num_rows; ++i) {
            if (places[i]) {
                this->data(places[i] + offset).deserialize_and_merge(*column, i);
            }
        }
    }

    void serialize_to_column(const std::vector<AggregateDataPtr>& places, size_t offset,
                             MutableColumnPtr& dst, const size_t num_rows) const override {
        for (size_t i = 0; i != num_rows; ++i) {
            Data& data_ = this->data(places[i] + offset);
            data_.insert_result_into(*dst);
        }
    }

    void streaming_agg_serialize_to_column(const IColumn** columns, MutableColumnPtr& dst,
                                           const size_t num_rows, Arena& arena) const override {
        auto& to_arr = assert_cast<ColumnArray&>(*dst);
        auto& to_nested_col = to_arr.get_data();
        DCHECK(num_rows == columns[0]->size());
        auto* col_null = assert_cast<ColumnNullable*>(&to_nested_col);
        const auto& col_src = assert_cast<const ColumnNullable&>(*(columns[0]));

        for (size_t i = 0; i < num_rows; ++i) {
            col_null->get_null_map_data().push_back(col_src.get_null_map_data()[i]);
            if constexpr (is_string_type(Data::PType)) {
                auto& vec = assert_cast<ColumnString&, TypeCheckOnRelease::DISABLE>(
                        col_null->get_nested_column());
                const auto& vec_src = assert_cast<const ColumnString&, TypeCheckOnRelease::DISABLE>(
                        col_src.get_nested_column());
                vec.insert_from(vec_src, i);
            } else if constexpr (!is_string_type(Data::PType) && !is_int_or_bool(Data::PType) &&
                                 !is_float_or_double(Data::PType) && !is_decimal(Data::PType) &&
                                 !is_date_type(Data::PType) && !is_ip(Data::PType)) {
                auto& vec = col_null->get_nested_column();
                vec.insert_from(col_src.get_nested_column(), i);
            } else {
                using ColVecType = typename PrimitiveTypeTraits<Data::PType>::ColumnType;
                auto& vec = assert_cast<ColVecType&, TypeCheckOnRelease::DISABLE>(
                                    col_null->get_nested_column())
                                    .get_data();
                auto& vec_src = assert_cast<const ColVecType&, TypeCheckOnRelease::DISABLE>(
                                        col_src.get_nested_column())
                                        .get_data();
                vec.push_back(vec_src[i]);
            }
            to_arr.get_offsets().push_back(to_nested_col.size());
        }
    }

    MutableColumnPtr create_serialize_column() const override {
        return get_serialized_type()->create_column();
    }

    DataTypePtr get_serialized_type() const override { return return_type; }

private:
    DataTypePtr return_type;
};

} // namespace doris::vectorized

#include "common/compile_check_end.h"
