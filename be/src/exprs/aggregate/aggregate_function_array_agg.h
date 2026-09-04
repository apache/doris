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

#include "common/check.h"
#include "core/assert_cast.h"
#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/column/column_decimal.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/string_buffer.hpp"
#include "core/string_ref.h"
#include "core/types.h"
#include "exprs/aggregate/aggregate_function.h"

namespace doris {
class Arena;

template <PrimitiveType T, size_t ElemIdx = 0, bool NullableInput = true>
struct AggregateFunctionArrayAggData {
    static constexpr PrimitiveType PType = T;
    static constexpr bool use_native_serde = false;
    using ElementType = typename PrimitiveTypeTraits<T>::CppType;
    using ColVecType = typename PrimitiveTypeTraits<T>::ColumnType;
    using Self = AggregateFunctionArrayAggData<T, ElemIdx, NullableInput>;
    MutableColumnPtr column_data;
    ColVecType* nested_column = nullptr;
    NullMap* null_map = nullptr;

    AggregateFunctionArrayAggData(const DataTypes& argument_types) {
        DataTypePtr column_type = make_nullable(argument_types[ElemIdx]);
        column_data = column_type->create_column();
        null_map = &(assert_cast<ColumnNullable&>(*column_data).get_null_map_data());
        nested_column = assert_cast<ColVecType*>(
                assert_cast<ColumnNullable&>(*column_data).get_nested_column_ptr().get());
    }

    void add(const IColumn& column, size_t row_num) {
        if constexpr (NullableInput) {
            const auto& col = assert_cast<const ColumnNullable&, TypeCheckOnRelease::DISABLE>(
                    column);
            const auto& vec =
                    assert_cast<const ColVecType&, TypeCheckOnRelease::DISABLE>(
                            col.get_nested_column())
                            .get_data();
            null_map->push_back(col.get_null_map_data()[row_num]);
            nested_column->get_data().push_back(vec[row_num]);
        } else {
            const auto& vec =
                    assert_cast<const ColVecType&, TypeCheckOnRelease::DISABLE>(column).get_data();
            null_map->push_back(0);
            nested_column->get_data().push_back(vec[row_num]);
        }
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
        auto& to_arr = assert_cast<ColumnArray&, TypeCheckOnRelease::DISABLE>(to);
        auto& to_nested_col = to_arr.get_data();
        auto* col_null = assert_cast<ColumnNullable*, TypeCheckOnRelease::DISABLE>(&to_nested_col);
        auto& vec =
                assert_cast<ColVecType&, TypeCheckOnRelease::DISABLE>(col_null->get_nested_column())
                        .get_data();
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
        column_data->insert_range_from(*rhs.column_data, 0, rhs.column_data->size());
    }
};

template <PrimitiveType T, size_t ElemIdx, bool NullableInput>
    requires(is_string_type(T))
struct AggregateFunctionArrayAggData<T, ElemIdx, NullableInput> {
    static constexpr PrimitiveType PType = T;
    static constexpr bool use_native_serde = false;
    using ElementType = StringRef;
    using ColVecType = ColumnString;
    using Self = AggregateFunctionArrayAggData<T, ElemIdx, NullableInput>;
    MutableColumnPtr column_data;
    ColVecType* nested_column = nullptr;
    NullMap* null_map = nullptr;

    AggregateFunctionArrayAggData(const DataTypes& argument_types) {
        DataTypePtr column_type = make_nullable(argument_types[ElemIdx]);
        column_data = column_type->create_column();
        null_map = &(assert_cast<ColumnNullable&>(*column_data).get_null_map_data());
        nested_column = assert_cast<ColVecType*>(
                assert_cast<ColumnNullable&>(*column_data).get_nested_column_ptr().get());
    }

    void add(const IColumn& column, size_t row_num) {
        if constexpr (NullableInput) {
            const auto& col = assert_cast<const ColumnNullable&, TypeCheckOnRelease::DISABLE>(
                    column);
            const auto& vec = assert_cast<const ColVecType&, TypeCheckOnRelease::DISABLE>(
                    col.get_nested_column());
            null_map->push_back(col.get_null_map_data()[row_num]);
            nested_column->insert_from(vec, row_num);
        } else {
            const auto& vec =
                    assert_cast<const ColVecType&, TypeCheckOnRelease::DISABLE>(column);
            null_map->push_back(0);
            nested_column->insert_from(vec, row_num);
        }
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
        auto& to_arr = assert_cast<ColumnArray&, TypeCheckOnRelease::DISABLE>(to);
        auto& to_nested_col = to_arr.get_data();
        auto* col_null = assert_cast<ColumnNullable*, TypeCheckOnRelease::DISABLE>(&to_nested_col);
        auto& vec = assert_cast<ColVecType&, TypeCheckOnRelease::DISABLE>(
                col_null->get_nested_column());
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
        column_data->insert_range_from(*rhs.column_data, 0, rhs.column_data->size());
    }
};

template <PrimitiveType T, size_t ElemIdx, bool NullableInput>
    requires(!is_string_type(T) && !is_int_or_bool(T) && !is_float_or_double(T) && !is_decimal(T) &&
             !is_date_type(T) && !is_ip(T))
struct AggregateFunctionArrayAggData<T, ElemIdx, NullableInput> {
    static constexpr PrimitiveType PType = T;
    static constexpr bool use_native_serde = true;
    using ElementType = StringRef;
    using Self = AggregateFunctionArrayAggData<T, ElemIdx, NullableInput>;
    MutableColumnPtr column_data;

    AggregateFunctionArrayAggData(const DataTypes& argument_types)
            : column_data(argument_types[ElemIdx]->create_column()) {}

    void add(const IColumn& column, size_t row_num) { column_data->insert_from(column, row_num); }

    void deserialize_and_merge(const IColumn& column, size_t row_num) {
        const auto& to_arr = assert_cast<const ColumnArray&>(column);
        const auto& to_nested_col = to_arr.get_data();
        auto start = to_arr.get_offsets()[row_num - 1];
        auto end = start + to_arr.get_offsets()[row_num] - to_arr.get_offsets()[row_num - 1];
        if constexpr (NullableInput) {
            column_data->insert_range_from(to_nested_col, start, end - start);
        } else {
            // Serialized state columns use the nullable result element type while raw-declared
            // state stores plain rows; rows serialized this way are all non-null.
            const auto& to_nested_nullable =
                    assert_cast<const ColumnNullable&, TypeCheckOnRelease::DISABLE>(to_nested_col);
            column_data->insert_range_from(to_nested_nullable.get_nested_column(), start,
                                           end - start);
        }
    }

    void reset() { column_data->clear(); }

    void insert_result_into(IColumn& to) const {
        auto& to_arr = assert_cast<ColumnArray&, TypeCheckOnRelease::DISABLE>(to);
        auto& to_nested_col = to_arr.get_data();
        if constexpr (NullableInput) {
            to_nested_col.insert_range_from(*column_data, 0, column_data->size());
        } else {
            // The state column stores raw (non-nullable) rows while the result array element
            // column is always nullable; copy the raw rows and mark them all non-null.
            auto& to_nested_nullable =
                    assert_cast<ColumnNullable&, TypeCheckOnRelease::DISABLE>(to_nested_col);
            to_nested_nullable.get_nested_column().insert_range_from(
                    *column_data, 0, column_data->size());
            to_nested_nullable.get_null_map_data().resize_fill(
                    to_nested_nullable.get_nested_column().size(), 0);
        }
        to_arr.get_offsets().push_back(to_nested_col.size());
    }

    void write(BufferWritable& buf, const IDataType& column_type, int be_exec_version) const {
        const auto max_serialized_bytes = cast_set<size_t>(
                column_type.get_uncompressed_serialized_bytes(*column_data, be_exec_version));
        buf.resize(sizeof(UInt64) + max_serialized_bytes);
        auto* serialized_data = buf.data() + sizeof(UInt64);
        const char* end = nullptr;
        try {
            end = column_type.serialize(*column_data, serialized_data, be_exec_version);
        } catch (...) {
            buf.resize(0);
            throw;
        }
        DORIS_CHECK_LE(end, serialized_data + max_serialized_bytes);
        const auto serialized_bytes = static_cast<UInt64>(end - serialized_data);
        memcpy(buf.data(), &serialized_bytes, sizeof(serialized_bytes));
        const auto frame_bytes = sizeof(serialized_bytes) + cast_set<size_t>(serialized_bytes);
        buf.resize(frame_bytes);
        buf.add_offset(frame_bytes);
    }

    void read(BufferReadable& buf, const IDataType& column_type, int be_exec_version) {
        DORIS_CHECK(column_data->empty());
        UInt64 serialized_bytes = 0;
        buf.read_binary(serialized_bytes);
        const auto* serialized_data = buf.data();
        const auto* end = column_type.deserialize(serialized_data, &column_data, be_exec_version);
        DORIS_CHECK_EQ(end, serialized_data + serialized_bytes);
        buf.add_offset(serialized_bytes);
    }

    void merge(const Self& rhs) {
        column_data->insert_range_from(*rhs.column_data, 0, rhs.column_data->size());
    }
};

//ShowNull is just used to support array_agg because array_agg needs to display NULL
//todo: Supports order by sorting for array_agg
template <typename Data>
class AggregateFunctionArrayAgg final
        : public IAggregateFunctionDataHelper<Data, AggregateFunctionArrayAgg<Data>, true>,
          UnaryExpression,
          NotNullableAggregateFunction {
public:
    using Base = IAggregateFunctionDataHelper<Data, AggregateFunctionArrayAgg<Data>, true>;

    AggregateFunctionArrayAgg(const DataTypes& argument_types_)
            : Base({argument_types_}),
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
        if constexpr (Data::use_native_serde) {
            this->data(place).write(buf, *this->argument_types[0], this->version);
        } else {
            this->data(place).write(buf);
        }
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena&) const override {
        if constexpr (Data::use_native_serde) {
            this->data(place).read(buf, *this->argument_types[0], this->version);
        } else {
            this->data(place).read(buf);
        }
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        auto& to_arr = assert_cast<ColumnArray&, TypeCheckOnRelease::DISABLE>(to);
        auto& to_nested_col = to_arr.get_data();
        DCHECK(to_nested_col.is_nullable());
        this->data(place).insert_result_into(to);
    }

    void serialize_without_key_to_column(ConstAggregateDataPtr __restrict place,
                                         IColumn& to) const override {
        this->data(place).insert_result_into(to);
    }

    void deserialize_and_merge_vec(const AggregateDataPtr* places, size_t offset,
                                   AggregateDataPtr rhs, const IColumn* column, Arena& arena,
                                   const size_t num_rows) const override {
        for (size_t i = 0; i != num_rows; ++i) {
            this->data(places[i] + offset).deserialize_and_merge(*column, i);
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
        if constexpr (is_string_type(Data::PType)) {
            check_array_nullable_string_column_type(*dst, true);
        }
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

    void check_input_columns_type(const IColumn** columns) const override {
        IAggregateFunction::check_input_columns_type(columns);
        if constexpr (is_string_type(Data::PType)) {
            const auto* nullable_column = check_and_get_column<ColumnNullable>(*columns[0]);
            if (UNLIKELY(nullable_column == nullptr)) {
                throw doris::Exception(Status::InternalError(
                        "Aggregate function {} argument 0 type check failed: Column type {} is "
                        "not ColumnNullable",
                        get_name(), columns[0]->get_name()));
            }
            this->template check_argument_column_type<ColumnString>(
                    &nullable_column->get_nested_column());
        }
    }

    void check_result_column_type(const IColumn& to) const override {
        IAggregateFunction::check_result_column_type(to);
        if constexpr (is_string_type(Data::PType)) {
            check_array_nullable_string_column_type(to, true);
        }
    }

private:
    void check_array_nullable_string_column_type(const IColumn& column,
                                                 bool is_result_column) const {
        const auto* array_column = check_and_get_column<ColumnArray>(column);
        if (UNLIKELY(array_column == nullptr)) {
            throw doris::Exception(Status::InternalError(
                    "Aggregate function {} {} type check failed: Column type {} is not "
                    "ColumnArray",
                    get_name(), is_result_column ? "result" : "argument", column.get_name()));
        }

        const auto& nested_column = array_column->get_data();
        const auto* nullable_column = check_and_get_column<ColumnNullable>(nested_column);
        if (UNLIKELY(nullable_column == nullptr)) {
            throw doris::Exception(Status::InternalError(
                    "Aggregate function {} {} type check failed: Column type {} is not "
                    "ColumnNullable",
                    get_name(), is_result_column ? "result" : "argument",
                    nested_column.get_name()));
        }
        this->template check_result_column_type_as<ColumnString>(
                nullable_column->get_nested_column());
    }

    DataTypePtr return_type;
};

/**
 * Conditional variant of array_agg: array_agg_if(cond, elem).
 *
 * <p>Rows where {@code cond} is false (or NULL) are skipped entirely -- they contribute neither an
 * element nor a NULL. Remaining rows follow array_agg semantics exactly (NULL elements are kept).
 * Used by IVM to compute insert/delete delta arrays in one aggregate over signed change rows.
 */
template <typename Data>
class AggregateFunctionArrayAggIf final
        : public IAggregateFunctionDataHelper<Data, AggregateFunctionArrayAggIf<Data>, true>,
          NotNullableAggregateFunction {
public:
    using Base = IAggregateFunctionDataHelper<Data, AggregateFunctionArrayAggIf<Data>, true>;

    AggregateFunctionArrayAggIf(const DataTypes& argument_types_)
            : Base(argument_types_),
              return_type(
                      std::make_shared<DataTypeArray>(make_nullable(argument_types_[1]))) {}

    std::string get_name() const override { return "array_agg_if"; }

    DataTypePtr get_return_type() const override { return return_type; }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena& arena) const override {
        if (!cond_true(columns[0], row_num)) {
            return;
        }
        this->data(place).add(*columns[1], row_num);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena& arena) const override {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        if constexpr (Data::use_native_serde) {
            this->data(place).write(buf, *this->argument_types[1], this->version);
        } else {
            this->data(place).write(buf);
        }
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena&) const override {
        if constexpr (Data::use_native_serde) {
            this->data(place).read(buf, *this->argument_types[1], this->version);
        } else {
            this->data(place).read(buf);
        }
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        auto& to_arr = assert_cast<ColumnArray&, TypeCheckOnRelease::DISABLE>(to);
        auto& to_nested_col = to_arr.get_data();
        DCHECK(to_nested_col.is_nullable());
        this->data(place).insert_result_into(to);
    }

    void serialize_without_key_to_column(ConstAggregateDataPtr __restrict place,
                                         IColumn& to) const override {
        this->data(place).insert_result_into(to);
    }

    void deserialize_and_merge_vec(const AggregateDataPtr* places, size_t offset,
                                   AggregateDataPtr rhs, const IColumn* column, Arena& arena,
                                   const size_t num_rows) const override {
        for (size_t i = 0; i != num_rows; ++i) {
            this->data(places[i] + offset).deserialize_and_merge(*column, i);
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
        if constexpr (is_string_type(Data::PType)) {
            check_array_nullable_string_column_type(*dst, true);
        }
        auto& to_arr = assert_cast<ColumnArray&>(*dst);
        auto& to_nested_col = to_arr.get_data();
        DCHECK(num_rows == columns[0]->size() && num_rows == columns[1]->size());
        auto* col_null = assert_cast<ColumnNullable*>(&to_nested_col);
        const auto* nullable_elem =
                check_and_get_column<ColumnNullable>(*columns[1]);

        for (size_t i = 0; i < num_rows; ++i) {
            if (!cond_true(columns[0], i)) {
                to_arr.get_offsets().push_back(to_nested_col.size());
                continue;
            }
            if (nullable_elem != nullptr) {
                col_null->get_null_map_data().push_back(nullable_elem->get_null_map_data()[i]);
            } else {
                col_null->get_null_map_data().push_back(0);
            }
            const IColumn& elem_src = nullable_elem != nullptr ? nullable_elem->get_nested_column()
                                                               : *columns[1];
            if constexpr (is_string_type(Data::PType)) {
                auto& vec = assert_cast<ColumnString&, TypeCheckOnRelease::DISABLE>(
                        col_null->get_nested_column());
                const auto& vec_src =
                        assert_cast<const ColumnString&, TypeCheckOnRelease::DISABLE>(elem_src);
                vec.insert_from(vec_src, i);
            } else if constexpr (!is_string_type(Data::PType) && !is_int_or_bool(Data::PType) &&
                                 !is_float_or_double(Data::PType) && !is_decimal(Data::PType) &&
                                 !is_date_type(Data::PType) && !is_ip(Data::PType)) {
                auto& vec = col_null->get_nested_column();
                vec.insert_from(elem_src, i);
            } else {
                using ColVecType = typename PrimitiveTypeTraits<Data::PType>::ColumnType;
                auto& vec = assert_cast<ColVecType&, TypeCheckOnRelease::DISABLE>(
                                    col_null->get_nested_column())
                                    .get_data();
                const auto& vec_src =
                        assert_cast<const ColVecType&, TypeCheckOnRelease::DISABLE>(elem_src)
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

    void check_input_columns_type(const IColumn** columns) const override {
        IAggregateFunction::check_input_columns_type(columns);
        if constexpr (is_string_type(Data::PType)) {
            const IColumn& elem_col = *columns[1];
            if (const auto* nullable_column =
                        check_and_get_column<ColumnNullable>(elem_col)) {
                this->template check_argument_column_type<ColumnString>(
                        &nullable_column->get_nested_column());
            } else {
                this->template check_argument_column_type<ColumnString>(&elem_col);
            }
        }
    }

    void check_result_column_type(const IColumn& to) const override {
        IAggregateFunction::check_result_column_type(to);
        if constexpr (is_string_type(Data::PType)) {
            check_array_nullable_string_column_type(to, true);
        }
    }

private:
    /** Returns whether the row passes the condition. NULL condition is treated as false. */
    static bool cond_true(const IColumn* cond_column, size_t row_num) {
        if (cond_column->is_nullable()) {
            const auto& nullable_cond = assert_cast<const ColumnNullable&,
                                                      TypeCheckOnRelease::DISABLE>(*cond_column);
            if (nullable_cond.is_null_at(row_num)) {
                return false;
            }
            cond_column = &nullable_cond.get_nested_column();
        }
        const auto& cond_data = assert_cast<const ColumnUInt8&, TypeCheckOnRelease::DISABLE>(
                                        *cond_column)
                                        .get_data();
        return cond_data[row_num] != 0;
    }

    void check_array_nullable_string_column_type(const IColumn& column,
                                                 bool is_result_column) const {
        const auto* array_column = check_and_get_column<ColumnArray>(column);
        if (UNLIKELY(array_column == nullptr)) {
            throw doris::Exception(Status::InternalError(
                    "Aggregate function {} {} type check failed: Column type {} is not "
                    "ColumnArray",
                    get_name(), is_result_column ? "result" : "argument", column.get_name()));
        }

        const auto& nested_column = array_column->get_data();
        const auto* nullable_column = check_and_get_column<ColumnNullable>(nested_column);
        if (UNLIKELY(nullable_column == nullptr)) {
            throw doris::Exception(Status::InternalError(
                    "Aggregate function {} {} type check failed: Column type {} is not "
                    "ColumnNullable",
                    get_name(), is_result_column ? "result" : "argument",
                    nested_column.get_name()));
        }
        this->template check_result_column_type_as<ColumnString>(
                nullable_column->get_nested_column());
    }

    DataTypePtr return_type;
};

} // namespace doris
