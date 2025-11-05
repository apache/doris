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
#include <type_traits>

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_vector.h"
#include "vec/common/arena.h"
#include "vec/common/string_buffer.hpp"
#include "vec/core/types.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"
#include "vec/io/io_helper.h"

namespace doris {
#include "common/compile_check_begin.h"
namespace vectorized {

template <PrimitiveType T>
struct AggregateFunctionProductData {
    typename PrimitiveTypeTraits<T>::ColumnItemType product {};

    void add_impl(typename PrimitiveTypeTraits<T>::ColumnItemType value,
                  typename PrimitiveTypeTraits<T>::ColumnItemType& product_ref) {
        if constexpr (std::is_integral_v<typename PrimitiveTypeTraits<T>::ColumnItemType>) {
            typename PrimitiveTypeTraits<T>::ColumnItemType new_product;
            if (__builtin_expect(common::mul_overflow(product_ref, value, new_product), false)) {
                // if overflow, set product to infinity to keep the same behavior with double type
                throw Exception(ErrorCode::INTERNAL_ERROR,
                                "Product overflow for type {} and value {} * {}", T, value,
                                product_ref);
            } else {
                product_ref = new_product;
            }
        } else {
            // which type is float or double
            product_ref *= value;
        }
    }

    void add(typename PrimitiveTypeTraits<T>::ColumnItemType value,
             typename PrimitiveTypeTraits<T>::ColumnItemType) {
        add_impl(value, product);
        VLOG_DEBUG << "product: " << product;
    }

    void merge(const AggregateFunctionProductData& other,
               typename PrimitiveTypeTraits<T>::ColumnItemType) {
        add_impl(other.product, product);
        VLOG_DEBUG << "product: " << product;
    }

    void write(BufferWritable& buffer) const { buffer.write_binary(product); }

    void read(BufferReadable& buffer) { buffer.read_binary(product); }

    typename PrimitiveTypeTraits<T>::ColumnItemType get() const { return product; }

    void reset(typename PrimitiveTypeTraits<T>::ColumnItemType value) {
        product = std::move(value);
    }
};

template <>
struct AggregateFunctionProductData<TYPE_DECIMALV2> {
    Decimal128V2 product {};

    void add(Decimal128V2 value, Decimal128V2) {
        DecimalV2Value decimal_product(product);
        DecimalV2Value decimal_value(value);
        DecimalV2Value ret = decimal_product * decimal_value;
        memcpy(&product, &ret, sizeof(Decimal128V2));
    }

    void merge(const AggregateFunctionProductData& other, Decimal128V2) {
        DecimalV2Value decimal_product(product);
        DecimalV2Value decimal_value(other.product);
        DecimalV2Value ret = decimal_product * decimal_value;
        memcpy(&product, &ret, sizeof(Decimal128V2));
    }

    void write(BufferWritable& buffer) const { buffer.write_binary(product); }

    void read(BufferReadable& buffer) { buffer.read_binary(product); }

    Decimal128V2 get() const { return product; }

    void reset(Decimal128V2 value) { product = std::move(value); }
};

template <PrimitiveType T>
    requires(T == TYPE_DECIMAL128I || T == TYPE_DECIMAL256)
struct AggregateFunctionProductData<T> {
    typename PrimitiveTypeTraits<T>::ColumnItemType product {};

    template <typename NestedType>
    void add(Decimal<NestedType> value,
             typename PrimitiveTypeTraits<T>::ColumnItemType multiplier) {
        product *= value;
        product /= multiplier;
    }

    void merge(const AggregateFunctionProductData& other,
               typename PrimitiveTypeTraits<T>::ColumnItemType multiplier) {
        product *= other.product;
        product /= multiplier;
    }

    void write(BufferWritable& buffer) const { buffer.write_binary(product); }

    void read(BufferReadable& buffer) { buffer.read_binary(product); }

    typename PrimitiveTypeTraits<T>::ColumnItemType get() const { return product; }

    void reset(typename PrimitiveTypeTraits<T>::ColumnItemType value) {
        product = std::move(value);
    }
};

template <PrimitiveType T, PrimitiveType TResult, typename Data>
class AggregateFunctionProduct
        : public IAggregateFunctionDataHelper<Data, AggregateFunctionProduct<T, TResult, Data>>,
          UnaryExpression,
          NullableAggregateFunction {
public:
    using ResultType = PrimitiveTypeTraits<TResult>::ColumnItemType;
    AggregateFunctionProduct(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionProduct<T, TResult, Data>>(
                      argument_types_) {}
    String get_name() const override {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "not implement get_name of AggregateFunctionProduct");
    }
    DataTypePtr get_return_type() const override {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "not implement get_return_type of AggregateFunctionProduct");
    }
    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena&) const override {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "not implement get_return_type of AggregateFunctionProduct");
    }
    void reset(AggregateDataPtr place) const override {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "not implement reset of AggregateFunctionProduct");
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena&) const override {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "not implement merge of AggregateFunctionProduct");
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "not implement serialize of AggregateFunctionProduct");
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena&) const override {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "not implement deserialize of AggregateFunctionProduct");
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "not implement insert_result_into of AggregateFunctionProduct");
    }

    void deserialize_from_column(AggregateDataPtr places, const IColumn& column, Arena&,
                                 size_t num_rows) const override {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "not implement deserialize_from_column of AggregateFunctionProduct");
    }

    void serialize_to_column(const std::vector<AggregateDataPtr>& places, size_t offset,
                             MutableColumnPtr& dst, const size_t num_rows) const override {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "not implement serialize_to_column of AggregateFunctionProduct");
    }

    void streaming_agg_serialize_to_column(const IColumn** columns, MutableColumnPtr& dst,
                                           const size_t num_rows, Arena&) const override {
        throw doris::Exception(
                ErrorCode::INTERNAL_ERROR,
                "not implement streaming_agg_serialize_to_column of AggregateFunctionProduct");
    }

    void deserialize_and_merge_from_column(AggregateDataPtr __restrict place, const IColumn& column,
                                           Arena&) const override {
        throw doris::Exception(
                ErrorCode::INTERNAL_ERROR,
                "not implement deserialize_and_merge_from_column of AggregateFunctionProduct");
    }

    void deserialize_and_merge_from_column_range(AggregateDataPtr __restrict place,
                                                 const IColumn& column, size_t begin, size_t end,
                                                 Arena&) const override {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "not implement deserialize_and_merge_from_column_range of "
                               "AggregateFunctionProduct");
    }

    void deserialize_and_merge_vec(const AggregateDataPtr* places, size_t offset,
                                   AggregateDataPtr rhs, const IColumn* column, Arena& arena,
                                   const size_t num_rows) const override {
        throw doris::Exception(
                ErrorCode::INTERNAL_ERROR,
                "not implement deserialize_and_merge_vec of AggregateFunctionProduct");
    }

    void deserialize_and_merge_vec_selected(const AggregateDataPtr* places, size_t offset,
                                            AggregateDataPtr rhs, const IColumn* column,
                                            Arena& arena, const size_t num_rows) const override {
        throw doris::Exception(
                ErrorCode::INTERNAL_ERROR,
                "not implement deserialize_and_merge_vec_selected of AggregateFunctionProduct");
    }

    void serialize_without_key_to_column(ConstAggregateDataPtr __restrict place,
                                         IColumn& to) const override {
        throw doris::Exception(
                ErrorCode::INTERNAL_ERROR,
                "not implement serialize_without_key_to_column of AggregateFunctionProduct");
    }

    MutableColumnPtr create_serialize_column() const override {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "not implement create_serialize_column of AggregateFunctionProduct");
    }

    DataTypePtr get_serialized_type() const override {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "not implement get_serialized_type of AggregateFunctionProduct");
    }

    bool supported_incremental_mode() const override {
        throw doris::Exception(
                ErrorCode::INTERNAL_ERROR,
                "not implement supported_incremental_mode of AggregateFunctionProduct");
    }

    NO_SANITIZE_UNDEFINED void execute_function_with_incremental(
            int64_t partition_start, int64_t partition_end, int64_t frame_start, int64_t frame_end,
            AggregateDataPtr place, const IColumn** columns, Arena& arena, bool previous_is_nul,
            bool end_is_nul, bool has_null, UInt8* use_null_result,
            UInt8* could_use_previous_result) const override {
        throw doris::Exception(
                ErrorCode::INTERNAL_ERROR,
                "not implement execute_function_with_incremental of AggregateFunctionProduct");
    }

    void add_range_single_place(int64_t partition_start, int64_t partition_end, int64_t frame_start,
                                int64_t frame_end, AggregateDataPtr place, const IColumn** columns,
                                Arena& arena, UInt8* use_null_result,
                                UInt8* could_use_previous_result) const override {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "not implement add_range_single_place of AggregateFunctionProduct");
    }
};

template <PrimitiveType T, PrimitiveType TResult>
constexpr static bool is_valid_product_types =
        (is_same_or_wider_decimalv3(T, TResult) ||
         (is_float_or_double(T) && is_float_or_double(TResult)) ||
         (is_int_or_bool(T) && is_float_or_double(TResult)));
template <PrimitiveType T, PrimitiveType TResult, typename Data>
    requires(is_valid_product_types<T, TResult>)
class AggregateFunctionProduct<T, TResult, Data> final
        : public IAggregateFunctionDataHelper<Data, AggregateFunctionProduct<T, TResult, Data>>,
          UnaryExpression,
          NullableAggregateFunction {
public:
    using ResultDataType = typename PrimitiveTypeTraits<TResult>::DataType;
    using ColVecType = typename PrimitiveTypeTraits<T>::ColumnType;
    using ColVecResult = typename PrimitiveTypeTraits<TResult>::ColumnType;

    std::string get_name() const override { return "product"; }

    AggregateFunctionProduct(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionProduct<T, TResult, Data>>(
                      argument_types_),
              scale(get_decimal_scale(*argument_types_[0])) {
        if constexpr (is_decimal(T)) {
            multiplier.value =
                    ResultDataType::get_scale_multiplier(get_decimal_scale(*argument_types_[0]));
        }
    }

    DataTypePtr get_return_type() const override {
        if constexpr (is_decimal(T)) {
            return std::make_shared<ResultDataType>(ResultDataType::max_precision(), scale);
        } else {
            return std::make_shared<ResultDataType>();
        }
    }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena&) const override {
        const auto& column =
                assert_cast<const ColVecType&, TypeCheckOnRelease::DISABLE>(*columns[0]);
        this->data(place).add(
                typename PrimitiveTypeTraits<TResult>::ColumnItemType(column.get_data()[row_num]),
                multiplier);
    }

    void reset(AggregateDataPtr place) const override {
        if constexpr (is_decimal(T)) {
            this->data(place).reset(multiplier);
        } else {
            this->data(place).reset(1);
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena&) const override {
        this->data(place).merge(this->data(rhs), multiplier);
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena&) const override {
        this->data(place).read(buf);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        auto& column = assert_cast<ColVecResult&>(to);
        column.get_data().push_back(this->data(place).get());
    }

private:
    UInt32 scale;
    typename PrimitiveTypeTraits<TResult>::ColumnItemType multiplier;
};

} // namespace vectorized
} // namespace doris

#include "common/compile_check_end.h"
