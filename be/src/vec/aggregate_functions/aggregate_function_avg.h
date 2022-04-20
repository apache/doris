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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/AggregateFunctions/AggregateFunctionAvg.h
// and modified by Doris

#pragma once

#include "common/status.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/columns_number.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"
#include "vec/io/io_helper.h"
#ifdef DORIS_ENABLE_JIT
#include "vec/data_types/native.h"
#endif

namespace doris::vectorized {

template <typename T>
struct AggregateFunctionAvgData {
    using NumeratorType = T;
    T sum = 0;
    UInt64 count = 0;

    template <typename ResultT>
    ResultT result() const {
        if constexpr (std::is_floating_point_v<ResultT>) {
            if constexpr (std::numeric_limits<ResultT>::is_iec559) {
                return static_cast<ResultT>(sum) / count; /// allow division by zero
            }
        }

        if (!count) {
            // null is handled in AggregationNode::_get_without_key_result
            return static_cast<ResultT>(sum);
        }
        // to keep the same result with row vesion; see AggregateFunctions::decimalv2_avg_get_value
        if constexpr (std::is_same_v<ResultT, Decimal128> && std::is_same_v<T, Decimal128>) {
            DecimalV2Value decimal_val_count(count, 0);
            DecimalV2Value decimal_val_sum(static_cast<Int128>(sum));
            DecimalV2Value cal_ret = decimal_val_sum / decimal_val_count;
            Decimal128 ret(cal_ret.value());
            return ret;
        }
        return static_cast<ResultT>(sum) / count;
    }

    void write(BufferWritable& buf) const {
        write_binary(sum, buf);
        write_binary(count, buf);
    }

    void read(BufferReadable& buf) {
        read_binary(sum, buf);
        read_binary(count, buf);
    }
};

/// Calculates arithmetic mean of numbers.
template <typename T, typename Data>
class AggregateFunctionAvg final
        : public IAggregateFunctionDataHelper<Data, AggregateFunctionAvg<T, Data>> {
public:
    using ResultType = std::conditional_t<IsDecimalNumber<T>, Decimal128, Float64>;
    using ResultDataType = std::conditional_t<IsDecimalNumber<T>, DataTypeDecimal<Decimal128>,
                                              DataTypeNumber<Float64>>;
    using ColVecType = std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<T>, ColumnVector<T>>;
    using ColVecResult = std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<Decimal128>,
                                            ColumnVector<Float64>>;
    using NumeratorType = typename Data::NumeratorType;
    using DenominatorType = UInt64;
    /// ctor for native types
    AggregateFunctionAvg(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionAvg<T, Data>>(argument_types_,
                                                                                {}),
              scale(0) {}

    /// ctor for Decimals
    AggregateFunctionAvg(const IDataType& data_type, const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionAvg<T, Data>>(argument_types_,
                                                                                {}),
              scale(get_decimal_scale(data_type)) {}

    String get_name() const override { return "avg"; }

    DataTypePtr get_return_type() const override {
        if constexpr (IsDecimalNumber<T>) {
            return std::make_shared<ResultDataType>(ResultDataType::max_precision(), scale);
        } else {
            return std::make_shared<ResultDataType>();
        }
    }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, size_t row_num,
             Arena*) const override {
        const auto& column = static_cast<const ColVecType&>(*columns[0]);
        this->data(place).sum += column.get_data()[row_num];
        ++this->data(place).count;
    }

    void reset(AggregateDataPtr place) const override {
        this->data(place).sum = 0;
        this->data(place).count = 0;
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena*) const override {
        this->data(place).sum += this->data(rhs).sum;
        this->data(place).count += this->data(rhs).count;
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena*) const override {
        this->data(place).read(buf);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        auto& column = static_cast<ColVecResult&>(to);
        column.get_data().push_back(this->data(place).template result<ResultType>());
    }

#ifdef DORIS_ENABLE_JIT
    virtual void compile_create(llvm::IRBuilderBase& builder, llvm::Value* aggregate_data_ptr) const override {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);
        b.CreateMemSet(aggregate_data_ptr, llvm::ConstantInt::get(b.getInt8Ty(), 0), sizeof(Data), llvm::assumeAligned(alignof(Data)));
    }

    virtual void compile_add(llvm::IRBuilderBase& builder,
                             llvm::Value* aggregate_data_ptr,
                             const DataTypes& arguments_types,
                             const std::vector<llvm::Value *>&
                             argument_values) const override {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        if constexpr (!can_be_native_type<NumeratorType>())
            return;

        auto* numerator_type = to_native_type<NumeratorType>(b);
        auto* numerator_ptr = b.CreatePointerCast(aggregate_data_ptr, numerator_type->getPointerTo());
        auto* numerator_value = b.CreateLoad(numerator_type, numerator_ptr);
        auto* value_cast_to_numerator = native_cast(b, arguments_types[0], argument_values[0], numerator_type);
        auto* numerator_result_value = numerator_type->isIntegerTy() ? b.CreateAdd(numerator_value, value_cast_to_numerator) : b.CreateFAdd(numerator_value, value_cast_to_numerator);
        b.CreateStore(numerator_result_value, numerator_ptr);

        auto * denominator_type = to_native_type<DenominatorType>(b);
        static constexpr size_t denominator_offset = offsetof(Data, count);
        auto * denominator_ptr = b.CreatePointerCast(b.CreateConstGEP1_32(aggregate_data_ptr->getType()->getPointerElementType(), aggregate_data_ptr, denominator_offset), denominator_type->getPointerTo());
        auto * denominator_value_updated = b.CreateAdd(b.CreateLoad(denominator_type, denominator_ptr), llvm::ConstantInt::get(denominator_type, 1));
        b.CreateStore(denominator_value_updated, denominator_ptr);
    }

    virtual void compile_merge(llvm::IRBuilderBase& builder, llvm::Value* aggregate_data_dst_ptr, llvm::Value* aggregate_data_src_ptr) const override {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        if constexpr (!can_be_native_type<NumeratorType>())
            return;

        auto* numerator_type = to_native_type<NumeratorType>(b);
        auto* numerator_dst_ptr = b.CreatePointerCast(aggregate_data_dst_ptr, numerator_type->getPointerTo());
        auto* numerator_dst_value = b.CreateLoad(numerator_type, numerator_dst_ptr);

        auto* numerator_src_ptr = b.CreatePointerCast(aggregate_data_src_ptr, numerator_type->getPointerTo());
        auto* numerator_src_value = b.CreateLoad(numerator_type, numerator_src_ptr);

        auto* numerator_result_value = numerator_type->isIntegerTy() ? b.CreateAdd(numerator_dst_value, numerator_src_value) : b.CreateFAdd(numerator_dst_value, numerator_src_value);
        b.CreateStore(numerator_result_value, numerator_dst_ptr);

        auto* denominator_type = to_native_type<DenominatorType>(b);
        static constexpr size_t denominator_offset = offsetof(Data, count);
        auto* denominator_dst_ptr = b.CreatePointerCast(b.CreateConstInBoundsGEP1_64(aggregate_data_dst_ptr->getType()->getPointerElementType(), aggregate_data_dst_ptr, denominator_offset), denominator_type->getPointerTo());
        auto* denominator_src_ptr = b.CreatePointerCast(b.CreateConstInBoundsGEP1_64(aggregate_data_dst_ptr->getType()->getPointerElementType(), aggregate_data_src_ptr, denominator_offset), denominator_type->getPointerTo());

        auto* denominator_dst_value = b.CreateLoad(denominator_type, denominator_dst_ptr);
        auto* denominator_src_value = b.CreateLoad(denominator_type, denominator_src_ptr);

        auto* denominator_result_value = denominator_type->isIntegerTy() ? b.CreateAdd(denominator_src_value, denominator_dst_value) : b.CreateFAdd(denominator_src_value, denominator_dst_value);
        b.CreateStore(denominator_result_value, denominator_dst_ptr);
    }

virtual llvm::Value* compile_get_result(llvm::IRBuilderBase& builder, llvm::Value* aggregate_data_ptr) const override {

        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        if constexpr (!can_be_native_type<NumeratorType>())
            return nullptr;

        auto* numerator_type = to_native_type<NumeratorType>(b);
        auto* numerator_ptr = b.CreatePointerCast(aggregate_data_ptr, numerator_type->getPointerTo());
        auto* numerator_value = b.CreateLoad(numerator_type, numerator_ptr);

        auto* denominator_type = to_native_type<DenominatorType>(b);
        static constexpr size_t denominator_offset = offsetof(Data, count);
        auto* denominator_ptr = b.CreatePointerCast(b.CreateConstGEP1_32(aggregate_data_ptr->getType()->getPointerElementType(), aggregate_data_ptr, denominator_offset), denominator_type->getPointerTo());
        auto* denominator_value = b.CreateLoad(denominator_type, denominator_ptr);

        auto* double_numerator = native_cast<NumeratorType>(b, numerator_value, b.getDoubleTy());
        auto* double_denominator = native_cast<DenominatorType>(b, denominator_value, b.getDoubleTy());

        return b.CreateFDiv(double_numerator, double_denominator);
    }

    virtual bool is_compilable() const override {
        if constexpr (!can_be_native_type<NumeratorType>())
            return false;
        auto return_type = get_return_type();
        return can_be_native_type(*return_type);
    }
#endif

private:
    UInt32 scale;
};

} // namespace doris::vectorized
