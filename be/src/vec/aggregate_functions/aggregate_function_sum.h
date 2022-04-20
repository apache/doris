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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/AggregateFunctions/AggregateFunctionSum.h
// and modified by Doris

#pragma once

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column_vector.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"
#include "vec/io/io_helper.h"

#ifdef DORIS_ENABLE_JIT
#include "vec/data_types/native.h"
#include <llvm/IR/IRBuilder.h>
#endif

namespace doris::vectorized {

template <typename T>
struct AggregateFunctionSumData {
    T sum {};

    void add(T value) { sum += value; }

    void merge(const AggregateFunctionSumData& rhs) { sum += rhs.sum; }

    void write(BufferWritable& buf) const { write_binary(sum, buf); }

    void read(BufferReadable& buf) { read_binary(sum, buf); }

    T get() const { return sum; }
};

/// Counts the sum of the numbers.
template <typename T, typename TResult, typename Data>
class AggregateFunctionSum final
        : public IAggregateFunctionDataHelper<Data, AggregateFunctionSum<T, TResult, Data>> {
public:
    using ResultDataType = std::conditional_t<IsDecimalNumber<T>, DataTypeDecimal<TResult>,
                                              DataTypeNumber<TResult>>;
    using ColVecType = std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<T>, ColumnVector<T>>;
    using ColVecResult =
            std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<TResult>, ColumnVector<TResult>>;

    String get_name() const override { return "sum"; }

    AggregateFunctionSum(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionSum<T, TResult, Data>>(
                      argument_types_, {}),
              scale(0) {}

    AggregateFunctionSum(const IDataType& data_type, const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionSum<T, TResult, Data>>(
                      argument_types_, {}),
              scale(get_decimal_scale(data_type)) {}

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
        this->data(place).add(column.get_data()[row_num]);
    }

    void reset(AggregateDataPtr place) const override { this->data(place).sum = {}; }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena*) const override {
        this->data(place).merge(this->data(rhs));
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
        column.get_data().push_back(this->data(place).get());
    }

#ifdef DORIS_ENABLE_JIT
    virtual void compile_create(llvm::IRBuilderBase& builder, llvm::Value* aggregate_data_ptr) const override {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        auto * return_type = to_native_type(b, get_return_type());
        auto * aggregate_sum_ptr = b.CreatePointerCast(aggregate_data_ptr, return_type->getPointerTo());

        b.CreateStore(llvm::Constant::getNullValue(return_type), aggregate_sum_ptr);
    }

    virtual void compile_add(llvm::IRBuilderBase& builder,
                             llvm::Value* aggregate_data_ptr,
                             const DataTypes& arguments_types,
                             const std::vector<llvm::Value *>&
                             argument_values) const override {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        auto * return_type = to_native_type(b, get_return_type());

        auto * sum_value_ptr = b.CreatePointerCast(aggregate_data_ptr, return_type->getPointerTo());
        auto * sum_value = b.CreateLoad(return_type, sum_value_ptr);

        const auto & argument_type = arguments_types[0];
        const auto & argument_value = argument_values[0];

        auto * value_cast_to_result = native_cast(b, argument_type, argument_value, return_type);
        auto * sum_result_value = sum_value->getType()->isIntegerTy() ? b.CreateAdd(sum_value, value_cast_to_result) : b.CreateFAdd(sum_value, value_cast_to_result);

        b.CreateStore(sum_result_value, sum_value_ptr);
    }

    virtual void compile_merge(llvm::IRBuilderBase& builder, llvm::Value* aggregate_data_dst_ptr, llvm::Value* aggregate_data_src_ptr) const override {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        auto * return_type = to_native_type(b, get_return_type());

        auto * sum_value_dst_ptr = b.CreatePointerCast(aggregate_data_dst_ptr, return_type->getPointerTo());
        auto * sum_value_dst = b.CreateLoad(return_type, sum_value_dst_ptr);

        auto * sum_value_src_ptr = b.CreatePointerCast(aggregate_data_src_ptr, return_type->getPointerTo());
        auto * sum_value_src = b.CreateLoad(return_type, sum_value_src_ptr);

        auto * sum_return_value = sum_value_dst->getType()->isIntegerTy() ? b.CreateAdd(sum_value_dst, sum_value_src) : b.CreateFAdd(sum_value_dst, sum_value_src);
        b.CreateStore(sum_return_value, sum_value_dst_ptr);
    }

    virtual llvm::Value* compile_get_result(llvm::IRBuilderBase& builder, llvm::Value* aggregate_data_ptr) const override {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        auto * return_type = to_native_type(b, get_return_type());
        auto * sum_value_ptr = b.CreatePointerCast(aggregate_data_ptr, return_type->getPointerTo());

        return b.CreateLoad(return_type, sum_value_ptr);
    }

    virtual bool is_compilable() const override {
        bool can_be_compiled = true;
        for (const auto& argument_type : this->argument_types)
            can_be_compiled &= can_be_native_type(*argument_type);
        
        auto return_type = get_return_type();
        can_be_compiled &= can_be_native_type(*return_type);
        return can_be_compiled;
    }
#endif

private:
    UInt32 scale;
};

AggregateFunctionPtr create_aggregate_function_sum_reader(const std::string& name,
                                                          const DataTypes& argument_types,
                                                          const Array& parameters,
                                                          const bool result_is_nullable);

} // namespace doris::vectorized
