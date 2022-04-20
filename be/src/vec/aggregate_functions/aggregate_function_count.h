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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/AggregateFunctions/AggregateFunctionCount.h
// and modified by Doris

#pragma once

#include <array>

#include "common/logging.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column_nullable.h"
#include "vec/common/assert_cast.h"
#include "vec/data_types/data_type_number.h"
#include "vec/io/io_helper.h"

#ifdef DORIS_ENABLE_JIT
#include "vec/data_types/native.h"
#endif

namespace doris::vectorized {

struct AggregateFunctionCountData {
    UInt64 count = 0;
};

/// Simply count number of calls.
class AggregateFunctionCount final
        : public IAggregateFunctionDataHelper<AggregateFunctionCountData, AggregateFunctionCount> {
public:
    AggregateFunctionCount(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper(argument_types_, {}) {}

    String get_name() const override { return "count"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeInt64>(); }

    void add(AggregateDataPtr __restrict place, const IColumn**, size_t, Arena*) const override {
        ++data(place).count;
    }

    void reset(AggregateDataPtr place) const override {
        AggregateFunctionCount::data(place).count = 0;
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena*) const override {
        data(place).count += data(rhs).count;
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        write_var_uint(data(place).count, buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena*) const override {
        read_var_uint(data(place).count, buf);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        assert_cast<ColumnInt64&>(to).get_data().push_back(data(place).count);
    }

#ifdef DORIS_ENABLE_JIT
    bool is_compilable() const override {
        return true;
    }

    void compile_create(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr) const override {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);
        b.CreateMemSet(aggregate_data_ptr, llvm::ConstantInt::get(b.getInt8Ty(), 0), sizeof(AggregateFunctionCountData), llvm::assumeAligned(alignof(Data)));
    }

    void compile_add(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr, const DataTypes &, const std::vector<llvm::Value *> & values) const override {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        auto * return_type = to_native_type(b, get_return_type());

        auto * count_value_ptr = b.CreatePointerCast(aggregate_data_ptr, return_type->getPointerTo());
        auto * count_value = b.CreateLoad(return_type, count_value_ptr);
        auto * updated_count_value = b.CreateAdd(count_value, llvm::ConstantInt::get(return_type, 1));

        b.CreateStore(updated_count_value, count_value_ptr);
    }

    void compile_merge(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_dst_ptr, llvm::Value * aggregate_data_src_ptr) const override {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        auto * return_type = to_native_type(b, get_return_type());

        auto * count_value_dst_ptr = b.CreatePointerCast(aggregate_data_dst_ptr, return_type->getPointerTo());
        auto * count_value_dst = b.CreateLoad(return_type, count_value_dst_ptr);

        auto * count_value_src_ptr = b.CreatePointerCast(aggregate_data_src_ptr, return_type->getPointerTo());
        auto * count_value_src = b.CreateLoad(return_type, count_value_src_ptr);

        auto * count_value_dst_updated = b.CreateAdd(count_value_dst, count_value_src);

        b.CreateStore(count_value_dst_updated, count_value_dst_ptr);
    }

    llvm::Value * compile_get_result(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr) const override {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        auto * return_type = to_native_type(b, get_return_type());
        auto * count_value_ptr = b.CreatePointerCast(aggregate_data_ptr, return_type->getPointerTo());

        return b.CreateLoad(return_type, count_value_ptr);
    }
#endif

};

/// Simply count number of not-NULL values.
class AggregateFunctionCountNotNullUnary final
        : public IAggregateFunctionDataHelper<AggregateFunctionCountData,
                                              AggregateFunctionCountNotNullUnary> {
public:
    AggregateFunctionCountNotNullUnary(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper(argument_types_, {}) {}

    String get_name() const override { return "count"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeInt64>(); }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, size_t row_num,
             Arena*) const override {
        data(place).count += !assert_cast<const ColumnNullable&>(*columns[0]).is_null_at(row_num);
    }

    void reset(AggregateDataPtr place) const override { data(place).count = 0; }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena*) const override {
        data(place).count += data(rhs).count;
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        write_var_uint(data(place).count, buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena*) const override {
        read_var_uint(data(place).count, buf);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        if (to.is_nullable()) {
            auto& null_column = assert_cast<ColumnNullable&>(to);
            null_column.get_null_map_data().push_back(0);
            assert_cast<ColumnInt64&>(null_column.get_nested_column())
                    .get_data()
                    .push_back(data(place).count);
        } else {
            assert_cast<ColumnInt64&>(to).get_data().push_back(data(place).count);
        }
    }

#ifdef DORIS_ENABLE_JIT
    bool is_compilable() const override {
        return true;
    }

    void compile_create(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr) const override {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);
        b.CreateMemSet(aggregate_data_ptr, llvm::ConstantInt::get(b.getInt8Ty(), 0), sizeof(AggregateFunctionCountData), llvm::assumeAligned(alignof(Data)));
    }

    void compile_add(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr, const DataTypes &, const std::vector<llvm::Value *> & values) const override {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        auto * return_type = to_native_type(b, get_return_type());

        auto * is_null_value = b.CreateExtractValue(values[0], {1});
        auto * increment_value = b.CreateSelect(is_null_value, llvm::ConstantInt::get(return_type, 0), llvm::ConstantInt::get(return_type, 1));

        auto * count_value_ptr = b.CreatePointerCast(aggregate_data_ptr, return_type->getPointerTo());
        auto * count_value = b.CreateLoad(return_type, count_value_ptr);
        auto * updated_count_value = b.CreateAdd(count_value, increment_value);

        b.CreateStore(updated_count_value, count_value_ptr);
    }

    void compile_merge(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_dst_ptr, llvm::Value * aggregate_data_src_ptr) const override {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        auto * return_type = to_native_type(b, get_return_type());

        auto * count_value_dst_ptr = b.CreatePointerCast(aggregate_data_dst_ptr, return_type->getPointerTo());
        auto * count_value_dst = b.CreateLoad(return_type, count_value_dst_ptr);

        auto * count_value_src_ptr = b.CreatePointerCast(aggregate_data_src_ptr, return_type->getPointerTo());
        auto * count_value_src = b.CreateLoad(return_type, count_value_src_ptr);

        auto * count_value_dst_updated = b.CreateAdd(count_value_dst, count_value_src);

        b.CreateStore(count_value_dst_updated, count_value_dst_ptr);
    }

    llvm::Value * compile_get_result(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr) const override {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        auto * return_type = to_native_type(b, get_return_type());
        auto * count_value_ptr = b.CreatePointerCast(aggregate_data_ptr, return_type->getPointerTo());

        return b.CreateLoad(return_type, count_value_ptr);
    }
#endif

};

} // namespace doris::vectorized
