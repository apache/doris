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

#include "exprs/hll_function.h"
#include "olap/hll.h"
#include "util/slice.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/io/io_helper.h"
#include "vec/data_types/data_type_hll.h"

namespace doris::vectorized {

struct AggregateFunctionHLLData {
    HyperLogLog dst_hll {};

    void add(const HyperLogLog& src) { dst_hll.merge(src); }

    void merge(const AggregateFunctionHLLData& rhs) { dst_hll.merge(rhs.dst_hll); }

    void write(BufferWritable& buf) const {
        std::string result(dst_hll.max_serialized_size(), '0');
        int size = dst_hll.serialize((uint8_t*)result.c_str());
        result.resize(size);
        write_binary(result, buf);
    }

    void read(BufferReadable& buf) {
        StringRef ref;
        read_binary(ref, buf);
        dst_hll.deserialize(Slice(ref.data, ref.size));
    }

    Int64 get_cardinality() const { return dst_hll.estimate_cardinality(); }

    HyperLogLog get() const {
        return dst_hll;
    }

};

class AggregateFunctionHLLUnionAgg
        : public IAggregateFunctionDataHelper<AggregateFunctionHLLData,
                                              AggregateFunctionHLLUnionAgg> {
public:
    virtual String get_name() const override { return "hll_union_agg"; }

    AggregateFunctionHLLUnionAgg(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper(argument_types_, {}) {}

    AggregateFunctionHLLUnionAgg(const IDataType& data_type, const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper(argument_types_, {}) {}

    virtual DataTypePtr get_return_type() const override {
        return std::make_shared<DataTypeInt64>();
    }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, size_t row_num,
             Arena*) const override {
        const auto& column = static_cast<const ColumnHLL&>(*columns[0]);
        this->data(place).add(column.get_element(row_num));
    }

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

    virtual void insert_result_into(ConstAggregateDataPtr __restrict place,
                                    IColumn& to) const override {
        auto& column = static_cast<ColumnVector<Int64>&>(to);
        column.get_data().push_back(this->data(place).get_cardinality());
    }
};

class AggregateFunctionHLLUnion final : public AggregateFunctionHLLUnionAgg {
public:
    String get_name() const override { return "hll_union"; }

    AggregateFunctionHLLUnion(const DataTypes& argument_types_)
            : AggregateFunctionHLLUnionAgg {argument_types_} {}

    AggregateFunctionHLLUnion(const IDataType& data_type, const DataTypes& argument_types_)
            : AggregateFunctionHLLUnionAgg(data_type, argument_types_) {}

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeHLL>(); } 

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        auto& column = static_cast<ColumnHLL&>(to);
        column.get_data().emplace_back(this->data(place).get());
    }
};

AggregateFunctionPtr create_aggregate_function_HLL_union(const std::string& name,
                                                         const DataTypes& argument_types,
                                                         const Array& parameters,
                                                         const bool result_is_nullable);

} // namespace doris::vectorized
