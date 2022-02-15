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

#include "exprs/anyval_util.h"
#include "olap/hll.h"
#include "udf/udf.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/common/string_ref.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {

struct AggregateFunctionApproxCountDistinctData {
    HyperLogLog hll_data;

    void add(StringRef value) {
        StringVal sv = value.to_string_val();
        uint64_t hash_value = AnyValUtil::hash64_murmur(sv, HashUtil::MURMUR_SEED);
        if (hash_value != 0) {
            hll_data.update(hash_value);
        }
    }

    void merge(const AggregateFunctionApproxCountDistinctData& rhs) {
        hll_data.merge(rhs.hll_data);
    }

    void write(BufferWritable& buf) const {
        std::string result;
        result.resize(hll_data.max_serialized_size());
        int size = hll_data.serialize((uint8_t*)result.data());
        result.resize(size);
        write_binary(result, buf);
    }

    void read(BufferReadable& buf) {
        StringRef result;
        read_binary(result, buf);
        Slice data = Slice(result.data, result.size);
        hll_data.deserialize(data);
    }

    int64_t get() const { return hll_data.estimate_cardinality(); }

    void reset() { hll_data.clear(); }
};

template <typename ColumnDataType>
class AggregateFunctionApproxCountDistinct final
        : public IAggregateFunctionDataHelper<
                  AggregateFunctionApproxCountDistinctData,
                  AggregateFunctionApproxCountDistinct<ColumnDataType>> {
public:
    String get_name() const override { return "approx_count_distinct"; }

    AggregateFunctionApproxCountDistinct(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<AggregateFunctionApproxCountDistinctData,
                                           AggregateFunctionApproxCountDistinct<ColumnDataType>>(
                      argument_types_, {}) {}

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeInt64>(); }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, size_t row_num,
             Arena*) const override {
        this->data(place).add(static_cast<const ColumnDataType*>(columns[0])->get_data_at(row_num));
    }

    void reset(AggregateDataPtr place) const override { this->data(place).reset(); }

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
        auto& column = static_cast<ColumnInt64&>(to);
        column.get_data().push_back(this->data(place).get());
    }
};

} // namespace doris::vectorized
