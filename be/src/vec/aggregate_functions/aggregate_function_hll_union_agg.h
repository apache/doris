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

#include <stddef.h>
#include <stdint.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <memory>
#include <string>

#include "olap/hll.h"
#include "util/slice.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column_complex.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_ref.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_hll.h"
#include "vec/data_types/data_type_number.h"
#include "vec/io/io_helper.h"

namespace doris {
namespace vectorized {
class Arena;
class BufferReadable;
class BufferWritable;
class IColumn;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

struct AggregateFunctionHLLData {
    HyperLogLog dst_hll {};

    AggregateFunctionHLLData() = default;
    ~AggregateFunctionHLLData() = default;

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

    HyperLogLog get() const { return dst_hll; }

    void reset() { dst_hll.clear(); }

    void add(const IColumn* column, size_t row_num) {
        const auto& sources = assert_cast<const ColumnHLL&, TypeCheckOnRelease::DISABLE>(*column);
        dst_hll.merge(sources.get_element(row_num));
    }
};

template <typename Data>
struct AggregateFunctionHLLUnionImpl : Data {
    void insert_result_into(IColumn& to) const {
        ColumnHLL& column = assert_cast<ColumnHLL&>(to);
        column.get_data().emplace_back(this->get());
    }

    static DataTypePtr get_return_type() { return std::make_shared<DataTypeHLL>(); }

    static const char* name() { return "hll_union"; }
};

template <typename Data>
struct AggregateFunctionHLLUnionAggImpl : Data {
    void insert_result_into(IColumn& to) const {
        ColumnInt64& column = assert_cast<ColumnInt64&>(to);
        column.get_data().emplace_back(this->get_cardinality());
    }

    static DataTypePtr get_return_type() { return std::make_shared<DataTypeInt64>(); }

    static const char* name() { return "hll_union_agg"; }
};

template <typename Data>
class AggregateFunctionHLLUnion
        : public IAggregateFunctionDataHelper<Data, AggregateFunctionHLLUnion<Data>> {
public:
    AggregateFunctionHLLUnion(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionHLLUnion<Data>>(argument_types_) {
    }

    String get_name() const override { return Data::name(); }

    DataTypePtr get_return_type() const override { return Data::get_return_type(); }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        this->data(place).insert_result_into(to);
    }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena* arena) const override {
        this->data(place).add(columns[0], row_num);
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

    void reset(AggregateDataPtr __restrict place) const override { this->data(place).reset(); }
};

template <template <typename> class Impl>
AggregateFunctionPtr create_aggregate_function_HLL(const std::string& name,
                                                   const DataTypes& argument_types,
                                                   const bool result_is_nullable);

} // namespace doris::vectorized
