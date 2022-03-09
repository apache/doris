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
#include "vec/data_types/data_type_hll.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {

template <bool is_nullable>
struct AggregateFunctionHLLData {
    HyperLogLog dst_hll {};

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

    void result_union_agg_impl(IColumn& to) const {
        if constexpr (is_nullable) {
            auto& null_column = assert_cast<ColumnNullable&>(to);
            null_column.get_null_map_data().push_back(0);
            assert_cast<ColumnInt64&>(null_column.get_nested_column())
                    .get_data()
                    .push_back(get_cardinality());
        } else {
            assert_cast<ColumnInt64&>(to).get_data().push_back(get_cardinality());
        }
    }

    void result_union_impl(IColumn& to) const {
        if constexpr (is_nullable) {
            auto& null_column = assert_cast<ColumnNullable&>(to);
            null_column.get_null_map_data().push_back(0);
            assert_cast<ColumnHLL&>(null_column.get_nested_column()).get_data().emplace_back(get());
        } else {
            auto& column = static_cast<ColumnHLL&>(to);
            column.get_data().emplace_back(get());
        }
    }

    static const DataTypePtr return_type_union_agg() {
        if constexpr (is_nullable) {
            return make_nullable(std::make_shared<DataTypeInt64>());
        } else {
            return std::make_shared<DataTypeInt64>();
        }
    }

    static const DataTypePtr return_type_union() {
        if constexpr (is_nullable) {
            return make_nullable(std::make_shared<DataTypeHLL>());
        } else {
            return std::make_shared<DataTypeHLL>();
        }
    }

    void add(const IColumn* column, size_t row_num) {
        if (auto* nullable_column = check_and_get_column<const ColumnNullable>(*column)) {
            if (nullable_column->is_null_at(row_num)) {
                return;
            }
            const auto& sources =
                    static_cast<const ColumnHLL&>((nullable_column->get_nested_column()));
            dst_hll.merge(sources.get_element(row_num));
        } else {
            const auto& sources = static_cast<const ColumnHLL&>(*column);
            dst_hll.merge(sources.get_element(row_num));
        }
    }
};

template <typename Data>
struct AggregateFunctionHLLUnionImpl : Data {
    void insert_result_into(IColumn& to) const { this->result_union_impl(to); }

    static DataTypePtr get_return_type() { return Data::return_type_union(); }

    static const char* name() { return "hll_union"; }
};

template <typename Data>
struct AggregateFunctionHLLUnionAggImpl : Data {
    void insert_result_into(IColumn& to) const { this->result_union_agg_impl(to); }

    static DataTypePtr get_return_type() { return Data::return_type_union_agg(); }

    static const char* name() { return "hll_union_agg"; }
};

template <typename Data>
class AggregateFunctionHLLUnion
        : public IAggregateFunctionDataHelper<Data, AggregateFunctionHLLUnion<Data>> {
public:
    AggregateFunctionHLLUnion(const DataTypes& argument_types)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionHLLUnion<Data>>(argument_types,
                                                                                  {}) {}

    String get_name() const override { return Data::name(); }

    DataTypePtr get_return_type() const override { return Data::get_return_type(); }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        this->data(place).insert_result_into(to);
    }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, size_t row_num,
             Arena* arena) const override {
        this->data(place).add(columns[0], row_num);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena*) const override {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf, Arena*) const override {
        this->data(place).read(buf);
    }
};

template <bool is_nullable = false>
AggregateFunctionPtr create_aggregate_function_HLL_union(const std::string& name,
                                                         const DataTypes& argument_types,
                                                         const Array& parameters,
                                                         const bool result_is_nullable);

} // namespace doris::vectorized
