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

#include "olap/ds_hll.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_number.h"
#include "vec/io/io_helper.h"

namespace doris {
#include "common/compile_check_begin.h"
namespace vectorized {
class Arena;
class BufferReadable;
class BufferWritable;
class IColumn;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {
struct AggregateFunctionDSHllCountDistinctData {
    std::unique_ptr<DSHyperLogLog> hll_data = nullptr;

    void init() {
        if (!hll_data) {
            hll_data = std::make_unique<DSHyperLogLog>();
        }
    }

    void init(uint8_t lg_k) {
        if (!hll_data) {
            hll_data = std::make_unique<DSHyperLogLog>(lg_k);
        }
    }

    void init(uint8_t lg_k, std::string& lg_hll_type_str) {
        if (!hll_data) {
            hll_data = std::make_unique<DSHyperLogLog>(lg_k, lg_hll_type_str);
        }
    }

    // should call init before this method
    void update(const void* data, size_t length_bytes) {
        if (length_bytes > 0) {
            hll_data->update(data, length_bytes);
         }
     }


    void merge(const AggregateFunctionDSHllCountDistinctData& rhs) {
        if (!(rhs.hll_data)) {
            return;
        }
        if (!hll_data) {
            hll_data = std::make_unique<DSHyperLogLog>(rhs.hll_data->get_lg_config_k(),
                                                       rhs.hll_data->get_hll_type());
        }
        hll_data->merge(*rhs.hll_data);
    }

    void write(BufferWritable& buf) const {
        if (hll_data) {
            ds_vector_bytes result;
            hll_data->serialize(result);
            write_binary(StringRef(result.data(), result.size()), buf);
        } else {
            write_binary("", buf);
        }
    }

    void read(BufferReadable& buf) {
        StringRef result;
        read_binary(result, buf);
        if (!result.empty()) {
            Slice data = Slice(result.data, result.size);
            hll_data = std::make_unique<DSHyperLogLog>(data);
        }
    }

    int64_t get() const {
        if (hll_data) {
            return hll_data->estimate_cardinality();
        }
        return 0;
    }

    void reset() {
        if (hll_data) {
            hll_data->clear();
            hll_data.reset();
        }
    }
};

template <typename ColumnDataType, int param_size>
class AggregateFunctionDSHllCountDistinct final
        : public IAggregateFunctionDataHelper<
                  AggregateFunctionDSHllCountDistinctData,
                  AggregateFunctionDSHllCountDistinct<ColumnDataType, param_size>> {
public:
    String get_name() const override { return "ds_hll_count_distinct"; }
    AggregateFunctionDSHllCountDistinct(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<
                      AggregateFunctionDSHllCountDistinctData,
                      AggregateFunctionDSHllCountDistinct<ColumnDataType, param_size>>(
                      argument_types_) {}

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeInt64>(); }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena*) const override {
        if constexpr (param_size == 1) {
            this->data(place).init();
        }
        if constexpr (param_size == 2) {
            const auto& lg_k_column =
                    assert_cast<const ColumnInt8&, TypeCheckOnRelease::DISABLE>(*columns[1]);
            auto lg_k = lg_k_column.get_element(0);
            this->data(place).init(lg_k);
        }
        if constexpr (param_size == 3) {
            const auto& lg_k_column =
                    assert_cast<const ColumnInt8&, TypeCheckOnRelease::DISABLE>(*columns[1]);
            auto lg_k = lg_k_column.get_element(0);
            const auto& lg_hll_type_column =
                    assert_cast<const ColumnString&, TypeCheckOnRelease::DISABLE>(*columns[2]);
            const auto& lg_hll_type_str_ref = lg_hll_type_column.get_data_at(0);
            auto lg_hll_type_str = lg_hll_type_str_ref.to_string();
            this->data(place).init(lg_k, lg_hll_type_str);
        }

        if constexpr (IsFixLenColumnType<ColumnDataType>::value) {
            auto column =
                    assert_cast<const ColumnDataType*, TypeCheckOnRelease::DISABLE>(columns[0]);
            auto value = column->get_element(row_num);
            this->data(place).update((void*)&value, sizeof(value));
        } else {
            auto value = assert_cast<const ColumnDataType*, TypeCheckOnRelease::DISABLE>(columns[0])
                                 ->get_data_at(row_num);
            this->data(place).update(value.data, value.size);
        }
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
        auto& column = assert_cast<ColumnInt64&>(to);
        column.get_data().push_back(this->data(place).get());
    }
};
} // namespace doris::vectorized

#include "common/compile_check_end.h"