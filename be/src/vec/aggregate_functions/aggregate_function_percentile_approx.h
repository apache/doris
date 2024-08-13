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

#include <glog/logging.h>
#include <stddef.h>
#include <stdint.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <cmath>
#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include "util/counts.h"
#include "util/tdigest.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/common/pod_array_fwd.h"
#include "vec/common/string_ref.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/io/io_helper.h"

namespace doris {
namespace vectorized {
class Arena;
class BufferReadable;
class BufferWritable;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

struct OldPercentileState {
    std::vector<OldCounts> vec_counts;
    std::vector<double> vec_quantile {-1};
    bool inited_flag = false;

    void write(BufferWritable& buf) const {
        write_binary(inited_flag, buf);
        int size_num = vec_quantile.size();
        write_binary(size_num, buf);
        for (const auto& quantile : vec_quantile) {
            write_binary(quantile, buf);
        }
        std::string serialize_str;
        for (const auto& counts : vec_counts) {
            serialize_str.resize(counts.serialized_size(), '0');
            counts.serialize((uint8_t*)serialize_str.c_str());
            write_binary(serialize_str, buf);
        }
    }

    void read(BufferReadable& buf) {
        read_binary(inited_flag, buf);
        int size_num = 0;
        read_binary(size_num, buf);
        double data = 0.0;
        vec_quantile.clear();
        for (int i = 0; i < size_num; ++i) {
            read_binary(data, buf);
            vec_quantile.emplace_back(data);
        }
        StringRef ref;
        vec_counts.clear();
        vec_counts.resize(size_num);
        for (int i = 0; i < size_num; ++i) {
            read_binary(ref, buf);
            vec_counts[i].unserialize((uint8_t*)ref.data);
        }
    }

    void add(int64_t source, const PaddedPODArray<Float64>& quantiles, int arg_size) {
        if (!inited_flag) {
            vec_counts.resize(arg_size);
            vec_quantile.resize(arg_size, -1);
            inited_flag = true;
            for (int i = 0; i < arg_size; ++i) {
                vec_quantile[i] = quantiles[i];
            }
        }
        for (int i = 0; i < arg_size; ++i) {
            vec_counts[i].increment(source, 1);
        }
    }

    void merge(const OldPercentileState& rhs) {
        if (!rhs.inited_flag) {
            return;
        }
        int size_num = rhs.vec_quantile.size();
        if (!inited_flag) {
            vec_counts.resize(size_num);
            vec_quantile.resize(size_num, -1);
            inited_flag = true;
        }

        for (int i = 0; i < size_num; ++i) {
            if (vec_quantile[i] == -1.0) {
                vec_quantile[i] = rhs.vec_quantile[i];
            }
            vec_counts[i].merge(&(rhs.vec_counts[i]));
        }
    }

    void reset() {
        vec_counts.clear();
        vec_quantile.clear();
        inited_flag = false;
    }

    double get() const { return vec_counts.empty() ? 0 : vec_counts[0].terminate(vec_quantile[0]); }

    void insert_result_into(IColumn& to) const {
        auto& column_data = assert_cast<ColumnFloat64&>(to).get_data();
        for (int i = 0; i < vec_counts.size(); ++i) {
            column_data.push_back(vec_counts[i].terminate(vec_quantile[i]));
        }
    }
};

class AggregateFunctionPercentileOld final
        : public IAggregateFunctionDataHelper<OldPercentileState, AggregateFunctionPercentileOld> {
public:
    AggregateFunctionPercentileOld(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<OldPercentileState, AggregateFunctionPercentileOld>(
                      argument_types_) {}

    String get_name() const override { return "percentile"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeFloat64>(); }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena*) const override {
        const auto& sources =
                assert_cast<const ColumnVector<Int64>&, TypeCheckOnRelease::DISABLE>(*columns[0]);
        const auto& quantile =
                assert_cast<const ColumnFloat64&, TypeCheckOnRelease::DISABLE>(*columns[1]);
        AggregateFunctionPercentileOld::data(place).add(sources.get_int(row_num),
                                                        quantile.get_data(), 1);
    }

    void reset(AggregateDataPtr __restrict place) const override {
        AggregateFunctionPercentileOld::data(place).reset();
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena*) const override {
        AggregateFunctionPercentileOld::data(place).merge(
                AggregateFunctionPercentileOld::data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        AggregateFunctionPercentileOld::data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena*) const override {
        AggregateFunctionPercentileOld::data(place).read(buf);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        auto& col = assert_cast<ColumnFloat64&>(to);
        col.insert_value(AggregateFunctionPercentileOld::data(place).get());
    }
};

class AggregateFunctionPercentileArrayOld final
        : public IAggregateFunctionDataHelper<OldPercentileState,
                                              AggregateFunctionPercentileArrayOld> {
public:
    AggregateFunctionPercentileArrayOld(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<OldPercentileState, AggregateFunctionPercentileArrayOld>(
                      argument_types_) {}

    String get_name() const override { return "percentile_array"; }

    DataTypePtr get_return_type() const override {
        return std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeFloat64>()));
    }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena*) const override {
        const auto& sources =
                assert_cast<const ColumnVector<Int64>&, TypeCheckOnRelease::DISABLE>(*columns[0]);
        const auto& quantile_array =
                assert_cast<const ColumnArray&, TypeCheckOnRelease::DISABLE>(*columns[1]);
        const auto& offset_column_data = quantile_array.get_offsets();
        const auto& nested_column = assert_cast<const ColumnNullable&, TypeCheckOnRelease::DISABLE>(
                                            quantile_array.get_data())
                                            .get_nested_column();
        const auto& nested_column_data =
                assert_cast<const ColumnFloat64&, TypeCheckOnRelease::DISABLE>(nested_column);

        AggregateFunctionPercentileArrayOld::data(place).add(
                sources.get_int(row_num), nested_column_data.get_data(),
                offset_column_data.data()[row_num] - offset_column_data[(ssize_t)row_num - 1]);
    }

    void reset(AggregateDataPtr __restrict place) const override {
        AggregateFunctionPercentileArrayOld::data(place).reset();
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena*) const override {
        AggregateFunctionPercentileArrayOld::data(place).merge(
                AggregateFunctionPercentileArrayOld::data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        AggregateFunctionPercentileArrayOld::data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena*) const override {
        AggregateFunctionPercentileArrayOld::data(place).read(buf);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        auto& to_arr = assert_cast<ColumnArray&>(to);
        auto& to_nested_col = to_arr.get_data();
        if (to_nested_col.is_nullable()) {
            auto col_null = reinterpret_cast<ColumnNullable*>(&to_nested_col);
            AggregateFunctionPercentileArrayOld::data(place).insert_result_into(
                    col_null->get_nested_column());
            col_null->get_null_map_data().resize_fill(col_null->get_nested_column().size(), 0);
        } else {
            AggregateFunctionPercentileArrayOld::data(place).insert_result_into(to_nested_col);
        }
        to_arr.get_offsets().push_back(to_nested_col.size());
    }
};

} // namespace doris::vectorized