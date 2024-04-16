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

#include <rapidjson/stringbuffer.h>
#include <stddef.h>

#include <iterator>
#include <map>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/columns/column.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_string.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_ref.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_string.h"
#include "vec/io/io_helper.h"
#include "vec/utils/histogram_helpers.hpp"

namespace doris {
namespace vectorized {
class Arena;
class BufferReadable;
class BufferWritable;
template <typename>
class ColumnVector;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

template <typename T>
struct AggregateFunctionHistogramData {
    using ColVecType =
            std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<Decimal128V2>, ColumnVector<T>>;

    void set_parameters(int input_max_num_buckets) {
        if (input_max_num_buckets > 0) {
            max_num_buckets = (size_t)input_max_num_buckets;
        }
    }

    void reset() { ordered_map.clear(); }

    void add(const StringRef& value, const UInt64& number = 1) {
        std::string data = value.to_string();
        auto it = ordered_map.find(data);
        if (it != ordered_map.end()) {
            it->second = it->second + number;
        } else {
            ordered_map.insert({data, number});
        }
    }

    void add(const T& value, const UInt64& number = 1) {
        auto it = ordered_map.find(value);
        if (it != ordered_map.end()) {
            it->second = it->second + number;
        } else {
            ordered_map.insert({value, number});
        }
    }

    void merge(const AggregateFunctionHistogramData& rhs) {
        if (!rhs.max_num_buckets) {
            return;
        }

        max_num_buckets = rhs.max_num_buckets;

        for (auto rhs_it : rhs.ordered_map) {
            auto lhs_it = ordered_map.find(rhs_it.first);
            if (lhs_it != ordered_map.end()) {
                lhs_it->second += rhs_it.second;
            } else {
                ordered_map.insert({rhs_it.first, rhs_it.second});
            }
        }
    }

    void write(BufferWritable& buf) const {
        write_binary(max_num_buckets, buf);

        size_t element_number = (size_t)ordered_map.size();
        write_binary(element_number, buf);

        auto pair_vector = map_to_vector();

        for (auto i = 0; i < element_number; i++) {
            auto element = pair_vector[i];
            write_binary(element.second, buf);
            write_binary(element.first, buf);
        }
    }

    void read(BufferReadable& buf) {
        read_binary(max_num_buckets, buf);

        size_t element_number = 0;
        read_binary(element_number, buf);

        ordered_map.clear();
        std::pair<T, size_t> element;
        for (auto i = 0; i < element_number; i++) {
            read_binary(element.first, buf);
            read_binary(element.second, buf);
            ordered_map.insert(element);
        }
    }

    void insert_result_into(IColumn& to) const {
        auto pair_vector = map_to_vector();
        for (auto i = 0; i < pair_vector.size(); i++) {
            const auto& element = pair_vector[i];
            if constexpr (std::is_same_v<T, std::string>) {
                assert_cast<ColumnString&>(to).insert_data(element.second.c_str(),
                                                           element.second.length());
            } else {
                assert_cast<ColVecType&>(to).get_data().push_back(element.second);
            }
        }
    }

    std::string get(const DataTypePtr& data_type) const {
        std::vector<Bucket<T>> buckets;
        rapidjson::StringBuffer buffer;
        build_histogram(buckets, ordered_map, max_num_buckets);
        histogram_to_json(buffer, buckets, data_type);
        return std::string(buffer.GetString());
    }

    std::vector<std::pair<size_t, T>> map_to_vector() const {
        std::vector<std::pair<size_t, T>> pair_vector;
        for (auto it : ordered_map) {
            pair_vector.emplace_back(it.second, it.first);
        }
        return pair_vector;
    }

private:
    size_t max_num_buckets = 128;
    std::map<T, size_t> ordered_map;
};

template <typename Data, typename T, bool has_input_param>
class AggregateFunctionHistogram final
        : public IAggregateFunctionDataHelper<
                  Data, AggregateFunctionHistogram<Data, T, has_input_param>> {
public:
    using ColVecType = ColumnVectorOrDecimal<T>;

    AggregateFunctionHistogram() = default;
    AggregateFunctionHistogram(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<Data,
                                           AggregateFunctionHistogram<Data, T, has_input_param>>(
                      argument_types_),
              _argument_type(argument_types_[0]) {}

    std::string get_name() const override { return "histogram"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeString>(); }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena* arena) const override {
        if (columns[0]->is_null_at(row_num)) {
            return;
        }

        if (has_input_param) {
            this->data(place).set_parameters(
                    assert_cast<const ColumnInt32*>(columns[1])->get_element(row_num));
        }

        if constexpr (std::is_same_v<T, std::string>) {
            this->data(place).add(
                    assert_cast<const ColumnString&>(*columns[0]).get_data_at(row_num));
        } else {
            this->data(place).add(assert_cast<const ColVecType&>(*columns[0]).get_data()[row_num]);
        }
    }

    void reset(AggregateDataPtr place) const override { this->data(place).reset(); }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena* arena) const override {
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
        const std::string bucket_json = this->data(place).get(_argument_type);
        assert_cast<ColumnString&>(to).insert_data(bucket_json.c_str(), bucket_json.length());
    }

private:
    DataTypePtr _argument_type;
};

} // namespace doris::vectorized
