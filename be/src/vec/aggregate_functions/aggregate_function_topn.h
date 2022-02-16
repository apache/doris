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

#include <parallel_hashmap/phmap.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <unordered_map>

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_group_concat.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/helpers.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {

// space-saving algorithm
struct AggregateFunctionTopNData {
    void set_paramenters(int input_top_num, int space_expand_rate = 50) {
        top_num = input_top_num;
        capacity = (uint64_t)top_num * space_expand_rate;
    }

    void add(const std::string& value) {
        auto it = counter_map.find(value);
        if (it != counter_map.end()) {
            it->second++;
        } else {
            counter_map.insert({value, 1});
        }
    }

    void merge(const AggregateFunctionTopNData& rhs) {
        if (!rhs.top_num) {
            return;
        }

        top_num = rhs.top_num;
        capacity = rhs.capacity;

        bool lhs_full = (counter_map.size() >= capacity);
        bool rhs_full = (rhs.counter_map.size() >= capacity);

        uint64_t lhs_min = 0;
        uint64_t rhs_min = 0;

        if (lhs_full) {
            lhs_min = UINT64_MAX;
            for (auto it : counter_map) {
                lhs_min = std::min(lhs_min, it.second);
            }
        }

        if (rhs_full) {
            rhs_min = UINT64_MAX;
            for (auto it : rhs.counter_map) {
                rhs_min = std::min(rhs_min, it.second);
            }

            for (auto& it : counter_map) {
                it.second += rhs_min;
            }
        }

        for (auto rhs_it : rhs.counter_map) {
            auto lhs_it = counter_map.find(rhs_it.first);
            if (lhs_it != counter_map.end()) {
                lhs_it->second += rhs_it.second - rhs_min;
            } else {
                counter_map.insert({rhs_it.first, rhs_it.second + lhs_min});
            }
        }
    }

    std::vector<std::pair<uint64_t, std::string>> get_remain_vector() const {
        std::vector<std::pair<uint64_t, std::string>> counter_vector;
        for (auto it : counter_map) {
            counter_vector.emplace_back(it.second, it.first);
        }
        std::sort(counter_vector.begin(), counter_vector.end(),
                  std::greater<std::pair<uint64_t, std::string>>());
        return counter_vector;
    }

    void write(BufferWritable& buf) const {
        write_binary(top_num, buf);
        write_binary(capacity, buf);

        uint64_t element_number = std::min(capacity, (uint64_t)counter_map.size());
        write_binary(element_number, buf);

        auto counter_vector = get_remain_vector();

        for (auto i = 0; i < element_number; i++) {
            auto element = counter_vector[i];
            write_binary(element.second, buf);
            write_binary(element.first, buf);
        }
    }

    void read(BufferReadable& buf) {
        read_binary(top_num, buf);
        read_binary(capacity, buf);

        uint64_t element_number = 0;
        read_binary(element_number, buf);

        counter_map.clear();
        std::pair<std::string, uint64_t> element;
        for (auto i = 0; i < element_number; i++) {
            read_binary(element.first, buf);
            read_binary(element.second, buf);
            counter_map.insert(element);
        }
    }

    std::string get() const {
        auto counter_vector = get_remain_vector();

        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);

        writer.StartObject();
        for (int i = 0; i < std::min((int)counter_vector.size(), top_num); i++) {
            const auto& element = counter_vector[i];
            writer.Key(element.second.c_str());
            writer.Uint64(element.first);
        }
        writer.EndObject();

        return buffer.GetString();
    }

    void reset() { counter_map.clear(); }

    int top_num = 0;
    uint64_t capacity = 0;
    phmap::flat_hash_map<std::string, uint64_t> counter_map;
};

struct StringDataImplTopN {
    using DataType = DataTypeString;
    static std::string to_string(const IColumn& column, size_t row_num) {
        StringRef ref =
                static_cast<const typename DataType::ColumnType&>(column).get_data_at(row_num);
        return std::string(ref.data, ref.size);
    }
};

struct AggregateFunctionTopNImplEmpty {
    // only used at AGGREGATE (merge finalize)
    static void add(AggregateFunctionTopNData& __restrict place, const IColumn** columns,
                    size_t row_num) {
        LOG(FATAL) << "AggregateFunctionTopNImplEmpty do not support add()";
    }
};

template <typename DataHelper>
struct AggregateFunctionTopNImplInt {
    static void add(AggregateFunctionTopNData& __restrict place, const IColumn** columns,
                    size_t row_num) {
        place.set_paramenters(static_cast<const ColumnInt32*>(columns[1])->get_element(row_num));
        place.add(DataHelper::to_string(*columns[0], row_num));
    }
};

template <typename DataHelper>
struct AggregateFunctionTopNImplIntInt {
    static void add(AggregateFunctionTopNData& __restrict place, const IColumn** columns,
                    size_t row_num) {
        place.set_paramenters(static_cast<const ColumnInt32*>(columns[1])->get_element(row_num),
                              static_cast<const ColumnInt32*>(columns[2])->get_element(row_num));
        place.add(DataHelper::to_string(*columns[0], row_num));
    }
};

//base function
template <typename Impl>
class AggregateFunctionTopN final
        : public IAggregateFunctionDataHelper<AggregateFunctionTopNData,
                                              AggregateFunctionTopN<Impl>> {
public:
    AggregateFunctionTopN(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<AggregateFunctionTopNData, AggregateFunctionTopN<Impl>>(
                      argument_types_, {}) {}

    String get_name() const override { return "topn"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeString>(); }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, size_t row_num,
             Arena*) const override {
        Impl::add(this->data(place), columns, row_num);
    }

    void reset(AggregateDataPtr __restrict place) const override { this->data(place).reset(); }

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
        std::string result = this->data(place).get();
        static_cast<ColumnString&>(to).insert_data(result.c_str(), result.length());
    }
};

} // namespace doris::vectorized
