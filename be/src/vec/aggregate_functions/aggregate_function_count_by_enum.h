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
#include "vec/utils/count_by_enum_helpers.hpp"

namespace doris::vectorized {

struct AggregateFunctionCountByEnumData {
    using MapType = std::unordered_map<std::string, uint64_t>;

    void reset() { data_vec.clear(); }

    void add(int idx, const StringRef& value, const UInt64& number = 1) {
        if (data_vec.size() <= idx) {
            data_vec.emplace_back();
        }

        std::string key = value.to_string();
        CountByEnumData& data = data_vec[idx];
        MapType& unordered_map = data.cbe;
        auto it = unordered_map.find(key);
        if (it != unordered_map.end()) {
            it->second += number;
        } else {
            unordered_map.emplace(key, number);
        }
        data.not_null += number;
        data.all += number;
    }

    void add(int idx, const UInt64& number = 1) {
        if (data_vec.size() <= idx) {
            data_vec.emplace_back();
        }

        data_vec[idx].null += number;
        data_vec[idx].all += number;
    }

    void merge(const AggregateFunctionCountByEnumData& rhs) {
        for (int idx = 0; idx < rhs.data_vec.size(); idx++) {
            CountByEnumData& data = data_vec.size() <= idx ? data_vec.emplace_back() : data_vec[idx];
            const CountByEnumData& rhs_data = rhs.data_vec[idx];
            const MapType& rhs_unordered_map = rhs_data.cbe;
            MapType& lhs_unordered_map = data.cbe;

            for (auto rhs_it : rhs_unordered_map) {
                auto lhs_it = lhs_unordered_map.find(rhs_it.first);
                if (lhs_it != lhs_unordered_map.end()) {
                    lhs_it->second += rhs_it.second;
                } else {
                    lhs_unordered_map.emplace(rhs_it.first, rhs_it.second);
                }
            }

            data.not_null += rhs_data.not_null;
            data.null += rhs_data.null;
            data.all += rhs_data.all;
        }
    }

    void write(BufferWritable& buf) const {
        write_binary(data_vec.size(), buf);

        for (const auto& data : data_vec) {
            const MapType& unordered_map = data.cbe;
            write_binary(unordered_map.size(), buf);

            for (const auto& [key, value] : unordered_map) {
                write_binary(value, buf);
                write_binary(key, buf);
            }

            write_binary(data.not_null, buf);
            write_binary(data.null, buf);
            write_binary(data.all, buf);
        }
    }

    void read(BufferReadable& buf) {
        data_vec.clear();

        uint64_t vec_size_number = 0;
        read_binary(vec_size_number, buf);

        for (int idx = 0; idx < vec_size_number; idx++) {
            uint64_t element_number = 0;
            read_binary(element_number, buf);

            MapType unordered_map;
            unordered_map.reserve(element_number);
            for (auto i = 0; i < element_number; i++) {
                std::string key;
                uint64_t value;
                read_binary(value, buf);
                read_binary(key, buf);
                unordered_map.emplace(std::move(key), value);
            }

            CountByEnumData data;
            data.cbe = std::move(unordered_map);
            read_binary(data.not_null, buf);
            read_binary(data.null, buf);
            read_binary(data.all, buf);
            data_vec.emplace_back(std::move(data));
        }
    }

    std::string get() const {
        rapidjson::StringBuffer buffer;
        build_json_from_vec(buffer, data_vec);
        return std::string(buffer.GetString());
    }

private:
    std::vector<CountByEnumData>  data_vec;
};

template <typename Data>
class AggregateFunctionCountByEnum final
        : public IAggregateFunctionDataHelper<Data, AggregateFunctionCountByEnum<Data>> {
public:
    AggregateFunctionCountByEnum() = default;
    AggregateFunctionCountByEnum(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<
                    Data,
                    AggregateFunctionCountByEnum<Data>>(argument_types_, {}), _argument_type(argument_types_[0]) {
        arg_count = argument_types_.size();
    }

    std::string get_name() const override { return "count_by_enum"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeString>(); }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, size_t row_num,
             Arena* arena) const override {

        for (int i = 0; i < arg_count; i++) {
            const auto* nullable_column = check_and_get_column<ColumnNullable>(columns[i]);
            if (nullable_column == nullptr) {
                this->data(place).add(i, static_cast<const ColumnString&>(*columns[i]).get_data_at(row_num));
            } else if (nullable_column->is_null_at(row_num)) {
                // TODO create a null vector
                this->data(place).add(i);
            } else {
                this->data(place).add(i, static_cast<const ColumnString&>(nullable_column->get_nested_column()).get_data_at(row_num));
            }
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
        const std::string json_arr = this->data(place).get();
        assert_cast<ColumnString&>(to).insert_data(json_arr.c_str(), json_arr.length());
    }

private:
    DataTypePtr _argument_type;
    size_t arg_count;
};

} // namespace doris::vectorized