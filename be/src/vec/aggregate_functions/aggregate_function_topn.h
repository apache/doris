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

#include <rapidjson/encodings.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <stddef.h>
#include <stdint.h>

#include <algorithm>
#include <functional>
#include <iterator>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/common/hash_table/phmap_fwd_decl.h"
#include "vec/common/string_ref.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_string.h"
#include "vec/io/io_helper.h"

namespace doris {
namespace vectorized {
class Arena;
class BufferReadable;
class BufferWritable;
template <typename T>
class ColumnDecimal;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

// space-saving algorithm
template <typename T>
struct AggregateFunctionTopNData {
    using ColVecType = std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<T>, ColumnVector<T>>;
    void set_paramenters(int input_top_num, int space_expand_rate = 50) {
        top_num = input_top_num;
        capacity = (uint64_t)top_num * space_expand_rate;
    }

    void add(const StringRef& value, const UInt64& increment = 1) {
        std::string data = value.to_string();
        auto it = counter_map.find(data);
        if (it != counter_map.end()) {
            it->second = it->second + increment;
        } else {
            counter_map.insert({data, increment});
        }
    }

    void add(const T& value, const UInt64& increment = 1) {
        auto it = counter_map.find(value);
        if (it != counter_map.end()) {
            it->second = it->second + increment;
        } else {
            counter_map.insert({value, increment});
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

    std::vector<std::pair<uint64_t, T>> get_remain_vector() const {
        std::vector<std::pair<uint64_t, T>> counter_vector;
        for (auto it : counter_map) {
            counter_vector.emplace_back(it.second, it.first);
        }
        std::sort(counter_vector.begin(), counter_vector.end(),
                  std::greater<std::pair<uint64_t, T>>());
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
        std::pair<T, uint64_t> element;
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

    void insert_result_into(IColumn& to) const {
        auto counter_vector = get_remain_vector();
        for (int i = 0; i < std::min((int)counter_vector.size(), top_num); i++) {
            const auto& element = counter_vector[i];
            if constexpr (std::is_same_v<T, std::string>) {
                assert_cast<ColumnString&, TypeCheckOnRelease::DISABLE>(to).insert_data(
                        element.second.c_str(), element.second.length());
            } else {
                assert_cast<ColVecType&, TypeCheckOnRelease::DISABLE>(to).get_data().push_back(
                        element.second);
            }
        }
    }

    void reset() { counter_map.clear(); }

    int top_num = 0;
    uint64_t capacity = 0;
    flat_hash_map<T, uint64_t> counter_map;
};

struct AggregateFunctionTopNImplInt {
    static void add(AggregateFunctionTopNData<std::string>& __restrict place,
                    const IColumn** columns, size_t row_num) {
        place.set_paramenters(
                assert_cast<const ColumnInt32*, TypeCheckOnRelease::DISABLE>(columns[1])
                        ->get_element(row_num));
        place.add(assert_cast<const ColumnString&, TypeCheckOnRelease::DISABLE>(*columns[0])
                          .get_data_at(row_num));
    }
};

struct AggregateFunctionTopNImplIntInt {
    static void add(AggregateFunctionTopNData<std::string>& __restrict place,
                    const IColumn** columns, size_t row_num) {
        place.set_paramenters(
                assert_cast<const ColumnInt32*, TypeCheckOnRelease::DISABLE>(columns[1])
                        ->get_element(row_num),
                assert_cast<const ColumnInt32*, TypeCheckOnRelease::DISABLE>(columns[2])
                        ->get_element(row_num));
        place.add(assert_cast<const ColumnString&>(*columns[0]).get_data_at(row_num));
    }
};

//for topn_array agg
template <typename T, bool has_default_param>
struct AggregateFunctionTopNImplArray {
    using ColVecType = std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<T>, ColumnVector<T>>;
    static void add(AggregateFunctionTopNData<T>& __restrict place, const IColumn** columns,
                    size_t row_num) {
        if constexpr (has_default_param) {
            place.set_paramenters(
                    assert_cast<const ColumnInt32*, TypeCheckOnRelease::DISABLE>(columns[1])
                            ->get_element(row_num),
                    assert_cast<const ColumnInt32*, TypeCheckOnRelease::DISABLE>(columns[2])
                            ->get_element(row_num));

        } else {
            place.set_paramenters(
                    assert_cast<const ColumnInt32*, TypeCheckOnRelease::DISABLE>(columns[1])
                            ->get_element(row_num));
        }
        if constexpr (std::is_same_v<T, std::string>) {
            place.add(assert_cast<const ColumnString&, TypeCheckOnRelease::DISABLE>(*columns[0])
                              .get_data_at(row_num));
        } else {
            T val = assert_cast<const ColVecType&, TypeCheckOnRelease::DISABLE>(*columns[0])
                            .get_data()[row_num];
            place.add(val);
        }
    }
};

//for topn_weighted agg
template <typename T, bool has_default_param>
struct AggregateFunctionTopNImplWeight {
    using ColVecType = std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<T>, ColumnVector<T>>;
    static void add(AggregateFunctionTopNData<T>& __restrict place, const IColumn** columns,
                    size_t row_num) {
        if constexpr (has_default_param) {
            place.set_paramenters(
                    assert_cast<const ColumnInt32*, TypeCheckOnRelease::DISABLE>(columns[2])
                            ->get_element(row_num),
                    assert_cast<const ColumnInt32*, TypeCheckOnRelease::DISABLE>(columns[3])
                            ->get_element(row_num));

        } else {
            place.set_paramenters(
                    assert_cast<const ColumnInt32*>(columns[2])->get_element(row_num));
        }
        if constexpr (std::is_same_v<T, std::string>) {
            auto weight = assert_cast<const ColumnVector<Int64>&, TypeCheckOnRelease::DISABLE>(
                                  *columns[1])
                                  .get_data()[row_num];
            place.add(assert_cast<const ColumnString&, TypeCheckOnRelease::DISABLE>(*columns[0])
                              .get_data_at(row_num),
                      weight);
        } else {
            T val = assert_cast<const ColVecType&, TypeCheckOnRelease::DISABLE>(*columns[0])
                            .get_data()[row_num];
            auto weight = assert_cast<const ColumnVector<Int64>&, TypeCheckOnRelease::DISABLE>(
                                  *columns[1])
                                  .get_data()[row_num];
            place.add(val, weight);
        }
    }
};

//base function
template <typename Impl, typename T>
class AggregateFunctionTopNBase
        : public IAggregateFunctionDataHelper<AggregateFunctionTopNData<T>,
                                              AggregateFunctionTopNBase<Impl, T>> {
public:
    AggregateFunctionTopNBase(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<AggregateFunctionTopNData<T>,
                                           AggregateFunctionTopNBase<Impl, T>>(argument_types_) {}

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
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
};

//topn function return string
template <typename Impl, typename T = std::string>
class AggregateFunctionTopN final : public AggregateFunctionTopNBase<Impl, T> {
public:
    AggregateFunctionTopN(const DataTypes& argument_types_)
            : AggregateFunctionTopNBase<Impl, T>(argument_types_) {}

    String get_name() const override { return "topn"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeString>(); }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        std::string result = this->data(place).get();
        assert_cast<ColumnString&>(to).insert_data(result.c_str(), result.length());
    }
};

//topn function return array
template <typename Impl, typename T, bool is_weighted>
class AggregateFunctionTopNArray final : public AggregateFunctionTopNBase<Impl, T> {
public:
    AggregateFunctionTopNArray(const DataTypes& argument_types_)
            : AggregateFunctionTopNBase<Impl, T>(argument_types_),
              _argument_type(argument_types_[0]) {}

    String get_name() const override {
        if constexpr (is_weighted) {
            return "topn_weighted";
        } else {
            return "topn_array";
        }
    }

    DataTypePtr get_return_type() const override {
        return std::make_shared<DataTypeArray>(make_nullable(_argument_type));
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        auto& to_arr = assert_cast<ColumnArray&>(to);
        auto& to_nested_col = to_arr.get_data();
        if (to_nested_col.is_nullable()) {
            auto col_null = reinterpret_cast<ColumnNullable*>(&to_nested_col);
            this->data(place).insert_result_into(col_null->get_nested_column());
            col_null->get_null_map_data().resize_fill(col_null->get_nested_column().size(), 0);
        } else {
            this->data(place).insert_result_into(to_nested_col);
        }
        to_arr.get_offsets().push_back(to_nested_col.size());
    }

private:
    DataTypePtr _argument_type;
};

} // namespace doris::vectorized
