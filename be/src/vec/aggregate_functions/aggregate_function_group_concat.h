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

#include <vector>

#include "common/status.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/common/string_ref.h"
#include "vec/data_types/data_type_string.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {

template <bool is_ordered>
struct GroupConcatData {
    std::vector<std::string> data;
    std::string separator;
    bool inited = false;

    void add(StringRef ref, StringRef sep) {
        if (!inited) {
            inited = true;
            separator.assign(sep.data, sep.data + sep.size);
        }
        data.emplace_back(ref.data, ref.size);
    }

    void merge(const GroupConcatData& rhs) {
        if (!rhs.inited) {
            return;
        }

        if (!inited) {
            inited = true;
            separator = rhs.separator;
            data = rhs.data;
        } else {
            data.assign(rhs.data.begin(), rhs.data.end());
        }
    }

    std::string get() {
        if constexpr (is_ordered) {
            std::sort(data.begin(), data.end());
        }

        bool is_first = true;
        std::string result;
        for (const auto& str : data) {
            if (is_first) {
                is_first = false;
            } else {
                result += separator;
            }
            result += str;
        }

        return result;
    }

    void write(BufferWritable& buf) const {
        write_binary(data, buf);
        write_binary(separator, buf);
        write_binary(inited, buf);
    }

    void read(BufferReadable& buf) {
        read_binary(data, buf);
        read_binary(separator, buf);
        read_binary(inited, buf);
    }

    void reset() {
        data.clear();
        separator = "";
        inited = false;
    }
};

const std::string DEFAULT_SEPARATOR = ", ";

template <typename Data>
struct GroupConcatImplStr {
    static void add(Data& __restrict place, const IColumn** columns, size_t row_num) {
        place.add(static_cast<const ColumnString&>(*columns[0]).get_data_at(row_num),
                  StringRef(DEFAULT_SEPARATOR.data(), DEFAULT_SEPARATOR.length()));
    }
};

template <typename Data>
struct GroupConcatImplStrStr {
    static void add(Data& __restrict place, const IColumn** columns, size_t row_num) {
        place.add(static_cast<const ColumnString&>(*columns[0]).get_data_at(row_num),
                  static_cast<const ColumnString&>(*columns[1]).get_data_at(row_num));
    }
};

template <typename Data, template <typename> typename Impl>
class AggregateFunctionGroupConcat final
        : public IAggregateFunctionDataHelper<Data, AggregateFunctionGroupConcat<Data, Impl>> {
public:
    AggregateFunctionGroupConcat(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionGroupConcat<Data, Impl>>(
                      argument_types_, {}) {}

    String get_name() const override { return "group_concat"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeString>(); }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, size_t row_num,
             Arena*) const override {
        Impl<Data>::add(this->data(place), columns, row_num);
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
        std::string result = const_cast<Data*>((const Data*)place)->get();
        static_cast<ColumnString&>(to).insert_data(result.c_str(), result.length());
    }
};

} // namespace doris::vectorized
