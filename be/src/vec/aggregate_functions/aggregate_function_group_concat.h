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

#include <string.h>

#include <memory>
#include <string>

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/columns/column_string.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_ref.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_string.h"
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

struct AggregateFunctionGroupConcatData {
    std::string data;
    std::string separator;
    bool inited = false;

    void add(StringRef ref, StringRef sep) {
        if (!inited) {
            inited = true;
            separator.assign(sep.data, sep.data + sep.size);
        } else {
            data += separator;
        }

        data.resize(data.length() + ref.size);
        memcpy(data.data() + data.length() - ref.size, ref.data, ref.size);
    }

    void merge(const AggregateFunctionGroupConcatData& rhs) {
        if (!rhs.inited) {
            return;
        }

        if (!inited) {
            inited = true;
            separator = rhs.separator;
            data = rhs.data;
        } else {
            data += separator;
            data += rhs.data;
        }
    }

    const std::string& get() const { return data; }

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
        data = "";
        separator = "";
        inited = false;
    }
};

struct AggregateFunctionGroupConcatImplStr {
    static const std::string separator;
    static void add(AggregateFunctionGroupConcatData& __restrict place, const IColumn** columns,
                    size_t row_num) {
        place.add(assert_cast<const ColumnString&, TypeCheckOnRelease::DISABLE>(*columns[0])
                          .get_data_at(row_num),
                  StringRef(separator.data(), separator.length()));
    }
};

struct AggregateFunctionGroupConcatImplStrStr {
    static void add(AggregateFunctionGroupConcatData& __restrict place, const IColumn** columns,
                    size_t row_num) {
        place.add(assert_cast<const ColumnString&, TypeCheckOnRelease::DISABLE>(*columns[0])
                          .get_data_at(row_num),
                  assert_cast<const ColumnString&, TypeCheckOnRelease::DISABLE>(*columns[1])
                          .get_data_at(row_num));
    }
};

template <typename Impl>
class AggregateFunctionGroupConcat final
        : public IAggregateFunctionDataHelper<AggregateFunctionGroupConcatData,
                                              AggregateFunctionGroupConcat<Impl>> {
public:
    AggregateFunctionGroupConcat(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<AggregateFunctionGroupConcatData,
                                           AggregateFunctionGroupConcat<Impl>>(argument_types_) {}

    String get_name() const override { return "group_concat"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeString>(); }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena*) const override {
        Impl::add(this->data(place), columns, row_num);
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
        const std::string& result = this->data(place).get();
        assert_cast<ColumnString&>(to).insert_data(result.c_str(), result.length());
    }
};

} // namespace doris::vectorized
