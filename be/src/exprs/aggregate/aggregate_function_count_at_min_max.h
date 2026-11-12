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

#include <cstdint>
#include <memory>
#include <string>

#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_number.h"
#include "core/types.h"
#include "exprs/aggregate/aggregate_function.h"
#include "exprs/aggregate/single_value_data.h"

namespace doris {

/**
 * State shared by COUNT_AT_MIN and COUNT_AT_MAX: the current extreme value and the
 * number of input rows that are equal to it. The wrapped Data type owns the value and
 * decides whether a new value is better (smaller for MIN, greater for MAX).
 */
template <typename Data>
struct AggregateFunctionCountAtMinData : Data {
    using Self = AggregateFunctionCountAtMinData;
    int64_t count = 0;

    AggregateFunctionCountAtMinData() { reset(); }

    void change_if_better(const IColumn& column, size_t row_num, Arena& arena) {
        if (!this->has()) {
            this->set(column, row_num, arena);
            count = 1;
        } else if (this->is_equal_to(column, row_num)) {
            ++count;
        } else if (this->set_if_smaller(column, row_num, arena)) {
            count = 1;
        }
    }

    void change_if_better(const Self& to, Arena& arena) {
        if (!to.has()) {
            return;
        }
        if (!this->has()) {
            this->set(to, arena);
            count = to.count;
        } else if (this->is_equal_to(to)) {
            count += to.count;
        } else if (this->set_if_smaller(to, arena)) {
            count = to.count;
        }
    }

    void reset() {
        Data::reset();
        count = 0;
    }

    static const char* name() { return "count_at_min"; }
};

template <typename Data>
struct AggregateFunctionCountAtMaxData : Data {
    using Self = AggregateFunctionCountAtMaxData;
    int64_t count = 0;

    AggregateFunctionCountAtMaxData() { reset(); }

    void change_if_better(const IColumn& column, size_t row_num, Arena& arena) {
        if (!this->has()) {
            this->set(column, row_num, arena);
            count = 1;
        } else if (this->is_equal_to(column, row_num)) {
            ++count;
        } else if (this->set_if_greater(column, row_num, arena)) {
            count = 1;
        }
    }

    void change_if_better(const Self& to, Arena& arena) {
        if (!to.has()) {
            return;
        }
        if (!this->has()) {
            this->set(to, arena);
            count = to.count;
        } else if (this->is_equal_to(to)) {
            count += to.count;
        } else if (this->set_if_greater(to, arena)) {
            count = to.count;
        }
    }

    void reset() {
        Data::reset();
        count = 0;
    }

    static const char* name() { return "count_at_max"; }
};

/**
 * Aggregate function returning the number of rows equal to the minimum (or maximum)
 * value in the group. NULL inputs are ignored. An empty group, or a group whose inputs
 * are all NULL, returns 0.
 */
template <typename Data>
class AggregateFunctionsCountAtExtreme final
        : public IAggregateFunctionDataHelper<Data, AggregateFunctionsCountAtExtreme<Data>> {
private:
    using Base = IAggregateFunctionDataHelper<Data, AggregateFunctionsCountAtExtreme<Data>>;

public:
    explicit AggregateFunctionsCountAtExtreme(const DataTypes& arguments) : Base(arguments) {}

    String get_name() const override { return Data::name(); }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeInt64>(); }

    void create(AggregateDataPtr __restrict place) const override { new (place) Data; }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena& arena) const override {
        this->data(place).change_if_better(*columns[0], row_num, arena);
    }

    void reset(AggregateDataPtr place) const override { this->data(place).reset(); }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena& arena) const override {
        this->data(place).change_if_better(this->data(rhs), arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        this->data(place).write(buf, this->argument_types[0], IAggregateFunction::version);
        buf.write_binary(this->data(place).count);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena& arena) const override {
        this->data(place).read(buf, this->argument_types[0], IAggregateFunction::version, arena);
        buf.read_binary(this->data(place).count);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        assert_cast<ColumnInt64&, TypeCheckOnRelease::DISABLE>(to).insert_value(
                this->data(place).count);
    }
};

} // namespace doris
