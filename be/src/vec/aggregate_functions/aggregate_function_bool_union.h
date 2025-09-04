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

#include <cmath>

#include "runtime/primitive_type.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

struct AggregateFunctionBoolOrData {
    static constexpr auto name = "bool_or";

    bool value = false;

    void add(bool x) { value |= x; }

    void merge(const AggregateFunctionBoolOrData& rhs) { value |= rhs.value; }

    void write(BufferWritable& buf) const { buf.write_binary(value); }

    void read(BufferReadable& buf) { buf.read_binary(value); }

    void reset() { value = false; }
};

struct AggregateFunctionBoolAndData {
    static constexpr auto name = "bool_and";

    bool value = true;

    void add(bool x) { value &= x; }

    void merge(const AggregateFunctionBoolAndData& rhs) { value &= rhs.value; }

    void write(BufferWritable& buf) const { buf.write_binary(value); }

    void read(BufferReadable& buf) { buf.read_binary(value); }

    void reset() { value = true; }
};

struct AggregateFunctionBoolXorData {
    static constexpr auto name = "bool_xor";

    // value represents the current XOR state
    // '0' represents that there are no true values currently
    // '1' represents that exactly one true value has appeared
    // '2' represents that two true values have already appeared and will not change thereafter
    uint8_t value = 0;

    void add(bool x) {
        if (x && value < 2) {
            ++value;
        }
    }

    void merge(const AggregateFunctionBoolXorData& rhs) {
        if (value == 0) {
            value = rhs.value;
        } else if (value == 1) {
            value = (rhs.value > 0) ? 2 : 1;
        }
    }

    void write(BufferWritable& buf) const { buf.write_binary(value); }

    void read(BufferReadable& buf) { buf.read_binary(value); }

    void reset() { value = 0; }
};

template <typename BoolFunc>
class AggregateFuntionBoolUnion final
        : public IAggregateFunctionDataHelper<BoolFunc, AggregateFuntionBoolUnion<BoolFunc>>,
          NullableAggregateFunction,
          UnaryExpression {
public:
    explicit AggregateFuntionBoolUnion(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<BoolFunc, AggregateFuntionBoolUnion<BoolFunc>>(
                      argument_types_) {}

    String get_name() const override { return BoolFunc::name; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeBool>(); }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena&) const override {
        this->data(place).add(
                assert_cast<const ColumnUInt8&, TypeCheckOnRelease::DISABLE>(*columns[0])
                        .get_element(row_num));
    }

    void reset(AggregateDataPtr __restrict place) const override { this->data(place).reset(); }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena&) const override {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena&) const override {
        this->data(place).read(buf);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        assert_cast<ColumnUInt8&>(to).insert_value(this->data(place).value == 1);
    }
};
} // namespace doris::vectorized

#include "common/compile_check_end.h"