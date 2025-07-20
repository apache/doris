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

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

// geomean = (x1 * x2 * ... * xn)^(1/n), directly compute
// (x1*x2*...*xn) maybe overflow easily, so we compute
// geomean = exp((log(x1) + log(x2) + ... + log(xn)) / n)
// like DuckDB, xi < 0 is not allowed but we allow xi = zero
// e.g. (0 * 4 * 8)  ^ {1/3} => 0

template <PrimitiveType T>
struct AggregateFunctionGeomeanData {
    using DataType = typename PrimitiveTypeTraits<T>::ColumnItemType;
    bool has_zero = false;
    bool has_negative = false;
    Int32 count_positive = 0;
    Float64 sum_log {};

    void write(BufferWritable& buf) const {
        buf.write_binary(has_zero);
        buf.write_binary(has_negative);
        buf.write_binary(count_positive);
        buf.write_binary(sum_log);
    }

    void read(BufferReadable& buf) {
        buf.read_binary(has_zero);
        buf.read_binary(has_negative);
        buf.read_binary(count_positive);
        buf.read_binary(sum_log);
    }

    void reset() {
        has_zero = false;
        has_negative = false;
        count_positive = 0;
        sum_log = {};
    }

    void merge(const AggregateFunctionGeomeanData& rhs) {
        if (has_negative || has_zero || rhs.has_negative || rhs.has_zero ||
            rhs.count_positive == 0) {
            return;
        }

        count_positive += rhs.count_positive;
        sum_log += rhs.sum_log;
    }

    // the caller makes sure `value_x` is not NULL
    void add(const DataType& value_x) {
        if (has_negative) {
            return;
        } else if (double(value_x) < 0) { // the conversion is safe here
            has_negative = true;
            return;
        } else if (has_zero) {
            return;
        } else if (double(value_x) == 0) {
            has_zero = true;
            return;
        }

        count_positive++; // value_x > 0
        sum_log += std::log((double)value_x);
    }

    Float64 get_geomean() const {
        if (has_negative) {
            return std::numeric_limits<Float64>::quiet_NaN();
        } else if (has_zero || count_positive == 0) {
            return 0;
        }
        return std::exp(sum_log / count_positive);
    }
};

template <PrimitiveType type>
class AggregateFunctionGeomean final
        : public IAggregateFunctionDataHelper<AggregateFunctionGeomeanData<type>,
                                              AggregateFunctionGeomean<type>> {
public:
    using ColVecType = typename PrimitiveTypeTraits<type>::ColumnType;

    AggregateFunctionGeomean(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<AggregateFunctionGeomeanData<type>,
                                           AggregateFunctionGeomean<type>>(argument_types_) {}

    String get_name() const override { return "geomean"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeFloat64>(); }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena&) const override {
        const auto& column =
                assert_cast<const ColVecType&, TypeCheckOnRelease::DISABLE>(*columns[0]);
        if (!column.is_null_at(row_num)) {
            this->data(place).add(column.get_data()[row_num]);
        }
    }

    void reset(AggregateDataPtr place) const override { this->data(place).reset(); }

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
        auto& column = assert_cast<ColumnFloat64&>(to);
        column.get_data().push_back(this->data(place).get_geomean());
    }
};

} // namespace doris::vectorized

#include "common/compile_check_end.h"