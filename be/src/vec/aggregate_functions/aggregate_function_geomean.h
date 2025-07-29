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

// geomean = (x1 * x2 * ... * xn)^(1/n)
// Supports negative numbers , zero and NULL based on mathematical definition:
// - If any value is 0, result is 0.
// - If value is NULL, ignore it.
// - If number of negative values is odd and n is even, result is undefined (complex).
// - Otherwise, result is sign * exp(sum(log(|x|)) / n), where sign is -1 for odd negatives, 1 otherwise.

template <PrimitiveType T>
struct AggregateFunctionGeomeanData {
    using DataType = typename PrimitiveTypeTraits<T>::ColumnItemType;
    bool has_zero = false;
    UInt32 count_negative = 0;
    UInt32 count_positive = 0;
    Float64 sum_log_abs = 0.0;

    void write(BufferWritable& buf) const {
        buf.write_binary(count_positive);
        buf.write_binary(count_negative);
        buf.write_binary(has_zero);
        buf.write_binary(sum_log_abs);
    }

    void read(BufferReadable& buf) {
        buf.read_binary(count_positive);
        buf.read_binary(count_negative);
        buf.read_binary(has_zero);
        buf.read_binary(sum_log_abs);
    }

    void reset() {
        count_positive = 0;
        count_negative = 0;
        has_zero = false;
        sum_log_abs = 0.0;
    }

    void merge(const AggregateFunctionGeomeanData& rhs) {
        if (rhs.has_zero) {
            has_zero = true;
            return;
        }
        if (has_zero || (rhs.count_negative == 0 && rhs.count_positive == 0)) {
            return;
        }

        count_positive += rhs.count_positive;
        count_negative += rhs.count_negative;
        sum_log_abs += rhs.sum_log_abs;
    }

    void add(const DataType& value_x) {
        double value = double(value_x); // the conversion is safe here
        if (has_zero || value == 0) {
            has_zero = true;
            return;
        }

        if (value < 0) {
            count_negative++;
        } else {
            count_positive++;
        }

        sum_log_abs += std::log(std::abs(value));
    }

    Status get_geomean(Float64& res) const {
        if (has_zero || (count_negative == 0 && count_positive == 0)) {
            res = 0.0;
            return Status::OK();
        }
        if ((count_negative % 2 == 1) && (count_positive % 2 == 1)) {
            return Status::InvalidArgument(
                    "Geometric mean is undefined for odd number of negatives with even n");
        }

        Float64 sign = (count_negative % 2 == 1) ? -1.0 : 1.0;
        res = sign * std::exp(sum_log_abs / (count_positive + count_negative));
        return Status::OK();
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
        this->data(place).add(column.get_data()[row_num]);
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
        Float64 res;
        auto status = this->data(place).get_geomean(res);
        if (!status.ok()) {
            throw Exception(status);
        }
        column.get_data().push_back(res);
    }
};

} // namespace doris::vectorized

#include "common/compile_check_end.h"