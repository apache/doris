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

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

template <PrimitiveType T>
struct AggregateFunctionRegrData {
    static constexpr PrimitiveType Type = T;
    UInt64 n {};
    Float64 sx {};
    Float64 sy {};
    Float64 sxx {};
    Float64 syy {};
    Float64 sxy {};

    void write(BufferWritable& buf) const {
        buf.write_binary(sx);
        buf.write_binary(sy);
        buf.write_binary(sxx);
        buf.write_binary(syy);
        buf.write_binary(sxy);
        buf.write_binary(n);
    }

    void read(BufferReadable& buf) {
        buf.read_binary(sx);
        buf.read_binary(sy);
        buf.read_binary(sxx);
        buf.read_binary(syy);
        buf.read_binary(sxy);
        buf.read_binary(n);
    }

    void reset() {
        sx = {};
        sy = {};
        sxx = {};
        syy = {};
        sxy = {};
        n = {};
    }

    /**
     * The merge function uses the Youngs–Cramer algorithm:
     * N   = N1 + N2
     * Sx  = Sx1 + Sx2
     * Sy  = Sy1 + Sy2
     * Sxx = Sxx1 + Sxx2 + N1 * N2 * (Sx1/N1 - Sx2/N2)^2 / N
     * Syy = Syy1 + Syy2 + N1 * N2 * (Sy1/N1 - Sy2/N2)^2 / N
     * Sxy = Sxy1 + Sxy2 + N1 * N2 * (Sx1/N1 - Sx2/N2) * (Sy1/N1 - Sy2/N2) / N
     */
    void merge(const AggregateFunctionRegrData& rhs) {
        if (rhs.n == 0) {
            return;
        }
        if (n == 0) {
            *this = rhs;
            return;
        }
        const auto n1 = static_cast<Float64>(n);
        const auto n2 = static_cast<Float64>(rhs.n);
        const auto nsum = n1 + n2;

        const auto dx = sx / n1 - rhs.sx / n2;
        const auto dy = sy / n1 - rhs.sy / n2;

        n += rhs.n;
        sx += rhs.sx;
        sy += rhs.sy;
        sxx += rhs.sxx + n1 * n2 * dx * dx / nsum;
        syy += rhs.syy + n1 * n2 * dy * dy / nsum;
        sxy += rhs.sxy + n1 * n2 * dx * dy / nsum;
    }

    /**
     * N
     * Sx  = sum(X)
     * Sy  = sum(Y)
     * Sxx = sum((X-Sx/N)^2)
     * Syy = sum((Y-Sy/N)^2)
     * Sxy = sum((X-Sx/N)*(Y-Sy/N))
     */
    void add(typename PrimitiveTypeTraits<T>::ColumnItemType value_y,
             typename PrimitiveTypeTraits<T>::ColumnItemType value_x) {
        const auto x = static_cast<Float64>(value_x);
        const auto y = static_cast<Float64>(value_y);
        sx += x;
        sy += y;

        if (n == 0) [[unlikely]] {
            n = 1;
            return;
        }
        const auto tmp_n = static_cast<Float64>(n + 1);
        const auto tmp_x = x * tmp_n - sx;
        const auto tmp_y = y * tmp_n - sy;
        const auto scale = 1.0 / (tmp_n * static_cast<Float64>(n));

        n += 1;
        sxx += tmp_x * tmp_x * scale;
        syy += tmp_y * tmp_y * scale;
        sxy += tmp_x * tmp_y * scale;
    }
};

template <PrimitiveType T>
struct RegrSlopeFunc : AggregateFunctionRegrData<T> {
    static constexpr const char* name = "regr_slope";

    Float64 get_result() const {
        if (this->n < 1 || this->sxx == 0.0) {
            return std::numeric_limits<Float64>::quiet_NaN();
        }
        return this->sxy / this->sxx;
    }
};

template <PrimitiveType T>
struct RegrInterceptFunc : AggregateFunctionRegrData<T> {
    static constexpr const char* name = "regr_intercept";

    Float64 get_result() const {
        if (this->n < 1 || this->sxx == 0.0) {
            return std::numeric_limits<Float64>::quiet_NaN();
        }
        return (this->sy - this->sx * this->sxy / this->sxx) / static_cast<Float64>(this->n);
    }
};

template <typename RegrFunc, bool y_nullable, bool x_nullable>
class AggregateFunctionRegrSimple
        : public IAggregateFunctionDataHelper<
                  RegrFunc, AggregateFunctionRegrSimple<RegrFunc, y_nullable, x_nullable>> {
public:
    using XInputCol = typename PrimitiveTypeTraits<RegrFunc::Type>::ColumnType;
    using YInputCol = XInputCol;
    using ResultCol = ColumnFloat64;

    explicit AggregateFunctionRegrSimple(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<
                      RegrFunc, AggregateFunctionRegrSimple<RegrFunc, y_nullable, x_nullable>>(
                      argument_types_) {
        DCHECK(!argument_types_.empty());
    }

    String get_name() const override { return RegrFunc::name; }

    DataTypePtr get_return_type() const override {
        return make_nullable(std::make_shared<DataTypeFloat64>());
    }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena&) const override {
        bool y_null = false;
        bool x_null = false;
        const YInputCol* y_nested_column = nullptr;
        const XInputCol* x_nested_column = nullptr;

        if constexpr (y_nullable) {
            const auto& y_column_nullable =
                    assert_cast<const ColumnNullable&, TypeCheckOnRelease::DISABLE>(*columns[0]);
            y_null = y_column_nullable.is_null_at(row_num);
            y_nested_column = assert_cast<const YInputCol*, TypeCheckOnRelease::DISABLE>(
                    y_column_nullable.get_nested_column_ptr().get());
        } else {
            y_nested_column = assert_cast<const YInputCol*, TypeCheckOnRelease::DISABLE>(
                    (*columns[0]).get_ptr().get());
        }

        if constexpr (x_nullable) {
            const auto& x_column_nullable =
                    assert_cast<const ColumnNullable&, TypeCheckOnRelease::DISABLE>(*columns[1]);
            x_null = x_column_nullable.is_null_at(row_num);
            x_nested_column = assert_cast<const XInputCol*, TypeCheckOnRelease::DISABLE>(
                    x_column_nullable.get_nested_column_ptr().get());
        } else {
            x_nested_column = assert_cast<const XInputCol*, TypeCheckOnRelease::DISABLE>(
                    (*columns[1]).get_ptr().get());
        }

        if (x_null || y_null) {
            return;
        }

        this->data(place).add(y_nested_column->get_data()[row_num],
                              x_nested_column->get_data()[row_num]);
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
        const auto& data = this->data(place);
        auto& dst_column_with_nullable = assert_cast<ColumnNullable&>(to);
        auto& dst_column = assert_cast<ResultCol&>(dst_column_with_nullable.get_nested_column());
        Float64 result = data.get_result();
        if (std::isnan(result)) {
            dst_column_with_nullable.get_null_map_data().push_back(1);
            dst_column.insert_default();
        } else {
            dst_column_with_nullable.get_null_map_data().push_back(0);
            dst_column.get_data().push_back(result);
        }
    }
};
} // namespace doris::vectorized

#include "common/compile_check_end.h"
