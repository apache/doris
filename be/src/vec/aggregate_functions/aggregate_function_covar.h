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

#include "agent/be_exec_version_manager.h"
#define POP true
#define NOTPOP false
#define NULLABLE true
#define NOTNULLABLE false

#include <stddef.h>
#include <stdint.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <cmath>
#include <memory>
#include <type_traits>

#include "olap/olap_common.h"
#include "runtime/decimalv2_value.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/common/assert_cast.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"
#include "vec/io/io_helper.h"

namespace doris {
namespace vectorized {
class Arena;
class BufferReadable;
class BufferWritable;
template <typename T>
class ColumnDecimal;
template <typename>
class ColumnVector;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

template <typename T>
struct BaseData {
    BaseData() : sum_x(0.0), sum_y(0.0), sum_xy(0.0), count(0) {}
    virtual ~BaseData() = default;

    void write(BufferWritable& buf) const {
        write_binary(sum_x, buf);
        write_binary(sum_y, buf);
        write_binary(sum_xy, buf);
        write_binary(count, buf);
    }

    void read(BufferReadable& buf) {
        read_binary(sum_x, buf);
        read_binary(sum_y, buf);
        read_binary(sum_xy, buf);
        read_binary(count, buf);
    }

    void reset() {
        sum_x = 0.0;
        sum_y = 0.0;
        sum_xy = 0.0;
        count = 0;
    }

    // Cov(X, Y) = E(XY) - E(X)E(Y)
    double get_pop_result() const {
        if (count == 1) {
            return 0.0;
        }
        return sum_xy / count - sum_x * sum_y / (count * count);
    }

    double get_samp_result() const {
        return sum_xy / (count - 1) - sum_x * sum_y / (count * (count - 1));
    }

    void merge(const BaseData& rhs) {
        if (rhs.count == 0) {
            return;
        }
        sum_x += rhs.sum_x;
        sum_y += rhs.sum_y;
        sum_xy += rhs.sum_xy;
        count += rhs.count;
    }

    void add(const IColumn* column_x, const IColumn* column_y, size_t row_num) {
        const auto& sources_x = assert_cast<const ColumnVector<T>&>(*column_x);
        double source_data_x = sources_x.get_data()[row_num];
        const auto& sources_y = assert_cast<const ColumnVector<T>&>(*column_y);
        double source_data_y = sources_y.get_data()[row_num];

        sum_x += source_data_x;
        sum_y += source_data_y;
        sum_xy += source_data_x * source_data_y;
        count += 1;
    }

    static DataTypePtr get_return_type() { return std::make_shared<DataTypeNumber<Float64>>(); }

    double sum_x;
    double sum_y;
    double sum_xy;
    int64_t count;
};

template <typename T>
struct BaseDatadecimal {
    BaseDatadecimal() : sum_x(0), sum_y(0), sum_xy(0), count(0) {}
    virtual ~BaseDatadecimal() = default;

    void write(BufferWritable& buf) const {
        write_binary(sum_x, buf);
        write_binary(sum_y, buf);
        write_binary(sum_xy, buf);
        write_binary(count, buf);
    }

    void read(BufferReadable& buf) {
        read_binary(sum_x, buf);
        read_binary(sum_y, buf);
        read_binary(sum_xy, buf);
        read_binary(count, buf);
    }

    void reset() {
        sum_x = DecimalV2Value();
        sum_y = DecimalV2Value();
        sum_xy = DecimalV2Value();
        count = {};
    }

    DecimalV2Value get_pop_result() const {
        if (count == 1) {
            return DecimalV2Value();
        }
        DecimalV2Value count_dec = DecimalV2Value(static_cast<int128_t>(count));
        return sum_xy / count_dec - sum_x * sum_y / (count_dec * count_dec);
    }

    DecimalV2Value get_samp_result() const {
        DecimalV2Value count_dec = DecimalV2Value(static_cast<int128_t>(count));
        DecimalV2Value one = DecimalV2Value(static_cast<int128_t>(1));
        return sum_xy / (count_dec - one) - sum_x * sum_y / (count_dec * (count_dec - one));
    }

    void merge(const BaseDatadecimal& rhs) {
        if (rhs.count == 0) {
            return;
        }
        sum_x += rhs.sum_x;
        sum_y += rhs.sum_y;
        sum_xy += rhs.sum_xy;
        count += rhs.count;
    }

    void add(const IColumn* column_x, const IColumn* column_y, size_t row_num) {
        auto source_data_x = get_source_data(column_x, row_num);
        auto source_data_y = get_source_data(column_y, row_num);
        sum_x += source_data_x;
        sum_y += source_data_y;
        sum_xy += source_data_x * source_data_y;
        count += 1;
    }

    DecimalV2Value get_source_data(const IColumn* column, size_t row_num) {
        const auto& sources = assert_cast<const ColumnDecimal<T>&>(*column);
        Field field = sources[row_num];
        auto decimal_field = field.template get<DecimalField<T>>();
        int128_t value;
        if (decimal_field.get_scale() > DecimalV2Value::SCALE) {
            value = static_cast<int128_t>(decimal_field.get_value()) /
                    (decimal_field.get_scale_multiplier() / DecimalV2Value::ONE_BILLION);
        } else {
            value = static_cast<int128_t>(decimal_field.get_value()) *
                    (DecimalV2Value::ONE_BILLION / decimal_field.get_scale_multiplier());
        }
        return DecimalV2Value(value);
    }

    static DataTypePtr get_return_type() {
        return std::make_shared<DataTypeDecimal<Decimal128V2>>(27, 9);
    }

    DecimalV2Value sum_x;
    DecimalV2Value sum_y;
    DecimalV2Value sum_xy;
    int64_t count;
};

template <typename T, typename Data>
struct PopData : Data {
    using ColVecResult =
            std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<Decimal128V2>, ColumnFloat64>;
    void insert_result_into(IColumn& to) const {
        auto& col = assert_cast<ColVecResult&>(to);
        if constexpr (IsDecimalNumber<T>) {
            col.get_data().push_back(this->get_pop_result().value());
        } else {
            col.get_data().push_back(this->get_pop_result());
        }
    }
};

template <typename T, typename Data>
struct SampData_OLDER : Data {
    using ColVecResult =
            std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<Decimal128V2>, ColumnFloat64>;
    void insert_result_into(IColumn& to) const {
        ColumnNullable& nullable_column = assert_cast<ColumnNullable&>(to);
        if (this->count == 1 || this->count == 0) {
            nullable_column.insert_default();
        } else {
            auto& col = assert_cast<ColVecResult&>(nullable_column.get_nested_column());
            if constexpr (IsDecimalNumber<T>) {
                col.get_data().push_back(this->get_samp_result().value());
            } else {
                col.get_data().push_back(this->get_samp_result());
            }
            nullable_column.get_null_map_data().push_back(0);
        }
    }
};

template <typename T, typename Data>
struct SampData : Data {
    using ColVecResult =
            std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<Decimal128V2>, ColumnFloat64>;
    void insert_result_into(IColumn& to) const {
        auto& col = assert_cast<ColVecResult&>(to);
        if (this->count == 1 || this->count == 0) {
            col.insert_default();
        } else {
            if constexpr (IsDecimalNumber<T>) {
                col.get_data().push_back(this->get_samp_result().value());
            } else {
                col.get_data().push_back(this->get_samp_result());
            }
        }
    }
};

template <typename Data>
struct CovarName : Data {
    static const char* name() { return "covar"; }
};

template <typename Data>
struct CovarSampName : Data {
    static const char* name() { return "covar_samp"; }
};

template <bool is_pop, typename Data, bool is_nullable>
class AggregateFunctionSampCovariance
        : public IAggregateFunctionDataHelper<
                  Data, AggregateFunctionSampCovariance<is_pop, Data, is_nullable>> {
public:
    AggregateFunctionSampCovariance(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<
                      Data, AggregateFunctionSampCovariance<is_pop, Data, is_nullable>>(
                      argument_types_) {}

    String get_name() const override { return Data::name(); }

    DataTypePtr get_return_type() const override {
        if constexpr (is_pop) {
            return Data::get_return_type();
        } else {
            if (IAggregateFunction::version < AGG_FUNCTION_NULLABLE) {
                return make_nullable(Data::get_return_type());
            } else {
                return Data::get_return_type();
            }
        }
    }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena*) const override {
        if constexpr (is_pop) {
            this->data(place).add(columns[0], columns[1], row_num);
        } else {
            if constexpr (is_nullable) { //this if check could remove with old function
                const auto* nullable_column_x = check_and_get_column<ColumnNullable>(columns[0]);
                const auto* nullable_column_y = check_and_get_column<ColumnNullable>(columns[1]);
                if (!nullable_column_x->is_null_at(row_num) &&
                    !nullable_column_y->is_null_at(row_num)) {
                    this->data(place).add(&nullable_column_x->get_nested_column(),
                                          &nullable_column_y->get_nested_column(), row_num);
                }
            } else {
                this->data(place).add(columns[0], columns[1], row_num);
            }
        }
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
        this->data(place).insert_result_into(to);
    }
};

template <typename Data, bool is_nullable>
class AggregateFunctionSamp_OLDER final
        : public AggregateFunctionSampCovariance<NOTPOP, Data, is_nullable> {
public:
    AggregateFunctionSamp_OLDER(const DataTypes& argument_types_)
            : AggregateFunctionSampCovariance<NOTPOP, Data, is_nullable>(argument_types_) {}
};

template <typename Data, bool is_nullable>
class AggregateFunctionSamp final
        : public AggregateFunctionSampCovariance<NOTPOP, Data, is_nullable> {
public:
    AggregateFunctionSamp(const DataTypes& argument_types_)
            : AggregateFunctionSampCovariance<NOTPOP, Data, is_nullable>(argument_types_) {}
};

template <typename Data, bool is_nullable>
class AggregateFunctionPop final : public AggregateFunctionSampCovariance<POP, Data, is_nullable> {
public:
    AggregateFunctionPop(const DataTypes& argument_types_)
            : AggregateFunctionSampCovariance<POP, Data, is_nullable>(argument_types_) {}
};

} // namespace doris::vectorized