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

#include <stddef.h>
#include <stdint.h>

#include <boost/iterator/iterator_facade.hpp>
#include <cmath>
#include <memory>
#include <type_traits>

#include "agent/be_exec_version_manager.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/common/assert_cast.h"
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

template <typename T, bool is_stddev>
struct BaseData {
    BaseData() : mean(0.0), m2(0.0), count(0) {}
    virtual ~BaseData() = default;

    void write(BufferWritable& buf) const {
        write_binary(mean, buf);
        write_binary(m2, buf);
        write_binary(count, buf);
    }

    void read(BufferReadable& buf) {
        read_binary(mean, buf);
        read_binary(m2, buf);
        read_binary(count, buf);
    }

    void reset() {
        mean = 0.0;
        m2 = 0.0;
        count = 0;
    }

    double get_result(double res) const {
        auto inf_to_nan = [](double val) {
            // This function performs squaring operations, and due to differences in computation order,
            // it might produce different values such as inf and nan.
            // In MySQL, this will directly result in an error due to exceeding the double range.
            // For performance reasons, we are uniformly changing it to nan
            if (std::isinf(val)) {
                return std::nan("");
            }
            return val;
        };
        if constexpr (is_stddev) {
            return inf_to_nan(std::sqrt(res));
        } else {
            return inf_to_nan(res);
        }
    }

    double get_pop_result() const {
        if (count == 1) {
            return 0.0;
        }
        double res = m2 / count;
        return get_result(res);
    }

    double get_samp_result() const {
        double res = m2 / (count - 1);
        return get_result(res);
    }

    void merge(const BaseData& rhs) {
        if (rhs.count == 0) {
            return;
        }
        double delta = mean - rhs.mean;
        double sum_count = count + rhs.count;
        mean = rhs.mean + delta * count / sum_count;
        m2 = rhs.m2 + m2 + (delta * delta) * rhs.count * count / sum_count;
        count = int64_t(sum_count);
    }

    void add(const IColumn* column, size_t row_num) {
        const auto& sources =
                assert_cast<const ColumnVector<T>&, TypeCheckOnRelease::DISABLE>(*column);
        double source_data = sources.get_data()[row_num];

        double delta = source_data - mean;
        double r = delta / (1 + count);
        mean += r;
        m2 += count * delta * r;
        count += 1;
    }

    double mean;
    double m2;
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

    static DataTypePtr get_return_type() { return std::make_shared<DataTypeNumber<Float64>>(); }
};

// For this series of functions, the Decimal type is not supported
// because the operations involve squaring,
// which can easily exceed the range of the Decimal type.

template <typename Data>
struct StddevName : Data {
    static const char* name() { return "stddev"; }
};

template <typename Data>
struct VarianceName : Data {
    static const char* name() { return "variance"; }
};

template <typename Data>
struct VarianceSampName : Data {
    static const char* name() { return "variance_samp"; }
};

template <typename Data>
struct StddevSampName : Data {
    static const char* name() { return "stddev_samp"; }
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

    static DataTypePtr get_return_type() {
        return make_nullable(std::make_shared<DataTypeNumber<Float64>>());
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

    static DataTypePtr get_return_type() { return std::make_shared<DataTypeNumber<Float64>>(); }
};

template <bool is_pop, typename Data, bool is_nullable>
class AggregateFunctionSampVariance
        : public IAggregateFunctionDataHelper<
                  Data, AggregateFunctionSampVariance<is_pop, Data, is_nullable>> {
public:
    AggregateFunctionSampVariance(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<
                      Data, AggregateFunctionSampVariance<is_pop, Data, is_nullable>>(
                      argument_types_) {}

    String get_name() const override { return Data::name(); }

    DataTypePtr get_return_type() const override { return Data::get_return_type(); }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena*) const override {
        if constexpr (is_pop) {
            this->data(place).add(columns[0], row_num);
        } else {
            if constexpr (is_nullable) { //this if check could remove with old function
                const auto* nullable_column = check_and_get_column<ColumnNullable>(columns[0]);
                if (!nullable_column->is_null_at(row_num)) {
                    this->data(place).add(&nullable_column->get_nested_column(), row_num);
                }
            } else {
                this->data(place).add(columns[0], row_num);
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

//samp function it's always nullables, it's need to handle nullable column
//so return type and add function should processing null values
template <typename Data, bool is_nullable>
class AggregateFunctionSamp_OLDER final
        : public AggregateFunctionSampVariance<false, Data, is_nullable> {
public:
    AggregateFunctionSamp_OLDER(const DataTypes& argument_types_)
            : AggregateFunctionSampVariance<false, Data, is_nullable>(argument_types_) {}
};

template <typename Data, bool is_nullable>
class AggregateFunctionSamp final : public AggregateFunctionSampVariance<false, Data, is_nullable> {
public:
    AggregateFunctionSamp(const DataTypes& argument_types_)
            : AggregateFunctionSampVariance<false, Data, is_nullable>(argument_types_) {}
};

//pop function have use AggregateFunctionNullBase function, so needn't processing null values
template <typename Data, bool is_nullable>
class AggregateFunctionPop final : public AggregateFunctionSampVariance<true, Data, is_nullable> {
public:
    AggregateFunctionPop(const DataTypes& argument_types_)
            : AggregateFunctionSampVariance<true, Data, is_nullable>(argument_types_) {}
};

} // namespace doris::vectorized
