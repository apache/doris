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
#include <cstdint>
#include <string>
#include <type_traits>

#include "common/exception.h"
#include "common/status.h"
#include "moments.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/moments.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"

namespace doris::vectorized {

enum class StatisticsFunctionKind : uint8_t { skewPop, kurtPop };

inline std::string to_string(StatisticsFunctionKind kind) {
    switch (kind) {
    case StatisticsFunctionKind::skewPop:
        return "skewness";
    case StatisticsFunctionKind::kurtPop:
        return "kurtosis";
    default:
        return "Unknown";
    }
}

template <typename T, std::size_t _level>
struct StatFuncOneArg {
    using Type1 = T;
    using Type2 = T;
    using ResultType = Float64;
    using Data = VarMoments<ResultType, _level>;

    static constexpr UInt32 num_args = 1;
};

template <typename StatFunc>
class AggregateFunctionVarianceSimple
        : public IAggregateFunctionDataHelper<typename StatFunc::Data,
                                              AggregateFunctionVarianceSimple<StatFunc>> {
public:
    using T1 = typename StatFunc::Type1;
    using T2 = typename StatFunc::Type2;
    using ColVecT1 = ColumnVectorOrDecimal<T1>;
    using ColVecT2 = ColumnVectorOrDecimal<T2>;
    using ResultType = typename StatFunc::ResultType;
    using ColVecResult = ColumnVector<ResultType>;

    explicit AggregateFunctionVarianceSimple(StatisticsFunctionKind kind_,
                                             const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<typename StatFunc::Data,
                                           AggregateFunctionVarianceSimple<StatFunc>>(
                      argument_types_),
              kind(kind_) {
        DCHECK(!argument_types_.empty());
    }

    String get_name() const override { return to_string(kind); }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeFloat64>(); }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena*) const override {
        this->data(place).add(assert_cast<const ColVecT1&>(*columns[0]).get_data()[row_num]);
    }

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
        const auto& data = this->data(place);
        ColVecResult* dst_column = assert_cast<ColVecResult*>(&to);

        switch (kind) {
        case StatisticsFunctionKind::skewPop: {
            // If input is empty set, we will get NAN from getPopulation()
            ResultType var_value = data.getPopulation();
            if (!std::isnan(var_value) && var_value > 0) {
                ResultType moments3 = data.getMoment3();
                if (!std::isnan(moments3)) [[likely]] {
                    dst_column->get_data().push_back(
                            static_cast<ResultType>(moments3 / pow(var_value, 1.5)));
                } else {
                    throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                                           "skewness calculation error, result is NAN");
                }
            } else {
                // Empty input, result column will be:
                // Nullable if without group by
                // Nullable if with group by, and input column is nullable
                // Non-Nullable if with group by, and input column is non-nullable
                dst_column->insert_default();
            }
            break;
        }
        case StatisticsFunctionKind::kurtPop: {
            ResultType var_value = data.getPopulation();
            if (!std::isnan(var_value) && var_value > 0) {
                ResultType moments4 = data.getMoment4();
                if (!std::isnan(moments4)) [[likely]] {
                    dst_column->get_data().push_back(
                            static_cast<ResultType>(moments4 / pow(var_value, 2)));
                } else {
                    throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                                           "skewness calculation error, result is NAN");
                }
            } else {
                dst_column->insert_default();
            }
            break;
        }
        default:
            throw doris::Exception(ErrorCode::INTERNAL_ERROR, "Unknown statistics function kind");
        }
    }

private:
    StatisticsFunctionKind kind;
};

} // namespace doris::vectorized