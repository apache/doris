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
#include "vec/common/assert_cast.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"

namespace doris::vectorized {

enum class STATISTICS_FUNCTION_KIND : uint8_t { SKEW_POP, KURT_POP };

inline std::string to_string(STATISTICS_FUNCTION_KIND kind) {
    switch (kind) {
    case STATISTICS_FUNCTION_KIND::SKEW_POP:
        return "skewness";
    case STATISTICS_FUNCTION_KIND::KURT_POP:
        return "kurtosis";
    default:
        return "Unknown";
    }
}

template <typename T, std::size_t _level>
struct StatFuncOneArg {
    using Type = T;
    using Data = VarMoments<Float64, _level>;
};

template <typename StatFunc, bool NullableInput>
class AggregateFunctionVarianceSimple
        : public IAggregateFunctionDataHelper<
                  typename StatFunc::Data,
                  AggregateFunctionVarianceSimple<StatFunc, NullableInput>> {
public:
    using InputCol = ColumnVector<typename StatFunc::Type>;
    using ResultCol = ColumnVector<Float64>;

    explicit AggregateFunctionVarianceSimple(STATISTICS_FUNCTION_KIND kind_,
                                             const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<
                      typename StatFunc::Data,
                      AggregateFunctionVarianceSimple<StatFunc, NullableInput>>(argument_types_),
              kind(kind_) {
        DCHECK(!argument_types_.empty());
    }

    String get_name() const override { return to_string(kind); }

    DataTypePtr get_return_type() const override {
        return make_nullable(std::make_shared<DataTypeFloat64>());
    }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena*) const override {
        if constexpr (NullableInput) {
            const ColumnNullable& column_with_nullable =
                    assert_cast<const ColumnNullable&, TypeCheckOnRelease::DISABLE>(*columns[0]);

            if (column_with_nullable.is_null_at(row_num)) {
                return;
            } else {
                this->data(place).add(assert_cast<const InputCol&, TypeCheckOnRelease::DISABLE>(
                                              column_with_nullable.get_nested_column())
                                              .get_data()[row_num]);
            }

        } else {
            this->data(place).add(
                    assert_cast<const InputCol&, TypeCheckOnRelease::DISABLE>(*columns[0])
                            .get_data()[row_num]);
        }
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
        ColumnNullable& dst_column_with_nullable = assert_cast<ColumnNullable&>(to);
        ResultCol* dst_column =
                assert_cast<ResultCol*>(&(dst_column_with_nullable.get_nested_column()));

        switch (kind) {
        case STATISTICS_FUNCTION_KIND::SKEW_POP: {
            // If input is empty set, we will get NAN from get_population()
            Float64 var_value = data.get_population();
            Float64 moments_3 = data.get_moment_3();

            if (std::isnan(var_value) || std::isnan(moments_3) || var_value <= 0) {
                dst_column_with_nullable.get_null_map_data().push_back(1);
                dst_column->insert_default();
            } else {
                dst_column_with_nullable.get_null_map_data().push_back(0);
                dst_column->get_data().push_back(
                        static_cast<Float64>(moments_3 / pow(var_value, 1.5)));
            }
            break;
        }
        case STATISTICS_FUNCTION_KIND::KURT_POP: {
            Float64 var_value = data.get_population();
            Float64 moments_4 = data.get_moment_4();

            if (std::isnan(var_value) || std::isnan(moments_4) || var_value <= 0) {
                dst_column_with_nullable.get_null_map_data().push_back(1);
                dst_column->insert_default();
            } else {
                dst_column_with_nullable.get_null_map_data().push_back(0);
                // kurtosis = E(X^4) / E(X^2)^2 - 3
                dst_column->get_data().push_back(
                        static_cast<Float64>(moments_4 / pow(var_value, 2)) - 3);
            }
            break;
        }
        default:
            throw doris::Exception(ErrorCode::INTERNAL_ERROR, "Unknown statistics function kind");
        }
    }

private:
    STATISTICS_FUNCTION_KIND kind;
};

} // namespace doris::vectorized