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

#include <stddef.h>
#include <stdint.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <cmath>
#include <memory>
#include <type_traits>
#include <utility>

#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_numbers.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/primitive_type.h"
#include "core/types.h"
#include "exprs/aggregate/aggregate_function.h"
#include "exprs/function/function.h"
#include "exprs/function/simple_function_factory.h"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris {
class FunctionWidthBucket : public IFunction {
public:
    static constexpr auto name = "width_bucket";
    static FunctionPtr create() { return std::make_shared<FunctionWidthBucket>(); }

    /// Get function name.
    String get_name() const override { return name; }

    bool is_variadic() const override { return false; }

    size_t get_number_of_arguments() const override { return 4; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeInt64>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        ColumnPtr expr_ptr =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        ColumnPtr min_value_ptr =
                block.get_by_position(arguments[1]).column->convert_to_full_column_if_const();
        ColumnPtr max_value_ptr =
                block.get_by_position(arguments[2]).column->convert_to_full_column_if_const();
        ColumnPtr num_buckets_ptr = block.get_by_position(arguments[3]).column;
        int64_t num_buckets = num_buckets_ptr->get_int(0);

        if (num_buckets <= 0) {
            return Status::InternalError(
                    "The desired number({}) of buckets must be a positive integer value.",
                    num_buckets);
        }

        auto nested_column_ptr = ColumnInt64::create(input_rows_count, 0);
        DataTypePtr expr_type = block.get_by_position(arguments[0]).type;

        if (!_execute_by_type(*expr_ptr, *min_value_ptr, *max_value_ptr, num_buckets,
                              nested_column_ptr, expr_type)) {
            return Status::InvalidArgument("Unsupported type for width_bucket: {}",
                                           expr_type->get_name());
        }

        block.replace_by_position(result, std::move(nested_column_ptr));
        return Status::OK();
    }

private:
    template <typename ColumnType>
    void _execute(const IColumn& expr_column, const IColumn& min_value_column,
                  const IColumn& max_value_column, const int64_t num_buckets,
                  ColumnInt64::MutablePtr& nested_column) const {
        const auto& expr_column_concrete = assert_cast<const ColumnType&>(expr_column);
        const auto& min_value_column_concrete = assert_cast<const ColumnType&>(min_value_column);
        const auto& max_value_column_concrete = assert_cast<const ColumnType&>(max_value_column);
        auto& nested_column_concrete = *nested_column;

        size_t input_rows_count = expr_column.size();
        const auto* expr_data = expr_column_concrete.get_data().data();
        const auto* min_value_data = min_value_column_concrete.get_data().data();
        const auto* max_value_data = max_value_column_concrete.get_data().data();
        auto* nested_data = nested_column_concrete.get_data().data();

        using ValueType = std::remove_cv_t<std::remove_reference_t<decltype(expr_data[0])>>;
        for (size_t i = 0; i < input_rows_count; ++i) {
            auto min_value = min_value_data[i];
            auto max_value = max_value_data[i];
            auto expr_value = expr_data[i];
            if (expr_value < min_value) {
                continue;
            } else if (expr_value >= max_value) {
                nested_data[i] = num_buckets + 1;
            } else if constexpr (std::is_integral_v<ValueType>) {
                const __int128 width =
                        static_cast<__int128>(max_value) - static_cast<__int128>(min_value);
                const __int128 distance =
                        static_cast<__int128>(expr_value) - static_cast<__int128>(min_value);
                nested_data[i] =
                        static_cast<int64_t>(1 + distance * static_cast<__int128>(num_buckets) / width);
            } else {
                const long double width =
                        static_cast<long double>(max_value) - static_cast<long double>(min_value);
                const long double scaled =
                        (static_cast<long double>(expr_value) - static_cast<long double>(min_value)) *
                        static_cast<long double>(num_buckets) / width;
                if (!std::isfinite(scaled)) {
                    continue;
                }
                nested_data[i] = static_cast<int64_t>(
                        1 + std::min(scaled, static_cast<long double>(num_buckets - 1)));
            }
        }
    }

    bool _execute_by_type(const IColumn& expr_column, const IColumn& min_value_column,
                          const IColumn& max_value_column, const int64_t num_buckets,
                          ColumnInt64::MutablePtr& nested_column_column,
                          DataTypePtr& expr_type) const {
        switch (expr_type->get_primitive_type()) {
        case PrimitiveType::TYPE_TINYINT:
            _execute<ColumnInt8>(expr_column, min_value_column, max_value_column, num_buckets,
                                 nested_column_column);
            break;
        case PrimitiveType::TYPE_SMALLINT:
            _execute<ColumnInt16>(expr_column, min_value_column, max_value_column, num_buckets,
                                  nested_column_column);
            break;
        case PrimitiveType::TYPE_INT:
            _execute<ColumnInt32>(expr_column, min_value_column, max_value_column, num_buckets,
                                  nested_column_column);
            break;
        case PrimitiveType::TYPE_BIGINT:
            _execute<ColumnInt64>(expr_column, min_value_column, max_value_column, num_buckets,
                                  nested_column_column);
            break;
        case PrimitiveType::TYPE_FLOAT:
            _execute<ColumnFloat32>(expr_column, min_value_column, max_value_column, num_buckets,
                                    nested_column_column);
            break;
        case PrimitiveType::TYPE_DOUBLE:
            _execute<ColumnFloat64>(expr_column, min_value_column, max_value_column, num_buckets,
                                    nested_column_column);
            break;
        default:
            return false;
            break;
        }
        return true;
    }
};

void register_function_width_bucket(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionWidthBucket>();
}

} // namespace doris
