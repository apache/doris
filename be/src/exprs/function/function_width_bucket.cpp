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
#include <array>
#include <boost/iterator/iterator_facade.hpp>
#include <cmath>
#include <limits>
#include <memory>
#include <type_traits>
#include <utility>

#include "common/exception.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_numbers.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/primitive_type.h"
#include "core/types.h"
#include "exec/common/util.hpp"
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
        auto type = std::make_shared<DataTypeInt64>();
        return std::ranges::any_of(arguments,
                                   [](const auto& argument) { return argument->is_nullable(); })
                       ? make_nullable(type)
                       : type;
    }

    bool use_default_implementation_for_nulls() const override { return false; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        auto null_map = ColumnUInt8::create(input_rows_count, 0);
        std::array<ColumnPtr, 4> columns;
        for (size_t i = 0; i < columns.size(); ++i) {
            columns[i] =
                    block.get_by_position(arguments[i]).column->convert_to_full_column_if_const();
            if (const auto* nullable = check_and_get_column<ColumnNullable>(*columns[i])) {
                if (i == 3 && nullable->is_null_at(0)) {
                    block.replace_by_position(
                            result, block.get_by_position(result).type->create_column_const(
                                            input_rows_count, Field()));
                    return Status::OK();
                }
                VectorizedUtils::update_null_map(null_map->get_data(),
                                                 nullable->get_null_map_data());
                columns[i] = nullable->get_nested_column_ptr();
            }
        }
        int64_t num_buckets = columns[3]->get_int(0);

        // The upper outside bucket is count + 1 and must also fit in the BIGINT result.
        if (num_buckets <= 0 || num_buckets == std::numeric_limits<Int64>::max()) {
            return Status::InvalidArgument(
                    "The number of buckets must be between 1 and 9223372036854775806, but was {}.",
                    num_buckets);
        }

        auto nested_column_ptr = ColumnInt64::create(input_rows_count, 0);
        DataTypePtr expr_type = remove_nullable(block.get_by_position(arguments[0]).type);

        if (!_execute_by_type(*columns[0], *columns[1], *columns[2], num_buckets, nested_column_ptr,
                              expr_type, null_map->get_data())) {
            return Status::InvalidArgument("Unsupported type for width_bucket: {}",
                                           expr_type->get_name());
        }

        if (block.get_by_position(result).type->is_nullable()) {
            block.replace_by_position(result, ColumnNullable::create(std::move(nested_column_ptr),
                                                                     std::move(null_map)));
        } else {
            block.replace_by_position(result, std::move(nested_column_ptr));
        }
        return Status::OK();
    }

private:
    template <typename ColumnType>
    void _execute(const IColumn& expr_column, const IColumn& min_value_column,
                  const IColumn& max_value_column, const int64_t num_buckets,
                  ColumnInt64::MutablePtr& nested_column, const NullMap& null_map) const {
        const auto& expr_column_concrete = assert_cast<const ColumnType&>(expr_column);
        const auto& min_value_column_concrete = assert_cast<const ColumnType&>(min_value_column);
        const auto& max_value_column_concrete = assert_cast<const ColumnType&>(max_value_column);
        auto& nested_column_concrete = *nested_column;

        size_t input_rows_count = expr_column.size();
        const auto* expr_data = expr_column_concrete.get_data().data();
        const auto* min_value_data = min_value_column_concrete.get_data().data();
        const auto* max_value_data = max_value_column_concrete.get_data().data();
        auto* nested_data = nested_column_concrete.get_data().data();

        for (size_t i = 0; i < input_rows_count; ++i) {
            // A NULL bound can have a zero payload. Do not report equal bounds for that row.
            if (null_map[i]) {
                continue;
            }
            auto min_value = min_value_data[i];
            auto max_value = max_value_data[i];
            if (min_value == max_value) {
                throw Exception(ErrorCode::INVALID_ARGUMENT,
                                "width_bucket bounds must be different");
            }
            if constexpr (std::is_floating_point_v<typename ColumnType::value_type>) {
                if (!std::isfinite(expr_data[i]) || !std::isfinite(min_value) ||
                    !std::isfinite(max_value)) {
                    throw Exception(ErrorCode::INVALID_ARGUMENT,
                                    "width_bucket values and bounds must be finite");
                }
            }
            if (expr_data[i] < min_value) {
                continue;
            } else if (expr_data[i] >= max_value) {
                nested_data[i] = num_buckets + 1;
            } else {
                if constexpr (std::is_integral_v<typename ColumnType::value_type>) {
                    // Widen before subtraction and multiply before division. For example,
                    // width_bucket(-1, -1, 0, 2) is 1 even though the bucket width is below 1.
                    Int128 width = static_cast<Int128>(max_value) - min_value;
                    Int128 distance = static_cast<Int128>(expr_data[i]) - min_value;
                    nested_data[i] = static_cast<Int64>(distance * num_buckets / width) + 1;
                } else {
                    long double width = static_cast<long double>(max_value) - min_value;
                    long double distance = static_cast<long double>(expr_data[i]) - min_value;
                    long double bucket = distance / width * num_buckets;
                    // Rounding near the upper bound must not produce an outside bucket.
                    nested_data[i] = static_cast<Int64>(std::min(
                                             bucket, static_cast<long double>(num_buckets - 1))) +
                                     1;
                }
            }
        }
    }

    bool _execute_by_type(const IColumn& expr_column, const IColumn& min_value_column,
                          const IColumn& max_value_column, const int64_t num_buckets,
                          ColumnInt64::MutablePtr& nested_column_column, DataTypePtr& expr_type,
                          const NullMap& null_map) const {
        switch (expr_type->get_primitive_type()) {
        case PrimitiveType::TYPE_TINYINT:
            _execute<ColumnInt8>(expr_column, min_value_column, max_value_column, num_buckets,
                                 nested_column_column, null_map);
            break;
        case PrimitiveType::TYPE_SMALLINT:
            _execute<ColumnInt16>(expr_column, min_value_column, max_value_column, num_buckets,
                                  nested_column_column, null_map);
            break;
        case PrimitiveType::TYPE_INT:
            _execute<ColumnInt32>(expr_column, min_value_column, max_value_column, num_buckets,
                                  nested_column_column, null_map);
            break;
        case PrimitiveType::TYPE_BIGINT:
            _execute<ColumnInt64>(expr_column, min_value_column, max_value_column, num_buckets,
                                  nested_column_column, null_map);
            break;
        case PrimitiveType::TYPE_FLOAT:
            _execute<ColumnFloat32>(expr_column, min_value_column, max_value_column, num_buckets,
                                    nested_column_column, null_map);
            break;
        case PrimitiveType::TYPE_DOUBLE:
            _execute<ColumnFloat64>(expr_column, min_value_column, max_value_column, num_buckets,
                                    nested_column_column, null_map);
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
