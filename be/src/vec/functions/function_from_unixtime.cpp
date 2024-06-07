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

#include <cctz/time_zone.h>

#include <type_traits>

#include "common/exception.h"
#include "common/status.h"
#include "olap/olap_common.h"
#include "runtime/runtime_state.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/common/int_exp.h"
#include "vec/common/string_ref.h"
#include "vec/core/column_numbers.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris::vectorized {

enum class ArgumentNumber { One, Two };

template <typename T, ArgumentNumber argument_num>
struct FromDecimal {
    static void apply(const ColumnDecimal<T>* col_dec, Int32 source_decimal_scale,
                      StringRef formatter, const cctz::time_zone& time_zone,
                      ColumnString::MutablePtr& col_res, ColumnUInt8::MutablePtr& col_null_map) {
        const size_t input_rows_count = col_dec->size();
        DateV2Value<DateTimeV2ValueType> max_datetime(0);
        // explain the magic number:
        // https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_from-unixtime
        max_datetime.from_unixtime(32536771199, 999999000, time_zone, 6);

        for (size_t i = 0; i < input_rows_count; ++i) {
            // For from_unixtime(x.10) we will get 10
            Int128 fraction = col_dec->get_fractional_part(i);
            if (fraction < 0) {
                col_res->insert_default();
                col_null_map->insert_value(1);
                continue;
            }

            // normalize the fraction to 9 digits to represent nanoseconds
            if (source_decimal_scale > 9) {
                fraction /= common::exp10_i32(source_decimal_scale - 9);
            } else {
                fraction *= common::exp10_i32(9 - source_decimal_scale);
            }

            source_decimal_scale = std::min(source_decimal_scale, 6);

            DateV2Value<DateTimeV2ValueType> datetime(0);
            datetime.from_unixtime(col_dec->get_whole_part(i), fraction, time_zone,
                                   source_decimal_scale);

            // The boundary check must be in the format as Datetime, we can not do the check by Decimal directly, because
            // of the time zone.
            if (datetime > max_datetime) {
                col_res->insert_default();
                col_null_map->insert_value(1);
                continue;
            }

            char buf[128];

            if constexpr (argument_num == ArgumentNumber::One) {
                // For decimal input, result str should have the same scale as the input
                if (datetime.to_string(buf, source_decimal_scale) != buf) {
                    col_res->insert_data(buf, strlen(buf));
                    col_null_map->insert_value(0);
                } else {
                    col_res->insert_default();
                    col_null_map->insert_value(1);
                }
            } else {
                if (datetime.to_format_string(formatter.data, formatter.size, buf)) {
                    col_res->insert_data(buf, strlen(buf));
                    col_null_map->insert_value(0);
                } else {
                    col_res->insert_default();
                    col_null_map->insert_value(1);
                }
            }
        }
    }
};

struct FromInt64 {
    // https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_from-unixtime
    // Keep consistent with MySQL
    static const int64_t TIMESTAMP_VALID_MAX = 32536771199;
    static void apply(const ColumnInt64* col_int64, Int32 scale, StringRef formatter,
                      const cctz::time_zone& time_zone, ColumnString::MutablePtr& col_res,
                      ColumnUInt8::MutablePtr& col_null_map) {
        const size_t input_rows_count = col_int64->size();
        for (size_t i = 0; i < input_rows_count; ++i) {
            const auto& data = col_int64->get_data()[i];
            if (formatter.size > 128 || data < 0 || data > TIMESTAMP_VALID_MAX) {
                col_res->insert_default();
                col_null_map->insert_value(1);
                continue;
            }

            DateV2Value<DateTimeV2ValueType> datetime;
            datetime.from_unixtime(static_cast<Int64>(data), 0, time_zone,
                                   static_cast<Int32>(scale));

            char buf[128];
            if (datetime.to_format_string(formatter.data, formatter.size, buf)) {
                col_res->insert_data(buf, strlen(buf));
                col_null_map->insert_value(0);
            } else {
                col_res->insert_default();
                col_null_map->insert_value(1);
            }
        }
    }
};

template <typename from_type, ArgumentNumber argument_num>
class FunctionFromUnixTime : public IFunction {
    static_assert(std::is_same_v<from_type, Int64> || IsDecimalNumber<from_type>,
                  "from_unixtime only support using Int64, Decimal as first arugment.");

public:
    static constexpr auto name = "from_unixtime";
    String get_name() const override { return name; }

    static FunctionPtr create() { return std::make_shared<FunctionFromUnixTime>(); }

    size_t get_number_of_arguments() const override {
        if constexpr (argument_num == ArgumentNumber::Two) {
            return 2;
        } else {
            return 1;
        }
    }

    ColumnNumbers get_arguments_that_are_always_constant() const override {
        if constexpr (argument_num == ArgumentNumber::Two) {
            return {1};
        } else {
            return {};
        }
    }

    DataTypes get_variadic_argument_types_impl() const override {
        if constexpr (IsDecimalNumber<from_type>) {
            if constexpr (argument_num == ArgumentNumber::Two) {
                return {std::make_shared<vectorized::DataTypeDecimal<Decimal32>>(9, 0),
                        std::make_shared<vectorized::DataTypeString>()};
            } else {
                return {std::make_shared<vectorized::DataTypeDecimal<Decimal32>>(9, 0)};
            }
        } else if constexpr (std::is_same_v<from_type, Int64>) {
            if constexpr (argument_num == ArgumentNumber::Two) {
                return {std::make_shared<vectorized::DataTypeInt64>(),
                        std::make_shared<vectorized::DataTypeString>()};
            } else {
                return {std::make_shared<vectorized::DataTypeInt64>()};
            }
        } else {
            throw doris::Exception(ErrorCode::INTERNAL_ERROR, "Illegal column type");
        }
    }

    DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) const override {
        if (arguments.empty()) {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                   "from_unixtime() requires at least 1 argument, 0 provided.");
        }

        if constexpr (argument_num == ArgumentNumber::Two) {
            if (arguments.size() != 2) {
                throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                                       "from_unixtime() requires 2 arguments, {} provided.",
                                       arguments.size());
            }
        }

        const TypeIndex type_idx = arguments[0].type->get_type_id();

        if constexpr (IsDecimalNumber<from_type>) {
            if (type_idx != TypeIndex::Decimal32 && type_idx != TypeIndex::Decimal64 &&
                type_idx != TypeIndex::Decimal128V3) {
                throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                                       "could not use {} as the first argument of from_unixtime()",
                                       arguments[0].type->get_name());
            }
        } else if constexpr (std::is_same_v<from_type, Int64>) {
            if (type_idx != TypeIndex::Int64) {
                throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                                       "could not use {} as the first argument of from_unixtime()",
                                       arguments[0].type->get_name());
            }
        } else {
            throw doris::Exception(ErrorCode::INTERNAL_ERROR, "Illegal column type");
        }

        return make_nullable(std::make_shared<DataTypeString>());
    }

    bool use_default_implementation_for_nulls() const override { return false; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        const ColumnPtr source_col = block.get_by_position(arguments[0]).column;
        const DataTypePtr data_type = block.get_by_position(arguments[0]).type;
        const cctz::time_zone& time_zone = context->state()->timezone_obj();

        const auto* nullable_column = check_and_get_column<ColumnNullable>(source_col.get());
        ColumnString::MutablePtr col_res = ColumnString::create();
        ColumnUInt8::MutablePtr col_null_map = ColumnUInt8::create();

        Int32 scale = 0;
        if constexpr (IsDecimalNumber<from_type>) {
            // The scale of the decimal column is used to determine the scale of the Datetime
            // If scale is larger than 6, it will be truncated to 6 in DateV2Value::from_unixtime
            // see https://github.com/apache/doris/blob/0939ab1271424449b508717daa77906ce6e71e01/be/src/vec/runtime/vdatetime_value.cpp#L3361
            scale = block.get_by_position(arguments[0]).type->get_scale();
        }

        // For integer input, do not print the microsecond part
        std::string formatter("%Y-%m-%d %H:%i:%s");
        if constexpr (argument_num == ArgumentNumber::Two) {
            // Use the second argument as the format string
            const IColumn& format_col = *block.get_by_position(arguments[1]).column;
            formatter = format_col.get_data_at(0);
        } else if constexpr (IsDecimalNumber<from_type>) {
            formatter = "";
        }

        if constexpr (IsDecimalNumber<from_type>) {
            if (data_type->get_type_id() == TypeIndex::Decimal32) {
                const auto* decimal_sources = assert_cast<const ColumnDecimal<Decimal32>*>(
                        nullable_column ? nullable_column->get_nested_column_ptr().get()
                                        : source_col.get());
                FromDecimal<Decimal32, argument_num>::apply(
                        decimal_sources, scale, StringRef(formatter.c_str(), formatter.size()),
                        time_zone, col_res, col_null_map);
            } else if (data_type->get_type_id() == TypeIndex::Decimal64) {
                const auto* decimal_sources = assert_cast<const ColumnDecimal<Decimal64>*>(
                        nullable_column ? nullable_column->get_nested_column_ptr().get()
                                        : source_col.get());
                FromDecimal<Decimal64, argument_num>::apply(
                        decimal_sources, scale, StringRef(formatter.c_str(), formatter.size()),
                        time_zone, col_res, col_null_map);
            } else if (data_type->get_type_id() == TypeIndex::Decimal128V3) {
                const auto* decimal_sources = assert_cast<const ColumnDecimal<Decimal128V3>*>(
                        nullable_column ? nullable_column->get_nested_column_ptr().get()
                                        : source_col.get());
                FromDecimal<Decimal128V3, argument_num>::apply(
                        decimal_sources, scale, StringRef(formatter.c_str(), formatter.size()),
                        time_zone, col_res, col_null_map);
            } else {
                return Status::InternalError("Illegal column {} of first argument of function {}",
                                             block.get_by_position(arguments[0]).column->get_name(),
                                             name);
            }
        } else if constexpr (std::is_same_v<from_type, Int64>) {
            FromInt64::apply(assert_cast<const ColumnInt64*>(source_col.get()), scale,
                             StringRef(formatter.c_str(), formatter.size()), time_zone, col_res,
                             col_null_map);
        } else {
            return Status::InternalError("Illegal column {} of first argument of function {}",
                                         block.get_by_position(arguments[0]).column->get_name(),
                                         name);
        }

        if (nullable_column) {
            const auto& origin_null_map = nullable_column->get_null_map_column().get_data();
            for (int i = 0; i < origin_null_map.size(); ++i) {
                col_null_map->get_data()[i] |= origin_null_map[i];
            }
        }

        block.get_by_position(result).column =
                ColumnNullable::create(std::move(col_res), std::move(col_null_map));

        return Status::OK();
    };
};

void register_function_from_unixtime(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionFromUnixTime<Decimal32, ArgumentNumber::One>>();
    factory.register_function<FunctionFromUnixTime<Decimal32, ArgumentNumber::Two>>();
    factory.register_function<FunctionFromUnixTime<Int64, ArgumentNumber::One>>();
    factory.register_function<FunctionFromUnixTime<Int64, ArgumentNumber::Two>>();
}

} // namespace doris::vectorized
