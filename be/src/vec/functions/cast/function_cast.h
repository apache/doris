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

#include <cctz/time_zone.h>
#include <fmt/format.h>
#include <gen_cpp/FrontendService_types.h>
#include <glog/logging.h>
#include <stddef.h>
#include <stdint.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <cmath>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>

#include "cast_base.h"
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/status.h"
#include "runtime/runtime_state.h"
#include "runtime/type_limit.h"
#include "udf/udf.h"
#include "util/jsonb_document.h"
#include "util/jsonb_stream.h"
#include "util/jsonb_writer.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_map.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_object.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_struct.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_common.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_buffer.hpp"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/core/call_on_type_index.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/columns_with_type_and_name.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_bitmap.h"
#include "vec/data_types/data_type_date.h"
#include "vec/data_types/data_type_date_or_datetime_v2.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_hll.h"
#include "vec/data_types/data_type_ipv4.h"
#include "vec/data_types/data_type_ipv6.h"
#include "vec/data_types/data_type_jsonb.h"
#include "vec/data_types/data_type_map.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_object.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_struct.h"
#include "vec/data_types/data_type_time.h"
#include "vec/data_types/serde/data_type_serde.h"
#include "vec/functions/function.h"
#include "vec/functions/function_helpers.h"
#include "vec/io/io_helper.h"
#include "vec/io/reader_buffer.h"
#include "vec/runtime/time_value.h"
#include "vec/runtime/vdatetime_value.h"

class DateLUTImpl;

namespace doris {
namespace vectorized {
template <typename T>
class ColumnDecimal;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {
/** Type conversion functions.
  * toType - conversion in "natural way";
  */
inline UInt32 extract_to_decimal_scale(const ColumnWithTypeAndName& named_column) {
    const auto* arg_type = named_column.type.get();
    bool ok = check_and_get_data_type<DataTypeUInt64>(arg_type) ||
              check_and_get_data_type<DataTypeUInt32>(arg_type) ||
              check_and_get_data_type<DataTypeUInt16>(arg_type) ||
              check_and_get_data_type<DataTypeUInt8>(arg_type);
    if (!ok) {
        throw doris::Exception(ErrorCode::INVALID_ARGUMENT, "Illegal type of toDecimal() scale {}",
                               named_column.type->get_name());
    }

    Field field;
    named_column.column->get(0, field);
    return field.get<UInt32>();
}

struct PrecisionScaleArg {
    UInt32 precision;
    UInt32 scale;
};

/** Conversion of number types to each other, enums to numbers, dates and datetimes to numbers and back: done by straight assignment.
  *  (Date is represented internally as number of days from some day; DateTime - as unix timestamp)
  */
template <typename FromDataType, typename ToDataType>
struct ConvertImpl {
    using FromFieldType = typename FromDataType::FieldType;
    using ToFieldType = typename ToDataType::FieldType;

    // `static_cast_set` is introduced to wrap `static_cast` and handle special cases.
    // Doris uses `uint8` to represent boolean values internally.
    // Directly `static_cast` to `uint8` can result in non-0/1 values,
    // To address this, `static_cast_set` performs an additional check:
    //  For `uint8` types, it explicitly uses `static_cast<bool>` to ensure
    //  the result is either 0 or 1.
    static void static_cast_set(ToFieldType& to, const FromFieldType& from) {
        // uint8_t now use as boolean in doris
        if constexpr (std::is_same_v<uint8_t, ToFieldType>) {
            to = static_cast<bool>(from);
        } else {
            to = static_cast<ToFieldType>(from);
        }
    }

    template <typename Additions = void*>
    static Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          uint32_t result, size_t input_rows_count,
                          Additions additions = Additions()) {
        const ColumnWithTypeAndName& named_from = block.get_by_position(arguments[0]);

        using ColVecFrom =
                std::conditional_t<IsDecimalNumber<FromFieldType>, ColumnDecimal<FromFieldType>,
                                   ColumnVector<FromFieldType>>;
        using ColVecTo = std::conditional_t<IsDecimalNumber<ToFieldType>,
                                            ColumnDecimal<ToFieldType>, ColumnVector<ToFieldType>>;

        if constexpr (IsDataTypeDecimal<FromDataType> || IsDataTypeDecimal<ToDataType>) {
            if constexpr (!(IsDataTypeDecimalOrNumber<FromDataType> ||
                            IsDatelikeV1Types<FromDataType> || IsDatelikeV2Types<FromDataType>) ||
                          !IsDataTypeDecimalOrNumber<ToDataType>) {
                return Status::RuntimeError("Illegal column {} of first argument of function cast",
                                            named_from.column->get_name());
            }
        }

        if (const ColVecFrom* col_from =
                    check_and_get_column<ColVecFrom>(named_from.column.get())) {
            typename ColVecTo::MutablePtr col_to = nullptr;
            UInt32 from_precision = NumberTraits::max_ascii_len<FromFieldType>();
            UInt32 from_scale = 0;

            if constexpr (IsDataTypeDecimal<FromDataType>) {
                const auto& from_decimal_type = assert_cast<const FromDataType&>(*named_from.type);
                from_precision = from_decimal_type.get_precision();
                from_scale = from_decimal_type.get_scale();
            }

            UInt32 to_max_digits = 0;
            UInt32 to_precision = 0;
            UInt32 to_scale = 0;

            ToFieldType max_result {0};
            ToFieldType min_result {0};
            if constexpr (IsDataTypeDecimal<ToDataType>) {
                to_max_digits = NumberTraits::max_ascii_len<typename ToFieldType::NativeType>();

                to_precision = ((PrecisionScaleArg)additions).precision;
                ToDataType::check_type_precision(to_precision);

                to_scale = ((PrecisionScaleArg)additions).scale;
                ToDataType::check_type_scale(to_scale);
                col_to = ColVecTo::create(0, to_scale);

                max_result = ToDataType::get_max_digits_number(to_precision);
                min_result = -max_result;
            } else {
                col_to = ColVecTo::create();
            }
            if constexpr (IsDataTypeNumber<ToDataType>) {
                max_result = type_limit<ToFieldType>::max();
                min_result = type_limit<ToFieldType>::min();
            }
            if constexpr (std::is_integral_v<ToFieldType>) {
                to_max_digits = NumberTraits::max_ascii_len<ToFieldType>();
                to_precision = to_max_digits;
            }

            const auto& vec_from = col_from->get_data();
            auto& vec_to = col_to->get_data();
            size_t size = vec_from.size();
            vec_to.resize(size);

            if constexpr (IsDataTypeDecimal<FromDataType> || IsDataTypeDecimal<ToDataType>) {
                // the result is rounded when doing cast, so it may still overflow after rounding
                // if destination integer digit count is the same as source integer digit count.
                bool narrow_integral = context->check_overflow_for_decimal() &&
                                       (to_precision - to_scale) <= (from_precision - from_scale);

                bool multiply_may_overflow = context->check_overflow_for_decimal();
                if (to_scale > from_scale) {
                    multiply_may_overflow &=
                            (from_precision + to_scale - from_scale) >= to_max_digits;
                }

                std::visit(
                        [&](auto multiply_may_overflow, auto narrow_integral) {
                            if constexpr (IsDataTypeDecimal<FromDataType> &&
                                          IsDataTypeDecimal<ToDataType>) {
                                convert_decimal_cols<FromDataType, ToDataType,
                                                     multiply_may_overflow, narrow_integral>(
                                        vec_from.data(), vec_to.data(), from_precision,
                                        vec_from.get_scale(), to_precision, vec_to.get_scale(),
                                        vec_from.size());
                            } else if constexpr (IsDataTypeDecimal<FromDataType>) {
                                convert_from_decimal<FromDataType, ToDataType, narrow_integral>(
                                        vec_to.data(), vec_from.data(), from_precision,
                                        vec_from.get_scale(), min_result, max_result, size);
                            } else {
                                convert_to_decimal<FromDataType, ToDataType, multiply_may_overflow,
                                                   narrow_integral>(
                                        vec_to.data(), vec_from.data(), from_scale, to_precision,
                                        to_scale, min_result, max_result, size);
                            }
                        },
                        make_bool_variant(multiply_may_overflow),
                        make_bool_variant(narrow_integral));

                block.replace_by_position(result, std::move(col_to));

                return Status::OK();
            } else if constexpr (IsDatelikeV1Types<FromDataType>) {
                for (size_t i = 0; i < size; ++i) {
                    if constexpr (IsDatelikeV1Types<ToDataType>) {
                        vec_to[i] = static_cast<ToFieldType>(vec_from[i]);
                        if constexpr (IsDateTimeType<ToDataType>) {
                            DataTypeDateTime::cast_to_date_time(vec_to[i]);
                        } else {
                            DataTypeDate::cast_to_date(vec_to[i]);
                        }
                    } else if constexpr (IsDateV2Type<ToDataType>) {
                        DataTypeDateV2::cast_from_date(vec_from[i], vec_to[i]);
                    } else if constexpr (IsDateTimeV2Type<ToDataType>) {
                        DataTypeDateTimeV2::cast_from_date(vec_from[i], vec_to[i]);
                    } else {
                        static_cast_set(
                                vec_to[i],
                                reinterpret_cast<const VecDateTimeValue&>(vec_from[i]).to_int64());
                    }
                }
            } else if constexpr (IsDatelikeV2Types<FromDataType>) {
                for (size_t i = 0; i < size; ++i) {
                    if constexpr (IsDatelikeV2Types<ToDataType>) {
                        if constexpr (IsDateTimeV2Type<ToDataType> && IsDateV2Type<FromDataType>) {
                            DataTypeDateV2::cast_to_date_time_v2(vec_from[i], vec_to[i]);
                        } else if constexpr (IsDateTimeV2Type<FromDataType> &&
                                             IsDateV2Type<ToDataType>) {
                            DataTypeDateTimeV2::cast_to_date_v2(vec_from[i], vec_to[i]);
                        } else {
                            UInt32 scale = additions;
                            vec_to[i] = ToFieldType(vec_from[i] / std::pow(10, 6 - scale));
                        }
                    } else if constexpr (IsDatelikeV1Types<ToDataType>) {
                        if constexpr (IsDateTimeType<ToDataType> && IsDateV2Type<FromDataType>) {
                            DataTypeDateV2::cast_to_date_time(vec_from[i], vec_to[i]);
                        } else if constexpr (IsDateType<ToDataType> && IsDateV2Type<FromDataType>) {
                            DataTypeDateV2::cast_to_date(vec_from[i], vec_to[i]);
                        } else if constexpr (IsDateTimeType<ToDataType> &&
                                             IsDateTimeV2Type<FromDataType>) {
                            DataTypeDateTimeV2::cast_to_date_time(vec_from[i], vec_to[i]);
                        } else if constexpr (IsDateType<ToDataType> &&
                                             IsDateTimeV2Type<FromDataType>) {
                            DataTypeDateTimeV2::cast_to_date(vec_from[i], vec_to[i]);
                        } else {
                            return Status::InvalidArgument("Wrong cast expression!");
                        }
                    } else {
                        if constexpr (IsDateTimeV2Type<FromDataType>) {
                            static_cast_set(
                                    vec_to[i],
                                    reinterpret_cast<const DateV2Value<DateTimeV2ValueType>&>(
                                            vec_from[i])
                                            .to_int64());
                        } else {
                            static_cast_set(vec_to[i],
                                            reinterpret_cast<const DateV2Value<DateV2ValueType>&>(
                                                    vec_from[i])
                                                    .to_int64());
                        }
                    }
                }
            } else {
                if constexpr (IsDataTypeNumber<FromDataType> &&
                              std::is_same_v<ToDataType, DataTypeTimeV2>) {
                    // 300 -> 00:03:00  360 will be parse failed , so value maybe null
                    ColumnUInt8::MutablePtr col_null_map_to;
                    ColumnUInt8::Container* vec_null_map_to = nullptr;
                    col_null_map_to = ColumnUInt8::create(size, 0);
                    vec_null_map_to = &col_null_map_to->get_data();
                    for (size_t i = 0; i < size; ++i) {
                        (*vec_null_map_to)[i] = !TimeValue::try_parse_time(vec_from[i], vec_to[i]);
                    }
                    block.get_by_position(result).column =
                            ColumnNullable::create(std::move(col_to), std::move(col_null_map_to));
                    return Status::OK();
                } else if constexpr ((std::is_same_v<FromDataType, DataTypeIPv4>)&&(
                                             std::is_same_v<ToDataType, DataTypeIPv6>)) {
                    for (size_t i = 0; i < size; ++i) {
                        map_ipv4_to_ipv6(vec_from[i], reinterpret_cast<UInt8*>(&vec_to[i]));
                    }
                } else {
                    for (size_t i = 0; i < size; ++i) {
                        static_cast_set(vec_to[i], vec_from[i]);
                    }
                }
            }

            block.replace_by_position(result, std::move(col_to));
        } else {
            return Status::RuntimeError("Illegal column {} of first argument of function cast",
                                        named_from.column->get_name());
        }
        return Status::OK();
    }
};

/** If types are identical, just take reference to column.
  */
template <typename T>
    requires(!T::is_parametric)
struct ConvertImpl<T, T> {
    static Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          uint32_t result, size_t /*input_rows_count*/) {
        block.get_by_position(result).column = block.get_by_position(arguments[0]).column;
        return Status::OK();
    }
};

// using other type cast to Date/DateTime, unless String
// Date/DateTime
template <typename FromDataType, typename ToDataType, typename Name>
struct ConvertImplToTimeType {
    using FromFieldType = typename FromDataType::FieldType;
    using ToFieldType = typename ToDataType::FieldType;

    static Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          uint32_t result, size_t /*input_rows_count*/) {
        const ColumnWithTypeAndName& named_from = block.get_by_position(arguments[0]);

        using ColVecFrom =
                std::conditional_t<IsDecimalNumber<FromFieldType>, ColumnDecimal<FromFieldType>,
                                   ColumnVector<FromFieldType>>;

        using DateValueType = std::conditional_t<
                IsDatelikeV2Types<ToDataType>,
                std::conditional_t<IsDateV2Type<ToDataType>, DateV2Value<DateV2ValueType>,
                                   DateV2Value<DateTimeV2ValueType>>,
                VecDateTimeValue>;
        using ColVecTo = ColumnVector<ToFieldType>;

        if (const ColVecFrom* col_from =
                    check_and_get_column<ColVecFrom>(named_from.column.get())) {
            const auto& vec_from = col_from->get_data();
            size_t size = vec_from.size();

            // create nested column
            auto col_to = ColVecTo::create(size);
            auto& vec_to = col_to->get_data();

            // create null column
            ColumnUInt8::MutablePtr col_null_map_to;
            col_null_map_to = ColumnUInt8::create(size, 0);
            auto& vec_null_map_to = col_null_map_to->get_data();

            if constexpr (std::is_same_v<FromDataType, DataTypeTimeV2>) {
                DateValueType current_date_value;
                current_date_value.from_unixtime(context->state()->timestamp_ms() / 1000,
                                                 context->state()->timezone_obj());
                uint32_t scale = 0;
                // Only DateTimeV2 has scale
                if (std::is_same_v<ToDataType, DataTypeDateTimeV2>) {
                    scale = remove_nullable(block.get_by_position(result).type)->get_scale();
                }
                // According to MySQL rules, when casting time type to date/datetime,
                // the current date is added to the time
                // So here we need to clear the time part
                current_date_value.reset_time_part();
                for (size_t i = 0; i < size; ++i) {
                    auto& date_value = reinterpret_cast<DateValueType&>(vec_to[i]);
                    date_value = current_date_value;
                    int64_t microsecond = TimeValue::round_time(vec_from[i], scale);
                    // Only TimeV2 type needs microseconds
                    if constexpr (IsDatelikeV2Types<ToDataType>) {
                        vec_null_map_to[i] = !date_value.template date_add_interval<MICROSECOND>(
                                TimeInterval {MICROSECOND, microsecond, false});
                    } else {
                        vec_null_map_to[i] =
                                !date_value.template date_add_interval<SECOND>(TimeInterval {
                                        SECOND, microsecond / TimeValue::ONE_SECOND_MICROSECONDS,
                                        false});
                    }

                    // DateType of VecDateTimeValue should cast to date
                    if constexpr (IsDateType<ToDataType>) {
                        date_value.cast_to_date();
                    } else if constexpr (IsDateTimeType<ToDataType>) {
                        date_value.to_datetime();
                    }
                }
            } else {
                for (size_t i = 0; i < size; ++i) {
                    auto& date_value = reinterpret_cast<DateValueType&>(vec_to[i]);
                    vec_null_map_to[i] = !date_value.from_date_int64(int64_t(vec_from[i]));
                    // DateType of VecDateTimeValue should cast to date
                    if constexpr (IsDateType<ToDataType>) {
                        date_value.cast_to_date();
                    } else if constexpr (IsDateTimeType<ToDataType>) {
                        date_value.to_datetime();
                    }
                }
            }
            block.get_by_position(result).column =
                    ColumnNullable::create(std::move(col_to), std::move(col_null_map_to));
        } else {
            return Status::RuntimeError("Illegal column {} of first argument of function {}",
                                        named_from.column->get_name(), Name::name);
        }

        return Status::OK();
    }
};
template <typename ToDataType>
struct ConvertImpl<DataTypeString, ToDataType> {
    template <typename Additions = void*>
    static Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          uint32_t result, size_t input_rows_count,
                          Additions additions [[maybe_unused]] = Additions()) {
        return Status::RuntimeError("not support convert from string");
    }
};

template <typename DataType, typename FromDataType = void*>
bool try_parse_impl(typename DataType::FieldType& x, ReadBuffer& rb, FunctionContext* context,
                    UInt32 scale [[maybe_unused]] = 0) {
    if constexpr (IsDateTimeType<DataType>) {
        return try_read_datetime_text(x, rb, context->state()->timezone_obj());
    }

    if constexpr (IsDateType<DataType>) {
        return try_read_date_text(x, rb, context->state()->timezone_obj());
    }

    if constexpr (IsDateV2Type<DataType>) {
        return try_read_date_v2_text(x, rb, context->state()->timezone_obj());
    }

    if constexpr (IsDateTimeV2Type<DataType>) {
        return try_read_datetime_v2_text(x, rb, context->state()->timezone_obj(), scale);
    }

    if constexpr (IsIPv4Type<DataType>) {
        return try_read_ipv4_text(x, rb);
    }

    if constexpr (IsIPv6Type<DataType>) {
        return try_read_ipv6_text(x, rb);
    }

    if constexpr (std::is_same_v<DataTypeString, FromDataType> &&
                  std::is_same_v<DataTypeTimeV2, DataType>) {
        // cast from string to time(float64)
        auto len = rb.count();
        auto s = rb.position();
        rb.position() = rb.end(); // make is_all_read = true
        auto ret = TimeValue::try_parse_time(s, len, x, context->state()->timezone_obj());
        return ret;
    }
    if constexpr (std::is_floating_point_v<typename DataType::FieldType>) {
        return try_read_float_text(x, rb);
    }

    // uint8_t now use as boolean in doris
    if constexpr (std::is_same_v<typename DataType::FieldType, uint8_t>) {
        return try_read_bool_text(x, rb);
    }

    if constexpr (std::is_integral_v<typename DataType::FieldType>) {
        return try_read_int_text(x, rb);
    }
}

template <typename DataType, typename Additions = void*>
StringParser::ParseResult try_parse_decimal_impl(typename DataType::FieldType& x, ReadBuffer& rb,
                                                 Additions additions
                                                 [[maybe_unused]] = Additions()) {
    if constexpr (IsDataTypeDecimalV2<DataType>) {
        UInt32 scale = ((PrecisionScaleArg)additions).scale;
        UInt32 precision = ((PrecisionScaleArg)additions).precision;
        return try_read_decimal_text<TYPE_DECIMALV2>(x, rb, precision, scale);
    }

    if constexpr (std::is_same_v<DataTypeDecimal<Decimal32>, DataType>) {
        UInt32 scale = ((PrecisionScaleArg)additions).scale;
        UInt32 precision = ((PrecisionScaleArg)additions).precision;
        return try_read_decimal_text<TYPE_DECIMAL32>(x, rb, precision, scale);
    }

    if constexpr (std::is_same_v<DataTypeDecimal<Decimal64>, DataType>) {
        UInt32 scale = ((PrecisionScaleArg)additions).scale;
        UInt32 precision = ((PrecisionScaleArg)additions).precision;
        return try_read_decimal_text<TYPE_DECIMAL64>(x, rb, precision, scale);
    }

    if constexpr (IsDataTypeDecimal128V3<DataType>) {
        UInt32 scale = ((PrecisionScaleArg)additions).scale;
        UInt32 precision = ((PrecisionScaleArg)additions).precision;
        return try_read_decimal_text<TYPE_DECIMAL128I>(x, rb, precision, scale);
    }

    if constexpr (IsDataTypeDecimal256<DataType>) {
        UInt32 scale = ((PrecisionScaleArg)additions).scale;
        UInt32 precision = ((PrecisionScaleArg)additions).precision;
        return try_read_decimal_text<TYPE_DECIMAL256>(x, rb, precision, scale);
    }
}

// always from DataTypeString
template <typename ToDataType>
struct StringParsing {
    using ToFieldType = typename ToDataType::FieldType;

    static bool is_all_read(ReadBuffer& in) { return in.eof(); }

    template <typename Additions = void*>
    static Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          uint32_t result, size_t input_rows_count,
                          Additions additions [[maybe_unused]] = Additions()) {
        using ColVecTo = std::conditional_t<IsDecimalNumber<ToFieldType>,
                                            ColumnDecimal<ToFieldType>, ColumnVector<ToFieldType>>;

        const IColumn* col_from = block.get_by_position(arguments[0]).column.get();
        const auto* col_from_string = check_and_get_column<ColumnString>(col_from);

        if (!col_from_string) {
            return Status::RuntimeError("Illegal column {} of first argument of function cast",
                                        col_from->get_name());
        }

        size_t row = input_rows_count;
        typename ColVecTo::MutablePtr col_to = nullptr;

        if constexpr (IsDataTypeDecimal<ToDataType>) {
            UInt32 scale = ((PrecisionScaleArg)additions).scale;
            ToDataType::check_type_scale(scale);
            col_to = ColVecTo::create(row, scale);
        } else {
            col_to = ColVecTo::create(row);
        }

        typename ColVecTo::Container& vec_to = col_to->get_data();

        ColumnUInt8::MutablePtr col_null_map_to;
        ColumnUInt8::Container* vec_null_map_to [[maybe_unused]] = nullptr;
        col_null_map_to = ColumnUInt8::create(row, 0);
        vec_null_map_to = &col_null_map_to->get_data();

        const ColumnString::Chars* chars = &col_from_string->get_chars();
        const IColumn::Offsets* offsets = &col_from_string->get_offsets();

        [[maybe_unused]] UInt32 scale = 0;
        if constexpr (IsDataTypeDateTimeV2<ToDataType>) {
            const auto* type = assert_cast<const DataTypeDateTimeV2*>(
                    block.get_by_position(result).type.get());
            scale = type->get_scale();
        }

        size_t current_offset = 0;

        for (size_t i = 0; i < row; ++i) {
            size_t next_offset = (*offsets)[i];
            size_t string_size = next_offset - current_offset;

            ReadBuffer read_buffer(&(*chars)[current_offset], string_size);

            bool parsed;
            if constexpr (IsDataTypeDecimal<ToDataType>) {
                ToDataType::check_type_precision((PrecisionScaleArg(additions).precision));
                StringParser::ParseResult res = try_parse_decimal_impl<ToDataType>(
                        vec_to[i], read_buffer, PrecisionScaleArg(additions));
                parsed = (res == StringParser::PARSE_SUCCESS ||
                          res == StringParser::PARSE_OVERFLOW ||
                          res == StringParser::PARSE_UNDERFLOW);
            } else if constexpr (IsDataTypeDateTimeV2<ToDataType>) {
                parsed = try_parse_impl<ToDataType>(vec_to[i], read_buffer, context, scale);
            } else {
                parsed =
                        try_parse_impl<ToDataType, DataTypeString>(vec_to[i], read_buffer, context);
            }
            (*vec_null_map_to)[i] = !parsed || !is_all_read(read_buffer);
            current_offset = next_offset;
        }

        block.get_by_position(result).column =
                ColumnNullable::create(std::move(col_to), std::move(col_null_map_to));
        return Status::OK();
    }
};
template <>
struct ConvertImpl<DataTypeString, DataTypeDecimal<Decimal32>>
        : StringParsing<DataTypeDecimal<Decimal32>> {};
template <>
struct ConvertImpl<DataTypeString, DataTypeDecimal<Decimal64>>
        : StringParsing<DataTypeDecimal<Decimal64>> {};
template <>
struct ConvertImpl<DataTypeString, DataTypeDecimal<Decimal128V2>>
        : StringParsing<DataTypeDecimal<Decimal128V2>> {};
template <>
struct ConvertImpl<DataTypeString, DataTypeDecimal<Decimal128V3>>
        : StringParsing<DataTypeDecimal<Decimal128V3>> {};
template <>
struct ConvertImpl<DataTypeString, DataTypeDecimal<Decimal256>>
        : StringParsing<DataTypeDecimal<Decimal256>> {};
template <>
struct ConvertImpl<DataTypeString, DataTypeIPv4> : StringParsing<DataTypeIPv4> {};
template <>
struct ConvertImpl<DataTypeString, DataTypeIPv6> : StringParsing<DataTypeIPv6> {};

struct NameCast {
    static constexpr auto name = "CAST";
};

template <typename ToDataType, typename Name>
class FunctionConvertFromString : public IFunction {
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create() { return std::make_shared<FunctionConvertFromString>(); }
    String get_name() const override { return name; }

    bool is_variadic() const override { return true; }
    size_t get_number_of_arguments() const override { return 0; }

    ColumnNumbers get_arguments_that_are_always_constant() const override { return {1}; }

    // This function should not be called for get DateType Ptr
    // using the FunctionCast::get_return_type_impl
    DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) const override {
        DataTypePtr res;
        if constexpr (IsDataTypeDecimal<ToDataType>) {
            auto error_type = std::make_shared<ToDataType>();
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                   "something wrong type in function {}.", get_name(),
                                   error_type->get_name());
        } else {
            res = std::make_shared<ToDataType>();
        }

        return res;
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        const IDataType* from_type = block.get_by_position(arguments[0]).type.get();

        if (check_and_get_data_type<DataTypeString>(from_type)) {
            return StringParsing<ToDataType>::execute(context, block, arguments, result,
                                                      input_rows_count);
        }

        return Status::RuntimeError(
                "Illegal type {} of argument of function {} . Only String or FixedString "
                "argument is accepted for try-conversion function. For other arguments, use "
                "function without 'orZero' or 'orNull'.",
                block.get_by_position(arguments[0]).type->get_name(), get_name());
    }
};

template <typename ToDataType, typename Name>
class FunctionConvertToTimeType : public IFunction {
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create() { return std::make_shared<FunctionConvertToTimeType>(); }

    String get_name() const override { return name; }

    bool is_variadic() const override { return true; }
    size_t get_number_of_arguments() const override { return 0; }

    ColumnNumbers get_arguments_that_are_always_constant() const override { return {1}; }

    // This function should not be called for get DateType Ptr
    // using the FunctionCast::get_return_type_impl
    DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) const override {
        return std::make_shared<ToDataType>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        Status ret_status = Status::OK();
        const IDataType* from_type = block.get_by_position(arguments[0]).type.get();
        auto call = [&](const auto& types) -> bool {
            using Types = std::decay_t<decltype(types)>;
            using LeftDataType = typename Types::LeftType;
            using RightDataType = typename Types::RightType;

            ret_status = ConvertImplToTimeType<LeftDataType, RightDataType, Name>::execute(
                    context, block, arguments, result, input_rows_count);
            return true;
        };

        bool done = call_on_index_and_number_data_type<ToDataType>(from_type->get_primitive_type(),
                                                                   call);
        if (!done) {
            return Status::RuntimeError("Illegal type {} of argument of function {}",
                                        block.get_by_position(arguments[0]).type->get_name(),
                                        get_name());
        }

        return ret_status;
    }
};

template <typename ToDataType>
class FunctionConvert : public CastToBase {
public:
    static FunctionPtr create() { return std::make_shared<FunctionConvert>(); }
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        if (!arguments.size()) {
            return Status::RuntimeError("Function cast expects at least 1 arguments");
        }

        const IDataType* from_type = block.get_by_position(arguments[0]).type.get();

        Status ret_status;
        /// Generic conversion of any type to String.

        auto call = [&](const auto& types) -> bool {
            using Types = std::decay_t<decltype(types)>;
            using LeftDataType = typename Types::LeftType;
            using RightDataType = typename Types::RightType;

            // now, cast to decimal do not execute the code
            if constexpr (IsDataTypeDecimal<RightDataType>) {
                if (arguments.size() != 2) {
                    ret_status =
                            Status::RuntimeError("Function cast expects 2 arguments for Decimal.");
                    return true;
                }

                const ColumnWithTypeAndName& scale_column = block.get_by_position(result);
                ret_status = ConvertImpl<LeftDataType, RightDataType>::execute(
                        context, block, arguments, result, input_rows_count,
                        scale_column.type->get_scale());
            } else if constexpr (IsDataTypeDateTimeV2<RightDataType>) {
                const ColumnWithTypeAndName& scale_column = block.get_by_position(result);
                ret_status = ConvertImpl<LeftDataType, RightDataType>::execute(
                        context, block, arguments, result, input_rows_count,
                        scale_column.type->get_scale());
            } else {
                ret_status = ConvertImpl<LeftDataType, RightDataType>::execute(
                        context, block, arguments, result, input_rows_count);
            }
            return true;
        };

        bool done = call_on_index_and_data_type<ToDataType>(from_type->get_primitive_type(), call);
        if (!done) {
            ret_status = Status::RuntimeError("Illegal type {} of argument of function cast",
                                              block.get_by_position(arguments[0]).type->get_name());
        }
        return ret_status;
    }
};

using FunctionToUInt8 = FunctionConvert<DataTypeUInt8>;
using FunctionToUInt16 = FunctionConvert<DataTypeUInt16>;
using FunctionToUInt32 = FunctionConvert<DataTypeUInt32>;
using FunctionToUInt64 = FunctionConvert<DataTypeUInt64>;
using FunctionToInt8 = FunctionConvert<DataTypeInt8>;
using FunctionToInt16 = FunctionConvert<DataTypeInt16>;
using FunctionToInt32 = FunctionConvert<DataTypeInt32>;
using FunctionToInt64 = FunctionConvert<DataTypeInt64>;
using FunctionToInt128 = FunctionConvert<DataTypeInt128>;
using FunctionToFloat32 = FunctionConvert<DataTypeFloat32>;
using FunctionToFloat64 = FunctionConvert<DataTypeFloat64>;

using FunctionToTimeV2 = FunctionConvert<DataTypeTimeV2>;
using FunctionToDecimal32 = FunctionConvert<DataTypeDecimal<Decimal32>>;
using FunctionToDecimal64 = FunctionConvert<DataTypeDecimal<Decimal64>>;
using FunctionToDecimal128 = FunctionConvert<DataTypeDecimal<Decimal128V2>>;
using FunctionToDecimal128V3 = FunctionConvert<DataTypeDecimal<Decimal128V3>>;
using FunctionToDecimal256 = FunctionConvert<DataTypeDecimal<Decimal256>>;
using FunctionToIPv4 = FunctionConvert<DataTypeIPv4>;
using FunctionToIPv6 = FunctionConvert<DataTypeIPv6>;
using FunctionToDate = FunctionConvert<DataTypeDate>;
using FunctionToDateTime = FunctionConvert<DataTypeDateTime>;
using FunctionToDateV2 = FunctionConvert<DataTypeDateV2>;
using FunctionToDateTimeV2 = FunctionConvert<DataTypeDateTimeV2>;

template <typename DataType>
struct FunctionTo;

template <>
struct FunctionTo<DataTypeUInt8> {
    using Type = FunctionToUInt8;
};
template <>
struct FunctionTo<DataTypeUInt16> {
    using Type = FunctionToUInt16;
};
template <>
struct FunctionTo<DataTypeUInt32> {
    using Type = FunctionToUInt32;
};
template <>
struct FunctionTo<DataTypeUInt64> {
    using Type = FunctionToUInt64;
};
template <>
struct FunctionTo<DataTypeInt8> {
    using Type = FunctionToInt8;
};
template <>
struct FunctionTo<DataTypeInt16> {
    using Type = FunctionToInt16;
};
template <>
struct FunctionTo<DataTypeInt32> {
    using Type = FunctionToInt32;
};
template <>
struct FunctionTo<DataTypeInt64> {
    using Type = FunctionToInt64;
};
template <>
struct FunctionTo<DataTypeInt128> {
    using Type = FunctionToInt128;
};
template <>
struct FunctionTo<DataTypeFloat32> {
    using Type = FunctionToFloat32;
};
template <>
struct FunctionTo<DataTypeFloat64> {
    using Type = FunctionToFloat64;
};
template <>
struct FunctionTo<DataTypeDecimal<Decimal32>> {
    using Type = FunctionToDecimal32;
};
template <>
struct FunctionTo<DataTypeDecimal<Decimal64>> {
    using Type = FunctionToDecimal64;
};
template <>
struct FunctionTo<DataTypeDecimal<Decimal128V2>> {
    using Type = FunctionToDecimal128;
};
template <>
struct FunctionTo<DataTypeDecimal<Decimal128V3>> {
    using Type = FunctionToDecimal128V3;
};
template <>
struct FunctionTo<DataTypeDecimal<Decimal256>> {
    using Type = FunctionToDecimal256;
};
template <>
struct FunctionTo<DataTypeIPv4> {
    using Type = FunctionToIPv4;
};
template <>
struct FunctionTo<DataTypeIPv6> {
    using Type = FunctionToIPv6;
};
template <>
struct FunctionTo<DataTypeDate> {
    using Type = FunctionToDate;
};
template <>
struct FunctionTo<DataTypeDateTime> {
    using Type = FunctionToDateTime;
};
template <>
struct FunctionTo<DataTypeDateV2> {
    using Type = FunctionToDateV2;
};
template <>
struct FunctionTo<DataTypeDateTimeV2> {
    using Type = FunctionToDateTimeV2;
};
template <>
struct FunctionTo<DataTypeTimeV2> {
    using Type = FunctionToTimeV2;
};
} // namespace doris::vectorized
