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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/FunctionsConversion.h
// and modified by Doris

#pragma once

#include <cctz/time_zone.h>
#include <fmt/format.h>
#include <gen_cpp/FrontendService_types.h>
#include <glog/logging.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

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
#include "vec/functions/function_convert_tz.h"
#include "vec/functions/function_helpers.h"
#include "vec/io/io_helper.h"
#include "vec/io/reader_buffer.h"
#include "vec/runtime/time_value.h"
#include "vec/runtime/vdatetime_value.h"
#include "vec/utils/util.hpp"

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
template <typename FromDataType, typename ToDataType, typename Name>
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
                return Status::RuntimeError("Illegal column {} of first argument of function {}",
                                            named_from.column->get_name(), Name::name);
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
            return Status::RuntimeError("Illegal column {} of first argument of function {}",
                                        named_from.column->get_name(), Name::name);
        }
        return Status::OK();
    }
};

/** If types are identical, just take reference to column.
  */
template <typename T, typename Name>
    requires(!T::is_parametric)
struct ConvertImpl<T, T, Name> {
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

// Generic conversion of any type to String.
struct ConvertImplGenericToString {
    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        const auto& col_with_type_and_name = block.get_by_position(arguments[0]);
        const IDataType& type = *col_with_type_and_name.type;
        const IColumn& col_from = *col_with_type_and_name.column;

        auto col_to = ColumnString::create();
        type.to_string_batch(col_from, *col_to);

        block.replace_by_position(result, std::move(col_to));
        return Status::OK();
    }

    static Status execute2(FunctionContext* /*ctx*/, Block& block, const ColumnNumbers& arguments,
                           const uint32_t result, size_t /*input_rows_count*/) {
        return execute(block, arguments, result);
    }
};
//this is for data in compound type
struct ConvertImplGenericFromString {
    static Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          const uint32_t result, size_t input_rows_count) {
        const auto& col_with_type_and_name = block.get_by_position(arguments[0]);
        const IColumn& col_from = *col_with_type_and_name.column;
        // result column must set type
        DCHECK(block.get_by_position(result).type != nullptr);
        auto data_type_to = block.get_by_position(result).type;
        if (const ColumnString* col_from_string = check_and_get_column<ColumnString>(&col_from)) {
            auto col_to = data_type_to->create_column();
            auto serde = data_type_to->get_serde();
            size_t size = col_from.size();
            col_to->reserve(size);

            ColumnUInt8::MutablePtr col_null_map_to = ColumnUInt8::create(size, 0);
            ColumnUInt8::Container* vec_null_map_to = &col_null_map_to->get_data();
            const bool is_complex = is_complex_type(data_type_to->get_primitive_type());
            DataTypeSerDe::FormatOptions format_options;
            format_options.converted_from_string = true;
            format_options.escape_char = '\\';

            for (size_t i = 0; i < size; ++i) {
                const auto& val = col_from_string->get_data_at(i);
                // Note: here we should handle the null element
                if (val.size == 0) {
                    col_to->insert_default();
                    // empty string('') is an invalid format for complex type, set null_map to 1
                    if (is_complex) {
                        (*vec_null_map_to)[i] = 1;
                    }
                    continue;
                }
                Slice string_slice(val.data, val.size);
                Status st = serde->deserialize_one_cell_from_json(*col_to, string_slice,
                                                                  format_options);
                // if parsing failed, will return null
                (*vec_null_map_to)[i] = !st.ok();
                if (!st.ok()) {
                    col_to->insert_default();
                }
            }
            block.get_by_position(result).column =
                    ColumnNullable::create(std::move(col_to), std::move(col_null_map_to));
        } else {
            return Status::RuntimeError(
                    "Illegal column {} of first argument of conversion function from string",
                    col_from.get_name());
        }
        return Status::OK();
    }
};

// Generic conversion of number to jsonb.
template <typename ColumnType>
struct ConvertImplNumberToJsonb {
    static Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          const uint32_t result, size_t input_rows_count) {
        const auto& col_with_type_and_name = block.get_by_position(arguments[0]);

        auto column_string = ColumnString::create();
        JsonbWriter writer;

        const auto* col =
                check_and_get_column<const ColumnType>(col_with_type_and_name.column.get());
        const auto& data = col->get_data();

        for (size_t i = 0; i < input_rows_count; i++) {
            writer.reset();
            if constexpr (std::is_same_v<ColumnUInt8, ColumnType>) {
                writer.writeBool(data[i]);
            } else if constexpr (std::is_same_v<ColumnInt8, ColumnType>) {
                writer.writeInt8(data[i]);
            } else if constexpr (std::is_same_v<ColumnInt16, ColumnType>) {
                writer.writeInt16(data[i]);
            } else if constexpr (std::is_same_v<ColumnInt32, ColumnType>) {
                writer.writeInt32(data[i]);
            } else if constexpr (std::is_same_v<ColumnInt64, ColumnType>) {
                writer.writeInt64(data[i]);
            } else if constexpr (std::is_same_v<ColumnInt128, ColumnType>) {
                writer.writeInt128(data[i]);
            } else if constexpr (std::is_same_v<ColumnFloat64, ColumnType>) {
                writer.writeDouble(data[i]);
            } else {
                static_assert(std::is_same_v<ColumnType, ColumnUInt8> ||
                                      std::is_same_v<ColumnType, ColumnInt8> ||
                                      std::is_same_v<ColumnType, ColumnInt16> ||
                                      std::is_same_v<ColumnType, ColumnInt32> ||
                                      std::is_same_v<ColumnType, ColumnInt64> ||
                                      std::is_same_v<ColumnType, ColumnInt128> ||
                                      std::is_same_v<ColumnType, ColumnFloat64>,
                              "unsupported type");
                __builtin_unreachable();
            }
            column_string->insert_data(writer.getOutput()->getBuffer(),
                                       writer.getOutput()->getSize());
        }

        block.replace_by_position(result, std::move(column_string));
        return Status::OK();
    }
};

struct ConvertImplStringToJsonbAsJsonbString {
    static Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          const uint32_t result, size_t input_rows_count) {
        auto data_type_to = block.get_by_position(result).type;
        const auto& col_with_type_and_name = block.get_by_position(arguments[0]);
        const IColumn& col_from = *col_with_type_and_name.column;
        auto dst = ColumnString::create();
        ColumnString* dst_str = assert_cast<ColumnString*>(dst.get());
        const auto* from_string = assert_cast<const ColumnString*>(&col_from);
        JsonbWriter writer;
        for (size_t i = 0; i < input_rows_count; i++) {
            auto str_ref = from_string->get_data_at(i);
            writer.reset();
            // write raw string to jsonb
            writer.writeStartString();
            writer.writeString(str_ref.data, str_ref.size);
            writer.writeEndString();
            dst_str->insert_data(writer.getOutput()->getBuffer(), writer.getOutput()->getSize());
        }
        block.replace_by_position(result, std::move(dst));
        return Status::OK();
    }
};

struct ConvertImplGenericFromJsonb {
    static Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          const uint32_t result, size_t input_rows_count) {
        auto data_type_to = block.get_by_position(result).type;
        const auto& col_with_type_and_name = block.get_by_position(arguments[0]);
        const IColumn& col_from = *col_with_type_and_name.column;
        if (const ColumnString* col_from_string = check_and_get_column<ColumnString>(&col_from)) {
            auto col_to = data_type_to->create_column();

            size_t size = col_from.size();
            col_to->reserve(size);

            ColumnUInt8::MutablePtr col_null_map_to = ColumnUInt8::create(size, 0);
            ColumnUInt8::Container* vec_null_map_to = &col_null_map_to->get_data();
            const bool is_complex = is_complex_type(data_type_to->get_primitive_type());
            const bool is_dst_string = is_string_type(data_type_to->get_primitive_type());
            for (size_t i = 0; i < size; ++i) {
                const auto& val = col_from_string->get_data_at(i);
                JsonbDocument* doc = JsonbDocument::checkAndCreateDocument(val.data, val.size);
                if (UNLIKELY(!doc || !doc->getValue())) {
                    (*vec_null_map_to)[i] = 1;
                    col_to->insert_default();
                    continue;
                }

                // value is NOT necessary to be deleted since JsonbValue will not allocate memory
                JsonbValue* value = doc->getValue();
                if (UNLIKELY(!value)) {
                    (*vec_null_map_to)[i] = 1;
                    col_to->insert_default();
                    continue;
                }
                // Note: here we should handle the null element
                if (val.size == 0) {
                    col_to->insert_default();
                    // empty string('') is an invalid format for complex type, set null_map to 1
                    if (is_complex) {
                        (*vec_null_map_to)[i] = 1;
                    }
                    continue;
                }
                // add string to string column
                if (context->jsonb_string_as_string() && is_dst_string && value->isString()) {
                    const auto* blob = static_cast<const JsonbBlobVal*>(value);
                    assert_cast<ColumnString&, TypeCheckOnRelease::DISABLE>(*col_to).insert_data(
                            blob->getBlob(), blob->getBlobLen());
                    (*vec_null_map_to)[i] = 0;
                    continue;
                }
                std::string input_str;
                if (context->jsonb_string_as_string() && value->isString()) {
                    const auto* blob = static_cast<const JsonbBlobVal*>(value);
                    input_str = std::string(blob->getBlob(), blob->getBlobLen());
                } else {
                    input_str = JsonbToJson::jsonb_to_json_string(val.data, val.size);
                }
                if (input_str.empty()) {
                    col_to->insert_default();
                    (*vec_null_map_to)[i] = 1;
                    continue;
                }
                ReadBuffer read_buffer((char*)(input_str.data()), input_str.size());
                Status st = data_type_to->from_string(read_buffer, col_to.get());
                // if parsing failed, will return null
                (*vec_null_map_to)[i] = !st.ok();
                if (!st.ok()) {
                    col_to->insert_default();
                }
            }
            block.get_by_position(result).column =
                    ColumnNullable::create(std::move(col_to), std::move(col_null_map_to));
        } else {
            return Status::RuntimeError(
                    "Illegal column {} of first argument of conversion function from string",
                    col_from.get_name());
        }
        return Status::OK();
    }
};

// Generic conversion of any type to jsonb.
struct ConvertImplGenericToJsonb {
    static Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          const uint32_t result, size_t input_rows_count) {
        auto data_type_to = block.get_by_position(result).type;
        const auto& col_with_type_and_name = block.get_by_position(arguments[0]);
        const IDataType& type = *col_with_type_and_name.type;
        const IColumn& col_from = *col_with_type_and_name.column;

        auto column_string = ColumnString::create();
        JsonbWriter writer;

        ColumnUInt8::MutablePtr col_null_map_to = ColumnUInt8::create(col_from.size(), 0);
        ColumnUInt8::Container* vec_null_map_to = &col_null_map_to->get_data();
        DataTypeSerDe::FormatOptions format_options;
        format_options.converted_from_string = true;
        DataTypeSerDeSPtr from_serde = type.get_serde();
        DataTypeSerDeSPtr to_serde = data_type_to->get_serde();
        auto col_to = data_type_to->create_column();

        auto tmp_col = ColumnString::create();
        vectorized::DataTypeSerDe::FormatOptions options;
        options.escape_char = '\\';
        for (size_t i = 0; i < input_rows_count; i++) {
            // convert to string
            tmp_col->clear();
            VectorBufferWriter write_buffer(*tmp_col.get());
            Status st =
                    from_serde->serialize_column_to_json(col_from, i, i + 1, write_buffer, options);
            // if serialized failed, will return null
            (*vec_null_map_to)[i] = !st.ok();
            if (!st.ok()) {
                col_to->insert_default();
                continue;
            }
            write_buffer.commit();
            writer.reset();
            auto str_ref = tmp_col->get_data_at(0);
            Slice data((char*)(str_ref.data), str_ref.size);
            // first try to parse string
            st = to_serde->deserialize_one_cell_from_json(*col_to, data, format_options);
            // if parsing failed, will return null
            (*vec_null_map_to)[i] = !st.ok();
            if (!st.ok()) {
                col_to->insert_default();
            }
        }

        block.replace_by_position(
                result, ColumnNullable::create(std::move(col_to), std::move(col_null_map_to)));
        return Status::OK();
    }
};

struct ConvertNothingToJsonb {
    static Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          const uint32_t result, size_t input_rows_count) {
        const auto& col_with_type_and_name = block.get_by_position(arguments[0]);
        const IColumn& col_from = *col_with_type_and_name.column;
        auto data_type_to = block.get_by_position(result).type;
        size_t size = col_from.size();
        auto col_to = data_type_to->create_column_const_with_default_value(size);
        ColumnUInt8::MutablePtr col_null_map_to = ColumnUInt8::create(size, 1);
        block.replace_by_position(result, ColumnNullable::create(col_to->assume_mutable(),
                                                                 std::move(col_null_map_to)));
        return Status::OK();
    }
};

template <PrimitiveType type, typename ColumnType>
struct ConvertImplFromJsonb {
    static Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          const uint32_t result, size_t input_rows_count) {
        const auto& col_with_type_and_name = block.get_by_position(arguments[0]);
        const IColumn& col_from = *col_with_type_and_name.column;
        // result column must set type
        DCHECK(block.get_by_position(result).type != nullptr);
        auto data_type_to = block.get_by_position(result).type;
        if (const ColumnString* column_string = check_and_get_column<ColumnString>(&col_from)) {
            auto null_map_col = ColumnUInt8::create(input_rows_count, 0);
            auto& null_map = null_map_col->get_data();
            auto col_to = ColumnType::create();

            //IColumn & col_to = *res;
            // size_t size = col_from.size();
            col_to->reserve(input_rows_count);
            auto& res = col_to->get_data();
            res.resize(input_rows_count);

            for (size_t i = 0; i < input_rows_count; ++i) {
                const auto& val = column_string->get_data_at(i);
                // ReadBuffer read_buffer((char*)(val.data), val.size);
                // RETURN_IF_ERROR(data_type_to->from_string(read_buffer, col_to));

                if (val.size == 0) {
                    null_map[i] = 1;
                    res[i] = 0;
                    continue;
                }

                // doc is NOT necessary to be deleted since JsonbDocument will not allocate memory
                JsonbDocument* doc = JsonbDocument::checkAndCreateDocument(val.data, val.size);
                if (UNLIKELY(!doc || !doc->getValue())) {
                    null_map[i] = 1;
                    res[i] = 0;
                    continue;
                }

                // value is NOT necessary to be deleted since JsonbValue will not allocate memory
                JsonbValue* value = doc->getValue();
                if (UNLIKELY(!value)) {
                    null_map[i] = 1;
                    res[i] = 0;
                    continue;
                }
                if constexpr (type == PrimitiveType::TYPE_BOOLEAN) {
                    // cast from json value to boolean type
                    if (value->isTrue()) {
                        res[i] = 1;
                    } else if (value->isFalse()) {
                        res[i] = 0;
                    } else if (value->isInt()) {
                        res[i] = ((const JsonbIntVal*)value)->val() == 0 ? 0 : 1;
                    } else if (value->isDouble()) {
                        res[i] = static_cast<ColumnType::value_type>(
                                         ((const JsonbDoubleVal*)value)->val()) == 0
                                         ? 0
                                         : 1;
                    } else {
                        null_map[i] = 1;
                        res[i] = 0;
                    }
                } else if constexpr (type == PrimitiveType::TYPE_TINYINT ||
                                     type == PrimitiveType::TYPE_SMALLINT ||
                                     type == PrimitiveType::TYPE_INT ||
                                     type == PrimitiveType::TYPE_BIGINT ||
                                     type == PrimitiveType::TYPE_LARGEINT) {
                    // cast from json value to integer types
                    if (value->isInt()) {
                        res[i] = ((const JsonbIntVal*)value)->val();
                    } else if (value->isDouble()) {
                        res[i] = static_cast<ColumnType::value_type>(
                                ((const JsonbDoubleVal*)value)->val());
                    } else if (value->isTrue()) {
                        res[i] = 1;
                    } else if (value->isFalse()) {
                        res[i] = 0;
                    } else {
                        null_map[i] = 1;
                        res[i] = 0;
                    }
                } else if constexpr (type == PrimitiveType::TYPE_FLOAT ||
                                     type == PrimitiveType::TYPE_DOUBLE) {
                    // cast from json value to floating point types
                    if (value->isDouble()) {
                        res[i] = ((const JsonbDoubleVal*)value)->val();
                    } else if (value->isFloat()) {
                        res[i] = ((const JsonbFloatVal*)value)->val();
                    } else if (value->isTrue()) {
                        res[i] = 1;
                    } else if (value->isFalse()) {
                        res[i] = 0;
                    } else if (value->isInt()) {
                        res[i] = ((const JsonbIntVal*)value)->val();
                    } else {
                        null_map[i] = 1;
                        res[i] = 0;
                    }
                } else {
                    throw Exception(Status::FatalError("unsupported type"));
                }
            }

            block.replace_by_position(
                    result, ColumnNullable::create(std::move(col_to), std::move(null_map_col)));
        } else {
            return Status::RuntimeError(
                    "Illegal column {} of first argument of conversion function from string",
                    col_from.get_name());
        }
        return Status::OK();
    }
};

template <typename ToDataType, typename Name>
struct ConvertImpl<DataTypeString, ToDataType, Name> {
    template <typename Additions = void*>
    static Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          uint32_t result, size_t input_rows_count,
                          Additions additions [[maybe_unused]] = Additions()) {
        return Status::RuntimeError("not support convert from string");
    }
};

struct NameToString {
    static constexpr auto name = "to_string";
};
struct NameToDecimal32 {
    static constexpr auto name = "toDecimal32";
};
struct NameToDecimal64 {
    static constexpr auto name = "toDecimal64";
};
struct NameToDecimal128 {
    static constexpr auto name = "toDecimal128";
};
struct NameToDecimal128V3 {
    static constexpr auto name = "toDecimal128V3";
};
struct NameToDecimal256 {
    static constexpr auto name = "toDecimal256";
};
struct NameToUInt8 {
    static constexpr auto name = "toUInt8";
};
struct NameToUInt16 {
    static constexpr auto name = "toUInt16";
};
struct NameToUInt32 {
    static constexpr auto name = "toUInt32";
};
struct NameToUInt64 {
    static constexpr auto name = "toUInt64";
};
struct NameToInt8 {
    static constexpr auto name = "toInt8";
};
struct NameToInt16 {
    static constexpr auto name = "toInt16";
};
struct NameToInt32 {
    static constexpr auto name = "toInt32";
};
struct NameToInt64 {
    static constexpr auto name = "toInt64";
};
struct NameToInt128 {
    static constexpr auto name = "toInt128";
};
struct NameToFloat32 {
    static constexpr auto name = "toFloat32";
};
struct NameToFloat64 {
    static constexpr auto name = "toFloat64";
};
struct NameToIPv4 {
    static constexpr auto name = "toIPv4";
};
struct NameToIPv6 {
    static constexpr auto name = "toIPv6";
};
struct NameToDate {
    static constexpr auto name = "toDate";
};
struct NameToDateTime {
    static constexpr auto name = "toDateTime";
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

template <typename ToDataType, typename Name>
class FunctionConvert : public IFunction {
public:
    static constexpr auto name = Name::name;

    static FunctionPtr create() { return std::make_shared<FunctionConvert>(); }

    String get_name() const override { return name; }

    bool is_variadic() const override { return true; }
    size_t get_number_of_arguments() const override { return 0; }

    // This function should not be called for get DateType Ptr
    // using the FunctionCast::get_return_type_impl
    DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) const override {
        return std::make_shared<ToDataType>();
    }

    ColumnNumbers get_arguments_that_are_always_constant() const override { return {1}; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        if (!arguments.size()) {
            return Status::RuntimeError("Function {} expects at least 1 arguments", get_name());
        }

        const IDataType* from_type = block.get_by_position(arguments[0]).type.get();

        Status ret_status;
        /// Generic conversion of any type to String.
        if constexpr (std::is_same_v<ToDataType, DataTypeString>) {
            return ConvertImplGenericToString::execute(block, arguments, result);
        } else {
            auto call = [&](const auto& types) -> bool {
                using Types = std::decay_t<decltype(types)>;
                using LeftDataType = typename Types::LeftType;
                using RightDataType = typename Types::RightType;

                // now, cast to decimal do not execute the code
                if constexpr (IsDataTypeDecimal<RightDataType>) {
                    if (arguments.size() != 2) {
                        ret_status = Status::RuntimeError(
                                "Function {} expects 2 arguments for Decimal.", get_name());
                        return true;
                    }

                    const ColumnWithTypeAndName& scale_column = block.get_by_position(result);
                    ret_status = ConvertImpl<LeftDataType, RightDataType, Name>::execute(
                            context, block, arguments, result, input_rows_count,
                            scale_column.type->get_scale());
                } else if constexpr (IsDataTypeDateTimeV2<RightDataType>) {
                    const ColumnWithTypeAndName& scale_column = block.get_by_position(result);
                    ret_status = ConvertImpl<LeftDataType, RightDataType, Name>::execute(
                            context, block, arguments, result, input_rows_count,
                            scale_column.type->get_scale());
                } else {
                    ret_status = ConvertImpl<LeftDataType, RightDataType, Name>::execute(
                            context, block, arguments, result, input_rows_count);
                }
                return true;
            };

            bool done =
                    call_on_index_and_data_type<ToDataType>(from_type->get_primitive_type(), call);
            if (!done) {
                ret_status = Status::RuntimeError(
                        "Illegal type {} of argument of function {}",
                        block.get_by_position(arguments[0]).type->get_name(), get_name());
            }
            return ret_status;
        }
    }
};

using FunctionToUInt8 = FunctionConvert<DataTypeUInt8, NameToUInt8>;
using FunctionToUInt16 = FunctionConvert<DataTypeUInt16, NameToUInt16>;
using FunctionToUInt32 = FunctionConvert<DataTypeUInt32, NameToUInt32>;
using FunctionToUInt64 = FunctionConvert<DataTypeUInt64, NameToUInt64>;
using FunctionToInt8 = FunctionConvert<DataTypeInt8, NameToInt8>;
using FunctionToInt16 = FunctionConvert<DataTypeInt16, NameToInt16>;
using FunctionToInt32 = FunctionConvert<DataTypeInt32, NameToInt32>;
using FunctionToInt64 = FunctionConvert<DataTypeInt64, NameToInt64>;
using FunctionToInt128 = FunctionConvert<DataTypeInt128, NameToInt128>;
using FunctionToFloat32 = FunctionConvert<DataTypeFloat32, NameToFloat32>;
using FunctionToFloat64 = FunctionConvert<DataTypeFloat64, NameToFloat64>;

using FunctionToTimeV2 = FunctionConvert<DataTypeTimeV2, NameToFloat64>;
using FunctionToString = FunctionConvert<DataTypeString, NameToString>;
using FunctionToDecimal32 = FunctionConvert<DataTypeDecimal<Decimal32>, NameToDecimal32>;
using FunctionToDecimal64 = FunctionConvert<DataTypeDecimal<Decimal64>, NameToDecimal64>;
using FunctionToDecimal128 = FunctionConvert<DataTypeDecimal<Decimal128V2>, NameToDecimal128>;
using FunctionToDecimal128V3 = FunctionConvert<DataTypeDecimal<Decimal128V3>, NameToDecimal128V3>;
using FunctionToDecimal256 = FunctionConvert<DataTypeDecimal<Decimal256>, NameToDecimal256>;
using FunctionToIPv4 = FunctionConvert<DataTypeIPv4, NameToIPv4>;
using FunctionToIPv6 = FunctionConvert<DataTypeIPv6, NameToIPv6>;
using FunctionToDate = FunctionConvert<DataTypeDate, NameToDate>;
using FunctionToDateTime = FunctionConvert<DataTypeDateTime, NameToDateTime>;
using FunctionToDateV2 = FunctionConvert<DataTypeDateV2, NameToDate>;
using FunctionToDateTimeV2 = FunctionConvert<DataTypeDateTimeV2, NameToDateTime>;

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
class PreparedFunctionCast : public PreparedFunctionImpl {
public:
    using WrapperType = std::function<Status(FunctionContext* context, Block&, const ColumnNumbers&,
                                             size_t, size_t)>;

    explicit PreparedFunctionCast(WrapperType&& wrapper_function_, const char* name_)
            : wrapper_function(std::move(wrapper_function_)), name(name_) {}

    String get_name() const override { return name; }

protected:
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        return wrapper_function(context, block, arguments, result, input_rows_count);
    }

    bool use_default_implementation_for_nulls() const override { return false; }
    bool use_default_implementation_for_low_cardinality_columns() const override { return false; }
    ColumnNumbers get_arguments_that_are_always_constant() const override { return {1}; }

private:
    WrapperType wrapper_function;
    const char* name;
};

// always from DataTypeString
template <typename ToDataType, typename Name>
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
            return Status::RuntimeError("Illegal column {} of first argument of function {}",
                                        col_from->get_name(), Name::name);
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

template <typename Name>
struct ConvertImpl<DataTypeString, DataTypeDecimal<Decimal32>, Name>
        : StringParsing<DataTypeDecimal<Decimal32>, Name> {};
template <typename Name>
struct ConvertImpl<DataTypeString, DataTypeDecimal<Decimal64>, Name>
        : StringParsing<DataTypeDecimal<Decimal64>, Name> {};
template <typename Name>
struct ConvertImpl<DataTypeString, DataTypeDecimal<Decimal128V2>, Name>
        : StringParsing<DataTypeDecimal<Decimal128V2>, Name> {};
template <typename Name>
struct ConvertImpl<DataTypeString, DataTypeDecimal<Decimal128V3>, Name>
        : StringParsing<DataTypeDecimal<Decimal128V3>, Name> {};
template <typename Name>
struct ConvertImpl<DataTypeString, DataTypeDecimal<Decimal256>, Name>
        : StringParsing<DataTypeDecimal<Decimal256>, Name> {};
template <typename Name>
struct ConvertImpl<DataTypeString, DataTypeIPv4, Name> : StringParsing<DataTypeIPv4, Name> {};
template <typename Name>
struct ConvertImpl<DataTypeString, DataTypeIPv6, Name> : StringParsing<DataTypeIPv6, Name> {};

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
            return StringParsing<ToDataType, Name>::execute(context, block, arguments, result,
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

class FunctionCast final : public IFunctionBase {
public:
    using WrapperType =
            std::function<Status(FunctionContext*, Block&, const ColumnNumbers&, size_t, size_t)>;
    using ElementWrappers = std::vector<WrapperType>;

    FunctionCast(const char* name_, const DataTypes& argument_types_,
                 const DataTypePtr& return_type_)
            : name(name_), argument_types(argument_types_), return_type(return_type_) {}

    const DataTypes& get_argument_types() const override { return argument_types; }
    const DataTypePtr& get_return_type() const override { return return_type; }

    PreparedFunctionPtr prepare(FunctionContext* context, const Block& /*sample_block*/,
                                const ColumnNumbers& /*arguments*/,
                                uint32_t /*result*/) const override {
        return std::make_shared<PreparedFunctionCast>(
                prepare_unpack_dictionaries(context, get_argument_types()[0], get_return_type()),
                name);
    }

    String get_name() const override { return name; }

    bool is_use_default_implementation_for_constants() const override { return true; }

private:
    const char* name = nullptr;

    DataTypes argument_types;
    DataTypePtr return_type;

    template <typename DataType>
    WrapperType create_wrapper(const DataTypePtr& from_type, const DataType* const,
                               bool requested_result_is_nullable) const {
        FunctionPtr function;

        if (requested_result_is_nullable &&
            check_and_get_data_type<DataTypeString>(from_type.get())) {
            /// In case when converting to Nullable type, we apply different parsing rule,
            /// that will not throw an exception but return NULL in case of malformed input.
            function = FunctionConvertFromString<DataType, NameCast>::create();
        } else if (requested_result_is_nullable &&
                   (IsDatelikeV1Types<DataType> || IsDatelikeV2Types<DataType>)&&!(
                           check_and_get_data_type<DataTypeDateTime>(from_type.get()) ||
                           check_and_get_data_type<DataTypeDate>(from_type.get()) ||
                           check_and_get_data_type<DataTypeDateV2>(from_type.get()) ||
                           check_and_get_data_type<DataTypeDateTimeV2>(from_type.get()))) {
            function = FunctionConvertToTimeType<DataType, NameCast>::create();
        } else {
            function = FunctionTo<DataType>::Type::create();
        }

        /// Check conversion using underlying function
        { function->get_return_type(ColumnsWithTypeAndName(1, {nullptr, from_type, ""})); }

        return [function](FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          const uint32_t result, size_t input_rows_count) {
            return function->execute(context, block, arguments, result, input_rows_count);
        };
    }

    WrapperType create_string_wrapper(const DataTypePtr& from_type) const {
        FunctionPtr function = FunctionToString::create();

        /// Check conversion using underlying function
        { function->get_return_type(ColumnsWithTypeAndName(1, {nullptr, from_type, ""})); }

        return [function](FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          const uint32_t result, size_t input_rows_count) {
            return function->execute(context, block, arguments, result, input_rows_count);
        };
    }

    template <typename FieldType>
    WrapperType create_decimal_wrapper(const DataTypePtr& from_type,
                                       const DataTypeDecimal<FieldType>* to_type) const {
        using ToDataType = DataTypeDecimal<FieldType>;

        auto type = from_type->get_primitive_type();
        UInt32 precision = to_type->get_precision();
        UInt32 scale = to_type->get_scale();

        bool ok = is_int_or_bool(type) || is_decimal(type) || is_float_or_double(type) ||
                  is_date_type(type) || is_string_type(type);
        if (!ok) {
            return create_unsupport_wrapper(from_type->get_name(), to_type->get_name());
        }

        return [type, precision, scale](FunctionContext* context, Block& block,
                                        const ColumnNumbers& arguments, const uint32_t result,
                                        size_t input_rows_count) {
            auto res =
                    call_on_index_and_data_type<ToDataType>(type, [&](const auto& types) -> bool {
                        using Types = std::decay_t<decltype(types)>;
                        using LeftDataType = typename Types::LeftType;
                        using RightDataType = typename Types::RightType;

                        auto state = ConvertImpl<LeftDataType, RightDataType, NameCast>::execute(
                                context, block, arguments, result, input_rows_count,
                                PrecisionScaleArg {precision, scale});
                        if (!state) {
                            throw Exception(state.code(), state.to_string());
                        }
                        return true;
                    });

            /// Additionally check if call_on_index_and_data_type wasn't called at all.
            if (!res) {
                auto to = DataTypeDecimal<FieldType>(precision, scale);
                return Status::RuntimeError("Conversion from {} to {} is not supported",
                                            type_to_string(type), to.get_name());
            }
            return Status::OK();
        };
    }

    WrapperType create_identity_wrapper(const DataTypePtr&) const {
        return [](FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                  const uint32_t result, size_t /*input_rows_count*/) {
            block.get_by_position(result).column = block.get_by_position(arguments.front()).column;
            return Status::OK();
        };
    }

    WrapperType create_nothing_wrapper(const IDataType* to_type) const {
        ColumnPtr res = to_type->create_column_const_with_default_value(1);
        return [res](FunctionContext* context, Block& block, const ColumnNumbers&,
                     const uint32_t result, size_t input_rows_count) {
            /// Column of Nothing type is trivially convertible to any other column
            block.get_by_position(result).column =
                    res->clone_resized(input_rows_count)->convert_to_full_column_if_const();
            return Status::OK();
        };
    }

    WrapperType create_unsupport_wrapper(const String error_msg) const {
        return [error_msg](FunctionContext* /*context*/, Block& /*block*/,
                           const ColumnNumbers& /*arguments*/, const size_t /*result*/,
                           size_t /*input_rows_count*/) {
            return Status::InvalidArgument(error_msg);
        };
    }

    WrapperType create_unsupport_wrapper(const String from_type_name,
                                         const String to_type_name) const {
        const String error_msg = fmt::format("Conversion from {} to {} is not supported",
                                             from_type_name, to_type_name);
        return create_unsupport_wrapper(error_msg);
    }

    WrapperType create_hll_wrapper(FunctionContext* context, const DataTypePtr& from_type_untyped,
                                   const DataTypeHLL& to_type) const {
        /// Conversion from String through parsing.
        if (check_and_get_data_type<DataTypeString>(from_type_untyped.get())) {
            return &ConvertImplGenericFromString::execute;
        }

        //TODO if from is not string, it must be HLL?
        const auto* from_type = check_and_get_data_type<DataTypeHLL>(from_type_untyped.get());

        if (!from_type) {
            return create_unsupport_wrapper(
                    "CAST AS HLL can only be performed between HLL, String "
                    "types");
        }

        return nullptr;
    }

    WrapperType create_bitmap_wrapper(FunctionContext* context,
                                      const DataTypePtr& from_type_untyped,
                                      const DataTypeBitMap& to_type) const {
        /// Conversion from String through parsing.
        if (check_and_get_data_type<DataTypeString>(from_type_untyped.get())) {
            return &ConvertImplGenericFromString::execute;
        }

        //TODO if from is not string, it must be BITMAP?
        const auto* from_type = check_and_get_data_type<DataTypeBitMap>(from_type_untyped.get());

        if (!from_type) {
            return create_unsupport_wrapper(
                    "CAST AS BITMAP can only be performed between BITMAP, String "
                    "types");
        }

        return nullptr;
    }

    WrapperType create_array_wrapper(FunctionContext* context, const DataTypePtr& from_type_untyped,
                                     const DataTypeArray& to_type) const {
        /// Conversion from String through parsing.
        if (check_and_get_data_type<DataTypeString>(from_type_untyped.get())) {
            return &ConvertImplGenericFromString::execute;
        }

        const auto* from_type = check_and_get_data_type<DataTypeArray>(from_type_untyped.get());

        if (!from_type) {
            return create_unsupport_wrapper(
                    "CAST AS Array can only be performed between same-dimensional Array, String "
                    "types");
        }

        DataTypePtr from_nested_type = from_type->get_nested_type();

        /// In query SELECT CAST([] AS Array(Array(String))) from type is Array(Nothing)
        bool from_empty_array = from_nested_type->get_primitive_type() == INVALID_TYPE;

        if (from_type->get_number_of_dimensions() != to_type.get_number_of_dimensions() &&
            !from_empty_array) {
            return create_unsupport_wrapper(
                    "CAST AS Array can only be performed between same-dimensional array types");
        }

        const DataTypePtr& to_nested_type = to_type.get_nested_type();

        /// Prepare nested type conversion
        const auto nested_function =
                prepare_unpack_dictionaries(context, from_nested_type, to_nested_type);

        return [nested_function, from_nested_type, to_nested_type](
                       FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                       const uint32_t result, size_t /*input_rows_count*/) -> Status {
            ColumnPtr from_column = block.get_by_position(arguments.front()).column;

            const ColumnArray* from_col_array =
                    check_and_get_column<ColumnArray>(from_column.get());

            if (from_col_array) {
                /// create columns for converting nested column containing original and result columns
                ColumnWithTypeAndName from_nested_column {from_col_array->get_data_ptr(),
                                                          from_nested_type, ""};

                /// convert nested column
                ColumnNumbers new_arguments {block.columns()};
                block.insert(from_nested_column);

                size_t nested_result = block.columns();
                block.insert({to_nested_type, ""});
                RETURN_IF_ERROR(nested_function(context, block, new_arguments, nested_result,
                                                from_col_array->get_data_ptr()->size()));
                auto nested_result_column = block.get_by_position(nested_result).column;

                /// set converted nested column to result
                block.get_by_position(result).column = ColumnArray::create(
                        nested_result_column, from_col_array->get_offsets_ptr());
            } else {
                return Status::RuntimeError("Illegal column {} for function CAST AS Array",
                                            from_column->get_name());
            }
            return Status::OK();
        };
    }

    // check jsonb value type and get to_type value
    WrapperType create_jsonb_wrapper(const DataTypeJsonb& from_type, const DataTypePtr& to_type,
                                     bool jsonb_string_as_string) const {
        switch (to_type->get_primitive_type()) {
        case PrimitiveType::TYPE_BOOLEAN:
            return &ConvertImplFromJsonb<PrimitiveType::TYPE_BOOLEAN, ColumnUInt8>::execute;
        case PrimitiveType::TYPE_TINYINT:
            return &ConvertImplFromJsonb<PrimitiveType::TYPE_TINYINT, ColumnInt8>::execute;
        case PrimitiveType::TYPE_SMALLINT:
            return &ConvertImplFromJsonb<PrimitiveType::TYPE_SMALLINT, ColumnInt16>::execute;
        case PrimitiveType::TYPE_INT:
            return &ConvertImplFromJsonb<PrimitiveType::TYPE_INT, ColumnInt32>::execute;
        case PrimitiveType::TYPE_BIGINT:
            return &ConvertImplFromJsonb<PrimitiveType::TYPE_BIGINT, ColumnInt64>::execute;
        case PrimitiveType::TYPE_LARGEINT:
            return &ConvertImplFromJsonb<PrimitiveType::TYPE_LARGEINT, ColumnInt128>::execute;
        case PrimitiveType::TYPE_DOUBLE:
            return &ConvertImplFromJsonb<PrimitiveType::TYPE_DOUBLE, ColumnFloat64>::execute;
        case PrimitiveType::TYPE_STRING:
        case PrimitiveType::TYPE_CHAR:
        case PrimitiveType::TYPE_VARCHAR:
            if (!jsonb_string_as_string) {
                // Conversion from String through parsing.
                return &ConvertImplGenericToString::execute2;
            } else {
                return ConvertImplGenericFromJsonb::execute;
            }
        default:
            return ConvertImplGenericFromJsonb::execute;
        }
    }

    // create cresponding jsonb value with type to_type
    // use jsonb writer to create jsonb value
    WrapperType create_jsonb_wrapper(const DataTypePtr& from_type, const DataTypeJsonb& to_type,
                                     bool string_as_jsonb_string) const {
        switch (from_type->get_primitive_type()) {
        case PrimitiveType::TYPE_BOOLEAN:
            return &ConvertImplNumberToJsonb<ColumnUInt8>::execute;
        case PrimitiveType::TYPE_TINYINT:
            return &ConvertImplNumberToJsonb<ColumnInt8>::execute;
        case PrimitiveType::TYPE_SMALLINT:
            return &ConvertImplNumberToJsonb<ColumnInt16>::execute;
        case PrimitiveType::TYPE_INT:
            return &ConvertImplNumberToJsonb<ColumnInt32>::execute;
        case PrimitiveType::TYPE_BIGINT:
            return &ConvertImplNumberToJsonb<ColumnInt64>::execute;
        case PrimitiveType::TYPE_LARGEINT:
            return &ConvertImplNumberToJsonb<ColumnInt128>::execute;
        case PrimitiveType::TYPE_DOUBLE:
            return &ConvertImplNumberToJsonb<ColumnFloat64>::execute;
        case PrimitiveType::TYPE_STRING:
        case PrimitiveType::TYPE_CHAR:
        case PrimitiveType::TYPE_VARCHAR:
            if (string_as_jsonb_string) {
                // We convert column string to jsonb type just add a string jsonb field to dst column instead of parse
                // each line in original string column.
                return &ConvertImplStringToJsonbAsJsonbString::execute;
            } else {
                return &ConvertImplGenericFromString::execute;
            }
        case PrimitiveType::INVALID_TYPE:
            return &ConvertNothingToJsonb::execute;
        default:
            return &ConvertImplGenericToJsonb::execute;
        }
    }

    struct ConvertImplGenericFromVariant {
        static Status execute(const FunctionCast* fn, FunctionContext* context, Block& block,
                              const ColumnNumbers& arguments, const uint32_t result,
                              size_t input_rows_count) {
            auto& data_type_to = block.get_by_position(result).type;
            const auto& col_with_type_and_name = block.get_by_position(arguments[0]);
            auto& col_from = col_with_type_and_name.column;
            auto& variant = assert_cast<const ColumnVariant&>(*col_from);
            ColumnPtr col_to = data_type_to->create_column();
            if (!variant.is_finalized()) {
                // ColumnObject should be finalized before parsing, finalize maybe modify original column structure
                variant.assume_mutable()->finalize();
            }
            // It's important to convert as many elements as possible in this context. For instance,
            // if the root of this variant column is a number column, converting it to a number column
            // is acceptable. However, if the destination type is a string and root is none scalar root, then
            // we should convert the entire tree to a string.
            bool is_root_valuable =
                    variant.is_scalar_variant() ||
                    (!variant.is_null_root() &&
                     variant.get_root_type()->get_primitive_type() != INVALID_TYPE &&
                     !is_string_type(data_type_to->get_primitive_type()) &&
                     data_type_to->get_primitive_type() != TYPE_JSONB);
            if (is_root_valuable) {
                ColumnPtr nested = variant.get_root();
                auto nested_from_type = variant.get_root_type();
                // DCHECK(nested_from_type->is_nullable());
                DCHECK(!data_type_to->is_nullable());
                auto new_context = context->clone();
                new_context->set_jsonb_string_as_string(true);
                // dst type nullable has been removed, so we should remove the inner nullable of root column
                auto wrapper = fn->prepare_impl(
                        new_context.get(), remove_nullable(nested_from_type), data_type_to, true);
                Block tmp_block {{remove_nullable(nested), remove_nullable(nested_from_type), ""}};
                tmp_block.insert({nullptr, data_type_to, ""});
                /// Perform the requested conversion.
                Status st = wrapper(new_context.get(), tmp_block, {0}, 1, input_rows_count);
                if (!st.ok()) {
                    // Fill with default values, which is null
                    col_to->assume_mutable()->insert_many_defaults(input_rows_count);
                    col_to = make_nullable(col_to, true);
                } else {
                    col_to = tmp_block.get_by_position(1).column;
                    // Note: here we should return the nullable result column
                    col_to = wrap_in_nullable(
                            col_to,
                            Block({{nested, nested_from_type, ""}, {col_to, data_type_to, ""}}),
                            {0}, 1, input_rows_count);
                }
            } else {
                if (variant.empty()) {
                    // TODO not found root cause, a tmp fix
                    col_to->assume_mutable()->insert_many_defaults(input_rows_count);
                    col_to = make_nullable(col_to, true);
                } else if (is_string_type(data_type_to->get_primitive_type())) {
                    // serialize to string
                    return ConvertImplGenericToString::execute2(context, block, arguments, result,
                                                                input_rows_count);
                } else if (data_type_to->get_primitive_type() == TYPE_JSONB) {
                    // serialize to json by parsing
                    return ConvertImplGenericToJsonb::execute(context, block, arguments, result,
                                                              input_rows_count);
                } else if (!data_type_to->is_nullable() &&
                           !is_string_type(data_type_to->get_primitive_type())) {
                    // other types
                    col_to->assume_mutable()->insert_many_defaults(input_rows_count);
                    col_to = make_nullable(col_to, true);
                } else {
                    assert_cast<ColumnNullable&>(*col_to->assume_mutable())
                            .insert_many_defaults(input_rows_count);
                }
            }
            if (col_to->size() != input_rows_count) {
                return Status::InternalError("Unmatched row count {}, expected {}", col_to->size(),
                                             input_rows_count);
            }

            block.replace_by_position(result, std::move(col_to));
            return Status::OK();
        }
    };

    struct ConvertImplGenericToVariant {
        static Status execute(FunctionContext* context, Block& block,
                              const ColumnNumbers& arguments, const uint32_t result,
                              size_t input_rows_count) {
            // auto& data_type_to = block.get_by_position(result).type;
            const auto& col_with_type_and_name = block.get_by_position(arguments[0]);
            auto& from_type = col_with_type_and_name.type;
            auto& col_from = col_with_type_and_name.column;
            // set variant root column/type to from column/type
            auto variant = ColumnVariant::create(true /*always nullable*/);
            variant->create_root(from_type, col_from->assume_mutable());
            block.replace_by_position(result, std::move(variant));
            return Status::OK();
        }
    };

    // create cresponding variant value to wrap from_type
    WrapperType create_variant_wrapper(const DataTypePtr& from_type,
                                       const DataTypeVariant& to_type) const {
        return &ConvertImplGenericToVariant::execute;
    }

    // create cresponding type convert from variant
    WrapperType create_variant_wrapper(const DataTypeVariant& from_type,
                                       const DataTypePtr& to_type) const {
        return [this](FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                      const uint32_t result, size_t input_rows_count) -> Status {
            return ConvertImplGenericFromVariant::execute(this, context, block, arguments, result,
                                                          input_rows_count);
        };
    }

    //TODO(Amory) . Need support more cast for key , value for map
    WrapperType create_map_wrapper(FunctionContext* context, const DataTypePtr& from_type,
                                   const DataTypeMap& to_type) const {
        if (is_string_type(from_type->get_primitive_type())) {
            return &ConvertImplGenericFromString::execute;
        }
        auto from = check_and_get_data_type<DataTypeMap>(from_type.get());
        if (!from) {
            return create_unsupport_wrapper(
                    fmt::format("CAST AS Map can only be performed between Map types or from "
                                "String. from type: {}, to type: {}",
                                from_type->get_name(), to_type.get_name()));
        }
        DataTypes from_kv_types;
        DataTypes to_kv_types;
        from_kv_types.reserve(2);
        to_kv_types.reserve(2);
        from_kv_types.push_back(from->get_key_type());
        from_kv_types.push_back(from->get_value_type());
        to_kv_types.push_back(to_type.get_key_type());
        to_kv_types.push_back(to_type.get_value_type());

        auto kv_wrappers = get_element_wrappers(context, from_kv_types, to_kv_types);
        return [kv_wrappers, from_kv_types, to_kv_types](
                       FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                       const uint32_t result, size_t /*input_rows_count*/) -> Status {
            auto& from_column = block.get_by_position(arguments.front()).column;
            auto from_col_map = check_and_get_column<ColumnMap>(from_column.get());
            if (!from_col_map) {
                return Status::RuntimeError("Illegal column {} for function CAST AS MAP",
                                            from_column->get_name());
            }

            Columns converted_columns(2);
            ColumnsWithTypeAndName columnsWithTypeAndName(2);
            columnsWithTypeAndName[0] = {from_col_map->get_keys_ptr(), from_kv_types[0], ""};
            columnsWithTypeAndName[1] = {from_col_map->get_values_ptr(), from_kv_types[1], ""};

            for (size_t i = 0; i < 2; ++i) {
                ColumnNumbers element_arguments {block.columns()};
                block.insert(columnsWithTypeAndName[i]);
                size_t element_result = block.columns();
                block.insert({to_kv_types[i], ""});
                RETURN_IF_ERROR(kv_wrappers[i](context, block, element_arguments, element_result,
                                               columnsWithTypeAndName[i].column->size()));
                converted_columns[i] = block.get_by_position(element_result).column;
            }

            auto map_column =
                    ColumnMap::create(std::move(*converted_columns[0]).mutate(),
                                      std::move(*converted_columns[1]).mutate(),
                                      std::move(*from_col_map->get_offsets_ptr()).mutate());
            RETURN_IF_ERROR(map_column->deduplicate_keys());
            block.get_by_position(result).column = std::move(map_column);
            return Status::OK();
        };
    }

    ElementWrappers get_element_wrappers(FunctionContext* context,
                                         const DataTypes& from_element_types,
                                         const DataTypes& to_element_types) const {
        DCHECK(from_element_types.size() == to_element_types.size());
        ElementWrappers element_wrappers;
        element_wrappers.reserve(from_element_types.size());
        for (size_t i = 0; i < from_element_types.size(); ++i) {
            const DataTypePtr& from_element_type = from_element_types[i];
            const DataTypePtr& to_element_type = to_element_types[i];
            element_wrappers.push_back(
                    prepare_unpack_dictionaries(context, from_element_type, to_element_type));
        }
        return element_wrappers;
    }

    // check struct value type and get to_type value
    // TODO: need handle another type to cast struct
    WrapperType create_struct_wrapper(FunctionContext* context, const DataTypePtr& from_type,
                                      const DataTypeStruct& to_type) const {
        // support CAST AS Struct from string
        if (is_string_type(from_type->get_primitive_type())) {
            return &ConvertImplGenericFromString::execute;
        }

        // only support CAST AS Struct from struct or string types
        auto from = check_and_get_data_type<DataTypeStruct>(from_type.get());
        if (!from) {
            return create_unsupport_wrapper(
                    fmt::format("CAST AS Struct can only be performed between struct types or from "
                                "String. Left type: {}, right type: {}",
                                from_type->get_name(), to_type.get_name()));
        }

        const auto& from_element_types = from->get_elements();
        const auto& to_element_types = to_type.get_elements();
        // only support CAST AS Struct from struct type with same number of elements
        if (from_element_types.size() != to_element_types.size()) {
            return create_unsupport_wrapper(
                    fmt::format("CAST AS Struct can only be performed between struct types with "
                                "the same number of elements. Left type: {}, right type: {}",
                                from_type->get_name(), to_type.get_name()));
        }

        auto element_wrappers = get_element_wrappers(context, from_element_types, to_element_types);
        return [element_wrappers, from_element_types, to_element_types](
                       FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                       const uint32_t result, size_t /*input_rows_count*/) -> Status {
            auto& from_column = block.get_by_position(arguments.front()).column;
            auto from_col_struct = check_and_get_column<ColumnStruct>(from_column.get());
            if (!from_col_struct) {
                return Status::RuntimeError("Illegal column {} for function CAST AS Struct",
                                            from_column->get_name());
            }

            size_t elements_num = to_element_types.size();
            Columns converted_columns(elements_num);
            for (size_t i = 0; i < elements_num; ++i) {
                ColumnWithTypeAndName from_element_column {from_col_struct->get_column_ptr(i),
                                                           from_element_types[i], ""};
                ColumnNumbers element_arguments {block.columns()};
                block.insert(from_element_column);

                size_t element_result = block.columns();
                block.insert({to_element_types[i], ""});

                RETURN_IF_ERROR(element_wrappers[i](context, block, element_arguments,
                                                    element_result,
                                                    from_col_struct->get_column(i).size()));
                converted_columns[i] = block.get_by_position(element_result).column;
            }

            block.get_by_position(result).column = ColumnStruct::create(converted_columns);
            return Status::OK();
        };
    }

    WrapperType prepare_unpack_dictionaries(FunctionContext* context, const DataTypePtr& from_type,
                                            const DataTypePtr& to_type) const {
        const auto& from_nested = from_type;
        const auto& to_nested = to_type;

        if (from_type->is_null_literal()) {
            if (!to_nested->is_nullable()) {
                return create_unsupport_wrapper("Cannot convert NULL to a non-nullable type");
            }

            return [](FunctionContext* context, Block& block, const ColumnNumbers&,
                      const uint32_t result, size_t input_rows_count) {
                auto& res = block.get_by_position(result);
                res.column = res.type->create_column_const_with_default_value(input_rows_count)
                                     ->convert_to_full_column_if_const();
                return Status::OK();
            };
        }

        bool skip_not_null_check = false;
        auto wrapper =
                prepare_remove_nullable(context, from_nested, to_nested, skip_not_null_check);

        return wrapper;
    }

    static bool need_replace_null_data_to_default(FunctionContext* context,
                                                  const DataTypePtr& from_type,
                                                  const DataTypePtr& to_type) {
        if (from_type->equals(*to_type)) {
            return false;
        }

        auto make_default_wrapper = [&](const auto& types) -> bool {
            using Types = std::decay_t<decltype(types)>;
            using ToDataType = typename Types::LeftType;

            if constexpr (!(IsDataTypeDecimalOrNumber<ToDataType> ||
                            IsDatelikeV1Types<ToDataType> || IsDatelikeV2Types<ToDataType> ||
                            std::is_same_v<ToDataType, DataTypeTimeV2>)) {
                return false;
            }
            return call_on_index_and_data_type<
                    ToDataType>(from_type->get_primitive_type(), [&](const auto& types2) -> bool {
                using Types2 = std::decay_t<decltype(types2)>;
                using FromDataType = typename Types2::LeftType;
                if constexpr (!(IsDataTypeDecimalOrNumber<FromDataType> ||
                                IsDatelikeV1Types<FromDataType> ||
                                IsDatelikeV2Types<FromDataType> ||
                                std::is_same_v<FromDataType, DataTypeTimeV2>)) {
                    return false;
                }
                if constexpr (IsDataTypeDecimal<FromDataType> || IsDataTypeDecimal<ToDataType>) {
                    using FromFieldType = typename FromDataType::FieldType;
                    using ToFieldType = typename ToDataType::FieldType;
                    UInt32 from_precision = NumberTraits::max_ascii_len<FromFieldType>();
                    UInt32 from_scale = 0;

                    if constexpr (IsDataTypeDecimal<FromDataType>) {
                        const auto* from_decimal_type =
                                check_and_get_data_type<FromDataType>(from_type.get());
                        from_precision =
                                NumberTraits::max_ascii_len<typename FromFieldType::NativeType>();
                        from_scale = from_decimal_type->get_scale();
                    }

                    UInt32 to_max_digits = 0;
                    UInt32 to_precision = 0;
                    UInt32 to_scale = 0;

                    ToFieldType max_result {0};
                    ToFieldType min_result {0};
                    if constexpr (IsDataTypeDecimal<ToDataType>) {
                        to_max_digits =
                                NumberTraits::max_ascii_len<typename ToFieldType::NativeType>();

                        const auto* to_decimal_type =
                                check_and_get_data_type<ToDataType>(to_type.get());
                        to_precision = to_decimal_type->get_precision();
                        ToDataType::check_type_precision(to_precision);

                        to_scale = to_decimal_type->get_scale();
                        ToDataType::check_type_scale(to_scale);

                        max_result = ToDataType::get_max_digits_number(to_precision);
                        min_result = -max_result;
                    }
                    if constexpr (std::is_integral_v<ToFieldType> ||
                                  std::is_floating_point_v<ToFieldType>) {
                        max_result = type_limit<ToFieldType>::max();
                        min_result = type_limit<ToFieldType>::min();
                        to_max_digits = NumberTraits::max_ascii_len<ToFieldType>();
                        to_precision = to_max_digits;
                    }

                    bool narrow_integral =
                            context->check_overflow_for_decimal() &&
                            (to_precision - to_scale) <= (from_precision - from_scale);

                    bool multiply_may_overflow = context->check_overflow_for_decimal();
                    if (to_scale > from_scale) {
                        multiply_may_overflow &=
                                (from_precision + to_scale - from_scale) >= to_max_digits;
                    }
                    return narrow_integral || multiply_may_overflow;
                }
                return false;
            });
        };

        return call_on_index_and_data_type<void>(to_type->get_primitive_type(),
                                                 make_default_wrapper);
    }

    WrapperType prepare_remove_nullable(FunctionContext* context, const DataTypePtr& from_type,
                                        const DataTypePtr& to_type,
                                        bool skip_not_null_check) const {
        /// Determine whether pre-processing and/or post-processing must take place during conversion.
        bool result_is_nullable = to_type->is_nullable();

        if (result_is_nullable) {
            return [this, from_type, to_type](FunctionContext* context, Block& block,
                                              const ColumnNumbers& arguments, const uint32_t result,
                                              size_t input_rows_count) {
                auto from_type_not_nullable = remove_nullable(from_type);
                auto to_type_not_nullable = remove_nullable(to_type);

                bool replace_null_data_to_default = need_replace_null_data_to_default(
                        context, from_type_not_nullable, to_type_not_nullable);

                auto nested_result_index = block.columns();
                block.insert(block.get_by_position(result).get_nested());
                auto nested_source_index = block.columns();
                block.insert(block.get_by_position(arguments[0])
                                     .get_nested(replace_null_data_to_default));

                RETURN_IF_ERROR(prepare_impl(context, from_type_not_nullable, to_type_not_nullable,
                                             true)(context, block, {nested_source_index},
                                                   nested_result_index, input_rows_count));

                block.get_by_position(result).column =
                        wrap_in_nullable(block.get_by_position(nested_result_index).column, block,
                                         arguments, result, input_rows_count);

                block.erase(nested_source_index);
                block.erase(nested_result_index);
                return Status::OK();
            };
        } else {
            return prepare_impl(context, from_type, to_type, false);
        }
    }

    /// 'from_type' and 'to_type' are nested types in case of Nullable.
    /// 'requested_result_is_nullable' is true if CAST to Nullable type is requested.
    WrapperType prepare_impl(FunctionContext* context, const DataTypePtr& from_type,
                             const DataTypePtr& to_type, bool requested_result_is_nullable) const {
        if (from_type->equals(*to_type)) {
            return create_identity_wrapper(from_type);
        }

        // variant needs to be judged first
        if (to_type->get_primitive_type() == PrimitiveType::TYPE_VARIANT) {
            return create_variant_wrapper(from_type, static_cast<const DataTypeVariant&>(*to_type));
        }
        if (from_type->get_primitive_type() == PrimitiveType::TYPE_VARIANT) {
            return create_variant_wrapper(static_cast<const DataTypeVariant&>(*from_type), to_type);
        }

        switch (from_type->get_primitive_type()) {
        case PrimitiveType::INVALID_TYPE:
            return create_nothing_wrapper(to_type.get());
        case PrimitiveType::TYPE_JSONB:
            return create_jsonb_wrapper(static_cast<const DataTypeJsonb&>(*from_type), to_type,
                                        context ? context->jsonb_string_as_string() : false);
        default:
            break;
        }

        WrapperType ret;

        auto make_default_wrapper = [&](const auto& types) -> bool {
            using Types = std::decay_t<decltype(types)>;
            using ToDataType = typename Types::LeftType;

            if constexpr (std::is_same_v<ToDataType, DataTypeUInt8> ||
                          std::is_same_v<ToDataType, DataTypeUInt16> ||
                          std::is_same_v<ToDataType, DataTypeUInt32> ||
                          std::is_same_v<ToDataType, DataTypeUInt64> ||
                          std::is_same_v<ToDataType, DataTypeInt8> ||
                          std::is_same_v<ToDataType, DataTypeInt16> ||
                          std::is_same_v<ToDataType, DataTypeInt32> ||
                          std::is_same_v<ToDataType, DataTypeInt64> ||
                          std::is_same_v<ToDataType, DataTypeInt128> ||
                          std::is_same_v<ToDataType, DataTypeFloat32> ||
                          std::is_same_v<ToDataType, DataTypeFloat64> ||
                          std::is_same_v<ToDataType, DataTypeDate> ||
                          std::is_same_v<ToDataType, DataTypeDateTime> ||
                          std::is_same_v<ToDataType, DataTypeDateV2> ||
                          std::is_same_v<ToDataType, DataTypeDateTimeV2> ||
                          std::is_same_v<ToDataType, DataTypeTimeV2> ||
                          std::is_same_v<ToDataType, DataTypeIPv4> ||
                          std::is_same_v<ToDataType, DataTypeIPv6>) {
                ret = create_wrapper(from_type, check_and_get_data_type<ToDataType>(to_type.get()),
                                     requested_result_is_nullable);
                return true;
            }

            if constexpr (std::is_same_v<ToDataType, DataTypeDecimal<Decimal32>> ||
                          std::is_same_v<ToDataType, DataTypeDecimal<Decimal64>> ||
                          std::is_same_v<ToDataType, DataTypeDecimal<Decimal128V2>> ||
                          std::is_same_v<ToDataType, DataTypeDecimal<Decimal128V3>> ||
                          std::is_same_v<ToDataType, DataTypeDecimal<Decimal256>>) {
                ret = create_decimal_wrapper(from_type,
                                             check_and_get_data_type<ToDataType>(to_type.get()));
                return true;
            }

            return false;
        };

        if (call_on_index_and_data_type<void>(to_type->get_primitive_type(),
                                              make_default_wrapper)) {
            return ret;
        }

        switch (to_type->get_primitive_type()) {
        case PrimitiveType::TYPE_CHAR:
        case PrimitiveType::TYPE_VARCHAR:
        case PrimitiveType::TYPE_STRING:
            return create_string_wrapper(from_type);
        case PrimitiveType::TYPE_ARRAY:
            return create_array_wrapper(context, from_type,
                                        static_cast<const DataTypeArray&>(*to_type));
        case PrimitiveType::TYPE_STRUCT:
            return create_struct_wrapper(context, from_type,
                                         static_cast<const DataTypeStruct&>(*to_type));
        case PrimitiveType::TYPE_MAP:
            return create_map_wrapper(context, from_type,
                                      static_cast<const DataTypeMap&>(*to_type));
        case PrimitiveType::TYPE_HLL:
            return create_hll_wrapper(context, from_type,
                                      static_cast<const DataTypeHLL&>(*to_type));
        case PrimitiveType::TYPE_OBJECT:
            return create_bitmap_wrapper(context, from_type,
                                         static_cast<const DataTypeBitMap&>(*to_type));
        case PrimitiveType::TYPE_JSONB:
            return create_jsonb_wrapper(from_type, static_cast<const DataTypeJsonb&>(*to_type),
                                        context ? context->string_as_jsonb_string() : false);
        default:
            break;
        }

        return create_unsupport_wrapper(from_type->get_name(), to_type->get_name());
    }
};

class FunctionBuilderCast : public FunctionBuilderImpl {
public:
    static constexpr auto name = "CAST";
    static FunctionBuilderPtr create() { return std::make_shared<FunctionBuilderCast>(); }

    FunctionBuilderCast() = default;

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 2; }

    ColumnNumbers get_arguments_that_are_always_constant() const override { return {1}; }

protected:
    FunctionBasePtr build_impl(const ColumnsWithTypeAndName& arguments,
                               const DataTypePtr& return_type) const override {
        DataTypes data_types(arguments.size());

        for (size_t i = 0; i < arguments.size(); ++i) data_types[i] = arguments[i].type;

        return std::make_shared<FunctionCast>(name, data_types, return_type);
    }

    DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) const override {
        DataTypePtr type = arguments[1].type;
        DCHECK(type != nullptr);
        bool need_to_be_nullable = false;
        // 1. from_type is nullable
        need_to_be_nullable |= arguments[0].type->is_nullable();
        // 2. from_type is string, to_type is not string
        need_to_be_nullable |= (is_string_type(arguments[0].type->get_primitive_type())) &&
                               (!is_string_type(type->get_primitive_type()));
        // 3. from_type is not DateTime/Date, to_type is DateTime/Date
        need_to_be_nullable |=
                (arguments[0].type->get_primitive_type() != PrimitiveType::TYPE_DATE &&
                 arguments[0].type->get_primitive_type() != PrimitiveType::TYPE_DATETIME) &&
                (type->get_primitive_type() == PrimitiveType::TYPE_DATE ||
                 type->get_primitive_type() == PrimitiveType::TYPE_DATETIME);
        // 4. from_type is not DateTimeV2/DateV2, to_type is DateTimeV2/DateV2
        need_to_be_nullable |=
                (arguments[0].type->get_primitive_type() != PrimitiveType::TYPE_DATEV2 &&
                 arguments[0].type->get_primitive_type() != PrimitiveType::TYPE_DATETIMEV2) &&
                (type->get_primitive_type() == PrimitiveType::TYPE_DATEV2 ||
                 type->get_primitive_type() == PrimitiveType::TYPE_DATETIMEV2);
        if (need_to_be_nullable && !type->is_nullable()) {
            return make_nullable(type);
        }

        return type;
    }

    bool use_default_implementation_for_nulls() const override { return false; }
    bool use_default_implementation_for_low_cardinality_columns() const override { return false; }
};
} // namespace doris::vectorized
