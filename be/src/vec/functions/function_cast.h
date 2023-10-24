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
#include <glog/logging.h>
#include <stddef.h>
#include <stdint.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <cmath>
#include <cstdint>
#include <functional>
#include <iterator>
#include <limits>
#include <memory>
#include <ostream>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/status.h"
#include "runtime/runtime_state.h"
#include "udf/udf.h"
#include "util/jsonb_document.h"
#include "util/jsonb_stream.h"
#include "util/jsonb_writer.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_nullable.h"
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
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_hll.h"
#include "vec/data_types/data_type_ipv4.h"
#include "vec/data_types/data_type_ipv6.h"
#include "vec/data_types/data_type_jsonb.h"
#include "vec/data_types/data_type_map.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_struct.h"
#include "vec/data_types/data_type_time.h"
#include "vec/data_types/data_type_time_v2.h"
#include "vec/functions/function.h"
#include "vec/functions/function_convert_tz.h"
#include "vec/functions/function_helpers.h"
#include "vec/io/reader_buffer.h"
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
        LOG(FATAL) << fmt::format("Illegal type of toDecimal() scale {}",
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
/** Cast from string or number to Time.
  * In Doris, the underlying storage type of the Time class is Float64.
  */
struct TimeCast {
    // Cast from string
    // Some examples of conversions.
    // '300' -> 00:03:00 '20:23' ->  20:23:00 '20:23:24' -> 20:23:24
    template <typename T>
    static bool try_parse_time(char* s, size_t len, T& x, const cctz::time_zone& local_time_zone) {
        /// TODO: Maybe we can move Timecast to the io_helper.
        if (try_as_time(s, len, x, local_time_zone)) {
            return true;
        } else {
            if (VecDateTimeValue dv {}; dv.from_date_str(s, len, local_time_zone)) {
                // can be parse as a datetime
                x = dv.hour() * 3600 + dv.minute() * 60 + dv.second();
                return true;
            }
            return false;
        }
    }

    template <typename T>
    static bool try_as_time(char* s, size_t len, T& x, const cctz::time_zone& local_time_zone) {
        char* first_char = s;
        char* end_char = s + len;
        int hour = 0, minute = 0, second = 0;
        auto parse_from_str_to_int = [](char* begin, size_t len, auto& num) {
            StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
            auto int_value = StringParser::string_to_unsigned_int<uint64_t>(
                    reinterpret_cast<char*>(begin), len, &parse_result);
            if (UNLIKELY(parse_result != StringParser::PARSE_SUCCESS)) {
                return false;
            }
            num = int_value;
            return true;
        };
        if (char* first_colon {nullptr};
            (first_colon = (char*)memchr(first_char, ':', len)) != nullptr) {
            if (char* second_colon {nullptr};
                (second_colon = (char*)memchr(first_colon + 1, ':', end_char - first_colon - 1)) !=
                nullptr) {
                // find two colon
                // parse hour
                if (!parse_from_str_to_int(first_char, first_colon - first_char, hour)) {
                    // hour  failed
                    return false;
                }
                // parse minute
                if (!parse_from_str_to_int(first_colon + 1, second_colon - first_colon - 1,
                                           minute)) {
                    return false;
                }
                // parse second
                if (!parse_from_str_to_int(second_colon + 1, end_char - second_colon - 1, second)) {
                    return false;
                }
            } else {
                // find one colon
                // parse hour
                if (!parse_from_str_to_int(first_char, first_colon - first_char, hour)) {
                    return false;
                }
                // parse minute
                if (!parse_from_str_to_int(first_colon + 1, end_char - first_colon - 1, minute)) {
                    return false;
                }
            }
        } else {
            // no colon ,so try to parse as a number
            size_t from {};
            if (!parse_from_str_to_int(first_char, len, from)) {
                return false;
            }
            return try_parse_time(from, x, local_time_zone);
        }
        // minute second must be < 60
        if (minute >= 60 || second >= 60) {
            return false;
        }
        x = hour * 3600 + minute * 60 + second;
        return true;
    }
    // Cast from number
    template <typename T, typename S>
    //requires {std::is_arithmetic_v<T> && std::is_arithmetic_v<S>}
    static bool try_parse_time(T from, S& x, const cctz::time_zone& local_time_zone) {
        int64 seconds = from / 100;
        int64 hour = 0, minute = 0, second = 0;
        second = from - 100 * seconds;
        from /= 100;
        seconds = from / 100;
        minute = from - 100 * seconds;
        hour = seconds;
        if (minute >= 60 || second >= 60) {
            return false;
        }
        x = hour * 3600 + minute * 60 + second;
        return true;
    }
    template <typename S>
    static bool try_parse_time(__int128 from, S& x, const cctz::time_zone& local_time_zone) {
        from %= (int64)(1000000000000);
        int64 seconds = from / 100;
        int64 hour = 0, minute = 0, second = 0;
        second = from - 100 * seconds;
        from /= 100;
        seconds = from / 100;
        minute = from - 100 * seconds;
        hour = seconds;
        if (minute >= 60 || second >= 60) {
            return false;
        }
        x = hour * 3600 + minute * 60 + second;
        return true;
    }
};

/** Conversion of number types to each other, enums to numbers, dates and datetimes to numbers and back: done by straight assignment.
  *  (Date is represented internally as number of days from some day; DateTime - as unix timestamp)
  */
template <typename FromDataType, typename ToDataType, typename Name>
struct ConvertImpl {
    using FromFieldType = typename FromDataType::FieldType;
    using ToFieldType = typename ToDataType::FieldType;

    template <typename Additions = void*>
    static Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          size_t result, size_t /*input_rows_count*/,
                          bool check_overflow [[maybe_unused]] = false,
                          Additions additions [[maybe_unused]] = Additions()) {
        const ColumnWithTypeAndName& named_from = block.get_by_position(arguments[0]);

        using ColVecFrom =
                std::conditional_t<IsDecimalNumber<FromFieldType>, ColumnDecimal<FromFieldType>,
                                   ColumnVector<FromFieldType>>;
        using ColVecTo = std::conditional_t<IsDecimalNumber<ToFieldType>,
                                            ColumnDecimal<ToFieldType>, ColumnVector<ToFieldType>>;

        if constexpr (IsDataTypeDecimal<FromDataType> || IsDataTypeDecimal<ToDataType>) {
            if constexpr (!(IsDataTypeDecimalOrNumber<FromDataType> || IsTimeType<FromDataType> ||
                            IsTimeV2Type<FromDataType>) ||
                          !IsDataTypeDecimalOrNumber<ToDataType>)
                return Status::RuntimeError("Illegal column {} of first argument of function {}",
                                            named_from.column->get_name(), Name::name);
        }

        if (const ColVecFrom* col_from =
                    check_and_get_column<ColVecFrom>(named_from.column.get())) {
            typename ColVecTo::MutablePtr col_to = nullptr;
            if constexpr (IsDataTypeDecimal<ToDataType>) {
                UInt32 scale = ((PrecisionScaleArg)additions).scale;
                ToDataType::check_type_scale(scale);
                col_to = ColVecTo::create(0, scale);
            } else {
                col_to = ColVecTo::create();
            }

            const auto& vec_from = col_from->get_data();
            auto& vec_to = col_to->get_data();
            size_t size = vec_from.size();
            vec_to.resize(size);

            if constexpr (IsDataTypeDecimal<FromDataType> || IsDataTypeDecimal<ToDataType>) {
                ColumnUInt8::MutablePtr col_null_map_to = nullptr;
                UInt8* vec_null_map_to = nullptr;
                if (check_overflow) {
                    col_null_map_to = ColumnUInt8::create(size, 0);
                    vec_null_map_to = col_null_map_to->get_data().data();
                }
                if constexpr (IsDataTypeDecimal<FromDataType> && IsDataTypeDecimal<ToDataType>) {
                    convert_decimal_cols<FromDataType, ToDataType>(
                            vec_from.data(), vec_to.data(), vec_from.get_scale(),
                            vec_to.get_scale(), vec_from.size(), vec_null_map_to);
                } else {
                    for (size_t i = 0; i < size; ++i) {
                        if constexpr (IsDataTypeDecimal<FromDataType> &&
                                      IsDataTypeDecimal<ToDataType>) {
                            vec_to[i] = convert_decimals<FromDataType, ToDataType>(
                                    vec_from[i], vec_from.get_scale(), vec_to.get_scale(),
                                    vec_null_map_to ? &vec_null_map_to[i] : vec_null_map_to);
                        } else if constexpr (IsDataTypeDecimal<FromDataType> &&
                                             IsDataTypeNumber<ToDataType>) {
                            vec_to[i] = convert_from_decimal<FromDataType, ToDataType>(
                                    vec_from[i], vec_from.get_scale());
                        } else if constexpr (IsDataTypeNumber<FromDataType> &&
                                             IsDataTypeDecimal<ToDataType>) {
                            vec_to[i] = convert_to_decimal<FromDataType, ToDataType>(
                                    vec_from[i], vec_to.get_scale(),
                                    vec_null_map_to ? &vec_null_map_to[i] : vec_null_map_to);
                        } else if constexpr (IsTimeType<FromDataType> &&
                                             IsDataTypeDecimal<ToDataType>) {
                            vec_to[i] = convert_to_decimal<DataTypeInt64, ToDataType>(
                                    reinterpret_cast<const VecDateTimeValue&>(vec_from[i])
                                            .to_int64(),
                                    vec_to.get_scale(),
                                    vec_null_map_to ? &vec_null_map_to[i] : vec_null_map_to);
                        } else if constexpr (IsDateV2Type<FromDataType> &&
                                             IsDataTypeDecimal<ToDataType>) {
                            vec_to[i] = convert_to_decimal<DataTypeUInt32, ToDataType>(
                                    reinterpret_cast<const DateV2Value<DateV2ValueType>&>(
                                            vec_from[i])
                                            .to_date_int_val(),
                                    vec_to.get_scale(),
                                    vec_null_map_to ? &vec_null_map_to[i] : vec_null_map_to);
                        } else if constexpr (IsDateTimeV2Type<FromDataType> &&
                                             IsDataTypeDecimal<ToDataType>) {
                            // TODO: should we consider the scale of datetimev2?
                            vec_to[i] = convert_to_decimal<DataTypeUInt64, ToDataType>(
                                    reinterpret_cast<const DateV2Value<DateTimeV2ValueType>&>(
                                            vec_from[i])
                                            .to_date_int_val(),
                                    vec_to.get_scale(),
                                    vec_null_map_to ? &vec_null_map_to[i] : vec_null_map_to);
                        }
                    }
                }

                if (check_overflow) {
                    block.replace_by_position(
                            result,
                            ColumnNullable::create(std::move(col_to), std::move(col_null_map_to)));
                } else {
                    block.replace_by_position(result, std::move(col_to));
                }

                return Status::OK();
            } else if constexpr (IsTimeType<FromDataType>) {
                for (size_t i = 0; i < size; ++i) {
                    if constexpr (IsTimeType<ToDataType>) {
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
                        vec_to[i] =
                                reinterpret_cast<const VecDateTimeValue&>(vec_from[i]).to_int64();
                    }
                }
            } else if constexpr (IsTimeV2Type<FromDataType>) {
                for (size_t i = 0; i < size; ++i) {
                    if constexpr (IsTimeV2Type<ToDataType>) {
                        if constexpr (IsDateTimeV2Type<ToDataType> && IsDateV2Type<FromDataType>) {
                            DataTypeDateV2::cast_to_date_time_v2(vec_from[i], vec_to[i]);
                        } else if constexpr (IsDateTimeV2Type<FromDataType> &&
                                             IsDateV2Type<ToDataType>) {
                            DataTypeDateTimeV2::cast_to_date_v2(vec_from[i], vec_to[i]);
                        } else {
                            UInt32 scale = additions;
                            vec_to[i] = vec_from[i] / std::pow(10, 6 - scale);
                        }
                    } else if constexpr (IsTimeType<ToDataType>) {
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
                            vec_to[i] = reinterpret_cast<const DateV2Value<DateTimeV2ValueType>&>(
                                                vec_from[i])
                                                .to_int64();
                        } else {
                            vec_to[i] = reinterpret_cast<const DateV2Value<DateV2ValueType>&>(
                                                vec_from[i])
                                                .to_int64();
                        }
                    }
                }
            } else {
                if constexpr (IsDataTypeNumber<FromDataType> &&
                              std::is_same_v<ToDataType, DataTypeTimeV2>) {
                    // 300 -> 00:03:00  360 will be parse failed , so value maybe null
                    ColumnUInt8::MutablePtr col_null_map_to;
                    ColumnUInt8::Container* vec_null_map_to = nullptr;
                    col_null_map_to = ColumnUInt8::create(size);
                    vec_null_map_to = &col_null_map_to->get_data();
                    for (size_t i = 0; i < size; ++i) {
                        (*vec_null_map_to)[i] = !TimeCast::try_parse_time(
                                vec_from[i], vec_to[i], context->state()->timezone_obj());
                        vec_to[i] *= (1000 * 1000);
                    }
                    block.get_by_position(result).column =
                            ColumnNullable::create(std::move(col_to), std::move(col_null_map_to));
                    return Status::OK();
                } else {
                    for (size_t i = 0; i < size; ++i) {
                        vec_to[i] = static_cast<ToFieldType>(vec_from[i]);
                    }
                }
            }
            // TODO: support boolean cast more reasonable
            if constexpr (std::is_same_v<uint8_t, ToFieldType>) {
                for (int i = 0; i < size; ++i) {
                    vec_to[i] = static_cast<bool>(vec_to[i]);
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
                          size_t result, size_t /*input_rows_count*/) {
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

    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result,
                          size_t /*input_rows_count*/) {
        const ColumnWithTypeAndName& named_from = block.get_by_position(arguments[0]);

        using ColVecFrom =
                std::conditional_t<IsDecimalNumber<FromFieldType>, ColumnDecimal<FromFieldType>,
                                   ColumnVector<FromFieldType>>;

        using DateValueType = std::conditional_t<
                IsTimeV2Type<ToDataType>,
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
            col_null_map_to = ColumnUInt8::create(size);
            auto& vec_null_map_to = col_null_map_to->get_data();

            for (size_t i = 0; i < size; ++i) {
                auto& date_value = reinterpret_cast<DateValueType&>(vec_to[i]);
                if constexpr (IsDecimalNumber<FromFieldType>) {
                    // TODO: should we consider the scale of datetimev2?
                    vec_null_map_to[i] = !date_value.from_date_int64(
                            convert_from_decimal<FromDataType, DataTypeInt64>(
                                    vec_from[i], vec_from.get_scale()));
                } else {
                    vec_null_map_to[i] = !date_value.from_date_int64(vec_from[i]);
                }
                // DateType of VecDateTimeValue should cast to date
                if constexpr (IsDateType<ToDataType>) {
                    date_value.cast_to_date();
                } else if constexpr (IsDateTimeType<ToDataType>) {
                    date_value.to_datetime();
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

        size_t size = col_from.size();

        auto col_to = ColumnString::create();
        col_to->reserve(size * 2);
        VectorBufferWriter write_buffer(*col_to.get());
        for (size_t i = 0; i < size; ++i) {
            type.to_string(col_from, i, write_buffer);
            write_buffer.commit();
        }

        block.replace_by_position(result, std::move(col_to));
        return Status::OK();
    }

    static Status execute2(FunctionContext* /*ctx*/, Block& block, const ColumnNumbers& arguments,
                           const size_t result, size_t /*input_rows_count*/) {
        return execute(block, arguments, result);
    }
};
//this is for data in compound type
struct ConvertImplGenericFromString {
    static Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          const size_t result, size_t input_rows_count) {
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

            ColumnUInt8::MutablePtr col_null_map_to = ColumnUInt8::create(size);
            ColumnUInt8::Container* vec_null_map_to = &col_null_map_to->get_data();
            const bool is_complex = is_complex_type(data_type_to);
            DataTypeSerDe::FormatOptions format_options;
            format_options.converted_from_string = true;

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
                          const size_t result, size_t input_rows_count) {
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
                LOG(FATAL) << "unsupported type ";
            }
            column_string->insert_data(writer.getOutput()->getBuffer(),
                                       writer.getOutput()->getSize());
        }

        block.replace_by_position(result, std::move(column_string));
        return Status::OK();
    }
};

// Generic conversion of any type to jsonb.
struct ConvertImplGenericToJsonb {
    static Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          const size_t result, size_t input_rows_count) {
        const auto& col_with_type_and_name = block.get_by_position(arguments[0]);
        const IDataType& type = *col_with_type_and_name.type;
        const IColumn& col_from = *col_with_type_and_name.column;

        auto column_string = ColumnString::create();
        JsonbWriter writer;

        auto tmp_col = ColumnString::create();
        for (size_t i = 0; i < input_rows_count; i++) {
            // convert to string
            tmp_col->clear();
            VectorBufferWriter write_buffer(*tmp_col.get());
            type.to_string(col_from, i, write_buffer);
            write_buffer.commit();
            // write string to jsonb
            writer.reset();
            writer.writeStartString();
            auto str_ref = tmp_col->get_data_at(0);
            writer.writeString(str_ref.data, str_ref.size);
            writer.writeEndString();
            column_string->insert_data(writer.getOutput()->getBuffer(),
                                       writer.getOutput()->getSize());
        }

        block.replace_by_position(result, std::move(column_string));
        return Status::OK();
    }
};

template <TypeIndex type_index, typename ColumnType>
struct ConvertImplFromJsonb {
    static Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          const size_t result, size_t input_rows_count) {
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
                JsonbDocument* doc = JsonbDocument::createDocument(val.data, val.size);
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

                if constexpr (type_index == TypeIndex::UInt8) {
                    if (value->isTrue()) {
                        res[i] = 1;
                    } else if (value->isFalse()) {
                        res[i] = 0;
                    } else {
                        null_map[i] = 1;
                        res[i] = 0;
                    }
                } else if constexpr (type_index == TypeIndex::Int8) {
                    if (value->isInt8()) {
                        res[i] = (int8_t)((const JsonbIntVal*)value)->val();
                    } else {
                        null_map[i] = 1;
                        res[i] = 0;
                    }
                } else if constexpr (type_index == TypeIndex::Int16) {
                    if (value->isInt8() || value->isInt16()) {
                        res[i] = (int16_t)((const JsonbIntVal*)value)->val();
                    } else {
                        null_map[i] = 1;
                        res[i] = 0;
                    }
                } else if constexpr (type_index == TypeIndex::Int32) {
                    if (value->isInt8() || value->isInt16() || value->isInt32()) {
                        res[i] = (int32_t)((const JsonbIntVal*)value)->val();
                    } else {
                        null_map[i] = 1;
                        res[i] = 0;
                    }
                } else if constexpr (type_index == TypeIndex::Int64) {
                    if (value->isInt8() || value->isInt16() || value->isInt32() ||
                        value->isInt64()) {
                        res[i] = (int64_t)((const JsonbIntVal*)value)->val();
                    } else {
                        null_map[i] = 1;
                        res[i] = 0;
                    }
                } else if constexpr (type_index == TypeIndex::Int128) {
                    if (value->isInt8() || value->isInt16() || value->isInt32() ||
                        value->isInt64() || value->isInt128()) {
                        res[i] = (int128_t)((const JsonbIntVal*)value)->val();
                    } else {
                        null_map[i] = 1;
                        res[i] = 0;
                    }
                } else if constexpr (type_index == TypeIndex::Float64) {
                    if (value->isDouble()) {
                        res[i] = ((const JsonbDoubleVal*)value)->val();
                    } else if (value->isInt8() || value->isInt16() || value->isInt32() ||
                               value->isInt64()) {
                        res[i] = ((const JsonbIntVal*)value)->val();
                    } else {
                        null_map[i] = 1;
                        res[i] = 0;
                    }
                } else {
                    LOG(FATAL) << "unsupported type ";
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
                          size_t result, size_t /*input_rows_count*/,
                          bool check_overflow [[maybe_unused]] = false,
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
struct NameToDecimal128I {
    static constexpr auto name = "toDecimal128I";
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

template <typename DataType, typename Additions = void*, typename FromDataType = void*>
bool try_parse_impl(typename DataType::FieldType& x, ReadBuffer& rb,
                    const cctz::time_zone& local_time_zone,
                    Additions additions [[maybe_unused]] = Additions()) {
    if constexpr (IsDateTimeType<DataType>) {
        return try_read_datetime_text(x, rb, local_time_zone);
    }

    if constexpr (IsDateType<DataType>) {
        return try_read_date_text(x, rb, local_time_zone);
    }

    if constexpr (IsDateV2Type<DataType>) {
        return try_read_date_v2_text(x, rb, local_time_zone);
    }

    if constexpr (IsDateTimeV2Type<DataType>) {
        UInt32 scale = additions;
        return try_read_datetime_v2_text(x, rb, local_time_zone, scale);
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
        auto ret = TimeCast::try_parse_time(s, len, x, local_time_zone);
        x *= (1000 * 1000);
        return ret;
    }
    if constexpr (std::is_floating_point_v<typename DataType::FieldType>) {
        return try_read_float_text(x, rb);
    }

    // uint8_t now use as boolean in doris
    if constexpr (std::is_same_v<typename DataType::FieldType, uint8_t>) {
        return try_read_bool_text(x, rb);
    }

    if constexpr (std::is_integral_v<typename DataType::FieldType> &&
                  !std::is_same_v<typename DataType::FieldType, IPv6>) {
        return try_read_int_text(x, rb);
    }
}

template <typename DataType, typename Additions = void*>
StringParser::ParseResult try_parse_decimal_impl(typename DataType::FieldType& x, ReadBuffer& rb,
                                                 const cctz::time_zone& local_time_zone,
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

    if constexpr (IsDataTypeDecimal128I<DataType>) {
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

/// Monotonicity.

struct PositiveMonotonicity {
    static bool has() { return true; }
    static IFunction::Monotonicity get(const IDataType&, const Field&, const Field&) {
        return {true};
    }
};

struct UnknownMonotonicity {
    static bool has() { return false; }
    static IFunction::Monotonicity get(const IDataType&, const Field&, const Field&) {
        return {false};
    }
};

template <typename T>
struct ToNumberMonotonicity {
    static bool has() { return true; }

    static UInt64 divide_by_range_of_type(UInt64 x) {
        if constexpr (sizeof(T) < sizeof(UInt64))
            return x >> (sizeof(T) * 8);
        else
            return 0;
    }

    static IFunction::Monotonicity get(const IDataType& type, const Field& left,
                                       const Field& right) {
        if (!type.is_value_represented_by_number()) return {};

        /// If type is same, the conversion is always monotonic.
        /// (Enum has separate case, because it is different data type)
        if (check_and_get_data_type<DataTypeNumber<T>>(
                    &type) /*|| check_and_get_data_type<DataTypeEnum<T>>(&type)*/)
            return {true, true, true};

        /// Float cases.

        /// When converting to Float, the conversion is always monotonic.
        if (std::is_floating_point_v<T>) return {true, true, true};

        /// If converting from Float, for monotonicity, arguments must fit in range of result type.
        if (WhichDataType(type).is_float()) {
            if (left.is_null() || right.is_null()) return {};

            Float64 left_float = left.get<Float64>();
            Float64 right_float = right.get<Float64>();

            if (left_float >= std::numeric_limits<T>::min() &&
                left_float <= static_cast<Float64>(std::numeric_limits<T>::max()) &&
                right_float >= std::numeric_limits<T>::min() &&
                right_float <= static_cast<Float64>(std::numeric_limits<T>::max()))
                return {true};

            return {};
        }

        /// Integer cases.

        const bool from_is_unsigned = type.is_value_represented_by_unsigned_integer();
        const bool to_is_unsigned = std::is_unsigned_v<T>;

        const size_t size_of_from = type.get_size_of_value_in_memory();
        const size_t size_of_to = sizeof(T);

        const bool left_in_first_half =
                left.is_null() ? from_is_unsigned : (left.get<Int64>() >= 0);

        const bool right_in_first_half =
                right.is_null() ? !from_is_unsigned : (right.get<Int64>() >= 0);

        /// Size of type is the same.
        if (size_of_from == size_of_to) {
            if (from_is_unsigned == to_is_unsigned) return {true, true, true};

            if (left_in_first_half == right_in_first_half) return {true};

            return {};
        }

        /// Size of type is expanded.
        if (size_of_from < size_of_to) {
            if (from_is_unsigned == to_is_unsigned) return {true, true, true};

            if (!to_is_unsigned) return {true, true, true};

            /// signed -> unsigned. If arguments from the same half, then function is monotonic.
            if (left_in_first_half == right_in_first_half) return {true};

            return {};
        }

        /// Size of type is shrinked.
        if (size_of_from > size_of_to) {
            /// Function cannot be monotonic on unbounded ranges.
            if (left.is_null() || right.is_null()) return {};

            if (from_is_unsigned == to_is_unsigned) {
                /// all bits other than that fits, must be same.
                if (divide_by_range_of_type(left.get<UInt64>()) ==
                    divide_by_range_of_type(right.get<UInt64>()))
                    return {true};

                return {};
            } else {
                /// When signedness is changed, it's also required for arguments to be from the same half.
                /// And they must be in the same half after converting to the result type.
                if (left_in_first_half == right_in_first_half &&
                    (T(left.get<Int64>()) >= 0) == (T(right.get<Int64>()) >= 0) &&
                    divide_by_range_of_type(left.get<UInt64>()) ==
                            divide_by_range_of_type(right.get<UInt64>()))
                    return {true};

                return {};
            }
        }
        LOG(FATAL) << "__builtin_unreachable";
        __builtin_unreachable();
    }
};

/** The monotonicity for the `to_string` function is mainly determined for test purposes.
  * It is doubtful that anyone is looking to optimize queries with conditions `std::to_string(CounterID) = 34`.
  */
struct ToStringMonotonicity {
    static bool has() { return true; }

    static IFunction::Monotonicity get(const IDataType& type, const Field& left,
                                       const Field& right) {
        IFunction::Monotonicity positive(true, true);
        IFunction::Monotonicity not_monotonic;

        if (left.is_null() || right.is_null()) return {};

        if (left.get_type() == Field::Types::UInt64 && right.get_type() == Field::Types::UInt64) {
            return (left.get<Int64>() == 0 && right.get<Int64>() == 0) ||
                                   (floor(log10(left.get<UInt64>())) ==
                                    floor(log10(right.get<UInt64>())))
                           ? positive
                           : not_monotonic;
        }

        if (left.get_type() == Field::Types::Int64 && right.get_type() == Field::Types::Int64) {
            return (left.get<Int64>() == 0 && right.get<Int64>() == 0) ||
                                   (left.get<Int64>() > 0 && right.get<Int64>() > 0 &&
                                    floor(log10(left.get<Int64>())) ==
                                            floor(log10(right.get<Int64>())))
                           ? positive
                           : not_monotonic;
        }

        return not_monotonic;
    }
};

template <typename ToDataType, typename Name, typename MonotonicityImpl>
class FunctionConvert : public IFunction {
public:
    using Monotonic = MonotonicityImpl;

    static constexpr auto name = Name::name;

    static FunctionPtr create() { return std::make_shared<FunctionConvert>(); }

    String get_name() const override { return name; }

    bool is_variadic() const override { return true; }
    size_t get_number_of_arguments() const override { return 0; }
    bool get_is_injective(const Block&) override { return std::is_same_v<Name, NameToString>; }

    // This function should not be called for get DateType Ptr
    // using the FunctionCast::get_return_type_impl
    DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) const override {
        return std::make_shared<ToDataType>();
    }

    ColumnNumbers get_arguments_that_are_always_constant() const override { return {1}; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
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
                            context->check_overflow_for_decimal(), scale_column.type->get_scale());
                } else if constexpr (IsDataTypeDateTimeV2<RightDataType>) {
                    const ColumnWithTypeAndName& scale_column = block.get_by_position(result);
                    ret_status = ConvertImpl<LeftDataType, RightDataType, Name>::execute(
                            context, block, arguments, result, input_rows_count,
                            context->check_overflow_for_decimal(), scale_column.type->get_scale());
                } else {
                    ret_status = ConvertImpl<LeftDataType, RightDataType, Name>::execute(
                            context, block, arguments, result, input_rows_count);
                }
                return true;
            };

            bool done = call_on_index_and_data_type<ToDataType>(from_type->get_type_id(), call);
            if (!done) {
                ret_status = Status::RuntimeError(
                        "Illegal type {} of argument of function {}",
                        block.get_by_position(arguments[0]).type->get_name(), get_name());
            }
            return ret_status;
        }
    }

    bool has_information_about_monotonicity() const override { return Monotonic::has(); }

    Monotonicity get_monotonicity_for_range(const IDataType& type, const Field& left,
                                            const Field& right) const override {
        return Monotonic::get(type, left, right);
    }
};

using FunctionToUInt8 = FunctionConvert<DataTypeUInt8, NameToUInt8, ToNumberMonotonicity<UInt8>>;
using FunctionToUInt16 =
        FunctionConvert<DataTypeUInt16, NameToUInt16, ToNumberMonotonicity<UInt16>>;
using FunctionToUInt32 =
        FunctionConvert<DataTypeUInt32, NameToUInt32, ToNumberMonotonicity<UInt32>>;
using FunctionToUInt64 =
        FunctionConvert<DataTypeUInt64, NameToUInt64, ToNumberMonotonicity<UInt64>>;
using FunctionToInt8 = FunctionConvert<DataTypeInt8, NameToInt8, ToNumberMonotonicity<Int8>>;
using FunctionToInt16 = FunctionConvert<DataTypeInt16, NameToInt16, ToNumberMonotonicity<Int16>>;
using FunctionToInt32 = FunctionConvert<DataTypeInt32, NameToInt32, ToNumberMonotonicity<Int32>>;
using FunctionToInt64 = FunctionConvert<DataTypeInt64, NameToInt64, ToNumberMonotonicity<Int64>>;
using FunctionToInt128 =
        FunctionConvert<DataTypeInt128, NameToInt128, ToNumberMonotonicity<Int128>>;
using FunctionToFloat32 =
        FunctionConvert<DataTypeFloat32, NameToFloat32, ToNumberMonotonicity<Float32>>;
using FunctionToFloat64 =
        FunctionConvert<DataTypeFloat64, NameToFloat64, ToNumberMonotonicity<Float64>>;

using FunctionToTimeV2 =
        FunctionConvert<DataTypeTimeV2, NameToFloat64, ToNumberMonotonicity<Float64>>;
using FunctionToString = FunctionConvert<DataTypeString, NameToString, ToStringMonotonicity>;
using FunctionToDecimal32 =
        FunctionConvert<DataTypeDecimal<Decimal32>, NameToDecimal32, UnknownMonotonicity>;
using FunctionToDecimal64 =
        FunctionConvert<DataTypeDecimal<Decimal64>, NameToDecimal64, UnknownMonotonicity>;
using FunctionToDecimal128 =
        FunctionConvert<DataTypeDecimal<Decimal128>, NameToDecimal128, UnknownMonotonicity>;
using FunctionToDecimal128I =
        FunctionConvert<DataTypeDecimal<Decimal128I>, NameToDecimal128I, UnknownMonotonicity>;
using FunctionToDecimal256 =
        FunctionConvert<DataTypeDecimal<Decimal256>, NameToDecimal256, UnknownMonotonicity>;
using FunctionToIPv4 = FunctionConvert<DataTypeIPv4, NameToIPv4, UnknownMonotonicity>;
using FunctionToIPv6 = FunctionConvert<DataTypeIPv6, NameToIPv6, UnknownMonotonicity>;
using FunctionToDate = FunctionConvert<DataTypeDate, NameToDate, UnknownMonotonicity>;
using FunctionToDateTime = FunctionConvert<DataTypeDateTime, NameToDateTime, UnknownMonotonicity>;
using FunctionToDateV2 = FunctionConvert<DataTypeDateV2, NameToDate, UnknownMonotonicity>;
using FunctionToDateTimeV2 =
        FunctionConvert<DataTypeDateTimeV2, NameToDateTime, UnknownMonotonicity>;

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
struct FunctionTo<DataTypeDecimal<Decimal128>> {
    using Type = FunctionToDecimal128;
};
template <>
struct FunctionTo<DataTypeDecimal<Decimal128I>> {
    using Type = FunctionToDecimal128I;
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
                        size_t result, size_t input_rows_count) const override {
        /// drop second argument, pass others
        ColumnNumbers new_arguments {arguments.front()};
        if (arguments.size() > 2)
            new_arguments.insert(std::end(new_arguments), std::next(std::begin(arguments), 2),
                                 std::end(arguments));
        return wrapper_function(context, block, new_arguments, result, input_rows_count);
    }

    bool use_default_implementation_for_nulls() const override { return false; }
    bool use_default_implementation_for_low_cardinality_columns() const override { return false; }
    ColumnNumbers get_arguments_that_are_always_constant() const override { return {1}; }

private:
    WrapperType wrapper_function;
    const char* name;
};

struct NameCast {
    static constexpr auto name = "CAST";
};

template <typename FromDataType, typename ToDataType, typename Name>
struct ConvertThroughParsing {
    static_assert(std::is_same_v<FromDataType, DataTypeString>,
                  "ConvertThroughParsing is only applicable for String or FixedString data types");

    using ToFieldType = typename ToDataType::FieldType;

    static bool is_all_read(ReadBuffer& in) { return in.eof(); }

    template <typename Additions = void*>
    static Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          size_t result, size_t input_rows_count,
                          bool check_overflow [[maybe_unused]] = false,
                          Additions additions [[maybe_unused]] = Additions()) {
        using ColVecTo = std::conditional_t<IsDecimalNumber<ToFieldType>,
                                            ColumnDecimal<ToFieldType>, ColumnVector<ToFieldType>>;

        const IColumn* col_from = block.get_by_position(arguments[0]).column.get();
        const ColumnString* col_from_string = check_and_get_column<ColumnString>(col_from);

        if (std::is_same_v<FromDataType, DataTypeString> && !col_from_string) {
            return Status::RuntimeError("Illegal column {} of first argument of function {}",
                                        col_from->get_name(), Name::name);
        }

        size_t size = input_rows_count;
        typename ColVecTo::MutablePtr col_to = nullptr;

        if constexpr (IsDataTypeDecimal<ToDataType>) {
            UInt32 scale = ((PrecisionScaleArg)additions).scale;
            ToDataType::check_type_scale(scale);
            col_to = ColVecTo::create(size, scale);
        } else {
            col_to = ColVecTo::create(size);
        }

        typename ColVecTo::Container& vec_to = col_to->get_data();

        ColumnUInt8::MutablePtr col_null_map_to;
        ColumnUInt8::Container* vec_null_map_to [[maybe_unused]] = nullptr;
        col_null_map_to = ColumnUInt8::create(size);
        vec_null_map_to = &col_null_map_to->get_data();

        const ColumnString::Chars* chars = nullptr;
        const IColumn::Offsets* offsets = nullptr;
        size_t fixed_string_size = 0;

        if constexpr (std::is_same_v<FromDataType, DataTypeString>) {
            chars = &col_from_string->get_chars();
            offsets = &col_from_string->get_offsets();
        }

        size_t current_offset = 0;
        for (size_t i = 0; i < size; ++i) {
            size_t next_offset = std::is_same_v<FromDataType, DataTypeString>
                                         ? (*offsets)[i]
                                         : (current_offset + fixed_string_size);
            size_t string_size = std::is_same_v<FromDataType, DataTypeString>
                                         ? next_offset - current_offset
                                         : fixed_string_size;

            ReadBuffer read_buffer(&(*chars)[current_offset], string_size);

            bool parsed;
            if constexpr (IsDataTypeDecimal<ToDataType>) {
                ToDataType::check_type_precision((PrecisionScaleArg(additions).precision));
                StringParser::ParseResult res = try_parse_decimal_impl<ToDataType>(
                        vec_to[i], read_buffer, context->state()->timezone_obj(),
                        PrecisionScaleArg(additions));
                parsed = (res == StringParser::PARSE_SUCCESS ||
                          res == StringParser::PARSE_OVERFLOW ||
                          res == StringParser::PARSE_UNDERFLOW);
            } else if constexpr (IsDataTypeDateTimeV2<ToDataType>) {
                auto type = check_and_get_data_type<DataTypeDateTimeV2>(
                        block.get_by_position(result).type.get());
                parsed = try_parse_impl<ToDataType>(vec_to[i], read_buffer,
                                                    context->state()->timezone_obj(),
                                                    type->get_scale());
            } else {
                parsed = try_parse_impl<ToDataType, void*, FromDataType>(
                        vec_to[i], read_buffer, context->state()->timezone_obj());
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
        : ConvertThroughParsing<DataTypeString, DataTypeDecimal<Decimal32>, Name> {};
template <typename Name>
struct ConvertImpl<DataTypeString, DataTypeDecimal<Decimal64>, Name>
        : ConvertThroughParsing<DataTypeString, DataTypeDecimal<Decimal64>, Name> {};
template <typename Name>
struct ConvertImpl<DataTypeString, DataTypeDecimal<Decimal128>, Name>
        : ConvertThroughParsing<DataTypeString, DataTypeDecimal<Decimal128>, Name> {};
template <typename Name>
struct ConvertImpl<DataTypeString, DataTypeDecimal<Decimal128I>, Name>
        : ConvertThroughParsing<DataTypeString, DataTypeDecimal<Decimal128I>, Name> {};
template <typename Name>
struct ConvertImpl<DataTypeString, DataTypeDecimal<Decimal256>, Name>
        : ConvertThroughParsing<DataTypeString, DataTypeDecimal<Decimal256>, Name> {};
template <typename Name>
struct ConvertImpl<DataTypeString, DataTypeIPv4, Name>
        : ConvertThroughParsing<DataTypeString, DataTypeIPv4, Name> {};
template <typename Name>
struct ConvertImpl<DataTypeString, DataTypeIPv6, Name>
        : ConvertThroughParsing<DataTypeString, DataTypeIPv6, Name> {};

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
            LOG(FATAL) << "Someting wrong with toDecimalNNOrZero() or toDecimalNNOrNull()";

        } else {
            res = std::make_shared<ToDataType>();
        }

        return res;
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        const IDataType* from_type = block.get_by_position(arguments[0]).type.get();

        if (check_and_get_data_type<DataTypeString>(from_type)) {
            return ConvertThroughParsing<DataTypeString, ToDataType, Name>::execute(
                    context, block, arguments, result, input_rows_count);
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
                        size_t result, size_t input_rows_count) const override {
        Status ret_status = Status::OK();
        const IDataType* from_type = block.get_by_position(arguments[0]).type.get();
        auto call = [&](const auto& types) -> bool {
            using Types = std::decay_t<decltype(types)>;
            using LeftDataType = typename Types::LeftType;
            using RightDataType = typename Types::RightType;

            ret_status = ConvertImplToTimeType<LeftDataType, RightDataType, Name>::execute(
                    block, arguments, result, input_rows_count);
            return true;
        };

        bool done = call_on_index_and_number_data_type<ToDataType>(from_type->get_type_id(), call);
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
    using MonotonicityForRange =
            std::function<Monotonicity(const IDataType&, const Field&, const Field&)>;

    FunctionCast(const char* name_, MonotonicityForRange&& monotonicity_for_range_,
                 const DataTypes& argument_types_, const DataTypePtr& return_type_)
            : name(name_),
              monotonicity_for_range(monotonicity_for_range_),
              argument_types(argument_types_),
              return_type(return_type_) {}

    const DataTypes& get_argument_types() const override { return argument_types; }
    const DataTypePtr& get_return_type() const override { return return_type; }

    PreparedFunctionPtr prepare(FunctionContext* context, const Block& /*sample_block*/,
                                const ColumnNumbers& /*arguments*/,
                                size_t /*result*/) const override {
        return std::make_shared<PreparedFunctionCast>(
                prepare_unpack_dictionaries(context, get_argument_types()[0], get_return_type()),
                name);
    }

    String get_name() const override { return name; }

    bool is_deterministic() const override { return true; }
    bool is_deterministic_in_scope_of_query() const override { return true; }

    bool has_information_about_monotonicity() const override {
        return static_cast<bool>(monotonicity_for_range);
    }

    Monotonicity get_monotonicity_for_range(const IDataType& type, const Field& left,
                                            const Field& right) const override {
        return monotonicity_for_range(type, left, right);
    }

    bool is_use_default_implementation_for_constants() const override { return true; }

private:
    const char* name;
    MonotonicityForRange monotonicity_for_range;

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
                   (IsTimeType<DataType> || IsTimeV2Type<DataType>)&&!(
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
                          const size_t result, size_t input_rows_count) {
            return function->execute(context, block, arguments, result, input_rows_count);
        };
    }

    WrapperType create_string_wrapper(const DataTypePtr& from_type) const {
        FunctionPtr function = FunctionToString::create();

        /// Check conversion using underlying function
        { function->get_return_type(ColumnsWithTypeAndName(1, {nullptr, from_type, ""})); }

        return [function](FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          const size_t result, size_t input_rows_count) {
            return function->execute(context, block, arguments, result, input_rows_count);
        };
    }

    template <typename FieldType>
    WrapperType create_decimal_wrapper(const DataTypePtr& from_type,
                                       const DataTypeDecimal<FieldType>* to_type) const {
        using ToDataType = DataTypeDecimal<FieldType>;

        TypeIndex type_index = from_type->get_type_id();
        UInt32 precision = to_type->get_precision();
        UInt32 scale = to_type->get_scale();

        WhichDataType which(type_index);
        bool ok = which.is_int() || which.is_native_uint() || which.is_decimal() ||
                  which.is_float() || which.is_date_or_datetime() ||
                  which.is_date_v2_or_datetime_v2() || which.is_string_or_fixed_string();
        if (!ok) {
            return create_unsupport_wrapper(from_type->get_name(), to_type->get_name());
        }

        return [type_index, precision, scale](FunctionContext* context, Block& block,
                                              const ColumnNumbers& arguments, const size_t result,
                                              size_t input_rows_count) {
            auto res = call_on_index_and_data_type<ToDataType>(
                    type_index, [&](const auto& types) -> bool {
                        using Types = std::decay_t<decltype(types)>;
                        using LeftDataType = typename Types::LeftType;
                        using RightDataType = typename Types::RightType;

                        auto state = ConvertImpl<LeftDataType, RightDataType, NameCast>::execute(
                                context, block, arguments, result, input_rows_count,
                                context->check_overflow_for_decimal(),
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
                                            getTypeName(type_index), to.get_name());
            }
            return Status::OK();
        };
    }

    WrapperType create_identity_wrapper(const DataTypePtr&) const {
        return [](FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                  const size_t result, size_t /*input_rows_count*/) {
            block.get_by_position(result).column = block.get_by_position(arguments.front()).column;
            return Status::OK();
        };
    }

    WrapperType create_nothing_wrapper(const IDataType* to_type) const {
        ColumnPtr res = to_type->create_column_const_with_default_value(1);
        return [res](FunctionContext* context, Block& block, const ColumnNumbers&,
                     const size_t result, size_t input_rows_count) {
            /// Column of Nothing type is trivially convertible to any other column
            block.get_by_position(result).column =
                    res->clone_resized(input_rows_count)->convert_to_full_column_if_const();
            return Status::OK();
        };
    }

    WrapperType create_unsupport_wrapper(const String error_msg) const {
        LOG(WARNING) << error_msg;
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
        bool from_empty_array = is_nothing(from_nested_type);

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
                       const size_t result, size_t /*input_rows_count*/) -> Status {
            auto& from_column = block.get_by_position(arguments.front()).column;

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
    WrapperType create_jsonb_wrapper(const DataTypeJsonb& from_type,
                                     const DataTypePtr& to_type) const {
        // Conversion from String through parsing.
        if (check_and_get_data_type<DataTypeString>(to_type.get())) {
            return &ConvertImplGenericToString::execute2;
        }

        switch (to_type->get_type_id()) {
        case TypeIndex::UInt8:
            return &ConvertImplFromJsonb<TypeIndex::UInt8, ColumnUInt8>::execute;
        case TypeIndex::Int8:
            return &ConvertImplFromJsonb<TypeIndex::Int8, ColumnInt8>::execute;
        case TypeIndex::Int16:
            return &ConvertImplFromJsonb<TypeIndex::Int16, ColumnInt16>::execute;
        case TypeIndex::Int32:
            return &ConvertImplFromJsonb<TypeIndex::Int32, ColumnInt32>::execute;
        case TypeIndex::Int64:
            return &ConvertImplFromJsonb<TypeIndex::Int64, ColumnInt64>::execute;
        case TypeIndex::Int128:
            return &ConvertImplFromJsonb<TypeIndex::Int128, ColumnInt128>::execute;
        case TypeIndex::Float64:
            return &ConvertImplFromJsonb<TypeIndex::Float64, ColumnFloat64>::execute;
        default:
            return create_unsupport_wrapper(from_type.get_name(), to_type->get_name());
        }

        return nullptr;
    }

    // create cresponding jsonb value with type to_type
    // use jsonb writer to create jsonb value
    WrapperType create_jsonb_wrapper(const DataTypePtr& from_type,
                                     const DataTypeJsonb& to_type) const {
        switch (from_type->get_type_id()) {
        case TypeIndex::UInt8:
            return &ConvertImplNumberToJsonb<ColumnUInt8>::execute;
        case TypeIndex::Int8:
            return &ConvertImplNumberToJsonb<ColumnInt8>::execute;
        case TypeIndex::Int16:
            return &ConvertImplNumberToJsonb<ColumnInt16>::execute;
        case TypeIndex::Int32:
            return &ConvertImplNumberToJsonb<ColumnInt32>::execute;
        case TypeIndex::Int64:
            return &ConvertImplNumberToJsonb<ColumnInt64>::execute;
        case TypeIndex::Int128:
            return &ConvertImplNumberToJsonb<ColumnInt128>::execute;
        case TypeIndex::Float64:
            return &ConvertImplNumberToJsonb<ColumnFloat64>::execute;
        case TypeIndex::String:
            return &ConvertImplGenericFromString::execute;
        default:
            return &ConvertImplGenericToJsonb::execute;
        }
    }

    //TODO(Amory) . Need support more cast for key , value for map
    WrapperType create_map_wrapper(const DataTypePtr& from_type, const DataTypeMap& to_type) const {
        switch (from_type->get_type_id()) {
        case TypeIndex::String:
            return &ConvertImplGenericFromString::execute;
        default:
            return create_unsupport_wrapper(from_type->get_name(), to_type.get_name());
        }
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
        if (from_type->get_type_id() == TypeIndex::String) {
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
                       const size_t result, size_t /*input_rows_count*/) -> Status {
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

        if (from_type->only_null() || from_type->is_null_literal()) {
            if (!to_nested->is_nullable()) {
                return create_unsupport_wrapper("Cannot convert NULL to a non-nullable type");
            }

            return [](FunctionContext* context, Block& block, const ColumnNumbers&,
                      const size_t result, size_t input_rows_count) {
                auto& res = block.get_by_position(result);
                res.column = res.type->create_column_const_with_default_value(input_rows_count)
                                     ->convert_to_full_column_if_const();
                return Status::OK();
            };
        }

        constexpr bool skip_not_null_check = false;

        auto wrapper =
                prepare_remove_nullable(context, from_nested, to_nested, skip_not_null_check);

        return wrapper;
    }

    WrapperType prepare_remove_nullable(FunctionContext* context, const DataTypePtr& from_type,
                                        const DataTypePtr& to_type,
                                        bool skip_not_null_check) const {
        /// Determine whether pre-processing and/or post-processing must take place during conversion.
        bool source_is_nullable = from_type->is_nullable();
        bool result_is_nullable = to_type->is_nullable();

        auto wrapper = prepare_impl(context, remove_nullable(from_type), remove_nullable(to_type),
                                    result_is_nullable);

        if (result_is_nullable) {
            return [wrapper, source_is_nullable](FunctionContext* context, Block& block,
                                                 const ColumnNumbers& arguments,
                                                 const size_t result, size_t input_rows_count) {
                /// Create a temporary block on which to perform the operation.
                auto& res = block.get_by_position(result);
                const auto& ret_type = res.type;
                const auto& nullable_type = static_cast<const DataTypeNullable&>(*ret_type);
                const auto& nested_type = nullable_type.get_nested_type();

                Block tmp_block;
                size_t tmp_res_index = 0;
                if (source_is_nullable) {
                    auto [t_block, tmp_args] =
                            create_block_with_nested_columns(block, arguments, true);
                    tmp_block = std::move(t_block);
                    tmp_res_index = tmp_block.columns();
                    tmp_block.insert({nullptr, nested_type, ""});

                    /// Perform the requested conversion.
                    RETURN_IF_ERROR(
                            wrapper(context, tmp_block, {0}, tmp_res_index, input_rows_count));
                } else {
                    tmp_block = block;

                    tmp_res_index = block.columns();
                    tmp_block.insert({nullptr, nested_type, ""});

                    /// Perform the requested conversion.
                    RETURN_IF_ERROR(wrapper(context, tmp_block, arguments, tmp_res_index,
                                            input_rows_count));
                }

                // Note: here we should return the nullable result column
                const auto& tmp_res = tmp_block.get_by_position(tmp_res_index);
                res.column = wrap_in_nullable(tmp_res.column,
                                              Block({block.get_by_position(arguments[0]), tmp_res}),
                                              {0}, 1, input_rows_count);

                return Status::OK();
            };
        } else if (source_is_nullable) {
            /// Conversion from Nullable to non-Nullable.

            return [wrapper, skip_not_null_check](FunctionContext* context, Block& block,
                                                  const ColumnNumbers& arguments,
                                                  const size_t result, size_t input_rows_count) {
                auto [tmp_block, tmp_args, tmp_res] =
                        create_block_with_nested_columns(block, arguments, result);

                /// Check that all values are not-NULL.
                /// Check can be skipped in case if LowCardinality dictionary is transformed.
                /// In that case, correctness will be checked beforehand.
                if (!skip_not_null_check) {
                    const auto& col = block.get_by_position(arguments[0]).column;
                    const auto& nullable_col = assert_cast<const ColumnNullable&>(*col);
                    const auto& null_map = nullable_col.get_null_map_data();

                    if (!memory_is_zero(null_map.data(), null_map.size())) {
                        return Status::RuntimeError(
                                "Cannot convert NULL value to non-Nullable type");
                    }
                }

                RETURN_IF_ERROR(wrapper(context, tmp_block, tmp_args, tmp_res, input_rows_count));
                block.get_by_position(result).column = tmp_block.get_by_position(tmp_res).column;
                return Status::OK();
            };
        } else {
            return wrapper;
        }
    }

    /// 'from_type' and 'to_type' are nested types in case of Nullable.
    /// 'requested_result_is_nullable' is true if CAST to Nullable type is requested.
    WrapperType prepare_impl(FunctionContext* context, const DataTypePtr& from_type,
                             const DataTypePtr& to_type, bool requested_result_is_nullable) const {
        if (from_type->equals(*to_type))
            return create_identity_wrapper(from_type);
        else if (WhichDataType(from_type).is_nothing())
            return create_nothing_wrapper(to_type.get());

        if (from_type->get_type_id() == TypeIndex::JSONB) {
            return create_jsonb_wrapper(static_cast<const DataTypeJsonb&>(*from_type), to_type);
        }
        if (to_type->get_type_id() == TypeIndex::JSONB) {
            return create_jsonb_wrapper(from_type, static_cast<const DataTypeJsonb&>(*to_type));
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
                          std::is_same_v<ToDataType, DataTypeTime> ||
                          std::is_same_v<ToDataType, DataTypeIPv4> ||
                          std::is_same_v<ToDataType, DataTypeIPv6>) {
                ret = create_wrapper(from_type, check_and_get_data_type<ToDataType>(to_type.get()),
                                     requested_result_is_nullable);
                return true;
            }

            if constexpr (std::is_same_v<ToDataType, DataTypeDecimal<Decimal32>> ||
                          std::is_same_v<ToDataType, DataTypeDecimal<Decimal64>> ||
                          std::is_same_v<ToDataType, DataTypeDecimal<Decimal128>> ||
                          std::is_same_v<ToDataType, DataTypeDecimal<Decimal128I>> ||
                          std::is_same_v<ToDataType, DataTypeDecimal<Decimal256>>) {
                ret = create_decimal_wrapper(from_type,
                                             check_and_get_data_type<ToDataType>(to_type.get()));
                return true;
            }

            return false;
        };

        if (call_on_index_and_data_type<void>(to_type->get_type_id(), make_default_wrapper))
            return ret;

        switch (to_type->get_type_id()) {
        case TypeIndex::String:
            return create_string_wrapper(from_type);
        case TypeIndex::Array:
            return create_array_wrapper(context, from_type,
                                        static_cast<const DataTypeArray&>(*to_type));
        case TypeIndex::Struct:
            return create_struct_wrapper(context, from_type,
                                         static_cast<const DataTypeStruct&>(*to_type));
        case TypeIndex::Map:
            return create_map_wrapper(from_type, static_cast<const DataTypeMap&>(*to_type));
        case TypeIndex::HLL:
            return create_hll_wrapper(context, from_type,
                                      static_cast<const DataTypeHLL&>(*to_type));
        case TypeIndex::BitMap:
            return create_bitmap_wrapper(context, from_type,
                                         static_cast<const DataTypeBitMap&>(*to_type));
        default:
            break;
        }

        return create_unsupport_wrapper(from_type->get_name(), to_type->get_name());
    }
};

class FunctionBuilderCast : public FunctionBuilderImpl {
public:
    using MonotonicityForRange = FunctionCast::MonotonicityForRange;

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

        auto monotonicity = get_monotonicity_information(arguments.front().type, return_type.get());
        return std::make_shared<FunctionCast>(name, std::move(monotonicity), data_types,
                                              return_type);
    }

    DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) const override {
        DataTypePtr type = arguments[1].type;
        DCHECK(type != nullptr);
        bool need_to_be_nullable = false;
        // 1. from_type is nullable
        need_to_be_nullable |= arguments[0].type->is_nullable();
        // 2. from_type is string, to_type is not string
        need_to_be_nullable |= (arguments[0].type->get_type_id() == TypeIndex::String) &&
                               (type->get_type_id() != TypeIndex::String);
        // 3. from_type is not DateTime/Date, to_type is DateTime/Date
        need_to_be_nullable |= (arguments[0].type->get_type_id() != TypeIndex::Date &&
                                arguments[0].type->get_type_id() != TypeIndex::DateTime) &&
                               (type->get_type_id() == TypeIndex::Date ||
                                type->get_type_id() == TypeIndex::DateTime);
        // 4. from_type is not DateTimeV2/DateV2, to_type is DateTimeV2/DateV2
        need_to_be_nullable |= (arguments[0].type->get_type_id() != TypeIndex::DateV2 &&
                                arguments[0].type->get_type_id() != TypeIndex::DateTimeV2) &&
                               (type->get_type_id() == TypeIndex::DateV2 ||
                                type->get_type_id() == TypeIndex::DateTimeV2);
        if (need_to_be_nullable && !type->is_nullable()) {
            return make_nullable(type);
        }

        return type;
    }

    bool use_default_implementation_for_nulls() const override { return false; }
    bool use_default_implementation_for_low_cardinality_columns() const override { return false; }

private:
    template <typename DataType>
    static auto monotonicity_for_type(const DataType* const) {
        return FunctionTo<DataType>::Type::Monotonic::get;
    }

    MonotonicityForRange get_monotonicity_information(const DataTypePtr& from_type,
                                                      const IDataType* to_type) const {
        if (const auto type = check_and_get_data_type<DataTypeUInt8>(to_type))
            return monotonicity_for_type(type);
        if (const auto type = check_and_get_data_type<DataTypeUInt16>(to_type))
            return monotonicity_for_type(type);
        if (const auto type = check_and_get_data_type<DataTypeUInt32>(to_type))
            return monotonicity_for_type(type);
        if (const auto type = check_and_get_data_type<DataTypeUInt64>(to_type))
            return monotonicity_for_type(type);
        if (const auto type = check_and_get_data_type<DataTypeInt8>(to_type))
            return monotonicity_for_type(type);
        if (const auto type = check_and_get_data_type<DataTypeInt16>(to_type))
            return monotonicity_for_type(type);
        if (const auto type = check_and_get_data_type<DataTypeInt32>(to_type))
            return monotonicity_for_type(type);
        if (const auto type = check_and_get_data_type<DataTypeInt64>(to_type))
            return monotonicity_for_type(type);
        if (const auto type = check_and_get_data_type<DataTypeFloat32>(to_type))
            return monotonicity_for_type(type);
        if (const auto type = check_and_get_data_type<DataTypeFloat64>(to_type))
            return monotonicity_for_type(type);
        /// other types like Null, FixedString, Array and Tuple have no monotonicity defined
        return {};
    }
};

} // namespace doris::vectorized
