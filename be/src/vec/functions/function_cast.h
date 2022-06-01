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

#include <fmt/format.h>

#include "common/logging.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/columns_common.h"
#include "vec/common/assert_cast.h"
#include "vec/common/field_visitors.h"
#include "vec/common/string_buffer.hpp"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_nothing.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/function.h"
#include "vec/functions/function_helpers.h"
#include "vec/io/io_helper.h"
#include "vec/io/reader_buffer.h"
#include "vec/runtime/vdatetime_value.h"

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

/** Conversion of number types to each other, enums to numbers, dates and datetimes to numbers and back: done by straight assignment.
  *  (Date is represented internally as number of days from some day; DateTime - as unix timestamp)
  */
template <typename FromDataType, typename ToDataType, typename Name>
struct ConvertImpl {
    using FromFieldType = typename FromDataType::FieldType;
    using ToFieldType = typename ToDataType::FieldType;

    template <typename Additions = void*>
    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result,
                          size_t /*input_rows_count*/,
                          Additions additions [[maybe_unused]] = Additions()) {
        const ColumnWithTypeAndName& named_from = block.get_by_position(arguments[0]);

        using ColVecFrom =
                std::conditional_t<IsDecimalNumber<FromFieldType>, ColumnDecimal<FromFieldType>,
                                   ColumnVector<FromFieldType>>;
        using ColVecTo = std::conditional_t<IsDecimalNumber<ToFieldType>,
                                            ColumnDecimal<ToFieldType>, ColumnVector<ToFieldType>>;

        if constexpr (IsDataTypeDecimal<FromDataType> || IsDataTypeDecimal<ToDataType>) {
            if constexpr (!(IsDataTypeDecimalOrNumber<FromDataType> || IsTimeType<FromDataType>) ||
                          !IsDataTypeDecimalOrNumber<ToDataType>)
                return Status::RuntimeError(
                        fmt::format("Illegal column {} of first argument of function {}",
                                    named_from.column->get_name(), Name::name));
        }

        if (const ColVecFrom* col_from =
                    check_and_get_column<ColVecFrom>(named_from.column.get())) {
            typename ColVecTo::MutablePtr col_to = nullptr;
            if constexpr (IsDataTypeDecimal<ToDataType>) {
                UInt32 scale = additions;
                col_to = ColVecTo::create(0, scale);
            } else
                col_to = ColVecTo::create();

            const auto& vec_from = col_from->get_data();
            auto& vec_to = col_to->get_data();
            size_t size = vec_from.size();
            vec_to.resize(size);

            for (size_t i = 0; i < size; ++i) {
                if constexpr (IsDataTypeDecimal<FromDataType> || IsDataTypeDecimal<ToDataType>) {
                    if constexpr (IsDataTypeDecimal<FromDataType> && IsDataTypeDecimal<ToDataType>)
                        vec_to[i] = convert_decimals<FromDataType, ToDataType>(
                                vec_from[i], vec_from.get_scale(), vec_to.get_scale());
                    else if constexpr (IsDataTypeDecimal<FromDataType> &&
                                       IsDataTypeNumber<ToDataType>)
                        vec_to[i] = convert_from_decimal<FromDataType, ToDataType>(
                                vec_from[i], vec_from.get_scale());
                    else if constexpr (IsDataTypeNumber<FromDataType> &&
                                       IsDataTypeDecimal<ToDataType>)
                        vec_to[i] = convert_to_decimal<FromDataType, ToDataType>(
                                vec_from[i], vec_to.get_scale());
                    else if constexpr (IsTimeType<FromDataType> && IsDataTypeDecimal<ToDataType>) {
                        vec_to[i] = convert_to_decimal<DataTypeInt64, ToDataType>(
                                reinterpret_cast<const VecDateTimeValue&>(vec_from[i]).to_int64(),
                                vec_to.get_scale());
                    }
                } else if constexpr (IsTimeType<FromDataType>) {
                    if constexpr (IsTimeType<ToDataType>) {
                        vec_to[i] = static_cast<ToFieldType>(vec_from[i]);
                        if constexpr (IsDateType<FromDataType> && IsDateTimeType<ToDataType>) {
                            DataTypeDateTime::cast_to_date_time(vec_to[i]);
                        } else {
                            DataTypeDate::cast_to_date(vec_to[i]);
                        }
                    } else {
                        vec_to[i] =
                                reinterpret_cast<const VecDateTimeValue&>(vec_from[i]).to_int64();
                    }
                } else
                    vec_to[i] = static_cast<ToFieldType>(vec_from[i]);
            }

            // TODO: support boolean cast more reasonable
            if constexpr (std::is_same_v<uint8_t, ToFieldType>) {
                for (int i = 0; i < size; ++i) {
                    vec_to[i] = static_cast<bool>(vec_to[i]);
                }
            }

            block.replace_by_position(result, std::move(col_to));
        } else {
            return Status::RuntimeError(
                    fmt::format("Illegal column {} of first argument of function {}",
                                named_from.column->get_name(), Name::name));
        }
        return Status::OK();
    }
};

/** If types are identical, just take reference to column.
  */
template <typename T, typename Name>
struct ConvertImpl<std::enable_if_t<!T::is_parametric, T>, T, Name> {
    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result,
                          size_t /*input_rows_count*/) {
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
        using ColVecTo = ColumnVector<Int64>;

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
                auto& date_value = reinterpret_cast<VecDateTimeValue&>(vec_to[i]);
                if constexpr (IsDecimalNumber<FromFieldType>) {
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
            return Status::RuntimeError(
                    fmt::format("Illegal column {} of first argument of function {}",
                                named_from.column->get_name(), Name::name));
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
        VectorBufferWriter write_buffer(*col_to.get());
        for (size_t i = 0; i < size; ++i) {
            type.to_string(col_from, i, write_buffer);
            write_buffer.commit();
        }

        block.replace_by_position(result, std::move(col_to));
        return Status::OK();
    }
};

template <typename ToDataType, typename Name>
struct ConvertImpl<DataTypeString, ToDataType, Name> {
    template <typename Additions = void*>

    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result,
                          size_t /*input_rows_count*/,
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
struct NameToDate {
    static constexpr auto name = "toDate";
};
struct NameToDateTime {
    static constexpr auto name = "toDateTime";
};

template <typename DataType>
bool try_parse_impl(typename DataType::FieldType& x, ReadBuffer& rb, const DateLUTImpl*) {
    if constexpr (IsDateTimeType<DataType>) {
        return try_read_datetime_text(x, rb);
    }

    if constexpr (IsDateType<DataType>) {
        return try_read_date_text(x, rb);
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

    if constexpr (IsDataTypeDecimal<DataType>) {
        return try_read_decimal_text(x, rb);
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
    static constexpr bool to_decimal = std::is_same_v<Name, NameToDecimal32> ||
                                       std::is_same_v<Name, NameToDecimal64> ||
                                       std::is_same_v<Name, NameToDecimal128>;

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

    bool use_default_implementation_for_constants() const override { return true; }
    ColumnNumbers get_arguments_that_are_always_constant() const override { return {1}; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        return executeInternal(block, arguments, result, input_rows_count);
    }

    bool has_information_about_monotonicity() const override { return Monotonic::has(); }

    Monotonicity get_monotonicity_for_range(const IDataType& type, const Field& left,
                                            const Field& right) const override {
        return Monotonic::get(type, left, right);
    }

private:
    Status executeInternal(Block& block, const ColumnNumbers& arguments, size_t result,
                           size_t input_rows_count) {
        if (!arguments.size()) {
            return Status::RuntimeError(
                    fmt::format("Function {} expects at least 1 arguments", get_name()));
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
                        ret_status = Status::RuntimeError(fmt::format(
                                "Function {} expects 2 arguments for Decimal.", get_name()));
                        return true;
                    }

                    const ColumnWithTypeAndName& scale_column = block.get_by_position(arguments[1]);
                    UInt32 scale = extract_to_decimal_scale(scale_column);

                    ret_status = ConvertImpl<LeftDataType, RightDataType, Name>::execute(
                            block, arguments, result, input_rows_count, scale);
                } else
                    ret_status = ConvertImpl<LeftDataType, RightDataType, Name>::execute(
                            block, arguments, result, input_rows_count);
                return true;
            };

            bool done = call_on_index_and_data_type<ToDataType>(from_type->get_type_id(), call);
            if (!done) {
                ret_status = Status::RuntimeError(fmt::format(
                        "Illegal type {} of argument of function {}",
                        block.get_by_position(arguments[0]).type->get_name(), get_name()));
            }
            return ret_status;
        }
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
using FunctionToString = FunctionConvert<DataTypeString, NameToString, ToStringMonotonicity>;
using FunctionToDecimal32 =
        FunctionConvert<DataTypeDecimal<Decimal32>, NameToDecimal32, UnknownMonotonicity>;
using FunctionToDecimal64 =
        FunctionConvert<DataTypeDecimal<Decimal64>, NameToDecimal64, UnknownMonotonicity>;
using FunctionToDecimal128 =
        FunctionConvert<DataTypeDecimal<Decimal128>, NameToDecimal128, UnknownMonotonicity>;
using FunctionToDate = FunctionConvert<DataTypeDate, NameToDate, UnknownMonotonicity>;
using FunctionToDateTime = FunctionConvert<DataTypeDateTime, NameToDateTime, UnknownMonotonicity>;

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
struct FunctionTo<DataTypeDate> {
    using Type = FunctionToDate;
};
template <>
struct FunctionTo<DataTypeDateTime> {
    using Type = FunctionToDateTime;
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
                        size_t result, size_t input_rows_count) override {
        /// drop second argument, pass others
        ColumnNumbers new_arguments {arguments.front()};
        if (arguments.size() > 2)
            new_arguments.insert(std::end(new_arguments), std::next(std::begin(arguments), 2),
                                 std::end(arguments));

        return wrapper_function(context, block, new_arguments, result, input_rows_count);
    }

    bool use_default_implementation_for_nulls() const override { return false; }
    bool use_default_implementation_for_constants() const override { return true; }
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
    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result,
                          size_t input_rows_count,
                          Additions additions [[maybe_unused]] = Additions()) {
        using ColVecTo = std::conditional_t<IsDecimalNumber<ToFieldType>,
                                            ColumnDecimal<ToFieldType>, ColumnVector<ToFieldType>>;

        const DateLUTImpl* local_time_zone [[maybe_unused]] = nullptr;
        const DateLUTImpl* utc_time_zone [[maybe_unused]] = nullptr;

        const IColumn* col_from = block.get_by_position(arguments[0]).column.get();
        const ColumnString* col_from_string = check_and_get_column<ColumnString>(col_from);

        if (std::is_same_v<FromDataType, DataTypeString> && !col_from_string) {
            return Status::RuntimeError(
                    fmt::format("Illegal column {} of first argument of function {}",
                                col_from->get_name(), Name::name));
        }

        size_t size = input_rows_count;
        typename ColVecTo::MutablePtr col_to = nullptr;

        if constexpr (IsDataTypeDecimal<ToDataType>) {
            col_to = ColVecTo::create(size, 9);
        } else
            col_to = ColVecTo::create(size);

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
                                         ? next_offset - current_offset - 1
                                         : fixed_string_size;

            ReadBuffer read_buffer(&(*chars)[current_offset], string_size);

            (*vec_null_map_to)[i] =
                    !try_parse_impl<ToDataType>(vec_to[i], read_buffer, local_time_zone) ||
                    !is_all_read(read_buffer);

            current_offset = next_offset;
        }

        block.get_by_position(result).column =
                ColumnNullable::create(std::move(col_to), std::move(col_null_map_to));
        return Status::OK();
    }
};

template <typename ToDataType, typename Name>
class FunctionConvertFromString : public IFunction {
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create() { return std::make_shared<FunctionConvertFromString>(); }
    String get_name() const override { return name; }

    bool is_variadic() const override { return true; }
    size_t get_number_of_arguments() const override { return 0; }

    bool use_default_implementation_for_constants() const override { return true; }
    ColumnNumbers get_arguments_that_are_always_constant() const override { return {1}; }

    // This function should not be called for get DateType Ptr
    // using the FunctionCast::get_return_type_impl
    DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) const override {
        DataTypePtr res;
        if constexpr (IsDataTypeDecimal<ToDataType>) {
            res = create_decimal(27, 9);

            if (!res) {
                LOG(FATAL) << "Someting wrong with toDecimalNNOrZero() or toDecimalNNOrNull()";
            }

        } else
            res = std::make_shared<ToDataType>();

        return res;
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        const IDataType* from_type = block.get_by_position(arguments[0]).type.get();

        if (check_and_get_data_type<DataTypeString>(from_type)) {
            return ConvertThroughParsing<DataTypeString, ToDataType, Name>::execute(
                    block, arguments, result, input_rows_count);
        }

        return Status::RuntimeError(fmt::format(
                "Illegal type {} of argument of function {} . Only String or FixedString "
                "argument is accepted for try-conversion function. For other arguments, use "
                "function without 'orZero' or 'orNull'.",
                block.get_by_position(arguments[0]).type->get_name(), get_name()));
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

    bool use_default_implementation_for_constants() const override { return true; }
    ColumnNumbers get_arguments_that_are_always_constant() const override { return {1}; }

    // This function should not be called for get DateType Ptr
    // using the FunctionCast::get_return_type_impl
    DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) const override {
        return std::make_shared<ToDataType>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
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
            return Status::RuntimeError(
                    fmt::format("Illegal type {} of argument of function {}",
                                block.get_by_position(arguments[0]).type->get_name(), get_name()));
        }

        return ret_status;
    }
};

class FunctionCast final : public IFunctionBase {
public:
    using WrapperType =
            std::function<Status(FunctionContext*, Block&, const ColumnNumbers&, size_t, size_t)>;
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
                prepare_unpack_dictionaries(get_argument_types()[0], get_return_type()), name);
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
        } else if (requested_result_is_nullable && IsTimeType<DataType> &&
                   !(check_and_get_data_type<DataTypeDateTime>(from_type.get()) ||
                     check_and_get_data_type<DataTypeDate>(from_type.get()))) {
            function = FunctionConvertToTimeType<DataType, NameCast>::create();
        } else
            function = FunctionTo<DataType>::Type::create();

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
                  which.is_string_or_fixed_string();
        if (!ok) {
            LOG(FATAL) << fmt::format(
                    "Conversion from {} to {} to_type->get_name() is not supported",
                    from_type->get_name(), to_type->get_name());
        }

        if (which.is_string_or_fixed_string()) {
            auto function =
                    FunctionConvertFromString<DataTypeDecimal<FieldType>, NameCast>::create();

            /// Check conversion using underlying function
            { function->get_return_type(ColumnsWithTypeAndName(1, {nullptr, from_type, ""})); }

            return [function](FunctionContext* context, Block& block,
                              const ColumnNumbers& arguments, const size_t result,
                              size_t input_rows_count) {
                return function->execute(context, block, arguments, result, input_rows_count);
            };
        }

        return [type_index, precision, scale](FunctionContext* context, Block& block,
                                              const ColumnNumbers& arguments, const size_t result,
                                              size_t input_rows_count) {
            auto res = call_on_index_and_data_type<ToDataType>(
                    type_index, [&](const auto& types) -> bool {
                        using Types = std::decay_t<decltype(types)>;
                        using LeftDataType = typename Types::LeftType;
                        using RightDataType = typename Types::RightType;

                        ConvertImpl<LeftDataType, RightDataType, NameCast>::execute(
                                block, arguments, result, input_rows_count, scale);
                        return true;
                    });

            /// Additionally check if call_on_index_and_data_type wasn't called at all.
            if (!res) {
                auto to = DataTypeDecimal<FieldType>(precision, scale);
                return Status::RuntimeError(fmt::format("Conversion from {} to {} is not supported",
                                                        getTypeName(type_index), to.get_name()));
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

    WrapperType prepare_unpack_dictionaries(const DataTypePtr& from_type,
                                            const DataTypePtr& to_type) const {
        const auto& from_nested = from_type;
        const auto& to_nested = to_type;

        if (from_type->only_null()) {
            if (!to_nested->is_nullable()) {
                LOG(FATAL) << "Cannot convert NULL to a non-nullable type";
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

        auto wrapper = prepare_remove_nullable(from_nested, to_nested, skip_not_null_check);

        return wrapper;
    }

    WrapperType prepare_remove_nullable(const DataTypePtr& from_type, const DataTypePtr& to_type,
                                        bool skip_not_null_check) const {
        /// Determine whether pre-processing and/or post-processing must take place during conversion.

        bool source_is_nullable = from_type->is_nullable();
        bool result_is_nullable = to_type->is_nullable();

        auto wrapper = prepare_impl(remove_nullable(from_type), remove_nullable(to_type),
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
                if (source_is_nullable)
                    tmp_block = create_block_with_nested_columns(block, arguments);
                else
                    tmp_block = block;

                size_t tmp_res_index = block.columns();
                tmp_block.insert({nullptr, nested_type, ""});

                /// Perform the requested conversion.
                wrapper(context, tmp_block, arguments, tmp_res_index, input_rows_count);

                const auto& tmp_res = tmp_block.get_by_position(tmp_res_index);

                /// May happen in fuzzy tests. For debug purpose.
                if (!tmp_res.column.get()) {
                    return Status::RuntimeError(fmt::format(
                            "Couldn't convert {} to {} in prepare_remove_nullable wrapper.",
                            block.get_by_position(arguments[0]).type->get_name(),
                            nested_type->get_name()));
                }

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
                Block tmp_block = create_block_with_nested_columns(block, arguments, result);

                /// Check that all values are not-NULL.
                /// Check can be skipped in case if LowCardinality dictionary is transformed.
                /// In that case, correctness will be checked beforehand.
                if (!skip_not_null_check) {
                    const auto& col = block.get_by_position(arguments[0]).column;
                    const auto& nullable_col = assert_cast<const ColumnNullable&>(*col);
                    const auto& null_map = nullable_col.get_null_map_data();

                    if (!memory_is_zero(null_map.data(), null_map.size())) {
                        return Status::RuntimeError(
                                fmt::format("Cannot convert NULL value to non-Nullable type"));
                    }
                }

                wrapper(context, tmp_block, arguments, result, input_rows_count);
                block.get_by_position(result).column = tmp_block.get_by_position(result).column;
                return Status::OK();
            };
        } else
            return wrapper;
    }

    /// 'from_type' and 'to_type' are nested types in case of Nullable.
    /// 'requested_result_is_nullable' is true if CAST to Nullable type is requested.
    WrapperType prepare_impl(const DataTypePtr& from_type, const DataTypePtr& to_type,
                             bool requested_result_is_nullable) const {
        if (from_type->equals(*to_type))
            return create_identity_wrapper(from_type);
        else if (WhichDataType(from_type).is_nothing())
            return create_nothing_wrapper(to_type.get());

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
                          std::is_same_v<ToDataType, DataTypeDateTime>) {
                ret = create_wrapper(from_type, check_and_get_data_type<ToDataType>(to_type.get()),
                                     requested_result_is_nullable);
                return true;
            }

            if constexpr (std::is_same_v<ToDataType, DataTypeDecimal<Decimal32>> ||
                          std::is_same_v<ToDataType, DataTypeDecimal<Decimal64>> ||
                          std::is_same_v<ToDataType, DataTypeDecimal<Decimal128>>) {
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

        default:
            break;
        }

        LOG(FATAL) << fmt::format("Conversion from {} to {} is not supported",
                                  from_type->get_name(), to_type->get_name());
        return WrapperType {};
    }
};

class FunctionBuilderCast : public FunctionBuilderImpl {
public:
    using MonotonicityForRange = FunctionCast::MonotonicityForRange;

    static constexpr auto name = "CAST";
    static FunctionBuilderPtr create() { return std::make_shared<FunctionBuilderCast>(); }

    FunctionBuilderCast() {}

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
        const auto type_col =
                check_and_get_column_const<ColumnString>(arguments.back().column.get());
        if (!type_col) {
            LOG(FATAL) << fmt::format(
                    "Second argument to {} must be a constant string describing type", get_name());
        }

        auto type = DataTypeFactory::instance().get(type_col->get_value<String>());

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
        if (need_to_be_nullable) {
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
