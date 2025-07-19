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

#include "cast_base.h"
#include "vec/data_types/data_type_decimal.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"
template <typename T>
constexpr static bool type_allow_cast_to_decimal =
        std::is_same_v<T, DataTypeString> || IsDataTypeNumber<T> || IsDataTypeDecimal<T>;

// Casting from string to decimal types.
template <CastModeType Mode, typename ToDataType>
    requires(IsDataTypeDecimal<ToDataType>)
class CastToImpl<Mode, DataTypeString, ToDataType> : public CastToBase {
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count,
                        const NullMap::value_type* null_map = nullptr) const override {
        const auto* col_from = check_and_get_column<DataTypeString::ColumnType>(
                block.get_by_position(arguments[0]).column.get());

        auto to_type = block.get_by_position(result).type;
        auto serde = remove_nullable(to_type)->get_serde();
        MutableColumnPtr column_to;

        if constexpr (Mode == CastModeType::NonStrictMode) {
            auto to_nullable_type = make_nullable(to_type);
            column_to = to_nullable_type->create_column();
            auto& nullable_col_to = assert_cast<ColumnNullable&>(*column_to);
            RETURN_IF_ERROR(serde->from_string_batch(*col_from, nullable_col_to, {}));
        } else if constexpr (Mode == CastModeType::StrictMode) {
            if (to_type->is_nullable()) {
                return Status::InternalError(
                        "result type should be not nullable when casting string to decimal in "
                        "strict cast mode");
            }
            column_to = to_type->create_column();
            RETURN_IF_ERROR(
                    serde->from_string_strict_mode_batch(*col_from, *column_to, {}, null_map));
        } else {
            return Status::InternalError("Unsupported cast mode");
        }

        block.get_by_position(result).column = std::move(column_to);
        return Status::OK();
    }
};

// cast decimalv3 types to decimalv2 types
template <CastModeType CastMode, typename FromDataType, typename ToDataType>
    requires(IsDataTypeDecimalV3<ToDataType> && IsDataTypeDecimalV2<ToDataType>)
class CastToImpl<CastMode, FromDataType, ToDataType> : public CastToBase {
public:
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count,
                        const NullMap::value_type* null_map = nullptr) const override {
        return Status::RuntimeError(
                "not support {} ",
                cast_mode_type_to_string(CastMode, block.get_by_position(arguments[0]).type,
                                         block.get_by_position(result).type));
    }
};

// cast decimalv2 types to decimalv3 types
template <CastModeType CastMode, typename FromDataType, typename ToDataType>
    requires(IsDataTypeDecimalV2<FromDataType> && IsDataTypeDecimalV3<ToDataType>)
class CastToImpl<CastMode, FromDataType, ToDataType> : public CastToBase {
public:
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count,
                        const NullMap::value_type* null_map = nullptr) const override {
        using ToFieldType = typename ToDataType::FieldType;
        const ColumnWithTypeAndName& named_from = block.get_by_position(arguments[0]);
        const auto* col_from =
                check_and_get_column<typename FromDataType::ColumnType>(named_from.column.get());
        if (!col_from) {
            return Status::RuntimeError("Illegal column {} of first argument of function cast",
                                        named_from.column->get_name());
        }

        const auto& from_decimal_type = assert_cast<const FromDataType&>(*named_from.type);
        UInt32 from_precision = from_decimal_type.get_precision();
        UInt32 from_scale = from_decimal_type.get_scale();
        UInt32 from_original_precision = from_decimal_type.get_original_precision();
        UInt32 from_original_scale = from_decimal_type.get_original_scale();
        UInt32 to_max_digits = NumberTraits::max_ascii_len<typename ToFieldType::NativeType>();

        const ColumnWithTypeAndName& named_to = block.get_by_position(result);
        const auto& to_decimal_type = assert_cast<const ToDataType&>(*named_to.type);
        UInt32 to_precision = to_decimal_type.get_precision();
        ToDataType::check_type_precision(to_precision);
        UInt32 to_scale = to_decimal_type.get_scale();
        ToDataType::check_type_scale(to_scale);

        auto from_max_int_digit_count = from_original_precision - from_original_scale;
        auto to_max_int_digit_count = to_precision - to_scale;
        bool narrow_integral = (to_max_int_digit_count < from_max_int_digit_count) ||
                               (to_max_int_digit_count == from_max_int_digit_count &&
                                to_scale < from_original_scale);
        bool multiply_may_overflow = false;
        if (to_scale > from_scale) {
            multiply_may_overflow = (from_precision + to_scale - from_scale) >= to_max_digits;
        }
        bool result_is_nullable = (CastMode == CastModeType::NonStrictMode) && narrow_integral;

        ColumnUInt8::MutablePtr col_null_map_to;
        NullMap::value_type* null_map_data = nullptr;
        if (result_is_nullable) {
            col_null_map_to = ColumnUInt8::create(input_rows_count, 0);
            null_map_data = col_null_map_to->get_data().data();
        }

        auto col_to = ToDataType::ColumnType::create(input_rows_count, to_scale);
        const auto& vec_from = col_from->get_data();
        auto& vec_to = col_to->get_data();

        size_t size = vec_from.size();
        std::visit(
                [&](auto multiply_may_overflow, auto narrow_integral, auto result_is_nullable) {
                    convert_decimal_cols<FromDataType, ToDataType, multiply_may_overflow,
                                         narrow_integral, result_is_nullable>(
                            vec_from.data(), vec_to.data(), from_precision, from_scale,
                            to_precision, vec_to.get_scale(), size, null_map_data);
                },
                make_bool_variant(multiply_may_overflow), make_bool_variant(narrow_integral),
                make_bool_variant(result_is_nullable));

        if (result_is_nullable) {
            block.get_by_position(result).column =
                    ColumnNullable::create(std::move(col_to), std::move(col_null_map_to));
        } else {
            block.get_by_position(result).column = std::move(col_to);
        }
        return Status::OK();
    }
};

// cast bool, integer, float, double and decimalv3 types to decimal types
template <CastModeType CastMode, typename FromDataType, typename ToDataType>
    requires((IsDataTypeDecimalV3<FromDataType> || IsDataTypeNumber<FromDataType>) &&
             IsDataTypeDecimal<ToDataType>)
class CastToImpl<CastMode, FromDataType, ToDataType> : public CastToBase {
public:
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count,
                        const NullMap::value_type* null_map = nullptr) const override {
        using FromFieldType = typename FromDataType::FieldType;
        using ToFieldType = typename ToDataType::FieldType;
        const ColumnWithTypeAndName& named_from = block.get_by_position(arguments[0]);
        const auto* col_from =
                check_and_get_column<typename FromDataType::ColumnType>(named_from.column.get());
        if (!col_from) {
            return Status::RuntimeError("Illegal column {} of first argument of function cast",
                                        named_from.column->get_name());
        }

        UInt32 from_precision = NumberTraits::max_ascii_len<FromFieldType>();
        UInt32 from_scale = 0;

        if constexpr (IsDataTypeDecimal<FromDataType>) {
            const auto& from_decimal_type = assert_cast<const FromDataType&>(*named_from.type);
            from_precision = from_decimal_type.get_precision();
            from_scale = from_decimal_type.get_scale();
        }
        UInt32 to_max_digits = NumberTraits::max_ascii_len<typename ToFieldType::NativeType>();

        const ColumnWithTypeAndName& named_to = block.get_by_position(result);
        const auto& to_decimal_type = assert_cast<const ToDataType&>(*named_to.type);
        UInt32 to_precision = to_decimal_type.get_precision();
        ToDataType::check_type_precision(to_precision);
        UInt32 to_scale = to_decimal_type.get_scale();
        ToDataType::check_type_scale(to_scale);

        ToFieldType max_result = ToDataType::get_max_digits_number(to_precision);
        ToFieldType min_result = -max_result;

        auto from_max_int_digit_count = from_precision - from_scale;
        auto to_max_int_digit_count = to_precision - to_scale;
        bool narrow_integral =
                (to_max_int_digit_count < from_max_int_digit_count) ||
                (to_max_int_digit_count == from_max_int_digit_count && to_scale < from_scale);
        bool multiply_may_overflow = false;
        if (to_scale > from_scale || IsDataTypeInt<FromDataType>) {
            multiply_may_overflow = (from_precision + to_scale - from_scale) >= to_max_digits;
        }
        bool result_is_nullable = (CastMode == CastModeType::NonStrictMode) && narrow_integral;

        ColumnUInt8::MutablePtr col_null_map_to;
        NullMap::value_type* null_map_data = nullptr;
        if (result_is_nullable) {
            col_null_map_to = ColumnUInt8::create(input_rows_count, 0);
            null_map_data = col_null_map_to->get_data().data();
        }

        auto col_to = ToDataType::ColumnType::create(input_rows_count, to_scale);
        const auto& vec_from = col_from->get_data();
        auto& vec_to = col_to->get_data();

        size_t size = vec_from.size();
        std::visit(
                [&](auto multiply_may_overflow, auto narrow_integral, auto result_is_nullable) {
                    if constexpr (IsDataTypeDecimal<FromDataType>) {
                        convert_decimal_cols<FromDataType, ToDataType, multiply_may_overflow,
                                             narrow_integral, result_is_nullable>(
                                vec_from.data(), vec_to.data(), from_precision, from_scale,
                                to_precision, vec_to.get_scale(), size, null_map_data);
                    } else {
                        convert_to_decimal<FromDataType, ToDataType, multiply_may_overflow,
                                           narrow_integral, result_is_nullable>(
                                vec_to.data(), vec_from.data(), from_scale, to_precision, to_scale,
                                min_result, max_result, size, null_map_data);
                    }
                },
                make_bool_variant(multiply_may_overflow), make_bool_variant(narrow_integral),
                make_bool_variant(result_is_nullable));

        if (result_is_nullable) {
            block.get_by_position(result).column =
                    ColumnNullable::create(std::move(col_to), std::move(col_null_map_to));
        } else {
            block.get_by_position(result).column = std::move(col_to);
        }
        return Status::OK();
    }
};

namespace CastWrapper {

template <typename ToDataType>
WrapperType create_decimal_wrapper(FunctionContext* context, const DataTypePtr& from_type) {
    std::shared_ptr<CastToBase> cast_impl;

    auto make_cast_wrapper = [&](const auto& types) -> bool {
        using Types = std::decay_t<decltype(types)>;
        using FromDataType = typename Types::LeftType;
        if constexpr (type_allow_cast_to_decimal<FromDataType>) {
            if (context->enable_strict_mode()) {
                cast_impl = std::make_shared<
                        CastToImpl<CastModeType::StrictMode, FromDataType, ToDataType>>();
            } else {
                cast_impl = std::make_shared<
                        CastToImpl<CastModeType::NonStrictMode, FromDataType, ToDataType>>();
            }
            return true;
        } else {
            return false;
        }
    };

    if (!call_on_index_and_data_type<void>(from_type->get_primitive_type(), make_cast_wrapper)) {
        return create_unsupport_wrapper(
                fmt::format("CAST AS decimal not supported {}", from_type->get_name()));
    }

    return [cast_impl](FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                       uint32_t result, size_t input_rows_count,
                       const NullMap::value_type* null_map = nullptr) {
        return cast_impl->execute_impl(context, block, arguments, result, input_rows_count,
                                       null_map);
    };
}
} // namespace CastWrapper
#include "common/compile_check_end.h"
} // namespace doris::vectorized