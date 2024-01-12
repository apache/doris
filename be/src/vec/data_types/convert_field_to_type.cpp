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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Interpreters/convert_field_to_type.cpp
// and modified by Doris

#include "vec/data_types/convert_field_to_type.h"

#include <fmt/format.h>
#include <gen_cpp/Opcodes_types.h>
#include <glog/logging.h>
#include <stddef.h>

#include <memory>
#include <ostream>
#include <string>
#include <type_traits>
#include <vector>

#include "common/exception.h"
#include "common/status.h"
#include "util/bitmap_value.h"
#include "util/jsonb_writer.h"
#include "vec/common/arithmetic_overflow.h"
#include "vec/common/field_visitors.h"
#include "vec/common/typeid_cast.h"
#include "vec/core/accurate_comparison.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_nullable.h"

namespace doris {
namespace vectorized {
struct UInt128;
} // namespace vectorized
} // namespace doris

// #include "vec/data_types/data_type_tuple.h"
namespace doris::vectorized {
/** Checking for a `Field from` of `From` type falls to a range of values of type `To`.
  * `From` and `To` - numeric types. They can be floating-point types.
  * `From` is one of UInt64, Int64, Float64,
  *  whereas `To` can also be 8, 16, 32 bit.
  *
  * If falls into a range, then `from` is converted to the `Field` closest to the `To` type.
  * If not, return Field(Null).
  */

/** simple types of implementation of visitor to string*/
// TODO support more types
class FieldVisitorToStringSimple : public StaticVisitor<String> {
public:
    String operator()(const Null& x) const { return "NULL"; }
    String operator()(const UInt64& x) const { return std::to_string(x); }
    String operator()(const Int64& x) const { return std::to_string(x); }
    String operator()(const Float64& x) const { return std::to_string(x); }
    String operator()(const String& x) const { return x; }
    [[noreturn]] String operator()(const UInt128& x) const {
        LOG(FATAL) << "not implemeted";
        __builtin_unreachable();
    }
    [[noreturn]] String operator()(const Int128& x) const {
        LOG(FATAL) << "not implemeted";
        __builtin_unreachable();
    }
    [[noreturn]] String operator()(const Array& x) const {
        LOG(FATAL) << "not implemeted";
        __builtin_unreachable();
    }
    [[noreturn]] String operator()(const Tuple& x) const {
        LOG(FATAL) << "not implemeted";
        __builtin_unreachable();
    }
    [[noreturn]] String operator()(const DecimalField<Decimal32>& x) const {
        LOG(FATAL) << "not implemeted";
        __builtin_unreachable();
    }
    [[noreturn]] String operator()(const DecimalField<Decimal64>& x) const {
        LOG(FATAL) << "not implemeted";
        __builtin_unreachable();
    }
    [[noreturn]] String operator()(const DecimalField<Decimal128V2>& x) const {
        LOG(FATAL) << "not implemeted";
        __builtin_unreachable();
    }
    [[noreturn]] String operator()(const DecimalField<Decimal128V3>& x) const {
        LOG(FATAL) << "not implemeted";
        __builtin_unreachable();
    }
    [[noreturn]] String operator()(const DecimalField<Decimal256>& x) const {
        LOG(FATAL) << "not implemeted";
        __builtin_unreachable();
    }
    [[noreturn]] String operator()(const JsonbField& x) const {
        LOG(FATAL) << "not implemeted";
        __builtin_unreachable();
    }
};

class FieldVisitorToJsonb : public StaticVisitor<void> {
public:
    void operator()(const Null& x, JsonbWriter* writer) const { writer->writeNull(); }
    void operator()(const UInt64& x, JsonbWriter* writer) const { writer->writeInt64(x); }
    void operator()(const UInt128& x, JsonbWriter* writer) const {
        writer->writeInt128(int128_t(x));
    }
    void operator()(const Int128& x, JsonbWriter* writer) const {
        writer->writeInt128(int128_t(x));
    }
    void operator()(const Int64& x, JsonbWriter* writer) const { writer->writeInt64(x); }
    void operator()(const Float64& x, JsonbWriter* writer) const { writer->writeDouble(x); }
    void operator()(const String& x, JsonbWriter* writer) const {
        writer->writeStartString();
        writer->writeString(x);
        writer->writeEndString();
    }
    void operator()(const Array& x, JsonbWriter* writer) const;

    void operator()(const Tuple& x, JsonbWriter* writer) const {
        throw doris::Exception(doris::ErrorCode::NOT_IMPLEMENTED_ERROR, "Not implemeted");
    }
    void operator()(const DecimalField<Decimal32>& x, JsonbWriter* writer) const {
        throw doris::Exception(doris::ErrorCode::NOT_IMPLEMENTED_ERROR, "Not implemeted");
    }
    void operator()(const DecimalField<Decimal64>& x, JsonbWriter* writer) const {
        throw doris::Exception(doris::ErrorCode::NOT_IMPLEMENTED_ERROR, "Not implemeted");
    }
    void operator()(const DecimalField<Decimal128V2>& x, JsonbWriter* writer) const {
        throw doris::Exception(doris::ErrorCode::NOT_IMPLEMENTED_ERROR, "Not implemeted");
    }
    void operator()(const DecimalField<Decimal128V3>& x, JsonbWriter* writer) const {
        throw doris::Exception(doris::ErrorCode::NOT_IMPLEMENTED_ERROR, "Not implemeted");
    }
    void operator()(const DecimalField<Decimal256>& x, JsonbWriter* writer) const {
        throw doris::Exception(doris::ErrorCode::NOT_IMPLEMENTED_ERROR, "Not implemeted");
    }
    void operator()(const doris::QuantileState& x, JsonbWriter* writer) const {
        throw doris::Exception(doris::ErrorCode::NOT_IMPLEMENTED_ERROR, "Not implemeted");
    }
    void operator()(const HyperLogLog& x, JsonbWriter* writer) const {
        throw doris::Exception(doris::ErrorCode::NOT_IMPLEMENTED_ERROR, "Not implemeted");
    }
    void operator()(const BitmapValue& x, JsonbWriter* writer) const {
        throw doris::Exception(doris::ErrorCode::NOT_IMPLEMENTED_ERROR, "Not implemeted");
    }
    void operator()(const VariantMap& x, JsonbWriter* writer) const {
        throw doris::Exception(doris::ErrorCode::NOT_IMPLEMENTED_ERROR, "Not implemeted");
    }
    void operator()(const Map& x, JsonbWriter* writer) const {
        throw doris::Exception(doris::ErrorCode::NOT_IMPLEMENTED_ERROR, "Not implemeted");
    }
    void operator()(const JsonbField& x, JsonbWriter* writer) const {
        throw doris::Exception(doris::ErrorCode::NOT_IMPLEMENTED_ERROR, "Not implemeted");
    }
};

void FieldVisitorToJsonb::operator()(const Array& x, JsonbWriter* writer) const {
    const size_t size = x.size();
    writer->writeStartArray();
    for (size_t i = 0; i < size; ++i) {
        Field::dispatch([writer](const auto& value) { FieldVisitorToJsonb()(value, writer); },
                        x[i]);
    }
    writer->writeEndArray();
}

namespace {
template <typename From, typename To>
Field convert_numeric_type_impl(const Field& from) {
    To result;
    if (!accurate::convertNumeric(from.get<From>(), result)) {
        return {};
    }
    return result;
}

template <typename To>
void convert_numric_type(const Field& from, const IDataType& type, Field* to) {
    if (from.get_type() == Field::Types::UInt64) {
        *to = convert_numeric_type_impl<UInt64, To>(from);
    } else if (from.get_type() == Field::Types::Int64) {
        *to = convert_numeric_type_impl<Int64, To>(from);
    } else if (from.get_type() == Field::Types::Float64) {
        *to = convert_numeric_type_impl<Float64, To>(from);
    } else if (from.get_type() == Field::Types::UInt128) {
        // *to = convert_numeric_type_impl<UInt128, To>(from);
    } else if (from.get_type() == Field::Types::Int128) {
        *to = convert_numeric_type_impl<Int128, To>(from);
    } else {
        throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                               "Type mismatch in IN or VALUES section. Expected: {}. Got: {}",
                               type.get_name(), from.get_type());
    }
}

template <typename FromDataType, typename ToDataType, typename ReturnType>
    requires(std::is_arithmetic_v<typename FromDataType::FieldType> &&
             IsDataTypeDecimal<ToDataType>)
inline ReturnType convert_to_decimal_impl(const typename FromDataType::FieldType& value,
                                          UInt32 scale, typename ToDataType::FieldType& result) {
    using FromFieldType = typename FromDataType::FieldType;
    using ToFieldType = typename ToDataType::FieldType;
    using ToNativeType = typename ToFieldType::NativeType;
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;
    if constexpr (std::is_floating_point_v<FromFieldType>) {
        if (!std::isfinite(value)) {
            if constexpr (throw_exception)
                throw Exception(ErrorCode::ARITHMETIC_OVERFLOW_ERRROR,
                                "convert overflow. Cannot convert infinity or NaN to decimal");
            else
                return ReturnType(false);
        }
        auto out = value * static_cast<FromFieldType>(ToDataType::get_scale_multiplier(scale));
        if (out <= static_cast<FromFieldType>(std::numeric_limits<ToNativeType>::min()) ||
            out >= static_cast<FromFieldType>(std::numeric_limits<ToNativeType>::max())) {
            if constexpr (throw_exception)
                throw Exception(ErrorCode::ARITHMETIC_OVERFLOW_ERRROR,
                                "{} convert overflow. Float is out of Decimal range");
            else
                return ReturnType(false);
        }
        result = static_cast<ToNativeType>(out);
        return ReturnType(true);
    } else {
        if constexpr (std::is_same_v<FromFieldType, UInt64>)
            return ReturnType(
                    convert_decimals_impl<DataTypeDecimal<Decimal128V3>, ToDataType, ReturnType>(
                            static_cast<Int128>(value), 0, scale, result));
        else
            return ReturnType(
                    convert_decimals_impl<DataTypeDecimal<Decimal64>, ToDataType, ReturnType>(
                            static_cast<Int64>(value), 0, scale, result));
    }
}

template <typename FromDataType, typename ToDataType>
    requires(std::is_arithmetic_v<typename FromDataType::FieldType> &&
             IsDataTypeDecimal<ToDataType>)
inline typename ToDataType::FieldType convert_to_decimal(
        const typename FromDataType::FieldType& value, UInt32 scale) {
    typename ToDataType::FieldType result;
    convert_to_decimal_impl<FromDataType, ToDataType, void>(value, scale, result);
    return result;
}

template <typename FromDataType, typename ToDataType, typename ReturnType = void>
    requires(IsDataTypeDecimal<FromDataType> && IsDataTypeDecimal<ToDataType>)
inline ReturnType convert_decimals_impl(const typename FromDataType::FieldType& value,
                                        UInt32 scale_from, UInt32 scale_to,
                                        typename ToDataType::FieldType& result) {
    using FromFieldType = typename FromDataType::FieldType;
    using ToFieldType = typename ToDataType::FieldType;
    using MaxFieldType = std::conditional_t<(sizeof(FromFieldType) > sizeof(ToFieldType)),
                                            FromFieldType, ToFieldType>;
    using MaxNativeType = typename MaxFieldType::NativeType;
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;
    MaxNativeType converted_value;
    if (scale_to > scale_from) {
        MaxFieldType multiplier =
                DataTypeDecimal<MaxFieldType>::get_scale_multiplier(scale_to - scale_from);
        MaxFieldType tmp;
        if (common::mul_overflow(static_cast<MaxNativeType>(value.value), multiplier.value,
                                 tmp.value)) {
            if constexpr (throw_exception)
                throw Exception(ErrorCode::ARITHMETIC_OVERFLOW_ERRROR,
                                "convert overflow while multiplying {} by scale {}", value.value,
                                converted_value);
            else
                return ReturnType(false);
        }
        converted_value = tmp.value;
    } else if (scale_to == scale_from) {
        converted_value = value.value;
    } else {
        converted_value = value.value / DataTypeDecimal<MaxFieldType>::get_scale_multiplier(
                                                scale_from - scale_to);
    }
    if constexpr (sizeof(FromFieldType) > sizeof(ToFieldType)) {
        if (converted_value < std::numeric_limits<typename ToFieldType::NativeType>::min() ||
            converted_value > std::numeric_limits<typename ToFieldType::NativeType>::max()) {
            if constexpr (throw_exception)
                throw Exception(ErrorCode::ARITHMETIC_OVERFLOW_ERRROR,
                                "convert overflow: {} is not in range ({}, {})", converted_value,
                                std::numeric_limits<typename ToFieldType::NativeType>::min(),
                                std::numeric_limits<typename ToFieldType::NativeType>::max());
            else
                return ReturnType(false);
        }
    }
    result = static_cast<typename ToFieldType::NativeType>(converted_value);
    return ReturnType(true);
}
template <typename FromDataType, typename ToDataType>
    requires(IsDataTypeDecimal<FromDataType> && IsDataTypeDecimal<ToDataType>)
inline typename ToDataType::FieldType convert_decimals(
        const typename FromDataType::FieldType& value, UInt32 scale_from, UInt32 scale_to) {
    using ToFieldType = typename ToDataType::FieldType;
    ToFieldType result;
    convert_decimals_impl<FromDataType, ToDataType, void>(value, scale_from, scale_to, result);
    return result;
}

template <typename From, typename T>
Field convert_int_to_decimal_type(const Field& from, const DataTypeDecimal<T>& type) {
    From value = from.get<From>();
    if (!type.can_store_whole(value))
        throw doris::Exception(ErrorCode::INVALID_ARGUMENT, "Number is too big to place in {}",
                               type.get_name());
    T scaled_value = type.get_scale_multiplier() * T(static_cast<typename T::NativeType>(value));
    return DecimalField<T>(scaled_value, type.get_scale());
}

template <typename T>
Field convertStringToDecimalType(const Field& from, const DataTypeDecimal<T>& type) {
    const String& str_value = from.get<String>();
    T value;
    type.parse_from_string(str_value, &value);
    return DecimalField<T>(value, type.get_scale());
}

template <typename From, typename T>
Field convert_decimal_to_decimal_type(const Field& from, const DataTypeDecimal<T>& type) {
    auto field = from.get<DecimalField<From>>();
    T value;
    convert_decimals<DataTypeDecimal<From>, DataTypeDecimal<T>>(
            field.get_value(), field.get_scale(), type.get_scale());
    return DecimalField<T>(value, type.get_scale());
}
template <typename From, typename T>
Field convert_float_to_decimal_type(const Field& from, const DataTypeDecimal<T>& type) {
    From value = from.get<From>();
    if (!type.can_store_whole(value))
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Number is too big to place in {}",
                        type.get_name());
    //String sValue = convertFieldToString(from);
    //int fromScale = sValue.length()- sValue.find('.') - 1;
    UInt32 scale = type.get_scale();
    auto scaled_value = convert_to_decimal<DataTypeNumber<From>, DataTypeDecimal<T>>(value, scale);
    return DecimalField<T>(scaled_value, scale);
}

template <typename To>
Field convert_decimal_type(const Field& from, const To& type) {
    if (from.get_type() == Field::Types::UInt64)
        return convert_int_to_decimal_type<UInt64>(from, type);
    if (from.get_type() == Field::Types::Int64)
        return convert_int_to_decimal_type<Int64>(from, type);
    if (from.get_type() == Field::Types::Int128)
        return convert_int_to_decimal_type<Int128>(from, type);
    if (from.get_type() == Field::Types::String) return convertStringToDecimalType(from, type);
    if (from.get_type() == Field::Types::Decimal32)
        return convert_decimal_to_decimal_type<Decimal32>(from, type);
    if (from.get_type() == Field::Types::Decimal64)
        return convert_decimal_to_decimal_type<Decimal64>(from, type);
    if (from.get_type() == Field::Types::Decimal128V2)
        return convert_decimal_to_decimal_type<Decimal128V2>(from, type);
    if (from.get_type() == Field::Types::Decimal128V3)
        return convert_decimal_to_decimal_type<Decimal128V3>(from, type);
    if (from.get_type() == Field::Types::Float64)
        return convert_float_to_decimal_type<Float64>(from, type);
    throw Exception(ErrorCode::INVALID_ARGUMENT,
                    "Type mismatch in IN or VALUES section. Expected: {}. Got: {}", type.get_name(),
                    from.get_type());
}

void convert_field_to_typeImpl(const Field& src, const IDataType& type,
                               const IDataType* from_type_hint, Field* to) {
    if (from_type_hint && from_type_hint->equals(type)) {
        *to = src;
        return;
    }
    WhichDataType which_type(type);
    // TODO add more types
    if (type.is_value_represented_by_number() && src.get_type() != Field::Types::String) {
        if (which_type.is_uint8()) {
            return convert_numric_type<UInt8>(src, type, to);
        }
        if (which_type.is_uint16()) {
            return convert_numric_type<UInt16>(src, type, to);
        }
        if (which_type.is_uint32()) {
            return convert_numric_type<UInt32>(src, type, to);
        }
        if (which_type.is_uint64()) {
            return convert_numric_type<UInt64>(src, type, to);
        }
        if (which_type.is_uint128()) {
            // return convert_numric_type<UInt128>(src, type, to);
        }
        if (which_type.is_int8()) {
            return convert_numric_type<Int8>(src, type, to);
        }
        if (which_type.is_int16()) {
            return convert_numric_type<Int16>(src, type, to);
        }
        if (which_type.is_int32()) {
            return convert_numric_type<Int32>(src, type, to);
        }
        if (which_type.is_int64()) {
            return convert_numric_type<Int64>(src, type, to);
        }
        if (which_type.is_int128()) {
            return convert_numric_type<Int128>(src, type, to);
        }
        if (which_type.is_float32()) {
            return convert_numric_type<Float32>(src, type, to);
        }
        if (which_type.is_float64()) {
            return convert_numric_type<Float64>(src, type, to);
        }
        if (const auto* ptype = typeid_cast<const DataTypeDecimal<Decimal32>*>(&type)) {
            *to = convert_decimal_type(src, *ptype);
            return;
        }
        if (const auto* ptype = typeid_cast<const DataTypeDecimal<Decimal64>*>(&type)) {
            *to = convert_decimal_type(src, *ptype);
            return;
        }
        if (const auto* ptype = typeid_cast<const DataTypeDecimal<Decimal128V3>*>(&type)) {
            *to = convert_decimal_type(src, *ptype);
            return;
        }
        if ((which_type.is_date() || which_type.is_date_time()) &&
            src.get_type() == Field::Types::UInt64) {
            /// We don't need any conversion UInt64 is under type of Date and DateTime
            *to = src;
            return;
        }
    } else if (which_type.is_string_or_fixed_string()) {
        if (src.get_type() == Field::Types::String) {
            *to = src;
            return;
        }
        // TODO this is a very simple translator, support more complex types
        *to = apply_visitor(FieldVisitorToStringSimple(), src);
        return;
    } else if (which_type.is_json()) {
        if (src.get_type() == Field::Types::JSONB) {
            *to = src;
            return;
        }
        JsonbWriter writer;
        Field::dispatch([&writer](const auto& value) { FieldVisitorToJsonb()(value, &writer); },
                        src);
        *to = JsonbField(writer.getOutput()->getBuffer(), writer.getOutput()->getSize());
        return;
    } else if (const DataTypeArray* type_array = typeid_cast<const DataTypeArray*>(&type)) {
        if (src.get_type() == Field::Types::Array) {
            const Array& src_arr = src.get<Array>();
            size_t src_arr_size = src_arr.size();
            const auto& element_type = *(type_array->get_nested_type());
            Array res(src_arr_size);
            for (size_t i = 0; i < src_arr_size; ++i) {
                convert_field_to_type(src_arr[i], element_type, &res[i]);
                if (res[i].is_null() && !element_type.is_nullable()) {
                    throw doris::Exception(ErrorCode::INVALID_ARGUMENT, "Cannot convert NULL to {}",
                                           element_type.get_name());
                }
            }
            *to = Field(res);
            return;
        }
    }
    throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                           "Type mismatch in IN or VALUES section. Expected: {}. Got: {}",
                           type.get_name(), src.get_type());
}
} // namespace
void convert_field_to_type(const Field& from_value, const IDataType& to_type, Field* to,
                           const IDataType* from_type_hint) {
    if (from_value.is_null()) {
        *to = from_value;
        return;
    }
    if (from_type_hint && from_type_hint->equals(to_type)) {
        *to = from_value;
        return;
    }
    if (const auto* nullable_type = typeid_cast<const DataTypeNullable*>(&to_type)) {
        const IDataType& nested_type = *nullable_type->get_nested_type();
        /// NULL remains NULL after any conversion.
        if (WhichDataType(nested_type).is_nothing()) {
            *to = {};
            return;
        }
        if (from_type_hint && from_type_hint->equals(nested_type)) {
            *to = from_value;
            return;
        }
        return convert_field_to_typeImpl(from_value, nested_type, from_type_hint, to);
    } else {
        return convert_field_to_typeImpl(from_value, to_type, from_type_hint, to);
    }
}
} // namespace doris::vectorized