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
#include <glog/logging.h>
#include <stddef.h>

#include <memory>
#include <ostream>
#include <string>
#include <type_traits>
#include <vector>

#include "common/cast_set.h"
#include "common/exception.h"
#include "common/status.h"
#include "util/bitmap_value.h"
#include "util/jsonb_document.h"
#include "util/jsonb_writer.h"
#include "vec/common/field_visitors.h"
#include "vec/common/typeid_cast.h"
#include "vec/core/accurate_comparison.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/functions/cast/cast_to_basic_number_common.h"
#include "vec/functions/cast/cast_to_boolean.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

template <typename F> /// Field template parameter may be const or non-const Field.
void dispatch(F&& f, const Field& field) {
    switch (field.get_type()) {
    case PrimitiveType::TYPE_NULL:
        f(field.template get<TYPE_NULL>());
        return;
    case PrimitiveType::TYPE_DATETIMEV2:
        f(field.template get<TYPE_DATETIMEV2>());
        return;
    case PrimitiveType::TYPE_TIMESTAMPTZ:
        f(field.template get<TYPE_TIMESTAMPTZ>());
        return;
    case PrimitiveType::TYPE_DATETIME:
        f(field.template get<TYPE_DATETIME>());
        return;
    case PrimitiveType::TYPE_DATE:
        f(field.template get<TYPE_DATE>());
        return;
    case PrimitiveType::TYPE_BOOLEAN:
        f(field.template get<TYPE_BOOLEAN>());
        return;
    case PrimitiveType::TYPE_TINYINT:
        f(field.template get<TYPE_TINYINT>());
        return;
    case PrimitiveType::TYPE_SMALLINT:
        f(field.template get<TYPE_SMALLINT>());
        return;
    case PrimitiveType::TYPE_INT:
        f(field.template get<TYPE_INT>());
        return;
    case PrimitiveType::TYPE_BIGINT:
        f(field.template get<TYPE_BIGINT>());
        return;
    case PrimitiveType::TYPE_LARGEINT:
        f(field.template get<TYPE_LARGEINT>());
        return;
    case PrimitiveType::TYPE_IPV6:
        f(field.template get<TYPE_IPV6>());
        return;
    case PrimitiveType::TYPE_TIMEV2:
        f(field.template get<TYPE_TIMEV2>());
        return;
    case PrimitiveType::TYPE_FLOAT:
        f(field.template get<TYPE_FLOAT>());
        return;
    case PrimitiveType::TYPE_DOUBLE:
        f(field.template get<TYPE_DOUBLE>());
        return;
    case PrimitiveType::TYPE_STRING:
    case PrimitiveType::TYPE_CHAR:
    case PrimitiveType::TYPE_VARCHAR:
        f(field.template get<TYPE_STRING>());
        return;
    case PrimitiveType::TYPE_VARBINARY:
        f(field.template get<TYPE_VARBINARY>());
        return;
    case PrimitiveType::TYPE_JSONB:
        f(field.template get<TYPE_JSONB>());
        return;
    case PrimitiveType::TYPE_ARRAY:
        f(field.template get<TYPE_ARRAY>());
        return;
    case PrimitiveType::TYPE_STRUCT:
        f(field.template get<TYPE_STRUCT>());
        return;
    case PrimitiveType::TYPE_MAP:
        f(field.template get<TYPE_MAP>());
        return;
    case PrimitiveType::TYPE_DECIMAL32:
        f(field.template get<TYPE_DECIMAL32>());
        return;
    case PrimitiveType::TYPE_DECIMAL64:
        f(field.template get<TYPE_DECIMAL64>());
        return;
    case PrimitiveType::TYPE_DECIMALV2:
        f(field.template get<TYPE_DECIMALV2>());
        return;
    case PrimitiveType::TYPE_DECIMAL128I:
        f(field.template get<TYPE_DECIMAL128I>());
        return;
    case PrimitiveType::TYPE_DECIMAL256:
        f(field.template get<TYPE_DECIMAL256>());
        return;
    case PrimitiveType::TYPE_VARIANT:
        f(field.template get<TYPE_VARIANT>());
        return;
    case PrimitiveType::TYPE_BITMAP:
        f(field.template get<TYPE_BITMAP>());
        return;
    case PrimitiveType::TYPE_HLL:
        f(field.template get<TYPE_HLL>());
        return;
    case PrimitiveType::TYPE_QUANTILE_STATE:
        f(field.template get<TYPE_QUANTILE_STATE>());
        return;
    default:
        throw Exception(Status::FatalError("type not supported, type={}", field.get_type_name()));
    }
}
/** Checking for a `Field from` of `From` type falls to a range of values of type `To`.
  * `From` and `To` - numeric types. They can be floating-point types.
  * `From` is one of UInt64, Int64, Float64,
  *  whereas `To` can also be 8, 16, 32 bit.
  *
  * If falls into a range, then `from` is converted to the `Field` closest to the `To` type.
  * If not, return Field(Null).
  */

// TODO support more types
class FieldVisitorToStringSimple : public StaticVisitor<String> {
public:
    template <PrimitiveType T>
    String apply(const typename PrimitiveTypeTraits<T>::CppType& x) const {
        if constexpr (T == TYPE_NULL) {
            return "NULL";
        } else if constexpr (is_int_or_bool(T) && T != TYPE_LARGEINT) {
            return std::to_string(x);
        } else if constexpr (is_string_type(T)) {
            return x;
        } else {
            throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR, "Not implemeted");
        }
    }
};

class FieldVisitorToJsonb : public StaticVisitor<void> {
public:
    void operator()(const Null& x, JsonbWriter* writer) const { writer->writeNull(); }
    void operator()(const DateV2Value<DateTimeV2ValueType>& x, JsonbWriter* writer) const {
        writer->writeInt64(*(UInt64*)&x);
    }
    void operator()(const TimestampTzValue& x, JsonbWriter* writer) const {
        writer->writeInt64(*(UInt64*)&x);
    }
    void operator()(const UInt128& x, JsonbWriter* writer) const {
        writer->writeInt128(int128_t(x));
    }
    void operator()(const Int128& x, JsonbWriter* writer) const {
        writer->writeInt128(int128_t(x));
    }
    void operator()(const IPv6& x, JsonbWriter* writer) const { writer->writeInt128(int128_t(x)); }
    void operator()(const bool& x, JsonbWriter* writer) const { writer->writeBool(x); }
    void operator()(const UInt8& x, JsonbWriter* writer) const { writer->writeBool(x); }
    void operator()(const Int8& x, JsonbWriter* writer) const { writer->writeInt8(x); }
    void operator()(const Int16& x, JsonbWriter* writer) const { writer->writeInt16(x); }
    void operator()(const Int32& x, JsonbWriter* writer) const { writer->writeInt32(x); }
    void operator()(const Int64& x, JsonbWriter* writer) const { writer->writeInt64(x); }
    void operator()(const Float32& x, JsonbWriter* writer) const { writer->writeFloat(x); }
    void operator()(const Float64& x, JsonbWriter* writer) const { writer->writeDouble(x); }
    void operator()(const String& x, JsonbWriter* writer) const {
        writer->writeStartString();
        writer->writeString(x);
        writer->writeEndString();
    }
    void operator()(const StringView& x, JsonbWriter* writer) const {
        writer->writeStartString();
        writer->writeString(x.data(), x.size());
        writer->writeEndString();
    }
    void operator()(const JsonbField& x, JsonbWriter* writer) const {
        const JsonbDocument* doc;
        THROW_IF_ERROR(JsonbDocument::checkAndCreateDocument(x.get_value(), x.get_size(), &doc));
        writer->writeValue(doc->getValue());
    }
    void operator()(const Array& x, JsonbWriter* writer) const;

    void operator()(const Tuple& x, JsonbWriter* writer) const {
        throw doris::Exception(doris::ErrorCode::NOT_IMPLEMENTED_ERROR, "Not implemeted");
    }
    void operator()(const Decimal32& x, JsonbWriter* writer) const {
        throw doris::Exception(doris::ErrorCode::NOT_IMPLEMENTED_ERROR, "Not implemeted");
    }
    void operator()(const Decimal64& x, JsonbWriter* writer) const {
        throw doris::Exception(doris::ErrorCode::NOT_IMPLEMENTED_ERROR, "Not implemeted");
    }
    void operator()(const DecimalV2Value& x, JsonbWriter* writer) const {
        throw doris::Exception(doris::ErrorCode::NOT_IMPLEMENTED_ERROR, "Not implemeted");
    }
    void operator()(const Decimal128V3& x, JsonbWriter* writer) const {
        throw doris::Exception(doris::ErrorCode::NOT_IMPLEMENTED_ERROR, "Not implemeted");
    }
    void operator()(const Decimal256& x, JsonbWriter* writer) const {
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
};

void FieldVisitorToJsonb::operator()(const Array& x, JsonbWriter* writer) const {
    const size_t size = x.size();
    writer->writeStartArray();
    for (size_t i = 0; i < size; ++i) {
        dispatch([writer](const auto& value) { FieldVisitorToJsonb()(value, writer); }, x[i]);
    }
    writer->writeEndArray();
}

struct ConvertNumeric {
    template <class SRC, class DST>
    static inline bool cast(const SRC& from, DST& to);

private:
    static CastParameters create_cast_params() {
        CastParameters params;
        params.is_strict = false;
        return params;
    }
};

// cast to boolean
template <>
bool ConvertNumeric::cast<Int64, UInt8>(const Int64& from, UInt8& to) {
    auto params = create_cast_params();
    return CastToBool::from_number(from, to, params);
}

template <>
bool ConvertNumeric::cast<Int128, UInt8>(const Int128& from, UInt8& to) {
    auto params = create_cast_params();
    return CastToBool::from_number(from, to, params);
}

template <>
bool ConvertNumeric::cast<Float64, UInt8>(const Float64& from, UInt8& to) {
    auto params = create_cast_params();
    return CastToBool::from_number(from, to, params);
}

template <>
bool ConvertNumeric::cast<UInt8, UInt8>(const UInt8& from, UInt8& to) {
    to = from;
    return true;
}

template <>
bool ConvertNumeric::cast<Float32, UInt8>(const Float32& from, UInt8& to) {
    auto params = create_cast_params();
    return CastToBool::from_number(from, to, params);
}

template <>
bool ConvertNumeric::cast<Int8, UInt8>(const Int8& from, UInt8& to) {
    auto params = create_cast_params();
    return CastToBool::from_number(from, to, params);
}

template <>
bool ConvertNumeric::cast<Int16, UInt8>(const Int16& from, UInt8& to) {
    auto params = create_cast_params();
    return CastToBool::from_number(from, to, params);
}

template <>
bool ConvertNumeric::cast<Int32, UInt8>(const Int32& from, UInt8& to) {
    auto params = create_cast_params();
    return CastToBool::from_number(from, to, params);
}

// cast to tinyint
template <>
bool ConvertNumeric::cast<Int64, Int8>(const Int64& from, Int8& to) {
    auto params = create_cast_params();
    return CastToInt::from_int(from, to, params);
}

template <>
bool ConvertNumeric::cast<Int128, Int8>(const Int128& from, Int8& to) {
    auto params = create_cast_params();
    return CastToInt::from_int(from, to, params);
}

template <>
bool ConvertNumeric::cast<Float64, Int8>(const Float64& from, Int8& to) {
    auto params = create_cast_params();
    return CastToInt::from_float(from, to, params);
}

template <>
bool ConvertNumeric::cast<Float32, Int8>(const Float32& from, Int8& to) {
    auto params = create_cast_params();
    return CastToInt::from_float(from, to, params);
}

template <>
bool ConvertNumeric::cast<UInt8, Int8>(const UInt8& from, Int8& to) {
    auto params = create_cast_params();
    return CastToInt::from_bool(from, to, params);
}

template <>
bool ConvertNumeric::cast<Int8, Int8>(const Int8& from, Int8& to) {
    auto params = create_cast_params();
    return CastToInt::from_int(from, to, params);
}

template <>
bool ConvertNumeric::cast<Int16, Int8>(const Int16& from, Int8& to) {
    auto params = create_cast_params();
    return CastToInt::from_int(from, to, params);
}

template <>
bool ConvertNumeric::cast<Int32, Int8>(const Int32& from, Int8& to) {
    auto params = create_cast_params();
    return CastToInt::from_int(from, to, params);
}

// cast to smallint
template <>
bool ConvertNumeric::cast<Int64, Int16>(const Int64& from, Int16& to) {
    auto params = create_cast_params();
    return CastToInt::from_int(from, to, params);
}

template <>
bool ConvertNumeric::cast<Int128, Int16>(const Int128& from, Int16& to) {
    auto params = create_cast_params();
    return CastToInt::from_int(from, to, params);
}

template <>
bool ConvertNumeric::cast<Float64, Int16>(const Float64& from, Int16& to) {
    auto params = create_cast_params();
    return CastToInt::from_float(from, to, params);
}

template <>
bool ConvertNumeric::cast<Float32, Int16>(const Float32& from, Int16& to) {
    auto params = create_cast_params();
    return CastToInt::from_float(from, to, params);
}

template <>
bool ConvertNumeric::cast<UInt8, Int16>(const UInt8& from, Int16& to) {
    auto params = create_cast_params();
    return CastToInt::from_bool(from, to, params);
}

template <>
bool ConvertNumeric::cast<Int8, Int16>(const Int8& from, Int16& to) {
    auto params = create_cast_params();
    return CastToInt::from_int(from, to, params);
}

template <>
bool ConvertNumeric::cast<Int16, Int16>(const Int16& from, Int16& to) {
    auto params = create_cast_params();
    return CastToInt::from_int(from, to, params);
}

template <>
bool ConvertNumeric::cast<Int32, Int16>(const Int32& from, Int16& to) {
    auto params = create_cast_params();
    return CastToInt::from_int(from, to, params);
}

// cast to int
template <>
bool ConvertNumeric::cast<Int64, Int32>(const Int64& from, Int32& to) {
    auto params = create_cast_params();
    return CastToInt::from_int(from, to, params);
}

template <>
bool ConvertNumeric::cast<Int128, Int32>(const Int128& from, Int32& to) {
    auto params = create_cast_params();
    return CastToInt::from_int(from, to, params);
}

template <>
bool ConvertNumeric::cast<Float64, Int32>(const Float64& from, Int32& to) {
    auto params = create_cast_params();
    return CastToInt::from_float(from, to, params);
}

template <>
bool ConvertNumeric::cast<Float32, Int32>(const Float32& from, Int32& to) {
    auto params = create_cast_params();
    return CastToInt::from_float(from, to, params);
}

template <>
bool ConvertNumeric::cast<UInt8, Int32>(const UInt8& from, Int32& to) {
    auto params = create_cast_params();
    return CastToInt::from_bool(from, to, params);
}

template <>
bool ConvertNumeric::cast<Int8, Int32>(const Int8& from, Int32& to) {
    auto params = create_cast_params();
    return CastToInt::from_int(from, to, params);
}

template <>
bool ConvertNumeric::cast<Int32, Int32>(const Int32& from, Int32& to) {
    auto params = create_cast_params();
    return CastToInt::from_int(from, to, params);
}

template <>
bool ConvertNumeric::cast<Int16, Int32>(const Int16& from, Int32& to) {
    auto params = create_cast_params();
    return CastToInt::from_int(from, to, params);
}

// cast to bigint
template <>
bool ConvertNumeric::cast<Int64, Int64>(const Int64& from, Int64& to) {
    auto params = create_cast_params();
    return CastToInt::from_int(from, to, params);
}

template <>
bool ConvertNumeric::cast<Int128, Int64>(const Int128& from, Int64& to) {
    auto params = create_cast_params();
    return CastToInt::from_int(from, to, params);
}

template <>
bool ConvertNumeric::cast<Float64, Int64>(const Float64& from, Int64& to) {
    auto params = create_cast_params();
    return CastToInt::from_float(from, to, params);
}

template <>
bool ConvertNumeric::cast<Float32, Int64>(const Float32& from, Int64& to) {
    auto params = create_cast_params();
    return CastToInt::from_float(from, to, params);
}

template <>
bool ConvertNumeric::cast<UInt8, Int64>(const UInt8& from, Int64& to) {
    auto params = create_cast_params();
    return CastToInt::from_bool(from, to, params);
}

template <>
bool ConvertNumeric::cast<Int8, Int64>(const Int8& from, Int64& to) {
    auto params = create_cast_params();
    return CastToInt::from_int(from, to, params);
}

template <>
bool ConvertNumeric::cast<Int32, Int64>(const Int32& from, Int64& to) {
    auto params = create_cast_params();
    return CastToInt::from_int(from, to, params);
}

template <>
bool ConvertNumeric::cast<Int16, Int64>(const Int16& from, Int64& to) {
    auto params = create_cast_params();
    return CastToInt::from_int(from, to, params);
}

// cast to largeint
template <>
bool ConvertNumeric::cast<Int64, Int128>(const Int64& from, Int128& to) {
    auto params = create_cast_params();
    return CastToInt::from_int(from, to, params);
}

template <>
bool ConvertNumeric::cast<Int128, Int128>(const Int128& from, Int128& to) {
    auto params = create_cast_params();
    return CastToInt::from_int(from, to, params);
}

template <>
bool ConvertNumeric::cast<Float64, Int128>(const Float64& from, Int128& to) {
    auto params = create_cast_params();
    return CastToInt::from_float(from, to, params);
}

template <>
bool ConvertNumeric::cast<Float32, Int128>(const Float32& from, Int128& to) {
    auto params = create_cast_params();
    return CastToInt::from_float(from, to, params);
}

template <>
bool ConvertNumeric::cast<UInt8, Int128>(const UInt8& from, Int128& to) {
    auto params = create_cast_params();
    return CastToInt::from_bool(from, to, params);
}

template <>
bool ConvertNumeric::cast<Int8, Int128>(const Int8& from, Int128& to) {
    auto params = create_cast_params();
    return CastToInt::from_int(from, to, params);
}

template <>
bool ConvertNumeric::cast<Int16, Int128>(const Int16& from, Int128& to) {
    auto params = create_cast_params();
    return CastToInt::from_int(from, to, params);
}

template <>
bool ConvertNumeric::cast<Int32, Int128>(const Int32& from, Int128& to) {
    auto params = create_cast_params();
    return CastToInt::from_int(from, to, params);
}

// cast to float
template <>
bool ConvertNumeric::cast<Int64, Float32>(const Int64& from, Float32& to) {
    auto params = create_cast_params();
    return CastToFloat::from_int(from, to, params);
}

template <>
bool ConvertNumeric::cast<Int128, Float32>(const Int128& from, Float32& to) {
    auto params = create_cast_params();
    return CastToFloat::from_int(from, to, params);
}

template <>
bool ConvertNumeric::cast<Float64, Float32>(const Float64& from, Float32& to) {
    auto params = create_cast_params();
    return CastToFloat::from_float(from, to, params);
}

template <>
bool ConvertNumeric::cast<Float32, Float32>(const Float32& from, Float32& to) {
    auto params = create_cast_params();
    return CastToFloat::from_float(from, to, params);
}

template <>
bool ConvertNumeric::cast<UInt8, Float32>(const UInt8& from, Float32& to) {
    auto params = create_cast_params();
    return CastToFloat::from_bool(from, to, params);
}

template <>
bool ConvertNumeric::cast<Int8, Float32>(const Int8& from, Float32& to) {
    auto params = create_cast_params();
    return CastToFloat::from_int(from, to, params);
}

template <>
bool ConvertNumeric::cast<Int16, Float32>(const Int16& from, Float32& to) {
    auto params = create_cast_params();
    return CastToFloat::from_int(from, to, params);
}

template <>
bool ConvertNumeric::cast<Int32, Float32>(const Int32& from, Float32& to) {
    auto params = create_cast_params();
    return CastToFloat::from_int(from, to, params);
}

// cast to double
template <>
bool ConvertNumeric::cast<Int64, Float64>(const Int64& from, Float64& to) {
    auto params = create_cast_params();
    return CastToFloat::from_int(from, to, params);
}

template <>
bool ConvertNumeric::cast<Int128, Float64>(const Int128& from, Float64& to) {
    auto params = create_cast_params();
    return CastToFloat::from_int(from, to, params);
}

template <>
bool ConvertNumeric::cast<Float64, Float64>(const Float64& from, Float64& to) {
    auto params = create_cast_params();
    return CastToFloat::from_float(from, to, params);
}

template <>
bool ConvertNumeric::cast<Float32, Float64>(const Float32& from, Float64& to) {
    auto params = create_cast_params();
    return CastToFloat::from_float(from, to, params);
}

template <>
bool ConvertNumeric::cast<UInt8, Float64>(const UInt8& from, Float64& to) {
    auto params = create_cast_params();
    return CastToFloat::from_bool(from, to, params);
}

template <>
bool ConvertNumeric::cast<Int8, Float64>(const Int8& from, Float64& to) {
    auto params = create_cast_params();
    return CastToFloat::from_int(from, to, params);
}

template <>
bool ConvertNumeric::cast<Int16, Float64>(const Int16& from, Float64& to) {
    auto params = create_cast_params();
    return CastToFloat::from_int(from, to, params);
}

template <>
bool ConvertNumeric::cast<Int32, Float64>(const Int32& from, Float64& to) {
    auto params = create_cast_params();
    return CastToFloat::from_int(from, to, params);
}

namespace {
template <PrimitiveType From, PrimitiveType T>
Field convert_numeric_type_impl(const Field& from) {
    typename PrimitiveTypeTraits<T>::CppType result;
    if (!ConvertNumeric::cast(from.get<From>(), result)) {
        return {};
    }
    return Field::create_field<T>(result);
}

template <PrimitiveType T>
void convert_numric_type(const Field& from, const IDataType& type, Field* to) {
    if (from.get_type() == PrimitiveType::TYPE_BIGINT) {
        *to = convert_numeric_type_impl<TYPE_BIGINT, T>(from);
    } else if (from.get_type() == PrimitiveType::TYPE_DOUBLE) {
        *to = convert_numeric_type_impl<TYPE_DOUBLE, T>(from);
    } else if (from.get_type() == PrimitiveType::TYPE_LARGEINT) {
        *to = convert_numeric_type_impl<TYPE_LARGEINT, T>(from);
    } else if (from.get_type() == PrimitiveType::TYPE_TINYINT) {
        *to = convert_numeric_type_impl<TYPE_TINYINT, T>(from);
    } else if (from.get_type() == PrimitiveType::TYPE_SMALLINT) {
        *to = convert_numeric_type_impl<TYPE_SMALLINT, T>(from);
    } else if (from.get_type() == PrimitiveType::TYPE_INT) {
        *to = convert_numeric_type_impl<TYPE_INT, T>(from);
    } else if (from.get_type() == PrimitiveType::TYPE_FLOAT) {
        *to = convert_numeric_type_impl<TYPE_FLOAT, T>(from);
    } else if (from.get_type() == PrimitiveType::TYPE_BOOLEAN) {
        *to = convert_numeric_type_impl<TYPE_BOOLEAN, T>(from);
    } else {
        throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                               "Type mismatch in IN or VALUES section. Expected: {}. Got: {}",
                               type.get_name(), from.get_type());
    }
}

void convert_field_to_typeImpl(const Field& src, const IDataType& type,
                               const IDataType* from_type_hint, Field* to) {
    if (from_type_hint && from_type_hint->equals(type)) {
        *to = src;
        return;
    }
    // TODO add more types
    if (is_string_type(type.get_primitive_type())) {
        if (is_string_type(src.get_type())) {
            *to = src;
            return;
        }
        // TODO this is a very simple translator, support more complex types
        FieldVisitorToStringSimple v;
        *to = Field::create_field<TYPE_STRING>(apply_visitor(v, src));
        return;
    } else if (type.get_primitive_type() == PrimitiveType::TYPE_JSONB) {
        if (src.get_type() == PrimitiveType::TYPE_JSONB) {
            *to = src;
            return;
        }
        JsonbWriter writer;
        dispatch([&writer](const auto& value) { FieldVisitorToJsonb()(value, &writer); }, src);
        *to = Field::create_field<TYPE_JSONB>(
                JsonbField(writer.getOutput()->getBuffer(),
                           cast_set<UInt32, size_t, false>(writer.getOutput()->getSize())));
        return;
    } else if (type.get_primitive_type() == PrimitiveType::TYPE_VARIANT) {
        if (src.get_type() == PrimitiveType::TYPE_VARIANT) {
            *to = src;
            return;
        }
        throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                               "Type mismatch in IN or VALUES section. Expected: {}. Got: {}",
                               type.get_name(), src.get_type());
        return;
    } else if (const auto* type_array = typeid_cast<const DataTypeArray*>(&type)) {
        if (src.get_type() == PrimitiveType::TYPE_ARRAY) {
            const auto& src_arr = src.get<TYPE_ARRAY>();
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
            *to = Field::create_field<TYPE_ARRAY>(res);
            return;
        }
    } else if (!is_string_type(src.get_type())) {
        switch (type.get_primitive_type()) {
        case PrimitiveType::TYPE_BOOLEAN: {
            convert_numric_type<TYPE_BOOLEAN>(src, type, to);
            return;
        }
        case PrimitiveType::TYPE_TINYINT: {
            convert_numric_type<TYPE_TINYINT>(src, type, to);
            return;
        }
        case PrimitiveType::TYPE_SMALLINT: {
            convert_numric_type<TYPE_SMALLINT>(src, type, to);
            return;
        }
        case PrimitiveType::TYPE_INT: {
            convert_numric_type<TYPE_INT>(src, type, to);
            return;
        }
        case PrimitiveType::TYPE_BIGINT: {
            convert_numric_type<TYPE_BIGINT>(src, type, to);
            return;
        }
        case PrimitiveType::TYPE_LARGEINT: {
            convert_numric_type<TYPE_LARGEINT>(src, type, to);
            return;
        }
        case PrimitiveType::TYPE_FLOAT: {
            convert_numric_type<TYPE_FLOAT>(src, type, to);
            return;
        }
        case PrimitiveType::TYPE_DOUBLE: {
            convert_numric_type<TYPE_DOUBLE>(src, type, to);
            return;
        }
        case PrimitiveType::TYPE_DATE:
        case PrimitiveType::TYPE_DATETIME: {
            /// We don't need any conversion UInt64 is under type of Date and DateTime
            if (is_date_type(src.get_type())) {
                *to = src;
                return;
            }
            break;
        }
        default:
            break;
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
        if (nested_type.get_primitive_type() == INVALID_TYPE) {
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
#include "common/compile_check_end.h"
} // namespace doris::vectorized