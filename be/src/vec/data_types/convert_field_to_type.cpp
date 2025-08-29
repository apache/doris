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

namespace doris::vectorized {
#include "common/compile_check_begin.h"
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
    String apply(const typename PrimitiveTypeTraits<T>::NearestFieldType& x) const {
        if constexpr (T == TYPE_NULL) {
            return "NULL";
        } else if constexpr (T == TYPE_BIGINT || T == TYPE_DOUBLE) {
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
    void operator()(const UInt64& x, JsonbWriter* writer) const { writer->writeInt64(x); }
    void operator()(const UInt128& x, JsonbWriter* writer) const {
        writer->writeInt128(int128_t(x));
    }
    void operator()(const Int128& x, JsonbWriter* writer) const {
        writer->writeInt128(int128_t(x));
    }
    void operator()(const IPv6& x, JsonbWriter* writer) const { writer->writeInt128(int128_t(x)); }
    void operator()(const Int64& x, JsonbWriter* writer) const { writer->writeInt64(x); }
    void operator()(const Float64& x, JsonbWriter* writer) const { writer->writeDouble(x); }
    void operator()(const String& x, JsonbWriter* writer) const {
        writer->writeStartString();
        writer->writeString(x);
        writer->writeEndString();
    }
    void operator()(const JsonbField& x, JsonbWriter* writer) const {
        JsonbDocument* doc;
        THROW_IF_ERROR(JsonbDocument::checkAndCreateDocument(x.get_value(), x.get_size(), &doc));
        writer->writeValue(doc->getValue());
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
template <typename From, PrimitiveType T>
Field convert_numeric_type_impl(const Field& from) {
    typename PrimitiveTypeTraits<T>::ColumnItemType result;
    if (!accurate::convertNumeric(from.get<From>(), result)) {
        return {};
    }
    return Field::create_field<T>(result);
}

template <PrimitiveType T>
void convert_numric_type(const Field& from, const IDataType& type, Field* to) {
    if (from.get_type() == PrimitiveType::TYPE_BIGINT) {
        *to = convert_numeric_type_impl<Int64, T>(from);
    } else if (from.get_type() == PrimitiveType::TYPE_DOUBLE) {
        *to = convert_numeric_type_impl<Float64, T>(from);
    } else if (from.get_type() == PrimitiveType::TYPE_LARGEINT) {
        *to = convert_numeric_type_impl<Int128, T>(from);
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
        Field::dispatch([&writer](const auto& value) { FieldVisitorToJsonb()(value, &writer); },
                        src);
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
            const auto& src_arr = src.get<Array>();
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