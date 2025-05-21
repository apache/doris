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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Core/Field.cpp
// and modified by Doris

#include "vec/core/field.h"

#include "runtime/primitive_type.h"
#include "vec/core/accurate_comparison.h"
#include "vec/core/decimal_comparison.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/io/io_helper.h"
#include "vec/io/var_int.h"

namespace doris::vectorized {
class BufferReadable;
class BufferWritable;

template <>
Decimal128V3 DecimalField<Decimal128V3>::get_scale_multiplier() const {
    return DataTypeDecimal<Decimal128V3>::get_scale_multiplier(scale);
}

template <typename T>
bool dec_equal(T x, T y, UInt32 x_scale, UInt32 y_scale) {
    using Comparator = DecimalComparison<T, T, EqualsOp>;
    return Comparator::compare(x, y, x_scale, y_scale);
}

template <typename T>
bool dec_less(T x, T y, UInt32 x_scale, UInt32 y_scale) {
    using Comparator = DecimalComparison<T, T, LessOp>;
    return Comparator::compare(x, y, x_scale, y_scale);
}

template <typename T>
bool dec_less_or_equal(T x, T y, UInt32 x_scale, UInt32 y_scale) {
    using Comparator = DecimalComparison<T, T, LessOrEqualsOp>;
    return Comparator::compare(x, y, x_scale, y_scale);
}

#define DECLARE_DECIMAL_COMPARISON(TYPE)                               \
    template <>                                                        \
    bool decimal_equal(TYPE x, TYPE y, UInt32 xs, UInt32 ys) {         \
        return dec_equal(x, y, xs, ys);                                \
    }                                                                  \
    template <>                                                        \
    bool decimal_less(TYPE x, TYPE y, UInt32 xs, UInt32 ys) {          \
        return dec_less(x, y, xs, ys);                                 \
    }                                                                  \
    template <>                                                        \
    bool decimal_less_or_equal(TYPE x, TYPE y, UInt32 xs, UInt32 ys) { \
        return dec_less_or_equal(x, y, xs, ys);                        \
    }                                                                  \
    template <>                                                        \
    TYPE DecimalField<TYPE>::get_scale_multiplier() const {            \
        return DataTypeDecimal<TYPE>::get_scale_multiplier(scale);     \
    }

DECLARE_DECIMAL_COMPARISON(Decimal32)
DECLARE_DECIMAL_COMPARISON(Decimal64)
DECLARE_DECIMAL_COMPARISON(Decimal128V2)
DECLARE_DECIMAL_COMPARISON(Decimal256)

template <>
bool decimal_equal(Decimal128V3 x, Decimal128V3 y, UInt32 xs, UInt32 ys) {
    return dec_equal(x, y, xs, ys);
}
template <>
bool decimal_less(Decimal128V3 x, Decimal128V3 y, UInt32 xs, UInt32 ys) {
    return dec_less(x, y, xs, ys);
}
template <>
bool decimal_less_or_equal(Decimal128V3 x, Decimal128V3 y, UInt32 xs, UInt32 ys) {
    return dec_less_or_equal(x, y, xs, ys);
}

template <typename T>
void Field::create_concrete(T&& x) {
    using UnqualifiedType = std::decay_t<T>;

    // In both Field and PODArray, small types may be stored as wider types,
    // e.g. char is stored as UInt64. Field can return this extended value
    // with get<StorageType>(). To avoid uninitialized results from get(),
    // we must initialize the entire wide stored type, and not just the
    // nominal type.
    using StorageType = NearestFieldType<UnqualifiedType>;
    new (&storage) StorageType(std::forward<T>(x));
    type = TypeToPrimitiveType<UnqualifiedType>::value;
    DCHECK_NE(type, PrimitiveType::INVALID_TYPE);
}

/// Assuming same types.
template <typename T>
void Field::assign_concrete(T&& x) {
    using JustT = std::decay_t<T>;
    assert(type == TypeToPrimitiveType<JustT>::value);
    auto* MAY_ALIAS ptr = reinterpret_cast<JustT*>(&storage);
    *ptr = std::forward<T>(x);
}

template <typename T>
    requires(!std::is_same_v<std::decay_t<T>, Field>)
Field& Field::operator=(T&& rhs) {
    auto&& val = cast_to_nearest_field_type(std::forward<T>(rhs));
    using U = decltype(val);
    if (type != TypeToPrimitiveType<std::decay_t<U>>::value) {
        destroy();
        create_concrete(std::forward<U>(val));
    } else {
        assign_concrete(std::forward<U>(val));
    }

    return *this;
}

std::string Field::get_type_name() const {
    return type_to_string(type);
}

template void Field::create_concrete(Int64&& rhs);
template void Field::create_concrete(Null&& rhs);
template void Field::create_concrete(UInt64&& rhs);
template void Field::create_concrete(Int128&& rhs);
template void Field::create_concrete(String&& rhs);
template void Field::create_concrete(double&& rhs);
template void Field::create_concrete(JsonbField&& rhs);
template void Field::create_concrete(Array&& rhs);
template void Field::create_concrete(Map&& rhs);
template void Field::create_concrete(VariantMap&& rhs);
template void Field::create_concrete(BitmapValue&& rhs);
template void Field::create_concrete(HyperLogLog&& rhs);
template void Field::create_concrete(QuantileState&& rhs);
template void Field::create_concrete(UInt128&& rhs);
template void Field::create_concrete(unsigned __int128&& rhs);
template void Field::create_concrete(Tuple&& rhs);
template void Field::create_concrete(DecimalField<Decimal32>&& rhs);
template void Field::create_concrete(DecimalField<Decimal64>&& rhs);
template void Field::create_concrete(DecimalField<Decimal128V3>&& rhs);
template void Field::create_concrete(DecimalField<Decimal128V2>&& rhs);
template void Field::create_concrete(DecimalField<Decimal256>&& rhs);
template void Field::create_concrete(const Int64& rhs);
template void Field::create_concrete(const Null& rhs);
template void Field::create_concrete(const UInt64& rhs);
template void Field::create_concrete(const Int128& rhs);
template void Field::create_concrete(const String& rhs);
template void Field::create_concrete(const double& rhs);
template void Field::create_concrete(const JsonbField& rhs);
template void Field::create_concrete(const Array& rhs);
template void Field::create_concrete(const Map& rhs);
template void Field::create_concrete(const VariantMap& rhs);
template void Field::create_concrete(const BitmapValue& rhs);
template void Field::create_concrete(const HyperLogLog& rhs);
template void Field::create_concrete(const QuantileState& rhs);
template void Field::create_concrete(const UInt128& rhs);
template void Field::create_concrete(const unsigned __int128& rhs);
template void Field::create_concrete(const Tuple& rhs);
template void Field::create_concrete(const DecimalField<Decimal32>& rhs);
template void Field::create_concrete(const DecimalField<Decimal64>& rhs);
template void Field::create_concrete(const DecimalField<Decimal128V3>& rhs);
template void Field::create_concrete(const DecimalField<Decimal128V2>& rhs);
template void Field::create_concrete(const DecimalField<Decimal256>& rhs);
template void Field::create_concrete(Int64& rhs);
template void Field::create_concrete(Null& rhs);
template void Field::create_concrete(UInt64& rhs);
template void Field::create_concrete(Int128& rhs);
template void Field::create_concrete(String& rhs);
template void Field::create_concrete(double& rhs);
template void Field::create_concrete(JsonbField& rhs);
template void Field::create_concrete(Array& rhs);
template void Field::create_concrete(Map& rhs);
template void Field::create_concrete(VariantMap& rhs);
template void Field::create_concrete(BitmapValue& rhs);
template void Field::create_concrete(HyperLogLog& rhs);
template void Field::create_concrete(QuantileState& rhs);
template void Field::create_concrete(UInt128& rhs);
template void Field::create_concrete(unsigned __int128& rhs);
template void Field::create_concrete(Tuple& rhs);
template void Field::create_concrete(DecimalField<Decimal32>& rhs);
template void Field::create_concrete(DecimalField<Decimal64>& rhs);
template void Field::create_concrete(DecimalField<Decimal128V3>& rhs);
template void Field::create_concrete(DecimalField<Decimal128V2>& rhs);
template void Field::create_concrete(DecimalField<Decimal256>& rhs);
template void Field::assign_concrete(Int64&& rhs);
template void Field::assign_concrete(Null&& rhs);
template void Field::assign_concrete(UInt64&& rhs);
template void Field::assign_concrete(Int128&& rhs);
template void Field::assign_concrete(double&& rhs);
template void Field::assign_concrete(JsonbField&& rhs);
template void Field::assign_concrete(Array&& rhs);
template void Field::assign_concrete(Map&& rhs);
template void Field::assign_concrete(VariantMap&& rhs);
template void Field::assign_concrete(BitmapValue&& rhs);
template void Field::assign_concrete(HyperLogLog&& rhs);
template void Field::assign_concrete(QuantileState&& rhs);
template void Field::assign_concrete(String&& rhs);
template void Field::assign_concrete(DecimalField<Decimal32>&& rhs);
template void Field::assign_concrete(DecimalField<Decimal64>&& rhs);
template void Field::assign_concrete(DecimalField<Decimal128V3>&& rhs);
template void Field::assign_concrete(DecimalField<Decimal128V2>&& rhs);
template void Field::assign_concrete(DecimalField<Decimal256>&& rhs);
template void Field::assign_concrete(UInt128&& rhs);
template void Field::assign_concrete(unsigned __int128&& rhs);
template void Field::assign_concrete(Tuple&& rhs);
template void Field::assign_concrete(const Int64& rhs);
template void Field::assign_concrete(const Null& rhs);
template void Field::assign_concrete(const UInt64& rhs);
template void Field::assign_concrete(const Int128& rhs);
template void Field::assign_concrete(const double& rhs);
template void Field::assign_concrete(const JsonbField& rhs);
template void Field::assign_concrete(const Array& rhs);
template void Field::assign_concrete(const Map& rhs);
template void Field::assign_concrete(const VariantMap& rhs);
template void Field::assign_concrete(const BitmapValue& rhs);
template void Field::assign_concrete(const HyperLogLog& rhs);
template void Field::assign_concrete(const QuantileState& rhs);
template void Field::assign_concrete(const String& rhs);
template void Field::assign_concrete(const DecimalField<Decimal32>& rhs);
template void Field::assign_concrete(const DecimalField<Decimal64>& rhs);
template void Field::assign_concrete(const DecimalField<Decimal128V3>& rhs);
template void Field::assign_concrete(const DecimalField<Decimal128V2>& rhs);
template void Field::assign_concrete(const DecimalField<Decimal256>& rhs);
template void Field::assign_concrete(const UInt128& rhs);
template void Field::assign_concrete(const unsigned __int128& rhs);
template void Field::assign_concrete(const Tuple& rhs);
template void Field::assign_concrete(Int64& rhs);
template void Field::assign_concrete(Null& rhs);
template void Field::assign_concrete(UInt64& rhs);
template void Field::assign_concrete(Int128& rhs);
template void Field::assign_concrete(double& rhs);
template void Field::assign_concrete(JsonbField& rhs);
template void Field::assign_concrete(Array& rhs);
template void Field::assign_concrete(Map& rhs);
template void Field::assign_concrete(VariantMap& rhs);
template void Field::assign_concrete(BitmapValue& rhs);
template void Field::assign_concrete(HyperLogLog& rhs);
template void Field::assign_concrete(QuantileState& rhs);
template void Field::assign_concrete(String& rhs);
template void Field::assign_concrete(DecimalField<Decimal32>& rhs);
template void Field::assign_concrete(DecimalField<Decimal64>& rhs);
template void Field::assign_concrete(DecimalField<Decimal128V3>& rhs);
template void Field::assign_concrete(DecimalField<Decimal128V2>& rhs);
template void Field::assign_concrete(DecimalField<Decimal256>& rhs);
template void Field::assign_concrete(UInt128& rhs);
template void Field::assign_concrete(unsigned __int128& rhs);
template void Field::assign_concrete(Tuple& rhs);
template Field& Field::operator=(Int8&& rhs);
template Field& Field::operator=(Int16&& rhs);
template Field& Field::operator=(Int32&& rhs);
template Field& Field::operator=(Int64&& rhs);
template Field& Field::operator=(Null&& rhs);
template Field& Field::operator=(UInt64&& rhs);
template Field& Field::operator=(Int128&& rhs);
template Field& Field::operator=(double&& rhs);
template Field& Field::operator=(JsonbField&& rhs);
template Field& Field::operator=(Array&& rhs);
template Field& Field::operator=(Map&& rhs);
template Field& Field::operator=(VariantMap&& rhs);
template Field& Field::operator=(BitmapValue&& rhs);
template Field& Field::operator=(HyperLogLog&& rhs);
template Field& Field::operator=(QuantileState&& rhs);
template Field& Field::operator=(String&& rhs);
template Field& Field::operator=(unsigned int& rhs);
template Field& Field::operator=(unsigned __int128& rhs);
template Field& Field::operator=(__int128& rhs);
template Field& Field::operator=(Tuple&& rhs);
template Field& Field::operator=(Tuple& rhs);
template Field& Field::operator=(Array& rhs);
template Field& Field::operator=(String& rhs);
template Field& Field::operator=(double& rhs);
template Field& Field::operator=(VariantMap& rhs);
template Field& Field::operator=(BitmapValue& rhs);
template Field& Field::operator=(HyperLogLog& rhs);
template Field& Field::operator=(QuantileState& rhs);
template Field& Field::operator=(const String& rhs);
template Field& Field::operator=(DecimalField<Decimal32>&& rhs);
template Field& Field::operator=(DecimalField<Decimal64>&& rhs);
template Field& Field::operator=(DecimalField<Decimal256>&& rhs);
template Field& Field::operator=(DecimalField<Decimal128V3>&& rhs);
template Field& Field::operator=(DecimalField<Decimal128V2>&& rhs);
} // namespace doris::vectorized
