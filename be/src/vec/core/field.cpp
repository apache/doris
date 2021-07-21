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

#include "vec/core/field.h"

#include "vec/core/decimal_comparison.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {

void read_binary(Array& x, BufferReadable& buf) {
    size_t size;
    UInt8 type;
    doris::vectorized::read_binary(type, buf);
    doris::vectorized::read_binary(size, buf);

    for (size_t index = 0; index < size; ++index) {
        switch (type) {
        case Field::Types::Null: {
            x.push_back(doris::vectorized::Field());
            break;
        }
        case Field::Types::UInt64: {
            UInt64 value;
            doris::vectorized::read_var_uint(value, buf);
            x.push_back(value);
            break;
        }
        case Field::Types::UInt128: {
            UInt128 value;
            doris::vectorized::read_binary(value, buf);
            x.push_back(value);
            break;
        }
        case Field::Types::Int64: {
            Int64 value;
            doris::vectorized::read_var_int(value, buf);
            x.push_back(value);
            break;
        }
        case Field::Types::Float64: {
            Float64 value;
            doris::vectorized::read_float_binary(value, buf);
            x.push_back(value);
            break;
        }
        case Field::Types::String: {
            std::string value;
            doris::vectorized::read_string_binary(value, buf);
            x.push_back(value);
            break;
        }
        case Field::Types::AggregateFunctionState: {
            AggregateFunctionStateData value;
            doris::vectorized::read_string_binary(value.name, buf);
            doris::vectorized::read_string_binary(value.data, buf);
            x.push_back(value);
            break;
        }
        }
    }
}

void write_binary(const Array& x, BufferWritable& buf) {
    UInt8 type = Field::Types::Null;
    size_t size = x.size();
    if (size) type = x.front().get_type();
    doris::vectorized::write_binary(type, buf);
    doris::vectorized::write_binary(size, buf);

    for (Array::const_iterator it = x.begin(); it != x.end(); ++it) {
        switch (type) {
        case Field::Types::Null:
            break;
        case Field::Types::UInt64: {
            doris::vectorized::write_var_uint(get<UInt64>(*it), buf);
            break;
        }
        case Field::Types::UInt128: {
            doris::vectorized::write_binary(get<UInt128>(*it), buf);
            break;
        }
        case Field::Types::Int64: {
            doris::vectorized::write_var_int(get<Int64>(*it), buf);
            break;
        }
        case Field::Types::Float64: {
            doris::vectorized::write_float_binary(get<Float64>(*it), buf);
            break;
        }
        case Field::Types::String: {
            doris::vectorized::write_string_binary(get<std::string>(*it), buf);
            break;
        }
        case Field::Types::AggregateFunctionState: {
            doris::vectorized::write_string_binary(it->get<AggregateFunctionStateData>().name, buf);
            doris::vectorized::write_string_binary(it->get<AggregateFunctionStateData>().data, buf);
            break;
        }
        }
    }
    ;
}

template <>
Decimal32 DecimalField<Decimal32>::get_scale_multiplier() const {
    return DataTypeDecimal<Decimal32>::get_scale_multiplier(scale);
}

template <>
Decimal64 DecimalField<Decimal64>::get_scale_multiplier() const {
    return DataTypeDecimal<Decimal64>::get_scale_multiplier(scale);
}

template <>
Decimal128 DecimalField<Decimal128>::get_scale_multiplier() const {
    return DataTypeDecimal<Decimal128>::get_scale_multiplier(scale);
}

template <typename T>
static bool dec_equal(T x, T y, UInt32 x_scale, UInt32 y_scale) {
    using Comparator = DecimalComparison<T, T, EqualsOp>;
    return Comparator::compare(x, y, x_scale, y_scale);
}

template <typename T>
static bool dec_less(T x, T y, UInt32 x_scale, UInt32 y_scale) {
    using Comparator = DecimalComparison<T, T, LessOp>;
    return Comparator::compare(x, y, x_scale, y_scale);
}

template <typename T>
static bool dec_less_or_equal(T x, T y, UInt32 x_scale, UInt32 y_scale) {
    using Comparator = DecimalComparison<T, T, LessOrEqualsOp>;
    return Comparator::compare(x, y, x_scale, y_scale);
}

template <>
bool decimal_equal(Decimal32 x, Decimal32 y, UInt32 xs, UInt32 ys) {
    return dec_equal(x, y, xs, ys);
}
template <>
bool decimal_less(Decimal32 x, Decimal32 y, UInt32 xs, UInt32 ys) {
    return dec_less(x, y, xs, ys);
}
template <>
bool decimal_less_or_equal(Decimal32 x, Decimal32 y, UInt32 xs, UInt32 ys) {
    return dec_less_or_equal(x, y, xs, ys);
}

template <>
bool decimal_equal(Decimal64 x, Decimal64 y, UInt32 xs, UInt32 ys) {
    return dec_equal(x, y, xs, ys);
}
template <>
bool decimal_less(Decimal64 x, Decimal64 y, UInt32 xs, UInt32 ys) {
    return dec_less(x, y, xs, ys);
}
template <>
bool decimal_less_or_equal(Decimal64 x, Decimal64 y, UInt32 xs, UInt32 ys) {
    return dec_less_or_equal(x, y, xs, ys);
}

template <>
bool decimal_equal(Decimal128 x, Decimal128 y, UInt32 xs, UInt32 ys) {
    return dec_equal(x, y, xs, ys);
}
template <>
bool decimal_less(Decimal128 x, Decimal128 y, UInt32 xs, UInt32 ys) {
    return dec_less(x, y, xs, ys);
}
template <>
bool decimal_less_or_equal(Decimal128 x, Decimal128 y, UInt32 xs, UInt32 ys) {
    return dec_less_or_equal(x, y, xs, ys);
}
} // namespace doris::vectorized
