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

#include "primitive_type.h"
#include "vec/common/string_buffer.hpp"
#include "vec/runtime/ipv4_value.h"
#include "vec/runtime/ipv6_value.h"
#include "vec/runtime/time_value.h"

namespace doris::vectorized::to_string {

template <PrimitiveType PT>
using column_t = const typename PrimitiveTypeTraits<PT>::ColumnItemType&;

template <PrimitiveType PT>
void primitive_to_writable();

template <PrimitiveType PT>
    requires(PT == TYPE_DATE || PT == TYPE_DATETIME)
void primitive_to_writable(column_t<PT> int_val, BufferWritable& bw) {
    doris::VecDateTimeValue value = binary_cast<Int64, doris::VecDateTimeValue>(int_val);
    char buf[64];
    char* pos = value.to_string(buf);
    bw.write(buf, pos - buf - 1);
}

template <PrimitiveType PT>
    requires(PT == TYPE_DATETIMEV2)
void primitive_to_writable(column_t<PT> int_val, BufferWritable& bw, int scale) {
    DateV2Value<DateTimeV2ValueType> val =
            binary_cast<UInt64, DateV2Value<DateTimeV2ValueType>>(int_val);
    char buf[64];
    char* pos = val.to_string(buf, scale);
    bw.write(buf, pos - buf - 1);
}

template <PrimitiveType PT>
    requires(PT == TYPE_DATEV2)
void primitive_to_writable(column_t<PT> int_val, BufferWritable& bw) {
    DateV2Value<DateV2ValueType> val = binary_cast<UInt32, DateV2Value<DateV2ValueType>>(int_val);
    char buf[64];
    char* pos = val.to_string(buf);
    bw.write(buf, pos - buf - 1);
}

template <PrimitiveType PT>
    requires(is_decimal(PT))
void primitive_to_writable(column_t<PT> value, BufferWritable& bw, int scale) {
    if constexpr (PT != TYPE_DECIMALV2) {
        auto decimal_str = value.to_string(scale);
        bw.write(decimal_str.data(), decimal_str.size());
    } else {
        using FieldType = typename PrimitiveTypeTraits<PT>::ColumnItemType;
        const typename FieldType::NativeType scale_multiplier =
                decimal_scale_multiplier<typename FieldType::NativeType>(scale);
        char buf[FieldType::max_string_length()];
        auto length = value.to_string(buf, scale, scale_multiplier);
        bw.write(buf, length);
    }
}

template <PrimitiveType PT>
    requires(PT == TYPE_IPV4)
void primitive_to_writable(column_t<PT> value, BufferWritable& bw) {
    IPv4Value ipv4_value(value);
    std::string ipv4_str = ipv4_value.to_string();
    bw.write(ipv4_str.c_str(), ipv4_str.length());
}

template <PrimitiveType PT>
    requires(PT == TYPE_IPV6)
void primitive_to_writable(column_t<PT> value, BufferWritable& bw) {
    IPv6Value ipv6_value(value);
    std::string ipv6_str = ipv6_value.to_string();
    bw.write(ipv6_str.c_str(), ipv6_str.length());
}

template <PrimitiveType PT>
    requires(is_int_or_bool(PT) || is_float_or_double(PT))
void primitive_to_writable(column_t<PT> value, BufferWritable& bw) {
    bw.write_number(value);
}

template <PrimitiveType PT>
    requires(PT == TYPE_TIMEV2)
void primitive_to_writable(column_t<PT> value, BufferWritable& bw, int scale) {
    auto time_str = TimeValue::to_string(value, scale);
    bw.write(time_str.data(), time_str.size());
}

} // namespace doris::vectorized::to_string