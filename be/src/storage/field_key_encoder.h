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

#include <string>

#include "core/data_type/primitive_type.h"
#include "core/field.h"
#include "storage/key_coder.h"

namespace doris {

// Convert a Field value to its storage representation (via PrimitiveTypeConvertor)
// and full-encode it as a byte-comparable ascending key via KeyCoder.
template <PrimitiveType PT>
inline void full_encode_field_as_key(const Field& f, const KeyCoder* coder, std::string* buf) {
    auto v = PrimitiveTypeConvertor<PT>::to_storage_field_type(f.get<PT>());
    coder->full_encode_ascending(&v, buf);
}

// Same as full_encode_field_as_key but truncates string keys to `index_size` bytes.
// For fixed-width types KeyCoder ignores `index_size`, so the output is identical
// to full_encode_field_as_key.
template <PrimitiveType PT>
inline void encode_field_as_key(const Field& f, const KeyCoder* coder, size_t index_size,
                                std::string* buf) {
    auto v = PrimitiveTypeConvertor<PT>::to_storage_field_type(f.get<PT>());
    coder->encode_ascending(&v, index_size, buf);
}

// X-macro listing every (FieldType, PrimitiveType) pair that goes through KeyCoder
// as a non-string scalar key. Strings are handled separately because they need
// length / padding logic outside KeyCoder. Each entry: M(FT_suffix, PT_suffix).
#define DORIS_APPLY_FOR_KEY_ENCODABLE_NON_STRING_TYPES(M) \
    M(OLAP_FIELD_TYPE_BOOL, TYPE_BOOLEAN)                 \
    M(OLAP_FIELD_TYPE_TINYINT, TYPE_TINYINT)              \
    M(OLAP_FIELD_TYPE_SMALLINT, TYPE_SMALLINT)            \
    M(OLAP_FIELD_TYPE_INT, TYPE_INT)                      \
    M(OLAP_FIELD_TYPE_BIGINT, TYPE_BIGINT)                \
    M(OLAP_FIELD_TYPE_LARGEINT, TYPE_LARGEINT)            \
    M(OLAP_FIELD_TYPE_FLOAT, TYPE_FLOAT)                  \
    M(OLAP_FIELD_TYPE_DOUBLE, TYPE_DOUBLE)                \
    M(OLAP_FIELD_TYPE_DECIMAL, TYPE_DECIMALV2)            \
    M(OLAP_FIELD_TYPE_DECIMAL32, TYPE_DECIMAL32)          \
    M(OLAP_FIELD_TYPE_DECIMAL64, TYPE_DECIMAL64)          \
    M(OLAP_FIELD_TYPE_DECIMAL128I, TYPE_DECIMAL128I)      \
    M(OLAP_FIELD_TYPE_DECIMAL256, TYPE_DECIMAL256)        \
    M(OLAP_FIELD_TYPE_DATE, TYPE_DATE)                    \
    M(OLAP_FIELD_TYPE_DATETIME, TYPE_DATETIME)            \
    M(OLAP_FIELD_TYPE_DATEV2, TYPE_DATEV2)                \
    M(OLAP_FIELD_TYPE_DATETIMEV2, TYPE_DATETIMEV2)        \
    M(OLAP_FIELD_TYPE_TIMESTAMPTZ, TYPE_TIMESTAMPTZ)      \
    M(OLAP_FIELD_TYPE_IPV4, TYPE_IPV4)                    \
    M(OLAP_FIELD_TYPE_IPV6, TYPE_IPV6)

} // namespace doris
