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

namespace doris {

// Storage-engine cell types, used by TabletColumn / KeyCoder and the
// data_type traits chain. When adding a new value, also extend CppTypeTraits,
// FieldTypeTraits and the field_type_size() switch in storage/types.h. Decide how it maps to
// PrimitiveType and explicitly define its behavior in primitive_type_to_storage_field_type() and
// storage_field_type_to_primitive_type(), either by providing a mapping or by throwing.
enum class FieldType {
    OLAP_FIELD_TYPE_TINYINT = 1, // MYSQL_TYPE_TINY
    OLAP_FIELD_TYPE_UNSIGNED_TINYINT = 2,
    OLAP_FIELD_TYPE_SMALLINT = 3, // MYSQL_TYPE_SHORT
    OLAP_FIELD_TYPE_UNSIGNED_SMALLINT = 4,
    OLAP_FIELD_TYPE_INT = 5, // MYSQL_TYPE_LONG
    OLAP_FIELD_TYPE_UNSIGNED_INT = 6,
    OLAP_FIELD_TYPE_BIGINT = 7, // MYSQL_TYPE_LONGLONG
    OLAP_FIELD_TYPE_UNSIGNED_BIGINT = 8,
    OLAP_FIELD_TYPE_LARGEINT = 9,
    OLAP_FIELD_TYPE_FLOAT = 10,  // MYSQL_TYPE_FLOAT
    OLAP_FIELD_TYPE_DOUBLE = 11, // MYSQL_TYPE_DOUBLE
    OLAP_FIELD_TYPE_DISCRETE_DOUBLE = 12,
    OLAP_FIELD_TYPE_CHAR = 13,     // MYSQL_TYPE_STRING
    OLAP_FIELD_TYPE_DATE = 14,     // MySQL_TYPE_NEWDATE
    OLAP_FIELD_TYPE_DATETIME = 15, // MySQL_TYPE_DATETIME
    OLAP_FIELD_TYPE_DECIMAL = 16,  // DECIMAL, using different store format against MySQL
    OLAP_FIELD_TYPE_VARCHAR = 17,

    OLAP_FIELD_TYPE_STRUCT = 18,  // Struct
    OLAP_FIELD_TYPE_ARRAY = 19,   // ARRAY
    OLAP_FIELD_TYPE_MAP = 20,     // Map
    OLAP_FIELD_TYPE_UNKNOWN = 21, // UNKNOW OLAP_FIELD_TYPE_STRING
    OLAP_FIELD_TYPE_NONE = 22,
    OLAP_FIELD_TYPE_HLL = 23,
    OLAP_FIELD_TYPE_BOOL = 24,
    OLAP_FIELD_TYPE_BITMAP = 25,
    OLAP_FIELD_TYPE_STRING = 26,
    OLAP_FIELD_TYPE_QUANTILE_STATE = 27,
    OLAP_FIELD_TYPE_DATEV2 = 28,
    OLAP_FIELD_TYPE_DATETIMEV2 = 29,
    OLAP_FIELD_TYPE_TIMEV2 = 30,
    OLAP_FIELD_TYPE_DECIMAL32 = 31,
    OLAP_FIELD_TYPE_DECIMAL64 = 32,
    OLAP_FIELD_TYPE_DECIMAL128I = 33,
    OLAP_FIELD_TYPE_JSONB = 34,
    OLAP_FIELD_TYPE_VARIANT = 35,
    OLAP_FIELD_TYPE_AGG_STATE = 36,
    OLAP_FIELD_TYPE_DECIMAL256 = 37,
    OLAP_FIELD_TYPE_IPV4 = 38,
    OLAP_FIELD_TYPE_IPV6 = 39,
    OLAP_FIELD_TYPE_TIMESTAMPTZ = 40,
    OLAP_FIELD_TYPE_TIMESTAMP_NS = 41,
};

constexpr bool field_is_slice_type(const FieldType& field_type) {
    return field_type == FieldType::OLAP_FIELD_TYPE_VARCHAR ||
           field_type == FieldType::OLAP_FIELD_TYPE_CHAR ||
           field_type == FieldType::OLAP_FIELD_TYPE_STRING;
}

constexpr bool field_is_decimal_type(const FieldType& field_type) {
    return field_type == FieldType::OLAP_FIELD_TYPE_DECIMAL ||
           field_type == FieldType::OLAP_FIELD_TYPE_DECIMAL32 ||
           field_type == FieldType::OLAP_FIELD_TYPE_DECIMAL64 ||
           field_type == FieldType::OLAP_FIELD_TYPE_DECIMAL128I ||
           field_type == FieldType::OLAP_FIELD_TYPE_DECIMAL256;
}

constexpr bool field_is_numeric_type(const FieldType& field_type) {
    return field_type == FieldType::OLAP_FIELD_TYPE_INT ||
           field_type == FieldType::OLAP_FIELD_TYPE_UNSIGNED_INT ||
           field_type == FieldType::OLAP_FIELD_TYPE_BIGINT ||
           field_type == FieldType::OLAP_FIELD_TYPE_SMALLINT ||
           field_type == FieldType::OLAP_FIELD_TYPE_UNSIGNED_TINYINT ||
           field_type == FieldType::OLAP_FIELD_TYPE_UNSIGNED_SMALLINT ||
           field_type == FieldType::OLAP_FIELD_TYPE_TINYINT ||
           field_type == FieldType::OLAP_FIELD_TYPE_DOUBLE ||
           field_type == FieldType::OLAP_FIELD_TYPE_FLOAT ||
           field_type == FieldType::OLAP_FIELD_TYPE_DATE ||
           field_type == FieldType::OLAP_FIELD_TYPE_DATEV2 ||
           field_type == FieldType::OLAP_FIELD_TYPE_DATETIME ||
           field_type == FieldType::OLAP_FIELD_TYPE_DATETIMEV2 ||
           field_type == FieldType::OLAP_FIELD_TYPE_TIMESTAMP_NS ||
           field_type == FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ ||
           field_type == FieldType::OLAP_FIELD_TYPE_LARGEINT ||
           field_type == FieldType::OLAP_FIELD_TYPE_DECIMAL ||
           field_type == FieldType::OLAP_FIELD_TYPE_DECIMAL32 ||
           field_type == FieldType::OLAP_FIELD_TYPE_DECIMAL64 ||
           field_type == FieldType::OLAP_FIELD_TYPE_DECIMAL128I ||
           field_type == FieldType::OLAP_FIELD_TYPE_DECIMAL256 ||
           field_type == FieldType::OLAP_FIELD_TYPE_BOOL ||
           field_type == FieldType::OLAP_FIELD_TYPE_IPV4 ||
           field_type == FieldType::OLAP_FIELD_TYPE_IPV6;
}

} // namespace doris
