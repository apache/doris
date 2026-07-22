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

#include "core/data_type/storage_field_type.h"

#include "common/exception.h"
#include "storage/olap_common.h"

namespace doris {

// NOLINTNEXTLINE(readability-function-size): keep the exhaustive mapping together for auditability.
FieldType primitive_type_to_storage_field_type(PrimitiveType type) {
    switch (type) {
    case PrimitiveType::INVALID_TYPE:
        return FieldType::OLAP_FIELD_TYPE_UNKNOWN;
    case PrimitiveType::TYPE_NULL:
        return FieldType::OLAP_FIELD_TYPE_NONE;
    case PrimitiveType::TYPE_BOOLEAN:
        return FieldType::OLAP_FIELD_TYPE_BOOL;
    case PrimitiveType::TYPE_TINYINT:
        return FieldType::OLAP_FIELD_TYPE_TINYINT;
    case PrimitiveType::TYPE_SMALLINT:
        return FieldType::OLAP_FIELD_TYPE_SMALLINT;
    case PrimitiveType::TYPE_INT:
        return FieldType::OLAP_FIELD_TYPE_INT;
    case PrimitiveType::TYPE_BIGINT:
        return FieldType::OLAP_FIELD_TYPE_BIGINT;
    case PrimitiveType::TYPE_LARGEINT:
        return FieldType::OLAP_FIELD_TYPE_LARGEINT;
    case PrimitiveType::TYPE_FLOAT:
        return FieldType::OLAP_FIELD_TYPE_FLOAT;
    case PrimitiveType::TYPE_DOUBLE:
        return FieldType::OLAP_FIELD_TYPE_DOUBLE;
    case PrimitiveType::TYPE_VARCHAR:
        return FieldType::OLAP_FIELD_TYPE_VARCHAR;
    case PrimitiveType::TYPE_DATE:
        return FieldType::OLAP_FIELD_TYPE_DATE;
    case PrimitiveType::TYPE_DATETIME:
        return FieldType::OLAP_FIELD_TYPE_DATETIME;
    case PrimitiveType::TYPE_CHAR:
        return FieldType::OLAP_FIELD_TYPE_CHAR;
    case PrimitiveType::TYPE_STRUCT:
        return FieldType::OLAP_FIELD_TYPE_STRUCT;
    case PrimitiveType::TYPE_ARRAY:
        return FieldType::OLAP_FIELD_TYPE_ARRAY;
    case PrimitiveType::TYPE_MAP:
        return FieldType::OLAP_FIELD_TYPE_MAP;
    case PrimitiveType::TYPE_HLL:
        return FieldType::OLAP_FIELD_TYPE_HLL;
    case PrimitiveType::TYPE_DECIMALV2:
        return FieldType::OLAP_FIELD_TYPE_DECIMAL;
    case PrimitiveType::TYPE_BITMAP:
        return FieldType::OLAP_FIELD_TYPE_BITMAP;
    case PrimitiveType::TYPE_STRING:
        return FieldType::OLAP_FIELD_TYPE_STRING;
    case PrimitiveType::TYPE_QUANTILE_STATE:
        return FieldType::OLAP_FIELD_TYPE_QUANTILE_STATE;
    case PrimitiveType::TYPE_DATEV2:
        return FieldType::OLAP_FIELD_TYPE_DATEV2;
    case PrimitiveType::TYPE_DATETIMEV2:
        return FieldType::OLAP_FIELD_TYPE_DATETIMEV2;
    case PrimitiveType::TYPE_TIMEV2:
        return FieldType::OLAP_FIELD_TYPE_TIMEV2;
    case PrimitiveType::TYPE_DECIMAL32:
        return FieldType::OLAP_FIELD_TYPE_DECIMAL32;
    case PrimitiveType::TYPE_DECIMAL64:
        return FieldType::OLAP_FIELD_TYPE_DECIMAL64;
    case PrimitiveType::TYPE_DECIMAL128I:
        return FieldType::OLAP_FIELD_TYPE_DECIMAL128I;
    case PrimitiveType::TYPE_JSONB:
        return FieldType::OLAP_FIELD_TYPE_JSONB;
    case PrimitiveType::TYPE_VARIANT:
        return FieldType::OLAP_FIELD_TYPE_VARIANT;
    case PrimitiveType::TYPE_AGG_STATE:
        return FieldType::OLAP_FIELD_TYPE_AGG_STATE;
    case PrimitiveType::TYPE_DECIMAL256:
        return FieldType::OLAP_FIELD_TYPE_DECIMAL256;
    case PrimitiveType::TYPE_IPV4:
        return FieldType::OLAP_FIELD_TYPE_IPV4;
    case PrimitiveType::TYPE_IPV6:
        return FieldType::OLAP_FIELD_TYPE_IPV6;
    case PrimitiveType::TYPE_UINT32:
        return FieldType::OLAP_FIELD_TYPE_UNSIGNED_INT;
    case PrimitiveType::TYPE_UINT64:
        return FieldType::OLAP_FIELD_TYPE_UNSIGNED_BIGINT;
    case PrimitiveType::TYPE_TIMESTAMPTZ:
        return FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ;
    case PrimitiveType::TYPE_BINARY:
    case static_cast<PrimitiveType>(14): // TYPE_DECIMAL (deprecated)
    case static_cast<PrimitiveType>(21): // TYPE_TIME (deprecated)
    case static_cast<PrimitiveType>(33): // TYPE_LAMBDA_FUNCTION (deprecated)
    case PrimitiveType::TYPE_FIXED_LENGTH_OBJECT:
    case PrimitiveType::TYPE_VARBINARY:
        break;
    }

    throw Exception(ErrorCode::INTERNAL_ERROR, "Cannot convert PrimitiveType {} to FieldType",
                    static_cast<int>(type));
}

// NOLINTNEXTLINE(readability-function-size): keep the exhaustive mapping together for auditability.
PrimitiveType storage_field_type_to_primitive_type(FieldType type) {
    switch (type) {
    case FieldType::OLAP_FIELD_TYPE_TINYINT:
        return PrimitiveType::TYPE_TINYINT;
    case FieldType::OLAP_FIELD_TYPE_SMALLINT:
        return PrimitiveType::TYPE_SMALLINT;
    case FieldType::OLAP_FIELD_TYPE_INT:
        return PrimitiveType::TYPE_INT;
    case FieldType::OLAP_FIELD_TYPE_UNSIGNED_INT:
        return PrimitiveType::TYPE_UINT32;
    case FieldType::OLAP_FIELD_TYPE_BIGINT:
        return PrimitiveType::TYPE_BIGINT;
    case FieldType::OLAP_FIELD_TYPE_UNSIGNED_BIGINT:
        return PrimitiveType::TYPE_UINT64;
    case FieldType::OLAP_FIELD_TYPE_LARGEINT:
        return PrimitiveType::TYPE_LARGEINT;
    case FieldType::OLAP_FIELD_TYPE_FLOAT:
        return PrimitiveType::TYPE_FLOAT;
    case FieldType::OLAP_FIELD_TYPE_DOUBLE:
        return PrimitiveType::TYPE_DOUBLE;
    case FieldType::OLAP_FIELD_TYPE_CHAR:
        return PrimitiveType::TYPE_CHAR;
    case FieldType::OLAP_FIELD_TYPE_DATE:
        return PrimitiveType::TYPE_DATE;
    case FieldType::OLAP_FIELD_TYPE_DATETIME:
        return PrimitiveType::TYPE_DATETIME;
    case FieldType::OLAP_FIELD_TYPE_DECIMAL:
        return PrimitiveType::TYPE_DECIMALV2;
    case FieldType::OLAP_FIELD_TYPE_VARCHAR:
        return PrimitiveType::TYPE_VARCHAR;
    case FieldType::OLAP_FIELD_TYPE_STRUCT:
        return PrimitiveType::TYPE_STRUCT;
    case FieldType::OLAP_FIELD_TYPE_ARRAY:
        return PrimitiveType::TYPE_ARRAY;
    case FieldType::OLAP_FIELD_TYPE_MAP:
        return PrimitiveType::TYPE_MAP;
    case FieldType::OLAP_FIELD_TYPE_UNKNOWN:
        return PrimitiveType::INVALID_TYPE;
    case FieldType::OLAP_FIELD_TYPE_NONE:
        return PrimitiveType::TYPE_NULL;
    case FieldType::OLAP_FIELD_TYPE_HLL:
        return PrimitiveType::TYPE_HLL;
    case FieldType::OLAP_FIELD_TYPE_BOOL:
        return PrimitiveType::TYPE_BOOLEAN;
    case FieldType::OLAP_FIELD_TYPE_BITMAP:
        return PrimitiveType::TYPE_BITMAP;
    case FieldType::OLAP_FIELD_TYPE_STRING:
        return PrimitiveType::TYPE_STRING;
    case FieldType::OLAP_FIELD_TYPE_QUANTILE_STATE:
        return PrimitiveType::TYPE_QUANTILE_STATE;
    case FieldType::OLAP_FIELD_TYPE_DATEV2:
        return PrimitiveType::TYPE_DATEV2;
    case FieldType::OLAP_FIELD_TYPE_DATETIMEV2:
        return PrimitiveType::TYPE_DATETIMEV2;
    case FieldType::OLAP_FIELD_TYPE_TIMEV2:
        return PrimitiveType::TYPE_TIMEV2;
    case FieldType::OLAP_FIELD_TYPE_DECIMAL32:
        return PrimitiveType::TYPE_DECIMAL32;
    case FieldType::OLAP_FIELD_TYPE_DECIMAL64:
        return PrimitiveType::TYPE_DECIMAL64;
    case FieldType::OLAP_FIELD_TYPE_DECIMAL128I:
        return PrimitiveType::TYPE_DECIMAL128I;
    case FieldType::OLAP_FIELD_TYPE_JSONB:
        return PrimitiveType::TYPE_JSONB;
    case FieldType::OLAP_FIELD_TYPE_VARIANT:
        return PrimitiveType::TYPE_VARIANT;
    case FieldType::OLAP_FIELD_TYPE_AGG_STATE:
        return PrimitiveType::TYPE_AGG_STATE;
    case FieldType::OLAP_FIELD_TYPE_DECIMAL256:
        return PrimitiveType::TYPE_DECIMAL256;
    case FieldType::OLAP_FIELD_TYPE_IPV4:
        return PrimitiveType::TYPE_IPV4;
    case FieldType::OLAP_FIELD_TYPE_IPV6:
        return PrimitiveType::TYPE_IPV6;
    case FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ:
        return PrimitiveType::TYPE_TIMESTAMPTZ;
    case FieldType::OLAP_FIELD_TYPE_UNSIGNED_TINYINT:
    case FieldType::OLAP_FIELD_TYPE_UNSIGNED_SMALLINT:
    case FieldType::OLAP_FIELD_TYPE_DISCRETE_DOUBLE:
        break;
    }

    throw Exception(ErrorCode::INTERNAL_ERROR, "Cannot convert FieldType {} to PrimitiveType",
                    static_cast<int>(type));
}

} // namespace doris
