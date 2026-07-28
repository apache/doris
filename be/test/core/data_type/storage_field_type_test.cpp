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

#include <gtest/gtest.h>

#include <array>
#include <cstddef>

#include "common/exception.h"
#include "storage/olap_common.h"

namespace doris {

namespace {

struct TypePair {
    PrimitiveType primitive_type;
    FieldType field_type;
};

constexpr std::array supported_mappings {
        TypePair {PrimitiveType::INVALID_TYPE, FieldType::OLAP_FIELD_TYPE_UNKNOWN},
        TypePair {PrimitiveType::TYPE_NULL, FieldType::OLAP_FIELD_TYPE_NONE},
        TypePair {PrimitiveType::TYPE_BOOLEAN, FieldType::OLAP_FIELD_TYPE_BOOL},
        TypePair {PrimitiveType::TYPE_TINYINT, FieldType::OLAP_FIELD_TYPE_TINYINT},
        TypePair {PrimitiveType::TYPE_SMALLINT, FieldType::OLAP_FIELD_TYPE_SMALLINT},
        TypePair {PrimitiveType::TYPE_INT, FieldType::OLAP_FIELD_TYPE_INT},
        TypePair {PrimitiveType::TYPE_BIGINT, FieldType::OLAP_FIELD_TYPE_BIGINT},
        TypePair {PrimitiveType::TYPE_LARGEINT, FieldType::OLAP_FIELD_TYPE_LARGEINT},
        TypePair {PrimitiveType::TYPE_FLOAT, FieldType::OLAP_FIELD_TYPE_FLOAT},
        TypePair {PrimitiveType::TYPE_DOUBLE, FieldType::OLAP_FIELD_TYPE_DOUBLE},
        TypePair {PrimitiveType::TYPE_VARCHAR, FieldType::OLAP_FIELD_TYPE_VARCHAR},
        TypePair {PrimitiveType::TYPE_DATE, FieldType::OLAP_FIELD_TYPE_DATE},
        TypePair {PrimitiveType::TYPE_DATETIME, FieldType::OLAP_FIELD_TYPE_DATETIME},
        TypePair {PrimitiveType::TYPE_CHAR, FieldType::OLAP_FIELD_TYPE_CHAR},
        TypePair {PrimitiveType::TYPE_STRUCT, FieldType::OLAP_FIELD_TYPE_STRUCT},
        TypePair {PrimitiveType::TYPE_ARRAY, FieldType::OLAP_FIELD_TYPE_ARRAY},
        TypePair {PrimitiveType::TYPE_MAP, FieldType::OLAP_FIELD_TYPE_MAP},
        TypePair {PrimitiveType::TYPE_HLL, FieldType::OLAP_FIELD_TYPE_HLL},
        TypePair {PrimitiveType::TYPE_DECIMALV2, FieldType::OLAP_FIELD_TYPE_DECIMAL},
        TypePair {PrimitiveType::TYPE_BITMAP, FieldType::OLAP_FIELD_TYPE_BITMAP},
        TypePair {PrimitiveType::TYPE_STRING, FieldType::OLAP_FIELD_TYPE_STRING},
        TypePair {PrimitiveType::TYPE_QUANTILE_STATE, FieldType::OLAP_FIELD_TYPE_QUANTILE_STATE},
        TypePair {PrimitiveType::TYPE_DATEV2, FieldType::OLAP_FIELD_TYPE_DATEV2},
        TypePair {PrimitiveType::TYPE_DATETIMEV2, FieldType::OLAP_FIELD_TYPE_DATETIMEV2},
        TypePair {PrimitiveType::TYPE_TIMEV2, FieldType::OLAP_FIELD_TYPE_TIMEV2},
        TypePair {PrimitiveType::TYPE_DECIMAL32, FieldType::OLAP_FIELD_TYPE_DECIMAL32},
        TypePair {PrimitiveType::TYPE_DECIMAL64, FieldType::OLAP_FIELD_TYPE_DECIMAL64},
        TypePair {PrimitiveType::TYPE_DECIMAL128I, FieldType::OLAP_FIELD_TYPE_DECIMAL128I},
        TypePair {PrimitiveType::TYPE_JSONB, FieldType::OLAP_FIELD_TYPE_JSONB},
        TypePair {PrimitiveType::TYPE_VARIANT, FieldType::OLAP_FIELD_TYPE_VARIANT},
        TypePair {PrimitiveType::TYPE_AGG_STATE, FieldType::OLAP_FIELD_TYPE_AGG_STATE},
        TypePair {PrimitiveType::TYPE_DECIMAL256, FieldType::OLAP_FIELD_TYPE_DECIMAL256},
        TypePair {PrimitiveType::TYPE_IPV4, FieldType::OLAP_FIELD_TYPE_IPV4},
        TypePair {PrimitiveType::TYPE_IPV6, FieldType::OLAP_FIELD_TYPE_IPV6},
        TypePair {PrimitiveType::TYPE_UINT32, FieldType::OLAP_FIELD_TYPE_UNSIGNED_INT},
        TypePair {PrimitiveType::TYPE_UINT64, FieldType::OLAP_FIELD_TYPE_UNSIGNED_BIGINT},
        TypePair {PrimitiveType::TYPE_TIMESTAMPTZ, FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ},
};

TEST(StorageFieldTypeTest, SupportedMappingsRoundTrip) {
    for (const auto& [primitive_type, field_type] : supported_mappings) {
        SCOPED_TRACE(static_cast<int>(primitive_type));
        EXPECT_EQ(primitive_type_to_storage_field_type(primitive_type), field_type);
        EXPECT_EQ(storage_field_type_to_primitive_type(field_type), primitive_type);
    }
}

TEST(StorageFieldTypeTest, UnsupportedPrimitiveTypesThrow) {
    constexpr std::array unsupported_types {
            PrimitiveType::TYPE_BINARY,
            static_cast<PrimitiveType>(14), // TYPE_DECIMAL (deprecated)
            static_cast<PrimitiveType>(21), // TYPE_TIME (deprecated)
            static_cast<PrimitiveType>(33), // TYPE_LAMBDA_FUNCTION (deprecated)
            PrimitiveType::TYPE_FIXED_LENGTH_OBJECT,
            PrimitiveType::TYPE_VARBINARY,
            static_cast<PrimitiveType>(43),
            static_cast<PrimitiveType>(255),
    };

    for (const auto type : unsupported_types) {
        SCOPED_TRACE(static_cast<int>(type));
        EXPECT_THROW((void)primitive_type_to_storage_field_type(type), Exception);
    }
}

TEST(StorageFieldTypeTest, UnsupportedOrInvalidFieldTypesThrow) {
    constexpr std::array unsupported_types {
            FieldType::OLAP_FIELD_TYPE_UNSIGNED_TINYINT,
            FieldType::OLAP_FIELD_TYPE_UNSIGNED_SMALLINT,
            FieldType::OLAP_FIELD_TYPE_DISCRETE_DOUBLE,
            static_cast<FieldType>(-1),
            static_cast<FieldType>(0),
            static_cast<FieldType>(41),
            static_cast<FieldType>(255),
    };

    for (const auto type : unsupported_types) {
        SCOPED_TRACE(static_cast<int>(type));
        EXPECT_THROW((void)storage_field_type_to_primitive_type(type), Exception);
    }
}

TEST(StorageFieldTypeTest, PersistedFieldTypeValuesStayStable) {
    constexpr std::array persisted_types {
            FieldType::OLAP_FIELD_TYPE_TINYINT,
            FieldType::OLAP_FIELD_TYPE_UNSIGNED_TINYINT,
            FieldType::OLAP_FIELD_TYPE_SMALLINT,
            FieldType::OLAP_FIELD_TYPE_UNSIGNED_SMALLINT,
            FieldType::OLAP_FIELD_TYPE_INT,
            FieldType::OLAP_FIELD_TYPE_UNSIGNED_INT,
            FieldType::OLAP_FIELD_TYPE_BIGINT,
            FieldType::OLAP_FIELD_TYPE_UNSIGNED_BIGINT,
            FieldType::OLAP_FIELD_TYPE_LARGEINT,
            FieldType::OLAP_FIELD_TYPE_FLOAT,
            FieldType::OLAP_FIELD_TYPE_DOUBLE,
            FieldType::OLAP_FIELD_TYPE_DISCRETE_DOUBLE,
            FieldType::OLAP_FIELD_TYPE_CHAR,
            FieldType::OLAP_FIELD_TYPE_DATE,
            FieldType::OLAP_FIELD_TYPE_DATETIME,
            FieldType::OLAP_FIELD_TYPE_DECIMAL,
            FieldType::OLAP_FIELD_TYPE_VARCHAR,
            FieldType::OLAP_FIELD_TYPE_STRUCT,
            FieldType::OLAP_FIELD_TYPE_ARRAY,
            FieldType::OLAP_FIELD_TYPE_MAP,
            FieldType::OLAP_FIELD_TYPE_UNKNOWN,
            FieldType::OLAP_FIELD_TYPE_NONE,
            FieldType::OLAP_FIELD_TYPE_HLL,
            FieldType::OLAP_FIELD_TYPE_BOOL,
            FieldType::OLAP_FIELD_TYPE_BITMAP,
            FieldType::OLAP_FIELD_TYPE_STRING,
            FieldType::OLAP_FIELD_TYPE_QUANTILE_STATE,
            FieldType::OLAP_FIELD_TYPE_DATEV2,
            FieldType::OLAP_FIELD_TYPE_DATETIMEV2,
            FieldType::OLAP_FIELD_TYPE_TIMEV2,
            FieldType::OLAP_FIELD_TYPE_DECIMAL32,
            FieldType::OLAP_FIELD_TYPE_DECIMAL64,
            FieldType::OLAP_FIELD_TYPE_DECIMAL128I,
            FieldType::OLAP_FIELD_TYPE_JSONB,
            FieldType::OLAP_FIELD_TYPE_VARIANT,
            FieldType::OLAP_FIELD_TYPE_AGG_STATE,
            FieldType::OLAP_FIELD_TYPE_DECIMAL256,
            FieldType::OLAP_FIELD_TYPE_IPV4,
            FieldType::OLAP_FIELD_TYPE_IPV6,
            FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ,
    };

    for (size_t i = 0; i < persisted_types.size(); ++i) {
        EXPECT_EQ(static_cast<size_t>(persisted_types[i]), i + 1);
    }
}

} // namespace

} // namespace doris
