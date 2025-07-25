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

#include "olap/tablet_schema.h"

#include <gtest/gtest.h>

namespace doris {

class TestSchemaTest : public testing::Test {
public:
    TestSchemaTest() = default;
    ~TestSchemaTest() override = default;

    void SetUp() override {}

    void TearDown() override {}
};

TEST_F(TestSchemaTest, TestGetPrimitiveTypeByFieldType) {
    EXPECT_EQ(PrimitiveType::TYPE_TINYINT,
              TabletColumn::get_primitive_type_by_field_type(FieldType::OLAP_FIELD_TYPE_TINYINT));
    EXPECT_EQ(PrimitiveType::TYPE_SMALLINT,
              TabletColumn::get_primitive_type_by_field_type(FieldType::OLAP_FIELD_TYPE_SMALLINT));
    EXPECT_EQ(PrimitiveType::TYPE_INT,
              TabletColumn::get_primitive_type_by_field_type(FieldType::OLAP_FIELD_TYPE_INT));
    EXPECT_EQ(PrimitiveType::TYPE_BIGINT,
              TabletColumn::get_primitive_type_by_field_type(FieldType::OLAP_FIELD_TYPE_BIGINT));
    EXPECT_EQ(PrimitiveType::TYPE_LARGEINT,
              TabletColumn::get_primitive_type_by_field_type(FieldType::OLAP_FIELD_TYPE_LARGEINT));
    EXPECT_EQ(PrimitiveType::TYPE_FLOAT,
              TabletColumn::get_primitive_type_by_field_type(FieldType::OLAP_FIELD_TYPE_FLOAT));
    EXPECT_EQ(PrimitiveType::TYPE_DOUBLE,
              TabletColumn::get_primitive_type_by_field_type(FieldType::OLAP_FIELD_TYPE_DOUBLE));
    EXPECT_EQ(PrimitiveType::TYPE_BOOLEAN,
              TabletColumn::get_primitive_type_by_field_type(FieldType::OLAP_FIELD_TYPE_BOOL));
    EXPECT_EQ(PrimitiveType::TYPE_DATE,
              TabletColumn::get_primitive_type_by_field_type(FieldType::OLAP_FIELD_TYPE_DATE));
    EXPECT_EQ(PrimitiveType::TYPE_DATETIME,
              TabletColumn::get_primitive_type_by_field_type(FieldType::OLAP_FIELD_TYPE_DATETIME));
    EXPECT_EQ(PrimitiveType::TYPE_DATEV2,
              TabletColumn::get_primitive_type_by_field_type(FieldType::OLAP_FIELD_TYPE_DATEV2));
    EXPECT_EQ(PrimitiveType::TYPE_DATETIMEV2, TabletColumn::get_primitive_type_by_field_type(
                                                      FieldType::OLAP_FIELD_TYPE_DATETIMEV2));
    EXPECT_EQ(PrimitiveType::TYPE_TIMEV2,
              TabletColumn::get_primitive_type_by_field_type(FieldType::OLAP_FIELD_TYPE_TIMEV2));
    EXPECT_EQ(PrimitiveType::TYPE_CHAR,
              TabletColumn::get_primitive_type_by_field_type(FieldType::OLAP_FIELD_TYPE_CHAR));
    EXPECT_EQ(PrimitiveType::TYPE_STRING,
              TabletColumn::get_primitive_type_by_field_type(FieldType::OLAP_FIELD_TYPE_STRING));
    EXPECT_EQ(PrimitiveType::TYPE_VARCHAR,
              TabletColumn::get_primitive_type_by_field_type(FieldType::OLAP_FIELD_TYPE_VARCHAR));
    EXPECT_EQ(PrimitiveType::TYPE_HLL,
              TabletColumn::get_primitive_type_by_field_type(FieldType::OLAP_FIELD_TYPE_HLL));
    EXPECT_EQ(PrimitiveType::TYPE_MAP,
              TabletColumn::get_primitive_type_by_field_type(FieldType::OLAP_FIELD_TYPE_MAP));
    EXPECT_EQ(PrimitiveType::TYPE_ARRAY,
              TabletColumn::get_primitive_type_by_field_type(FieldType::OLAP_FIELD_TYPE_ARRAY));
    EXPECT_EQ(PrimitiveType::TYPE_STRUCT,
              TabletColumn::get_primitive_type_by_field_type(FieldType::OLAP_FIELD_TYPE_STRUCT));
    EXPECT_EQ(PrimitiveType::TYPE_DECIMAL32,
              TabletColumn::get_primitive_type_by_field_type(FieldType::OLAP_FIELD_TYPE_DECIMAL32));
    EXPECT_EQ(PrimitiveType::TYPE_DECIMAL64,
              TabletColumn::get_primitive_type_by_field_type(FieldType::OLAP_FIELD_TYPE_DECIMAL64));
    EXPECT_EQ(PrimitiveType::TYPE_DECIMAL128I, TabletColumn::get_primitive_type_by_field_type(
                                                       FieldType::OLAP_FIELD_TYPE_DECIMAL128I));
    EXPECT_EQ(PrimitiveType::TYPE_DECIMAL256, TabletColumn::get_primitive_type_by_field_type(
                                                      FieldType::OLAP_FIELD_TYPE_DECIMAL256));
    EXPECT_EQ(PrimitiveType::TYPE_IPV4,
              TabletColumn::get_primitive_type_by_field_type(FieldType::OLAP_FIELD_TYPE_IPV4));
    EXPECT_EQ(PrimitiveType::TYPE_IPV6,
              TabletColumn::get_primitive_type_by_field_type(FieldType::OLAP_FIELD_TYPE_IPV6));
    EXPECT_EQ(PrimitiveType::TYPE_JSONB,
              TabletColumn::get_primitive_type_by_field_type(FieldType::OLAP_FIELD_TYPE_JSONB));
    EXPECT_EQ(PrimitiveType::TYPE_VARIANT,
              TabletColumn::get_primitive_type_by_field_type(FieldType::OLAP_FIELD_TYPE_VARIANT));
    EXPECT_EQ(PrimitiveType::TYPE_AGG_STATE,
              TabletColumn::get_primitive_type_by_field_type(FieldType::OLAP_FIELD_TYPE_AGG_STATE));
    EXPECT_EQ(PrimitiveType::INVALID_TYPE,
              TabletColumn::get_primitive_type_by_field_type(FieldType::OLAP_FIELD_TYPE_UNKNOWN));
}

TEST_F(TestSchemaTest, TestGetFieldTypeByPrimitiveType) {
    EXPECT_EQ(FieldType::OLAP_FIELD_TYPE_TINYINT,
              TabletColumn::get_field_type_by_type(PrimitiveType::TYPE_TINYINT));
    EXPECT_EQ(FieldType::OLAP_FIELD_TYPE_SMALLINT,
              TabletColumn::get_field_type_by_type(PrimitiveType::TYPE_SMALLINT));
    EXPECT_EQ(FieldType::OLAP_FIELD_TYPE_INT,
              TabletColumn::get_field_type_by_type(PrimitiveType::TYPE_INT));
    EXPECT_EQ(FieldType::OLAP_FIELD_TYPE_BIGINT,
              TabletColumn::get_field_type_by_type(PrimitiveType::TYPE_BIGINT));
    EXPECT_EQ(FieldType::OLAP_FIELD_TYPE_LARGEINT,
              TabletColumn::get_field_type_by_type(PrimitiveType::TYPE_LARGEINT));
    EXPECT_EQ(FieldType::OLAP_FIELD_TYPE_FLOAT,
              TabletColumn::get_field_type_by_type(PrimitiveType::TYPE_FLOAT));
    EXPECT_EQ(FieldType::OLAP_FIELD_TYPE_DOUBLE,
              TabletColumn::get_field_type_by_type(PrimitiveType::TYPE_DOUBLE));
    EXPECT_EQ(FieldType::OLAP_FIELD_TYPE_BOOL,
              TabletColumn::get_field_type_by_type(PrimitiveType::TYPE_BOOLEAN));
    EXPECT_EQ(FieldType::OLAP_FIELD_TYPE_DATE,
              TabletColumn::get_field_type_by_type(PrimitiveType::TYPE_DATE));
    EXPECT_EQ(FieldType::OLAP_FIELD_TYPE_DATETIME,
              TabletColumn::get_field_type_by_type(PrimitiveType::TYPE_DATETIME));
    EXPECT_EQ(FieldType::OLAP_FIELD_TYPE_DATEV2,
              TabletColumn::get_field_type_by_type(PrimitiveType::TYPE_DATEV2));
    EXPECT_EQ(FieldType::OLAP_FIELD_TYPE_DATETIMEV2,
              TabletColumn::get_field_type_by_type(PrimitiveType::TYPE_DATETIMEV2));
    EXPECT_EQ(FieldType::OLAP_FIELD_TYPE_TIMEV2,
              TabletColumn::get_field_type_by_type(PrimitiveType::TYPE_TIMEV2));
    EXPECT_EQ(FieldType::OLAP_FIELD_TYPE_CHAR,
              TabletColumn::get_field_type_by_type(PrimitiveType::TYPE_CHAR));
    EXPECT_EQ(FieldType::OLAP_FIELD_TYPE_STRING,
              TabletColumn::get_field_type_by_type(PrimitiveType::TYPE_STRING));
    EXPECT_EQ(FieldType::OLAP_FIELD_TYPE_VARCHAR,
              TabletColumn::get_field_type_by_type(PrimitiveType::TYPE_VARCHAR));
    EXPECT_EQ(FieldType::OLAP_FIELD_TYPE_HLL,
              TabletColumn::get_field_type_by_type(PrimitiveType::TYPE_HLL));
    EXPECT_EQ(FieldType::OLAP_FIELD_TYPE_MAP,
              TabletColumn::get_field_type_by_type(PrimitiveType::TYPE_MAP));
    EXPECT_EQ(FieldType::OLAP_FIELD_TYPE_ARRAY,
              TabletColumn::get_field_type_by_type(PrimitiveType::TYPE_ARRAY));
    EXPECT_EQ(FieldType::OLAP_FIELD_TYPE_STRUCT,
              TabletColumn::get_field_type_by_type(PrimitiveType::TYPE_STRUCT));
    EXPECT_EQ(FieldType::OLAP_FIELD_TYPE_DECIMAL32,
              TabletColumn::get_field_type_by_type(PrimitiveType::TYPE_DECIMAL32));
    EXPECT_EQ(FieldType::OLAP_FIELD_TYPE_DECIMAL64,
              TabletColumn::get_field_type_by_type(PrimitiveType::TYPE_DECIMAL64));
    EXPECT_EQ(FieldType::OLAP_FIELD_TYPE_DECIMAL128I,
              TabletColumn::get_field_type_by_type(PrimitiveType::TYPE_DECIMAL128I));
    EXPECT_EQ(FieldType::OLAP_FIELD_TYPE_JSONB,
              TabletColumn::get_field_type_by_type(PrimitiveType::TYPE_JSONB));
    EXPECT_EQ(FieldType::OLAP_FIELD_TYPE_VARIANT,
              TabletColumn::get_field_type_by_type(PrimitiveType::TYPE_VARIANT));
    EXPECT_EQ(FieldType::OLAP_FIELD_TYPE_AGG_STATE,
              TabletColumn::get_field_type_by_type(PrimitiveType::TYPE_AGG_STATE));
    EXPECT_EQ(FieldType::OLAP_FIELD_TYPE_UNKNOWN,
              TabletColumn::get_field_type_by_type(PrimitiveType::INVALID_TYPE));
}

} // namespace doris