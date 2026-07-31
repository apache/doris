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

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "agent/be_exec_version_manager.h"
#include "core/data_type/data_type_agg_state.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_fixed_length_object.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nothing.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/data_type_varbinary.h"
#include "core/data_type/data_type_variant.h"
#include "core/field.h"
#include "core/types.h"
#include "core/value/timestamptz_value.h"
#include "core/value/vdatetime_value.h"

namespace doris {
namespace {

Field get_default_field(const DataTypePtr& data_type) {
    auto column = data_type->create_column();
    if (column->size() != 0) {
        ADD_FAILURE() << "column should be empty before insert_default";
        return Field();
    }

    column->insert_default();
    if (column->size() != 1) {
        ADD_FAILURE() << "column should contain one row after insert_default";
        return Field();
    }

    Field actual;
    column->get(0, actual);
    return actual;
}

TEST(DataTypeInsertDefaultTest, AllTypeFamilies) {
    auto make_simple_type = [](PrimitiveType type) {
        return DataTypeFactory::instance().create_data_type(type, false);
    };

    auto int_type = make_simple_type(TYPE_INT);
    auto nullable_int_type = std::make_shared<DataTypeNullable>(int_type);
    auto nullable_string_type = std::make_shared<DataTypeNullable>(make_simple_type(TYPE_STRING));

    {
        SCOPED_TRACE("bool");
        auto actual = get_default_field(make_simple_type(TYPE_BOOLEAN));
        ASSERT_EQ(actual.get_type(), TYPE_BOOLEAN);
        EXPECT_EQ(actual.get<TYPE_BOOLEAN>(), UInt8(0));
    }

    {
        SCOPED_TRACE("tinyint");
        auto actual = get_default_field(make_simple_type(TYPE_TINYINT));
        ASSERT_EQ(actual.get_type(), TYPE_TINYINT);
        EXPECT_EQ(actual.get<TYPE_TINYINT>(), Int8(0));
    }

    {
        SCOPED_TRACE("smallint");
        auto actual = get_default_field(make_simple_type(TYPE_SMALLINT));
        ASSERT_EQ(actual.get_type(), TYPE_SMALLINT);
        EXPECT_EQ(actual.get<TYPE_SMALLINT>(), Int16(0));
    }

    {
        SCOPED_TRACE("int");
        auto actual = get_default_field(make_simple_type(TYPE_INT));
        ASSERT_EQ(actual.get_type(), TYPE_INT);
        EXPECT_EQ(actual.get<TYPE_INT>(), Int32(0));
    }

    {
        SCOPED_TRACE("bigint");
        auto actual = get_default_field(make_simple_type(TYPE_BIGINT));
        ASSERT_EQ(actual.get_type(), TYPE_BIGINT);
        EXPECT_EQ(actual.get<TYPE_BIGINT>(), Int64(0));
    }

    {
        SCOPED_TRACE("largeint");
        auto actual = get_default_field(make_simple_type(TYPE_LARGEINT));
        ASSERT_EQ(actual.get_type(), TYPE_LARGEINT);
        EXPECT_EQ(actual.get<TYPE_LARGEINT>(), Int128(0));
    }

    {
        SCOPED_TRACE("float");
        auto actual = get_default_field(make_simple_type(TYPE_FLOAT));
        ASSERT_EQ(actual.get_type(), TYPE_FLOAT);
        EXPECT_EQ(actual.get<TYPE_FLOAT>(), Float32(0));
    }

    {
        SCOPED_TRACE("double");
        auto actual = get_default_field(make_simple_type(TYPE_DOUBLE));
        ASSERT_EQ(actual.get_type(), TYPE_DOUBLE);
        EXPECT_EQ(actual.get<TYPE_DOUBLE>(), Float64(0));
    }

    {
        SCOPED_TRACE("decimalv2");
        auto actual = get_default_field(
                DataTypeFactory::instance().create_data_type(TYPE_DECIMALV2, false, 27, 9));
        ASSERT_EQ(actual.get_type(), TYPE_DECIMALV2);
        EXPECT_EQ(actual.get<TYPE_DECIMALV2>(), DecimalV2Value());
    }

    {
        SCOPED_TRACE("decimal32");
        auto actual = get_default_field(
                DataTypeFactory::instance().create_data_type(TYPE_DECIMAL32, false, 9, 2));
        ASSERT_EQ(actual.get_type(), TYPE_DECIMAL32);
        EXPECT_EQ(actual.get<TYPE_DECIMAL32>(), Decimal32(0));
    }

    {
        SCOPED_TRACE("decimal64");
        auto actual = get_default_field(
                DataTypeFactory::instance().create_data_type(TYPE_DECIMAL64, false, 18, 9));
        ASSERT_EQ(actual.get_type(), TYPE_DECIMAL64);
        EXPECT_EQ(actual.get<TYPE_DECIMAL64>(), Decimal64(0));
    }

    {
        SCOPED_TRACE("decimal128");
        auto actual = get_default_field(
                DataTypeFactory::instance().create_data_type(TYPE_DECIMAL128I, false, 38, 18));
        ASSERT_EQ(actual.get_type(), TYPE_DECIMAL128I);
        EXPECT_EQ(actual.get<TYPE_DECIMAL128I>(), Decimal128V3(0));
    }

    {
        SCOPED_TRACE("decimal256");
        auto actual = get_default_field(
                DataTypeFactory::instance().create_data_type(TYPE_DECIMAL256, false, 76, 18));
        ASSERT_EQ(actual.get_type(), TYPE_DECIMAL256);
        EXPECT_EQ(actual.get<TYPE_DECIMAL256>(), Decimal256(0));
    }

    {
        SCOPED_TRACE("date");
        auto actual = get_default_field(make_simple_type(TYPE_DATE));
        ASSERT_EQ(actual.get_type(), TYPE_DATE);
        EXPECT_EQ(actual.get<TYPE_DATE>(), VecDateTimeValue::DEFAULT_VALUE);
    }

    {
        SCOPED_TRACE("datetime");
        auto actual = get_default_field(make_simple_type(TYPE_DATETIME));
        ASSERT_EQ(actual.get_type(), TYPE_DATETIME);
        EXPECT_EQ(actual.get<TYPE_DATETIME>(), VecDateTimeValue::DEFAULT_VALUE);
    }

    {
        SCOPED_TRACE("datev2");
        auto actual = get_default_field(make_simple_type(TYPE_DATEV2));
        ASSERT_EQ(actual.get_type(), TYPE_DATEV2);
        EXPECT_EQ(actual.get<TYPE_DATEV2>(), DateV2Value<DateV2ValueType>::DEFAULT_VALUE);
    }

    {
        SCOPED_TRACE("datetimev2");
        auto actual = get_default_field(
                DataTypeFactory::instance().create_data_type(TYPE_DATETIMEV2, false, 0, 6));
        ASSERT_EQ(actual.get_type(), TYPE_DATETIMEV2);
        EXPECT_EQ(actual.get<TYPE_DATETIMEV2>(), DateV2Value<DateTimeV2ValueType>::DEFAULT_VALUE);
    }

    {
        SCOPED_TRACE("timev2");
        auto actual = get_default_field(
                DataTypeFactory::instance().create_data_type(TYPE_TIMEV2, false, 0, 6));
        ASSERT_EQ(actual.get_type(), TYPE_TIMEV2);
        EXPECT_EQ(actual.get<TYPE_TIMEV2>(), Float64(0));
    }

    {
        SCOPED_TRACE("timestamptz");
        auto actual = get_default_field(
                DataTypeFactory::instance().create_data_type(TYPE_TIMESTAMPTZ, false, 0, 6));
        ASSERT_EQ(actual.get_type(), TYPE_TIMESTAMPTZ);
        EXPECT_EQ(actual.get<TYPE_TIMESTAMPTZ>(), TimestampTzValue::DEFAULT_VALUE);
    }

    {
        SCOPED_TRACE("ipv4");
        auto actual = get_default_field(make_simple_type(TYPE_IPV4));
        ASSERT_EQ(actual.get_type(), TYPE_IPV4);
        EXPECT_EQ(actual.get<TYPE_IPV4>(), IPv4(0));
    }

    {
        SCOPED_TRACE("ipv6");
        auto actual = get_default_field(make_simple_type(TYPE_IPV6));
        ASSERT_EQ(actual.get_type(), TYPE_IPV6);
        EXPECT_EQ(actual.get<TYPE_IPV6>(), IPv6(0));
    }

    {
        SCOPED_TRACE("char");
        auto actual = get_default_field(
                DataTypeFactory::instance().create_data_type(TYPE_CHAR, false, 8, 0));
        ASSERT_EQ(actual.get_type(), TYPE_STRING);
        EXPECT_EQ(actual.get<TYPE_STRING>(), String());
    }

    {
        SCOPED_TRACE("varchar");
        auto actual = get_default_field(
                DataTypeFactory::instance().create_data_type(TYPE_VARCHAR, false, 32, 0));
        ASSERT_EQ(actual.get_type(), TYPE_STRING);
        EXPECT_EQ(actual.get<TYPE_STRING>(), String());
    }

    {
        SCOPED_TRACE("string");
        auto actual = get_default_field(make_simple_type(TYPE_STRING));
        ASSERT_EQ(actual.get_type(), TYPE_STRING);
        EXPECT_EQ(actual.get<TYPE_STRING>(), String());
    }

    {
        SCOPED_TRACE("varbinary");
        auto actual = get_default_field(std::make_shared<DataTypeVarbinary>());
        ASSERT_EQ(actual.get_type(), TYPE_VARBINARY);
        EXPECT_EQ(actual.get<TYPE_VARBINARY>(), StringView());
    }

    {
        SCOPED_TRACE("jsonb");
        auto actual = get_default_field(make_simple_type(TYPE_JSONB));
        ASSERT_TRUE(actual.get_type() == TYPE_JSONB || is_string_type(actual.get_type()));
        if (actual.get_type() == TYPE_JSONB) {
            EXPECT_EQ(actual.get<TYPE_JSONB>().get_size(), 0);
        } else {
            EXPECT_TRUE(actual.get<TYPE_STRING>().empty());
        }
    }

    {
        SCOPED_TRACE("bitmap");
        auto actual = get_default_field(make_simple_type(TYPE_BITMAP));
        ASSERT_EQ(actual.get_type(), TYPE_BITMAP);
        EXPECT_EQ(actual.get<TYPE_BITMAP>().cardinality(), 0);
    }

    {
        SCOPED_TRACE("hll");
        auto actual = get_default_field(make_simple_type(TYPE_HLL));
        ASSERT_EQ(actual.get_type(), TYPE_HLL);
        EXPECT_EQ(actual.get<TYPE_HLL>().estimate_cardinality(), 0);
    }

    {
        SCOPED_TRACE("quantile_state");
        auto actual = get_default_field(make_simple_type(TYPE_QUANTILE_STATE));
        QuantileState empty_state;
        ASSERT_EQ(actual.get_type(), TYPE_QUANTILE_STATE);
        EXPECT_EQ(actual.get<TYPE_QUANTILE_STATE>().get_serialized_size(),
                  empty_state.get_serialized_size());
    }

    {
        SCOPED_TRACE("variant");
        auto actual = get_default_field(std::make_shared<DataTypeVariant>());
        ASSERT_EQ(actual.get_type(), TYPE_NULL);
        EXPECT_TRUE(actual.is_null());
    }

    {
        SCOPED_TRACE("nothing");
        auto actual = get_default_field(std::make_shared<DataTypeNothing>());
        ASSERT_EQ(actual.get_type(), TYPE_NULL);
        EXPECT_TRUE(actual.is_null());
    }

    {
        SCOPED_TRACE("nullable<int>");
        auto actual = get_default_field(nullable_int_type);
        ASSERT_EQ(actual.get_type(), TYPE_NULL);
        EXPECT_TRUE(actual.is_null());
    }

    {
        SCOPED_TRACE("array<nullable<int>>");
        auto actual = get_default_field(std::make_shared<DataTypeArray>(nullable_int_type));
        ASSERT_EQ(actual.get_type(), TYPE_ARRAY);
        EXPECT_TRUE(actual.get<TYPE_ARRAY>().empty());
    }

    {
        SCOPED_TRACE("map<nullable<string>,nullable<int>>");
        auto actual = get_default_field(
                std::make_shared<DataTypeMap>(nullable_string_type, nullable_int_type));
        ASSERT_EQ(actual.get_type(), TYPE_MAP);
        const auto& map = actual.get<TYPE_MAP>();
        ASSERT_EQ(map.size(), 2);
        EXPECT_TRUE(map[0].get<TYPE_ARRAY>().empty());
        EXPECT_TRUE(map[1].get<TYPE_ARRAY>().empty());
    }

    {
        SCOPED_TRACE("struct<nullable<int>,nullable<string>>");
        auto actual = get_default_field(std::make_shared<DataTypeStruct>(
                DataTypes {nullable_int_type, nullable_string_type}));
        ASSERT_EQ(actual.get_type(), TYPE_STRUCT);
        const auto& tuple = actual.get<TYPE_STRUCT>();
        ASSERT_EQ(tuple.size(), 2);
        EXPECT_TRUE(tuple[0].is_null());
        EXPECT_TRUE(tuple[1].is_null());
    }

    {
        SCOPED_TRACE("agg_state<count>");
        auto actual = get_default_field(std::make_shared<DataTypeAggState>(
                DataTypes {int_type}, false, "count", BeExecVersionManager::get_newest_version()));
        ASSERT_EQ(actual.get_type(), TYPE_STRING);
        EXPECT_EQ(actual.get<TYPE_STRING>(), String(8, '\0'));
    }
}

} // namespace
} // namespace doris