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
#include <string>
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

struct DefaultCase {
    std::string name;
    DataTypePtr data_type;
    Field expected;
};

void assert_field_equals(const Field& actual, const Field& expected);

void assert_string_field_equals(const Field& actual, const Field& expected) {
    EXPECT_EQ(actual.get<TYPE_STRING>(), expected.get<TYPE_STRING>());
}

void assert_array_field_equals(const Field& actual, const Field& expected) {
    const auto& actual_array = actual.get<TYPE_ARRAY>();
    const auto& expected_array = expected.get<TYPE_ARRAY>();
    ASSERT_EQ(actual_array.size(), expected_array.size());
    for (size_t i = 0; i < actual_array.size(); ++i) {
        assert_field_equals(actual_array[i], expected_array[i]);
    }
}

void assert_map_field_equals(const Field& actual, const Field& expected) {
    const auto& actual_fields = actual.get<TYPE_MAP>();
    const auto& expected_fields = expected.get<TYPE_MAP>();
    ASSERT_EQ(actual_fields.size(), expected_fields.size());
    for (size_t i = 0; i < actual_fields.size(); ++i) {
        assert_field_equals(actual_fields[i], expected_fields[i]);
    }
}

void assert_struct_field_equals(const Field& actual, const Field& expected) {
    const auto& actual_fields = actual.get<TYPE_STRUCT>();
    const auto& expected_fields = expected.get<TYPE_STRUCT>();
    ASSERT_EQ(actual_fields.size(), expected_fields.size());
    for (size_t i = 0; i < actual_fields.size(); ++i) {
        assert_field_equals(actual_fields[i], expected_fields[i]);
    }
}

void assert_empty_jsonb_field(const Field& actual) {
    EXPECT_EQ(actual.get<TYPE_JSONB>().get_size(), 0);
}

void assert_empty_complex_state(const Field& actual) {
    switch (actual.get_type()) {
    case TYPE_BITMAP:
        EXPECT_EQ(actual.get<TYPE_BITMAP>().cardinality(), 0);
        return;
    case TYPE_HLL:
        EXPECT_EQ(actual.get<TYPE_HLL>().estimate_cardinality(), 0);
        return;
    case TYPE_QUANTILE_STATE: {
        QuantileState empty_state;
        EXPECT_EQ(actual.get<TYPE_QUANTILE_STATE>().get_serialized_size(),
                  empty_state.get_serialized_size());
        return;
    }
    default:
        FAIL() << "unexpected complex type: " << actual.get_type_name();
    }
}

void assert_field_equals(const Field& actual, const Field& expected) {
    if (expected.get_type() == TYPE_JSONB) {
        ASSERT_TRUE(actual.get_type() == TYPE_JSONB || is_string_type(actual.get_type()));
        if (actual.get_type() == TYPE_JSONB) {
            assert_empty_jsonb_field(actual);
        } else {
            EXPECT_TRUE(actual.get<TYPE_STRING>().empty());
        }
        return;
    }

    if (is_string_type(actual.get_type()) && is_string_type(expected.get_type())) {
        assert_string_field_equals(actual, expected);
        return;
    }

    ASSERT_EQ(actual.get_type(), expected.get_type());

    if (actual.get_type() == TYPE_NULL) {
        EXPECT_TRUE(actual.is_null());
        return;
    }

    if (actual.get_type() == TYPE_ARRAY) {
        assert_array_field_equals(actual, expected);
        return;
    }

    if (actual.get_type() == TYPE_MAP) {
        assert_map_field_equals(actual, expected);
        return;
    }

    if (actual.get_type() == TYPE_STRUCT) {
        assert_struct_field_equals(actual, expected);
        return;
    }

    if (actual.get_type() == TYPE_VARIANT) {
        EXPECT_TRUE(actual.get<TYPE_VARIANT>().empty());
        return;
    }

    if (actual.get_type() == TYPE_BITMAP || actual.get_type() == TYPE_HLL ||
        actual.get_type() == TYPE_QUANTILE_STATE) {
        assert_empty_complex_state(actual);
        return;
    }

    EXPECT_EQ(actual, expected);
}

void assert_insert_default(const DefaultCase& test_case) {
    auto column = test_case.data_type->create_column();
    ASSERT_EQ(column->size(), 0);

    column->insert_default();
    ASSERT_EQ(column->size(), 1);

    Field actual;
    column->get(0, actual);
    assert_field_equals(actual, test_case.expected);
}

TEST(DataTypeInsertDefaultTest, AllTypeFamilies) {
    auto make_simple_type = [](PrimitiveType type) {
        return DataTypeFactory::instance().create_data_type(type, false);
    };

    std::vector<DefaultCase> cases;
    cases.push_back(
            {"bool", make_simple_type(TYPE_BOOLEAN), Field::create_field<TYPE_BOOLEAN>(UInt8(0))});
    cases.push_back({"tinyint", make_simple_type(TYPE_TINYINT),
                     Field::create_field<TYPE_TINYINT>(Int8(0))});
    cases.push_back({"smallint", make_simple_type(TYPE_SMALLINT),
                     Field::create_field<TYPE_SMALLINT>(Int16(0))});
    cases.push_back({"int", make_simple_type(TYPE_INT), Field::create_field<TYPE_INT>(Int32(0))});
    cases.push_back(
            {"bigint", make_simple_type(TYPE_BIGINT), Field::create_field<TYPE_BIGINT>(Int64(0))});
    cases.push_back({"largeint", make_simple_type(TYPE_LARGEINT),
                     Field::create_field<TYPE_LARGEINT>(Int128(0))});
    cases.push_back(
            {"float", make_simple_type(TYPE_FLOAT), Field::create_field<TYPE_FLOAT>(Float32(0))});
    cases.push_back({"double", make_simple_type(TYPE_DOUBLE),
                     Field::create_field<TYPE_DOUBLE>(Float64(0))});
    cases.push_back({"decimalv2",
                     DataTypeFactory::instance().create_data_type(TYPE_DECIMALV2, false, 27, 9),
                     Field::create_field<TYPE_DECIMALV2>(DecimalV2Value())});
    cases.push_back({"decimal32",
                     DataTypeFactory::instance().create_data_type(TYPE_DECIMAL32, false, 9, 2),
                     Field::create_field<TYPE_DECIMAL32>(Decimal32(0))});
    cases.push_back({"decimal64",
                     DataTypeFactory::instance().create_data_type(TYPE_DECIMAL64, false, 18, 9),
                     Field::create_field<TYPE_DECIMAL64>(Decimal64(0))});
    cases.push_back({"decimal128",
                     DataTypeFactory::instance().create_data_type(TYPE_DECIMAL128I, false, 38, 18),
                     Field::create_field<TYPE_DECIMAL128I>(Decimal128V3(0))});
    cases.push_back({"decimal256",
                     DataTypeFactory::instance().create_data_type(TYPE_DECIMAL256, false, 76, 18),
                     Field::create_field<TYPE_DECIMAL256>(Decimal256(0))});
    cases.push_back({"date", make_simple_type(TYPE_DATE),
                     Field::create_field<TYPE_DATE>(VecDateTimeValue::DEFAULT_VALUE)});
    cases.push_back({"datetime", make_simple_type(TYPE_DATETIME),
                     Field::create_field<TYPE_DATETIME>(VecDateTimeValue::DEFAULT_VALUE)});
    cases.push_back(
            {"datev2", make_simple_type(TYPE_DATEV2),
             Field::create_field<TYPE_DATEV2>(DateV2Value<DateV2ValueType>::DEFAULT_VALUE)});
    cases.push_back({"datetimev2",
                     DataTypeFactory::instance().create_data_type(TYPE_DATETIMEV2, false, 0, 6),
                     Field::create_field<TYPE_DATETIMEV2>(
                             DateV2Value<DateTimeV2ValueType>::DEFAULT_VALUE)});
    cases.push_back({"timev2",
                     DataTypeFactory::instance().create_data_type(TYPE_TIMEV2, false, 0, 6),
                     Field::create_field<TYPE_TIMEV2>(Float64(0))});
    cases.push_back({"timestamptz",
                     DataTypeFactory::instance().create_data_type(TYPE_TIMESTAMPTZ, false, 0, 6),
                     Field::create_field<TYPE_TIMESTAMPTZ>(TimestampTzValue::DEFAULT_VALUE)});
    cases.push_back({"ipv4", make_simple_type(TYPE_IPV4), Field::create_field<TYPE_IPV4>(IPv4(0))});
    cases.push_back({"ipv6", make_simple_type(TYPE_IPV6), Field::create_field<TYPE_IPV6>(IPv6(0))});
    cases.push_back({"char", DataTypeFactory::instance().create_data_type(TYPE_CHAR, false, 8, 0),
                     Field::create_field<TYPE_STRING>(String())});
    cases.push_back({"varchar",
                     DataTypeFactory::instance().create_data_type(TYPE_VARCHAR, false, 32, 0),
                     Field::create_field<TYPE_STRING>(String())});
    cases.push_back(
            {"string", make_simple_type(TYPE_STRING), Field::create_field<TYPE_STRING>(String())});
    cases.push_back({"varbinary", std::make_shared<DataTypeVarbinary>(),
                     Field::create_field<TYPE_VARBINARY>(StringView())});
    cases.push_back(
            {"jsonb", make_simple_type(TYPE_JSONB), Field::create_field<TYPE_JSONB>(JsonbField())});
    cases.push_back({"bitmap", make_simple_type(TYPE_BITMAP),
                     Field::create_field<TYPE_BITMAP>(BitmapValue::empty_bitmap())});
    cases.push_back({"hll", make_simple_type(TYPE_HLL),
                     Field::create_field<TYPE_HLL>(HyperLogLog::empty())});
    cases.push_back({"quantile_state", make_simple_type(TYPE_QUANTILE_STATE),
                     Field::create_field<TYPE_QUANTILE_STATE>(QuantileState())});
    cases.push_back({"variant", std::make_shared<DataTypeVariant>(),
                     Field::create_field<TYPE_NULL>(Null())});
    cases.push_back({"nothing", std::make_shared<DataTypeNothing>(),
                     Field::create_field<TYPE_NULL>(Null())});

    auto int_type = make_simple_type(TYPE_INT);
    auto nullable_int_type = std::make_shared<DataTypeNullable>(int_type);
    cases.push_back({"nullable<int>", nullable_int_type, Field::create_field<TYPE_NULL>(Null())});

    auto nullable_string_type = std::make_shared<DataTypeNullable>(make_simple_type(TYPE_STRING));
    cases.push_back({"array<nullable<int>>", std::make_shared<DataTypeArray>(nullable_int_type),
                     Field::create_field<TYPE_ARRAY>(Array())});
    cases.push_back(
            {"map<nullable<string>,nullable<int>>",
             std::make_shared<DataTypeMap>(nullable_string_type, nullable_int_type),
             Field::create_field<TYPE_MAP>(Map {Field::create_field<TYPE_ARRAY>(Array()),
                                                Field::create_field<TYPE_ARRAY>(Array())})});
    cases.push_back(
            {"struct<nullable<int>,nullable<string>>",
             std::make_shared<DataTypeStruct>(DataTypes {nullable_int_type, nullable_string_type}),
             Field::create_field<TYPE_STRUCT>(Tuple {Field::create_field<TYPE_NULL>(Null()),
                                                     Field::create_field<TYPE_NULL>(Null())})});
    cases.push_back({"agg_state<count>",
                     std::make_shared<DataTypeAggState>(DataTypes {int_type}, false, "count",
                                                        BeExecVersionManager::get_newest_version()),
                     Field::create_field<TYPE_STRING>(String(8, '\0'))});

    for (const auto& test_case : cases) {
        SCOPED_TRACE(test_case.name);
        assert_insert_default(test_case);
    }
}

} // namespace
} // namespace doris