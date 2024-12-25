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

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <iostream>

#include "olap/schema.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_map.h"
#include "vec/columns/columns_number.h"
#include "vec/core/field.h"
#include "vec/core/sort_block.h"
#include "vec/core/sort_description.h"
#include "vec/core/types.h"
#include "vec/data_types/common_data_type_serder_test.h"
#include "vec/data_types/common_data_type_test.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_nullable.h"

// this test is gonna to be a data type test template for all DataType which should make ut test to coverage the function defined
// for example DataTypeIPv4 should test this function:
// 1. datatype meta info:
//         get_type_id, get_type_as_type_descriptor, get_storage_field_type, have_subtypes, get_pdata_type (const IDataType *data_type), to_pb_column_meta (PColumnMeta *col_meta)
//         get_family_name, get_is_parametric, should_align_right_in_pretty_formats
//         text_can_contain_only_valid_utf8
//         have_maximum_size_of_value, get_maximum_size_of_value_in_memory, get_size_of_value_in_memory
//         get_precision, get_scale
//         is_null_literal, is_value_represented_by_number, is_value_unambiguously_represented_in_contiguous_memory_region
// 2. datatype creation with column : create_column, create_column_const (size_t size, const Field &field), create_column_const_with_default_value (size_t size), get_uncompressed_serialized_bytes (const IColumn &column, int be_exec_version)
// 3. serde related: get_serde (int nesting_level=1)
//          to_string (const IColumn &column, size_t row_num, BufferWritable &ostr), to_string (const IColumn &column, size_t row_num), to_string_batch (const IColumn &column, ColumnString &column_to), from_string (ReadBuffer &rb, IColumn *column)
//          serialize (const IColumn &column, char *buf, int be_exec_version), deserialize (const char *buf, MutableColumnPtr *column, int be_exec_version)
// 4. compare: equals (const IDataType &rhs), is_comparable
// 5. others: update_avg_value_size_hint (const IColumn &column, double &avg_value_size_hint)

namespace doris::vectorized {

class DataTypeIPTest : public CommonDataTypeTest {
protected:
    void SetUp() override {
        data_files = {"regression-test/data/nereids_function_p0/fn_test_ip_invalid.csv",
                      "regression-test/data/nereids_function_p0/fn_test_ip_normal.csv",
                      "regression-test/data/nereids_function_p0/fn_test_ip_nullable.csv",
                      "regression-test/data/nereids_function_p0/fn_test_ip_special.csv",
                      "regression-test/data/nereids_function_p0/fn_test_ip_special_no_null.csv"};
    }

public:
    DataTypePtr dt_ipv4 =
            DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_IPV4, 0, 0);
    DataTypePtr dt_ipv6 =
            DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_IPV6, 0, 0);
    DataTypePtr dt_ipv4_nullable = std::make_shared<vectorized::DataTypeNullable>(dt_ipv4);
    DataTypePtr dt_ipv6_nullable = std::make_shared<vectorized::DataTypeNullable>(dt_ipv6);
    // common ip data
    std::vector<string> data_files;
};

TEST_F(DataTypeIPTest, MetaInfoTest) {
    TypeDescriptor ipv4_type_descriptor = {PrimitiveType::TYPE_IPV4};
    auto col_meta = std::make_shared<PColumnMeta>();
    col_meta->set_type(PGenericType_TypeId_IPV4);
    DataTypeMetaInfo ipv4_meta_info_to_assert = {
            .type_id = TypeIndex::IPv4,
            .type_as_type_descriptor = &ipv4_type_descriptor,
            .family_name = "IPv4",
            .has_subtypes = false,
            .storage_field_type = doris::FieldType::OLAP_FIELD_TYPE_IPV4,
            .should_align_right_in_pretty_formats = true,
            .text_can_contain_only_valid_utf8 = true,
            .have_maximum_size_of_value = true,
            .size_of_value_in_memory = sizeof(IPv4),
            .precision = size_t(-1),
            .scale = size_t(-1),
            .is_null_literal = false,
            .is_value_represented_by_number = true,
            .pColumnMeta = col_meta.get()
            //                .is_value_unambiguously_represented_in_contiguous_memory_region = true
    };
    TypeDescriptor ipv6_type_descriptor = {PrimitiveType::TYPE_IPV6};
    auto col_meta6 = std::make_shared<PColumnMeta>();
    col_meta6->set_type(PGenericType_TypeId_IPV6);
    DataTypeMetaInfo ipv6_meta_info = {
            .type_id = TypeIndex::IPv6,
            .type_as_type_descriptor = &ipv6_type_descriptor,
            .family_name = "IPv6",
            .has_subtypes = false,
            .storage_field_type = doris::FieldType::OLAP_FIELD_TYPE_IPV6,
            .should_align_right_in_pretty_formats = true,
            .text_can_contain_only_valid_utf8 = true,
            .have_maximum_size_of_value = true,
            .size_of_value_in_memory = sizeof(IPv6),
            .precision = size_t(-1),
            .scale = size_t(-1),
            .is_null_literal = false,
            .is_value_represented_by_number = true,
            .pColumnMeta = col_meta6.get()
            //                .is_value_unambiguously_represented_in_contiguous_memory_region = true
    };
    meta_info_assert(dt_ipv4, ipv4_meta_info_to_assert);
    meta_info_assert(dt_ipv6, ipv6_meta_info);
}

TEST_F(DataTypeIPTest, CreateColumnTest) {
    Field default_field_ipv4 = IPv4(0);
    Field default_field_ipv6 = IPv6(0);
    create_column_assert(dt_ipv4, default_field_ipv4);
    create_column_assert(dt_ipv6, default_field_ipv6);
}

TEST_F(DataTypeIPTest, GetFieldTest) {
    auto serde_ipv4 = dt_ipv4->get_serde(1);
    auto serde_ipv6 = dt_ipv6->get_serde(1);
    auto column_ipv4 = dt_ipv4->create_column();
    auto column_ipv6 = dt_ipv6->create_column();

    // insert from data csv and assert insert result
    MutableColumns ip_cols;
    ip_cols.push_back(column_ipv4->get_ptr());
    ip_cols.push_back(column_ipv6->get_ptr());
    DataTypeSerDeSPtrs serde = {dt_ipv4->get_serde(), dt_ipv6->get_serde()};
    CommonDataTypeSerdeTest::load_data_and_assert_from_csv<true, true>(serde, ip_cols,
                                                                       data_files[1], ';', {1, 2});
    TExprNode node_ipv4;
    node_ipv4.node_type = TExprNodeType::IPV4_LITERAL;
    for (size_t i = 0; i < ip_cols[0]->size(); ++i) {
        node_ipv4.ipv4_literal.value = ip_cols[0]->get_int(i);
        Field assert_field;
        ip_cols[0]->get(i, assert_field);
        get_field_assert(dt_ipv4, node_ipv4, assert_field);
    }

    TExprNode node_ipv6;
    node_ipv6.node_type = TExprNodeType::IPV6_LITERAL;
    for (size_t i = 0; i < ip_cols[1]->size(); ++i) {
        node_ipv6.ipv6_literal.value =
                IPv6Value::to_string(assert_cast<ColumnIPv6&>(*ip_cols[1]).get_data()[i]);
        Field assert_field;
        ip_cols[1]->get(i, assert_field);
        get_field_assert(dt_ipv6, node_ipv6, assert_field);
    }

    TExprNode invalid_node_ipv6;
    invalid_node_ipv6.node_type = TExprNodeType::IPV6_LITERAL;
    // todo.(check) 2001:db8:::1 this is invalid ipv6 value, but it can pass the test
    std::vector<string> invalid_ipv6 = {"2001:db8::12345",
                                        "",
                                        "::fffff:0:0",
                                        "2001:db8::g123",
                                        "2001:db8:85a3::8a2e:0370:",
                                        "2001:0db8:85a3:0000:0000:8a2e:0370:7334:1234",
                                        "::12345:abcd"};
    for (auto& ipv6 : invalid_ipv6) {
        invalid_node_ipv6.ipv6_literal.value = ipv6;
        Field field;
        get_field_assert(dt_ipv6, invalid_node_ipv6, field, true);
    }
}

TEST_F(DataTypeIPTest, FromAndToStringTest) {
    auto serde_ipv4 = dt_ipv4->get_serde(1);
    auto serde_ipv6 = dt_ipv6->get_serde(1);
    auto column_ipv4 = dt_ipv4->create_column();
    auto column_ipv6 = dt_ipv6->create_column();

    // insert from data csv and assert insert result
    MutableColumns ip_cols;
    ip_cols.push_back(column_ipv4->get_ptr());
    ip_cols.push_back(column_ipv6->get_ptr());
    DataTypeSerDeSPtrs serde = {dt_ipv4->get_serde(), dt_ipv6->get_serde()};
    load_data_from_csv(serde, ip_cols, data_files[0], ';', {1, 2});
    // test ipv4
    assert_to_string_from_string_assert(ip_cols[0]->get_ptr(), dt_ipv4);
    // test ipv6
    assert_to_string_from_string_assert(ip_cols[1]->get_ptr(), dt_ipv6);
}

TEST_F(DataTypeIPTest, CompareTest) {
    assert_compare_behavior(dt_ipv4, dt_ipv6);
}

TEST_F(DataTypeIPTest, SerdeHiveTextAndJsonFormatTest) {
    auto serde_ipv4 = dt_ipv4->get_serde(1);
    auto serde_ipv6 = dt_ipv6->get_serde(1);
    auto column_ipv4 = dt_ipv4->create_column();
    auto column_ipv6 = dt_ipv6->create_column();

    // insert from data csv and assert insert result
    MutableColumns ip_cols;
    ip_cols.push_back(column_ipv4->get_ptr());
    ip_cols.push_back(column_ipv6->get_ptr());
    DataTypeSerDeSPtrs serde = {dt_ipv4->get_serde(), dt_ipv6->get_serde()};
    CommonDataTypeSerdeTest::load_data_and_assert_from_csv<true, true>(serde, ip_cols,
                                                                       data_files[1], ';', {1, 2});
    CommonDataTypeSerdeTest::load_data_and_assert_from_csv<false, true>(serde, ip_cols,
                                                                        data_files[1], ';', {1, 2});
}

TEST_F(DataTypeIPTest, SerdePbTest) {
    auto serde_ipv4 = dt_ipv4->get_serde(1);
    auto serde_ipv6 = dt_ipv6->get_serde(1);
    auto column_ipv4 = dt_ipv4->create_column();
    auto column_ipv6 = dt_ipv6->create_column();

    // insert from data csv and assert insert result
    MutableColumns ip_cols;
    ip_cols.push_back(column_ipv4->get_ptr());
    ip_cols.push_back(column_ipv6->get_ptr());
    DataTypeSerDeSPtrs serde = {dt_ipv4->get_serde(), dt_ipv6->get_serde()};
    CommonDataTypeSerdeTest::check_data(ip_cols, serde, ';', {1, 2}, data_files[0],
                                        CommonDataTypeSerdeTest::assert_pb_format);
}

TEST_F(DataTypeIPTest, SerdeJsonbTest) {
    auto serde_ipv4 = dt_ipv4->get_serde(1);
    auto serde_ipv6 = dt_ipv6->get_serde(1);
    auto column_ipv4 = dt_ipv4->create_column();
    auto column_ipv6 = dt_ipv6->create_column();

    // insert from data csv and assert insert result
    MutableColumns ip_cols;
    ip_cols.push_back(column_ipv4->get_ptr());
    ip_cols.push_back(column_ipv6->get_ptr());
    DataTypeSerDeSPtrs serde = {dt_ipv4->get_serde(), dt_ipv6->get_serde()};
    CommonDataTypeSerdeTest::check_data(ip_cols, serde, ';', {1, 2}, data_files[0],
                                        CommonDataTypeSerdeTest::assert_jsonb_format);
}

} // namespace doris::vectorized