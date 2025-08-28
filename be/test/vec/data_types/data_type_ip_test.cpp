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

#include "runtime/raw_value.h"
#include "vec/columns/column.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/common_data_type_serder_test.h"
#include "vec/data_types/common_data_type_test.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_struct.h"
#include "vec/runtime/ipv4_value.h"

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
    std::vector<std::string> data_files;
};

TEST_F(DataTypeIPTest, MetaInfoTest) {
    auto ipv4_type_descriptor =
            DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_IPV4, false);
    auto col_meta = std::make_shared<PColumnMeta>();
    col_meta->set_type(PGenericType_TypeId_IPV4);
    DataTypeMetaInfo ipv4_meta_info_to_assert = {
            .type_id = PrimitiveType::TYPE_IPV4,
            .type_as_type_descriptor = ipv4_type_descriptor,
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
            .pColumnMeta = col_meta.get(),
            .is_value_unambiguously_represented_in_contiguous_memory_region = true,
            .default_field = Field::create_field<TYPE_IPV4>(IPv4(0)),
    };
    auto ipv6_type_descriptor =
            DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_IPV6, false);
    auto col_meta6 = std::make_shared<PColumnMeta>();
    col_meta6->set_type(PGenericType_TypeId_IPV6);
    DataTypeMetaInfo ipv6_meta_info = {
            .type_id = PrimitiveType::TYPE_IPV6,
            .type_as_type_descriptor = ipv6_type_descriptor,
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
            .pColumnMeta = col_meta6.get(),
            .is_value_unambiguously_represented_in_contiguous_memory_region = true,
            .default_field = Field::create_field<TYPE_IPV6>(IPv6(0))};
    meta_info_assert(dt_ipv4, ipv4_meta_info_to_assert);
    meta_info_assert(dt_ipv6, ipv6_meta_info);
}

TEST_F(DataTypeIPTest, CreateColumnTest) {
    Field default_field_ipv4 = Field::create_field<TYPE_IPV4>(IPv4(0));
    Field default_field_ipv6 = Field::create_field<TYPE_IPV6>(IPv6(0));
    create_column_assert(dt_ipv4, default_field_ipv4, 17);
    create_column_assert(dt_ipv6, default_field_ipv6, 17);
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
                doris::IPv6Value::to_string(assert_cast<ColumnIPv6&>(*ip_cols[1]).get_data()[i]);
        Field assert_field;
        ip_cols[1]->get(i, assert_field);
        get_field_assert(dt_ipv6, node_ipv6, assert_field);
    }

    TExprNode invalid_node_ipv6;
    invalid_node_ipv6.node_type = TExprNodeType::IPV6_LITERAL;
    // todo.(check) 2001:db8:::1 this is invalid ipv6 value, but it can pass the test
    std::vector<std::string> invalid_ipv6 = {"2001:db8::12345",
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

TEST_F(DataTypeIPTest, SerdeMysqlAndArrowTest) {
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
                                        CommonDataTypeSerdeTest::assert_mysql_format);

    CommonDataTypeSerdeTest::assert_arrow_format(ip_cols, {dt_ipv4, dt_ipv6});
}

TEST_F(DataTypeIPTest, SerdeTOJsonInComplex) {
    // make array<ip>
    auto array_ipv4 = std::make_shared<DataTypeArray>(dt_ipv4);
    auto array_ipv6 = std::make_shared<DataTypeArray>(dt_ipv6);
    auto map_ipv4 = std::make_shared<DataTypeMap>(dt_ipv4, dt_ipv6);
    auto map_ipv6 = std::make_shared<DataTypeMap>(dt_ipv6, dt_ipv4);
    auto column_array_ipv4 = array_ipv4->create_column();
    auto column_array_ipv6 = array_ipv6->create_column();
    auto column_map_ipv4 = map_ipv4->create_column();
    auto column_map_ipv6 = map_ipv6->create_column();
    // struct
    auto struct_ip = std::make_shared<DataTypeStruct>(std::vector<DataTypePtr> {
            dt_ipv4, dt_ipv6, array_ipv4, array_ipv6, map_ipv4, map_ipv6});
    auto column_struct_ip = struct_ip->create_column();

    // insert some data into column
    std::vector<std::string> ipv4_data = {"190.0.0.1", "127.0.0.1", "10.0.0.1"};
    std::vector<std::string> ipv6_data = {"2001:db8::1234", "2001:db8::1234:5678", "::"};
    std::vector<IPv4> ipv4_values;
    std::vector<IPv6> ipv6_values;
    // put data into column
    for (size_t i = 0; i < ipv4_data.size(); ++i) {
        IPv4 ipv4_val = 0;
        IPv6 ipv6_val = 0;
        IPv4Value::from_string(ipv4_val, ipv4_data[i]);
        ipv4_values.push_back(ipv4_val);
        IPv6Value::from_string(ipv6_val, ipv6_data[i]);
        ipv6_values.push_back(ipv6_val);
    }

    // pack array ipv4
    Array ipv4_array;
    for (auto& ipv4 : ipv4_values) {
        ipv4_array.push_back(Field::create_field<TYPE_IPV4>(ipv4));
    }
    column_array_ipv4->insert(Field::create_field<TYPE_ARRAY>(ipv4_array));

    // pack array ipv6
    Array ipv6_array;
    for (auto& ipv6 : ipv6_values) {
        ipv6_array.push_back(Field::create_field<TYPE_IPV6>(ipv6));
    }
    column_array_ipv6->insert(Field::create_field<TYPE_ARRAY>(ipv6_array));

    Map ipv4_map;
    // pack map ipv4
    ipv4_map.push_back(Field::create_field<TYPE_ARRAY>(ipv4_array));
    ipv4_map.push_back(Field::create_field<TYPE_ARRAY>(ipv6_array));
    column_map_ipv4->insert(Field::create_field<TYPE_MAP>(ipv4_map));

    // pack map ipv6
    Map ipv6_map;
    ipv6_map.push_back(Field::create_field<TYPE_ARRAY>(ipv6_array));
    ipv6_map.push_back(Field::create_field<TYPE_ARRAY>(ipv4_array));
    column_map_ipv6->insert(Field::create_field<TYPE_MAP>(ipv6_map));

    // pack struct
    Tuple tuple;
    tuple.push_back(Field::create_field<TYPE_IPV4>(ipv4_values[0]));
    tuple.push_back(Field::create_field<TYPE_IPV6>(ipv6_values[0]));
    tuple.push_back(Field::create_field<TYPE_ARRAY>(ipv4_array));
    tuple.push_back(Field::create_field<TYPE_ARRAY>(ipv6_array));
    tuple.push_back(Field::create_field<TYPE_MAP>(ipv4_map));
    tuple.push_back(Field::create_field<TYPE_MAP>(ipv6_map));
    column_struct_ip->insert(Field::create_field<TYPE_STRUCT>(tuple));

    auto assert_func = [](DataTypePtr dt, MutableColumnPtr& col, std::string assert_json_str) {
        // serde to json
        auto from_serde = dt->get_serde(1);
        auto dst_str = ColumnString::create();
        dst_str->clear();
        dst_str->reserve(1);
        VectorBufferWriter write_buffer(*dst_str.get());
        DataTypeSerDe::FormatOptions options;
        options.escape_char = '\\';
        auto st = from_serde->serialize_column_to_json(*col, 0, 1, write_buffer, options);
        ASSERT_TRUE(st.ok());
        write_buffer.commit();
        StringRef json_str = dst_str->get_data_at(0);
        // print
        ASSERT_EQ(json_str.to_string(), assert_json_str);
    };

    std::vector<std::string> assert_json_arr_str = {
            R"(["190.0.0.1", "127.0.0.1", "10.0.0.1"])",
            R"(["2001:db8::1234", "2001:db8::1234:5678", "::"])"};
    std::vector<std::string> assert_json_map_str = {
            "{\"190.0.0.1\":\"2001:db8::1234\", \"127.0.0.1\":\"2001:db8::1234:5678\", "
            "\"10.0.0.1\":\"::\"}",
            "{\"2001:db8::1234\":\"190.0.0.1\", \"2001:db8::1234:5678\":\"127.0.0.1\", "
            "\"::\":\"10.0.0.1\"}"};
    std::string assert_json_struct_str =
            "{\"1\": \"190.0.0.1\", \"2\": \"2001:db8::1234\", \"3\": [\"190.0.0.1\", "
            "\"127.0.0.1\", \"10.0.0.1\"], \"4\": [\"2001:db8::1234\", \"2001:db8::1234:5678\", "
            "\"::\"], \"5\": {\"190.0.0.1\":\"2001:db8::1234\", "
            "\"127.0.0.1\":\"2001:db8::1234:5678\", \"10.0.0.1\":\"::\"}, \"6\": "
            "{\"2001:db8::1234\":\"190.0.0.1\", \"2001:db8::1234:5678\":\"127.0.0.1\", "
            "\"::\":\"10.0.0.1\"}}";

    assert_func(array_ipv4, column_array_ipv4, assert_json_arr_str[0]);
    assert_func(array_ipv6, column_array_ipv6, assert_json_arr_str[1]);
    assert_func(map_ipv4, column_map_ipv4, assert_json_map_str[0]);
    assert_func(map_ipv6, column_map_ipv6, assert_json_map_str[1]);
    assert_func(struct_ip, column_struct_ip, assert_json_struct_str);
}

TEST_F(DataTypeIPTest, GetFieldWithDataTypeTest) {
    auto column_ipv4 = dt_ipv4->create_column();
    auto column_ipv6 = dt_ipv6->create_column();
    Field field_ipv4 = Field::create_field<TYPE_IPV4>(IPv4(123));
    Field field_ipv6 = Field::create_field<TYPE_IPV6>(IPv6(123));
    column_ipv4->insert(field_ipv4);
    column_ipv6->insert(field_ipv6);
    ASSERT_EQ(dt_ipv4->get_field_with_data_type(*column_ipv4, 0).field, field_ipv4);
    ASSERT_EQ(dt_ipv6->get_field_with_data_type(*column_ipv6, 0).field, field_ipv6);
}

TEST_F(DataTypeIPTest, Crc32Test) {
    auto column_ipv4 = dt_ipv4->create_column();
    auto column_ipv6 = dt_ipv6->create_column();
    std::vector<std::string> ipv4_data = {"190.0.0.1", "127.0.0.1", "10.0.0.1"};
    std::vector<std::string> ipv6_data = {"2001:db8::1234", "2001:db8::1234:5678", "::"};
    for (size_t i = 0; i < ipv4_data.size(); ++i) {
        IPv4 ipv4_val = 0;
        IPv6 ipv6_val = 0;
        IPv4Value::from_string(ipv4_val, ipv4_data[i]);
        column_ipv4->insert(Field::create_field<TYPE_IPV4>(ipv4_val));
        IPv6Value::from_string(ipv6_val, ipv6_data[i]);
        column_ipv6->insert(Field::create_field<TYPE_IPV6>(ipv6_val));
    }
    auto column_value_ipv4 = column_ipv4->get_data_at(0);
    uint32_t hash_val = 0;
    hash_val = RawValue::zlib_crc32(column_value_ipv4.data, column_value_ipv4.size,
                                    PrimitiveType::TYPE_IPV4, hash_val);
    EXPECT_EQ(hash_val, 3038848754);
    // ipv6
    auto column_value_ipv6 = column_ipv6->get_data_at(0);
    hash_val = 0;
    hash_val = RawValue::zlib_crc32(column_value_ipv6.data, column_value_ipv6.size,
                                    PrimitiveType::TYPE_IPV6, hash_val);
    EXPECT_EQ(hash_val, 3609036864);
    column_value_ipv4 = column_ipv4->get_data_at(1);
    hash_val = 0;
    hash_val = RawValue::zlib_crc32(column_value_ipv4.data, column_value_ipv4.size,
                                    PrimitiveType::TYPE_IPV4, hash_val);
    EXPECT_EQ(hash_val, 1497552084);
    column_value_ipv6 = column_ipv6->get_data_at(1);
    hash_val = 0;
    hash_val = RawValue::zlib_crc32(column_value_ipv6.data, column_value_ipv6.size,
                                    PrimitiveType::TYPE_IPV6, hash_val);
    EXPECT_EQ(hash_val, 2028432210);
    column_value_ipv4 = column_ipv4->get_data_at(2);
    hash_val = 0;
    hash_val = RawValue::zlib_crc32(column_value_ipv4.data, column_value_ipv4.size,
                                    PrimitiveType::TYPE_IPV4, hash_val);
    EXPECT_EQ(hash_val, 2033013095);
    column_value_ipv6 = column_ipv6->get_data_at(2);
    hash_val = 0;
    hash_val = RawValue::zlib_crc32(column_value_ipv6.data, column_value_ipv6.size,
                                    PrimitiveType::TYPE_IPV6, hash_val);
    EXPECT_EQ(hash_val, 3971697493);
}
} // namespace doris::vectorized
