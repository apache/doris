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

#include <execinfo.h> // for backtrace on Linux
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <iostream>
#include <stdexcept>

#include "vec/columns/column.h"
#include "vec/columns/columns_number.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/common_data_type_serder_test.h"
#include "vec/data_types/common_data_type_test.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/function/function_test_util.h"

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

class DataTypeArrayTest : public CommonDataTypeTest {
protected:
    void SetUp() override {
        // insert from data csv and assert insert result
        MutableColumns array_cols;
        // we need to load data from csv file into column_array list
        // step1. create data type for array nested type (const and nullable)
        // array<tinyint>
        BaseInputTypeSet array_tinyint = {TypeIndex::Array, TypeIndex::Int8};
        // array<smallint>
        BaseInputTypeSet array_smallint = {TypeIndex::Array, TypeIndex::Int16};
        // array<int>
        BaseInputTypeSet array_int = {TypeIndex::Array, TypeIndex::Int32};
        // array<bigint>
        BaseInputTypeSet array_bigint = {TypeIndex::Array, TypeIndex::Int64};
        // array<largeint>
        BaseInputTypeSet array_largeint = {TypeIndex::Array, TypeIndex::Int128};
        // array<float>
        BaseInputTypeSet array_float = {TypeIndex::Array, TypeIndex::Float32};
        // array<double>
        BaseInputTypeSet array_double = {TypeIndex::Array, TypeIndex::Float64};
        // array<ipv4>
        BaseInputTypeSet array_ipv4 = {TypeIndex::Array, TypeIndex::IPv4};
        // array<ipv6>
        BaseInputTypeSet array_ipv6 = {TypeIndex::Array, TypeIndex::IPv6};
        // array<date>
        BaseInputTypeSet array_date = {TypeIndex::Array, TypeIndex::Date};
        // array<datetime>
        BaseInputTypeSet array_datetime = {TypeIndex::Array, TypeIndex::DateTime};
        // array<datev2>
        BaseInputTypeSet array_datev2 = {TypeIndex::Array, TypeIndex::DateV2};
        // array<datetimev2>
        BaseInputTypeSet array_datetimev2 = {TypeIndex::Array, TypeIndex::DateTimeV2};
        // array<varchar>
        BaseInputTypeSet array_varchar = {TypeIndex::Array, TypeIndex::String};
        // array<decimal32(9, 5)> UT
        BaseInputTypeSet array_decimal = {TypeIndex::Array, TypeIndex::Decimal32};
        // array<decimal64(18, 9)> UT
        BaseInputTypeSet array_decimal64 = {TypeIndex::Array, TypeIndex::Decimal64};
        // array<decimal128(38, 20)> UT
        BaseInputTypeSet array_decimal128 = {TypeIndex::Array, TypeIndex::Decimal128V3};
        // array<decimal256(76, 40)> UT
        BaseInputTypeSet array_decimal256 = {TypeIndex::Array, TypeIndex::Decimal256};
        std::vector<BaseInputTypeSet> array_typeIndex = {
                array_tinyint,   array_smallint,   array_int,        array_bigint,  array_largeint,
                array_float,     array_double,     array_ipv4,       array_ipv6,    array_date,
                array_datetime,  array_datev2,     array_datetimev2, array_varchar, array_decimal,
                array_decimal64, array_decimal128, array_decimal256};
        // array<array<tinyint>>
        BaseInputTypeSet array_array_tinyint = {TypeIndex::Array, TypeIndex::Array,
                                                TypeIndex::Int8};
        // array<array<smallint>>
        BaseInputTypeSet array_array_smallint = {TypeIndex::Array, TypeIndex::Array,
                                                 TypeIndex::Int16};
        // array<array<int>>
        BaseInputTypeSet array_array_int = {TypeIndex::Array, TypeIndex::Array, TypeIndex::Int32};
        // array<array<bigint>>
        BaseInputTypeSet array_array_bigint = {TypeIndex::Array, TypeIndex::Array,
                                               TypeIndex::Int64};
        // array<array<largeint>>
        BaseInputTypeSet array_array_largeint = {TypeIndex::Array, TypeIndex::Array,
                                                 TypeIndex::Int128};
        // array<array<float>>
        BaseInputTypeSet array_array_float = {TypeIndex::Array, TypeIndex::Array,
                                              TypeIndex::Float32};
        // array<array<double>>
        BaseInputTypeSet array_array_double = {TypeIndex::Array, TypeIndex::Array,
                                               TypeIndex::Float64};
        // array<array<ipv4>>
        BaseInputTypeSet array_array_ipv4 = {TypeIndex::Array, TypeIndex::Array, TypeIndex::IPv4};
        // array<array<ipv6>>
        BaseInputTypeSet array_array_ipv6 = {TypeIndex::Array, TypeIndex::Array, TypeIndex::IPv6};
        // array<array<date>>
        BaseInputTypeSet array_array_date = {TypeIndex::Array, TypeIndex::Array, TypeIndex::Date};
        // array<array<datetime>>
        BaseInputTypeSet array_array_datetime = {TypeIndex::Array, TypeIndex::Array,
                                                 TypeIndex::DateTime};
        // array<array<datev2>>
        BaseInputTypeSet array_array_datev2 = {TypeIndex::Array, TypeIndex::Array,
                                               TypeIndex::DateV2};
        // array<array<datetimev2>>
        BaseInputTypeSet array_array_datetimev2 = {TypeIndex::Array, TypeIndex::Array,
                                                   TypeIndex::DateTimeV2};
        // array<array<varchar>>
        BaseInputTypeSet array_array_varchar = {TypeIndex::Array, TypeIndex::Array,
                                                TypeIndex::String};
        // array<array<decimal32(9, 5)>> UT
        BaseInputTypeSet array_array_decimal = {TypeIndex::Array, TypeIndex::Array,
                                                TypeIndex::Decimal32};
        // array<array<decimal64(18, 9)>> UT
        BaseInputTypeSet array_array_decimal64 = {TypeIndex::Array, TypeIndex::Array,
                                                  TypeIndex::Decimal64};
        // array<array<decimal128(38, 20)>> UT
        BaseInputTypeSet array_array_decimal128 = {TypeIndex::Array, TypeIndex::Array,
                                                   TypeIndex::Decimal128V3};
        // array<array<decimal256(76, 40)>> UT
        BaseInputTypeSet array_array_decimal256 = {TypeIndex::Array, TypeIndex::Array,
                                                   TypeIndex::Decimal256};
        // array<map<char,double>>
        BaseInputTypeSet array_map_char_double = {TypeIndex::Array, TypeIndex::Map,
                                                  TypeIndex::String, TypeIndex::Float64};
        // test_array_map<datetime,decimal<76,56>>.csv
        BaseInputTypeSet array_map_datetime_decimal = {TypeIndex::Array, TypeIndex::Map,
                                                       TypeIndex::DateTime, TypeIndex::Decimal256};
        // test_array_map<ipv4,ipv6>.csv
        BaseInputTypeSet array_map_ipv4_ipv6 = {TypeIndex::Array, TypeIndex::Map, TypeIndex::IPv4,
                                                TypeIndex::IPv6};
        // test_array_map<largeInt,string>.csv
        BaseInputTypeSet array_map_largeint_string = {TypeIndex::Array, TypeIndex::Map,
                                                      TypeIndex::Int128, TypeIndex::String};
        // array<struct<f1:int,f2:date,f3:decimal,f4:string,f5:double,f6:ipv4,f7:ipv6>>
        BaseInputTypeSet array_struct = {
                TypeIndex::Array,   TypeIndex::Struct,    TypeIndex::Int32,
                TypeIndex::Date,    TypeIndex::Decimal32, TypeIndex::String,
                TypeIndex::Float64, TypeIndex::IPv4,      TypeIndex::IPv6};

        std::vector<BaseInputTypeSet> array_array_typeIndex = {
                array_array_tinyint,    array_array_smallint,   array_array_int,
                array_array_bigint,     array_array_largeint,   array_array_float,
                array_array_double,     array_array_ipv4,       array_array_ipv6,
                array_array_date,       array_array_datetime,   array_array_datev2,
                array_array_datetimev2, array_array_varchar,    array_array_decimal,
                array_array_decimal64,  array_array_decimal128, array_array_decimal256};
        std::vector<BaseInputTypeSet> array_map_typeIndex = {
                array_map_char_double, array_map_datetime_decimal, array_map_ipv4_ipv6,
                array_map_largeint_string};
        std::vector<BaseInputTypeSet> array_struct_typeIndex = {array_struct};

        array_descs.reserve(array_typeIndex.size() + array_array_typeIndex.size() +
                            array_map_typeIndex.size() + array_struct_typeIndex.size());
        for (int i = 0; i < array_typeIndex.size(); i++) {
            array_descs.push_back(ut_type::UTDataTypeDescs());
            InputTypeSet input_types {};
            input_types.push_back(array_typeIndex[i][0]);
            input_types.push_back(Nullable {static_cast<TypeIndex>(array_typeIndex[i][1])});
            EXPECT_EQ(input_types[1].type(), &typeid(Nullable)) << "nested type is not nullable";
            EXPECT_TRUE(parse_ut_data_type(input_types, array_descs[i]));
        }
        for (int i = 0; i < array_array_typeIndex.size(); i++) {
            array_descs.push_back(ut_type::UTDataTypeDescs());
            InputTypeSet input_types {};
            input_types.push_back(array_array_typeIndex[i][0]);
            input_types.push_back(Nullable {static_cast<TypeIndex>(array_array_typeIndex[i][1])});
            input_types.push_back(Nullable {static_cast<TypeIndex>(array_array_typeIndex[i][2])});
            EXPECT_EQ(input_types[1].type(), &typeid(Nullable)) << "nested type is not nullable";
            EXPECT_EQ(input_types[2].type(), &typeid(Nullable)) << "nested type is not nullable";
            EXPECT_TRUE(parse_ut_data_type(input_types, array_descs[i + array_typeIndex.size()]));
        }

        for (int i = 0; i < array_map_typeIndex.size(); i++) {
            array_descs.push_back(ut_type::UTDataTypeDescs());
            InputTypeSet input_types {};
            input_types.push_back(array_map_typeIndex[i][0]); // array
            input_types.push_back(
                    Nullable {static_cast<TypeIndex>(array_map_typeIndex[i][1])}); // map
            input_types.push_back(
                    Nullable {static_cast<TypeIndex>(array_map_typeIndex[i][2])}); // key
            input_types.push_back(
                    Nullable {static_cast<TypeIndex>(array_map_typeIndex[i][3])}); // val
            EXPECT_EQ(input_types[1].type(), &typeid(Nullable)) << "nested type is not nullable";
            EXPECT_EQ(input_types[2].type(), &typeid(Nullable)) << "nested type is not nullable";
            EXPECT_TRUE(parse_ut_data_type(
                    input_types,
                    array_descs[i + array_typeIndex.size() + array_array_typeIndex.size()]));
        }

        for (int i = 0; i < array_struct_typeIndex.size(); i++) {
            array_descs.push_back(ut_type::UTDataTypeDescs());
            InputTypeSet input_types {};
            input_types.push_back(array_struct_typeIndex[i][0]); // arr
            input_types.push_back(
                    Nullable {static_cast<TypeIndex>(array_struct_typeIndex[i][1])}); // struct
            input_types.push_back(
                    Nullable {static_cast<TypeIndex>(array_struct_typeIndex[i][2])}); // f1
            input_types.push_back(
                    Nullable {static_cast<TypeIndex>(array_struct_typeIndex[i][3])}); // f2
            input_types.push_back(
                    Nullable {static_cast<TypeIndex>(array_struct_typeIndex[i][4])}); // f3
            input_types.push_back(
                    Nullable {static_cast<TypeIndex>(array_struct_typeIndex[i][5])}); // f4
            input_types.push_back(
                    Nullable {static_cast<TypeIndex>(array_struct_typeIndex[i][6])}); // f5
            input_types.push_back(
                    Nullable {static_cast<TypeIndex>(array_struct_typeIndex[i][7])}); // f6
            input_types.push_back(
                    Nullable {static_cast<TypeIndex>(array_struct_typeIndex[i][8])}); // f7

            EXPECT_EQ(input_types[1].type(), &typeid(Nullable)) << "nested type is not nullable";
            EXPECT_TRUE(parse_ut_data_type(
                    input_types,
                    array_descs[i + array_typeIndex.size() + array_array_typeIndex.size() +
                                array_map_typeIndex.size()]));
        }

        // create column_array for each data type
        // step2. according to the datatype to make column_array
        //          && load data from csv file into column_array
        EXPECT_EQ(array_descs.size(), data_files.size());
        for (int i = 0; i < array_descs.size(); i++) {
            auto& desc = array_descs[i];
            auto& data_file = data_files[i];
            // first is array type
            auto& type = desc[0].data_type;
            std::cout << "type: " << type->get_name() << " with file: " << data_file << std::endl;
            MutableColumns columns;
            columns.push_back(type->create_column());
            auto serde = type->get_serde(1);
            load_data_from_csv({serde}, columns, data_file, ';');
            array_columns.push_back(std::move(columns[0]));
            array_types.push_back(type);
            serdes.push_back(serde);
        }
    }

    std::string data_file_dir = "regression-test/data/nereids_function_p0/array/";

    vector<string> data_files = {
            // array-scalar
            data_file_dir + "test_array_tinyint.csv", data_file_dir + "test_array_smallint.csv",
            data_file_dir + "test_array_int.csv", data_file_dir + "test_array_bigint.csv",
            data_file_dir + "test_array_largeint.csv", data_file_dir + "test_array_float.csv",
            data_file_dir + "test_array_double.csv", data_file_dir + "test_array_ipv4.csv",
            data_file_dir + "test_array_ipv6.csv", data_file_dir + "test_array_date.csv",
            data_file_dir + "test_array_datetime.csv", data_file_dir + "test_array_date.csv",
            data_file_dir + "test_array_datetimev2(6).csv",
            data_file_dir + "test_array_varchar(65535).csv",
            data_file_dir + "test_array_decimalv3(7,4).csv",
            data_file_dir + "test_array_decimalv3(16,10).csv",
            data_file_dir + "test_array_decimalv3(38,30).csv",
            data_file_dir + "test_array_decimalv3(76,56).csv",
            // array-array
            data_file_dir + "test_array_array_tinyint.csv",
            data_file_dir + "test_array_array_smallint.csv",
            data_file_dir + "test_array_array_int.csv",
            data_file_dir + "test_array_array_bigint.csv",
            data_file_dir + "test_array_array_largeint.csv",
            data_file_dir + "test_array_array_float.csv",
            data_file_dir + "test_array_array_double.csv",
            data_file_dir + "test_array_array_ipv4.csv",
            data_file_dir + "test_array_array_ipv6.csv",
            data_file_dir + "test_array_array_date.csv",
            data_file_dir + "test_array_array_datetime.csv",
            data_file_dir + "test_array_array_date.csv",
            data_file_dir + "test_array_array_datetimev2(5).csv",
            data_file_dir + "test_array_array_varchar(65535).csv",
            data_file_dir + "test_array_array_decimalv3(1,0).csv",
            data_file_dir + "test_array_array_decimalv3(27,9).csv",
            data_file_dir + "test_array_array_decimalv3(38,30).csv",
            data_file_dir + "test_array_array_decimalv3(76,56).csv",
            // array-map - 36
            data_file_dir + "test_array_map_char_double.csv",
            data_file_dir + "test_array_map_datetime_decimal.csv",
            data_file_dir + "test_array_map_ipv4_ipv6.csv",
            data_file_dir + "test_array_map_largeInt_string.csv",
            // array-struct
            data_file_dir + "test_array_struct.csv"};

    vector<ut_type::UTDataTypeDescs> array_descs; // array<> descs matrix
    MutableColumns array_columns;                 // column_array list
    DataTypes array_types;
    DataTypeSerDeSPtrs serdes;
};

// ================ common data type and serder test =================
TEST_F(DataTypeArrayTest, MetaInfoTest) {
    for (int i = 0; i < array_types.size(); i++) {
        auto& type = array_types[i];
        auto& desc = array_descs[i];
        auto array_type = assert_cast<const DataTypeArray*>(remove_nullable(type).get());
        auto nested_type =
                assert_cast<const DataTypeArray*>(remove_nullable(type).get())->get_nested_type();

        TypeDescriptor arr_type_descriptor = {PrimitiveType::TYPE_ARRAY};
        arr_type_descriptor.add_sub_type(desc[0].type_desc.children[0]);
        auto col_meta = std::make_shared<PColumnMeta>();
        array_type->to_pb_column_meta(col_meta.get());
        Array a;
        a.push_back(nested_type->get_default());

        DataTypeMetaInfo arr_meta_info_to_assert = {
                .type_id = TypeIndex::Array,
                .type_as_type_descriptor = &arr_type_descriptor,
                .family_name = "Array",
                .has_subtypes = true,
                .storage_field_type = doris::FieldType::OLAP_FIELD_TYPE_ARRAY,
                .should_align_right_in_pretty_formats = false,
                .text_can_contain_only_valid_utf8 = nested_type->text_can_contain_only_valid_utf8(),
                .have_maximum_size_of_value = false,
                .size_of_value_in_memory = size_t(-1),
                .precision = size_t(-1),
                .scale = size_t(-1),
                .is_null_literal = false,
                .is_value_represented_by_number = false,
                .pColumnMeta = col_meta.get(),
                .is_value_unambiguously_represented_in_contiguous_memory_region =
                        nested_type
                                ->is_value_unambiguously_represented_in_contiguous_memory_region(),
                .default_field = a,
        };
        DataTypePtr arr = remove_nullable(type);
        meta_info_assert(arr, arr_meta_info_to_assert);
    }
}

TEST_F(DataTypeArrayTest, CreateColumnTest) {
    // for tinyint -> largeint, float,double, ipv4/6, all date type  here uncompressed size is 16
    // for string here uncompressed size is 24
    for (int i = 0; i < 13; i++) {
        auto type = remove_nullable(array_types[i]);
        // any different nested type in arr with same default array ?
        Field default_field_array = Array();
        create_column_assert(type, default_field_array, 51); // 17 * 3
    }
    {
        auto type = remove_nullable(array_types[13]);
        Field default_field_array = Array();
        create_column_assert(type, default_field_array, 59); // add addtional sizeof(8)
    }
    // for decimal32/64/128/256 here uncompressed size is 16
    // one scalar type
    for (int i = 14; i < 18; i++) {
        auto type = remove_nullable(array_types[i]);
        Field default_field_array = Array();
        create_column_assert(type, default_field_array, 51);
    }
    // for array-array-scala
    for (int i = 18; i < 31; i++) {
        auto type = remove_nullable(array_types[i]);
        Field default_field_array = Array();
        create_column_assert(type, default_field_array, 85); // 17 * 5
    }
    {
        // string type
        auto type = remove_nullable(array_types[31]);
        Field default_field_array = Array();
        create_column_assert(type, default_field_array, 93); // add addtional sizeof(8)
    }
    for (int i = 32; i < 36; i++) {
        auto type = remove_nullable(array_types[i]);
        Field default_field_array = Array();
        create_column_assert(type, default_field_array, 85); // 17 * 5
    }
    // for array-map
    {
        auto type = remove_nullable(array_types[36]);
        Field default_field_array = Array();
        create_column_assert(type, default_field_array, 127); // 17 * 7 + 8 add addtional sizeof(8)
        type = remove_nullable(array_types[39]);
        default_field_array = Array();
        create_column_assert(type, default_field_array, 127);
    }
    {
        auto type = remove_nullable(array_types[37]);
        Field default_field_array = Array();
        create_column_assert(type, default_field_array, 119);
        type = remove_nullable(array_types[38]);
        default_field_array = Array();
        create_column_assert(type, default_field_array, 119); // 17 * 7
    }
    // for array-struct
    {
        auto type = remove_nullable(array_types[40]);
        Field default_field_array = Array();
        create_column_assert(type, default_field_array, 297); // 17 * 17
    }
}

TEST_F(DataTypeArrayTest, GetFieldTest) {
    TExprNode node;
    node.node_type = TExprNodeType::ARRAY_LITERAL;
    for (size_t i = 0; i < array_columns.size(); ++i) {
        Field assert_field;
        array_columns[i]->get(0, assert_field);
        get_field_assert(array_types[i], node, assert_field, true);
    }
}

TEST_F(DataTypeArrayTest, FromAndToStringTest) {
    // insert from data csv and assert insert result
    for (int i = 0; i < array_columns.size(); i++) {
        auto& column = array_columns[i];
        auto& type = array_types[i];
        std::cout << "type: " << type->get_name() << " for column size: " << column->size()
                  << std::endl;
        // datatype array<string> has some different behavior maybe wrong with given data
        if (i == 13 || i == 31) {
            continue;
        }
        assert_to_string_from_string_assert(column->assume_mutable(), type);
    }
}

TEST_F(DataTypeArrayTest, CompareTest) {
    for (int i = 0; i < array_types.size(); i++) {
        auto& type = array_types[i];
        auto array_type = assert_cast<const DataTypeArray*>(remove_nullable(type).get());
        auto nested_type = array_type->get_nested_type();
        EXPECT_EQ(nested_type->is_comparable(), array_type->is_comparable());
        EXPECT_FALSE(array_type->equals(*type));
    }
}

TEST_F(DataTypeArrayTest, SerdeHiveTextAndJsonFormatTest) {
    // insert from data csv and assert insert result
    for (int i = 0; i < 40; i++) {
        MutableColumns array_cols;
        array_cols.push_back(array_columns[i]->get_ptr());
        // array-struct would cause be core:heap-buffer-overflow for hive_text deser as '[]'
        CommonDataTypeSerdeTest::load_data_and_assert_from_csv<true, true>({serdes[i]}, array_cols,
                                                                           data_files[i], ';');
        CommonDataTypeSerdeTest::load_data_and_assert_from_csv<false, true>({serdes[i]}, array_cols,
                                                                            data_files[i], ';');
    }
}

TEST_F(DataTypeArrayTest, SerdePbTest) {
    // fix serde pb for read decimal64 not support
    MutableColumns array_cols;
    DataTypeSerDeSPtrs serdes_pb;
    for (int i = 0; i < 40; i++) {
        array_cols.push_back(array_columns[i]->get_ptr());
        serdes_pb.push_back(serdes[i]);
    }
    CommonDataTypeSerdeTest::assert_pb_format(array_cols, serdes_pb);
}

TEST_F(DataTypeArrayTest, SerdeJsonbTest) {
    CommonDataTypeSerdeTest::assert_jsonb_format(array_columns, serdes);
}

TEST_F(DataTypeArrayTest, SerdeMysqlTest) {
    // insert from data csv and assert insert result
    CommonDataTypeSerdeTest::assert_mysql_format(array_columns, serdes);
}

TEST_F(DataTypeArrayTest, SerializeDeserializeTest) {
    // insert from data csv and assert insert result
    CommonDataTypeTest::serialize_deserialize_assert(array_columns, array_types);
}

TEST_F(DataTypeArrayTest, SerdeArrowTest) {
    // todo. fix decimal256 serde
    MutableColumns array_cols;
    for (int i = 0; i < 17; i++) {
        array_cols.push_back(array_columns[i]->get_ptr());
    }
    for (int i = 18; i < 35; i++) {
        array_cols.push_back(array_columns[i]->get_ptr());
    }
    array_cols.push_back(array_columns[36]->get_ptr());
    array_cols.push_back(array_columns[38]->get_ptr());
    DataTypes types;
    for (int i = 0; i < 17; i++) {
        types.push_back(array_types[i]);
    }
    for (int i = 18; i < 35; i++) {
        types.push_back(array_types[i]);
    }
    types.push_back(array_types[36]);
    types.push_back(array_types[38]);
    DataTypeSerDeSPtrs serde;
    for (int i = 0; i < 17; i++) {
        serde.push_back(serdes[i]);
    }
    for (int i = 18; i < 35; i++) {
        serde.push_back(serdes[i]);
    }
    serde.push_back(serdes[36]);
    serde.push_back(serdes[38]);
    CommonDataTypeSerdeTest::assert_arrow_format(array_cols, serde, types);
    {
        for (int i = 39; i < 41; ++i) {
            MutableColumns error_cols;
            error_cols.push_back(array_columns[i]->get_ptr());
            DataTypeSerDeSPtrs serde1;
            serde1.push_back(serdes[i]);
            DataTypes typ;
            typ.push_back(array_types[i]);
            EXPECT_ANY_THROW(CommonDataTypeSerdeTest::assert_arrow_format(error_cols, serde1, typ));
        }
    }
}

//================== datatype for array ut test ==================
TEST_F(DataTypeArrayTest, GetNumberOfDimensionsTest) {
    // for array-scalar
    for (int i = 0; i < 18; i++) {
        auto& type = array_types[i];
        auto array_type = assert_cast<const DataTypeArray*>(remove_nullable(type).get());
        // array dimension is only for array to nested array , if array nested map or struct, the dimension also be is 1
        EXPECT_EQ(array_type->get_number_of_dimensions(), 1) << "for type: " << type->get_name();
    }
    // for array-array
    for (int i = 18; i < 36; i++) {
        auto& type = array_types[i];
        auto desc = array_descs[i];
        auto array_type = assert_cast<const DataTypeArray*>(remove_nullable(type).get());
        // array dimension is only for array to nested array , if array nested map or struct, the dimension also be is 1
        EXPECT_EQ(array_type->get_number_of_dimensions(), 2) << "for type: " << type->get_name();
    }
    for (int i = 36; i < 41; i++) {
        auto& type = array_types[i];
        auto array_type = assert_cast<const DataTypeArray*>(remove_nullable(type).get());
        // array dimension is only for array to nested array , if array nested map or struct, the dimension also be is 1
        EXPECT_EQ(array_type->get_number_of_dimensions(), 1) << "for type: " << type->get_name();
    }
}

} // namespace doris::vectorized
