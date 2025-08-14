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

#include "vec/data_types/data_type_array.h"

#include <execinfo.h> // for backtrace on Linux
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <iostream>
#include <stdexcept>

#include "runtime/primitive_type.h"
#include "vec/columns/column.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/common_data_type_serder_test.h"
#include "vec/data_types/common_data_type_test.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_jsonb.h"
#include "vec/data_types/data_type_number.h"
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
        InputTypeSet array_tinyint = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_TINYINT};
        // array<smallint>
        InputTypeSet array_smallint = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_SMALLINT};
        // array<int>
        InputTypeSet array_int = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_INT};
        // array<bigint>
        InputTypeSet array_bigint = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_BIGINT};
        // array<largeint>
        InputTypeSet array_largeint = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_LARGEINT};
        // array<float>
        InputTypeSet array_float = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_FLOAT};
        // array<double>
        InputTypeSet array_double = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_DOUBLE};
        // array<ipv4>
        InputTypeSet array_ipv4 = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_IPV4};
        // array<ipv6>
        InputTypeSet array_ipv6 = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_IPV6};
        // array<date>
        InputTypeSet array_date = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_DATE};
        // array<datetime>
        InputTypeSet array_datetime = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_DATETIME};
        // array<datev2>
        InputTypeSet array_datev2 = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_DATEV2};
        // array<datetimev2>
        InputTypeSet array_datetimev2 = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_DATETIMEV2};
        // array<varchar>
        InputTypeSet array_varchar = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_VARCHAR};
        // array<decimal32(9, 5)> UT
        InputTypeSet array_decimal = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_DECIMAL32};
        // array<decimal64(18, 9)> UT
        InputTypeSet array_decimal64 = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_DECIMAL64};
        // array<decimal128(38, 20)> UT
        InputTypeSet array_decimal128 = {PrimitiveType::TYPE_ARRAY,
                                         PrimitiveType::TYPE_DECIMAL128I};
        // array<decimal256(76, 40)> UT
        InputTypeSet array_decimal256 = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_DECIMAL256};
        std::vector<InputTypeSet> array_typeIndex = {
                array_tinyint,   array_smallint,   array_int,        array_bigint,  array_largeint,
                array_float,     array_double,     array_ipv4,       array_ipv6,    array_date,
                array_datetime,  array_datev2,     array_datetimev2, array_varchar, array_decimal,
                array_decimal64, array_decimal128, array_decimal256};
        // array<array<tinyint>>
        InputTypeSet array_array_tinyint = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_ARRAY,
                                            PrimitiveType::TYPE_TINYINT};
        // array<array<smallint>>
        InputTypeSet array_array_smallint = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_ARRAY,
                                             PrimitiveType::TYPE_SMALLINT};
        // array<array<int>>
        InputTypeSet array_array_int = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_ARRAY,
                                        PrimitiveType::TYPE_INT};
        // array<array<bigint>>
        InputTypeSet array_array_bigint = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_ARRAY,
                                           PrimitiveType::TYPE_BIGINT};
        // array<array<largeint>>
        InputTypeSet array_array_largeint = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_ARRAY,
                                             PrimitiveType::TYPE_LARGEINT};
        // array<array<float>>
        InputTypeSet array_array_float = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_ARRAY,
                                          PrimitiveType::TYPE_FLOAT};
        // array<array<double>>
        InputTypeSet array_array_double = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_ARRAY,
                                           PrimitiveType::TYPE_DOUBLE};
        // array<array<ipv4>>
        InputTypeSet array_array_ipv4 = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_ARRAY,
                                         PrimitiveType::TYPE_IPV4};
        // array<array<ipv6>>
        InputTypeSet array_array_ipv6 = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_ARRAY,
                                         PrimitiveType::TYPE_IPV6};
        // array<array<date>>
        InputTypeSet array_array_date = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_ARRAY,
                                         PrimitiveType::TYPE_DATE};
        // array<array<datetime>>
        InputTypeSet array_array_datetime = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_ARRAY,
                                             PrimitiveType::TYPE_DATETIME};
        // array<array<datev2>>
        InputTypeSet array_array_datev2 = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_ARRAY,
                                           PrimitiveType::TYPE_DATEV2};
        // array<array<datetimev2>>
        InputTypeSet array_array_datetimev2 = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_ARRAY,
                                               PrimitiveType::TYPE_DATETIMEV2};
        // array<array<varchar>>
        InputTypeSet array_array_varchar = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_ARRAY,
                                            PrimitiveType::TYPE_VARCHAR};
        // array<array<decimal32(9, 5)>> UT
        InputTypeSet array_array_decimal = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_ARRAY,
                                            PrimitiveType::TYPE_DECIMAL32};
        // array<array<decimal64(18, 9)>> UT
        InputTypeSet array_array_decimal64 = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_ARRAY,
                                              PrimitiveType::TYPE_DECIMAL64};
        // array<array<decimal128(38, 20)>> UT
        InputTypeSet array_array_decimal128 = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_ARRAY,
                                               PrimitiveType::TYPE_DECIMAL128I};
        // array<array<decimal256(76, 40)>> UT
        InputTypeSet array_array_decimal256 = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_ARRAY,
                                               PrimitiveType::TYPE_DECIMAL256};
        // array<map<char,double>>
        InputTypeSet array_map_char_double = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_MAP,
                                              PrimitiveType::TYPE_VARCHAR,
                                              PrimitiveType::TYPE_DOUBLE};
        // test_array_map<datetime,decimal<76,56>>.csv
        InputTypeSet array_map_datetime_decimal = {
                PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_DATETIMEV2,
                PrimitiveType::TYPE_DECIMAL256};
        // test_array_map<ipv4,ipv6>.csv
        InputTypeSet array_map_ipv4_ipv6 = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_MAP,
                                            PrimitiveType::TYPE_IPV4, PrimitiveType::TYPE_IPV6};
        // test_array_map<largeInt,string>.csv
        InputTypeSet array_map_largeint_string = {
                PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_LARGEINT,
                PrimitiveType::TYPE_VARCHAR};
        // array<struct<f1:int,f2:date,f3:decimal,f4:string,f5:double,f6:ipv4,f7:ipv6>>
        InputTypeSet array_struct = {PrimitiveType::TYPE_ARRAY,     PrimitiveType::TYPE_STRUCT,
                                     PrimitiveType::TYPE_INT,       PrimitiveType::TYPE_DATE,
                                     PrimitiveType::TYPE_DECIMAL32, PrimitiveType::TYPE_VARCHAR,
                                     PrimitiveType::TYPE_DOUBLE,    PrimitiveType::TYPE_IPV4,
                                     PrimitiveType::TYPE_IPV6};

        std::vector<InputTypeSet> array_array_typeIndex = {
                array_array_tinyint,    array_array_smallint,   array_array_int,
                array_array_bigint,     array_array_largeint,   array_array_float,
                array_array_double,     array_array_ipv4,       array_array_ipv6,
                array_array_date,       array_array_datetime,   array_array_datev2,
                array_array_datetimev2, array_array_varchar,    array_array_decimal,
                array_array_decimal64,  array_array_decimal128, array_array_decimal256};
        std::vector<InputTypeSet> array_map_typeIndex = {
                array_map_char_double, array_map_datetime_decimal, array_map_ipv4_ipv6,
                array_map_largeint_string};
        std::vector<InputTypeSet> array_struct_typeIndex = {array_struct};

        array_descs.reserve(array_typeIndex.size() + array_array_typeIndex.size() +
                            array_map_typeIndex.size() + array_struct_typeIndex.size());
        for (int i = 0; i < array_typeIndex.size(); i++) {
            array_descs.emplace_back();
            InputTypeSet input_types {};
            input_types.push_back(array_typeIndex[i][0]);
            input_types.emplace_back(Nullable {any_cast<PrimitiveType>(array_typeIndex[i][1])});
            EXPECT_EQ(input_types[1].type(), &typeid(Nullable)) << "nested type is not nullable";
            EXPECT_TRUE(parse_ut_data_type(input_types, array_descs[i]));
        }
        for (int i = 0; i < array_array_typeIndex.size(); i++) {
            array_descs.emplace_back();
            InputTypeSet input_types {};
            input_types.push_back(array_array_typeIndex[i][0]);
            input_types.emplace_back(
                    Nullable {any_cast<PrimitiveType>(array_array_typeIndex[i][1])});
            input_types.emplace_back(
                    Nullable {any_cast<PrimitiveType>(array_array_typeIndex[i][2])});
            EXPECT_EQ(input_types[1].type(), &typeid(Nullable)) << "nested type is not nullable";
            EXPECT_EQ(input_types[2].type(), &typeid(Nullable)) << "nested type is not nullable";
            EXPECT_TRUE(parse_ut_data_type(input_types, array_descs[i + array_typeIndex.size()]));
        }

        for (int i = 0; i < array_map_typeIndex.size(); i++) {
            array_descs.emplace_back();
            InputTypeSet input_types {};
            input_types.push_back(array_map_typeIndex[i][0]); // array
            input_types.emplace_back(
                    Nullable {any_cast<PrimitiveType>(array_map_typeIndex[i][1])}); // map
            input_types.emplace_back(
                    Nullable {any_cast<PrimitiveType>(array_map_typeIndex[i][2])}); // key
            input_types.emplace_back(
                    Nullable {any_cast<PrimitiveType>(array_map_typeIndex[i][3])}); // val
            EXPECT_EQ(input_types[1].type(), &typeid(Nullable)) << "nested type is not nullable";
            EXPECT_EQ(input_types[2].type(), &typeid(Nullable)) << "nested type is not nullable";
            EXPECT_TRUE(parse_ut_data_type(
                    input_types,
                    array_descs[i + array_typeIndex.size() + array_array_typeIndex.size()]));
        }

        for (int i = 0; i < array_struct_typeIndex.size(); i++) {
            array_descs.emplace_back();
            InputTypeSet input_types {};
            input_types.push_back(array_struct_typeIndex[i][0]); // arr
            input_types.emplace_back(
                    Nullable {any_cast<PrimitiveType>(array_struct_typeIndex[i][1])}); // struct
            input_types.emplace_back(
                    Nullable {any_cast<PrimitiveType>(array_struct_typeIndex[i][2])}); // f1
            input_types.emplace_back(
                    Nullable {any_cast<PrimitiveType>(array_struct_typeIndex[i][3])}); // f2
            input_types.emplace_back(
                    Nullable {any_cast<PrimitiveType>(array_struct_typeIndex[i][4])}); // f3
            input_types.emplace_back(
                    Nullable {any_cast<PrimitiveType>(array_struct_typeIndex[i][5])}); // f4
            input_types.emplace_back(
                    Nullable {any_cast<PrimitiveType>(array_struct_typeIndex[i][6])}); // f5
            input_types.emplace_back(
                    Nullable {any_cast<PrimitiveType>(array_struct_typeIndex[i][7])}); // f6
            input_types.emplace_back(
                    Nullable {any_cast<PrimitiveType>(array_struct_typeIndex[i][8])}); // f7

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

    std::vector<std::string> data_files = {
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

    std::vector<ut_type::UTDataTypeDescs> array_descs; // array<> descs matrix
    MutableColumns array_columns;                      // column_array list
    DataTypes array_types;
    DataTypeSerDeSPtrs serdes;
};

// ================ common data type and serder test =================
TEST_F(DataTypeArrayTest, MetaInfoTest) {
    for (auto& type : array_types) {
        const auto* array_type = assert_cast<const DataTypeArray*>(remove_nullable(type).get());
        auto nested_type =
                assert_cast<const DataTypeArray*>(remove_nullable(type).get())->get_nested_type();

        auto arr_type_descriptor = type;
        auto col_meta = std::make_shared<PColumnMeta>();
        array_type->to_pb_column_meta(col_meta.get());
        Array a;
        a.push_back(nested_type->get_default());

        DataTypeMetaInfo arr_meta_info_to_assert = {
                .type_id = PrimitiveType::TYPE_ARRAY,
                .type_as_type_descriptor = remove_nullable(arr_type_descriptor),
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
                .default_field = Field::create_field<TYPE_ARRAY>(a),
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
        Field default_field_array = Field::create_field<TYPE_ARRAY>(Array());
        create_column_assert(type, default_field_array, 51); // 17 * 3
    }
    {
        auto type = remove_nullable(array_types[13]);
        Field default_field_array = Field::create_field<TYPE_ARRAY>(Array());
        create_column_assert(type, default_field_array, 59); // add addtional sizeof(8)
    }
    // for decimal32/64/128/256 here uncompressed size is 16
    // one scalar type
    for (int i = 14; i < 18; i++) {
        auto type = remove_nullable(array_types[i]);
        Field default_field_array = Field::create_field<TYPE_ARRAY>(Array());
        create_column_assert(type, default_field_array, 51);
    }
    // for array-array-scala
    for (int i = 18; i < 31; i++) {
        auto type = remove_nullable(array_types[i]);
        Field default_field_array = Field::create_field<TYPE_ARRAY>(Array());
        create_column_assert(type, default_field_array, 85); // 17 * 5
    }
    {
        // string type
        auto type = remove_nullable(array_types[31]);
        Field default_field_array = Field::create_field<TYPE_ARRAY>(Array());
        create_column_assert(type, default_field_array, 93); // add addtional sizeof(8)
    }
    for (int i = 32; i < 36; i++) {
        auto type = remove_nullable(array_types[i]);
        Field default_field_array = Field::create_field<TYPE_ARRAY>(Array());
        create_column_assert(type, default_field_array, 85); // 17 * 5
    }
    // for array-map
    {
        auto type = remove_nullable(array_types[36]);
        Field default_field_array = Field::create_field<TYPE_ARRAY>(Array());
        create_column_assert(type, default_field_array, 127); // 17 * 7 + 8 add addtional sizeof(8)
        type = remove_nullable(array_types[39]);
        default_field_array = Field::create_field<TYPE_ARRAY>(Array());
        create_column_assert(type, default_field_array, 127);
    }
    {
        auto type = remove_nullable(array_types[37]);
        Field default_field_array = Field::create_field<TYPE_ARRAY>(Array());
        create_column_assert(type, default_field_array, 119);
        type = remove_nullable(array_types[38]);
        default_field_array = Field::create_field<TYPE_ARRAY>(Array());
        create_column_assert(type, default_field_array, 119); // 17 * 7
    }
    // for array-struct
    {
        auto type = remove_nullable(array_types[40]);
        Field default_field_array = Field::create_field<TYPE_ARRAY>(Array());
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
    for (auto& type : array_types) {
        const auto* array_type = assert_cast<const DataTypeArray*>(remove_nullable(type).get());
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
    MutableColumns array_cols;
    DataTypes types;
    for (int i = 0; i < array_descs.size(); i++) {
        array_cols.push_back(array_columns[i]->get_ptr());
        types.push_back(array_types[i]);
    }
    CommonDataTypeSerdeTest::assert_arrow_format(array_cols, types);
}

//================== datatype for array ut test ==================
TEST_F(DataTypeArrayTest, GetNumberOfDimensionsTest) {
    // for array-scalar
    for (int i = 0; i < 18; i++) {
        auto& type = array_types[i];
        const auto* array_type = assert_cast<const DataTypeArray*>(remove_nullable(type).get());
        // array dimension is only for array to nested array , if array nested map or struct, the dimension also be is 1
        EXPECT_EQ(array_type->get_number_of_dimensions(), 1) << "for type: " << type->get_name();
    }
    // for array-array
    for (int i = 18; i < 36; i++) {
        auto& type = array_types[i];
        auto desc = array_descs[i];
        const auto* array_type = assert_cast<const DataTypeArray*>(remove_nullable(type).get());
        // array dimension is only for array to nested array , if array nested map or struct, the dimension also be is 1
        EXPECT_EQ(array_type->get_number_of_dimensions(), 2) << "for type: " << type->get_name();
    }
    for (int i = 36; i < 41; i++) {
        auto& type = array_types[i];
        const auto* array_type = assert_cast<const DataTypeArray*>(remove_nullable(type).get());
        // array dimension is only for array to nested array , if array nested map or struct, the dimension also be is 1
        EXPECT_EQ(array_type->get_number_of_dimensions(), 1) << "for type: " << type->get_name();
    }
}

TEST_F(DataTypeArrayTest, GetFieldWithDataTypeTest) {
    // 1. Simple type array: Array<Int32>
    {
        auto nested_type = std::make_shared<DataTypeNumber<TYPE_INT>>();
        auto array_type = std::make_shared<DataTypeArray>(nested_type);
        auto column = array_type->create_column();
        Array arr;
        arr.push_back(Field::create_field<TYPE_INT>(1));
        arr.push_back(Field::create_field<TYPE_INT>(2));
        column->insert(Field::create_field<TYPE_ARRAY>(arr));

        auto fdt = array_type->get_field_with_data_type(*column, 0);
        EXPECT_EQ(fdt.field.get_type(), TYPE_ARRAY);
        EXPECT_EQ(fdt.field.get<Array>(), arr);
        EXPECT_EQ(fdt.base_scalar_type_id, TYPE_INT);
        EXPECT_EQ(fdt.num_dimensions, 1);
        EXPECT_EQ(fdt.precision, -1);
        EXPECT_EQ(fdt.scale, -1);
    }

    // 2. Decimal array: Array<Decimal128V2>
    {
        auto nested_type = std::make_shared<DataTypeDecimal128>(27, 9);
        auto array_type = std::make_shared<DataTypeArray>(nested_type);
        auto column = array_type->create_column();
        Array arr;
        arr.push_back(
                Field::create_field<TYPE_DECIMAL128I>(DecimalField<Decimal128V3>(-12345678, 0)));
        arr.push_back(Field::create_field<TYPE_DECIMAL128I>(DecimalField<Decimal128V3>(12345, 9)));
        column->insert(Field::create_field<TYPE_ARRAY>(arr));

        auto fdt = array_type->get_field_with_data_type(*column, 0);
        EXPECT_EQ(fdt.field.get_type(), TYPE_ARRAY);
        EXPECT_EQ(fdt.base_scalar_type_id, TYPE_DECIMAL128I);
        EXPECT_EQ(fdt.num_dimensions, 1);
        EXPECT_EQ(fdt.precision, 27);
        EXPECT_EQ(fdt.scale, 9);
    }

    // 3. DateTimeV2 array: Array<DateTimeV2>
    {
        auto nested_type = std::make_shared<DataTypeDateTimeV2>(3);
        auto array_type = std::make_shared<DataTypeArray>(nested_type);
        auto column = array_type->create_column();
        Array arr;
        arr.push_back(Field::create_field<TYPE_DATETIMEV2>(static_cast<UInt64>(1234567890)));
        column->insert(Field::create_field<TYPE_ARRAY>(arr));

        auto fdt = array_type->get_field_with_data_type(*column, 0);
        EXPECT_EQ(fdt.field.get_type(), TYPE_ARRAY);
        EXPECT_EQ(fdt.base_scalar_type_id, TYPE_DATETIMEV2);
        EXPECT_EQ(fdt.num_dimensions, 1);
        EXPECT_EQ(fdt.precision, -1);
        EXPECT_EQ(fdt.scale, 3);
    }

    // 4. Jsonb array: Array<Jsonb>
    {
        auto nested_type = std::make_shared<DataTypeJsonb>();
        auto array_type = std::make_shared<DataTypeArray>(nested_type);
        auto column = array_type->create_column();
        const char* json_str = "{\"key\": \"value\"}";
        JsonbField jsonb_field(json_str, strlen(json_str));
        Array arr;
        arr.push_back(Field::create_field<TYPE_JSONB>(jsonb_field));
        column->insert(Field::create_field<TYPE_ARRAY>(arr));

        auto fdt = array_type->get_field_with_data_type(*column, 0);
        EXPECT_EQ(fdt.field.get_type(), TYPE_ARRAY);
        EXPECT_EQ(fdt.base_scalar_type_id, TYPE_JSONB);
        EXPECT_EQ(fdt.num_dimensions, 1);
        const auto& result_arr = fdt.field.get<Array>();
        EXPECT_EQ(result_arr.size(), 1);
        EXPECT_EQ(result_arr[0].get_type(), TYPE_JSONB);
        EXPECT_EQ(std::string(result_arr[0].get<JsonbField>().get_value(),
                              result_arr[0].get<JsonbField>().get_size()),
                  json_str);
    }

    // 5. Multi-dimensional array: Array<Array<Int32>>
    {
        auto nested_type = std::make_shared<DataTypeNumber<TYPE_INT>>();
        auto array_type_inner = std::make_shared<DataTypeArray>(nested_type);
        auto array_type_outer = std::make_shared<DataTypeArray>(array_type_inner);
        auto column = array_type_outer->create_column();
        Array inner_arr;
        inner_arr.push_back(Field::create_field<TYPE_INT>(1));
        Array outer_arr;
        outer_arr.push_back(Field::create_field<TYPE_ARRAY>(inner_arr));
        column->insert(Field::create_field<TYPE_ARRAY>(outer_arr));

        auto fdt = array_type_outer->get_field_with_data_type(*column, 0);
        EXPECT_EQ(fdt.field.get_type(), TYPE_ARRAY);
        EXPECT_EQ(fdt.base_scalar_type_id, TYPE_INT);
        EXPECT_EQ(fdt.num_dimensions, 2);
    }
}

} // namespace doris::vectorized
