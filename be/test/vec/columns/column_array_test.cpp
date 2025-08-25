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

#include "vec/columns/column.h"
#include "vec/columns/common_column_test.h"
#include "vec/core/types.h"
#include "vec/function/function_test_util.h"

// this test is gonna to make a template ColumnTest
// for example column_ip should test these functions

namespace doris::vectorized {
class ColumnArrayTest : public CommonColumnTest {
protected:
    void SetUp() override {
        // insert from data csv and assert insert result
        std::string data_file_dir = "regression-test/data/nereids_function_p0/array/";
        MutableColumns array_cols;
        // we need to load data from csv file into column_array list
        // step1. create data type for array nested type (const and nullable)
        // array<bool>
        InputTypeSet array_uint8 = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_BOOLEAN};
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
        // array<array<bool>>
        InputTypeSet array_array_uint8 = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_ARRAY,
                                          PrimitiveType::TYPE_BOOLEAN};
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
                PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_DATETIME,
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

        std::vector<InputTypeSet> array_typeIndex = {
                array_uint8,    array_tinyint,   array_smallint,   array_int,        array_bigint,
                array_largeint, array_float,     array_double,     array_ipv4,       array_ipv6,
                array_date,     array_datetime,  array_datev2,     array_datetimev2, array_varchar,
                array_decimal,  array_decimal64, array_decimal128, array_decimal256};
        std::vector<InputTypeSet> array_array_typeIndex = {
                array_array_uint8,     array_array_tinyint,    array_array_smallint,
                array_array_int,       array_array_bigint,     array_array_largeint,
                array_array_float,     array_array_double,     array_array_ipv4,
                array_array_ipv6,      array_array_date,       array_array_datetime,
                array_array_datev2,    array_array_datetimev2, array_array_varchar,
                array_array_decimal,   array_array_decimal64,  array_array_decimal128,
                array_array_decimal256};
        std::vector<InputTypeSet> array_map_typeIndex = {
                array_map_char_double, array_map_datetime_decimal, array_map_ipv4_ipv6,
                array_map_largeint_string};
        std::vector<InputTypeSet> array_struct_typeIndex = {array_struct};

        std::vector<ut_type::UTDataTypeDescs> descs;
        descs.reserve(array_typeIndex.size());
        for (int i = 0; i < array_typeIndex.size(); i++) {
            descs.emplace_back();
            InputTypeSet input_types {};
            input_types.push_back(array_typeIndex[i][0]);
            input_types.emplace_back(Nullable {any_cast<PrimitiveType>(array_typeIndex[i][1])});
            EXPECT_EQ(input_types[1].type(), &typeid(Nullable)) << "nested type is not nullable";
            EXPECT_TRUE(parse_ut_data_type(input_types, descs[i]));
        }

        for (int i = 0; i < array_array_typeIndex.size(); i++) {
            descs.emplace_back();
            InputTypeSet input_types {};
            input_types.push_back(array_array_typeIndex[i][0]);
            input_types.emplace_back(
                    Nullable {any_cast<PrimitiveType>(array_array_typeIndex[i][1])});
            input_types.emplace_back(
                    Nullable {any_cast<PrimitiveType>(array_array_typeIndex[i][2])});
            EXPECT_EQ(input_types[1].type(), &typeid(Nullable)) << "nested type is not nullable";
            EXPECT_EQ(input_types[2].type(), &typeid(Nullable)) << "nested type is not nullable";
            EXPECT_TRUE(parse_ut_data_type(input_types, descs[i + array_typeIndex.size()]));
        }

        for (int i = 0; i < array_map_typeIndex.size(); i++) {
            descs.emplace_back();
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
                    input_types, descs[i + array_typeIndex.size() + array_array_typeIndex.size()]));
        }

        for (int i = 0; i < array_struct_typeIndex.size(); i++) {
            descs.emplace_back();
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
                    input_types, descs[i + array_typeIndex.size() + array_array_typeIndex.size() +
                                       array_map_typeIndex.size()]));
        }

        // create column_array for each data type
        std::vector<std::string> data_files = {data_file_dir + "test_array_bool.csv",
                                               data_file_dir + "test_array_tinyint.csv",
                                               data_file_dir + "test_array_smallint.csv",
                                               data_file_dir + "test_array_int.csv",
                                               data_file_dir + "test_array_bigint.csv",
                                               data_file_dir + "test_array_largeint.csv",
                                               data_file_dir + "test_array_float.csv",
                                               data_file_dir + "test_array_double.csv",
                                               data_file_dir + "test_array_ipv4.csv",
                                               data_file_dir + "test_array_ipv6.csv",
                                               data_file_dir + "test_array_date.csv",
                                               data_file_dir + "test_array_datetime.csv",
                                               data_file_dir + "test_array_date.csv",
                                               data_file_dir + "test_array_datetimev2(6).csv",
                                               data_file_dir + "test_array_varchar(65535).csv",
                                               data_file_dir + "test_array_decimalv3(7,4).csv",
                                               data_file_dir + "test_array_decimalv3(16,10).csv",
                                               data_file_dir + "test_array_decimalv3(38,30).csv",
                                               data_file_dir + "test_array_decimalv3(76,56).csv"};

        data_files.insert(data_files.end(),
                          {data_file_dir + "test_array_array_bool.csv",
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
                           data_file_dir + "test_array_array_decimalv3(76,56).csv"});

        data_files.insert(data_files.end(), {data_file_dir + "test_array_map_char_double.csv",
                                             data_file_dir + "test_array_map_datetime_decimal.csv",
                                             data_file_dir + "test_array_map_ipv4_ipv6.csv",
                                             data_file_dir + "test_array_map_largeInt_string.csv"});

        data_files.insert(data_files.end(), {data_file_dir + "test_array_struct.csv"});

        // step2. according to the datatype to make column_array
        //          && load data from csv file into column_array
        EXPECT_EQ(descs.size(), data_files.size());
        for (int i = 0; i < array_typeIndex.size(); i++) {
            auto& desc = descs[i];
            auto& data_file = data_files[i];
            // first is array type
            auto& type = desc[0].data_type;
            LOG(INFO) << "type: " << type->get_name() << " with file: " << data_file;
            MutableColumns columns;
            columns.push_back(type->create_column());
            auto serde = type->get_serde(1);
            load_data_from_csv({serde}, columns, data_file, ';');
            array_columns.push_back(std::move(columns[0]));
            array_types.push_back(type);
            serdes.push_back(serde);
        }

        for (int i = 0; i < array_array_typeIndex.size(); i++) {
            auto& desc = descs[i + array_typeIndex.size()];
            auto& data_file = data_files[i + array_typeIndex.size()];
            // first is array type
            auto& type = desc[0].data_type;
            LOG(INFO) << "type: " << type->get_name() << " with file: " << data_file;
            MutableColumns columns;
            columns.push_back(type->create_column());
            auto serde = type->get_serde(1);
            load_data_from_csv({serde}, columns, data_file, ';');
            array_columns.push_back(std::move(columns[0]));
            array_types.push_back(type);
            serdes.push_back(serde);
        }

        for (int i = 0; i < array_map_typeIndex.size(); i++) {
            auto& desc = descs[i + array_typeIndex.size() + array_array_typeIndex.size()];
            auto& data_file = data_files[i + array_typeIndex.size() + array_array_typeIndex.size()];
            // first is array type
            auto& type = desc[0].data_type;
            LOG(INFO) << "type: " << type->get_name() << " with file: " << data_file;
            MutableColumns columns;
            columns.push_back(type->create_column());
            auto serde = type->get_serde(1);
            load_data_from_csv({serde}, columns, data_file, ';');
            array_columns.push_back(std::move(columns[0]));
            array_types.push_back(type);
            serdes.push_back(serde);
        }

        for (int i = 0; i < array_struct_typeIndex.size(); i++) {
            auto& desc = descs[i + array_typeIndex.size() + array_array_typeIndex.size() +
                               array_map_typeIndex.size()];
            auto& data_file = data_files[i + array_typeIndex.size() + array_array_typeIndex.size() +
                                         array_map_typeIndex.size()];
            // first is array type
            auto& type = desc[0].data_type;
            LOG(INFO) << "type: " << type->get_name() << " with file: " << data_file;
            MutableColumns columns;
            columns.push_back(type->create_column());
            auto serde = type->get_serde(1);
            load_data_from_csv({serde}, columns, data_file, ';');
            array_columns.push_back(std::move(columns[0]));
            array_types.push_back(type);
            serdes.push_back(serde);
        }
    }

    MutableColumns array_columns; // column_array list
    DataTypes array_types;
    DataTypeSerDeSPtrs serdes;
};

//////////////////////// basic function from column.h ////////////////////////
TEST_F(ColumnArrayTest, InsertManyFixLengthDataTest) {
    auto callback = [&](MutableColumns& load_cols, DataTypeSerDeSPtrs serders) {
        for (auto& col : array_columns) {
            EXPECT_ANY_THROW(col->insert_many_fix_len_data(nullptr, 0));
        }
    };
    assert_insert_many_fix_len_data(array_columns, serdes, callback);
}

TEST_F(ColumnArrayTest, InsertManyDictDataTest) {
    auto callback = [&](MutableColumns& load_cols, DataTypeSerDeSPtrs serders) {
        for (auto& col : array_columns) {
            EXPECT_ANY_THROW(col->insert_many_dict_data(nullptr, 0, nullptr, 0));
        }
    };
    assert_insert_many_dict_data(array_columns, serdes, callback);
}
// test assert_insert_many_continuous_binary_data
TEST_F(ColumnArrayTest, InsertManyContinuousBinaryDataTest) {
    auto callback = [&](MutableColumns& load_cols, DataTypeSerDeSPtrs serders) {
        for (auto& col : array_columns) {
            EXPECT_ANY_THROW(col->insert_many_continuous_binary_data(nullptr, nullptr, 1));
        }
    };
    assert_insert_many_continuous_binary_data(array_columns, serdes, callback);
}

TEST_F(ColumnArrayTest, InsertManyStringsOverflowTest) {
    assert_insert_many_strings_overflow(array_columns, serdes, nullptr);
}

TEST_F(ColumnArrayTest, InsertManyStringsTest) {
    assert_insert_many_strings(array_columns, serdes, nullptr);
}

// test insert_from
TEST_F(ColumnArrayTest, InsertFromTest) {
    assert_insert_from_callback(array_columns, serdes);
}

// test assert_insert_from_multi_column_callback
TEST_F(ColumnArrayTest, InsertFromMultiColumnTest) {
    assert_insert_from_multi_column_callback(array_columns, serdes);
}

TEST_F(ColumnArrayTest, InsertRangeFromTest) {
    assert_insert_range_from_callback(array_columns, serdes);
}

TEST_F(ColumnArrayTest, InsertManyFromTest) {
    assert_insert_many_from_callback(array_columns, serdes);
}

TEST_F(ColumnArrayTest, InsertIndicesFromTest) {
    assert_insert_indices_from_callback(array_columns, serdes);
}

TEST_F(ColumnArrayTest, InsertDefaultTest) {
    assert_insert_default_callback(array_columns, serdes);
}

TEST_F(ColumnArrayTest, InsertManyDefaultsTest) {
    assert_insert_many_defaults_callback(array_columns, serdes);
}

TEST_F(ColumnArrayTest, InsertDataTest) {
    // we expect insert_data will throw exception
    EXPECT_ANY_THROW(assert_insert_data_callback(array_columns, serdes));
}

TEST_F(ColumnArrayTest, InsertManyRawDataTest) {
    // we expect insert_many_row_data will throw exception
    EXPECT_ANY_THROW(assert_insert_many_raw_data_from_callback(array_columns, serdes));
}

TEST_F(ColumnArrayTest, GetDataAtTest) {
    // get_data_at is not support in column_array
    EXPECT_ANY_THROW(assert_get_data_at_callback(array_columns, serdes));
}

TEST_F(ColumnArrayTest, FieldTest) {
    MutableColumns array_columns_copy;
    DataTypeSerDeSPtrs serdes_copy;
    array_columns_copy.push_back(array_columns[42]->assume_mutable());
    serdes_copy.push_back(serdes[42]);
    assert_field_callback(array_columns_copy, serdes_copy);
}

TEST_F(ColumnArrayTest, GetRawDataTest) {
    EXPECT_ANY_THROW({ array_columns[0]->get_raw_data(); });
}

TEST_F(ColumnArrayTest, GetBoolTest) {
    EXPECT_ANY_THROW({ array_columns[0]->get_bool(0); });
}

TEST_F(ColumnArrayTest, GetIntTest) {
    EXPECT_ANY_THROW({ array_columns[0]->get_int(0); });
}

TEST_F(ColumnArrayTest, GetNameTest) {
    for (auto& col : array_columns) {
        // name should contains "Array"
        EXPECT_TRUE(col->get_name().find("Array") != std::string::npos);
    }
}

// test get_ratio_of_default_rows
TEST_F(ColumnArrayTest, GetRatioOfDefaultRowsTest) {
    assert_get_ratio_of_default_rows(array_columns, serdes);
}

//TEST_F(ColumnArrayTest, SerDeVecTest) {
//    // get_max_row_byte_size is not support in column_array
//    EXPECT_ANY_THROW(ser_deser_vec(array_columns, array_types));
//}

TEST_F(ColumnArrayTest, serDeserializeWithArenaImpl) {
    ser_deserialize_with_arena_impl(array_columns, array_types);
}

TEST_F(ColumnArrayTest, SizeTest) {
    assert_size_callback(array_columns, serdes);
}

TEST_F(ColumnArrayTest, ByteSizeTest) {
    assert_byte_size_callback(array_columns, serdes);
}

TEST_F(ColumnArrayTest, AllocateBytesTest) {
    assert_allocated_bytes_callback(array_columns, serdes);
}

TEST_F(ColumnArrayTest, PopbackTest) {
    assert_pop_back_callback(array_columns, serdes);
}

TEST_F(ColumnArrayTest, CloneTest) {
    assert_clone_resized_callback(array_columns, serdes);
}

// test assert_clone_empty
TEST_F(ColumnArrayTest, CloneEmptyTest) {
    for (auto& col : array_columns) {
        assert_clone_empty(*col);
    }
}

TEST_F(ColumnArrayTest, CutTest) {
    assert_cut_callback(array_columns, serdes);
}

TEST_F(ColumnArrayTest, ShrinkTest) {
    assert_shrink_callback(array_columns, serdes);
}

TEST_F(ColumnArrayTest, ResizeTest) {
    assert_resize_callback(array_columns, serdes);
}

TEST_F(ColumnArrayTest, ReserveTest) {
    assert_reserve_callback(array_columns, serdes);
}

TEST_F(ColumnArrayTest, ReplaceColumnTest) {
    // replace_column_data is not support in column_array, only support non-variable length column
    EXPECT_ANY_THROW(assert_replace_column_data_callback(array_columns, serdes));
    assert_replace_column_null_data_callback(array_columns, serdes);
}

TEST_F(ColumnArrayTest, AppendDataBySelectorTest) {
    assert_append_data_by_selector_callback(array_columns, serdes);
}

TEST_F(ColumnArrayTest, FilterInplaceTest) {
    // The filter method implemented by column_array does not achieve the memory reuse acceleration effect like other basic types,
    // and still returns a new ptr, which can be make a todo task
    assert_filter_callback(array_columns, serdes);
}

TEST_F(ColumnArrayTest, FilterTest) {
    // filter with result_size_hint
    assert_filter_with_result_hint_callback(array_columns, serdes);
}

// HASH Interfaces
TEST_F(ColumnArrayTest, HashTest) {
    // XXHash
    assert_update_hashes_with_value_callback(array_columns, serdes);
    // XXhash with null_data

    // CrcHash
    std::vector<PrimitiveType> pts(array_columns.size(), PrimitiveType::TYPE_ARRAY);
    assert_update_crc_hashes_callback(array_columns, serdes, pts);
    // CrcHash with null_data

    // SipHash
    assert_update_siphashes_with_value_callback(array_columns, serdes);
};

// test assert_convert_to_full_column_if_const_callback
TEST_F(ColumnArrayTest, ConvertToFullColumnIfConstTest) {
    assert_convert_to_full_column_if_const_callback(array_columns, array_types, nullptr);
}

// test assert_convert_column_if_overflow_callback
TEST_F(ColumnArrayTest, ConvertColumnIfOverflowTest) {
    assert_convert_column_if_overflow_callback(array_columns, serdes);
}

// test assert_convert_to_predicate_column_if_dictionary_callback
TEST_F(ColumnArrayTest, ConvertToPredicateColumnIfDictionaryTest) {
    assert_convert_to_predicate_column_if_dictionary_callback(array_columns, array_types, nullptr);
}

// test assert_convert_dict_codes_if_necessary_callback
TEST_F(ColumnArrayTest, ConvertDictCodesIfNecessaryTest) {
    auto callback = [&](IColumn* col, size_t index) {
        checkColumn(*col->get_ptr(), *array_columns[index], col->size());
    };
    assert_convert_dict_codes_if_necessary_callback(array_columns, callback);
}

// test assert_column_nullable_funcs
TEST_F(ColumnArrayTest, ColumnNullableFuncsTest) {
    assert_column_nullable_funcs(array_columns, nullptr);
}

// test assert_column_string_funcs
TEST_F(ColumnArrayTest, ColumnStringFuncsTest) {
    assert_column_string_funcs(array_columns);
}

// test shrink_padding_chars_callback
TEST_F(ColumnArrayTest, ShrinkPaddingCharsTest) {
    shrink_padding_chars_callback(array_columns, serdes);
}

//////////////////////// special function from column_array.h ////////////////////////
TEST_F(ColumnArrayTest, CreateArrayTest) {
    // test create_array : nested_column && offsets_column should not be const, and convert_to_full_column_if_const should not impl in array
    // in some situation,
    //  like join_probe_operator.cpp::_build_output_block,
    //  we call column.convert_to_full_column_if_const,
    //  then we may call clear_column_data() to clear the column (eg. in HashJoinProbeOperatorX::pull() which call local_state._probe_block.clear_column_data after filter_data_and_build_output())
    //  in clear_column_data() if use_count() == 1, we will call column->clear() to clear the column data
    //
    //  however in array impl for convert_to_full_column_if_const: ``` ColumnArray::create(data->convert_to_full_column_if_const(), offsets);```
    //  may make the nested_column use_count() more than 1 which means it is shared with other block, but return ColumnArray is new which use_count() is 1,
    //  then in clear_column_data() if we will call array_column->use_count() == 1 will be true to clear the column with nested_column, and shared nested_column block will meet undefined behavior cause maybe core
    //
    //  so actually according to the semantics of the function, it should not impl in array,
    //  but we should make sure in creation of array, the nested_column && offsets_column should not be const
    for (auto& array_column : array_columns) {
        const auto* column = check_and_get_column<ColumnArray>(
                remove_nullable(array_column->assume_mutable()).get());
        auto column_size = column->size();
        LOG(INFO) << "column_type: " << column->get_name();
        // test create_array
        // test create expect exception case
        // 1.offsets is not ColumnOffset64
        auto tmp_data_col = column->get_data_ptr()->clone_resized(1);
        MutableColumnPtr tmp_offsets_col =
                assert_cast<const ColumnArray::ColumnOffsets&>(column->get_offsets_column())
                        .clone_resized(1);
        // 2.offsets size is not equal to data size
        auto tmp_data_col1 = column->get_data_ptr()->clone_resized(2);
        EXPECT_ANY_THROW({
            auto new_array_column = ColumnArray::create(
                    tmp_data_col1->assume_mutable(),
                    column->get_offsets_column().clone_resized(1)->assume_mutable());
        });
        // 3.data is const
        auto last_offset = column->get_offsets().back();
        EXPECT_ANY_THROW(
                { auto const_col = ColumnConst::create(column->get_data_ptr(), last_offset); });
        Field assert_field;
        column->get(0, assert_field);
        auto const_col = ColumnConst::create(tmp_data_col->assume_mutable(), last_offset);
        EXPECT_ANY_THROW({
            // const_col is not empty
            auto new_array_column = ColumnArray::create(const_col->assume_mutable());
        });
        auto new_array_column =
                ColumnArray::create(const_col->assume_mutable(), column->get_offsets_ptr());
        EXPECT_EQ(new_array_column->size(), column_size)
                << "array_column size is not equal to column size";
        EXPECT_EQ(new_array_column->get_data_ptr()->size(), column->get_data_ptr()->size());
        EXPECT_EQ(new_array_column->get_offsets_ptr()->size(), column->get_offsets_ptr()->size());
        // check column data
        for (size_t j = 0; j < column_size; j++) {
            Field f1;
            new_array_column->get(j, f1);
            EXPECT_EQ(f1, assert_field) << "array_column data is not equal to column data";
        }
    }
}

TEST_F(ColumnArrayTest, MetaInfoTest) {
    // test is_variable_length which should all be true
    for (int i = 0; i < array_columns.size(); i++) {
        auto& column = array_columns[i];
        auto& type = array_types[i];
        auto column_type = type->get_name();
        EXPECT_TRUE(column->is_variable_length()) << "column is not variable length";
    }
}

TEST_F(ColumnArrayTest, ConvertIfOverflowAndInsertTest) {
    // test nested string in array which like ColumnArray<ColumnString> only use in join
    // test convert_column_if_overflow
    for (int i = 0; i < array_columns.size(); i++) {
        auto& column = array_columns[i];
        auto type = array_types[i];
        auto nested_type =
                assert_cast<const DataTypeArray*>(remove_nullable(type).get())->get_nested_type();
        // check ptr is itself
        auto ptr = column->convert_column_if_overflow();
        EXPECT_EQ(ptr.get(), column.get());
        auto arr_col =
                check_and_get_column<ColumnArray>(remove_nullable(column->assume_mutable()).get());
        auto nested_col = arr_col->get_data_ptr();
        auto array_col1 = check_and_get_column<ColumnArray>(remove_nullable(ptr).get());
        auto nested_col1 = array_col1->get_data_ptr();
        EXPECT_EQ(nested_col.get(), nested_col1.get());
        // check column data
        auto column_size = column->size();
        LOG(INFO) << "column_size: " << column_size;
        for (size_t j = 0; j < column_size; j++) {
            Field f1;
            column->get(j, f1);
            Field f2;
            ptr->get(j, f2);
            EXPECT_EQ(f1, f2) << "array_column data is not equal to column data";
        }
    }
}

// Test insert_range_from_ignore_overflow
TEST_F(ColumnArrayTest, InsertRangeFromIgnoreOverflowTest) {
    // test insert_range_from_ignore_overflow
    assert_insert_range_from_ignore_overflow(array_columns, array_types);
}

TEST_F(ColumnArrayTest, GetNumberOfDimensionsTest) {
    // test dimension of array
    for (int i = 0; i < array_columns.size(); i++) {
        auto column = check_and_get_column<ColumnArray>(
                remove_nullable(array_columns[i]->assume_mutable()).get());
        auto check_type = remove_nullable(array_types[i]);
        auto dimension = 0;
        while (check_type->get_primitive_type() == TYPE_ARRAY && !check_type->is_nullable()) {
            auto nested_type =
                    assert_cast<const vectorized::DataTypeArray&>(*check_type).get_nested_type();
            dimension++;
            check_type = nested_type;
        }
        EXPECT_EQ(column->get_number_of_dimensions(), dimension)
                << "column " << column->get_name()
                << " dimension is not equal to check_type dimension";
    }
}

TEST_F(ColumnArrayTest, IsExclusiveTest) {
    auto callback = [&](const MutableColumns& columns, const DataTypeSerDeSPtrs& serdes) {
        for (int i = 0; i < columns.size(); i++) {
            auto column = check_and_get_column<ColumnArray>(
                    remove_nullable(columns[i]->assume_mutable()).get());
            auto cloned = columns[i]->clone_resized(1);
            // test expect true
            EXPECT_TRUE(column->is_exclusive());
            // new column with different data column
            const ColumnPtr new_data_column =
                    column->get_data_ptr()->clone_resized(0)->convert_column_if_overflow();
            auto new_array_column = ColumnArray::create(new_data_column);
            EXPECT_FALSE(new_array_column->is_exclusive());
            // new column with different offsets column
            const ColumnPtr new_offsets_column =
                    column->get_offsets_ptr()->clone_resized(0)->convert_column_if_overflow();
            new_array_column = ColumnArray::create(column->get_data_ptr(), new_offsets_column);
            EXPECT_FALSE(new_array_column->is_exclusive());
        }
    };
    assert_is_exclusive(array_columns, serdes, callback);
}

TEST_F(ColumnArrayTest, MaxArraySizeAsFieldTest) {
    // test array max_array_size_as_field which is set to 100w
    //  in operator[] and get()
    for (int i = 0; i < array_columns.size(); i++) {
        auto column = check_and_get_column<ColumnArray>(
                remove_nullable(array_columns[i]->assume_mutable()).get());
        auto check_type = remove_nullable(array_types[i]);
        Field a;
        column->get(column->size() - 1, a);
        Array af = a.get<Array>();
        if (af.size() > 0) {
            auto start_size = af.size();
            Field ef = af[0];
            for (int j = 0; j < max_array_size_as_field; ++j) {
                af.push_back(ef);
            }
            EXPECT_EQ(af.size(), start_size + max_array_size_as_field)
                    << "array size is not equal to start size + max_array_size_as_field";
            auto cloned = column->clone_resized(0);
            cloned->insert(Field::create_field<TYPE_ARRAY>(af));
            // get cloned offset size
            auto cloned_offset_size =
                    check_and_get_column<ColumnArray>(cloned.get())->get_offsets().back();
            EXPECT_EQ(cloned_offset_size, start_size + max_array_size_as_field)
                    << "cloned offset size is not equal to start size + max_array_size_as_field";

            Field f;
            // test get
            EXPECT_ANY_THROW({ cloned->get(0, f); });
            // test operator[]
            EXPECT_ANY_THROW({ cloned->operator[](0); });
        }
    }
}

TEST_F(ColumnArrayTest, IsDefaultAtTest) {
    // default means meet empty array row in column_array, now just only used in ColumnVariant.
    // test is_default_at
    for (int i = 0; i < array_columns.size(); i++) {
        auto column = check_and_get_column<ColumnArray>(
                remove_nullable(array_columns[i]->assume_mutable()).get());
        auto column_size = column->size();
        for (int j = 0; j < column_size; j++) {
            auto is_default = column->is_default_at(j);
            if (is_default) {
                // check field Array is empty
                Field f;
                column->get(j, f);
                auto array = f.get<Array>();
                EXPECT_EQ(array.size(), 0) << "array is not empty";
            } else {
                Field f;
                column->get(j, f);
                auto array = f.get<Array>();
                EXPECT_GT(array.size(), 0) << "array is empty";
            }
        }
    }
}

TEST_F(ColumnArrayTest, HasEqualOffsetsTest) {
    // test has_equal_offsets which more likely used in function, eg: function_array_zip
    for (int i = 0; i < array_columns.size(); i++) {
        auto column = check_and_get_column<ColumnArray>(
                remove_nullable(array_columns[i]->assume_mutable()).get());
        auto cloned = array_columns[i]->clone_resized(array_columns[i]->size());
        auto cloned_arr =
                check_and_get_column<ColumnArray>(remove_nullable(cloned->assume_mutable()).get());
        // test expect true
        EXPECT_EQ(column->get_offsets().size(), cloned_arr->get_offsets().size());
        EXPECT_TRUE(column->has_equal_offsets(*cloned_arr));
        // cloned column size is not equal to column size
        cloned->pop_back(1);
        EXPECT_FALSE(column->has_equal_offsets(*cloned_arr));
        cloned->insert_default();
        EXPECT_FALSE(column->has_equal_offsets(*cloned_arr));
    }
}

TEST_F(ColumnArrayTest, String64ArrayTest) {
    auto off_column = ColumnOffset64::create();
    auto str64_column = ColumnString64::create();
    // init column array with [["abc","d"],["ef"],[], [""]];
    std::vector<ColumnArray::Offset64> offs = {0, 2, 3, 3, 4};
    std::vector<std::string> vals = {"abc", "d", "ef", ""};
    for (size_t i = 1; i < offs.size(); ++i) {
        off_column->insert_data((const char*)(&offs[i]), 0);
    }
    for (auto& v : vals) {
        str64_column->insert_data(v.data(), v.size());
    }
    auto str64_array_column = ColumnArray::create(std::move(str64_column), std::move(off_column));
    EXPECT_EQ(str64_array_column->size(), offs.size() - 1);
    for (size_t i = 0; i < str64_array_column->size(); ++i) {
        auto v = get<Array>(str64_array_column->operator[](i));
        EXPECT_EQ(v.size(), offs[i + 1] - offs[i]);
        for (size_t j = 0; j < v.size(); ++j) {
            EXPECT_EQ(vals[offs[i] + j], get<std::string>(v[j]));
        }
    }
    // test insert ColumnArray<ColumnStr<uint64_t>> into ColumnArray<ColumnStr<uint32_t>>
    auto str32_column = ColumnString::create();
    auto str32_array_column = ColumnArray::create(std::move(str32_column));
    std::vector<uint32_t> indices;
    indices.push_back(0);
    indices.push_back(1);
    indices.push_back(3);
    str32_array_column->insert_indices_from(*str64_array_column, indices.data(),
                                            indices.data() + indices.size());
    EXPECT_EQ(str32_array_column->size(), 3);

    auto v = get<Array>(str32_array_column->operator[](0));
    EXPECT_EQ(v.size(), 2);
    EXPECT_EQ(get<std::string>(v[0]), vals[0]);
    EXPECT_EQ(get<std::string>(v[1]), vals[1]);

    v = get<Array>(str32_array_column->operator[](1));
    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(get<std::string>(v[0]), vals[2]);

    v = get<Array>(str32_array_column->operator[](2));
    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(get<std::string>(v[0]), vals[3]);
}

TEST_F(ColumnArrayTest, IntArrayPermuteTest) {
    auto off_column = ColumnOffset64::create();
    auto data_column = ColumnInt32::create();
    // init column array with [[1,2,3],[],[4],[5,6]]
    std::vector<ColumnArray::Offset64> offs = {0, 3, 3, 4, 6};
    std::vector<int32_t> vals = {1, 2, 3, 4, 5, 6};
    for (size_t i = 1; i < offs.size(); ++i) {
        off_column->insert_data((const char*)(&offs[i]), 0);
    }
    for (auto& v : vals) {
        data_column->insert_data((const char*)(&v), 0);
    }
    auto array_column = ColumnArray::create(std::move(data_column), std::move(off_column));

    IColumn::Permutation perm = {3, 2, 1, 0};
    // return array column: [[5,6],[4]];
    auto res1 = array_column->permute(perm, 2);
    // check offsets
    IColumn::Offsets offs1 = {2, 3};
    auto arr_col = check_and_get_column<ColumnArray>(*res1);
    ASSERT_EQ(arr_col->size(), offs1.size());
    for (size_t i = 0; i < arr_col->size(); ++i) {
        ASSERT_EQ(arr_col->get_offsets()[i], offs1[i]);
    }
    // check data
    std::vector<int32_t> data = {5, 6, 4};
    auto data_col = arr_col->get_data_ptr();
    ASSERT_EQ(data_col->size(), data.size());
    for (size_t i = 0; i < data_col->size(); ++i) {
        auto element = data_col->get_data_at(i);
        ASSERT_EQ(*((int32_t*)element.data), data[i]);
    }

    // return array column: [[5,6],[4],[],[1,2,3]]
    auto res2 = array_column->permute(perm, 0);
    // check offsets
    IColumn::Offsets offs2 = {2, 3, 3, 6};
    arr_col = check_and_get_column<ColumnArray>(*res2);
    ASSERT_EQ(arr_col->size(), offs2.size());
    for (size_t i = 0; i < arr_col->size(); ++i) {
        ASSERT_EQ(arr_col->get_offsets()[i], offs2[i]);
    }
    // check data
    std::vector<int32_t> data2 = {5, 6, 4, 1, 2, 3};
    data_col = arr_col->get_data_ptr();
    ASSERT_EQ(data_col->size(), data2.size());
    for (size_t i = 0; i < data_col->size(); ++i) {
        auto element = data_col->get_data_at(i);
        ASSERT_EQ(*((int32_t*)element.data), data2[i]);
    }
}

TEST_F(ColumnArrayTest, ArrayTypeTesterase) {
    DataTypePtr datetype_32 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
    DataTypePtr datetype_array = std::make_shared<DataTypeArray>(datetype_32);
    auto c = datetype_array->create_column();
    auto column_res = datetype_array->create_column();
    auto* column_array = assert_cast<ColumnArray*>(c.get());
    auto& column_offsets = column_array->get_offsets_column();
    auto& column_data = column_array->get_data();

    std::vector<int32_t> data = {1, 2, 3, 4, 5};
    std::vector<int32_t> data2 = {11, 22};
    std::vector<int32_t> data3 = {33, 44, 55};
    // insert null
    std::vector<int32_t> data5 = {66};

    std::vector<UInt64> offset = {5, 7, 10, 11, 12};
    for (auto d : data) {
        column_data.insert_data(reinterpret_cast<const char*>(&d), sizeof(d));
    }
    for (auto d : data2) {
        column_data.insert_data(reinterpret_cast<const char*>(&d), sizeof(d));
    }
    for (auto d : data3) {
        column_data.insert_data(reinterpret_cast<const char*>(&d), sizeof(d));
    }
    column_data.insert_default();
    for (auto d : data5) {
        column_data.insert_data(reinterpret_cast<const char*>(&d), sizeof(d));
    }

    for (auto d : offset) {
        column_offsets.insert_data(reinterpret_cast<const char*>(&d), sizeof(d));
    }

    column_res->insert_range_from(*column_array, 0, offset.size());
    column_array->erase(0, 2);
    EXPECT_EQ(column_array->size(), 3);
    // Block tmp;
    // tmp.insert({std::move(c), datetype_array, "asd"});
    // std::cout << tmp.dump_data(0, tmp.rows());

    // Block tmp2;
    // tmp2.insert({std::move(column_res), datetype_array, "asd2"});
    // std::cout << tmp2.dump_data(0, tmp2.rows());
    auto* column_result = assert_cast<ColumnArray*>(column_res.get());
    auto& column_offsets_res = column_result->get_offsets_column();
    auto& offset_data_res = assert_cast<ColumnOffset64&>(column_offsets_res);
    auto& offset_data = assert_cast<ColumnOffset64&>(column_offsets);
    auto& column_data_res = assert_cast<ColumnInt32&>(
            assert_cast<ColumnNullable&>(column_result->get_data()).get_nested_column());
    auto& column_data_origin = assert_cast<ColumnInt32&>(
            assert_cast<ColumnNullable&>(column_data).get_nested_column());
    for (int i = 0; i < column_array->size(); ++i) {
        std::cout << datetype_array->to_string(*column_array, i) << std::endl;
        std::cout << datetype_array->to_string(*column_res, i + 2) << std::endl;
        EXPECT_EQ(column_data_origin.get_element(i), column_data_res.get_element(i + 7));
        EXPECT_EQ(offset_data.get_element(i), offset_data_res.get_element(i + 2) - 7);
    }
}

TEST_F(ColumnArrayTest, ArrayTypeTest2erase) {
    DataTypePtr datetype_32 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
    DataTypePtr datetype_array = std::make_shared<DataTypeArray>(datetype_32);
    auto c = datetype_array->create_column();
    auto* column_array = assert_cast<ColumnArray*>(c.get());
    auto column_res = column_array->clone_empty();
    auto& column_offsets = column_array->get_offsets_column();
    auto& column_data = column_array->get_data();

    std::vector<int32_t> data = {1, 2, 3, 4, 5};
    std::vector<int32_t> data2 = {11, 22};
    std::vector<int32_t> data3 = {33, 44, 55};
    // insert null
    std::vector<int32_t> data5 = {66};

    std::vector<UInt64> offset = {5, 7, 10, 11, 12};
    for (auto d : data) {
        column_data.insert_data(reinterpret_cast<const char*>(&d), sizeof(d));
    }
    for (auto d : data2) {
        column_data.insert_data(reinterpret_cast<const char*>(&d), sizeof(d));
    }
    for (auto d : data3) {
        column_data.insert_data(reinterpret_cast<const char*>(&d), sizeof(d));
    }
    column_data.insert_default();
    for (auto d : data5) {
        column_data.insert_data(reinterpret_cast<const char*>(&d), sizeof(d));
    }

    for (auto d : offset) {
        column_offsets.insert_data(reinterpret_cast<const char*>(&d), sizeof(d));
    }

    column_res->insert_from(*column_array, 0);
    column_res->insert_from(*column_array, 1);
    column_res->insert_from(*column_array, 4);

    column_array->erase(2, 2);
    EXPECT_EQ(column_array->size(), 3);
    std::cout << "have call erase" << std::endl;
    // Block tmp;
    // tmp.insert({std::move(c), datetype_array, "asd"});
    // std::cout << tmp.dump_data(0, tmp.rows());

    // Block tmp2;
    // tmp2.insert({std::move(column_res), datetype_array, "asd2"});
    // std::cout << tmp2.dump_data(0, tmp2.rows());

    auto* column_result = assert_cast<ColumnArray*>(column_res.get());
    auto& column_offsets_res = column_result->get_offsets_column();
    auto& offset_data_res = assert_cast<ColumnOffset64&>(column_offsets_res);
    auto& offset_data = assert_cast<ColumnOffset64&>(column_offsets);
    auto& column_data_res = assert_cast<ColumnInt32&>(
            assert_cast<ColumnNullable&>(column_result->get_data()).get_nested_column());
    auto& column_data_origin = assert_cast<ColumnInt32&>(
            assert_cast<ColumnNullable&>(column_data).get_nested_column());
    for (int i = 0; i < column_array->size(); ++i) {
        std::cout << datetype_array->to_string(*column_array, i) << std::endl;
        std::cout << datetype_array->to_string(*column_res, i) << std::endl;
        EXPECT_EQ(column_data_origin.get_element(i), column_data_res.get_element(i));
        EXPECT_EQ(offset_data.get_element(i), offset_data_res.get_element(i));
    }
}

} // namespace doris::vectorized
