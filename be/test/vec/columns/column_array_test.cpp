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
        BaseInputTypeSet array_uint8 = {TypeIndex::Array, TypeIndex::UInt8};
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
        // array<array<bool>>
        BaseInputTypeSet array_array_uint8 = {TypeIndex::Array, TypeIndex::Array, TypeIndex::UInt8};
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

        std::vector<BaseInputTypeSet> array_typeIndex = {
                array_uint8,    array_tinyint,   array_smallint,   array_int,        array_bigint,
                array_largeint, array_float,     array_double,     array_ipv4,       array_ipv6,
                array_date,     array_datetime,  array_datev2,     array_datetimev2, array_varchar,
                array_decimal,  array_decimal64, array_decimal128, array_decimal256};
        std::vector<BaseInputTypeSet> array_array_typeIndex = {
                array_array_uint8,     array_array_tinyint,    array_array_smallint,
                array_array_int,       array_array_bigint,     array_array_largeint,
                array_array_float,     array_array_double,     array_array_ipv4,
                array_array_ipv6,      array_array_date,       array_array_datetime,
                array_array_datev2,    array_array_datetimev2, array_array_varchar,
                array_array_decimal,   array_array_decimal64,  array_array_decimal128,
                array_array_decimal256};
        std::vector<BaseInputTypeSet> array_map_typeIndex = {
                array_map_char_double, array_map_datetime_decimal, array_map_ipv4_ipv6,
                array_map_largeint_string};
        std::vector<BaseInputTypeSet> array_struct_typeIndex = {array_struct};

        vector<ut_type::UTDataTypeDescs> descs;
        descs.reserve(array_typeIndex.size());
        for (int i = 0; i < array_typeIndex.size(); i++) {
            descs.push_back(ut_type::UTDataTypeDescs());
            InputTypeSet input_types {};
            input_types.push_back(array_typeIndex[i][0]);
            input_types.push_back(Nullable {static_cast<TypeIndex>(array_typeIndex[i][1])});
            EXPECT_EQ(input_types[1].type(), &typeid(Nullable)) << "nested type is not nullable";
            EXPECT_TRUE(parse_ut_data_type(input_types, descs[i]));
        }

        for (int i = 0; i < array_array_typeIndex.size(); i++) {
            descs.push_back(ut_type::UTDataTypeDescs());
            InputTypeSet input_types {};
            input_types.push_back(array_array_typeIndex[i][0]);
            input_types.push_back(Nullable {static_cast<TypeIndex>(array_array_typeIndex[i][1])});
            input_types.push_back(Nullable {static_cast<TypeIndex>(array_array_typeIndex[i][2])});
            EXPECT_EQ(input_types[1].type(), &typeid(Nullable)) << "nested type is not nullable";
            EXPECT_EQ(input_types[2].type(), &typeid(Nullable)) << "nested type is not nullable";
            EXPECT_TRUE(parse_ut_data_type(input_types, descs[i + array_typeIndex.size()]));
        }

        for (int i = 0; i < array_map_typeIndex.size(); i++) {
            descs.push_back(ut_type::UTDataTypeDescs());
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
                    input_types, descs[i + array_typeIndex.size() + array_array_typeIndex.size()]));
        }

        for (int i = 0; i < array_struct_typeIndex.size(); i++) {
            descs.push_back(ut_type::UTDataTypeDescs());
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
                    input_types, descs[i + array_typeIndex.size() + array_array_typeIndex.size() +
                                       array_map_typeIndex.size()]));
        }

        // create column_array for each data type
        vector<string> data_files = {data_file_dir + "test_array_bool.csv",
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
            EXPECT_ANY_THROW(col->insert_many_continuous_binary_data(nullptr, 0, 1));
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

TEST_F(ColumnArrayTest, SerDeVecTest) {
    // get_max_row_byte_size is not support in column_array
    EXPECT_ANY_THROW(ser_deser_vec(array_columns, array_types));
}

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

TEST_F(ColumnArrayTest, ReplicateTest) {
    // array_array_char will cause exception in replicate with: string column length is too large: total_length=4295103210, element_number=327295
    // so we need to skip it
    MutableColumns array_columns_copy;
    DataTypeSerDeSPtrs serdes_copy;
    // just skip array_array_char use vector copy
    for (int i = 0; i < array_columns.size(); i++) {
        if (i == 33) {
            continue;
        }
        array_columns_copy.push_back(array_columns[i]->assume_mutable());
        serdes_copy.push_back(serdes[i]);
    }
    assert_replicate_callback(array_columns_copy, serdes_copy);
    // expect error columns
    MutableColumns error_columns;
    error_columns.push_back(array_columns[33]->assume_mutable());
    DataTypeSerDeSPtrs error_serdes;
    error_serdes.push_back(serdes[33]);
    EXPECT_ANY_THROW(assert_replicate_callback(error_columns, error_serdes));
}

TEST_F(ColumnArrayTest, ReplaceColumnTest) {
    // replace_column_data is not support in column_array, only support non-variable length column
    EXPECT_ANY_THROW(assert_replace_column_data_callback(array_columns, serdes));
    assert_replace_column_null_data_callback(array_columns, serdes);
}

TEST_F(ColumnArrayTest, AppendDataBySelectorTest) {
    assert_append_data_by_selector_callback(array_columns, serdes);
}

TEST_F(ColumnArrayTest, PermutationAndSortTest) {
    for (int i = 0; i < array_columns.size(); i++) {
        auto& column = array_columns[i];
        auto& type = array_types[i];
        auto column_type = type->get_name();
        LOG(INFO) << "column_type: " << column_type;
        // permutation
        EXPECT_ANY_THROW(assert_column_permutations(column->assume_mutable_ref(), type));
    }
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
        checkColumn(*col->get_ptr(), *array_columns[index], *array_types[index], col->size());
    };
    assert_convert_dict_codes_if_necessary_callback(array_columns, callback);
}

// test assert_copy_date_types_callback
TEST_F(ColumnArrayTest, CopyDateTypesTest) {
    assert_copy_date_types_callback(array_columns);
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
    for (int i = 0; i < array_columns.size(); i++) {
        auto column = check_and_get_column<ColumnArray>(
                remove_nullable(array_columns[i]->assume_mutable()).get());
        auto column_size = column->size();
        LOG(INFO) << "column_type: " << column->get_name();
        // test create_array
        // test create expect exception case
        // 1.offsets is not ColumnUInt64
        auto tmp_data_col = column->get_data_ptr()->clone_resized(1);
        MutableColumnPtr tmp_offsets_col =
                assert_cast<const ColumnArray::ColumnOffsets&>(column->get_offsets_column())
                        .clone_resized(1);
        UInt64 off = tmp_offsets_col->operator[](0).get<UInt64>();
        // make offsets_col into column_int32
        auto wrong_type_offsets_col = vectorized::ColumnVector<vectorized::UInt128>::create(1, off);
        EXPECT_ANY_THROW({
            auto new_array_column = ColumnArray::create(tmp_data_col->assume_mutable(),
                                                        wrong_type_offsets_col->assume_mutable());
        });
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
        while (is_array(check_type)) {
            auto nested_type = reinterpret_cast<const vectorized::DataTypeArray&>(*check_type)
                                       .get_nested_type();
            dimension++;
            check_type = nested_type;
        }
        EXPECT_EQ(column->get_number_of_dimensions(), dimension)
                << "column dimension is not equal to check_type dimension";
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
            cloned->insert(af);
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
    // default means meet empty array row in column_array, now just only used in ColumnObject.
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
    auto off_column = ColumnVector<ColumnArray::Offset64>::create();
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
    auto off_column = ColumnVector<ColumnArray::Offset64>::create();
    auto data_column = ColumnVector<int32_t>::create();
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

} // namespace doris::vectorized
