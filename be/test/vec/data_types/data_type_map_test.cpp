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

#include <iostream>

#include "vec/columns/column.h"
#include "vec/core/types.h"
#include "vec/data_types/common_data_type_serder_test.h"
#include "vec/data_types/common_data_type_test.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_struct.h"
#include "vec/function/function_test_util.h"

/* similar to DataTypeArrayTest
 *
 * TODO: `DataTypeMapSerDe::deserialize_one_cell_from_json` has bug, must be fixed before continuing testing.
 *  1. json->map<float:float> and json->map<double:double> deserialization result lost decimals.
 *  2. json->map<datetime:datetime> deserialization result is NULL.
 *  3. json->map<ipv6:ipv6> deserialization result is wrong, '2001:0db8:0:0:0:0:0:1' -> '2001:db8::1'
 *  4. json->map<array<>, array<>> deserialization result is map<array<>, NULL>, value is NULL.
 *  6. json->map<map<double, decimal>, map<double, decimal>> deserialization result is map<map<double, decimal>, double>, value is not deserialized into map.
 *  7. json->map<map<ipv4, ipv6>, map<ipv4, ipv6>> deserialization result is map<map<ipv4, ipv6>, NULL>, value is NULL.
 *  7. json->map<struct<>, struct<>> deserialization result is map<NULL, NULL>.
*/

namespace doris::vectorized {

class DataTypeMapTest : public CommonDataTypeTest {
protected:
    void SetUp() override {
        // we need to load data from csv file into column_map list
        // step1. create data type for map nested type (const and nullable)
        // map<tinyint, tinyint>
        InputTypeSet map_tinyint = {PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_TINYINT,
                                    PrimitiveType::TYPE_TINYINT};
        // map<smallint, smallint>
        InputTypeSet map_smallint = {PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_SMALLINT,
                                     PrimitiveType::TYPE_SMALLINT};
        // map<int, int>
        InputTypeSet map_int = {PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_INT,
                                PrimitiveType::TYPE_INT};
        // map<bigintm, bigint>
        InputTypeSet map_bigint = {PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_BIGINT,
                                   PrimitiveType::TYPE_BIGINT};
        // map<largeint, largeint>
        InputTypeSet map_largeint = {PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_LARGEINT,
                                     PrimitiveType::TYPE_LARGEINT};
        // map<float, float>
        InputTypeSet map_float = {PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_FLOAT,
                                  PrimitiveType::TYPE_FLOAT};
        // map<double, double>
        InputTypeSet map_double = {PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_DOUBLE,
                                   PrimitiveType::TYPE_DOUBLE};
        // map<ipv4, ipv4>
        InputTypeSet map_ipv4 = {PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_IPV4,
                                 PrimitiveType::TYPE_IPV4};
        // map<ipv6, ipv6>
        InputTypeSet map_ipv6 = {PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_IPV6,
                                 PrimitiveType::TYPE_IPV6};
        // map<date, date>
        InputTypeSet map_date = {PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_DATE,
                                 PrimitiveType::TYPE_DATE};
        // map<datetime, datetime>
        InputTypeSet map_datetime = {PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_DATETIME,
                                     PrimitiveType::TYPE_DATETIME};
        // map<datev2, datev2>
        InputTypeSet map_datev2 = {PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_DATEV2,
                                   PrimitiveType::TYPE_DATEV2};
        // map<datetimev2, datetimev2>
        InputTypeSet map_datetimev2 = {PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_DATETIMEV2,
                                       PrimitiveType::TYPE_DATETIMEV2};
        // map<varchar, varchar>
        InputTypeSet map_varchar = {PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_VARCHAR,
                                    PrimitiveType::TYPE_VARCHAR};
        // map<decimal32(9, 5), decimal32(9, 5)> UT
        InputTypeSet map_decimal = {PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_DECIMAL32,
                                    PrimitiveType::TYPE_DECIMAL32};
        // map<decimal64(18, 9), decimal64(18, 9)> UT
        InputTypeSet map_decimal64 = {PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_DECIMAL64,
                                      PrimitiveType::TYPE_DECIMAL64};
        // map<decimal128(38, 20), decimal128(38, 20)> UT
        InputTypeSet map_decimal128 = {PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_DECIMAL128I,
                                       PrimitiveType::TYPE_DECIMAL128I};
        // map<decimal256(76, 40), decimal256(76, 40)> UT
        InputTypeSet map_decimal256 = {PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_DECIMAL256,
                                       PrimitiveType::TYPE_DECIMAL256};
        std::vector<InputTypeSet> map_typeIndex = {
                map_tinyint,   map_smallint,   map_int,        map_bigint,  map_largeint,
                map_float,     map_double,     map_ipv4,       map_ipv6,    map_date,
                map_datetime,  map_datev2,     map_datetimev2, map_varchar, map_decimal,
                map_decimal64, map_decimal128, map_decimal256};
        // map<array<tinyint>, array<tinyint>>
        InputTypeSet map_array_tinyint = {PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_ARRAY,
                                          PrimitiveType::TYPE_TINYINT, PrimitiveType::TYPE_ARRAY,
                                          PrimitiveType::TYPE_TINYINT};
        // map<array<smallint>, array<smallint>>
        InputTypeSet map_array_smallint = {PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_ARRAY,
                                           PrimitiveType::TYPE_SMALLINT, PrimitiveType::TYPE_ARRAY,
                                           PrimitiveType::TYPE_SMALLINT};
        // map<array<int, int>, array<int, int>>
        InputTypeSet map_array_int = {PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_ARRAY,
                                      PrimitiveType::TYPE_INT, PrimitiveType::TYPE_ARRAY,
                                      PrimitiveType::TYPE_INT};
        // map<array<bigint>, array<bigint>>
        InputTypeSet map_array_bigint = {PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_ARRAY,
                                         PrimitiveType::TYPE_BIGINT, PrimitiveType::TYPE_ARRAY,
                                         PrimitiveType::TYPE_BIGINT};
        // map<array<largeint>, array<largeint>>
        InputTypeSet map_array_largeint = {PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_ARRAY,
                                           PrimitiveType::TYPE_LARGEINT, PrimitiveType::TYPE_ARRAY,
                                           PrimitiveType::TYPE_LARGEINT};
        // map<array<float>, array<float>>
        InputTypeSet map_array_float = {PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_ARRAY,
                                        PrimitiveType::TYPE_FLOAT, PrimitiveType::TYPE_ARRAY,
                                        PrimitiveType::TYPE_FLOAT};
        // map<array<double>, array<double>>
        InputTypeSet map_array_double = {PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_ARRAY,
                                         PrimitiveType::TYPE_DOUBLE, PrimitiveType::TYPE_ARRAY,
                                         PrimitiveType::TYPE_DOUBLE};
        // map<array<ipv4>, array<ipv4>>
        InputTypeSet map_array_ipv4 = {PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_ARRAY,
                                       PrimitiveType::TYPE_IPV4, PrimitiveType::TYPE_ARRAY,
                                       PrimitiveType::TYPE_IPV4};
        // map<array<ipv6>, array<ipv6>>
        InputTypeSet map_array_ipv6 = {PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_ARRAY,
                                       PrimitiveType::TYPE_IPV6, PrimitiveType::TYPE_ARRAY,
                                       PrimitiveType::TYPE_IPV6};
        // map<array<date>, array<date>>
        InputTypeSet map_array_date = {PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_ARRAY,
                                       PrimitiveType::TYPE_DATE, PrimitiveType::TYPE_ARRAY,
                                       PrimitiveType::TYPE_DATE};
        // map<array<datetime>, array<datetime>>
        InputTypeSet map_array_datetime = {PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_ARRAY,
                                           PrimitiveType::TYPE_DATETIME, PrimitiveType::TYPE_ARRAY,
                                           PrimitiveType::TYPE_DATETIME};
        // map<array<datev2>, array<datev2>>
        InputTypeSet map_array_datev2 = {PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_ARRAY,
                                         PrimitiveType::TYPE_DATEV2, PrimitiveType::TYPE_ARRAY,
                                         PrimitiveType::TYPE_DATEV2};
        // map<array<datetimev2>, array<datetimev2>>
        InputTypeSet map_array_datetimev2 = {
                PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_DATETIMEV2,
                PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_DATETIMEV2};
        // map<array<varchar>, array<varchar>>
        InputTypeSet map_array_varchar = {PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_ARRAY,
                                          PrimitiveType::TYPE_VARCHAR, PrimitiveType::TYPE_ARRAY,
                                          PrimitiveType::TYPE_VARCHAR};
        // map<array<decimal32(9, 5)>, array<decimal32(9, 5)>>
        InputTypeSet map_array_decimal = {PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_ARRAY,
                                          PrimitiveType::TYPE_DECIMAL32, PrimitiveType::TYPE_ARRAY,
                                          PrimitiveType::TYPE_DECIMAL32};
        // map<array<decimal64(18, 9)>, array<decimal64(18, 9)>>
        InputTypeSet map_array_decimal64 = {
                PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_DECIMAL64,
                PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_DECIMAL64};
        // map<array<decimal128(38, 20)>, array<decimal128(38, 20)>>
        InputTypeSet map_array_decimal128 = {
                PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_DECIMAL128I,
                PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_DECIMAL128I};
        // map<array<decimal256(76, 40)>, array<decimal256(76, 40)>>
        InputTypeSet map_array_decimal256 = {
                PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_DECIMAL256,
                PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_DECIMAL256};
        // map<map<char, double>, map<char, double>>
        InputTypeSet map_map_char_double = {
                PrimitiveType::TYPE_MAP,    PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_VARCHAR,
                PrimitiveType::TYPE_DOUBLE, PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_VARCHAR,
                PrimitiveType::TYPE_DOUBLE};
        // map<map<datetime, decimal<76,56>>, map<datetime, decimal<76,56>>>
        InputTypeSet map_map_datetime_decimal = {
                PrimitiveType::TYPE_MAP,        PrimitiveType::TYPE_MAP,
                PrimitiveType::TYPE_DATETIMEV2, PrimitiveType::TYPE_DECIMAL256,
                PrimitiveType::TYPE_MAP,        PrimitiveType::TYPE_DATETIMEV2,
                PrimitiveType::TYPE_DECIMAL256};
        // map<map<ipv4, ipv6>, map<ipv4, ipv6>>
        InputTypeSet map_map_ipv4_ipv6 = {PrimitiveType::TYPE_MAP,  PrimitiveType::TYPE_MAP,
                                          PrimitiveType::TYPE_IPV4, PrimitiveType::TYPE_IPV6,
                                          PrimitiveType::TYPE_MAP,  PrimitiveType::TYPE_IPV4,
                                          PrimitiveType::TYPE_IPV6};
        // map<map<largeInt, string>, map<largeInt, string>>
        InputTypeSet map_map_largeint_string = {
                PrimitiveType::TYPE_MAP,     PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_LARGEINT,
                PrimitiveType::TYPE_VARCHAR, PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_LARGEINT,
                PrimitiveType::TYPE_VARCHAR};
        // map<struct<f1:int,f2:date,f3:decimal>, struct<f4:string,f5:double,f6:ipv4,f7:ipv6>>
        InputTypeSet map_struct = {PrimitiveType::TYPE_MAP,       PrimitiveType::TYPE_STRUCT,
                                   PrimitiveType::TYPE_INT,       PrimitiveType::TYPE_DATE,
                                   PrimitiveType::TYPE_DECIMAL32, PrimitiveType::TYPE_STRUCT,
                                   PrimitiveType::TYPE_VARCHAR,   PrimitiveType::TYPE_DOUBLE,
                                   PrimitiveType::TYPE_IPV4,      PrimitiveType::TYPE_IPV6};

        std::vector<InputTypeSet> map_array_typeIndex = {
                map_array_tinyint,    map_array_smallint,  map_array_int,      map_array_bigint,
                map_array_largeint,   map_array_float,     map_array_double,   map_array_ipv4,
                map_array_ipv6,       map_array_date,      map_array_datetime, map_array_datev2,
                map_array_datetimev2, map_array_varchar,   map_array_decimal,  map_array_decimal64,
                map_array_decimal128, map_array_decimal256};
        std::vector<InputTypeSet> map_map_typeIndex = {map_map_char_double,
                                                       map_map_datetime_decimal, map_map_ipv4_ipv6,
                                                       map_map_largeint_string};
        std::vector<InputTypeSet> map_struct_typeIndex = {map_struct};

        descs_.reserve(map_typeIndex.size() + map_array_typeIndex.size() +
                       map_map_typeIndex.size() + map_struct_typeIndex.size());
        for (int i = 0; i < map_typeIndex.size(); i++) {
            descs_.emplace_back();
            InputTypeSet input_types {};
            input_types.emplace_back(map_typeIndex[i][0]);
            input_types.emplace_back(Nullable {any_cast<PrimitiveType>(map_typeIndex[i][1])});
            input_types.emplace_back(Nullable {any_cast<PrimitiveType>(map_typeIndex[i][2])});
            EXPECT_EQ(input_types[1].type(), &typeid(Nullable)) << "nested type is not nullable";
            EXPECT_EQ(input_types[2].type(), &typeid(Nullable)) << "nested type is not nullable";
            EXPECT_TRUE(parse_ut_data_type(input_types, descs_[i]));
        }
        for (int i = 0; i < map_array_typeIndex.size(); i++) {
            descs_.emplace_back();
            InputTypeSet input_types {};
            input_types.emplace_back(map_array_typeIndex[i][0]);
            input_types.emplace_back(
                    Nullable {any_cast<PrimitiveType>(map_array_typeIndex[i][1])}); // array1
            input_types.emplace_back(Nullable {any_cast<PrimitiveType>(map_array_typeIndex[i][2])});
            input_types.emplace_back(
                    Nullable {any_cast<PrimitiveType>(map_array_typeIndex[i][3])}); // array2
            input_types.emplace_back(Nullable {any_cast<PrimitiveType>(map_array_typeIndex[i][4])});
            EXPECT_EQ(input_types[1].type(), &typeid(Nullable)) << "nested type is not nullable";
            EXPECT_EQ(input_types[2].type(), &typeid(Nullable)) << "nested type is not nullable";
            EXPECT_EQ(input_types[3].type(), &typeid(Nullable)) << "nested type is not nullable";
            EXPECT_EQ(input_types[4].type(), &typeid(Nullable)) << "nested type is not nullable";
            EXPECT_TRUE(parse_ut_data_type(input_types, descs_[i + map_typeIndex.size()]));
        }

        for (int i = 0; i < map_map_typeIndex.size(); i++) {
            descs_.emplace_back();
            InputTypeSet input_types {};
            input_types.emplace_back(map_map_typeIndex[i][0]); // map
            input_types.emplace_back(
                    Nullable {any_cast<PrimitiveType>(map_map_typeIndex[i][1])}); // map1
            input_types.emplace_back(Nullable {any_cast<PrimitiveType>(map_map_typeIndex[i][2])});
            input_types.emplace_back(Nullable {any_cast<PrimitiveType>(map_map_typeIndex[i][3])});
            input_types.emplace_back(
                    Nullable {any_cast<PrimitiveType>(map_map_typeIndex[i][4])}); // map2
            input_types.emplace_back(Nullable {any_cast<PrimitiveType>(map_map_typeIndex[i][5])});
            input_types.emplace_back(Nullable {any_cast<PrimitiveType>(map_map_typeIndex[i][6])});
            EXPECT_EQ(input_types[1].type(), &typeid(Nullable)) << "nested type is not nullable";
            EXPECT_EQ(input_types[2].type(), &typeid(Nullable)) << "nested type is not nullable";
            EXPECT_EQ(input_types[3].type(), &typeid(Nullable)) << "nested type is not nullable";
            EXPECT_EQ(input_types[4].type(), &typeid(Nullable)) << "nested type is not nullable";
            EXPECT_EQ(input_types[5].type(), &typeid(Nullable)) << "nested type is not nullable";
            EXPECT_EQ(input_types[6].type(), &typeid(Nullable)) << "nested type is not nullable";
            EXPECT_TRUE(parse_ut_data_type(
                    input_types, descs_[i + map_typeIndex.size() + map_array_typeIndex.size()]));
        }

        for (int i = 0; i < map_struct_typeIndex.size(); i++) {
            descs_.emplace_back();
            InputTypeSet input_types {};
            input_types.emplace_back(map_struct_typeIndex[i][0]); // map
            input_types.emplace_back(
                    Nullable {any_cast<PrimitiveType>(map_struct_typeIndex[i][1])}); // struct
            input_types.emplace_back(
                    Nullable {any_cast<PrimitiveType>(map_struct_typeIndex[i][2])});
            input_types.emplace_back(
                    Nullable {any_cast<PrimitiveType>(map_struct_typeIndex[i][3])});
            input_types.emplace_back(
                    Nullable {any_cast<PrimitiveType>(map_struct_typeIndex[i][4])});
            input_types.emplace_back(
                    Nullable {any_cast<PrimitiveType>(map_struct_typeIndex[i][5])});
            input_types.emplace_back(
                    Nullable {any_cast<PrimitiveType>(map_struct_typeIndex[i][6])});
            input_types.emplace_back(
                    Nullable {any_cast<PrimitiveType>(map_struct_typeIndex[i][7])});
            input_types.emplace_back(
                    Nullable {any_cast<PrimitiveType>(map_struct_typeIndex[i][8])});
            input_types.emplace_back(
                    Nullable {any_cast<PrimitiveType>(map_struct_typeIndex[i][9])});

            EXPECT_EQ(input_types[1].type(), &typeid(Nullable)) << "nested type is not nullable";
            EXPECT_TRUE(parse_ut_data_type(
                    input_types, descs_[i + map_typeIndex.size() + map_array_typeIndex.size() +
                                        map_map_typeIndex.size()]));
        }

        // create column_map for each data type
        // step2. according to the datatype to make column_map
        //          && load data from csv file into column_map
        EXPECT_EQ(descs_.size(), data_files.size());
        for (int i = 0; i < descs_.size(); i++) {
            auto& desc = descs_[i];
            auto& data_file = data_files[i];
            // first is map type
            auto& type = desc[0].data_type;
            MutableColumns cols;
            cols.push_back(type->create_column());
            std::cout << "load_data_from_csv type: " << type->get_name()
                      << ", col: " << cols[0]->get_name() << " with file: " << data_file
                      << std::endl;
            auto serde = type->get_serde(1);
            load_data_from_csv({serde}, cols, data_file, ';');
            columns_.push_back(std::move(cols[0]));
            types_.push_back(type);
            serdes_.push_back(serde);
        }
    }

    std::string data_file_dir = "regression-test/data/nereids_function_p0/map/";

    std::vector<std::string> data_files = {
            // map-scalar
            data_file_dir + "test_map_tinyint.csv", data_file_dir + "test_map_smallint.csv",
            data_file_dir + "test_map_int.csv", data_file_dir + "test_map_bigint.csv",
            data_file_dir + "test_map_largeint.csv",
            // TODO, json->map<float:float> and json->map<double:double> deserialization result lost decimals
            data_file_dir + "test_map_float.csv", data_file_dir + "test_map_double.csv",
            data_file_dir + "test_map_ipv4.csv",
            // TODO, json->map<ipv6:ipv6> deserialization result is wrong, '2001:0db8:0:0:0:0:0:1' -> '2001:db8::1'
            data_file_dir + "test_map_ipv6.csv", data_file_dir + "test_map_date.csv",
            // TODO, json->map<datetime:datetime> deserialization result is null
            data_file_dir + "test_map_datetime.csv", data_file_dir + "test_map_date.csv",
            data_file_dir + "test_map_datetimev2_6.csv",
            data_file_dir + "test_map_varchar_65535.csv",
            data_file_dir + "test_map_decimalv3_7_4.csv",
            data_file_dir + "test_map_decimalv3_16_10.csv",
            data_file_dir + "test_map_decimalv3_38_30.csv",
            data_file_dir + "test_map_decimalv3_76_56.csv",
            // map-array
            // TODO, json->map<array<>, array<>> deserialization result is map<array<>, NULL>, value is NULL.
            data_file_dir + "test_map_array_tinyint.csv",
            data_file_dir + "test_map_array_smallint.csv", data_file_dir + "test_map_array_int.csv",
            data_file_dir + "test_map_array_bigint.csv",
            data_file_dir + "test_map_array_largeint.csv",
            data_file_dir + "test_map_array_float.csv", data_file_dir + "test_map_array_double.csv",
            data_file_dir + "test_map_array_ipv4.csv", data_file_dir + "test_map_array_ipv6.csv",
            data_file_dir + "test_map_array_date.csv",
            data_file_dir + "test_map_array_datetime.csv",
            data_file_dir + "test_map_array_date.csv",
            data_file_dir + "test_map_array_datetimev2_5.csv",
            data_file_dir + "test_map_array_varchar_65535.csv",
            data_file_dir + "test_map_array_decimalv3_1_0.csv",
            data_file_dir + "test_map_array_decimalv3_27_9.csv",
            data_file_dir + "test_map_array_decimalv3_38_30.csv",
            data_file_dir + "test_map_array_decimalv3_76_56.csv",
            // map-map
            // TODO, json->map<map<double, decimal>, map<double, decimal>> deserialization result
            // is map<map<double, decimal>, double>, value is not deserialized into map.
            data_file_dir + "test_map_map_char_double.csv",
            data_file_dir + "test_map_map_datetime_decimal.csv",
            // TODO, json->map<map<ipv4, ipv6>, map<ipv4, ipv6>> deserialization result
            // is map<map<ipv4, ipv6>, NULL>, value is NULL.
            data_file_dir + "test_map_map_ipv4_ipv6.csv",
            data_file_dir + "test_map_map_largeInt_string.csv",
            // map-struct
            // TODO, json->map<struct<>, struct<>> deserialization result is map<NULL, NULL>.
            data_file_dir + "test_map_struct.csv"};

    std::vector<ut_type::UTDataTypeDescs> descs_; // map<> descs matrix
    MutableColumns columns_;                      // column_map list
    DataTypes types_;
    DataTypeSerDeSPtrs serdes_;
};

TEST_F(DataTypeMapTest, SerdeArrowTest) {
    MutableColumns columns;
    DataTypes types;
    for (int i = 0; i < descs_.size(); i++) {
        columns.push_back(columns_[i]->get_ptr());
        types.push_back(types_[i]);
    }
    CommonDataTypeSerdeTest::assert_arrow_format(columns, types);
}

// TODO `DataTypeMapSerDe::deserialize_one_cell_from_json` has a bug,
// `SerdeArrowTest` cannot test Map type nested Array and Struct and Map,
// so manually construct data to test them.
// Expect to delete this TEST after `deserialize_one_cell_from_json` is fixed.
TEST_F(DataTypeMapTest, SerdeNestedTypeArrowTest) {
    auto block = std::make_shared<Block>();
    {
        std::string col_name = "map_nesting_array";
        DataTypePtr f1 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
        DataTypePtr f2 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
        DataTypePtr dt1 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeArray>(f1));
        DataTypePtr dt2 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeArray>(f2));
        DataTypePtr ma = std::make_shared<DataTypeMap>(dt1, dt2);

        Array a1, a2, a3, a4;
        a1.push_back(Field::create_field<TYPE_STRING>("cute"));
        a1.push_back(Field());
        a2.push_back(Field::create_field<TYPE_STRING>("clever"));
        a1.push_back(Field::create_field<TYPE_STRING>("hello"));
        a3.push_back(Field::create_field<TYPE_INT>(1));
        a3.push_back(Field::create_field<TYPE_INT>(2));
        a4.push_back(Field::create_field<TYPE_INT>(11));
        a4.push_back(Field::create_field<TYPE_INT>(22));

        Array k1, v1;
        k1.push_back(Field::create_field<TYPE_ARRAY>(a1));
        k1.push_back(Field::create_field<TYPE_ARRAY>(a2));
        v1.push_back(Field::create_field<TYPE_ARRAY>(a3));
        v1.push_back(Field::create_field<TYPE_ARRAY>(a4));

        Map m1;
        m1.push_back(Field::create_field<TYPE_ARRAY>(k1));
        m1.push_back(Field::create_field<TYPE_ARRAY>(v1));

        MutableColumnPtr map_column = ma->create_column();
        map_column->reserve(1);
        map_column->insert(Field::create_field<TYPE_MAP>(m1));
        vectorized::ColumnWithTypeAndName type_and_name(map_column->get_ptr(), ma, col_name);
        block->insert(type_and_name);
    }
    {
        std::string col_name = "map_nesting_struct";
        DataTypePtr f1 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
        DataTypePtr f2 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt128>());
        DataTypePtr f3 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>());
        DataTypePtr f4 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
        DataTypePtr dt1 = std::make_shared<DataTypeNullable>(
                std::make_shared<DataTypeStruct>(std::vector<DataTypePtr> {f1, f2, f3}));
        DataTypePtr dt2 = std::make_shared<DataTypeNullable>(
                std::make_shared<DataTypeStruct>(std::vector<DataTypePtr> {f4}));
        DataTypePtr ma = std::make_shared<DataTypeMap>(dt1, dt2);

        Tuple t1, t2, t3, t4;
        t1.push_back(Field::create_field<TYPE_STRING>("clever"));
        t1.push_back(Field::create_field<TYPE_LARGEINT>(__int128_t(37)));
        t1.push_back(Field::create_field<TYPE_BOOLEAN>(true));
        t2.push_back(Field::create_field<TYPE_STRING>("null"));
        t2.push_back(Field::create_field<TYPE_LARGEINT>(__int128_t(26)));
        t2.push_back(Field::create_field<TYPE_BOOLEAN>(false));
        t3.push_back(Field::create_field<TYPE_STRING>("cute"));
        t4.push_back(Field::create_field<TYPE_STRING>("null"));

        Array k1, v1;
        k1.push_back(Field::create_field<TYPE_STRUCT>(t1));
        k1.push_back(Field::create_field<TYPE_STRUCT>(t2));
        v1.push_back(Field::create_field<TYPE_STRUCT>(t3));
        v1.push_back(Field::create_field<TYPE_STRUCT>(t4));

        Map m1;
        m1.push_back(Field::create_field<TYPE_ARRAY>(k1));
        m1.push_back(Field::create_field<TYPE_ARRAY>(v1));

        MutableColumnPtr map_column = ma->create_column();
        map_column->reserve(1);
        map_column->insert(Field::create_field<TYPE_MAP>(m1));
        vectorized::ColumnWithTypeAndName type_and_name(map_column->get_ptr(), ma, col_name);
        block->insert(type_and_name);
    }
    {
        std::string col_name = "map_nesting_map";
        DataTypePtr f1 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
        DataTypePtr f2 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
        DataTypePtr f3 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt128>());
        DataTypePtr f4 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>());
        DataTypePtr dt1 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeMap>(f1, f2));
        DataTypePtr dt2 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeMap>(f3, f4));
        DataTypePtr ma = std::make_shared<DataTypeMap>(dt1, dt2);

        Array k1, k2, k3, k4, v1, v2, v3, v4;
        k1.push_back(Field::create_field<TYPE_INT>(1));
        k1.push_back(Field::create_field<TYPE_INT>(2));
        k2.push_back(Field::create_field<TYPE_INT>(11));
        k2.push_back(Field::create_field<TYPE_INT>(22));
        v1.push_back(Field::create_field<TYPE_STRING>("map"));
        v1.push_back(Field());
        v2.push_back(Field::create_field<TYPE_STRING>("clever map"));
        v2.push_back(Field::create_field<TYPE_STRING>("hello map"));
        k3.push_back(Field::create_field<TYPE_LARGEINT>(__int128_t(37)));
        k3.push_back(Field::create_field<TYPE_LARGEINT>(__int128_t(26)));
        k4.push_back(Field::create_field<TYPE_LARGEINT>(__int128_t(1111)));
        k4.push_back(Field::create_field<TYPE_LARGEINT>(__int128_t(432535423)));
        v3.push_back(Field::create_field<TYPE_BOOLEAN>(true));
        v3.push_back(Field::create_field<TYPE_BOOLEAN>(false));
        v4.push_back(Field::create_field<TYPE_BOOLEAN>(false));
        v4.push_back(Field::create_field<TYPE_BOOLEAN>(true));

        Map m11, m12, m21, m22;
        m11.push_back(Field::create_field<TYPE_ARRAY>(k1));
        m11.push_back(Field::create_field<TYPE_ARRAY>(v1));
        m12.push_back(Field::create_field<TYPE_ARRAY>(k2));
        m12.push_back(Field::create_field<TYPE_ARRAY>(v2));
        m21.push_back(Field::create_field<TYPE_ARRAY>(k3));
        m21.push_back(Field::create_field<TYPE_ARRAY>(v3));
        m22.push_back(Field::create_field<TYPE_ARRAY>(k4));
        m22.push_back(Field::create_field<TYPE_ARRAY>(v4));

        Array kk1, vv1;
        kk1.push_back(Field::create_field<TYPE_MAP>(m11));
        kk1.push_back(Field::create_field<TYPE_MAP>(m12));
        vv1.push_back(Field::create_field<TYPE_MAP>(m21));
        vv1.push_back(Field::create_field<TYPE_MAP>(m22));

        Map m1;
        m1.push_back(Field::create_field<TYPE_ARRAY>(kk1));
        m1.push_back(Field::create_field<TYPE_ARRAY>(vv1));

        MutableColumnPtr map_column = ma->create_column();
        map_column->reserve(1);
        map_column->insert(Field::create_field<TYPE_MAP>(m1));
        vectorized::ColumnWithTypeAndName type_and_name(map_column->get_ptr(), ma, col_name);
        block->insert(type_and_name);
    }
    std::shared_ptr<arrow::RecordBatch> record_batch =
            CommonDataTypeSerdeTest::serialize_arrow(block);
    auto assert_block = std::make_shared<Block>(block->clone_empty());
    CommonDataTypeSerdeTest::deserialize_arrow(assert_block, record_batch);
    CommonDataTypeSerdeTest::compare_two_blocks(block, assert_block);
}

} // namespace doris::vectorized
