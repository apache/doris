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
#include "vec/data_types/data_type_factory.hpp"
#include "vec/function/function_test_util.h"

/* similar to DataTypeArrayTest
*/

namespace doris::vectorized {

class DataTypeMapTest : public CommonDataTypeTest {
protected:
    void SetUp() override {
        // insert from data csv and assert insert result
        MutableColumns map_cols;
        // we need to load data from csv file into column_map list
        // step1. create data type for map nested type (const and nullable)
        // map<tinyint, tinyint>
        BaseInputTypeSet map_tinyint = {TypeIndex::Map, TypeIndex::Int8, TypeIndex::Int8};
        // map<smallint, smallint>
        BaseInputTypeSet map_smallint = {TypeIndex::Map, TypeIndex::Int16, TypeIndex::Int16};
        // map<int, int>
        BaseInputTypeSet map_int = {TypeIndex::Map, TypeIndex::Int32, TypeIndex::Int32};
        // map<bigintm, bigint>
        BaseInputTypeSet map_bigint = {TypeIndex::Map, TypeIndex::Int64, TypeIndex::Int64};
        // map<largeint, largeint>
        BaseInputTypeSet map_largeint = {TypeIndex::Map, TypeIndex::Int128, TypeIndex::Int128};
        // map<float, float>
        BaseInputTypeSet map_float = {TypeIndex::Map, TypeIndex::Float32, TypeIndex::Float32};
        // map<double, double>
        BaseInputTypeSet map_double = {TypeIndex::Map, TypeIndex::Float64, TypeIndex::Float64};
        // map<ipv4, ipv4>
        BaseInputTypeSet map_ipv4 = {TypeIndex::Map, TypeIndex::IPv4, TypeIndex::IPv4};
        // map<ipv6, ipv6>
        BaseInputTypeSet map_ipv6 = {TypeIndex::Map, TypeIndex::IPv6, TypeIndex::IPv6};
        // map<date, date>
        BaseInputTypeSet map_date = {TypeIndex::Map, TypeIndex::Date, TypeIndex::Date};
        // map<datetime, datetime>
        BaseInputTypeSet map_datetime = {TypeIndex::Map, TypeIndex::DateTime, TypeIndex::DateTime};
        // map<datev2, datev2>
        BaseInputTypeSet map_datev2 = {TypeIndex::Map, TypeIndex::DateV2, TypeIndex::DateV2};
        // map<datetimev2, datetimev2>
        BaseInputTypeSet map_datetimev2 = {TypeIndex::Map, TypeIndex::DateTimeV2,
                                           TypeIndex::DateTimeV2};
        // map<varchar, varchar>
        BaseInputTypeSet map_varchar = {TypeIndex::Map, TypeIndex::String, TypeIndex::String};
        // map<decimal32(9, 5), decimal32(9, 5)> UT
        BaseInputTypeSet map_decimal = {TypeIndex::Map, TypeIndex::Decimal32, TypeIndex::Decimal32};
        // map<decimal64(18, 9), decimal64(18, 9)> UT
        BaseInputTypeSet map_decimal64 = {TypeIndex::Map, TypeIndex::Decimal64,
                                          TypeIndex::Decimal64};
        // map<decimal128(38, 20), decimal128(38, 20)> UT
        BaseInputTypeSet map_decimal128 = {TypeIndex::Map, TypeIndex::Decimal128V3,
                                           TypeIndex::Decimal128V3};
        // map<decimal256(76, 40), decimal256(76, 40)> UT
        BaseInputTypeSet map_decimal256 = {TypeIndex::Map, TypeIndex::Decimal256,
                                           TypeIndex::Decimal256};
        std::vector<BaseInputTypeSet> map_typeIndex = {
                map_tinyint,   map_smallint,   map_int,        map_bigint,  map_largeint,
                map_float,     map_double,     map_ipv4,       map_ipv6,    map_date,
                map_datetime,  map_datev2,     map_datetimev2, map_varchar, map_decimal,
                map_decimal64, map_decimal128, map_decimal256};
        // map<array<tinyint>, array<tinyint>>
        BaseInputTypeSet map_array_tinyint = {TypeIndex::Map, TypeIndex::Array, TypeIndex::Int8,
                                              TypeIndex::Array, TypeIndex::Int8};
        // map<array<smallint>, array<smallint>>
        BaseInputTypeSet map_array_smallint = {TypeIndex::Map, TypeIndex::Array, TypeIndex::Int16,
                                               TypeIndex::Array, TypeIndex::Int16};
        // map<array<int, int>, array<int, int>>
        BaseInputTypeSet map_array_int = {TypeIndex::Map, TypeIndex::Array, TypeIndex::Int32,
                                          TypeIndex::Array, TypeIndex::Int32};
        // map<array<bigint>, array<bigint>>
        BaseInputTypeSet map_array_bigint = {TypeIndex::Map, TypeIndex::Array, TypeIndex::Int64,
                                             TypeIndex::Array, TypeIndex::Int64};
        // map<array<largeint>, array<largeint>>
        BaseInputTypeSet map_array_largeint = {TypeIndex::Map, TypeIndex::Array, TypeIndex::Int128,
                                               TypeIndex::Array, TypeIndex::Int128};
        // map<array<float>, array<float>>
        BaseInputTypeSet map_array_float = {TypeIndex::Map, TypeIndex::Array, TypeIndex::Float32,
                                            TypeIndex::Array, TypeIndex::Float32};
        // map<array<double>, array<double>>
        BaseInputTypeSet map_array_double = {TypeIndex::Map, TypeIndex::Array, TypeIndex::Float64,
                                             TypeIndex::Array, TypeIndex::Float64};
        // map<array<ipv4>, array<ipv4>>
        BaseInputTypeSet map_array_ipv4 = {TypeIndex::Map, TypeIndex::Array, TypeIndex::IPv4,
                                           TypeIndex::Array, TypeIndex::IPv4};
        // map<array<ipv6>, array<ipv6>>
        BaseInputTypeSet map_array_ipv6 = {TypeIndex::Map, TypeIndex::Array, TypeIndex::IPv6,
                                           TypeIndex::Array, TypeIndex::IPv6};
        // map<array<date>, array<date>>
        BaseInputTypeSet map_array_date = {TypeIndex::Map, TypeIndex::Array, TypeIndex::Date,
                                           TypeIndex::Array, TypeIndex::Date};
        // map<array<datetime>, array<datetime>>
        BaseInputTypeSet map_array_datetime = {TypeIndex::Map, TypeIndex::Array,
                                               TypeIndex::DateTime, TypeIndex::Array,
                                               TypeIndex::DateTime};
        // map<array<datev2>, array<datev2>>
        BaseInputTypeSet map_array_datev2 = {TypeIndex::Map, TypeIndex::Array, TypeIndex::DateV2,
                                             TypeIndex::Array, TypeIndex::DateV2};
        // map<array<datetimev2>, array<datetimev2>>
        BaseInputTypeSet map_array_datetimev2 = {TypeIndex::Map, TypeIndex::Array,
                                                 TypeIndex::DateTimeV2, TypeIndex::Array,
                                                 TypeIndex::DateTimeV2};
        // map<array<varchar>, array<varchar>>
        BaseInputTypeSet map_array_varchar = {TypeIndex::Map, TypeIndex::Array, TypeIndex::String,
                                              TypeIndex::Array, TypeIndex::String};
        // map<array<decimal32(9, 5)>, array<decimal32(9, 5)>> UT
        BaseInputTypeSet map_array_decimal = {TypeIndex::Map, TypeIndex::Array,
                                              TypeIndex::Decimal32, TypeIndex::Array,
                                              TypeIndex::Decimal32};
        // map<array<decimal64(18, 9)>, array<decimal64(18, 9)>> UT
        BaseInputTypeSet map_array_decimal64 = {TypeIndex::Map, TypeIndex::Array,
                                                TypeIndex::Decimal64, TypeIndex::Array,
                                                TypeIndex::Decimal64};
        // map<array<decimal128(38, 20)>, array<decimal128(38, 20)>> UT
        BaseInputTypeSet map_array_decimal128 = {TypeIndex::Map, TypeIndex::Array,
                                                 TypeIndex::Decimal128V3, TypeIndex::Array,
                                                 TypeIndex::Decimal128V3};
        // map<array<decimal256(76, 40)>, array<decimal256(76, 40)>> UT
        BaseInputTypeSet map_array_decimal256 = {TypeIndex::Map, TypeIndex::Array,
                                                 TypeIndex::Decimal256, TypeIndex::Array,
                                                 TypeIndex::Decimal256};
        // map<map<char, double>, map<char, double>>
        BaseInputTypeSet map_map_char_double = {
                TypeIndex::Map, TypeIndex::Map,    TypeIndex::String, TypeIndex::Float64,
                TypeIndex::Map, TypeIndex::String, TypeIndex::Float64};
        // map<map<datetime, decimal<76,56>>, map<datetime, decimal<76,56>>>
        BaseInputTypeSet map_map_datetime_decimal = {
                TypeIndex::Map, TypeIndex::Map,      TypeIndex::DateTime,  TypeIndex::Decimal256,
                TypeIndex::Map, TypeIndex::DateTime, TypeIndex::Decimal256};
        // map<map<ipv4, ipv6>, map<ipv4, ipv6>>
        BaseInputTypeSet map_map_ipv4_ipv6 = {TypeIndex::Map,  TypeIndex::Map, TypeIndex::IPv4,
                                              TypeIndex::IPv6, TypeIndex::Map, TypeIndex::IPv4,
                                              TypeIndex::IPv6};
        // map<map<largeInt, string>, map<largeInt, string>>
        BaseInputTypeSet map_map_largeint_string = {
                TypeIndex::Map, TypeIndex::Map,    TypeIndex::Int128, TypeIndex::String,
                TypeIndex::Map, TypeIndex::Int128, TypeIndex::String};
        // map<struct<f1:int,f2:date,f3:decimal>, struct<f4:string,f5:double,f6:ipv4,f7:ipv6>>
        BaseInputTypeSet map_struct = {TypeIndex::Map,    TypeIndex::Struct,    TypeIndex::Int32,
                                       TypeIndex::Date,   TypeIndex::Decimal32, TypeIndex::Struct,
                                       TypeIndex::String, TypeIndex::Float64,   TypeIndex::IPv4,
                                       TypeIndex::IPv6};

        std::vector<BaseInputTypeSet> map_array_typeIndex = {
                map_array_tinyint,    map_array_smallint,  map_array_int,      map_array_bigint,
                map_array_largeint,   map_array_float,     map_array_double,   map_array_ipv4,
                map_array_ipv6,       map_array_date,      map_array_datetime, map_array_datev2,
                map_array_datetimev2, map_array_varchar,   map_array_decimal,  map_array_decimal64,
                map_array_decimal128, map_array_decimal256};
        std::vector<BaseInputTypeSet> map_map_typeIndex = {
                map_map_char_double, map_map_datetime_decimal, map_map_ipv4_ipv6,
                map_map_largeint_string};
        std::vector<BaseInputTypeSet> map_struct_typeIndex = {map_struct};

        descs.reserve(map_typeIndex.size() + map_array_typeIndex.size() + map_map_typeIndex.size() +
                      map_struct_typeIndex.size());
        for (int i = 0; i < map_typeIndex.size(); i++) {
            descs.emplace_back();
            InputTypeSet input_types {};
            input_types.emplace_back(map_typeIndex[i][0]);
            input_types.emplace_back(Nullable {static_cast<TypeIndex>(map_typeIndex[i][1])});
            input_types.emplace_back(Nullable {static_cast<TypeIndex>(map_typeIndex[i][2])});
            EXPECT_EQ(input_types[1].type(), &typeid(Nullable)) << "nested type is not nullable";
            EXPECT_EQ(input_types[2].type(), &typeid(Nullable)) << "nested type is not nullable";
            EXPECT_TRUE(parse_ut_data_type(input_types, descs[i]));
        }
        for (int i = 0; i < map_array_typeIndex.size(); i++) {
            descs.emplace_back();
            InputTypeSet input_types {};
            input_types.emplace_back(map_array_typeIndex[i][0]);
            input_types.emplace_back(
                    Nullable {static_cast<TypeIndex>(map_array_typeIndex[i][1])}); // array1
            input_types.emplace_back(Nullable {static_cast<TypeIndex>(map_array_typeIndex[i][2])});
            input_types.emplace_back(
                    Nullable {static_cast<TypeIndex>(map_array_typeIndex[i][3])}); // array2
            input_types.emplace_back(Nullable {static_cast<TypeIndex>(map_array_typeIndex[i][4])});
            EXPECT_EQ(input_types[1].type(), &typeid(Nullable)) << "nested type is not nullable";
            EXPECT_EQ(input_types[2].type(), &typeid(Nullable)) << "nested type is not nullable";
            EXPECT_EQ(input_types[3].type(), &typeid(Nullable)) << "nested type is not nullable";
            EXPECT_EQ(input_types[4].type(), &typeid(Nullable)) << "nested type is not nullable";
            EXPECT_TRUE(parse_ut_data_type(input_types, descs[i + map_typeIndex.size()]));
        }

        for (int i = 0; i < map_map_typeIndex.size(); i++) {
            descs.emplace_back();
            InputTypeSet input_types {};
            input_types.emplace_back(map_map_typeIndex[i][0]); // map
            input_types.emplace_back(
                    Nullable {static_cast<TypeIndex>(map_map_typeIndex[i][1])}); // map1
            input_types.emplace_back(Nullable {static_cast<TypeIndex>(map_map_typeIndex[i][2])});
            input_types.emplace_back(Nullable {static_cast<TypeIndex>(map_map_typeIndex[i][3])});
            input_types.emplace_back(
                    Nullable {static_cast<TypeIndex>(map_map_typeIndex[i][4])}); // map2
            input_types.emplace_back(Nullable {static_cast<TypeIndex>(map_map_typeIndex[i][5])});
            input_types.emplace_back(Nullable {static_cast<TypeIndex>(map_map_typeIndex[i][6])});
            EXPECT_EQ(input_types[1].type(), &typeid(Nullable)) << "nested type is not nullable";
            EXPECT_EQ(input_types[2].type(), &typeid(Nullable)) << "nested type is not nullable";
            EXPECT_EQ(input_types[3].type(), &typeid(Nullable)) << "nested type is not nullable";
            EXPECT_EQ(input_types[4].type(), &typeid(Nullable)) << "nested type is not nullable";
            EXPECT_EQ(input_types[5].type(), &typeid(Nullable)) << "nested type is not nullable";
            EXPECT_EQ(input_types[6].type(), &typeid(Nullable)) << "nested type is not nullable";
            EXPECT_TRUE(parse_ut_data_type(
                    input_types, descs[i + map_typeIndex.size() + map_array_typeIndex.size()]));
        }

        for (int i = 0; i < map_struct_typeIndex.size(); i++) {
            descs.emplace_back();
            InputTypeSet input_types {};
            input_types.emplace_back(map_struct_typeIndex[i][0]); // map
            input_types.emplace_back(
                    Nullable {static_cast<TypeIndex>(map_struct_typeIndex[i][1])}); // struct
            input_types.emplace_back(Nullable {static_cast<TypeIndex>(map_struct_typeIndex[i][2])});
            input_types.emplace_back(Nullable {static_cast<TypeIndex>(map_struct_typeIndex[i][3])});
            input_types.emplace_back(Nullable {static_cast<TypeIndex>(map_struct_typeIndex[i][4])});
            input_types.emplace_back(Nullable {static_cast<TypeIndex>(map_struct_typeIndex[i][5])});
            input_types.emplace_back(Nullable {static_cast<TypeIndex>(map_struct_typeIndex[i][6])});
            input_types.emplace_back(Nullable {static_cast<TypeIndex>(map_struct_typeIndex[i][7])});
            input_types.emplace_back(Nullable {static_cast<TypeIndex>(map_struct_typeIndex[i][8])});
            input_types.emplace_back(Nullable {static_cast<TypeIndex>(map_struct_typeIndex[i][9])});

            EXPECT_EQ(input_types[1].type(), &typeid(Nullable)) << "nested type is not nullable";
            EXPECT_TRUE(parse_ut_data_type(
                    input_types, descs[i + map_typeIndex.size() + map_array_typeIndex.size() +
                                       map_map_typeIndex.size()]));
        }

        // create column_map for each data type
        // step2. according to the datatype to make column_map
        //          && load data from csv file into column_map
        EXPECT_EQ(descs.size(), data_files.size());
        for (int i = 0; i < descs.size(); i++) {
            auto& desc = descs[i];
            auto& data_file = data_files[i];
            // first is map type
            auto& type = desc[0].data_type;
            std::cout << "type: " << type->get_name() << " with file: " << data_file << std::endl;
            MutableColumns cols;
            cols.push_back(type->create_column());
            auto serde = type->get_serde(1);
            load_data_from_csv({serde}, cols, data_file, ';');
            columns.push_back(std::move(cols[0]));
            types.push_back(type);
            serdes.push_back(serde);
        }
    }

    std::string data_file_dir = "regression-test/data/nereids_function_p0/map/";

    vector<string> data_files = {
            // map-scalar
            data_file_dir + "test_map_tinyint.csv", data_file_dir + "test_map_smallint.csv",
            data_file_dir + "test_map_int.csv", data_file_dir + "test_map_bigint.csv",
            data_file_dir + "test_map_largeint.csv", data_file_dir + "test_map_float.csv",
            data_file_dir + "test_map_double.csv", data_file_dir + "test_map_ipv4.csv",
            data_file_dir + "test_map_ipv6.csv", data_file_dir + "test_map_date.csv",
            data_file_dir + "test_map_datetime.csv", data_file_dir + "test_map_date.csv",
            data_file_dir + "test_map_datetimev2_6.csv",
            data_file_dir + "test_map_varchar_65535.csv",
            data_file_dir + "test_map_decimalv3_7_4.csv",
            data_file_dir + "test_map_decimalv3_16_10.csv",
            data_file_dir + "test_map_decimalv3_38_30.csv",
            data_file_dir + "test_map_decimalv3_76_56.csv",
            // map-array
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
            data_file_dir + "test_map_map_char_double.csv",
            data_file_dir + "test_map_map_datetime_decimal.csv",
            data_file_dir + "test_map_map_ipv4_ipv6.csv",
            data_file_dir + "test_map_map_largeInt_string.csv",
            // map-struct
            data_file_dir + "test_map_struct.csv"};

    vector<ut_type::UTDataTypeDescs> descs; // map<> descs matrix
    MutableColumns columns;                 // column_map list
    DataTypes types;
    DataTypeSerDeSPtrs serdes;
};

TEST_F(DataTypeMapTest, SerdeArrowTest) {
    // todo. fix decimal256 serde
    MutableColumns map_cols;
    for (int i = 0; i < 17; i++) {
        map_cols.push_back(columns[i]->get_ptr());
    }
    for (int i = 18; i < 35; i++) {
        map_cols.push_back(columns[i]->get_ptr());
    }
    // map_cols.push_back(columns[36]->get_ptr());
    // map_cols.push_back(columns[38]->get_ptr());
    // DataTypes types;
    // for (int i = 0; i < 17; i++) {
    //     types.push_back(types[i]);
    // }
    // for (int i = 18; i < 35; i++) {
    //     types.push_back(types[i]);
    // }
    // types.push_back(types[36]);
    // types.push_back(types[38]);
    // CommonDataTypeSerdeTest::assert_arrow_format(map_cols, types);
    // {
    //     for (int i = 39; i < 41; ++i) {
    //         MutableColumns error_cols;
    //         error_cols.push_back(columns[i]->get_ptr());
    //         DataTypes typ;
    //         typ.push_back(types[i]);
    //         EXPECT_ANY_THROW(CommonDataTypeSerdeTest::assert_arrow_format(error_cols, typ));
    //     }
    // }
}

} // namespace doris::vectorized
