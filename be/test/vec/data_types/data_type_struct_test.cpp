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

#include "vec/data_types/data_type_struct.h"

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
 *
 * TODO: `DataTypeStructSerDe::deserialize_one_cell_from_json` has bug, must be fixed before continuing testing.
 *  1. json->struct<ipv6> deserialization result is wrong, '2001:0db8:0:0:0:0:0:1' -> '2001:db8::1'
 *  2. json->struct<datetime> deserialization result is null
 *  3. json->struct<array<>> deserialization result is NULL.
 *  4. json->struct<map<>> deserialization result is NULL.
 *  5. json->struct<struct<>> deserialization result is NULL.
*/

namespace doris::vectorized {

class DataTypeStructTest : public CommonDataTypeTest {
protected:
    void SetUp() override {
        // we need to load data from csv file into column_struct list
        // step1. create data type for struct nested type (const and nullable)
        // struct<tinyint>
        InputTypeSet struct_tinyint = {TypeIndex::Struct, TypeIndex::Int8};
        // struct<smallint>
        InputTypeSet struct_smallint = {TypeIndex::Struct, TypeIndex::Int16};
        // struct<int>
        InputTypeSet struct_int = {TypeIndex::Struct, TypeIndex::Int32};
        // struct<bigint>
        InputTypeSet struct_bigint = {TypeIndex::Struct, TypeIndex::Int64};
        // struct<largeint>
        InputTypeSet struct_largeint = {TypeIndex::Struct, TypeIndex::Int128};
        // struct<float>
        InputTypeSet struct_float = {TypeIndex::Struct, TypeIndex::Float32};
        // struct<double>
        InputTypeSet struct_double = {TypeIndex::Struct, TypeIndex::Float64};
        // struct<ipv4>
        InputTypeSet struct_ipv4 = {TypeIndex::Struct, TypeIndex::IPv4};
        // struct<ipv6>
        InputTypeSet struct_ipv6 = {TypeIndex::Struct, TypeIndex::IPv6};
        // struct<date>
        InputTypeSet struct_date = {TypeIndex::Struct, TypeIndex::Date};
        // struct<datetime>
        InputTypeSet struct_datetime = {TypeIndex::Struct, TypeIndex::DateTime};
        // struct<datev2>
        InputTypeSet struct_datev2 = {TypeIndex::Struct, TypeIndex::DateV2};
        // struct<datetimev2>
        InputTypeSet struct_datetimev2 = {TypeIndex::Struct, TypeIndex::DateTimeV2};
        // struct<varchar>
        InputTypeSet struct_varchar = {TypeIndex::Struct, TypeIndex::String};
        // struct<decimal32(9, 5)>
        InputTypeSet struct_decimal = {TypeIndex::Struct, TypeIndex::Decimal32};
        // struct<decimal64(18, 9)>
        InputTypeSet struct_decimal64 = {TypeIndex::Struct, TypeIndex::Decimal64};
        // struct<decimal128(38, 20)>
        InputTypeSet struct_decimal128 = {TypeIndex::Struct, TypeIndex::Decimal128V3};
        // struct<decimal256(76, 40)>
        InputTypeSet struct_decimal256 = {TypeIndex::Struct, TypeIndex::Decimal256};
        std::vector<InputTypeSet> struct_typeIndex = {
                struct_tinyint,    struct_smallint,  struct_int,      struct_bigint,
                struct_largeint,   struct_float,     struct_double,   struct_ipv4,
                struct_ipv6,       struct_date,      struct_datetime, struct_datev2,
                struct_datetimev2, struct_varchar,   struct_decimal,  struct_decimal64,
                struct_decimal128, struct_decimal256};
        // struct<array<tinyint>>
        InputTypeSet struct_array_tinyint = {TypeIndex::Struct, TypeIndex::Array, TypeIndex::Int8};
        // struct<array<smallint>>
        InputTypeSet struct_array_smallint = {TypeIndex::Struct, TypeIndex::Array,
                                              TypeIndex::Int16};
        // struct<array<int>>
        InputTypeSet struct_array_int = {TypeIndex::Struct, TypeIndex::Array, TypeIndex::Int32};
        // struct<array<bigint>>
        InputTypeSet struct_array_bigint = {TypeIndex::Struct, TypeIndex::Array, TypeIndex::Int64};
        // struct<array<largeint>>
        InputTypeSet struct_array_largeint = {TypeIndex::Struct, TypeIndex::Array,
                                              TypeIndex::Int128};
        // struct<array<float>>
        InputTypeSet struct_array_float = {TypeIndex::Struct, TypeIndex::Array, TypeIndex::Float32};
        // struct<array<double>>
        InputTypeSet struct_array_double = {TypeIndex::Struct, TypeIndex::Array,
                                            TypeIndex::Float64};
        // struct<array<ipv4>>
        InputTypeSet struct_array_ipv4 = {TypeIndex::Struct, TypeIndex::Array, TypeIndex::IPv4};
        // struct<array<ipv6>>
        InputTypeSet struct_array_ipv6 = {TypeIndex::Struct, TypeIndex::Array, TypeIndex::IPv6};
        // struct<array<date>>
        InputTypeSet struct_array_date = {TypeIndex::Struct, TypeIndex::Array, TypeIndex::Date};
        // struct<array<datetime>>
        InputTypeSet struct_array_datetime = {TypeIndex::Struct, TypeIndex::Array,
                                              TypeIndex::DateTime};
        // struct<array<datev2>>
        InputTypeSet struct_array_datev2 = {TypeIndex::Struct, TypeIndex::Array, TypeIndex::DateV2};
        // struct<array<datetimev2>>
        InputTypeSet struct_array_datetimev2 = {TypeIndex::Struct, TypeIndex::Array,
                                                TypeIndex::DateTimeV2};
        // struct<array<varchar>>
        InputTypeSet struct_array_varchar = {TypeIndex::Struct, TypeIndex::Array,
                                             TypeIndex::String};
        // struct<array<decimal32(9, 5)>>
        InputTypeSet struct_array_decimal = {TypeIndex::Struct, TypeIndex::Array,
                                             TypeIndex::Decimal32};
        // struct<array<decimal64(18, 9)>>
        InputTypeSet struct_array_decimal64 = {TypeIndex::Struct, TypeIndex::Array,
                                               TypeIndex::Decimal64};
        // struct<array<decimal128(38, 20)>>
        InputTypeSet struct_array_decimal128 = {TypeIndex::Struct, TypeIndex::Array,
                                                TypeIndex::Decimal128V3};
        // struct<array<decimal256(76, 40)>>
        InputTypeSet struct_array_decimal256 = {TypeIndex::Struct, TypeIndex::Array,
                                                TypeIndex::Decimal256};
        // struct<map<char,double>>
        InputTypeSet struct_map_char_double = {TypeIndex::Struct, TypeIndex::Map, TypeIndex::String,
                                               TypeIndex::Float64};
        // struct_map<datetime,decimal<76,56>>
        InputTypeSet struct_map_datetime_decimal = {TypeIndex::Struct, TypeIndex::Map,
                                                    TypeIndex::DateTimeV2, TypeIndex::Decimal256};
        // struct_map<ipv4,ipv6>
        InputTypeSet struct_map_ipv4_ipv6 = {TypeIndex::Struct, TypeIndex::Map, TypeIndex::IPv4,
                                             TypeIndex::IPv6};
        // struct_map<largeInt,string>
        InputTypeSet struct_map_largeint_string = {TypeIndex::Struct, TypeIndex::Map,
                                                   TypeIndex::Int128, TypeIndex::String};
        // struct<struct<f1:int,f2:date,f3:decimal>, struct<f4:string,f5:double,f6:ipv4,f7:ipv6>>
        InputTypeSet struct_struct = {TypeIndex::Struct, TypeIndex::Struct,    TypeIndex::Int32,
                                      TypeIndex::Date,   TypeIndex::Decimal32, TypeIndex::Struct,
                                      TypeIndex::String, TypeIndex::Float64,   TypeIndex::IPv4,
                                      TypeIndex::IPv6};

        std::vector<InputTypeSet> struct_array_typeIndex = {
                struct_array_tinyint,    struct_array_smallint,   struct_array_int,
                struct_array_bigint,     struct_array_largeint,   struct_array_float,
                struct_array_double,     struct_array_ipv4,       struct_array_ipv6,
                struct_array_date,       struct_array_datetime,   struct_array_datev2,
                struct_array_datetimev2, struct_array_varchar,    struct_array_decimal,
                struct_array_decimal64,  struct_array_decimal128, struct_array_decimal256};
        std::vector<InputTypeSet> struct_map_typeIndex = {
                struct_map_char_double, struct_map_datetime_decimal, struct_map_ipv4_ipv6,
                struct_map_largeint_string};
        std::vector<InputTypeSet> struct_struct_typeIndex = {struct_struct};

        descs_.reserve(struct_typeIndex.size() + struct_array_typeIndex.size() +
                       struct_map_typeIndex.size() + struct_struct_typeIndex.size());
        for (int i = 0; i < struct_typeIndex.size(); i++) {
            descs_.emplace_back();
            InputTypeSet input_types {};
            input_types.emplace_back(struct_typeIndex[i][0]);
            input_types.emplace_back(Nullable {any_cast<TypeIndex>(struct_typeIndex[i][1])});
            EXPECT_EQ(input_types[1].type(), &typeid(Nullable)) << "nested type is not nullable";
            EXPECT_TRUE(parse_ut_data_type(input_types, descs_[i]));
        }
        for (int i = 0; i < struct_array_typeIndex.size(); i++) {
            descs_.emplace_back();
            InputTypeSet input_types {};
            input_types.emplace_back(struct_array_typeIndex[i][0]);
            input_types.emplace_back(Nullable {any_cast<TypeIndex>(struct_array_typeIndex[i][1])});
            input_types.emplace_back(Nullable {any_cast<TypeIndex>(struct_array_typeIndex[i][2])});
            EXPECT_EQ(input_types[1].type(), &typeid(Nullable)) << "nested type is not nullable";
            EXPECT_EQ(input_types[2].type(), &typeid(Nullable)) << "nested type is not nullable";
            EXPECT_TRUE(parse_ut_data_type(input_types, descs_[i + struct_typeIndex.size()]));
        }

        for (int i = 0; i < struct_map_typeIndex.size(); i++) {
            descs_.emplace_back();
            InputTypeSet input_types {};
            input_types.emplace_back(struct_map_typeIndex[i][0]); // struct
            input_types.emplace_back(
                    Nullable {any_cast<TypeIndex>(struct_map_typeIndex[i][1])}); // map
            input_types.emplace_back(
                    Nullable {any_cast<TypeIndex>(struct_map_typeIndex[i][2])}); // key
            input_types.emplace_back(
                    Nullable {any_cast<TypeIndex>(struct_map_typeIndex[i][3])}); // val
            EXPECT_EQ(input_types[1].type(), &typeid(Nullable)) << "nested type is not nullable";
            EXPECT_EQ(input_types[2].type(), &typeid(Nullable)) << "nested type is not nullable";
            EXPECT_TRUE(parse_ut_data_type(
                    input_types,
                    descs_[i + struct_typeIndex.size() + struct_array_typeIndex.size()]));
        }

        for (int i = 0; i < struct_struct_typeIndex.size(); i++) {
            descs_.emplace_back();
            InputTypeSet input_types {};
            input_types.emplace_back(struct_struct_typeIndex[i][0]); // struct
            input_types.emplace_back(
                    Nullable {any_cast<TypeIndex>(struct_struct_typeIndex[i][1])}); // struct
            input_types.emplace_back(
                    Nullable {any_cast<TypeIndex>(struct_struct_typeIndex[i][2])}); // f1
            input_types.emplace_back(
                    Nullable {any_cast<TypeIndex>(struct_struct_typeIndex[i][3])}); // f2
            input_types.emplace_back(
                    Nullable {any_cast<TypeIndex>(struct_struct_typeIndex[i][4])}); // f3
            input_types.emplace_back(
                    Nullable {any_cast<TypeIndex>(struct_struct_typeIndex[i][5])}); // f4
            input_types.emplace_back(
                    Nullable {any_cast<TypeIndex>(struct_struct_typeIndex[i][6])}); // f5
            input_types.emplace_back(
                    Nullable {any_cast<TypeIndex>(struct_struct_typeIndex[i][7])}); // f6
            input_types.emplace_back(
                    Nullable {any_cast<TypeIndex>(struct_struct_typeIndex[i][8])}); // f7

            EXPECT_EQ(input_types[1].type(), &typeid(Nullable)) << "nested type is not nullable";
            EXPECT_TRUE(parse_ut_data_type(
                    input_types,
                    descs_[i + struct_typeIndex.size() + struct_array_typeIndex.size() +
                           struct_map_typeIndex.size()]));
        }

        // create column_struct for each data type
        // step2. according to the datatype to make column_struct
        //          && load data from csv file into column_struct
        EXPECT_EQ(descs_.size(), data_files.size());
        for (int i = 0; i < descs_.size(); i++) {
            auto& desc = descs_[i];
            auto& data_file = data_files[i];
            // first is struct type
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

    std::string data_file_dir = "regression-test/data/nereids_function_p0/struct/";

    vector<string> data_files = {
            // struct-scalar
            data_file_dir + "test_struct_tinyint.csv", data_file_dir + "test_struct_smallint.csv",
            data_file_dir + "test_struct_int.csv", data_file_dir + "test_struct_bigint.csv",
            data_file_dir + "test_struct_largeint.csv", data_file_dir + "test_struct_float.csv",
            data_file_dir + "test_struct_double.csv", data_file_dir + "test_struct_ipv4.csv",
            // TODO, json->struct<ipv6> deserialization result is wrong, '2001:0db8:0:0:0:0:0:1' -> '2001:db8::1'
            data_file_dir + "test_struct_ipv6.csv", data_file_dir + "test_struct_date.csv",
            // TODO, json->struct<datetime> deserialization result is null
            data_file_dir + "test_struct_datetime.csv", data_file_dir + "test_struct_date.csv",
            data_file_dir + "test_struct_datetimev2_6.csv",
            data_file_dir + "test_struct_varchar_65535.csv",
            data_file_dir + "test_struct_decimalv3_7_4.csv",
            data_file_dir + "test_struct_decimalv3_16_10.csv",
            data_file_dir + "test_struct_decimalv3_38_30.csv",
            data_file_dir + "test_struct_decimalv3_76_56.csv",
            // struct-array
            // TODO, json->struct<array<>> deserialization result is NULL.
            data_file_dir + "test_struct_array_tinyint.csv",
            data_file_dir + "test_struct_array_smallint.csv",
            data_file_dir + "test_struct_array_int.csv",
            data_file_dir + "test_struct_array_bigint.csv",
            data_file_dir + "test_struct_array_largeint.csv",
            data_file_dir + "test_struct_array_float.csv",
            data_file_dir + "test_struct_array_double.csv",
            data_file_dir + "test_struct_array_ipv4.csv",
            data_file_dir + "test_struct_array_ipv6.csv",
            data_file_dir + "test_struct_array_date.csv",
            data_file_dir + "test_struct_array_datetime.csv",
            data_file_dir + "test_struct_array_date.csv",
            data_file_dir + "test_struct_array_datetimev2_5.csv",
            data_file_dir + "test_struct_array_varchar_65535.csv",
            data_file_dir + "test_struct_array_decimalv3_1_0.csv",
            data_file_dir + "test_struct_array_decimalv3_27_9.csv",
            data_file_dir + "test_struct_array_decimalv3_38_30.csv",
            data_file_dir + "test_struct_array_decimalv3_76_56.csv",
            // struct-map
            // TODO, json->struct<map<>> deserialization result is NULL.
            data_file_dir + "test_struct_map_char_double.csv",
            data_file_dir + "test_struct_map_datetime_decimal.csv",
            data_file_dir + "test_struct_map_ipv4_ipv6.csv",
            data_file_dir + "test_struct_map_largeInt_string.csv",
            // struct-struct
            // TODO, json->struct<struct<>> deserialization result is NULL.
            data_file_dir + "test_struct_struct.csv"};

    vector<ut_type::UTDataTypeDescs> descs_; // struct<> descs matrix
    MutableColumns columns_;                 // column_struct list
    DataTypes types_;
    DataTypeSerDeSPtrs serdes_;
};

TEST_F(DataTypeStructTest, SerdeArrowTest) {
    MutableColumns columns;
    DataTypes types;
    for (int i = 0; i < descs_.size(); i++) {
        columns.push_back(columns_[i]->get_ptr());
        types.push_back(types_[i]);
    }
    CommonDataTypeSerdeTest::assert_arrow_format(columns, types);
}

// TODO `DataTypeStructSerDe::deserialize_one_cell_from_json` has a bug,
// `SerdeArrowTest` cannot test Struct type nested Array and Map and Struct,
// so manually construct data to test them.
// Expect to delete this TEST after `deserialize_one_cell_from_json` is fixed.
TEST_F(DataTypeStructTest, SerdeNestedTypeArrowTest) {
    auto block = std::make_shared<Block>();
    {
        std::string col_name = "struct_nesting_array_map_struct";
        DataTypePtr f1 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
        DataTypePtr f2 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
        DataTypePtr f3 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
        DataTypePtr f4 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
        DataTypePtr f5 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt128>());
        DataTypePtr f6 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>());
        DataTypePtr dt1 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeArray>(f1));
        DataTypePtr dt2 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeMap>(f2, f3));
        DataTypePtr dt3 = std::make_shared<DataTypeNullable>(
                std::make_shared<DataTypeStruct>(std::vector<DataTypePtr> {f4, f5, f6}));
        DataTypePtr st = std::make_shared<DataTypeStruct>(std::vector<DataTypePtr> {dt1, dt2, dt3});

        // nested Array
        Array a1, a2;
        a1.push_back(Field("array"));
        a1.push_back(Null());
        a2.push_back(Field("lucky array"));
        a2.push_back(Field("cute array"));

        // nested Map
        Array k1, k2, v1, v2;
        k1.push_back(1);
        k1.push_back(2);
        k2.push_back(11);
        k2.push_back(22);
        v1.push_back(Field("map"));
        v1.push_back(Null());
        v2.push_back(Field("clever map"));
        v2.push_back(Field("hello map"));

        Map m1, m2;
        m1.push_back(k1);
        m1.push_back(v1);
        m2.push_back(k2);
        m2.push_back(v2);

        // nested Struct
        Tuple t1, t2;
        t1.push_back(Field("clever"));
        t1.push_back(__int128_t(37));
        t1.push_back(true);
        t2.push_back("null");
        t2.push_back(__int128_t(26));
        t2.push_back(false);

        // Struct
        Tuple tt1, tt2;
        tt1.push_back(a1);
        tt1.push_back(m1);
        tt1.push_back(t1);
        tt2.push_back(a2);
        tt2.push_back(m2);
        tt2.push_back(t2);

        MutableColumnPtr struct_column = st->create_column();
        struct_column->reserve(2);
        struct_column->insert(tt1);
        struct_column->insert(tt2);
        vectorized::ColumnWithTypeAndName type_and_name(struct_column->get_ptr(), st, col_name);
        block->insert(type_and_name);
    }
    std::shared_ptr<arrow::RecordBatch> record_batch =
            CommonDataTypeSerdeTest::serialize_arrow(block);
    auto assert_block = std::make_shared<Block>(block->clone_empty());
    CommonDataTypeSerdeTest::deserialize_arrow(assert_block, record_batch);
    CommonDataTypeSerdeTest::compare_two_blocks(block, assert_block);
}

} // namespace doris::vectorized
