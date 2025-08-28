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

#include <cstdint>

#include "vec/columns/column.h"
#include "vec/columns/common_column_test.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_factory.hpp"

namespace doris::vectorized {
static std::string test_data_dir;
static std::string test_result_dir;
static DataTypePtr dt_float32 =
        DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_FLOAT, false);
static DataTypePtr dt_float64 =
        DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_DOUBLE, false);

static DataTypePtr dt_int8 =
        DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_TINYINT, 0, 0);
static DataTypePtr dt_int16 =
        DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_SMALLINT, 0, 0);
static DataTypePtr dt_int32 =
        DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_INT, 0, 0);
static DataTypePtr dt_int64 =
        DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_BIGINT, 0, 0);
static DataTypePtr dt_int128 =
        DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_LARGEINT, 0, 0);
static DataTypePtr dt_uint8 =
        DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_BOOLEAN, false);

static DataTypePtr dt_datetime =
        DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_DATETIME, false);
static DataTypePtr dt_date =
        DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_DATE, false);

static DataTypePtr dt_datetime_v2_0 =
        DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_DATETIMEV2, 0, 0);
static DataTypePtr dt_datetime_v2_5 =
        DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_DATETIMEV2, 0, 5);
static DataTypePtr dt_datetime_v2_6 =
        DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_DATETIMEV2, 0, 6);
static DataTypePtr dt_date_v2 =
        DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_DATEV2, false);

static ColumnFloat32::MutablePtr column_float32;
static ColumnFloat64::MutablePtr column_float64;

static ColumnInt8::MutablePtr column_int8;
static ColumnInt16::MutablePtr column_int16;
static ColumnInt32::MutablePtr column_int32;
static ColumnInt64::MutablePtr column_int64;
static ColumnInt128::MutablePtr column_int128;
static ColumnUInt8::MutablePtr column_uint8;

static ColumnDateTime::MutablePtr column_datetime;
static ColumnDate::MutablePtr column_date;
static ColumnDateTimeV2::MutablePtr column_datetime_v2_0;
static ColumnDateTimeV2::MutablePtr column_datetime_v2_5;
static ColumnDateTimeV2::MutablePtr column_datetime_v2_6;
static ColumnDateV2::MutablePtr column_date_v2;

class ColumnVectorTest : public CommonColumnTest {
protected:
    static void SetUpTestSuite() {
        auto root_dir = std::string(getenv("ROOT"));
        test_data_dir = root_dir + "/be/test/data/vec/columns";
        test_result_dir = root_dir + "/be/test/expected_result/vec/columns";

        column_float32 = ColumnFloat32::create();
        column_float64 = ColumnFloat64::create();

        column_int8 = ColumnInt8::create();
        column_int16 = ColumnInt16::create();
        column_int32 = ColumnInt32::create();
        column_int64 = ColumnInt64::create();
        column_int128 = ColumnInt128::create();

        column_uint8 = ColumnUInt8::create();

        column_date = ColumnDate::create();
        column_datetime = ColumnDateTime::create();
        column_datetime_v2_0 = ColumnDateTimeV2::create();
        column_datetime_v2_5 = ColumnDateTimeV2::create();
        column_datetime_v2_6 = ColumnDateTimeV2::create();
        column_date_v2 = ColumnDateV2::create();

        load_columns_data();
    }
    static void load_columns_data() {
        std::cout << "loading test dataset" << std::endl;
        auto test_func = [&](const MutableColumnPtr& column, const DataTypePtr& dt,
                             const std::string& data_file_name) {
            MutableColumns columns;
            columns.push_back(column->get_ptr());
            DataTypeSerDeSPtrs serde = {dt->get_serde()};
            load_columns_data_from_file(columns, serde, ';', {0},
                                        test_data_dir + "/" + data_file_name);
            EXPECT_TRUE(!column->empty());
        };
        test_func(column_float32->get_ptr(), dt_float32, "FLOAT.csv");
        test_func(column_float64->get_ptr(), dt_float64, "DOUBLE.csv");

        test_func(column_int8->get_ptr(), dt_int8, "TINYINT.csv");
        test_func(column_int16->get_ptr(), dt_int16, "SMALLINT.csv");
        test_func(column_int32->get_ptr(), dt_int32, "INT.csv");
        test_func(column_int64->get_ptr(), dt_int64, "BIGINT.csv");
        test_func(column_int128->get_ptr(), dt_int128, "LARGEINT.csv");

        test_func(column_uint8->get_ptr(), dt_uint8, "TINYINT_UNSIGNED.csv");

        test_func(column_datetime->get_ptr(), dt_datetime, "DATETIMEV1.csv");
        test_func(column_date->get_ptr(), dt_date, "DATEV1.csv");
        test_func(column_datetime_v2_0->get_ptr(), dt_datetime_v2_0, "DATETIMEV2(0).csv");
        test_func(column_datetime_v2_5->get_ptr(), dt_datetime_v2_5, "DATETIMEV2(5).csv");
        test_func(column_datetime_v2_6->get_ptr(), dt_datetime_v2_6, "DATETIMEV2(6).csv");
        test_func(column_date_v2->get_ptr(), dt_date_v2, "DATEV2.csv");
    }

    void hash_common_test(
            const std::string& function_name,
            std::function<void(const MutableColumns& load_cols, DataTypeSerDeSPtrs serders,
                               const std::string& res_file_name)>
                    assert_callback) {
        auto test_func = [&](const MutableColumnPtr& column, const DataTypePtr& dt,
                             const std::string& res_file_name) {
            MutableColumns columns;
            auto col_clone = column->clone();
            columns.push_back(col_clone->get_ptr());
            DataTypeSerDeSPtrs serdes = {dt->get_serde()};
            assert_callback(columns, serdes,
                            test_result_dir + "/" + res_file_name + "_" + function_name + ".out");
        };
        // test_func(column_float32->get_ptr(), dt_float32, "column_float32");
        // test_func(column_float64->get_ptr(), dt_float64, "column_float64");
        test_func(column_int8->get_ptr(), dt_int8, "column_int8");
        test_func(column_int16->get_ptr(), dt_int16, "column_int16");
        test_func(column_int32->get_ptr(), dt_int32, "column_int32");
        test_func(column_int64->get_ptr(), dt_int64, "column_int64");
        test_func(column_int128->get_ptr(), dt_int128, "column_int128");
        test_func(column_uint8->get_ptr(), dt_uint8, "column_uint8");
        test_func(column_date_v2->get_ptr(), dt_date_v2, "column_date_v2");
        test_func(column_datetime_v2_0->get_ptr(), dt_datetime_v2_0, "column_datetime_v2_0");
        test_func(column_datetime_v2_5->get_ptr(), dt_datetime_v2_5, "column_datetime_v2_5");
        test_func(column_datetime_v2_6->get_ptr(), dt_datetime_v2_6, "column_datetime_v2_6");
        test_func(column_datetime->get_ptr(), dt_datetime, "column_datetime_v1");
        test_func(column_date->get_ptr(), dt_date, "column_date_v1");
    }

#define _column_vector_common_test(callback)                               \
    callback<TYPE_TINYINT>((Int8)0, column_int8->get_ptr());               \
    callback<TYPE_SMALLINT>((Int16)0, column_int16->get_ptr());            \
    callback<TYPE_INT>((Int32)0, column_int32->get_ptr());                 \
    callback<TYPE_BIGINT>((Int64)0, column_int64->get_ptr());              \
    callback<TYPE_LARGEINT>((Int128)0, column_int128->get_ptr());          \
    callback<TYPE_BOOLEAN>((UInt8)0, column_uint8->get_ptr());             \
    callback<TYPE_DATE>((Int64)0, column_date->get_ptr());                 \
    callback<TYPE_DATETIME>((Int64)0, column_datetime->get_ptr());         \
    callback<TYPE_DATETIMEV2>((UInt64)0, column_datetime_v2_0->get_ptr()); \
    callback<TYPE_DATETIMEV2>((UInt64)0, column_datetime_v2_5->get_ptr()); \
    callback<TYPE_DATETIMEV2>((UInt64)0, column_datetime_v2_6->get_ptr()); \
    callback<TYPE_DATEV2>((UInt32)0, column_date_v2->get_ptr());
};

TEST_F(ColumnVectorTest, get_name) {
    column_int8->get_name();
}
TEST_F(ColumnVectorTest, structure_equals) {
    EXPECT_TRUE(column_int8->structure_equals(*column_int8));
}
TEST_F(ColumnVectorTest, size) {
    auto test_func = [](const auto& column) {
        using ColumnType = std::decay_t<decltype(column)>;
        EXPECT_EQ(column.byte_size(), column.size() * sizeof(typename ColumnType::value_type));
        EXPECT_EQ(column.allocated_bytes(), column.data.allocated_bytes());
    };
    test_func(*column_int8);
    test_func(*column_int16);
    test_func(*column_int32);
    test_func(*column_int64);
    test_func(*column_int128);
    test_func(*column_uint8);
    test_func(*column_datetime);
    test_func(*column_date);
    test_func(*column_datetime_v2_0);
}
TEST_F(ColumnVectorTest, clear) {
    auto test_func = [&](const MutableColumnPtr& column) {
        auto col_cloned = column->clone();
        col_cloned->clear();
        EXPECT_EQ(col_cloned->size(), 0);
    };
    test_func(column_int8->get_ptr());
    test_func(column_int16->get_ptr());
    test_func(column_int32->get_ptr());
    test_func(column_int64->get_ptr());
    test_func(column_int128->get_ptr());
    test_func(column_uint8->get_ptr());
    test_func(column_date_v2->get_ptr());
    test_func(column_datetime_v2_0->get_ptr());
    test_func(column_datetime_v2_5->get_ptr());
    test_func(column_datetime_v2_6->get_ptr());
    test_func(column_datetime->get_ptr());
    test_func(column_date->get_ptr());
}
TEST_F(ColumnVectorTest, get_data_at) {
    _column_vector_common_test(assert_column_vector_get_data_at_callback);
}

TEST_F(ColumnVectorTest, field) {
    _column_vector_common_test(assert_column_vector_field_callback);
}
TEST_F(ColumnVectorTest, insert_from) {
    // insert from data csv and assert insert result
    _column_vector_common_test(assert_column_vector_insert_from_callback);
}
TEST_F(ColumnVectorTest, insert_data) {
    _column_vector_common_test(assert_column_vector_insert_data_callback);
}
TEST_F(ColumnVectorTest, insert_many_vals) {
    _column_vector_common_test(assert_column_vector_insert_many_vals_callback);
}
TEST_F(ColumnVectorTest, insert_many_from) {
    _column_vector_common_test(assert_column_vector_insert_many_from_callback);
}
TEST_F(ColumnVectorTest, insert_range_of_integer) {
    _column_vector_common_test(assert_column_vector_insert_range_of_integer_callback);
    assert_column_vector_insert_range_of_integer_callback<TYPE_FLOAT>((Float32)0,
                                                                      column_float32->get_ptr());
    assert_column_vector_insert_range_of_integer_callback<TYPE_DOUBLE>((Float64)0,
                                                                       column_float64->get_ptr());
}
// void insert_date_column(const char* data_ptr, size_t num) {
// decimal, vector, nullable, PredicateColumnType
TEST_F(ColumnVectorTest, insert_many_fix_len_data) {
    assert_column_vector_insert_many_fix_len_data_callback<TYPE_TINYINT>((Int8)0,
                                                                         column_int8->get_ptr());
    assert_column_vector_insert_many_fix_len_data_callback<TYPE_SMALLINT>((Int16)0,
                                                                          column_int16->get_ptr());
    assert_column_vector_insert_many_fix_len_data_callback<TYPE_INT>((Int32)0,
                                                                     column_int32->get_ptr());
    assert_column_vector_insert_many_fix_len_data_callback<TYPE_BIGINT>((Int64)0,
                                                                        column_int64->get_ptr());
    assert_column_vector_insert_many_fix_len_data_callback<TYPE_LARGEINT>((Int128)0,
                                                                          column_int128->get_ptr());
    assert_column_vector_insert_many_fix_len_data_callback<TYPE_BOOLEAN>((UInt8)0,
                                                                         column_uint8->get_ptr());
    assert_column_vector_insert_many_fix_len_data_callback<TYPE_DATETIMEV2>(
            (UInt64)0, column_datetime_v2_0->get_ptr());
    assert_column_vector_insert_many_fix_len_data_callback<TYPE_DATETIMEV2>(
            (UInt64)0, column_datetime_v2_5->get_ptr());
    assert_column_vector_insert_many_fix_len_data_callback<TYPE_DATETIMEV2>(
            (UInt64)0, column_datetime_v2_6->get_ptr());
    assert_column_vector_insert_many_fix_len_data_callback<TYPE_DATEV2>((UInt32)0,
                                                                        column_date_v2->get_ptr());
}
TEST_F(ColumnVectorTest, insert_many_raw_data) {
    _column_vector_common_test(assert_column_vector_insert_many_raw_data_callback);
}
TEST_F(ColumnVectorTest, insert_default) {
    _column_vector_common_test(assert_column_vector_insert_default_callback);
}
TEST_F(ColumnVectorTest, insert_many_defaults) {
    _column_vector_common_test(assert_column_vector_insert_many_defaults_callback);
}
TEST_F(ColumnVectorTest, ser_deser) {
    auto test_func = [&](const MutableColumnPtr& column, const DataTypePtr& dt) {
        MutableColumns columns;
        auto col_cloned = column->clone();
        columns.push_back(col_cloned->get_ptr());
        ser_deserialize_with_arena_impl(columns, {dt});
    };
    test_func(column_int8->get_ptr(), dt_int8);
    test_func(column_int16->get_ptr(), dt_int16);
    test_func(column_int32->get_ptr(), dt_int32);
    test_func(column_int64->get_ptr(), dt_int64);
    test_func(column_int128->get_ptr(), dt_int128);
    test_func(column_uint8->get_ptr(), dt_uint8);
    test_func(column_date_v2->get_ptr(), dt_date_v2);
    test_func(column_datetime_v2_0->get_ptr(), dt_datetime_v2_0);
    test_func(column_datetime_v2_5->get_ptr(), dt_datetime_v2_5);
    test_func(column_datetime_v2_6->get_ptr(), dt_datetime_v2_6);
    test_func(column_datetime->get_ptr(), dt_datetime);
    test_func(column_date->get_ptr(), dt_date);
}
TEST_F(ColumnVectorTest, ser_deser_vec) {
    _column_vector_common_test(assert_column_vector_serialize_vec_callback);
}
TEST_F(ColumnVectorTest, update_xxHash_with_value) {
    hash_common_test("update_xxHash_with_value", assert_update_xxHash_with_value_callback);
}
TEST_F(ColumnVectorTest, update_crc_with_value) {
    hash_common_test("update_crc_with_value", assert_update_crc_with_value_callback);
}
TEST_F(ColumnVectorTest, update_sip_hash_with_value) {
    hash_common_test("update_sip_hash_with_value",
                     assert_column_vector_update_siphashes_with_value_callback);
}
TEST_F(ColumnVectorTest, update_hashes_with_value) {
    hash_common_test("update_hashes_with_value",
                     assert_column_vector_update_hashes_with_value_callback);
}
TEST_F(ColumnVectorTest, update_crcs_with_value) {
    std::string function_name = "update_crcs_with_value";
    auto test_func = [&](const MutableColumnPtr& column, const DataTypePtr& dt, PrimitiveType pt,
                         const std::string& res_file_name) {
        MutableColumns columns;
        columns.push_back(column->get_ptr());
        DataTypeSerDeSPtrs serdes = {dt->get_serde()};
        std::vector<PrimitiveType> pts(columns.size(), pt);
        assert_column_vector_update_crc_hashes_callback(
                columns, serdes, pts,
                test_result_dir + "/" + res_file_name + "_" + function_name + ".out");
    };
    test_func(column_int8->get_ptr(), dt_int8, PrimitiveType::TYPE_TINYINT, "column_int8");
    test_func(column_int16->get_ptr(), dt_int16, PrimitiveType::TYPE_SMALLINT, "column_int16");
    test_func(column_int32->get_ptr(), dt_int32, PrimitiveType::TYPE_INT, "column_int32");
    test_func(column_int64->get_ptr(), dt_int64, PrimitiveType::TYPE_BIGINT, "column_int64");
    test_func(column_int128->get_ptr(), dt_int128, PrimitiveType::TYPE_LARGEINT, "column_int128");
    test_func(column_uint8->get_ptr(), dt_uint8, PrimitiveType::TYPE_TINYINT, "column_uint8");
    test_func(column_date_v2->get_ptr(), dt_date_v2, PrimitiveType::TYPE_DATEV2, "column_date_v2");
    test_func(column_datetime_v2_0->get_ptr(), dt_datetime_v2_0, PrimitiveType::TYPE_DATETIMEV2,
              "column_datetime_v2_0");
    test_func(column_datetime_v2_5->get_ptr(), dt_datetime_v2_5, PrimitiveType::TYPE_DATETIMEV2,
              "column_datetime_v2_5");
    test_func(column_datetime_v2_6->get_ptr(), dt_datetime_v2_6, PrimitiveType::TYPE_DATETIMEV2,
              "column_datetime_v2_6");
    test_func(column_datetime->get_ptr(), dt_datetime, PrimitiveType::TYPE_DATETIME,
              "column_datetime_v1");
    test_func(column_date->get_ptr(), dt_date, PrimitiveType::TYPE_DATE, "column_date_v1");
}
template <PrimitiveType T>
void insert_value_test(ColumnVector<T>* src_col) {
    auto clone_col = src_col->clone_empty();
    auto* col = assert_cast<ColumnVector<T>*>(clone_col.get());
    for (size_t i = 0; i < src_col->size(); ++i) {
        col->insert_value(src_col->get_element(i));
    }
    for (size_t i = 0; i < src_col->size(); ++i) {
        EXPECT_EQ(col->get_element(i), src_col->get_element(i));
    }
}

TEST_F(ColumnVectorTest, insert_value) {
    insert_value_test(column_int8.get());
    insert_value_test(column_int16.get());
    insert_value_test(column_int32.get());
    insert_value_test(column_int64.get());
    insert_value_test(column_int128.get());

    insert_value_test(column_uint8.get());
}

TEST_F(ColumnVectorTest, get_bool) {
    _column_vector_common_test(assert_column_vector_get_bool_callback);
}
TEST_F(ColumnVectorTest, get_int64) {
    _column_vector_common_test(assert_column_vector_get_int64_callback);
}
TEST_F(ColumnVectorTest, insert_range_from) {
    _column_vector_common_test(assert_column_vector_insert_range_from_callback);
}
TEST_F(ColumnVectorTest, insert_indices_from) {
    _column_vector_common_test(assert_column_vector_insert_indices_from_callback);
}

TEST_F(ColumnVectorTest, pop_back) {
    _column_vector_common_test(assert_column_vector_pop_back_callback);
}

TEST_F(ColumnVectorTest, filter) {
    _column_vector_common_test(assert_column_vector_filter_callback);
}
TEST_F(ColumnVectorTest, get_permutation) {
    assert_column_permutations2(*column_int8, dt_int8);
    assert_column_permutations2(*column_int16, dt_int16);
    assert_column_permutations2(*column_int32, dt_int32);
    assert_column_permutations2(*column_int64, dt_int64);
    assert_column_permutations2(*column_int128, dt_int128);

    assert_column_permutations2(*column_uint8, dt_uint8);

    assert_column_permutations2(*column_datetime, dt_datetime);
    assert_column_permutations2(*column_date, dt_date);
    assert_column_permutations2(*column_datetime_v2_0, dt_datetime_v2_0);
    assert_column_permutations2(*column_datetime_v2_5, dt_datetime_v2_5);
    assert_column_permutations2(*column_datetime_v2_6, dt_datetime_v2_6);
    assert_column_permutations2(*column_date_v2, dt_date_v2);
}

TEST_F(ColumnVectorTest, permute) {
    {
        IColumn::Permutation permutation(0);
        EXPECT_THROW(column_int8->permute(permutation, 10), Exception);
        EXPECT_THROW(column_int16->permute(permutation, 10), Exception);
        EXPECT_THROW(column_int32->permute(permutation, 10), Exception);
        EXPECT_THROW(column_int64->permute(permutation, 10), Exception);
        EXPECT_THROW(column_int128->permute(permutation, 10), Exception);
        EXPECT_THROW(column_uint8->permute(permutation, 10), Exception);
        EXPECT_THROW(column_datetime->permute(permutation, 10), Exception);
        EXPECT_THROW(column_date->permute(permutation, 10), Exception);
        EXPECT_THROW(column_datetime_v2_0->permute(permutation, 10), Exception);
        EXPECT_THROW(column_datetime_v2_5->permute(permutation, 10), Exception);
        EXPECT_THROW(column_datetime_v2_6->permute(permutation, 10), Exception);
        EXPECT_THROW(column_date_v2->permute(permutation, 10), Exception);
    }
    MutableColumns columns;
    columns.push_back(column_int8->get_ptr());
    columns.push_back(column_int16->get_ptr());
    columns.push_back(column_int32->get_ptr());
    columns.push_back(column_int64->get_ptr());
    columns.push_back(column_int128->get_ptr());
    columns.push_back(column_uint8->get_ptr());
    columns.push_back(column_datetime->get_ptr());
    columns.push_back(column_date->get_ptr());
    columns.push_back(column_datetime_v2_0->get_ptr());
    columns.push_back(column_datetime_v2_5->get_ptr());
    columns.push_back(column_datetime_v2_6->get_ptr());
    columns.push_back(column_date_v2->get_ptr());

    assert_column_vector_permute(columns, 0);
    assert_column_vector_permute(columns, 1);
    assert_column_vector_permute(columns, column_int8->size());
    assert_column_vector_permute(columns, UINT64_MAX);
}

TEST_F(ColumnVectorTest, replace_column_data) {
    _column_vector_common_test(assert_column_vector_replace_column_data_callback);
}

TEST_F(ColumnVectorTest, replace_column_null_data) {
    _column_vector_common_test(assert_column_vector_replace_column_null_data_callback);
}

TEST_F(ColumnVectorTest, compare_internal) {
    _column_vector_common_test(assert_column_vector_compare_internal_callback);
    // assert_column_vector_compare_internal_callback<TYPE_FLOAT>((Float32)0,
    //                                                            column_float32->get_ptr());
    // assert_column_vector_compare_internal_callback<TYPE_DOUBLE>((Float64)0,
    //                                                             column_float64->get_ptr());
}
TEST_F(ColumnVectorTest, has_enough_capacity) {
    _column_vector_common_test(assert_column_vector_has_enough_capacity_callback);
}
TEST_F(ColumnVectorTest, clone_resized) {
    _column_vector_common_test(assert_column_vector_clone_resized_callback);
}
TEST_F(ColumnVectorTest, sort_column) {
    _column_vector_common_test(assert_sort_column_callback);
}

TEST_F(ColumnVectorTest, ScalaTypeUInt8Testerase) {
    auto column = ColumnUInt8::create();
    std::vector<Int8> data = {1, 2, 3, 4, 5};
    for (auto d : data) {
        column->insert_data(reinterpret_cast<const char*>(&d), sizeof(d));
    }
    column->erase(0, 2);
    EXPECT_EQ(column->size(), 3);
    for (int i = 0; i < column->size(); ++i) {
        EXPECT_EQ(column->get_int(i), data[i + 2]);
    }
}

TEST_F(ColumnVectorTest, ScalaTypeUInt8Test2erase) {
    auto column = ColumnUInt8::create();
    std::vector<Int8> data = {1, 2, 3, 4, 5};
    for (auto d : data) {
        column->insert_data(reinterpret_cast<const char*>(&d), sizeof(d));
    }
    column->erase(2, 2);
    EXPECT_EQ(column->size(), 3);
    std::vector<Int8> data2 = {1, 2, 5};
    for (int i = 0; i < column->size(); ++i) {
        EXPECT_EQ(column->get_int(i), data2[i]);
    }
}

TEST_F(ColumnVectorTest, ScalaTypeInt32Testerase) {
    auto column = ColumnInt32::create();
    std::vector<int32_t> data = {1, 2, 3, 4, 5};
    for (auto d : data) {
        column->insert_data(reinterpret_cast<const char*>(&d), sizeof(d));
    }
    column->erase(0, 2);
    EXPECT_EQ(column->size(), 3);
    for (int i = 0; i < column->size(); ++i) {
        EXPECT_EQ(column->get_int(i), data[i + 2]);
    }
}

TEST_F(ColumnVectorTest, ScalaTypeInt32Test2erase) {
    auto column = ColumnInt32::create();
    std::vector<int32_t> data = {1, 2, 3, 4, 5};
    for (auto d : data) {
        column->insert_data(reinterpret_cast<const char*>(&d), sizeof(d));
    }
    column->erase(2, 2);
    EXPECT_EQ(column->size(), 3);
    std::vector<int32_t> data2 = {1, 2, 5};
    for (int i = 0; i < column->size(); ++i) {
        EXPECT_EQ(column->get_int(i), data2[i]);
    }
}

} // namespace doris::vectorized