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

#include "vec/columns/column_decimal.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include <cstddef>

#include "vec/columns/column.h"
#include "vec/columns/common_column_test.h"
#include "vec/core/extended_types.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_nullable.h"

namespace doris::vectorized {
static DataTypePtr dt_decimal128v2 =
        DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_DECIMAL, 27, 9);

static DataTypePtr dt_decimal32_1 =
        DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_DECIMAL32, 1, 0);
static DataTypePtr dt_decimal32_2 =
        DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_DECIMAL32, 1, 1);
static DataTypePtr dt_decimal32_3 =
        DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_DECIMAL32, 8, 3);
static DataTypePtr dt_decimal32_4 =
        DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_DECIMAL32, 9, 0);
static DataTypePtr dt_decimal32_5 =
        DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_DECIMAL32, 9, 9);

static DataTypePtr dt_decimal64_1 =
        DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_DECIMAL64, 18, 0);
static DataTypePtr dt_decimal64_2 =
        DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_DECIMAL64, 18, 9);
static DataTypePtr dt_decimal64_3 =
        DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_DECIMAL64, 18, 18);

static DataTypePtr dt_decimal128_1 =
        DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_DECIMAL128I, 38, 0);
static DataTypePtr dt_decimal128_2 = DataTypeFactory::instance().create_data_type(
        FieldType::OLAP_FIELD_TYPE_DECIMAL128I, 38, 30);
static DataTypePtr dt_decimal128_3 = DataTypeFactory::instance().create_data_type(
        FieldType::OLAP_FIELD_TYPE_DECIMAL128I, 38, 38);

static DataTypePtr dt_decimal256_1 =
        DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_DECIMAL256, 76, 0);
static DataTypePtr dt_decimal256_2 =
        DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_DECIMAL256, 76, 38);
static DataTypePtr dt_decimal256_3 =
        DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_DECIMAL256, 76, 76);

static ColumnDecimal128V2::MutablePtr column_decimal128v2_1; // decimal128v2(27,9)

static ColumnDecimal32::MutablePtr column_decimal32_1; // decimal32(1,0)
static ColumnDecimal32::MutablePtr column_decimal32_2; // decimal32(1,1)
static ColumnDecimal32::MutablePtr column_decimal32_3; // decimal32(8,3)
static ColumnDecimal32::MutablePtr column_decimal32_4; // decimal32(9,0)
static ColumnDecimal32::MutablePtr column_decimal32_5; // decimal32(9,9)

static ColumnDecimal64::MutablePtr column_decimal64_1; // decimal64(18,0)
static ColumnDecimal64::MutablePtr column_decimal64_2; // decimal64(18,9)
static ColumnDecimal64::MutablePtr column_decimal64_3; // decimal64(18,18)

static ColumnDecimal128V3::MutablePtr column_decimal128_1; // decimal128(38,0)
static ColumnDecimal128V3::MutablePtr column_decimal128_2; // decimal128(38,30)
static ColumnDecimal128V3::MutablePtr column_decimal128_3; // decimal128(38,38)

static ColumnDecimal256::MutablePtr column_decimal256_1; // decimal256(76,0)
static ColumnDecimal256::MutablePtr column_decimal256_2; // decimal256(76,38)
static ColumnDecimal256::MutablePtr column_decimal256_3; // decimal256(76,76)

static std::string test_data_dir;
static std::string test_result_dir;
class ColumnDecimalTest : public CommonColumnTest {
protected:
    static void SetUpTestSuite() {
        auto root_dir = std::string(getenv("ROOT"));
        test_data_dir = root_dir + "/be/test/data/vec/columns";
        test_result_dir = root_dir + "/be/test/expected_result/vec/columns";

        column_decimal128v2_1 = ColumnDecimal128V2::create(0, 9);

        column_decimal32_1 = ColumnDecimal32::create(0, 0);
        column_decimal32_2 = ColumnDecimal32::create(0, 1);
        column_decimal32_3 = ColumnDecimal32::create(0, 3);
        column_decimal32_4 = ColumnDecimal32::create(0, 0);
        column_decimal32_5 = ColumnDecimal32::create(0, 9);

        column_decimal64_1 = ColumnDecimal64::create(0, 0);
        column_decimal64_2 = ColumnDecimal64::create(0, 9);
        column_decimal64_3 = ColumnDecimal64::create(0, 18);

        column_decimal128_1 = ColumnDecimal128V3::create(0, 0);
        column_decimal128_2 = ColumnDecimal128V3::create(0, 30);
        column_decimal128_3 = ColumnDecimal128V3::create(0, 38);

        column_decimal256_1 = ColumnDecimal256::create(0, 0);
        column_decimal256_2 = ColumnDecimal256::create(0, 38);
        column_decimal256_3 = ColumnDecimal256::create(0, 76);

        load_columns_data();
    }

    static void load_columns_data() {
        std::cout << "loading test dataset" << std::endl;
        auto load_data_func = [](auto& dt, auto& column, const std::string& data_file) {
            MutableColumns columns;
            columns.push_back(column->get_ptr());
            DataTypeSerDeSPtrs serde = {dt->get_serde()};
            load_columns_data_from_file(columns, serde, ';', {0}, data_file);
            EXPECT_TRUE(!column->empty());
        };
        load_data_func(dt_decimal128v2, column_decimal128v2_1,
                       test_data_dir + "/DECIMALV2(27,9).csv");

        load_data_func(dt_decimal32_1, column_decimal32_1, test_data_dir + "/DECIMALV3(1,0).csv");
        load_data_func(dt_decimal32_2, column_decimal32_2, test_data_dir + "/DECIMALV3(1,1).csv");
        load_data_func(dt_decimal32_3, column_decimal32_3, test_data_dir + "/DECIMALV3(8,3).csv");
        load_data_func(dt_decimal32_4, column_decimal32_4, test_data_dir + "/DECIMALV3(9,0).csv");
        load_data_func(dt_decimal32_5, column_decimal32_5, test_data_dir + "/DECIMALV3(9,9).csv");

        load_data_func(dt_decimal64_1, column_decimal64_1, test_data_dir + "/DECIMALV3(18,0).csv");
        load_data_func(dt_decimal64_2, column_decimal64_2, test_data_dir + "/DECIMALV3(18,9).csv");
        load_data_func(dt_decimal64_3, column_decimal64_3, test_data_dir + "/DECIMALV3(18,18).csv");

        load_data_func(dt_decimal128_1, column_decimal128_1,
                       test_data_dir + "/DECIMALV3(38,0).csv");
        load_data_func(dt_decimal128_2, column_decimal128_2,
                       test_data_dir + "/DECIMALV3(38,30).csv");
        load_data_func(dt_decimal128_3, column_decimal128_3,
                       test_data_dir + "/DECIMALV3(38,38).csv");

        load_data_func(dt_decimal256_1, column_decimal256_1,
                       test_data_dir + "/DECIMALV3(76,0).csv");
        load_data_func(dt_decimal256_2, column_decimal256_2,
                       test_data_dir + "/DECIMALV3(76,38).csv");
        load_data_func(dt_decimal256_3, column_decimal256_3,
                       test_data_dir + "/DECIMALV3(76,76).csv");
        std::cout << "loading test dataset done" << std::endl;
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

        test_func(column_decimal32_1->get_ptr(), dt_decimal32_1, "column_decimal32_1");
        test_func(column_decimal64_1->get_ptr(), dt_decimal64_1, "column_decimal64_1");
        test_func(column_decimal128_1->get_ptr(), dt_decimal128_1, "column_decimal128_1");
        test_func(column_decimal256_1->get_ptr(), dt_decimal256_1, "column_decimal256_1");

        test_func(column_decimal128v2_1->get_ptr(), dt_decimal128v2, "column_decimal128v2_1");
    }
#define _column_decimal_common_test_with_type(callback, exclude_decimal128v2)       \
    callback<TYPE_DECIMAL32>(Decimal32(), column_decimal32_1->get_ptr());           \
    callback<TYPE_DECIMAL32>(Decimal32(), column_decimal32_2->get_ptr());           \
    callback<TYPE_DECIMAL32>(Decimal32(), column_decimal32_3->get_ptr());           \
    callback<TYPE_DECIMAL32>(Decimal32(), column_decimal32_4->get_ptr());           \
    callback<TYPE_DECIMAL32>(Decimal32(), column_decimal32_5->get_ptr());           \
    callback<TYPE_DECIMAL64>(Decimal64(), column_decimal64_1->get_ptr());           \
    callback<TYPE_DECIMAL64>(Decimal64(), column_decimal64_2->get_ptr());           \
    callback<TYPE_DECIMAL64>(Decimal64(), column_decimal64_3->get_ptr());           \
    callback<TYPE_DECIMAL128I>(Decimal128V3(), column_decimal128_1->get_ptr());     \
    callback<TYPE_DECIMAL128I>(Decimal128V3(), column_decimal128_2->get_ptr());     \
    callback<TYPE_DECIMAL128I>(Decimal128V3(), column_decimal128_3->get_ptr());     \
    callback<TYPE_DECIMAL256>(Decimal256(), column_decimal256_1->get_ptr());        \
    callback<TYPE_DECIMAL256>(Decimal256(), column_decimal256_2->get_ptr());        \
    callback<TYPE_DECIMAL256>(Decimal256(), column_decimal256_3->get_ptr());        \
    if (!exclude_decimal128v2) {                                                    \
        callback<TYPE_DECIMALV2>(Decimal128V2(), column_decimal128v2_1->get_ptr()); \
    }
};

TEST_F(ColumnDecimalTest, byte_size) {
    EXPECT_EQ(column_decimal32_1->byte_size(), column_decimal32_1->size() * sizeof(Decimal32));
    EXPECT_EQ(column_decimal64_1->byte_size(), column_decimal64_1->size() * sizeof(Decimal64));
    EXPECT_EQ(column_decimal128_1->byte_size(), column_decimal128_1->size() * sizeof(Decimal128V3));
    EXPECT_EQ(column_decimal256_1->byte_size(), column_decimal256_1->size() * sizeof(Decimal256));

    EXPECT_EQ(column_decimal128v2_1->byte_size(),
              column_decimal128v2_1->size() * sizeof(Decimal128V2));
}
TEST_F(ColumnDecimalTest, allocated_bytes) {
    EXPECT_GE(column_decimal32_1->allocated_bytes(),
              column_decimal32_1->size() * sizeof(Decimal32));
    EXPECT_GE(column_decimal64_1->allocated_bytes(),
              column_decimal64_1->size() * sizeof(Decimal64));
    EXPECT_GE(column_decimal128_1->allocated_bytes(),
              column_decimal128_1->size() * sizeof(Decimal128V3));
    EXPECT_GE(column_decimal256_1->allocated_bytes(),
              column_decimal256_1->size() * sizeof(Decimal256));

    EXPECT_GE(column_decimal128v2_1->allocated_bytes(),
              column_decimal128v2_1->size() * sizeof(Decimal128V2));
}
TEST_F(ColumnDecimalTest, has_enough_capacity) {
    _column_decimal_common_test_with_type(assert_column_vector_has_enough_capacity_callback, false);
}
TEST_F(ColumnDecimalTest, get_data_at) {
    _column_decimal_common_test_with_type(assert_column_vector_get_data_at_callback, false);
}
TEST_F(ColumnDecimalTest, field) {
    _column_decimal_common_test_with_type(assert_column_vector_field_callback, false);
}
TEST_F(ColumnDecimalTest, insert_from) {
    _column_decimal_common_test_with_type(assert_column_vector_insert_from_callback, false);
}
TEST_F(ColumnDecimalTest, insert_indices_from) {
    _column_decimal_common_test_with_type(assert_column_vector_insert_indices_from_callback, false);
}

// decimal, vector, nullable, PredicateColumnType
TEST_F(ColumnDecimalTest, insert_many_fix_len_data) {
    _column_decimal_common_test_with_type(assert_column_vector_insert_many_fix_len_data_callback,
                                          true);
}

TEST_F(ColumnDecimalTest, insert_many_raw_data) {
    _column_decimal_common_test_with_type(assert_column_vector_insert_many_raw_data_callback,
                                          false);
}

TEST_F(ColumnDecimalTest, insert_data) {
    _column_decimal_common_test_with_type(assert_column_vector_insert_data_callback, false);
}

TEST_F(ColumnDecimalTest, insert_default) {
    _column_decimal_common_test_with_type(assert_column_vector_insert_default_callback, false);
}

TEST_F(ColumnDecimalTest, insert_range_from) {
    _column_decimal_common_test_with_type(assert_column_vector_insert_range_from_callback, false);
}

TEST_F(ColumnDecimalTest, insert_many_defaults) {
    _column_decimal_common_test_with_type(assert_column_vector_insert_many_defaults_callback,
                                          false);
}

TEST_F(ColumnDecimalTest, insert_many_from) {
    _column_decimal_common_test_with_type(assert_column_vector_insert_many_from_callback, false);
}

TEST_F(ColumnDecimalTest, pop_back) {
    _column_decimal_common_test_with_type(assert_column_vector_pop_back_callback, false);
}

TEST_F(ColumnDecimalTest, ser_deser) {
    {
        MutableColumns columns;
        auto col_cloned = column_decimal32_1->clone();
        columns.push_back(col_cloned->get_ptr());
        ser_deserialize_with_arena_impl(columns, {dt_decimal32_1});
    }
    {
        MutableColumns columns;
        auto col_cloned = column_decimal64_1->clone();
        columns.push_back(col_cloned->get_ptr());
        ser_deserialize_with_arena_impl(columns, {dt_decimal64_1});
    }
    {
        MutableColumns columns;
        auto col_cloned = column_decimal128_1->clone();
        columns.push_back(col_cloned->get_ptr());
        ser_deserialize_with_arena_impl(columns, {dt_decimal128_1});
    }
    {
        MutableColumns columns;
        auto col_cloned = column_decimal256_1->clone();
        columns.push_back(col_cloned->get_ptr());
        ser_deserialize_with_arena_impl(columns, {dt_decimal256_1});
    }
    {
        MutableColumns columns;
        auto col_cloned = column_decimal128v2_1->clone();
        columns.push_back(col_cloned->get_ptr());
        ser_deserialize_with_arena_impl(columns, {dt_decimal128v2});
    }
}
TEST_F(ColumnDecimalTest, ser_deser_vec) {
    _column_decimal_common_test_with_type(assert_column_vector_serialize_vec_callback, false);
}

TEST_F(ColumnDecimalTest, get_max_row_byte_size) {
    _column_decimal_common_test_with_type(assert_column_vector_get_max_row_byte_size_callback,
                                          false);
}
TEST_F(ColumnDecimalTest, update_sip_hash_with_value) {
    hash_common_test("update_sip_hash_with_value",
                     assert_column_vector_update_siphashes_with_value_callback);
}
TEST_F(ColumnDecimalTest, update_hashes_with_value) {
    hash_common_test("update_hashes_with_value",
                     assert_column_vector_update_hashes_with_value_callback);
}
TEST_F(ColumnDecimalTest, update_crcs_with_value) {
    std::string function_name = "update_crcs_with_value";
    {
        MutableColumns columns;
        columns.push_back(column_decimal32_1->get_ptr());
        DataTypeSerDeSPtrs serdes = {dt_decimal32_1->get_serde()};
        std::vector<PrimitiveType> pts(columns.size(), PrimitiveType::TYPE_DECIMAL32);
        assert_column_vector_update_crc_hashes_callback(
                columns, serdes, pts,
                test_result_dir + "/column_decimal32_1_" + function_name + ".out");
    }
    {
        MutableColumns columns;
        columns.push_back(column_decimal64_1->get_ptr());
        DataTypeSerDeSPtrs serdes = {dt_decimal64_1->get_serde()};
        std::vector<PrimitiveType> pts(columns.size(), PrimitiveType::TYPE_DECIMAL64);
        assert_column_vector_update_crc_hashes_callback(
                columns, serdes, pts,
                test_result_dir + "/column_decimal64_1_" + function_name + ".out");
    }
    {
        MutableColumns columns;
        columns.push_back(column_decimal128_1->get_ptr());
        DataTypeSerDeSPtrs serdes = {dt_decimal128_1->get_serde()};
        std::vector<PrimitiveType> pts(columns.size(), PrimitiveType::TYPE_DECIMAL128I);
        assert_column_vector_update_crc_hashes_callback(
                columns, serdes, pts,
                test_result_dir + "/column_decimal128_1_" + function_name + ".out");
    }
    {
        MutableColumns columns;
        columns.push_back(column_decimal256_1->get_ptr());
        DataTypeSerDeSPtrs serdes = {dt_decimal256_1->get_serde()};
        std::vector<PrimitiveType> pts(columns.size(), PrimitiveType::TYPE_DECIMAL256);
        assert_column_vector_update_crc_hashes_callback(
                columns, serdes, pts,
                test_result_dir + "/column_decimal256_1_" + function_name + ".out");
    }
}

TEST_F(ColumnDecimalTest, update_xxHash_with_value) {
    hash_common_test("update_xxHash_with_value", assert_update_xxHash_with_value_callback);
}
TEST_F(ColumnDecimalTest, update_crc_with_value) {
    hash_common_test("update_crc_with_value", assert_update_crc_with_value_callback);
}
TEST_F(ColumnDecimalTest, compare_at) {
    {
        auto col_dec32 = column_decimal32_3->clone();
        const auto* col_vec_dec32 = assert_cast<const ColumnDecimal32*>(col_dec32.get());
        const auto& src_data = col_vec_dec32->get_data();
        auto item_count = col_dec32->size();
        // create a new column with less scale
        auto col_dec32_cloned_positive =
                ColumnDecimal32::create(0, column_decimal32_3->get_scale());
        auto col_dec32_cloned_negative =
                ColumnDecimal32::create(0, column_decimal32_3->get_scale());
        auto col_dec32_less_scale_positive =
                ColumnDecimal32::create(0, column_decimal32_3->get_scale() - 1);
        auto col_dec32_less_scale_negative =
                ColumnDecimal32::create(0, column_decimal32_3->get_scale() - 1);
        // data should be truncated
        for (size_t i = 0; i < item_count; ++i) {
            if (src_data[i].value > 0) {
                col_dec32_cloned_positive->insert_value(src_data[i]);
                col_dec32_less_scale_positive->insert_value(src_data[i].value / 10);
            } else {
                col_dec32_cloned_negative->insert_value(src_data[i]);
                col_dec32_less_scale_negative->insert_value(src_data[i].value / 10);
            }
        }
        auto positive_count = col_dec32_cloned_positive->size();
        auto negative_count = col_dec32_cloned_negative->size();
        for (size_t i = 0; i < positive_count; ++i) {
            EXPECT_EQ(col_dec32_cloned_positive->compare_at(i, i, *col_dec32_cloned_positive, 0),
                      0);
            auto res =
                    col_dec32_less_scale_positive->compare_at(i, i, *col_dec32_cloned_positive, 0);
            if (res != 0) {
                EXPECT_EQ(res, -1) << col_dec32_less_scale_positive->get_element(i).to_string(
                                              column_decimal32_3->get_scale() - 1)
                                   << " VS "
                                   << col_dec32_cloned_positive->get_element(i).to_string(
                                              column_decimal32_3->get_scale());
            }
        }
        for (size_t i = 0; i < negative_count; ++i) {
            EXPECT_EQ(col_dec32_cloned_negative->compare_at(i, i, *col_dec32_cloned_negative, 0),
                      0);
            auto res =
                    col_dec32_less_scale_negative->compare_at(i, i, *col_dec32_cloned_negative, 0);
            if (res != 0) {
                EXPECT_EQ(res, 1) << col_dec32_less_scale_negative->get_element(i).to_string(
                                             column_decimal32_3->get_scale() - 1)
                                  << " VS "
                                  << col_dec32_cloned_negative->get_element(i).to_string(
                                             column_decimal32_3->get_scale());
            }
        }
    }
    {
        auto col_dec128 = column_decimal128_2->clone();
        const auto* col_vec_dec128 = assert_cast<const ColumnDecimal128V3*>(col_dec128.get());
        const auto& src_data = col_vec_dec128->get_data();
        auto item_count = col_dec128->size();
        auto col_dec128_cloned_positive =
                ColumnDecimal128V3::create(0, column_decimal128_2->get_scale());
        auto col_dec128_cloned_negative =
                ColumnDecimal128V3::create(0, column_decimal128_2->get_scale());
        auto col_dec128_less_scale_positive =
                ColumnDecimal128V3::create(0, column_decimal128_2->get_scale() - 1);
        auto col_dec128_less_scale_negative =
                ColumnDecimal128V3::create(0, column_decimal128_2->get_scale() - 1);
        for (size_t i = 0; i < item_count; ++i) {
            if (src_data[i].value > 0) {
                col_dec128_cloned_positive->insert_value(src_data[i]);
                col_dec128_less_scale_positive->insert_value(Decimal128V3(src_data[i].value / 10));
            } else {
                col_dec128_cloned_negative->insert_value(src_data[i]);
                col_dec128_less_scale_negative->insert_value(Decimal128V3(src_data[i].value / 10));
            }
        }
        auto positive_count = col_dec128_cloned_positive->size();
        auto negative_count = col_dec128_cloned_negative->size();
        for (size_t i = 0; i < positive_count; ++i) {
            EXPECT_EQ(col_dec128_cloned_positive->compare_at(i, i, *col_dec128_cloned_positive, 0),
                      0);
            auto res = col_dec128_less_scale_positive->compare_at(i, i, *col_dec128_cloned_positive,
                                                                  0);
            if (res != 0) {
                EXPECT_EQ(res, -1);
            }
        }
        for (size_t i = 0; i < negative_count; ++i) {
            EXPECT_EQ(col_dec128_cloned_negative->compare_at(i, i, *col_dec128_cloned_negative, 0),
                      0);
            auto res = col_dec128_less_scale_negative->compare_at(i, i, *col_dec128_cloned_negative,
                                                                  0);
            if (res != 0) {
                EXPECT_EQ(res, 1);
            }
        }
    }
    {
        auto col_dec256 = column_decimal256_2->clone();
        const auto* col_vec_dec256 = assert_cast<const ColumnDecimal256*>(col_dec256.get());
        const auto& src_data = col_vec_dec256->get_data();
        auto item_count = col_dec256->size();
        auto col_dec256_cloned_positive =
                ColumnDecimal256::create(0, column_decimal256_2->get_scale());
        auto col_dec256_cloned_negative =
                ColumnDecimal256::create(0, column_decimal256_2->get_scale());
        auto col_dec256_less_scale_positive =
                ColumnDecimal256::create(0, column_decimal256_2->get_scale() - 1);
        auto col_dec256_less_scale_negative =
                ColumnDecimal256::create(0, column_decimal256_2->get_scale() - 1);
        for (size_t i = 0; i < item_count; ++i) {
            if (src_data[i].value > 0) {
                col_dec256_cloned_positive->insert_value(src_data[i]);
                col_dec256_less_scale_positive->insert_value(Decimal256(src_data[i].value / 10));
            } else {
                col_dec256_cloned_negative->insert_value(src_data[i]);
                col_dec256_less_scale_negative->insert_value(Decimal256(src_data[i].value / 10));
            }
        }
        auto positive_count = col_dec256_cloned_positive->size();
        auto negative_count = col_dec256_cloned_negative->size();
        for (size_t i = 0; i < positive_count; ++i) {
            EXPECT_EQ(col_dec256_cloned_positive->compare_at(i, i, *col_dec256_cloned_positive, 0),
                      0);
            auto res = col_dec256_less_scale_positive->compare_at(i, i, *col_dec256_cloned_positive,
                                                                  0);
            if (res != 0) {
                EXPECT_EQ(res, -1);
            }
        }
        for (size_t i = 0; i < negative_count; ++i) {
            EXPECT_EQ(col_dec256_cloned_negative->compare_at(i, i, *col_dec256_cloned_negative, 0),
                      0);
            auto res = col_dec256_less_scale_negative->compare_at(i, i, *col_dec256_cloned_negative,
                                                                  0);
            if (res != 0) {
                EXPECT_EQ(res, 1);
            }
        }
    }
}

TEST_F(ColumnDecimalTest, get_int64) {
    _column_decimal_common_test_with_type(assert_column_vector_get_int64_callback, false);
}
TEST_F(ColumnDecimalTest, filter) {
    _column_decimal_common_test_with_type(assert_column_vector_filter_callback, false);
}
TEST_F(ColumnDecimalTest, get_permutation) {
    assert_column_permutations2(*column_decimal64_1, dt_decimal64_1);
    assert_column_permutations2(*column_decimal32_1, dt_decimal32_1);
    assert_column_permutations2(*column_decimal32_2, dt_decimal32_2);
    assert_column_permutations2(*column_decimal32_3, dt_decimal32_3);
    assert_column_permutations2(*column_decimal32_4, dt_decimal32_4);
    assert_column_permutations2(*column_decimal32_5, dt_decimal32_5);
    assert_column_permutations2(*column_decimal64_1, dt_decimal64_1);
    assert_column_permutations2(*column_decimal64_2, dt_decimal64_2);
    assert_column_permutations2(*column_decimal64_3, dt_decimal64_3);
    assert_column_permutations2(*column_decimal128_1, dt_decimal128_1);
    assert_column_permutations2(*column_decimal128_2, dt_decimal128_2);
    assert_column_permutations2(*column_decimal128_3, dt_decimal128_3);
    assert_column_permutations2(*column_decimal256_1, dt_decimal256_1);
    assert_column_permutations2(*column_decimal256_2, dt_decimal256_2);
    assert_column_permutations2(*column_decimal256_3, dt_decimal256_3);
}

TEST_F(ColumnDecimalTest, clone_resized) {
    _column_decimal_common_test_with_type(assert_column_vector_clone_resized_callback, false);
}
TEST_F(ColumnDecimalTest, permute) {
    {
        IColumn::Permutation permutation(0);
        EXPECT_THROW(column_decimal32_1->permute(permutation, 10), Exception);
        EXPECT_THROW(column_decimal64_1->permute(permutation, 10), Exception);
        EXPECT_THROW(column_decimal128_1->permute(permutation, 10), Exception);
        EXPECT_THROW(column_decimal256_1->permute(permutation, 10), Exception);
    }
    MutableColumns columns;
    columns.push_back(column_decimal32_1->get_ptr());
    columns.push_back(column_decimal64_1->get_ptr());
    columns.push_back(column_decimal128_1->get_ptr());
    columns.push_back(column_decimal256_1->get_ptr());
    assert_column_vector_permute(columns, 0);
    assert_column_vector_permute(columns, 1);
    assert_column_vector_permute(columns, column_decimal32_1->size());
    assert_column_vector_permute(columns, UINT64_MAX);
}

template <PrimitiveType T>
void insert_value_test(ColumnDecimal<T>* src_col) {
    auto col = src_col->clone_empty();
    auto* dec_col = assert_cast<ColumnDecimal<T>*>(col.get());
    for (size_t i = 0; i < src_col->size(); ++i) {
        dec_col->insert_value(src_col->get_element(i));
    }
    for (size_t i = 0; i < src_col->size(); ++i) {
        EXPECT_EQ(dec_col->get_element(i), src_col->get_element(i));
    }
}

TEST_F(ColumnDecimalTest, insert_value) {
    insert_value_test(column_decimal32_1.get());
    insert_value_test(column_decimal64_1.get());
    insert_value_test(column_decimal128_1.get());
    insert_value_test(column_decimal256_1.get());
}

TEST_F(ColumnDecimalTest, replace_column_data) {
    _column_decimal_common_test_with_type(assert_column_vector_replace_column_data_callback, false);
}

TEST_F(ColumnDecimalTest, replace_column_null_data) {
    _column_decimal_common_test_with_type(assert_column_vector_replace_column_null_data_callback,
                                          false);
}
TEST_F(ColumnDecimalTest, compare_internal) {
    _column_decimal_common_test_with_type(assert_column_vector_compare_internal_callback, false);
}

TEST_F(ColumnDecimalTest, get_scale) {
    EXPECT_EQ(column_decimal32_1->get_scale(), dt_decimal32_1->get_scale());
    EXPECT_EQ(column_decimal64_1->get_scale(), dt_decimal64_1->get_scale());
    EXPECT_EQ(column_decimal128_1->get_scale(), dt_decimal128_1->get_scale());
    EXPECT_EQ(column_decimal256_1->get_scale(), dt_decimal256_1->get_scale());
}

TEST_F(ColumnDecimalTest, sort_column) {
    _column_decimal_common_test_with_type(assert_sort_column_callback, false);
}

TEST_F(ColumnDecimalTest, ScalaTypeDecimalTesterase) {
    auto datetype_decimal = vectorized::create_decimal(10, 2, false);
    auto column = datetype_decimal->create_column();
    auto column_res = datetype_decimal->create_column();

    std::vector<double> data = {1.1, 2.2, 3.3, 4.4, 5.5};
    for (auto d : data) {
        column->insert_data(reinterpret_cast<const char*>(&d), sizeof(d));
        column_res->insert_data(reinterpret_cast<const char*>(&d), sizeof(d));
    }
    column->erase(0, 2);
    EXPECT_EQ(column->size(), 3);
    for (int i = 0; i < column->size(); ++i) {
        EXPECT_EQ(column->get_data_at(i), column_res->get_data_at(i + 2));
    }
}

TEST_F(ColumnDecimalTest, ScalaTypeDecimalTest2erase) {
    auto datetype_decimal = vectorized::create_decimal(10, 2, false);
    auto column = datetype_decimal->create_column();
    auto column_res = datetype_decimal->create_column();

    std::vector<double> data = {1.1, 2.2, 3.3, 4.4, 5.5};
    for (auto d : data) {
        column->insert_data(reinterpret_cast<const char*>(&d), sizeof(d));
    }

    std::vector<double> res = {1.1, 2.2, 5.5};
    for (auto d : res) {
        column_res->insert_data(reinterpret_cast<const char*>(&d), sizeof(d));
    }
    column->erase(2, 2);
    EXPECT_EQ(column->size(), 3);
    for (int i = 0; i < column->size(); ++i) {
        EXPECT_EQ(column->get_data_at(i), column_res->get_data_at(i));
    }
}

} // namespace doris::vectorized
