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

#include "vec/columns/column.h"
#include "vec/columns/columns_number.h"
#include "vec/columns/common_column_test.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_nullable.h"

namespace doris::vectorized {
class ColumnDecimalTest : public CommonColumnTest {
protected:
    std::string test_data_dir;
    std::string test_result_dir;
    void SetUp() override {
        auto root_dir = std::string(getenv("ROOT"));
        test_data_dir = root_dir + "/be/test/data/vec/columns";
        test_result_dir = root_dir + "/be/test/expected_result/vec/columns";

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

    DataTypePtr dt_decimal32_1 = DataTypeFactory::instance().create_data_type(
            FieldType::OLAP_FIELD_TYPE_DECIMAL32, 1, 0);
    DataTypePtr dt_decimal32_2 = DataTypeFactory::instance().create_data_type(
            FieldType::OLAP_FIELD_TYPE_DECIMAL32, 1, 1);
    DataTypePtr dt_decimal32_3 = DataTypeFactory::instance().create_data_type(
            FieldType::OLAP_FIELD_TYPE_DECIMAL32, 8, 3);
    DataTypePtr dt_decimal32_4 = DataTypeFactory::instance().create_data_type(
            FieldType::OLAP_FIELD_TYPE_DECIMAL32, 9, 0);
    DataTypePtr dt_decimal32_5 = DataTypeFactory::instance().create_data_type(
            FieldType::OLAP_FIELD_TYPE_DECIMAL32, 9, 9);

    DataTypePtr dt_decimal64_1 = DataTypeFactory::instance().create_data_type(
            FieldType::OLAP_FIELD_TYPE_DECIMAL64, 18, 0);
    DataTypePtr dt_decimal64_2 = DataTypeFactory::instance().create_data_type(
            FieldType::OLAP_FIELD_TYPE_DECIMAL64, 18, 9);
    DataTypePtr dt_decimal64_3 = DataTypeFactory::instance().create_data_type(
            FieldType::OLAP_FIELD_TYPE_DECIMAL64, 18, 18);

    DataTypePtr dt_decimal128_1 = DataTypeFactory::instance().create_data_type(
            FieldType::OLAP_FIELD_TYPE_DECIMAL128I, 38, 0);
    DataTypePtr dt_decimal128_2 = DataTypeFactory::instance().create_data_type(
            FieldType::OLAP_FIELD_TYPE_DECIMAL128I, 38, 30);
    DataTypePtr dt_decimal128_3 = DataTypeFactory::instance().create_data_type(
            FieldType::OLAP_FIELD_TYPE_DECIMAL128I, 38, 38);

    DataTypePtr dt_decimal256_1 = DataTypeFactory::instance().create_data_type(
            FieldType::OLAP_FIELD_TYPE_DECIMAL256, 76, 0);
    DataTypePtr dt_decimal256_2 = DataTypeFactory::instance().create_data_type(
            FieldType::OLAP_FIELD_TYPE_DECIMAL256, 76, 38);
    DataTypePtr dt_decimal256_3 = DataTypeFactory::instance().create_data_type(
            FieldType::OLAP_FIELD_TYPE_DECIMAL256, 76, 76);

    /*
/// The maximum precision representable by a 4-byte decimal (Decimal4Value)
constexpr int MAX_DECIMAL32_PRECISION = 9;
/// The maximum precision representable by a 8-byte decimal (Decimal8Value)
constexpr int MAX_DECIMAL64_PRECISION = 18;
/// The maximum precision representable by a 16-byte decimal
constexpr int MAX_DECIMAL128_PRECISION = 38;
/// The maximum precision representable by a 32-byte decimal
constexpr int MAX_DECIMAL256_PRECISION = 76;
*/
    ColumnDecimal32::MutablePtr column_decimal32_1; // decimal32(1,0)
    ColumnDecimal32::MutablePtr column_decimal32_2; // decimal32(1,1)
    ColumnDecimal32::MutablePtr column_decimal32_3; // decimal32(8,3)
    ColumnDecimal32::MutablePtr column_decimal32_4; // decimal32(9,0)
    ColumnDecimal32::MutablePtr column_decimal32_5; // decimal32(9,9)

    ColumnDecimal64::MutablePtr column_decimal64_1; // decimal64(18,0)
    ColumnDecimal64::MutablePtr column_decimal64_2; // decimal64(18,9)
    ColumnDecimal64::MutablePtr column_decimal64_3; // decimal64(18,18)

    ColumnDecimal128V3::MutablePtr column_decimal128_1; // decimal128(38,0)
    ColumnDecimal128V3::MutablePtr column_decimal128_2; // decimal128(38,30)
    ColumnDecimal128V3::MutablePtr column_decimal128_3; // decimal128(38,38)

    ColumnDecimal256::MutablePtr column_decimal256_1; // decimal256(76,0)
    ColumnDecimal256::MutablePtr column_decimal256_2; // decimal256(76,38)
    ColumnDecimal256::MutablePtr column_decimal256_3; // decimal256(76,76)

    ColumnConst::MutablePtr column_decimal32_const;

    void load_columns_data() {
        std::cout << "loading test dataset" << std::endl;
        // decimal32
        {
            MutableColumns columns;
            columns.push_back(column_decimal32_1->get_ptr());
            DataTypeSerDeSPtrs serde = {dt_decimal32_1->get_serde()};
            std::string data_file = test_data_dir + "/DECIMALV3(1,0).csv";
            load_columns_data_from_file(columns, serde, ';', {0}, data_file);
        }
        {
            MutableColumns columns;
            columns.push_back(column_decimal32_2->get_ptr());
            DataTypeSerDeSPtrs serde = {dt_decimal32_2->get_serde()};
            std::string data_file = test_data_dir + "/DECIMALV3(1,1).csv";
            load_columns_data_from_file(columns, serde, ';', {0}, data_file);
        }
        {
            MutableColumns columns;
            columns.push_back(column_decimal32_3->get_ptr());
            DataTypeSerDeSPtrs serde = {dt_decimal32_3->get_serde()};
            std::string data_file = test_data_dir + "/DECIMALV3(8,3).csv";
            load_columns_data_from_file(columns, serde, ';', {0}, data_file);
        }
        {
            MutableColumns columns;
            columns.push_back(column_decimal32_4->get_ptr());
            DataTypeSerDeSPtrs serde = {dt_decimal32_4->get_serde()};
            std::string data_file = test_data_dir + "/DECIMALV3(9,0).csv";
            load_columns_data_from_file(columns, serde, ';', {0}, data_file);
        }
        {
            MutableColumns columns;
            columns.push_back(column_decimal32_5->get_ptr());
            DataTypeSerDeSPtrs serde = {dt_decimal32_5->get_serde()};
            std::string data_file = test_data_dir + "/DECIMALV3(9,9).csv";
            load_columns_data_from_file(columns, serde, ';', {0}, data_file);
        }

        // decimal64
        {
            MutableColumns columns;
            columns.push_back(column_decimal64_1->get_ptr());
            DataTypeSerDeSPtrs serde = {dt_decimal64_1->get_serde()};
            std::string data_file = test_data_dir + "/DECIMALV3(18,0).csv";
            load_columns_data_from_file(columns, serde, ';', {0}, data_file);
        }
        {
            MutableColumns columns;
            columns.push_back(column_decimal64_2->get_ptr());
            DataTypeSerDeSPtrs serde = {dt_decimal64_2->get_serde()};
            std::string data_file = test_data_dir + "/DECIMALV3(18,9).csv";
            load_columns_data_from_file(columns, serde, ';', {0}, data_file);
        }
        {
            MutableColumns columns;
            columns.push_back(column_decimal64_3->get_ptr());
            DataTypeSerDeSPtrs serde = {dt_decimal64_3->get_serde()};
            std::string data_file = test_data_dir + "/DECIMALV3(18,18).csv";
            load_columns_data_from_file(columns, serde, ';', {0}, data_file);
        }

        // decimal128
        {
            MutableColumns columns;
            columns.push_back(column_decimal128_1->get_ptr());
            DataTypeSerDeSPtrs serde = {dt_decimal128_1->get_serde()};
            std::string data_file = test_data_dir + "/DECIMALV3(38,0).csv";
            load_columns_data_from_file(columns, serde, ';', {0}, data_file);
        }
        {
            MutableColumns columns;
            columns.push_back(column_decimal128_2->get_ptr());
            DataTypeSerDeSPtrs serde = {dt_decimal128_2->get_serde()};
            std::string data_file = test_data_dir + "/DECIMALV3(38,30).csv";
            load_columns_data_from_file(columns, serde, ';', {0}, data_file);
        }
        {
            MutableColumns columns;
            columns.push_back(column_decimal128_3->get_ptr());
            DataTypeSerDeSPtrs serde = {dt_decimal128_3->get_serde()};
            std::string data_file = test_data_dir + "/DECIMALV3(38,38).csv";
            load_columns_data_from_file(columns, serde, ';', {0}, data_file);
        }

        // decimal256
        {
            MutableColumns columns;
            columns.push_back(column_decimal256_1->get_ptr());
            DataTypeSerDeSPtrs serde = {dt_decimal256_1->get_serde()};
            std::string data_file = test_data_dir + "/DECIMALV3(76,0).csv";
            load_columns_data_from_file(columns, serde, ';', {0}, data_file);
        }
        {
            MutableColumns columns;
            columns.push_back(column_decimal256_2->get_ptr());
            DataTypeSerDeSPtrs serde = {dt_decimal256_2->get_serde()};
            std::string data_file = test_data_dir + "/DECIMALV3(76,38).csv";
            load_columns_data_from_file(columns, serde, ';', {0}, data_file);
        }
        {
            MutableColumns columns;
            columns.push_back(column_decimal256_3->get_ptr());
            DataTypeSerDeSPtrs serde = {dt_decimal256_3->get_serde()};
            std::string data_file = test_data_dir + "/DECIMALV3(76,76).csv";
            load_columns_data_from_file(columns, serde, ';', {0}, data_file);
        }
        std::cout << "loading test dataset done" << std::endl;
    }

    void hash_common_test(
            const std::string& function_name,
            std::function<void(const MutableColumns& load_cols, DataTypeSerDeSPtrs serders,
                               const std::string& res_file_name)>
                    assert_callback) {
        {
            MutableColumns columns;
            columns.push_back(column_decimal32_1->get_ptr());
            DataTypeSerDeSPtrs serdes = {dt_decimal32_1->get_serde()};
            assert_callback(columns, serdes,
                            test_result_dir + "/column_decimal32_1_" + function_name + ".out");
        }
        {
            MutableColumns columns;
            columns.push_back(column_decimal64_1->get_ptr());
            DataTypeSerDeSPtrs serdes = {dt_decimal64_1->get_serde()};
            assert_callback(columns, serdes,
                            test_result_dir + "/column_decimal64_1_" + function_name + ".out");
        }
        {
            MutableColumns columns;
            columns.push_back(column_decimal128_1->get_ptr());
            DataTypeSerDeSPtrs serdes = {dt_decimal128_1->get_serde()};
            assert_callback(columns, serdes,
                            test_result_dir + "/column_decimal128_1_" + function_name + ".out");
        }
        {
            MutableColumns columns;
            columns.push_back(column_decimal256_1->get_ptr());
            DataTypeSerDeSPtrs serdes = {dt_decimal256_1->get_serde()};
            assert_callback(columns, serdes,
                            test_result_dir + "/column_decimal256_1_" + function_name + ".out");
        }
    }
    template <typename T>
    void _column_decimal_common_test_with_type(const std::string& function_name, T callback) {
        {
            MutableColumns columns;
            columns.push_back(column_decimal32_1->get_ptr());
            DataTypeSerDeSPtrs serdes = {dt_decimal32_1->get_serde()};
            callback(Decimal32(), columns);
        }
        {
            MutableColumns columns;
            columns.push_back(column_decimal32_2->get_ptr());
            DataTypeSerDeSPtrs serdes = {dt_decimal32_2->get_serde()};
            callback(Decimal32(), columns);
        }
        {
            MutableColumns columns;
            columns.push_back(column_decimal32_3->get_ptr());
            DataTypeSerDeSPtrs serdes = {dt_decimal32_3->get_serde()};
            callback(Decimal32(), columns);
        }
        {
            MutableColumns columns;
            columns.push_back(column_decimal32_4->get_ptr());
            DataTypeSerDeSPtrs serdes = {dt_decimal32_4->get_serde()};
            callback(Decimal32(), columns);
        }
        {
            MutableColumns columns;
            columns.push_back(column_decimal32_5->get_ptr());
            DataTypeSerDeSPtrs serdes = {dt_decimal32_5->get_serde()};
            callback(Decimal32(), columns);
        }
        {
            MutableColumns columns;
            columns.push_back(column_decimal64_1->get_ptr());
            DataTypeSerDeSPtrs serdes = {dt_decimal64_1->get_serde()};
            callback(Decimal64(), columns);
        }
        {
            MutableColumns columns;
            columns.push_back(column_decimal64_2->get_ptr());
            DataTypeSerDeSPtrs serdes = {dt_decimal64_2->get_serde()};
            callback(Decimal64(), columns);
        }
        {
            MutableColumns columns;
            columns.push_back(column_decimal64_3->get_ptr());
            DataTypeSerDeSPtrs serdes = {dt_decimal64_3->get_serde()};
            callback(Decimal64(), columns);
        }

        {
            MutableColumns columns;
            columns.push_back(column_decimal128_1->get_ptr());
            DataTypeSerDeSPtrs serdes = {dt_decimal128_1->get_serde()};
            callback(Decimal128V3(), columns);
        }
        {
            MutableColumns columns;
            columns.push_back(column_decimal128_2->get_ptr());
            DataTypeSerDeSPtrs serdes = {dt_decimal128_2->get_serde()};
            callback(Decimal128V3(), columns);
        }
        {
            MutableColumns columns;
            columns.push_back(column_decimal128_3->get_ptr());
            DataTypeSerDeSPtrs serdes = {dt_decimal128_3->get_serde()};
            callback(Decimal128V3(), columns);
        }
        {
            MutableColumns columns;
            columns.push_back(column_decimal256_1->get_ptr());
            DataTypeSerDeSPtrs serdes = {dt_decimal256_1->get_serde()};
            callback(Decimal256(), columns);
        }
        {
            MutableColumns columns;
            columns.push_back(column_decimal256_2->get_ptr());
            DataTypeSerDeSPtrs serdes = {dt_decimal256_2->get_serde()};
            callback(Decimal256(), columns);
        }
        {
            MutableColumns columns;
            columns.push_back(column_decimal256_3->get_ptr());
            DataTypeSerDeSPtrs serdes = {dt_decimal256_3->get_serde()};
            callback(Decimal256(), columns);
        }
    }
};

TEST_F(ColumnDecimalTest, has_enough_capacity_test) {
    _column_decimal_common_test_with_type("has_enough_capacity",
                                          assert_column_vector_has_enough_capacity_callback);
}
TEST_F(ColumnDecimalTest, get_data_at_test) {
    _column_decimal_common_test_with_type("get_data_at", assert_column_vector_get_data_at_callback);
}
TEST_F(ColumnDecimalTest, field_test) {
    _column_decimal_common_test_with_type("field", assert_column_vector_field_callback);
}
TEST_F(ColumnDecimalTest, insert_from_test) {
    _column_decimal_common_test_with_type("insert_from", assert_column_vector_insert_from_callback);
}
TEST_F(ColumnDecimalTest, insert_indices_from_test) {
    _column_decimal_common_test_with_type("insert_indices_from",
                                          assert_column_vector_insert_indices_from_callback);
}

// decimal, vector, nullable, PredicateColumnType
TEST_F(ColumnDecimalTest, insert_many_fix_len_data_test) {
    _column_decimal_common_test_with_type("insert_many_fix_len_data",
                                          assert_column_vector_insert_many_fix_len_data_callback);
}

TEST_F(ColumnDecimalTest, insert_many_raw_data_test) {
    _column_decimal_common_test_with_type("insert_many_raw_data",
                                          assert_column_vector_insert_many_raw_data_callback);
}

TEST_F(ColumnDecimalTest, insert_data_test) {
    _column_decimal_common_test_with_type("insert_data", assert_column_vector_insert_data_callback);
}

TEST_F(ColumnDecimalTest, insert_default_test) {
    _column_decimal_common_test_with_type("insert_default",
                                          assert_column_vector_insert_default_callback);
}

TEST_F(ColumnDecimalTest, insert_range_from_test) {
    _column_decimal_common_test_with_type("insert_range_from",
                                          assert_column_vector_insert_range_from_callback);
}

TEST_F(ColumnDecimalTest, insert_many_defaults_test) {
    _column_decimal_common_test_with_type("insert_many_defaults",
                                          assert_column_vector_insert_many_defaults_callback);
}

TEST_F(ColumnDecimalTest, insert_many_from_test) {
    _column_decimal_common_test_with_type("insert_many_from",
                                          assert_column_vector_insert_many_from_callback);
}

TEST_F(ColumnDecimalTest, pop_back_test) {
    _column_decimal_common_test_with_type("pop_back", assert_column_vector_pop_back_callback);
}

TEST_F(ColumnDecimalTest, ser_deser_test) {
    {
        MutableColumns columns;
        columns.push_back(column_decimal32_1->get_ptr());
        ser_deserialize_with_arena_impl(columns, {dt_decimal32_1});
    }
    {
        MutableColumns columns;
        columns.push_back(column_decimal64_1->get_ptr());
        ser_deserialize_with_arena_impl(columns, {dt_decimal64_1});
    }
    {
        MutableColumns columns;
        columns.push_back(column_decimal128_1->get_ptr());
        ser_deserialize_with_arena_impl(columns, {dt_decimal128_1});
    }
    {
        MutableColumns columns;
        columns.push_back(column_decimal256_1->get_ptr());
        ser_deserialize_with_arena_impl(columns, {dt_decimal256_1});
    }
}
TEST_F(ColumnDecimalTest, ser_deser_vec_test) {
    {
        MutableColumns columns;
        columns.push_back(column_decimal32_1->get_ptr());
        ser_deser_vec(columns, {dt_decimal32_1});
    }
    {
        MutableColumns columns;
        columns.push_back(column_decimal64_1->get_ptr());
        ser_deser_vec(columns, {dt_decimal64_1});
    }
    {
        MutableColumns columns;
        columns.push_back(column_decimal128_1->get_ptr());
        ser_deser_vec(columns, {dt_decimal128_1});
    }
    {
        MutableColumns columns;
        columns.push_back(column_decimal256_1->get_ptr());
        ser_deser_vec(columns, {dt_decimal256_1});
    }
}

TEST_F(ColumnDecimalTest, get_max_row_byte_size_test) {
    _column_decimal_common_test_with_type("get_max_row_byte_size",
                                          assert_column_vector_get_max_row_byte_size_callback);
}
TEST_F(ColumnDecimalTest, update_sip_hash_with_value_test) {
    hash_common_test("update_sip_hash_with_value", assert_update_siphashes_with_value_callback);
}
TEST_F(ColumnDecimalTest, update_hashes_with_value_test) {
    hash_common_test("update_hashes_with_value", assert_update_hashes_with_value_callback);
}
TEST_F(ColumnDecimalTest, update_crcs_with_value_test) {
    std::string function_name = "update_crcs_with_value";
    {
        MutableColumns columns;
        columns.push_back(column_decimal32_1->get_ptr());
        DataTypeSerDeSPtrs serdes = {dt_decimal32_1->get_serde()};
        std::vector<PrimitiveType> pts(columns.size(), PrimitiveType::TYPE_DECIMAL32);
        assert_update_crc_hashes_callback(
                columns, serdes, pts,
                test_result_dir + "/column_decimal32_1_" + function_name + ".out");
    }
    {
        MutableColumns columns;
        columns.push_back(column_decimal64_1->get_ptr());
        DataTypeSerDeSPtrs serdes = {dt_decimal64_1->get_serde()};
        std::vector<PrimitiveType> pts(columns.size(), PrimitiveType::TYPE_DECIMAL64);
        assert_update_crc_hashes_callback(
                columns, serdes, pts,
                test_result_dir + "/column_decimal64_1_" + function_name + ".out");
    }
    {
        MutableColumns columns;
        columns.push_back(column_decimal128_1->get_ptr());
        DataTypeSerDeSPtrs serdes = {dt_decimal128_1->get_serde()};
        std::vector<PrimitiveType> pts(columns.size(), PrimitiveType::TYPE_DECIMAL128I);
        assert_update_crc_hashes_callback(
                columns, serdes, pts,
                test_result_dir + "/column_decimal128_1_" + function_name + ".out");
    }
    {
        MutableColumns columns;
        columns.push_back(column_decimal256_1->get_ptr());
        DataTypeSerDeSPtrs serdes = {dt_decimal256_1->get_serde()};
        std::vector<PrimitiveType> pts(columns.size(), PrimitiveType::TYPE_DECIMAL256);
        assert_update_crc_hashes_callback(
                columns, serdes, pts,
                test_result_dir + "/column_decimal256_1_" + function_name + ".out");
    }
}

TEST_F(ColumnDecimalTest, update_xxHash_with_value_test) {
    hash_common_test("update_xxHash_with_value", assert_update_xxHash_with_value_callback);
}
TEST_F(ColumnDecimalTest, update_crc_with_value_test) {
    hash_common_test("update_crc_with_value", assert_update_crc_with_value_callback);
}

TEST_F(ColumnDecimalTest, get_bool_test) {
    _column_decimal_common_test_with_type("get_bool", assert_column_vector_get_bool_callback);
}
TEST_F(ColumnDecimalTest, get_int64_test) {
    _column_decimal_common_test_with_type("get_int", assert_column_vector_get_int64_callback);
}
TEST_F(ColumnDecimalTest, filter_test) {
    _column_decimal_common_test_with_type("filter", assert_column_vector_filter_callback);
}
TEST_F(ColumnDecimalTest, get_permutation_test) {
    assert_column_permutations(*column_decimal64_1, dt_decimal64_1);
    assert_column_permutations(*column_decimal32_1, dt_decimal32_1);
    assert_column_permutations(*column_decimal32_2, dt_decimal32_2);
    assert_column_permutations(*column_decimal32_3, dt_decimal32_3);
    assert_column_permutations(*column_decimal32_4, dt_decimal32_4);
    assert_column_permutations(*column_decimal32_5, dt_decimal32_5);
    assert_column_permutations(*column_decimal64_1, dt_decimal64_1);
    assert_column_permutations(*column_decimal64_2, dt_decimal64_2);
    assert_column_permutations(*column_decimal64_3, dt_decimal64_3);
    assert_column_permutations(*column_decimal128_1, dt_decimal128_1);
    assert_column_permutations(*column_decimal128_2, dt_decimal128_2);
    assert_column_permutations(*column_decimal128_3, dt_decimal128_3);
    assert_column_permutations(*column_decimal256_1, dt_decimal256_1);
    assert_column_permutations(*column_decimal256_2, dt_decimal256_2);
    assert_column_permutations(*column_decimal256_3, dt_decimal256_3);
}

TEST_F(ColumnDecimalTest, permute_test) {
    MutableColumns columns;
    columns.push_back(column_decimal32_1->get_ptr());
    columns.push_back(column_decimal64_1->get_ptr());
    columns.push_back(column_decimal128_1->get_ptr());
    columns.push_back(column_decimal256_1->get_ptr());
    assert_permute(columns, 0);
    assert_permute(columns, 1);
    assert_permute(columns, column_decimal32_1->size());
    assert_permute(columns, UINT64_MAX);
}

TEST_F(ColumnDecimalTest, replicate_test) {
    _column_decimal_common_test_with_type("replicate", assert_column_vector_replicate_callback);
}

template <typename T>
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

TEST_F(ColumnDecimalTest, insert_value_test) {
    insert_value_test(column_decimal32_1.get());
    insert_value_test(column_decimal64_1.get());
    insert_value_test(column_decimal128_1.get());
    insert_value_test(column_decimal256_1.get());
}

TEST_F(ColumnDecimalTest, replace_column_data_test) {
    _column_decimal_common_test_with_type("replace_column_data",
                                          assert_column_vector_replace_column_data_callback);
}

TEST_F(ColumnDecimalTest, replace_column_null_data_test) {
    _column_decimal_common_test_with_type("replace_column_null_data",
                                          assert_column_vector_replace_column_null_data_callback);
}

void assert_sort_column_callback(const MutableColumns& load_cols, DataTypeSerDeSPtrs serders,
                                 const std::string& res_file_path = "") {
    std::vector<UInt64> limits = {0, 10, 100, 1000, 10000, 100000};
    for (auto orig_limit : limits) {
        for (const auto& col : load_cols) {
            size_t size = col->size();
            auto limit = orig_limit;
            if (limit >= size) {
                limit = 0;
            }
            IColumn::Permutation perm(size);
            for (size_t i = 0; i < size; ++i) {
                perm[i] = i;
            }
            EqualFlags flags(size, 1);
            EqualRange range {0, size};
            ColumnWithSortDescription column_with_sort_desc(col.get(),
                                                            SortColumnDescription(0, 1, 0));
            ColumnSorter sorter(column_with_sort_desc, limit);
            col->sort_column(&sorter, flags, perm, range, true);
        }
    }
}

} // namespace doris::vectorized
