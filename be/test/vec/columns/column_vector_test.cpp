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
#include "vec/columns/columns_number.h"
#include "vec/columns/common_column_test.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_factory.hpp"

namespace doris::vectorized {
class ColumnVectorTest : public CommonColumnTest {
protected:
    std::string test_data_dir;
    std::string test_result_dir;
    void SetUp() override {
        auto root_dir = std::string(getenv("ROOT"));
        test_data_dir = root_dir + "/be/test/data/vec/columns";
        test_result_dir = root_dir + "/be/test/expected_result/vec/columns";

        column_int8 = ColumnInt8::create();
        column_int16 = ColumnInt16::create();
        column_int32 = ColumnInt32::create();
        column_int64 = ColumnInt64::create();
        column_int128 = ColumnInt128::create();

        column_uint8 = ColumnUInt8::create();
        column_uint16 = ColumnUInt16::create();
        column_uint32 = ColumnUInt32::create();
        column_uint64 = ColumnUInt64::create();

        load_columns_data();
    }
    DataTypePtr dt_int8 =
            DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_TINYINT, 0, 0);
    DataTypePtr dt_int16 =
            DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_SMALLINT, 0, 0);
    DataTypePtr dt_int32 =
            DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_INT, 0, 0);
    DataTypePtr dt_int64 =
            DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_BIGINT, 0, 0);
    DataTypePtr dt_int128 =
            DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_LARGEINT, 0, 0);

    DataTypePtr dt_uint8 = DataTypeFactory::instance().create_data_type(TypeIndex::UInt8);
    DataTypePtr dt_uint16 = DataTypeFactory::instance().create_data_type(TypeIndex::UInt16);
    DataTypePtr dt_uint32 = DataTypeFactory::instance().create_data_type(TypeIndex::UInt32);
    DataTypePtr dt_uint64 = DataTypeFactory::instance().create_data_type(TypeIndex::UInt64);

    ColumnInt8::MutablePtr column_int8;
    ColumnInt16::MutablePtr column_int16;
    ColumnInt32::MutablePtr column_int32;
    ColumnInt64::MutablePtr column_int64;
    ColumnInt128::MutablePtr column_int128;

    ColumnUInt8::MutablePtr column_uint8;
    ColumnUInt16::MutablePtr column_uint16;
    ColumnUInt32::MutablePtr column_uint32;
    ColumnUInt64::MutablePtr column_uint64;

    void load_columns_data() {
        std::cout << "loading test dataset" << std::endl;
        {
            MutableColumns columns;
            columns.push_back(column_int8->get_ptr());
            DataTypeSerDeSPtrs serde = {dt_int8->get_serde()};
            std::string data_file = test_data_dir + "/TINYINT.csv";
            load_columns_data_from_file(columns, serde, ';', {0}, data_file);
        }
        {
            MutableColumns columns;
            columns.push_back(column_uint8->get_ptr());
            DataTypeSerDeSPtrs serde = {dt_uint8->get_serde()};
            std::string data_file = test_data_dir + "/TINYINT_UNSIGNED.csv";
            load_columns_data_from_file(columns, serde, ';', {0}, data_file);
        }
        {
            MutableColumns columns;
            columns.push_back(column_int16->get_ptr());
            DataTypeSerDeSPtrs serde = {dt_int16->get_serde()};
            std::string data_file = test_data_dir + "/SMALLINT.csv";
            load_columns_data_from_file(columns, serde, ';', {0}, data_file);
        }
        {
            MutableColumns columns;
            columns.push_back(column_uint16->get_ptr());
            DataTypeSerDeSPtrs serde = {dt_uint16->get_serde()};
            std::string data_file = test_data_dir + "/SMALLINT_UNSIGNED.csv";
            load_columns_data_from_file(columns, serde, ';', {0}, data_file);
        }
        {
            MutableColumns columns;
            columns.push_back(column_int32->get_ptr());
            DataTypeSerDeSPtrs serde = {dt_int32->get_serde()};
            std::string data_file = test_data_dir + "/INT.csv";
            load_columns_data_from_file(columns, serde, ';', {0}, data_file);
        }
        {
            MutableColumns columns;
            columns.push_back(column_uint32->get_ptr());
            DataTypeSerDeSPtrs serde = {dt_uint32->get_serde()};
            std::string data_file = test_data_dir + "/INT_UNSIGNED.csv";
            load_columns_data_from_file(columns, serde, ';', {0}, data_file);
        }
        {
            MutableColumns columns;
            columns.push_back(column_int64->get_ptr());
            DataTypeSerDeSPtrs serde = {dt_int64->get_serde()};
            std::string data_file = test_data_dir + "/BIGINT.csv";
            load_columns_data_from_file(columns, serde, ';', {0}, data_file);
        }
        {
            MutableColumns columns;
            columns.push_back(column_uint64->get_ptr());
            DataTypeSerDeSPtrs serde = {dt_uint64->get_serde()};
            std::string data_file = test_data_dir + "/BIGINT_UNSIGNED.csv";
            load_columns_data_from_file(columns, serde, ';', {0}, data_file);
        }
        {
            MutableColumns columns;
            columns.push_back(column_int128->get_ptr());
            DataTypeSerDeSPtrs serde = {dt_int128->get_serde()};
            std::string data_file = test_data_dir + "/LARGEINT.csv";
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
            columns.push_back(column_int8->get_ptr());
            DataTypeSerDeSPtrs serdes = {dt_int8->get_serde()};
            assert_callback(columns, serdes,
                            test_result_dir + "/column_int8_" + function_name + ".out");
        }
        {
            MutableColumns columns;
            columns.push_back(column_int16->get_ptr());
            DataTypeSerDeSPtrs serdes = {dt_int16->get_serde()};
            assert_callback(columns, serdes,
                            test_result_dir + "/column_int16_" + function_name + ".out");
        }
        {
            MutableColumns columns;
            columns.push_back(column_int32->get_ptr());
            DataTypeSerDeSPtrs serdes = {dt_int32->get_serde()};
            assert_callback(columns, serdes,
                            test_result_dir + "/column_int32_" + function_name + ".out");
        }
        {
            MutableColumns columns;
            columns.push_back(column_int64->get_ptr());
            DataTypeSerDeSPtrs serdes = {dt_int64->get_serde()};
            assert_callback(columns, serdes,
                            test_result_dir + "/column_int64_" + function_name + ".out");
        }
        {
            MutableColumns columns;
            columns.push_back(column_int128->get_ptr());
            DataTypeSerDeSPtrs serdes = {dt_int128->get_serde()};
            assert_callback(columns, serdes,
                            test_result_dir + "/column_int128_" + function_name + ".out");
        }
    }

    template <typename T>
    void _column_vector_common_test(const std::string& function_name, T callback) {
        {
            MutableColumns columns;
            columns.push_back(column_int8->get_ptr());
            callback((Int8)0, columns);
        }
        {
            MutableColumns columns;
            columns.push_back(column_int16->get_ptr());
            DataTypeSerDeSPtrs serdes = {dt_int16->get_serde()};
            callback((Int16)0, columns);
        }
        {
            MutableColumns columns;
            columns.push_back(column_int32->get_ptr());
            DataTypeSerDeSPtrs serdes = {dt_int32->get_serde()};
            callback((Int32)0, columns);
        }
        {
            MutableColumns columns;
            columns.push_back(column_int64->get_ptr());
            DataTypeSerDeSPtrs serdes = {dt_int64->get_serde()};
            callback((Int64)0, columns);
        }
        {
            MutableColumns columns;
            columns.push_back(column_int128->get_ptr());
            DataTypeSerDeSPtrs serdes = {dt_int128->get_serde()};
            callback((Int128)0, columns);
        }
        {
            MutableColumns columns;
            columns.push_back(column_uint8->get_ptr());
            DataTypeSerDeSPtrs serdes = {dt_uint8->get_serde()};
            callback((UInt8)0, columns);
        }
        {
            MutableColumns columns;
            columns.push_back(column_uint16->get_ptr());
            DataTypeSerDeSPtrs serdes = {dt_uint16->get_serde()};
            callback((UInt16)0, columns);
        }
        {
            MutableColumns columns;
            columns.push_back(column_uint32->get_ptr());
            DataTypeSerDeSPtrs serdes = {dt_uint32->get_serde()};
            callback((UInt32)0, columns);
        }
        {
            MutableColumns columns;
            columns.push_back(column_uint64->get_ptr());
            DataTypeSerDeSPtrs serdes = {dt_uint64->get_serde()};
            callback((UInt64)0, columns);
        }
    }
};

TEST_F(ColumnVectorTest, get_data_at_test) {
    _column_vector_common_test("get_data_at", assert_column_vector_get_data_at_callback);
}

TEST_F(ColumnVectorTest, field_test) {
    _column_vector_common_test("field", assert_column_vector_field_callback);
}
TEST_F(ColumnVectorTest, insert_from_test) {
    // insert from data csv and assert insert result
    _column_vector_common_test("insert_from", assert_column_vector_insert_from_callback);
}
TEST_F(ColumnVectorTest, insert_data_test) {
    _column_vector_common_test("insert_data", assert_column_vector_insert_data_callback);
}
TEST_F(ColumnVectorTest, insert_many_vals_test) {
    _column_vector_common_test("insert_many_vals", assert_column_vector_insert_many_vals_callback);
}
TEST_F(ColumnVectorTest, insert_many_from_test) {
    _column_vector_common_test("insert_many_from", assert_column_vector_insert_many_from_callback);
}
TEST_F(ColumnVectorTest, insert_range_of_integer_test) {
    _column_vector_common_test("insert_range_of_integer",
                               assert_column_vector_insert_range_of_integer_callback);
}
// void insert_date_column(const char* data_ptr, size_t num) {
// decimal, vector, nullable, PredicateColumnType
TEST_F(ColumnVectorTest, insert_many_fix_len_data_test) {
    _column_vector_common_test("insert_many_fix_len_data",
                               assert_column_vector_insert_many_fix_len_data_callback);
}
TEST_F(ColumnVectorTest, insert_many_raw_data_test) {
    _column_vector_common_test("insert_many_raw_data",
                               assert_column_vector_insert_many_raw_data_callback);
}
TEST_F(ColumnVectorTest, insert_default_test) {
    _column_vector_common_test("insert_default", assert_column_vector_insert_default_callback);
}
TEST_F(ColumnVectorTest, insert_many_defaults_test) {
    _column_vector_common_test("insert_many_defaults",
                               assert_column_vector_insert_many_defaults_callback);
}
TEST_F(ColumnVectorTest, ser_deser_test) {
    {
        MutableColumns columns;
        columns.push_back(column_int8->get_ptr());
        ser_deserialize_with_arena_impl(columns, {dt_int8});
    }
    {
        MutableColumns columns;
        columns.push_back(column_int16->get_ptr());
        ser_deserialize_with_arena_impl(columns, {dt_int16});
    }
    {
        MutableColumns columns;
        columns.push_back(column_int32->get_ptr());
        ser_deserialize_with_arena_impl(columns, {dt_int32});
    }
    {
        MutableColumns columns;
        columns.push_back(column_int64->get_ptr());
        ser_deserialize_with_arena_impl(columns, {dt_int64});
    }
    {
        MutableColumns columns;
        columns.push_back(column_int128->get_ptr());
        ser_deserialize_with_arena_impl(columns, {dt_int128});
    }
}
TEST_F(ColumnVectorTest, ser_deser_vec_test) {
    {
        MutableColumns columns;
        columns.push_back(column_int8->get_ptr());
        ser_deser_vec(columns, {dt_int8});
    }
    {
        MutableColumns columns;
        columns.push_back(column_int16->get_ptr());
        ser_deser_vec(columns, {dt_int16});
    }
    {
        MutableColumns columns;
        columns.push_back(column_int32->get_ptr());
        ser_deser_vec(columns, {dt_int32});
    }
    {
        MutableColumns columns;
        columns.push_back(column_int64->get_ptr());
        ser_deser_vec(columns, {dt_int64});
    }
    {
        MutableColumns columns;
        columns.push_back(column_int128->get_ptr());
        ser_deser_vec(columns, {dt_int128});
    }
}
TEST_F(ColumnVectorTest, update_xxHash_with_value_test) {
    hash_common_test("update_xxHash_with_value", assert_update_xxHash_with_value_callback);
}
TEST_F(ColumnVectorTest, update_crc_with_value_test) {
    hash_common_test("update_crc_with_value", assert_update_crc_with_value_callback);
}
TEST_F(ColumnVectorTest, update_sip_hash_with_value_test) {
    hash_common_test("update_sip_hash_with_value", assert_update_siphashes_with_value_callback);
}
TEST_F(ColumnVectorTest, update_hashes_with_value_test) {
    hash_common_test("update_hashes_with_value", assert_update_hashes_with_value_callback);
}
TEST_F(ColumnVectorTest, update_crcs_with_value_test) {
    std::string function_name = "update_crcs_with_value";
    {
        MutableColumns columns;
        columns.push_back(column_int8->get_ptr());
        DataTypeSerDeSPtrs serdes = {dt_int8->get_serde()};
        std::vector<PrimitiveType> pts(columns.size(), PrimitiveType::TYPE_TINYINT);
        assert_update_crc_hashes_callback(
                columns, serdes, pts, test_result_dir + "/column_int8_" + function_name + ".out");
    }
    {
        MutableColumns columns;
        columns.push_back(column_int16->get_ptr());
        DataTypeSerDeSPtrs serdes = {dt_int16->get_serde()};
        std::vector<PrimitiveType> pts(columns.size(), PrimitiveType::TYPE_SMALLINT);
        assert_update_crc_hashes_callback(
                columns, serdes, pts, test_result_dir + "/column_int16_" + function_name + ".out");
    }
    {
        MutableColumns columns;
        columns.push_back(column_int32->get_ptr());
        DataTypeSerDeSPtrs serdes = {dt_int32->get_serde()};
        std::vector<PrimitiveType> pts(columns.size(), PrimitiveType::TYPE_INT);
        assert_update_crc_hashes_callback(
                columns, serdes, pts, test_result_dir + "/column_int32_" + function_name + ".out");
    }
    {
        MutableColumns columns;
        columns.push_back(column_int64->get_ptr());
        DataTypeSerDeSPtrs serdes = {dt_int64->get_serde()};
        std::vector<PrimitiveType> pts(columns.size(), PrimitiveType::TYPE_BIGINT);
        assert_update_crc_hashes_callback(
                columns, serdes, pts, test_result_dir + "/column_int64_" + function_name + ".out");
    }
    {
        MutableColumns columns;
        columns.push_back(column_int128->get_ptr());
        DataTypeSerDeSPtrs serdes = {dt_int128->get_serde()};
        std::vector<PrimitiveType> pts(columns.size(), PrimitiveType::TYPE_LARGEINT);
        assert_update_crc_hashes_callback(
                columns, serdes, pts, test_result_dir + "/column_int128_" + function_name + ".out");
    }
}
template <typename T>
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

TEST_F(ColumnVectorTest, insert_value_test) {
    insert_value_test(column_int8.get());
    insert_value_test(column_int16.get());
    insert_value_test(column_int32.get());
    insert_value_test(column_int64.get());
    insert_value_test(column_int128.get());

    insert_value_test(column_uint8.get());
    insert_value_test(column_uint16.get());
    insert_value_test(column_uint32.get());
    insert_value_test(column_uint64.get());
}

TEST_F(ColumnVectorTest, get_bool_test) {
    _column_vector_common_test("get_bool", assert_column_vector_get_bool_callback);
}
TEST_F(ColumnVectorTest, get_int64_test) {
    _column_vector_common_test("get_int", assert_column_vector_get_int64_callback);
}
TEST_F(ColumnVectorTest, insert_range_from_test) {
    _column_vector_common_test("insert_range_from",
                               assert_column_vector_insert_range_from_callback);
}
TEST_F(ColumnVectorTest, insert_indices_from_test) {
    _column_vector_common_test("insert_indices_from",
                               assert_column_vector_insert_indices_from_callback);
}

TEST_F(ColumnVectorTest, pop_back_test) {
    _column_vector_common_test("pop_back", assert_column_vector_pop_back_callback);
}

TEST_F(ColumnVectorTest, filter_test) {
    _column_vector_common_test("filter", assert_column_vector_filter_callback);
}
TEST_F(ColumnVectorTest, get_permutation_test) {
    assert_column_permutations(*column_int8, dt_int8);
    assert_column_permutations(*column_int16, dt_int16);
    assert_column_permutations(*column_int32, dt_int32);
    assert_column_permutations(*column_int64, dt_int64);
    assert_column_permutations(*column_int128, dt_int128);
}

TEST_F(ColumnVectorTest, permute_test) {
    MutableColumns columns;
    columns.push_back(column_int8->get_ptr());
    columns.push_back(column_int16->get_ptr());
    columns.push_back(column_int32->get_ptr());
    columns.push_back(column_int64->get_ptr());
    columns.push_back(column_int128->get_ptr());
    assert_permute(columns, 0);
    assert_permute(columns, 1);
    assert_permute(columns, column_int8->size());
    assert_permute(columns, UINT64_MAX);
}

TEST_F(ColumnVectorTest, replicate_test) {
    _column_vector_common_test("replicate", assert_column_vector_replicate_callback);
}

TEST_F(ColumnVectorTest, replace_column_data_test) {
    _column_vector_common_test("replace_column_data",
                               assert_column_vector_replace_column_data_callback);
}

TEST_F(ColumnVectorTest, replace_column_null_data_test) {
    _column_vector_common_test("replace_column_null_data",
                               assert_column_vector_replace_column_null_data_callback);
}

} // namespace doris::vectorized