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

#include <gmock/gmock-more-matchers.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>

#include "testutil/test_util.h"
#include "testutil/variant_util.h"
#include "vec/columns/column_object.cpp"
#include "vec/columns/column_object.h"
#include "vec/columns/common_column_test.h"
#include "vec/columns/subcolumn_tree.h"
#include "vec/common/schema_util.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_factory.hpp"

using namespace doris;
namespace doris::vectorized {
static std::string root_dir;
static std::string test_data_dir;
static std::string test_result_dir;
static std::string test_data_dir_json;
static DataTypePtr dt_variant =
        DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_VARIANT, 0, 0);

DataTypeSerDeSPtrs serde;

static ColumnObject::MutablePtr column_variant;

class ColumnObjectTest : public CommonColumnTest {
protected:
    static void SetUpTestSuite() {
        column_variant = VariantUtil::construct_advanced_varint_column();
        std::cout << column_variant->get_name() << std::endl;
        root_dir = std::string(getenv("DORIS_HOME"));
        // which is /root/doris/be/ut_build_ASAN/test//
        std::cout << "root_dir: " << root_dir << std::endl;
        test_data_dir = root_dir + "../../../be/test/data/vec/columns";
        test_result_dir = root_dir + "../../../be/test/expected_result/vec/columns";
        //load_json_columns_data();
    }

    static void load_json_columns_data() {
        std::cout << "loading json dataset : " << FLAGS_gen_out << std::endl;
        {
            MutableColumns columns;
            columns.push_back(column_variant->get_ptr());
            serde = {dt_variant->get_serde()};
            test_data_dir_json = root_dir + "/regression-test/data/nereids_function_p0/";
            std::vector<string> json_files = {
                    test_data_dir_json + "json_variant/boolean_boundary.jsonl",
                    test_data_dir_json + "json_variant/null_boundary.jsonl",
                    test_data_dir_json + "json_variant/number_boundary.jsonl",
                    test_data_dir_json + "json_variant/string_boundary.jsonl",
                    test_data_dir_json + "json_variant/array_boolean_boundary.jsonl",
                    test_data_dir_json + "json_variant/array_nullable_null_boundary.jsonl",
                    test_data_dir_json + "json_variant/array_number_boundary.jsonl",
                    test_data_dir_json + "json_variant/array_string_boundary.jsonl",
                    test_data_dir_json + "json_variant/array_object_boundary.jsonl",
                    test_data_dir_json + "json_variant/array_nullable_boolean_boundary.jsonl",
                    test_data_dir_json + "json_variant/array_nullable_number_boundary.jsonl",
                    test_data_dir_json + "json_variant/array_nullable_string_boundary.jsonl",
                    test_data_dir_json + "json_variant/array_nullable_object_boundary.jsonl",
                    test_data_dir_json + "json_variant/array_array_boolean_boundary.jsonl",
                    test_data_dir_json + "json_variant/array_array_number_boundary.jsonl",
                    test_data_dir_json +
                            "json_variant/array_nullable_array_nullable_boolean_boundary.jsonl",
                    test_data_dir_json +
                            "json_variant/array_nullable_array_nullable_null_boundary.jsonl",
                    test_data_dir_json +
                            "json_variant/array_nullable_array_nullable_number_boundary.jsonl",
                    test_data_dir_json + "json_variant/object_boundary.jsonl",
                    test_data_dir_json + "json_variant/object_nested_1025.jsonl"};

            for (const auto& json_file : json_files) {
                load_columns_data_from_file(columns, serde, '\n', {0}, json_file);
                EXPECT_TRUE(!column_variant->empty());
                column_variant->insert_default();
                std::cout << "column variant size: " << column_variant->size() << std::endl;
            }
            column_variant->finalize();
            std::cout << "column variant finalize size: " << column_variant->size() << std::endl;
        }
    }

    template <typename T>
    void column_common_test(T callback) {
        callback(ColumnObject(true), column_variant->get_ptr());
    }

    void hash_common_test(
            const std::string& function_name,
            std::function<void(const MutableColumns& load_cols, DataTypeSerDeSPtrs serders,
                               const std::string& res_file_name)>
                    assert_callback) {
        {
            MutableColumns columns;
            columns.push_back(column_variant->get_ptr());
            DataTypeSerDeSPtrs serdes = {dt_variant->get_serde()};
            assert_callback(columns, serdes,
                            test_result_dir + "/column_variant_" + function_name + ".out");
        }
    }
};

TEST_F(ColumnObjectTest, is_variable_length) {
    EXPECT_TRUE(column_variant->is_variable_length());
}

TEST_F(ColumnObjectTest, byte_size) {
    hash_common_test("byte_size", assert_byte_size_with_file_callback);
}

//TEST_F(ColumnObjectTest, has_enough_capacity) {
//    auto test_func = [](const auto& src_col) {
//        auto src_size = src_col->size();
//        // variant always return fasle
//        auto assert_col = src_col->clone_empty();
//        ASSERT_FALSE(assert_col->has_enough_capacity(*src_col));
//        assert_col->reserve(src_size);
//        ASSERT_FALSE(assert_col->has_enough_capacity(*src_col));
//    };
//    test_func(column_variant);
//}

TEST_F(ColumnObjectTest, allocated_bytes) {
    hash_common_test("allocated_bytes", assert_allocated_bytes_with_file_callback);
}

TEST_F(ColumnObjectTest, clone_resized) {
    auto src_size = column_variant->size();
    auto test_func = [&](size_t clone_count) {
        auto target_column = column_variant->clone_resized(clone_count);
        EXPECT_NE(target_column.get(), column_variant.get());
        EXPECT_EQ(target_column->size(), clone_count);
        size_t same_count = std::min(clone_count, src_size);
        size_t i = 0;
        for (; i < same_count; ++i) {
            checkField(*target_column, *column_variant, i, i);
        }
        for (; i < clone_count; ++i) {
            // more than source size
            Field target_field;
            Field source_field = column_variant->get_root_type()->get_default();
            target_column->get(i, target_field);
            EXPECT_EQ(target_field, source_field)
                    << "target_field: " << target_field.get_type_name()
                    << ", source_field: " << source_field.get_type_name();
        }
    };
    test_func(0);
    test_func(3);
    test_func(src_size);
    test_func(src_size + 10);
    // test clone_empty

    auto target_column = column_variant->clone_empty();
    EXPECT_NE(target_column.get(), column_variant.get());
    // assert subcolumns
    auto target_subcolumns = assert_cast<ColumnObject*>(target_column.get())->get_subcolumns();
    // always has root for ColumnObject(0)
    EXPECT_EQ(target_subcolumns.size(), 1);
}
TEST_F(ColumnObjectTest, field_test) {
    auto test_func = [](const auto& source_column) {
        auto src_size = source_column->size();
        {
            auto assert_col = source_column->clone();
            for (size_t i = 0; i != src_size; ++i) {
                Field f;
                source_column->get(i, f);
                assert_col->insert(f);
            }
            for (size_t i = 0; i != src_size; ++i) {
                Field assert_field;
                assert_col->get(i, assert_field);
                Field source_field;
                source_column->get(i, source_field);
                ASSERT_EQ(assert_field, source_field);
            }
        }
        {
            auto assert_col = source_column->clone();
            std::cout << source_column->size() << std::endl;
            for (size_t i = 0; i != src_size; ++i) {
                VariantMap jsonbf;
                Field f(std::move(jsonbf));
                source_column->get(i, f);
                assert_col->insert(f);
            }
            for (size_t i = 0; i != src_size; ++i) {
                VariantMap jsonbf;
                Field f(std::move(jsonbf));
                assert_col->get(i, f);
                const auto& real_field = vectorized::get<const VariantMap&>(f);
                Field source_field;
                source_column->get(i, source_field);
                ASSERT_EQ(real_field, source_field);
            }
        }
    };
    ColumnObject::MutablePtr obj;
    obj = VariantUtil::construct_advanced_varint_column();
    EXPECT_TRUE(!obj->empty());
    test_func(obj);
}

// is seri
TEST_F(ColumnObjectTest, is_column_string64) {
    EXPECT_FALSE(column_variant->is_column_string64());
}

TEST_F(ColumnObjectTest, is_column_string) {
    EXPECT_FALSE(column_variant->is_column_string());
}

TEST_F(ColumnObjectTest, serialize_one_row_to_string) {
    {
        const auto* variant = assert_cast<const ColumnObject*>(column_variant.get());
        // Serialize hierarchy types to json format
        std::string buffer;
        for (size_t row_idx = 2000; row_idx < variant->size(); ++row_idx) {
            variant->serialize_one_row_to_string(row_idx, &buffer);
        }
        {
            // TEST buffer
            auto tmp_col = ColumnString::create();
            VectorBufferWriter write_buffer(*tmp_col.get());
            for (size_t row_idx = 2000; row_idx < variant->size(); ++row_idx) {
                variant->serialize_one_row_to_string(row_idx, write_buffer);
            }
        }
    }
    {
        // TEST SCALA_VARAINT
        // 1. create an empty variant column
        auto v = ColumnObject::create(true);
        auto dt = DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_STRING, 0,
                                                               0);
        auto cs = dt->create_column();
        cs->insert(Field("amory"));
        cs->insert(Field("doris"));
        v->create_root(dt, std::move(cs));
        EXPECT_TRUE(v->is_scalar_variant());

        // 3. serialize
        std::string buf2;
        for (size_t row_idx = 0; row_idx < v->size(); ++row_idx) {
            v->serialize_one_row_to_string(row_idx, &buf2);
        }
        auto tmp_col = ColumnString::create();
        VectorBufferWriter write_buffer(*tmp_col.get());
        for (size_t row_idx = 0; row_idx < v->size(); ++row_idx) {
            v->serialize_one_row_to_string(row_idx, write_buffer);
        }
    }
}
// insert interface
// not implemented: insert_many_fix_len_data, insert_many_dict_data, insert_many_continuous_binary_data, insert_from_multi_column
// insert_many_strings, insert_many_strings_overflow, insert_range_from_ignore_overflow, insert_many_raw_data, insert_data, get_data_at, replace_column_data,
// serialize_value_into_arena, deserialize_and_insert_from_arena
TEST_F(ColumnObjectTest, insert_many_fix_len_data) {
    EXPECT_ANY_THROW(column_variant->insert_many_fix_len_data(nullptr, 0));
}

TEST_F(ColumnObjectTest, insert_many_dict_data) {
    EXPECT_ANY_THROW(column_variant->insert_many_dict_data(nullptr, 0, nullptr, 0, 0));
}

TEST_F(ColumnObjectTest, insert_many_continuous_binary_data) {
    EXPECT_ANY_THROW(column_variant->insert_many_continuous_binary_data(nullptr, 0, 0));
}

//TEST_F(ColumnObjectTest, insert_from_multi_column) {
//    EXPECT_ANY_THROW(column_variant->insert_from_multi_column({column_variant.get()}, {0}));
//}

TEST_F(ColumnObjectTest, insert_many_strings) {
    EXPECT_ANY_THROW(column_variant->insert_many_strings(nullptr, 0));
}

TEST_F(ColumnObjectTest, insert_many_strings_overflow) {
    EXPECT_ANY_THROW(column_variant->insert_many_strings_overflow(nullptr, 0, 0));
}

TEST_F(ColumnObjectTest, insert_many_raw_data) {
    EXPECT_ANY_THROW(column_variant->insert_many_raw_data(nullptr, 0));
}

TEST_F(ColumnObjectTest, insert_data) {
    EXPECT_ANY_THROW(column_variant->insert_data(nullptr, 0));
}

TEST_F(ColumnObjectTest, get_data_at) {
    EXPECT_ANY_THROW(column_variant->get_data_at(0));
}

TEST_F(ColumnObjectTest, replace_column_data) {
    EXPECT_ANY_THROW(
            column_variant->replace_column_data(column_variant->assume_mutable_ref(), 0, 0));
}

TEST_F(ColumnObjectTest, serialize_value_into_arena) {
    Arena a;
    const char* begin = nullptr;
    EXPECT_ANY_THROW(column_variant->serialize_value_into_arena(0, a, begin));
}

TEST_F(ColumnObjectTest, deserialize_and_insert_from_arena) {
    EXPECT_ANY_THROW(column_variant->deserialize_and_insert_from_arena(nullptr));
}

// insert series:
// insert_from, insert_many_from, insert_range_from, insert_range_from_ignore_overflow, insert_indices_from
// insert_default, insert_many_defaults
TEST_F(ColumnObjectTest, insert_many_from) {
    assert_insert_many_from_with_field_callback(column_variant->get_ptr());
}

TEST_F(ColumnObjectTest, insert_from) {
    assert_insert_from_with_field_callback(column_variant->get_ptr());
}

TEST_F(ColumnObjectTest, insert_range_from) {
    // insert_range_from_ignore_overflow call insert_range_from
    assert_insert_range_from_with_field_callback(column_variant->get_ptr());
}

TEST_F(ColumnObjectTest, insert_indices_from) {
    assert_insert_indices_from_with_field_callback(column_variant->get_ptr());
}

TEST_F(ColumnObjectTest, insert_default_insert_many_defaults) {
    assert_insert_default_with_field_callback(column_variant->get_ptr());
}

TEST_F(ColumnObjectTest, get_name) {
    EXPECT_TRUE(column_variant->get_name().find("variant") != std::string::npos);
}

// pop_back interface
TEST_F(ColumnObjectTest, pop_back_test) {
    assert_pop_back_with_field_callback(column_variant->get_ptr());
}

// serialize and deserialize is not implemented
// serialize_vec, deserialize_vec, serialize_vec_with_null_map, deserialize_vec_with_null_map, get_max_row_byte_size
TEST_F(ColumnObjectTest, ser_deser_test) {
    std::vector<StringRef> keys;
    EXPECT_ANY_THROW(column_variant->get_max_row_byte_size());
    EXPECT_ANY_THROW(column_variant->serialize_vec(keys, 0, 0));
    EXPECT_ANY_THROW(column_variant->deserialize_vec(keys, 0));
    EXPECT_ANY_THROW(column_variant->serialize_vec_with_null_map(keys, 0, nullptr));
    EXPECT_ANY_THROW(column_variant->deserialize_vec_with_null_map(keys, 0, nullptr));
}

// hash interface
TEST_F(ColumnObjectTest, update_xxHash_with_value) {
    hash_common_test("update_xxHash_with_value", assert_update_xxHash_with_value_callback);
}

// hang
//TEST_F(ColumnObjectTest, update_sip_hash_with_value_test) {
//    hash_common_test("update_sip_hash_with_value",
//                     assert_column_vector_update_siphashes_with_value_callback);
//}
TEST_F(ColumnObjectTest, update_hashes_with_value_test) {
    hash_common_test("update_hashes_with_value",
                     assert_column_vector_update_hashes_with_value_callback);
}
TEST_F(ColumnObjectTest, update_crc_with_value_test) {
    hash_common_test("update_crc_with_value", assert_update_crc_with_value_callback);
}
TEST_F(ColumnObjectTest, update_crcs_with_value_test) {
    std::string function_name = "update_crcs_with_value";
    MutableColumns columns;
    columns.push_back(column_variant->get_ptr());
    DataTypeSerDeSPtrs serdes = {dt_variant->get_serde()};
    std::vector<PrimitiveType> pts(columns.size(), PrimitiveType::TYPE_VARIANT);
    assert_column_vector_update_crc_hashes_callback(
            columns, serdes, pts, test_result_dir + "/column_variant_" + function_name + ".out");
}

// filter interface
TEST_F(ColumnObjectTest, filter) {
    assert_filter_with_field_callback(column_variant->get_ptr());
}

TEST_F(ColumnObjectTest, filter_by_selector) {
    auto test_func = [&](const auto& source_column) {
        auto src_size = source_column->size();
        EXPECT_TRUE(src_size <= UINT16_MAX);

        auto target_column = source_column->clone_empty();

        std::vector<uint16_t> indices(src_size);
        std::iota(indices.begin(), indices.end(), 0);
        std::random_device rd;
        std::mt19937 g(rd());
        std::shuffle(indices.begin(), indices.end(), g);
        size_t sel_size = src_size / 2;
        indices.resize(sel_size);
        std::sort(indices.begin(), indices.end());

        EXPECT_ANY_THROW(Status st = source_column->filter_by_selector(indices.data(), 0,
                                                                       target_column.get()));
    };
    test_func(column_variant);
}
TEST_F(ColumnObjectTest, permute) {
    {
        // test empty column and limit == 0
        IColumn::Permutation permutation(0);
        auto col = column_variant->clone_empty();
        col->permute(permutation, 0);
        EXPECT_EQ(col->size(), 0);
    }
    {
        IColumn::Permutation permutation(0);
        EXPECT_THROW(column_variant->permute(permutation, 10), Exception);
    }

    MutableColumns columns;
    columns.push_back(column_variant->get_ptr());
    assert_column_vector_permute(columns, 0, false);
    assert_column_vector_permute(columns, 1, false);
    assert_column_vector_permute(columns, column_variant->size(), false);
    assert_column_vector_permute(columns, UINT64_MAX, false);
}

// not support
TEST_F(ColumnObjectTest, get_permutation) {
    EXPECT_ANY_THROW(assert_column_permutations2(*column_variant, dt_variant));
}
TEST_F(ColumnObjectTest, structure_equals) {
    auto cl = column_variant->clone_empty();
    EXPECT_ANY_THROW(column_variant->structure_equals(*cl));
}

TEST_F(ColumnObjectTest, replicate) {
    assert_replicate_with_field(column_variant->get_ptr());
}

// Compare Interface not implement: compare_at, compare_internal
TEST_F(ColumnObjectTest, compare_at) {
    EXPECT_ANY_THROW(column_variant->compare_at(0, 0, *column_variant, -1));
    std::vector<uint8> com_res(column_variant->size());
    EXPECT_ANY_THROW(column_variant->compare_internal(0, *column_variant, 0, 0, com_res, nullptr));
}

TEST_F(ColumnObjectTest, clear) {
    auto tmp_col = column_variant->clone();
    EXPECT_EQ(tmp_col->size(), column_variant->size());

    tmp_col->clear();
    EXPECT_EQ(tmp_col->size(), 0);
}

TEST_F(ColumnObjectTest, convert_column_if_overflow) {
    // convert_column_if_overflow may need impl in ColumnObject, like ColumnArray?
    auto ret = column_variant->convert_column_if_overflow();
    EXPECT_EQ(ret.get(), column_variant.get());
}

TEST_F(ColumnObjectTest, clone_finalized) {
    auto test_func = [](const auto& source_column) {
        auto src_size = source_column->size();
        EXPECT_TRUE(src_size > 0);

        // Store original data for comparison
        auto original_subcolumns = source_column->get_subcolumns();

        // Test clone_finalized
        auto cloned = source_column->clone_finalized();
        EXPECT_TRUE(cloned.get() != nullptr);
        EXPECT_EQ(cloned->size(), src_size);

        // Verify cloned column has same subcolumns
        auto cloned_subcolumns = assert_cast<ColumnObject*>(cloned.get())->get_subcolumns();
        EXPECT_EQ(cloned_subcolumns.size(), original_subcolumns.size());

        // Verify data integrity
        for (size_t i = 0; i < src_size; ++i) {
            Field original_field, cloned_field;
            source_column->get(i, original_field);
            cloned->get(i, cloned_field);
            EXPECT_EQ(original_field, cloned_field);
        }
    };
    auto temp = column_variant->clone();
    auto cloned_object = assert_cast<ColumnObject*>(temp.get());
    test_func(std::move(cloned_object));
}

TEST_F(ColumnObjectTest, resize) {
    auto test_func = [](const auto& source_column, size_t add_count) {
        {
            auto source_size = source_column->size();
            auto tmp_col = source_column->clone();
            auto default_col = source_column->clone_empty();
            default_col->insert_default();
            tmp_col->resize(source_size + add_count);
            EXPECT_EQ(tmp_col->size(), source_size + add_count);
            for (size_t i = 0; i != source_size; ++i) {
                checkField(*tmp_col, *source_column, i, i);
            }
            for (size_t i = 0; i != add_count; ++i) {
                checkField(*tmp_col, *default_col, source_size + i, 0);
            }
        }
        {
            // resize in self
            auto ptr = source_column.get();
            source_column->resize(add_count);
            EXPECT_EQ(source_column.get(), ptr);
            EXPECT_EQ(source_column->size(), add_count);
        }
    };
    test_func(column_variant, 0);
    test_func(column_variant, 10);
}

// ================= variant specific interface =================
// meta info related interface
TEST_F(ColumnObjectTest, get_least_common_type) {
    auto test_func = [](const auto& source_column) {
        auto src_size = source_column->size();
        EXPECT_TRUE(src_size > 0);

        // Test get_least_common_type for root column
        const auto& root = source_column->get_subcolumns().get_root();
        EXPECT_TRUE(root != nullptr);
        EXPECT_TRUE(root->data.get_least_common_type() != nullptr);

        // Test get_least_common_type for subcolumns
        for (const auto& subcolumn : source_column->get_subcolumns()) {
            EXPECT_TRUE(subcolumn != nullptr);
            EXPECT_TRUE(subcolumn->data.get_least_common_type() != nullptr);
        }
    };
    test_func(column_variant);
}

TEST_F(ColumnObjectTest, get_dimensions) {
    auto test_func = [](const auto& source_column) {
        auto src_size = source_column->size();
        EXPECT_TRUE(src_size > 0);

        // Test get_dimensions for root column
        const auto& root = source_column->get_subcolumns().get_root();
        EXPECT_TRUE(root != nullptr);
        EXPECT_GE(root->data.get_dimensions(), 0);

        // Test get_dimensions for subcolumns
        for (auto& entry : source_column->get_subcolumns()) {
            EXPECT_TRUE(entry != nullptr);
            EXPECT_GE(entry->data.get_dimensions(), 0);
        }
    };
    test_func(column_variant);
}

TEST_F(ColumnObjectTest, get_last_field) {
    auto test_func = [](const auto& source_column) {
        auto src_size = source_column->size();
        EXPECT_TRUE(src_size > 0);

        // Test get_last_field for root column
        const auto& root = source_column->get_subcolumns().get_root();
        EXPECT_TRUE(root != nullptr);
        Field last_field;
        root->data.get_last_field();

        // Test get_last_field for subcolumns
        for (const auto& subcolumn : source_column->get_subcolumns()) {
            EXPECT_TRUE(subcolumn != nullptr);
            subcolumn->data.get_last_field();
        }
    };
    test_func(column_variant);
}

// sub column op related interface
TEST_F(ColumnObjectTest, get_finalized_column) {
    auto test_func = [](const auto& source_column) {
        // do not clone and then get , will case heap-after-use-free cause of defined in COW as temporary Ptr
        // auto source_column = assert_cast<ColumnObject*>(var_column->clone_resized(var_column->size()).get());
        auto src_size = source_column->size();
        EXPECT_TRUE(src_size > 0);
        // Test get_finalized_column for root column
        auto root = source_column->get_subcolumns().get_root();
        EXPECT_TRUE(root != nullptr);
        source_column->finalize();
        root = source_column->get_subcolumns().get_root();
        const auto& finalized_col = root->data.get_finalized_column();
        EXPECT_TRUE(source_column->is_finalized());
        Field rf;
        finalized_col.get(0, rf);
        EXPECT_TRUE(strlen(rf.get_type_name()) > 0);

        // Test get_finalized_column for subcolumns
        for (const auto& subcolumn : source_column->get_subcolumns()) {
            EXPECT_TRUE(subcolumn != nullptr);
            EXPECT_TRUE(subcolumn->data.is_finalized());
            const auto& subcolumn_finalized = subcolumn->data.get_finalized_column();

            // Verify finalized column data
            Field field;
            subcolumn_finalized.get(0, field);
            EXPECT_TRUE(strlen(field.get_type_name()) > 0);
            // Verify column size
            EXPECT_EQ(subcolumn_finalized.size(), src_size);
        }
    };

    auto temp = column_variant->clone();
    auto cloned_object = assert_cast<ColumnObject*>(temp.get());
    EXPECT_NE(cloned_object, column_variant.get());
    test_func(std::move(cloned_object));
}

TEST_F(ColumnObjectTest, get_finalized_column_ptr) {
    auto test_func = [](const auto& source_column) {
        auto src_size = source_column->size();
        EXPECT_TRUE(src_size > 0);
        // Test get_finalized_column_ptr for root column
        auto root = source_column->get_subcolumns().get_root();
        EXPECT_TRUE(root != nullptr);
        source_column->finalize();
        // when finalized , the root will be changed
        root = source_column->get_subcolumns().get_root();
        const auto& finalized_col_ptr = root->data.get_finalized_column_ptr();
        EXPECT_TRUE(finalized_col_ptr.get() != nullptr);

        // Test get_finalized_column_ptr for subcolumns
        for (const auto& subcolumn : source_column->get_subcolumns()) {
            EXPECT_TRUE(subcolumn != nullptr);
            const auto& subcolumn_finalized_ptr = subcolumn->data.get_finalized_column_ptr();
            EXPECT_TRUE(subcolumn_finalized_ptr.get() != nullptr);
            EXPECT_TRUE(subcolumn->data.is_finalized());

            // Verify finalized column data
            Field field;
            subcolumn_finalized_ptr->get(0, field);
            EXPECT_TRUE(strlen(field.get_type_name()) > 0);
            // Verify column size
            EXPECT_EQ(subcolumn_finalized_ptr->size(), src_size);
        }
    };
    auto temp = column_variant->clone();
    auto cloned_object = assert_cast<ColumnObject*>(temp.get());
    test_func(std::move(cloned_object));
}

TEST_F(ColumnObjectTest, remove_nullable) {
    auto test_func = [](const auto& source_column) {
        auto src_size = source_column->size();
        EXPECT_TRUE(src_size > 0);

        // Test remove_nullable for subcolumns
        for (const auto& subcolumn : source_column->get_subcolumns()) {
            EXPECT_TRUE(subcolumn != nullptr);
            subcolumn->data.finalize();
            auto subcolumn_type_before = subcolumn->data.get_least_common_type();
            subcolumn->data.remove_nullable();
            auto subcolumn_type_after = subcolumn->data.get_least_common_type();
            EXPECT_TRUE(remove_nullable(subcolumn_type_before)->equals(*subcolumn_type_after));
        }
    };
    auto temp = column_variant->clone();
    auto cloned_object = assert_cast<ColumnObject*>(temp.get());
    test_func(std::move(cloned_object));
}

TEST_F(ColumnObjectTest, add_new_column_part) {
    auto test_func = [](const auto& source_column) {
        auto src_size = source_column->size();
        EXPECT_TRUE(src_size > 0);

        // Test add_new_column_part for subcolumns
        for (const auto& subcolumn : source_column->get_subcolumns()) {
            EXPECT_TRUE(subcolumn != nullptr);

            // Store original type before adding new part
            auto original_type = subcolumn->data.get_least_common_type();

            // The add_new_column_part interface must be added to the minimum common type of the data type vector in the current subcolumn,
            // otherwise an error will be reported: [E3] Not implemeted
            subcolumn->data.add_new_column_part(original_type);
            // Verify the type is updated
            auto updated_type = subcolumn->data.get_least_common_type();
            EXPECT_TRUE(updated_type != nullptr);
            // Verify column size
            EXPECT_EQ(subcolumn->data.size(), src_size);
        }
    };
    auto temp = column_variant->clone();
    auto cloned_object = assert_cast<ColumnObject*>(temp.get());
    test_func(std::move(cloned_object));
}

TEST_F(ColumnObjectTest, get_subcolumn) {
    auto test_func = [](const auto& source_column) {
        auto src_size = source_column->size();
        EXPECT_TRUE(src_size > 0);

        // Test get_subcolumn
        for (const auto& subcolumn : source_column->get_subcolumns()) {
            EXPECT_TRUE(subcolumn != nullptr);

            // Verify subcolumn properties
            EXPECT_TRUE(subcolumn->data.get_least_common_type() != nullptr);
            EXPECT_GE(subcolumn->data.get_dimensions(), 0);

            // Verify subcolumn data
            Field field;
            subcolumn->data.get(0, field);
            EXPECT_TRUE(strlen(field.get_type_name()) > 0);
            EXPECT_EQ(subcolumn->data.size(), src_size);
        }
    };
    auto temp = column_variant->clone();
    auto cloned_object = assert_cast<ColumnObject*>(temp.get());
    test_func(std::move(cloned_object));
}

TEST_F(ColumnObjectTest, ensure_root_node_type) {
    ColumnObject::MutablePtr obj;
    obj = VariantUtil::construct_advanced_varint_column();
    EXPECT_TRUE(!obj->empty());
    // Store original root type
    auto root = obj->get_subcolumns().get_root();
    auto original_root_type = root->data.get_least_common_type();
    obj->finalize();

    // Test ensure_root_node_type
    auto new_type =
            DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_STRING, 0, 0);
    obj->ensure_root_node_type(new_type);

    // Verify root type is updated
    root = obj->get_subcolumns().get_root();
    auto updated_root_type = root->data.get_least_common_type();
    EXPECT_TRUE(updated_root_type->equals(*new_type));
};

TEST_F(ColumnObjectTest, create_root) {
    auto test_func = [](const auto& source_column) {
        auto src_size = source_column->size();
        EXPECT_TRUE(src_size > 0);

        // Test case 1: Create root with string type
        {
            auto col = source_column->clone_empty();
            auto obj = assert_cast<ColumnObject*>(col.get());
            auto type = DataTypeFactory::instance().create_data_type(
                    FieldType::OLAP_FIELD_TYPE_STRING, 0, 0);
            auto column = type->create_column();
            obj->create_root(type, std::move(column));

            // Verify root is created with correct type
            const auto& root = obj->get_subcolumns().get_root();
            EXPECT_TRUE(root != nullptr);
            EXPECT_TRUE(root->data.get_least_common_type()->equals(*type));
        }

        // Test case 2: Create root with int type
        {
            auto col = source_column->clone_empty();
            auto obj = assert_cast<ColumnObject*>(col.get());
            auto type = DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_INT,
                                                                     0, 0);
            auto column = type->create_column();
            obj->create_root(type, std::move(column));

            // Verify root is created with correct type
            const auto& root = obj->get_subcolumns().get_root();
            EXPECT_TRUE(root != nullptr);
            EXPECT_TRUE(root->data.get_least_common_type()->equals(*type));
        }

        // Test case 3: Create root on existing column
        {
            auto col = source_column->clone();
            auto obj = assert_cast<ColumnObject*>(col.get());
            auto original_root = obj->get_subcolumns().get_root();
            EXPECT_TRUE(original_root != nullptr);

            // Create root with new type
            auto type = DataTypeFactory::instance().create_data_type(
                    FieldType::OLAP_FIELD_TYPE_STRING, 0, 0);
            auto column = type->create_column();
            EXPECT_ANY_THROW(obj->create_root(type, std::move(column)));

            // Verify root is replaced with new type
            const auto& new_root = obj->get_subcolumns().get_root();
            EXPECT_TRUE(new_root != nullptr);
            EXPECT_EQ(new_root, original_root);
        }

        // Test case 4: Create root and verify data operations
        {
            auto col = source_column->clone_empty();
            auto obj = assert_cast<ColumnObject*>(col.get());
            auto type = DataTypeFactory::instance().create_data_type(
                    FieldType::OLAP_FIELD_TYPE_STRING, 0, 0);
            auto column = type->create_column();
            obj->create_root(type, std::move(column));

            // Insert some data
            Field field;
            source_column->get(0, field);
            obj->insert(field);

            // Verify data is inserted correctly
            Field inserted_field;
            obj->get(0, inserted_field);
        }

        // Test case 5: Create root with nullable type
        {
            auto col = source_column->clone_empty();
            auto obj = assert_cast<ColumnObject*>(col.get());
            auto type = DataTypeFactory::instance().create_data_type(
                    FieldType::OLAP_FIELD_TYPE_STRING, 0, 0);
            auto nullable_type = make_nullable(type);
            auto column = nullable_type->create_column();
            obj->create_root(nullable_type, std::move(column));

            // Verify root is created with nullable type
            const auto& root = obj->get_subcolumns().get_root();
            EXPECT_TRUE(root != nullptr);
            EXPECT_TRUE(root->data.get_least_common_type()->equals(*nullable_type));
        }
    };
    auto temp = column_variant->clone();
    auto cloned_object = assert_cast<ColumnObject*>(temp.get());
    test_func(std::move(cloned_object));
}
TEST_F(ColumnObjectTest, get_most_common_type) {
    auto test_func = [](const auto& source_column) {
        auto src_size = source_column->size();
        EXPECT_TRUE(src_size > 0);

        // Test get_most_common_type
        DataTypePtr most_common_type = source_column->get_most_common_type();
        EXPECT_TRUE(most_common_type != nullptr);
    };
    test_func(column_variant);
}

TEST_F(ColumnObjectTest, is_null_root) {
    auto test_func = [](const auto& source_column) {
        auto src_size = source_column->size();
        EXPECT_TRUE(src_size > 0);

        // Test is_null_root
        bool is_null = source_column->is_null_root();
        EXPECT_FALSE(is_null); // Since we have data, root should not be null
    };
    test_func(column_variant);
}

TEST_F(ColumnObjectTest, is_scalar_variant) {
    auto test_func = [](const auto& source_column) {
        auto src_size = source_column->size();
        EXPECT_TRUE(src_size > 0);

        // Test is_scalar_variant
        bool is_scalar = source_column->is_scalar_variant();
        // The result depends on the actual data structure
        EXPECT_FALSE(is_scalar);
    };
    test_func(column_variant);
}

TEST_F(ColumnObjectTest, is_exclusive) {
    auto test_func = [](const auto& source_column) {
        auto src_size = source_column->size();
        EXPECT_TRUE(src_size > 0);

        // Test is_exclusive
        bool is_exclusive = source_column->is_exclusive();
        // The result depends on the actual data structure
        EXPECT_TRUE(is_exclusive);
    };
    test_func(column_variant);
}

TEST_F(ColumnObjectTest, get_root_type) {
    auto test_func = [](const auto& source_column) {
        auto src_size = source_column->size();
        EXPECT_TRUE(src_size > 0);

        // Test get_root_type
        DataTypePtr root_type = source_column->get_root_type();
        EXPECT_TRUE(root_type != nullptr);
    };
    test_func(column_variant);
}

TEST_F(ColumnObjectTest, has_subcolumn) {
    auto test_func = [](const auto& source_column) {
        auto src_size = source_column->size();
        EXPECT_TRUE(src_size > 0);

        // Test has_subcolumn
        for (const auto& subcolumn : source_column->get_subcolumns()) {
            EXPECT_TRUE(subcolumn != nullptr);
            bool has_subcolumn = source_column->has_subcolumn(subcolumn->path);
            EXPECT_TRUE(has_subcolumn);
        }
    };
    test_func(column_variant);
}

TEST_F(ColumnObjectTest, finalize) {
    auto test_func = [](const auto& source_column) {
        auto src_size = source_column->size();
        EXPECT_TRUE(src_size > 0);

        // Test case 1: Test finalize with READ_MODE
        {
            auto col = source_column->clone_empty();
            auto obj = assert_cast<ColumnObject*>(col.get());

            // Insert data from source column
            for (size_t i = 0; i < src_size; ++i) {
                Field field;
                source_column->get(i, field);
                obj->insert(field);
            }

            // Verify initial state
            EXPECT_FALSE(obj->is_finalized());

            // Finalize in READ_MODE
            Status st = obj->finalize(ColumnObject::FinalizeMode::READ_MODE);
            EXPECT_TRUE(st.ok());
            EXPECT_TRUE(obj->is_finalized());

            // Verify data integrity
            for (size_t i = 0; i < src_size; ++i) {
                Field original_field, finalized_field;
                source_column->get(i, original_field);
                obj->get(i, finalized_field);
                EXPECT_EQ(original_field, finalized_field);
            }
        }

        // Test case 2: Test finalize with WRITE_MODE
        {
            auto col = source_column->clone_empty();
            auto obj = assert_cast<ColumnObject*>(col.get());

            // Insert data from source column
            for (size_t i = 0; i < src_size; ++i) {
                Field field;
                source_column->get(i, field);
                obj->insert(field);
            }

            // Verify initial state
            EXPECT_FALSE(obj->is_finalized());

            // Finalize in WRITE_MODE
            Status st = obj->finalize(ColumnObject::FinalizeMode::WRITE_MODE);
            EXPECT_TRUE(st.ok());
            EXPECT_TRUE(obj->is_finalized());

            // Verify data integrity
            for (size_t i = 0; i < src_size; ++i) {
                Field original_field, finalized_field;
                source_column->get(i, original_field);
                obj->get(i, finalized_field);
                EXPECT_EQ(original_field, finalized_field);
            }
        }

        // Test case 3: Test finalize without mode (default READ_MODE)
        {
            auto col = source_column->clone_empty();
            auto obj = assert_cast<ColumnObject*>(col.get());

            // Insert data from source column
            for (size_t i = 0; i < src_size; ++i) {
                Field field;
                source_column->get(i, field);
                obj->insert(field);
            }

            // Verify initial state
            EXPECT_FALSE(obj->is_finalized());

            // Finalize without mode
            obj->finalize();
            EXPECT_TRUE(obj->is_finalized());

            // Verify data integrity
            for (size_t i = 0; i < src_size; ++i) {
                Field original_field, finalized_field;
                source_column->get(i, original_field);
                obj->get(i, finalized_field);
                EXPECT_EQ(original_field, finalized_field);
            }
        }

        // Test case 4: Test finalize on empty column
        {
            auto col = source_column->clone_empty();
            auto obj = assert_cast<ColumnObject*>(col.get());

            // Empty column always finalized
            EXPECT_TRUE(obj->is_finalized());

            // Finalize empty column
            Status st = obj->finalize(ColumnObject::FinalizeMode::READ_MODE);
            EXPECT_TRUE(st.ok());
            EXPECT_TRUE(obj->is_finalized());
            EXPECT_EQ(obj->size(), 0);
        }

        // Test case 5: Test finalize preserves column structure
        {
            auto col = source_column->clone_empty();
            auto obj = assert_cast<ColumnObject*>(col.get());

            // Insert data from source column
            for (size_t i = 0; i < src_size; ++i) {
                Field field;
                source_column->get(i, field);
                obj->insert(field);
            }

            // Verify initial state
            EXPECT_FALSE(obj->is_finalized());

            // Store original structure
            auto original_subcolumns = obj->get_subcolumns();

            // Finalize
            Status st = obj->finalize(ColumnObject::FinalizeMode::READ_MODE);
            EXPECT_TRUE(st.ok());
            EXPECT_TRUE(obj->is_finalized());

            // Verify structure is preserved
            auto final_subcolumns = obj->get_subcolumns();
            EXPECT_EQ(final_subcolumns.size(), original_subcolumns.size());

            // Verify each subcolumn is finalized
            for (const auto& subcolumn : final_subcolumns) {
                EXPECT_TRUE(subcolumn->data.is_finalized());
            }
        }

        // Test case 6: Test finalize with WRITE_MODE on sparse columns
        {
            auto col = source_column->clone_empty();
            auto obj = assert_cast<ColumnObject*>(col.get());

            // Insert data from source column
            for (size_t i = 0; i < src_size; ++i) {
                Field field;
                source_column->get(i, field);
                obj->insert(field);
            }

            // Verify initial state
            EXPECT_FALSE(obj->is_finalized());

            // Finalize in WRITE_MODE
            Status st = obj->finalize(ColumnObject::FinalizeMode::WRITE_MODE);
            EXPECT_TRUE(st.ok());
            EXPECT_TRUE(obj->is_finalized());

            // Verify sparse columns are handled
            auto sparse_column = obj->get_sparse_column().get();
            EXPECT_TRUE(sparse_column != nullptr);

            // Verify data integrity
            for (size_t i = 0; i < src_size; ++i) {
                Field original_field, finalized_field;
                source_column->get(i, original_field);
                obj->get(i, finalized_field);
                EXPECT_EQ(original_field, finalized_field);
            }
        }
    };
    auto temp = column_variant->clone();
    auto cloned_object = assert_cast<ColumnObject*>(temp.get());
    test_func(std::move(cloned_object));
}

TEST_F(ColumnObjectTest, sanitize) {
    auto test_func = [](const auto& source_column) {
        auto src_size = source_column->size();
        EXPECT_TRUE(src_size > 0);

        // Store original data for comparison
        auto original_subcolumns = source_column->get_subcolumns();

        // Test sanitize
        Status status = source_column->sanitize();
        EXPECT_TRUE(status.ok());

        // Verify data integrity after sanitization
        auto subcolumns_after = source_column->get_subcolumns();
        EXPECT_EQ(subcolumns_after.size(), original_subcolumns.size());

        // Verify all subcolumns are valid
        for (const auto& subcolumn : subcolumns_after) {
            EXPECT_TRUE(subcolumn != nullptr);
        }
    };
    auto temp = column_variant->clone();
    auto cloned_object = assert_cast<ColumnObject*>(temp.get());
    test_func(std::move(cloned_object));
}

TEST_F(ColumnObjectTest, debug_string) {
    auto test_func = [](const auto& source_column) {
        auto src_size = source_column->size();
        EXPECT_TRUE(src_size > 0);

        // Test debug_string
        std::string debug = source_column->debug_string();
        EXPECT_FALSE(debug.empty());
    };
    test_func(column_variant);
}

// used in function_element_at for variant
TEST_F(ColumnObjectTest, find_path_lower_bound_in_sparse_data) {
    auto test_func = [](const auto& source_column) {
        auto src_size = source_column->size();
        EXPECT_TRUE(src_size > 0);
        auto* mutable_ptr = assert_cast<ColumnObject*>(source_column.get());
        //        auto [sparse_data_paths, sparse_data_values] = mutable_ptr->get_sparse_data_paths_and_values();
        // forloop
        PathInData pat("object.array");
        StringRef prefix_ref(pat.get_path());
        std::string_view path_prefix(prefix_ref.data, prefix_ref.size);
        const auto& sparse_data_map =
                assert_cast<const ColumnMap&>(*mutable_ptr->get_sparse_column());
        const auto& src_sparse_data_offsets = sparse_data_map.get_offsets();
        const auto& src_sparse_data_paths =
                assert_cast<const ColumnString&>(sparse_data_map.get_keys());

        for (size_t i = 0; i != src_sparse_data_offsets.size(); ++i) {
            size_t start = src_sparse_data_offsets[ssize_t(i) - 1];
            size_t end = src_sparse_data_offsets[ssize_t(i)];
            size_t lower_bound_index =
                    vectorized::ColumnObject::find_path_lower_bound_in_sparse_data(
                            prefix_ref, src_sparse_data_paths, start, end);
            for (; lower_bound_index != end; ++lower_bound_index) {
                auto path_ref = src_sparse_data_paths.get_data_at(lower_bound_index);
                std::string_view path(path_ref.data, path_ref.size);
                std::cout << "path : " << path << std::endl;
            }
        }
    };
    ColumnObject::MutablePtr obj;
    obj = VariantUtil::construct_advanced_varint_column();
    EXPECT_TRUE(!obj->empty());
    std::cout << "column variant size: " << obj->size() << std::endl;
    test_func(obj);
}

// used in SparseColumnExtractIterator::_fill_path_column
TEST_F(ColumnObjectTest, fill_path_column_from_sparse_data) {
    ColumnObject::MutablePtr obj;
    obj = VariantUtil::construct_advanced_varint_column();
    EXPECT_TRUE(!obj->empty());
    auto sparse_col = obj->get_sparse_column();
    auto cloned_sparse = sparse_col->clone_empty();
    auto& offsets = obj->serialized_sparse_column_offsets();
    for (size_t i = 0; i != offsets.size(); ++i) {
        auto start = offsets[i - 1];
        auto end = offsets[i];
        vectorized::ColumnObject::fill_path_column_from_sparse_data(
                *obj->get_subcolumn({}) /*root*/, nullptr, StringRef {"array"},
                cloned_sparse->get_ptr(), start, end);
    }

    EXPECT_NE(cloned_sparse->size(), sparse_col->size());

    vectorized::ColumnObject::fill_path_column_from_sparse_data(
            *obj->get_subcolumn({}) /*root*/, nullptr, StringRef {"array"}, sparse_col->get_ptr(),
            0, sparse_col->size());
    EXPECT_ANY_THROW(obj->check_consistency());
}

doris::vectorized::Field get_field_v2(std::string_view type, size_t array_element_cnt = 0) {
    static std::unordered_map<std::string_view, doris::vectorized::Field> field_map;
    if (field_map.empty()) {
        doris::vectorized::Field int_field = 20;
        doris::vectorized::Field str_field(String("str", 3));
        doris::vectorized::Field arr_int_field = Array();
        doris::vectorized::Field arr_str_field = Array();
        auto& array1 = arr_int_field.get<Array>();
        auto& array2 = arr_str_field.get<Array>();
        for (size_t i = 0; i < array_element_cnt; ++i) {
            array1.emplace_back(int_field);
            array2.emplace_back(str_field);
        }
        field_map["int"] = int_field;
        field_map["string"] = str_field;
        field_map["ai"] = arr_int_field;
        field_map["as"] = arr_str_field;
    }
    return field_map[type];
}

TEST_F(ColumnObjectTest, array_field_operations) {
    auto test_func = [](const auto& source_column) {
        auto src_size = source_column->size();
        EXPECT_TRUE(src_size > 0);

        // Test case 1: Test create_empty_array_field
        {
            EXPECT_ANY_THROW(create_empty_array_field(0));
            // Test with different dimensions
            for (size_t dim = 1; dim <= 3; ++dim) {
                Field array_field = create_empty_array_field(dim);
                EXPECT_TRUE(array_field.get_type() == Field::Types::Array);
                const Array& array = array_field.get<Array>();
                if (dim > 1) {
                    EXPECT_FALSE(array.empty());
                } else {
                    EXPECT_TRUE(array.empty());
                }
            }
        }

        // Test case 2: Test create_array
        {
            // Test with different types
            std::vector<TypeIndex> types = {TypeIndex::Int8, TypeIndex::String, TypeIndex::Float64};
            for (const auto& type : types) {
                for (size_t dim = 1; dim <= 3; ++dim) {
                    DataTypePtr array_type = create_array(type, dim);
                    EXPECT_TRUE(array_type != nullptr);
                }
            }
            // Test create_array_of_type with TypeIndex::Nothing
            auto dt_ptr = create_array_of_type(TypeIndex::Nothing, 0, false);
            EXPECT_TRUE(dt_ptr->get_type_id() == TypeIndex::Nothing);
        }

        // Test case 3: Test recreate_column_with_default_values
        {
            auto col = source_column->clone_empty();
            auto obj = assert_cast<ColumnObject*>(col.get());

            // Create a subcolumn with array type
            PathInData path("array_field");
            auto array_type = create_array(TypeIndex::Int8, 2);
            auto column = array_type->create_column();
            auto column_a = array_type->create_column();
            column_a->insert(Array(1));
            obj->add_sub_column(path, std::move(column));

            // Get the subcolumn
            const auto* subcolumn = obj->get_subcolumn(path);
            EXPECT_TRUE(subcolumn != nullptr);

            EXPECT_ANY_THROW(subcolumn->get_finalized_column_ptr());

            // Recreate column with default values
            auto new_column = recreate_column_with_default_values(
                    column_a->convert_to_full_column_if_const(), TypeIndex::Int8, 2);
            EXPECT_TRUE(new_column->get_name().find("Array") != std::string::npos);
            EXPECT_EQ(new_column->size(), subcolumn->size());
        }

        // Test case 4: Test clone_with_default_values
        {
            auto col = source_column->clone_empty();
            auto obj = assert_cast<ColumnObject*>(col.get());

            // Create a subcolumn with array type
            PathInData path("array_field");
            auto array_type = create_array(TypeIndex::Int8, 1);
            auto column = array_type->create_column();
            Array array1 = {1, 2, 3};
            column->insert(array1);
            obj->add_sub_column(path, std::move(column), array_type);

            // Get the subcolumn
            const auto* subcolumn = obj->get_subcolumn(path);
            EXPECT_TRUE(subcolumn != nullptr);
            EXPECT_TRUE(subcolumn->size() > 0);
            std::cout << "subcolumn size: " << subcolumn->size() << std::endl;
            Field f = subcolumn->get_last_field();
            EXPECT_TRUE(f.get_type() == Field::Types::Array);

            // Create field info
            FieldInfo info;
            info.scalar_type_id = TypeIndex::Int8;
            info.num_dimensions = 1;
            info.have_nulls = false;
            info.need_convert = false;

            // Clone with default values
            auto cloned = subcolumn->clone_with_default_values(info);
            std::cout << "cloned size: " << cloned.size() << std::endl;
            EXPECT_TRUE(cloned.size() == subcolumn->size());
        }

        // Test case 5: Test Subcolumn::resize
        {
            auto col = source_column->clone_empty();
            auto obj = assert_cast<ColumnObject*>(col.get());

            // Create a subcolumn
            PathInData path("test_field");
            obj->add_sub_column(path, src_size);

            // Get the subcolumn
            auto* subcolumn = obj->get_subcolumn(path);
            EXPECT_TRUE(subcolumn != nullptr);

            // Test resize to larger size
            size_t new_size = src_size + 10;
            subcolumn->resize(new_size);
            EXPECT_EQ(subcolumn->size(), new_size);

            // Test resize to smaller size
            new_size = src_size / 2;
            subcolumn->resize(new_size);
            EXPECT_EQ(subcolumn->size(), new_size);

            // Test resize to zero
            subcolumn->resize(0);
            EXPECT_EQ(subcolumn->size(), 0);
        }
        {
            // Test wrapp_array_nullable
            // 1. create an empty variant column
            auto variant = ColumnObject::create(2);

            std::vector<std::pair<std::string, doris::vectorized::Field>> data;

            // 2. subcolumn path
            data.emplace_back("v.ai", get_field_v2("ai", 1));
            data.emplace_back("v.as", get_field_v2("as", 1));

            for (int i = 0; i < 2; ++i) {
                auto field = VariantUtil::construct_variant_map(data);
                variant->try_insert(field);
            }
            EXPECT_FALSE(variant->is_finalized());
            Status st = variant->finalize(ColumnObject::FinalizeMode::WRITE_MODE);
            EXPECT_TRUE(st.ok());
            EXPECT_TRUE(variant->is_finalized());
            std::cout << "sub: " << variant->get_subcolumns().size() << std::endl;
            for (auto& entry : variant->get_subcolumns()) {
                std::cout << "entry path: " << entry->path.get_path() << std::endl;
                std::cout << "entry type: " << entry->data.get_least_common_typeBase()->get_name()
                          << std::endl;
                std::cout << "entry dimension " << entry->data.get_dimensions() << std::endl;
            }

            // then clear
            variant->clear_column_data();
            EXPECT_TRUE(variant->size() == 0);
        }
    };
    auto temp = column_variant->clone();
    auto cloned_object = assert_cast<ColumnObject*>(temp.get());
    test_func(std::move(cloned_object));
}

TEST_F(ColumnObjectTest, assert_exception_happen) {
    // Test case 1: Test assert_exception_happen
    {
        // 1. create an empty variant column
        vectorized::ColumnObject::Subcolumns dynamic_subcolumns;
        dynamic_subcolumns.create_root(vectorized::ColumnObject::Subcolumn(0, true, true /*root*/));
        dynamic_subcolumns.add(vectorized::PathInData("v.f"),
                               vectorized::ColumnObject::Subcolumn {0, true});
        dynamic_subcolumns.add(vectorized::PathInData("v.e"),
                               vectorized::ColumnObject::Subcolumn {0, true});
        dynamic_subcolumns.add(vectorized::PathInData("v.b"),
                               vectorized::ColumnObject::Subcolumn {0, true});
        dynamic_subcolumns.add(vectorized::PathInData("v.b.d"),
                               vectorized::ColumnObject::Subcolumn {0, true});
        dynamic_subcolumns.add(vectorized::PathInData("v.c.d"),
                               vectorized::ColumnObject::Subcolumn {0, true});
        std::cout << "dynamic_subcolumns size: " << dynamic_subcolumns.size() << std::endl;
        EXPECT_ANY_THROW(ColumnObject::create(2, std::move(dynamic_subcolumns)));
    }

    {
        // 1. create an empty variant column
        auto variant = ColumnObject::create(5);

        std::vector<std::pair<std::string, doris::vectorized::Field>> data;

        // 2. subcolumn path
        data.emplace_back("v.a", get_field_v2("int"));
        data.emplace_back("v.b", get_field_v2("string"));
        data.emplace_back("v.c", get_field_v2("ai", 2));
        data.emplace_back("v.f", get_field_v2("as", 2));
        data.emplace_back("v.e", get_field_v2("string"));

        for (int i = 0; i < 5; ++i) {
            auto field = VariantUtil::construct_variant_map(data);
            variant->try_insert(field);
        }

        // 3. sparse column path
        data.emplace_back("v.d.d", get_field_v2("ai", 2));
        data.emplace_back("v.c.d", get_field_v2("string"));
        data.emplace_back("v.b.d", get_field_v2("ai", 2));
        for (int i = 0; i < 5; ++i) {
            auto field = VariantUtil::construct_variant_map(data);
            variant->try_insert(field);
        }

        data.clear();
        data.emplace_back("v.a", get_field_v2("int"));
        data.emplace_back("v.b", get_field_v2("int"));
        data.emplace_back("v.c", get_field_v2("ai", 2));
        data.emplace_back("v.f", get_field_v2("as", 2));
        data.emplace_back("v.e", get_field_v2("string"));
        data.emplace_back("v.d.d", get_field_v2("as", 2));
        data.emplace_back("v.c.d", get_field_v2("int"));
        data.emplace_back("v.b.d", get_field_v2("as", 2));
        for (int i = 0; i < 5; ++i) {
            auto field = VariantUtil::construct_variant_map(data);
            variant->try_insert(field);
        }
        EXPECT_FALSE(variant->is_finalized());
        for (const auto& column : variant->get_subcolumns()) {
            if (!column->data.is_finalized()) {
                EXPECT_ANY_THROW(column->data.remove_nullable());
                EXPECT_ANY_THROW(column->data.get_finalized_column());
            } else {
                std::cout << "column path: " << column->path.get_path() << std::endl;
                EXPECT_NO_THROW(column->data.remove_nullable());
                EXPECT_NO_THROW(column->data.get_finalized_column());
            }
        }
    }
}

TEST_F(ColumnObjectTest, try_insert_default_from_nested) {
    // 1. create an empty variant column
    vectorized::ColumnObject::Subcolumns dynamic_subcolumns;
    auto array_type = create_array(TypeIndex::String, 1);
    auto column = array_type->create_column();
    Array array1 = {"amory", "commit"};
    Array array2 = {"amory", "doris"};
    column->insert(array1);
    column->insert(array2);

    auto array_type2 = create_array(TypeIndex::String, 2);
    auto column2 = array_type2->create_column();
    Array array22, array23;
    array22.push_back(array1);
    array22.push_back(array2);
    array23.push_back(array2);
    array23.push_back(array1);
    column2->insert(array22);
    column2->insert(array23);

    dynamic_subcolumns.create_root(vectorized::ColumnObject::Subcolumn(0, true, true /*root*/));
    dynamic_subcolumns.add(vectorized::PathInData("v.f"),
                           vectorized::ColumnObject::Subcolumn {0, true});
    dynamic_subcolumns.add(
            vectorized::PathInData("v.a"),
            vectorized::ColumnObject::Subcolumn {std::move(column2), array_type2, false, false});
    dynamic_subcolumns.add(vectorized::PathInData("v.b"),
                           vectorized::ColumnObject::Subcolumn {0, true});
    dynamic_subcolumns.add(
            vectorized::PathInData("v.b.a"),
            vectorized::ColumnObject::Subcolumn {std::move(column), array_type, false, false});
    dynamic_subcolumns.add(vectorized::PathInData("v.c.d"),
                           vectorized::ColumnObject::Subcolumn {0, true});
    std::cout << "dynamic_subcolumns size: " << dynamic_subcolumns.size() << std::endl;
    auto obj = ColumnObject::create(5, std::move(dynamic_subcolumns));

    for (auto& entry : obj->get_subcolumns()) {
        std::cout << "entry path: " << entry->path.get_path() << std::endl;
        std::cout << "entry type: " << entry->data.get_least_common_typeBase()->get_name()
                  << std::endl;
        std::cout << "entry dimension " << entry->data.get_dimensions() << std::endl;
        bool inserted = obj->try_insert_default_from_nested(entry);
        if (!inserted) {
            entry->data.insert_default();
        }
    }
}

// unnest, clear_column_data
TEST_F(ColumnObjectTest, unnest) {
    // 1. create an empty variant column
    vectorized::ColumnObject::Subcolumns dynamic_subcolumns;
    auto nested_col = ColumnObject::NESTED_TYPE->create_column();
    Array array1 = {"amory", "commit"};
    Array array2 = {"amory", "doris"};
    std::cout << "array: " << array1.size() << std::endl;
    nested_col->insert(array1);
    nested_col->insert(array2);
    std::cout << nested_col->size() << std::endl;

    // 2. subcolumn path
    dynamic_subcolumns.create_root(vectorized::ColumnObject::Subcolumn(2, true, true /*root*/));
    dynamic_subcolumns.add(vectorized::PathInData("v.f"),
                           vectorized::ColumnObject::Subcolumn {2, true});
    dynamic_subcolumns.add(vectorized::PathInData("v.a"),
                           vectorized::ColumnObject::Subcolumn {
                                   std::move(nested_col), ColumnObject::NESTED_TYPE, true, false});
    std::cout << "dynamic_subcolumns size: " << dynamic_subcolumns.size() << std::endl;
    auto obj = ColumnObject::create(2, std::move(dynamic_subcolumns));
    obj->set_num_rows(2);
    EXPECT_TRUE(!obj->empty());
    std::cout << obj->size() << std::endl;
    Status st = obj->finalize(ColumnObject::FinalizeMode::WRITE_MODE);
    EXPECT_TRUE(st.ok());
}

TEST_F(ColumnObjectTest, path_in_data_builder_test) {
    // Create a ColumnObject with nested subcolumns
    auto variant = ColumnObject::create(5);

    // Test case 1: Build a nested path with PathInDataBuilder
    {
        PathInDataBuilder builder;
        builder.append("v", false); // First part is not array
        builder.append("a", true);  // Second part is array
        builder.append("b", true);  // Third part is array
        builder.append("c", false); // Fourth part is not array

        PathInData path = builder.build();
        EXPECT_TRUE(path.has_nested_part());

        // Create field info for nested type
        FieldInfo field_info;
        field_info.scalar_type_id = TypeIndex::Int8;
        field_info.have_nulls = true;
        field_info.need_convert = false;
        field_info.num_dimensions = 2; // Array of Array

        // Test add_nested_subcolumn
        variant->add_nested_subcolumn(path, field_info, 5);

        // Verify the subcolumn was added correctly
        const auto* subcolumn = variant->get_subcolumn(path);
        EXPECT_TRUE(subcolumn != nullptr);

        // then clear
        variant->clear_column_data();
        EXPECT_TRUE(variant->size() == 0);
    }
}

TEST_F(ColumnObjectTest, get_field_info_all_types) {
    // Test Int32
    {
        Int32 field(42);
        FieldInfo info;
        schema_util::get_field_info(field, &info);
        EXPECT_EQ(info.scalar_type_id, TypeIndex::Int8);
        EXPECT_FALSE(info.have_nulls);
        EXPECT_FALSE(info.need_convert);
        EXPECT_EQ(info.num_dimensions, 0);
    }

    // Test Int64
    {
        Int64 field(42);
        FieldInfo info;
        schema_util::get_field_info(field, &info);
        EXPECT_EQ(info.scalar_type_id, TypeIndex::Int8);
        EXPECT_FALSE(info.have_nulls);
        EXPECT_FALSE(info.need_convert);
        EXPECT_EQ(info.num_dimensions, 0);
    }

    // Test UInt64
    {
        Field field(UInt64(42));
        FieldInfo info;
        schema_util::get_field_info(field, &info);
        EXPECT_EQ(info.scalar_type_id, TypeIndex::Int8);
        EXPECT_FALSE(info.have_nulls);
        EXPECT_FALSE(info.need_convert);
        EXPECT_EQ(info.num_dimensions, 0);
    }

    // Test Int64 with different ranges
    // Test Int64 with different ranges
    {
        // Test Int64 <= Int8::max()
        Int64 field1(std::numeric_limits<Int8>::max());
        FieldInfo info1;
        schema_util::get_field_info(field1, &info1);
        EXPECT_EQ(info1.scalar_type_id, TypeIndex::Int8);
        EXPECT_FALSE(info1.have_nulls);
        EXPECT_FALSE(info1.need_convert);
        EXPECT_EQ(info1.num_dimensions, 0);

        // Test Int64 <= Int16::max()
        Int64 field2(std::numeric_limits<Int16>::max());
        FieldInfo info2;
        schema_util::get_field_info(field2, &info2);
        EXPECT_EQ(info2.scalar_type_id, TypeIndex::Int16);
        EXPECT_FALSE(info2.have_nulls);
        EXPECT_FALSE(info2.need_convert);
        EXPECT_EQ(info2.num_dimensions, 0);

        // Test Int64 <= Int32::max()
        Int64 field3(std::numeric_limits<Int32>::max());
        FieldInfo info3;
        schema_util::get_field_info(field3, &info3);
        EXPECT_EQ(info3.scalar_type_id, TypeIndex::Int32);
        EXPECT_FALSE(info3.have_nulls);
        EXPECT_FALSE(info3.need_convert);
        EXPECT_EQ(info3.num_dimensions, 0);

        // Test Int64 > Int32::max()
        Int64 field4(static_cast<Int64>(std::numeric_limits<Int32>::max()) + 1);
        FieldInfo info4;
        schema_util::get_field_info(field4, &info4);
        EXPECT_EQ(info4.scalar_type_id, TypeIndex::Int64);
        EXPECT_FALSE(info4.have_nulls);
        EXPECT_FALSE(info4.need_convert);
        EXPECT_EQ(info4.num_dimensions, 0);

        // Test Int64 <= Int8::min()
        Int64 field5(std::numeric_limits<Int8>::min());
        FieldInfo info5;
        schema_util::get_field_info(field5, &info5);
        EXPECT_EQ(info5.scalar_type_id, TypeIndex::Int8);
        EXPECT_FALSE(info5.have_nulls);
        EXPECT_FALSE(info5.need_convert);
        EXPECT_EQ(info5.num_dimensions, 0);

        // Test Int64 <= Int16::min()
        Int64 field6(std::numeric_limits<Int16>::min());
        FieldInfo info6;
        schema_util::get_field_info(field6, &info6);
        EXPECT_EQ(info6.scalar_type_id, TypeIndex::Int16);
        EXPECT_FALSE(info6.have_nulls);
        EXPECT_FALSE(info6.need_convert);
        EXPECT_EQ(info6.num_dimensions, 0);

        // Test Int64 <= Int32::min()
        Int64 field7(std::numeric_limits<Int32>::min());
        FieldInfo info7;
        schema_util::get_field_info(field7, &info7);
        EXPECT_EQ(info7.scalar_type_id, TypeIndex::Int32);
        EXPECT_FALSE(info7.have_nulls);
        EXPECT_FALSE(info7.need_convert);
        EXPECT_EQ(info7.num_dimensions, 0);

        // Test Int64 < Int32::min()
        Int64 field8(static_cast<Int64>(std::numeric_limits<Int32>::min()) - 1);
        FieldInfo info8;
        schema_util::get_field_info(field8, &info8);
        EXPECT_EQ(info8.scalar_type_id, TypeIndex::Int64);
    }

    // Test UInt64 with different ranges
    {
        // Test UInt64 <= UInt8::max()
        UInt64 field1(std::numeric_limits<UInt8>::max());
        FieldInfo info1;
        schema_util::get_field_info(field1, &info1);
        EXPECT_EQ(info1.scalar_type_id, TypeIndex::Int16);
        EXPECT_FALSE(info1.have_nulls);
        EXPECT_FALSE(info1.need_convert);
        EXPECT_EQ(info1.num_dimensions, 0);

        // Test UInt64 <= UInt16::max()
        UInt64 field2(std::numeric_limits<UInt16>::max());
        FieldInfo info2;
        schema_util::get_field_info(field2, &info2);
        EXPECT_EQ(info2.scalar_type_id, TypeIndex::Int32);
        EXPECT_FALSE(info2.have_nulls);
        EXPECT_FALSE(info2.need_convert);
        EXPECT_EQ(info2.num_dimensions, 0);

        // Test UInt64 <= UInt32::max()
        UInt64 field3(std::numeric_limits<UInt32>::max());
        FieldInfo info3;
        schema_util::get_field_info(field3, &info3);
        EXPECT_EQ(info3.scalar_type_id, TypeIndex::Int64);
        EXPECT_FALSE(info3.have_nulls);
        EXPECT_FALSE(info3.need_convert);
        EXPECT_EQ(info3.num_dimensions, 0);

        // Test UInt64 > UInt32::max()
        UInt64 field4(static_cast<UInt64>(std::numeric_limits<UInt32>::max()) + 1);
        FieldInfo info4;
        schema_util::get_field_info(field4, &info4);
        EXPECT_EQ(info4.scalar_type_id, TypeIndex::Int64);
    }

    // Test Float32
    {
        Field field(Float32(42.0f));
        FieldInfo info;
        schema_util::get_field_info(field, &info);
        EXPECT_EQ(info.scalar_type_id, TypeIndex::Float64);
        EXPECT_FALSE(info.have_nulls);
        EXPECT_FALSE(info.need_convert);
        EXPECT_EQ(info.num_dimensions, 0);
    }

    // Test Float64
    {
        Field field(Float64(42.0));
        FieldInfo info;
        schema_util::get_field_info(field, &info);
        EXPECT_EQ(info.scalar_type_id, TypeIndex::Float64);
        EXPECT_FALSE(info.have_nulls);
        EXPECT_FALSE(info.need_convert);
        EXPECT_EQ(info.num_dimensions, 0);
    }

    // Test String
    {
        Field field(String("test"));
        FieldInfo info;
        schema_util::get_field_info(field, &info);
        EXPECT_EQ(info.scalar_type_id, TypeIndex::String);
        EXPECT_FALSE(info.have_nulls);
        EXPECT_FALSE(info.need_convert);
        EXPECT_EQ(info.num_dimensions, 0);
    }

    {
        Slice slice("\"amory is cute\"");
        JsonBinaryValue value;
        Status st = value.from_json_string(slice.data, slice.size);
        EXPECT_TRUE(st.ok()) << st.to_string();
        JsonbField field(value.value(), value.size());

        FieldInfo info;
        schema_util::get_field_info(field, &info);
        EXPECT_EQ(info.scalar_type_id, TypeIndex::JSONB);
    }

    // Test Array
    {
        Array array;
        array.push_back(Int64(1));
        array.push_back(Int64(2));
        Field field(array);
        FieldInfo info;
        schema_util::get_field_info(field, &info);
        EXPECT_EQ(info.scalar_type_id, TypeIndex::Int8);
        EXPECT_FALSE(info.have_nulls);
        EXPECT_FALSE(info.need_convert);
        EXPECT_EQ(info.num_dimensions, 1);
    }

    // Test nested Array
    {
        Array inner_array;
        inner_array.push_back(Int64(1));
        inner_array.push_back(Int64(2));

        Array outer_array;
        outer_array.push_back(inner_array);
        outer_array.push_back(inner_array);

        Field field(outer_array);
        FieldInfo info;
        schema_util::get_field_info(field, &info);
        EXPECT_EQ(info.scalar_type_id, TypeIndex::Int8);
        EXPECT_FALSE(info.have_nulls);
        EXPECT_FALSE(info.need_convert);
        EXPECT_EQ(info.num_dimensions, 2);
    }

    // Test Tuple
    {
        Tuple t1;
        t1.push_back(Field("amory cute"));
        t1.push_back(__int128_t(37));
        t1.push_back(true);
        FieldInfo info;
        schema_util::get_field_info(t1, &info);
        EXPECT_EQ(info.scalar_type_id, TypeIndex::JSONB)
                << "info.scalar_type_id: " << getTypeName(info.scalar_type_id);
        EXPECT_FALSE(info.have_nulls);
        EXPECT_FALSE(info.need_convert);
        EXPECT_EQ(info.num_dimensions, 0);
    }

    // Test Map will throw exception:  Bad type of Field 25
    {
        Array k1 = {"a", "b", "c"};
        Array v1 = {1, 2, 3};
        Map map;
        map.push_back(k1);
        map.push_back(v1);
        FieldInfo info;
        EXPECT_ANY_THROW(schema_util::get_field_info(map, &info));
    }

    // Test VariantMap
    {
        VariantMap variant_map;
        variant_map[PathInData("key1")] = Int64(1);
        variant_map[PathInData("key2")] = String("value");
        Field field(variant_map);
        FieldInfo info;
        schema_util::get_field_info(field, &info);
        EXPECT_EQ(info.scalar_type_id, TypeIndex::VARIANT);
        EXPECT_FALSE(info.have_nulls);
        EXPECT_FALSE(info.need_convert);
        EXPECT_EQ(info.num_dimensions, 0);
    }

    // Test Array with different types
    {
        Array array;
        array.push_back(Int64(1));
        Field field(array);
        FieldInfo info;
        schema_util::get_field_info(field, &info);
        EXPECT_EQ(info.scalar_type_id, TypeIndex::Int8)
                << "info.scalar_type_id: " << getTypeName(info.scalar_type_id);
        EXPECT_FALSE(info.have_nulls);
        EXPECT_FALSE(info.need_convert);
        EXPECT_EQ(info.num_dimensions, 1);
    }

    // Test Array with nulls
    {
        Array array;
        array.push_back(Int64(1));
        array.push_back(Null());
        Field field(array);
        FieldInfo info;
        schema_util::get_field_info(field, &info);
        EXPECT_EQ(info.scalar_type_id, TypeIndex::Int8);
        EXPECT_TRUE(info.have_nulls);
        EXPECT_FALSE(info.need_convert);
        EXPECT_EQ(info.num_dimensions, 1);
    }

    // Test nested Array with Int64 in different ranges
    {// Test nested Array with Int64 <= Int8::max()
     {Array inner_array;
    inner_array.push_back(Int64(std::numeric_limits<Int8>::max()));
    inner_array.push_back(Int64(std::numeric_limits<Int8>::max()));

    Array outer_array;
    outer_array.push_back(inner_array);
    outer_array.push_back(inner_array);

    Field field(outer_array);
    FieldInfo info;
    schema_util::get_field_info(field, &info);
    EXPECT_EQ(info.scalar_type_id, TypeIndex::Int8);
    EXPECT_FALSE(info.have_nulls);
    EXPECT_FALSE(info.need_convert);
    EXPECT_EQ(info.num_dimensions, 2);
}

// Test nested Array with Int64 <= Int16::max()
{
    Array inner_array;
    inner_array.push_back(Int64(std::numeric_limits<Int16>::max()));
    inner_array.push_back(Int64(std::numeric_limits<Int16>::max()));

    Array outer_array;
    outer_array.push_back(inner_array);
    outer_array.push_back(inner_array);

    Field field(outer_array);
    FieldInfo info;
    schema_util::get_field_info(field, &info);
    EXPECT_EQ(info.scalar_type_id, TypeIndex::Int16);
    EXPECT_FALSE(info.have_nulls);
    EXPECT_FALSE(info.need_convert);
    EXPECT_EQ(info.num_dimensions, 2);
}

// Test nested Array with Int64 <= Int32::max()
{
    Array inner_array;
    inner_array.push_back(Int64(std::numeric_limits<Int32>::max()));
    inner_array.push_back(Int64(std::numeric_limits<Int32>::max()));

    Array outer_array;
    outer_array.push_back(inner_array);
    outer_array.push_back(inner_array);

    Field field(outer_array);
    FieldInfo info;
    schema_util::get_field_info(field, &info);
    EXPECT_EQ(info.scalar_type_id, TypeIndex::Int32);
    EXPECT_FALSE(info.have_nulls);
    EXPECT_FALSE(info.need_convert);
    EXPECT_EQ(info.num_dimensions, 2);
}

// Test nested Array with Int64 > Int32::max()
{
    Array inner_array;
    inner_array.push_back(Int64(static_cast<Int64>(std::numeric_limits<Int32>::max()) + 1));
    inner_array.push_back(Int64(static_cast<Int64>(std::numeric_limits<Int32>::max()) + 1));

    Array outer_array;
    outer_array.push_back(inner_array);
    outer_array.push_back(inner_array);

    Field field(outer_array);
    FieldInfo info;
    schema_util::get_field_info(field, &info);
    EXPECT_EQ(info.scalar_type_id, TypeIndex::Int64);
    EXPECT_FALSE(info.have_nulls);
    EXPECT_FALSE(info.need_convert);
    EXPECT_EQ(info.num_dimensions, 2);
}
} // namespace doris::vectorized

// Test nested Array with UInt64 in different ranges
{// Test nested Array with UInt64 <= UInt8::max()
 {Array inner_array;
inner_array.push_back(UInt64(std::numeric_limits<UInt8>::max()));
inner_array.push_back(UInt64(std::numeric_limits<UInt8>::max()));

Array outer_array;
outer_array.push_back(inner_array);
outer_array.push_back(inner_array);

Field field(outer_array);
FieldInfo info;
schema_util::get_field_info(field, &info);
EXPECT_EQ(info.scalar_type_id, TypeIndex::Int16);
EXPECT_FALSE(info.have_nulls);
EXPECT_FALSE(info.need_convert);
EXPECT_EQ(info.num_dimensions, 2);
}

// Test nested Array with UInt64 <= UInt16::max()
{
    Array inner_array;
    inner_array.push_back(UInt64(std::numeric_limits<UInt16>::max()));
    inner_array.push_back(UInt64(std::numeric_limits<UInt16>::max()));

    Array outer_array;
    outer_array.push_back(inner_array);
    outer_array.push_back(inner_array);

    Field field(outer_array);
    FieldInfo info;
    schema_util::get_field_info(field, &info);
    EXPECT_EQ(info.scalar_type_id, TypeIndex::Int32);
    EXPECT_FALSE(info.have_nulls);
    EXPECT_FALSE(info.need_convert);
    EXPECT_EQ(info.num_dimensions, 2);
}

// Test nested Array with UInt64 <= UInt32::max()
{
    Array inner_array;
    inner_array.push_back(UInt64(std::numeric_limits<UInt32>::max()));
    inner_array.push_back(UInt64(std::numeric_limits<UInt32>::max()));

    Array outer_array;
    outer_array.push_back(inner_array);
    outer_array.push_back(inner_array);

    Field field(outer_array);
    FieldInfo info;
    schema_util::get_field_info(field, &info);
    EXPECT_EQ(info.scalar_type_id, TypeIndex::Int64);
    EXPECT_FALSE(info.have_nulls);
    EXPECT_FALSE(info.need_convert);
    EXPECT_EQ(info.num_dimensions, 2);
}

// Test nested Array with UInt64 > UInt32::max()
{
    Array inner_array;
    inner_array.push_back(UInt64(static_cast<UInt64>(std::numeric_limits<UInt32>::max()) + 1));
    inner_array.push_back(UInt64(static_cast<UInt64>(std::numeric_limits<UInt32>::max()) + 1));

    Array outer_array;
    outer_array.push_back(inner_array);
    outer_array.push_back(inner_array);

    Field field(outer_array);
    FieldInfo info;
    schema_util::get_field_info(field, &info);
    EXPECT_EQ(info.scalar_type_id, TypeIndex::Int64);
    EXPECT_FALSE(info.have_nulls);
    EXPECT_FALSE(info.need_convert);
    EXPECT_EQ(info.num_dimensions, 2);
}
}

// Test nested Array with mixed Int64 and UInt64
{
    Array inner_array1;
    inner_array1.push_back(Int64(std::numeric_limits<Int32>::max()));
    inner_array1.push_back(Int64(std::numeric_limits<Int32>::max()));

    Array inner_array2;
    inner_array2.push_back(UInt64(std::numeric_limits<UInt32>::max()));
    inner_array2.push_back(UInt64(std::numeric_limits<UInt32>::max()));

    Array outer_array;
    outer_array.push_back(inner_array1);
    outer_array.push_back(inner_array2);

    Field field(outer_array);
    FieldInfo info;
    schema_util::get_field_info(field, &info);
    EXPECT_EQ(info.scalar_type_id, TypeIndex::Int64);
    EXPECT_FALSE(info.have_nulls);
    EXPECT_TRUE(info.need_convert);
    EXPECT_EQ(info.num_dimensions, 2);
}

// Test nested Array with nulls
{
    Array inner_array1;
    inner_array1.push_back(Int64(1));
    inner_array1.push_back(Int64(2));

    Array inner_array2;
    inner_array2.push_back(Int64(3));
    inner_array2.push_back(Null());

    Array outer_array;
    outer_array.push_back(inner_array1);
    outer_array.push_back(inner_array2);

    Field field(outer_array);
    FieldInfo info;
    schema_util::get_field_info(field, &info);
    EXPECT_EQ(info.scalar_type_id, TypeIndex::Int8);
    EXPECT_TRUE(info.have_nulls);
    EXPECT_FALSE(info.need_convert);
    EXPECT_EQ(info.num_dimensions, 2);
}

// Test Array with JsonbField
{
    Slice slice("\"amory is cute\"");
    JsonBinaryValue value;
    Status st = value.from_json_string(slice.data, slice.size);
    EXPECT_TRUE(st.ok()) << st.to_string();
    JsonbField field(value.value(), value.size());

    Array array;
    array.push_back(field);
    array.push_back(field);
    FieldInfo info;
    schema_util::get_field_info(array, &info);
    EXPECT_EQ(info.scalar_type_id, TypeIndex::JSONB);
}
}

TEST_F(ColumnObjectTest, field_visitor) {
    // Test replacing scalar values in a flat array
    {
        Array array;
        array.push_back(Int64(1));
        array.push_back(Int64(2));
        array.push_back(Int64(3));

        Field field(array);
        Field replacement(Int64(42));
        Field result = apply_visitor(FieldVisitorReplaceScalars(replacement, 0), field);

        EXPECT_EQ(result.get<Int64>(), 42);

        Field replacement1(Int64(42));
        Field result1 = apply_visitor(FieldVisitorReplaceScalars(replacement, 1), field);

        EXPECT_EQ(result1.get<Array>().size(), 3);
        EXPECT_EQ(result1.get<Array>()[0].get<Int64>(), 42);
        EXPECT_EQ(result1.get<Array>()[1].get<Int64>(), 42);
        EXPECT_EQ(result1.get<Array>()[2].get<Int64>(), 42);
    }
}

TEST_F(ColumnObjectTest, subcolumn_operations_coverage) {
    // Test insert_range_from
    {
        auto src_column = VariantUtil::construct_basic_varint_column();
        auto dst_column = VariantUtil::construct_dst_varint_column();

        // Test normal case
        auto* dst_subcolumn = const_cast<ColumnObject::Subcolumn*>(
                &dst_column->get_subcolumns().get_root()->data);
        dst_subcolumn->insert_range_from(src_column->get_subcolumns().get_root()->data, 0, 2);

        // Test empty range
        dst_subcolumn->insert_range_from(src_column->get_subcolumns().get_root()->data, 0, 0);

        // Test with different types
        auto src_column2 = VariantUtil::construct_advanced_varint_column();
        dst_subcolumn->insert_range_from(src_column2->get_subcolumns().get_root()->data, 0, 1);
    }

    // Test parse_binary_from_sparse_column
    {
        auto column = VariantUtil::construct_basic_varint_column();
        vectorized::Field res;
        FieldInfo field_info;

        // Test String type
        {
            std::string test_str = "test_data";
            std::vector<char> binary_data;
            size_t str_size = test_str.size();
            binary_data.resize(sizeof(size_t) + test_str.size());
            memcpy(binary_data.data(), &str_size, sizeof(size_t));
            memcpy(binary_data.data() + sizeof(size_t), test_str.data(), test_str.size());
            const char* data = binary_data.data();
            parse_binary_from_sparse_column(FieldType::OLAP_FIELD_TYPE_STRING, data, res,
                                            field_info);
            EXPECT_EQ(res.get<String>(), "test_data");
        }

        // Test integer types
        {
            Int8 int8_val = 42;
            const char* data = reinterpret_cast<const char*>(&int8_val);
            parse_binary_from_sparse_column(FieldType::OLAP_FIELD_TYPE_TINYINT, data, res,
                                            field_info);
            EXPECT_EQ(res.get<Int8>(), 42);
        }

        {
            Int16 int16_val = 12345;
            const char* data = reinterpret_cast<const char*>(&int16_val);
            parse_binary_from_sparse_column(FieldType::OLAP_FIELD_TYPE_SMALLINT, data, res,
                                            field_info);
            EXPECT_EQ(res.get<Int16>(), 12345);
        }

        {
            Int32 int32_val = 123456789;
            const char* data = reinterpret_cast<const char*>(&int32_val);
            parse_binary_from_sparse_column(FieldType::OLAP_FIELD_TYPE_INT, data, res, field_info);
            EXPECT_EQ(res.get<Int32>(), 123456789);
        }

        {
            Int64 int64_val = 1234567890123456789LL;
            const char* data = reinterpret_cast<const char*>(&int64_val);
            parse_binary_from_sparse_column(FieldType::OLAP_FIELD_TYPE_BIGINT, data, res,
                                            field_info);
            EXPECT_EQ(res.get<Int64>(), 1234567890123456789LL);
        }

        // Test floating point types
        {
            Float32 float32_val = 3.1415901f;
            const char* data = reinterpret_cast<const char*>(&float32_val);
            parse_binary_from_sparse_column(FieldType::OLAP_FIELD_TYPE_FLOAT, data, res,
                                            field_info);
            EXPECT_FLOAT_EQ(res.get<Float32>(), 0);
        }

        {
            Float64 float64_val = 3.141592653589793;
            const char* data = reinterpret_cast<const char*>(&float64_val);
            parse_binary_from_sparse_column(FieldType::OLAP_FIELD_TYPE_DOUBLE, data, res,
                                            field_info);
            EXPECT_DOUBLE_EQ(res.get<Float64>(), 3.141592653589793);
        }

        // Test JSONB type
        {
            std::string json_str = "{\"key\": \"value\"}";
            std::vector<char> binary_data;
            size_t json_size = json_str.size();
            binary_data.resize(sizeof(size_t) + json_str.size());
            memcpy(binary_data.data(), &json_size, sizeof(size_t));
            memcpy(binary_data.data() + sizeof(size_t), json_str.data(), json_str.size());
            const char* data = binary_data.data();
            parse_binary_from_sparse_column(FieldType::OLAP_FIELD_TYPE_JSONB, data, res,
                                            field_info);
        }

        // Test Array type
        {
            std::vector<char> binary_data;
            size_t array_size = 2;
            binary_data.resize(sizeof(size_t) + 2 * (sizeof(uint8_t) + sizeof(Int32)));
            char* data_ptr = binary_data.data();

            // Write array size
            memcpy(data_ptr, &array_size, sizeof(size_t));
            data_ptr += sizeof(size_t);

            // Write first element (Int32)
            *data_ptr++ = static_cast<uint8_t>(FieldType::OLAP_FIELD_TYPE_INT);
            Int32 val1 = 42;
            memcpy(data_ptr, &val1, sizeof(Int32));
            data_ptr += sizeof(Int32);

            // Write second element (Int32)
            *data_ptr++ = static_cast<uint8_t>(FieldType::OLAP_FIELD_TYPE_INT);
            Int32 val2 = 43;
            memcpy(data_ptr, &val2, sizeof(Int32));

            const char* data = binary_data.data();
            parse_binary_from_sparse_column(FieldType::OLAP_FIELD_TYPE_ARRAY, data, res,
                                            field_info);
            const Array& array = res.get<Array>();
            EXPECT_EQ(array.size(), 2);
            EXPECT_EQ(array[0].get<Int32>(), 42);
            EXPECT_EQ(array[1].get<Int32>(), 43);
        }
    }

    // Test add_sub_column
    {
        auto column = VariantUtil::construct_basic_varint_column();
        PathInData path("test.path");

        // Test normal case
        column->add_sub_column(path, 10);

        // Test with existing path
        column->add_sub_column(path, 10);

        // Test with max subcolumns limit
        for (int i = 0; i < 1000; i++) {
            PathInData new_path("test.path." + std::to_string(i));
            column->add_sub_column(new_path, 10);
        }
    }

    // Test wrapp_array_nullable
    {
        auto column = VariantUtil::construct_advanced_varint_column();
        EXPECT_TRUE(column->finalize(ColumnObject::FinalizeMode::WRITE_MODE).ok());
        PathInData path("v.f");
        auto* subcolumn = column->get_subcolumn(path);
        subcolumn->wrapp_array_nullable();
        EXPECT_TRUE(subcolumn->get_least_common_type()->is_nullable());
    }

    // Test is_empty_nested
    {
        vectorized::ColumnObject container_variant(1, true);
        // v:  {"k": [1,2,3]} ==》 [{"k": 1}, {"k": 2}, {"k": 3}]
        //     {"k": []} => [{}] vs  {"k": null} -> [null]
        //     {"k": [4]} => [{"k": 4}]
        auto col_arr =
                ColumnArray::create(ColumnInt64::create(), ColumnArray::ColumnOffsets::create());
        //        Array array1 = {1, 2, 3};
        //        Array array2 = {4};
        //        col_arr->insert(array1);
        //        col_arr->insert(array2);
        Array an;
        an.push_back(Null());
        col_arr->insert(an);
        col_arr->insert(an);
        col_arr->insert(an);
        MutableColumnPtr nested_object = ColumnObject::create(
                container_variant.max_subcolumns_count(), col_arr->get_data().size());
        MutableColumnPtr offset = col_arr->get_offsets_ptr()->assume_mutable(); // [3, 3, 4]
        auto* nested_object_ptr = assert_cast<ColumnObject*>(nested_object.get());
        // flatten nested arrays
        MutableColumnPtr flattend_column = col_arr->get_data_ptr()->assume_mutable();
        DataTypePtr flattend_type = DataTypeFactory::instance().create_data_type(
                FieldType::OLAP_FIELD_TYPE_BIGINT, 0, 0);
        // add sub path without parent prefix
        PathInData sub_path("k");
        nested_object_ptr->add_sub_column(sub_path, std::move(flattend_column),
                                          std::move(flattend_type));
        nested_object = make_nullable(nested_object->get_ptr())->assume_mutable();
        auto array =
                make_nullable(ColumnArray::create(std::move(nested_object), std::move(offset)));
        PathInData path("v.k");
        container_variant.add_sub_column(path, array->assume_mutable(),
                                         container_variant.NESTED_TYPE);
        container_variant.set_num_rows(3);
        for (auto subcolumn : container_variant.get_subcolumns()) {
            if (subcolumn->data.is_root) {
                // Nothing
                EXPECT_TRUE(subcolumn->data.is_empty_nested(0));
                continue;
            }
            for (int i = 0; i < 3; ++i) {
                EXPECT_FALSE(subcolumn->data.is_empty_nested(i));
            }
        }
    }

    // Test is_empty_nested
    {
        auto v = ColumnObject::create(1);
        auto sub_dt = make_nullable(std::make_unique<DataTypeArray>(
                make_nullable(std::make_unique<DataTypeObject>(1))));
        auto sub_col = sub_dt->create_column();

        std::vector<std::pair<std::string, doris::vectorized::Field>> data;
        Array an;
        an.push_back(Null());
        data.emplace_back("v.a", an);
        // 2. subcolumn path
        auto vf = VariantUtil::construct_variant_map(data);
        v->try_insert(vf);

        for (auto subcolumn : v->get_subcolumns()) {
            for (int i = 0; i < v->size(); ++i) {
                if (subcolumn->data.is_root) {
                    EXPECT_TRUE(subcolumn->data.is_empty_nested(i));
                }
                EXPECT_TRUE(subcolumn->data.is_empty_nested(i));
            }
        }
        Status st = v->finalize(ColumnObject::FinalizeMode::WRITE_MODE);
        EXPECT_TRUE(st.ok());
        PathInData path("v.a");
        for (auto sub : v->get_subcolumns()) {
            if (sub->data.is_root) {
                continue;
            }
            sub->kind = SubcolumnsTree<ColumnObject::Subcolumn, false>::Node::NESTED;
            EXPECT_FALSE(v->try_insert_default_from_nested(sub));
        }
    }
}

TEST_F(ColumnObjectTest, subcolumn_insert_range_from_test) {
    ColumnObject::Subcolumn subcolumn(0, true /* is_nullable */, false /* is_root */);
    Field int_field(200000);
    Field string_field("hello");

    Array array_int(2);
    array_int[0] = int_field;
    array_int[1] = int_field;
    Field array_int_field(array_int);
    ColumnObject::Subcolumn subcolumn2(0, true /* is_nullable */, false /* is_root */);
    subcolumn2.insert(array_int_field);
    subcolumn2.finalize();

    Array array_tiny_int(2);
    Field tiny_int(100);
    array_tiny_int[0] = tiny_int;
    array_tiny_int[1] = tiny_int;
    Field array_tiny_int_field(array_tiny_int);
    ColumnObject::Subcolumn subcolumn1(0, true /* is_nullable */, false /* is_root */);
    subcolumn1.insert(array_tiny_int_field);
    subcolumn1.finalize();

    Array array_string(2);
    array_string[0] = string_field;
    array_string[1] = string_field;
    Field array_string_field(array_string);
    ColumnObject::Subcolumn subcolumn3(0, true /* is_nullable */, false /* is_root */);
    subcolumn3.insert(array_string_field);
    subcolumn3.finalize();

    subcolumn.insert_range_from(subcolumn1, 0, 1);
    subcolumn.insert_range_from(subcolumn2, 0, 1);
    subcolumn.insert_range_from(subcolumn3, 0, 1);
    subcolumn.finalize();
    EXPECT_EQ(subcolumn.data.size(), 1);
    EXPECT_EQ(remove_nullable(subcolumn.get_least_common_type())->get_type_id(), TypeIndex::Array);
}

TEST_F(ColumnObjectTest, subcolumn_insert_test) {
    ColumnObject::Subcolumn subcolumn(0, true /* is_nullable */, false /* is_root */);
    Field int_field(200000);
    Field string_field("hello");
    Array array_int(2);
    array_int[0] = int_field;
    array_int[1] = int_field;
    Field array_int_field(array_int);

    Array array_int2(2);
    Field tiny_int(100);
    array_int2[0] = tiny_int;
    array_int2[1] = tiny_int;
    Field array_int2_field(array_int2);

    Array array_string(2);
    array_string[0] = string_field;
    array_string[1] = string_field;
    Field array_string_field(array_string);

    subcolumn.insert(array_int2_field);
    subcolumn.insert(array_int_field);
    subcolumn.insert(array_string_field);
    subcolumn.finalize();
    EXPECT_EQ(subcolumn.data.size(), 1);
    EXPECT_EQ(remove_nullable(subcolumn.get_least_common_type())->get_type_id(), TypeIndex::Array);

    subcolumn.insert(string_field);
    subcolumn.insert(int_field);
    EXPECT_EQ(subcolumn.data.size(), 2);
    EXPECT_EQ(remove_nullable(subcolumn.get_least_common_type())->get_type_id(), TypeIndex::JSONB);
    subcolumn.finalize();
    EXPECT_EQ(subcolumn.data.size(), 1);
    EXPECT_EQ(remove_nullable(subcolumn.get_least_common_type())->get_type_id(), TypeIndex::JSONB);
}

TEST_F(ColumnObjectTest, subcolumn_insert_test_advanced) {
    std::vector<Field> fields;

    fields.emplace_back(Field(Null()));

    fields.emplace_back(Field(true));

    fields.emplace_back(Field(922337203685477588));

    fields.emplace_back(Field(-3.14159265359));

    fields.emplace_back(Field("hello world"));

    Array arr_boolean(2);
    arr_boolean[0] = Field(true);
    arr_boolean[1] = Field(false);
    Field arr_boolean_field(arr_boolean);
    fields.emplace_back(arr_boolean_field);

    Array arr_int64(2);
    arr_int64[0] = Field(1232323232323232323);
    arr_int64[1] = Field(2232323223232323232);
    Field arr_int64_field(arr_int64);
    fields.emplace_back(arr_int64_field);

    Array arr_double(2);
    arr_double[0] = Field(1.1);
    arr_double[1] = Field(2.2);
    Field arr_double_field(arr_double);
    fields.emplace_back(arr_double_field);

    Array arr_string(2);
    arr_string[0] = Field("one");
    arr_string[1] = Field("two");
    Field arr_string_field(arr_string);
    fields.emplace_back(arr_string_field);

    Array arr_jsonb(5);
    arr_jsonb[0] = Field("one");
    arr_jsonb[1] = Field(1.1);
    arr_jsonb[2] = Field(true);
    arr_jsonb[3] = Field(1232323232323232323);
    arr_jsonb[4] = Field(1232323232323232323);
    Field arr_jsonb_field(arr_jsonb);
    fields.emplace_back(arr_jsonb_field);

    std::random_device rd;
    std::mt19937 g(rd());

    for (int i = 0; i < (1 << fields.size()); i++) {
        std::shuffle(fields.begin(), fields.end(), g);
        auto subcolumn = ColumnObject::Subcolumn(0, true, false);

        for (const auto& field : fields) {
            subcolumn.insert(field);
        }

        subcolumn.finalize();
        EXPECT_EQ(subcolumn.data.size(), 1);
        // std::cout << "least common type: " << subcolumn.get_least_common_type()->get_name() << std::endl;
        EXPECT_EQ(subcolumn.least_common_type.get_base_type_id(), TypeIndex::JSONB);

        for (const auto& field : fields) {
            subcolumn.insert(field);
        }
        EXPECT_EQ(subcolumn.least_common_type.get_base_type_id(), TypeIndex::JSONB);
        subcolumn.finalize();
        EXPECT_EQ(subcolumn.data.size(), 1);
        EXPECT_EQ(remove_nullable(subcolumn.get_least_common_type())->get_type_id(),
                  TypeIndex::JSONB);

        if (i % 1000 == 0) {
            std::cout << "insert count " << i << std::endl;
        }
    }
}

TEST_F(ColumnObjectTest, subcolumn_insert_range_from_test_advanced) {
    std::vector<Field> fields;

    fields.emplace_back(Field(Null()));

    fields.emplace_back(Field(true));

    fields.emplace_back(Field(922337203685477588));

    fields.emplace_back(Field(-3.14159265359));

    fields.emplace_back(Field("hello world"));

    Array arr_boolean(2);
    arr_boolean[0] = Field(true);
    arr_boolean[1] = Field(false);
    Field arr_boolean_field(arr_boolean);
    fields.emplace_back(arr_boolean_field);

    Array arr_int64(2);
    arr_int64[0] = Field(1232323232323232323);
    arr_int64[1] = Field(2232323223232323232);
    Field arr_int64_field(arr_int64);
    fields.emplace_back(arr_int64_field);

    Array arr_largeint(2);
    arr_largeint[0] = Field(1232323232323232323);
    arr_largeint[1] = Field(2232323223232323232);
    Field arr_largeint_field(arr_largeint);
    fields.emplace_back(arr_largeint_field);

    Array arr_double(2);
    arr_double[0] = Field(1.1);
    arr_double[1] = Field(2.2);
    Field arr_double_field(arr_double);
    fields.emplace_back(arr_double_field);

    Array arr_string(2);
    arr_string[0] = Field("one");
    arr_string[1] = Field("two");
    Field arr_string_field(arr_string);
    fields.emplace_back(arr_string_field);

    Array arr_jsonb(5);
    arr_jsonb[0] = Field("one");
    arr_jsonb[1] = Field(1.1);
    arr_jsonb[2] = Field(true);
    arr_jsonb[3] = Field(1232323232323232323);
    arr_jsonb[4] = Field(1232323232323232323);
    Field arr_jsonb_field(arr_jsonb);
    fields.emplace_back(arr_jsonb_field);

    std::random_device rd;
    std::mt19937 g(rd());

    for (int i = 0; i < (1 << fields.size()); i++) {
        std::shuffle(fields.begin(), fields.end(), g);
        auto subcolumn = ColumnObject::Subcolumn(0, true, false);

        for (const auto& field : fields) {
            auto subcolumn_tmp = ColumnObject::Subcolumn(0, true, false);
            subcolumn_tmp.insert(field);
            subcolumn.insert_range_from(subcolumn_tmp, 0, 1);
        }

        subcolumn.finalize();
        EXPECT_EQ(subcolumn.data.size(), 1);
        // std::cout << "least common type: " << subcolumn.get_least_common_type()->get_name() << std::endl;
        EXPECT_EQ(subcolumn.least_common_type.get_base_type_id(), TypeIndex::JSONB);

        for (const auto& field : fields) {
            subcolumn.insert(field);
        }
        EXPECT_EQ(subcolumn.least_common_type.get_base_type_id(), TypeIndex::JSONB);
        subcolumn.finalize();
        EXPECT_EQ(subcolumn.data.size(), 1);
        EXPECT_EQ(remove_nullable(subcolumn.get_least_common_type())->get_type_id(),
                  TypeIndex::JSONB);

        if (i % 1000 == 0) {
            std::cout << "insert count " << i << std::endl;
        }
    }
}
} // namespace doris::vectorized
