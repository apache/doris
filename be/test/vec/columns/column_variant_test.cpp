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
#include "vec/columns/column_vector.h"
#include "vec/columns/column_object.h"
#include "vec/columns/common_column_test.h"
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
        root_dir = std::string(getenv("ROOT"));
        std::cout << "root_dir: " << root_dir << std::endl;
        test_data_dir = root_dir + "/be/test/data/vec/columns";
        test_result_dir = root_dir + "/be/test/expected_result/vec/columns";

        column_variant = ColumnObject::create(true);
        std::cout << dt_variant->get_name() << std::endl;

        load_json_columns_data();
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
                    test_data_dir_json + "json_variant/array_array_string_boundary.jsonl",
                    test_data_dir_json +
                            "json_variant/array_nullable_array_nullable_boolean_boundary.jsonl",
                    test_data_dir_json +
                            "json_variant/array_nullable_array_nullable_null_boundary.jsonl",
                    test_data_dir_json +
                            "json_variant/array_nullable_array_nullable_number_boundary.jsonl",
                    test_data_dir_json +
                            "json_variant/array_nullable_array_nullable_string_boundary.jsonl",
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

TEST_F(ColumnObjectTest, has_enough_capacity) {
    auto test_func = [](const auto& src_col) {
        auto src_size = src_col->size();
        // variant always return fasle
        auto assert_col = src_col->clone_empty();
        ASSERT_FALSE(assert_col->has_enough_capacity(*src_col));
        assert_col->reserve(src_size);
        ASSERT_FALSE(assert_col->has_enough_capacity(*src_col));
    };
    test_func(column_variant);
}

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
            EXPECT_EQ(target_field, source_field) << "target_field: " << target_field.get_type_name()
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
            auto assert_col = source_column->clone_empty();
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
            auto assert_col = source_column->clone_empty();
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
    test_func(column_variant);
}

// is seri
TEST_F(ColumnObjectTest, is_column_string64) {
    EXPECT_FALSE(column_variant->is_column_string64());
}

TEST_F(ColumnObjectTest, is_column_string) {
    EXPECT_FALSE(column_variant->is_column_string());
}

TEST_F(ColumnObjectTest, serialize_one_row_to_string) {
    const auto* variant = assert_cast<const ColumnObject*>(column_variant.get());
    // Serialize hierarchy types to json format
    std::string buffer;
    for (size_t row_idx = 2000; row_idx < variant->size(); ++row_idx) {
        Status st = variant->serialize_one_row_to_string(row_idx, &buffer);
        EXPECT_TRUE(st.ok());
        //        std::cout << buffer << std::endl;
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
    EXPECT_ANY_THROW(column_variant->replace_column_data(column_variant->assume_mutable_ref(), 0, 0));
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
    EXPECT_ANY_THROW(column_variant->get_max_row_byte_size());
    EXPECT_ANY_THROW(column_variant->serialize_vec(nullptr, 0, 0));
    EXPECT_ANY_THROW(column_variant->deserialize_vec(nullptr, 0));
    EXPECT_ANY_THROW(column_variant->serialize_vec_with_null_map(nullptr, 0, nullptr));
    EXPECT_ANY_THROW(column_variant->deserialize_vec_with_null_map(nullptr, 0, nullptr));
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
            columns, serdes, pts,
            test_result_dir + "/column_variant_" + function_name + ".out");
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

        EXPECT_ANY_THROW(Status st = source_column->filter_by_selector(indices.data(), 0, target_column.get()));
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
    assert_column_vector_permute(columns, 0, false );
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
    test_func(column_variant);
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
    auto test_func = [](const auto& source_column) {
        auto src_size = source_column->size();
        EXPECT_TRUE(src_size > 0);

        // Store original root type
        const auto& root = source_column->get_subcolumns().get_root();
        auto original_root_type = root->data.get_least_common_type();

        // Test ensure_root_node_type
        auto new_type = DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_STRING, 0, 0);
        source_column->ensure_root_node_type(new_type);

        // Verify root type is updated
        auto updated_root_type = root->data.get_least_common_type();
        EXPECT_TRUE(updated_root_type->equals(*new_type));
    };
    test_func(column_variant);
}

TEST_F(ColumnObjectTest, create_root) {
    auto test_func = [](const auto& source_column) {
        auto src_size = source_column->size();
        EXPECT_TRUE(src_size > 0);

        // Test case 1: Create root with string type
        {
            auto col = source_column->clone_empty();
            auto obj = assert_cast<ColumnObject*>(col.get());
            auto type = DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_STRING, 0, 0);
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
            auto type = DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_INT, 0, 0);
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
            auto type = DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_STRING, 0, 0);
            auto column = type->create_column();
            obj->create_root(type, std::move(column));

            // Verify root is replaced with new type
            const auto& new_root = obj->get_subcolumns().get_root();
            EXPECT_TRUE(new_root != nullptr);
            EXPECT_EQ(new_root, original_root);
            EXPECT_TRUE(new_root->data.get_least_common_type()->equals(*type));
        }

        // Test case 4: Create root and verify data operations
        {
            auto col = source_column->clone_empty();
            auto obj = assert_cast<ColumnObject*>(col.get());
            auto type = DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_STRING, 0, 0);
            auto column = type->create_column();
            obj->create_root(type, std::move(column));

            // Insert some data
            Field field;
            source_column->get(0, field);
            obj->insert(field);

            // Verify data is inserted correctly
            Field inserted_field;
            obj->get(0, inserted_field);
            EXPECT_EQ(field, inserted_field);
        }

        // Test case 5: Create root with nullable type
        {
            auto col = source_column->clone_empty();
            auto obj = assert_cast<ColumnObject*>(col.get());
            auto type = DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_STRING, 0, 0);
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
            EXPECT_TRUE(obj->is_finalized());

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
            EXPECT_TRUE(obj->is_finalized());

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
            EXPECT_TRUE(obj->is_finalized());

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
            EXPECT_TRUE(obj->is_finalized());

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
            EXPECT_TRUE(obj->is_finalized());

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

TEST_F(ColumnObjectTest, sanitize) {
    auto test_func = [](const auto& source_column) {
        auto src_size = source_column->size();
        EXPECT_TRUE(src_size > 0);

        // Store original data for comparison
        auto original_subcolumns = source_column->get_subcolumns();

        // Test sanitize
        Status status = source_column->sanitize();
        if (source_column->is_finalized()) {
            EXPECT_TRUE(status.ok()) << status.to_string();
        } else {
            EXPECT_FALSE(status.ok()) << status.to_string();
        }

        // Verify data integrity after sanitization
        auto subcolumns_after = source_column->get_subcolumns();
        EXPECT_EQ(subcolumns_after.size(), original_subcolumns.size());

        // Verify all subcolumns are valid
        for (const auto& subcolumn : subcolumns_after) {
            EXPECT_TRUE(subcolumn != nullptr);
            EXPECT_TRUE(subcolumn->data.is_finalized());
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

TEST_F(ColumnObjectTest, add_nested_subcolumn) {
    auto test_func = [](const auto& source_column) {
        auto src_size = source_column->size();
        EXPECT_TRUE(src_size > 0);

        // Test case 1: Test adding nested subcolumn
        {
            auto obj = assert_cast<ColumnObject*>(source_column.get());
            // Create field info for nested type
            FieldInfo field_info;
            field_info.num_dimensions = 1; // Array dimension
            field_info.scalar_type_id = TypeIndex::String;
            field_info.have_nulls = true;
            field_info.need_convert = false;

            // Add nested subcolumn
            PathInData path("");
            std::cout << " :" << path.has_nested_part();
            obj->add_nested_subcolumn(path, field_info, src_size);

            // Verify subcolumn was added
            auto subcolumn = obj->get_subcolumn(path);
            EXPECT_TRUE(subcolumn != nullptr);
        }

        // Test case 2: Test adding nested subcolumn with multiple dimensions
        {
            auto obj = assert_cast<ColumnObject*>(source_column.get());

            // Create field info for multi-dimensional nested type
            FieldInfo field_info;
            field_info.num_dimensions = 2; // Array of arrays
            field_info.scalar_type_id = TypeIndex::String;
            field_info.have_nulls = true;
            field_info.need_convert = false;

            // Add nested subcolumn
            PathInData path("object");
            obj->add_nested_subcolumn(path, field_info, src_size);

            // Verify subcolumn was added
            auto subcolumn = obj->get_subcolumn(path);
            EXPECT_TRUE(subcolumn != nullptr);
        }
    };
    ColumnObject::MutablePtr obj;
    obj = ColumnObject::create(0);
    MutableColumns cols;
    cols.push_back(obj->get_ptr());
    const auto& json_file_obj = test_data_dir_json + "json_variant/object_boundary.jsonl";
    load_columns_data_from_file(cols, serde, '\n', {0}, json_file_obj);
    EXPECT_TRUE(!obj->empty());
    std::cout << "column variant size: " << obj->size() << std::endl;
    test_func(obj);
}

}