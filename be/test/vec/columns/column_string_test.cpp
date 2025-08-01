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

#include "vec/columns/column_string.h"

#include <gmock/gmock-more-matchers.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>

#include "vec/columns/column_vector.h"
#include "vec/columns/common_column_test.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/function_string.h"

using namespace doris;
namespace doris::vectorized {
static std::string test_data_dir;
static std::string test_result_dir;
static DataTypePtr dt_str =
        DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_STRING, 0, 0);
static DataTypePtr dt_jsonb =
        DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_JSONB, 0, 0);

static ColumnString::MutablePtr column_str32;
static ColumnString64::MutablePtr column_str64;

static ColumnString::MutablePtr column_str32_json;
static ColumnString64::MutablePtr column_str64_json;

class ColumnStringTest : public CommonColumnTest {
protected:
    static void SetUpTestSuite() {
        auto root_dir = std::string(getenv("ROOT"));
        test_data_dir = root_dir + "/be/test/data/vec/columns";
        test_result_dir = root_dir + "/be/test/expected_result/vec/columns";

        column_str32 = ColumnString::create();
        column_str64 = ColumnString64::create();

        load_columns_data();

        column_str32_json = ColumnString::create();
        column_str64_json = ColumnString64::create();
        load_json_columns_data();
    }

    static void load_columns_data() {
        std::cout << "loading test dataset" << std::endl;
        {
            MutableColumns columns;
            columns.push_back(column_str32->get_ptr());
            DataTypeSerDeSPtrs serde = {dt_str->get_serde()};
            std::string data_file = test_data_dir + "/STRING.csv";
            load_columns_data_from_file(columns, serde, ';', {0}, data_file);
            EXPECT_TRUE(!column_str32->empty());
            column_str32->insert_default();

            column_str64->insert_range_from(*column_str32, 0, column_str32->size());
        }
        std::cout << "column str size: " << column_str32->size() << std::endl;
    }

    static void load_json_columns_data() {
        std::cout << "loading json dataset" << std::endl;
        {
            MutableColumns columns;
            columns.push_back(column_str32_json->get_ptr());
            DataTypeSerDeSPtrs serde = {dt_jsonb->get_serde()};
            std::string test_data_dir_json =
                    std::string(getenv("ROOT")) + "/regression-test/data/nereids_function_p0/";
            std::vector<std::string> json_files = {
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
            };

            for (const auto& json_file : json_files) {
                load_columns_data_from_file(columns, serde, '\n', {0}, json_file);
                EXPECT_TRUE(!column_str32_json->empty());
                column_str32_json->insert_default();
                column_str64_json->insert_range_from(*column_str32_json, 0,
                                                     column_str32_json->size());
                std::cout << "column str size: " << column_str32_json->size() << std::endl;
                std::cout << "column str64 size: " << column_str64_json->size() << std::endl;
            }
        }
    }

#define column_string_common_test(callback, only_str32)                   \
    callback<TYPE_STRING>(ColumnString(), column_str32->get_ptr());       \
    if (!only_str32) {                                                    \
        callback<TYPE_STRING>(ColumnString64(), column_str64->get_ptr()); \
    }
    void hash_common_test(
            const std::string& function_name,
            std::function<void(const MutableColumns& load_cols, DataTypeSerDeSPtrs serders,
                               const std::string& res_file_name)>
                    assert_callback) {
        {
            MutableColumns columns;
            columns.push_back(column_str32->get_ptr());
            DataTypeSerDeSPtrs serdes = {dt_str->get_serde()};
            assert_callback(columns, serdes,
                            test_result_dir + "/column_str32_" + function_name + ".out");
        }
        {
            MutableColumns columns;
            columns.push_back(column_str64->get_ptr());
            DataTypeSerDeSPtrs serdes = {dt_str->get_serde()};
            assert_callback(columns, serdes,
                            test_result_dir + "/column_str64_" + function_name + ".out");
        }
        {
            MutableColumns columns;
            columns.push_back(column_str32_json->get_ptr());
            DataTypeSerDeSPtrs serdes = {dt_jsonb->get_serde()};
            assert_callback(columns, serdes,
                            test_result_dir + "/column_str32_json_" + function_name + ".out");
        }
        {
            MutableColumns columns;
            columns.push_back(column_str64_json->get_ptr());
            DataTypeSerDeSPtrs serdes = {dt_jsonb->get_serde()};
            assert_callback(columns, serdes,
                            test_result_dir + "/column_str64_json_" + function_name + ".out");
        }
    }
};

TEST_F(ColumnStringTest, check_chars_length) {
    ColumnString::check_chars_length(0, 0);
    ColumnString::check_chars_length(std::numeric_limits<uint32_t>::max() - 1, 0);
    ColumnString::check_chars_length(std::numeric_limits<uint32_t>::max(), 0);
    EXPECT_THROW(
            ColumnString::check_chars_length((size_t)std::numeric_limits<uint32_t>::max() + 1, 0),
            Exception);
}
TEST_F(ColumnStringTest, is_variable_length) {
    ColumnString::MutablePtr col = ColumnString::create();
    EXPECT_TRUE(col->is_variable_length());
    ColumnString64::MutablePtr col64 = ColumnString64::create();
    EXPECT_TRUE(col64->is_variable_length());
}
TEST_F(ColumnStringTest, sanity_check) {
    auto test_func = [](auto& col) {
        auto& chars = col->get_chars();
        auto& offsets = col->get_offsets();

        col->sanity_check();

        std::string data = "123";
        col->insert_data(data.data(), data.size());
        col->sanity_check();

        offsets[0] = 1;
        // chars.size() != offsets[count - 1]
        EXPECT_THROW(col->sanity_check(), Exception);
        offsets[0] = chars.size();

        offsets[-1] = 1;
        // offsets[-1] != 0
        EXPECT_THROW(col->sanity_check(), Exception);
        offsets[-1] = 0;

        // (offsets[i] < offsets[i - 1])
        col->insert_data(data.data(), data.size());
        offsets[0] = 1000;
        EXPECT_THROW(col->sanity_check(), Exception);
    };
    {
        ColumnString::MutablePtr col = ColumnString::create();
        test_func(col);
    }
    {
        ColumnString64::MutablePtr col = ColumnString64::create();
        test_func(col);
    }
}
TEST_F(ColumnStringTest, byte_size) {
    {
        ColumnString::MutablePtr col = ColumnString::create();
        EXPECT_EQ(col->byte_size(), 0);
        col->insert_data("123", 3);
        col->insert_data("456", 3);
        EXPECT_EQ(col->byte_size(),
                  col->get_chars().size() +
                          col->get_offsets().size() * sizeof(col->get_offsets()[0]));
    }
    {
        ColumnString64::MutablePtr col = ColumnString64::create();
        EXPECT_EQ(col->byte_size(), 0);
        col->insert_data("123", 3);
        col->insert_data("456", 3);
        EXPECT_EQ(col->byte_size(),
                  col->get_chars().size() +
                          col->get_offsets().size() * sizeof(col->get_offsets()[0]));
    }
}
TEST_F(ColumnStringTest, has_enough_capacity) {
    auto test_func = [](const auto& src_col) {
        auto src_size = src_col->size();
        auto assert_col = src_col->clone_empty();
        ASSERT_FALSE(assert_col->has_enough_capacity(*src_col));
        assert_col->reserve(src_size);
        ASSERT_TRUE(assert_col->has_enough_capacity(*src_col));
    };
    {
        ColumnString::MutablePtr col = ColumnString::create();
        col->insert_data("123", 3);
        col->insert_data("456", 3);
        test_func(col);
    }
    {
        ColumnString64::MutablePtr col = ColumnString64::create();
        col->insert_data("123", 3);
        col->insert_data("456", 3);
        test_func(col);
    }
}
TEST_F(ColumnStringTest, allocated_bytes) {
    {
        ColumnString::MutablePtr col = ColumnString::create();
        EXPECT_EQ(col->allocated_bytes(), 0);
        col->insert_data("123", 3);
        col->insert_data("456", 3);
        EXPECT_EQ(col->allocated_bytes(),
                  col->get_chars().allocated_bytes() + col->get_offsets().allocated_bytes());
    }
    {
        ColumnString64::MutablePtr col = ColumnString64::create();
        EXPECT_EQ(col->allocated_bytes(), 0);
        col->insert_data("123", 3);
        col->insert_data("456", 3);
        EXPECT_EQ(col->allocated_bytes(),
                  col->get_chars().allocated_bytes() + col->get_offsets().allocated_bytes());
    }
}
TEST_F(ColumnStringTest, clone_resized) {
    column_string_common_test(assert_column_vector_clone_resized_callback, false);
}
TEST_F(ColumnStringTest, field_test) {
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
                Field f;
                assert_col->get(i, f);
                ASSERT_EQ(f.get<StringRef>(), source_column->get_data_at(i));
            }
        }
        {
            auto assert_col = source_column->clone_empty();
            for (size_t i = 0; i != src_size; ++i) {
                JsonbField jsonbf;
                Field f = Field::create_field<TYPE_JSONB>(std::move(jsonbf));
                source_column->get(i, f);
                assert_col->insert(f);
            }
            for (size_t i = 0; i != src_size; ++i) {
                JsonbField jsonbf;
                Field f = Field::create_field<TYPE_JSONB>((std::move(jsonbf)));
                assert_col->get(i, f);
                const auto& real_field = vectorized::get<const JsonbField&>(f);
                ASSERT_EQ(StringRef(real_field.get_value(), real_field.get_size()),
                          source_column->get_data_at(i));
            }
        }
    };
    test_func(column_str32);
    test_func(column_str64);
    test_func(column_str32_json);
    test_func(column_str64_json);
}
TEST_F(ColumnStringTest, insert_many_from) {
    column_string_common_test(assert_column_vector_insert_many_from_callback, false);
}
TEST_F(ColumnStringTest, is_column_string64) {
    EXPECT_FALSE(column_str32->is_column_string64());
    EXPECT_TRUE(column_str64->is_column_string64());
    EXPECT_FALSE(column_str32_json->is_column_string64());
    EXPECT_TRUE(column_str64_json->is_column_string64());
}
TEST_F(ColumnStringTest, insert_from) {
    {
        assert_column_vector_insert_from_callback<TYPE_STRING>(ColumnString(),
                                                               column_str32->get_ptr());

        auto tmp_col_str32 = ColumnString::create();
        assert_column_vector_insert_from_callback<TYPE_STRING>(ColumnString(),
                                                               tmp_col_str32->get_ptr());
    }
    {
        assert_column_vector_insert_from_callback<TYPE_STRING>(ColumnString64(),
                                                               column_str64->get_ptr());

        auto tmp_col_str = ColumnString64::create();
        assert_column_vector_insert_from_callback<TYPE_STRING>(ColumnString64(),
                                                               tmp_col_str->get_ptr());
    }
    {
        assert_column_vector_insert_from_callback<TYPE_JSONB>(ColumnString(),
                                                              column_str32_json->get_ptr());

        auto tmp_col_str32 = ColumnString::create();
        assert_column_vector_insert_from_callback<TYPE_JSONB>(ColumnString(),
                                                              tmp_col_str32->get_ptr());
    }
    {
        assert_column_vector_insert_from_callback<TYPE_JSONB>(ColumnString64(),
                                                              column_str64_json->get_ptr());

        auto tmp_col_str = ColumnString64::create();
        assert_column_vector_insert_from_callback<TYPE_JSONB>(ColumnString64(),
                                                              tmp_col_str->get_ptr());
    }
}
TEST_F(ColumnStringTest, insert_data) {
    column_string_common_test(assert_column_vector_insert_data_callback, false);
}
TEST_F(ColumnStringTest, insert_data_without_reserve) {
    auto test_func = [](auto& col) {
        size_t input_rows_count = 10;
        std::string test_str = "1234567890";
        col->get_offsets().reserve(input_rows_count);
        col->get_chars().reserve(input_rows_count * test_str.length());

        for (int i = 0; i < input_rows_count; i++) {
            col->insert_data_without_reserve(test_str.data(), test_str.size());
        }
        EXPECT_EQ(col->size(), input_rows_count);
        for (size_t i = 0; i < input_rows_count; i++) {
            EXPECT_EQ(col->get_data_at(i), StringRef(test_str));
        }
    };
    {
        auto col = ColumnString::create();
        test_func(col);
    }
    {
        auto col = ColumnString64::create();
        test_func(col);
    }
}
TEST_F(ColumnStringTest, insert_many_strings_without_reserve) {
    auto test_func = [&](size_t clone_count, auto x, const auto& source_column) {
        using ColumnType = decltype(x);
        auto src_size = source_column->size();
        size_t actual_clone_count = std::min(clone_count, src_size);

        auto target_column = source_column->clone_resized(actual_clone_count);
        auto* col_vec_target = assert_cast<ColumnType*>(target_column.get());
        for (size_t i = 0; i != actual_clone_count; ++i) {
            EXPECT_EQ(col_vec_target->get_data_at(i), source_column->get_data_at(i));
        }

        std::vector<size_t> indices(src_size);
        std::iota(indices.begin(), indices.end(), 0);
        std::random_device rd;
        std::mt19937 g(rd());
        std::shuffle(indices.begin(), indices.end(), g);
        size_t sel_size = src_size / 2;
        indices.resize(sel_size);

        std::vector<StringRef> strings(sel_size);
        size_t length = 0;
        for (size_t i = 0; i != sel_size; ++i) {
            auto value = source_column->get_data_at(indices[i]);
            strings[i].data = value.data;
            strings[i].size = value.size;
            length += value.size;
        }
        col_vec_target->get_offsets().reserve(sel_size + col_vec_target->get_offsets().size());
        col_vec_target->get_chars().reserve(length + col_vec_target->get_chars().size());
        col_vec_target->insert_many_strings_without_reserve(strings.data(), sel_size);
        EXPECT_EQ(col_vec_target->size(), actual_clone_count + sel_size);
        for (size_t i = 0; i != actual_clone_count; ++i) {
            auto real_data = col_vec_target->get_data_at(i);
            auto expect_data = source_column->get_data_at(i);
            if (real_data != expect_data) {
                std::cout << "index: " << i << ", real_data: " << real_data.to_string()
                          << "\nexpect_data: " << expect_data.to_string() << std::endl;
            }
            EXPECT_EQ(real_data, expect_data);
        }
        for (size_t i = 0; i != sel_size; ++i) {
            EXPECT_EQ(col_vec_target->get_data_at(actual_clone_count + i), strings[i]);
        }

        // test insert consecutive strings
        target_column = source_column->clone_resized(actual_clone_count);
        col_vec_target = assert_cast<ColumnType*>(target_column.get());
        sel_size = src_size / 2;
        strings.resize(sel_size);
        length = 0;
        for (size_t i = 0; i != sel_size; ++i) {
            auto value = source_column->get_data_at(i);
            strings[i].data = value.data;
            strings[i].size = value.size;
            length += value.size;
        }
        col_vec_target->get_offsets().reserve(sel_size + col_vec_target->get_offsets().size());
        col_vec_target->get_chars().reserve(length + col_vec_target->get_chars().size());
        col_vec_target->insert_many_strings_without_reserve(strings.data(), sel_size);
        EXPECT_EQ(col_vec_target->size(), actual_clone_count + sel_size);
        for (size_t i = 0; i != actual_clone_count; ++i) {
            auto real_data = col_vec_target->get_data_at(i);
            auto expect_data = source_column->get_data_at(i);
            EXPECT_EQ(real_data, expect_data);
        }
        for (size_t i = 0; i != sel_size; ++i) {
            EXPECT_EQ(col_vec_target->get_data_at(actual_clone_count + i), strings[i]);
        }
    };
    test_func(0, ColumnString(), column_str32);
    test_func(10, ColumnString(), column_str32);
    test_func(0, ColumnString64(), column_str64);
    test_func(10, ColumnString64(), column_str64);

    test_func(0, ColumnString(), column_str32_json);
    test_func(10, ColumnString(), column_str32_json);
    test_func(0, ColumnString64(), column_str64_json);
    test_func(10, ColumnString64(), column_str64_json);
}
TEST_F(ColumnStringTest, insert_many_continuous_binary_data) {
    auto test_func = [&](size_t clone_count, auto x, const auto& source_column) {
        using ColumnType = decltype(x);
        auto src_size = source_column->size();
        auto* col_vec_src = assert_cast<ColumnType*>(source_column.get());
        size_t actual_clone_count = std::min(clone_count, src_size);

        auto target_column = source_column->clone_resized(actual_clone_count);
        auto* col_vec_target = assert_cast<ColumnType*>(target_column.get());
        for (size_t i = 0; i != actual_clone_count; ++i) {
            EXPECT_EQ(col_vec_target->get_data_at(i), source_column->get_data_at(i));
        }
        srand((unsigned)time(nullptr));
        auto start_offset = rand() % src_size;
        auto insert_count = src_size - start_offset;
        const auto* insert_data = (const char*)col_vec_src->get_chars().data();
        const auto* insert_offsets = col_vec_src->get_offsets().data() + start_offset - 1;
        col_vec_target->insert_many_continuous_binary_data(insert_data, insert_offsets,
                                                           insert_count);
        EXPECT_EQ(col_vec_target->size(), actual_clone_count + insert_count);
        size_t i = 0;
        for (; i != actual_clone_count; ++i) {
            EXPECT_EQ(col_vec_target->get_data_at(i), source_column->get_data_at(i));
        }
        for (size_t j = start_offset; i != col_vec_target->size(); ++i, ++j) {
            auto real_data = col_vec_target->get_data_at(i);
            auto expect_data = source_column->get_data_at(j);
            if (real_data != expect_data) {
                std::cout << "index: " << i << ", real_data: " << real_data.to_string()
                          << "\nexpect_data: " << expect_data.to_string() << std::endl;
            }
            EXPECT_EQ(real_data, expect_data);
        }

        // test insert 0 items
        insert_count = 0;
        target_column = source_column->clone_resized(actual_clone_count);
        col_vec_target = assert_cast<ColumnType*>(target_column.get());
        col_vec_target->insert_many_continuous_binary_data(insert_data, insert_offsets,
                                                           insert_count);
        EXPECT_EQ(col_vec_target->size(), actual_clone_count);
        for (i = 0; i != actual_clone_count; ++i) {
            EXPECT_EQ(col_vec_target->get_data_at(i), source_column->get_data_at(i));
        }
    };
    test_func(0, ColumnString(), column_str32);
    test_func(10, ColumnString(), column_str32);

    test_func(0, ColumnString(), column_str32_json);
    test_func(10, ColumnString(), column_str32_json);
}
TEST_F(ColumnStringTest, insert_many_strings) {
    auto test_func = [&](size_t clone_count, auto x, const auto& source_column) {
        using ColumnType = decltype(x);
        auto src_size = source_column->size();
        size_t actual_clone_count = std::min(clone_count, src_size);

        auto target_column = source_column->clone_resized(actual_clone_count);
        auto* col_vec_target = assert_cast<ColumnType*>(target_column.get());
        for (size_t i = 0; i != actual_clone_count; ++i) {
            std::cout << "index: " << i
                      << ", real_data: " << col_vec_target->get_data_at(i).to_string() << "\n";
            EXPECT_EQ(col_vec_target->get_data_at(i), source_column->get_data_at(i));
        }

        std::vector<size_t> indices(src_size);
        std::iota(indices.begin(), indices.end(), 0);
        std::random_device rd;
        std::mt19937 g(rd());
        std::shuffle(indices.begin(), indices.end(), g);
        size_t sel_size = src_size / 2;
        indices.resize(sel_size);

        std::vector<StringRef> strings(sel_size);
        size_t length = 0;
        for (size_t i = 0; i != sel_size; ++i) {
            auto value = source_column->get_data_at(indices[i]);
            strings[i].data = value.data;
            strings[i].size = value.size;
            length += value.size;
        }
        col_vec_target->insert_many_strings(strings.data(), sel_size);
        EXPECT_EQ(col_vec_target->size(), actual_clone_count + sel_size);
        for (size_t i = 0; i != actual_clone_count; ++i) {
            auto real_data = col_vec_target->get_data_at(i);
            auto expect_data = source_column->get_data_at(i);
            if (real_data != expect_data) {
                std::cout << "index: " << i << ", real_data: " << real_data.to_string()
                          << "\nexpect_data: " << expect_data.to_string() << std::endl;
            }
            EXPECT_EQ(real_data, expect_data);
        }
        for (size_t i = 0; i != sel_size; ++i) {
            EXPECT_EQ(col_vec_target->get_data_at(actual_clone_count + i), strings[i]);
        }
    };
    test_func(0, ColumnString(), column_str32);
    test_func(10, ColumnString(), column_str32);
    test_func(0, ColumnString64(), column_str64);
    test_func(10, ColumnString64(), column_str64);

    test_func(0, ColumnString(), column_str32_json);
    test_func(10, ColumnString(), column_str32_json);
    test_func(0, ColumnString64(), column_str64_json);
    test_func(10, ColumnString64(), column_str64_json);
}
TEST_F(ColumnStringTest, insert_many_strings_overflow) {
    auto test_func = [&](size_t clone_count, auto x, const auto& source_column, size_t max_length) {
        using ColumnType = decltype(x);
        auto src_size = source_column->size();
        size_t actual_clone_count = std::min(clone_count, src_size);

        auto target_column = source_column->clone_resized(actual_clone_count);
        auto* col_vec_target = assert_cast<ColumnType*>(target_column.get());
        for (size_t i = 0; i != actual_clone_count; ++i) {
            EXPECT_EQ(col_vec_target->get_data_at(i), source_column->get_data_at(i));
        }

        std::vector<size_t> indices(src_size);
        std::iota(indices.begin(), indices.end(), 0);
        std::random_device rd;
        std::mt19937 g(rd());
        std::shuffle(indices.begin(), indices.end(), g);
        size_t sel_size = src_size / 2;
        indices.resize(sel_size);

        std::vector<StringRef> strings(sel_size);
        ColumnString tmp_strings;
        tmp_strings.reserve(sel_size * max_length);
        for (size_t i = 0; i != sel_size; ++i) {
            auto value = source_column->get_data_at(indices[i]);
            strings[i].data = value.data;
            strings[i].size = value.size;
            if (strings[i].size > max_length) {
                strings[i].size = max_length;
            } else if (strings[i].size < max_length) {
                auto tmp_str = std::string(value.data, value.size);
                tmp_str.resize(max_length, 'a');
                tmp_strings.insert_data(tmp_str.data(), tmp_str.size());
                auto tmp_item = tmp_strings.get_data_at(tmp_strings.size() - 1);
                strings[i].data = tmp_item.data;
                strings[i].size = max_length;
            }
        }
        col_vec_target->insert_many_strings_overflow(strings.data(), sel_size, max_length);
        EXPECT_EQ(col_vec_target->size(), actual_clone_count + sel_size);
        for (size_t i = 0; i != actual_clone_count; ++i) {
            auto real_data = col_vec_target->get_data_at(i);
            auto expect_data = source_column->get_data_at(i);
            EXPECT_EQ(real_data, expect_data);
        }
        for (size_t i = 0; i != sel_size; ++i) {
            EXPECT_EQ(col_vec_target->get_data_at(actual_clone_count + i), strings[i]);
        }
    };
    std::vector<size_t> clone_counts = {0, 10};
    std::vector<size_t> max_lengths = {0, 3, 8, 13, 16, 29, 32, 33, 64, 66, 100, 128, 256};
    for (auto clone_count : clone_counts) {
        for (auto max_length : max_lengths) {
            test_func(clone_count, ColumnString(), column_str32, max_length);
            test_func(clone_count, ColumnString(), column_str32_json, max_length);
        }
        for (auto max_length : max_lengths) {
            test_func(clone_count, ColumnString64(), column_str64, max_length);
            test_func(clone_count, ColumnString64(), column_str64_json, max_length);
        }
    }
}
TEST_F(ColumnStringTest, insert_many_dict_data) {
    auto test_func = [&](size_t clone_count, auto x, const auto& source_column) {
        using ColumnType = decltype(x);
        auto src_size = source_column->size();
        size_t actual_clone_count = std::min(clone_count, src_size);
        std::vector<StringRef> dict(src_size);
        for (size_t i = 0; i != src_size; ++i) {
            auto value = source_column->get_data_at(i);
            dict[i].data = value.data;
            dict[i].size = value.size;
        }

        auto target_column = source_column->clone_resized(actual_clone_count);
        auto* col_vec_target = assert_cast<ColumnType*>(target_column.get());
        for (size_t i = 0; i != actual_clone_count; ++i) {
            std::cout << "index: " << i
                      << ", real_data: " << col_vec_target->get_data_at(i).to_string() << "\n";
            EXPECT_EQ(col_vec_target->get_data_at(i), source_column->get_data_at(i));
        }

        std::vector<int32_t> data_array(src_size);
        std::iota(data_array.begin(), data_array.end(), 0);
        std::random_device rd;
        std::mt19937 g(rd());
        std::shuffle(data_array.begin(), data_array.end(), g);
        size_t start_index = 0;
        size_t num = src_size - start_index;

        col_vec_target->insert_many_dict_data(data_array.data(), start_index, dict.data(), num,
                                              dict.size());
        EXPECT_EQ(col_vec_target->size(), actual_clone_count + num);
        for (size_t i = 0; i != actual_clone_count; ++i) {
            auto real_data = col_vec_target->get_data_at(i);
            auto expect_data = source_column->get_data_at(i);
            if (real_data != expect_data) {
                std::cout << "index: " << i << ", real_data: " << real_data.to_string()
                          << "\nexpect_data: " << expect_data.to_string() << std::endl;
            }
            EXPECT_EQ(real_data, expect_data);
        }
        for (size_t i = 0; i != num; ++i) {
            auto real_data = col_vec_target->get_data_at(actual_clone_count + i);
            auto expected_data = source_column->get_data_at(data_array[start_index + i]);
            EXPECT_EQ(real_data, expected_data);
        }
    };
    test_func(0, ColumnString(), column_str32);
    test_func(10, ColumnString(), column_str32);
    test_func(0, ColumnString64(), column_str64);
    test_func(10, ColumnString64(), column_str64);

    test_func(0, ColumnString(), column_str32_json);
    test_func(10, ColumnString(), column_str32_json);
    test_func(0, ColumnString64(), column_str64_json);
    test_func(10, ColumnString64(), column_str64_json);
}
TEST_F(ColumnStringTest, pop_back_test) {
    column_string_common_test(assert_column_vector_pop_back_callback, false);
}
TEST_F(ColumnStringTest, ser_deser_test) {
    {
        MutableColumns columns;
        columns.push_back(column_str32->get_ptr());
        ser_deserialize_with_arena_impl(columns, {dt_str});
    }
    {
        MutableColumns columns;
        columns.push_back(column_str64->get_ptr());
        ser_deserialize_with_arena_impl(columns, {dt_str});
    }

    {
        MutableColumns columns;
        columns.push_back(column_str32_json->get_ptr());
        ser_deserialize_with_arena_impl(columns, {dt_jsonb});
    }
    {
        MutableColumns columns;
        columns.push_back(column_str64_json->get_ptr());
        ser_deserialize_with_arena_impl(columns, {dt_jsonb});
    }
}
TEST_F(ColumnStringTest, ser_deser_vec_test) {
    column_string_common_test(assert_column_vector_serialize_vec_callback, false);
}
TEST_F(ColumnStringTest, get_max_row_byte_size) {
    {
        size_t max_size = 0;
        size_t num_rows = column_str32->size();
        for (size_t i = 0; i != num_rows; ++i) {
            max_size = std::max<size_t>(max_size, column_str32->size_at(i));
        }

        EXPECT_EQ(column_str32->get_max_row_byte_size(), max_size + sizeof(uint32_t));
    }
    {
        size_t max_size = 0;
        size_t num_rows = column_str64->size();
        for (size_t i = 0; i != num_rows; ++i) {
            max_size = std::max<size_t>(max_size, column_str64->size_at(i));
        }

        EXPECT_EQ(column_str64->get_max_row_byte_size(), max_size + sizeof(uint32_t));
    }

    {
        size_t max_size = 0;
        size_t num_rows = column_str32_json->size();
        for (size_t i = 0; i != num_rows; ++i) {
            max_size = std::max<size_t>(max_size, column_str32_json->size_at(i));
        }

        EXPECT_EQ(column_str32_json->get_max_row_byte_size(), max_size + sizeof(uint32_t));
    }
    {
        size_t max_size = 0;
        size_t num_rows = column_str64_json->size();
        for (size_t i = 0; i != num_rows; ++i) {
            max_size = std::max<size_t>(max_size, column_str64_json->size_at(i));
        }

        EXPECT_EQ(column_str64_json->get_max_row_byte_size(), max_size + sizeof(uint32_t));
    }
}
TEST_F(ColumnStringTest, update_xxHash_with_value) {
    hash_common_test("update_xxHash_with_value", assert_update_xxHash_with_value_callback);
}
TEST_F(ColumnStringTest, update_sip_hash_with_value_test) {
    hash_common_test("update_sip_hash_with_value",
                     assert_column_vector_update_siphashes_with_value_callback);
}
TEST_F(ColumnStringTest, update_hashes_with_value_test) {
    hash_common_test("update_hashes_with_value",
                     assert_column_vector_update_hashes_with_value_callback);
}
TEST_F(ColumnStringTest, update_crc_with_value_test) {
    hash_common_test("update_crc_with_value", assert_update_crc_with_value_callback);
}
TEST_F(ColumnStringTest, update_crcs_with_value_test) {
    std::string function_name = "update_crcs_with_value";
    {
        MutableColumns columns;
        columns.push_back(column_str32->get_ptr());
        DataTypeSerDeSPtrs serdes = {dt_str->get_serde()};
        std::vector<PrimitiveType> pts(columns.size(), PrimitiveType::TYPE_STRING);
        assert_column_vector_update_crc_hashes_callback(
                columns, serdes, pts, test_result_dir + "/column_str32_" + function_name + ".out");
    }
    {
        MutableColumns columns;
        columns.push_back(column_str64->get_ptr());
        DataTypeSerDeSPtrs serdes = {dt_str->get_serde()};
        std::vector<PrimitiveType> pts(columns.size(), PrimitiveType::TYPE_STRING);
        assert_column_vector_update_crc_hashes_callback(
                columns, serdes, pts, test_result_dir + "/column_str64_" + function_name + ".out");
    }
    {
        MutableColumns columns;
        columns.push_back(column_str32_json->get_ptr());
        DataTypeSerDeSPtrs serdes = {dt_jsonb->get_serde()};
        std::vector<PrimitiveType> pts(columns.size(), PrimitiveType::TYPE_JSONB);
        assert_column_vector_update_crc_hashes_callback(
                columns, serdes, pts,
                test_result_dir + "/column_str32_json_" + function_name + ".out");
    }
    {
        MutableColumns columns;
        columns.push_back(column_str64_json->get_ptr());
        DataTypeSerDeSPtrs serdes = {dt_jsonb->get_serde()};
        std::vector<PrimitiveType> pts(columns.size(), PrimitiveType::TYPE_JSONB);
        assert_column_vector_update_crc_hashes_callback(
                columns, serdes, pts,
                test_result_dir + "/column_str64_json_" + function_name + ".out");
    }
}
TEST_F(ColumnStringTest, insert_range_from) {
    column_string_common_test(assert_column_vector_insert_range_from_callback, false);
}
TEST_F(ColumnStringTest, insert_range_from_ignore_overflow) {
    column_string_common_test(assert_column_vector_insert_range_from_ignore_overflow_callback,
                              false);
}
TEST_F(ColumnStringTest, insert_indices_from) {
    auto test_func = [](auto& target_column, const auto& source_column) {
        // Test case 1: Empty source column
        // Test case 2: Empty indices array
        // Test case 3: Normal case with multiple indices
        // Select elements in different order
        // Test case 4: Duplicate indices

        auto src_size = source_column->size();

        // Test case 1: Empty target column
        {
            auto tmp_target_column = target_column->clone_empty();
            std::vector<uint32_t> indices;

            // empty indices array
            tmp_target_column->insert_indices_from(*source_column, indices.data(), indices.data());
            EXPECT_EQ(tmp_target_column->size(), 0);
        }
        auto test_func2 = [&](size_t clone_count) {
            size_t actual_clone_count = std::min(clone_count, src_size);
            {
                auto tmp_target_column = target_column->clone_resized(actual_clone_count);
                // insert all elements from source column
                std::vector<uint32_t> indices(src_size);
                std::iota(indices.begin(), indices.end(), 0);
                tmp_target_column->insert_indices_from(*source_column, indices.data(),
                                                       indices.data() + src_size);
                EXPECT_EQ(tmp_target_column->size(), actual_clone_count + indices.size());
                size_t j = 0;
                for (j = 0; j != actual_clone_count; ++j) {
                    EXPECT_EQ(tmp_target_column->get_data_at(j), target_column->get_data_at(j));
                }
                for (size_t k = 0; j < actual_clone_count + indices.size(); ++j, ++k) {
                    EXPECT_EQ(tmp_target_column->get_data_at(j),
                              source_column->get_data_at(indices[k]));
                }
            }
            {
                // Normal case with random indices
                auto tmp_target_column = target_column->clone_resized(actual_clone_count);
                std::vector<uint32_t> indices(src_size);
                std::iota(indices.begin(), indices.end(), 0);
                std::random_device rd;
                std::mt19937 g(rd());
                std::shuffle(indices.begin(), indices.end(), g);
                tmp_target_column->insert_indices_from(*source_column, indices.data(),
                                                       indices.data() + indices.size());
                EXPECT_EQ(tmp_target_column->size(), actual_clone_count + indices.size());
                size_t j = 0;
                for (j = 0; j != actual_clone_count; ++j) {
                    EXPECT_EQ(tmp_target_column->get_data_at(j), target_column->get_data_at(j));
                }
                for (size_t k = 0; j < actual_clone_count + indices.size(); ++j, ++k) {
                    EXPECT_EQ(tmp_target_column->get_data_at(j),
                              source_column->get_data_at(indices[k]));
                }
            }
            {
                // Normal case with duplicate indices
                auto tmp_target_column = target_column->clone_resized(actual_clone_count);
                std::vector<uint32_t> indices = {0, uint32_t(source_column->size() - 1),
                                                 uint32_t((source_column->size() + 1) >> 1),
                                                 uint32_t(source_column->size() - 1), 0};
                tmp_target_column->insert_indices_from(*source_column, indices.data(),
                                                       indices.data() + indices.size());
                EXPECT_EQ(tmp_target_column->size(), actual_clone_count + indices.size());
                size_t j = 0;
                for (j = 0; j != actual_clone_count; ++j) {
                    EXPECT_EQ(tmp_target_column->get_data_at(j), target_column->get_data_at(j));
                }
                for (size_t k = 0; j < actual_clone_count + indices.size(); ++j, ++k) {
                    EXPECT_EQ(tmp_target_column->get_data_at(j),
                              source_column->get_data_at(indices[k]));
                }
            }
        };
        test_func2(0);
        test_func2(10);
    };
    test_func(column_str32, column_str32);
    test_func(column_str32, column_str64);
    test_func(column_str64, column_str32);
    test_func(column_str64, column_str64);

    test_func(column_str32, column_str32_json);
    test_func(column_str32, column_str64_json);
    test_func(column_str64, column_str32_json);
    test_func(column_str64, column_str64_json);
}
TEST_F(ColumnStringTest, filter) {
    column_string_common_test(assert_column_vector_filter_callback, true);
    {
        IColumn::Filter filter;
        EXPECT_THROW(column_str64->filter(filter, column_str64->size()), Exception);
        EXPECT_THROW(column_str64->filter(filter), Exception);
    }
}
TEST_F(ColumnStringTest, filter_by_selector) {
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
        std::cout << "selection count: " << sel_size << ", indices: ";
        for (auto i : indices) {
            std::cout << i << ",";
        }
        std::cout << std::endl;

        auto status =
                source_column->filter_by_selector(indices.data(), sel_size, target_column.get());
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(target_column->size(), sel_size);
        for (size_t i = 0; i != sel_size; ++i) {
            auto real_data = target_column->get_data_at(i);
            auto expect_data = source_column->get_data_at(indices[i]);
            if (real_data != expect_data) {
                std::cout << "index: " << i << ", real_data: " << real_data.to_string()
                          << "\nexpect_data: " << expect_data.to_string() << std::endl;
            }
            EXPECT_EQ(real_data, expect_data);
        }
    };
    test_func(column_str32);
    test_func(column_str32_json);
    {
        auto target_column = column_str64->clone_empty();
        std::vector<uint16_t> indices(10, 0);
        auto status = column_str64->filter_by_selector(indices.data(), 10, target_column.get());
        EXPECT_FALSE(status.ok());
    }
    {
        auto target_column = column_str64_json->clone_empty();
        std::vector<uint16_t> indices(10, 0);
        auto status = column_str64_json->filter_by_selector(indices.data(), 0, target_column.get());
        EXPECT_FALSE(status.ok());
    }
}
TEST_F(ColumnStringTest, permute) {
    {
        // test empty column and limit == 0
        IColumn::Permutation permutation(0);
        auto col = column_str32->clone_empty();
        col->permute(permutation, 0);
        EXPECT_EQ(col->size(), 0);
    }
    {
        IColumn::Permutation permutation(0);
        auto col = column_str64->clone_empty();
        col->permute(permutation, 0);
        EXPECT_EQ(col->size(), 0);
    }
    {
        IColumn::Permutation permutation(0);
        EXPECT_THROW(column_str32->permute(permutation, 10), Exception);
        EXPECT_THROW(column_str64->permute(permutation, 10), Exception);
    }
    {
        // test empty column and limit == 0
        IColumn::Permutation permutation(0);
        auto col = column_str32_json->clone_empty();
        col->permute(permutation, 0);
        EXPECT_EQ(col->size(), 0);
    }
    {
        IColumn::Permutation permutation(0);
        auto col = column_str64_json->clone_empty();
        col->permute(permutation, 0);
        EXPECT_EQ(col->size(), 0);
    }
    {
        IColumn::Permutation permutation(0);
        EXPECT_THROW(column_str32_json->permute(permutation, 10), Exception);
        EXPECT_THROW(column_str64_json->permute(permutation, 10), Exception);
    }
    MutableColumns columns;
    columns.push_back(column_str32->get_ptr());
    columns.push_back(column_str64->get_ptr());
    columns.push_back(column_str32_json->get_ptr());
    columns.push_back(column_str64_json->get_ptr());
    assert_column_vector_permute(columns, 0);
    assert_column_vector_permute(columns, 1);
    assert_column_vector_permute(columns, column_str32->size());
    assert_column_vector_permute(columns, UINT64_MAX);
}
TEST_F(ColumnStringTest, insert_default) {
    column_string_common_test(assert_column_vector_insert_default_callback, false);
}

TEST_F(ColumnStringTest, insert_many_default) {
    column_string_common_test(assert_column_vector_insert_many_defaults_callback, false);
}
TEST_F(ColumnStringTest, get_permutation) {
    assert_column_permutations2(*column_str32, dt_str);
    assert_column_permutations2(*column_str64, dt_str);
    assert_column_permutations2(*column_str32_json, dt_jsonb);
    assert_column_permutations2(*column_str64_json, dt_jsonb);
}
TEST_F(ColumnStringTest, is_column_string) {
    EXPECT_TRUE(column_str32->is_column_string());
    EXPECT_TRUE(column_str64->is_column_string());
    EXPECT_TRUE(column_str32_json->is_column_string());
    EXPECT_TRUE(column_str64_json->is_column_string());
}
TEST_F(ColumnStringTest, structure_equals) {
    EXPECT_TRUE(column_str32->structure_equals(ColumnString()));
    EXPECT_FALSE(column_str32->structure_equals(*column_str64));
    EXPECT_TRUE(column_str64->structure_equals(ColumnString64()));
    EXPECT_FALSE(column_str64->structure_equals(*column_str32));

    EXPECT_TRUE(column_str32_json->structure_equals(ColumnString()));
    EXPECT_FALSE(column_str32_json->structure_equals(*column_str64_json));
    EXPECT_TRUE(column_str64_json->structure_equals(ColumnString64()));
    EXPECT_FALSE(column_str64_json->structure_equals(*column_str32_json));
    EXPECT_FALSE(column_str32->structure_equals(ColumnInt32()));
}
TEST_F(ColumnStringTest, clear) {
    auto tmp_col = column_str32->clone();
    EXPECT_EQ(tmp_col->size(), column_str32->size());

    auto* tmp_col_str = assert_cast<ColumnString*>(tmp_col.get());
    EXPECT_EQ(tmp_col_str->get_offsets().size(), column_str32->size());
    tmp_col->clear();
    EXPECT_EQ(tmp_col->size(), 0);
    EXPECT_EQ(tmp_col_str->get_offsets().size(), 0);
    EXPECT_EQ(tmp_col_str->get_chars().size(), 0);

    {
        auto tmp_col = column_str32_json->clone();
        EXPECT_EQ(tmp_col->size(), column_str32_json->size());

        auto* tmp_col_str = assert_cast<ColumnString*>(tmp_col.get());
        EXPECT_EQ(tmp_col_str->get_offsets().size(), column_str32_json->size());
        tmp_col->clear();
        EXPECT_EQ(tmp_col->size(), 0);
        EXPECT_EQ(tmp_col_str->get_offsets().size(), 0);
        EXPECT_EQ(tmp_col_str->get_chars().size(), 0);
    }

    {
        auto tmp_col = column_str64->clone();
        EXPECT_EQ(tmp_col->size(), column_str64->size());

        auto* tmp_col_str = assert_cast<ColumnString64*>(tmp_col.get());
        EXPECT_EQ(tmp_col_str->get_offsets().size(), column_str64->size());
        tmp_col->clear();
        EXPECT_EQ(tmp_col->size(), 0);
        EXPECT_EQ(tmp_col_str->get_offsets().size(), 0);
        EXPECT_EQ(tmp_col_str->get_chars().size(), 0);
    }
    {
        auto tmp_col = column_str64_json->clone();
        EXPECT_EQ(tmp_col->size(), column_str64_json->size());

        auto* tmp_col_str = assert_cast<ColumnString64*>(tmp_col.get());
        EXPECT_EQ(tmp_col_str->get_offsets().size(), column_str64_json->size());
        tmp_col->clear();
        EXPECT_EQ(tmp_col->size(), 0);
        EXPECT_EQ(tmp_col_str->get_offsets().size(), 0);
        EXPECT_EQ(tmp_col_str->get_chars().size(), 0);
    }
}
TEST_F(ColumnStringTest, replace_column_data) {
    EXPECT_THROW(column_str32->replace_column_data(ColumnString(), 0, 0), Exception);
    EXPECT_THROW(column_str64->replace_column_data(ColumnString(), 0, 0), Exception);
    EXPECT_THROW(column_str32_json->replace_column_data(ColumnString(), 0, 0), Exception);
    EXPECT_THROW(column_str64_json->replace_column_data(ColumnString(), 0, 0), Exception);
}
TEST_F(ColumnStringTest, compare_internal) {
    column_string_common_test(assert_column_vector_compare_internal_callback, false);
}
TEST_F(ColumnStringTest, convert_column_if_overflow) {
    {
        auto tmp_col = ColumnString::create();
        tmp_col->insert_data("abc", 3);
        auto src_size = tmp_col->size();
        auto tmp_col_converted = tmp_col->convert_column_if_overflow();
        EXPECT_TRUE(tmp_col_converted->is_column_string());
        EXPECT_FALSE(tmp_col_converted->is_column_string64());
        for (size_t i = 0; i < src_size; ++i) {
            EXPECT_EQ(tmp_col_converted->get_data_at(i), tmp_col->get_data_at(i));
        }
    }
    {
        auto tmp_col = column_str32->clone();
        auto* tmp_col_str32 = assert_cast<ColumnString*>(tmp_col.get());
        auto src_size = column_str32->size();
        auto chars_size = column_str32->get_chars().size();
        auto max_chars_size = config::string_overflow_size;
        while (chars_size < max_chars_size) {
            tmp_col->insert_range_from_ignore_overflow(*column_str32, 0, column_str32->size());
            chars_size = tmp_col_str32->get_chars().size();
        }
        tmp_col->insert_range_from_ignore_overflow(*column_str32, 0, column_str32->size());
        auto tmp_col_row_count = tmp_col->size();
        chars_size = tmp_col_str32->get_chars().size();
        EXPECT_GT(chars_size, max_chars_size);
        auto tmp_col_converted = tmp_col->convert_column_if_overflow();
        EXPECT_TRUE(tmp_col_converted->is_column_string64());
        for (size_t i = 0; i < tmp_col_row_count; ++i) {
            EXPECT_EQ(tmp_col_converted->get_data_at(i), column_str32->get_data_at(i % src_size));
        }
    }

    {
        auto tmp_col = column_str32_json->clone();
        auto* tmp_col_str32 = assert_cast<ColumnString*>(tmp_col.get());
        auto src_size = column_str32_json->size();
        auto chars_size = column_str32_json->get_chars().size();
        auto max_chars_size = config::string_overflow_size;
        while (chars_size < max_chars_size) {
            tmp_col->insert_range_from_ignore_overflow(*column_str32_json, 0,
                                                       column_str32_json->size());
            chars_size = tmp_col_str32->get_chars().size();
        }
        tmp_col->insert_range_from_ignore_overflow(*column_str32_json, 0,
                                                   column_str32_json->size());
        auto tmp_col_row_count = tmp_col->size();
        chars_size = tmp_col_str32->get_chars().size();
        EXPECT_GT(chars_size, max_chars_size);
        auto tmp_col_converted = tmp_col->convert_column_if_overflow();
        EXPECT_TRUE(tmp_col_converted->is_column_string64());
        for (size_t i = 0; i < tmp_col_row_count; ++i) {
            EXPECT_EQ(tmp_col_converted->get_data_at(i),
                      column_str32_json->get_data_at(i % src_size));
        }
    }

    {
        auto tmp_col = column_str64_json->clone();
        auto* tmp_col_str64 = assert_cast<ColumnString64*>(tmp_col.get());
        auto src_size = column_str64_json->size();
        auto chars_size = column_str64_json->get_chars().size();
        auto max_chars_size = config::string_overflow_size;
        while (chars_size < max_chars_size) {
            tmp_col->insert_range_from_ignore_overflow(*column_str64_json, 0,
                                                       column_str64_json->size());
            chars_size = tmp_col_str64->get_chars().size();
        }
        tmp_col->insert_range_from_ignore_overflow(*column_str64_json, 0,
                                                   column_str64_json->size());
        auto tmp_col_row_count = tmp_col->size();
        chars_size = tmp_col_str64->get_chars().size();
        EXPECT_GT(chars_size, max_chars_size);
        auto tmp_col_converted = tmp_col->convert_column_if_overflow();
        EXPECT_TRUE(tmp_col_converted->is_column_string64());
        for (size_t i = 0; i < tmp_col_row_count; ++i) {
            EXPECT_EQ(tmp_col_converted->get_data_at(i),
                      column_str64_json->get_data_at(i % src_size));
        }
    }
}
TEST_F(ColumnStringTest, resize) {
    auto test_func = [](const auto& source_column) {
        auto source_size = source_column->size();
        auto tmp_col = source_column->clone();
        size_t add_count = 10;
        tmp_col->resize(source_size + add_count);
        EXPECT_EQ(tmp_col->size(), source_size + add_count);
        for (size_t i = 0; i != source_size; ++i) {
            EXPECT_EQ(tmp_col->get_data_at(i), source_column->get_data_at(i));
        }
        for (size_t i = 0; i != add_count; ++i) {
            EXPECT_EQ(tmp_col->get_data_at(source_size + i).to_string(), "");
        }
    };
    test_func(column_str32);
    test_func(column_str64);
    test_func(column_str32_json);
    test_func(column_str64_json);
}
TEST_F(ColumnStringTest, TestConcat) {
    Block block;
    vectorized::DataTypePtr str_type = std::make_shared<vectorized::DataTypeString>();

    auto str_col0 = ColumnString::create();
    std::vector<std::string> vals0 = {"aaa", "bb", "cccc"};
    for (auto& v : vals0) {
        str_col0->insert_data(v.data(), v.size());
    }
    block.insert({std::move(str_col0), str_type, "test_str_col0"});

    auto str_col1 = ColumnString::create();
    std::vector<std::string> vals1 = {"3", "2", "4"};
    for (auto& v : vals1) {
        str_col1->insert_data(v.data(), v.size());
    }
    block.insert({std::move(str_col1), str_type, "test_str_col1"});

    auto str_col_res = ColumnString::create();
    block.insert({std::move(str_col_res), str_type, "test_str_res"});

    ColumnNumbers arguments = {0, 1};

    FunctionStringConcat func_concat;
    auto fn_ctx = FunctionContext::create_context(nullptr, nullptr, {});
    {
        auto status =
                func_concat.open(fn_ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        EXPECT_TRUE(status.ok());
    }
    {
        auto status = func_concat.execute_impl(fn_ctx.get(), block, arguments, 2, 3);
        EXPECT_TRUE(status.ok());
    }

    auto actual_res_col = block.get_by_position(2).column;
    EXPECT_EQ(actual_res_col->size(), 3);
    auto actual_res_col_str = assert_cast<const ColumnString*>(actual_res_col.get());
    actual_res_col_str->sanity_check();
}

TEST_F(ColumnStringTest, TestStringInsert) {
    {
        auto str32_column = ColumnString::create();
        std::vector<std::string> vals_tmp = {"x", "yy", "zzz", ""};
        auto str32_column_tmp = ColumnString::create();
        for (auto& v : vals_tmp) {
            str32_column_tmp->insert_data(v.data(), v.size());
        }
        str32_column->insert_range_from(*str32_column_tmp, 0, vals_tmp.size());
        str32_column->insert_range_from(*str32_column_tmp, 0, vals_tmp.size());
        auto row_count = str32_column->size();
        EXPECT_EQ(row_count, vals_tmp.size() * 2);
        for (size_t i = 0; i < row_count; ++i) {
            auto row_data = str32_column->get_data_at(i);
            EXPECT_EQ(row_data.to_string(), vals_tmp[i % vals_tmp.size()]);
        }
    }

    {
        // test insert ColumnString64 to ColumnString
        auto str32_column = ColumnString::create();
        std::vector<std::string> vals_tmp = {"x", "yy", "zzz", ""};
        auto str64_column_tmp = ColumnString64::create();
        for (auto& v : vals_tmp) {
            str64_column_tmp->insert_data(v.data(), v.size());
        }
        str32_column->insert_range_from(*str64_column_tmp, 0, vals_tmp.size());
        str32_column->insert_range_from(*str64_column_tmp, 0, vals_tmp.size());
        auto row_count = str32_column->size();
        EXPECT_EQ(row_count, vals_tmp.size() * 2);
        for (size_t i = 0; i < row_count; ++i) {
            auto row_data = str32_column->get_data_at(i);
            EXPECT_EQ(row_data.to_string(), vals_tmp[i % vals_tmp.size()]);
        }
    }
}
TEST_F(ColumnStringTest, shrink_padding_chars) {
    ColumnString::MutablePtr col = ColumnString::create();
    col->shrink_padding_chars();

    col->insert_data("123\0   ", 7);
    col->insert_data("456\0xx", 6);
    col->insert_data("78", 2);
    col->shrink_padding_chars();

    EXPECT_EQ(col->size(), 3);
    EXPECT_EQ(col->get_data_at(0), StringRef("123"));
    EXPECT_EQ(col->get_data_at(0).size, 3);
    EXPECT_EQ(col->get_data_at(1), StringRef("456"));
    EXPECT_EQ(col->get_data_at(1).size, 3);
    EXPECT_EQ(col->get_data_at(2), StringRef("78"));
    EXPECT_EQ(col->get_data_at(2).size, 2);

    col->insert_data("xyz", 2); // only xy

    EXPECT_EQ(col->size(), 4);
    EXPECT_EQ(col->get_data_at(3), StringRef("xy"));
}
TEST_F(ColumnStringTest, sort_column) {
    column_string_common_test(assert_sort_column_callback, false);
}

TEST_F(ColumnStringTest, ScalaTypeStringTesterase) {
    auto column = ColumnString::create();
    std::vector<StringRef> data = {StringRef("asd"), StringRef("1234567"), StringRef("3"),
                                   StringRef("4"), StringRef("5")};
    for (auto d : data) {
        column->insert_data(d.data, d.size);
    }
    column->erase(0, 2);
    EXPECT_EQ(column->size(), 3);
    for (int i = 0; i < column->size(); ++i) {
        std::cout << column->get_data_at(i).to_string() << std::endl;
        EXPECT_EQ(column->get_data_at(i).to_string(), data[i + 2].to_string());
    }

    auto column2 = ColumnString::create();
    std::vector<StringRef> data2 = {StringRef(""), StringRef("1234567"), StringRef("asd"),
                                    StringRef("4"), StringRef("5")};
    for (auto d : data2) {
        column2->insert_data(d.data, d.size);
    }
    column2->erase(0, 2);
    EXPECT_EQ(column2->size(), 3);
    for (int i = 0; i < column2->size(); ++i) {
        std::cout << column2->get_data_at(i).to_string() << std::endl;
        EXPECT_EQ(column2->get_data_at(i).to_string(), data2[i + 2].to_string());
    }
}

TEST_F(ColumnStringTest, ScalaTypeStringTest2erase) {
    auto column = ColumnString::create();
    std::vector<StringRef> data = {StringRef("asd"), StringRef("1234567"), StringRef("3"),
                                   StringRef("4"), StringRef("5")};
    std::vector<StringRef> res = {StringRef("asd"), StringRef("1234567"), StringRef("5")};
    for (auto d : data) {
        column->insert_data(d.data, d.size);
    }
    column->erase(2, 2);
    EXPECT_EQ(column->size(), 3);
    for (int i = 0; i < column->size(); ++i) {
        std::cout << column->get_data_at(i).to_string() << std::endl;
        EXPECT_EQ(column->get_data_at(i).to_string(), res[i].to_string());
    }

    auto column2 = ColumnString::create();
    std::vector<StringRef> data2 = {StringRef(""), StringRef("1234567"), StringRef("asd"),
                                    StringRef("4"), StringRef("5")};
    std::vector<StringRef> res2 = {StringRef(""), StringRef("1234567"), StringRef("5")};
    for (auto d : data2) {
        column2->insert_data(d.data, d.size);
    }
    column2->erase(2, 2);
    EXPECT_EQ(column2->size(), 3);
    for (int i = 0; i < column2->size(); ++i) {
        std::cout << column2->get_data_at(i).to_string() << std::endl;
        EXPECT_EQ(column2->get_data_at(i).to_string(), res2[i].to_string());
    }
}

TEST_F(ColumnStringTest, is_ascii) {
    {
        auto column = ColumnString::create();
        std::vector<StringRef> data = {StringRef("asd"), StringRef("1234567"), StringRef("3"),
                                       StringRef("4"), StringRef("5")};
        for (auto d : data) {
            column->insert_data(d.data, d.size);
        }
        EXPECT_TRUE(column->is_ascii());
    }

    {
        auto column = ColumnString::create();
        std::vector<StringRef> data = {StringRef("asd"), StringRef("1234567"),
                                       StringRef("3"),   StringRef("4"),
                                       StringRef("5"),   StringRef("你好世界")};
        for (auto d : data) {
            column->insert_data(d.data, d.size);
        }
        EXPECT_FALSE(column->is_ascii());
    }
    {
        auto column = ColumnString::create();
        std::vector<StringRef> data = {StringRef(""), StringRef(""), StringRef(""),
                                       StringRef(""), StringRef(""), StringRef("")};
        EXPECT_TRUE(column->is_ascii());
    }
}

} // namespace doris::vectorized