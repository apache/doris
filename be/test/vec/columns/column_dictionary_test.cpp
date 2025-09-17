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

#include "vec/columns/column_dictionary.h"

#include <gmock/gmock-more-matchers.h>
#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>

#include "common/exception.h"
#include "runtime/primitive_type.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/common_column_test.h"
#include "vec/common/sip_hash.h"
#include "vec/common/string_ref.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_string.h"

using namespace doris;
namespace doris::vectorized {
class ColumnDictionaryTest : public CommonColumnTest {
protected:
    static std::string test_data_dir;
    static std::string test_result_dir;
    static void SetUpTestSuite() {
        // void SetUp() override {
        auto root_dir = std::string(getenv("ROOT"));
        test_data_dir = root_dir + "/be/test/data/vec/columns";
        test_result_dir = root_dir + "/be/test/expected_result/vec/columns";

        dt_str = DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_STRING, 0,
                                                              0);
        dt_int32 =
                DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_INT, 0, 0);

        column_dict_data = ColumnString::create();
        column_dict_indices = ColumnInt32::create();

        column_dict_char = ColumnDictI32::create(FieldType::OLAP_FIELD_TYPE_CHAR);
        EXPECT_TRUE(column_dict_char->is_dict_empty());
        column_dict_varchar = ColumnDictI32::create(FieldType::OLAP_FIELD_TYPE_STRING);
        column_dict_str = ColumnDictI32::create(FieldType::OLAP_FIELD_TYPE_STRING);

        load_columns_data();
    }
    static DataTypePtr dt_str;
    static DataTypePtr dt_int32;

    static ColumnString::MutablePtr column_dict_data;
    static ColumnInt32::MutablePtr column_dict_indices;
    static std::vector<StringRef> dict_array;

    static ColumnDictI32::MutablePtr column_dict_char;
    static ColumnDictI32::MutablePtr column_dict_varchar;
    static ColumnDictI32::MutablePtr column_dict_str;

    static void load_columns_data() {
        std::cout << "loading test dataset" << std::endl;
        {
            MutableColumns columns;
            columns.push_back(column_dict_data->get_ptr());
            DataTypeSerDeSPtrs serde = {dt_str->get_serde()};
            std::string data_file = test_data_dir + "/DICT_STR.csv";
            load_columns_data_from_file(columns, serde, ';', {0}, data_file);
        }
        {
            MutableColumns columns;
            columns.push_back(column_dict_indices->get_ptr());
            DataTypeSerDeSPtrs serde = {dt_int32->get_serde()};
            std::string data_file = test_data_dir + "/DICT_INDICES.csv";
            load_columns_data_from_file(columns, serde, ';', {0}, data_file);
        }
        const auto& dict_indices_data = column_dict_indices->get_data();
        auto dict_data_row_count = column_dict_data->size();
        auto dict_indices_row_count = column_dict_indices->size();
        std::cout << "dict size: " << dict_data_row_count
                  << ", dict indices size: " << dict_indices_row_count << std::endl;

        dict_array.resize(dict_data_row_count);
        for (size_t i = 0; i != dict_data_row_count; ++i) {
            auto dict_item = column_dict_data->get_data_at(i);
            dict_array[i].data = dict_item.data;
            dict_array[i].size = dict_item.size;
        }

        auto check_func = [&](auto& col) {
            col->reserve(dict_indices_row_count);
            col->insert_many_dict_data(column_dict_indices->get_data().data(), 0, dict_array.data(),
                                       dict_indices_row_count, dict_data_row_count);
            (void)col->dict_debug_string();
            EXPECT_EQ(col->dict_size(), dict_data_row_count);
            EXPECT_EQ(col->size(), dict_indices_row_count);
            for (size_t i = 0; i < dict_indices_row_count; ++i) {
                EXPECT_EQ(col->get_value(dict_indices_data[i]), dict_array[dict_indices_data[i]]);
            }
        };
        check_func(column_dict_char);
        check_func(column_dict_varchar);
        check_func(column_dict_str);
    }
};
TEST_F(ColumnDictionaryTest, is_column_dictionary) {
    EXPECT_TRUE(column_dict_char->is_column_dictionary());
    EXPECT_TRUE(column_dict_varchar->is_column_dictionary());
    EXPECT_TRUE(column_dict_str->is_column_dictionary());
}
TEST_F(ColumnDictionaryTest, get_data_at) {
    EXPECT_THROW(column_dict_char->get_data_at(0), Exception);
    EXPECT_THROW(column_dict_varchar->get_data_at(1), Exception);
    EXPECT_THROW(column_dict_str->get_data_at(2), Exception);
}
TEST_F(ColumnDictionaryTest, insert_from) {
    EXPECT_THROW(column_dict_char->insert_from(*column_dict_char, 0), Exception);
    EXPECT_THROW(column_dict_varchar->insert_from(*column_dict_char, 1), Exception);
    EXPECT_THROW(column_dict_str->insert_from(*column_dict_char, 2), Exception);
}
TEST_F(ColumnDictionaryTest, insert_range_from) {
    EXPECT_THROW(column_dict_char->insert_range_from(*column_dict_char, 0, 1), Exception);
    EXPECT_THROW(column_dict_varchar->insert_range_from(*column_dict_char, 1, 2), Exception);
    EXPECT_THROW(column_dict_str->insert_range_from(*column_dict_char, 3, 4), Exception);
}
TEST_F(ColumnDictionaryTest, insert_indices_from) {
    EXPECT_THROW(column_dict_char->insert_indices_from(*column_dict_char, nullptr, nullptr),
                 Exception);
    EXPECT_THROW(column_dict_varchar->insert_indices_from(*column_dict_char, nullptr, nullptr),
                 Exception);
    EXPECT_THROW(column_dict_str->insert_indices_from(*column_dict_char, nullptr, nullptr),
                 Exception);
}
TEST_F(ColumnDictionaryTest, update_hash_with_value) {
    SipHash h;
    EXPECT_THROW(column_dict_char->update_hash_with_value(0, h), Exception);
    EXPECT_THROW(column_dict_varchar->update_hash_with_value(1, h), Exception);
    EXPECT_THROW(column_dict_str->update_hash_with_value(2, h), Exception);
}
TEST_F(ColumnDictionaryTest, insert_data) {
    EXPECT_THROW(column_dict_char->insert_data(nullptr, 0), Exception);
}
/*
TEST_F(ColumnDictionaryTest, insert_default) {
    auto test_func = [](const ColumnDictI32::MutablePtr& source_column) {
        auto* col_vec_src = assert_cast<ColumnDictI32*>(source_column.get());
        auto src_size = source_column->size();

        auto check_func = [&](size_t clone_count) {
            size_t actual_clone_count = std::min(clone_count, src_size);
            auto target_column = source_column->clone_resized(actual_clone_count);
            auto* col_vec_target = assert_cast<ColumnDictI32*>(target_column.get());
            const auto& codes_data = col_vec_target->get_data();
            col_vec_target->insert_default();
            auto target_size = col_vec_target->size();
            EXPECT_EQ(target_size, actual_clone_count + 1);
            size_t i = 0;
            for (; i < actual_clone_count; ++i) {
                EXPECT_EQ(col_vec_target->get_value(codes_data[i]),
                          col_vec_src->get_value(codes_data[i]));
            }
            EXPECT_EQ(col_vec_target->get_value(codes_data[i]), StringRef {});
        };
        check_func(0);
        check_func(10);
    };
    test_func(column_dict_char);
    test_func(column_dict_varchar);
    test_func(column_dict_str);
}
*/
TEST_F(ColumnDictionaryTest, clear) {
    auto target_column = ColumnDictI32::create(FieldType::OLAP_FIELD_TYPE_CHAR);
    target_column->insert_many_dict_data(dict_array.data(), dict_array.size());

    target_column->clear();
    EXPECT_EQ(target_column->size(), 0);
}
TEST_F(ColumnDictionaryTest, byte_size) {
    EXPECT_EQ(column_dict_char->byte_size(), column_dict_char->size() * 4);
}
TEST_F(ColumnDictionaryTest, allocated_bytes) {
    EXPECT_EQ(column_dict_char->allocated_bytes(), column_dict_char->size() * 4);
}
TEST_F(ColumnDictionaryTest, has_enough_capacity) {
    EXPECT_THROW(column_dict_char->has_enough_capacity(
                         ColumnDictI32(FieldType::OLAP_FIELD_TYPE_VARCHAR)),
                 Exception);
}
TEST_F(ColumnDictionaryTest, pop_back) {
    EXPECT_THROW(column_dict_char->pop_back(9), Exception);
}
/*
TEST_F(ColumnDictionaryTest, reserve) {
    auto target_column = column_dict_char->clone();
    target_column->reserve(target_column->size() + 9);
    auto* col_vec_target = assert_cast<ColumnDictI32*>(target_column.get());
    EXPECT_EQ(col_vec_target->get_data().capacity(), target_column->size() + 9);
}
*/
TEST_F(ColumnDictionaryTest, insert) {
    EXPECT_THROW(column_dict_char->insert(Field {}), Exception);
}
TEST_F(ColumnDictionaryTest, field) {
    auto count = column_dict_char->size();
    auto* col_vec = assert_cast<ColumnDictI32*>(column_dict_char.get());
    const auto& codes_data = col_vec->get_data();
    for (size_t i = 0; i != count; ++i) {
        Field f;
        column_dict_char->get(i, f);
        EXPECT_EQ(f.get<int32_t>(), codes_data[i]);
    }
}
TEST_F(ColumnDictionaryTest, serialize_value_into_arena) {
    Arena arena;
    const char* ptr = nullptr;
    EXPECT_THROW(column_dict_char->serialize_value_into_arena(0, arena, ptr), Exception);
}
TEST_F(ColumnDictionaryTest, deserialize_and_insert_from_arena) {
    const char* ptr = nullptr;
    EXPECT_THROW(column_dict_char->deserialize_and_insert_from_arena(ptr), Exception);
}
TEST_F(ColumnDictionaryTest, get_raw_data) {
    EXPECT_THROW(column_dict_char->get_raw_data(), Exception);
}
TEST_F(ColumnDictionaryTest, filter) {
    IColumn::Filter filt;
    EXPECT_THROW(column_dict_char->filter(filt, 10), Exception);
    EXPECT_THROW(column_dict_char->filter(filt), Exception);
}
TEST_F(ColumnDictionaryTest, clone) {
    EXPECT_THROW(column_dict_char->clone(), Exception);
}
TEST_F(ColumnDictionaryTest, permute) {
    IColumn::Permutation perm;
    EXPECT_THROW(column_dict_char->permute(perm, 1), Exception);
}
TEST_F(ColumnDictionaryTest, filter_by_selector) {
    auto test_func = [&](const auto& source_column) {
        auto src_size = source_column->size();
        const auto& codes_data = source_column->get_data();
        EXPECT_TRUE(src_size <= UINT16_MAX);

        auto target_column = ColumnString::create();

        std::vector<uint16_t> indices(src_size);
        std::iota(indices.begin(), indices.end(), 0);
        std::random_device rd;
        std::mt19937 g(rd());
        std::shuffle(indices.begin(), indices.end(), g);
        size_t sel_size = src_size / 2;
        indices.resize(sel_size);

        auto status =
                source_column->filter_by_selector(indices.data(), sel_size, target_column.get());
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(target_column->size(), sel_size);
        for (size_t i = 0; i != sel_size; ++i) {
            auto real_data = target_column->get_data_at(i);
            auto expect_data = source_column->get_value(codes_data[indices[i]]);
            if (real_data != expect_data) {
                std::cout << "index: " << i << ", real_data: " << real_data.to_string()
                          << "\nexpect_data: " << expect_data.to_string() << std::endl;
            }
            EXPECT_EQ(real_data, expect_data);
        }
    };
    test_func(column_dict_char);
}
TEST_F(ColumnDictionaryTest, insert_many_dict_data) {
    ColumnDictI32::MutablePtr tmp_column_dict =
            ColumnDictI32::create(FieldType::OLAP_FIELD_TYPE_CHAR);
    tmp_column_dict->insert_many_dict_data(dict_array.data(), dict_array.size());
    for (size_t i = 0; i != dict_array.size(); ++i) {
        EXPECT_EQ(tmp_column_dict->get_value(i), dict_array[i]);
    }
}
TEST_F(ColumnDictionaryTest, convert_dict_codes_if_necessary) {
    {
        ColumnDictI32::MutablePtr tmp_column_dict =
                ColumnDictI32::create(FieldType::OLAP_FIELD_TYPE_CHAR);
        tmp_column_dict->convert_dict_codes_if_necessary();
        EXPECT_FALSE(tmp_column_dict->is_dict_sorted());
        EXPECT_FALSE(tmp_column_dict->is_dict_code_converted());

        auto dict_data_row_count = column_dict_data->size();
        auto dict_indices_row_count = column_dict_indices->size();
        tmp_column_dict->reserve(dict_indices_row_count);
        tmp_column_dict->insert_many_dict_data(column_dict_indices->get_data().data(), 0,
                                               dict_array.data(), dict_indices_row_count,
                                               dict_data_row_count);
        tmp_column_dict->convert_dict_codes_if_necessary();
        EXPECT_TRUE(tmp_column_dict->is_dict_sorted());
        EXPECT_TRUE(tmp_column_dict->is_dict_code_converted());
    }
}
TEST_F(ColumnDictionaryTest, find_code) {
    auto size = dict_array.size();
    for (size_t i = 0; i != size; ++i) {
        EXPECT_EQ(column_dict_char->find_code(dict_array[i]), i);
    }
}
/*
TEST_F(ColumnDictionaryTest, initialize_hash_values_for_runtime_filter) {
    ColumnDictI32::MutablePtr tmp_column_dict =
            ColumnDictI32::create(FieldType::OLAP_FIELD_TYPE_CHAR);
    auto dict_data_row_count = column_dict_data->size();
    auto dict_indices_row_count = column_dict_indices->size();
    tmp_column_dict->reserve(dict_indices_row_count);
    tmp_column_dict->insert_many_dict_data(column_dict_indices->get_data().data(), 0,
                                           dict_array.data(), dict_indices_row_count,
                                           dict_data_row_count);
    tmp_column_dict->initialize_hash_values_for_runtime_filter();
    for (size_t i = 0; i != dict_data_row_count; ++i) {
        EXPECT_EQ(tmp_column_dict->_dict._compute_hash_value_flags[i], 0);
    }
    for (size_t i = 0; i != dict_data_row_count; ++i) {
        EXPECT_NE(tmp_column_dict->get_hash_value(i), 0);
    }
    for (size_t i = 0; i != dict_data_row_count; ++i) {
        EXPECT_EQ(tmp_column_dict->_dict._compute_hash_value_flags[i], 1);
    }
}
*/
TEST_F(ColumnDictionaryTest, rowset_segment_id) {
    RowsetId rowset_id {1, 2, 3};
    uint32_t segment_id = 100;
    column_dict_char->set_rowset_segment_id({rowset_id, segment_id});
    auto ids = column_dict_char->get_rowset_segment_id();
    EXPECT_EQ(ids.first, rowset_id);
    EXPECT_EQ(ids.second, segment_id);
}
TEST_F(ColumnDictionaryTest, convert_to_predicate_column_if_dictionary) {
    ColumnDictI32::MutablePtr tmp_column_dict =
            ColumnDictI32::create(FieldType::OLAP_FIELD_TYPE_CHAR);
    auto dict_data_row_count = column_dict_data->size();
    auto dict_indices_row_count = column_dict_indices->size();
    tmp_column_dict->reserve(dict_indices_row_count);
    tmp_column_dict->insert_many_dict_data(column_dict_indices->get_data().data(), 0,
                                           dict_array.data(), dict_indices_row_count,
                                           dict_data_row_count);
    auto col_converted = tmp_column_dict->convert_to_predicate_column_if_dictionary();
    for (size_t i = 0; i != dict_indices_row_count; ++i) {
        // EXPECT_EQ(col_converted->get_data_at(i), column_dict_data->get_data_at(i));
    }
}
std::string ColumnDictionaryTest::test_data_dir;
std::string ColumnDictionaryTest::test_result_dir;
DataTypePtr ColumnDictionaryTest::dt_str;
DataTypePtr ColumnDictionaryTest::dt_int32;

ColumnString::MutablePtr ColumnDictionaryTest::column_dict_data;
ColumnInt32::MutablePtr ColumnDictionaryTest::column_dict_indices;
std::vector<StringRef> ColumnDictionaryTest::dict_array;

ColumnDictI32::MutablePtr ColumnDictionaryTest::column_dict_char;
ColumnDictI32::MutablePtr ColumnDictionaryTest::column_dict_varchar;
ColumnDictI32::MutablePtr ColumnDictionaryTest::column_dict_str;
} // namespace doris::vectorized