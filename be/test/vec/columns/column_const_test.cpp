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
#include <gtest/gtest.h>
#include <testutil/column_helper.h>
#include <vec/columns/column_array.h>
#include <vec/columns/column_const.h>
#include <vec/columns/column_vector.h>
#include <vec/data_types/data_type_array.h>
#include <vec/data_types/data_type_number.h>

#include <vector>

#include "vec/columns/column.h"

namespace doris::vectorized {

TEST(ColumnConstTest, TestCreate) {
    auto column_data = ColumnHelper::create_column<DataTypeInt64>({7});
    auto column_const = ColumnConst::create(column_data, 10);
    auto column_const2 = ColumnConst::create(std::move(column_const), 12);
    EXPECT_EQ(column_const2->size(), 12);

    EXPECT_TRUE(!is_column_const(column_const2->get_data_column()));
}

TEST(ColumnConstTest, TestFilter) {
    {
        auto column_data = ColumnHelper::create_column<DataTypeInt64>({7});
        auto column_const = ColumnConst::create(column_data, 3);
        IColumn::Filter filter = {1, 0, 1};

        auto res = column_const->filter(filter, 2);
        EXPECT_EQ(res->size(), 2);
        EXPECT_EQ(assert_cast<const ColumnConst&>(*res).get_data_column_ptr()->size(), 1);
        EXPECT_EQ(assert_cast<const ColumnConst&>(*res).get_data_column_ptr()->get_int(0), 7);
    }

    {
        auto column_data = ColumnHelper::create_column<DataTypeInt64>({7});
        auto column_const = ColumnConst::create(column_data, 3);
        IColumn::Filter filter = {1, 0, 1};

        auto size = column_const->filter(filter);
        EXPECT_EQ(size, 2);
        EXPECT_EQ(assert_cast<const ColumnConst&>(*column_const).get_data_column_ptr()->size(), 1);
        EXPECT_EQ(assert_cast<const ColumnConst&>(*column_const).get_data_column_ptr()->get_int(0),
                  7);
    }
}

TEST(ColumnConstTest, TestPermutation) {
    {
        auto column_data = ColumnHelper::create_column<DataTypeInt64>({7});
        auto column_const = ColumnConst::create(column_data, 3);
        IColumn::Permutation perm = {2, 1, 0};
        auto res = column_const->permute(perm, 3);
        EXPECT_EQ(res->size(), 3);
    }

    {
        auto column_data = ColumnHelper::create_column<DataTypeInt64>({7});
        auto column_const = ColumnConst::create(column_data, 3);
        IColumn::Permutation perm = {2, 1, 0};
        auto res = column_const->permute(perm, 0);
        EXPECT_EQ(res->size(), 3);
    }
}

TEST(ColumnConstTest, Testget_permutation) {
    auto column_data = ColumnHelper::create_column<DataTypeInt64>({7});
    auto column_const = ColumnConst::create(column_data, 3);
    IColumn::Permutation res;
    column_const->get_permutation(false, 0, 0, res);
    EXPECT_EQ(res.size(), 3);
    for (size_t i = 0; i < res.size(); ++i) {
        EXPECT_EQ(res[i], i);
    }
}

TEST(ColumnConstTest, is_variable_length) {
    auto column_data = ColumnHelper::create_column<DataTypeInt64>({7});
    auto column_const = ColumnConst::create(column_data, 3);
    EXPECT_FALSE(column_const->is_variable_length());
}

TEST(ColumnConstTest, get_and_insert) {
    auto column_data = ColumnHelper::create_column<DataTypeInt64>({7});
    auto column_const = ColumnConst::create(column_data, 3);
    EXPECT_EQ(column_const->get_data_column().get_int(0), 7);

    {
        auto dummy_column = ColumnHelper::create_column<DataTypeInt64>({6});
        try {
            column_const->insert_range_from(*dummy_column, 0, 5);
            EXPECT_FALSE(true);
        } catch (const Exception& e) {
            EXPECT_EQ(e.code(), ErrorCode::INTERNAL_ERROR);
        }

        try {
            column_const->insert_many_from(*dummy_column, 0, 5);
            EXPECT_FALSE(true);
        } catch (const Exception& e) {
            EXPECT_EQ(e.code(), ErrorCode::INTERNAL_ERROR);
        }

        try {
            column_const->insert_from(*dummy_column, 0);
            EXPECT_FALSE(true);
        } catch (const Exception& e) {
            EXPECT_EQ(e.code(), ErrorCode::INTERNAL_ERROR);
        }

        try {
            std::vector<uint32_t> indices = {0, 1, 2, 3, 4};
            column_const->insert_indices_from(*dummy_column, indices.data(),
                                              indices.data() + indices.size());
            EXPECT_FALSE(true);
        } catch (const Exception& e) {
            EXPECT_EQ(e.code(), ErrorCode::INTERNAL_ERROR);
        }
    }

    auto dummy_column = ColumnConst::create(column_data, 3);
    column_const->insert_range_from(*dummy_column, 0, 5);
    EXPECT_EQ(column_const->size(), 8);

    column_const->insert_many_from(*dummy_column, 0, 5);
    EXPECT_EQ(column_const->size(), 13);

    std::vector<uint32_t> indices = {0, 1, 2, 3, 4};
    column_const->insert_indices_from(*dummy_column, indices.data(),
                                      indices.data() + indices.size());
    EXPECT_EQ(column_const->size(), 18);

    Field field;
    column_const->insert(field);
    EXPECT_EQ(column_const->size(), 19);

    column_const->insert_data(reinterpret_cast<const char*>(&field), 0);
    EXPECT_EQ(column_const->size(), 20);

    column_const->insert_from(*dummy_column, 0);
    EXPECT_EQ(column_const->size(), 21);

    column_const->insert_default();
    EXPECT_EQ(column_const->size(), 22);

    column_const->pop_back(2);
    EXPECT_EQ(column_const->size(), 20);

    EXPECT_EQ(column_const->get_max_row_byte_size(), 8);
}

TEST(ColumnConstTest, serialize_deserialize) {
    auto column_data = ColumnHelper::create_column<DataTypeInt64>({7});
    auto column_const = ColumnConst::create(column_data, 3);
    Arena arena;
    char const* begin = nullptr;
    StringRef ref = column_const->serialize_value_into_arena(0, arena, begin);
    EXPECT_EQ(ref.size, 8);

    auto new_column_const = ColumnConst::create(column_data, 3);
    const char* pos = new_column_const->deserialize_and_insert_from_arena(ref.data);
    EXPECT_EQ(pos, ref.data + ref.size);
    EXPECT_EQ(new_column_const->size(), 4);
}

TEST(ColumnConstTest, update_xxHash_with_value) {
    {
        auto column_data = ColumnHelper::create_column<DataTypeInt64>({7});
        auto column_const = ColumnConst::create(column_data, 3);
        uint64_t hash = 0;
        column_const->update_xxHash_with_value(0, 1, hash, nullptr);
        std::cout << "hash: " << hash << std::endl;
        EXPECT_EQ(hash, 9324454920402081455ULL);
    }
    {
        auto column_data = ColumnHelper::create_nullable_column<DataTypeInt64>({7}, {true});
        auto column_const = ColumnConst::create(column_data, 3);
        uint64_t hash = 0;
        column_const->update_xxHash_with_value(0, 1, hash, nullptr);
        std::cout << "hash: " << hash << std::endl;
        EXPECT_EQ(hash, 5238470482016868669ULL);
    }

    {
        auto column_data = ColumnHelper::create_column<DataTypeInt64>({7});
        auto column_const = ColumnConst::create(column_data, 3);
        uint32_t hash = 0;
        column_const->update_crc_with_value(0, 1, hash, nullptr);
        std::cout << "hash: " << hash << std::endl;
        EXPECT_EQ(hash, 1877464688U);
    }
    {
        auto column_data = ColumnHelper::create_column<DataTypeInt64>({7});
        auto column_const = ColumnConst::create(column_data, 3);
        SipHash hash;
        column_const->update_hash_with_value(0, hash);
        EXPECT_EQ(hash.get64(), 13180436571133193200ULL);
    }
}

TEST(ColumnConstTest, compare_at) {
    {
        auto column_data = ColumnHelper::create_column<DataTypeInt64>({7});
        auto column_const = ColumnConst::create(column_data, 3);
        EXPECT_EQ(column_const->compare_at(0, 0, *column_const, 1), 0);
        EXPECT_EQ(column_const->compare_at(0, 1, *column_const, 1), 0);
    }

    {
        auto column_data = ColumnHelper::create_column<DataTypeInt64>({7});
        auto column_const = ColumnConst::create(column_data, 3);
        auto column_data2 = ColumnHelper::create_column<DataTypeInt64>({8});
        auto column_const2 = ColumnConst::create(column_data2, 3);
        EXPECT_EQ(column_const->compare_at(0, 0, *column_const2, 1), -1);
    }
}

TEST(ColumnConstTest, structure_equals) {
    {
        auto column_data = ColumnHelper::create_column<DataTypeInt64>({7});
        auto column_const = ColumnConst::create(column_data, 3);
        EXPECT_TRUE(column_const->structure_equals(*column_const));
    }

    {
        auto column_data = ColumnHelper::create_column<DataTypeInt64>({7});
        auto column_const = ColumnConst::create(column_data, 3);
        auto column_data2 = ColumnHelper::create_column<DataTypeInt64>({8});
        auto column_const2 = ColumnConst::create(column_data2, 3);
        EXPECT_TRUE(column_const->structure_equals(*column_const2));
    }

    {
        auto column_data = ColumnHelper::create_column<DataTypeInt64>({7});
        auto column_const = ColumnConst::create(column_data, 3);
        auto column_data2 = ColumnHelper::create_column<DataTypeInt64>({7});
        auto column_const2 = ColumnConst::create(column_data2, 4);
        EXPECT_TRUE(column_const->structure_equals(*column_const2));
    }
}

TEST(ColumnConstTest, erase) {
    auto column_data = ColumnHelper::create_column<DataTypeInt64>({7});
    auto column_const = ColumnConst::create(column_data, 3);
    column_const->erase(0, 1);
    EXPECT_EQ(column_const->size(), 2);
    EXPECT_EQ(column_const->get_data_column().get_int(0), 7);

    column_const->erase(1, 1);
    EXPECT_EQ(column_const->size(), 1);
    EXPECT_EQ(column_const->get_data_column().get_int(0), 7);

    column_const->erase(0, 1);
    EXPECT_EQ(column_const->size(), 0);
}

TEST(ColumnConstTest, replace_column_data) {
    auto column_data = ColumnHelper::create_column<DataTypeInt64>({7});
    auto column_const = ColumnConst::create(column_data, 3);
    auto column_data2 = ColumnHelper::create_column<DataTypeInt64>({8});
    column_const->replace_column_data(*column_data2, 0, 0);
    EXPECT_EQ(column_const->get_data_column().get_int(0), 8);
    EXPECT_EQ(column_const->size(), 3);
    column_const->finalize();
}
} // namespace doris::vectorized