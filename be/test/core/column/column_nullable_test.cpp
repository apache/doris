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

#include "core/column/column_nullable.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include <atomic>
#include <thread>

#include "common/status.h"
#include "core/block/block.h"
#include "core/column/column_decimal.h"
#include "core/column/column_nullable_test.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/define_primitive_type.h"
#include "core/field.h"
#include "core/types.h"
#include "testutil/column_helper.h"
#include "util/hash_util.hpp"

namespace doris {

TEST(ColumnNullableTest, NullTest) {
    ColumnNullable::MutablePtr null_col = create_column_nullable<TYPE_BIGINT>(500, true);
    EXPECT_TRUE(null_col->has_null());

    ColumnNullable::MutablePtr dst_col = ColumnNullable::create(
            create_nested_column<TYPE_BIGINT>(10), ColumnUInt8::create(10, 0));
    EXPECT_FALSE(dst_col->has_null());

    ColumnInt64::MutablePtr source_col = ColumnInt64::create();
    source_col->insert_range_of_integer(0, 100);

    dst_col->insert(Field());
    EXPECT_TRUE(dst_col->has_null());
    dst_col->clear();
    EXPECT_FALSE(dst_col->has_null());
    dst_col->insert_range_from(
            *ColumnNullable::create(std::move(source_col), ColumnUInt8::create(100, 0)), 5, 5);
    EXPECT_FALSE(dst_col->has_null());

    dst_col->clear();
    EXPECT_FALSE(dst_col->has_null());
    dst_col->insert_many_defaults(10);
    EXPECT_TRUE(dst_col->has_null());

    dst_col->clear();
    EXPECT_FALSE(dst_col->has_null());
    dst_col->insert_from(*null_col, 100);
    EXPECT_TRUE(dst_col->has_null());

    auto tmp_col = ColumnNullable::create(create_nested_column<TYPE_BIGINT>(10),
                                          ColumnUInt8::create(10, 1));

    dst_col->clear();
    EXPECT_FALSE(dst_col->has_null());
    dst_col->insert_from(*tmp_col, 9);
    EXPECT_TRUE(dst_col->has_null());

    dst_col->clear();
    EXPECT_FALSE(dst_col->has_null());
    dst_col->insert_range_from(*tmp_col, 0, 3);
    EXPECT_TRUE(dst_col->has_null());

    dst_col->clear();
    EXPECT_FALSE(dst_col->has_null());
    dst_col->insert_from(*tmp_col, 9);
    EXPECT_TRUE(dst_col->has_null());
}

TEST(ColumnNullableTest, CreateRejectsMismatchedNestedAndNullMapSizes) {
    EXPECT_THROW(
            {
                auto nullable = ColumnNullable::create(create_nested_column<TYPE_BIGINT>(100),
                                                       ColumnUInt8::create(10, 0));
            },
            doris::Exception);
}

TEST(ColumnNullableTest, PredicateTest) {
    auto nullable_pred = ColumnNullable::create(ColumnDate::create(), ColumnUInt8::create());
    nullable_pred->insert_many_defaults(3);
    EXPECT_TRUE(nullable_pred->has_null());
    nullable_pred->insert_many_defaults(10);
    EXPECT_TRUE(nullable_pred->has_null());

    nullable_pred->clear();
    EXPECT_FALSE(nullable_pred->has_null());
    nullable_pred->insert_many_defaults(10);
    EXPECT_TRUE(nullable_pred->has_null()); // now it have 10 nulls

    auto null_dst = ColumnNullable::create(ColumnDate::create(), ColumnUInt8::create());
    EXPECT_FALSE(null_dst->has_null());

    uint16_t selector[] = {5, 8}; // both null
    EXPECT_EQ(nullable_pred->filter_by_selector(selector, 2, null_dst.get()), Status::OK());
    // filter_by_selector must announce to update has_null to make below right.
    EXPECT_TRUE(null_dst->has_null());
}

TEST(ColumnNullableTest, SharedCreatePreservesImmutableSubcolumns) {
    auto nested_mut = ColumnInt64::create();
    nested_mut->insert_value(10);
    ColumnPtr nested = std::move(nested_mut);
    ColumnPtr nested_alias = nested;

    auto null_map_mut = ColumnUInt8::create();
    null_map_mut->insert_value(0);
    ColumnPtr null_map = std::move(null_map_mut);
    ColumnPtr null_map_alias = null_map;

    auto nullable = ColumnNullable::create(nested, null_map);
    const auto& nullable_ref = *nullable;
    EXPECT_EQ(nullable_ref.get_nested_column_ptr().get(), nested_alias.get());
    EXPECT_EQ(nullable_ref.get_null_map_column_ptr().get(), null_map_alias.get());
    EXPECT_EQ(nested_alias->size(), 1);
    EXPECT_EQ(null_map_alias->size(), 1);
}

TEST(ColumnNullableTest, UpdateCrc32cBatchKeepsBlockInsertable) {
    auto nested = ColumnInt32::create();
    nested->insert_value(1);
    nested->insert_value(2);
    nested->insert_value(3);
    nested->insert_value(4);

    auto null_map = ColumnUInt8::create();
    null_map->insert_value(0);
    null_map->insert_value(1);
    null_map->insert_value(0);
    null_map->insert_value(1);

    ColumnPtr nullable = ColumnNullable::create(std::move(nested), std::move(null_map));
    const auto& nullable_ref = assert_cast<const ColumnNullable&>(*nullable);
    const auto* nested_before_hash = nullable_ref.get_nested_column_ptr().get();

    auto nullable_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
    Block block({ColumnWithTypeAndName(nullable, nullable_type, "v")});
    ASSERT_TRUE(block.check_type_and_column().ok());

    std::vector<uint32_t> hashes(block.rows(), 0);
    nullable->update_crc32c_batch(hashes.data(), nullptr);

    ASSERT_TRUE(block.check_type_and_column().ok());

    const auto& nullable_after_hash =
            assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
    EXPECT_EQ(nullable_after_hash.get_nested_column_ptr().get(), nested_before_hash);

    auto dst_block = block.clone_empty();
    auto mutable_block = MutableBlock::create_unique(std::move(dst_block));
    std::vector<uint32_t> indices = {0, 1, 2, 3};
    ASSERT_TRUE(
            mutable_block->add_rows(&block, indices.data(), indices.data() + indices.size()).ok());
    EXPECT_EQ(mutable_block->rows(), block.rows());
}

TEST(ColumnNullableTest, UpdateCrc32cBatchDoesNotMutateSharedNestedColumn) {
    auto nested_mut = ColumnInt32::create();
    nested_mut->insert_value(10);
    nested_mut->insert_value(20);
    nested_mut->insert_value(30);
    nested_mut->insert_value(40);
    ColumnPtr nested = std::move(nested_mut);
    ColumnPtr nested_alias = nested;

    auto null_map_mut = ColumnUInt8::create();
    null_map_mut->insert_value(0);
    null_map_mut->insert_value(1);
    null_map_mut->insert_value(0);
    null_map_mut->insert_value(1);
    ColumnPtr null_map = std::move(null_map_mut);

    ColumnPtr nullable = ColumnNullable::create(nested, null_map);
    const auto& nullable_ref = assert_cast<const ColumnNullable&>(*nullable);
    EXPECT_EQ(nullable_ref.get_nested_column_ptr().get(), nested_alias.get());

    auto expected_nested = nested->clone_resized(nested->size());
    expected_nested->replace_column_null_data(
            assert_cast<const ColumnUInt8&>(*null_map).get_data().data());
    std::vector<uint32_t> expected_hashes(nullable->size(), 0);
    expected_nested->update_crc32c_batch(expected_hashes.data(), nullptr);

    std::vector<uint32_t> hashes(nullable->size(), 0);
    nullable->update_crc32c_batch(hashes.data(), nullptr);
    EXPECT_EQ(hashes, expected_hashes);

    const auto& nullable_after_hash = assert_cast<const ColumnNullable&>(*nullable);
    EXPECT_EQ(nullable_after_hash.get_nested_column_ptr().get(), nested_alias.get());
    EXPECT_EQ(assert_cast<const ColumnInt32&>(*nested_alias).get_data()[1], 20);
    EXPECT_EQ(
            assert_cast<const ColumnInt32&>(nullable_after_hash.get_nested_column()).get_data()[1],
            20);

    auto nullable_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
    Block block({ColumnWithTypeAndName(nullable, nullable_type, "v")});
    ASSERT_TRUE(block.check_type_and_column().ok());

    auto dst_block = block.clone_empty();
    auto mutable_block = MutableBlock::create_unique(std::move(dst_block));
    std::vector<uint32_t> indices = {3, 2, 1, 0};
    ASSERT_TRUE(
            mutable_block->add_rows(&block, indices.data(), indices.data() + indices.size()).ok());
    EXPECT_EQ(mutable_block->rows(), block.rows());
}

TEST(ColumnNullableTest, UpdateCrc32cBatchHashesNullAsNestedDefaultForWideType) {
    auto nested_mut = ColumnInt64::create();
    nested_mut->insert_value(10);
    nested_mut->insert_value(20);
    nested_mut->insert_value(30);
    nested_mut->insert_value(40);
    ColumnPtr nested = std::move(nested_mut);

    auto null_map_mut = ColumnUInt8::create();
    null_map_mut->insert_value(0);
    null_map_mut->insert_value(1);
    null_map_mut->insert_value(0);
    null_map_mut->insert_value(1);
    ColumnPtr null_map = std::move(null_map_mut);

    ColumnPtr nullable = ColumnNullable::create(nested, null_map);

    auto expected_nested = nested->clone_resized(nested->size());
    expected_nested->replace_column_null_data(
            assert_cast<const ColumnUInt8&>(*null_map).get_data().data());
    std::vector<uint32_t> expected_hashes(nullable->size(), 0);
    expected_nested->update_crc32c_batch(expected_hashes.data(), nullptr);

    std::vector<uint32_t> hashes(nullable->size(), 0);
    nullable->update_crc32c_batch(hashes.data(), nullptr);

    EXPECT_EQ(hashes, expected_hashes);
    EXPECT_NE(hashes[1], HashUtil::crc32c_null(0));
    EXPECT_NE(hashes[3], HashUtil::crc32c_null(0));
}

TEST(ColumnNullableTest, UpdateCrc32cBatchHashesNullAsDecimalDefault) {
    auto nested_mut = ColumnDecimal64::create(0, 2);
    nested_mut->insert_value(Decimal64(1010));
    nested_mut->insert_value(Decimal64(2020));
    nested_mut->insert_value(Decimal64(3030));
    nested_mut->insert_value(Decimal64(4040));
    ColumnPtr nested = std::move(nested_mut);

    auto null_map_mut = ColumnUInt8::create();
    null_map_mut->insert_value(0);
    null_map_mut->insert_value(1);
    null_map_mut->insert_value(0);
    null_map_mut->insert_value(1);
    ColumnPtr null_map = std::move(null_map_mut);

    ColumnPtr nullable = ColumnNullable::create(nested, null_map);

    auto expected_nested = nested->clone_resized(nested->size());
    expected_nested->replace_column_null_data(
            assert_cast<const ColumnUInt8&>(*null_map).get_data().data());
    std::vector<uint32_t> expected_hashes(nullable->size(), 0);
    expected_nested->update_crc32c_batch(expected_hashes.data(), nullptr);

    std::vector<uint32_t> hashes(nullable->size(), 0);
    nullable->update_crc32c_batch(hashes.data(), nullptr);

    EXPECT_EQ(hashes, expected_hashes);
    EXPECT_NE(hashes[1], HashUtil::crc32c_null(0));
    EXPECT_NE(hashes[3], HashUtil::crc32c_null(0));
}

TEST(ColumnNullableTest, ConcurrentUpdateCrc32cBatchAndInsertIndicesFrom) {
    constexpr int rows = 4096;
    constexpr int hash_threads = 4;
    constexpr int insert_threads = 4;
    constexpr int iterations = 2000;

    auto nested = ColumnInt32::create();
    auto null_map = ColumnUInt8::create();
    for (int i = 0; i < rows; ++i) {
        nested->insert_value(i);
        null_map->insert_value(i % 3 == 0);
    }

    ColumnPtr nullable = ColumnNullable::create(std::move(nested), std::move(null_map));
    std::vector<uint32_t> indices(rows);
    for (uint32_t i = 0; i < rows; ++i) {
        indices[i] = rows - i - 1;
    }

    std::atomic<bool> start = false;
    std::atomic<bool> stop = false;
    std::atomic<int> failures = 0;
    std::vector<std::thread> threads;

    for (int t = 0; t < hash_threads; ++t) {
        threads.emplace_back([&] {
            while (!start.load(std::memory_order_acquire)) {
            }
            std::vector<uint32_t> hashes(rows);
            for (int i = 0; i < iterations && !stop.load(std::memory_order_relaxed); ++i) {
                std::ranges::fill(hashes, 0);
                nullable->update_crc32c_batch(hashes.data(), nullptr);
            }
        });
    }

    for (int t = 0; t < insert_threads; ++t) {
        threads.emplace_back([&] {
            while (!start.load(std::memory_order_acquire)) {
            }
            for (int i = 0; i < iterations && !stop.load(std::memory_order_relaxed); ++i) {
                auto dst = ColumnNullable::create(ColumnInt32::create(), ColumnUInt8::create());
                try {
                    dst->insert_indices_from(*nullable, indices.data(), indices.data() + rows);
                    if (dst->size() != rows) {
                        failures.fetch_add(1, std::memory_order_relaxed);
                        stop.store(true, std::memory_order_relaxed);
                    }
                } catch (...) {
                    failures.fetch_add(1, std::memory_order_relaxed);
                    stop.store(true, std::memory_order_relaxed);
                }
            }
        });
    }

    start.store(true, std::memory_order_release);
    for (auto& thread : threads) {
        thread.join();
    }

    EXPECT_EQ(failures.load(std::memory_order_relaxed), 0);
}

TEST(ColumnNullableTest, append_data_by_selector) {
    auto srt_column = ColumnHelper::create_nullable_column<DataTypeInt64>(
            {1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
            {true, false, true, false, false, false, true, false, false, false});

    IColumn::Selector selector = {0, 2, 4, 6, 8};

    IColumn::MutablePtr dst_column =
            ColumnNullable::create(ColumnInt64::create(), ColumnUInt8::create());

    srt_column->append_data_by_selector(dst_column, selector);

    auto expected_column = ColumnHelper::create_nullable_column<DataTypeInt64>(
            {1, 3, 5, 7, 9}, {true, true, false, true, false});

    EXPECT_TRUE(ColumnHelper::column_equal(std::move(dst_column), expected_column));
}

TEST(ColumnNullableTest, ScalaTypeNullInt32Testerase) {
    auto datetype_int32 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
    auto column = datetype_int32->create_column();
    auto column_res = datetype_int32->create_column();
    std::vector<int32_t> data = {1, 2, 3, 4, 5};
    for (auto d : data) {
        column->insert_data(reinterpret_cast<const char*>(&d), sizeof(d));
    }
    column_res->insert_range_from(*column, 0, data.size());
    column->erase(0, 2);
    EXPECT_EQ(column->size(), 3);

    for (int i = 0; i < column->size(); ++i) {
        EXPECT_EQ(column->get_data_at(i), column_res->get_data_at(i + 2));
    }
}

TEST(ColumnNullableTest, ScalaTypeNullInt32Test2erase) {
    auto datetype_int32 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
    auto column = datetype_int32->create_column();
    auto column_res = datetype_int32->create_column();
    std::vector<int32_t> data = {1, 2, 3, 4, 5};
    std::vector<int32_t> res = {1, 2, 5};
    for (auto d : data) {
        column->insert_data(reinterpret_cast<const char*>(&d), sizeof(d));
    }
    for (auto d : res) {
        column_res->insert_data(reinterpret_cast<const char*>(&d), sizeof(d));
    }
    column->erase(2, 2);
    EXPECT_EQ(column->size(), 3);

    for (int i = 0; i < column->size(); ++i) {
        EXPECT_EQ(column->get_data_at(i), column_res->get_data_at(i));
    }
}

TEST(ColumnNullableTest, ScalaTypeNullStringTesterase) {
    auto datetype_string = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    auto column = datetype_string->create_column();
    auto column_res = datetype_string->create_column();
    std::vector<StringRef> data = {StringRef("asd"), StringRef("1234567"), StringRef("3"),
                                   StringRef("4"), StringRef("5")};
    column->insert_default();
    for (auto d : data) {
        column->insert_data(d.data, d.size);
    }
    column->insert_default();
    column_res->insert_range_from(*column, 0, data.size() + 2);
    column->erase(0, 2);
    EXPECT_EQ(column->size(), 5);
    for (int i = 0; i < column->size(); ++i) {
        std::cout << column->get_data_at(i).to_string() << std::endl;
        EXPECT_EQ(column->get_data_at(i).to_string(), column_res->get_data_at(i + 2).to_string());
    }
}

TEST(ColumnNullableTest, ScalaTypeNullStringTest2erase) {
    auto datetype_string = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    auto column = datetype_string->create_column();
    auto column_res = datetype_string->create_column();
    std::vector<StringRef> data = {StringRef("asd"), StringRef("1234567"), StringRef("3"),
                                   StringRef("4"), StringRef("5")};
    std::vector<StringRef> res = {StringRef("asd"), StringRef("4"), StringRef("5")};
    column->insert_default();
    for (auto d : data) {
        column->insert_data(d.data, d.size);
    }
    column->insert_default();

    column_res->insert_default();
    for (auto d : res) {
        column_res->insert_data(d.data, d.size);
    }
    column_res->insert_default();

    column->erase(2, 2);
    EXPECT_EQ(column->size(), 5);
    for (int i = 0; i < column->size(); ++i) {
        std::cout << column->get_data_at(i).to_string() << " , "
                  << column_res->get_data_at(i).to_string() << std::endl;
        // EXPECT_EQ(column->get_data_at(i).to_string(), column_res->get_data_at(i).to_string());
    }
}

} // namespace doris
