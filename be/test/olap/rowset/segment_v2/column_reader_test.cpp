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
#include "olap/rowset/segment_v2/column_reader.h"

#include <gen_cpp/Descriptors_types.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <thread>
#include <vector>

#include "agent/be_exec_version_manager.h"
#include "common/config.h"
#include "gen_cpp/olap_file.pb.h"
#include "gen_cpp/segment_v2.pb.h"
#include "io/fs/file_reader.h"
#include "mock/mock_segment.h"
#include "olap/rowset/segment_v2/column_reader_cache.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/rowset/segment_v2/variant/variant_column_reader.h"
#include "olap/tablet_schema.h"
#include "vec/json/path_in_data.h"

namespace doris::segment_v2 {
namespace {
class TestColumnIterator final : public ColumnIterator {
public:
    Status seek_to_ordinal(ordinal_t /* ord */) override { return Status::OK(); }

    Status next_batch(size_t* n, vectorized::MutableColumnPtr& dst, bool* has_null) override {
        dst->insert_many_defaults(*n);
        if (has_null != nullptr) {
            *has_null = false;
        }
        return Status::OK();
    }

    Status read_by_rowids(const rowid_t* /* rowids */, const size_t count,
                          vectorized::MutableColumnPtr& dst) override {
        dst->insert_many_defaults(count);
        return Status::OK();
    }

    ordinal_t get_current_ordinal() const override { return 0; }

    void force_set_reading_flag(ReadingFlag flag) { _reading_flag = flag; }

    Result<TColumnAccessPaths> process_sub_access_paths(const TColumnAccessPaths& access_paths,
                                                        bool is_predicate) {
        return _process_sub_access_paths(access_paths, is_predicate);
    }

    void convert_to_place_holder_column(vectorized::MutableColumnPtr& dst, size_t count) {
        _convert_to_place_holder_column(dst, count);
    }
};
} // namespace

class ColumnReaderTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(ColumnReaderTest, StructAccessPaths) {
    auto create_struct_iterator = []() {
        auto null_reader = std::make_shared<ColumnReader>();
        auto null_iterator = std::make_unique<FileColumnIterator>(null_reader);

        std::vector<ColumnIteratorUPtr> sub_column_iterators;
        auto sub_reader1 = std::make_shared<ColumnReader>();
        auto sub_iterator1 = std::make_unique<FileColumnIterator>(sub_reader1);
        sub_iterator1->set_column_name("sub_col_1");
        auto sub_reader2 = std::make_shared<ColumnReader>();
        auto sub_iterator2 = std::make_unique<FileColumnIterator>(sub_reader2);
        sub_iterator2->set_column_name("sub_col_2");

        sub_column_iterators.emplace_back(std::move(sub_iterator1));
        sub_column_iterators.emplace_back(std::move(sub_iterator2));
        auto iterator = std::make_unique<StructFileColumnIterator>(std::make_shared<ColumnReader>(),
                                                                   std::move(null_iterator),
                                                                   std::move(sub_column_iterators));
        return iterator;
    };

    auto iterator = create_struct_iterator();
    auto st = iterator->set_access_paths(TColumnAccessPaths {}, TColumnAccessPaths {});

    ASSERT_TRUE(st.ok()) << "failed to set access paths: " << st.to_string();
    ASSERT_EQ(iterator->_reading_flag, ColumnIterator::ReadingFlag::NORMAL_READING);

    TColumnAccessPaths all_access_paths;
    all_access_paths.emplace_back();

    TColumnAccessPaths predicate_access_paths;
    predicate_access_paths.emplace_back();

    st = iterator->set_access_paths(all_access_paths, predicate_access_paths);
    // empty paths leads to error
    ASSERT_FALSE(st.ok());

    // Only reading sub_col_1
    // sub_col_2 should be set to SKIP_READING
    all_access_paths[0].data_access_path.path = {"self", "sub_col_1"};

    predicate_access_paths[0].data_access_path.path = {"self", "sub_col_1"};

    st = iterator->set_access_paths(all_access_paths, predicate_access_paths);
    // invalid name leads to error
    ASSERT_FALSE(st.ok());

    iterator->set_column_name("self");
    // now column name is "self", should be ok
    st = iterator->set_access_paths(all_access_paths, predicate_access_paths);
    ASSERT_TRUE(st.ok()) << "failed to set access paths: " << st.to_string();
    ASSERT_EQ(iterator->_reading_flag, ColumnIterator::ReadingFlag::READING_FOR_PREDICATE);

    ASSERT_EQ(iterator->_sub_column_iterators[0]->_reading_flag,
              ColumnIterator::ReadingFlag::READING_FOR_PREDICATE);
    ASSERT_EQ(iterator->_sub_column_iterators[1]->_reading_flag,
              ColumnIterator::ReadingFlag::SKIP_READING);

    // Reading all sub columns
    all_access_paths[0].data_access_path.path = {"self"};
    iterator = create_struct_iterator();
    iterator->set_column_name("self");
    st = iterator->set_access_paths(all_access_paths, predicate_access_paths);

    ASSERT_TRUE(st.ok()) << "failed to set access paths: " << st.to_string();
    ASSERT_EQ(iterator->_reading_flag, ColumnIterator::ReadingFlag::READING_FOR_PREDICATE);

    ASSERT_EQ(iterator->_sub_column_iterators[0]->_reading_flag,
              ColumnIterator::ReadingFlag::READING_FOR_PREDICATE);
    ASSERT_EQ(iterator->_sub_column_iterators[1]->_reading_flag,
              ColumnIterator::ReadingFlag::NEED_TO_READ);
}

TEST_F(ColumnReaderTest, ReadingModeMatrix) {
    TestColumnIterator iterator;

    iterator.force_set_reading_flag(ColumnIterator::ReadingFlag::SKIP_READING);
    iterator.set_reading_mode(ColumnIterator::ReadingMode::NORMAL);
    EXPECT_FALSE(iterator.need_to_read());
    EXPECT_FALSE(iterator.need_to_read_meta_columns());

    iterator.force_set_reading_flag(ColumnIterator::ReadingFlag::NEED_TO_READ);
    EXPECT_TRUE(iterator.need_to_read());
    EXPECT_TRUE(iterator.need_to_read_meta_columns());

    iterator.force_set_reading_flag(ColumnIterator::ReadingFlag::NORMAL_READING);
    iterator.set_reading_mode(ColumnIterator::ReadingMode::PREDICATE);
    EXPECT_FALSE(iterator.need_to_read());
    EXPECT_TRUE(iterator.need_to_read_meta_columns());

    iterator.force_set_reading_flag(ColumnIterator::ReadingFlag::READING_FOR_PREDICATE);
    EXPECT_TRUE(iterator.need_to_read());
    EXPECT_TRUE(iterator.need_to_read_meta_columns());

    iterator.force_set_reading_flag(ColumnIterator::ReadingFlag::READING_FOR_PREDICATE);
    iterator.set_reading_mode(ColumnIterator::ReadingMode::LAZY);
    EXPECT_FALSE(iterator.need_to_read());
    EXPECT_FALSE(iterator.need_to_read_meta_columns());

    iterator.force_set_reading_flag(ColumnIterator::ReadingFlag::NEED_TO_READ);
    EXPECT_TRUE(iterator.need_to_read());
    EXPECT_TRUE(iterator.need_to_read_meta_columns());
}

TEST_F(ColumnReaderTest, ReadingFlagPriorityAndNeedToRead) {
    TestColumnIterator iterator;

    iterator.set_reading_flag(ColumnIterator::ReadingFlag::SKIP_READING);
    EXPECT_EQ(iterator.reading_flag(), ColumnIterator::ReadingFlag::SKIP_READING);

    iterator.set_need_to_read();
    EXPECT_EQ(iterator.reading_flag(), ColumnIterator::ReadingFlag::NEED_TO_READ);

    iterator.set_reading_flag(ColumnIterator::ReadingFlag::SKIP_READING);
    EXPECT_EQ(iterator.reading_flag(), ColumnIterator::ReadingFlag::NEED_TO_READ);

    iterator.set_reading_flag(ColumnIterator::ReadingFlag::READING_FOR_PREDICATE);
    EXPECT_EQ(iterator.reading_flag(), ColumnIterator::ReadingFlag::READING_FOR_PREDICATE);

    iterator.set_reading_flag(ColumnIterator::ReadingFlag::NEED_TO_READ);
    EXPECT_EQ(iterator.reading_flag(), ColumnIterator::ReadingFlag::READING_FOR_PREDICATE);
}

TEST_F(ColumnReaderTest, PlaceHolderLifecycleInLazyMode) {
    TestColumnIterator iterator;
    iterator.force_set_reading_flag(ColumnIterator::ReadingFlag::READING_FOR_PREDICATE);
    iterator.set_reading_mode(ColumnIterator::ReadingMode::PREDICATE);

    auto column = vectorized::ColumnInt32::create();
    auto dst = column->assume_mutable();
    iterator.convert_to_place_holder_column(dst, 3);

    EXPECT_EQ(3, dst->size());
    EXPECT_TRUE(iterator._place_holder_columns.contains(dst.get()));

    iterator.set_reading_mode(ColumnIterator::ReadingMode::LAZY);
    iterator.finalize_lazy_mode(dst);
    EXPECT_EQ(0, dst->size());
    EXPECT_TRUE(iterator._place_holder_columns.empty());

    auto lazy_column = vectorized::ColumnInt32::create();
    auto lazy_dst = lazy_column->assume_mutable();
    iterator.force_set_reading_flag(ColumnIterator::ReadingFlag::READING_FOR_PREDICATE);
    iterator.set_reading_mode(ColumnIterator::ReadingMode::LAZY);
    iterator.convert_to_place_holder_column(lazy_dst, 4);
    EXPECT_EQ(0, lazy_dst->size());
}

TEST_F(ColumnReaderTest, SetReadingFlagRecursivelyPropagates) {
    auto null_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    std::vector<ColumnIteratorUPtr> struct_sub_iters;

    auto sub_col = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    sub_col->set_column_name("sub_col");
    struct_sub_iters.emplace_back(std::move(sub_col));

    auto array_item = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    array_item->set_column_name("item");
    auto array_offsets = std::make_unique<OffsetFileColumnIterator>(
            std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()));
    auto array_null = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    auto array_iter = std::make_unique<ArrayFileColumnIterator>(
            std::make_shared<ColumnReader>(), std::move(array_offsets), std::move(array_item),
            std::move(array_null));
    array_iter->set_column_name("arr");
    struct_sub_iters.emplace_back(std::move(array_iter));

    StructFileColumnIterator struct_iter(std::make_shared<ColumnReader>(), std::move(null_iter),
                                         std::move(struct_sub_iters));
    struct_iter.set_reading_flag_recursively(ColumnIterator::ReadingFlag::READING_FOR_PREDICATE);

    EXPECT_EQ(struct_iter.reading_flag(), ColumnIterator::ReadingFlag::READING_FOR_PREDICATE);
    EXPECT_EQ(struct_iter._sub_column_iterators[0]->_reading_flag,
              ColumnIterator::ReadingFlag::READING_FOR_PREDICATE);

    auto* nested_array =
            static_cast<ArrayFileColumnIterator*>(struct_iter._sub_column_iterators[1].get());
    EXPECT_EQ(nested_array->_reading_flag, ColumnIterator::ReadingFlag::READING_FOR_PREDICATE);
    EXPECT_EQ(nested_array->_item_iterator->_reading_flag,
              ColumnIterator::ReadingFlag::READING_FOR_PREDICATE);

    auto map_null_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    auto map_offsets_iter = std::make_unique<OffsetFileColumnIterator>(
            std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()));
    auto map_key_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    auto map_val_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    MapFileColumnIterator map_iter(std::make_shared<ColumnReader>(), std::move(map_null_iter),
                                   std::move(map_offsets_iter), std::move(map_key_iter),
                                   std::move(map_val_iter));
    map_iter.set_reading_flag_recursively(ColumnIterator::ReadingFlag::READING_FOR_PREDICATE);
    EXPECT_EQ(map_iter._key_iterator->_reading_flag,
              ColumnIterator::ReadingFlag::READING_FOR_PREDICATE);
    EXPECT_EQ(map_iter._val_iterator->_reading_flag,
              ColumnIterator::ReadingFlag::READING_FOR_PREDICATE);
}

TEST_F(ColumnReaderTest, RemovePrunedSubIterators) {
    auto struct_null_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    std::vector<ColumnIteratorUPtr> struct_sub_iters;
    auto sub_keep = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    sub_keep->set_column_name("keep");
    auto sub_prune = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    sub_prune->set_column_name("prune");
    sub_prune->set_reading_flag(ColumnIterator::ReadingFlag::SKIP_READING);
    struct_sub_iters.emplace_back(std::move(sub_keep));
    struct_sub_iters.emplace_back(std::move(sub_prune));

    auto array_item_null = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    std::vector<ColumnIteratorUPtr> item_struct_sub_iters;
    auto item_keep = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    item_keep->set_column_name("keep");
    auto item_prune = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    item_prune->set_column_name("prune");
    item_prune->set_reading_flag(ColumnIterator::ReadingFlag::SKIP_READING);
    item_struct_sub_iters.emplace_back(std::move(item_keep));
    item_struct_sub_iters.emplace_back(std::move(item_prune));
    auto item_struct = std::make_unique<StructFileColumnIterator>(std::make_shared<ColumnReader>(),
                                                                  std::move(array_item_null),
                                                                  std::move(item_struct_sub_iters));

    auto array_offsets = std::make_unique<OffsetFileColumnIterator>(
            std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()));
    auto array_null = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    auto array_iter = std::make_unique<ArrayFileColumnIterator>(
            std::make_shared<ColumnReader>(), std::move(array_offsets), std::move(item_struct),
            std::move(array_null));
    struct_sub_iters.emplace_back(std::move(array_iter));

    StructFileColumnIterator struct_iter(std::make_shared<ColumnReader>(),
                                         std::move(struct_null_iter), std::move(struct_sub_iters));
    ASSERT_EQ(3, struct_iter._sub_column_iterators.size());
    struct_iter.remove_pruned_sub_iterators();
    ASSERT_EQ(2, struct_iter._sub_column_iterators.size());

    auto* nested_array =
            static_cast<ArrayFileColumnIterator*>(struct_iter._sub_column_iterators[1].get());
    auto* nested_struct =
            static_cast<StructFileColumnIterator*>(nested_array->_item_iterator.get());
    ASSERT_EQ(1, nested_struct->_sub_column_iterators.size());
    EXPECT_EQ(nested_struct->_sub_column_iterators[0]->column_name(), "keep");
}

TEST_F(ColumnReaderTest, FinalizeLazyModeOnNestedStruct) {
    auto sub_iter = std::make_unique<TestColumnIterator>();
    auto* sub_iter_ptr = sub_iter.get();
    auto null_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    std::vector<ColumnIteratorUPtr> sub_iters;
    sub_iters.emplace_back(std::move(sub_iter));

    StructFileColumnIterator struct_iter(std::make_shared<ColumnReader>(), std::move(null_iter),
                                         std::move(sub_iters));
    struct_iter.set_reading_mode(ColumnIterator::ReadingMode::PREDICATE);
    sub_iter_ptr->set_reading_mode(ColumnIterator::ReadingMode::PREDICATE);

    auto nested_column = vectorized::ColumnInt32::create();
    auto nested_mut = nested_column->assume_mutable();
    sub_iter_ptr->convert_to_place_holder_column(nested_mut, 5);
    EXPECT_EQ(5, nested_mut->size());

    vectorized::Columns struct_columns;
    struct_columns.emplace_back(std::move(nested_column));
    auto struct_column = vectorized::ColumnStruct::create(struct_columns);
    vectorized::MutableColumnPtr struct_mut = std::move(struct_column);
    struct_iter.set_reading_mode(ColumnIterator::ReadingMode::LAZY);
    sub_iter_ptr->set_reading_mode(ColumnIterator::ReadingMode::LAZY);
    struct_iter.finalize_lazy_mode(struct_mut);

    auto& column_struct =
            assert_cast<vectorized::ColumnStruct&, TypeCheckOnRelease::DISABLE>(*struct_mut);
    auto nested_after = column_struct.get_column_ptr(0);
    EXPECT_EQ(0, nested_after->size());
}

TEST_F(ColumnReaderTest, ProcessSubAccessPathsSetsPredicateFlag) {
    TestColumnIterator iterator;
    iterator.set_column_name("self");

    TColumnAccessPaths access_paths;
    access_paths.emplace_back();
    access_paths[0].data_access_path.path = {"self"};

    iterator.force_set_reading_flag(ColumnIterator::ReadingFlag::NORMAL_READING);
    auto sub_paths = TEST_TRY(iterator.process_sub_access_paths(access_paths, false));
    EXPECT_TRUE(sub_paths.empty());
    EXPECT_EQ(iterator._reading_flag, ColumnIterator::ReadingFlag::NEED_TO_READ);

    iterator.force_set_reading_flag(ColumnIterator::ReadingFlag::NORMAL_READING);
    sub_paths = TEST_TRY(iterator.process_sub_access_paths(access_paths, true));
    EXPECT_TRUE(sub_paths.empty());
    EXPECT_EQ(iterator._reading_flag, ColumnIterator::ReadingFlag::READING_FOR_PREDICATE);
}

TEST_F(ColumnReaderTest, NestedIteratorsPropagateReadingMode) {
    auto struct_null_iterator =
            std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    std::vector<ColumnIteratorUPtr> struct_sub_iters;
    struct_sub_iters.emplace_back(
            std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()));
    struct_sub_iters.emplace_back(
            std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()));
    auto struct_iterator = std::make_unique<StructFileColumnIterator>(
            std::make_shared<ColumnReader>(), std::move(struct_null_iterator),
            std::move(struct_sub_iters));

    struct_iterator->set_reading_mode(ColumnIterator::ReadingMode::LAZY);
    EXPECT_EQ(struct_iterator->_sub_column_iterators[0]->_reading_mode,
              ColumnIterator::ReadingMode::LAZY);
    EXPECT_EQ(struct_iterator->_sub_column_iterators[1]->_reading_mode,
              ColumnIterator::ReadingMode::LAZY);

    auto array_item_iterator =
            std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    auto array_offsets_iter = std::make_unique<OffsetFileColumnIterator>(
            std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()));
    auto array_null_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    ArrayFileColumnIterator array_iterator(
            std::make_shared<ColumnReader>(), std::move(array_offsets_iter),
            std::move(array_item_iterator), std::move(array_null_iter));
    array_iterator.set_reading_mode(ColumnIterator::ReadingMode::PREDICATE);
    EXPECT_EQ(array_iterator._item_iterator->_reading_mode, ColumnIterator::ReadingMode::PREDICATE);

    auto map_null_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    auto map_offsets_iter = std::make_unique<OffsetFileColumnIterator>(
            std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()));
    auto map_key_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    auto map_val_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    MapFileColumnIterator map_iterator(std::make_shared<ColumnReader>(), std::move(map_null_iter),
                                       std::move(map_offsets_iter), std::move(map_key_iter),
                                       std::move(map_val_iter));
    map_iterator.set_reading_mode(ColumnIterator::ReadingMode::LAZY);
    EXPECT_EQ(map_iterator._key_iterator->_reading_mode, ColumnIterator::ReadingMode::LAZY);
    EXPECT_EQ(map_iterator._val_iterator->_reading_mode, ColumnIterator::ReadingMode::LAZY);
}

TEST_F(ColumnReaderTest, AccessPathsPropagatePredicateToChildren) {
    auto struct_null_iterator =
            std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    std::vector<ColumnIteratorUPtr> struct_sub_iters;
    struct_sub_iters.emplace_back(
            std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()));
    struct_sub_iters.emplace_back(
            std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()));
    auto struct_iterator = std::make_unique<StructFileColumnIterator>(
            std::make_shared<ColumnReader>(), std::move(struct_null_iterator),
            std::move(struct_sub_iters));
    struct_iterator->set_column_name("s");

    TColumnAccessPaths all_access_paths;
    all_access_paths.emplace_back();
    all_access_paths[0].data_access_path.path = {"s"};
    TColumnAccessPaths predicate_access_paths = all_access_paths;

    auto st = struct_iterator->set_access_paths(all_access_paths, predicate_access_paths);
    ASSERT_TRUE(st.ok()) << "failed to set struct access paths: " << st.to_string();
    EXPECT_TRUE(struct_iterator->is_pruned());
    EXPECT_EQ(struct_iterator->_reading_flag, ColumnIterator::ReadingFlag::READING_FOR_PREDICATE);
    EXPECT_EQ(struct_iterator->_sub_column_iterators[0]->_reading_flag,
              ColumnIterator::ReadingFlag::READING_FOR_PREDICATE);
    EXPECT_EQ(struct_iterator->_sub_column_iterators[1]->_reading_flag,
              ColumnIterator::ReadingFlag::READING_FOR_PREDICATE);

    auto array_item_iterator =
            std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    auto array_offsets_iter = std::make_unique<OffsetFileColumnIterator>(
            std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()));
    auto array_null_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    ArrayFileColumnIterator array_iterator(
            std::make_shared<ColumnReader>(), std::move(array_offsets_iter),
            std::move(array_item_iterator), std::move(array_null_iter));
    array_iterator.set_column_name("a");
    TColumnAccessPaths array_access_paths;
    array_access_paths.emplace_back();
    array_access_paths[0].data_access_path.path = {"a"};
    TColumnAccessPaths array_predicate_paths = array_access_paths;
    st = array_iterator.set_access_paths(array_access_paths, array_predicate_paths);
    ASSERT_TRUE(st.ok()) << "failed to set array access paths: " << st.to_string();
    EXPECT_TRUE(array_iterator.is_pruned());
    EXPECT_EQ(array_iterator._reading_flag, ColumnIterator::ReadingFlag::READING_FOR_PREDICATE);
    EXPECT_EQ(array_iterator._item_iterator->_reading_flag,
              ColumnIterator::ReadingFlag::READING_FOR_PREDICATE);

    auto map_null_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    auto map_offsets_iter = std::make_unique<OffsetFileColumnIterator>(
            std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()));
    auto map_key_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    auto map_val_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    MapFileColumnIterator map_iterator(std::make_shared<ColumnReader>(), std::move(map_null_iter),
                                       std::move(map_offsets_iter), std::move(map_key_iter),
                                       std::move(map_val_iter));
    map_iterator.set_column_name("m");
    TColumnAccessPaths map_access_paths;
    map_access_paths.emplace_back();
    map_access_paths[0].data_access_path.path = {"m"};
    TColumnAccessPaths map_predicate_paths = map_access_paths;
    st = map_iterator.set_access_paths(map_access_paths, map_predicate_paths);
    ASSERT_TRUE(st.ok()) << "failed to set map access paths: " << st.to_string();
    EXPECT_TRUE(map_iterator.is_pruned());
    EXPECT_EQ(map_iterator._reading_flag, ColumnIterator::ReadingFlag::READING_FOR_PREDICATE);
    EXPECT_EQ(map_iterator._key_iterator->_reading_flag,
              ColumnIterator::ReadingFlag::READING_FOR_PREDICATE);
    EXPECT_EQ(map_iterator._val_iterator->_reading_flag,
              ColumnIterator::ReadingFlag::READING_FOR_PREDICATE);
}

TEST_F(ColumnReaderTest, NestedStructArrayMapStructAccessPaths) {
    auto make_value_struct = []() {
        auto null_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
        std::vector<ColumnIteratorUPtr> sub_iters;
        auto sub_a = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
        sub_a->set_column_name("a");
        auto sub_b = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
        sub_b->set_column_name("b");
        sub_iters.emplace_back(std::move(sub_a));
        sub_iters.emplace_back(std::move(sub_b));

        auto value_struct = std::make_unique<StructFileColumnIterator>(
                std::make_shared<ColumnReader>(), std::move(null_iter), std::move(sub_iters));
        value_struct->set_column_name("value");
        return value_struct;
    };

    auto map_null_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    auto map_offsets_iter = std::make_unique<OffsetFileColumnIterator>(
            std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()));
    auto map_key_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    map_key_iter->set_column_name("key");
    auto map_val_iter = make_value_struct();
    auto map_iterator = std::make_unique<MapFileColumnIterator>(
            std::make_shared<ColumnReader>(), std::move(map_null_iter), std::move(map_offsets_iter),
            std::move(map_key_iter), std::move(map_val_iter));
    map_iterator->set_column_name("item");

    auto array_null_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    auto array_offsets_iter = std::make_unique<OffsetFileColumnIterator>(
            std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()));
    auto array_iterator = std::make_unique<ArrayFileColumnIterator>(
            std::make_shared<ColumnReader>(), std::move(array_offsets_iter),
            std::move(map_iterator), std::move(array_null_iter));
    array_iterator->set_column_name("col2");

    auto struct_null_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    std::vector<ColumnIteratorUPtr> struct_sub_iters;
    auto sub_col1 = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    sub_col1->set_column_name("col1");
    struct_sub_iters.emplace_back(std::move(sub_col1));
    struct_sub_iters.emplace_back(std::move(array_iterator));
    auto top_struct = std::make_unique<StructFileColumnIterator>(std::make_shared<ColumnReader>(),
                                                                 std::move(struct_null_iter),
                                                                 std::move(struct_sub_iters));
    top_struct->set_column_name("root");

    TColumnAccessPaths access_paths;
    access_paths.emplace_back();
    access_paths[0].data_access_path.path = {"root", "col2", "*", "VALUES", "a"};
    TColumnAccessPaths predicate_access_paths = access_paths;

    auto st = top_struct->set_access_paths(access_paths, predicate_access_paths);
    ASSERT_TRUE(st.ok()) << "failed to set nested access paths: " << st.to_string();

    EXPECT_TRUE(top_struct->is_pruned());
    EXPECT_EQ(top_struct->_reading_flag, ColumnIterator::ReadingFlag::READING_FOR_PREDICATE);
    EXPECT_EQ(top_struct->_sub_column_iterators[0]->_reading_flag,
              ColumnIterator::ReadingFlag::SKIP_READING);
    EXPECT_EQ(top_struct->_sub_column_iterators[1]->_reading_flag,
              ColumnIterator::ReadingFlag::READING_FOR_PREDICATE);

    auto* array_iter =
            static_cast<ArrayFileColumnIterator*>(top_struct->_sub_column_iterators[1].get());
    EXPECT_TRUE(array_iter->is_pruned());
    auto* map_iter = static_cast<MapFileColumnIterator*>(array_iter->_item_iterator.get());
    EXPECT_TRUE(map_iter->is_pruned());
    EXPECT_EQ(map_iter->_key_iterator->_reading_flag, ColumnIterator::ReadingFlag::SKIP_READING);
    EXPECT_EQ(map_iter->_val_iterator->_reading_flag,
              ColumnIterator::ReadingFlag::READING_FOR_PREDICATE);

    auto* value_struct = static_cast<StructFileColumnIterator*>(map_iter->_val_iterator.get());
    EXPECT_TRUE(value_struct->is_pruned());
    EXPECT_EQ(value_struct->_sub_column_iterators[0]->_reading_flag,
              ColumnIterator::ReadingFlag::READING_FOR_PREDICATE);
    EXPECT_EQ(value_struct->_sub_column_iterators[1]->_reading_flag,
              ColumnIterator::ReadingFlag::SKIP_READING);
}

TEST_F(ColumnReaderTest, NestedStructArrayMapStructAccessPathsVariants) {
    auto build_nested_iterator = []() {
        auto make_value_struct = []() {
            auto null_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
            std::vector<ColumnIteratorUPtr> sub_iters;
            auto sub_a = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
            sub_a->set_column_name("a");
            auto sub_b = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
            sub_b->set_column_name("b");
            sub_iters.emplace_back(std::move(sub_a));
            sub_iters.emplace_back(std::move(sub_b));

            auto value_struct = std::make_unique<StructFileColumnIterator>(
                    std::make_shared<ColumnReader>(), std::move(null_iter), std::move(sub_iters));
            value_struct->set_column_name("value");
            return value_struct;
        };

        auto map_null_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
        auto map_offsets_iter = std::make_unique<OffsetFileColumnIterator>(
                std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()));
        auto map_key_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
        map_key_iter->set_column_name("key");
        auto map_val_iter = make_value_struct();
        auto map_iterator = std::make_unique<MapFileColumnIterator>(
                std::make_shared<ColumnReader>(), std::move(map_null_iter),
                std::move(map_offsets_iter), std::move(map_key_iter), std::move(map_val_iter));
        map_iterator->set_column_name("item");

        auto array_null_iter =
                std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
        auto array_offsets_iter = std::make_unique<OffsetFileColumnIterator>(
                std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()));
        auto array_iterator = std::make_unique<ArrayFileColumnIterator>(
                std::make_shared<ColumnReader>(), std::move(array_offsets_iter),
                std::move(map_iterator), std::move(array_null_iter));
        array_iterator->set_column_name("col2");

        auto struct_null_iter =
                std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
        std::vector<ColumnIteratorUPtr> struct_sub_iters;
        auto sub_col1 = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
        sub_col1->set_column_name("col1");
        struct_sub_iters.emplace_back(std::move(sub_col1));
        struct_sub_iters.emplace_back(std::move(array_iterator));
        auto top_struct = std::make_unique<StructFileColumnIterator>(
                std::make_shared<ColumnReader>(), std::move(struct_null_iter),
                std::move(struct_sub_iters));
        top_struct->set_column_name("root");
        return top_struct;
    };

    {
        auto top_struct = build_nested_iterator();
        TColumnAccessPaths all_access_paths;
        all_access_paths.emplace_back();
        all_access_paths[0].data_access_path.path = {"root", "col1"};
        TColumnAccessPaths predicate_access_paths;

        auto st = top_struct->set_access_paths(all_access_paths, predicate_access_paths);
        ASSERT_TRUE(st.ok()) << "failed to set access paths: " << st.to_string();

        EXPECT_TRUE(top_struct->is_pruned());
        EXPECT_EQ(top_struct->_reading_flag, ColumnIterator::ReadingFlag::NEED_TO_READ);
        EXPECT_EQ(top_struct->_sub_column_iterators[0]->_reading_flag,
                  ColumnIterator::ReadingFlag::NEED_TO_READ);
        EXPECT_EQ(top_struct->_sub_column_iterators[1]->_reading_flag,
                  ColumnIterator::ReadingFlag::SKIP_READING);
    }

    {
        auto top_struct = build_nested_iterator();
        TColumnAccessPaths all_access_paths;
        all_access_paths.emplace_back();
        all_access_paths[0].data_access_path.path = {"root", "col2", "*", "KEYS"};
        TColumnAccessPaths predicate_access_paths;
        predicate_access_paths.emplace_back();
        predicate_access_paths[0].data_access_path.path = {"root", "col2", "*", "VALUES", "b"};

        auto st = top_struct->set_access_paths(all_access_paths, predicate_access_paths);
        EXPECT_TRUE(st.ok()) << "failed to set access paths: " << st.to_string();
    }

    {
        auto top_struct = build_nested_iterator();
        TColumnAccessPaths all_access_paths;
        all_access_paths.emplace_back();
        all_access_paths[0].data_access_path.path = {"root", "col2"};
        TColumnAccessPaths predicate_access_paths = all_access_paths;

        auto st = top_struct->set_access_paths(all_access_paths, predicate_access_paths);
        ASSERT_TRUE(st.ok()) << "failed to set access paths: " << st.to_string();

        EXPECT_EQ(top_struct->_reading_flag, ColumnIterator::ReadingFlag::READING_FOR_PREDICATE);
        EXPECT_EQ(top_struct->_sub_column_iterators[1]->_reading_flag,
                  ColumnIterator::ReadingFlag::READING_FOR_PREDICATE);

        auto* array_iter =
                static_cast<ArrayFileColumnIterator*>(top_struct->_sub_column_iterators[1].get());
        auto* map_iter = static_cast<MapFileColumnIterator*>(array_iter->_item_iterator.get());
        EXPECT_EQ(map_iter->_key_iterator->_reading_flag,
                  ColumnIterator::ReadingFlag::READING_FOR_PREDICATE);
        EXPECT_EQ(map_iter->_val_iterator->_reading_flag,
                  ColumnIterator::ReadingFlag::READING_FOR_PREDICATE);

        auto* value_struct = static_cast<StructFileColumnIterator*>(map_iter->_val_iterator.get());
        EXPECT_EQ(value_struct->_sub_column_iterators[0]->_reading_flag,
                  ColumnIterator::ReadingFlag::READING_FOR_PREDICATE);
        EXPECT_EQ(value_struct->_sub_column_iterators[1]->_reading_flag,
                  ColumnIterator::ReadingFlag::READING_FOR_PREDICATE);
    }

    {
        auto top_struct = build_nested_iterator();
        TColumnAccessPaths all_access_paths;
        TColumnAccessPaths predicate_access_paths;
        predicate_access_paths.emplace_back();
        predicate_access_paths[0].data_access_path.path = {"root", "col2", "*", "VALUES", "a"};

        auto st = top_struct->set_access_paths(all_access_paths, predicate_access_paths);
        EXPECT_TRUE(st.ok()) << "failed to set access paths: " << st.to_string();
    }
    {
        auto top_struct = build_nested_iterator();
        TColumnAccessPaths all_access_paths;
        all_access_paths.emplace_back();
        all_access_paths[0].data_access_path.path = {"root", "col2", "*", "KEYS"};
        TColumnAccessPaths predicate_access_paths;

        auto st = top_struct->set_access_paths(all_access_paths, predicate_access_paths);
        ASSERT_TRUE(st.ok()) << "failed to set access paths: " << st.to_string();

        auto* array_iter =
                static_cast<ArrayFileColumnIterator*>(top_struct->_sub_column_iterators[1].get());
        auto* map_iter = static_cast<MapFileColumnIterator*>(array_iter->_item_iterator.get());
        EXPECT_EQ(map_iter->_key_iterator->_reading_flag,
                  ColumnIterator::ReadingFlag::NEED_TO_READ);
        EXPECT_EQ(map_iter->_val_iterator->_reading_flag,
                  ColumnIterator::ReadingFlag::SKIP_READING);
    }

    {
        auto top_struct = build_nested_iterator();
        TColumnAccessPaths all_access_paths;
        all_access_paths.emplace_back();
        all_access_paths[0].data_access_path.path = {"root", "col2", "*", "VALUES"};
        TColumnAccessPaths predicate_access_paths;

        auto st = top_struct->set_access_paths(all_access_paths, predicate_access_paths);
        ASSERT_TRUE(st.ok()) << "failed to set access paths: " << st.to_string();

        auto* array_iter =
                static_cast<ArrayFileColumnIterator*>(top_struct->_sub_column_iterators[1].get());
        auto* map_iter = static_cast<MapFileColumnIterator*>(array_iter->_item_iterator.get());
        EXPECT_EQ(map_iter->_key_iterator->_reading_flag,
                  ColumnIterator::ReadingFlag::SKIP_READING);
        EXPECT_EQ(map_iter->_val_iterator->_reading_flag,
                  ColumnIterator::ReadingFlag::NEED_TO_READ);

        auto* value_struct = static_cast<StructFileColumnIterator*>(map_iter->_val_iterator.get());
        EXPECT_EQ(value_struct->_sub_column_iterators[0]->_reading_flag,
                  ColumnIterator::ReadingFlag::NEED_TO_READ);
        EXPECT_EQ(value_struct->_sub_column_iterators[1]->_reading_flag,
                  ColumnIterator::ReadingFlag::NEED_TO_READ);
    }

    {
        auto top_struct = build_nested_iterator();
        TColumnAccessPaths all_access_paths;
        all_access_paths.emplace_back();
        all_access_paths[0].data_access_path.path = {"root", "col2", "*"};
        TColumnAccessPaths predicate_access_paths;

        auto st = top_struct->set_access_paths(all_access_paths, predicate_access_paths);
        ASSERT_TRUE(st.ok()) << "failed to set access paths: " << st.to_string();

        auto* array_iter =
                static_cast<ArrayFileColumnIterator*>(top_struct->_sub_column_iterators[1].get());
        auto* map_iter = static_cast<MapFileColumnIterator*>(array_iter->_item_iterator.get());
        EXPECT_EQ(map_iter->_key_iterator->_reading_flag,
                  ColumnIterator::ReadingFlag::NEED_TO_READ);
        EXPECT_EQ(map_iter->_val_iterator->_reading_flag,
                  ColumnIterator::ReadingFlag::NEED_TO_READ);

        auto* value_struct = static_cast<StructFileColumnIterator*>(map_iter->_val_iterator.get());
        EXPECT_EQ(value_struct->_sub_column_iterators[0]->_reading_flag,
                  ColumnIterator::ReadingFlag::NEED_TO_READ);
        EXPECT_EQ(value_struct->_sub_column_iterators[1]->_reading_flag,
                  ColumnIterator::ReadingFlag::NEED_TO_READ);
    }

    {
        auto top_struct = build_nested_iterator();
        TColumnAccessPaths all_access_paths;
        all_access_paths.emplace_back();
        all_access_paths[0].data_access_path.path = {"root", "col2", "*", "VALUES", "a"};
        TColumnAccessPaths predicate_access_paths = all_access_paths;

        auto st = top_struct->set_access_paths(all_access_paths, predicate_access_paths);
        ASSERT_TRUE(st.ok()) << "failed to set access paths: " << st.to_string();

        EXPECT_EQ(top_struct->_reading_flag, ColumnIterator::ReadingFlag::READING_FOR_PREDICATE);
        EXPECT_EQ(top_struct->_sub_column_iterators[1]->_reading_flag,
                  ColumnIterator::ReadingFlag::READING_FOR_PREDICATE);

        auto* array_iter =
                static_cast<ArrayFileColumnIterator*>(top_struct->_sub_column_iterators[1].get());
        auto* map_iter = static_cast<MapFileColumnIterator*>(array_iter->_item_iterator.get());
        EXPECT_EQ(map_iter->_key_iterator->_reading_flag,
                  ColumnIterator::ReadingFlag::SKIP_READING);
        EXPECT_EQ(map_iter->_val_iterator->_reading_flag,
                  ColumnIterator::ReadingFlag::READING_FOR_PREDICATE);

        auto* value_struct = static_cast<StructFileColumnIterator*>(map_iter->_val_iterator.get());
        EXPECT_EQ(value_struct->_sub_column_iterators[0]->_reading_flag,
                  ColumnIterator::ReadingFlag::READING_FOR_PREDICATE);
        EXPECT_EQ(value_struct->_sub_column_iterators[1]->_reading_flag,
                  ColumnIterator::ReadingFlag::SKIP_READING);
    }

    {
        auto top_struct = build_nested_iterator();
        TColumnAccessPaths all_access_paths;
        all_access_paths.emplace_back();
        all_access_paths[0].data_access_path.path = {"root", "col2", "*"};
        TColumnAccessPaths predicate_access_paths;
        predicate_access_paths.emplace_back();
        predicate_access_paths[0].data_access_path.path = {"root", "col2", "*", "VALUES"};

        auto st = top_struct->set_access_paths(all_access_paths, predicate_access_paths);
        ASSERT_TRUE(st.ok()) << "failed to set access paths: " << st.to_string();

        auto* array_iter =
                static_cast<ArrayFileColumnIterator*>(top_struct->_sub_column_iterators[1].get());
        auto* map_iter = static_cast<MapFileColumnIterator*>(array_iter->_item_iterator.get());
        EXPECT_EQ(map_iter->_key_iterator->_reading_flag,
                  ColumnIterator::ReadingFlag::NEED_TO_READ);
        EXPECT_EQ(map_iter->_val_iterator->_reading_flag,
                  ColumnIterator::ReadingFlag::NEED_TO_READ);
    }

    {
        auto top_struct = build_nested_iterator();
        TColumnAccessPaths all_access_paths;
        all_access_paths.emplace_back();
        all_access_paths[0].data_access_path.path = {};
        TColumnAccessPaths predicate_access_paths;

        auto st = top_struct->set_access_paths(all_access_paths, predicate_access_paths);
        EXPECT_FALSE(st.ok());
    }

    {
        auto top_struct = build_nested_iterator();
        TColumnAccessPaths all_access_paths;
        all_access_paths.emplace_back();
        all_access_paths[0].data_access_path.path = {"wrong_root", "col2"};
        TColumnAccessPaths predicate_access_paths;

        auto st = top_struct->set_access_paths(all_access_paths, predicate_access_paths);
        EXPECT_FALSE(st.ok());
    }

    {
        auto top_struct = build_nested_iterator();
        TColumnAccessPaths all_access_paths;
        all_access_paths.emplace_back();
        all_access_paths[0].data_access_path.path = {"root", "col2", "wrong_item"};
        TColumnAccessPaths predicate_access_paths;

        auto st = top_struct->set_access_paths(all_access_paths, predicate_access_paths);
        EXPECT_FALSE(st.ok());
    }
}

TEST_F(ColumnReaderTest, DeepNestedAccessPathsFiveLevels) {
    auto make_item_struct = []() {
        auto null_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
        std::vector<ColumnIteratorUPtr> sub_iters;
        auto sub_p = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
        sub_p->set_column_name("p");
        auto sub_q = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
        sub_q->set_column_name("q");
        sub_iters.emplace_back(std::move(sub_p));
        sub_iters.emplace_back(std::move(sub_q));

        auto item_struct = std::make_unique<StructFileColumnIterator>(
                std::make_shared<ColumnReader>(), std::move(null_iter), std::move(sub_iters));
        item_struct->set_column_name("item");
        return item_struct;
    };

    auto make_value_struct = [make_item_struct]() {
        auto null_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
        std::vector<ColumnIteratorUPtr> sub_iters;
        auto array_offsets = std::make_unique<OffsetFileColumnIterator>(
                std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()));
        auto array_null = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
        auto array_iter = std::make_unique<ArrayFileColumnIterator>(
                std::make_shared<ColumnReader>(), std::move(array_offsets), make_item_struct(),
                std::move(array_null));
        array_iter->set_column_name("arr");
        sub_iters.emplace_back(std::move(array_iter));

        auto sub_z = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
        sub_z->set_column_name("z");
        sub_iters.emplace_back(std::move(sub_z));

        auto value_struct = std::make_unique<StructFileColumnIterator>(
                std::make_shared<ColumnReader>(), std::move(null_iter), std::move(sub_iters));
        value_struct->set_column_name("value");
        return value_struct;
    };

    auto map_null_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    auto map_offsets_iter = std::make_unique<OffsetFileColumnIterator>(
            std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()));
    auto map_key_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    map_key_iter->set_column_name("key");
    auto map_val_iter = make_value_struct();
    auto map_iter = std::make_unique<MapFileColumnIterator>(
            std::make_shared<ColumnReader>(), std::move(map_null_iter), std::move(map_offsets_iter),
            std::move(map_key_iter), std::move(map_val_iter));
    map_iter->set_column_name("m");

    auto struct_null_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    std::vector<ColumnIteratorUPtr> struct_sub_iters;
    auto sub_x = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    sub_x->set_column_name("x");
    struct_sub_iters.emplace_back(std::move(sub_x));
    struct_sub_iters.emplace_back(std::move(map_iter));
    auto top_struct = std::make_unique<StructFileColumnIterator>(std::make_shared<ColumnReader>(),
                                                                 std::move(struct_null_iter),
                                                                 std::move(struct_sub_iters));
    top_struct->set_column_name("root");

    TColumnAccessPaths all_access_paths;
    all_access_paths.emplace_back();
    all_access_paths[0].data_access_path.path = {"root", "m", "VALUES", "arr", "*"};
    TColumnAccessPaths predicate_access_paths;
    predicate_access_paths.emplace_back();
    predicate_access_paths[0].data_access_path.path = {"root", "m", "VALUES", "arr", "*", "q"};

    auto st = top_struct->set_access_paths(all_access_paths, predicate_access_paths);
    ASSERT_TRUE(st.ok()) << "failed to set deep access paths: " << st.to_string();

    auto* map_ptr = static_cast<MapFileColumnIterator*>(top_struct->_sub_column_iterators[1].get());
    EXPECT_EQ(map_ptr->_key_iterator->_reading_flag, ColumnIterator::ReadingFlag::SKIP_READING);
    EXPECT_EQ(map_ptr->_val_iterator->_reading_flag,
              ColumnIterator::ReadingFlag::READING_FOR_PREDICATE);

    auto* value_struct = static_cast<StructFileColumnIterator*>(map_ptr->_val_iterator.get());
    auto* array_iter =
            static_cast<ArrayFileColumnIterator*>(value_struct->_sub_column_iterators[0].get());
    auto* item_struct = static_cast<StructFileColumnIterator*>(array_iter->_item_iterator.get());
    EXPECT_EQ(item_struct->_sub_column_iterators[0]->_reading_flag,
              ColumnIterator::ReadingFlag::NEED_TO_READ);
    EXPECT_EQ(item_struct->_sub_column_iterators[1]->_reading_flag,
              ColumnIterator::ReadingFlag::READING_FOR_PREDICATE);
}

TEST_F(ColumnReaderTest, NestedNeedToReadInLazyPredicateMode) {
    auto struct_null_iterator =
            std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    std::vector<ColumnIteratorUPtr> struct_sub_iters;
    struct_sub_iters.emplace_back(
            std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()));
    struct_sub_iters.emplace_back(
            std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()));
    StructFileColumnIterator struct_iterator(std::make_shared<ColumnReader>(),
                                             std::move(struct_null_iterator),
                                             std::move(struct_sub_iters));
    struct_iterator.set_reading_mode(ColumnIterator::ReadingMode::LAZY);
    struct_iterator.set_reading_flag(ColumnIterator::ReadingFlag::READING_FOR_PREDICATE);
    EXPECT_TRUE(struct_iterator.need_to_read());

    auto array_item_iterator =
            std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    auto array_offsets_iter = std::make_unique<OffsetFileColumnIterator>(
            std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()));
    auto array_null_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    ArrayFileColumnIterator array_iterator(
            std::make_shared<ColumnReader>(), std::move(array_offsets_iter),
            std::move(array_item_iterator), std::move(array_null_iter));
    array_iterator.set_reading_mode(ColumnIterator::ReadingMode::LAZY);
    array_iterator.set_reading_flag(ColumnIterator::ReadingFlag::READING_FOR_PREDICATE);
    EXPECT_TRUE(array_iterator.need_to_read());

    auto map_null_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    auto map_offsets_iter = std::make_unique<OffsetFileColumnIterator>(
            std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()));
    auto map_key_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    auto map_val_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    MapFileColumnIterator map_iterator(std::make_shared<ColumnReader>(), std::move(map_null_iter),
                                       std::move(map_offsets_iter), std::move(map_key_iter),
                                       std::move(map_val_iter));
    map_iterator.set_reading_mode(ColumnIterator::ReadingMode::LAZY);
    map_iterator.set_reading_flag(ColumnIterator::ReadingFlag::READING_FOR_PREDICATE);
    EXPECT_TRUE(map_iterator.need_to_read());
}

TEST_F(ColumnReaderTest, NestedReadingModeNeedToReadMatrix) {
    auto build_nested_iterator = []() {
        auto make_value_struct = []() {
            auto null_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
            std::vector<ColumnIteratorUPtr> sub_iters;
            auto sub_a = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
            sub_a->set_column_name("a");
            auto sub_b = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
            sub_b->set_column_name("b");
            sub_iters.emplace_back(std::move(sub_a));
            sub_iters.emplace_back(std::move(sub_b));

            auto value_struct = std::make_unique<StructFileColumnIterator>(
                    std::make_shared<ColumnReader>(), std::move(null_iter), std::move(sub_iters));
            value_struct->set_column_name("value");
            return value_struct;
        };

        auto map_null_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
        auto map_offsets_iter = std::make_unique<OffsetFileColumnIterator>(
                std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()));
        auto map_key_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
        map_key_iter->set_column_name("key");
        auto map_val_iter = make_value_struct();
        auto map_iterator = std::make_unique<MapFileColumnIterator>(
                std::make_shared<ColumnReader>(), std::move(map_null_iter),
                std::move(map_offsets_iter), std::move(map_key_iter), std::move(map_val_iter));
        map_iterator->set_column_name("item");

        auto array_null_iter =
                std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
        auto array_offsets_iter = std::make_unique<OffsetFileColumnIterator>(
                std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()));
        auto array_iterator = std::make_unique<ArrayFileColumnIterator>(
                std::make_shared<ColumnReader>(), std::move(array_offsets_iter),
                std::move(map_iterator), std::move(array_null_iter));
        array_iterator->set_column_name("col2");

        auto struct_null_iter =
                std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
        std::vector<ColumnIteratorUPtr> struct_sub_iters;
        auto sub_col1 = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
        sub_col1->set_column_name("col1");
        struct_sub_iters.emplace_back(std::move(sub_col1));
        struct_sub_iters.emplace_back(std::move(array_iterator));
        auto top_struct = std::make_unique<StructFileColumnIterator>(
                std::make_shared<ColumnReader>(), std::move(struct_null_iter),
                std::move(struct_sub_iters));
        top_struct->set_column_name("root");
        return top_struct;
    };

    auto assert_need_to_read = [](StructFileColumnIterator* top_struct) {
        auto* array_iter =
                static_cast<ArrayFileColumnIterator*>(top_struct->_sub_column_iterators[1].get());
        auto* map_iter = static_cast<MapFileColumnIterator*>(array_iter->_item_iterator.get());
        auto* value_struct = static_cast<StructFileColumnIterator*>(map_iter->_val_iterator.get());
        auto expect_scalar = [](ColumnIterator::ReadingFlag flag,
                                ColumnIterator::ReadingMode mode) {
            switch (mode) {
            case ColumnIterator::ReadingMode::NORMAL:
                return flag != ColumnIterator::ReadingFlag::SKIP_READING;
            case ColumnIterator::ReadingMode::PREDICATE:
                return flag == ColumnIterator::ReadingFlag::READING_FOR_PREDICATE;
            case ColumnIterator::ReadingMode::LAZY:
                return flag == ColumnIterator::ReadingFlag::NEED_TO_READ;
            default:
                return false;
            }
        };
        auto expect_nested = [](ColumnIterator::ReadingFlag flag,
                                ColumnIterator::ReadingMode mode) {
            switch (mode) {
            case ColumnIterator::ReadingMode::NORMAL:
                return flag != ColumnIterator::ReadingFlag::SKIP_READING;
            case ColumnIterator::ReadingMode::PREDICATE:
                return flag == ColumnIterator::ReadingFlag::READING_FOR_PREDICATE;
            case ColumnIterator::ReadingMode::LAZY:
                return flag == ColumnIterator::ReadingFlag::NEED_TO_READ ||
                       flag == ColumnIterator::ReadingFlag::READING_FOR_PREDICATE;
            default:
                return false;
            }
        };

        top_struct->set_reading_mode(ColumnIterator::ReadingMode::NORMAL);
        EXPECT_EQ(expect_nested(top_struct->reading_flag(), ColumnIterator::ReadingMode::NORMAL),
                  top_struct->need_to_read());
        EXPECT_EQ(expect_nested(array_iter->reading_flag(), ColumnIterator::ReadingMode::NORMAL),
                  array_iter->need_to_read());
        EXPECT_EQ(expect_nested(map_iter->reading_flag(), ColumnIterator::ReadingMode::NORMAL),
                  map_iter->need_to_read());
        EXPECT_EQ(expect_nested(value_struct->reading_flag(), ColumnIterator::ReadingMode::NORMAL),
                  value_struct->need_to_read());
        EXPECT_EQ(expect_scalar(map_iter->_key_iterator->reading_flag(),
                                ColumnIterator::ReadingMode::NORMAL),
                  map_iter->_key_iterator->need_to_read());
        EXPECT_EQ(expect_nested(map_iter->_val_iterator->reading_flag(),
                                ColumnIterator::ReadingMode::NORMAL),
                  map_iter->_val_iterator->need_to_read());

        top_struct->set_reading_mode(ColumnIterator::ReadingMode::PREDICATE);
        EXPECT_EQ(expect_nested(top_struct->reading_flag(), ColumnIterator::ReadingMode::PREDICATE),
                  top_struct->need_to_read());
        EXPECT_EQ(expect_nested(array_iter->reading_flag(), ColumnIterator::ReadingMode::PREDICATE),
                  array_iter->need_to_read());
        EXPECT_EQ(expect_nested(map_iter->reading_flag(), ColumnIterator::ReadingMode::PREDICATE),
                  map_iter->need_to_read());
        EXPECT_EQ(
                expect_nested(value_struct->reading_flag(), ColumnIterator::ReadingMode::PREDICATE),
                value_struct->need_to_read());
        EXPECT_EQ(expect_scalar(map_iter->_key_iterator->reading_flag(),
                                ColumnIterator::ReadingMode::PREDICATE),
                  map_iter->_key_iterator->need_to_read());
        EXPECT_EQ(expect_nested(map_iter->_val_iterator->reading_flag(),
                                ColumnIterator::ReadingMode::PREDICATE),
                  map_iter->_val_iterator->need_to_read());

        top_struct->set_reading_mode(ColumnIterator::ReadingMode::LAZY);
        EXPECT_EQ(expect_nested(top_struct->reading_flag(), ColumnIterator::ReadingMode::LAZY),
                  top_struct->need_to_read());
        EXPECT_EQ(expect_nested(array_iter->reading_flag(), ColumnIterator::ReadingMode::LAZY),
                  array_iter->need_to_read());
        EXPECT_EQ(expect_nested(map_iter->reading_flag(), ColumnIterator::ReadingMode::LAZY),
                  map_iter->need_to_read());
        EXPECT_EQ(expect_nested(value_struct->reading_flag(), ColumnIterator::ReadingMode::LAZY),
                  value_struct->need_to_read());
        EXPECT_EQ(expect_scalar(map_iter->_key_iterator->reading_flag(),
                                ColumnIterator::ReadingMode::LAZY),
                  map_iter->_key_iterator->need_to_read());
        EXPECT_EQ(expect_nested(map_iter->_val_iterator->reading_flag(),
                                ColumnIterator::ReadingMode::LAZY),
                  map_iter->_val_iterator->need_to_read());
    };

    {
        auto top_struct = build_nested_iterator();
        TColumnAccessPaths all_access_paths;
        all_access_paths.emplace_back();
        all_access_paths[0].data_access_path.path = {"root", "col2", "*", "VALUES", "a"};
        TColumnAccessPaths predicate_access_paths = all_access_paths;

        auto st = top_struct->set_access_paths(all_access_paths, predicate_access_paths);
        ASSERT_TRUE(st.ok()) << "failed to set access paths: " << st.to_string();
        assert_need_to_read(top_struct.get());
    }

    {
        auto top_struct = build_nested_iterator();
        TColumnAccessPaths all_access_paths;
        all_access_paths.emplace_back();
        all_access_paths[0].data_access_path.path = {"root", "col2", "*", "KEYS"};
        TColumnAccessPaths predicate_access_paths;

        auto st = top_struct->set_access_paths(all_access_paths, predicate_access_paths);
        ASSERT_TRUE(st.ok()) << "failed to set access paths: " << st.to_string();
        assert_need_to_read(top_struct.get());
    }

    {
        auto top_struct = build_nested_iterator();
        TColumnAccessPaths all_access_paths;
        all_access_paths.emplace_back();
        all_access_paths[0].data_access_path.path = {"root", "col2", "*"};
        TColumnAccessPaths predicate_access_paths;
        predicate_access_paths.emplace_back();
        predicate_access_paths[0].data_access_path.path = {"root", "col2", "*", "VALUES"};

        auto st = top_struct->set_access_paths(all_access_paths, predicate_access_paths);
        ASSERT_TRUE(st.ok()) << "failed to set access paths: " << st.to_string();
        assert_need_to_read(top_struct.get());
    }

    {
        auto top_struct = build_nested_iterator();
        TColumnAccessPaths all_access_paths;
        all_access_paths.emplace_back();
        all_access_paths[0].data_access_path.path = {"root", "col2", "*", "VALUES"};
        TColumnAccessPaths predicate_access_paths;
        predicate_access_paths.emplace_back();
        predicate_access_paths[0].data_access_path.path = {"root", "col2", "*", "KEYS"};

        auto st = top_struct->set_access_paths(all_access_paths, predicate_access_paths);
        EXPECT_TRUE(st.ok());
    }
}

TEST_F(ColumnReaderTest, MultiAccessPaths) {
    auto create_struct_iterator = []() {
        auto null_reader = std::make_shared<ColumnReader>();
        auto null_iterator = std::make_unique<FileColumnIterator>(null_reader);

        std::vector<ColumnIteratorUPtr> sub_column_iterators;
        auto sub_reader1 = std::make_shared<ColumnReader>();
        auto sub_iterator1 = std::make_unique<FileColumnIterator>(sub_reader1);
        sub_iterator1->set_column_name("sub_col_1");
        auto sub_reader2 = std::make_shared<ColumnReader>();
        auto sub_iterator2 = std::make_unique<FileColumnIterator>(sub_reader2);
        sub_iterator2->set_column_name("sub_col_2");

        sub_column_iterators.emplace_back(std::move(sub_iterator1));
        sub_column_iterators.emplace_back(std::move(sub_iterator2));
        auto iterator = std::make_unique<StructFileColumnIterator>(std::make_shared<ColumnReader>(),
                                                                   std::move(null_iterator),
                                                                   std::move(sub_column_iterators));
        return iterator;
    };

    auto create_struct_iterator2 = [](ColumnIteratorUPtr&& nested_iterator) {
        auto null_reader = std::make_shared<ColumnReader>();
        auto null_iterator = std::make_unique<FileColumnIterator>(null_reader);

        std::vector<ColumnIteratorUPtr> sub_column_iterators;
        auto sub_reader1 = std::make_shared<ColumnReader>();
        auto sub_iterator1 = std::make_unique<FileColumnIterator>(sub_reader1);
        sub_iterator1->set_column_name("sub_col_1");

        sub_column_iterators.emplace_back(std::move(sub_iterator1));
        sub_column_iterators.emplace_back(std::move(nested_iterator));
        auto iterator = std::make_unique<StructFileColumnIterator>(std::make_shared<ColumnReader>(),
                                                                   std::move(null_iterator),
                                                                   std::move(sub_column_iterators));
        return iterator;
    };

    auto struct_iterator = create_struct_iterator();
    struct_iterator->set_column_name("struct");

    auto map_iterator = std::make_unique<MapFileColumnIterator>(
            std::make_shared<ColumnReader>(),
            std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()), // null iterator
            std::make_unique<OffsetFileColumnIterator>(
                    std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>())),
            std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()),
            std::move(struct_iterator));

    auto array_iterator = std::make_unique<ArrayFileColumnIterator>(
            std::make_shared<ColumnReader>(),
            std::make_unique<OffsetFileColumnIterator>(
                    std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>())),
            std::move(map_iterator),
            std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()));

    // here create:
    // struct<
    //      sub_col_1,
    //      sub_col_2: array<
    //          map<
    //              key,
    //              value: struct<
    //                  sub_col_1,
    //                  sub_col_2
    //              >
    //          >
    //      >
    //  >
    array_iterator->set_column_name("sub_col_2");
    auto iterator = create_struct_iterator2(std::move(array_iterator));
    TColumnAccessPaths all_access_paths;
    all_access_paths.emplace_back();

    // all access paths:
    // self.sub_col_2.*.KEYS
    // predicates paths empty
    all_access_paths[0].data_access_path.path = {"self", "sub_col_2", "*", "KEYS"};

    TColumnAccessPaths predicate_access_paths;

    iterator->set_column_name("self");
    auto st = iterator->set_access_paths(all_access_paths, predicate_access_paths);

    ASSERT_TRUE(st.ok()) << "failed to set access paths: " << st.to_string();
    ASSERT_EQ(iterator->_reading_flag, ColumnIterator::ReadingFlag::NEED_TO_READ);

    ASSERT_EQ(iterator->_sub_column_iterators[0]->_reading_flag,
              ColumnIterator::ReadingFlag::SKIP_READING);
    ASSERT_EQ(iterator->_sub_column_iterators[1]->_reading_flag,
              ColumnIterator::ReadingFlag::NEED_TO_READ);

    auto* array_iter =
            static_cast<ArrayFileColumnIterator*>(iterator->_sub_column_iterators[1].get());
    ASSERT_EQ(array_iter->_item_iterator->_reading_flag, ColumnIterator::ReadingFlag::NEED_TO_READ);

    auto* map_iter = static_cast<MapFileColumnIterator*>(array_iter->_item_iterator.get());
    ASSERT_EQ(map_iter->_key_iterator->_reading_flag, ColumnIterator::ReadingFlag::NEED_TO_READ);
    ASSERT_EQ(map_iter->_val_iterator->_reading_flag, ColumnIterator::ReadingFlag::SKIP_READING);
}

TEST_F(ColumnReaderTest, OffsetPeekUsesPageSentinelWhenNoRemaining) {
    // create a bare FileColumnIterator with a dummy ColumnReader
    auto reader = std::make_shared<ColumnReader>();
    auto file_iter = std::make_unique<FileColumnIterator>(reader);
    auto* page = file_iter->get_current_page();

    // simulate a page that has no remaining offsets in decoder but has a valid
    // next_array_item_ordinal recorded in footer
    page->num_rows = 0;
    page->offset_in_page = 0;
    page->next_array_item_ordinal = 12345;

    OffsetFileColumnIterator offset_iter(std::move(file_iter));
    ordinal_t offset = 0;
    auto st = offset_iter._peek_one_offset(&offset);

    ASSERT_TRUE(st.ok()) << "peek one offset failed: " << st.to_string();
    ASSERT_EQ(static_cast<ordinal_t>(12345), offset);
}

TEST_F(ColumnReaderTest, OffsetCalculateOffsetsUsesPageSentinelForLastOffset) {
    // create offset iterator with a page whose sentinel offset is set in footer
    auto reader = std::make_shared<ColumnReader>();
    auto file_iter = std::make_unique<FileColumnIterator>(reader);
    auto* page = file_iter->get_current_page();

    // simulate page with no remaining values, but a valid next_array_item_ordinal
    page->num_rows = 0;
    page->offset_in_page = 0;
    page->next_array_item_ordinal = 15;

    OffsetFileColumnIterator offset_iter(std::move(file_iter));

    // prepare in-memory column offsets:
    // offsets_data = [first_column_offset, first_storage_offset, next_storage_offset_placeholder]
    // first_column_offset = 100
    // first_storage_offset = 10
    // placeholder real next_storage_offset will be fetched from page sentinel (15)
    vectorized::ColumnArray::ColumnOffsets column_offsets;
    auto& data = column_offsets.get_data();
    data.push_back(100); // index 0: first_column_offset
    data.push_back(10);  // index 1: first_storage_offset
    data.push_back(12);  // index 2: placeholder storage offset for middle element

    auto st = offset_iter._calculate_offsets(1, column_offsets);
    ASSERT_TRUE(st.ok()) << "calculate offsets failed: " << st.to_string();

    // after calculation:
    // data[1] = 100 + (12 - 10) = 102
    // data[2] = 100 + (15 - 10) = 105 (using page sentinel as next_storage_offset)
    ASSERT_EQ(static_cast<ordinal_t>(100), data[0]);
    ASSERT_EQ(static_cast<ordinal_t>(102), data[1]);
    ASSERT_EQ(static_cast<ordinal_t>(105), data[2]);
}

TEST_F(ColumnReaderTest, MapReadByRowidsSkipReadingResizesDestination) {
    // create a basic map iterator with dummy readers/iterators
    auto map_reader = std::make_shared<ColumnReader>();
    auto null_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    auto offsets_iter = std::make_unique<OffsetFileColumnIterator>(
            std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()));
    auto key_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    auto val_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());

    MapFileColumnIterator map_iter(map_reader, std::move(null_iter), std::move(offsets_iter),
                                   std::move(key_iter), std::move(val_iter));
    map_iter.set_column_name("map_col");
    map_iter.set_reading_flag(ColumnIterator::ReadingFlag::SKIP_READING);

    // prepare an empty ColumnMap as destination
    auto keys = vectorized::ColumnInt32::create();
    auto values = vectorized::ColumnInt32::create();
    auto offsets = vectorized::ColumnArray::ColumnOffsets::create();
    auto column_map =
            vectorized::ColumnMap::create(std::move(keys), std::move(values), std::move(offsets));
    vectorized::MutableColumnPtr dst = std::move(column_map);

    const rowid_t rowids[] = {1, 5, 7};
    size_t count = sizeof(rowids) / sizeof(rowids[0]);
    auto st = map_iter.read_by_rowids(rowids, count, dst);

    ASSERT_TRUE(st.ok()) << "read_by_rowids failed: " << st.to_string();
    ASSERT_EQ(count, dst->size());
}
} // namespace doris::segment_v2
