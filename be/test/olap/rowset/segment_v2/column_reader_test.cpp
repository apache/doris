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