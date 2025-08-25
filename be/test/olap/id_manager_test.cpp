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

#include "olap/id_manager.h"

#include <gtest/gtest.h>

#include <atomic>
#include <memory>
#include <thread>
#include <vector>

#include "olap/olap_common.h"

using namespace doris;

TEST(IdFileMapTest, BasicOperations) {
    IdFileMap id_file_map(1024);

    // Test adding a file mapping

    int64_t tablet_id = 1;
    RowsetId rowset_id;
    rowset_id.init(2);
    uint32_t segment_id = 3;

    auto mapping1 = std::make_shared<FileMapping>(tablet_id, rowset_id, segment_id);
    uint32_t id1 = id_file_map.get_file_mapping_id(mapping1);
    EXPECT_EQ(id1, 0);

    TFileRangeDesc scan_range;
    scan_range.path = "https:a/b/c/d/a.parquet";
    scan_range.start_offset = 120;

    auto mapping2 = std::make_shared<FileMapping>(2, scan_range, false);
    uint32_t id2 = id_file_map.get_file_mapping_id(mapping2);
    EXPECT_EQ(id2, 1);

    // Test getting a file mapping
    auto retrieved_mapping1 = id_file_map.get_file_mapping(id1);
    EXPECT_EQ(retrieved_mapping1->type, FileMappingType::INTERNAL);
    std::cout << retrieved_mapping1->file_mapping_info_to_string() << "\n";
    auto internal = retrieved_mapping1->file_mapping_info_to_string();

    int64_t tablet_id_ans = 0;
    memcpy(&tablet_id_ans, internal.data(), sizeof(tablet_id_ans));
    EXPECT_EQ(tablet_id_ans, tablet_id);

    RowsetId rowset_id_ans;
    memcpy(&rowset_id_ans, internal.data() + sizeof(tablet_id_ans), sizeof(rowset_id_ans));
    EXPECT_EQ(rowset_id_ans, rowset_id);

    uint32_t segment_id_ans = 0;
    memcpy(&segment_id_ans, internal.data() + sizeof(tablet_id_ans) + sizeof(rowset_id_ans),
           sizeof(segment_id_ans));
    EXPECT_EQ(segment_id_ans, segment_id);

    auto retrieved_mapping2 = id_file_map.get_file_mapping(id2);
    EXPECT_EQ(retrieved_mapping2->type, FileMappingType::EXTERNAL);
    auto str = retrieved_mapping2->file_mapping_info_to_string();
    EXPECT_TRUE(str.find(scan_range.path) != str.npos);

    // Test getting a non-existent file mapping
    auto retrieved_mapping3 = id_file_map.get_file_mapping(999);
    EXPECT_EQ(retrieved_mapping3, nullptr);
}

TEST(IdFileMapTest, ConcurrentAddAndGet) {
    IdFileMap id_file_map(1024);
    std::vector<std::thread> threads;

    int64_t tablet_id = 1;
    RowsetId rowset_id;
    rowset_id.init(2);

    for (int i = 0; i < 10; ++i) {
        threads.emplace_back([i, &id_file_map, &tablet_id, &rowset_id]() {
            for (int j = 0; j < 100; ++j) {
                uint32_t segment_id = i * 1000 + j;

                auto mapping = std::make_shared<FileMapping>(
                        FileMapping {tablet_id, rowset_id, segment_id});
                uint32_t id = id_file_map.get_file_mapping_id(mapping);
                auto retrieved_mapping = id_file_map.get_file_mapping(id);
                EXPECT_EQ(retrieved_mapping->type, mapping->type);
                EXPECT_EQ(retrieved_mapping->file_mapping_info_to_string(),
                          mapping->file_mapping_info_to_string());
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }
}

TEST(IdManagerTest, BasicOperations) {
    IdManager id_manager;

    // Test adding an IdFileMap
    UniqueId query_id1 = UniqueId::gen_uid();
    auto id_file_map1 = id_manager.add_id_file_map(query_id1, 1024);
    EXPECT_NE(id_file_map1, nullptr);

    UniqueId query_id2 = UniqueId::gen_uid();
    auto id_file_map2 = id_manager.add_id_file_map(query_id2, 1024);
    EXPECT_NE(id_file_map2, nullptr);

    // Test getting an existing IdFileMap
    auto retrieved_id_file_map1 = id_manager.add_id_file_map(query_id1, 1024);
    EXPECT_EQ(retrieved_id_file_map1, id_file_map1);

    // Test removing an IdFileMap
    id_manager.remove_id_file_map(query_id1);
    auto retrieved_id_file_map2 = id_manager.add_id_file_map(query_id1, 1024);
    EXPECT_NE(retrieved_id_file_map2, id_file_map1);
}

TEST(IdManagerTest, ConcurrentAddAndRemove) {
    IdManager id_manager;
    std::vector<std::thread> threads;

    for (int i = 0; i < 10; ++i) {
        threads.emplace_back([&]() {
            for (int j = 0; j < 10; ++j) {
                UniqueId query_id = UniqueId::gen_uid();
                auto id_file_map = id_manager.add_id_file_map(query_id, 1024);
                EXPECT_NE(id_file_map, nullptr);

                id_manager.remove_id_file_map(query_id);
                auto retrieved_id_file_map = id_manager.add_id_file_map(query_id, 1024);
                EXPECT_NE(retrieved_id_file_map, id_file_map);
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }
}
