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

#include "olap/rowid_spill_manager.h"

#include <gtest/gtest.h>

#include <filesystem>

#include "io/fs/local_file_system.h"

using namespace doris;

static const uint32_t MAX_PATH_LEN = 1024;

class RowIdSpillManagerTest : public testing::Test {
public:
    void SetUp() override {
        char buffer[MAX_PATH_LEN];
        EXPECT_NE(getcwd(buffer, MAX_PATH_LEN), nullptr);
        absolute_dir = std::string(buffer) + "/rowid_spill_test";
        auto st = io::global_local_filesystem()->delete_directory(absolute_dir);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(absolute_dir);
        ASSERT_TRUE(st.ok()) << st;
    }

    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(absolute_dir).ok());
    }

public:
    RowIdMappingType make_mapping(
            const std::vector<std::tuple<uint32_t, uint32_t, uint32_t>>& triples) {
        RowIdMappingType map(&mr);
        for (auto& [src, dst_seg, dst_row] : triples) {
            map.emplace(src, std::make_pair(dst_seg, dst_row));
        }
        return map;
    }
    std::string temp_spill_file() {
        static int count = 0;
        return fmt::format("{}/{}.spill", absolute_dir, count++);
    }

    std::string absolute_dir;
    std::pmr::monotonic_buffer_resource mr;
};

TEST_F(RowIdSpillManagerTest, InitAndNewSegment) {
    std::string path = temp_spill_file();
    RowIdSpillManager mgr(path);

    ASSERT_TRUE(mgr.init().ok());

    ASSERT_TRUE(mgr.init_new_segment(0, 10).ok());
    ASSERT_TRUE(mgr.init_new_segment(1, 20).ok());
}

TEST_F(RowIdSpillManagerTest, SpillAndReadSmall) {
    std::string path = temp_spill_file();
    RowIdSpillManager mgr(path);
    ASSERT_TRUE(mgr.init().ok());
    ASSERT_TRUE(mgr.init_new_segment(0, 5).ok());

    auto mappings = make_mapping({{0, 100, 200}, {1, 101, 201}, {2, 102, 202}});

    ASSERT_TRUE(mgr.spill_segment_mapping(0, mappings).ok());

    RowIdMappingType read_map(&mr);
    ASSERT_TRUE(mgr.read_segment_mapping(0, &read_map).ok());

    ASSERT_EQ(read_map.size(), mappings.size());
    for (auto& kv : mappings) {
        ASSERT_EQ(read_map[kv.first], kv.second);
    }
}

TEST_F(RowIdSpillManagerTest, SpillEmptyMappings) {
    std::string path = temp_spill_file();
    RowIdSpillManager mgr(path);
    ASSERT_TRUE(mgr.init().ok());
    ASSERT_TRUE(mgr.init_new_segment(0, 3).ok());

    auto mappings = make_mapping({
            {0, UINT32_MAX, UINT32_MAX}, // 空映射应跳过
            {1, UINT32_MAX, UINT32_MAX},
    });

    ASSERT_TRUE(mgr.spill_segment_mapping(0, mappings).ok());

    RowIdMappingType read_map(&mr);
    ASSERT_TRUE(mgr.read_segment_mapping(0, &read_map).ok());
    ASSERT_TRUE(read_map.empty());
}

TEST_F(RowIdSpillManagerTest, InvalidSegmentId) {
    std::string path = temp_spill_file();
    RowIdSpillManager mgr(path);
    ASSERT_TRUE(mgr.init().ok());
    ASSERT_TRUE(mgr.init_new_segment(0, 5).ok());

    auto mappings = make_mapping({{0, 1, 2}});
    // spill 非法 ID
    auto st1 = mgr.spill_segment_mapping(5, mappings);
    ASSERT_FALSE(st1.ok());

    RowIdMappingType read_map(&mr);
    auto st2 = mgr.read_segment_mapping(5, &read_map);
    ASSERT_FALSE(st2.ok());
}

TEST_F(RowIdSpillManagerTest, LargeData) {
    std::string path = temp_spill_file();
    RowIdSpillManager mgr(path);
    ASSERT_TRUE(mgr.init().ok());

    const uint32_t row_count = 50000;
    ASSERT_TRUE(mgr.init_new_segment(0, row_count).ok());

    RowIdMappingType mappings(&mr);
    for (uint32_t i = 0; i < row_count; i++) {
        mappings.emplace(i, std::make_pair(i % 100, i + 1000));
    }

    ASSERT_TRUE(mgr.spill_segment_mapping(0, mappings).ok());

    RowIdMappingType read_map(&mr);
    ASSERT_TRUE(mgr.read_segment_mapping(0, &read_map).ok());

    ASSERT_EQ(read_map.size(), mappings.size());
    for (auto& kv : mappings) {
        ASSERT_EQ(read_map[kv.first], kv.second);
    }
}

TEST_F(RowIdSpillManagerTest, MultipleSegments) {
    std::string path = temp_spill_file();
    RowIdSpillManager mgr(path);
    ASSERT_TRUE(mgr.init().ok());

    ASSERT_TRUE(mgr.init_new_segment(0, 3).ok());
    ASSERT_TRUE(mgr.init_new_segment(1, 2).ok());

    auto map0 = make_mapping({{0, 10, 20}, {1, 11, 21}});
    auto map1 = make_mapping({{0, 30, 40}});

    ASSERT_TRUE(mgr.spill_segment_mapping(0, map0).ok());
    ASSERT_TRUE(mgr.spill_segment_mapping(1, map1).ok());

    RowIdMappingType read0(&mr), read1(&mr);
    ASSERT_TRUE(mgr.read_segment_mapping(0, &read0).ok());
    ASSERT_TRUE(mgr.read_segment_mapping(1, &read1).ok());

    ASSERT_EQ(read0, map0);
    ASSERT_EQ(read1, map1);
}

TEST_F(RowIdSpillManagerTest, LargeDataMultiSegment) {
    std::string path = temp_spill_file();
    RowIdSpillManager mgr(path);
    ASSERT_TRUE(mgr.init().ok());

    constexpr int segment_count = 3;
    constexpr uint32_t row_counts[segment_count] = {30000, 20000, 40000};

    for (int i = 0; i < segment_count; ++i) {
        ASSERT_TRUE(mgr.init_new_segment(i, row_counts[i]).ok());
    }

    // 生成映射，保证每个segment内的 key 唯一
    std::vector<RowIdMappingType> segments_data;
    for (int i = 0; i < segment_count; ++i) {
        RowIdMappingType map(&mr);
        for (uint32_t r = 0; r < row_counts[i]; ++r) {
            // key在当前segment内唯一，从0开始计数即可
            map.emplace(r, std::make_pair(i, r + 1000));
        }
        segments_data.push_back(std::move(map));
    }

    // 交替 spill，不按 segment 顺序写入
    ASSERT_TRUE(mgr.spill_segment_mapping(0, segments_data[0]).ok());
    ASSERT_TRUE(mgr.spill_segment_mapping(2, segments_data[2]).ok());
    ASSERT_TRUE(mgr.spill_segment_mapping(1, segments_data[1]).ok());

    // 对第2段追加写入，key仍旧唯一（不同段允许重复，追加写入这里key避免重复）
    RowIdMappingType extra_map(&mr);
    // 追加写入，key从之前最大key+1开始，这里用row_counts[2]作为起点
    for (uint32_t k = row_counts[2]; k < row_counts[2] + 100; ++k) {
        extra_map.emplace(k, std::make_pair(2, 9999 + k));
    }
    ASSERT_TRUE(mgr.spill_segment_mapping(2, extra_map).ok());

    // 读取校验
    for (int i = 0; i < segment_count; ++i) {
        RowIdMappingType read_map(&mr);
        ASSERT_TRUE(mgr.read_segment_mapping(i, &read_map).ok());

        if (i == 2) {
            // 合并 segments_data[2] + extra_map
            ASSERT_EQ(read_map.size(), segments_data[2].size() + extra_map.size());
            for (auto& kv : segments_data[2]) {
                ASSERT_EQ(read_map[kv.first], kv.second);
            }
            for (auto& kv : extra_map) {
                ASSERT_EQ(read_map[kv.first], kv.second);
            }
        } else {
            ASSERT_EQ(read_map, segments_data[i]);
        }
    }
}

TEST_F(RowIdSpillManagerTest, InitAndSpillAlternatingMultipleSegments) {
    std::string path = temp_spill_file();
    RowIdSpillManager mgr(path);
    ASSERT_TRUE(mgr.init().ok());

    // 先初始化第0段
    ASSERT_TRUE(mgr.init_new_segment(0, 15000).ok());

    // 准备第0段映射数据（keys从0开始唯一）
    RowIdMappingType segment0_map(&mr);
    for (uint32_t i = 0; i < 10000; ++i) {
        segment0_map.emplace(i, std::make_pair(0, i + 100));
    }
    // 写入第0段
    ASSERT_TRUE(mgr.spill_segment_mapping(0, segment0_map).ok());
    std::cout << fmt::format("after spill segment 0:\n{}", mgr.dump_info()) << std::endl;
    // 初始化第1段
    ASSERT_TRUE(mgr.init_new_segment(1, 8000).ok());

    // 准备第1段映射数据（keys从0开始唯一，和第0段无关）
    RowIdMappingType segment1_map(&mr);
    for (uint32_t i = 0; i < 8000; ++i) {
        segment1_map.emplace(i, std::make_pair(1, i + 200));
    }
    // 写入第1段
    ASSERT_TRUE(mgr.spill_segment_mapping(1, segment1_map).ok());
    std::cout << fmt::format("after spill segment 1:\n{}", mgr.dump_info()) << std::endl;
    // 追加初始化第2段
    ASSERT_TRUE(mgr.init_new_segment(2, 12000).ok());

    // 准备第2段映射数据
    RowIdMappingType segment2_map(&mr);
    for (uint32_t i = 0; i < 12000; ++i) {
        segment2_map.emplace(i, std::make_pair(2, i + 300));
    }
    // 写入第2段
    ASSERT_TRUE(mgr.spill_segment_mapping(2, segment2_map).ok());
    std::cout << fmt::format("after spill segment 2:\n{}", mgr.dump_info()) << std::endl;

    // 追加写入第0段（key从之前最大 + 1 开始，避免重复）
    RowIdMappingType segment0_extra(&mr);
    for (uint32_t i = 10000; i < 10200; ++i) {
        segment0_extra.emplace(i, std::make_pair(0, i + 400));
    }
    ASSERT_TRUE(mgr.spill_segment_mapping(0, segment0_extra).ok());
    std::cout << fmt::format("after spill segment 0 again:\n{}", mgr.dump_info()) << std::endl;

    // 读取并验证所有段数据
    for (int seg = 0; seg <= 2; ++seg) {
        RowIdMappingType read_map;
        ASSERT_TRUE(mgr.read_segment_mapping(seg, &read_map).ok());

        if (seg == 0) {
            // 合并 segment0_map + segment0_extra
            ASSERT_EQ(read_map.size(), segment0_map.size() + segment0_extra.size());
            for (auto& kv : segment0_map) {
                ASSERT_EQ(read_map[kv.first], kv.second);
            }
            for (auto& kv : segment0_extra) {
                ASSERT_EQ(read_map[kv.first], kv.second);
            }
        } else if (seg == 1) {
            for (auto& kv : segment1_map) {
                ASSERT_EQ(read_map[kv.first], kv.second);
            }
        } else if (seg == 2) {
            for (auto& kv : segment2_map) {
                ASSERT_EQ(read_map[kv.first], kv.second);
            }
        }
    }
}
