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
#include <unistd.h>

#include <memory>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "io/fs/local_file_system.h"
#include "olap/olap_common.h"
#include "olap/rowid_conversion.h"
#include "olap/rowid_conversion_storage.h"
#include "olap/rowid_spill_manager.h"
#include "olap/utils.h"

using namespace doris;

static const uint32_t MAX_PATH_LEN = 1024;

class RowIdConversionSpillTest : public testing::Test {
public:
    void SetUp() override {
        char buffer[MAX_PATH_LEN];
        EXPECT_NE(getcwd(buffer, MAX_PATH_LEN), nullptr);
        absolute_dir = std::string(buffer) + "/rowid_conversion_spill_test";
        auto st = io::global_local_filesystem()->delete_directory(absolute_dir);
        ASSERT_TRUE(st.ok() || st.is<ErrorCode::NOT_FOUND>()) << st;
        st = io::global_local_filesystem()->create_directory(absolute_dir);
        ASSERT_TRUE(st.ok()) << st;
        origin_max_bytes = config::rowid_conversion_max_bytes;
    }

    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(absolute_dir).ok());
        config::rowid_conversion_max_bytes = origin_max_bytes;
    }

    std::string get_tablet_path() { return absolute_dir; }

    RowsetId create_rowset_id(int64_t id) {
        RowsetId rowset_id;
        rowset_id.init(id);
        return rowset_id;
    }

    void add_test_data(RowIdConversion* conversion, const RowsetId& rowset_id,
                       uint32_t segment_count, uint32_t rows_per_segment,
                       uint32_t dst_segments_count) {
        std::vector<uint32_t> segment_rows(segment_count, rows_per_segment);
        ASSERT_TRUE(conversion->init_segment_map(rowset_id, segment_rows).ok());

        // Create test row locations
        std::vector<RowLocation> rss_row_ids;
        for (uint32_t seg = 0; seg < segment_count; ++seg) {
            for (uint32_t row = 0; row < rows_per_segment; ++row) {
                RowLocation loc(rowset_id, seg, row);
                rss_row_ids.push_back(loc);
            }
        }

        std::vector<uint32_t> dst_segments_num_row(
                dst_segments_count, (segment_count * rows_per_segment) / dst_segments_count);
        ASSERT_TRUE(conversion->add(rss_row_ids, dst_segments_num_row).ok());
    }

private:
    std::string absolute_dir;
    int64_t origin_max_bytes;
};

TEST_F(RowIdConversionSpillTest, BasicSpillEnableDisable) {
    // Test spill disabled
    {
        RowIdConversion conversion;
        ASSERT_TRUE(conversion.init().ok());

        auto* rowid_storage = conversion._storage.get();
        ASSERT_TRUE(dynamic_cast<detail::RowIdMemoryStorage*>(rowid_storage) != nullptr);

        RowsetId rowset_id = create_rowset_id(1);
        conversion.set_dst_rowset_id(create_rowset_id(2));

        add_test_data(&conversion, rowset_id, 2, 100, 1);

        RowLocation src(rowset_id, 0, 0);
        RowLocation dst;
        ASSERT_EQ(conversion.get(src, &dst), 0);
        ASSERT_EQ(dst.segment_id, 0);
        ASSERT_EQ(dst.row_id, 0);
    }

    int64_t tablet_id = 101;
    std::string spill_path = fmt::format("{}/rowid_conversion_{}", get_tablet_path(), tablet_id);

    // Test spill enabled and memory limit is reached
    {
        RowIdConversion conversion;
        ASSERT_TRUE(conversion.init(true, 1, tablet_id, get_tablet_path()).ok());

        bool exists {false};
        ASSERT_TRUE(io::global_local_filesystem()->exists(spill_path, &exists));
        ASSERT_TRUE(exists);

        RowsetId rowset_id = create_rowset_id(1);
        conversion.set_dst_rowset_id(create_rowset_id(2));

        uint32_t segment_count = 2;
        add_test_data(&conversion, rowset_id, segment_count, 100, 1);

        auto* rowid_storage = conversion._storage.get();
        auto* storage = dynamic_cast<detail::RowIdSpillableStorage*>(rowid_storage);
        ASSERT_TRUE(storage != nullptr);
        // segments' mapping should be spilled
        for (int i = 0; i < segment_count; i++) {
            ASSERT_TRUE(storage->_segment_to_id_map.contains({rowset_id, i}));
            int64_t internal_id = storage->_segment_to_id_map[{rowset_id, i}];
            ASSERT_TRUE(storage->_segments[internal_id].is_spilled);
        }

        for (int i = 0; i < segment_count; i++) {
            for (int j = 0; j < segment_count; j++) {
                RowLocation src(rowset_id, i, j);
                RowLocation dst;
                ASSERT_EQ(conversion.get(src, &dst), 0);
                ASSERT_EQ(dst.segment_id, 0);
                int64_t expected_row_id = i * 100 + j;
                ASSERT_EQ(dst.row_id, expected_row_id);
            }
            int64_t internal_id = storage->_segment_to_id_map[{rowset_id, i}];
            ASSERT_FALSE(storage->_segments[internal_id].is_spilled);
        }
    }

    // Test spill enabled and memory limit is not reached
    {
        RowIdConversion conversion;
        ASSERT_TRUE(conversion.init(true, 10000000, tablet_id, get_tablet_path()).ok());

        bool exists {false};
        ASSERT_TRUE(io::global_local_filesystem()->exists(spill_path, &exists));
        ASSERT_TRUE(exists);

        RowsetId rowset_id = create_rowset_id(1);
        conversion.set_dst_rowset_id(create_rowset_id(2));
        uint32_t segment_count = 2;
        add_test_data(&conversion, rowset_id, segment_count, 100, 1);

        auto* rowid_storage = conversion._storage.get();
        auto* storage = dynamic_cast<detail::RowIdSpillableStorage*>(rowid_storage);
        ASSERT_TRUE(storage != nullptr);
        // segments' mapping should not be spilled
        for (int i = 0; i < segment_count; i++) {
            ASSERT_TRUE(storage->_segment_to_id_map.contains({rowset_id, i}));
            int64_t internal_id = storage->_segment_to_id_map[{rowset_id, i}];
            ASSERT_FALSE(storage->_segments[internal_id].is_spilled);
        }

        for (int i = 0; i < segment_count; i++) {
            for (int j = 0; j < segment_count; j++) {
                RowLocation src(rowset_id, i, j);
                RowLocation dst;
                ASSERT_EQ(conversion.get(src, &dst), 0);
                ASSERT_EQ(dst.segment_id, 0);
                int64_t expected_row_id = i * 100 + j;
                ASSERT_EQ(dst.row_id, expected_row_id);
            }
        }
    }
}
