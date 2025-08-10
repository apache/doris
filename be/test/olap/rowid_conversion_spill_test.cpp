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

#include <filesystem>
#include <memory>
#include <vector>
#include <thread>
#include <future>

#include "common/config.h"
#include "common/status.h"
#include "io/fs/local_file_system.h"
#include "olap/rowid_conversion.h"
#include "olap/rowid_conversion_storage.h"
#include "olap/rowid_spill_manager.h"
#include "olap/olap_common.h"
#include "runtime/memory/global_memory_arbitrator.h"
#include "runtime/thread_context.h"

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

        // Backup original config values
        original_enable_spill = config::enable_rowid_conversion_spill;
        original_max_mb = config::rowid_conversion_max_mb;
    }

    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(absolute_dir).ok());
        
        // Restore original config values
        config::enable_rowid_conversion_spill = original_enable_spill;
        config::rowid_conversion_max_mb = original_max_mb;
    }

    std::string get_tablet_path() {
        return absolute_dir;
    }

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

        std::vector<uint32_t> dst_segments_num_row(dst_segments_count, 
                                                  (segment_count * rows_per_segment) / dst_segments_count);
        ASSERT_TRUE(conversion->add(rss_row_ids, dst_segments_num_row).ok());
    }

private:
    std::string absolute_dir;
    bool original_enable_spill;
    int64_t original_max_mb;
};

TEST_F(RowIdConversionSpillTest, BasicSpillEnableDisable) {
    // Test spill disabled
    {
        config::enable_rowid_conversion_spill = false;
        RowIdConversion conversion(false, 12345, get_tablet_path());
        ASSERT_TRUE(conversion.init().ok());
        
        RowsetId rowset_id = create_rowset_id(1);
        conversion.set_dst_rowset_id(create_rowset_id(2));
        
        add_test_data(&conversion, rowset_id, 2, 100, 1);
        
        // Should not spill when disabled
        RowLocation src(rowset_id, 0, 0);
        RowLocation dst;
        ASSERT_EQ(conversion.get(src, &dst), 0);
        ASSERT_EQ(dst.segment_id, 0);
        ASSERT_EQ(dst.row_id, 0);
    }

    // Test spill enabled
    {
        config::enable_rowid_conversion_spill = true;
        RowIdConversion conversion(true, 12345, get_tablet_path());
        ASSERT_TRUE(conversion.init().ok());
        
        RowsetId rowset_id = create_rowset_id(1);
        conversion.set_dst_rowset_id(create_rowset_id(2));
        
        add_test_data(&conversion, rowset_id, 2, 100, 1);
        
        RowLocation src(rowset_id, 0, 0);
        RowLocation dst;
        ASSERT_EQ(conversion.get(src, &dst), 0);
        ASSERT_EQ(dst.segment_id, 0);
        ASSERT_EQ(dst.row_id, 0);
    }
}

TEST_F(RowIdConversionSpillTest, MemoryThresholdTrigger) {
    config::enable_rowid_conversion_spill = true;
    config::rowid_conversion_max_mb = 1; // 1MB threshold
    
    RowIdConversion conversion(true, 12345, get_tablet_path());
    ASSERT_TRUE(conversion.init().ok());
    conversion.set_dst_rowset_id(create_rowset_id(100));
    
    // Add enough data to trigger spill (approximately 1MB+)
    // Each mapping entry is ~24 bytes (key + value in unordered_map)
    // So ~45000 entries should exceed 1MB
    RowsetId rowset_id = create_rowset_id(1);
    uint32_t rows_per_segment = 15000;
    uint32_t segment_count = 3;
    
    add_test_data(&conversion, rowset_id, segment_count, rows_per_segment, 1);
    
    // Verify data can still be retrieved after potential spilling
    for (uint32_t seg = 0; seg < segment_count; ++seg) {
        for (uint32_t row = 0; row < rows_per_segment; row += 1000) { // Sample every 1000th row
            RowLocation src(rowset_id, seg, row);
            RowLocation dst;
            ASSERT_EQ(conversion.get(src, &dst), 0) << "Failed to get mapping for segment " << seg << " row " << row;
            
            // Calculate expected destination
            uint32_t global_row_id = seg * rows_per_segment + row;
            ASSERT_EQ(dst.segment_id, 0); // All goes to segment 0 based on our dst_segments_num_row
            ASSERT_EQ(dst.row_id, global_row_id);
        }
    }
}

TEST_F(RowIdConversionSpillTest, LargeDataSpill) {
    config::enable_rowid_conversion_spill = true;
    config::rowid_conversion_max_mb = 2; // 2MB threshold
    
    RowIdConversion conversion(true, 12345, get_tablet_path());
    ASSERT_TRUE(conversion.init().ok());
    conversion.set_dst_rowset_id(create_rowset_id(100));
    
    // Create multiple rowsets with large data
    std::vector<RowsetId> rowset_ids;
    for (int rs = 0; rs < 3; ++rs) {
        RowsetId rowset_id = create_rowset_id(rs + 1);
        rowset_ids.push_back(rowset_id);
        
        // Each rowset has significant data to force spilling
        add_test_data(&conversion, rowset_id, 4, 8000, 2);
    }
    
    // Verify all data is accessible
    for (int rs = 0; rs < 3; ++rs) {
        const RowsetId& rowset_id = rowset_ids[rs];
        for (uint32_t seg = 0; seg < 4; ++seg) {
            for (uint32_t row = 0; row < 8000; row += 799) { // Sample rows
                RowLocation src(rowset_id, seg, row);
                RowLocation dst;
                int result = conversion.get(src, &dst);
                ASSERT_EQ(result, 0) << "Failed for rowset " << rs << " segment " << seg << " row " << row;
            }
        }
    }
}

TEST_F(RowIdConversionSpillTest, MultipleSegmentSpill) {
    config::enable_rowid_conversion_spill = true;
    config::rowid_conversion_max_mb = 1;
    
    RowIdConversion conversion(true, 12345, get_tablet_path());
    ASSERT_TRUE(conversion.init().ok());
    conversion.set_dst_rowset_id(create_rowset_id(100));
    
    RowsetId rowset_id = create_rowset_id(1);
    
    // Create many segments, each should potentially trigger individual spill
    uint32_t segment_count = 10;
    uint32_t rows_per_segment = 5000;
    
    std::vector<uint32_t> segment_rows(segment_count, rows_per_segment);
    ASSERT_TRUE(conversion.init_segment_map(rowset_id, segment_rows).ok());
    
    // Add data segment by segment to test incremental spilling
    for (uint32_t seg = 0; seg < segment_count; ++seg) {
        std::vector<RowLocation> segment_rows;
        for (uint32_t row = 0; row < rows_per_segment; ++row) {
            segment_rows.emplace_back(rowset_id, seg, row);
        }
        
        std::vector<uint32_t> dst_segments_num_row(1, segment_count * rows_per_segment);
        ASSERT_TRUE(conversion.add(segment_rows, dst_segments_num_row).ok());
    }
    
    // Verify random access patterns work
    for (uint32_t seg = 0; seg < segment_count; seg += 2) {
        for (uint32_t row = 0; row < rows_per_segment; row += 500) {
            RowLocation src(rowset_id, seg, row);
            RowLocation dst;
            ASSERT_EQ(conversion.get(src, &dst), 0);
        }
    }
}

TEST_F(RowIdConversionSpillTest, ReadAfterSpill) {
    config::enable_rowid_conversion_spill = true;
    config::rowid_conversion_max_mb = 1;
    
    RowIdConversion conversion(true, 12345, get_tablet_path());
    ASSERT_TRUE(conversion.init().ok());
    
    RowsetId src_rowset = create_rowset_id(1);
    RowsetId dst_rowset = create_rowset_id(2);
    conversion.set_dst_rowset_id(dst_rowset);
    
    // Add large amount of data to force spill
    uint32_t segment_count = 3;
    uint32_t rows_per_segment = 20000;
    
    add_test_data(&conversion, src_rowset, segment_count, rows_per_segment, 2);
    
    // Test multiple reads of the same data
    for (int iteration = 0; iteration < 3; ++iteration) {
        for (uint32_t seg = 0; seg < segment_count; ++seg) {
            for (uint32_t row = 0; row < rows_per_segment; row += 1000) {
                RowLocation src(src_rowset, seg, row);
                RowLocation dst;
                ASSERT_EQ(conversion.get(src, &dst), 0) << "Iteration " << iteration << " failed";
                ASSERT_EQ(dst.rowset_id, dst_rowset);
                
                // Verify destination mapping is consistent
                uint32_t global_row = seg * rows_per_segment + row;
                uint32_t expected_dst_seg = global_row >= (segment_count * rows_per_segment / 2) ? 1 : 0;
                uint32_t expected_dst_row = global_row % (segment_count * rows_per_segment / 2);
                ASSERT_EQ(dst.segment_id, expected_dst_seg);
                ASSERT_EQ(dst.row_id, expected_dst_row);
            }
        }
    }
}

TEST_F(RowIdConversionSpillTest, PruneAfterSpill) {
    config::enable_rowid_conversion_spill = true;
    config::rowid_conversion_max_mb = 1;
    
    RowIdConversion conversion(true, 12345, get_tablet_path());
    ASSERT_TRUE(conversion.init().ok());
    conversion.set_dst_rowset_id(create_rowset_id(100));
    
    RowsetId rowset_id = create_rowset_id(1);
    
    // Add data that will spill
    add_test_data(&conversion, rowset_id, 3, 15000, 1);
    
    // Verify data exists before pruning
    RowLocation src(rowset_id, 1, 100);
    RowLocation dst;
    ASSERT_EQ(conversion.get(src, &dst), 0);
    
    // Prune segment 1
    conversion.prune_segment_mapping(rowset_id, 1);
    
    // Verify segment 1 data is gone
    ASSERT_NE(conversion.get(src, &dst), 0);
    
    // Verify other segments still work
    RowLocation src_seg0(rowset_id, 0, 100);
    RowLocation src_seg2(rowset_id, 2, 100);
    ASSERT_EQ(conversion.get(src_seg0, &dst), 0);
    ASSERT_EQ(conversion.get(src_seg2, &dst), 0);
}

TEST_F(RowIdConversionSpillTest, InvalidAccess) {
    config::enable_rowid_conversion_spill = true;
    config::rowid_conversion_max_mb = 1;
    
    RowIdConversion conversion(true, 12345, get_tablet_path());
    ASSERT_TRUE(conversion.init().ok());
    conversion.set_dst_rowset_id(create_rowset_id(100));
    
    RowsetId rowset_id = create_rowset_id(1);
    add_test_data(&conversion, rowset_id, 2, 1000, 1);
    
    RowLocation dst;
    
    // Invalid rowset
    RowLocation invalid_rowset(create_rowset_id(999), 0, 0);
    ASSERT_NE(conversion.get(invalid_rowset, &dst), 0);
    
    // Invalid segment
    RowLocation invalid_segment(rowset_id, 999, 0);
    ASSERT_NE(conversion.get(invalid_segment, &dst), 0);
    
    // Invalid row (out of bounds)
    RowLocation invalid_row(rowset_id, 0, 9999);
    ASSERT_NE(conversion.get(invalid_row, &dst), 0);
}

TEST_F(RowIdConversionSpillTest, SpillManagerDirectTest) {
    std::string spill_file = get_tablet_path() + "/test_direct_spill.tmp";
    
    RowIdSpillManager manager(spill_file);
    ASSERT_TRUE(manager.init().ok());
    
    // Initialize segments
    ASSERT_TRUE(manager.init_new_segment(0, 1000).ok());
    ASSERT_TRUE(manager.init_new_segment(1, 2000).ok());
    
    // Create mappings
    TrackableResource resource;
    RowIdMappingType mappings(&resource);
    for (uint32_t i = 0; i < 1000; ++i) {
        mappings[i] = std::make_pair(i % 10, i + 1000);
    }
    
    // Spill segment 0
    ASSERT_TRUE(manager.spill_segment_mapping(0, mappings).ok());
    
    // Read back
    RowIdMappingType read_mappings(&resource);
    ASSERT_TRUE(manager.read_segment_mapping(0, &read_mappings).ok());
    
    ASSERT_EQ(read_mappings.size(), mappings.size());
    for (const auto& [key, value] : mappings) {
        ASSERT_EQ(read_mappings[key], value);
    }
}

TEST_F(RowIdConversionSpillTest, ConcurrentAccess) {
    config::enable_rowid_conversion_spill = true;
    config::rowid_conversion_max_mb = 2;
    
    RowIdConversion conversion(true, 12345, get_tablet_path());
    ASSERT_TRUE(conversion.init().ok());
    conversion.set_dst_rowset_id(create_rowset_id(100));
    
    RowsetId rowset_id = create_rowset_id(1);
    
    // Add substantial data
    add_test_data(&conversion, rowset_id, 5, 10000, 3);
    
    // Launch multiple threads to read concurrently
    const int num_threads = 4;
    std::vector<std::future<bool>> futures;
    
    for (int t = 0; t < num_threads; ++t) {
        futures.push_back(std::async(std::launch::async, [&conversion, rowset_id, t]() -> bool {
            for (uint32_t seg = 0; seg < 5; ++seg) {
                for (uint32_t row = t * 1000; row < (t + 1) * 1000 && row < 10000; row += 100) {
                    RowLocation src(rowset_id, seg, row);
                    RowLocation dst;
                    if (conversion.get(src, &dst) != 0) {
                        return false;
                    }
                }
            }
            return true;
        }));
    }
    
    // Wait for all threads
    for (auto& future : futures) {
        ASSERT_TRUE(future.get()) << "Concurrent access test failed";
    }
}

TEST_F(RowIdConversionSpillTest, MemoryUsageTracking) {
    config::enable_rowid_conversion_spill = true;
    config::rowid_conversion_max_mb = 1;
    
    RowIdConversion conversion(true, 12345, get_tablet_path());
    ASSERT_TRUE(conversion.init().ok());
    conversion.set_dst_rowset_id(create_rowset_id(100));
    
    RowsetId rowset_id = create_rowset_id(1);
    
    // Add data incrementally and check that spilling occurs
    uint32_t segment_count = 1;
    uint32_t rows_per_segment = 50000; // Large enough to trigger spill
    
    std::vector<uint32_t> segment_rows(segment_count, rows_per_segment);
    ASSERT_TRUE(conversion.init_segment_map(rowset_id, segment_rows).ok());
    
    // Add data in batches
    const uint32_t batch_size = 10000;
    for (uint32_t start = 0; start < rows_per_segment; start += batch_size) {
        uint32_t end = std::min(start + batch_size, rows_per_segment);
        
        std::vector<RowLocation> batch_rows;
        for (uint32_t row = start; row < end; ++row) {
            batch_rows.emplace_back(rowset_id, 0, row);
        }
        
        std::vector<uint32_t> dst_segments_num_row(1, rows_per_segment);
        ASSERT_TRUE(conversion.add(batch_rows, dst_segments_num_row).ok());
    }
    
    // Verify all data is still accessible
    for (uint32_t row = 0; row < rows_per_segment; row += 5000) {
        RowLocation src(rowset_id, 0, row);
        RowLocation dst;
        ASSERT_EQ(conversion.get(src, &dst), 0) << "Row " << row << " not found after spill";
        ASSERT_EQ(dst.row_id, row);
    }
}

TEST_F(RowIdConversionSpillTest, EmptyDataHandling) {
    config::enable_rowid_conversion_spill = true;
    config::rowid_conversion_max_mb = 1;
    
    RowIdConversion conversion(true, 12345, get_tablet_path());
    ASSERT_TRUE(conversion.init().ok());
    conversion.set_dst_rowset_id(create_rowset_id(100));
    
    RowsetId rowset_id = create_rowset_id(1);
    
    // Initialize with empty segments
    std::vector<uint32_t> segment_rows = {0, 100, 0};
    ASSERT_TRUE(conversion.init_segment_map(rowset_id, segment_rows).ok());
    
    // Add data only to middle segment
    std::vector<RowLocation> row_locs;
    for (uint32_t row = 0; row < 100; ++row) {
        row_locs.emplace_back(rowset_id, 1, row);
    }
    
    std::vector<uint32_t> dst_segments_num_row(1, 100);
    ASSERT_TRUE(conversion.add(row_locs, dst_segments_num_row).ok());
    
    // Verify empty segments return not found
    RowLocation dst;
    RowLocation src_empty0(rowset_id, 0, 0);
    RowLocation src_empty2(rowset_id, 2, 0);
    ASSERT_NE(conversion.get(src_empty0, &dst), 0);
    ASSERT_NE(conversion.get(src_empty2, &dst), 0);
    
    // Verify middle segment works
    RowLocation src_valid(rowset_id, 1, 50);
    ASSERT_EQ(conversion.get(src_valid, &dst), 0);
    ASSERT_EQ(dst.row_id, 50);
}

TEST_F(RowIdConversionSpillTest, VeryLargeDataset) {
    config::enable_rowid_conversion_spill = true;
    config::rowid_conversion_max_mb = 5;
    
    RowIdConversion conversion(true, 12345, get_tablet_path());
    ASSERT_TRUE(conversion.init().ok());
    conversion.set_dst_rowset_id(create_rowset_id(100));
    
    // Create a very large dataset
    RowsetId rowset_id = create_rowset_id(1);
    uint32_t segment_count = 10;
    uint32_t rows_per_segment = 50000; // 500K total rows
    
    add_test_data(&conversion, rowset_id, segment_count, rows_per_segment, 5);
    
    // Sample verification - check scattered rows across all segments
    std::vector<uint32_t> test_rows = {0, 1000, 25000, 49999};
    for (uint32_t seg = 0; seg < segment_count; ++seg) {
        for (uint32_t row : test_rows) {
            if (row < rows_per_segment) {
                RowLocation src(rowset_id, seg, row);
                RowLocation dst;
                ASSERT_EQ(conversion.get(src, &dst), 0) 
                    << "Failed to get segment " << seg << " row " << row;
            }
        }
    }
}

TEST_F(RowIdConversionSpillTest, ConfigurationChanges) {
    // Test behavior with different threshold configurations
    struct TestConfig {
        int64_t threshold_mb;
        uint32_t rows_per_segment;
        bool should_spill;
    };
    
    std::vector<TestConfig> configs = {
        {100, 1000, false},   // High threshold, small data - no spill
        {1, 50000, true},     // Low threshold, large data - should spill
        {10, 20000, false},   // Medium threshold, medium data - might not spill
    };
    
    for (const auto& cfg : configs) {
        config::enable_rowid_conversion_spill = true;
        config::rowid_conversion_max_mb = cfg.threshold_mb;
        
        RowIdConversion conversion(true, 12345, get_tablet_path());
        ASSERT_TRUE(conversion.init().ok());
        conversion.set_dst_rowset_id(create_rowset_id(100));
        
        RowsetId rowset_id = create_rowset_id(1);
        add_test_data(&conversion, rowset_id, 2, cfg.rows_per_segment, 1);
        
        // Verify data accessibility regardless of spill behavior
        for (uint32_t seg = 0; seg < 2; ++seg) {
            for (uint32_t row = 0; row < cfg.rows_per_segment; row += cfg.rows_per_segment / 10) {
                RowLocation src(rowset_id, seg, row);
                RowLocation dst;
                ASSERT_EQ(conversion.get(src, &dst), 0) 
                    << "Config test failed: threshold=" << cfg.threshold_mb 
                    << "MB, rows=" << cfg.rows_per_segment;
            }
        }
    }
}

TEST_F(RowIdConversionSpillTest, SpillFileCleanup) {
    config::enable_rowid_conversion_spill = true;
    config::rowid_conversion_max_mb = 1;
    
    std::string spill_file_path;
    
    {
        RowIdConversion conversion(true, 12345, get_tablet_path());
        ASSERT_TRUE(conversion.init().ok());
        conversion.set_dst_rowset_id(create_rowset_id(100));
        
        RowsetId rowset_id = create_rowset_id(1);
        add_test_data(&conversion, rowset_id, 3, 20000, 1);
        
        // Force some data access to potentially create spill files
        for (uint32_t seg = 0; seg < 3; ++seg) {
            for (uint32_t row = 0; row < 20000; row += 10000) {
                RowLocation src(rowset_id, seg, row);
                RowLocation dst;
                conversion.get(src, &dst);
            }
        }
    } // RowIdConversion destructor should clean up spill files
    
    // Note: In a real implementation, you might want to verify that temporary spill files
    // are properly cleaned up, but this would require access to internal file paths
}

TEST_F(RowIdConversionSpillTest, EdgeCaseRowIds) {
    config::enable_rowid_conversion_spill = true;
    config::rowid_conversion_max_mb = 1;
    
    RowIdConversion conversion(true, 12345, get_tablet_path());
    ASSERT_TRUE(conversion.init().ok());
    conversion.set_dst_rowset_id(create_rowset_id(100));
    
    RowsetId rowset_id = create_rowset_id(1);
    
    // Test with some invalid row IDs in the input
    std::vector<uint32_t> segment_rows = {10000};
    ASSERT_TRUE(conversion.init_segment_map(rowset_id, segment_rows).ok());
    
    std::vector<RowLocation> mixed_rows;
    for (uint32_t row = 0; row < 10000; ++row) {
        if (row % 100 == 0) {
            // Add some invalid row locations
            mixed_rows.emplace_back(rowset_id, 0, -1); // This should be skipped
        } else {
            mixed_rows.emplace_back(rowset_id, 0, row);
        }
    }
    
    std::vector<uint32_t> dst_segments_num_row(1, 10000);
    ASSERT_TRUE(conversion.add(mixed_rows, dst_segments_num_row).ok());
    
    // Verify valid rows are accessible
    for (uint32_t row = 1; row < 10000; row += 1000) {
        if (row % 100 != 0) {
            RowLocation src(rowset_id, 0, row);
            RowLocation dst;
            ASSERT_EQ(conversion.get(src, &dst), 0) << "Row " << row << " should be accessible";
        }
    }
}