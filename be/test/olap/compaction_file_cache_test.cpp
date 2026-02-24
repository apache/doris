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

#include "cloud/config.h"
#include "io/fs/file_writer.h"
#include "olap/olap_common.h"
#include "olap/rowset/rowset_writer_context.h"

namespace doris {

// Extern declaration for the free function in compaction.cpp
extern bool should_enable_compaction_cache_index_only(bool write_file_cache,
                                                      ReaderType compaction_type,
                                                      bool enable_base_index_only,
                                                      bool enable_cumu_index_only);

class CompactionFileCacheTest : public testing::Test {
public:
    void SetUp() override {
        // Save original configuration
        _orig_base_config = config::enable_file_cache_write_base_compaction_index_only;
        _orig_cumu_config = config::enable_file_cache_write_cumu_compaction_index_only;
    }

    void TearDown() override {
        // Restore original configuration
        config::enable_file_cache_write_base_compaction_index_only = _orig_base_config;
        config::enable_file_cache_write_cumu_compaction_index_only = _orig_cumu_config;
    }

private:
    bool _orig_base_config;
    bool _orig_cumu_config;
};

// ============================================================================
// Base Compaction Tests
// ============================================================================

TEST_F(CompactionFileCacheTest, BaseCompaction_IndexOnly_False_IndexFile) {
    // Setup: Disable index-only mode for base compaction
    config::enable_file_cache_write_base_compaction_index_only = false;

    // Create context
    RowsetWriterContext ctx;
    ctx.write_file_cache = true;
    ctx.compaction_output_write_index_only = false;

    // Test: Get file writer options for index file
    auto opts = ctx.get_file_writer_options(true);

    // Verify: write_file_cache should be true
    EXPECT_TRUE(opts.write_file_cache);
}

TEST_F(CompactionFileCacheTest, BaseCompaction_IndexOnly_False_DataFile) {
    // Setup: Disable index-only mode for base compaction
    config::enable_file_cache_write_base_compaction_index_only = false;

    // Create context
    RowsetWriterContext ctx;
    ctx.write_file_cache = true;
    ctx.compaction_output_write_index_only = false;

    // Test: Get file writer options for data file
    auto opts = ctx.get_file_writer_options(false);

    // Verify: write_file_cache should be true
    EXPECT_TRUE(opts.write_file_cache);
}

TEST_F(CompactionFileCacheTest, BaseCompaction_IndexOnly_True_IndexFile) {
    // Setup: Enable index-only mode for base compaction
    config::enable_file_cache_write_base_compaction_index_only = true;

    // Create context
    RowsetWriterContext ctx;
    ctx.write_file_cache = true;
    ctx.compaction_output_write_index_only = true;

    // Test: Get file writer options for index file
    auto opts = ctx.get_file_writer_options(true);

    // Verify: write_file_cache should be true (index files are always cached)
    EXPECT_TRUE(opts.write_file_cache);
}

TEST_F(CompactionFileCacheTest, BaseCompaction_IndexOnly_True_DataFile) {
    // Setup: Enable index-only mode for base compaction
    config::enable_file_cache_write_base_compaction_index_only = true;

    // Create context
    RowsetWriterContext ctx;
    ctx.write_file_cache = true;
    ctx.compaction_output_write_index_only = true;

    // Test: Get file writer options for data file
    auto opts = ctx.get_file_writer_options(false);

    // Verify: write_file_cache should be false (data files are NOT cached when index-only is enabled)
    EXPECT_FALSE(opts.write_file_cache);
}

// ============================================================================
// Cumulative Compaction Tests
// ============================================================================

TEST_F(CompactionFileCacheTest, CumuCompaction_IndexOnly_False_IndexFile) {
    // Setup: Disable index-only mode for cumulative compaction
    config::enable_file_cache_write_cumu_compaction_index_only = false;

    // Create context
    RowsetWriterContext ctx;
    ctx.write_file_cache = true;
    ctx.compaction_output_write_index_only = false;

    // Test: Get file writer options for index file
    auto opts = ctx.get_file_writer_options(true);

    // Verify: write_file_cache should be true
    EXPECT_TRUE(opts.write_file_cache);
}

TEST_F(CompactionFileCacheTest, CumuCompaction_IndexOnly_False_DataFile) {
    // Setup: Disable index-only mode for cumulative compaction
    config::enable_file_cache_write_cumu_compaction_index_only = false;

    // Create context
    RowsetWriterContext ctx;
    ctx.write_file_cache = true;
    ctx.compaction_output_write_index_only = false;

    // Test: Get file writer options for data file
    auto opts = ctx.get_file_writer_options(false);

    // Verify: write_file_cache should be true
    EXPECT_TRUE(opts.write_file_cache);
}

TEST_F(CompactionFileCacheTest, CumuCompaction_IndexOnly_True_IndexFile) {
    // Setup: Enable index-only mode for cumulative compaction
    config::enable_file_cache_write_cumu_compaction_index_only = true;

    // Create context
    RowsetWriterContext ctx;
    ctx.write_file_cache = true;
    ctx.compaction_output_write_index_only = true;

    // Test: Get file writer options for index file
    auto opts = ctx.get_file_writer_options(true);

    // Verify: write_file_cache should be true (index files are always cached)
    EXPECT_TRUE(opts.write_file_cache);
}

TEST_F(CompactionFileCacheTest, CumuCompaction_IndexOnly_True_DataFile) {
    // Setup: Enable index-only mode for cumulative compaction
    config::enable_file_cache_write_cumu_compaction_index_only = true;

    // Create context
    RowsetWriterContext ctx;
    ctx.write_file_cache = true;
    ctx.compaction_output_write_index_only = true;

    // Test: Get file writer options for data file
    auto opts = ctx.get_file_writer_options(false);

    // Verify: write_file_cache should be false (data files are NOT cached when index-only is enabled)
    EXPECT_FALSE(opts.write_file_cache);
}

// ============================================================================
// Base Compaction Tests with write_file_cache = false
// ============================================================================

TEST_F(CompactionFileCacheTest, BaseCompaction_WriteCacheFalse_IndexOnly_False_IndexFile) {
    // Setup: Disable index-only mode for base compaction
    config::enable_file_cache_write_base_compaction_index_only = false;

    // Create context with write_file_cache = false
    RowsetWriterContext ctx;
    ctx.write_file_cache = false;
    ctx.compaction_output_write_index_only = false;

    // Test: Get file writer options for index file
    auto opts = ctx.get_file_writer_options(true);

    // Verify: write_file_cache should remain false
    EXPECT_FALSE(opts.write_file_cache);
}

TEST_F(CompactionFileCacheTest, BaseCompaction_WriteCacheFalse_IndexOnly_False_DataFile) {
    // Setup: Disable index-only mode for base compaction
    config::enable_file_cache_write_base_compaction_index_only = false;

    // Create context with write_file_cache = false
    RowsetWriterContext ctx;
    ctx.write_file_cache = false;
    ctx.compaction_output_write_index_only = false;

    // Test: Get file writer options for data file
    auto opts = ctx.get_file_writer_options(false);

    // Verify: write_file_cache should remain false
    EXPECT_FALSE(opts.write_file_cache);
}

TEST_F(CompactionFileCacheTest, BaseCompaction_WriteCacheFalse_IndexOnly_True_IndexFile) {
    // Setup: Enable index-only mode for base compaction
    config::enable_file_cache_write_base_compaction_index_only = true;

    // Create context with write_file_cache = false
    RowsetWriterContext ctx;
    ctx.write_file_cache = false;
    ctx.compaction_output_write_index_only = true;

    // Test: Get file writer options for index file
    auto opts = ctx.get_file_writer_options(true);

    // Verify: write_file_cache should remain false (base cache setting takes precedence)
    EXPECT_FALSE(opts.write_file_cache);
}

TEST_F(CompactionFileCacheTest, BaseCompaction_WriteCacheFalse_IndexOnly_True_DataFile) {
    // Setup: Enable index-only mode for base compaction
    config::enable_file_cache_write_base_compaction_index_only = true;

    // Create context with write_file_cache = false
    RowsetWriterContext ctx;
    ctx.write_file_cache = false;
    ctx.compaction_output_write_index_only = true;

    // Test: Get file writer options for data file
    auto opts = ctx.get_file_writer_options(false);

    // Verify: write_file_cache should remain false
    EXPECT_FALSE(opts.write_file_cache);
}

// ============================================================================
// Cumulative Compaction Tests with write_file_cache = false
// ============================================================================

TEST_F(CompactionFileCacheTest, CumuCompaction_WriteCacheFalse_IndexOnly_False_IndexFile) {
    // Setup: Disable index-only mode for cumulative compaction
    config::enable_file_cache_write_cumu_compaction_index_only = false;

    // Create context with write_file_cache = false
    RowsetWriterContext ctx;
    ctx.write_file_cache = false;
    ctx.compaction_output_write_index_only = false;

    // Test: Get file writer options for index file
    auto opts = ctx.get_file_writer_options(true);

    // Verify: write_file_cache should remain false
    EXPECT_FALSE(opts.write_file_cache);
}

TEST_F(CompactionFileCacheTest, CumuCompaction_WriteCacheFalse_IndexOnly_False_DataFile) {
    // Setup: Disable index-only mode for cumulative compaction
    config::enable_file_cache_write_cumu_compaction_index_only = false;

    // Create context with write_file_cache = false
    RowsetWriterContext ctx;
    ctx.write_file_cache = false;
    ctx.compaction_output_write_index_only = false;

    // Test: Get file writer options for data file
    auto opts = ctx.get_file_writer_options(false);

    // Verify: write_file_cache should remain false
    EXPECT_FALSE(opts.write_file_cache);
}

TEST_F(CompactionFileCacheTest, CumuCompaction_WriteCacheFalse_IndexOnly_True_IndexFile) {
    // Setup: Enable index-only mode for cumulative compaction
    config::enable_file_cache_write_cumu_compaction_index_only = true;

    // Create context with write_file_cache = false
    RowsetWriterContext ctx;
    ctx.write_file_cache = false;
    ctx.compaction_output_write_index_only = true;

    // Test: Get file writer options for index file
    auto opts = ctx.get_file_writer_options(true);

    // Verify: write_file_cache should remain false (base cache setting takes precedence)
    EXPECT_FALSE(opts.write_file_cache);
}

TEST_F(CompactionFileCacheTest, CumuCompaction_WriteCacheFalse_IndexOnly_True_DataFile) {
    // Setup: Enable index-only mode for cumulative compaction
    config::enable_file_cache_write_cumu_compaction_index_only = true;

    // Create context with write_file_cache = false
    RowsetWriterContext ctx;
    ctx.write_file_cache = false;
    ctx.compaction_output_write_index_only = true;

    // Test: Get file writer options for data file
    auto opts = ctx.get_file_writer_options(false);

    // Verify: write_file_cache should remain false
    EXPECT_FALSE(opts.write_file_cache);
}

// ============================================================================
// Tests for should_enable_compaction_cache_index_only function
// ============================================================================

TEST_F(CompactionFileCacheTest, FreeFunction_WriteCacheFalse_AlwaysReturnsFalse) {
    // When write_file_cache is false, the function should always return false
    // regardless of other parameters
    EXPECT_FALSE(should_enable_compaction_cache_index_only(
            false, ReaderType::READER_BASE_COMPACTION, false, false));
    EXPECT_FALSE(should_enable_compaction_cache_index_only(
            false, ReaderType::READER_BASE_COMPACTION, true, false));
    EXPECT_FALSE(should_enable_compaction_cache_index_only(
            false, ReaderType::READER_CUMULATIVE_COMPACTION, false, false));
    EXPECT_FALSE(should_enable_compaction_cache_index_only(
            false, ReaderType::READER_CUMULATIVE_COMPACTION, false, true));
    EXPECT_FALSE(should_enable_compaction_cache_index_only(
            false, ReaderType::READER_BASE_COMPACTION, true, true));
    EXPECT_FALSE(should_enable_compaction_cache_index_only(
            false, ReaderType::READER_CUMULATIVE_COMPACTION, true, true));
}

TEST_F(CompactionFileCacheTest, FreeFunction_BaseCompaction_ConfigDisabled) {
    // Base compaction with config disabled should return false
    bool result = should_enable_compaction_cache_index_only(
            true, ReaderType::READER_BASE_COMPACTION, false, false);
    EXPECT_FALSE(result);
}

TEST_F(CompactionFileCacheTest, FreeFunction_BaseCompaction_ConfigEnabled) {
    // Base compaction with config enabled should return true
    bool result = should_enable_compaction_cache_index_only(
            true, ReaderType::READER_BASE_COMPACTION, true, false);
    EXPECT_TRUE(result);
}

TEST_F(CompactionFileCacheTest, FreeFunction_CumuCompaction_ConfigDisabled) {
    // Cumulative compaction with config disabled should return false
    bool result = should_enable_compaction_cache_index_only(
            true, ReaderType::READER_CUMULATIVE_COMPACTION, false, false);
    EXPECT_FALSE(result);
}

TEST_F(CompactionFileCacheTest, FreeFunction_CumuCompaction_ConfigEnabled) {
    // Cumulative compaction with config enabled should return true
    bool result = should_enable_compaction_cache_index_only(
            true, ReaderType::READER_CUMULATIVE_COMPACTION, false, true);
    EXPECT_TRUE(result);
}

TEST_F(CompactionFileCacheTest, FreeFunction_BaseCompaction_WrongConfigEnabled) {
    // Base compaction with cumulative config enabled (but base disabled) should return false
    bool result = should_enable_compaction_cache_index_only(
            true, ReaderType::READER_BASE_COMPACTION, false, true);
    EXPECT_FALSE(result);
}

TEST_F(CompactionFileCacheTest, FreeFunction_CumuCompaction_WrongConfigEnabled) {
    // Cumulative compaction with base config enabled (but cumu disabled) should return false
    bool result = should_enable_compaction_cache_index_only(
            true, ReaderType::READER_CUMULATIVE_COMPACTION, true, false);
    EXPECT_FALSE(result);
}

TEST_F(CompactionFileCacheTest, FreeFunction_BothConfigsEnabled) {
    // When both configs are enabled, each compaction type should return true
    EXPECT_TRUE(should_enable_compaction_cache_index_only(true, ReaderType::READER_BASE_COMPACTION,
                                                          true, true));
    EXPECT_TRUE(should_enable_compaction_cache_index_only(
            true, ReaderType::READER_CUMULATIVE_COMPACTION, true, true));
}

TEST_F(CompactionFileCacheTest, FreeFunction_OtherCompactionTypes) {
    // Other compaction types should return false
    EXPECT_FALSE(
            should_enable_compaction_cache_index_only(true, ReaderType::READER_QUERY, true, true));
    EXPECT_FALSE(should_enable_compaction_cache_index_only(true, ReaderType::READER_ALTER_TABLE,
                                                           true, true));
}

} // namespace doris
