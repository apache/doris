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

// Use #define private public to access private/protected members for testing
#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wkeyword-macro"
#endif
#define private public
#define protected public
#include "storage/iterator/block_reader.h"
#undef private
#undef protected
#if defined(__clang__)
#pragma clang diagnostic pop
#endif

#include <gtest/gtest.h>

#include "common/config.h"
#include "core/column/column_vector.h"

namespace doris {

namespace {

constexpr size_t kMinPublicBlockBudgetBytes = 1048576; // 1MB

MutableColumns make_int64_columns(size_t rows) {
    MutableColumns columns;
    auto col = ColumnInt64::create();
    for (size_t i = 0; i < rows; ++i) {
        col->insert_value(cast_set<int64_t>(i));
    }
    columns.push_back(std::move(col));
    return columns;
}

} // namespace

class BlockReaderBatchMaxRowsTest : public testing::Test {
protected:
    void SetUp() override { _saved_enable_adaptive = config::enable_adaptive_batch_size; }

    void TearDown() override { config::enable_adaptive_batch_size = _saved_enable_adaptive; }

    bool _saved_enable_adaptive = false;
};

TEST_F(BlockReaderBatchMaxRowsTest, FallbackToBatchSizeWhenAdaptiveDisabled) {
    config::enable_adaptive_batch_size = false;

    BlockReader reader;
    reader._reader_context.batch_size = 4096;

    EXPECT_EQ(reader.batch_max_rows(), 4096);
}

TEST_F(BlockReaderBatchMaxRowsTest, UseBatchSizeWhenAdaptiveEnabled) {
    config::enable_adaptive_batch_size = true;

    BlockReader reader;
    reader._reader_context.batch_size = 4096;
    reader._reader_context.preferred_block_size_bytes = 8388608; // byte budget must be active

    EXPECT_EQ(reader.batch_max_rows(), 4096);
}

TEST_F(BlockReaderBatchMaxRowsTest, BatchMaxRowsIgnoresByteBudget) {
    config::enable_adaptive_batch_size = true;

    BlockReader reader;
    reader._reader_context.batch_size = 1024;
    reader._reader_context.preferred_block_size_bytes = 8388608;

    EXPECT_EQ(reader.batch_max_rows(), 1024);
}

TEST_F(BlockReaderBatchMaxRowsTest, PreferredBlockSizeBytesWhenEnabled) {
    config::enable_adaptive_batch_size = true;

    BlockReader reader;
    reader._reader_context.preferred_block_size_bytes = 8388608; // 8MB

    EXPECT_EQ(reader.preferred_block_size_bytes(), 8388608);
}

TEST_F(BlockReaderBatchMaxRowsTest, PreferredBlockSizeBytesWhenDisabled) {
    config::enable_adaptive_batch_size = false;

    BlockReader reader;
    reader._reader_context.preferred_block_size_bytes = 8388608;

    EXPECT_EQ(reader.preferred_block_size_bytes(), 0);
}

TEST_F(BlockReaderBatchMaxRowsTest, ReachedByteBudgetReturnsFalseWhenDisabled) {
    config::enable_adaptive_batch_size = false;

    BlockReader reader;
    reader._reader_context.preferred_block_size_bytes = kMinPublicBlockBudgetBytes;

    auto columns = make_int64_columns(200000); // ~1.6MB > 1MB min public budget

    EXPECT_FALSE(reader._reached_byte_budget(columns));
}

TEST_F(BlockReaderBatchMaxRowsTest, ReachedByteBudgetReturnsTrueWhenExceeded) {
    config::enable_adaptive_batch_size = true;

    BlockReader reader;
    reader._reader_context.preferred_block_size_bytes = kMinPublicBlockBudgetBytes;

    auto columns = make_int64_columns(200000); // ~1.6MB >= 1MB min public budget

    EXPECT_TRUE(reader._reached_byte_budget(columns));
}

TEST_F(BlockReaderBatchMaxRowsTest, ReachedByteBudgetReturnsFalseWhenUnderBudget) {
    config::enable_adaptive_batch_size = true;

    BlockReader reader;
    reader._reader_context.preferred_block_size_bytes = kMinPublicBlockBudgetBytes;

    auto columns = make_int64_columns(10); // 80 bytes < 1MB min public budget

    EXPECT_FALSE(reader._reached_byte_budget(columns));
}

} // namespace doris
