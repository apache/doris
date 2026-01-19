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

#include <memory>
#include <vector>

#include "io/cache/block_file_cache.h"

namespace doris::io {
namespace {

FileBlockSPtr create_block(int idx) {
    FileCacheKey key;
    key.hash = UInt128Wrapper(vectorized::UInt128(static_cast<uint64_t>(idx + 1)));
    key.offset = static_cast<size_t>(idx * 16);
    key.meta.expiration_time = 0;
    key.meta.type = FileCacheType::NORMAL;
    key.meta.tablet_id = idx;
    return std::make_shared<FileBlock>(key, /*size*/ 1, /*mgr*/ nullptr, FileBlock::State::EMPTY);
}

void insert_blocks(NeedUpdateLRUBlocks* pending, int count, int start_idx = 0) {
    for (int i = 0; i < count; ++i) {
        ASSERT_TRUE(pending->insert(create_block(start_idx + i)))
                << "Block " << (start_idx + i) << " should be inserted";
    }
}

} // namespace

TEST(NeedUpdateLRUBlocksTest, InsertRejectsNullAndDeduplicates) {
    NeedUpdateLRUBlocks pending;
    FileBlockSPtr null_block;
    EXPECT_FALSE(pending.insert(null_block));
    EXPECT_EQ(0, pending.size());

    auto block = create_block(0);
    EXPECT_TRUE(pending.insert(block));
    EXPECT_EQ(1, pending.size());

    EXPECT_FALSE(pending.insert(block)) << "Same pointer should not enqueue twice";
    EXPECT_EQ(1, pending.size());
}

TEST(NeedUpdateLRUBlocksTest, DrainHandlesZeroLimitAndNullOutput) {
    NeedUpdateLRUBlocks pending;
    insert_blocks(&pending, 3);
    std::vector<FileBlockSPtr> drained;

    EXPECT_EQ(0, pending.drain(0, &drained));
    EXPECT_TRUE(drained.empty());
    EXPECT_EQ(3, pending.size());

    EXPECT_EQ(0, pending.drain(2, nullptr));
    EXPECT_EQ(3, pending.size());
}

TEST(NeedUpdateLRUBlocksTest, DrainRespectsLimitAndLeavesRemainder) {
    NeedUpdateLRUBlocks pending;
    insert_blocks(&pending, 5);
    std::vector<FileBlockSPtr> drained;

    size_t drained_now = pending.drain(2, &drained);
    EXPECT_EQ(2u, drained_now);
    EXPECT_EQ(2u, drained.size());
    EXPECT_EQ(3u, pending.size());

    drained_now = pending.drain(10, &drained);
    EXPECT_EQ(3u, drained_now);
    EXPECT_EQ(5u, drained.size());
    EXPECT_EQ(0u, pending.size());
}

TEST(NeedUpdateLRUBlocksTest, DrainFromEmptyReturnsZero) {
    NeedUpdateLRUBlocks pending;
    std::vector<FileBlockSPtr> drained;
    EXPECT_EQ(0u, pending.drain(4, &drained));
    EXPECT_TRUE(drained.empty());
}

TEST(NeedUpdateLRUBlocksTest, ClearIsIdempotent) {
    NeedUpdateLRUBlocks pending;
    pending.clear();
    EXPECT_EQ(0u, pending.size());

    insert_blocks(&pending, 4);
    EXPECT_EQ(4u, pending.size());

    pending.clear();
    EXPECT_EQ(0u, pending.size());

    std::vector<FileBlockSPtr> drained;
    EXPECT_EQ(0u, pending.drain(4, &drained));
}

} // namespace doris::io
