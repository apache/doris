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

#include "io/cache/range_writeback_dispatcher.h"

#include <gtest/gtest.h>

#include <string>
#include <utility>
#include <vector>

namespace doris::io {
namespace {

struct OwnedFragment {
    size_t block_offset {0};
    size_t block_valid_size {0};
    size_t fragment_offset {0};
    std::string data;

    bool operator==(const OwnedFragment&) const = default;
};

OwnedFragment copy_fragment(const FileCacheBlockFragment& fragment) {
    return {.block_offset = fragment.block_offset,
            .block_valid_size = fragment.block_valid_size,
            .fragment_offset = fragment.fragment_offset,
            .data = std::string(fragment.data.data, fragment.data.size)};
}

TEST(RangeWritebackDispatcherTest, SplitsMixedRangeAtBlockBoundaries) {
    std::vector<OwnedFragment> complete;
    std::vector<OwnedFragment> partial;
    RangeWritebackDispatcher dispatcher(
            4096, 1024,
            [&](const FileCacheBlockFragment& fragment) {
                complete.push_back(copy_fragment(fragment));
                return true;
            },
            [&](const FileCacheBlockFragment& fragment) {
                partial.push_back(copy_fragment(fragment));
                return false;
            });
    const std::string data(2048, 'r');

    const auto result = dispatcher.dispatch({.offset = 256, .size = data.size()}, Slice(data));

    EXPECT_EQ(complete, (std::vector<OwnedFragment> {{.block_offset = 1024,
                                                      .block_valid_size = 1024,
                                                      .fragment_offset = 0,
                                                      .data = std::string(1024, 'r')}}));
    EXPECT_EQ(partial, (std::vector<OwnedFragment> {{.block_offset = 0,
                                                     .block_valid_size = 1024,
                                                     .fragment_offset = 256,
                                                     .data = std::string(768, 'r')},
                                                    {.block_offset = 2048,
                                                     .block_valid_size = 1024,
                                                     .fragment_offset = 0,
                                                     .data = std::string(256, 'r')}}));
    EXPECT_EQ(result.complete_block_count, 1);
    EXPECT_EQ(result.partial_fragment_count, 2);
    EXPECT_EQ(result.complete_block_bytes, 1024);
    EXPECT_EQ(result.partial_fragment_bytes, 1024);
    EXPECT_EQ(result.submitted_complete_block_count, 1);
    EXPECT_EQ(result.submitted_partial_fragment_count, 0);
}

TEST(RangeWritebackDispatcherTest, TreatsPhysicalEofBlockAsComplete) {
    std::vector<OwnedFragment> complete;
    std::vector<OwnedFragment> partial;
    RangeWritebackDispatcher dispatcher(
            2500, 1024,
            [&](const FileCacheBlockFragment& fragment) {
                complete.push_back(copy_fragment(fragment));
                return true;
            },
            [&](const FileCacheBlockFragment& fragment) {
                partial.push_back(copy_fragment(fragment));
                return true;
            });
    const std::string data(452, 'e');

    const auto result = dispatcher.dispatch({.offset = 2048, .size = data.size()}, Slice(data));

    EXPECT_EQ(complete, (std::vector<OwnedFragment> {{.block_offset = 2048,
                                                      .block_valid_size = 452,
                                                      .fragment_offset = 0,
                                                      .data = data}}));
    EXPECT_TRUE(partial.empty());
    EXPECT_EQ(result.complete_block_count, 1);
    EXPECT_EQ(result.complete_block_bytes, 452);
    EXPECT_EQ(result.submitted_complete_block_count, 1);
}

TEST(RangeWritebackDispatcherTest, PreservesSourceOffsetsAcrossCompleteBlocks) {
    std::vector<OwnedFragment> complete;
    RangeWritebackDispatcher dispatcher(
            3072, 1024,
            [&](const FileCacheBlockFragment& fragment) {
                complete.push_back(copy_fragment(fragment));
                return true;
            },
            [](const FileCacheBlockFragment&) { return false; });
    const std::string data =
            std::string(1024, 'a') + std::string(1024, 'b') + std::string(1024, 'c');

    const auto result = dispatcher.dispatch({.offset = 0, .size = data.size()}, Slice(data));

    ASSERT_EQ(complete.size(), 3);
    EXPECT_EQ(complete[0].data, std::string(1024, 'a'));
    EXPECT_EQ(complete[1].data, std::string(1024, 'b'));
    EXPECT_EQ(complete[2].data, std::string(1024, 'c'));
    EXPECT_EQ(result.complete_block_count, 3);
    EXPECT_EQ(result.complete_block_bytes, data.size());
}

} // namespace
} // namespace doris::io
