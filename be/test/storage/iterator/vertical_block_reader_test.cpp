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

#include "storage/iterator/vertical_block_reader.h"

#include <gtest/gtest.h>

#include "storage/rowset/rowset_meta.h"

namespace doris {

class VerticalBlockReaderTestAccessor {
public:
    static std::vector<size_t> segment_group_sizes(const RowsetMeta& rowset_meta,
                                                   std::pair<int64_t, int64_t> segment_offsets,
                                                   size_t num_segment_iters) {
        return VerticalBlockReader::_segment_group_sizes(rowset_meta, segment_offsets,
                                                         num_segment_iters);
    }
};

TEST(VerticalBlockReaderTest, SegmentGroupSizesForWholeRowset) {
    RowsetMeta rowset_meta;
    rowset_meta.set_num_segments(5);
    rowset_meta.set_segment_group_sizes({2, 2, 1});

    EXPECT_EQ(VerticalBlockReaderTestAccessor::segment_group_sizes(rowset_meta, {0, 0}, 5),
              std::vector<size_t>({2, 2, 1}));
}

TEST(VerticalBlockReaderTest, SegmentGroupSizesForSegmentRange) {
    RowsetMeta rowset_meta;
    rowset_meta.set_num_segments(8);
    rowset_meta.set_segment_group_sizes({3, 2, 3});

    EXPECT_EQ(VerticalBlockReaderTestAccessor::segment_group_sizes(rowset_meta, {0, 3}, 3),
              std::vector<size_t>({3}));
    EXPECT_EQ(VerticalBlockReaderTestAccessor::segment_group_sizes(rowset_meta, {1, 3}, 2),
              std::vector<size_t>({2}));
    EXPECT_EQ(VerticalBlockReaderTestAccessor::segment_group_sizes(rowset_meta, {1, 2}, 1),
              std::vector<size_t>({1}));
    EXPECT_EQ(VerticalBlockReaderTestAccessor::segment_group_sizes(rowset_meta, {1, 4}, 3),
              std::vector<size_t>({2, 1}));
    EXPECT_EQ(VerticalBlockReaderTestAccessor::segment_group_sizes(rowset_meta, {3, 5}, 2),
              std::vector<size_t>({2}));
    EXPECT_EQ(VerticalBlockReaderTestAccessor::segment_group_sizes(rowset_meta, {4, 7}, 3),
              std::vector<size_t>({1, 2}));
    EXPECT_EQ(VerticalBlockReaderTestAccessor::segment_group_sizes(rowset_meta, {5, 8}, 3),
              std::vector<size_t>({3}));
    EXPECT_EQ(VerticalBlockReaderTestAccessor::segment_group_sizes(rowset_meta, {7, 8}, 1),
              std::vector<size_t>({1}));
}

TEST(VerticalBlockReaderTest, SegmentGroupSizesRejectInvalidInput) {
    RowsetMeta rowset_meta;
    rowset_meta.set_num_segments(5);
    rowset_meta.set_segment_group_sizes({2, 2, 1});

    RowsetMetaPB invalid_group_layout_pb;
    invalid_group_layout_pb.set_rowset_id(1);
    invalid_group_layout_pb.set_num_segments(5);
    invalid_group_layout_pb.add_segment_group_sizes(2);
    invalid_group_layout_pb.add_segment_group_sizes(2);
    RowsetMeta invalid_group_layout;
    ASSERT_TRUE(invalid_group_layout.init_from_pb(invalid_group_layout_pb));

#ifndef NDEBUG
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH(
            {
                static_cast<void>(VerticalBlockReaderTestAccessor::segment_group_sizes(rowset_meta,
                                                                                       {1, 4}, 2));
            },
            "Check failed");
    EXPECT_DEATH(
            {
                static_cast<void>(VerticalBlockReaderTestAccessor::segment_group_sizes(rowset_meta,
                                                                                       {0, 6}, 6));
            },
            "Check failed");
    EXPECT_DEATH(
            {
                static_cast<void>(VerticalBlockReaderTestAccessor::segment_group_sizes(
                        invalid_group_layout, {0, 0}, 5));
            },
            "Check failed");
#else
    EXPECT_ANY_THROW(static_cast<void>(
            VerticalBlockReaderTestAccessor::segment_group_sizes(rowset_meta, {1, 4}, 2)));
    EXPECT_ANY_THROW(static_cast<void>(
            VerticalBlockReaderTestAccessor::segment_group_sizes(rowset_meta, {0, 6}, 6)));
    EXPECT_ANY_THROW(static_cast<void>(
            VerticalBlockReaderTestAccessor::segment_group_sizes(invalid_group_layout, {0, 0}, 5)));
#endif
}

} // namespace doris
