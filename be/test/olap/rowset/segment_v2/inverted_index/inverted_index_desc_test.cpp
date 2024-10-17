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

#include "olap/rowset/segment_v2/inverted_index_desc.h"

#include <gtest/gtest.h>

namespace doris::segment_v2 {

class InvertedIndexDescTest : public testing::Test {
public:
    InvertedIndexDescTest() = default;
    ~InvertedIndexDescTest() override = default;
};

TEST_F(InvertedIndexDescTest, test_decompose_local_index_file_name) {
    std::string local_file_name_v1 = "rowsetid_0_indexid@suffix.idx";
    std::string local_file_name_v2 = "rowsetid_0.idx";

    const auto& local_fragment_v1 =
            InvertedIndexDescriptor::decompose_local_index_file_name(local_file_name_v1);
    EXPECT_EQ(local_fragment_v1.rowset_id, "rowsetid");
    EXPECT_EQ(local_fragment_v1.seg_id, 0);
    EXPECT_EQ(local_fragment_v1.index_suffix, "_indexid@suffix.idx");

    const auto& local_fragment_v2 =
            InvertedIndexDescriptor::decompose_local_index_file_name(local_file_name_v2);
    EXPECT_EQ(local_fragment_v2.rowset_id, "rowsetid");
    EXPECT_EQ(local_fragment_v2.seg_id, 0);
    EXPECT_EQ(local_fragment_v2.index_suffix, ".idx");

    const auto& unvalid_name =
            InvertedIndexDescriptor::decompose_local_index_file_name("local_file_name_v2.idx");
    EXPECT_EQ(unvalid_name.rowset_id, "");
    EXPECT_EQ(unvalid_name.seg_id, -1);
    EXPECT_EQ(unvalid_name.index_suffix, "");
}

TEST_F(InvertedIndexDescTest, test_snapshot_index_file_name) {
    std::string local_file_name_v1 = "rowsetid_0_indexid@suffix.idx";
    std::string local_file_name_v2 = "rowsetid_0.idx";

    std::string remote_v0_file_name_v1 = "rowsetid_1_indexid@suffix.idx";
    std::string remote_v0_file_name_v2 = "rowsetid_1.idx";

    std::string remote_v1_file_name_v1 = "2_indexid@suffix.idx";
    std::string remote_v1_file_name_v2 = "2.idx";

    const auto& snapshot_name_1 =
            InvertedIndexDescriptor::snapshot_index_file_name(local_file_name_v1);
    EXPECT_EQ(snapshot_name_1, "rowsetid_0_indexid@suffix.binlog-index");

    const auto& snapshot_name_2 =
            InvertedIndexDescriptor::snapshot_index_file_name(local_file_name_v2);
    EXPECT_EQ(snapshot_name_2, "rowsetid_0.binlog-index");

    const auto& snapshot_name_3 =
            InvertedIndexDescriptor::snapshot_index_file_name(remote_v0_file_name_v1);
    EXPECT_EQ(snapshot_name_3, "rowsetid_1_indexid@suffix.binlog-index");

    const auto& snapshot_name_4 =
            InvertedIndexDescriptor::snapshot_index_file_name(remote_v0_file_name_v2);
    EXPECT_EQ(snapshot_name_4, "rowsetid_1.binlog-index");

    const auto& snapshot_name_5 =
            InvertedIndexDescriptor::snapshot_index_file_name(remote_v1_file_name_v1);
    EXPECT_EQ(snapshot_name_5, "2_indexid@suffix.binlog-index");

    const auto& snapshot_name_6 =
            InvertedIndexDescriptor::snapshot_index_file_name(remote_v1_file_name_v2);
    EXPECT_EQ(snapshot_name_6, "2.binlog-index");

    std::string file_name = "123456.idx";
    const auto& snapshot_name_7 = InvertedIndexDescriptor::snapshot_index_file_name(file_name);
    EXPECT_EQ(snapshot_name_7, "123456.binlog-index");
}

} // namespace doris::segment_v2
