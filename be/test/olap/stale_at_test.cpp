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
#include <ctime>

#include "olap/rowset/rowset_meta.h"
#include "olap/version_graph.h"

namespace doris {

class StaleAtTest : public testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(StaleAtTest, TestRowsetMetaStaleAt) {
    // Create a RowsetMeta and test stale_at functionality
    RowsetMeta rowset_meta;

    int64_t creation_time = 1000000;
    int64_t stale_at_time = 2000000;

    // Set creation time
    rowset_meta.set_creation_time(creation_time);

    // Initially, stale_at should return creation_time since stale_at is not set
    EXPECT_EQ(rowset_meta.stale_at(), creation_time);

    // Set stale_at time
    rowset_meta.set_stale_at(stale_at_time);

    // Now stale_at should return stale_at time
    EXPECT_EQ(rowset_meta.stale_at(), stale_at_time);
}

TEST_F(StaleAtTest, TestTimestampedVersionWithStaleTime) {
    // Test that TimestampedVersion works correctly with stale_time
    RowsetMetaSharedPtr rowset_meta = std::make_shared<RowsetMeta>();

    int64_t creation_time = 1000000;
    int64_t stale_at_time = 2000000;

    rowset_meta->set_creation_time(creation_time);
    rowset_meta->set_stale_at(stale_at_time);

    // Create a TimestampedVersion using stale_at
    Version version(1, 5);
    TimestampedVersionSharedPtr tv_ptr(
        new TimestampedVersion(version, rowset_meta->stale_at()));

    EXPECT_EQ(tv_ptr->get_create_time(), stale_at_time);
    EXPECT_EQ(tv_ptr->version(), version);
}

TEST_F(StaleAtTest, TestStalePathVersionWithStaleAt) {
    // Test that add_stale_path_version uses stale_at correctly
    TimestampedVersionTracker tracker;

    std::vector<RowsetMetaSharedPtr> stale_rs_metas;

    // Create rowset metas with different creation and stale times
    for (int i = 0; i < 3; ++i) {
        RowsetMetaSharedPtr rs_meta = std::make_shared<RowsetMeta>();
        rs_meta->set_creation_time(1000000 + i * 1000);  // Old creation time
        rs_meta->set_stale_at(2000000);  // Recent stale time (same for all)
        rs_meta->set_version(Version(i * 2 + 1, i * 2 + 2));
        stale_rs_metas.push_back(rs_meta);
    }

    // Add stale path version
    tracker.add_stale_path_version(stale_rs_metas);

    // Check that expired paths are captured correctly using stale_at time
    std::vector<int64_t> expired_paths;

    // With endtime before stale_at, no paths should be expired
    tracker.capture_expired_paths(1999999, &expired_paths);
    EXPECT_EQ(expired_paths.size(), 0);

    // With endtime after stale_at, paths should be expired
    tracker.capture_expired_paths(2000001, &expired_paths);
    EXPECT_EQ(expired_paths.size(), 1);
}

} // namespace doris
