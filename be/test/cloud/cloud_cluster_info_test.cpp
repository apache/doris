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

#include "cloud/cloud_cluster_info.h"

#include <gtest/gtest.h>

#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "cloud/config.h"
#include "gen_cpp/cloud.pb.h"
#include "olap/tablet_meta.h"
#include "util/time.h"
#include "util/uid_util.h"

namespace doris {

class CloudClusterInfoTest : public testing::Test {
public:
    CloudClusterInfoTest() : _engine(CloudStorageEngine(EngineOptions {})) {}

    void SetUp() override {
        _cluster_info = std::make_unique<CloudClusterInfo>();

        _tablet_meta.reset(new TabletMeta(1, 2, 15673, 15674, 4, 5, TTabletSchema(), 6, {{7, 8}},
                                          UniqueId(9, 10), TTabletType::TABLET_TYPE_DISK,
                                          TCompressionType::LZ4F));
        _tablet =
                std::make_shared<CloudTablet>(_engine, std::make_shared<TabletMeta>(*_tablet_meta));
    }

    void TearDown() override {
        _cluster_info.reset();
        _tablet.reset();
    }

protected:
    CloudStorageEngine _engine;
    std::unique_ptr<CloudClusterInfo> _cluster_info;
    std::shared_ptr<TabletMeta> _tablet_meta;
    std::shared_ptr<CloudTablet> _tablet;
};

// Test my_cluster_id get/set
TEST_F(CloudClusterInfoTest, MyClusterId) {
    EXPECT_EQ(_cluster_info->my_cluster_id(), "");

    _cluster_info->set_my_cluster_id("cluster_a");
    EXPECT_EQ(_cluster_info->my_cluster_id(), "cluster_a");

    _cluster_info->set_my_cluster_id("cluster_b");
    EXPECT_EQ(_cluster_info->my_cluster_id(), "cluster_b");
}

// Test cluster status cache get/set/clear
TEST_F(CloudClusterInfoTest, ClusterStatusCache) {
    ClusterStatusCache cache;

    // Not found initially
    EXPECT_FALSE(_cluster_info->get_cluster_status("cluster_a", &cache));

    // Set and get
    _cluster_info->set_cluster_status("cluster_a", cloud::ClusterStatus::NORMAL, 1000);
    EXPECT_TRUE(_cluster_info->get_cluster_status("cluster_a", &cache));
    EXPECT_EQ(cache.status, cloud::ClusterStatus::NORMAL);
    EXPECT_EQ(cache.mtime_ms, 1000);

    // Set another cluster
    _cluster_info->set_cluster_status("cluster_b", cloud::ClusterStatus::SUSPENDED, 2000);
    EXPECT_TRUE(_cluster_info->get_cluster_status("cluster_b", &cache));
    EXPECT_EQ(cache.status, cloud::ClusterStatus::SUSPENDED);
    EXPECT_EQ(cache.mtime_ms, 2000);

    // Clear
    _cluster_info->clear_cluster_status_cache();
    EXPECT_FALSE(_cluster_info->get_cluster_status("cluster_a", &cache));
    EXPECT_FALSE(_cluster_info->get_cluster_status("cluster_b", &cache));
}

// Test is_in_standby
TEST_F(CloudClusterInfoTest, IsInStandby) {
    EXPECT_FALSE(_cluster_info->is_in_standby());
    _cluster_info->set_is_in_standby(true);
    EXPECT_TRUE(_cluster_info->is_in_standby());
    _cluster_info->set_is_in_standby(false);
    EXPECT_FALSE(_cluster_info->is_in_standby());
}

// Case 1: Feature disabled, should never skip
TEST_F(CloudClusterInfoTest, ShouldSkipCompactionDisabled) {
    config::enable_compaction_rw_separation = false;

    _cluster_info->set_my_cluster_id("cluster_a");
    _tablet->set_last_active_cluster_info("cluster_b", UnixMillis());

    EXPECT_FALSE(_cluster_info->should_skip_compaction(_tablet.get()));
}

// Case 2: No last_active_cluster, allow compaction
TEST_F(CloudClusterInfoTest, ShouldSkipCompactionNoActiveCluster) {
    config::enable_compaction_rw_separation = true;

    _cluster_info->set_my_cluster_id("cluster_a");
    // tablet has no last_active_cluster set (empty)

    EXPECT_FALSE(_cluster_info->should_skip_compaction(_tablet.get()));
}

// Case 3: This is the active cluster, allow compaction
TEST_F(CloudClusterInfoTest, ShouldSkipCompactionSameCluster) {
    config::enable_compaction_rw_separation = true;

    _cluster_info->set_my_cluster_id("cluster_a");
    _tablet->set_last_active_cluster_info("cluster_a", UnixMillis());

    EXPECT_FALSE(_cluster_info->should_skip_compaction(_tablet.get()));
}

// Case 4: Active cluster not in cache (deleted), allow takeover
TEST_F(CloudClusterInfoTest, ShouldSkipCompactionClusterDeleted) {
    config::enable_compaction_rw_separation = true;

    _cluster_info->set_my_cluster_id("cluster_a");
    _tablet->set_last_active_cluster_info("cluster_deleted", UnixMillis());
    // cluster_deleted is not in cache

    EXPECT_FALSE(_cluster_info->should_skip_compaction(_tablet.get()));
}

// Case 5: Active cluster is NORMAL, skip compaction
TEST_F(CloudClusterInfoTest, ShouldSkipCompactionActiveClusterNormal) {
    config::enable_compaction_rw_separation = true;

    _cluster_info->set_my_cluster_id("cluster_a");
    _tablet->set_last_active_cluster_info("cluster_b", UnixMillis());
    _cluster_info->set_cluster_status("cluster_b", cloud::ClusterStatus::NORMAL, UnixMillis());

    EXPECT_TRUE(_cluster_info->should_skip_compaction(_tablet.get()));
}

// Case 6: Active cluster SUSPENDED, timeout not reached, skip
TEST_F(CloudClusterInfoTest, ShouldSkipCompactionSuspendedNotTimedOut) {
    config::enable_compaction_rw_separation = true;
    config::compaction_cluster_takeover_timeout_ms = 60000; // 60s

    _cluster_info->set_my_cluster_id("cluster_a");
    _tablet->set_last_active_cluster_info("cluster_b", UnixMillis());
    // Set suspended just now — timeout not reached
    _cluster_info->set_cluster_status("cluster_b", cloud::ClusterStatus::SUSPENDED, UnixMillis());

    EXPECT_TRUE(_cluster_info->should_skip_compaction(_tablet.get()));
}

// Case 7: Active cluster SUSPENDED, timeout exceeded, allow takeover
TEST_F(CloudClusterInfoTest, ShouldSkipCompactionSuspendedTimedOut) {
    config::enable_compaction_rw_separation = true;
    config::compaction_cluster_takeover_timeout_ms = 1000; // 1s

    _cluster_info->set_my_cluster_id("cluster_a");
    _tablet->set_last_active_cluster_info("cluster_b", UnixMillis());
    // Set suspended long ago
    _cluster_info->set_cluster_status("cluster_b", cloud::ClusterStatus::SUSPENDED,
                                      UnixMillis() - 5000);

    EXPECT_FALSE(_cluster_info->should_skip_compaction(_tablet.get()));
}

// Case 8: Active cluster MANUAL_SHUTDOWN, timeout exceeded, allow takeover
TEST_F(CloudClusterInfoTest, ShouldSkipCompactionManualShutdownTimedOut) {
    config::enable_compaction_rw_separation = true;
    config::compaction_cluster_takeover_timeout_ms = 1000;

    _cluster_info->set_my_cluster_id("cluster_a");
    _tablet->set_last_active_cluster_info("cluster_b", UnixMillis());
    _cluster_info->set_cluster_status("cluster_b", cloud::ClusterStatus::MANUAL_SHUTDOWN,
                                      UnixMillis() - 5000);

    EXPECT_FALSE(_cluster_info->should_skip_compaction(_tablet.get()));
}

// Test CloudTablet last_active_cluster_info accessors
TEST_F(CloudClusterInfoTest, TabletLastActiveClusterInfo) {
    EXPECT_EQ(_tablet->last_active_cluster_id(), "");
    EXPECT_EQ(_tablet->last_active_time_ms(), 0);

    int64_t now = UnixMillis();
    _tablet->set_last_active_cluster_info("cluster_x", now);
    EXPECT_EQ(_tablet->last_active_cluster_id(), "cluster_x");
    EXPECT_EQ(_tablet->last_active_time_ms(), now);

    _tablet->set_last_active_cluster_info("cluster_y", now + 1000);
    EXPECT_EQ(_tablet->last_active_cluster_id(), "cluster_y");
    EXPECT_EQ(_tablet->last_active_time_ms(), now + 1000);
}

// Test start/stop bg worker (just lifecycle, no actual RPC)
TEST_F(CloudClusterInfoTest, BgWorkerLifecycle) {
    // Stop on a never-started worker should be safe
    _cluster_info->stop_bg_worker();

    // Double stop should be safe
    _cluster_info->stop_bg_worker();
}

} // namespace doris
