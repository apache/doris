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

#include "cloud/cloud_compaction_util.h"
#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "cloud/config.h"
#include "gen_cpp/cloud.pb.h"
#include "olap/tablet_meta.h"
#include "util/time.h"

namespace doris {

class CloudCompactionRWSeparationTest : public testing::Test {
public:
    CloudCompactionRWSeparationTest() : _engine(CloudStorageEngine(EngineOptions {})) {}

    void SetUp() override {
        _tablet_meta = std::make_shared<TabletMeta>(
                1, 2, 15673, 15674, 4, 5, TTabletSchema(), 6, {{7, 8}}, UniqueId(9, 10),
                TTabletType::TABLET_TYPE_DISK, TCompressionType::LZ4F);
        _tablet = std::make_shared<CloudTablet>(_engine, _tablet_meta);

        // Save original config values
        _original_enable = config::enable_compaction_rw_separation;
        _original_timeout = config::compaction_cluster_takeover_timeout_ms;

        // Enable the feature for testing
        config::enable_compaction_rw_separation = true;
        config::compaction_cluster_takeover_timeout_ms = 1800000; // 30 min
    }

    void TearDown() override {
        // Restore original config values
        config::enable_compaction_rw_separation = _original_enable;
        config::compaction_cluster_takeover_timeout_ms = _original_timeout;
    }

    void set_cluster_info(const std::string& my_cluster, const std::string& last_active,
                          int32_t status, int64_t status_mtime) {
        _tablet->set_my_cluster_id(my_cluster);
        _tablet->set_last_active_cluster_info(last_active, UnixMillis(), status, status_mtime);
    }

protected:
    CloudStorageEngine _engine;
    TabletMetaSharedPtr _tablet_meta;
    std::shared_ptr<CloudTablet> _tablet;
    bool _original_enable;
    int64_t _original_timeout;
};

// Test case 1: last_active_cluster_id is empty, should return true
TEST_F(CloudCompactionRWSeparationTest, NoActiveClusterRecord) {
    set_cluster_info("cluster-read", "", cloud::ClusterStatus::UNKNOWN, 0);

    EXPECT_TRUE(should_do_compaction_for_cluster(_tablet.get()));
}

// Test case 2: last_active_cluster_id is self, should return true
TEST_F(CloudCompactionRWSeparationTest, SelfIsActiveCluster) {
    set_cluster_info("cluster-write", "cluster-write", cloud::ClusterStatus::NORMAL, 0);

    EXPECT_TRUE(should_do_compaction_for_cluster(_tablet.get()));
}

// Test case 3: Not self, original cluster status is NORMAL, should return false
TEST_F(CloudCompactionRWSeparationTest, OtherClusterIsNormal) {
    set_cluster_info("cluster-read", "cluster-write", cloud::ClusterStatus::NORMAL, 0);

    EXPECT_FALSE(should_do_compaction_for_cluster(_tablet.get()));
}

// Test case 4: Not self, original cluster status is SUSPENDED but not timed out, should return false
TEST_F(CloudCompactionRWSeparationTest, OtherClusterSuspendedNotTimedOut) {
    int64_t now = UnixMillis();
    // status_mtime is recent, not timed out
    set_cluster_info("cluster-read", "cluster-write", cloud::ClusterStatus::SUSPENDED,
                     now - 60000); // 1 minute ago

    EXPECT_FALSE(should_do_compaction_for_cluster(_tablet.get()));
}

// Test case 5: Not self, original cluster status is SUSPENDED and timed out, should return true
TEST_F(CloudCompactionRWSeparationTest, OtherClusterSuspendedTimedOut) {
    int64_t now = UnixMillis();
    // status_mtime is old, timed out
    set_cluster_info("cluster-read", "cluster-write", cloud::ClusterStatus::SUSPENDED,
                     now - 3600000); // 1 hour ago (> 30 min timeout)

    EXPECT_TRUE(should_do_compaction_for_cluster(_tablet.get()));
}

// Test case 6: Not self, original cluster status is UNKNOWN (deleted) but not timed out, should return false
TEST_F(CloudCompactionRWSeparationTest, OtherClusterDeletedNotTimedOut) {
    int64_t now = UnixMillis();
    // status_mtime is recent, not timed out
    set_cluster_info("cluster-read", "cluster-write", cloud::ClusterStatus::UNKNOWN,
                     now - 60000); // 1 minute ago

    EXPECT_FALSE(should_do_compaction_for_cluster(_tablet.get()));
}

// Test case 7: Not self, original cluster status is UNKNOWN (deleted) and timed out, should return true
TEST_F(CloudCompactionRWSeparationTest, OtherClusterDeletedTimedOut) {
    int64_t now = UnixMillis();
    // status_mtime is old, timed out
    set_cluster_info("cluster-read", "cluster-write", cloud::ClusterStatus::UNKNOWN,
                     now - 3600000); // 1 hour ago (> 30 min timeout)

    EXPECT_TRUE(should_do_compaction_for_cluster(_tablet.get()));
}

// Test case 8: MANUAL_SHUTDOWN status should also be considered unavailable
TEST_F(CloudCompactionRWSeparationTest, OtherClusterManualShutdownTimedOut) {
    int64_t now = UnixMillis();
    set_cluster_info("cluster-read", "cluster-write", cloud::ClusterStatus::MANUAL_SHUTDOWN,
                     now - 3600000); // 1 hour ago

    EXPECT_TRUE(should_do_compaction_for_cluster(_tablet.get()));
}

// Test case 9: MANUAL_SHUTDOWN not timed out should return false
TEST_F(CloudCompactionRWSeparationTest, OtherClusterManualShutdownNotTimedOut) {
    int64_t now = UnixMillis();
    set_cluster_info("cluster-read", "cluster-write", cloud::ClusterStatus::MANUAL_SHUTDOWN,
                     now - 60000); // 1 minute ago

    EXPECT_FALSE(should_do_compaction_for_cluster(_tablet.get()));
}

// Test case 10: Timeout boundary test - exactly at timeout
TEST_F(CloudCompactionRWSeparationTest, TimeoutBoundary) {
    int64_t now = UnixMillis();
    int64_t timeout = config::compaction_cluster_takeover_timeout_ms;

    // Exactly at timeout boundary - should NOT take over (elapsed == timeout, not > timeout)
    set_cluster_info("cluster-read", "cluster-write", cloud::ClusterStatus::SUSPENDED,
                     now - timeout);

    // elapsed == timeout, condition is elapsed > timeout, so should return false
    EXPECT_FALSE(should_do_compaction_for_cluster(_tablet.get()));

    // Just past timeout - should take over
    set_cluster_info("cluster-read", "cluster-write", cloud::ClusterStatus::SUSPENDED,
                     now - timeout - 1);

    EXPECT_TRUE(should_do_compaction_for_cluster(_tablet.get()));
}

// Test case 11: Cluster info getter/setter test
TEST_F(CloudCompactionRWSeparationTest, ClusterInfoGetterSetter) {
    std::string my_cluster = "my_cluster_123";
    std::string last_active = "last_active_456";
    int64_t time_ms = 1234567890000;
    int32_t status = cloud::ClusterStatus::SUSPENDED;
    int64_t status_mtime = 9876543210000;

    _tablet->set_my_cluster_id(my_cluster);
    _tablet->set_last_active_cluster_info(last_active, time_ms, status, status_mtime);

    EXPECT_EQ(_tablet->my_cluster_id(), my_cluster);
    EXPECT_EQ(_tablet->last_active_cluster_id(), last_active);
    EXPECT_EQ(_tablet->last_active_time_ms(), time_ms);
    EXPECT_EQ(_tablet->last_active_cluster_status(), status);
    EXPECT_EQ(_tablet->last_active_cluster_status_mtime_ms(), status_mtime);
}

// Test case 12: Custom timeout configuration
TEST_F(CloudCompactionRWSeparationTest, CustomTimeoutConfig) {
    // Set a short timeout for this test
    config::compaction_cluster_takeover_timeout_ms = 5000; // 5 seconds

    int64_t now = UnixMillis();

    // 3 seconds ago - should not time out with 5s timeout
    set_cluster_info("cluster-read", "cluster-write", cloud::ClusterStatus::SUSPENDED,
                     now - 3000);
    EXPECT_FALSE(should_do_compaction_for_cluster(_tablet.get()));

    // 6 seconds ago - should time out with 5s timeout
    set_cluster_info("cluster-read", "cluster-write", cloud::ClusterStatus::SUSPENDED,
                     now - 6000);
    EXPECT_TRUE(should_do_compaction_for_cluster(_tablet.get()));
}

} // namespace doris
