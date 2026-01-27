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

#include "cloud/cloud_committed_rs_mgr.h"

#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <thread>

#include "cloud/config.h"
#include "olap/rowset/rowset_meta.h"

namespace doris {

class CloudCommittedRSMgrTest : public testing::Test {
protected:
    void SetUp() override {
        _mgr = std::make_unique<CloudCommittedRSMgr>();
        // Do not call init() to avoid starting background cleanup thread
    }

    void TearDown() override { _mgr.reset(); }

    RowsetMetaSharedPtr create_rowset_meta(int64_t tablet_id, int64_t txn_id,
                                           int64_t rowset_id_val = 0) {
        RowsetMetaPB rowset_meta_pb;
        rowset_meta_pb.set_tablet_id(tablet_id);
        rowset_meta_pb.set_txn_id(txn_id);
        rowset_meta_pb.set_num_segments(1);
        rowset_meta_pb.set_num_rows(100);
        rowset_meta_pb.set_total_disk_size(1024);
        rowset_meta_pb.set_data_disk_size(512);

        RowsetId rowset_id;
        if (rowset_id_val == 0) {
            rowset_id.init(txn_id);
        } else {
            rowset_id.init(rowset_id_val);
        }
        rowset_meta_pb.set_rowset_id(0);
        rowset_meta_pb.set_rowset_id_v2(rowset_id.to_string());

        auto rowset_meta = std::make_shared<RowsetMeta>();
        rowset_meta->init_from_pb(rowset_meta_pb);
        return rowset_meta;
    }

    int64_t current_time_seconds() {
        return std::chrono::duration_cast<std::chrono::seconds>(
                       std::chrono::system_clock::now().time_since_epoch())
                .count();
    }

protected:
    std::unique_ptr<CloudCommittedRSMgr> _mgr;
};

TEST_F(CloudCommittedRSMgrTest, TestAddAndGetCommittedRowset) {
    int64_t txn_id = 1000;
    int64_t tablet_id = 2000;
    auto rowset_meta = create_rowset_meta(tablet_id, txn_id);
    int64_t expiration_time = current_time_seconds() + 3600;

    // Add committed rowset
    _mgr->add_committed_rowset(txn_id, tablet_id, rowset_meta, expiration_time);

    // Get committed rowset
    auto result = _mgr->get_committed_rowset(txn_id, tablet_id);
    ASSERT_TRUE(result.has_value());
    auto [retrieved_meta, retrieved_expiration] = result.value();
    ASSERT_NE(retrieved_meta, nullptr);
    EXPECT_EQ(retrieved_meta->tablet_id(), tablet_id);
    EXPECT_EQ(retrieved_meta->txn_id(), txn_id);
    EXPECT_EQ(retrieved_meta->rowset_id().to_string(), rowset_meta->rowset_id().to_string());
    EXPECT_EQ(retrieved_expiration, expiration_time);
}

TEST_F(CloudCommittedRSMgrTest, TestGetNonExistentRowset) {
    int64_t txn_id = 1000;
    int64_t tablet_id = 2000;

    auto result = _mgr->get_committed_rowset(txn_id, tablet_id);
    EXPECT_FALSE(result.has_value());
    EXPECT_TRUE(result.error().is<ErrorCode::NOT_FOUND>());
}

TEST_F(CloudCommittedRSMgrTest, TestRemoveCommittedRowset) {
    int64_t txn_id = 1000;
    int64_t tablet_id = 2000;
    auto rowset_meta = create_rowset_meta(tablet_id, txn_id);
    int64_t expiration_time = current_time_seconds() + 3600;

    // Add committed rowset
    _mgr->add_committed_rowset(txn_id, tablet_id, rowset_meta, expiration_time);

    // Verify it exists
    auto result = _mgr->get_committed_rowset(txn_id, tablet_id);
    ASSERT_TRUE(result.has_value());

    // Remove it
    _mgr->remove_committed_rowset(txn_id, tablet_id);

    // Verify it's gone
    result = _mgr->get_committed_rowset(txn_id, tablet_id);
    EXPECT_FALSE(result.has_value());
}

TEST_F(CloudCommittedRSMgrTest, TestRemoveExpiredCommittedRowsets) {
    // Save original config value
    int32_t original_min_expired_seconds = config::tablet_txn_info_min_expired_seconds;
    // Set min expiration to 0 to allow testing of past expiration times
    config::tablet_txn_info_min_expired_seconds = -100;

    int64_t current_time = current_time_seconds();

    // Add expired rowset
    int64_t txn_id_1 = 1000;
    int64_t tablet_id_1 = 2000;
    auto rowset_meta_1 = create_rowset_meta(tablet_id_1, txn_id_1);
    int64_t expiration_time_1 = current_time - 10; // Already expired
    _mgr->add_committed_rowset(txn_id_1, tablet_id_1, rowset_meta_1, expiration_time_1);

    // Add non-expired rowset
    int64_t txn_id_2 = 1001;
    int64_t tablet_id_2 = 2001;
    auto rowset_meta_2 = create_rowset_meta(tablet_id_2, txn_id_2);
    int64_t expiration_time_2 = current_time + 3600; // Not expired
    _mgr->add_committed_rowset(txn_id_2, tablet_id_2, rowset_meta_2, expiration_time_2);

    // Verify both exist
    EXPECT_TRUE(_mgr->get_committed_rowset(txn_id_1, tablet_id_1).has_value());
    EXPECT_TRUE(_mgr->get_committed_rowset(txn_id_2, tablet_id_2).has_value());

    // Remove expired rowsets
    _mgr->remove_expired_committed_rowsets();

    // Verify expired rowset is removed
    EXPECT_FALSE(_mgr->get_committed_rowset(txn_id_1, tablet_id_1).has_value());
    // Verify non-expired rowset still exists
    EXPECT_TRUE(_mgr->get_committed_rowset(txn_id_2, tablet_id_2).has_value());

    // Restore config
    config::tablet_txn_info_min_expired_seconds = original_min_expired_seconds;
}

TEST_F(CloudCommittedRSMgrTest, TestMarkAndCheckEmptyRowset) {
    int64_t txn_id = 1000;
    int64_t tablet_id = 2000;
    int64_t txn_expiration = current_time_seconds() + 3600;

    // Initially not marked as empty
    auto result = _mgr->get_committed_rowset(txn_id, tablet_id);
    EXPECT_FALSE(result.has_value());

    // Mark as empty
    _mgr->mark_empty_rowset(txn_id, tablet_id, txn_expiration);

    // Check it's marked as empty
    result = _mgr->get_committed_rowset(txn_id, tablet_id);
    ASSERT_TRUE(result.has_value());
    auto [retrieved_meta, retrieved_expiration] = result.value();
    EXPECT_EQ(retrieved_meta, nullptr);
    EXPECT_EQ(retrieved_expiration, txn_expiration);
}

TEST_F(CloudCommittedRSMgrTest, TestEmptyRowsetExpiration) {
    // Save original config value
    int32_t original_min_expired_seconds = config::tablet_txn_info_min_expired_seconds;
    // Set min expiration to 0 to allow testing of past expiration times
    config::tablet_txn_info_min_expired_seconds = -100;

    int64_t current_time = current_time_seconds();

    // Mark as empty with past expiration
    int64_t txn_id = 1000;
    int64_t tablet_id = 2000;
    int64_t txn_expiration = current_time - 10; // Already expired
    _mgr->mark_empty_rowset(txn_id, tablet_id, txn_expiration);

    // Verify it's marked
    auto result = _mgr->get_committed_rowset(txn_id, tablet_id);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result.value().first, nullptr);

    // Remove expired rowsets
    _mgr->remove_expired_committed_rowsets();

    // Verify it's removed
    result = _mgr->get_committed_rowset(txn_id, tablet_id);
    EXPECT_FALSE(result.has_value());

    // Restore config
    config::tablet_txn_info_min_expired_seconds = original_min_expired_seconds;
}

TEST_F(CloudCommittedRSMgrTest, TestMultipleRowsets) {
    int64_t expiration_time = current_time_seconds() + 3600;

    // Add multiple rowsets for different tablets and transactions
    for (int i = 0; i < 10; i++) {
        int64_t txn_id = 1000 + i;
        int64_t tablet_id = 2000 + i;
        auto rowset_meta = create_rowset_meta(tablet_id, txn_id);
        _mgr->add_committed_rowset(txn_id, tablet_id, rowset_meta, expiration_time);
    }

    // Verify all rowsets can be retrieved
    for (int i = 0; i < 10; i++) {
        int64_t txn_id = 1000 + i;
        int64_t tablet_id = 2000 + i;
        auto result = _mgr->get_committed_rowset(txn_id, tablet_id);
        ASSERT_TRUE(result.has_value());
        auto [retrieved_meta, retrieved_expiration] = result.value();
        EXPECT_EQ(retrieved_meta->tablet_id(), tablet_id);
        EXPECT_EQ(retrieved_meta->txn_id(), txn_id);
    }
}

TEST_F(CloudCommittedRSMgrTest, TestSameTransactionDifferentTablets) {
    int64_t txn_id = 1000;
    int64_t expiration_time = current_time_seconds() + 3600;

    // Add same txn_id for different tablets
    for (int i = 0; i < 5; i++) {
        int64_t tablet_id = 2000 + i;
        auto rowset_meta = create_rowset_meta(tablet_id, txn_id);
        _mgr->add_committed_rowset(txn_id, tablet_id, rowset_meta, expiration_time);
    }

    // Verify all can be retrieved independently
    for (int i = 0; i < 5; i++) {
        int64_t tablet_id = 2000 + i;
        auto result = _mgr->get_committed_rowset(txn_id, tablet_id);
        ASSERT_TRUE(result.has_value());
        auto [retrieved_meta, retrieved_expiration] = result.value();
        EXPECT_EQ(retrieved_meta->tablet_id(), tablet_id);
        EXPECT_EQ(retrieved_meta->txn_id(), txn_id);
    }
}

TEST_F(CloudCommittedRSMgrTest, TestMinExpirationTime) {
    // Save original config value
    int64_t original_min_expired_seconds = config::tablet_txn_info_min_expired_seconds;

    // Set min expiration to 100 seconds
    config::tablet_txn_info_min_expired_seconds = 100;

    int64_t txn_id = 1000;
    int64_t tablet_id = 2000;
    auto rowset_meta = create_rowset_meta(tablet_id, txn_id);

    // Try to set expiration time less than min
    int64_t current_time = current_time_seconds();
    int64_t short_expiration = current_time + 10; // Less than min

    _mgr->add_committed_rowset(txn_id, tablet_id, rowset_meta, short_expiration);

    // Get and verify expiration is at least min
    auto result = _mgr->get_committed_rowset(txn_id, tablet_id);
    ASSERT_TRUE(result.has_value());
    auto [retrieved_meta, retrieved_expiration] = result.value();
    EXPECT_GE(retrieved_expiration, current_time + config::tablet_txn_info_min_expired_seconds);

    // Restore config
    config::tablet_txn_info_min_expired_seconds = original_min_expired_seconds;
}

TEST_F(CloudCommittedRSMgrTest, TestMixedRowsetsAndEmptyMarkers) {
    int64_t expiration_time = current_time_seconds() + 3600;

    // Add some normal rowsets
    for (int i = 0; i < 5; i++) {
        int64_t txn_id = 1000 + i;
        int64_t tablet_id = 2000 + i;
        auto rowset_meta = create_rowset_meta(tablet_id, txn_id);
        _mgr->add_committed_rowset(txn_id, tablet_id, rowset_meta, expiration_time);
    }

    // Add some empty markers
    for (int i = 5; i < 10; i++) {
        int64_t txn_id = 1000 + i;
        int64_t tablet_id = 2000 + i;
        _mgr->mark_empty_rowset(txn_id, tablet_id, expiration_time);
    }

    // Verify normal rowsets
    for (int i = 0; i < 5; i++) {
        int64_t txn_id = 1000 + i;
        int64_t tablet_id = 2000 + i;
        auto result = _mgr->get_committed_rowset(txn_id, tablet_id);
        ASSERT_TRUE(result.has_value());
        auto [retrieved_meta, retrieved_expiration] = result.value();
        EXPECT_NE(retrieved_meta, nullptr);
    }

    // Verify empty markers
    for (int i = 5; i < 10; i++) {
        int64_t txn_id = 1000 + i;
        int64_t tablet_id = 2000 + i;
        auto result = _mgr->get_committed_rowset(txn_id, tablet_id);
        ASSERT_TRUE(result.has_value());
        auto [retrieved_meta, retrieved_expiration] = result.value();
        EXPECT_EQ(retrieved_meta, nullptr);
    }
}

TEST_F(CloudCommittedRSMgrTest, TestExpiredRowsetsCleanupWithMixedTypes) {
    // Save original config value
    int64_t original_min_expired_seconds = config::tablet_txn_info_min_expired_seconds;
    // Set min expiration to 0 to allow testing of past expiration times
    config::tablet_txn_info_min_expired_seconds = 0;

    int64_t current_time = current_time_seconds();

    // Add expired normal rowset
    int64_t txn_id_1 = 1000;
    int64_t tablet_id_1 = 2000;
    auto rowset_meta_1 = create_rowset_meta(tablet_id_1, txn_id_1);
    _mgr->add_committed_rowset(txn_id_1, tablet_id_1, rowset_meta_1, current_time - 10);

    // Add expired empty marker
    int64_t txn_id_2 = 1001;
    int64_t tablet_id_2 = 2001;
    _mgr->mark_empty_rowset(txn_id_2, tablet_id_2, current_time - 10);

    // Add non-expired normal rowset
    int64_t txn_id_3 = 1002;
    int64_t tablet_id_3 = 2002;
    auto rowset_meta_3 = create_rowset_meta(tablet_id_3, txn_id_3);
    _mgr->add_committed_rowset(txn_id_3, tablet_id_3, rowset_meta_3, current_time + 3600);

    // Add non-expired empty marker
    int64_t txn_id_4 = 1003;
    int64_t tablet_id_4 = 2003;
    _mgr->mark_empty_rowset(txn_id_4, tablet_id_4, current_time + 3600);

    // Verify all exist
    EXPECT_TRUE(_mgr->get_committed_rowset(txn_id_1, tablet_id_1).has_value());
    auto result_2 = _mgr->get_committed_rowset(txn_id_2, tablet_id_2);
    EXPECT_TRUE(result_2.has_value());
    EXPECT_EQ(result_2.value().first, nullptr);
    EXPECT_TRUE(_mgr->get_committed_rowset(txn_id_3, tablet_id_3).has_value());
    auto result_4 = _mgr->get_committed_rowset(txn_id_4, tablet_id_4);
    EXPECT_TRUE(result_4.has_value());
    EXPECT_EQ(result_4.value().first, nullptr);

    // Remove expired
    _mgr->remove_expired_committed_rowsets();

    // Verify expired are removed
    EXPECT_FALSE(_mgr->get_committed_rowset(txn_id_1, tablet_id_1).has_value());
    EXPECT_FALSE(_mgr->get_committed_rowset(txn_id_2, tablet_id_2).has_value());

    // Verify non-expired still exist
    EXPECT_TRUE(_mgr->get_committed_rowset(txn_id_3, tablet_id_3).has_value());
    result_4 = _mgr->get_committed_rowset(txn_id_4, tablet_id_4);
    EXPECT_TRUE(result_4.has_value());
    EXPECT_EQ(result_4.value().first, nullptr);

    // Restore config
    config::tablet_txn_info_min_expired_seconds = original_min_expired_seconds;
}

TEST_F(CloudCommittedRSMgrTest, TestUpdateSameRowset) {
    int64_t txn_id = 1000;
    int64_t tablet_id = 2000;
    int64_t expiration_time_1 = current_time_seconds() + 1800;
    int64_t expiration_time_2 = current_time_seconds() + 3600;

    // Add rowset first time
    auto rowset_meta_1 = create_rowset_meta(tablet_id, txn_id, 10001);
    _mgr->add_committed_rowset(txn_id, tablet_id, rowset_meta_1, expiration_time_1);

    // Verify first rowset is added
    auto result = _mgr->get_committed_rowset(txn_id, tablet_id);
    ASSERT_TRUE(result.has_value());
    auto [retrieved_meta_1, retrieved_expiration_1] = result.value();
    EXPECT_EQ(retrieved_meta_1->rowset_id().to_string(), rowset_meta_1->rowset_id().to_string());

    // Add same txn_id and tablet_id again with different rowset and expiration
    // Due to using insert_or_assign(), the second insert should overwrite the first one
    auto rowset_meta_2 = create_rowset_meta(tablet_id, txn_id, 10002);
    _mgr->add_committed_rowset(txn_id, tablet_id, rowset_meta_2, expiration_time_2);

    // Get and verify it's the second one (insert_or_assign overwrites)
    result = _mgr->get_committed_rowset(txn_id, tablet_id);
    ASSERT_TRUE(result.has_value());
    auto [retrieved_meta_2, retrieved_expiration_2] = result.value();
    EXPECT_EQ(retrieved_meta_2->rowset_id().to_string(), rowset_meta_2->rowset_id().to_string());
}

} // namespace doris
