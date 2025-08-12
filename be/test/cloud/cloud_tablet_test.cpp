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

#include "cloud/cloud_tablet.h"

#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <ranges>

#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_warm_up_manager.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/tablet_meta.h"
#include "util/uid_util.h"

namespace doris {

using namespace std::chrono;

class CloudTabletWarmUpStateTest : public testing::Test {
public:
    CloudTabletWarmUpStateTest() : _engine(CloudStorageEngine(EngineOptions {})) {}

    void SetUp() override {
        _tablet_meta.reset(new TabletMeta(1, 2, 15673, 15674, 4, 5, TTabletSchema(), 6, {{7, 8}},
                                          UniqueId(9, 10), TTabletType::TABLET_TYPE_DISK,
                                          TCompressionType::LZ4F));
        _tablet =
                std::make_shared<CloudTablet>(_engine, std::make_shared<TabletMeta>(*_tablet_meta));
    }
    void TearDown() override {}

    RowsetSharedPtr create_rowset(Version version, int num_segments = 1) {
        auto rs_meta = std::make_shared<RowsetMeta>();
        rs_meta->set_rowset_type(BETA_ROWSET);
        rs_meta->set_version(version);
        rs_meta->set_rowset_id(_engine.next_rowset_id());
        rs_meta->set_num_segments(num_segments);
        RowsetSharedPtr rowset;
        Status st = RowsetFactory::create_rowset(nullptr, "", rs_meta, &rowset);
        if (!st.ok()) {
            return nullptr;
        }
        return rowset;
    }

    // void add_new_version_rowset(CloudTabletSPtr tablet, int64_t version, bool warmed_up,
    //                             time_point<system_clock> visible_timestamp) {
    //     auto rowset = create_rowset(Version {version, version}, visible_timestamp);
    //     if (warmed_up) {
    //         tablet->add_warmed_up_rowset(rowset->rowset_id());
    //     }
    //     std::unique_lock wlock {tablet->get_header_lock()};
    //     tablet->add_rowsets({rowset}, false, wlock, false);
    // }

    // void do_cumu_compaction(CloudTabletSPtr tablet, int64_t start_version, int64_t end_version,
    //                         bool warmed_up, time_point<system_clock> visible_timestamp) {
    //     std::unique_lock wrlock {tablet->get_header_lock()};
    //     std::vector<RowsetSharedPtr> input_rowsets;
    //     auto output_rowset = create_rowset(Version {start_version, end_version}, visible_timestamp);
    //     if (warmed_up) {
    //         tablet->add_warmed_up_rowset(output_rowset->rowset_id());
    //     }
    //     std::ranges::copy_if(std::views::values(tablet->rowset_map()),
    //                          std::back_inserter(input_rowsets), [=](const RowsetSharedPtr& rowset) {
    //                              return rowset->version().first >= start_version &&
    //                                     rowset->version().first <= end_version;
    //                          });
    //     if (input_rowsets.size() == 1) {
    //         tablet->add_rowsets({output_rowset}, true, wrlock);
    //     } else {
    //         tablet->delete_rowsets(input_rowsets, wrlock);
    //         tablet->add_rowsets({output_rowset}, false, wrlock);
    //     }
    // }

protected:
    std::string _json_rowset_meta;
    TabletMetaSharedPtr _tablet_meta;
    std::shared_ptr<CloudTablet> _tablet;
    CloudStorageEngine _engine;
};

// Test get_rowset_warmup_state for non-existent rowset
TEST_F(CloudTabletWarmUpStateTest, TestGetRowsetWarmupStateNonExistent) {
    auto rowset = create_rowset(Version(1, 1));
    ASSERT_NE(rowset, nullptr);

    auto non_existent_id = _engine.next_rowset_id();

    WarmUpState state = _tablet->get_rowset_warmup_state(non_existent_id);
    EXPECT_EQ(state, WarmUpState::NONE);
}

// Test add_rowset_warmup_state with TRIGGERED_BY_JOB state
TEST_F(CloudTabletWarmUpStateTest, TestAddRowsetWarmupStateTriggeredByJob) {
    auto rowset = create_rowset(Version(1, 1), 5);
    ASSERT_NE(rowset, nullptr);

    bool result = _tablet->add_rowset_warmup_state(*(rowset->rowset_meta()),
                                                   WarmUpState::TRIGGERED_BY_JOB);
    EXPECT_TRUE(result);

    // Verify the state is correctly set
    WarmUpState state = _tablet->get_rowset_warmup_state(rowset->rowset_id());
    EXPECT_EQ(state, WarmUpState::TRIGGERED_BY_JOB);
}

// Test add_rowset_warmup_state with TRIGGERED_BY_SYNC_ROWSET state
TEST_F(CloudTabletWarmUpStateTest, TestAddRowsetWarmupStateTriggeredBySyncRowset) {
    auto rowset = create_rowset(Version(2, 2), 3);
    ASSERT_NE(rowset, nullptr);

    bool result = _tablet->add_rowset_warmup_state(*(rowset->rowset_meta()),
                                                   WarmUpState::TRIGGERED_BY_SYNC_ROWSET);
    EXPECT_TRUE(result);

    // Verify the state is correctly set
    WarmUpState state = _tablet->get_rowset_warmup_state(rowset->rowset_id());
    EXPECT_EQ(state, WarmUpState::TRIGGERED_BY_SYNC_ROWSET);
}

// Test adding duplicate rowset warmup state should fail
TEST_F(CloudTabletWarmUpStateTest, TestAddDuplicateRowsetWarmupState) {
    auto rowset = create_rowset(Version(3, 3), 2);
    ASSERT_NE(rowset, nullptr);

    // First addition should succeed
    bool result1 = _tablet->add_rowset_warmup_state(*(rowset->rowset_meta()),
                                                    WarmUpState::TRIGGERED_BY_JOB);
    EXPECT_TRUE(result1);

    // Second addition should fail
    bool result2 = _tablet->add_rowset_warmup_state(*(rowset->rowset_meta()),
                                                    WarmUpState::TRIGGERED_BY_SYNC_ROWSET);
    EXPECT_FALSE(result2);

    // State should remain the original one
    WarmUpState state = _tablet->get_rowset_warmup_state(rowset->rowset_id());
    EXPECT_EQ(state, WarmUpState::TRIGGERED_BY_JOB);
}

// Test complete_rowset_segment_warmup for non-existent rowset
TEST_F(CloudTabletWarmUpStateTest, TestCompleteRowsetSegmentWarmupNonExistent) {
    auto non_existent_id = _engine.next_rowset_id();

    WarmUpState result = _tablet->complete_rowset_segment_warmup(non_existent_id, Status::OK());
    EXPECT_EQ(result, WarmUpState::NONE);
}

// Test complete_rowset_segment_warmup with partial completion
TEST_F(CloudTabletWarmUpStateTest, TestCompleteRowsetSegmentWarmupPartial) {
    auto rowset = create_rowset(Version(4, 4), 3);
    ASSERT_NE(rowset, nullptr);

    // Add rowset warmup state
    bool add_result = _tablet->add_rowset_warmup_state(*(rowset->rowset_meta()),
                                                       WarmUpState::TRIGGERED_BY_JOB);
    EXPECT_TRUE(add_result);

    // Complete one segment, should still be in TRIGGERED_BY_JOB state
    WarmUpState result1 =
            _tablet->complete_rowset_segment_warmup(rowset->rowset_id(), Status::OK());
    EXPECT_EQ(result1, WarmUpState::TRIGGERED_BY_JOB);

    // Complete second segment, should still be in TRIGGERED_BY_JOB state
    WarmUpState result2 =
            _tablet->complete_rowset_segment_warmup(rowset->rowset_id(), Status::OK());
    EXPECT_EQ(result2, WarmUpState::TRIGGERED_BY_JOB);

    // Verify current state is still TRIGGERED_BY_JOB
    WarmUpState current_state = _tablet->get_rowset_warmup_state(rowset->rowset_id());
    EXPECT_EQ(current_state, WarmUpState::TRIGGERED_BY_JOB);
}

// Test complete_rowset_segment_warmup with full completion
TEST_F(CloudTabletWarmUpStateTest, TestCompleteRowsetSegmentWarmupFull) {
    auto rowset = create_rowset(Version(5, 5), 2);
    ASSERT_NE(rowset, nullptr);

    // Add rowset warmup state
    bool add_result = _tablet->add_rowset_warmup_state(*(rowset->rowset_meta()),
                                                       WarmUpState::TRIGGERED_BY_SYNC_ROWSET);
    EXPECT_TRUE(add_result);

    // Complete first segment
    WarmUpState result1 =
            _tablet->complete_rowset_segment_warmup(rowset->rowset_id(), Status::OK());
    EXPECT_EQ(result1, WarmUpState::TRIGGERED_BY_SYNC_ROWSET);

    // Complete second segment, should transition to DONE state
    WarmUpState result2 =
            _tablet->complete_rowset_segment_warmup(rowset->rowset_id(), Status::OK());
    EXPECT_EQ(result2, WarmUpState::DONE);

    // Verify final state is DONE
    WarmUpState final_state = _tablet->get_rowset_warmup_state(rowset->rowset_id());
    EXPECT_EQ(final_state, WarmUpState::DONE);
}

// Test complete_rowset_segment_warmup with error status
TEST_F(CloudTabletWarmUpStateTest, TestCompleteRowsetSegmentWarmupWithError) {
    auto rowset = create_rowset(Version(6, 6), 1);
    ASSERT_NE(rowset, nullptr);

    // Add rowset warmup state
    bool add_result = _tablet->add_rowset_warmup_state(*(rowset->rowset_meta()),
                                                       WarmUpState::TRIGGERED_BY_JOB);
    EXPECT_TRUE(add_result);

    // Complete with error status, should still transition to DONE when all segments complete
    Status error_status = Status::InternalError("Test error");
    WarmUpState result = _tablet->complete_rowset_segment_warmup(rowset->rowset_id(), error_status);
    EXPECT_EQ(result, WarmUpState::DONE);

    // Verify final state is DONE even with error
    WarmUpState final_state = _tablet->get_rowset_warmup_state(rowset->rowset_id());
    EXPECT_EQ(final_state, WarmUpState::DONE);
}

// Test multiple rowsets warmup state management
TEST_F(CloudTabletWarmUpStateTest, TestMultipleRowsetsWarmupState) {
    auto rowset1 = create_rowset(Version(7, 7), 2);
    auto rowset2 = create_rowset(Version(8, 8), 3);
    auto rowset3 = create_rowset(Version(9, 9), 1);
    ASSERT_NE(rowset1, nullptr);
    ASSERT_NE(rowset2, nullptr);
    ASSERT_NE(rowset3, nullptr);

    // Add multiple rowsets
    EXPECT_TRUE(_tablet->add_rowset_warmup_state(*(rowset1->rowset_meta()),
                                                 WarmUpState::TRIGGERED_BY_JOB));
    EXPECT_TRUE(_tablet->add_rowset_warmup_state(*(rowset2->rowset_meta()),
                                                 WarmUpState::TRIGGERED_BY_SYNC_ROWSET));
    EXPECT_TRUE(_tablet->add_rowset_warmup_state(*(rowset3->rowset_meta()),
                                                 WarmUpState::TRIGGERED_BY_JOB));

    // Verify all states
    EXPECT_EQ(_tablet->get_rowset_warmup_state(rowset1->rowset_id()),
              WarmUpState::TRIGGERED_BY_JOB);
    EXPECT_EQ(_tablet->get_rowset_warmup_state(rowset2->rowset_id()),
              WarmUpState::TRIGGERED_BY_SYNC_ROWSET);
    EXPECT_EQ(_tablet->get_rowset_warmup_state(rowset3->rowset_id()),
              WarmUpState::TRIGGERED_BY_JOB);

    // Complete rowset1 (2 segments)
    EXPECT_EQ(_tablet->complete_rowset_segment_warmup(rowset1->rowset_id(), Status::OK()),
              WarmUpState::TRIGGERED_BY_JOB);
    EXPECT_EQ(_tablet->complete_rowset_segment_warmup(rowset1->rowset_id(), Status::OK()),
              WarmUpState::DONE);

    // Complete rowset3 (1 segment)
    EXPECT_EQ(_tablet->complete_rowset_segment_warmup(rowset3->rowset_id(), Status::OK()),
              WarmUpState::DONE);

    // Verify states after completion
    EXPECT_EQ(_tablet->get_rowset_warmup_state(rowset1->rowset_id()), WarmUpState::DONE);
    EXPECT_EQ(_tablet->get_rowset_warmup_state(rowset2->rowset_id()),
              WarmUpState::TRIGGERED_BY_SYNC_ROWSET);
    EXPECT_EQ(_tablet->get_rowset_warmup_state(rowset3->rowset_id()), WarmUpState::DONE);
}

// Test warmup state with zero segments (edge case)
TEST_F(CloudTabletWarmUpStateTest, TestWarmupStateWithZeroSegments) {
    auto rowset = create_rowset(Version(10, 10), 0);
    ASSERT_NE(rowset, nullptr);

    // Add rowset with zero segments
    bool add_result = _tablet->add_rowset_warmup_state(*(rowset->rowset_meta()),
                                                       WarmUpState::TRIGGERED_BY_JOB);
    EXPECT_TRUE(add_result);

    // State should be immediately ready for completion since there are no segments to warm up
    WarmUpState state = _tablet->get_rowset_warmup_state(rowset->rowset_id());
    EXPECT_EQ(state, WarmUpState::TRIGGERED_BY_JOB);

    // Any completion call should handle the edge case gracefully
    WarmUpState result = _tablet->complete_rowset_segment_warmup(rowset->rowset_id(), Status::OK());
    // With 0 segments, the counter should already be 0, so this should transition to DONE
    EXPECT_EQ(result, WarmUpState::DONE);
}

// Test concurrent access to warmup state (basic thread safety verification)
TEST_F(CloudTabletWarmUpStateTest, TestConcurrentWarmupStateAccess) {
    auto rowset1 = create_rowset(Version(11, 11), 4);
    auto rowset2 = create_rowset(Version(12, 12), 3);
    ASSERT_NE(rowset1, nullptr);
    ASSERT_NE(rowset2, nullptr);

    // Add rowsets from different "threads" (simulated by sequential calls)
    EXPECT_TRUE(_tablet->add_rowset_warmup_state(*(rowset1->rowset_meta()),
                                                 WarmUpState::TRIGGERED_BY_JOB));
    EXPECT_TRUE(_tablet->add_rowset_warmup_state(*(rowset2->rowset_meta()),
                                                 WarmUpState::TRIGGERED_BY_SYNC_ROWSET));

    // Interleaved completion operations
    EXPECT_EQ(_tablet->complete_rowset_segment_warmup(rowset1->rowset_id(), Status::OK()),
              WarmUpState::TRIGGERED_BY_JOB);
    EXPECT_EQ(_tablet->complete_rowset_segment_warmup(rowset2->rowset_id(), Status::OK()),
              WarmUpState::TRIGGERED_BY_SYNC_ROWSET);
    EXPECT_EQ(_tablet->complete_rowset_segment_warmup(rowset1->rowset_id(), Status::OK()),
              WarmUpState::TRIGGERED_BY_JOB);

    // Check states are maintained correctly
    EXPECT_EQ(_tablet->get_rowset_warmup_state(rowset1->rowset_id()),
              WarmUpState::TRIGGERED_BY_JOB);
    EXPECT_EQ(_tablet->get_rowset_warmup_state(rowset2->rowset_id()),
              WarmUpState::TRIGGERED_BY_SYNC_ROWSET);
}
} // namespace doris
