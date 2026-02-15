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
#include "common/config.h"
#include "cpp/sync_point.h"
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
    WarmUpState expected_state = WarmUpState {WarmUpTriggerSource::NONE, WarmUpProgress::NONE};
    EXPECT_EQ(state, expected_state);
}

// Test add_rowset_warmup_state with TRIGGERED_BY_EVENT_DRIVEN state
TEST_F(CloudTabletWarmUpStateTest, TestAddRowsetWarmupStateTriggeredByJob) {
    auto rowset = create_rowset(Version(1, 1), 5);
    ASSERT_NE(rowset, nullptr);

    bool result = _tablet->add_rowset_warmup_state(*(rowset->rowset_meta()),
                                                   WarmUpTriggerSource::EVENT_DRIVEN);
    EXPECT_TRUE(result);

    // Verify the state is correctly set
    WarmUpState state = _tablet->get_rowset_warmup_state(rowset->rowset_id());
    WarmUpState expected_state =
            WarmUpState {WarmUpTriggerSource::EVENT_DRIVEN, WarmUpProgress::DOING};
    EXPECT_EQ(state, expected_state);
}

// Test add_rowset_warmup_state with TRIGGERED_BY_SYNC_ROWSET state
TEST_F(CloudTabletWarmUpStateTest, TestAddRowsetWarmupStateTriggeredBySyncRowset) {
    auto rowset = create_rowset(Version(2, 2), 3);
    ASSERT_NE(rowset, nullptr);

    bool result = _tablet->add_rowset_warmup_state(*(rowset->rowset_meta()),
                                                   WarmUpTriggerSource::SYNC_ROWSET);
    EXPECT_TRUE(result);

    // Verify the state is correctly set
    WarmUpState state = _tablet->get_rowset_warmup_state(rowset->rowset_id());
    WarmUpState expected_state =
            WarmUpState {WarmUpTriggerSource::SYNC_ROWSET, WarmUpProgress::DOING};
    EXPECT_EQ(state, expected_state);
}

// Test adding duplicate rowset warmup state should fail
TEST_F(CloudTabletWarmUpStateTest, TestAddDuplicateRowsetWarmupState) {
    auto rowset = create_rowset(Version(3, 3), 2);
    ASSERT_NE(rowset, nullptr);

    // First addition should succeed
    bool result1 = _tablet->add_rowset_warmup_state(*(rowset->rowset_meta()),
                                                    WarmUpTriggerSource::EVENT_DRIVEN);
    EXPECT_TRUE(result1);

    // Second addition should fail
    bool result2 = _tablet->add_rowset_warmup_state(*(rowset->rowset_meta()),
                                                    WarmUpTriggerSource::SYNC_ROWSET);
    EXPECT_FALSE(result2);

    // State should remain the original one
    WarmUpState state = _tablet->get_rowset_warmup_state(rowset->rowset_id());
    WarmUpState expected_state =
            WarmUpState {WarmUpTriggerSource::EVENT_DRIVEN, WarmUpProgress::DOING};
    EXPECT_EQ(state, expected_state);
}

// Test complete_rowset_segment_warmup for non-existent rowset
TEST_F(CloudTabletWarmUpStateTest, TestCompleteRowsetSegmentWarmupNonExistent) {
    auto non_existent_id = _engine.next_rowset_id();

    WarmUpState result = _tablet->complete_rowset_segment_warmup(
            WarmUpTriggerSource::SYNC_ROWSET, non_existent_id, Status::OK(), 1, 0);
    WarmUpState expected_state = WarmUpState {WarmUpTriggerSource::NONE, WarmUpProgress::NONE};
    EXPECT_EQ(result, expected_state);
}

// Test complete_rowset_segment_warmup with partial completion
TEST_F(CloudTabletWarmUpStateTest, TestCompleteRowsetSegmentWarmupPartial) {
    auto rowset = create_rowset(Version(4, 4), 3);
    ASSERT_NE(rowset, nullptr);

    // Add rowset warmup state
    bool add_result = _tablet->add_rowset_warmup_state(*(rowset->rowset_meta()),
                                                       WarmUpTriggerSource::EVENT_DRIVEN);
    EXPECT_TRUE(add_result);

    // Complete one segment, should still be in TRIGGERED_BY_EVENT_DRIVEN state
    WarmUpState result1 = _tablet->complete_rowset_segment_warmup(
            WarmUpTriggerSource::EVENT_DRIVEN, rowset->rowset_id(), Status::OK(), 1, 0);
    WarmUpState expected_state =
            WarmUpState {WarmUpTriggerSource::EVENT_DRIVEN, WarmUpProgress::DOING};
    EXPECT_EQ(result1, expected_state);

    // Complete second segment, should still be in TRIGGERED_BY_EVENT_DRIVEN state
    WarmUpState result2 = _tablet->complete_rowset_segment_warmup(
            WarmUpTriggerSource::EVENT_DRIVEN, rowset->rowset_id(), Status::OK(), 1, 0);
    expected_state = WarmUpState {WarmUpTriggerSource::EVENT_DRIVEN, WarmUpProgress::DOING};
    EXPECT_EQ(result2, expected_state);

    // Verify current state is still TRIGGERED_BY_EVENT_DRIVEN
    WarmUpState current_state = _tablet->get_rowset_warmup_state(rowset->rowset_id());
    expected_state = WarmUpState {WarmUpTriggerSource::EVENT_DRIVEN, WarmUpProgress::DOING};
    EXPECT_EQ(current_state, expected_state);
}

// Test complete_rowset_segment_warmup with full completion
TEST_F(CloudTabletWarmUpStateTest, TestCompleteRowsetSegmentWarmupFull) {
    auto rowset = create_rowset(Version(5, 5), 2);
    ASSERT_NE(rowset, nullptr);

    // Add rowset warmup state
    bool add_result = _tablet->add_rowset_warmup_state(*(rowset->rowset_meta()),
                                                       WarmUpTriggerSource::SYNC_ROWSET);
    EXPECT_TRUE(add_result);

    // Complete first segment
    WarmUpState result1 = _tablet->complete_rowset_segment_warmup(
            WarmUpTriggerSource::SYNC_ROWSET, rowset->rowset_id(), Status::OK(), 1, 0);
    WarmUpState expected_state =
            WarmUpState {WarmUpTriggerSource::SYNC_ROWSET, WarmUpProgress::DOING};
    EXPECT_EQ(result1, expected_state);

    // Complete second segment, should transition to DONE state
    WarmUpState result2 = _tablet->complete_rowset_segment_warmup(
            WarmUpTriggerSource::SYNC_ROWSET, rowset->rowset_id(), Status::OK(), 1, 0);
    expected_state = WarmUpState {WarmUpTriggerSource::SYNC_ROWSET, WarmUpProgress::DONE};
    EXPECT_EQ(result2, expected_state);

    // Verify final state is DONE
    WarmUpState final_state = _tablet->get_rowset_warmup_state(rowset->rowset_id());
    expected_state = WarmUpState {WarmUpTriggerSource::SYNC_ROWSET, WarmUpProgress::DONE};
    EXPECT_EQ(final_state, expected_state);
}

// Test complete_rowset_segment_warmup with inverted index file, partial completion
TEST_F(CloudTabletWarmUpStateTest, TestCompleteRowsetSegmentWarmupWithInvertedIndexPartial) {
    auto rowset = create_rowset(Version(6, 6), 1);
    ASSERT_NE(rowset, nullptr);

    // Add rowset warmup state
    bool add_result = _tablet->add_rowset_warmup_state(*(rowset->rowset_meta()),
                                                       WarmUpTriggerSource::EVENT_DRIVEN);
    EXPECT_TRUE(add_result);

    EXPECT_TRUE(_tablet->update_rowset_warmup_state_inverted_idx_num(
            WarmUpTriggerSource::EVENT_DRIVEN, rowset->rowset_id(), 1));
    EXPECT_TRUE(_tablet->update_rowset_warmup_state_inverted_idx_num(
            WarmUpTriggerSource::EVENT_DRIVEN, rowset->rowset_id(), 1));

    // Complete one segment file
    WarmUpState result1 = _tablet->complete_rowset_segment_warmup(
            WarmUpTriggerSource::EVENT_DRIVEN, rowset->rowset_id(), Status::OK(), 1, 0);
    WarmUpState expected_state =
            WarmUpState {WarmUpTriggerSource::EVENT_DRIVEN, WarmUpProgress::DOING};
    EXPECT_EQ(result1, expected_state);

    // Complete inverted index file, should still be in TRIGGERED_BY_EVENT_DRIVEN state
    WarmUpState result2 = _tablet->complete_rowset_segment_warmup(
            WarmUpTriggerSource::EVENT_DRIVEN, rowset->rowset_id(), Status::OK(), 0, 1);
    expected_state = WarmUpState {WarmUpTriggerSource::EVENT_DRIVEN, WarmUpProgress::DOING};
    EXPECT_EQ(result2, expected_state);

    // Verify current state is still TRIGGERED_BY_EVENT_DRIVEN
    WarmUpState current_state = _tablet->get_rowset_warmup_state(rowset->rowset_id());
    expected_state = WarmUpState {WarmUpTriggerSource::EVENT_DRIVEN, WarmUpProgress::DOING};
    EXPECT_EQ(current_state, expected_state);
}

// Test complete_rowset_segment_warmup with inverted index file, full completion
TEST_F(CloudTabletWarmUpStateTest, TestCompleteRowsetSegmentWarmupWithInvertedIndexFull) {
    auto rowset = create_rowset(Version(6, 6), 1);
    ASSERT_NE(rowset, nullptr);

    // Add rowset warmup state
    bool add_result = _tablet->add_rowset_warmup_state(*(rowset->rowset_meta()),
                                                       WarmUpTriggerSource::EVENT_DRIVEN);
    EXPECT_TRUE(add_result);

    EXPECT_TRUE(_tablet->update_rowset_warmup_state_inverted_idx_num(
            WarmUpTriggerSource::EVENT_DRIVEN, rowset->rowset_id(), 1));

    // Complete segment file
    WarmUpState result1 = _tablet->complete_rowset_segment_warmup(
            WarmUpTriggerSource::EVENT_DRIVEN, rowset->rowset_id(), Status::OK(), 1, 0);
    WarmUpState expected_state =
            WarmUpState {WarmUpTriggerSource::EVENT_DRIVEN, WarmUpProgress::DOING};
    EXPECT_EQ(result1, expected_state);

    // Complete inverted index file
    WarmUpState result2 = _tablet->complete_rowset_segment_warmup(
            WarmUpTriggerSource::EVENT_DRIVEN, rowset->rowset_id(), Status::OK(), 0, 1);
    expected_state = WarmUpState {WarmUpTriggerSource::EVENT_DRIVEN, WarmUpProgress::DONE};
    EXPECT_EQ(result2, expected_state);

    // Verify final state is DONE
    WarmUpState final_state = _tablet->get_rowset_warmup_state(rowset->rowset_id());
    expected_state = WarmUpState {WarmUpTriggerSource::EVENT_DRIVEN, WarmUpProgress::DONE};
    EXPECT_EQ(final_state, expected_state);
}

// Test complete_rowset_segment_warmup with error status
TEST_F(CloudTabletWarmUpStateTest, TestCompleteRowsetSegmentWarmupWithError) {
    auto rowset = create_rowset(Version(6, 6), 1);
    ASSERT_NE(rowset, nullptr);

    // Add rowset warmup state
    bool add_result = _tablet->add_rowset_warmup_state(*(rowset->rowset_meta()),
                                                       WarmUpTriggerSource::EVENT_DRIVEN);
    EXPECT_TRUE(add_result);

    // Complete with error status, should still transition to DONE when all segments complete
    Status error_status = Status::InternalError("Test error");
    WarmUpState result = _tablet->complete_rowset_segment_warmup(
            WarmUpTriggerSource::EVENT_DRIVEN, rowset->rowset_id(), error_status, 1, 0);
    WarmUpState expected_state =
            WarmUpState {WarmUpTriggerSource::EVENT_DRIVEN, WarmUpProgress::DONE};
    EXPECT_EQ(result, expected_state);

    // Verify final state is DONE even with error
    WarmUpState final_state = _tablet->get_rowset_warmup_state(rowset->rowset_id());
    expected_state = WarmUpState {WarmUpTriggerSource::EVENT_DRIVEN, WarmUpProgress::DONE};
    EXPECT_EQ(final_state, expected_state);
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
                                                 WarmUpTriggerSource::EVENT_DRIVEN));
    EXPECT_TRUE(_tablet->add_rowset_warmup_state(*(rowset2->rowset_meta()),
                                                 WarmUpTriggerSource::SYNC_ROWSET));
    EXPECT_TRUE(_tablet->add_rowset_warmup_state(*(rowset3->rowset_meta()),
                                                 WarmUpTriggerSource::EVENT_DRIVEN));

    // Verify all states
    WarmUpState expected_state =
            WarmUpState {WarmUpTriggerSource::EVENT_DRIVEN, WarmUpProgress::DOING};
    EXPECT_EQ(_tablet->get_rowset_warmup_state(rowset1->rowset_id()), expected_state);
    expected_state = WarmUpState {WarmUpTriggerSource::SYNC_ROWSET, WarmUpProgress::DOING};
    EXPECT_EQ(_tablet->get_rowset_warmup_state(rowset2->rowset_id()), expected_state);
    expected_state = WarmUpState {WarmUpTriggerSource::EVENT_DRIVEN, WarmUpProgress::DOING};
    EXPECT_EQ(_tablet->get_rowset_warmup_state(rowset3->rowset_id()), expected_state);

    // Complete rowset1 (2 segments)
    WarmUpState result = _tablet->complete_rowset_segment_warmup(
            WarmUpTriggerSource::EVENT_DRIVEN, rowset1->rowset_id(), Status::OK(), 1, 0);
    expected_state = WarmUpState {WarmUpTriggerSource::EVENT_DRIVEN, WarmUpProgress::DOING};
    EXPECT_EQ(result, expected_state);
    result = _tablet->complete_rowset_segment_warmup(WarmUpTriggerSource::EVENT_DRIVEN,
                                                     rowset1->rowset_id(), Status::OK(), 1, 0);
    expected_state = WarmUpState {WarmUpTriggerSource::EVENT_DRIVEN, WarmUpProgress::DONE};
    EXPECT_EQ(result, expected_state);

    // Complete rowset3 (1 segment)
    result = _tablet->complete_rowset_segment_warmup(WarmUpTriggerSource::EVENT_DRIVEN,
                                                     rowset3->rowset_id(), Status::OK(), 1, 0);
    expected_state = WarmUpState {WarmUpTriggerSource::EVENT_DRIVEN, WarmUpProgress::DONE};
    EXPECT_EQ(result, expected_state);

    // Verify states after completion
    expected_state = WarmUpState {WarmUpTriggerSource::EVENT_DRIVEN, WarmUpProgress::DONE};
    EXPECT_EQ(_tablet->get_rowset_warmup_state(rowset1->rowset_id()), expected_state);
    expected_state = WarmUpState {WarmUpTriggerSource::SYNC_ROWSET, WarmUpProgress::DOING};
    EXPECT_EQ(_tablet->get_rowset_warmup_state(rowset2->rowset_id()), expected_state);
    expected_state = WarmUpState {WarmUpTriggerSource::EVENT_DRIVEN, WarmUpProgress::DONE};
    EXPECT_EQ(_tablet->get_rowset_warmup_state(rowset3->rowset_id()), expected_state);
}

// Test warmup state with zero segments (edge case)
TEST_F(CloudTabletWarmUpStateTest, TestWarmupStateWithZeroSegments) {
    auto rowset = create_rowset(Version(10, 10), 0);
    ASSERT_NE(rowset, nullptr);

    // Add rowset with zero segments
    bool add_result = _tablet->add_rowset_warmup_state(*(rowset->rowset_meta()),
                                                       WarmUpTriggerSource::EVENT_DRIVEN);
    EXPECT_TRUE(add_result);

    // State should be immediately ready for completion since there are no segments to warm up
    WarmUpState state = _tablet->get_rowset_warmup_state(rowset->rowset_id());
    WarmUpState expected_state =
            WarmUpState {WarmUpTriggerSource::EVENT_DRIVEN, WarmUpProgress::DONE};
    EXPECT_EQ(state, expected_state);
}

// Test concurrent access to warmup state (basic thread safety verification)
TEST_F(CloudTabletWarmUpStateTest, TestConcurrentWarmupStateAccess) {
    auto rowset1 = create_rowset(Version(11, 11), 4);
    auto rowset2 = create_rowset(Version(12, 12), 3);
    ASSERT_NE(rowset1, nullptr);
    ASSERT_NE(rowset2, nullptr);

    // Add rowsets from different "threads" (simulated by sequential calls)
    EXPECT_TRUE(_tablet->add_rowset_warmup_state(*(rowset1->rowset_meta()),
                                                 WarmUpTriggerSource::EVENT_DRIVEN));
    EXPECT_TRUE(_tablet->add_rowset_warmup_state(*(rowset2->rowset_meta()),
                                                 WarmUpTriggerSource::SYNC_ROWSET));

    // Interleaved completion operations
    WarmUpState result = _tablet->complete_rowset_segment_warmup(
            WarmUpTriggerSource::EVENT_DRIVEN, rowset1->rowset_id(), Status::OK(), 1, 0);
    WarmUpState expected_state =
            WarmUpState {WarmUpTriggerSource::EVENT_DRIVEN, WarmUpProgress::DOING};
    EXPECT_EQ(result, expected_state);
    result = _tablet->complete_rowset_segment_warmup(WarmUpTriggerSource::SYNC_ROWSET,
                                                     rowset2->rowset_id(), Status::OK(), 1, 0);
    expected_state = WarmUpState {WarmUpTriggerSource::SYNC_ROWSET, WarmUpProgress::DOING};
    EXPECT_EQ(result, expected_state);
    result = _tablet->complete_rowset_segment_warmup(WarmUpTriggerSource::EVENT_DRIVEN,
                                                     rowset1->rowset_id(), Status::OK(), 1, 0);
    expected_state = WarmUpState {WarmUpTriggerSource::EVENT_DRIVEN, WarmUpProgress::DOING};
    EXPECT_EQ(result, expected_state);

    // Check states are maintained correctly
    expected_state = WarmUpState {WarmUpTriggerSource::EVENT_DRIVEN, WarmUpProgress::DOING};
    EXPECT_EQ(_tablet->get_rowset_warmup_state(rowset1->rowset_id()), expected_state);
    expected_state = WarmUpState {WarmUpTriggerSource::SYNC_ROWSET, WarmUpProgress::DOING};
    EXPECT_EQ(_tablet->get_rowset_warmup_state(rowset2->rowset_id()), expected_state);
}

// Test only when the trigger source matches can the state be updated
TEST_F(CloudTabletWarmUpStateTest, TestCompleteRowsetSegmentWarmupTriggerSource) {
    auto rowset = create_rowset(Version(13, 13), 1);
    ASSERT_NE(rowset, nullptr);
    // Add rowset warmup state
    bool add_result =
            _tablet->add_rowset_warmup_state(*(rowset->rowset_meta()), WarmUpTriggerSource::JOB);
    EXPECT_TRUE(add_result);

    // Attempt to update inverted index num with a different trigger source, should fail
    bool update_result = _tablet->update_rowset_warmup_state_inverted_idx_num(
            WarmUpTriggerSource::SYNC_ROWSET, rowset->rowset_id(), 1);
    EXPECT_FALSE(update_result);
    update_result = _tablet->update_rowset_warmup_state_inverted_idx_num(
            WarmUpTriggerSource::EVENT_DRIVEN, rowset->rowset_id(), 1);
    EXPECT_FALSE(update_result);

    // Attempt to complete with a different trigger source, should not update state
    WarmUpState result = _tablet->complete_rowset_segment_warmup(
            WarmUpTriggerSource::SYNC_ROWSET, rowset->rowset_id(), Status::OK(), 1, 0);
    WarmUpState expected_state = WarmUpState {WarmUpTriggerSource::JOB, WarmUpProgress::DOING};
    EXPECT_EQ(result, expected_state);
    result = _tablet->complete_rowset_segment_warmup(WarmUpTriggerSource::EVENT_DRIVEN,
                                                     rowset->rowset_id(), Status::OK(), 1, 0);
    expected_state = WarmUpState {WarmUpTriggerSource::JOB, WarmUpProgress::DOING};
    EXPECT_EQ(result, expected_state);

    // Now complete with the correct trigger source
    result = _tablet->complete_rowset_segment_warmup(WarmUpTriggerSource::JOB, rowset->rowset_id(),
                                                     Status::OK(), 1, 0);
    expected_state = WarmUpState {WarmUpTriggerSource::JOB, WarmUpProgress::DONE};
    EXPECT_EQ(result, expected_state);
}

// Test JOB trigger source can override non-JOB warmup states
TEST_F(CloudTabletWarmUpStateTest, TestJobTriggerOverridesNonJobStates) {
    auto rowset = create_rowset(Version(14, 14), 2);
    ASSERT_NE(rowset, nullptr);

    // First, add EVENT_DRIVEN warmup state
    bool result1 = _tablet->add_rowset_warmup_state(*(rowset->rowset_meta()),
                                                    WarmUpTriggerSource::EVENT_DRIVEN);
    EXPECT_TRUE(result1);

    // Verify EVENT_DRIVEN state is set
    WarmUpState state = _tablet->get_rowset_warmup_state(rowset->rowset_id());
    WarmUpState expected_state =
            WarmUpState {WarmUpTriggerSource::EVENT_DRIVEN, WarmUpProgress::DOING};
    EXPECT_EQ(state, expected_state);

    // Now add JOB warmup state, should override EVENT_DRIVEN
    bool result2 =
            _tablet->add_rowset_warmup_state(*(rowset->rowset_meta()), WarmUpTriggerSource::JOB);
    EXPECT_TRUE(result2);

    // Verify JOB state has overridden EVENT_DRIVEN
    state = _tablet->get_rowset_warmup_state(rowset->rowset_id());
    expected_state = WarmUpState {WarmUpTriggerSource::JOB, WarmUpProgress::DOING};
    EXPECT_EQ(state, expected_state);
}

// Test JOB trigger source can override SYNC_ROWSET warmup state
TEST_F(CloudTabletWarmUpStateTest, TestJobTriggerOverridesSyncRowsetState) {
    auto rowset = create_rowset(Version(15, 15), 3);
    ASSERT_NE(rowset, nullptr);

    // First, add SYNC_ROWSET warmup state
    bool result1 = _tablet->add_rowset_warmup_state(*(rowset->rowset_meta()),
                                                    WarmUpTriggerSource::SYNC_ROWSET);
    EXPECT_TRUE(result1);

    // Verify SYNC_ROWSET state is set
    WarmUpState state = _tablet->get_rowset_warmup_state(rowset->rowset_id());
    WarmUpState expected_state =
            WarmUpState {WarmUpTriggerSource::SYNC_ROWSET, WarmUpProgress::DOING};
    EXPECT_EQ(state, expected_state);

    // Now add JOB warmup state, should override SYNC_ROWSET
    bool result2 =
            _tablet->add_rowset_warmup_state(*(rowset->rowset_meta()), WarmUpTriggerSource::JOB);
    EXPECT_TRUE(result2);

    // Verify JOB state has overridden SYNC_ROWSET
    state = _tablet->get_rowset_warmup_state(rowset->rowset_id());
    expected_state = WarmUpState {WarmUpTriggerSource::JOB, WarmUpProgress::DOING};
    EXPECT_EQ(state, expected_state);
}

// Test JOB trigger source cannot override another JOB warmup state
TEST_F(CloudTabletWarmUpStateTest, TestJobTriggerCannotOverrideAnotherJob) {
    auto rowset = create_rowset(Version(16, 16), 2);
    ASSERT_NE(rowset, nullptr);

    // First, add JOB warmup state
    bool result1 =
            _tablet->add_rowset_warmup_state(*(rowset->rowset_meta()), WarmUpTriggerSource::JOB);
    EXPECT_TRUE(result1);

    // Verify first JOB state is set
    WarmUpState state = _tablet->get_rowset_warmup_state(rowset->rowset_id());
    WarmUpState expected_state = WarmUpState {WarmUpTriggerSource::JOB, WarmUpProgress::DOING};
    EXPECT_EQ(state, expected_state);

    // Try to add another JOB warmup state, should fail
    bool result2 =
            _tablet->add_rowset_warmup_state(*(rowset->rowset_meta()), WarmUpTriggerSource::JOB);
    EXPECT_FALSE(result2);

    // Verify state remains the original JOB state
    state = _tablet->get_rowset_warmup_state(rowset->rowset_id());
    expected_state = WarmUpState {WarmUpTriggerSource::JOB, WarmUpProgress::DOING};
    EXPECT_EQ(state, expected_state);
}

// Test EVENT_DRIVEN cannot override existing JOB state
TEST_F(CloudTabletWarmUpStateTest, TestEventDrivenCannotOverrideJobState) {
    auto rowset = create_rowset(Version(17, 17), 1);
    ASSERT_NE(rowset, nullptr);

    // First, add JOB warmup state
    bool result1 =
            _tablet->add_rowset_warmup_state(*(rowset->rowset_meta()), WarmUpTriggerSource::JOB);
    EXPECT_TRUE(result1);

    // Verify JOB state is set
    WarmUpState state = _tablet->get_rowset_warmup_state(rowset->rowset_id());
    WarmUpState expected_state = WarmUpState {WarmUpTriggerSource::JOB, WarmUpProgress::DOING};
    EXPECT_EQ(state, expected_state);

    // Try to add EVENT_DRIVEN warmup state, should fail
    bool result2 = _tablet->add_rowset_warmup_state(*(rowset->rowset_meta()),
                                                    WarmUpTriggerSource::EVENT_DRIVEN);
    EXPECT_FALSE(result2);

    // Verify state remains JOB
    state = _tablet->get_rowset_warmup_state(rowset->rowset_id());
    expected_state = WarmUpState {WarmUpTriggerSource::JOB, WarmUpProgress::DOING};
    EXPECT_EQ(state, expected_state);
}

// Test SYNC_ROWSET cannot override existing JOB state
TEST_F(CloudTabletWarmUpStateTest, TestSyncRowsetCannotOverrideJobState) {
    auto rowset = create_rowset(Version(18, 18), 2);
    ASSERT_NE(rowset, nullptr);

    // First, add JOB warmup state
    bool result1 =
            _tablet->add_rowset_warmup_state(*(rowset->rowset_meta()), WarmUpTriggerSource::JOB);
    EXPECT_TRUE(result1);

    // Verify JOB state is set
    WarmUpState state = _tablet->get_rowset_warmup_state(rowset->rowset_id());
    WarmUpState expected_state = WarmUpState {WarmUpTriggerSource::JOB, WarmUpProgress::DOING};
    EXPECT_EQ(state, expected_state);

    // Try to add SYNC_ROWSET warmup state, should fail
    bool result2 = _tablet->add_rowset_warmup_state(*(rowset->rowset_meta()),
                                                    WarmUpTriggerSource::SYNC_ROWSET);
    EXPECT_FALSE(result2);

    // Verify state remains JOB
    state = _tablet->get_rowset_warmup_state(rowset->rowset_id());
    expected_state = WarmUpState {WarmUpTriggerSource::JOB, WarmUpProgress::DOING};
    EXPECT_EQ(state, expected_state);
}

// Test EVENT_DRIVEN cannot override existing SYNC_ROWSET state
TEST_F(CloudTabletWarmUpStateTest, TestEventDrivenCannotOverrideSyncRowsetState) {
    auto rowset = create_rowset(Version(19, 19), 1);
    ASSERT_NE(rowset, nullptr);

    // First, add SYNC_ROWSET warmup state
    bool result1 = _tablet->add_rowset_warmup_state(*(rowset->rowset_meta()),
                                                    WarmUpTriggerSource::SYNC_ROWSET);
    EXPECT_TRUE(result1);

    // Verify SYNC_ROWSET state is set
    WarmUpState state = _tablet->get_rowset_warmup_state(rowset->rowset_id());
    WarmUpState expected_state =
            WarmUpState {WarmUpTriggerSource::SYNC_ROWSET, WarmUpProgress::DOING};
    EXPECT_EQ(state, expected_state);

    // Try to add EVENT_DRIVEN warmup state, should fail
    bool result2 = _tablet->add_rowset_warmup_state(*(rowset->rowset_meta()),
                                                    WarmUpTriggerSource::EVENT_DRIVEN);
    EXPECT_FALSE(result2);

    // Verify state remains SYNC_ROWSET
    state = _tablet->get_rowset_warmup_state(rowset->rowset_id());
    expected_state = WarmUpState {WarmUpTriggerSource::SYNC_ROWSET, WarmUpProgress::DOING};
    EXPECT_EQ(state, expected_state);
}

// Test SYNC_ROWSET cannot override existing EVENT_DRIVEN state
TEST_F(CloudTabletWarmUpStateTest, TestSyncRowsetCannotOverrideEventDrivenState) {
    auto rowset = create_rowset(Version(20, 20), 3);
    ASSERT_NE(rowset, nullptr);

    // First, add EVENT_DRIVEN warmup state
    bool result1 = _tablet->add_rowset_warmup_state(*(rowset->rowset_meta()),
                                                    WarmUpTriggerSource::EVENT_DRIVEN);
    EXPECT_TRUE(result1);

    // Verify EVENT_DRIVEN state is set
    WarmUpState state = _tablet->get_rowset_warmup_state(rowset->rowset_id());
    WarmUpState expected_state =
            WarmUpState {WarmUpTriggerSource::EVENT_DRIVEN, WarmUpProgress::DOING};
    EXPECT_EQ(state, expected_state);

    // Try to add SYNC_ROWSET warmup state, should fail
    bool result2 = _tablet->add_rowset_warmup_state(*(rowset->rowset_meta()),
                                                    WarmUpTriggerSource::SYNC_ROWSET);
    EXPECT_FALSE(result2);

    // Verify state remains EVENT_DRIVEN
    state = _tablet->get_rowset_warmup_state(rowset->rowset_id());
    expected_state = WarmUpState {WarmUpTriggerSource::EVENT_DRIVEN, WarmUpProgress::DOING};
    EXPECT_EQ(state, expected_state);
}

// Test JOB can override DONE state from non-JOB source
TEST_F(CloudTabletWarmUpStateTest, TestJobCanOverrideDoneStateFromNonJob) {
    auto rowset = create_rowset(Version(21, 21), 1);
    ASSERT_NE(rowset, nullptr);

    // First, add EVENT_DRIVEN warmup state
    bool result1 = _tablet->add_rowset_warmup_state(*(rowset->rowset_meta()),
                                                    WarmUpTriggerSource::EVENT_DRIVEN);
    EXPECT_TRUE(result1);

    // Complete the warmup to DONE state
    WarmUpState result = _tablet->complete_rowset_segment_warmup(
            WarmUpTriggerSource::EVENT_DRIVEN, rowset->rowset_id(), Status::OK(), 1, 0);
    WarmUpState expected_state =
            WarmUpState {WarmUpTriggerSource::EVENT_DRIVEN, WarmUpProgress::DONE};
    EXPECT_EQ(result, expected_state);

    // Now add JOB warmup state, should override the DONE state
    bool result2 =
            _tablet->add_rowset_warmup_state(*(rowset->rowset_meta()), WarmUpTriggerSource::JOB);
    EXPECT_TRUE(result2);

    // Verify JOB state has overridden the DONE state
    WarmUpState state = _tablet->get_rowset_warmup_state(rowset->rowset_id());
    expected_state = WarmUpState {WarmUpTriggerSource::JOB, WarmUpProgress::DOING};
    EXPECT_EQ(state, expected_state);
}

// Test JOB can override DONE state from JOB source
TEST_F(CloudTabletWarmUpStateTest, TestJobCanOverrideDoneStateFromJob) {
    auto rowset = create_rowset(Version(21, 21), 1);
    ASSERT_NE(rowset, nullptr);

    // First, add EVENT_DRIVEN warmup state
    bool result1 =
            _tablet->add_rowset_warmup_state(*(rowset->rowset_meta()), WarmUpTriggerSource::JOB);
    EXPECT_TRUE(result1);

    // Complete the warmup to DONE state
    WarmUpState result = _tablet->complete_rowset_segment_warmup(
            WarmUpTriggerSource::JOB, rowset->rowset_id(), Status::OK(), 1, 0);
    WarmUpState expected_state = WarmUpState {WarmUpTriggerSource::JOB, WarmUpProgress::DONE};
    EXPECT_EQ(result, expected_state);

    // Now add JOB warmup state, should override the DONE state
    bool result2 =
            _tablet->add_rowset_warmup_state(*(rowset->rowset_meta()), WarmUpTriggerSource::JOB);
    EXPECT_TRUE(result2);

    // Verify JOB state has overridden the DONE state
    WarmUpState state = _tablet->get_rowset_warmup_state(rowset->rowset_id());
    expected_state = WarmUpState {WarmUpTriggerSource::JOB, WarmUpProgress::DOING};
    EXPECT_EQ(state, expected_state);
}

// Test class for sync_meta functionality
class CloudTabletSyncMetaTest : public testing::Test {
public:
    CloudTabletSyncMetaTest() : _engine(CloudStorageEngine(EngineOptions {})) {}

    void SetUp() override {
        config::enable_file_cache = true;

        // Use incrementing tablet_id to create unique schema cache keys for each test
        // This avoids cache pollution between tests
        int64_t unique_tablet_id = 15673 + _test_counter++;

        // Create tablet meta with a schema that has disable_auto_compaction = false
        TTabletSchema tablet_schema;
        tablet_schema.__set_disable_auto_compaction(false);
        // Add a unique column to ensure unique cache key
        TColumn col;
        col.__set_column_name("test_col_" + std::to_string(unique_tablet_id));
        col.__set_column_type(TColumnType());
        col.column_type.__set_type(TPrimitiveType::INT);
        col.__set_is_key(true);
        col.__set_aggregation_type(TAggregationType::NONE);
        col.__set_col_unique_id(0);
        tablet_schema.__set_columns({col});
        tablet_schema.__set_keys_type(TKeysType::DUP_KEYS);

        // Use column ordinal 0 -> unique_id 0 mapping
        _tablet_meta.reset(new TabletMeta(1, 2, unique_tablet_id, 15674, 4, 5, tablet_schema, 1,
                                          {{0, 0}}, UniqueId(9, 10), TTabletType::TABLET_TYPE_DISK,
                                          TCompressionType::LZ4F));

        _tablet =
                std::make_shared<CloudTablet>(_engine, std::make_shared<TabletMeta>(*_tablet_meta));

        _current_tablet_id = unique_tablet_id;
    }

    void TearDown() override { config::enable_file_cache = false; }

protected:
    // Helper to create a unique TabletMeta for mock responses
    TabletMetaSharedPtr createMockTabletMeta(bool disable_auto_compaction,
                                             const std::string& compaction_policy = "size_based") {
        TTabletSchema new_schema;
        new_schema.__set_disable_auto_compaction(disable_auto_compaction);
        TColumn col;
        col.__set_column_name("test_col_" + std::to_string(_current_tablet_id));
        col.__set_column_type(TColumnType());
        col.column_type.__set_type(TPrimitiveType::INT);
        col.__set_is_key(true);
        col.__set_aggregation_type(TAggregationType::NONE);
        col.__set_col_unique_id(0);
        new_schema.__set_columns({col});
        new_schema.__set_keys_type(TKeysType::DUP_KEYS);

        TabletMetaSharedPtr meta;
        meta.reset(new TabletMeta(1, 2, _current_tablet_id, 15674, 4, 5, new_schema, 1, {{0, 0}},
                                  UniqueId(9, 10), TTabletType::TABLET_TYPE_DISK,
                                  TCompressionType::LZ4F, 0, false, std::nullopt,
                                  compaction_policy));
        return meta;
    }

    TabletMetaSharedPtr _tablet_meta;
    std::shared_ptr<CloudTablet> _tablet;
    CloudStorageEngine _engine;
    int64_t _current_tablet_id;
    static inline int _test_counter = 0;
};

// Test sync_meta syncs disable_auto_compaction from false to true
TEST_F(CloudTabletSyncMetaTest, TestSyncMetaDisableAutoCompactionFalseToTrue) {
    // Verify initial state: disable_auto_compaction = false
    EXPECT_FALSE(_tablet->tablet_meta()->tablet_schema()->disable_auto_compaction());

    auto sp = SyncPoint::get_instance();
    sp->clear_all_call_backs();
    sp->enable_processing();

    // Create mock tablet meta with disable_auto_compaction = true
    auto mock_tablet_meta = createMockTabletMeta(true);

    // Mock get_tablet_meta to return tablet_meta with disable_auto_compaction = true
    sp->set_call_back("CloudMetaMgr::get_tablet_meta", [mock_tablet_meta](auto&& args) {
        auto* tablet_meta_ptr = try_any_cast<TabletMetaSharedPtr*>(args[1]);
        *tablet_meta_ptr = mock_tablet_meta;

        // Tell the sync point to return with Status::OK()
        try_any_cast_ret<Status>(args)->second = true;
    });

    // Call sync_meta
    Status st = _tablet->sync_meta();
    EXPECT_TRUE(st.ok());

    // Verify disable_auto_compaction has been synced to true
    EXPECT_TRUE(_tablet->tablet_meta()->tablet_schema()->disable_auto_compaction());

    sp->disable_processing();
    sp->clear_all_call_backs();
}

// Test sync_meta syncs disable_auto_compaction from true to false
TEST_F(CloudTabletSyncMetaTest, TestSyncMetaDisableAutoCompactionTrueToFalse) {
    // Create a tablet with disable_auto_compaction = true from the start
    // We need to create a fresh tablet with true to avoid cache pollution issues
    TTabletSchema initial_schema;
    initial_schema.__set_disable_auto_compaction(true);
    TColumn col;
    col.__set_column_name("test_col_true_to_false_" + std::to_string(_current_tablet_id));
    col.__set_column_type(TColumnType());
    col.column_type.__set_type(TPrimitiveType::INT);
    col.__set_is_key(true);
    col.__set_aggregation_type(TAggregationType::NONE);
    col.__set_col_unique_id(0);
    initial_schema.__set_columns({col});
    initial_schema.__set_keys_type(TKeysType::DUP_KEYS);

    TabletMetaSharedPtr tablet_meta_true;
    tablet_meta_true.reset(new TabletMeta(1, 2, _current_tablet_id + 1000, 15674, 4, 5,
                                          initial_schema, 1, {{0, 0}}, UniqueId(9, 10),
                                          TTabletType::TABLET_TYPE_DISK, TCompressionType::LZ4F));
    _tablet =
            std::make_shared<CloudTablet>(_engine, std::make_shared<TabletMeta>(*tablet_meta_true));

    // Verify initial state: disable_auto_compaction = true
    EXPECT_TRUE(_tablet->tablet_meta()->tablet_schema()->disable_auto_compaction());

    auto sp = SyncPoint::get_instance();
    sp->clear_all_call_backs();
    sp->enable_processing();

    // Create mock tablet meta with disable_auto_compaction = false (with matching column name)
    TTabletSchema mock_schema;
    mock_schema.__set_disable_auto_compaction(false);
    TColumn mock_col;
    mock_col.__set_column_name("test_col_true_to_false_" + std::to_string(_current_tablet_id));
    mock_col.__set_column_type(TColumnType());
    mock_col.column_type.__set_type(TPrimitiveType::INT);
    mock_col.__set_is_key(true);
    mock_col.__set_aggregation_type(TAggregationType::NONE);
    mock_col.__set_col_unique_id(0);
    mock_schema.__set_columns({mock_col});
    mock_schema.__set_keys_type(TKeysType::DUP_KEYS);

    TabletMetaSharedPtr mock_tablet_meta;
    mock_tablet_meta.reset(new TabletMeta(1, 2, _current_tablet_id + 1000, 15674, 4, 5, mock_schema,
                                          1, {{0, 0}}, UniqueId(9, 10),
                                          TTabletType::TABLET_TYPE_DISK, TCompressionType::LZ4F));

    // Mock get_tablet_meta to return tablet_meta with disable_auto_compaction = false
    sp->set_call_back("CloudMetaMgr::get_tablet_meta", [mock_tablet_meta](auto&& args) {
        auto* tablet_meta_ptr = try_any_cast<TabletMetaSharedPtr*>(args[1]);
        *tablet_meta_ptr = mock_tablet_meta;

        // Tell the sync point to return with Status::OK()
        try_any_cast_ret<Status>(args)->second = true;
    });

    // Call sync_meta
    Status st = _tablet->sync_meta();
    EXPECT_TRUE(st.ok());

    // Verify disable_auto_compaction has been synced to false
    EXPECT_FALSE(_tablet->tablet_meta()->tablet_schema()->disable_auto_compaction());

    sp->disable_processing();
    sp->clear_all_call_backs();
}

// Test sync_meta when disable_auto_compaction is unchanged
TEST_F(CloudTabletSyncMetaTest, TestSyncMetaDisableAutoCompactionUnchanged) {
    // Verify initial state: disable_auto_compaction = false
    EXPECT_FALSE(_tablet->tablet_meta()->tablet_schema()->disable_auto_compaction());

    auto sp = SyncPoint::get_instance();
    sp->clear_all_call_backs();
    sp->enable_processing();

    // Create mock tablet meta with disable_auto_compaction = false (same as current)
    auto mock_tablet_meta = createMockTabletMeta(false);

    // Mock get_tablet_meta to return tablet_meta with same disable_auto_compaction = false
    sp->set_call_back("CloudMetaMgr::get_tablet_meta", [mock_tablet_meta](auto&& args) {
        auto* tablet_meta_ptr = try_any_cast<TabletMetaSharedPtr*>(args[1]);
        *tablet_meta_ptr = mock_tablet_meta;

        // Tell the sync point to return with Status::OK()
        try_any_cast_ret<Status>(args)->second = true;
    });

    // Call sync_meta
    Status st = _tablet->sync_meta();
    EXPECT_TRUE(st.ok());

    // Verify disable_auto_compaction remains false
    EXPECT_FALSE(_tablet->tablet_meta()->tablet_schema()->disable_auto_compaction());

    sp->disable_processing();
    sp->clear_all_call_backs();
}

// Test sync_meta is skipped when enable_file_cache is false
TEST_F(CloudTabletSyncMetaTest, TestSyncMetaSkippedWhenFileCacheDisabled) {
    // Disable file cache
    config::enable_file_cache = false;

    // Set initial state: disable_auto_compaction = false
    EXPECT_FALSE(_tablet->tablet_meta()->tablet_schema()->disable_auto_compaction());

    auto sp = SyncPoint::get_instance();
    sp->clear_all_call_backs();
    sp->enable_processing();

    bool callback_called = false;
    sp->set_call_back("CloudMetaMgr::get_tablet_meta",
                      [&callback_called](auto&& args) { callback_called = true; });

    // Call sync_meta - should return early without calling get_tablet_meta
    Status st = _tablet->sync_meta();
    EXPECT_TRUE(st.ok());
    EXPECT_FALSE(callback_called);

    // Verify disable_auto_compaction is not changed
    EXPECT_FALSE(_tablet->tablet_meta()->tablet_schema()->disable_auto_compaction());

    sp->disable_processing();
    sp->clear_all_call_backs();
}

// Test sync_meta syncs compaction_policy together with disable_auto_compaction
TEST_F(CloudTabletSyncMetaTest, TestSyncMetaMultipleProperties) {
    // Verify initial states
    EXPECT_FALSE(_tablet->tablet_meta()->tablet_schema()->disable_auto_compaction());
    // Default compaction_policy is "size_based"
    EXPECT_EQ(_tablet->tablet_meta()->compaction_policy(), "size_based");

    auto sp = SyncPoint::get_instance();
    sp->clear_all_call_backs();
    sp->enable_processing();

    // Create mock tablet meta with updated properties
    auto mock_tablet_meta = createMockTabletMeta(true, "time_series");

    // Mock get_tablet_meta to return tablet_meta with updated properties
    sp->set_call_back("CloudMetaMgr::get_tablet_meta", [mock_tablet_meta](auto&& args) {
        auto* tablet_meta_ptr = try_any_cast<TabletMetaSharedPtr*>(args[1]);
        *tablet_meta_ptr = mock_tablet_meta;

        // Tell the sync point to return with Status::OK()
        try_any_cast_ret<Status>(args)->second = true;
    });

    // Call sync_meta
    Status st = _tablet->sync_meta();
    EXPECT_TRUE(st.ok());

    // Verify both properties are synced
    EXPECT_TRUE(_tablet->tablet_meta()->tablet_schema()->disable_auto_compaction());
    EXPECT_EQ(_tablet->tablet_meta()->compaction_policy(), "time_series");

    sp->disable_processing();
    sp->clear_all_call_backs();
}
} // namespace doris
