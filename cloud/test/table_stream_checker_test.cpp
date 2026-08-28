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

#include <gen_cpp/cloud.pb.h>
#include <gtest/gtest.h>

#include <array>

#include "cpp/sync_point.h"
#include "meta-store/blob_message.h"
#include "meta-store/keys.h"
#include "meta-store/mem_txn_kv.h"
#include "meta-store/versioned_value.h"
#include "recycler/checker.h"

namespace doris::cloud {

class TableStreamCheckerTest : public testing::Test {
protected:
    void SetUp() override {
        txn_kv_ = std::make_shared<MemTxnKv>();
        ASSERT_EQ(txn_kv_->init(), 0);
    }

    InstanceInfoPB write_only_instance(std::string_view instance_id) {
        return make_instance(instance_id, MULTI_VERSION_WRITE_ONLY);
    }

    InstanceInfoPB make_instance(std::string_view instance_id, MultiVersionStatus status) {
        InstanceInfoPB instance;
        instance.set_instance_id(std::string(instance_id));
        instance.set_multi_version_status(status);
        return instance;
    }

    TableStreamOffsetPB offset(int64_t partition_id, int64_t tso) {
        TableStreamOffsetPB value;
        value.set_partition_id(partition_id);
        value.set_state(TABLE_STREAM_OFFSET_CONSUMED);
        value.set_offset_tso(tso);
        value.set_last_consumption_time_ms(1000);
        return value;
    }

    void put_latest_offset(Transaction* txn, std::string_view instance_id, int64_t base_db_id,
                           int64_t base_table_id, int64_t stream_db_id, int64_t stream_id,
                           int64_t partition_id, const TableStreamOffsetPB& value) {
        txn->put(table_stream_offset_key({instance_id, base_db_id, base_table_id, stream_db_id,
                                          stream_id, partition_id}),
                 value.SerializeAsString());
    }

    void put_versioned_offset(Transaction* txn, std::string_view instance_id, int64_t base_db_id,
                              int64_t base_table_id, int64_t stream_db_id, int64_t stream_id,
                              int64_t partition_id, const TableStreamOffsetPB& value) {
        versioned_put(txn,
                      versioned::table_stream_offset_key({instance_id, base_db_id, base_table_id,
                                                          stream_db_id, stream_id, partition_id}),
                      Versionstamp(10, 1), value.SerializeAsString());
    }

    void put_offsets(Transaction* txn, std::string_view instance_id, int64_t base_db_id,
                     int64_t base_table_id, int64_t stream_db_id, int64_t stream_id,
                     int64_t partition_id, const TableStreamOffsetPB& latest,
                     const TableStreamOffsetPB& versioned) {
        put_latest_offset(txn, instance_id, base_db_id, base_table_id, stream_db_id, stream_id,
                          partition_id, latest);
        put_versioned_offset(txn, instance_id, base_db_id, base_table_id, stream_db_id, stream_id,
                             partition_id, versioned);
    }

    std::shared_ptr<MemTxnKv> txn_kv_;
};

TEST_F(TableStreamCheckerTest, ConsistentOffsets) {
    const std::string instance_id = "checker-consistent";
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
    auto value = offset(5, 100);
    put_offsets(txn.get(), instance_id, 1, 2, 3, 4, 5, value, value);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    InstanceChecker checker(txn_kv_, instance_id);
    ASSERT_EQ(checker.init(write_only_instance(instance_id)), 0);
    EXPECT_EQ(checker.do_table_stream_check(), 0);
}

TEST_F(TableStreamCheckerTest, DetectsLatestVersionedMismatch) {
    const std::string instance_id = "checker-offset-mismatch";
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
    put_offsets(txn.get(), instance_id, 1, 2, 3, 4, 5, offset(5, 100), offset(5, 99));
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    InstanceChecker checker(txn_kv_, instance_id);
    ASSERT_EQ(checker.init(write_only_instance(instance_id)), 0);
    EXPECT_EQ(checker.do_table_stream_check(), 1);
}

TEST_F(TableStreamCheckerTest, ProjectionValidationFollowsMultiVersionStatus) {
    enum class ProjectionShape { LATEST_ONLY, VERSIONED_ONLY, MISMATCHED, CONSISTENT };
    struct TestCase {
        const char* name;
        MultiVersionStatus status;
        ProjectionShape shape;
        int expected;
    };
    const std::array<TestCase, 12> test_cases {{
            {.name = "disabled_latest_only",
             .status = MULTI_VERSION_DISABLED,
             .shape = ProjectionShape::LATEST_ONLY,
             .expected = 0},
            {.name = "disabled_versioned_residual",
             .status = MULTI_VERSION_DISABLED,
             .shape = ProjectionShape::VERSIONED_ONLY,
             .expected = 0},
            {.name = "disabled_mismatched_residual",
             .status = MULTI_VERSION_DISABLED,
             .shape = ProjectionShape::MISMATCHED,
             .expected = 0},
            {.name = "disabled_consistent",
             .status = MULTI_VERSION_DISABLED,
             .shape = ProjectionShape::CONSISTENT,
             .expected = 0},
            {.name = "write_only_latest_only",
             .status = MULTI_VERSION_WRITE_ONLY,
             .shape = ProjectionShape::LATEST_ONLY,
             .expected = 1},
            {.name = "write_only_versioned_only",
             .status = MULTI_VERSION_WRITE_ONLY,
             .shape = ProjectionShape::VERSIONED_ONLY,
             .expected = 1},
            {.name = "write_only_mismatched",
             .status = MULTI_VERSION_WRITE_ONLY,
             .shape = ProjectionShape::MISMATCHED,
             .expected = 1},
            {.name = "write_only_consistent",
             .status = MULTI_VERSION_WRITE_ONLY,
             .shape = ProjectionShape::CONSISTENT,
             .expected = 0},
            {.name = "read_write_latest_only",
             .status = MULTI_VERSION_READ_WRITE,
             .shape = ProjectionShape::LATEST_ONLY,
             .expected = 1},
            {.name = "read_write_versioned_only",
             .status = MULTI_VERSION_READ_WRITE,
             .shape = ProjectionShape::VERSIONED_ONLY,
             .expected = 1},
            {.name = "read_write_mismatched",
             .status = MULTI_VERSION_READ_WRITE,
             .shape = ProjectionShape::MISMATCHED,
             .expected = 1},
            {.name = "read_write_consistent",
             .status = MULTI_VERSION_READ_WRITE,
             .shape = ProjectionShape::CONSISTENT,
             .expected = 0},
    }};

    for (const TestCase& test_case : test_cases) {
        SCOPED_TRACE(test_case.name);
        const std::string instance_id = "checker-mode-" + std::string(test_case.name);
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
        const TableStreamOffsetPB latest = offset(5, 100);
        const TableStreamOffsetPB versioned =
                offset(5, test_case.shape == ProjectionShape::MISMATCHED ? 99 : 100);
        if (test_case.shape != ProjectionShape::VERSIONED_ONLY) {
            put_latest_offset(txn.get(), instance_id, 1, 2, 3, 4, 5, latest);
        }
        if (test_case.shape != ProjectionShape::LATEST_ONLY) {
            put_versioned_offset(txn.get(), instance_id, 1, 2, 3, 4, 5, versioned);
        }
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        InstanceChecker checker(txn_kv_, instance_id);
        ASSERT_EQ(checker.init(make_instance(instance_id, test_case.status)), 0);
        EXPECT_EQ(checker.do_table_stream_check(), test_case.expected);
    }
}

TEST_F(TableStreamCheckerTest, RecycleIndexMustMatchStreamAndLifecycleState) {
    constexpr int64_t base_db_id = 1;
    constexpr int64_t base_table_id = 2;
    constexpr int64_t stream_db_id = 3;
    constexpr int64_t stream_id = 4;
    constexpr int64_t partition_id = 5;

    struct TestCase {
        const char* name;
        RecycleIndexPB::State state;
        IndexObjectTypePB object_type;
        int64_t recycle_db_id;
        bool set_table_id;
        int expected;
    };
    const std::array<TestCase, 6> test_cases {{
            {.name = "recycling",
             .state = RecycleIndexPB::RECYCLING,
             .object_type = TABLE_STREAM,
             .recycle_db_id = base_db_id,
             .set_table_id = true,
             .expected = 0},
            {.name = "prepared",
             .state = RecycleIndexPB::PREPARED,
             .object_type = TABLE_STREAM,
             .recycle_db_id = base_db_id,
             .set_table_id = true,
             .expected = 1},
            {.name = "dropped",
             .state = RecycleIndexPB::DROPPED,
             .object_type = TABLE_STREAM,
             .recycle_db_id = base_db_id,
             .set_table_id = true,
             .expected = 1},
            {.name = "unknown",
             .state = RecycleIndexPB::UNKNOWN,
             .object_type = TABLE_STREAM,
             .recycle_db_id = base_db_id,
             .set_table_id = true,
             .expected = 1},
            {.name = "physical_index",
             .state = RecycleIndexPB::RECYCLING,
             .object_type = MATERIALIZED_INDEX,
             .recycle_db_id = base_db_id,
             .set_table_id = true,
             .expected = 1},
            {.name = "missing_table_id",
             .state = RecycleIndexPB::RECYCLING,
             .object_type = TABLE_STREAM,
             .recycle_db_id = base_db_id,
             .set_table_id = false,
             .expected = 1},
    }};

    for (const TestCase& test_case : test_cases) {
        SCOPED_TRACE(test_case.name);
        const std::string instance_id = "checker-recycle-index-" + std::string(test_case.name);
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
        const auto value = offset(partition_id, 100);
        if (test_case.state == RecycleIndexPB::PREPARED ||
            test_case.state == RecycleIndexPB::DROPPED) {
            put_latest_offset(txn.get(), instance_id, base_db_id, base_table_id, stream_db_id,
                              stream_id, partition_id, value);
        } else {
            put_offsets(txn.get(), instance_id, base_db_id, base_table_id, stream_db_id, stream_id,
                        partition_id, value, value);
        }
        RecycleIndexPB recycle_index;
        recycle_index.set_db_id(test_case.recycle_db_id);
        if (test_case.set_table_id) {
            recycle_index.set_table_id(base_table_id);
        }
        recycle_index.set_stream_db_id(stream_db_id);
        recycle_index.set_object_type(test_case.object_type);
        recycle_index.set_state(test_case.state);
        txn->put(recycle_index_key({instance_id, stream_id}), recycle_index.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        InstanceChecker checker(txn_kv_, instance_id);
        ASSERT_EQ(checker.init(write_only_instance(instance_id)), 0);
        EXPECT_EQ(checker.do_table_stream_check(), test_case.expected);
    }
}

TEST_F(TableStreamCheckerTest, RejectsMalformedOrMismatchedRecycleIndex) {
    constexpr int64_t base_db_id = 1;
    constexpr int64_t base_table_id = 2;
    constexpr int64_t stream_db_id = 3;
    constexpr int64_t stream_id = 4;
    constexpr int64_t partition_id = 5;

    {
        const std::string instance_id = "checker-malformed-recycle-index";
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
        const auto matching = offset(partition_id, 100);
        put_offsets(txn.get(), instance_id, base_db_id, base_table_id, stream_db_id, stream_id,
                    partition_id, matching, matching);
        txn->put(recycle_index_key({instance_id, stream_id}), "malformed");
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        InstanceChecker checker(txn_kv_, instance_id);
        ASSERT_EQ(checker.init(write_only_instance(instance_id)), 0);
        EXPECT_EQ(checker.do_table_stream_check(), -1);
    }

    {
        const std::string instance_id = "checker-mismatched-recycle-index";
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
        put_latest_offset(txn.get(), instance_id, base_db_id, base_table_id, stream_db_id,
                          stream_id, partition_id, offset(partition_id, 100));
        const auto mismatched = offset(partition_id + 1, 100);
        put_offsets(txn.get(), instance_id, base_db_id + 1, base_table_id, stream_db_id, stream_id,
                    partition_id + 1, mismatched, mismatched);
        RecycleIndexPB recycle_index;
        recycle_index.set_db_id(base_db_id);
        recycle_index.set_table_id(base_table_id);
        recycle_index.set_stream_db_id(stream_db_id);
        recycle_index.set_object_type(TABLE_STREAM);
        recycle_index.set_state(RecycleIndexPB::RECYCLING);
        txn->put(recycle_index_key({instance_id, stream_id}), recycle_index.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        InstanceChecker checker(txn_kv_, instance_id);
        ASSERT_EQ(checker.init(write_only_instance(instance_id)), 0);
        EXPECT_EQ(checker.do_table_stream_check(), 1);
    }
}

TEST_F(TableStreamCheckerTest, RecheckAcceptsProjectionGapAfterRecyclingStarts) {
    const std::string instance_id = "checker-recycling-during-check";
    constexpr int64_t base_db_id = 1;
    constexpr int64_t base_table_id = 2;
    constexpr int64_t stream_db_id = 3;
    constexpr int64_t stream_id = 4;
    constexpr int64_t partition_id = 5;
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
    put_latest_offset(txn.get(), instance_id, base_db_id, base_table_id, stream_db_id, stream_id,
                      partition_id, offset(partition_id, 100));
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    auto* sync_point = SyncPoint::get_instance();
    SyncPoint::CallbackGuard guard;
    sync_point->set_call_back(
            "InstanceChecker::do_table_stream_check::after_latest_scan",
            [&](auto&&) {
                std::unique_ptr<Transaction> update_txn;
                ASSERT_EQ(txn_kv_->create_txn(&update_txn), TxnErrorCode::TXN_OK);
                RecycleIndexPB recycle_index;
                recycle_index.set_db_id(base_db_id);
                recycle_index.set_table_id(base_table_id);
                recycle_index.set_stream_db_id(stream_db_id);
                recycle_index.set_object_type(TABLE_STREAM);
                recycle_index.set_state(RecycleIndexPB::RECYCLING);
                update_txn->put(recycle_index_key({instance_id, stream_id}),
                                recycle_index.SerializeAsString());
                ASSERT_EQ(update_txn->commit(), TxnErrorCode::TXN_OK);
            },
            &guard);
    sync_point->enable_processing();

    InstanceChecker checker(txn_kv_, instance_id);
    ASSERT_EQ(checker.init(write_only_instance(instance_id)), 0);
    EXPECT_EQ(checker.do_table_stream_check(), 0);
    sync_point->disable_processing();
}

TEST_F(TableStreamCheckerTest, RechecksConcurrentProjectionChanges) {
    const std::string instance_id = "checker-concurrent-offset-update";
    constexpr int64_t base_db_id = 1;
    constexpr int64_t base_table_id = 2;
    constexpr int64_t stream_db_id = 3;
    constexpr int64_t stream_id = 4;
    constexpr int64_t partition_id = 5;
    constexpr int64_t created_stream_id = 6;
    constexpr int64_t created_partition_id = 7;
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
    auto initial = offset(partition_id, 100);
    put_offsets(txn.get(), instance_id, base_db_id, base_table_id, stream_db_id, stream_id,
                partition_id, initial, initial);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    bool updated = false;
    auto* sync_point = SyncPoint::get_instance();
    SyncPoint::CallbackGuard guard;
    sync_point->set_call_back(
            "InstanceChecker::do_table_stream_check::after_latest_scan",
            [&](auto&&) {
                std::unique_ptr<Transaction> update_txn;
                ASSERT_EQ(txn_kv_->create_txn(&update_txn), TxnErrorCode::TXN_OK);
                auto current = offset(partition_id, 120);
                update_txn->put(table_stream_offset_key({instance_id, base_db_id, base_table_id,
                                                         stream_db_id, stream_id, partition_id}),
                                current.SerializeAsString());
                versioned_put(
                        update_txn.get(),
                        versioned::table_stream_offset_key({instance_id, base_db_id, base_table_id,
                                                            stream_db_id, stream_id, partition_id}),
                        Versionstamp(20, 1), current.SerializeAsString());
                auto created = offset(created_partition_id, 130);
                update_txn->put(table_stream_offset_key({instance_id, base_db_id, base_table_id,
                                                         stream_db_id, created_stream_id,
                                                         created_partition_id}),
                                created.SerializeAsString());
                versioned_put(update_txn.get(),
                              versioned::table_stream_offset_key(
                                      {instance_id, base_db_id, base_table_id, stream_db_id,
                                       created_stream_id, created_partition_id}),
                              Versionstamp(20, 2), created.SerializeAsString());
                ASSERT_EQ(update_txn->commit(), TxnErrorCode::TXN_OK);
                updated = true;
            },
            &guard);
    sync_point->enable_processing();

    InstanceChecker checker(txn_kv_, instance_id);
    ASSERT_EQ(checker.init(write_only_instance(instance_id)), 0);
    EXPECT_EQ(checker.do_table_stream_check(), 0);
    EXPECT_TRUE(updated);
}

TEST_F(TableStreamCheckerTest, DetectsMalformedOffsetValue) {
    const std::string instance_id = "checker-malformed-offset";
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
    TableStreamOffsetPB value;
    value.set_partition_id(5);
    value.set_state(TABLE_STREAM_OFFSET_CONSUMED);
    put_offsets(txn.get(), instance_id, 1, 2, 3, 4, 5, value, value);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    InstanceChecker checker(txn_kv_, instance_id);
    ASSERT_EQ(checker.init(write_only_instance(instance_id)), 0);
    EXPECT_EQ(checker.do_table_stream_check(), 1);
}

TEST_F(TableStreamCheckerTest, CollectsPendingTableStreamDrop) {
    const std::string instance_id = "checker-pending-stream-drop";
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
    OperationLogPB operation_log;
    DropIndexLogPB* drop_index = operation_log.mutable_drop_index();
    drop_index->set_db_id(1);
    drop_index->set_table_id(2);
    drop_index->set_object_type(TABLE_STREAM);
    drop_index->set_stream_db_id(3);
    drop_index->add_index_ids(4);
    versioned::blob_put(txn.get(), versioned::log_key(instance_id), Versionstamp(10, 1),
                        operation_log);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    std::unordered_map<int64_t, PendingTableStreamDrop> pending_drops;
    ASSERT_EQ(collect_pending_table_stream_drops(txn_kv_, instance_id, &pending_drops),
              TxnErrorCode::TXN_OK);
    ASSERT_EQ(pending_drops.size(), 1);
    ASSERT_TRUE(pending_drops.contains(4));

    EXPECT_TRUE(pending_drops.at(4).matches(1, 2, 3));
    EXPECT_FALSE(pending_drops.at(4).matches(1, 2, 5));
}

TEST_F(TableStreamCheckerTest, DoesNotCollectPhysicalIndexDrop) {
    const std::string instance_id = "checker-pending-physical-index-drop";
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
    OperationLogPB operation_log;
    DropIndexLogPB* drop_index = operation_log.mutable_drop_index();
    drop_index->set_db_id(1);
    drop_index->set_table_id(2);
    drop_index->add_index_ids(4);
    versioned::blob_put(txn.get(), versioned::log_key(instance_id), Versionstamp(10, 1),
                        operation_log);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    std::unordered_map<int64_t, PendingTableStreamDrop> pending_drops;
    EXPECT_EQ(collect_pending_table_stream_drops(txn_kv_, instance_id, &pending_drops),
              TxnErrorCode::TXN_OK);
    EXPECT_TRUE(pending_drops.empty());
}

TEST_F(TableStreamCheckerTest, RejectsConflictingPendingTableStreamDrops) {
    const std::string instance_id = "checker-conflicting-stream-drops";
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
    for (int64_t stream_db_id : {3, 5}) {
        OperationLogPB operation_log;
        DropIndexLogPB* drop_index = operation_log.mutable_drop_index();
        drop_index->set_db_id(1);
        drop_index->set_table_id(2);
        drop_index->set_object_type(TABLE_STREAM);
        drop_index->set_stream_db_id(stream_db_id);
        drop_index->add_index_ids(4);
        versioned::blob_put(txn.get(), versioned::log_key(instance_id),
                            Versionstamp(10 + stream_db_id, 1), operation_log);
    }
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    std::unordered_map<int64_t, PendingTableStreamDrop> pending_drops;
    EXPECT_EQ(collect_pending_table_stream_drops(txn_kv_, instance_id, &pending_drops),
              TxnErrorCode::TXN_INVALID_DATA);
}

TEST_F(TableStreamCheckerTest, RejectsMalformedPendingOperationLog) {
    const std::string instance_id = "checker-malformed-pending-drop";
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
    versioned::blob_put(txn.get(), versioned::log_key(instance_id), Versionstamp(10, 1),
                        "malformed");
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    std::unordered_map<int64_t, PendingTableStreamDrop> pending_drops;
    EXPECT_EQ(collect_pending_table_stream_drops(txn_kv_, instance_id, &pending_drops),
              TxnErrorCode::TXN_INVALID_DATA);
}

} // namespace doris::cloud
