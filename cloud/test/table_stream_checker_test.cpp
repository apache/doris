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
        InstanceInfoPB instance;
        instance.set_instance_id(std::string(instance_id));
        instance.set_multi_version_status(MULTI_VERSION_WRITE_ONLY);
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

    void put_offsets(Transaction* txn, std::string_view instance_id, int64_t base_db_id,
                     int64_t base_table_id, int64_t stream_db_id, int64_t stream_id,
                     int64_t partition_id, const TableStreamOffsetPB& latest,
                     const TableStreamOffsetPB& versioned) {
        txn->put(table_stream_offset_key({instance_id, base_db_id, base_table_id, stream_db_id,
                                          stream_id, partition_id}),
                 latest.SerializeAsString());
        versioned_put(txn,
                      versioned::table_stream_offset_key({instance_id, base_db_id, base_table_id,
                                                          stream_db_id, stream_id, partition_id}),
                      Versionstamp(10, 1), versioned.SerializeAsString());
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
