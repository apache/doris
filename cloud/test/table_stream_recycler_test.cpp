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

#include <algorithm>
#include <memory>
#include <string>

#include "common/bvars.h"
#include "common/config.h"
#include "common/defer.h"
#include "meta-store/keys.h"
#include "meta-store/mem_txn_kv.h"
#include "meta-store/versioned_value.h"
#include "recycler/recycler.h"

namespace doris::cloud {
namespace {

constexpr std::string_view kInstanceId = "table_stream_recycler_instance";
constexpr int64_t kBaseDbId = 1001;
constexpr int64_t kBaseTableId = 1002;
constexpr int64_t kStreamDbId = 1003;
constexpr int64_t kStreamId = 1004;
constexpr int64_t kPartitionId = 1005;

std::string stream_offset_key(int64_t partition_id, int64_t stream_id = kStreamId) {
    return table_stream_offset_key({std::string(kInstanceId), kBaseDbId, kBaseTableId, kStreamDbId,
                                    stream_id, partition_id});
}

std::string versioned_stream_offset_key(int64_t partition_id, int64_t stream_id = kStreamId) {
    return versioned::table_stream_offset_key({std::string(kInstanceId), kBaseDbId, kBaseTableId,
                                               kStreamDbId, stream_id, partition_id});
}

void put_offset(Transaction* txn, int64_t partition_id, int64_t stream_id = kStreamId) {
    TableStreamOffsetPB offset;
    offset.set_partition_id(partition_id);
    offset.set_state(TableStreamOffsetStatePB::TABLE_STREAM_OFFSET_CONSUMED);
    offset.set_offset_tso(100);
    const auto latest_key = stream_offset_key(partition_id, stream_id);
    const auto versioned_key = versioned_stream_offset_key(partition_id, stream_id);
    txn->put(latest_key, offset.SerializeAsString());
    versioned_put(txn, versioned_key, Versionstamp(11, 0), offset.SerializeAsString());
    offset.set_offset_tso(110);
    versioned_put(txn, versioned_key, Versionstamp(12, 0), offset.SerializeAsString());
}

void put_single_version_offset(Transaction* txn, int64_t partition_id) {
    TableStreamOffsetPB offset;
    offset.set_partition_id(partition_id);
    offset.set_state(TableStreamOffsetStatePB::TABLE_STREAM_OFFSET_CONSUMED);
    offset.set_offset_tso(100);
    txn->put(stream_offset_key(partition_id), offset.SerializeAsString());
    versioned_put(txn, versioned_stream_offset_key(partition_id), Versionstamp(11, 0),
                  offset.SerializeAsString());
}

void remove_offset(Transaction* txn, int64_t partition_id, int64_t stream_id = kStreamId) {
    txn->remove(stream_offset_key(partition_id, stream_id));
    versioned_remove_all(txn, versioned_stream_offset_key(partition_id, stream_id));
}

void put_stream_recycle_index(Transaction* txn, RecycleIndexPB::State state,
                              int64_t stream_id = kStreamId) {
    RecycleIndexPB recycle_index;
    recycle_index.set_db_id(kBaseDbId);
    recycle_index.set_table_id(kBaseTableId);
    recycle_index.set_creation_time(0);
    recycle_index.set_expiration(0);
    recycle_index.set_state(state);
    recycle_index.set_object_type(IndexObjectTypePB::TABLE_STREAM);
    recycle_index.set_stream_db_id(kStreamDbId);
    txn->put(recycle_index_key({std::string(kInstanceId), stream_id}),
             recycle_index.SerializeAsString());
}

void add_stream_identity(RecyclePartitionPB* recycle_partition, int64_t stream_id) {
    TableStreamIdentityPB* identity = recycle_partition->add_table_streams();
    identity->set_base_db_id(kBaseDbId);
    identity->set_base_table_id(kBaseTableId);
    identity->set_stream_db_id(kStreamDbId);
    identity->set_stream_id(stream_id);
}

bool key_exists(TxnKv* txn_kv, std::string_view key) {
    std::unique_ptr<Transaction> txn;
    EXPECT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string value;
    return txn->get(key, &value, true) == TxnErrorCode::TXN_OK;
}

size_t range_size(TxnKv* txn_kv, const std::string& prefix) {
    std::unique_ptr<Transaction> txn;
    EXPECT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string end = prefix;
    end.push_back('\xff');
    std::unique_ptr<RangeGetIterator> iter;
    EXPECT_EQ(txn->get(prefix, end, &iter, true), TxnErrorCode::TXN_OK);
    return iter->size();
}

InstanceRecycler make_recycler(const std::shared_ptr<TxnKv>& txn_kv) {
    InstanceInfoPB instance;
    instance.set_instance_id(std::string(kInstanceId));
    return InstanceRecycler(txn_kv, instance, RecyclerThreadPoolGroup {},
                            std::make_shared<TxnLazyCommitter>(txn_kv));
}

TEST(TableStreamRecyclerTest, RecycleStreamDeletesOnlyOffsets) {
    const bool old_force_immediate_recycle = config::force_immediate_recycle;
    config::force_immediate_recycle = true;
    DORIS_CLOUD_DEFER {
        config::force_immediate_recycle = old_force_immediate_recycle;
    };

    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    put_offset(txn.get(), kPartitionId);
    put_offset(txn.get(), kPartitionId + 1);

    RecycleIndexPB recycle_index;
    recycle_index.set_db_id(kBaseDbId);
    recycle_index.set_table_id(kBaseTableId);
    recycle_index.set_creation_time(0);
    recycle_index.set_expiration(0);
    recycle_index.set_state(RecycleIndexPB::DROPPED);
    recycle_index.set_object_type(IndexObjectTypePB::TABLE_STREAM);
    recycle_index.set_stream_db_id(kStreamDbId);
    txn->put(recycle_index_key({std::string(kInstanceId), kStreamId}),
             recycle_index.SerializeAsString());
    const std::string unrelated_key = meta_tablet_key(
            {std::string(kInstanceId), kBaseTableId, kStreamId, kPartitionId, 2001});
    txn->put(unrelated_key, "unrelated physical data");
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    const int recycle_round_before = g_bvar_recycler_instance_recycle_round.get(
            {std::string(kInstanceId), "recycle_stream"});
    const int64_t recycle_total_before =
            g_bvar_recycler_instance_recycle_total_num_since_started.get(
                    {std::string(kInstanceId), "recycle_stream"});

    InstanceRecycler recycler = make_recycler(txn_kv);
    ASSERT_EQ(recycler.recycle_indexes(), 0);

    EXPECT_FALSE(
            key_exists(txn_kv.get(), recycle_index_key({std::string(kInstanceId), kStreamId})));
    EXPECT_EQ(range_size(txn_kv.get(),
                         table_stream_offset_key_prefix(std::string(kInstanceId), kBaseDbId,
                                                        kBaseTableId, kStreamDbId, kStreamId)),
              0);
    EXPECT_EQ(range_size(txn_kv.get(), versioned::table_stream_offset_key_prefix(
                                               std::string(kInstanceId), kBaseDbId, kBaseTableId,
                                               kStreamDbId, kStreamId)),
              0);
    EXPECT_TRUE(key_exists(txn_kv.get(), unrelated_key));
    EXPECT_EQ(g_bvar_recycler_instance_last_round_recycled_num.get(
                      {std::string(kInstanceId), "recycle_stream"}),
              6);
    EXPECT_EQ(g_bvar_recycler_instance_recycle_round.get(
                      {std::string(kInstanceId), "recycle_stream"}),
              recycle_round_before + 1);
    EXPECT_EQ(g_bvar_recycler_instance_recycle_total_num_since_started.get(
                      {std::string(kInstanceId), "recycle_stream"}),
              recycle_total_before + 6);
}

TEST(TableStreamRecyclerTest, RecyclePreparedStreamAfterPartialOffsetInitialization) {
    const bool old_force_immediate_recycle = config::force_immediate_recycle;
    config::force_immediate_recycle = true;
    DORIS_CLOUD_DEFER {
        config::force_immediate_recycle = old_force_immediate_recycle;
    };

    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    put_offset(txn.get(), kPartitionId);
    put_offset(txn.get(), kPartitionId + 1);
    put_offset(txn.get(), kPartitionId + 2);
    txn->remove(stream_offset_key(kPartitionId + 1));
    versioned_remove_all(txn.get(), versioned_stream_offset_key(kPartitionId + 2));
    put_stream_recycle_index(txn.get(), RecycleIndexPB::PREPARED);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    ASSERT_TRUE(key_exists(txn_kv.get(), stream_offset_key(kPartitionId)));
    ASSERT_FALSE(key_exists(txn_kv.get(), stream_offset_key(kPartitionId + 1)));
    ASSERT_GT(range_size(txn_kv.get(), versioned_stream_offset_key(kPartitionId + 1)), 0);
    ASSERT_TRUE(key_exists(txn_kv.get(), stream_offset_key(kPartitionId + 2)));
    ASSERT_EQ(range_size(txn_kv.get(), versioned_stream_offset_key(kPartitionId + 2)), 0);

    InstanceRecycler recycler = make_recycler(txn_kv);
    ASSERT_EQ(recycler.recycle_indexes(), 0);

    EXPECT_FALSE(
            key_exists(txn_kv.get(), recycle_index_key({std::string(kInstanceId), kStreamId})));
    EXPECT_EQ(range_size(txn_kv.get(),
                         table_stream_offset_key_prefix(std::string(kInstanceId), kBaseDbId,
                                                        kBaseTableId, kStreamDbId, kStreamId)),
              0);
    EXPECT_EQ(range_size(txn_kv.get(), versioned::table_stream_offset_key_prefix(
                                               std::string(kInstanceId), kBaseDbId, kBaseTableId,
                                               kStreamDbId, kStreamId)),
              0);
}

TEST(TableStreamRecyclerTest, RecycleStreamResumesFromRecyclingAfterPartialDeletion) {
    const bool old_force_immediate_recycle = config::force_immediate_recycle;
    config::force_immediate_recycle = true;
    DORIS_CLOUD_DEFER {
        config::force_immediate_recycle = old_force_immediate_recycle;
    };

    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    put_offset(txn.get(), kPartitionId);
    put_offset(txn.get(), kPartitionId + 1);
    put_offset(txn.get(), kPartitionId + 2);
    remove_offset(txn.get(), kPartitionId);
    txn->remove(stream_offset_key(kPartitionId + 1));
    put_stream_recycle_index(txn.get(), RecycleIndexPB::RECYCLING);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    ASSERT_FALSE(key_exists(txn_kv.get(), stream_offset_key(kPartitionId)));
    ASSERT_EQ(range_size(txn_kv.get(), versioned_stream_offset_key(kPartitionId)), 0);
    ASSERT_FALSE(key_exists(txn_kv.get(), stream_offset_key(kPartitionId + 1)));
    ASSERT_GT(range_size(txn_kv.get(), versioned_stream_offset_key(kPartitionId + 1)), 0);
    ASSERT_TRUE(key_exists(txn_kv.get(), stream_offset_key(kPartitionId + 2)));

    InstanceRecycler recycler = make_recycler(txn_kv);
    ASSERT_EQ(recycler.recycle_indexes(), 0);
    ASSERT_EQ(recycler.recycle_indexes(), 0);

    EXPECT_FALSE(
            key_exists(txn_kv.get(), recycle_index_key({std::string(kInstanceId), kStreamId})));
    EXPECT_EQ(range_size(txn_kv.get(),
                         table_stream_offset_key_prefix(std::string(kInstanceId), kBaseDbId,
                                                        kBaseTableId, kStreamDbId, kStreamId)),
              0);
    EXPECT_EQ(range_size(txn_kv.get(), versioned::table_stream_offset_key_prefix(
                                               std::string(kInstanceId), kBaseDbId, kBaseTableId,
                                               kStreamDbId, kStreamId)),
              0);
}

TEST(TableStreamRecyclerTest, RecycleStreamDeletesOffsetsAcrossRangePages) {
    const bool old_force_immediate_recycle = config::force_immediate_recycle;
    config::force_immediate_recycle = true;
    DORIS_CLOUD_DEFER {
        config::force_immediate_recycle = old_force_immediate_recycle;
    };

    constexpr int64_t offset_count = 10001;
    constexpr int64_t seed_batch_size = 500;
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);
    for (int64_t begin = 0; begin < offset_count; begin += seed_batch_size) {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        const int64_t end = std::min(offset_count, begin + seed_batch_size);
        for (int64_t i = begin; i < end; ++i) {
            put_single_version_offset(txn.get(), kPartitionId + i);
        }
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    put_stream_recycle_index(txn.get(), RecycleIndexPB::DROPPED);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    ASSERT_EQ(range_size(txn_kv.get(),
                         table_stream_offset_key_prefix(std::string(kInstanceId), kBaseDbId,
                                                        kBaseTableId, kStreamDbId, kStreamId)),
              10000);
    ASSERT_EQ(range_size(txn_kv.get(), versioned::table_stream_offset_key_prefix(
                                               std::string(kInstanceId), kBaseDbId, kBaseTableId,
                                               kStreamDbId, kStreamId)),
              10000);
    ASSERT_TRUE(key_exists(txn_kv.get(), stream_offset_key(kPartitionId + offset_count - 1)));
    ASSERT_GT(
            range_size(txn_kv.get(), versioned_stream_offset_key(kPartitionId + offset_count - 1)),
            0);

    InstanceRecycler recycler = make_recycler(txn_kv);
    ASSERT_EQ(recycler.recycle_indexes(), 0);

    EXPECT_FALSE(
            key_exists(txn_kv.get(), recycle_index_key({std::string(kInstanceId), kStreamId})));
    EXPECT_EQ(range_size(txn_kv.get(),
                         table_stream_offset_key_prefix(std::string(kInstanceId), kBaseDbId,
                                                        kBaseTableId, kStreamDbId, kStreamId)),
              0);
    EXPECT_EQ(range_size(txn_kv.get(), versioned::table_stream_offset_key_prefix(
                                               std::string(kInstanceId), kBaseDbId, kBaseTableId,
                                               kStreamDbId, kStreamId)),
              0);
}

TEST(TableStreamRecyclerTest, StatisticsDispatchesStreamToOffsetScan) {
    const bool old_force_immediate_recycle = config::force_immediate_recycle;
    config::force_immediate_recycle = true;
    DORIS_CLOUD_DEFER {
        config::force_immediate_recycle = old_force_immediate_recycle;
    };

    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    put_offset(txn.get(), kPartitionId);
    put_offset(txn.get(), kPartitionId + 1);

    RecycleIndexPB recycle_index;
    recycle_index.set_db_id(kBaseDbId);
    recycle_index.set_table_id(kBaseTableId);
    recycle_index.set_creation_time(0);
    recycle_index.set_expiration(0);
    recycle_index.set_state(RecycleIndexPB::DROPPED);
    recycle_index.set_object_type(IndexObjectTypePB::TABLE_STREAM);
    recycle_index.set_stream_db_id(kStreamDbId);
    txn->put(recycle_index_key({std::string(kInstanceId), kStreamId}),
             recycle_index.SerializeAsString());
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    InstanceRecycler recycler = make_recycler(txn_kv);
    ASSERT_EQ(recycler.scan_and_statistics_indexes(), 0);

    EXPECT_EQ(g_bvar_recycler_instance_last_round_to_recycle_num.get(
                      {std::string(kInstanceId), "recycle_stream"}),
              6);
    EXPECT_EQ(g_bvar_recycler_instance_last_round_to_recycle_num.get(
                      {std::string(kInstanceId), "recycle_indexes"}),
              0);
}

TEST(TableStreamRecyclerTest, RecyclePartitionDeletesOnlyThatPartitionOffsets) {
    const bool old_force_immediate_recycle = config::force_immediate_recycle;
    config::force_immediate_recycle = true;
    DORIS_CLOUD_DEFER {
        config::force_immediate_recycle = old_force_immediate_recycle;
    };

    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    put_offset(txn.get(), kPartitionId);
    put_offset(txn.get(), kPartitionId + 1);

    RecyclePartitionPB recycle_partition;
    recycle_partition.set_db_id(kBaseDbId);
    recycle_partition.set_table_id(kBaseTableId);
    recycle_partition.add_index_id(2001);
    recycle_partition.set_creation_time(0);
    recycle_partition.set_expiration(0);
    recycle_partition.set_state(RecyclePartitionPB::DROPPED);
    TableStreamIdentityPB* identity = recycle_partition.add_table_streams();
    identity->set_base_db_id(kBaseDbId);
    identity->set_base_table_id(kBaseTableId);
    identity->set_stream_db_id(kStreamDbId);
    identity->set_stream_id(kStreamId);
    txn->put(recycle_partition_key({std::string(kInstanceId), kPartitionId}),
             recycle_partition.SerializeAsString());
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    InstanceRecycler recycler = make_recycler(txn_kv);
    ASSERT_EQ(recycler.recycle_partitions(), 0);

    const auto dropped_latest =
            table_stream_offset_key({std::string(kInstanceId), kBaseDbId, kBaseTableId, kStreamDbId,
                                     kStreamId, kPartitionId});
    const auto retained_latest =
            table_stream_offset_key({std::string(kInstanceId), kBaseDbId, kBaseTableId, kStreamDbId,
                                     kStreamId, kPartitionId + 1});
    EXPECT_FALSE(key_exists(txn_kv.get(), dropped_latest));
    EXPECT_EQ(range_size(txn_kv.get(), versioned::table_stream_offset_key(
                                               {std::string(kInstanceId), kBaseDbId, kBaseTableId,
                                                kStreamDbId, kStreamId, kPartitionId})),
              0);
    EXPECT_TRUE(key_exists(txn_kv.get(), retained_latest));
    EXPECT_GT(range_size(txn_kv.get(), versioned::table_stream_offset_key(
                                               {std::string(kInstanceId), kBaseDbId, kBaseTableId,
                                                kStreamDbId, kStreamId, kPartitionId + 1})),
              0);
}

TEST(TableStreamRecyclerTest, RecyclePartitionUsesPersistedStreamIdentity) {
    const bool old_force_immediate_recycle = config::force_immediate_recycle;
    config::force_immediate_recycle = true;
    DORIS_CLOUD_DEFER {
        config::force_immediate_recycle = old_force_immediate_recycle;
    };

    constexpr std::string_view source_instance_id = "table_stream_recycler_source";
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

    InstanceInfoPB source_instance;
    source_instance.set_instance_id(std::string(source_instance_id));
    txn->put(instance_key({std::string(source_instance_id)}), source_instance.SerializeAsString());
    InstanceInfoPB child_instance;
    child_instance.set_instance_id(std::string(kInstanceId));
    child_instance.set_multi_version_status(MULTI_VERSION_READ_WRITE);
    child_instance.set_source_instance_id(std::string(source_instance_id));
    child_instance.set_source_snapshot_id(Versionstamp(20, 0).to_string());
    txn->put(instance_key({std::string(kInstanceId)}), child_instance.SerializeAsString());

    put_offset(txn.get(), kPartitionId);
    TableStreamOffsetPB source_offset;
    source_offset.set_partition_id(kPartitionId);
    source_offset.set_state(TableStreamOffsetStatePB::TABLE_STREAM_OFFSET_CONSUMED);
    source_offset.set_offset_tso(90);
    const auto source_latest =
            table_stream_offset_key({std::string(source_instance_id), kBaseDbId, kBaseTableId,
                                     kStreamDbId, kStreamId, kPartitionId});
    const auto source_versioned = versioned::table_stream_offset_key(
            {std::string(source_instance_id), kBaseDbId, kBaseTableId, kStreamDbId, kStreamId,
             kPartitionId});
    txn->put(source_latest, source_offset.SerializeAsString());
    versioned_put(txn.get(), source_versioned, Versionstamp(11, 0),
                  source_offset.SerializeAsString());

    RecyclePartitionPB recycle_partition;
    recycle_partition.set_db_id(kBaseDbId);
    recycle_partition.set_table_id(kBaseTableId);
    recycle_partition.add_index_id(2001);
    recycle_partition.set_creation_time(0);
    recycle_partition.set_expiration(0);
    recycle_partition.set_state(RecyclePartitionPB::DROPPED);
    TableStreamIdentityPB* identity = recycle_partition.add_table_streams();
    identity->set_base_db_id(kBaseDbId);
    identity->set_base_table_id(kBaseTableId);
    identity->set_stream_db_id(kStreamDbId);
    identity->set_stream_id(kStreamId);
    txn->put(recycle_partition_key({std::string(kInstanceId), kPartitionId}),
             recycle_partition.SerializeAsString());
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    InstanceRecycler recycler(txn_kv, child_instance, RecyclerThreadPoolGroup {},
                              std::make_shared<TxnLazyCommitter>(txn_kv));
    ASSERT_EQ(recycler.recycle_partitions(), 0);

    const auto child_latest =
            table_stream_offset_key({std::string(kInstanceId), kBaseDbId, kBaseTableId, kStreamDbId,
                                     kStreamId, kPartitionId});
    const auto child_versioned =
            versioned::table_stream_offset_key({std::string(kInstanceId), kBaseDbId, kBaseTableId,
                                                kStreamDbId, kStreamId, kPartitionId});
    EXPECT_FALSE(key_exists(txn_kv.get(), child_latest));
    EXPECT_EQ(range_size(txn_kv.get(), child_versioned), 0);
    EXPECT_TRUE(key_exists(txn_kv.get(), source_latest));
    EXPECT_GT(range_size(txn_kv.get(), source_versioned), 0);
}

TEST(TableStreamRecyclerTest, RecyclePartitionOffsetsResumesAcrossBatches) {
    const bool old_force_immediate_recycle = config::force_immediate_recycle;
    const int32_t old_batch_size = config::recycler_max_tasks_per_batch;
    config::force_immediate_recycle = true;
    config::recycler_max_tasks_per_batch = 2;
    DORIS_CLOUD_DEFER {
        config::force_immediate_recycle = old_force_immediate_recycle;
        config::recycler_max_tasks_per_batch = old_batch_size;
    };

    constexpr int64_t stream_count = 5;
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

    RecyclePartitionPB recycle_partition;
    recycle_partition.set_db_id(kBaseDbId);
    recycle_partition.set_table_id(kBaseTableId);
    recycle_partition.add_index_id(2001);
    recycle_partition.set_creation_time(0);
    recycle_partition.set_expiration(0);
    recycle_partition.set_state(RecyclePartitionPB::RECYCLING);
    for (int64_t i = 0; i < stream_count; ++i) {
        const int64_t stream_id = kStreamId + i;
        add_stream_identity(&recycle_partition, stream_id);
        put_offset(txn.get(), kPartitionId, stream_id);
        put_offset(txn.get(), kPartitionId + 1, stream_id);
    }
    // The first batch was already committed before the previous recycler stopped.
    remove_offset(txn.get(), kPartitionId, kStreamId);
    remove_offset(txn.get(), kPartitionId, kStreamId + 1);
    txn->put(recycle_partition_key({std::string(kInstanceId), kPartitionId}),
             recycle_partition.SerializeAsString());
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    for (int64_t i = 0; i < 2; ++i) {
        const int64_t stream_id = kStreamId + i;
        ASSERT_FALSE(key_exists(txn_kv.get(), stream_offset_key(kPartitionId, stream_id)));
        ASSERT_EQ(range_size(txn_kv.get(), versioned_stream_offset_key(kPartitionId, stream_id)),
                  0);
    }
    for (int64_t i = 2; i < stream_count; ++i) {
        const int64_t stream_id = kStreamId + i;
        ASSERT_TRUE(key_exists(txn_kv.get(), stream_offset_key(kPartitionId, stream_id)));
        ASSERT_GT(range_size(txn_kv.get(), versioned_stream_offset_key(kPartitionId, stream_id)),
                  0);
    }

    InstanceRecycler recycler = make_recycler(txn_kv);
    ASSERT_EQ(recycler.recycle_partitions(), 0);
    ASSERT_EQ(recycler.recycle_partitions(), 0);

    EXPECT_FALSE(key_exists(txn_kv.get(),
                            recycle_partition_key({std::string(kInstanceId), kPartitionId})));
    for (int64_t i = 0; i < stream_count; ++i) {
        const int64_t stream_id = kStreamId + i;
        EXPECT_FALSE(key_exists(txn_kv.get(), stream_offset_key(kPartitionId, stream_id)));
        EXPECT_EQ(range_size(txn_kv.get(), versioned_stream_offset_key(kPartitionId, stream_id)),
                  0);
        EXPECT_TRUE(key_exists(txn_kv.get(), stream_offset_key(kPartitionId + 1, stream_id)));
        EXPECT_GT(
                range_size(txn_kv.get(), versioned_stream_offset_key(kPartitionId + 1, stream_id)),
                0);
    }
}

TEST(TableStreamRecyclerTest, DropStreamRemovesOffsetsLeftByLegacyPartitionRecycle) {
    const bool old_force_immediate_recycle = config::force_immediate_recycle;
    config::force_immediate_recycle = true;
    DORIS_CLOUD_DEFER {
        config::force_immediate_recycle = old_force_immediate_recycle;
    };

    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    put_offset(txn.get(), kPartitionId);

    RecyclePartitionPB recycle_partition;
    recycle_partition.set_db_id(kBaseDbId);
    recycle_partition.set_table_id(kBaseTableId);
    recycle_partition.add_index_id(2001);
    recycle_partition.set_creation_time(0);
    recycle_partition.set_expiration(0);
    recycle_partition.set_state(RecyclePartitionPB::DROPPED);
    txn->put(recycle_partition_key({std::string(kInstanceId), kPartitionId}),
             recycle_partition.SerializeAsString());
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    InstanceRecycler partition_recycler = make_recycler(txn_kv);
    ASSERT_EQ(partition_recycler.recycle_partitions(), 0);
    ASSERT_FALSE(key_exists(txn_kv.get(),
                            recycle_partition_key({std::string(kInstanceId), kPartitionId})));
    ASSERT_TRUE(key_exists(txn_kv.get(), stream_offset_key(kPartitionId)));
    ASSERT_GT(range_size(txn_kv.get(), versioned_stream_offset_key(kPartitionId)), 0);

    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    put_stream_recycle_index(txn.get(), RecycleIndexPB::DROPPED);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    InstanceRecycler stream_recycler = make_recycler(txn_kv);
    ASSERT_EQ(stream_recycler.recycle_indexes(), 0);

    EXPECT_FALSE(
            key_exists(txn_kv.get(), recycle_index_key({std::string(kInstanceId), kStreamId})));
    EXPECT_FALSE(key_exists(txn_kv.get(), stream_offset_key(kPartitionId)));
    EXPECT_EQ(range_size(txn_kv.get(), versioned_stream_offset_key(kPartitionId)), 0);
}

} // namespace
} // namespace doris::cloud
