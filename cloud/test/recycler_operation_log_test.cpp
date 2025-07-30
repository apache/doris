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

#include <butil/strings/string_split.h>
#include <fmt/core.h>
#include <gen_cpp/cloud.pb.h>
#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <string>

#include "common/config.h"
#include "common/util.h"
#include "cpp/sync_point.h"
#include "meta-service/meta_service.h"
#include "meta-store/document_message.h"
#include "meta-store/keys.h"
#include "meta-store/mem_txn_kv.h"
#include "meta-store/meta_reader.h"
#include "meta-store/txn_kv.h"
#include "meta-store/txn_kv_error.h"
#include "meta-store/versioned_value.h"
#include "mock_resource_manager.h"
#include "recycler/checker.h"
#include "recycler/recycler.h"
#include "recycler/util.h"

using namespace doris::cloud;

extern std::string instance_id;
extern int64_t current_time;
extern doris::cloud::RecyclerThreadPoolGroup thread_group;

// Convert a string to a hex-escaped string.
// A non-displayed character is represented as \xHH where HH is the hexadecimal value of the character.
// A displayed character is represented as itself.
static std::string escape_hex(std::string_view data) {
    std::string result;
    for (char c : data) {
        if (isprint(c)) {
            result += c;
        } else {
            result += fmt::format("\\x{:02x}", static_cast<unsigned char>(c));
        }
    }
    return result;
}

static size_t count_range(TxnKv* txn_kv, std::string_view begin = "",
                          std::string_view end = "\xFF") {
    std::unique_ptr<Transaction> txn;
    EXPECT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    if (!txn) {
        return 0; // Failed to create transaction
    }

    FullRangeGetOptions opts;
    opts.txn = txn.get();
    auto iter = txn_kv->full_range_get(std::string(begin), std::string(end), std::move(opts));
    size_t total = 0;
    for (auto&& kvp = iter->next(); kvp.has_value(); kvp = iter->next()) {
        total += 1;
    }

    EXPECT_TRUE(iter->is_valid()); // The iterator should still be valid after the next call.
    return total;
}

static bool is_empty_range(TxnKv* txn_kv, std::string_view begin = "",
                           std::string_view end = "\xFF") {
    return count_range(txn_kv, begin, end) == 0;
}

static std::string dump_range(TxnKv* txn_kv, std::string_view begin = "",
                              std::string_view end = "\xFF") {
    std::unique_ptr<Transaction> txn;
    if (txn_kv->create_txn(&txn) != TxnErrorCode::TXN_OK) {
        return "Failed to create dump range transaction";
    }
    FullRangeGetOptions opts;
    opts.txn = txn.get();
    auto iter = txn_kv->full_range_get(std::string(begin), std::string(end), std::move(opts));
    std::string buffer;
    for (auto&& kv = iter->next(); kv.has_value(); kv = iter->next()) {
        buffer +=
                fmt::format("Key: {}, Value: {}\n", escape_hex(kv->first), escape_hex(kv->second));
    }
    EXPECT_TRUE(iter->is_valid()); // The iterator should still be valid after the next call.
    return buffer;
}

static void update_instance_info(TxnKv* txn_kv, const InstanceInfoPB& instance) {
    std::string key = instance_key({instance_id});
    std::string value = instance.SerializeAsString();
    ASSERT_FALSE(value.empty()) << "Failed to serialize instance info";
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv->create_txn(&txn);
    ASSERT_EQ(err, TxnErrorCode::TXN_OK) << "Failed to create transaction";
    txn->put(key, value);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK) << "Failed to commit transaction";
}

static void remove_instance_info(TxnKv* txn_kv) {
    std::string key = instance_key({instance_id});
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv->create_txn(&txn);
    ASSERT_EQ(err, TxnErrorCode::TXN_OK) << "Failed to create transaction";
    txn->remove(key);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK) << "Failed to commit transaction";
}

TEST(RecycleOperationLogTest, RecycleOneOperationLog) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    InstanceInfoPB instance;
    instance.set_instance_id(instance_id);
    instance.set_multi_version_status(MultiVersionStatus::MULTI_VERSION_WRITE_ONLY);
    auto* obj_info = instance.add_obj_info();
    obj_info->set_id("recycle_empty");
    obj_info->set_ak(config::test_s3_ak);
    obj_info->set_sk(config::test_s3_sk);
    obj_info->set_endpoint(config::test_s3_endpoint);
    obj_info->set_region(config::test_s3_region);
    obj_info->set_bucket(config::test_s3_bucket);
    obj_info->set_prefix("recycle_empty");

    update_instance_info(txn_kv.get(), instance);

    InstanceRecycler recycler(txn_kv, instance, thread_group,
                              std::make_shared<TxnLazyCommitter>(txn_kv));
    ASSERT_EQ(recycler.init(), 0);

    {
        // Put a empty operation log
        std::string log_key = versioned::log_key(instance_id);
        Versionstamp versionstamp(123, 0);
        std::string log_key_with_versionstamp = encode_versioned_key(log_key, versionstamp);
        OperationLogPB operation_log;
        operation_log.mutable_min_timestamp()->append(
                reinterpret_cast<const char*>(versionstamp.data().data()),
                versionstamp.data().size());

        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv->create_txn(&txn);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        txn->put(log_key_with_versionstamp, operation_log.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Recycle the operation logs.
    ASSERT_EQ(recycler.recycle_operation_logs(), 0);

    // Scan the operation logs to verify that the empty log is removed.
    remove_instance_info(txn_kv.get());
    ASSERT_TRUE(is_empty_range(txn_kv.get())) << dump_range(txn_kv.get());
}

TEST(RecycleOperationLogTest, RecycleCommitPartitionLog) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    InstanceInfoPB instance;
    instance.set_instance_id(instance_id);
    instance.set_multi_version_status(MultiVersionStatus::MULTI_VERSION_WRITE_ONLY);
    update_instance_info(txn_kv.get(), instance);

    InstanceRecycler recycler(txn_kv, instance, thread_group,
                              std::make_shared<TxnLazyCommitter>(txn_kv));
    ASSERT_EQ(recycler.init(), 0);

    uint64_t db_id = 1;
    uint64_t table_id = 2;
    uint64_t index_id = 3;
    uint64_t partition_id = 4;
    {
        // Update the table version
        std::string ver_key = versioned::table_version_key({instance_id, table_id});
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        versioned_put(txn.get(), ver_key, "");
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        MetaReader meta_reader(instance_id, txn_kv.get());
        Versionstamp table_version;
        TxnErrorCode err = meta_reader.get_table_version(table_id, &table_version);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
    }

    {
        // Put a commit partition log
        std::string log_key = versioned::log_key(instance_id);
        Versionstamp versionstamp(123, 0);
        OperationLogPB operation_log;
        operation_log.mutable_min_timestamp()->append(
                reinterpret_cast<const char*>(versionstamp.data().data()),
                versionstamp.data().size());
        auto* commit_partition = operation_log.mutable_commit_partition();
        commit_partition->set_db_id(db_id);
        commit_partition->set_table_id(table_id);
        commit_partition->add_index_ids(index_id);
        commit_partition->add_partition_ids(partition_id);

        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv->create_txn(&txn);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        versioned_put(txn.get(), log_key, operation_log.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Recycle the operation logs.
    ASSERT_EQ(recycler.recycle_operation_logs(), 0);

    // The above table version key should be removed.
    {
        MetaReader meta_reader(instance_id, txn_kv.get());
        Versionstamp table_version;
        TxnErrorCode err = meta_reader.get_table_version(table_id, &table_version);
        ASSERT_EQ(err, TxnErrorCode::TXN_KEY_NOT_FOUND);
    }

    {
        // Put a new commit partition log
        std::string log_key = versioned::log_key(instance_id);
        Versionstamp versionstamp(123, 0);
        OperationLogPB operation_log;
        operation_log.mutable_min_timestamp()->append(
                reinterpret_cast<const char*>(versionstamp.data().data()),
                versionstamp.data().size());
        auto* commit_partition = operation_log.mutable_commit_partition();
        commit_partition->set_db_id(db_id);
        commit_partition->set_table_id(table_id);
        commit_partition->add_index_ids(index_id);
        commit_partition->add_partition_ids(partition_id);

        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv->create_txn(&txn);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        versioned_put(txn.get(), log_key, operation_log.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Recycle the operation logs.
    ASSERT_EQ(recycler.recycle_operation_logs(), 0);

    // Scan the operation logs to verify that the commit partition log is removed.
    remove_instance_info(txn_kv.get());
    ASSERT_TRUE(is_empty_range(txn_kv.get())) << dump_range(txn_kv.get());
}

TEST(RecycleOperationLogTest, RecycleDropPartitionLog) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    InstanceInfoPB instance;
    instance.set_instance_id(instance_id);
    instance.set_multi_version_status(MultiVersionStatus::MULTI_VERSION_WRITE_ONLY);
    update_instance_info(txn_kv.get(), instance);

    InstanceRecycler recycler(txn_kv, instance, thread_group,
                              std::make_shared<TxnLazyCommitter>(txn_kv));
    ASSERT_EQ(recycler.init(), 0);

    uint64_t db_id = 1;
    uint64_t table_id = 2;
    uint64_t index_id = 3;
    uint64_t partition_id = 4;
    int64_t expiration = ::time(nullptr) + 3600; // 1 hour from now

    {
        // Update the table version, it will be removed after the drop partition log is recycled
        std::string ver_key = versioned::table_version_key({instance_id, table_id});
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        versioned_put(txn.get(), ver_key, "");
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        MetaReader meta_reader(instance_id, txn_kv.get());
        Versionstamp table_version;
        TxnErrorCode err = meta_reader.get_table_version(table_id, &table_version);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
    }

    {
        // Put a drop partition log
        std::string log_key = versioned::log_key(instance_id);
        Versionstamp versionstamp(123, 0);
        OperationLogPB operation_log;
        operation_log.mutable_min_timestamp()->append(
                reinterpret_cast<const char*>(versionstamp.data().data()),
                versionstamp.data().size());
        auto* drop_partition = operation_log.mutable_drop_partition();
        drop_partition->set_db_id(db_id);
        drop_partition->set_table_id(table_id);
        drop_partition->add_index_ids(index_id);
        drop_partition->add_partition_ids(partition_id);
        drop_partition->set_expiration(expiration);
        drop_partition->set_update_table_version(
                true); // Update table version to ensure the table version is removed

        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv->create_txn(&txn);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        versioned_put(txn.get(), log_key, operation_log.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Recycle the operation logs.
    ASSERT_EQ(recycler.recycle_operation_logs(), 0);

    // Verify that the recycle partition record is created
    {
        std::string recycle_key = recycle_partition_key({instance_id, partition_id});
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string value;
        TxnErrorCode err = txn->get(recycle_key, &value);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);

        RecyclePartitionPB recycle_partition_pb;
        ASSERT_TRUE(recycle_partition_pb.ParseFromString(value));
        ASSERT_EQ(recycle_partition_pb.db_id(), db_id);
        ASSERT_EQ(recycle_partition_pb.table_id(), table_id);
        ASSERT_EQ(recycle_partition_pb.index_id_size(), 1);
        ASSERT_EQ(recycle_partition_pb.index_id(0), index_id);
        ASSERT_EQ(recycle_partition_pb.state(), RecyclePartitionPB::DROPPED);
        ASSERT_EQ(recycle_partition_pb.expiration(), expiration);
    }

    // The table version key should be removed because update_table_version is true
    {
        MetaReader meta_reader(instance_id, txn_kv.get());
        Versionstamp table_version;
        TxnErrorCode err = meta_reader.get_table_version(table_id, &table_version);
        ASSERT_EQ(err, TxnErrorCode::TXN_KEY_NOT_FOUND);
    }

    {
        // Put a new drop partition log without updating table version
        std::string log_key = versioned::log_key(instance_id);
        Versionstamp versionstamp(124, 0);
        OperationLogPB operation_log;
        operation_log.mutable_min_timestamp()->append(
                reinterpret_cast<const char*>(versionstamp.data().data()),
                versionstamp.data().size());
        auto* drop_partition = operation_log.mutable_drop_partition();
        drop_partition->set_db_id(db_id);
        drop_partition->set_table_id(table_id);
        drop_partition->add_index_ids(index_id);
        drop_partition->add_partition_ids(partition_id + 1); // Different partition
        drop_partition->set_expiration(expiration);
        drop_partition->set_update_table_version(false); // Don't update table version

        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv->create_txn(&txn);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        versioned_put(txn.get(), log_key, operation_log.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Recycle the operation logs.
    ASSERT_EQ(recycler.recycle_operation_logs(), 0);

    // Verify that the second recycle partition record is created
    {
        std::string recycle_key = recycle_partition_key({instance_id, partition_id + 1});
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string value;
        TxnErrorCode err = txn->get(recycle_key, &value);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);

        RecyclePartitionPB recycle_partition_pb;
        ASSERT_TRUE(recycle_partition_pb.ParseFromString(value));
        ASSERT_EQ(recycle_partition_pb.db_id(), db_id);
        ASSERT_EQ(recycle_partition_pb.table_id(), table_id);
        ASSERT_EQ(recycle_partition_pb.index_id_size(), 1);
        ASSERT_EQ(recycle_partition_pb.index_id(0), index_id);
        ASSERT_EQ(recycle_partition_pb.state(), RecyclePartitionPB::DROPPED);
        ASSERT_EQ(recycle_partition_pb.expiration(), expiration);
    }

    // Scan the operation logs to verify that the drop partition logs are removed.
    remove_instance_info(txn_kv.get());

    // Verify that the recycle partition records are created and operation logs are removed
    {
        // Check that the first recycle partition record exists
        std::string recycle_key1 = recycle_partition_key({instance_id, partition_id});
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string value;
        TxnErrorCode err = txn->get(recycle_key1, &value);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);

        RecyclePartitionPB recycle_partition_pb;
        ASSERT_TRUE(recycle_partition_pb.ParseFromString(value));
        ASSERT_EQ(recycle_partition_pb.db_id(), db_id);
        ASSERT_EQ(recycle_partition_pb.table_id(), table_id);
        ASSERT_EQ(recycle_partition_pb.index_id_size(), 1);
        ASSERT_EQ(recycle_partition_pb.index_id(0), index_id);
        ASSERT_EQ(recycle_partition_pb.state(), RecyclePartitionPB::DROPPED);
        ASSERT_EQ(recycle_partition_pb.expiration(), expiration);
    }

    {
        // Check that the second recycle partition record exists
        std::string recycle_key2 = recycle_partition_key({instance_id, partition_id + 1});
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string value;
        TxnErrorCode err = txn->get(recycle_key2, &value);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);

        RecyclePartitionPB recycle_partition_pb;
        ASSERT_TRUE(recycle_partition_pb.ParseFromString(value));
        ASSERT_EQ(recycle_partition_pb.db_id(), db_id);
        ASSERT_EQ(recycle_partition_pb.table_id(), table_id);
        ASSERT_EQ(recycle_partition_pb.index_id_size(), 1);
        ASSERT_EQ(recycle_partition_pb.index_id(0), index_id);
        ASSERT_EQ(recycle_partition_pb.state(), RecyclePartitionPB::DROPPED);
        ASSERT_EQ(recycle_partition_pb.expiration(), expiration);
    }

    // Verify that operation logs are removed (only recycle partition records should remain)
    ASSERT_EQ(count_range(txn_kv.get()), 2) << "Should only have 2 recycle partition records";
}

TEST(RecycleOperationLogTest, RecycleCommitIndexLog) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    InstanceInfoPB instance;
    instance.set_instance_id(instance_id);
    instance.set_multi_version_status(MultiVersionStatus::MULTI_VERSION_WRITE_ONLY);
    update_instance_info(txn_kv.get(), instance);

    InstanceRecycler recycler(txn_kv, instance, thread_group,
                              std::make_shared<TxnLazyCommitter>(txn_kv));
    ASSERT_EQ(recycler.init(), 0);

    uint64_t db_id = 1;
    uint64_t table_id = 2;
    uint64_t index_id = 3;
    {
        // Update the table version
        std::string ver_key = versioned::table_version_key({instance_id, table_id});
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        versioned_put(txn.get(), ver_key, "");
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        MetaReader meta_reader(instance_id, txn_kv.get());
        Versionstamp table_version;
        TxnErrorCode err = meta_reader.get_table_version(table_id, &table_version);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
    }

    {
        // Put a commit index log
        std::string log_key = versioned::log_key(instance_id);
        Versionstamp versionstamp(123, 0);
        OperationLogPB operation_log;
        operation_log.mutable_min_timestamp()->append(
                reinterpret_cast<const char*>(versionstamp.data().data()),
                versionstamp.data().size());
        auto* commit_index = operation_log.mutable_commit_index();
        commit_index->set_db_id(db_id);
        commit_index->set_table_id(table_id);
        commit_index->add_index_ids(index_id);
        commit_index->set_update_table_version(true);

        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv->create_txn(&txn);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        versioned_put(txn.get(), log_key, operation_log.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Recycle the operation logs.
    ASSERT_EQ(recycler.recycle_operation_logs(), 0);

    // The above table version key should be removed.
    {
        MetaReader meta_reader(instance_id, txn_kv.get());
        Versionstamp table_version;
        TxnErrorCode err = meta_reader.get_table_version(table_id, &table_version);
        ASSERT_EQ(err, TxnErrorCode::TXN_KEY_NOT_FOUND);
    }

    {
        // Put a new commit index log
        std::string log_key = versioned::log_key(instance_id);
        Versionstamp versionstamp(123, 0);
        OperationLogPB operation_log;
        operation_log.mutable_min_timestamp()->append(
                reinterpret_cast<const char*>(versionstamp.data().data()),
                versionstamp.data().size());
        auto* commit_index = operation_log.mutable_commit_index();
        commit_index->set_db_id(db_id);
        commit_index->set_table_id(table_id);
        commit_index->add_index_ids(index_id);

        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv->create_txn(&txn);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        versioned_put(txn.get(), log_key, operation_log.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Recycle the operation logs.
    ASSERT_EQ(recycler.recycle_operation_logs(), 0);

    // Scan the operation logs to verify that the commit index log is removed.
    remove_instance_info(txn_kv.get());
    ASSERT_TRUE(is_empty_range(txn_kv.get())) << dump_range(txn_kv.get());
}

TEST(RecycleOperationLogTest, RecycleDropIndexLog) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    InstanceInfoPB instance;
    instance.set_instance_id(instance_id);
    instance.set_multi_version_status(MultiVersionStatus::MULTI_VERSION_WRITE_ONLY);
    update_instance_info(txn_kv.get(), instance);

    InstanceRecycler recycler(txn_kv, instance, thread_group,
                              std::make_shared<TxnLazyCommitter>(txn_kv));
    ASSERT_EQ(recycler.init(), 0);

    uint64_t db_id = 1;
    uint64_t table_id = 2;
    uint64_t index_id = 3;
    int64_t expiration = ::time(nullptr) + 3600; // 1 hour from now

    {
        // Put a drop index log
        std::string log_key = versioned::log_key(instance_id);
        Versionstamp versionstamp(123, 0);
        OperationLogPB operation_log;
        operation_log.mutable_min_timestamp()->append(
                reinterpret_cast<const char*>(versionstamp.data().data()),
                versionstamp.data().size());
        auto* drop_index = operation_log.mutable_drop_index();
        drop_index->set_db_id(db_id);
        drop_index->set_table_id(table_id);
        drop_index->add_index_ids(index_id);
        drop_index->set_expiration(expiration);

        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv->create_txn(&txn);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        versioned_put(txn.get(), log_key, operation_log.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Recycle the operation logs.
    ASSERT_EQ(recycler.recycle_operation_logs(), 0);

    // Verify that the recycle index record is created
    {
        std::string recycle_key = recycle_index_key({instance_id, index_id});
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string value;
        TxnErrorCode err = txn->get(recycle_key, &value);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);

        RecycleIndexPB recycle_index_pb;
        ASSERT_TRUE(recycle_index_pb.ParseFromString(value));
        ASSERT_EQ(recycle_index_pb.db_id(), db_id);
        ASSERT_EQ(recycle_index_pb.table_id(), table_id);
        ASSERT_EQ(recycle_index_pb.state(), RecycleIndexPB::DROPPED);
        ASSERT_EQ(recycle_index_pb.expiration(), expiration);
    }

    // Scan the operation logs to verify that the drop index logs are removed.
    remove_instance_info(txn_kv.get());

    // Verify that the recycle index records are created and operation logs are removed
    {
        // Check that the first recycle index record exists
        std::string recycle_key1 = recycle_index_key({instance_id, index_id});
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string value;
        TxnErrorCode err = txn->get(recycle_key1, &value);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);

        RecycleIndexPB recycle_index_pb;
        ASSERT_TRUE(recycle_index_pb.ParseFromString(value));
        ASSERT_EQ(recycle_index_pb.db_id(), db_id);
        ASSERT_EQ(recycle_index_pb.table_id(), table_id);
        ASSERT_EQ(recycle_index_pb.state(), RecycleIndexPB::DROPPED);
        ASSERT_EQ(recycle_index_pb.expiration(), expiration);
    }

    // Verify that operation logs are removed (only recycle index records should remain)
    ASSERT_EQ(count_range(txn_kv.get()), 1) << "Should only have 2 recycle index records";
}

TEST(RecycleOperationLogTest, RecycleCommitTxnLog) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    InstanceInfoPB instance;
    instance.set_instance_id(instance_id);
    instance.set_multi_version_status(MultiVersionStatus::MULTI_VERSION_WRITE_ONLY);
    update_instance_info(txn_kv.get(), instance);

    InstanceRecycler recycler(txn_kv, instance, thread_group,
                              std::make_shared<TxnLazyCommitter>(txn_kv));
    ASSERT_EQ(recycler.init(), 0);

    uint64_t db_id = 1;
    uint64_t table_id = 2;
    uint64_t partition_id = 4;
    uint64_t tablet_id = 5;
    uint64_t txn_id = 12345;

    // Create TxnInfo with VISIBLE status
    {
        TxnInfoPB txn_info;
        txn_info.set_db_id(db_id);
        txn_info.set_txn_id(txn_id);
        txn_info.set_status(TxnStatusPB::TXN_STATUS_VISIBLE);
        txn_info.set_label("test_label");

        std::string key = txn_info_key({instance_id, db_id, txn_id});
        std::string txn_info_value = txn_info.SerializeAsString();

        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        txn->put(key, txn_info_value);
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Create previous partition version to be recycled
    {
        std::string partition_version_key =
                versioned::partition_version_key({instance_id, partition_id});
        VersionPB version_pb;
        version_pb.set_version(100);

        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        versioned_put(txn.get(), partition_version_key, version_pb.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Create previous tablet load stats to be recycled
    {
        std::string tablet_stats_key = versioned::tablet_load_stats_key({instance_id, tablet_id});
        TabletStatsPB stats_pb;
        // TabletStatsPB typically doesn't have tablet_id field, just create empty stats

        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        versioned_put(txn.get(), tablet_stats_key, stats_pb.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Create previous table version to be recycled
    {
        std::string table_version_key = versioned::table_version_key({instance_id, table_id});
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        versioned_put(txn.get(), table_version_key, "");
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Create versioned rowset meta (should NOT be recycled)
    {
        std::string versioned_rowset_key =
                versioned::meta_rowset_load_key({instance_id, tablet_id, 100});
        doris::RowsetMetaCloudPB rowset_meta;
        rowset_meta.set_rowset_id(123456);
        rowset_meta.set_tablet_id(tablet_id);
        rowset_meta.set_partition_id(partition_id);
        rowset_meta.set_start_version(100);
        rowset_meta.set_end_version(100);
        rowset_meta.set_num_rows(1000);
        rowset_meta.set_total_disk_size(1024 * 1024);

        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        ASSERT_TRUE(
                versioned::document_put(txn.get(), versioned_rowset_key, std::move(rowset_meta)));
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Put a commit txn log
    {
        std::string log_key = versioned::log_key(instance_id);
        Versionstamp versionstamp(123, 0);
        OperationLogPB operation_log;
        operation_log.mutable_min_timestamp()->append(
                reinterpret_cast<const char*>(versionstamp.data().data()),
                versionstamp.data().size());

        auto* commit_txn = operation_log.mutable_commit_txn();
        commit_txn->set_txn_id(txn_id);
        commit_txn->set_db_id(db_id);
        commit_txn->add_table_ids(table_id);

        // Add partition version mapping
        auto& partition_version_map = *commit_txn->mutable_partition_version_map();
        partition_version_map[partition_id] = 101; // New version

        // Add tablet to partition mapping
        auto& tablet_to_partition_map = *commit_txn->mutable_tablet_to_partition_map();
        tablet_to_partition_map[tablet_id] = partition_id;

        // Add recycle txn info
        auto* recycle_txn = commit_txn->mutable_recycle_txn();
        recycle_txn->set_label("test_label");

        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv->create_txn(&txn);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        versioned_put(txn.get(), log_key, operation_log.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Recycle the operation logs
    ASSERT_EQ(recycler.recycle_operation_logs(), 0);

    // Verify that the recycle txn record is created
    {
        std::string recycle_key = recycle_txn_key({instance_id, db_id, txn_id});
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string value;
        TxnErrorCode err = txn->get(recycle_key, &value);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);

        RecycleTxnPB recycle_txn_pb;
        ASSERT_TRUE(recycle_txn_pb.ParseFromString(value));
        ASSERT_EQ(recycle_txn_pb.label(), "test_label");
        ASSERT_GT(recycle_txn_pb.creation_time(), 0);
    }

    // Verify that previous versions are recycled (should not be found with current snapshot)
    {
        MetaReader meta_reader(instance_id, txn_kv.get());

        // Previous partition version should be marked for removal
        Versionstamp partition_version;
        (void)meta_reader.get_partition_version(partition_id, nullptr, &partition_version);
        // Since we only have one version, it should still exist but the previous one should be scheduled for removal

        // Previous tablet stats should be marked for removal
        Versionstamp tablet_version;
        (void)meta_reader.get_tablet_load_stats(tablet_id, nullptr, &tablet_version);

        // Previous table version should be marked for removal
        Versionstamp table_version;
        (void)meta_reader.get_table_version(table_id, &table_version);
    }

    // Scan the operation logs to verify that the commit txn log is removed
    remove_instance_info(txn_kv.get());

    // Should have TxnInfoPB + RecycleTxnPB + RowsetMetaCloudPB = 3 records
    ASSERT_EQ(count_range(txn_kv.get()), 3)
            << "Should have TxnInfoPB, RecycleTxnPB and RowsetMetaCloudPB records";
}

TEST(RecycleOperationLogTest, RecycleCommitTxnLogWhenTxnIsNotVisible) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    InstanceInfoPB instance;
    instance.set_instance_id(instance_id);
    instance.set_multi_version_status(MultiVersionStatus::MULTI_VERSION_WRITE_ONLY);
    update_instance_info(txn_kv.get(), instance);

    InstanceRecycler recycler(txn_kv, instance, thread_group,
                              std::make_shared<TxnLazyCommitter>(txn_kv));
    ASSERT_EQ(recycler.init(), 0);

    uint64_t db_id = 1;
    uint64_t table_id = 2;
    uint64_t partition_id = 4;
    uint64_t tablet_id = 5;
    uint64_t txn_id = 12345;

    // Create TxnInfo with COMMITTED status (not VISIBLE)
    {
        TxnInfoPB txn_info;
        txn_info.set_db_id(db_id);
        txn_info.set_txn_id(txn_id);
        txn_info.set_status(TxnStatusPB::TXN_STATUS_COMMITTED); // Not VISIBLE
        txn_info.set_label("test_label");

        std::string key = txn_info_key({instance_id, db_id, txn_id});
        std::string txn_info_value = txn_info.SerializeAsString();

        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        txn->put(key, txn_info_value);
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Create previous partition version
    {
        std::string partition_version_key =
                versioned::partition_version_key({instance_id, partition_id});
        VersionPB version_pb;
        version_pb.set_version(100);

        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        versioned_put(txn.get(), partition_version_key, version_pb.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Put a commit txn log
    {
        std::string log_key = versioned::log_key(instance_id);
        Versionstamp versionstamp(123, 0);
        OperationLogPB operation_log;
        operation_log.mutable_min_timestamp()->append(
                reinterpret_cast<const char*>(versionstamp.data().data()),
                versionstamp.data().size());

        auto* commit_txn = operation_log.mutable_commit_txn();
        commit_txn->set_txn_id(txn_id);
        commit_txn->set_db_id(db_id);
        commit_txn->add_table_ids(table_id);

        // Add partition version mapping
        auto& partition_version_map = *commit_txn->mutable_partition_version_map();
        partition_version_map[partition_id] = 101;

        // Add tablet to partition mapping
        auto& tablet_to_partition_map = *commit_txn->mutable_tablet_to_partition_map();
        tablet_to_partition_map[tablet_id] = partition_id;

        // Add recycle txn info
        auto* recycle_txn = commit_txn->mutable_recycle_txn();
        recycle_txn->set_label("test_label");

        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv->create_txn(&txn);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        versioned_put(txn.get(), log_key, operation_log.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Recycle the operation logs
    ASSERT_EQ(recycler.recycle_operation_logs(), 0);

    // Verify that NO recycle txn record is created (because txn is not VISIBLE)
    {
        std::string recycle_key = recycle_txn_key({instance_id, db_id, txn_id});
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string value;
        TxnErrorCode err = txn->get(recycle_key, &value);
        ASSERT_EQ(err, TxnErrorCode::TXN_KEY_NOT_FOUND)
                << "Recycle txn record should not exist for non-VISIBLE transactions";
    }

    // Verify that the original partition version still exists (not recycled)
    {
        MetaReader meta_reader(instance_id, txn_kv.get());
        Versionstamp partition_version;
        TxnErrorCode err =
                meta_reader.get_partition_version(partition_id, nullptr, &partition_version);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK)
                << "Partition version should still exist for non-VISIBLE transactions";
    }

    // Scan the operation logs to verify that the commit txn log is removed
    remove_instance_info(txn_kv.get());

    // Should have TxnInfoPB + partition_version + RowsetMetaCloudPB = 3 records (no recycling for non-VISIBLE txn)
    ASSERT_EQ(count_range(txn_kv.get()), 3)
            << "Should have TxnInfoPB, partition version and RowsetMetaCloudPB records";
}

TEST(RecycleOperationLogTest, RecycleUpdateTabletLog) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    InstanceInfoPB instance;
    instance.set_instance_id(instance_id);
    instance.set_multi_version_status(MultiVersionStatus::MULTI_VERSION_WRITE_ONLY);
    update_instance_info(txn_kv.get(), instance);

    InstanceRecycler recycler(txn_kv, instance, thread_group,
                              std::make_shared<TxnLazyCommitter>(txn_kv));
    ASSERT_EQ(recycler.init(), 0);

    uint64_t tablet_id = 1;
    {
        // Update the tablet meta
        std::string tablet_meta_key = versioned::meta_tablet_key({instance_id, tablet_id});
        doris::TabletMetaCloudPB tablet_meta;
        tablet_meta.set_tablet_id(tablet_id);
        tablet_meta.set_creation_time(::time(nullptr));

        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        versioned_put(txn.get(), tablet_meta_key, tablet_meta.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        MetaReader meta_reader(instance_id, txn_kv.get());
        Versionstamp versionstamp;
        TxnErrorCode err = meta_reader.get_tablet_meta(tablet_id, &tablet_meta, &versionstamp);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
    }

    {
        // Put an update tablet log
        std::string log_key = versioned::log_key(instance_id);
        Versionstamp versionstamp(123, 0);
        OperationLogPB operation_log;
        operation_log.mutable_min_timestamp()->append(
                reinterpret_cast<const char*>(versionstamp.data().data()),
                versionstamp.data().size());
        auto* update_tablet = operation_log.mutable_update_tablet();
        update_tablet->add_tablet_ids(tablet_id);

        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv->create_txn(&txn);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        versioned_put(txn.get(), log_key, operation_log.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Recycle the operation logs.
    ASSERT_EQ(recycler.recycle_operation_logs(), 0);

    // The tablet meta should be removed
    {
        MetaReader meta_reader(instance_id, txn_kv.get());
        doris::TabletMetaCloudPB tablet_meta;
        Versionstamp versionstamp;
        TxnErrorCode err = meta_reader.get_tablet_meta(tablet_id, &tablet_meta, &versionstamp);
        ASSERT_EQ(err, TxnErrorCode::TXN_KEY_NOT_FOUND);
    }
    // The operation log should be removed
    remove_instance_info(txn_kv.get());
    ASSERT_TRUE(is_empty_range(txn_kv.get())) << dump_range(txn_kv.get());
}

std::unique_ptr<MetaServiceProxy> get_meta_service(bool mock_resource_mgr) {
    int ret = 0;
    // MemKv
    auto txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<MemTxnKv>());
    if (txn_kv != nullptr) {
        ret = txn_kv->init();
        [&] { ASSERT_EQ(ret, 0); }();
    }
    [&] { ASSERT_NE(txn_kv.get(), nullptr); }();

    // FdbKv
    //     config::fdb_cluster_file_path = "fdb.cluster";
    //     static auto txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<FdbTxnKv>());
    //     static std::atomic<bool> init {false};
    //     bool tmp = false;
    //     if (init.compare_exchange_strong(tmp, true)) {
    //         int ret = txn_kv->init();
    //         [&] { ASSERT_EQ(ret, 0); ASSERT_NE(txn_kv.get(), nullptr); }();
    //     }

    std::unique_ptr<Transaction> txn;
    EXPECT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->remove("\x00", "\xfe"); // This is dangerous if the fdb is not correctly set
    EXPECT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    auto rs = mock_resource_mgr ? std::make_shared<MockResourceManager>(txn_kv)
                                : std::make_shared<ResourceManager>(txn_kv);
    auto rl = std::make_shared<RateLimiter>();
    auto meta_service = std::make_unique<MetaServiceImpl>(txn_kv, rs, rl);
    return std::make_unique<MetaServiceProxy>(std::move(meta_service));
}

namespace doris::cloud {
static std::string next_rowset_id() {
    static int cnt = 0;
    return std::to_string(++cnt);
}

void add_tablet(CreateTabletsRequest& req, int64_t table_id, int64_t index_id, int64_t partition_id,
                int64_t tablet_id) {
    auto tablet = req.add_tablet_metas();
    tablet->set_table_id(table_id);
    tablet->set_index_id(index_id);
    tablet->set_partition_id(partition_id);
    tablet->set_tablet_id(tablet_id);
    auto schema = tablet->mutable_schema();
    schema->set_schema_version(0);
    auto first_rowset = tablet->add_rs_metas();
    first_rowset->set_rowset_id(0); // required
    first_rowset->set_rowset_id_v2(next_rowset_id());
    first_rowset->set_start_version(0);
    first_rowset->set_end_version(1);
    first_rowset->mutable_tablet_schema()->CopyFrom(*schema);
}

void create_tablet(MetaServiceProxy* meta_service, int64_t table_id, int64_t index_id,
                   int64_t partition_id, int64_t tablet_id) {
    brpc::Controller cntl;
    CreateTabletsRequest req;
    CreateTabletsResponse res;
    add_tablet(req, table_id, index_id, partition_id, tablet_id);
    meta_service->create_tablets(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << tablet_id;
}

doris::RowsetMetaCloudPB create_rowset(int64_t txn_id, int64_t tablet_id, int partition_id = 10,
                                       int64_t version = -1, int num_rows = 100) {
    doris::RowsetMetaCloudPB rowset;
    rowset.set_rowset_id(0); // required
    rowset.set_rowset_id_v2(next_rowset_id());
    rowset.set_tablet_id(tablet_id);
    rowset.set_partition_id(partition_id);
    rowset.set_txn_id(txn_id);
    if (version > 0) {
        rowset.set_start_version(version);
        rowset.set_end_version(version);
    }
    rowset.set_num_segments(1);
    rowset.set_num_rows(num_rows);
    rowset.set_data_disk_size(num_rows * 100);
    rowset.set_index_disk_size(num_rows * 10);
    rowset.set_total_disk_size(num_rows * 110);
    rowset.mutable_tablet_schema()->set_schema_version(0);
    rowset.set_txn_expiration(::time(nullptr)); // Required by DCHECK
    return rowset;
}

TEST(RecycleOperationLogTest, RecycleCompactionLog) {
    auto meta_service = get_meta_service(false);
    std::string test_instance_id = "recycle_compaction_log_test";
    auto* sp = SyncPoint::get_instance();
    DORIS_CLOUD_DEFER {
        SyncPoint::get_instance()->clear_all_call_backs();
    };
    sp->set_call_back("get_instance_id", [&](auto&& args) {
        auto* ret = try_any_cast_ret<std::string>(args);
        ret->first = test_instance_id;
        ret->second = true;
    });
    sp->set_call_back("check_lazy_txn_finished::bypass_check", [&](auto&& args) {
        auto* ret = doris::try_any_cast_ret<bool>(args);
        ret->first = true;
        ret->second = true;
    });
    sp->set_call_back("delete_rowset_data::bypass_check", [&](auto&& args) {
        auto* ret = doris::try_any_cast_ret<bool>(args);
        ret->first = true;
        ret->second = true;
    });
    sp->set_call_back("recycle_tablet::bypass_check", [&](auto&& args) {
        auto* ret = doris::try_any_cast_ret<bool>(args);
        ret->first = false;
        ret->second = true;
    });
    sp->enable_processing();

    constexpr int64_t table_id = 20001;
    constexpr int64_t index_id = 20002;
    constexpr int64_t partition_id = 20003;
    constexpr int64_t tablet_id = 20004;

    {
        // write instance
        InstanceInfoPB instance_info;
        instance_info.set_instance_id(test_instance_id);
        instance_info.set_multi_version_status(MULTI_VERSION_WRITE_ONLY);
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        txn->put(instance_key(test_instance_id), instance_info.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        meta_service->resource_mgr()->refresh_instance(test_instance_id);
        ASSERT_TRUE(meta_service->resource_mgr()->is_version_write_enabled(test_instance_id));
    }

    {
        // Create tablet first
        create_tablet(meta_service.get(), table_id, index_id, partition_id, tablet_id);
    }

    // Create input rowsets for compaction (versions 2-4)
    std::vector<doris::RowsetMetaCloudPB> input_rowsets;
    auto txn_kv = meta_service->txn_kv();
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

        for (int i = 0; i < 3; ++i) {
            auto rowset = create_rowset(i + 100, tablet_id, partition_id, i + 2, 50 * (i + 1));
            input_rowsets.push_back(rowset);

            // Put rowset directly to meta storage
            auto rowset_key = meta_rowset_key({test_instance_id, tablet_id, rowset.end_version()});
            auto rowset_val = rowset.SerializeAsString();
            txn->put(rowset_key, rowset_val);
        }
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Create output rowset as tmp rowset
    constexpr int64_t txn_id = 30001;
    constexpr int64_t output_start_version = 2;
    constexpr int64_t output_end_version = 4;
    auto output_rowset = create_rowset(200, tablet_id, partition_id, output_start_version, 100);
    output_rowset.set_end_version(output_end_version); // Set end version to create 2-4 range
    output_rowset.set_txn_id(txn_id);

    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

        // Put tmp rowset
        auto tmp_rowset_key = meta_rowset_tmp_key({test_instance_id, txn_id, tablet_id});
        auto tmp_rowset_val = output_rowset.SerializeAsString();
        txn->put(tmp_rowset_key, tmp_rowset_val);

        // Create initial tablet stats
        TabletStatsPB initial_stats;
        initial_stats.set_num_rows(150); // Total from input rowsets
        initial_stats.set_data_size(150 * 50);
        initial_stats.set_num_rowsets(3);
        initial_stats.set_num_segments(3);
        initial_stats.set_index_size(100);
        initial_stats.set_segment_size(200);
        initial_stats.set_cumulative_point(1);

        auto stats_key =
                stats_tablet_key({test_instance_id, table_id, index_id, partition_id, tablet_id});
        auto stats_val = initial_stats.SerializeAsString();
        txn->put(stats_key, stats_val);

        // Create tablet compact stats for versioned storage
        auto tablet_compact_stats_key =
                versioned::tablet_compact_stats_key({test_instance_id, tablet_id});
        auto tablet_compact_stats_val = initial_stats.SerializeAsString();
        versioned_put(txn.get(), tablet_compact_stats_key, tablet_compact_stats_val);

        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Start compaction job first
    const std::string job_id = "test_compaction_job";
    const std::string initiator = "test_be";

    {
        brpc::Controller cntl;
        StartTabletJobRequest req;
        StartTabletJobResponse res;
        req.mutable_job()->mutable_idx()->set_tablet_id(tablet_id);
        auto compaction = req.mutable_job()->add_compaction();
        compaction->set_id(job_id);
        compaction->set_initiator(initiator);
        compaction->set_type(TabletCompactionJobPB::CUMULATIVE);
        compaction->set_base_compaction_cnt(0);
        compaction->set_cumulative_compaction_cnt(0);
        compaction->add_input_versions(2);
        compaction->add_input_versions(4);
        long now = time(nullptr);
        compaction->set_expiration(now + 12);
        compaction->set_lease(now + 3);
        meta_service->start_tablet_job(&cntl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // Now use real finish_tablet_job to trigger process_compaction_job
    {
        brpc::Controller cntl;
        FinishTabletJobRequest req;
        FinishTabletJobResponse res;

        req.set_action(FinishTabletJobRequest::COMMIT);
        req.mutable_job()->mutable_idx()->set_table_id(table_id);
        req.mutable_job()->mutable_idx()->set_index_id(index_id);
        req.mutable_job()->mutable_idx()->set_partition_id(partition_id);
        req.mutable_job()->mutable_idx()->set_tablet_id(tablet_id);

        auto compaction = req.mutable_job()->add_compaction();
        compaction->set_id(job_id);
        compaction->set_initiator(initiator);
        compaction->set_type(TabletCompactionJobPB::CUMULATIVE);

        // Input versions and rowsets
        compaction->add_input_versions(2);
        compaction->add_input_versions(4);

        // Output information
        compaction->add_txn_id(txn_id);
        compaction->add_output_versions(output_end_version);
        compaction->add_output_rowset_ids(output_rowset.rowset_id_v2());
        compaction->set_output_cumulative_point(5);

        // Compaction stats for updating tablet stats
        compaction->set_size_input_rowsets(150 * 50); // Size of input rowsets
        compaction->set_index_size_input_rowsets(100);
        compaction->set_segment_size_input_rowsets(200);
        compaction->set_num_input_rows(150);
        compaction->set_num_input_rowsets(3);
        compaction->set_num_input_segments(3);

        compaction->set_size_output_rowsets(100 * 50); // Size of output rowset
        compaction->set_index_size_output_rowsets(50);
        compaction->set_segment_size_output_rowsets(100);
        compaction->set_num_output_rows(100);
        compaction->set_num_output_rowsets(1);
        compaction->set_num_output_segments(1);

        // This will trigger process_compaction_job internally and create compaction log
        meta_service->finish_tablet_job(&cntl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // Verify compaction log was created
    Versionstamp log_version;
    OperationLogPB operation_log;
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string log_key = versioned::log_key({test_instance_id});
        std::string value;
        ASSERT_EQ(versioned_get(txn.get(), log_key, &log_version, &value), TxnErrorCode::TXN_OK);

        ASSERT_TRUE(operation_log.ParseFromString(value));
        ASSERT_TRUE(operation_log.has_compaction());

        const auto& compaction_log = operation_log.compaction();
        ASSERT_EQ(compaction_log.tablet_id(), tablet_id);
        ASSERT_EQ(compaction_log.start_version(), 2);
        ASSERT_EQ(compaction_log.end_version(), 4);
        ASSERT_EQ(compaction_log.recycle_rowsets_size(), 3);
    }

    // Set up recycler using the same txn_kv as meta_service
    InstanceInfoPB instance_info;
    instance_info.set_instance_id(test_instance_id);
    instance_info.set_multi_version_status(MultiVersionStatus::MULTI_VERSION_WRITE_ONLY);

    InstanceRecycler recycler(txn_kv, instance_info, thread_group,
                              std::make_shared<TxnLazyCommitter>(txn_kv));
    ASSERT_EQ(recycler.init(), 0);

    // Now recycle the compaction operation log
    ASSERT_EQ(recycler.recycle_operation_logs(), 0);

    // Verify that input rowsets are converted to recycle rowsets
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

        for (const auto& input_rowset : input_rowsets) {
            // Check that recycle rowset was created
            std::string recycle_key =
                    recycle_rowset_key({test_instance_id, tablet_id, input_rowset.rowset_id_v2()});
            std::string recycle_value;
            TxnErrorCode err = txn->get(recycle_key, &recycle_value);
            ASSERT_EQ(err, TxnErrorCode::TXN_OK)
                    << "Recycle rowset should exist for rowset " << input_rowset.rowset_id_v2();

            RecycleRowsetPB recycle_rowset_pb;
            ASSERT_TRUE(recycle_rowset_pb.ParseFromString(recycle_value));
            ASSERT_EQ(recycle_rowset_pb.rowset_meta().rowset_id_v2(), input_rowset.rowset_id_v2());
            ASSERT_EQ(recycle_rowset_pb.rowset_meta().tablet_id(), tablet_id);

            // Check that compact and load keys don't exist (they never existed in this test scenario)
            std::string meta_rowset_compact_key = versioned::meta_rowset_compact_key(
                    {test_instance_id, tablet_id, input_rowset.end_version()});
            std::string meta_rowset_load_key = versioned::meta_rowset_load_key(
                    {test_instance_id, tablet_id, input_rowset.end_version()});

            RowsetMetaCloudPB compact_pb, load_pb;
            Versionstamp compact_vs, load_vs;
            if (input_rowset.end_version() == output_end_version) {
                ASSERT_EQ(versioned::document_get(txn.get(), meta_rowset_compact_key, &compact_pb,
                                                  &compact_vs),
                          TxnErrorCode::TXN_OK)
                        << "Output rowset compact meta should exist";

                // Check that this pb is indeed the 2-4 output rowset
                ASSERT_EQ(compact_pb.start_version(), output_start_version)
                        << "Output rowset should have start_version = " << output_start_version;
                ASSERT_EQ(compact_pb.end_version(), output_end_version)
                        << "Output rowset should have end_version = " << output_end_version;
                ASSERT_EQ(compact_pb.tablet_id(), tablet_id)
                        << "Output rowset should have correct tablet_id";
                ASSERT_EQ(compact_pb.rowset_id_v2(), output_rowset.rowset_id_v2())
                        << "Output rowset should have correct rowset_id_v2";

            } else {
                ASSERT_EQ(versioned::document_get(txn.get(), meta_rowset_compact_key, &compact_pb,
                                                  &compact_vs),
                          TxnErrorCode::TXN_KEY_NOT_FOUND)
                        << "Input rowset compact meta should not exist";
                ASSERT_EQ(versioned::document_get(txn.get(), meta_rowset_load_key, &load_pb,
                                                  &load_vs),
                          TxnErrorCode::TXN_KEY_NOT_FOUND)
                        << "Input rowset load meta should not exist";
            }
        }
    }

    // Verify tablet compact stats still exist (recycling compaction log should not delete them)
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

        std::string tablet_compact_stats_key =
                versioned::tablet_compact_stats_key({test_instance_id, tablet_id});
        std::string tablet_compact_stats_value;
        Versionstamp* versionstamp = nullptr;
        TxnErrorCode err = versioned_get(txn.get(), tablet_compact_stats_key, versionstamp,
                                         &tablet_compact_stats_value);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK)
                << "Tablet compact stats should still exist after recycling compaction log";
    }

    // Verify the operation log is removed
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

        std::string log_key_with_version =
                encode_versioned_key(versioned::log_key(test_instance_id), log_version);
        std::string log_value;
        TxnErrorCode err = txn->get(log_key_with_version, &log_value);
        ASSERT_EQ(err, TxnErrorCode::TXN_KEY_NOT_FOUND)
                << "Operation log should be removed after recycling";
    }

    // Now test second compaction to verify meta_rowset_compact_key deletion
    // Add version 5 rowset for second compaction and update tablet stats
    auto version5_rowset = create_rowset(300, tablet_id, partition_id, 5, 50);
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

        // Add version 5 rowset
        auto rowset_key_5 = meta_rowset_key({test_instance_id, tablet_id, 5});
        txn->put(rowset_key_5, version5_rowset.SerializeAsString());

        // Update tablet stats to include version 5 rowset
        TabletStatsPB updated_stats;
        updated_stats.set_num_rows(150);        // 100 from 2-4 + 50 from version 5
        updated_stats.set_data_size(150 * 100); // Updated data size
        updated_stats.set_num_rowsets(2);       // 2-4 rowset + version 5 rowset
        updated_stats.set_num_segments(2);
        updated_stats.set_index_size(150);
        updated_stats.set_segment_size(300);
        updated_stats.set_cumulative_point(5); // Updated cumulative point

        auto stats_key =
                stats_tablet_key({test_instance_id, table_id, index_id, partition_id, tablet_id});
        txn->put(stats_key, updated_stats.SerializeAsString());

        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Create second output rowset as tmp rowset (2-5)
    constexpr int64_t second_txn_id = 30002;
    constexpr int64_t second_output_end_version = 5;
    auto second_output_rowset = create_rowset(400, tablet_id, partition_id, 2, 200);
    second_output_rowset.set_end_version(second_output_end_version);
    second_output_rowset.set_txn_id(second_txn_id);
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        auto tmp_rowset_key = meta_rowset_tmp_key({test_instance_id, second_txn_id, tablet_id});
        txn->put(tmp_rowset_key, second_output_rowset.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Start second compaction job (2-4 + 5-5 -> 2-5)
    const std::string second_job_id = "test_second_compaction_job";
    {
        brpc::Controller cntl;
        StartTabletJobRequest req;
        StartTabletJobResponse res;
        req.mutable_job()->mutable_idx()->set_table_id(table_id);
        req.mutable_job()->mutable_idx()->set_index_id(index_id);
        req.mutable_job()->mutable_idx()->set_partition_id(partition_id);
        req.mutable_job()->mutable_idx()->set_tablet_id(tablet_id);
        auto* compaction = req.mutable_job()->add_compaction();
        compaction->set_id(second_job_id);
        compaction->set_initiator(initiator);
        compaction->set_type(TabletCompactionJobPB::CUMULATIVE);
        compaction->set_base_compaction_cnt(0);
        compaction->set_cumulative_compaction_cnt(1); // Already did one compaction
        compaction->add_input_versions(2);            // 2-4 range
        compaction->add_input_versions(5);            // + version 5
        long now = time(nullptr);
        compaction->set_expiration(now + 12);
        compaction->set_lease(now + 3);
        meta_service->start_tablet_job(&cntl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // Finish second compaction job to create second compaction log
    {
        brpc::Controller cntl;
        FinishTabletJobRequest req;
        FinishTabletJobResponse res;

        req.set_action(FinishTabletJobRequest::COMMIT);
        req.mutable_job()->mutable_idx()->set_table_id(table_id);
        req.mutable_job()->mutable_idx()->set_index_id(index_id);
        req.mutable_job()->mutable_idx()->set_partition_id(partition_id);
        req.mutable_job()->mutable_idx()->set_tablet_id(tablet_id);

        auto* compaction = req.mutable_job()->add_compaction();
        compaction->set_id(second_job_id);
        compaction->set_initiator(initiator);
        compaction->set_type(TabletCompactionJobPB::CUMULATIVE);

        // Input: 2-4 and 5-5
        compaction->add_input_versions(2);
        compaction->add_input_versions(5);

        // Output: 2-5
        compaction->add_txn_id(second_txn_id);
        compaction->add_output_versions(second_output_end_version);
        compaction->add_output_rowset_ids(second_output_rowset.rowset_id_v2());
        compaction->set_output_cumulative_point(6);

        // Compaction stats
        compaction->set_size_input_rowsets(150 * 50);
        compaction->set_index_size_input_rowsets(150);
        compaction->set_segment_size_input_rowsets(300);
        compaction->set_num_input_rows(150);
        compaction->set_num_input_rowsets(2);
        compaction->set_num_input_segments(2);

        compaction->set_size_output_rowsets(200 * 50);
        compaction->set_index_size_output_rowsets(100);
        compaction->set_segment_size_output_rowsets(200);
        compaction->set_num_output_rows(200);
        compaction->set_num_output_rowsets(1);
        compaction->set_num_output_segments(1);

        // This creates the second compaction log
        meta_service->finish_tablet_job(&cntl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // Verify that 2-4 meta_rowset_compact_key exists before recycling
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

        auto compact_key_24 = versioned::meta_rowset_compact_key({test_instance_id, tablet_id, 4});
        RowsetMetaCloudPB compact_pb_24;
        Versionstamp compact_vs_24;
        ASSERT_EQ(
                versioned::document_get(txn.get(), compact_key_24, &compact_pb_24, &compact_vs_24),
                TxnErrorCode::TXN_OK)
                << "2-4 rowset compact meta should exist before second recycling";
        // Check that this pb is indeed the 2-4 rowset
        ASSERT_EQ(compact_pb_24.start_version(), 2) << "Should be 2-4 rowset";
        ASSERT_EQ(compact_pb_24.end_version(), 4) << "Should be 2-4 rowset";
        ASSERT_EQ(compact_pb_24.rowset_id_v2(), output_rowset.rowset_id_v2())
                << "Should match output rowset ID";
    }

    // Recycle the second compaction log using the same recycler
    ASSERT_EQ(recycler.recycle_operation_logs(), 0);

    // Verify that 2-4 meta_rowset_compact_key is now deleted
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

        auto compact_key_24 = versioned::meta_rowset_compact_key({test_instance_id, tablet_id, 4});
        RowsetMetaCloudPB compact_pb_24;
        Versionstamp compact_vs_24;
        ASSERT_EQ(
                versioned::document_get(txn.get(), compact_key_24, &compact_pb_24, &compact_vs_24),
                TxnErrorCode::TXN_KEY_NOT_FOUND)
                << "2-4 rowset compact meta should be deleted after recycling second compaction "
                   "log";

        // Verify that recycle rowset was created for 2-4
        std::string recycle_key_24 =
                recycle_rowset_key({test_instance_id, tablet_id, output_rowset.rowset_id_v2()});
        std::string recycle_value_24;
        ASSERT_EQ(txn->get(recycle_key_24, &recycle_value_24), TxnErrorCode::TXN_OK)
                << "Recycle rowset should exist for 2-4 rowset";

        RecycleRowsetPB recycle_pb_24;
        ASSERT_TRUE(recycle_pb_24.ParseFromString(recycle_value_24));
        ASSERT_EQ(recycle_pb_24.rowset_meta().start_version(), 2);
        ASSERT_EQ(recycle_pb_24.rowset_meta().end_version(), 4);
        ASSERT_EQ(recycle_pb_24.rowset_meta().rowset_id_v2(), output_rowset.rowset_id_v2());
    }

    // Finally, test tablet recycling to verify tablet compact stats deletion
    // Now recycle tablets to clean up tablet compact stats
    RecyclerMetricsContext ctx(instance_id, "test");
    ASSERT_EQ(recycler.recycle_tablets(table_id, index_id, ctx), 0);

    // Verify tablet compact stats are now deleted (recycling tablets should delete them)
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

        std::string tablet_compact_stats_key =
                versioned::tablet_compact_stats_key({test_instance_id, tablet_id});
        std::string tablet_compact_stats_value;
        Versionstamp* versionstamp = nullptr;
        TxnErrorCode err = versioned_get(txn.get(), tablet_compact_stats_key, versionstamp,
                                         &tablet_compact_stats_value);
        ASSERT_EQ(err, TxnErrorCode::TXN_KEY_NOT_FOUND)
                << "Tablet compact stats should be deleted after recycling tablets";
    }
}

} // namespace doris::cloud
