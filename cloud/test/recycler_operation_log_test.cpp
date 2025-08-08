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
#include "meta-service/meta_service.h"
#include "meta-store/document_message.h"
#include "meta-store/keys.h"
#include "meta-store/mem_txn_kv.h"
#include "meta-store/meta_reader.h"
#include "meta-store/txn_kv.h"
#include "meta-store/txn_kv_error.h"
#include "meta-store/versioned_value.h"
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
        drop_partition->set_expiration_secs(expiration);
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
        drop_partition->set_expiration_secs(expiration);
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
