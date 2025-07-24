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
