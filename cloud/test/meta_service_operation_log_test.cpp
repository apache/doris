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

#include <brpc/controller.h>
#include <fmt/format.h>
#include <gen_cpp/cloud.pb.h>
#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <functional>
#include <memory>
#include <string>

#include "common/defer.h"
#include "common/util.h"
#include "cpp/sync_point.h"
#include "meta-service/meta_service.h"
#include "meta-store/document_message.h"
#include "meta-store/keys.h"
#include "meta-store/meta_reader.h"
#include "meta-store/txn_kv.h"
#include "meta-store/txn_kv_error.h"
#include "meta-store/versioned_value.h"

namespace doris::cloud {
// External functions from meta_service_test.cpp
extern std::unique_ptr<MetaServiceProxy> get_meta_service();
extern std::unique_ptr<MetaServiceProxy> get_meta_service(bool mock_resource_mgr);
extern void create_tablet(MetaServiceProxy* meta_service, int64_t table_id, int64_t index_id,
                          int64_t partition_id, int64_t tablet_id);
extern doris::RowsetMetaCloudPB create_rowset(int64_t txn_id, int64_t tablet_id, int partition_id,
                                              int64_t version, int num_rows);
extern void commit_rowset(MetaServiceProxy* meta_service, const doris::RowsetMetaCloudPB& rowset,
                          CreateRowsetResponse& res);
extern void add_tablet(CreateTabletsRequest& req, int64_t table_id, int64_t index_id,
                       int64_t partition_id, int64_t tablet_id);

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

TEST(MetaServiceOperationLogTest, CommitPartitionLog) {
    auto meta_service = get_meta_service(false);
    std::string instance_id = "commit_partition_log";
    auto* sp = SyncPoint::get_instance();
    DORIS_CLOUD_DEFER {
        SyncPoint::get_instance()->clear_all_call_backs();
    };
    sp->set_call_back("get_instance_id", [&](auto&& args) {
        auto* ret = try_any_cast_ret<std::string>(args);
        ret->first = instance_id;
        ret->second = true;
    });
    sp->enable_processing();

    constexpr int64_t db_id = 123;
    constexpr int64_t table_id = 10001;
    constexpr int64_t index_id = 10002;
    constexpr int64_t partition_id = 10003;

    {
        // write instance
        InstanceInfoPB instance_info;
        instance_info.set_instance_id(instance_id);
        instance_info.set_multi_version_status(MULTI_VERSION_WRITE_ONLY);
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        txn->put(instance_key(instance_id), instance_info.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        meta_service->resource_mgr()->refresh_instance(instance_id);
        ASSERT_TRUE(meta_service->resource_mgr()->is_version_write_enabled(instance_id));
    }

    {
        // Prepare partition
        brpc::Controller ctrl;
        PartitionRequest req;
        PartitionResponse res;
        req.set_db_id(db_id);
        req.set_table_id(table_id);
        req.add_index_ids(index_id);
        req.add_partition_ids(partition_id);
        meta_service->prepare_partition(&ctrl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().DebugString();
    }

    {
        // Commit partition
        brpc::Controller ctrl;
        PartitionRequest req;
        PartitionResponse res;
        req.set_db_id(db_id);
        req.set_table_id(table_id);
        req.add_index_ids(index_id);
        req.add_partition_ids(partition_id);
        meta_service->commit_partition(&ctrl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().DebugString();
    }

    auto txn_kv = meta_service->txn_kv();
    Versionstamp version1;
    {
        // Verify partition meta/index/inverted indexes are exists
        std::string partition_meta_key = versioned::meta_partition_key({instance_id, partition_id});
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string value;
        ASSERT_EQ(versioned_get(txn.get(), partition_meta_key, &version1, &value),
                  TxnErrorCode::TXN_OK);

        std::string partition_inverted_index_key = versioned::partition_inverted_index_key(
                {instance_id, db_id, table_id, partition_id});
        ASSERT_EQ(txn->get(partition_inverted_index_key, &value), TxnErrorCode::TXN_OK);

        std::string partition_index_key =
                versioned::partition_index_key({instance_id, partition_id});
        ASSERT_EQ(txn->get(partition_index_key, &value), TxnErrorCode::TXN_OK);
        PartitionIndexPB partition_index;
        ASSERT_TRUE(partition_index.ParseFromString(value));
        ASSERT_EQ(partition_index.db_id(), db_id);
        ASSERT_EQ(partition_index.table_id(), table_id);
    }

    Versionstamp version2;
    {
        // Verify table version exists
        MetaReader meta_reader(instance_id, txn_kv.get());
        ASSERT_EQ(meta_reader.get_table_version(table_id, &version2), TxnErrorCode::TXN_OK);
    }

    ASSERT_EQ(version1, version2);

    {
        // verify commit partition log
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string log_key = versioned::log_key({instance_id});
        std::string value;
        ASSERT_EQ(versioned_get(txn.get(), log_key, &version2, &value), TxnErrorCode::TXN_OK);
        OperationLogPB operation_log;
        ASSERT_TRUE(operation_log.ParseFromString(value));
        ASSERT_TRUE(operation_log.has_commit_partition());
    }

    ASSERT_EQ(version1, version2);
}

TEST(MetaServiceOperationLogTest, DropPartitionLog) {
    auto meta_service = get_meta_service(false);
    std::string instance_id = "commit_partition_log";
    auto* sp = SyncPoint::get_instance();
    DORIS_CLOUD_DEFER {
        SyncPoint::get_instance()->clear_all_call_backs();
    };
    sp->set_call_back("get_instance_id", [&](auto&& args) {
        auto* ret = try_any_cast_ret<std::string>(args);
        ret->first = instance_id;
        ret->second = true;
    });
    sp->enable_processing();

    constexpr int64_t db_id = 123;
    constexpr int64_t table_id = 10001;
    constexpr int64_t index_id = 10002;
    constexpr int64_t partition_id = 10003;

    {
        // write instance
        InstanceInfoPB instance_info;
        instance_info.set_instance_id(instance_id);
        instance_info.set_multi_version_status(MULTI_VERSION_WRITE_ONLY);
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        txn->put(instance_key(instance_id), instance_info.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        meta_service->resource_mgr()->refresh_instance(instance_id);
        ASSERT_TRUE(meta_service->resource_mgr()->is_version_write_enabled(instance_id));
    }

    {
        // Prepare partition 0,1,2,3
        brpc::Controller ctrl;
        PartitionRequest req;
        PartitionResponse res;
        req.set_db_id(db_id);
        req.set_table_id(table_id);
        req.add_index_ids(index_id);
        req.add_partition_ids(partition_id);
        req.add_partition_ids(partition_id + 1);
        req.add_partition_ids(partition_id + 2);
        req.add_partition_ids(partition_id + 3);
        meta_service->prepare_partition(&ctrl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().DebugString();
    }

    {
        // Commit partition 2,3
        brpc::Controller ctrl;
        PartitionRequest req;
        PartitionResponse res;
        req.set_db_id(db_id);
        req.set_table_id(table_id);
        req.add_index_ids(index_id);
        req.add_partition_ids(partition_id + 2);
        req.add_partition_ids(partition_id + 3);
        meta_service->commit_partition(&ctrl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().DebugString();
    }

    auto txn_kv = meta_service->txn_kv();
    size_t num_logs = count_range(txn_kv.get(), versioned::log_key(instance_id),
                                  versioned::log_key(instance_id) + "\xFF");

    {
        // Drop partition 0
        brpc::Controller ctrl;
        PartitionRequest req;
        PartitionResponse res;
        req.set_db_id(db_id);
        req.set_table_id(table_id);
        req.add_index_ids(index_id);
        req.add_partition_ids(partition_id);
        meta_service->drop_partition(&ctrl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().DebugString();

        // No operation log are generated for drop partition 0 (it is not committed).
        size_t new_num_logs = count_range(txn_kv.get(), versioned::log_key(instance_id),
                                          versioned::log_key(instance_id) + "\xFF");
        ASSERT_EQ(new_num_logs, num_logs)
                << "Expected no new operation logs for drop partition 0, but found "
                << new_num_logs - num_logs << dump_range(txn_kv.get());

        // The recycle partition key must exists.
        std::string recycle_key = recycle_partition_key({instance_id, partition_id});
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string value;
        TxnErrorCode err = txn->get(recycle_key, &value);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
    }

    {
        // Drop partition 1,2, it should generate operation logs.
        brpc::Controller ctrl;
        PartitionRequest req;
        PartitionResponse res;
        req.set_db_id(db_id);
        req.set_table_id(table_id);
        req.add_index_ids(index_id);
        req.add_partition_ids(partition_id + 1);
        req.add_partition_ids(partition_id + 2);
        req.set_need_update_table_version(true);
        meta_service->drop_partition(&ctrl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().DebugString();
        // Check that new operation logs are generated.
        size_t new_num_logs = count_range(txn_kv.get(), versioned::log_key(instance_id),
                                          versioned::log_key(instance_id) + "\xFF");
        ASSERT_GT(new_num_logs, num_logs)
                << "Expected new operation logs for drop partition 1,2, but found "
                << new_num_logs - num_logs << dump_range(txn_kv.get());
        num_logs = new_num_logs;
    }

    {
        // Drop partition 3, it should generate operation logs.
        brpc::Controller ctrl;
        PartitionRequest req;
        PartitionResponse res;
        req.set_db_id(db_id);
        req.set_table_id(table_id);
        req.add_index_ids(index_id);
        req.add_partition_ids(partition_id + 3);
        req.set_need_update_table_version(true);
        meta_service->drop_partition(&ctrl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().DebugString();
        // Check that new operation logs are generated.
        size_t new_num_logs = count_range(txn_kv.get(), versioned::log_key(instance_id),
                                          versioned::log_key(instance_id) + "\xFF");
        ASSERT_GT(new_num_logs, num_logs)
                << "Expected new operation logs for drop partition 3, but found "
                << new_num_logs - num_logs << dump_range(txn_kv.get());
        num_logs = new_num_logs;

        // The recycle partition key should not be exists.
        std::string recycle_key = recycle_partition_key({instance_id, partition_id + 3});
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string value;
        TxnErrorCode err = txn->get(recycle_key, &value);
        ASSERT_EQ(err, TxnErrorCode::TXN_KEY_NOT_FOUND)
                << "Expected recycle partition key to not exist, but found it: " << hex(recycle_key)
                << " with value: " << escape_hex(value);
    }

    Versionstamp version1;
    Versionstamp version2;
    {
        // Verify table version exists
        MetaReader meta_reader(instance_id, txn_kv.get());
        ASSERT_EQ(meta_reader.get_table_version(table_id, &version1), TxnErrorCode::TXN_OK);
    }

    {
        // verify last drop partition log
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string log_key = versioned::log_key({instance_id});
        std::string value;
        ASSERT_EQ(versioned_get(txn.get(), log_key, &version2, &value), TxnErrorCode::TXN_OK);
        OperationLogPB operation_log;
        ASSERT_TRUE(operation_log.ParseFromString(value));
        ASSERT_TRUE(operation_log.has_drop_partition());
        ASSERT_EQ(operation_log.drop_partition().partition_ids_size(), 1);
        ASSERT_EQ(operation_log.drop_partition().partition_ids(0), partition_id + 3);
    }

    ASSERT_EQ(version1, version2);
}

TEST(MetaServiceOperationLogTest, CommitIndexLog) {
    auto meta_service = get_meta_service(false);
    std::string instance_id = "commit_index_log";
    auto* sp = SyncPoint::get_instance();
    DORIS_CLOUD_DEFER {
        SyncPoint::get_instance()->clear_all_call_backs();
    };
    sp->set_call_back("get_instance_id", [&](auto&& args) {
        auto* ret = try_any_cast_ret<std::string>(args);
        ret->first = instance_id;
        ret->second = true;
    });
    sp->enable_processing();

    constexpr int64_t db_id = 123;
    constexpr int64_t table_id = 10001;
    constexpr int64_t index_id = 10002;

    {
        // write instance
        InstanceInfoPB instance_info;
        instance_info.set_instance_id(instance_id);
        instance_info.set_multi_version_status(MULTI_VERSION_WRITE_ONLY);
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        txn->put(instance_key(instance_id), instance_info.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        meta_service->resource_mgr()->refresh_instance(instance_id);
        ASSERT_TRUE(meta_service->resource_mgr()->is_version_write_enabled(instance_id));
    }

    {
        // Prepare index
        brpc::Controller ctrl;
        IndexRequest req;
        IndexResponse res;
        req.set_db_id(db_id);
        req.set_table_id(table_id);
        req.add_index_ids(index_id);
        meta_service->prepare_index(&ctrl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().DebugString();
    }

    {
        // Commit index
        brpc::Controller ctrl;
        IndexRequest req;
        IndexResponse res;
        req.set_db_id(db_id);
        req.set_table_id(table_id);
        req.add_index_ids(index_id);
        req.set_is_new_table(true);
        meta_service->commit_index(&ctrl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().DebugString();
    }

    auto txn_kv = meta_service->txn_kv();
    Versionstamp version1;
    {
        // Verify index meta/index/inverted indexes are exists
        std::string index_meta_key = versioned::meta_index_key({instance_id, index_id});
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string value;
        ASSERT_EQ(versioned_get(txn.get(), index_meta_key, &version1, &value),
                  TxnErrorCode::TXN_OK);

        std::string index_inverted_key =
                versioned::index_inverted_key({instance_id, db_id, table_id, index_id});
        ASSERT_EQ(txn->get(index_inverted_key, &value), TxnErrorCode::TXN_OK);

        std::string index_index_key = versioned::index_index_key({instance_id, index_id});
        ASSERT_EQ(txn->get(index_index_key, &value), TxnErrorCode::TXN_OK);
        IndexIndexPB index_index;
        ASSERT_TRUE(index_index.ParseFromString(value));
        ASSERT_EQ(index_index.db_id(), db_id);
        ASSERT_EQ(index_index.table_id(), table_id);
    }

    Versionstamp version2;
    {
        // Verify table version exists
        MetaReader meta_reader(instance_id, txn_kv.get());
        ASSERT_EQ(meta_reader.get_table_version(table_id, &version2), TxnErrorCode::TXN_OK);
    }

    ASSERT_EQ(version1, version2);

    {
        // verify commit index log
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string log_key = versioned::log_key({instance_id});
        std::string value;
        ASSERT_EQ(versioned_get(txn.get(), log_key, &version2, &value), TxnErrorCode::TXN_OK);
        OperationLogPB operation_log;
        ASSERT_TRUE(operation_log.ParseFromString(value));
        ASSERT_TRUE(operation_log.has_commit_index());
    }

    ASSERT_EQ(version1, version2);

    {
        // Prepare index2
        brpc::Controller ctrl;
        IndexRequest req;
        IndexResponse res;
        req.set_db_id(db_id);
        req.set_table_id(table_id);
        req.add_index_ids(index_id + 1);
        meta_service->prepare_index(&ctrl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().DebugString();
    }

    {
        // Commit index2 without set_is_new_table(true)
        brpc::Controller ctrl;
        IndexRequest req;
        IndexResponse res;
        req.set_db_id(db_id);
        req.set_table_id(table_id);
        req.add_index_ids(index_id + 1);
        meta_service->commit_index(&ctrl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().DebugString();
    }

    {
        // Verify index1 meta/index/inverted indexes are exists
        std::string index_meta_key = versioned::meta_index_key({instance_id, index_id + 1});
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string value;
        ASSERT_EQ(versioned_get(txn.get(), index_meta_key, &version1, &value),
                  TxnErrorCode::TXN_OK);

        std::string index_inverted_key =
                versioned::index_inverted_key({instance_id, db_id, table_id, index_id + 1});
        ASSERT_EQ(txn->get(index_inverted_key, &value), TxnErrorCode::TXN_OK);

        std::string index_index_key = versioned::index_index_key({instance_id, index_id + 1});
        ASSERT_EQ(txn->get(index_index_key, &value), TxnErrorCode::TXN_OK);
        IndexIndexPB index_index;
        ASSERT_TRUE(index_index.ParseFromString(value));
        ASSERT_EQ(index_index.db_id(), db_id);
        ASSERT_EQ(index_index.table_id(), table_id);
    }

    Versionstamp version3;
    {
        // Verify table version exists and does not update
        MetaReader meta_reader(instance_id, txn_kv.get());
        ASSERT_EQ(meta_reader.get_table_version(table_id, &version3), TxnErrorCode::TXN_OK);
    }

    ASSERT_EQ(version3, version2);
    ASSERT_NE(version3, version1);
}

TEST(MetaServiceOperationLogTest, DropIndexLog) {
    auto meta_service = get_meta_service(false);
    std::string instance_id = "drop_index_log";
    auto* sp = SyncPoint::get_instance();
    DORIS_CLOUD_DEFER {
        SyncPoint::get_instance()->clear_all_call_backs();
    };
    sp->set_call_back("get_instance_id", [&](auto&& args) {
        auto* ret = try_any_cast_ret<std::string>(args);
        ret->first = instance_id;
        ret->second = true;
    });
    sp->enable_processing();

    constexpr int64_t db_id = 123;
    constexpr int64_t table_id = 10001;
    constexpr int64_t index_id = 10002;

    {
        // write instance
        InstanceInfoPB instance_info;
        instance_info.set_instance_id(instance_id);
        instance_info.set_multi_version_status(MULTI_VERSION_WRITE_ONLY);
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        txn->put(instance_key(instance_id), instance_info.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        meta_service->resource_mgr()->refresh_instance(instance_id);
        ASSERT_TRUE(meta_service->resource_mgr()->is_version_write_enabled(instance_id));
    }

    {
        // Prepare index 0,1,2,3
        brpc::Controller ctrl;
        IndexRequest req;
        IndexResponse res;
        req.set_db_id(db_id);
        req.set_table_id(table_id);
        req.add_index_ids(index_id);
        req.add_index_ids(index_id + 1);
        req.add_index_ids(index_id + 2);
        req.add_index_ids(index_id + 3);
        meta_service->prepare_index(&ctrl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().DebugString();
    }

    {
        // Commit index 2,3
        brpc::Controller ctrl;
        IndexRequest req;
        IndexResponse res;
        req.set_db_id(db_id);
        req.set_table_id(table_id);
        req.add_index_ids(index_id + 2);
        req.add_index_ids(index_id + 3);
        meta_service->commit_index(&ctrl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().DebugString();
    }

    auto txn_kv = meta_service->txn_kv();
    size_t num_logs = count_range(txn_kv.get(), versioned::log_key(instance_id),
                                  versioned::log_key(instance_id) + "\xFF");

    {
        // Drop index 0
        brpc::Controller ctrl;
        IndexRequest req;
        IndexResponse res;
        req.set_db_id(db_id);
        req.set_table_id(table_id);
        req.add_index_ids(index_id);
        meta_service->drop_index(&ctrl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().DebugString();

        // No operation log are generated for drop index 0 (it is not committed).
        size_t new_num_logs = count_range(txn_kv.get(), versioned::log_key(instance_id),
                                          versioned::log_key(instance_id) + "\xFF");
        ASSERT_EQ(new_num_logs, num_logs)
                << "Expected no new operation logs for drop index 0, but found "
                << new_num_logs - num_logs << dump_range(txn_kv.get());

        // The recycle index key must exists.
        std::string recycle_key = recycle_index_key({instance_id, index_id});
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string value;
        TxnErrorCode err = txn->get(recycle_key, &value);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
    }

    {
        // Drop index 1,2, it should generate operation logs.
        brpc::Controller ctrl;
        IndexRequest req;
        IndexResponse res;
        req.set_db_id(db_id);
        req.set_table_id(table_id);
        req.add_index_ids(index_id + 1);
        req.add_index_ids(index_id + 2);
        meta_service->drop_index(&ctrl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().DebugString();
        // Check that new operation logs are generated.
        size_t new_num_logs = count_range(txn_kv.get(), versioned::log_key(instance_id),
                                          versioned::log_key(instance_id) + "\xFF");
        ASSERT_GT(new_num_logs, num_logs)
                << "Expected new operation logs for drop index 1,2, but found "
                << new_num_logs - num_logs << dump_range(txn_kv.get());
        num_logs = new_num_logs;

        // The recycle index 1 key must exists.
        std::string recycle_key = recycle_index_key({instance_id, index_id + 1});
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string value;
        TxnErrorCode err = txn->get(recycle_key, &value);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);

        // The recycle index 2 key should not be exists.
        recycle_key = recycle_index_key({instance_id, index_id + 2});
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        err = txn->get(recycle_key, &value);
        ASSERT_EQ(err, TxnErrorCode::TXN_KEY_NOT_FOUND)
                << "Expected recycle index key to not exist, but found it: " << hex(recycle_key)
                << " with value: " << escape_hex(value);
    }

    {
        // Drop index 3, it should generate operation logs.
        brpc::Controller ctrl;
        IndexRequest req;
        IndexResponse res;
        req.set_db_id(db_id);
        req.set_table_id(table_id);
        req.add_index_ids(index_id + 3);
        meta_service->drop_index(&ctrl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().DebugString();
        // Check that new operation logs are generated.
        size_t new_num_logs = count_range(txn_kv.get(), versioned::log_key(instance_id),
                                          versioned::log_key(instance_id) + "\xFF");
        ASSERT_GT(new_num_logs, num_logs)
                << "Expected new operation logs for drop index 3, but found "
                << new_num_logs - num_logs << dump_range(txn_kv.get());
        num_logs = new_num_logs;

        // The recycle index key should not be exists.
        std::string recycle_key = recycle_index_key({instance_id, index_id + 3});
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string value;
        TxnErrorCode err = txn->get(recycle_key, &value);
        ASSERT_EQ(err, TxnErrorCode::TXN_KEY_NOT_FOUND)
                << "Expected recycle index key to not exist, but found it: " << hex(recycle_key)
                << " with value: " << escape_hex(value);
    }

    {
        // Verify table version not exists
        Versionstamp version;
        MetaReader meta_reader(instance_id, txn_kv.get());
        ASSERT_EQ(meta_reader.get_table_version(table_id, &version),
                  TxnErrorCode::TXN_KEY_NOT_FOUND);
    }

    {
        // verify last drop index log
        Versionstamp version;
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string log_key = versioned::log_key({instance_id});
        std::string value;
        ASSERT_EQ(versioned_get(txn.get(), log_key, &version, &value), TxnErrorCode::TXN_OK);
        OperationLogPB operation_log;
        ASSERT_TRUE(operation_log.ParseFromString(value));
        ASSERT_TRUE(operation_log.has_drop_index());
        ASSERT_EQ(operation_log.drop_index().index_ids_size(), 1);
        ASSERT_EQ(operation_log.drop_index().index_ids(0), index_id + 3);
    }
}

TEST(MetaServiceOperationLogTest, CommitTxn) {
    auto meta_service = get_meta_service(false);
    std::string instance_id = "commit_txn_versioned_write";
    auto* sp = SyncPoint::get_instance();
    DORIS_CLOUD_DEFER {
        SyncPoint::get_instance()->clear_all_call_backs();
    };
    sp->set_call_back("get_instance_id", [&](auto&& args) {
        auto* ret = try_any_cast_ret<std::string>(args);
        ret->first = instance_id;
        ret->second = true;
    });
    sp->enable_processing();

    constexpr int64_t db_id = 123;
    constexpr int64_t table_id = 10001;
    constexpr int64_t partition_id = 10003;
    constexpr int64_t tablet_id_base = 8113;

    {
        // write instance with versioned write enabled
        InstanceInfoPB instance_info;
        instance_info.set_instance_id(instance_id);
        instance_info.set_multi_version_status(MULTI_VERSION_WRITE_ONLY);
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        txn->put(instance_key(instance_id), instance_info.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        meta_service->resource_mgr()->refresh_instance(instance_id);
        ASSERT_TRUE(meta_service->resource_mgr()->is_version_write_enabled(instance_id));
    }

    int64_t txn_id = -1;
    const std::string& label = "test_versioned_write_commit";

    // begin txn
    {
        brpc::Controller cntl;
        BeginTxnRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        TxnInfoPB txn_info_pb;
        txn_info_pb.set_db_id(db_id);
        txn_info_pb.set_label(label);
        txn_info_pb.add_table_ids(table_id);
        txn_info_pb.set_timeout_ms(36000);
        req.mutable_txn_info()->CopyFrom(txn_info_pb);
        BeginTxnResponse res;
        meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        txn_id = res.txn_id();
    }

    // create tablets and rowsets
    for (int i = 0; i < 3; ++i) {
        create_tablet(meta_service.get(), table_id, 1235, partition_id, tablet_id_base + i);
        auto tmp_rowset = create_rowset(txn_id, tablet_id_base + i, partition_id, 2, 100);
        CreateRowsetResponse res;
        LOG(INFO) << "Creating rowset for tablet_id=" << (tablet_id_base + i)
                  << ", partition_id=" << partition_id << ", txn_id=" << txn_id
                  << ", rowset=" << tmp_rowset.ShortDebugString();

        commit_rowset(meta_service.get(), tmp_rowset, res);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // commit txn with versioned write
    Versionstamp commit_version;
    {
        brpc::Controller cntl;
        CommitTxnRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_db_id(db_id);
        req.set_txn_id(txn_id);
        CommitTxnResponse res;
        meta_service->commit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                 &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    auto txn_kv = meta_service->txn_kv();
    Versionstamp version;

    {
        // Verify txn_info has versioned_write flag set
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string txn_key = txn_info_key({instance_id, db_id, txn_id});
        std::string txn_val;
        ASSERT_EQ(txn->get(txn_key, &txn_val), TxnErrorCode::TXN_OK);

        TxnInfoPB txn_info;
        ASSERT_TRUE(txn_info.ParseFromString(txn_val));
        ASSERT_TRUE(txn_info.has_versioned_write());
        ASSERT_TRUE(txn_info.versioned_write());
    }

    {
        // Verify no recycle txn key is written immediately for versioned write
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string recycle_key = recycle_txn_key({instance_id, db_id, txn_id});
        std::string recycle_val;
        ASSERT_EQ(txn->get(recycle_key, &recycle_val), TxnErrorCode::TXN_KEY_NOT_FOUND);
    }

    {
        // Verify table version exists
        MetaReader meta_reader(instance_id, txn_kv.get());
        ASSERT_EQ(meta_reader.get_table_version(table_id, &version), TxnErrorCode::TXN_OK);
        commit_version = version;
    }

    {
        // Verify versioned tablet stats are written
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

        for (int i = 0; i < 3; ++i) {
            int64_t tablet_id = tablet_id_base + i;
            std::string versioned_stats_key =
                    versioned::tablet_load_stats_key({instance_id, tablet_id});
            std::string versioned_stats_val;
            ASSERT_EQ(versioned_get(txn.get(), versioned_stats_key, &version, &versioned_stats_val),
                      TxnErrorCode::TXN_OK);

            TabletStatsPB versioned_stats;
            ASSERT_TRUE(versioned_stats.ParseFromString(versioned_stats_val));
            ASSERT_GT(versioned_stats.num_rows(), 0);
            ASSERT_GT(versioned_stats.data_size(), 0);
            ASSERT_GT(versioned_stats.num_rowsets(), 0);
        }
    }

    {
        // Verify versioned rowset meta are written
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

        for (int i = 0; i < 3; ++i) {
            int64_t tablet_id = tablet_id_base + i;
            // Construct versioned rowset meta key for version 2
            std::string versioned_rowset_key =
                    versioned::meta_rowset_load_key({instance_id, tablet_id, 2});

            // Try to get the versioned rowset meta using document_get
            RowsetMetaCloudPB versioned_rowset_meta;
            auto ret = versioned::document_get(txn.get(), versioned_rowset_key,
                                               &versioned_rowset_meta, &version);
            ASSERT_EQ(ret, TxnErrorCode::TXN_OK)
                    << "Failed to get versioned rowset meta for tablet " << tablet_id;

            ASSERT_EQ(versioned_rowset_meta.tablet_id(), tablet_id);
            ASSERT_EQ(versioned_rowset_meta.partition_id(), partition_id);
            ASSERT_GT(versioned_rowset_meta.num_rows(), 0);
        }
    }

    {
        // verify versioned partition version key
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string key = versioned::partition_version_key({instance_id, partition_id});
        std::string partition_version_val;
        Versionstamp version;
        ASSERT_EQ(versioned_get(txn.get(), key, &version, &partition_version_val),
                  TxnErrorCode::TXN_OK);
        VersionPB versioned_partition_version;
        ASSERT_TRUE(versioned_partition_version.ParseFromString(partition_version_val));
        ASSERT_EQ(version, commit_version);

        key = partition_version_key({instance_id, db_id, table_id, partition_id});
        ASSERT_EQ(txn->get(key, &partition_version_val), TxnErrorCode::TXN_OK);
        VersionPB partition_version;
        ASSERT_TRUE(partition_version.ParseFromString(partition_version_val));
        ASSERT_EQ(versioned_partition_version.version(), partition_version.version());
    }

    {
        // verify commit txn operation log
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string log_key = versioned::log_key({instance_id});
        std::string value;
        ASSERT_EQ(versioned_get(txn.get(), log_key, &version, &value), TxnErrorCode::TXN_OK);
        OperationLogPB operation_log;
        ASSERT_TRUE(operation_log.ParseFromString(value));
        ASSERT_TRUE(operation_log.has_commit_txn());

        const auto& commit_log = operation_log.commit_txn();
        ASSERT_EQ(commit_log.txn_id(), txn_id);
        ASSERT_TRUE(commit_log.has_recycle_txn());
        ASSERT_EQ(commit_log.recycle_txn().label(), label);
        ASSERT_GT(commit_log.tablet_to_partition_map_size(), 0);
        ASSERT_GT(commit_log.partition_version_map_size(), 0);
        ASSERT_EQ(commit_log.db_id(), db_id);

        // Verify tablet to partition mapping
        for (const auto& [tablet_id, partition_id_in_map] : commit_log.tablet_to_partition_map()) {
            ASSERT_EQ(partition_id_in_map, partition_id);
        }

        // Verify partition version mapping
        ASSERT_EQ(commit_log.partition_version_map_size(), 1);
        auto it = commit_log.partition_version_map().find(partition_id);
        ASSERT_NE(it, commit_log.partition_version_map().end());
        ASSERT_GT(it->second, 0);
    }

    ASSERT_EQ(version, commit_version);
}

TEST(MetaServiceOperationLogTest, CommitTxnEventually) {
    auto meta_service = get_meta_service(false);
    std::string instance_id = "commit_txn_eventually_versioned_write";
    auto* sp = SyncPoint::get_instance();
    DORIS_CLOUD_DEFER {
        SyncPoint::get_instance()->clear_all_call_backs();
    };
    sp->set_call_back("get_instance_id", [&](auto&& args) {
        auto* ret = try_any_cast_ret<std::string>(args);
        ret->first = instance_id;
        ret->second = true;
    });
    sp->enable_processing();

    constexpr int64_t db_id = 125;
    constexpr int64_t table_id = 10003;
    constexpr int64_t partition_id = 10005;
    constexpr int64_t tablet_id_base = 8115;

    {
        // write instance with versioned write enabled
        InstanceInfoPB instance_info;
        instance_info.set_instance_id(instance_id);
        instance_info.set_multi_version_status(MULTI_VERSION_WRITE_ONLY);
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        txn->put(instance_key(instance_id), instance_info.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        meta_service->resource_mgr()->refresh_instance(instance_id);
        ASSERT_TRUE(meta_service->resource_mgr()->is_version_write_enabled(instance_id));
    }

    int64_t txn_id = -1;
    const std::string& label = "test_eventually_versioned_write_commit";

    // begin txn
    {
        brpc::Controller cntl;
        BeginTxnRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        TxnInfoPB txn_info_pb;
        txn_info_pb.set_db_id(db_id);
        txn_info_pb.set_label(label);
        txn_info_pb.add_table_ids(table_id);
        txn_info_pb.set_timeout_ms(36000);
        req.mutable_txn_info()->CopyFrom(txn_info_pb);
        BeginTxnResponse res;
        meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        txn_id = res.txn_id();
    }

    create_tablet(meta_service.get(), table_id, 1237, partition_id, tablet_id_base);
    auto tmp_rowset = create_rowset(txn_id, tablet_id_base, partition_id, 1, 100);
    CreateRowsetResponse res;
    commit_rowset(meta_service.get(), tmp_rowset, res);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

    // Force eventual commit path by setting max_txn_bytes to a small value
    int64_t old_max_txn_bytes = config::max_txn_commit_byte;
    config::max_txn_commit_byte = 100;

    // commit txn with versioned write (should use eventual commit)
    {
        brpc::Controller cntl;
        CommitTxnRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_db_id(db_id);
        req.set_txn_id(txn_id);
        CommitTxnResponse res;
        meta_service->commit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                 &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // Restore config
    config::max_txn_commit_byte = old_max_txn_bytes;

    auto txn_kv = meta_service->txn_kv();
    Versionstamp version;

    {
        // Verify txn is marked as versioned_write
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string txn_key = txn_info_key({instance_id, db_id, txn_id});
        std::string txn_val;
        ASSERT_EQ(txn->get(txn_key, &txn_val), TxnErrorCode::TXN_OK);

        TxnInfoPB txn_info;
        ASSERT_TRUE(txn_info.ParseFromString(txn_val));
        ASSERT_TRUE(txn_info.has_versioned_write());
        ASSERT_TRUE(txn_info.versioned_write());
    }

    {
        // In lazy commit, recycle txn should not be written when versioned_write is true
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string recycle_key = recycle_txn_key({instance_id, db_id, txn_id});
        std::string recycle_val;
        ASSERT_EQ(txn->get(recycle_key, &recycle_val), TxnErrorCode::TXN_KEY_NOT_FOUND);
    }

    {
        // Verify table version exists
        MetaReader meta_reader(instance_id, txn_kv.get());
        ASSERT_EQ(meta_reader.get_table_version(table_id, &version), TxnErrorCode::TXN_OK);
    }

    // Store the commit versionstamp to verify consistency later
    Versionstamp commit_versionstamp;

    {
        // verify commit txn operation log is written in eventual commit
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string log_key = versioned::log_key({instance_id});
        std::string value;
        ASSERT_EQ(versioned_get(txn.get(), log_key, &commit_versionstamp, &value),
                  TxnErrorCode::TXN_OK);
        OperationLogPB operation_log;
        ASSERT_TRUE(operation_log.ParseFromString(value));
        ASSERT_TRUE(operation_log.has_commit_txn());

        const auto& commit_log = operation_log.commit_txn();
        ASSERT_EQ(commit_log.txn_id(), txn_id);
        ASSERT_TRUE(commit_log.has_recycle_txn());
        ASSERT_EQ(commit_log.recycle_txn().label(), label);
        ASSERT_GT(commit_log.tablet_to_partition_map_size(), 0);
        ASSERT_GT(commit_log.partition_version_map_size(), 0);
        ASSERT_EQ(commit_log.db_id(), db_id);
    }

    {
        // Verify versioned tablet stats are written with consistent versionstamp
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

        std::string versioned_stats_key =
                versioned::tablet_load_stats_key({instance_id, tablet_id_base});
        std::string versioned_stats_val;
        Versionstamp stats_versionstamp;
        ASSERT_EQ(versioned_get(txn.get(), versioned_stats_key, &stats_versionstamp,
                                &versioned_stats_val),
                  TxnErrorCode::TXN_OK);

        TabletStatsPB versioned_stats;
        ASSERT_TRUE(versioned_stats.ParseFromString(versioned_stats_val));
        ASSERT_GT(versioned_stats.num_rows(), 0);
        ASSERT_GT(versioned_stats.data_size(), 0);
        ASSERT_GT(versioned_stats.num_rowsets(), 0);

        // Verify versionstamp consistency with commit log
        ASSERT_EQ(stats_versionstamp, commit_versionstamp)
                << "Versioned tablet stats versionstamp should match commit log versionstamp";
    }

    {
        // verify versioned partition version key
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string key = versioned::partition_version_key({instance_id, partition_id});
        std::string partition_version_val;
        Versionstamp version;
        ASSERT_EQ(versioned_get(txn.get(), key, &version, &partition_version_val),
                  TxnErrorCode::TXN_OK);
        VersionPB versioned_partition_version;
        ASSERT_TRUE(versioned_partition_version.ParseFromString(partition_version_val));
        ASSERT_EQ(version, commit_versionstamp);

        key = partition_version_key({instance_id, db_id, table_id, partition_id});
        ASSERT_EQ(txn->get(key, &partition_version_val), TxnErrorCode::TXN_OK);
        VersionPB partition_version;
        ASSERT_TRUE(partition_version.ParseFromString(partition_version_val));
        ASSERT_EQ(versioned_partition_version.version(), partition_version.version());
    }

    {
        // Verify versioned rowset meta are written with consistent versionstamp
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

        // Construct versioned rowset meta key for version 2 (first committed version after initial)
        std::string versioned_rowset_key =
                versioned::meta_rowset_load_key({instance_id, tablet_id_base, 2});

        // Try to get the versioned rowset meta using document_get
        RowsetMetaCloudPB versioned_rowset_meta;
        Versionstamp rowset_versionstamp;
        auto ret = versioned::document_get(txn.get(), versioned_rowset_key, &versioned_rowset_meta,
                                           &rowset_versionstamp);
        ASSERT_EQ(ret, TxnErrorCode::TXN_OK)
                << "Failed to get versioned rowset meta for tablet " << tablet_id_base;

        ASSERT_EQ(versioned_rowset_meta.tablet_id(), tablet_id_base);
        ASSERT_EQ(versioned_rowset_meta.partition_id(), partition_id);
        ASSERT_GT(versioned_rowset_meta.num_rows(), 0);

        // Verify versionstamp consistency with commit log
        ASSERT_EQ(rowset_versionstamp, commit_versionstamp)
                << "Versioned rowset meta versionstamp should match commit log versionstamp";
    }
}

TEST(MetaServiceOperationLogTest, CommitTxnWithSubTxn) {
    auto meta_service = get_meta_service(false);
    std::string instance_id = "commit_txn_with_sub_txn_versioned_write";
    auto* sp = SyncPoint::get_instance();
    DORIS_CLOUD_DEFER {
        SyncPoint::get_instance()->clear_all_call_backs();
    };
    sp->set_call_back("get_instance_id", [&](auto&& args) {
        auto* ret = try_any_cast_ret<std::string>(args);
        ret->first = instance_id;
        ret->second = true;
    });
    sp->enable_processing();

    constexpr int64_t db_id = 126;
    constexpr int64_t table_id = 10004;
    constexpr int64_t partition_id = 10006;
    constexpr int64_t tablet_id_base = 8116;

    {
        // write instance with versioned write enabled
        InstanceInfoPB instance_info;
        instance_info.set_instance_id(instance_id);
        instance_info.set_multi_version_status(MULTI_VERSION_WRITE_ONLY);
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        txn->put(instance_key(instance_id), instance_info.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        meta_service->resource_mgr()->refresh_instance(instance_id);
        ASSERT_TRUE(meta_service->resource_mgr()->is_version_write_enabled(instance_id));
    }

    int64_t txn_id = -1;
    const std::string& label = "test_sub_txn_versioned_write_commit";

    // begin txn
    {
        brpc::Controller cntl;
        BeginTxnRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        TxnInfoPB txn_info_pb;
        txn_info_pb.set_db_id(db_id);
        txn_info_pb.set_label(label);
        txn_info_pb.add_table_ids(table_id);
        txn_info_pb.set_timeout_ms(36000);
        req.mutable_txn_info()->CopyFrom(txn_info_pb);
        BeginTxnResponse res;
        meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        txn_id = res.txn_id();
    }

    // begin sub txn
    int64_t sub_txn_id = -1;
    {
        brpc::Controller cntl;
        BeginSubTxnRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_txn_id(txn_id);
        req.set_db_id(db_id);
        req.set_label(label);
        req.set_sub_txn_num(0);
        req.add_table_ids(table_id);
        BeginSubTxnResponse res;
        meta_service->begin_sub_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        sub_txn_id = res.sub_txn_id();
    }

    // create tablets and rowsets for sub txn
    for (int i = 0; i < 2; ++i) {
        create_tablet(meta_service.get(), table_id, 1238, partition_id, tablet_id_base + i);
        auto tmp_rowset = create_rowset(sub_txn_id, tablet_id_base + i, partition_id, 1, 100);
        CreateRowsetResponse res;
        commit_rowset(meta_service.get(), tmp_rowset, res);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // commit txn with sub txn and versioned write
    CommitTxnResponse commit_res;
    {
        brpc::Controller cntl;
        CommitTxnRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_db_id(db_id);
        req.set_txn_id(txn_id);
        req.set_is_txn_load(true);
        // Add sub txn info
        SubTxnInfo* sub_txn_info = req.add_sub_txn_infos();
        sub_txn_info->set_sub_txn_id(sub_txn_id);
        meta_service->commit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                 &commit_res, nullptr);
        ASSERT_EQ(commit_res.status().code(), MetaServiceCode::OK);
    }

    auto txn_kv = meta_service->txn_kv();
    Versionstamp commit_versionstamp;

    {
        // Verify txn_info has versioned_write flag set
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string txn_key = txn_info_key({instance_id, db_id, txn_id});
        std::string txn_val;
        ASSERT_EQ(txn->get(txn_key, &txn_val), TxnErrorCode::TXN_OK);

        TxnInfoPB txn_info;
        ASSERT_TRUE(txn_info.ParseFromString(txn_val));
        ASSERT_TRUE(txn_info.has_versioned_write());
        ASSERT_TRUE(txn_info.versioned_write());
    }

    {
        // Verify no recycle txn key is written immediately for versioned write
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string recycle_key = recycle_txn_key({instance_id, db_id, txn_id});
        std::string recycle_val;
        ASSERT_EQ(txn->get(recycle_key, &recycle_val), TxnErrorCode::TXN_KEY_NOT_FOUND);
    }

    {
        // Verify table version exists
        MetaReader meta_reader(instance_id, txn_kv.get());
        ASSERT_EQ(meta_reader.get_table_version(table_id, &commit_versionstamp),
                  TxnErrorCode::TXN_OK);
    }

    {
        // Verify versioned tablet stats are written
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

        for (int i = 0; i < 2; ++i) {
            int64_t tablet_id = tablet_id_base + i;
            std::string versioned_stats_key =
                    versioned::tablet_load_stats_key({instance_id, tablet_id});
            std::string versioned_stats_val;
            ASSERT_EQ(versioned_get(txn.get(), versioned_stats_key, &commit_versionstamp,
                                    &versioned_stats_val),
                      TxnErrorCode::TXN_OK);

            TabletStatsPB versioned_stats;
            ASSERT_TRUE(versioned_stats.ParseFromString(versioned_stats_val));
            ASSERT_GT(versioned_stats.num_rows(), 0);
            ASSERT_GT(versioned_stats.data_size(), 0);
            ASSERT_GT(versioned_stats.num_rowsets(), 0);
        }
    }

    {
        // Verify versioned rowset meta are written
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

        // Get the actual version from the commit response
        ASSERT_GT(commit_res.versions_size(), 0);
        int64_t actual_version = commit_res.versions(0);

        for (int i = 0; i < 2; ++i) {
            int64_t tablet_id = tablet_id_base + i;
            // Construct versioned rowset meta key using the actual version
            std::string versioned_rowset_key =
                    versioned::meta_rowset_load_key({instance_id, tablet_id, actual_version});

            // Try to get the versioned rowset meta using document_get
            RowsetMetaCloudPB versioned_rowset_meta;
            auto ret = versioned::document_get(txn.get(), versioned_rowset_key,
                                               &versioned_rowset_meta, &commit_versionstamp);
            ASSERT_EQ(ret, TxnErrorCode::TXN_OK)
                    << "Failed to get versioned rowset meta for tablet " << tablet_id
                    << " with version " << actual_version;

            ASSERT_EQ(versioned_rowset_meta.tablet_id(), tablet_id);
            ASSERT_EQ(versioned_rowset_meta.partition_id(), partition_id);
            ASSERT_GT(versioned_rowset_meta.num_rows(), 0);
        }
    }

    {
        // verify versioned partition version key
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string key = versioned::partition_version_key({instance_id, partition_id});
        std::string partition_version_val;
        Versionstamp versionstamp;
        ASSERT_EQ(versioned_get(txn.get(), key, &versionstamp, &partition_version_val),
                  TxnErrorCode::TXN_OK);
        VersionPB versioned_partition_version;
        ASSERT_TRUE(versioned_partition_version.ParseFromString(partition_version_val));
        ASSERT_EQ(versionstamp, commit_versionstamp);

        key = partition_version_key({instance_id, db_id, table_id, partition_id});
        ASSERT_EQ(txn->get(key, &partition_version_val), TxnErrorCode::TXN_OK);
        VersionPB partition_version;
        ASSERT_TRUE(partition_version.ParseFromString(partition_version_val));
        ASSERT_EQ(versioned_partition_version.version(), partition_version.version());
    }

    {
        // verify commit txn operation log for sub txn
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string log_key = versioned::log_key({instance_id});
        std::string value;
        ASSERT_EQ(versioned_get(txn.get(), log_key, &commit_versionstamp, &value),
                  TxnErrorCode::TXN_OK);
        OperationLogPB operation_log;
        ASSERT_TRUE(operation_log.ParseFromString(value));
        ASSERT_TRUE(operation_log.has_commit_txn());

        const auto& commit_log = operation_log.commit_txn();
        ASSERT_EQ(commit_log.txn_id(), txn_id);
        ASSERT_TRUE(commit_log.has_recycle_txn());
        ASSERT_EQ(commit_log.recycle_txn().label(), label);
        ASSERT_GT(commit_log.tablet_to_partition_map_size(), 0);
        ASSERT_GT(commit_log.partition_version_map_size(), 0);
        ASSERT_EQ(commit_log.db_id(), db_id);

        // Verify tablet to partition mapping
        for (const auto& [tablet_id, partition_id_in_map] : commit_log.tablet_to_partition_map()) {
            ASSERT_EQ(partition_id_in_map, partition_id);
        }

        // Verify partition version mapping
        ASSERT_EQ(commit_log.partition_version_map_size(), 1);
        auto it = commit_log.partition_version_map().find(partition_id);
        ASSERT_NE(it, commit_log.partition_version_map().end());
        ASSERT_GT(it->second, 0);
    }
}

TEST(MetaServiceOperationLogTest, UpdateVersionedTabletMeta) {
    auto meta_service = get_meta_service(false);
    std::string instance_id = "commit_partition_log";
    std::string cloud_unique_id = "1:" + instance_id + ":1";

    auto* sp = SyncPoint::get_instance();
    DORIS_CLOUD_DEFER {
        SyncPoint::get_instance()->clear_all_call_backs();
    };
    sp->set_call_back("get_instance_id", [&](auto&& args) {
        auto* ret = try_any_cast_ret<std::string>(args);
        ret->first = instance_id;
        ret->second = true;
    });
    sp->enable_processing();

    constexpr int64_t table_id = 10001;
    constexpr int64_t index_id = 10002;
    constexpr int64_t partition_id = 10003;
    constexpr int64_t tablet_id1 = 10004;
    constexpr int64_t tablet_id2 = 10005;

    {
        // write instance
        InstanceInfoPB instance_info;
        instance_info.set_instance_id(instance_id);
        instance_info.set_multi_version_status(MULTI_VERSION_WRITE_ONLY);
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        txn->put(instance_key(instance_id), instance_info.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        meta_service->resource_mgr()->refresh_instance(instance_id);
        ASSERT_TRUE(meta_service->resource_mgr()->is_version_write_enabled(instance_id));
    }

    {
        // Create tablets
        create_tablet(meta_service.get(), table_id, index_id, partition_id, tablet_id1);
        create_tablet(meta_service.get(), table_id, index_id, partition_id, tablet_id2);
    }

    // Update tablets
    {
        brpc::Controller cntl;
        UpdateTabletRequest req;
        UpdateTabletResponse resp;
        req.set_cloud_unique_id(cloud_unique_id);
        TabletMetaInfoPB* tablet_meta_info = req.add_tablet_meta_infos();
        tablet_meta_info->set_tablet_id(tablet_id1);
        tablet_meta_info->set_ttl_seconds(300);
        tablet_meta_info = req.add_tablet_meta_infos();
        tablet_meta_info->set_tablet_id(tablet_id2);
        tablet_meta_info->set_ttl_seconds(3000);
        meta_service->update_tablet(&cntl, &req, &resp, nullptr);
        ASSERT_EQ(resp.status().code(), MetaServiceCode::OK);
    }

    // Verify versioned tablet meta keys exist and have same commit_versionstamp
    Versionstamp versionstamp1;
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);

        // Check versioned tablet meta for tablet_id1
        std::string tablet_meta_key1 = versioned::meta_tablet_key({instance_id, tablet_id1});
        doris::TabletMetaCloudPB tablet_meta1;
        TxnErrorCode err =
                versioned::document_get(txn.get(), tablet_meta_key1, &tablet_meta1, &versionstamp1);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        EXPECT_EQ(tablet_meta1.ttl_seconds(), 300);
    }

    // Check versioned tablet meta for tablet_id2
    Versionstamp versionstamp2;
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);

        std::string tablet_meta_key2 = versioned::meta_tablet_key({instance_id, tablet_id2});
        doris::TabletMetaCloudPB tablet_meta2;
        TxnErrorCode err =
                versioned::document_get(txn.get(), tablet_meta_key2, &tablet_meta2, &versionstamp2);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        EXPECT_EQ(tablet_meta2.ttl_seconds(), 3000);
    }

    // Check operation log exists
    Versionstamp log_versionstamp;
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);

        std::string log_key = versioned::log_key(instance_id);
        OperationLogPB operation_log;
        TxnErrorCode err =
                versioned::document_get(txn.get(), log_key, &operation_log, &log_versionstamp);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        ASSERT_TRUE(operation_log.has_update_tablet());
        EXPECT_EQ(operation_log.update_tablet().tablet_ids_size(), 2);
        EXPECT_TRUE(std::find(operation_log.update_tablet().tablet_ids().begin(),
                              operation_log.update_tablet().tablet_ids().end(),
                              tablet_id1) != operation_log.update_tablet().tablet_ids().end());
        EXPECT_TRUE(std::find(operation_log.update_tablet().tablet_ids().begin(),
                              operation_log.update_tablet().tablet_ids().end(),
                              tablet_id2) != operation_log.update_tablet().tablet_ids().end());
    }

    // Verify all versioned keys have the same commit_versionstamp
    EXPECT_EQ(versionstamp1, versionstamp2);
    EXPECT_EQ(versionstamp1, log_versionstamp);
    EXPECT_EQ(versionstamp2, log_versionstamp);
}

TEST(MetaServiceOperationLogTest, CompactionLog) {
    auto meta_service = get_meta_service(false);
    std::string instance_id = "compaction_log_test";
    auto* sp = SyncPoint::get_instance();
    DORIS_CLOUD_DEFER {
        SyncPoint::get_instance()->clear_all_call_backs();
    };
    sp->set_call_back("get_instance_id", [&](auto&& args) {
        auto* ret = try_any_cast_ret<std::string>(args);
        ret->first = instance_id;
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
        instance_info.set_instance_id(instance_id);
        instance_info.set_multi_version_status(MULTI_VERSION_WRITE_ONLY);
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        txn->put(instance_key(instance_id), instance_info.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        meta_service->resource_mgr()->refresh_instance(instance_id);
        ASSERT_TRUE(meta_service->resource_mgr()->is_version_write_enabled(instance_id));
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
            auto rowset_key = meta_rowset_key({instance_id, tablet_id, rowset.end_version()});
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
        auto tmp_rowset_key = meta_rowset_tmp_key({instance_id, txn_id, tablet_id});
        auto tmp_rowset_val = output_rowset.SerializeAsString();
        txn->put(tmp_rowset_key, tmp_rowset_val);

        // Create initial tablet stats
        TabletStatsPB initial_stats;
        initial_stats.set_num_rows(150);
        initial_stats.set_data_size(150 * 50);
        initial_stats.set_num_rowsets(3);
        initial_stats.set_num_segments(3);
        initial_stats.set_index_size(100);
        initial_stats.set_segment_size(200);
        initial_stats.set_cumulative_point(1);

        auto stats_key =
                stats_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
        auto stats_val = initial_stats.SerializeAsString();
        txn->put(stats_key, stats_val);

        // Create tablet compact stats for versioned storage
        initial_stats.set_num_rows(-150);
        initial_stats.set_data_size(-150 * 50);
        initial_stats.set_num_rowsets(-3);
        initial_stats.set_num_segments(-3);
        initial_stats.set_index_size(-100);
        initial_stats.set_segment_size(-200);
        initial_stats.set_cumulative_point(1);
        auto tablet_compact_stats_key =
                versioned::tablet_compact_stats_key({instance_id, tablet_id});
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

    size_t num_logs_before = count_range(txn_kv.get(), versioned::log_key(instance_id),
                                         versioned::log_key(instance_id) + "\xFF");

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

        // This will trigger process_compaction_job internally
        meta_service->finish_tablet_job(&cntl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // Verify that operation log was created
    size_t num_logs_after = count_range(txn_kv.get(), versioned::log_key(instance_id),
                                        versioned::log_key(instance_id) + "\xFF");
    ASSERT_GT(num_logs_after, num_logs_before)
            << "Expected new compaction operation log, but found no new logs";

    Versionstamp log_version;
    {
        // Verify compaction operation log content
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string log_key = versioned::log_key({instance_id});
        std::string value;
        ASSERT_EQ(versioned_get(txn.get(), log_key, &log_version, &value), TxnErrorCode::TXN_OK);

        OperationLogPB operation_log;
        ASSERT_TRUE(operation_log.ParseFromString(value));
        ASSERT_TRUE(operation_log.has_compaction());

        const auto& compaction_log = operation_log.compaction();
        ASSERT_EQ(compaction_log.tablet_id(), tablet_id);
        ASSERT_EQ(compaction_log.start_version(), 2);
        ASSERT_EQ(compaction_log.end_version(), 4);
        ASSERT_EQ(compaction_log.recycle_rowsets_size(), 3);

        // Verify recycle rowsets content - these should be the input rowsets marked for recycling
        for (int i = 0; i < compaction_log.recycle_rowsets_size(); ++i) {
            const auto& recycle_rowset = compaction_log.recycle_rowsets(i);
            ASSERT_EQ(recycle_rowset.type(), RecycleRowsetPB::COMPACT);
            ASSERT_EQ(recycle_rowset.rowset_meta().tablet_id(), tablet_id);
            ASSERT_EQ(recycle_rowset.rowset_meta().partition_id(), partition_id);
            ASSERT_GT(recycle_rowset.creation_time(), 0);

            // Verify this matches one of our input rowsets
            bool found_matching_rowset = false;
            for (const auto& input_rowset : input_rowsets) {
                if (recycle_rowset.rowset_meta().rowset_id_v2() == input_rowset.rowset_id_v2()) {
                    found_matching_rowset = true;
                    ASSERT_EQ(recycle_rowset.rowset_meta().start_version(),
                              input_rowset.start_version());
                    ASSERT_EQ(recycle_rowset.rowset_meta().end_version(),
                              input_rowset.end_version());
                    break;
                }
            }
            ASSERT_TRUE(found_matching_rowset) << "Recycle rowset should match an input rowset";
        }
    }

    // Verify tablet compact stats were updated
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

        std::string tablet_compact_stats_key =
                versioned::tablet_compact_stats_key({instance_id, tablet_id});
        std::string tablet_compact_stats_value;
        Versionstamp* versionstamp = nullptr;
        ASSERT_EQ(versioned_get(txn.get(), tablet_compact_stats_key, versionstamp,
                                &tablet_compact_stats_value),
                  TxnErrorCode::TXN_OK);

        TabletStatsPB compact_stats;
        ASSERT_TRUE(compact_stats.ParseFromString(tablet_compact_stats_value));
        ASSERT_EQ(compact_stats.cumulative_compaction_cnt(), 1);
        ASSERT_EQ(compact_stats.cumulative_point(), 5);
        ASSERT_GT(compact_stats.last_cumu_compaction_time_ms(), 0);
        // Check that rowset count decreased: 3 input -> 1 output = -2
        ASSERT_EQ(compact_stats.num_rowsets(), -5);    // - 3 - 3 + 1 = -5
        ASSERT_EQ(compact_stats.data_size(), -10000);  // - 7500 - 7500 + 5000 = -10000
        ASSERT_EQ(compact_stats.num_rows(), -200);     // - 150 - 150 + 100 = -200
        ASSERT_EQ(compact_stats.num_segments(), -5);   // - 3 - 3 + 1 = -5
        ASSERT_EQ(compact_stats.index_size(), -150);   // - 200 - 200 + 50 = -150
        ASSERT_EQ(compact_stats.segment_size(), -300); // - 200 - 200 + 100 = -300
    }

    // Verify input rowsets were removed and output rowset was created correctly
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

        // All original input rowsets (versions 2, 3, 4) should be deleted
        for (const auto& rowset : input_rowsets) {
            if (rowset.end_version() < 4) {
                auto rowset_key = meta_rowset_key({instance_id, tablet_id, rowset.end_version()});
                std::string rowset_value;
                auto result = txn->get(rowset_key, &rowset_value);
                EXPECT_EQ(result, TxnErrorCode::TXN_KEY_NOT_FOUND)
                        << "Input rowset version " << rowset.end_version()
                        << " should have been removed after compaction";
            } else {
                // The new output rowset (2-4) should exist at version 4 (end_version)
                auto output_rowset_key =
                        meta_rowset_key({instance_id, tablet_id, output_end_version});
                std::string output_rowset_value;
                ASSERT_EQ(txn->get(output_rowset_key, &output_rowset_value), TxnErrorCode::TXN_OK)
                        << "Output rowset should exist at version " << output_end_version;
                // Verify this is the correct output rowset (2-4 range)
                doris::RowsetMetaCloudPB output_meta;
                ASSERT_TRUE(output_meta.ParseFromString(output_rowset_value));
                ASSERT_EQ(output_meta.rowset_id_v2(), output_rowset.rowset_id_v2())
                        << "Output rowset should have the correct rowset ID";
                ASSERT_EQ(output_meta.tablet_id(), tablet_id);
                ASSERT_EQ(output_meta.start_version(), output_start_version);
                ASSERT_EQ(output_meta.end_version(), output_end_version);
            }
        }

        // Verify versioned compact rowset exists for output version
        auto meta_rowset_compact_key =
                versioned::meta_rowset_compact_key({instance_id, tablet_id, output_end_version});
        std::string compact_rowset_value;
        Versionstamp* versionstamp = nullptr;
        ASSERT_EQ(versioned_get(txn.get(), meta_rowset_compact_key, versionstamp,
                                &compact_rowset_value),
                  TxnErrorCode::TXN_OK);

        // Check meta rowset compact value is not empty and valid
        ASSERT_FALSE(compact_rowset_value.empty())
                << "Meta rowset compact value should not be empty";

        // Parse and verify the compact rowset content
        doris::RowsetMetaCloudPB compact_meta;
        ASSERT_TRUE(compact_meta.ParseFromString(compact_rowset_value))
                << "Meta rowset compact value should be valid protobuf";
        ASSERT_EQ(compact_meta.tablet_id(), tablet_id)
                << "Meta rowset compact value tablet_id should match";
        ASSERT_EQ(compact_meta.end_version(), output_end_version)
                << "Meta rowset compact value version should match output version";
    }

    // Verify tmp rowset was removed
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

        auto tmp_rowset_key = meta_rowset_tmp_key({instance_id, txn_id, tablet_id});
        std::string tmp_rowset_value;
        ASSERT_EQ(txn->get(tmp_rowset_key, &tmp_rowset_value), TxnErrorCode::TXN_KEY_NOT_FOUND)
                << "Tmp rowset should have been removed after compaction";
    }
}

TEST(MetaServiceOperationLogTest, CompactionLogFirstTimeTabletStats) {
    auto meta_service = get_meta_service(false);
    std::string instance_id = "first_time_tablet_stats_test";
    auto* sp = SyncPoint::get_instance();
    DORIS_CLOUD_DEFER {
        SyncPoint::get_instance()->clear_all_call_backs();
    };
    sp->set_call_back("get_instance_id", [&](auto&& args) {
        auto* ret = try_any_cast_ret<std::string>(args);
        ret->first = instance_id;
        ret->second = true;
    });
    sp->enable_processing();

    constexpr int64_t table_id = 60001;
    constexpr int64_t index_id = 60002;
    constexpr int64_t partition_id = 60003;
    constexpr int64_t tablet_id = 60004;

    {
        // Create instance without multi-version settings first
        InstanceInfoPB instance_info;
        instance_info.set_instance_id(instance_id);
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        txn->put(instance_key(instance_id), instance_info.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
        meta_service->resource_mgr()->refresh_instance(instance_id);
    }

    {
        // Create tablet first
        create_tablet(meta_service.get(), table_id, index_id, partition_id, tablet_id);
    }

    {
        // Now enable versioned write after tablet creation
        InstanceInfoPB instance_info;
        instance_info.set_instance_id(instance_id);
        instance_info.set_multi_version_status(MULTI_VERSION_WRITE_ONLY);
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        txn->put(instance_key(instance_id), instance_info.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        meta_service->resource_mgr()->refresh_instance(instance_id);
        ASSERT_TRUE(meta_service->resource_mgr()->is_version_write_enabled(instance_id));
    }

    // Create input rowset for compaction (version 2)
    auto txn_kv = meta_service->txn_kv();
    constexpr int64_t input_version = 2;
    auto input_rowset = create_rowset(100, tablet_id, partition_id, input_version, 100);
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

        // Put input rowset
        auto rowset_key = meta_rowset_key({instance_id, tablet_id, input_version});
        auto rowset_val = input_rowset.SerializeAsString();
        txn->put(rowset_key, rowset_val);

        // Create initial tablet stats with some existing compaction data
        TabletStatsPB initial_stats;
        initial_stats.set_num_rows(100);
        initial_stats.set_data_size(5000);
        initial_stats.set_num_rowsets(2);
        initial_stats.set_num_segments(2);
        initial_stats.set_index_size(100);
        initial_stats.set_segment_size(200);
        initial_stats.set_cumulative_point(2);
        // Set some existing compaction stats to verify they are copied
        initial_stats.set_base_compaction_cnt(2);
        initial_stats.set_cumulative_compaction_cnt(3);
        initial_stats.set_full_compaction_cnt(1);
        initial_stats.set_last_base_compaction_time_ms(1000);
        initial_stats.set_last_cumu_compaction_time_ms(2000);
        initial_stats.set_last_full_compaction_time_ms(3000);

        auto stats_key =
                stats_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
        auto stats_val = initial_stats.SerializeAsString();
        txn->put(stats_key, stats_val);

        // DO NOT create versioned tablet_compact_stats - this will trigger TXN_KEY_NOT_FOUND

        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Create output rowset as tmp rowset
    constexpr int64_t txn_id = 60001;
    auto output_rowset = create_rowset(200, tablet_id, partition_id, input_version, 80);
    output_rowset.set_txn_id(txn_id);

    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

        // Put tmp rowset
        auto tmp_rowset_key = meta_rowset_tmp_key({instance_id, txn_id, tablet_id});
        auto tmp_rowset_val = output_rowset.SerializeAsString();
        txn->put(tmp_rowset_key, tmp_rowset_val);

        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Start compaction job
    const std::string job_id = "first_time_stats_compaction_job";
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
        compaction->set_base_compaction_cnt(3);
        compaction->set_cumulative_compaction_cnt(4);
        compaction->add_input_versions(input_version);
        compaction->add_input_versions(input_version);
        long now = time(nullptr);
        compaction->set_expiration(now + 12);
        compaction->set_lease(now + 3);
        meta_service->start_tablet_job(&cntl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // Finish compaction job - this should handle first-time tablet stats creation
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
        compaction->add_input_versions(input_version);
        compaction->add_input_versions(input_version);
        compaction->add_txn_id(txn_id);
        compaction->add_output_versions(input_version);
        compaction->add_output_rowset_ids(output_rowset.rowset_id_v2());
        compaction->set_output_cumulative_point(3);

        // Compaction stats
        compaction->set_size_input_rowsets(5000);
        compaction->set_index_size_input_rowsets(100);
        compaction->set_segment_size_input_rowsets(200);
        compaction->set_num_input_rows(100);
        compaction->set_num_input_rowsets(2);
        compaction->set_num_input_segments(2);

        compaction->set_size_output_rowsets(4000);
        compaction->set_index_size_output_rowsets(80);
        compaction->set_segment_size_output_rowsets(160);
        compaction->set_num_output_rows(80);
        compaction->set_num_output_rowsets(1);
        compaction->set_num_output_segments(1);

        meta_service->finish_tablet_job(&cntl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // Check that tablet compact stats were created with proper zero values for first-time write
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

        std::string tablet_compact_stats_key =
                versioned::tablet_compact_stats_key({instance_id, tablet_id});
        std::string tablet_compact_stats_value;
        Versionstamp* versionstamp = nullptr;
        ASSERT_EQ(versioned_get(txn.get(), tablet_compact_stats_key, versionstamp,
                                &tablet_compact_stats_value),
                  TxnErrorCode::TXN_OK)
                << "Tablet compact stats should be created by process_compaction_job";

        TabletStatsPB compact_stats;
        ASSERT_TRUE(compact_stats.ParseFromString(tablet_compact_stats_value));

        // Check TXN_KEY_NOT_FOUND logic: size fields should be cleared to zero for versioned stats
        ASSERT_EQ(compact_stats.num_rows(), -20) // 80 - 100 = -20
                << "num_rows should be -20 in TXN_KEY_NOT_FOUND logic";
        ASSERT_EQ(compact_stats.data_size(), -1000) // 4000 - 5000 = -1000
                << "data_size should be -1000 in TXN_KEY_NOT_FOUND logic";
        ASSERT_EQ(compact_stats.num_rowsets(), -1) // 1 - 2 = -1
                << "num_rowsets should be -1 in TXN_KEY_NOT_FOUND logic";
        ASSERT_EQ(compact_stats.num_segments(), -1) // 1 - 2 = -1
                << "num_segments should be -1 in TXN_KEY_NOT_FOUND logic";
        ASSERT_EQ(compact_stats.index_size(), -20) // 80 - 100 = -20
                << "index_size should be -20 in TXN_KEY_NOT_FOUND logic";
        ASSERT_EQ(compact_stats.segment_size(), -40) // 160 - 200 = -40
                << "segment_size should be -40 in TXN_KEY_NOT_FOUND logic";

        // Check that compaction count fields are copied from initial tablet stats
        ASSERT_EQ(compact_stats.base_compaction_cnt(), 2)
                << "Base compaction count should be copied from initial tablet stats";
        ASSERT_EQ(compact_stats.cumulative_compaction_cnt(), 4)
                << "Cumulative compaction count should be incremented from 3 to 4";
        ASSERT_EQ(compact_stats.full_compaction_cnt(), 1)
                << "Full compaction count should be copied from initial tablet stats";

        // Check that time fields are copied from initial tablet stats
        ASSERT_EQ(compact_stats.last_base_compaction_time_ms(), 1000)
                << "Last base compaction time should be copied from initial tablet stats";
        ASSERT_GT(compact_stats.last_cumu_compaction_time_ms(), 2000)
                << "Last cumulative compaction time should be updated after compaction";
        ASSERT_EQ(compact_stats.last_full_compaction_time_ms(), 3000)
                << "Last full compaction time should be copied from initial tablet stats";

        // Check that cumulative_point is properly set
        ASSERT_EQ(compact_stats.cumulative_point(), 3);
    }
}

} // namespace doris::cloud
