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
#include <gen_cpp/cloud.pb.h>
#include <gtest/gtest.h>

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <initializer_list>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "common/config.h"
#include "common/defer.h"
#include "common/lexical_util.h"
#include "cpp/sync_point.h"
#include "meta-service/meta_service.h"
#include "meta-store/keys.h"
#include "meta-store/txn_kv.h"
#include "meta-store/txn_kv_error.h"
#include "meta-store/versioned_value.h"
#include "recycler/recycler.h"
#include "resource-manager/resource_manager.h"
#include "snapshot/snapshot_manager.h"

namespace doris::cloud {

extern std::unique_ptr<MetaServiceProxy> get_meta_service(bool mock_resource_mgr);
extern void add_tablet(CreateTabletsRequest& request, int64_t table_id, int64_t index_id,
                       int64_t partition_id, int64_t tablet_id);
extern doris::RowsetMetaCloudPB create_rowset(int64_t txn_id, int64_t tablet_id, int partition_id,
                                              int64_t version, int num_rows);
extern TxnErrorCode read_operation_log(Transaction* txn, std::string_view log_key,
                                       Versionstamp* log_version, OperationLogPB* operation_log);

class MetaServiceTableStreamTest : public ::testing::Test {
protected:
    void SetUp() override {
        service_ = get_meta_service(false);
        instance_id_ = "table_stream_read_state_instance";
        cloud_unique_id_ = "1:" + instance_id_ + ":test";
        identity_.set_base_db_id(1001);
        identity_.set_base_table_id(1002);
        identity_.set_stream_db_id(1003);
        identity_.set_stream_id(1004);
    }

    void set_multi_version_status(MultiVersionStatus mode) {
        multi_version_status_ = mode;
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        InstanceInfoPB instance;
        instance.set_instance_id(instance_id_);
        instance.set_multi_version_status(mode);
        txn->put(instance_key({instance_id_}), instance.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
        auto [code, message] = service_->resource_mgr()->refresh_instance(instance_id_);
        ASSERT_EQ(code, MetaServiceCode::OK) << message;
    }

    void put_partition_mapping(Transaction* txn, int64_t partition_id) {
        PartitionIndexPB partition;
        partition.set_db_id(identity_.base_db_id());
        partition.set_table_id(identity_.base_table_id());
        txn->put(versioned::partition_index_key({instance_id_, partition_id}),
                 partition.SerializeAsString());
    }

    void put_latest_partition_state(Transaction* txn, int64_t partition_id, int64_t visible_version,
                                    int64_t commit_tso, std::optional<int64_t> offset_tso) {
        VersionPB version;
        version.set_version(visible_version);
        version.set_commit_tso(commit_tso);
        txn->put(partition_version_key({instance_id_, identity_.base_db_id(),
                                        identity_.base_table_id(), partition_id}),
                 version.SerializeAsString());
        if (offset_tso.has_value()) {
            TableStreamOffsetPB offset;
            offset.set_partition_id(partition_id);
            offset.set_state(TABLE_STREAM_OFFSET_CONSUMED);
            offset.set_offset_tso(*offset_tso);
            offset.set_last_consumption_time_ms(1234);
            txn->put(table_stream_offset_key({instance_id_, identity_.base_db_id(),
                                              identity_.base_table_id(), identity_.stream_db_id(),
                                              identity_.stream_id(), partition_id}),
                     offset.SerializeAsString());
        }
    }

    void put_versioned_partition_state(Transaction* txn, int64_t partition_id,
                                       int64_t visible_version, int64_t commit_tso,
                                       std::optional<int64_t> offset_tso) {
        put_partition_mapping(txn, partition_id);
        versioned_put(txn, versioned::meta_partition_key({instance_id_, partition_id}),
                      Versionstamp(41, 0), "");

        VersionPB version;
        version.set_version(visible_version);
        version.set_commit_tso(commit_tso);
        versioned_put(txn, versioned::partition_version_key({instance_id_, partition_id}),
                      Versionstamp(42, 0), version.SerializeAsString());
        if (offset_tso.has_value()) {
            TableStreamOffsetPB offset;
            offset.set_partition_id(partition_id);
            offset.set_state(TABLE_STREAM_OFFSET_INITIAL_SNAPSHOT_PENDING);
            offset.set_offset_tso(*offset_tso);
            versioned_put(txn,
                          versioned::table_stream_offset_key(
                                  {instance_id_, identity_.base_db_id(), identity_.base_table_id(),
                                   identity_.stream_db_id(), identity_.stream_id(), partition_id}),
                          Versionstamp(43, 0), offset.SerializeAsString());
        }
    }

    GetTableStreamOffsetResponse get_read_state(MetaServiceProxy* service,
                                                const std::vector<int64_t>& partitions) {
        GetTableStreamOffsetRequest request;
        request.set_cloud_unique_id(cloud_unique_id_);
        auto* binding = request.add_bindings();
        binding->mutable_identity()->CopyFrom(identity_);
        for (int64_t partition_id : partitions) {
            binding->add_partition_ids(partition_id);
        }
        GetTableStreamOffsetResponse response;
        brpc::Controller controller;
        service->get_table_stream_offset(&controller, &request, &response, nullptr);
        return response;
    }

    GetTableStreamOffsetResponse get_read_state(const std::vector<int64_t>& partitions) {
        return get_read_state(service_.get(), partitions);
    }

    GetTableStreamOffsetResponse get_read_state(std::initializer_list<int64_t> partitions) {
        return get_read_state(std::vector<int64_t>(partitions));
    }

    int64_t begin_target_transaction(const std::string& label) {
        return begin_transaction(cloud_unique_id_, label);
    }

    int64_t begin_transaction(const std::string& cloud_unique_id, const std::string& label) {
        BeginTxnRequest request;
        request.set_cloud_unique_id(cloud_unique_id);
        TxnInfoPB* txn_info = request.mutable_txn_info();
        txn_info->set_db_id(3001);
        txn_info->set_label(label);
        txn_info->add_table_ids(3002);
        txn_info->set_timeout_ms(36000);
        BeginTxnResponse response;
        brpc::Controller controller;
        service_->begin_txn(&controller, &request, &response, nullptr);
        EXPECT_EQ(response.status().code(), MetaServiceCode::OK) << response.status().msg();
        return response.txn_id();
    }

    void register_instance(const std::string& instance_id, MultiVersionStatus mode) {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        InstanceInfoPB instance;
        instance.set_instance_id(instance_id);
        instance.set_multi_version_status(mode);
        txn->put(instance_key({instance_id}), instance.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
        auto [code, message] = service_->resource_mgr()->refresh_instance(instance_id);
        ASSERT_EQ(code, MetaServiceCode::OK) << message;
    }

    void mark_transaction_visible(const std::string& instance_id, int64_t txn_id,
                                  Transaction* txn) {
        std::string txn_info_value;
        ASSERT_EQ(txn->get(txn_info_key({instance_id, 3001, txn_id}), &txn_info_value),
                  TxnErrorCode::TXN_OK);
        TxnInfoPB txn_info;
        ASSERT_TRUE(txn_info.ParseFromString(txn_info_value));
        txn_info.set_status(TXN_STATUS_VISIBLE);
        txn->put(txn_info_key({instance_id, 3001, txn_id}), txn_info.SerializeAsString());
    }

    CommitTxnRequest make_consume_request(int64_t txn_id, int64_t partition_id,
                                          TableStreamOffsetStatePB expected_state,
                                          int64_t expected_tso, int64_t next_tso) {
        CommitTxnRequest request;
        request.set_cloud_unique_id(cloud_unique_id_);
        request.set_db_id(3001);
        request.set_txn_id(txn_id);
        TableStreamUpdatePB* stream_update = request.add_table_stream_updates();
        stream_update->mutable_identity()->CopyFrom(identity_);
        TableStreamPartitionUpdatePB* partition_update = stream_update->add_partition_updates();
        partition_update->set_partition_id(partition_id);
        partition_update->set_expected_state(expected_state);
        if (expected_state != TABLE_STREAM_OFFSET_UNKNOWN) {
            partition_update->set_expected_offset_tso(expected_tso);
        }
        partition_update->set_next_offset_tso(next_tso);
        return request;
    }

    CommitTxnResponse commit_transaction(const CommitTxnRequest& request) {
        CommitTxnResponse response;
        brpc::Controller controller;
        service_->commit_txn(&controller, &request, &response, nullptr);
        return response;
    }

    std::pair<CommitTxnResponse, CommitTxnResponse> commit_concurrently(
            const CommitTxnRequest& first_request, const CommitTxnRequest& second_request) {
        std::mutex mutex;
        std::condition_variable condition;
        int commits_ready = 0;
        bool release_commits = false;
        auto* sync_point = SyncPoint::get_instance();
        DORIS_CLOUD_DEFER {
            sync_point->clear_all_call_backs();
            sync_point->clear_trace();
            sync_point->disable_processing();
        };
        sync_point->set_call_back("commit_txn_immediately::before_commit", [&](auto&&) {
            std::unique_lock lock(mutex);
            ++commits_ready;
            condition.notify_all();
            condition.wait(lock, [&] { return release_commits; });
        });
        sync_point->enable_processing();

        CommitTxnResponse first_response;
        CommitTxnResponse second_response;
        std::thread first_thread([&] { first_response = commit_transaction(first_request); });
        std::thread second_thread([&] { second_response = commit_transaction(second_request); });
        bool both_commits_ready = false;
        {
            std::unique_lock lock(mutex);
            both_commits_ready = condition.wait_for(lock, std::chrono::seconds(10),
                                                    [&] { return commits_ready == 2; });
            release_commits = true;
            condition.notify_all();
        }
        first_thread.join();
        second_thread.join();
        EXPECT_TRUE(both_commits_ready);
        EXPECT_EQ(commits_ready, 2);
        return {std::move(first_response), std::move(second_response)};
    }

    CommitTxnResponse consume_partition(int64_t txn_id, int64_t partition_id,
                                        TableStreamOffsetStatePB expected_state,
                                        int64_t expected_tso, int64_t next_tso) {
        return commit_transaction(
                make_consume_request(txn_id, partition_id, expected_state, expected_tso, next_tso));
    }

    void create_target_tablet(int64_t db_id, int64_t table_id, int64_t index_id,
                              int64_t partition_id, int64_t tablet_id) {
        CreateTabletsRequest request;
        request.set_cloud_unique_id(cloud_unique_id_);
        request.set_db_id(db_id);
        add_tablet(request, table_id, index_id, partition_id, tablet_id);
        CreateTabletsResponse response;
        brpc::Controller controller;
        service_->create_tablets(&controller, &request, &response, nullptr);
        ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.status().msg();

        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        VersionPB version;
        version.set_version(1);
        txn->put(partition_version_key({instance_id_, db_id, table_id, partition_id}),
                 version.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    void stage_target_rowset(int64_t txn_id, int64_t partition_id, int64_t tablet_id,
                             bool include_row_binlog_mapping = false) {
        doris::RowsetMetaCloudPB rowset = create_rowset(txn_id, tablet_id, partition_id, -1, 100);
        if (include_row_binlog_mapping) {
            auto* mapping = rowset.mutable_row_binlog_column_mappings()->add_entries();
            mapping->set_source_column_unique_id(1);
            mapping->set_current_column_unique_id(10);
            mapping->set_before_column_unique_id(11);
        }
        auto call = [&](bool prepare) {
            CreateRowsetRequest request;
            request.set_cloud_unique_id(cloud_unique_id_);
            request.mutable_rowset_meta()->CopyFrom(rowset);
            CreateRowsetResponse response;
            brpc::Controller controller;
            if (prepare) {
                service_->prepare_rowset(&controller, &request, &response, nullptr);
            } else {
                service_->commit_rowset(&controller, &request, &response, nullptr);
            }
            ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.status().msg();
        };
        ASSERT_NO_FATAL_FAILURE(call(true));
        ASSERT_NO_FATAL_FAILURE(call(false));
    }

    TableStreamOffsetPB get_latest_offset(const TableStreamIdentityPB& identity,
                                          int64_t partition_id) {
        std::unique_ptr<Transaction> txn;
        EXPECT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string value;
        EXPECT_EQ(txn->get(table_stream_offset_key(
                                   {instance_id_, identity.base_db_id(), identity.base_table_id(),
                                    identity.stream_db_id(), identity.stream_id(), partition_id}),
                           &value),
                  TxnErrorCode::TXN_OK);
        TableStreamOffsetPB offset;
        EXPECT_TRUE(offset.ParseFromString(value));
        return offset;
    }

    TableStreamOffsetPB get_latest_offset(int64_t partition_id) {
        return get_latest_offset(identity_, partition_id);
    }

    size_t count_keys(std::string_view begin, std::string_view end) {
        std::unique_ptr<Transaction> txn;
        EXPECT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::unique_ptr<RangeGetIterator> iter;
        EXPECT_EQ(txn->get(begin, end, &iter, true, 0), TxnErrorCode::TXN_OK);
        return iter->size();
    }

    std::vector<std::pair<std::string, std::string>> read_keys(std::string_view begin,
                                                               std::string_view end) {
        std::unique_ptr<Transaction> txn;
        EXPECT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        FullRangeGetOptions options;
        options.txn = txn.get();
        auto iter = service_->txn_kv()->full_range_get(std::string(begin), std::string(end),
                                                       std::move(options));
        std::vector<std::pair<std::string, std::string>> entries;
        for (auto kv = iter->next(); kv.has_value(); kv = iter->next()) {
            entries.emplace_back(kv->first, kv->second);
        }
        EXPECT_TRUE(iter->is_valid());
        return entries;
    }

    std::unique_ptr<MetaServiceProxy> restart_meta_service() {
        const std::shared_ptr<TxnKv> txn_kv = service_->txn_kv();
        auto resource_manager = std::make_shared<ResourceManager>(txn_kv);
        auto [code, message] = resource_manager->refresh_instance(instance_id_);
        EXPECT_EQ(code, MetaServiceCode::OK) << message;
        auto rate_limiter = std::make_shared<RateLimiter>();
        auto snapshot_manager = std::make_shared<SnapshotManager>(txn_kv);
        auto restarted_impl = std::make_unique<MetaServiceImpl>(txn_kv, resource_manager,
                                                                rate_limiter, snapshot_manager);
        return std::make_unique<MetaServiceProxy>(std::move(restarted_impl));
    }

    void set_clone_source(const std::string& source_instance_id, Versionstamp snapshot_version) {
        multi_version_status_ = MULTI_VERSION_READ_WRITE;
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        InstanceInfoPB instance;
        instance.set_instance_id(instance_id_);
        instance.set_multi_version_status(MULTI_VERSION_READ_WRITE);
        instance.set_source_instance_id(source_instance_id);
        instance.set_source_snapshot_id(SnapshotManager::serialize_snapshot_id(snapshot_version));
        txn->put(instance_key({instance_id_}), instance.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
        auto [code, message] = service_->resource_mgr()->refresh_instance(instance_id_);
        ASSERT_EQ(code, MetaServiceCode::OK) << message;
    }

    Versionstamp put_auto_versioned_offset(const std::string& target_instance_id,
                                           const TableStreamIdentityPB& identity,
                                           int64_t partition_id, int64_t offset_tso,
                                           bool write_latest = false) {
        std::unique_ptr<Transaction> txn;
        EXPECT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        TableStreamOffsetPB offset;
        offset.set_partition_id(partition_id);
        offset.set_state(TABLE_STREAM_OFFSET_CONSUMED);
        offset.set_offset_tso(offset_tso);
        const TableStreamOffsetKeyInfo key_info {target_instance_id,       identity.base_db_id(),
                                                 identity.base_table_id(), identity.stream_db_id(),
                                                 identity.stream_id(),     partition_id};
        const std::string value = offset.SerializeAsString();
        if (write_latest) {
            txn->put(table_stream_offset_key(key_info), value);
        }
        txn->enable_get_versionstamp();
        versioned_put(txn.get(), versioned::table_stream_offset_key(key_info), value);
        EXPECT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
        Versionstamp version;
        EXPECT_EQ(txn->get_versionstamp(&version), TxnErrorCode::TXN_OK);
        return version;
    }

    std::unique_ptr<MetaServiceProxy> service_;
    std::string instance_id_;
    std::string cloud_unique_id_;
    TableStreamIdentityPB identity_;
    MultiVersionStatus multi_version_status_ = MULTI_VERSION_DISABLED;
};

TEST_F(MetaServiceTableStreamTest, ReadLatestAndUnknownOffsets) {
    set_multi_version_status(MULTI_VERSION_DISABLED);
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    put_latest_partition_state(txn.get(), 2001, 8, 130, 100);
    put_latest_partition_state(txn.get(), 2002, 9, 140, std::nullopt);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    GetTableStreamOffsetResponse response = get_read_state({2001, 2002});
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.status().msg();
    ASSERT_EQ(response.bindings_size(), 1);
    ASSERT_EQ(response.bindings(0).partition_states_size(), 2);

    const auto& consumed = response.bindings(0).partition_states(0);
    EXPECT_EQ(consumed.partition_id(), 2001);
    EXPECT_EQ(consumed.offset_state(), TABLE_STREAM_OFFSET_CONSUMED);
    EXPECT_EQ(consumed.offset_tso(), 100);
    EXPECT_EQ(consumed.end_tso(), 130);
    EXPECT_EQ(consumed.visible_version(), 8);
    EXPECT_EQ(consumed.last_consumption_time_ms(), 1234);

    const auto& unknown = response.bindings(0).partition_states(1);
    EXPECT_EQ(unknown.partition_id(), 2002);
    EXPECT_EQ(unknown.offset_state(), TABLE_STREAM_OFFSET_UNKNOWN);
    EXPECT_FALSE(unknown.has_offset_tso());
    EXPECT_EQ(unknown.end_tso(), 140);
    EXPECT_EQ(unknown.visible_version(), 9);
}

TEST_F(MetaServiceTableStreamTest, ReadVersionAndOffsetFromOneSnapshot) {
    set_multi_version_status(MULTI_VERSION_DISABLED);
    constexpr int64_t kPartitionId = 2001;
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    put_latest_partition_state(txn.get(), kPartitionId, 8, 130, 100);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    bool updated = false;
    auto* sync_point = SyncPoint::get_instance();
    DORIS_CLOUD_DEFER {
        sync_point->disable_processing();
        sync_point->clear_all_call_backs();
    };
    sync_point->set_call_back("get_table_stream_offset::after_read_versions", [&](auto&&) {
        std::unique_ptr<Transaction> update_txn;
        ASSERT_EQ(service_->txn_kv()->create_txn(&update_txn), TxnErrorCode::TXN_OK);
        put_latest_partition_state(update_txn.get(), kPartitionId, 9, 140, 120);
        ASSERT_EQ(update_txn->commit(), TxnErrorCode::TXN_OK);
        updated = true;
    });
    sync_point->enable_processing();

    const GetTableStreamOffsetResponse response = get_read_state({kPartitionId});
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.status().msg();
    ASSERT_TRUE(updated);
    ASSERT_EQ(response.bindings_size(), 1);
    ASSERT_EQ(response.bindings(0).partition_states_size(), 1);
    const TableStreamPartitionReadStatePB& state = response.bindings(0).partition_states(0);
    EXPECT_EQ(state.visible_version(), 8);
    EXPECT_EQ(state.end_tso(), 130);
    EXPECT_EQ(state.offset_tso(), 100);

    sync_point->disable_processing();
    sync_point->clear_all_call_backs();
    const GetTableStreamOffsetResponse next_response = get_read_state({kPartitionId});
    ASSERT_EQ(next_response.status().code(), MetaServiceCode::OK) << next_response.status().msg();
    const TableStreamPartitionReadStatePB& next_state =
            next_response.bindings(0).partition_states(0);
    EXPECT_EQ(next_state.visible_version(), 9);
    EXPECT_EQ(next_state.end_tso(), 140);
    EXPECT_EQ(next_state.offset_tso(), 120);
}

TEST_F(MetaServiceTableStreamTest, ReadStateSurvivesMetaServiceRestartAndDoesNotWriteKv) {
    set_multi_version_status(MULTI_VERSION_DISABLED);
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    put_latest_partition_state(txn.get(), 2001, 8, 130, 100);
    put_latest_partition_state(txn.get(), 2002, 9, 140, std::nullopt);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    const std::string all_keys_begin(1, '\x00');
    const std::string all_keys_end(1, '\xff');
    const auto keys_before = read_keys(all_keys_begin, all_keys_end);
    const GetTableStreamOffsetResponse first_response = get_read_state({2001, 2002});
    ASSERT_EQ(first_response.status().code(), MetaServiceCode::OK) << first_response.status().msg();
    EXPECT_EQ(read_keys(all_keys_begin, all_keys_end), keys_before);

    const std::shared_ptr<TxnKv> txn_kv = service_->txn_kv();
    auto resource_manager = std::make_shared<ResourceManager>(txn_kv);
    auto [code, message] = resource_manager->refresh_instance(instance_id_);
    ASSERT_EQ(code, MetaServiceCode::OK) << message;
    auto rate_limiter = std::make_shared<RateLimiter>();
    auto snapshot_manager = std::make_shared<SnapshotManager>(txn_kv);
    auto restarted_impl = std::make_unique<MetaServiceImpl>(txn_kv, resource_manager, rate_limiter,
                                                            snapshot_manager);
    auto restarted_service = std::make_unique<MetaServiceProxy>(std::move(restarted_impl));

    const GetTableStreamOffsetResponse restarted_response =
            get_read_state(restarted_service.get(), {2001, 2002});
    ASSERT_EQ(restarted_response.status().code(), MetaServiceCode::OK)
            << restarted_response.status().msg();
    EXPECT_EQ(restarted_response.SerializeAsString(), first_response.SerializeAsString());
    EXPECT_EQ(read_keys(all_keys_begin, all_keys_end), keys_before);
}

TEST_F(MetaServiceTableStreamTest, CreateResumesAcrossMetaServiceRestarts) {
    set_multi_version_status(MULTI_VERSION_DISABLED);
    constexpr int64_t kFirstPartitionId = 2001;
    constexpr int64_t kSecondPartitionId = 2002;
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    put_latest_partition_state(txn.get(), kFirstPartitionId, 8, 130, std::nullopt);
    put_latest_partition_state(txn.get(), kSecondPartitionId, 9, 140, std::nullopt);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    IndexRequest index_request;
    index_request.set_cloud_unique_id(cloud_unique_id_);
    index_request.set_db_id(identity_.base_db_id());
    index_request.set_table_id(identity_.base_table_id());
    index_request.add_index_ids(identity_.stream_id());
    index_request.set_object_type(IndexObjectTypePB::TABLE_STREAM);
    index_request.set_stream_db_id(identity_.stream_db_id());
    index_request.set_expiration(::time(nullptr) + 3600);
    IndexResponse index_response;
    brpc::Controller prepare_controller;
    service_->prepare_index(&prepare_controller, &index_request, &index_response, nullptr);
    ASSERT_EQ(index_response.status().code(), MetaServiceCode::OK) << index_response.status().msg();

    auto make_partition_request = [&](int64_t partition_id, int64_t offset_tso) {
        PartitionRequest request;
        request.set_cloud_unique_id(cloud_unique_id_);
        request.set_db_id(identity_.base_db_id());
        request.set_table_id(identity_.base_table_id());
        request.add_index_ids(identity_.stream_id());
        request.set_object_type(IndexObjectTypePB::TABLE_STREAM);
        request.set_stream_db_id(identity_.stream_db_id());
        request.add_partition_ids(partition_id);
        TableStreamOffsetPB* offset = request.add_table_stream_offsets();
        offset->set_partition_id(partition_id);
        offset->set_state(TABLE_STREAM_OFFSET_INITIAL_SNAPSHOT_PENDING);
        offset->set_offset_tso(offset_tso);
        return request;
    };
    const PartitionRequest first_partition_request = make_partition_request(kFirstPartitionId, 100);
    const PartitionRequest second_partition_request =
            make_partition_request(kSecondPartitionId, 110);

    auto restarted_after_prepare = restart_meta_service();
    PartitionResponse partition_response;
    brpc::Controller first_partition_controller;
    restarted_after_prepare->commit_partition(&first_partition_controller, &first_partition_request,
                                              &partition_response, nullptr);
    ASSERT_EQ(partition_response.status().code(), MetaServiceCode::OK)
            << partition_response.status().msg();

    auto restarted_after_partial_commit = restart_meta_service();
    partition_response.Clear();
    brpc::Controller retry_partition_controller;
    restarted_after_partial_commit->commit_partition(
            &retry_partition_controller, &first_partition_request, &partition_response, nullptr);
    ASSERT_EQ(partition_response.status().code(), MetaServiceCode::OK)
            << partition_response.status().msg();
    partition_response.Clear();
    brpc::Controller second_partition_controller;
    restarted_after_partial_commit->commit_partition(
            &second_partition_controller, &second_partition_request, &partition_response, nullptr);
    ASSERT_EQ(partition_response.status().code(), MetaServiceCode::OK)
            << partition_response.status().msg();

    index_response.Clear();
    brpc::Controller commit_index_controller;
    restarted_after_partial_commit->commit_index(&commit_index_controller, &index_request,
                                                 &index_response, nullptr);
    ASSERT_EQ(index_response.status().code(), MetaServiceCode::OK) << index_response.status().msg();

    EXPECT_EQ(get_latest_offset(kFirstPartitionId).offset_tso(), 100);
    EXPECT_EQ(get_latest_offset(kSecondPartitionId).offset_tso(), 110);
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string value;
    EXPECT_EQ(txn->get(recycle_index_key({instance_id_, identity_.stream_id()}), &value, true),
              TxnErrorCode::TXN_KEY_NOT_FOUND);
    Versionstamp offset_version;
    EXPECT_EQ(versioned_get(
                      txn.get(),
                      versioned::table_stream_offset_key(
                              {instance_id_, identity_.base_db_id(), identity_.base_table_id(),
                               identity_.stream_db_id(), identity_.stream_id(), kFirstPartitionId}),
                      &offset_version, &value),
              TxnErrorCode::TXN_KEY_NOT_FOUND);
}

TEST_F(MetaServiceTableStreamTest, ReadVersionedState) {
    set_multi_version_status(MULTI_VERSION_READ_WRITE);
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    put_versioned_partition_state(txn.get(), 2001, 18, 230, 200);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    GetTableStreamOffsetResponse response = get_read_state({2001});
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.status().msg();
    ASSERT_EQ(response.bindings_size(), 1);
    ASSERT_EQ(response.bindings(0).partition_states_size(), 1);
    const auto& state = response.bindings(0).partition_states(0);
    EXPECT_EQ(state.offset_state(), TABLE_STREAM_OFFSET_INITIAL_SNAPSHOT_PENDING);
    EXPECT_EQ(state.offset_tso(), 200);
    EXPECT_EQ(state.end_tso(), 230);
    EXPECT_EQ(state.visible_version(), 18);
}

TEST_F(MetaServiceTableStreamTest,
       ReadVersionedStateFromCloneChainInBatchSurvivesRestartAndDoesNotWriteKv) {
    const std::string source_instance_id = "table_stream_batch_read_source";
    set_clone_source(source_instance_id, Versionstamp(100, 0));
    const std::vector<int64_t> partition_ids = {2001, 2002, 2003};

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    for (size_t i = 0; i < partition_ids.size(); ++i) {
        int64_t partition_id = partition_ids[i];
        PartitionIndexPB partition;
        partition.set_db_id(identity_.base_db_id());
        partition.set_table_id(identity_.base_table_id());
        txn->put(versioned::partition_index_key({source_instance_id, partition_id}),
                 partition.SerializeAsString());
        versioned_put(txn.get(), versioned::meta_partition_key({source_instance_id, partition_id}),
                      Versionstamp(51 + i, 0), "");

        VersionPB version;
        version.set_version(10 + i);
        version.set_commit_tso(1000 + i);
        versioned_put(txn.get(),
                      versioned::partition_version_key({source_instance_id, partition_id}),
                      Versionstamp(60 + i, 0), version.SerializeAsString());
    }

    auto put_offset = [&](const std::string& target_instance_id, int64_t partition_id,
                          Versionstamp versionstamp, int64_t offset_tso) {
        TableStreamOffsetPB offset;
        offset.set_partition_id(partition_id);
        offset.set_state(TABLE_STREAM_OFFSET_CONSUMED);
        offset.set_offset_tso(offset_tso);
        versioned_put(
                txn.get(),
                versioned::table_stream_offset_key(
                        {target_instance_id, identity_.base_db_id(), identity_.base_table_id(),
                         identity_.stream_db_id(), identity_.stream_id(), partition_id}),
                versionstamp, offset.SerializeAsString());
    };
    put_offset(source_instance_id, 2001, Versionstamp(70, 0), 701);
    put_offset(source_instance_id, 2002, Versionstamp(71, 0), 702);
    put_offset(instance_id_, 2002, Versionstamp(200, 0), 802);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    const std::string all_keys_begin(1, '\x00');
    const std::string all_keys_end(1, '\xff');
    const auto keys_before = read_keys(all_keys_begin, all_keys_end);
    const GetTableStreamOffsetResponse response = get_read_state(partition_ids);
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.status().msg();
    ASSERT_EQ(response.bindings_size(), 1);
    ASSERT_EQ(response.bindings(0).partition_states_size(), partition_ids.size());
    EXPECT_EQ(response.bindings(0).partition_states(0).offset_tso(), 701);
    EXPECT_EQ(response.bindings(0).partition_states(1).offset_tso(), 802);
    EXPECT_EQ(response.bindings(0).partition_states(2).offset_state(), TABLE_STREAM_OFFSET_UNKNOWN);
    EXPECT_EQ(read_keys(all_keys_begin, all_keys_end), keys_before);

    auto restarted_service = restart_meta_service();
    const GetTableStreamOffsetResponse restarted_response =
            get_read_state(restarted_service.get(), partition_ids);
    ASSERT_EQ(restarted_response.status().code(), MetaServiceCode::OK)
            << restarted_response.status().msg();
    EXPECT_EQ(restarted_response.SerializeAsString(), response.SerializeAsString());
    EXPECT_EQ(read_keys(all_keys_begin, all_keys_end), keys_before);
}

TEST_F(MetaServiceTableStreamTest, ReadLargePartitionBatch) {
    set_multi_version_status(MULTI_VERSION_DISABLED);
    constexpr int kPartitionCount = 1201;
    std::vector<int64_t> partition_ids;
    partition_ids.reserve(kPartitionCount);

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    for (int i = 0; i < kPartitionCount; ++i) {
        int64_t partition_id = 10000 + i;
        partition_ids.push_back(partition_id);
        put_latest_partition_state(txn.get(), partition_id, i + 1, 100000 + i, 90000 + i);
    }
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    GetTableStreamOffsetResponse response = get_read_state(partition_ids);
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.status().msg();
    ASSERT_EQ(response.bindings(0).partition_states_size(), kPartitionCount);
    EXPECT_EQ(response.bindings(0).partition_states(0).partition_id(), partition_ids.front());
    EXPECT_EQ(response.bindings(0).partition_states(0).offset_tso(), 90000);
    EXPECT_EQ(response.bindings(0).partition_states(kPartitionCount - 1).partition_id(),
              partition_ids.back());
    EXPECT_EQ(response.bindings(0).partition_states(kPartitionCount - 1).offset_tso(),
              90000 + kPartitionCount - 1);
}

TEST_F(MetaServiceTableStreamTest, ReadMultipleBindingsInBatch) {
    set_multi_version_status(MULTI_VERSION_READ_WRITE);
    constexpr int kBindingCount = 101;
    constexpr int64_t kPartitionId = 2001;

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    PartitionIndexPB partition;
    partition.set_db_id(identity_.base_db_id());
    partition.set_table_id(identity_.base_table_id());
    txn->put(versioned::partition_index_key({instance_id_, kPartitionId}),
             partition.SerializeAsString());
    versioned_put(txn.get(), versioned::meta_partition_key({instance_id_, kPartitionId}),
                  Versionstamp(20, 0), "");
    VersionPB version;
    version.set_version(8);
    version.set_commit_tso(130);
    versioned_put(txn.get(), versioned::partition_version_key({instance_id_, kPartitionId}),
                  Versionstamp(21, 0), version.SerializeAsString());

    GetTableStreamOffsetRequest request;
    request.set_cloud_unique_id(cloud_unique_id_);
    for (int i = 0; i < kBindingCount; ++i) {
        int64_t stream_id = identity_.stream_id() + i;
        TableStreamPartitionSetPB* binding = request.add_bindings();
        binding->mutable_identity()->CopyFrom(identity_);
        binding->mutable_identity()->set_stream_id(stream_id);
        binding->add_partition_ids(kPartitionId);
    }
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    GetTableStreamOffsetResponse response;
    brpc::Controller controller;
    service_->get_table_stream_offset(&controller, &request, &response, nullptr);
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.status().msg();
    ASSERT_EQ(response.bindings_size(), kBindingCount);
    for (const TableStreamReadBindingResultPB& binding : response.bindings()) {
        ASSERT_EQ(binding.partition_states_size(), 1);
        EXPECT_EQ(binding.partition_states(0).offset_state(), TABLE_STREAM_OFFSET_UNKNOWN);
    }
}

TEST_F(MetaServiceTableStreamTest, ReadWriteOnlyState) {
    set_multi_version_status(MULTI_VERSION_WRITE_ONLY);
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    put_latest_partition_state(txn.get(), 2001, 28, 330, 300);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    GetTableStreamOffsetResponse response = get_read_state({2001});
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.status().msg();
    ASSERT_EQ(response.bindings_size(), 1);
    ASSERT_EQ(response.bindings(0).partition_states_size(), 1);
    const auto& state = response.bindings(0).partition_states(0);
    EXPECT_EQ(state.offset_state(), TABLE_STREAM_OFFSET_CONSUMED);
    EXPECT_EQ(state.offset_tso(), 300);
    EXPECT_EQ(state.end_tso(), 330);
    EXPECT_EQ(state.visible_version(), 28);
}

TEST_F(MetaServiceTableStreamTest, WaitsForPendingSourceTxnAndRereadsSnapshot) {
    set_multi_version_status(MULTI_VERSION_DISABLED);
    const int64_t pending_txn_id = begin_target_transaction("pending-source-version");

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string txn_info_value;
    ASSERT_EQ(txn->get(txn_info_key({instance_id_, 3001, pending_txn_id}), &txn_info_value),
              TxnErrorCode::TXN_OK);
    TxnInfoPB txn_info;
    ASSERT_TRUE(txn_info.ParseFromString(txn_info_value));
    txn_info.set_status(TXN_STATUS_VISIBLE);
    txn->put(txn_info_key({instance_id_, 3001, pending_txn_id}), txn_info.SerializeAsString());

    VersionPB pending_version;
    pending_version.set_commit_tso(130);
    pending_version.add_pending_txn_ids(pending_txn_id);
    txn->put(partition_version_key(
                     {instance_id_, identity_.base_db_id(), identity_.base_table_id(), 2001}),
             pending_version.SerializeAsString());
    TableStreamOffsetPB offset;
    offset.set_partition_id(2001);
    offset.set_state(TABLE_STREAM_OFFSET_CONSUMED);
    offset.set_offset_tso(100);
    txn->put(table_stream_offset_key({instance_id_, identity_.base_db_id(),
                                      identity_.base_table_id(), identity_.stream_db_id(),
                                      identity_.stream_id(), 2001}),
             offset.SerializeAsString());
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    bool after_wait_called = false;
    auto* sync_point = SyncPoint::get_instance();
    DORIS_CLOUD_DEFER {
        sync_point->disable_processing();
        sync_point->clear_all_call_backs();
    };
    sync_point->set_call_back("get_table_stream_offset::after_wait_for_pending_txns", [&](auto&&) {
        std::unique_ptr<Transaction> update_txn;
        ASSERT_EQ(service_->txn_kv()->create_txn(&update_txn), TxnErrorCode::TXN_OK);
        put_latest_partition_state(update_txn.get(), 2001, 2, 130, 120);
        ASSERT_EQ(update_txn->commit(), TxnErrorCode::TXN_OK);
        after_wait_called = true;
    });
    sync_point->enable_processing();

    GetTableStreamOffsetResponse response = get_read_state({2001});
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.status().msg();
    ASSERT_TRUE(after_wait_called);
    ASSERT_EQ(response.bindings_size(), 1);
    ASSERT_EQ(response.bindings(0).partition_states_size(), 1);
    const TableStreamPartitionReadStatePB& state = response.bindings(0).partition_states(0);
    EXPECT_EQ(state.visible_version(), 2);
    EXPECT_EQ(state.end_tso(), 130);
    EXPECT_EQ(state.offset_tso(), 120);
}

TEST_F(MetaServiceTableStreamTest, WaitsForPendingVersionFromCloneSourceInstance) {
    const std::string source_instance_id = "table_stream_pending_version_source";
    register_instance(source_instance_id, MULTI_VERSION_READ_WRITE);
    set_clone_source(source_instance_id, Versionstamp(100, 0));
    const int64_t pending_txn_id = begin_transaction(fmt::format("1:{}:test", source_instance_id),
                                                     "pending-clone-source-version");

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    mark_transaction_visible(source_instance_id, pending_txn_id, txn.get());

    PartitionIndexPB partition;
    partition.set_db_id(identity_.base_db_id());
    partition.set_table_id(identity_.base_table_id());
    txn->put(versioned::partition_index_key({source_instance_id, 2001}),
             partition.SerializeAsString());
    versioned_put(txn.get(), versioned::meta_partition_key({source_instance_id, 2001}),
                  Versionstamp(50, 0), "");

    VersionPB pending_version;
    pending_version.set_commit_tso(130);
    pending_version.add_pending_txn_ids(pending_txn_id);
    versioned_put(txn.get(), versioned::partition_version_key({source_instance_id, 2001}),
                  Versionstamp(60, 0), pending_version.SerializeAsString());

    TableStreamOffsetPB offset;
    offset.set_partition_id(2001);
    offset.set_state(TABLE_STREAM_OFFSET_CONSUMED);
    offset.set_offset_tso(100);
    versioned_put(txn.get(),
                  versioned::table_stream_offset_key(
                          {source_instance_id, identity_.base_db_id(), identity_.base_table_id(),
                           identity_.stream_db_id(), identity_.stream_id(), 2001}),
                  Versionstamp(70, 0), offset.SerializeAsString());
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    bool after_wait_called = false;
    auto* sync_point = SyncPoint::get_instance();
    DORIS_CLOUD_DEFER {
        sync_point->disable_processing();
        sync_point->clear_all_call_backs();
    };
    sync_point->set_call_back("get_table_stream_offset::after_wait_for_pending_txns", [&](auto&&) {
        std::unique_ptr<Transaction> update_txn;
        ASSERT_EQ(service_->txn_kv()->create_txn(&update_txn), TxnErrorCode::TXN_OK);
        VersionPB committed_version;
        committed_version.set_version(2);
        committed_version.set_commit_tso(130);
        versioned_put(update_txn.get(),
                      versioned::partition_version_key({source_instance_id, 2001}),
                      Versionstamp(60, 0), committed_version.SerializeAsString());
        offset.set_offset_tso(120);
        versioned_put(
                update_txn.get(),
                versioned::table_stream_offset_key(
                        {source_instance_id, identity_.base_db_id(), identity_.base_table_id(),
                         identity_.stream_db_id(), identity_.stream_id(), 2001}),
                Versionstamp(70, 0), offset.SerializeAsString());
        ASSERT_EQ(update_txn->commit(), TxnErrorCode::TXN_OK);
        after_wait_called = true;
    });
    sync_point->enable_processing();

    GetTableStreamOffsetResponse response = get_read_state({2001});
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.status().msg();
    ASSERT_TRUE(after_wait_called);
    ASSERT_EQ(response.bindings_size(), 1);
    ASSERT_EQ(response.bindings(0).partition_states_size(), 1);
    const TableStreamPartitionReadStatePB& state = response.bindings(0).partition_states(0);
    EXPECT_EQ(state.visible_version(), 2);
    EXPECT_EQ(state.end_tso(), 130);
    EXPECT_EQ(state.offset_tso(), 120);
}

TEST_F(MetaServiceTableStreamTest, SingleGetVersionWaitsUsingCloneSourceInstance) {
    const std::string source_instance_id = "table_stream_single_pending_version_source";
    register_instance(source_instance_id, MULTI_VERSION_READ_WRITE);
    set_clone_source(source_instance_id, Versionstamp(100, 0));
    const int64_t pending_txn_id = begin_transaction(fmt::format("1:{}:test", source_instance_id),
                                                     "pending-clone-single-version");

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    mark_transaction_visible(source_instance_id, pending_txn_id, txn.get());
    VersionPB pending_version;
    pending_version.set_commit_tso(130);
    pending_version.add_pending_txn_ids(pending_txn_id);
    versioned_put(txn.get(), versioned::partition_version_key({source_instance_id, 2001}),
                  Versionstamp(60, 0), pending_version.SerializeAsString());
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    GetVersionRequest request;
    request.set_cloud_unique_id(cloud_unique_id_);
    request.set_db_id(identity_.base_db_id());
    request.set_table_id(identity_.base_table_id());
    request.set_partition_id(2001);
    request.set_wait_for_pending_txn(true);
    GetVersionResponse response;
    brpc::Controller controller;
    service_->get_version(&controller, &request, &response, nullptr);

    ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.status().msg();
    EXPECT_EQ(response.version(), 2);
    ASSERT_EQ(response.commit_tsos_size(), 1);
    EXPECT_EQ(response.commit_tsos(0), 130);
}

TEST_F(MetaServiceTableStreamTest, BatchGetVersionWaitsUsingCloneSourceInstance) {
    const std::string source_instance_id = "table_stream_batch_pending_version_source";
    register_instance(source_instance_id, MULTI_VERSION_READ_WRITE);
    set_clone_source(source_instance_id, Versionstamp(100, 0));
    const int64_t pending_txn_id = begin_transaction(fmt::format("1:{}:test", source_instance_id),
                                                     "pending-clone-batch-version");

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    mark_transaction_visible(source_instance_id, pending_txn_id, txn.get());
    PartitionIndexPB partition;
    partition.set_db_id(identity_.base_db_id());
    partition.set_table_id(identity_.base_table_id());
    txn->put(versioned::partition_index_key({source_instance_id, 2001}),
             partition.SerializeAsString());
    VersionPB pending_version;
    pending_version.set_commit_tso(130);
    pending_version.add_pending_txn_ids(pending_txn_id);
    versioned_put(txn.get(), versioned::partition_version_key({source_instance_id, 2001}),
                  Versionstamp(60, 0), pending_version.SerializeAsString());
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    GetVersionRequest request;
    request.set_cloud_unique_id(cloud_unique_id_);
    request.set_batch_mode(true);
    request.set_wait_for_pending_txn(true);
    request.add_db_ids(identity_.base_db_id());
    request.add_table_ids(identity_.base_table_id());
    request.add_partition_ids(2001);
    GetVersionResponse response;
    brpc::Controller controller;
    service_->get_version(&controller, &request, &response, nullptr);

    ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.status().msg();
    ASSERT_EQ(response.versions_size(), 1);
    ASSERT_EQ(response.commit_tsos_size(), 1);
    EXPECT_EQ(response.versions(0), 2);
    EXPECT_EQ(response.commit_tsos(0), 130);
}

TEST_F(MetaServiceTableStreamTest, RejectDuplicateBindingsAndPartitions) {
    GetTableStreamOffsetRequest request;
    request.set_cloud_unique_id(cloud_unique_id_);
    auto* binding = request.add_bindings();
    binding->mutable_identity()->CopyFrom(identity_);
    binding->add_partition_ids(2001);
    binding->add_partition_ids(2001);
    GetTableStreamOffsetResponse response;
    brpc::Controller controller;
    service_->get_table_stream_offset(&controller, &request, &response, nullptr);
    EXPECT_EQ(response.status().code(), MetaServiceCode::INVALID_ARGUMENT);

    request.mutable_bindings()->Clear();
    for (int i = 0; i < 2; ++i) {
        binding = request.add_bindings();
        binding->mutable_identity()->CopyFrom(identity_);
        binding->add_partition_ids(2001 + i);
    }
    response.Clear();
    brpc::Controller second_controller;
    service_->get_table_stream_offset(&second_controller, &request, &response, nullptr);
    EXPECT_EQ(response.status().code(), MetaServiceCode::INVALID_ARGUMENT);
}

TEST_F(MetaServiceTableStreamTest, RejectEnabledModeAndRecyclingStream) {
    set_multi_version_status(MULTI_VERSION_DISABLED);
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    InstanceInfoPB instance;
    instance.set_instance_id(instance_id_);
    instance.set_multi_version_status(MULTI_VERSION_ENABLED);
    txn->put(instance_key({instance_id_}), instance.SerializeAsString());
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    GetTableStreamOffsetResponse response = get_read_state({2001});
    EXPECT_EQ(response.status().code(), MetaServiceCode::INVALID_ARGUMENT);

    set_multi_version_status(MULTI_VERSION_DISABLED);
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    put_latest_partition_state(txn.get(), 2001, 8, 130, 100);
    RecycleIndexPB recycle_index;
    recycle_index.set_table_id(identity_.base_table_id());
    recycle_index.set_state(RecycleIndexPB::RECYCLING);
    recycle_index.set_object_type(TABLE_STREAM);
    txn->put(recycle_index_key({instance_id_, identity_.stream_id()}),
             recycle_index.SerializeAsString());
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    response = get_read_state({2001});
    EXPECT_EQ(response.status().code(), MetaServiceCode::INVALID_ARGUMENT);
}

TEST_F(MetaServiceTableStreamTest, CommitUpdatesLatestOffsetAndIsIdempotent) {
    set_multi_version_status(MULTI_VERSION_DISABLED);
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    put_latest_partition_state(txn.get(), 2001, 8, 130, 100);
    put_latest_partition_state(txn.get(), 2002, 9, 140, std::nullopt);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    int64_t txn_id = begin_target_transaction("consume-existing-offset");
    CommitTxnResponse response =
            consume_partition(txn_id, 2001, TABLE_STREAM_OFFSET_CONSUMED, 100, 120);
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.status().msg();
    TableStreamOffsetPB offset = get_latest_offset(2001);
    EXPECT_EQ(offset.state(), TABLE_STREAM_OFFSET_CONSUMED);
    EXPECT_EQ(offset.offset_tso(), 120);
    EXPECT_GT(offset.last_consumption_time_ms(), 0);

    response = consume_partition(txn_id, 2001, TABLE_STREAM_OFFSET_CONSUMED, 100, 120);
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.status().msg();
    EXPECT_EQ(get_latest_offset(2001).offset_tso(), 120);

    txn_id = begin_target_transaction("consume-unknown-offset");
    response = consume_partition(txn_id, 2002, TABLE_STREAM_OFFSET_UNKNOWN, 0, 140);
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.status().msg();
    EXPECT_EQ(get_latest_offset(2002).offset_tso(), 140);
}

TEST_F(MetaServiceTableStreamTest, CommitMultipleStreamsAtomically) {
    set_multi_version_status(MULTI_VERSION_DISABLED);
    TableStreamIdentityPB second_identity = identity_;
    second_identity.set_stream_db_id(1005);
    second_identity.set_stream_id(1006);

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    put_latest_partition_state(txn.get(), 2001, 8, 130, 100);
    TableStreamOffsetPB second_offset;
    second_offset.set_partition_id(2001);
    second_offset.set_state(TABLE_STREAM_OFFSET_CONSUMED);
    second_offset.set_offset_tso(105);
    second_offset.set_last_consumption_time_ms(1234);
    txn->put(table_stream_offset_key(
                     {instance_id_, second_identity.base_db_id(), second_identity.base_table_id(),
                      second_identity.stream_db_id(), second_identity.stream_id(), 2001}),
             second_offset.SerializeAsString());
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    auto make_request = [&](int64_t txn_id, int64_t second_expected_tso) {
        CommitTxnRequest request =
                make_consume_request(txn_id, 2001, TABLE_STREAM_OFFSET_CONSUMED, 100, 120);
        TableStreamUpdatePB* second_stream_update = request.add_table_stream_updates();
        second_stream_update->mutable_identity()->CopyFrom(second_identity);
        TableStreamPartitionUpdatePB* second_partition_update =
                second_stream_update->add_partition_updates();
        second_partition_update->set_partition_id(2001);
        second_partition_update->set_expected_state(TABLE_STREAM_OFFSET_CONSUMED);
        second_partition_update->set_expected_offset_tso(second_expected_tso);
        second_partition_update->set_next_offset_tso(125);
        return request;
    };

    CommitTxnResponse response = commit_transaction(
            make_request(begin_target_transaction("consume-multiple-streams-stale"), 104));
    EXPECT_EQ(response.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    EXPECT_EQ(get_latest_offset(identity_, 2001).offset_tso(), 100);
    EXPECT_EQ(get_latest_offset(second_identity, 2001).offset_tso(), 105);

    response = commit_transaction(
            make_request(begin_target_transaction("consume-multiple-streams"), 105));
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.status().msg();
    EXPECT_EQ(get_latest_offset(identity_, 2001).offset_tso(), 120);
    EXPECT_EQ(get_latest_offset(second_identity, 2001).offset_tso(), 125);
}

TEST_F(MetaServiceTableStreamTest, CommitTargetRowsetAndOffsetAtomically) {
    set_multi_version_status(MULTI_VERSION_DISABLED);
    constexpr int64_t kSourcePartitionId = 2001;
    constexpr int64_t kTargetDbId = 3001;
    constexpr int64_t kTargetTableId = 3002;
    constexpr int64_t kTargetIndexId = 3003;
    constexpr int64_t kTargetPartitionId = 4001;
    constexpr int64_t kTargetTabletId = 5001;

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    put_latest_partition_state(txn.get(), kSourcePartitionId, 8, 130, 100);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    create_target_tablet(kTargetDbId, kTargetTableId, kTargetIndexId, kTargetPartitionId,
                         kTargetTabletId);

    int64_t stale_txn_id = begin_target_transaction("consume-target-rowset-stale");
    stage_target_rowset(stale_txn_id, kTargetPartitionId, kTargetTabletId);
    CommitTxnRequest stale_request = make_consume_request(stale_txn_id, kSourcePartitionId,
                                                          TABLE_STREAM_OFFSET_CONSUMED, 99, 120);
    CommitTxnResponse response = commit_transaction(stale_request);
    ASSERT_EQ(response.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    EXPECT_EQ(get_latest_offset(kSourcePartitionId).offset_tso(), 100);

    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string value;
    EXPECT_EQ(txn->get(meta_rowset_tmp_key({instance_id_, stale_txn_id, kTargetTabletId}), &value),
              TxnErrorCode::TXN_OK);
    EXPECT_EQ(txn->get(meta_rowset_key({instance_id_, kTargetTabletId, 2}), &value),
              TxnErrorCode::TXN_KEY_NOT_FOUND);
    ASSERT_EQ(txn->get(partition_version_key(
                               {instance_id_, kTargetDbId, kTargetTableId, kTargetPartitionId}),
                       &value),
              TxnErrorCode::TXN_OK);
    VersionPB target_version;
    ASSERT_TRUE(target_version.ParseFromString(value));
    EXPECT_EQ(target_version.version(), 1);

    int64_t committed_txn_id = begin_target_transaction("consume-target-rowset");
    stage_target_rowset(committed_txn_id, kTargetPartitionId, kTargetTabletId, true);
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(meta_rowset_tmp_key({instance_id_, committed_txn_id, kTargetTabletId}),
                       &value),
              TxnErrorCode::TXN_OK);
    doris::RowsetMetaCloudPB staged_rowset;
    ASSERT_TRUE(staged_rowset.ParseFromString(value));
    EXPECT_TRUE(staged_rowset.has_row_binlog_column_mappings());
    CommitTxnRequest commit_request = make_consume_request(committed_txn_id, kSourcePartitionId,
                                                           TABLE_STREAM_OFFSET_CONSUMED, 100, 120);
    commit_request.set_commit_tso(999);
    response = commit_transaction(commit_request);
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.status().msg();
    EXPECT_EQ(get_latest_offset(kSourcePartitionId).offset_tso(), 120);

    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    EXPECT_EQ(txn->get(meta_rowset_tmp_key({instance_id_, committed_txn_id, kTargetTabletId}),
                       &value),
              TxnErrorCode::TXN_KEY_NOT_FOUND);
    ASSERT_EQ(txn->get(meta_rowset_key({instance_id_, kTargetTabletId, 2}), &value),
              TxnErrorCode::TXN_OK);
    doris::RowsetMetaCloudPB committed_rowset;
    ASSERT_TRUE(committed_rowset.ParseFromString(value));
    EXPECT_EQ(committed_rowset.txn_id(), committed_txn_id);
    EXPECT_FALSE(committed_rowset.has_row_binlog_column_mappings());
    ASSERT_EQ(txn->get(partition_version_key(
                               {instance_id_, kTargetDbId, kTargetTableId, kTargetPartitionId}),
                       &value),
              TxnErrorCode::TXN_OK);
    ASSERT_TRUE(target_version.ParseFromString(value));
    EXPECT_EQ(target_version.version(), 2);
    EXPECT_EQ(target_version.commit_tso(), 999);

    response = commit_transaction(commit_request);
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.status().msg();
    EXPECT_EQ(get_latest_offset(kSourcePartitionId).offset_tso(), 120);
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(meta_rowset_key({instance_id_, kTargetTabletId, 2}), &value),
              TxnErrorCode::TXN_OK);
    ASSERT_TRUE(committed_rowset.ParseFromString(value));
    EXPECT_EQ(committed_rowset.txn_id(), committed_txn_id);
    ASSERT_EQ(txn->get(partition_version_key(
                               {instance_id_, kTargetDbId, kTargetTableId, kTargetPartitionId}),
                       &value),
              TxnErrorCode::TXN_OK);
    ASSERT_TRUE(target_version.ParseFromString(value));
    EXPECT_EQ(target_version.version(), 2);
    EXPECT_EQ(txn->get(meta_rowset_key({instance_id_, kTargetTabletId, 3}), &value),
              TxnErrorCode::TXN_KEY_NOT_FOUND);
}

TEST_F(MetaServiceTableStreamTest, SourcePublishDoesNotConflictWithConsumptionCommit) {
    set_multi_version_status(MULTI_VERSION_DISABLED);
    constexpr int64_t kSourcePartitionId = 2001;
    constexpr int64_t kTargetDbId = 3001;
    constexpr int64_t kTargetTableId = 3002;
    constexpr int64_t kTargetIndexId = 3003;
    constexpr int64_t kTargetPartitionId = 4001;
    constexpr int64_t kTargetTabletId = 5001;

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    put_latest_partition_state(txn.get(), kSourcePartitionId, 8, 130, 100);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    create_target_tablet(kTargetDbId, kTargetTableId, kTargetIndexId, kTargetPartitionId,
                         kTargetTabletId);

    const int64_t consume_txn_id = begin_target_transaction("consume-during-source-publish");
    stage_target_rowset(consume_txn_id, kTargetPartitionId, kTargetTabletId);
    CommitTxnRequest consume_request = make_consume_request(consume_txn_id, kSourcePartitionId,
                                                            TABLE_STREAM_OFFSET_CONSUMED, 100, 120);
    consume_request.set_commit_tso(999);

    std::mutex mutex;
    std::condition_variable condition;
    bool consume_ready = false;
    bool release_consume = false;
    auto* sync_point = SyncPoint::get_instance();
    DORIS_CLOUD_DEFER {
        {
            std::lock_guard lock(mutex);
            release_consume = true;
        }
        condition.notify_all();
        sync_point->disable_processing();
        sync_point->clear_all_call_backs();
    };
    sync_point->set_call_back("commit_txn_immediately::before_commit", [&](auto&&) {
        std::unique_lock lock(mutex);
        consume_ready = true;
        condition.notify_all();
        condition.wait(lock, [&] { return release_consume; });
    });
    sync_point->enable_processing();

    CommitTxnResponse consume_response;
    std::thread consume_thread([&] { consume_response = commit_transaction(consume_request); });
    bool reached_commit = false;
    {
        std::unique_lock lock(mutex);
        reached_commit =
                condition.wait_for(lock, std::chrono::seconds(10), [&] { return consume_ready; });
    }

    TxnErrorCode create_publish_txn = TxnErrorCode::TXN_OK;
    TxnErrorCode commit_publish_txn = TxnErrorCode::TXN_OK;
    if (reached_commit) {
        create_publish_txn = service_->txn_kv()->create_txn(&txn);
        if (create_publish_txn == TxnErrorCode::TXN_OK) {
            VersionPB published_version;
            published_version.set_version(9);
            published_version.set_commit_tso(160);
            txn->put(partition_version_key({instance_id_, identity_.base_db_id(),
                                            identity_.base_table_id(), kSourcePartitionId}),
                     published_version.SerializeAsString());
            commit_publish_txn = txn->commit();
        }
    }
    {
        std::lock_guard lock(mutex);
        release_consume = true;
    }
    condition.notify_all();
    consume_thread.join();

    ASSERT_TRUE(reached_commit);
    ASSERT_EQ(create_publish_txn, TxnErrorCode::TXN_OK);
    ASSERT_EQ(commit_publish_txn, TxnErrorCode::TXN_OK);
    ASSERT_EQ(consume_response.status().code(), MetaServiceCode::OK)
            << consume_response.status().msg();
    EXPECT_EQ(get_latest_offset(kSourcePartitionId).offset_tso(), 120);

    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string value;
    ASSERT_EQ(txn->get(partition_version_key({instance_id_, identity_.base_db_id(),
                                              identity_.base_table_id(), kSourcePartitionId}),
                       &value),
              TxnErrorCode::TXN_OK);
    VersionPB source_version;
    ASSERT_TRUE(source_version.ParseFromString(value));
    EXPECT_EQ(source_version.version(), 9);
    EXPECT_EQ(source_version.commit_tso(), 160);

    EXPECT_EQ(
            txn->get(meta_rowset_tmp_key({instance_id_, consume_txn_id, kTargetTabletId}), &value),
            TxnErrorCode::TXN_KEY_NOT_FOUND);
    ASSERT_EQ(txn->get(meta_rowset_key({instance_id_, kTargetTabletId, 2}), &value),
              TxnErrorCode::TXN_OK);
    doris::RowsetMetaCloudPB committed_rowset;
    ASSERT_TRUE(committed_rowset.ParseFromString(value));
    EXPECT_EQ(committed_rowset.txn_id(), consume_txn_id);

    ASSERT_EQ(txn->get(partition_version_key(
                               {instance_id_, kTargetDbId, kTargetTableId, kTargetPartitionId}),
                       &value),
              TxnErrorCode::TXN_OK);
    VersionPB target_version;
    ASSERT_TRUE(target_version.ParseFromString(value));
    EXPECT_EQ(target_version.version(), 2);
    EXPECT_EQ(target_version.commit_tso(), 999);

    ASSERT_EQ(txn->get(txn_info_key({instance_id_, kTargetDbId, consume_txn_id}), &value),
              TxnErrorCode::TXN_OK);
    TxnInfoPB txn_info;
    ASSERT_TRUE(txn_info.ParseFromString(value));
    EXPECT_EQ(txn_info.status(), TXN_STATUS_VISIBLE);
}

TEST_F(MetaServiceTableStreamTest, CommitRetryAfterMetaServiceRestartIsExactlyOnce) {
    set_multi_version_status(MULTI_VERSION_DISABLED);
    constexpr int64_t kSourcePartitionId = 2001;
    constexpr int64_t kTargetDbId = 3001;
    constexpr int64_t kTargetTableId = 3002;
    constexpr int64_t kTargetIndexId = 3003;
    constexpr int64_t kTargetPartitionId = 4001;
    constexpr int64_t kTargetTabletId = 5001;

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    put_latest_partition_state(txn.get(), kSourcePartitionId, 8, 130, 100);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    create_target_tablet(kTargetDbId, kTargetTableId, kTargetIndexId, kTargetPartitionId,
                         kTargetTabletId);

    const int64_t txn_id = begin_target_transaction("consume-retry-after-ms-restart");
    stage_target_rowset(txn_id, kTargetPartitionId, kTargetTabletId);
    CommitTxnRequest request = make_consume_request(txn_id, kSourcePartitionId,
                                                    TABLE_STREAM_OFFSET_CONSUMED, 100, 120);
    request.set_commit_tso(999);

    // The commit succeeds, but its response is lost before the caller observes it.
    const CommitTxnResponse lost_response = commit_transaction(request);
    ASSERT_EQ(lost_response.status().code(), MetaServiceCode::OK) << lost_response.status().msg();

    auto restarted_service = restart_meta_service();
    CommitTxnResponse retry_response;
    brpc::Controller retry_controller;
    restarted_service->commit_txn(&retry_controller, &request, &retry_response, nullptr);
    ASSERT_EQ(retry_response.status().code(), MetaServiceCode::OK) << retry_response.status().msg();

    EXPECT_EQ(get_latest_offset(kSourcePartitionId).offset_tso(), 120);
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string value;
    EXPECT_EQ(txn->get(meta_rowset_tmp_key({instance_id_, txn_id, kTargetTabletId}), &value),
              TxnErrorCode::TXN_KEY_NOT_FOUND);
    ASSERT_EQ(txn->get(meta_rowset_key({instance_id_, kTargetTabletId, 2}), &value),
              TxnErrorCode::TXN_OK);
    doris::RowsetMetaCloudPB committed_rowset;
    ASSERT_TRUE(committed_rowset.ParseFromString(value));
    EXPECT_EQ(committed_rowset.txn_id(), txn_id);
    EXPECT_EQ(txn->get(meta_rowset_key({instance_id_, kTargetTabletId, 3}), &value),
              TxnErrorCode::TXN_KEY_NOT_FOUND);
    ASSERT_EQ(txn->get(partition_version_key(
                               {instance_id_, kTargetDbId, kTargetTableId, kTargetPartitionId}),
                       &value),
              TxnErrorCode::TXN_OK);
    VersionPB target_version;
    ASSERT_TRUE(target_version.ParseFromString(value));
    EXPECT_EQ(target_version.version(), 2);
    EXPECT_EQ(target_version.commit_tso(), 999);
}

TEST_F(MetaServiceTableStreamTest, CommitRejectsRecyclingStreamBeforePublishingRowsetAndOffset) {
    set_multi_version_status(MULTI_VERSION_DISABLED);
    constexpr int64_t kSourcePartitionId = 2001;
    constexpr int64_t kTargetDbId = 3001;
    constexpr int64_t kTargetTableId = 3002;
    constexpr int64_t kTargetIndexId = 3003;
    constexpr int64_t kTargetPartitionId = 4001;
    constexpr int64_t kTargetTabletId = 5001;

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    put_latest_partition_state(txn.get(), kSourcePartitionId, 8, 130, 100);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    create_target_tablet(kTargetDbId, kTargetTableId, kTargetIndexId, kTargetPartitionId,
                         kTargetTabletId);

    const int64_t txn_id = begin_target_transaction("consume-recycling-stream");
    stage_target_rowset(txn_id, kTargetPartitionId, kTargetTabletId);
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    RecycleIndexPB recycle_index;
    recycle_index.set_db_id(identity_.base_db_id());
    recycle_index.set_table_id(identity_.base_table_id());
    recycle_index.set_stream_db_id(identity_.stream_db_id());
    recycle_index.set_state(RecycleIndexPB::RECYCLING);
    recycle_index.set_object_type(IndexObjectTypePB::TABLE_STREAM);
    txn->put(recycle_index_key({instance_id_, identity_.stream_id()}),
             recycle_index.SerializeAsString());
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    const CommitTxnRequest request = make_consume_request(txn_id, kSourcePartitionId,
                                                          TABLE_STREAM_OFFSET_CONSUMED, 100, 120);
    const CommitTxnResponse response = commit_transaction(request);
    EXPECT_EQ(response.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    EXPECT_EQ(get_latest_offset(kSourcePartitionId).offset_tso(), 100);

    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string value;
    EXPECT_EQ(txn->get(meta_rowset_tmp_key({instance_id_, txn_id, kTargetTabletId}), &value),
              TxnErrorCode::TXN_OK);
    EXPECT_EQ(txn->get(meta_rowset_key({instance_id_, kTargetTabletId, 2}), &value),
              TxnErrorCode::TXN_KEY_NOT_FOUND);
    ASSERT_EQ(txn->get(partition_version_key(
                               {instance_id_, kTargetDbId, kTargetTableId, kTargetPartitionId}),
                       &value),
              TxnErrorCode::TXN_OK);
    VersionPB target_version;
    ASSERT_TRUE(target_version.ParseFromString(value));
    EXPECT_EQ(target_version.version(), 1);
}

TEST_F(MetaServiceTableStreamTest, DropPartitionConflictsWithInFlightConsumption) {
    set_multi_version_status(MULTI_VERSION_DISABLED);
    constexpr int64_t kSourcePartitionId = 2001;
    constexpr int64_t kTargetDbId = 3001;
    constexpr int64_t kTargetTableId = 3002;
    constexpr int64_t kTargetIndexId = 3003;
    constexpr int64_t kTargetPartitionId = 4001;
    constexpr int64_t kTargetTabletId = 5001;

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    put_latest_partition_state(txn.get(), kSourcePartitionId, 8, 130, 100);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    create_target_tablet(kTargetDbId, kTargetTableId, kTargetIndexId, kTargetPartitionId,
                         kTargetTabletId);

    const int64_t txn_id = begin_target_transaction("consume-while-dropping-source-partition");
    stage_target_rowset(txn_id, kTargetPartitionId, kTargetTabletId);
    const CommitTxnRequest consume_request = make_consume_request(
            txn_id, kSourcePartitionId, TABLE_STREAM_OFFSET_CONSUMED, 100, 120);

    std::mutex mutex;
    std::condition_variable condition;
    bool commit_ready = false;
    bool release_commit = false;
    auto* sync_point = SyncPoint::get_instance();
    DORIS_CLOUD_DEFER {
        {
            std::lock_guard lock(mutex);
            release_commit = true;
        }
        condition.notify_all();
        sync_point->disable_processing();
        sync_point->clear_all_call_backs();
    };
    sync_point->set_call_back("commit_txn_immediately::before_commit", [&](auto&&) {
        std::unique_lock lock(mutex);
        commit_ready = true;
        condition.notify_all();
        condition.wait(lock, [&] { return release_commit; });
    });
    sync_point->enable_processing();

    CommitTxnResponse consume_response;
    std::thread consume_thread([&] { consume_response = commit_transaction(consume_request); });
    bool reached_commit = false;
    {
        std::unique_lock lock(mutex);
        reached_commit =
                condition.wait_for(lock, std::chrono::seconds(10), [&] { return commit_ready; });
    }

    PartitionRequest drop_request;
    PartitionResponse drop_response;
    if (reached_commit) {
        drop_request.set_cloud_unique_id(cloud_unique_id_);
        drop_request.set_db_id(identity_.base_db_id());
        drop_request.set_table_id(identity_.base_table_id());
        drop_request.add_index_ids(identity_.base_table_id() + 1);
        drop_request.add_partition_ids(kSourcePartitionId);
        drop_request.mutable_table_streams()->Add()->CopyFrom(identity_);
        drop_request.set_expiration(::time(nullptr) + 3600);
        brpc::Controller drop_controller;
        service_->drop_partition(&drop_controller, &drop_request, &drop_response, nullptr);
    }
    {
        std::lock_guard lock(mutex);
        release_commit = true;
    }
    condition.notify_all();
    consume_thread.join();

    ASSERT_TRUE(reached_commit);
    ASSERT_EQ(drop_response.status().code(), MetaServiceCode::OK) << drop_response.status().msg();
    EXPECT_EQ(consume_response.status().code(), MetaServiceCode::KV_TXN_CONFLICT)
            << consume_response.status().msg();
    EXPECT_EQ(get_latest_offset(kSourcePartitionId).offset_tso(), 100);

    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string value;
    ASSERT_EQ(txn->get(recycle_partition_key({instance_id_, kSourcePartitionId}), &value),
              TxnErrorCode::TXN_OK);
    RecyclePartitionPB recycle_partition;
    ASSERT_TRUE(recycle_partition.ParseFromString(value));
    EXPECT_EQ(recycle_partition.state(), RecyclePartitionPB::DROPPED);
    EXPECT_EQ(txn->get(meta_rowset_tmp_key({instance_id_, txn_id, kTargetTabletId}), &value),
              TxnErrorCode::TXN_OK);
    EXPECT_EQ(txn->get(meta_rowset_key({instance_id_, kTargetTabletId, 2}), &value),
              TxnErrorCode::TXN_KEY_NOT_FOUND);
    ASSERT_EQ(txn->get(partition_version_key(
                               {instance_id_, kTargetDbId, kTargetTableId, kTargetPartitionId}),
                       &value),
              TxnErrorCode::TXN_OK);
    VersionPB target_version;
    ASSERT_TRUE(target_version.ParseFromString(value));
    EXPECT_EQ(target_version.version(), 1);
}

TEST_F(MetaServiceTableStreamTest, ConcurrentOffsetCasAllowsOnlyOneCommit) {
    set_multi_version_status(MULTI_VERSION_DISABLED);
    constexpr int64_t kPartitionId = 2001;
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    put_latest_partition_state(txn.get(), kPartitionId, 8, 130, 100);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    const CommitTxnRequest first_request =
            make_consume_request(begin_target_transaction("concurrent-cas-first"), kPartitionId,
                                 TABLE_STREAM_OFFSET_CONSUMED, 100, 120);
    const CommitTxnRequest second_request =
            make_consume_request(begin_target_transaction("concurrent-cas-second"), kPartitionId,
                                 TABLE_STREAM_OFFSET_CONSUMED, 100, 125);

    auto [first_response, second_response] = commit_concurrently(first_request, second_request);

    const int successful_commits = (first_response.status().code() == MetaServiceCode::OK ? 1 : 0) +
                                   (second_response.status().code() == MetaServiceCode::OK ? 1 : 0);
    const int conflicted_commits =
            (first_response.status().code() == MetaServiceCode::KV_TXN_CONFLICT ? 1 : 0) +
            (second_response.status().code() == MetaServiceCode::KV_TXN_CONFLICT ? 1 : 0);
    EXPECT_EQ(successful_commits, 1);
    EXPECT_EQ(conflicted_commits, 1);
    const int64_t final_offset = get_latest_offset(kPartitionId).offset_tso();
    EXPECT_TRUE(final_offset == 120 || final_offset == 125);
}

TEST_F(MetaServiceTableStreamTest, ConcurrentUnknownOffsetCasAllowsOnlyOneFirstConsumer) {
    set_multi_version_status(MULTI_VERSION_DISABLED);
    constexpr int64_t kPartitionId = 2001;
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    put_latest_partition_state(txn.get(), kPartitionId, 8, 130, std::nullopt);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    const CommitTxnRequest first_request =
            make_consume_request(begin_target_transaction("concurrent-unknown-first"), kPartitionId,
                                 TABLE_STREAM_OFFSET_UNKNOWN, 0, 120);
    const CommitTxnRequest second_request =
            make_consume_request(begin_target_transaction("concurrent-unknown-second"),
                                 kPartitionId, TABLE_STREAM_OFFSET_UNKNOWN, 0, 125);
    auto [first_response, second_response] = commit_concurrently(first_request, second_request);

    const int successful_commits = (first_response.status().code() == MetaServiceCode::OK ? 1 : 0) +
                                   (second_response.status().code() == MetaServiceCode::OK ? 1 : 0);
    const int conflicted_commits =
            (first_response.status().code() == MetaServiceCode::KV_TXN_CONFLICT ? 1 : 0) +
            (second_response.status().code() == MetaServiceCode::KV_TXN_CONFLICT ? 1 : 0);
    EXPECT_EQ(successful_commits, 1);
    EXPECT_EQ(conflicted_commits, 1);
    const int64_t final_offset = get_latest_offset(kPartitionId).offset_tso();
    EXPECT_TRUE(final_offset == 120 || final_offset == 125);
}

TEST_F(MetaServiceTableStreamTest, ConcurrentIndependentOffsetUpdatesDoNotConflict) {
    set_multi_version_status(MULTI_VERSION_DISABLED);
    constexpr int64_t kSharedPartitionId = 2101;
    constexpr int64_t kFirstPartitionId = 2201;
    constexpr int64_t kSecondPartitionId = 2202;
    TableStreamIdentityPB second_identity = identity_;
    second_identity.set_stream_db_id(identity_.stream_db_id() + 1);
    second_identity.set_stream_id(identity_.stream_id() + 1);

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    put_latest_partition_state(txn.get(), kSharedPartitionId, 8, 130, std::nullopt);
    put_latest_partition_state(txn.get(), kFirstPartitionId, 9, 140, std::nullopt);
    put_latest_partition_state(txn.get(), kSecondPartitionId, 10, 150, std::nullopt);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    CommitTxnRequest first_request =
            make_consume_request(begin_target_transaction("concurrent-stream-first"),
                                 kSharedPartitionId, TABLE_STREAM_OFFSET_UNKNOWN, 0, 120);
    CommitTxnRequest second_request =
            make_consume_request(begin_target_transaction("concurrent-stream-second"),
                                 kSharedPartitionId, TABLE_STREAM_OFFSET_UNKNOWN, 0, 125);
    second_request.mutable_table_stream_updates(0)->mutable_identity()->CopyFrom(second_identity);
    auto [first_stream_response, second_stream_response] =
            commit_concurrently(first_request, second_request);
    ASSERT_EQ(first_stream_response.status().code(), MetaServiceCode::OK)
            << first_stream_response.status().msg();
    ASSERT_EQ(second_stream_response.status().code(), MetaServiceCode::OK)
            << second_stream_response.status().msg();
    EXPECT_EQ(get_latest_offset(identity_, kSharedPartitionId).offset_tso(), 120);
    EXPECT_EQ(get_latest_offset(second_identity, kSharedPartitionId).offset_tso(), 125);

    first_request = make_consume_request(begin_target_transaction("concurrent-partition-first"),
                                         kFirstPartitionId, TABLE_STREAM_OFFSET_UNKNOWN, 0, 135);
    second_request = make_consume_request(begin_target_transaction("concurrent-partition-second"),
                                          kSecondPartitionId, TABLE_STREAM_OFFSET_UNKNOWN, 0, 145);
    auto [first_partition_response, second_partition_response] =
            commit_concurrently(first_request, second_request);
    ASSERT_EQ(first_partition_response.status().code(), MetaServiceCode::OK)
            << first_partition_response.status().msg();
    ASSERT_EQ(second_partition_response.status().code(), MetaServiceCode::OK)
            << second_partition_response.status().msg();
    EXPECT_EQ(get_latest_offset(kFirstPartitionId).offset_tso(), 135);
    EXPECT_EQ(get_latest_offset(kSecondPartitionId).offset_tso(), 145);
}

TEST_F(MetaServiceTableStreamTest, MultiVersionModeChangeConflictsWithInFlightConsumption) {
    set_multi_version_status(MULTI_VERSION_DISABLED);
    constexpr int64_t kPartitionId = 2001;
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    put_latest_partition_state(txn.get(), kPartitionId, 8, 130, 100);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    const CommitTxnRequest request = make_consume_request(
            begin_target_transaction("consume-while-changing-multi-version-mode"), kPartitionId,
            TABLE_STREAM_OFFSET_CONSUMED, 100, 120);
    std::mutex mutex;
    std::condition_variable condition;
    bool commit_ready = false;
    bool release_commit = false;
    auto* sync_point = SyncPoint::get_instance();
    DORIS_CLOUD_DEFER {
        {
            std::lock_guard lock(mutex);
            release_commit = true;
        }
        condition.notify_all();
        sync_point->disable_processing();
        sync_point->clear_all_call_backs();
    };
    sync_point->set_call_back("commit_txn_immediately::before_commit", [&](auto&&) {
        std::unique_lock lock(mutex);
        commit_ready = true;
        condition.notify_all();
        condition.wait(lock, [&] { return release_commit; });
    });
    sync_point->enable_processing();

    CommitTxnResponse response;
    std::thread commit_thread([&] { response = commit_transaction(request); });
    bool reached_commit = false;
    {
        std::unique_lock lock(mutex);
        reached_commit =
                condition.wait_for(lock, std::chrono::seconds(10), [&] { return commit_ready; });
    }
    TxnErrorCode create_mode_txn = TxnErrorCode::TXN_OK;
    TxnErrorCode commit_mode_txn = TxnErrorCode::TXN_OK;
    if (reached_commit) {
        create_mode_txn = service_->txn_kv()->create_txn(&txn);
        if (create_mode_txn == TxnErrorCode::TXN_OK) {
            InstanceInfoPB instance;
            instance.set_instance_id(instance_id_);
            instance.set_multi_version_status(MULTI_VERSION_WRITE_ONLY);
            txn->put(instance_key({instance_id_}), instance.SerializeAsString());
            commit_mode_txn = txn->commit();
        }
    }
    {
        std::lock_guard lock(mutex);
        release_commit = true;
    }
    condition.notify_all();
    commit_thread.join();

    ASSERT_TRUE(reached_commit);
    ASSERT_EQ(create_mode_txn, TxnErrorCode::TXN_OK);
    ASSERT_EQ(commit_mode_txn, TxnErrorCode::TXN_OK);
    EXPECT_EQ(response.status().code(), MetaServiceCode::KV_TXN_CONFLICT)
            << response.status().msg();
    EXPECT_EQ(get_latest_offset(kPartitionId).offset_tso(), 100);
}

TEST_F(MetaServiceTableStreamTest, CommitLargePartitionBatch) {
    set_multi_version_status(MULTI_VERSION_WRITE_ONLY);
    constexpr int kPartitionCount = 1201;

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    for (int i = 0; i < kPartitionCount; ++i) {
        int64_t partition_id = 30000 + i;
        put_latest_partition_state(txn.get(), partition_id, i + 1, 40000 + i, std::nullopt);
    }
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    CommitTxnRequest request;
    request.set_cloud_unique_id(cloud_unique_id_);
    request.set_db_id(3001);
    request.set_txn_id(begin_target_transaction("consume-large-partition-batch"));
    TableStreamUpdatePB* stream_update = request.add_table_stream_updates();
    stream_update->mutable_identity()->CopyFrom(identity_);
    for (int i = 0; i < kPartitionCount; ++i) {
        int64_t partition_id = 30000 + i;
        TableStreamPartitionUpdatePB* update = stream_update->add_partition_updates();
        update->set_partition_id(partition_id);
        update->set_expected_state(TABLE_STREAM_OFFSET_UNKNOWN);
        update->set_next_offset_tso(40000 + i);
    }

    CommitTxnResponse response = commit_transaction(request);
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.status().msg();
    EXPECT_EQ(get_latest_offset(30000).offset_tso(), 40000);
    EXPECT_EQ(get_latest_offset(30000 + kPartitionCount / 2).offset_tso(),
              40000 + kPartitionCount / 2);
    EXPECT_EQ(get_latest_offset(30000 + kPartitionCount - 1).offset_tso(),
              40000 + kPartitionCount - 1);
}

TEST_F(MetaServiceTableStreamTest, CommitRejectsStaleAndInvalidOffsets) {
    set_multi_version_status(MULTI_VERSION_DISABLED);
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    put_latest_partition_state(txn.get(), 2001, 8, 130, 100);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    int64_t txn_id = begin_target_transaction("consume-stale-offset");
    CommitTxnResponse response =
            consume_partition(txn_id, 2001, TABLE_STREAM_OFFSET_CONSUMED, 99, 120);
    EXPECT_EQ(response.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    EXPECT_EQ(get_latest_offset(2001).offset_tso(), 100);

    txn_id = begin_target_transaction("consume-backward-offset");
    response = consume_partition(txn_id, 2001, TABLE_STREAM_OFFSET_CONSUMED, 100, 90);
    EXPECT_EQ(response.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    EXPECT_EQ(get_latest_offset(2001).offset_tso(), 100);

    txn_id = begin_target_transaction("consume-beyond-visible-tso");
    response = consume_partition(txn_id, 2001, TABLE_STREAM_OFFSET_CONSUMED, 100, 131);
    EXPECT_EQ(response.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    EXPECT_EQ(get_latest_offset(2001).offset_tso(), 100);
}

TEST_F(MetaServiceTableStreamTest, CommitValidatesOffsetTsoDomain) {
    set_multi_version_status(MULTI_VERSION_DISABLED);
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    put_latest_partition_state(txn.get(), 2001, 8, 130, -1);
    put_latest_partition_state(txn.get(), 2002, 9, 140, std::nullopt);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    CommitTxnResponse response =
            consume_partition(begin_target_transaction("consume-empty-boundary-existing"), 2001,
                              TABLE_STREAM_OFFSET_CONSUMED, -1, -1);
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.status().msg();
    EXPECT_EQ(get_latest_offset(2001).offset_tso(), -1);

    response = consume_partition(begin_target_transaction("consume-empty-boundary-unknown"), 2002,
                                 TABLE_STREAM_OFFSET_UNKNOWN, 0, -1);
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.status().msg();
    EXPECT_EQ(get_latest_offset(2002).offset_tso(), -1);

    for (int64_t invalid_tso : {0, -2}) {
        response = consume_partition(
                begin_target_transaction("consume-invalid-expected-" + std::to_string(invalid_tso)),
                2001, TABLE_STREAM_OFFSET_CONSUMED, invalid_tso, -1);
        EXPECT_EQ(response.status().code(), MetaServiceCode::INVALID_ARGUMENT);
        EXPECT_EQ(get_latest_offset(2001).offset_tso(), -1);

        response = consume_partition(
                begin_target_transaction("consume-invalid-next-" + std::to_string(invalid_tso)),
                2001, TABLE_STREAM_OFFSET_CONSUMED, -1, invalid_tso);
        EXPECT_EQ(response.status().code(), MetaServiceCode::INVALID_ARGUMENT);
        EXPECT_EQ(get_latest_offset(2001).offset_tso(), -1);
    }
}

TEST_F(MetaServiceTableStreamTest, CommitUsesCloneEffectiveOffset) {
    const std::string source_instance_id = "table_stream_read_state_source";
    set_clone_source(source_instance_id, Versionstamp(100, 0));

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    PartitionIndexPB partition_index;
    partition_index.set_db_id(identity_.base_db_id());
    partition_index.set_table_id(identity_.base_table_id());
    txn->put(versioned::partition_index_key({source_instance_id, 2001}),
             partition_index.SerializeAsString());
    versioned_put(txn.get(), versioned::meta_partition_key({source_instance_id, 2001}),
                  Versionstamp(41, 0), "");
    VersionPB version;
    version.set_version(18);
    version.set_commit_tso(230);
    versioned_put(txn.get(), versioned::partition_version_key({source_instance_id, 2001}),
                  Versionstamp(42, 0), version.SerializeAsString());
    TableStreamOffsetPB inherited_offset;
    inherited_offset.set_partition_id(2001);
    inherited_offset.set_state(TABLE_STREAM_OFFSET_INITIAL_SNAPSHOT_PENDING);
    inherited_offset.set_offset_tso(200);
    versioned_put(txn.get(),
                  versioned::table_stream_offset_key(
                          {source_instance_id, identity_.base_db_id(), identity_.base_table_id(),
                           identity_.stream_db_id(), identity_.stream_id(), 2001}),
                  Versionstamp(43, 0), inherited_offset.SerializeAsString());
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    int64_t txn_id = begin_target_transaction("consume-inherited-offset-as-unknown");
    CommitTxnResponse response =
            consume_partition(txn_id, 2001, TABLE_STREAM_OFFSET_UNKNOWN, 0, 220);
    EXPECT_EQ(response.status().code(), MetaServiceCode::INVALID_ARGUMENT);

    txn_id = begin_target_transaction("consume-inherited-offset");
    response =
            consume_partition(txn_id, 2001, TABLE_STREAM_OFFSET_INITIAL_SNAPSHOT_PENDING, 200, 220);
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.status().msg();
    EXPECT_EQ(get_latest_offset(2001).offset_tso(), 220);

    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string value;
    Versionstamp offset_version;
    ASSERT_EQ(
            versioned_get(txn.get(),
                          versioned::table_stream_offset_key(
                                  {instance_id_, identity_.base_db_id(), identity_.base_table_id(),
                                   identity_.stream_db_id(), identity_.stream_id(), 2001}),
                          &offset_version, &value),
            TxnErrorCode::TXN_OK);
    TableStreamOffsetPB versioned_offset;
    ASSERT_TRUE(versioned_offset.ParseFromString(value));
    EXPECT_EQ(versioned_offset.offset_tso(), 220);

    OperationLogPB operation_log;
    Versionstamp log_version;
    ASSERT_EQ(read_operation_log(txn.get(), versioned::log_key({instance_id_}), &log_version,
                                 &operation_log),
              TxnErrorCode::TXN_OK);
    ASSERT_TRUE(operation_log.has_commit_txn());
    ASSERT_EQ(operation_log.commit_txn().table_stream_offset_gc_size(), 1);
    const TableStreamPartitionSetPB& offset_gc =
            operation_log.commit_txn().table_stream_offset_gc(0);
    EXPECT_EQ(offset_gc.identity().SerializeAsString(), identity_.SerializeAsString());
    ASSERT_EQ(offset_gc.partition_ids_size(), 1);
    EXPECT_EQ(offset_gc.partition_ids(0), 2001);
}

TEST_F(MetaServiceTableStreamTest, CommitRejectsLatestOffsetWithoutVersionedOffset) {
    set_multi_version_status(MULTI_VERSION_READ_WRITE);
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    put_versioned_partition_state(txn.get(), 2001, 8, 130, std::nullopt);
    put_latest_partition_state(txn.get(), 2001, 8, 130, 100);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    int64_t txn_id = begin_target_transaction("consume-inconsistent-local-offset");
    CommitTxnResponse response =
            consume_partition(txn_id, 2001, TABLE_STREAM_OFFSET_UNKNOWN, 0, 120);
    EXPECT_EQ(response.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    EXPECT_EQ(get_latest_offset(2001).offset_tso(), 100);
}

TEST_F(MetaServiceTableStreamTest, CommitRejectsDifferentLatestAndVersionedOffsetValues) {
    set_multi_version_status(MULTI_VERSION_READ_WRITE);
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    put_versioned_partition_state(txn.get(), 2001, 8, 130, std::nullopt);

    TableStreamOffsetPB latest_offset;
    latest_offset.set_partition_id(2001);
    latest_offset.set_state(TABLE_STREAM_OFFSET_CONSUMED);
    latest_offset.set_offset_tso(100);
    latest_offset.set_last_consumption_time_ms(1234);
    TableStreamOffsetKeyInfo key_info {instance_id_,
                                       identity_.base_db_id(),
                                       identity_.base_table_id(),
                                       identity_.stream_db_id(),
                                       identity_.stream_id(),
                                       2001};
    txn->put(table_stream_offset_key(key_info), latest_offset.SerializeAsString());
    TableStreamOffsetPB versioned_offset = latest_offset;
    versioned_offset.set_last_consumption_time_ms(1235);
    versioned_put(txn.get(), versioned::table_stream_offset_key(key_info), Versionstamp(43, 0),
                  versioned_offset.SerializeAsString());
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    int64_t txn_id = begin_target_transaction("consume-inconsistent-offset-values");
    CommitTxnResponse response =
            consume_partition(txn_id, 2001, TABLE_STREAM_OFFSET_CONSUMED, 100, 120);
    EXPECT_EQ(response.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    EXPECT_EQ(get_latest_offset(2001).last_consumption_time_ms(), 1234);
}

TEST_F(MetaServiceTableStreamTest, CommitRejectsUnsupportedModeAndMissingCommitTso) {
    set_multi_version_status(MULTI_VERSION_DISABLED);
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    VersionPB version;
    version.set_version(8);
    txn->put(partition_version_key(
                     {instance_id_, identity_.base_db_id(), identity_.base_table_id(), 2001}),
             version.SerializeAsString());
    TableStreamOffsetPB offset;
    offset.set_partition_id(2001);
    offset.set_state(TABLE_STREAM_OFFSET_CONSUMED);
    offset.set_offset_tso(100);
    txn->put(table_stream_offset_key({instance_id_, identity_.base_db_id(),
                                      identity_.base_table_id(), identity_.stream_db_id(),
                                      identity_.stream_id(), 2001}),
             offset.SerializeAsString());
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    int64_t txn_id = begin_target_transaction("consume-missing-visible-tso");
    CommitTxnResponse response =
            consume_partition(txn_id, 2001, TABLE_STREAM_OFFSET_CONSUMED, 100, 110);
    EXPECT_EQ(response.status().code(), MetaServiceCode::VERSION_NOT_FOUND);
    EXPECT_EQ(get_latest_offset(2001).offset_tso(), 100);

    set_multi_version_status(MULTI_VERSION_ENABLED);
    txn_id = begin_target_transaction("consume-enabled-mode");
    response = consume_partition(txn_id, 2001, TABLE_STREAM_OFFSET_CONSUMED, 100, 110);
    EXPECT_EQ(response.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    EXPECT_EQ(get_latest_offset(2001).offset_tso(), 100);
}

TEST_F(MetaServiceTableStreamTest, CommitRejectsUnsupportedTxnModesAndForcesImmediate) {
    set_multi_version_status(MULTI_VERSION_DISABLED);
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    put_latest_partition_state(txn.get(), 2001, 8, 130, 100);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    CommitTxnRequest request = make_consume_request(begin_target_transaction("consume-2pc"), 2001,
                                                    TABLE_STREAM_OFFSET_CONSUMED, 100, 120);
    request.set_is_2pc(true);
    EXPECT_EQ(commit_transaction(request).status().code(), MetaServiceCode::INVALID_ARGUMENT);

    request = make_consume_request(begin_target_transaction("consume-txn-load"), 2001,
                                   TABLE_STREAM_OFFSET_CONSUMED, 100, 120);
    request.set_is_txn_load(true);
    EXPECT_EQ(commit_transaction(request).status().code(), MetaServiceCode::INVALID_ARGUMENT);

    request = make_consume_request(begin_target_transaction("consume-sub-txn"), 2001,
                                   TABLE_STREAM_OFFSET_CONSUMED, 100, 120);
    request.add_sub_txn_infos()->set_sub_txn_id(1);
    EXPECT_EQ(commit_transaction(request).status().code(), MetaServiceCode::INVALID_ARGUMENT);

    constexpr int64_t kTargetDbId = 3001;
    constexpr int64_t kTargetTableId = 3002;
    constexpr int64_t kTargetIndexId = 3003;
    constexpr int64_t kTargetPartitionId = 4001;
    constexpr int64_t kTargetTabletId = 5001;
    create_target_tablet(kTargetDbId, kTargetTableId, kTargetIndexId, kTargetPartitionId,
                         kTargetTabletId);

    const int old_rowset_threshold = config::txn_lazy_commit_rowsets_thresold;
    const int old_fuzzy_possibility = config::cloud_txn_lazy_commit_fuzzy_possibility;
    const bool old_lazy_commit_enabled = config::enable_cloud_txn_lazy_commit;
    config::txn_lazy_commit_rowsets_thresold = 0;
    config::cloud_txn_lazy_commit_fuzzy_possibility = 100;
    config::enable_cloud_txn_lazy_commit = true;
    DORIS_CLOUD_DEFER {
        config::txn_lazy_commit_rowsets_thresold = old_rowset_threshold;
        config::cloud_txn_lazy_commit_fuzzy_possibility = old_fuzzy_possibility;
        config::enable_cloud_txn_lazy_commit = old_lazy_commit_enabled;
        SyncPoint::get_instance()->clear_all_call_backs();
        SyncPoint::get_instance()->clear_trace();
        SyncPoint::get_instance()->disable_processing();
    };
    bool eventually_committed = false;
    SyncPoint::get_instance()->set_call_back("commit_txn_eventually::finish",
                                             [&](auto&&) { eventually_committed = true; });
    SyncPoint::get_instance()->enable_processing();

    const int64_t immediate_txn_id = begin_target_transaction("consume-immediate");
    stage_target_rowset(immediate_txn_id, kTargetPartitionId, kTargetTabletId);
    request = make_consume_request(immediate_txn_id, 2001, TABLE_STREAM_OFFSET_CONSUMED, 100, 120);
    request.set_enable_txn_lazy_commit(true);
    request.set_commit_tso(999);
    CommitTxnResponse response = commit_transaction(request);
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.status().msg();
    EXPECT_FALSE(response.is_lazy_commit());
    EXPECT_FALSE(eventually_committed);
    EXPECT_EQ(get_latest_offset(2001).offset_tso(), 120);

    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string value;
    EXPECT_EQ(txn->get(meta_rowset_tmp_key({instance_id_, immediate_txn_id, kTargetTabletId}),
                       &value),
              TxnErrorCode::TXN_KEY_NOT_FOUND);
    EXPECT_EQ(txn->get(meta_rowset_key({instance_id_, kTargetTabletId, 2}), &value),
              TxnErrorCode::TXN_OK);
}

TEST_F(MetaServiceTableStreamTest, CommitTxnTooLargeDoesNotFallBackToLazyCommit) {
    set_multi_version_status(MULTI_VERSION_DISABLED);
    constexpr int64_t kSourcePartitionId = 2001;
    constexpr int64_t kTargetDbId = 3001;
    constexpr int64_t kTargetTableId = 3002;
    constexpr int64_t kTargetIndexId = 3003;
    constexpr int64_t kTargetPartitionId = 4001;
    constexpr int64_t kTargetTabletId = 5001;

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    put_latest_partition_state(txn.get(), kSourcePartitionId, 8, 130, 100);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    create_target_tablet(kTargetDbId, kTargetTableId, kTargetIndexId, kTargetPartitionId,
                         kTargetTabletId);
    const int64_t txn_id = begin_target_transaction("consume-too-large");
    stage_target_rowset(txn_id, kTargetPartitionId, kTargetTabletId);

    const int old_rowset_threshold = config::txn_lazy_commit_rowsets_thresold;
    const int old_fuzzy_possibility = config::cloud_txn_lazy_commit_fuzzy_possibility;
    const bool old_lazy_commit_enabled = config::enable_cloud_txn_lazy_commit;
    config::txn_lazy_commit_rowsets_thresold = 0;
    config::cloud_txn_lazy_commit_fuzzy_possibility = 100;
    config::enable_cloud_txn_lazy_commit = true;
    DORIS_CLOUD_DEFER {
        config::txn_lazy_commit_rowsets_thresold = old_rowset_threshold;
        config::cloud_txn_lazy_commit_fuzzy_possibility = old_fuzzy_possibility;
        config::enable_cloud_txn_lazy_commit = old_lazy_commit_enabled;
        SyncPoint::get_instance()->clear_all_call_backs();
        SyncPoint::get_instance()->clear_trace();
        SyncPoint::get_instance()->disable_processing();
    };

    bool immediate_commit_injected = false;
    bool eventually_committed = false;
    SyncPoint::get_instance()->set_call_back(
            "commit_txn_immediately::before_commit", [&](auto&& args) {
                auto* err = try_any_cast<TxnErrorCode*>(args[0]);
                *err = TxnErrorCode::TXN_BYTES_TOO_LARGE;
                auto* code = try_any_cast<MetaServiceCode*>(args[1]);
                *code = cast_as<ErrCategory::COMMIT>(*err);
                auto* pred = try_any_cast<bool*>(args.back());
                *pred = true;
                immediate_commit_injected = true;
            });
    SyncPoint::get_instance()->set_call_back("commit_txn_eventually::finish",
                                             [&](auto&&) { eventually_committed = true; });
    SyncPoint::get_instance()->enable_processing();

    CommitTxnRequest request = make_consume_request(txn_id, kSourcePartitionId,
                                                    TABLE_STREAM_OFFSET_CONSUMED, 100, 120);
    request.set_enable_txn_lazy_commit(true);
    request.set_commit_tso(999);
    const CommitTxnResponse response = commit_transaction(request);
    EXPECT_EQ(response.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    EXPECT_NE(response.status().msg().find("table stream offset updates cannot use lazy commit"),
              std::string::npos);
    EXPECT_TRUE(immediate_commit_injected);
    EXPECT_FALSE(eventually_committed);
    EXPECT_FALSE(response.is_lazy_commit());
    EXPECT_EQ(get_latest_offset(kSourcePartitionId).offset_tso(), 100);

    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string value;
    EXPECT_EQ(txn->get(meta_rowset_tmp_key({instance_id_, txn_id, kTargetTabletId}), &value),
              TxnErrorCode::TXN_OK);
    EXPECT_EQ(txn->get(meta_rowset_key({instance_id_, kTargetTabletId, 2}), &value),
              TxnErrorCode::TXN_KEY_NOT_FOUND);
    ASSERT_EQ(txn->get(partition_version_key(
                               {instance_id_, kTargetDbId, kTargetTableId, kTargetPartitionId}),
                       &value),
              TxnErrorCode::TXN_OK);
    VersionPB target_version;
    ASSERT_TRUE(target_version.ParseFromString(value));
    EXPECT_EQ(target_version.version(), 1);
}

TEST_F(MetaServiceTableStreamTest, CommitWritesOffsetsForEachMultiVersionMode) {
    struct ModeCase {
        MultiVersionStatus mode;
        const char* label;
        bool succeeds;
        bool writes_versioned;
    };
    const std::vector<ModeCase> mode_cases = {
            {MULTI_VERSION_DISABLED, "disabled", true, false},
            {MULTI_VERSION_WRITE_ONLY, "write-only", true, true},
            {MULTI_VERSION_READ_WRITE, "read-write", true, true},
            {MULTI_VERSION_ENABLED, "enabled", false, false},
    };

    for (size_t i = 0; i < mode_cases.size(); ++i) {
        const ModeCase& mode_case = mode_cases[i];
        SCOPED_TRACE(mode_case.label);
        const int64_t partition_id = 2101 + i;
        const std::string log_prefix = versioned::log_key({instance_id_});

        if (mode_case.mode == MULTI_VERSION_READ_WRITE) {
            set_multi_version_status(mode_case.mode);
            std::unique_ptr<Transaction> txn;
            ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
            put_versioned_partition_state(txn.get(), partition_id, 8, 130, std::nullopt);
            TableStreamOffsetPB latest_offset;
            latest_offset.set_partition_id(partition_id);
            latest_offset.set_state(TABLE_STREAM_OFFSET_INITIAL_SNAPSHOT_PENDING);
            latest_offset.set_offset_tso(100);
            const TableStreamOffsetKeyInfo key_info {instance_id_,
                                                     identity_.base_db_id(),
                                                     identity_.base_table_id(),
                                                     identity_.stream_db_id(),
                                                     identity_.stream_id(),
                                                     partition_id};
            txn->put(table_stream_offset_key(key_info), latest_offset.SerializeAsString());
            versioned_put(txn.get(), versioned::table_stream_offset_key(key_info),
                          latest_offset.SerializeAsString());
            ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
        } else {
            set_multi_version_status(MULTI_VERSION_DISABLED);
            std::unique_ptr<Transaction> txn;
            ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
            put_latest_partition_state(txn.get(), partition_id, 8, 130, 100);
            ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
            if (mode_case.mode != MULTI_VERSION_DISABLED) {
                set_multi_version_status(mode_case.mode);
            }
        }

        const size_t log_count_before = count_keys(log_prefix, lexical_end(log_prefix));
        const TableStreamOffsetStatePB expected_state =
                mode_case.mode == MULTI_VERSION_READ_WRITE
                        ? TABLE_STREAM_OFFSET_INITIAL_SNAPSHOT_PENDING
                        : TABLE_STREAM_OFFSET_CONSUMED;
        const CommitTxnResponse response = consume_partition(
                begin_target_transaction(fmt::format("consume-mode-{}", mode_case.label)),
                partition_id, expected_state, 100, 120);
        EXPECT_EQ(response.status().code(),
                  mode_case.succeeds ? MetaServiceCode::OK : MetaServiceCode::INVALID_ARGUMENT)
                << response.status().msg();
        EXPECT_EQ(get_latest_offset(partition_id).offset_tso(), mode_case.succeeds ? 120 : 100);

        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        Versionstamp value_version;
        std::string value;
        const TxnErrorCode versioned_err = versioned_get(
                txn.get(),
                versioned::table_stream_offset_key(
                        {instance_id_, identity_.base_db_id(), identity_.base_table_id(),
                         identity_.stream_db_id(), identity_.stream_id(), partition_id}),
                &value_version, &value);
        if (mode_case.writes_versioned) {
            ASSERT_EQ(versioned_err, TxnErrorCode::TXN_OK);
            TableStreamOffsetPB versioned_offset;
            ASSERT_TRUE(versioned_offset.ParseFromString(value));
            EXPECT_EQ(versioned_offset.state(), TABLE_STREAM_OFFSET_CONSUMED);
            EXPECT_EQ(versioned_offset.offset_tso(), 120);
        } else {
            EXPECT_EQ(versioned_err, TxnErrorCode::TXN_KEY_NOT_FOUND);
        }
        EXPECT_EQ(count_keys(log_prefix, lexical_end(log_prefix)),
                  log_count_before + (mode_case.writes_versioned ? 1 : 0));
    }
}

TEST_F(MetaServiceTableStreamTest, DropWritesTypedRecycleIndexInDisabledMode) {
    set_multi_version_status(MULTI_VERSION_DISABLED);
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    IndexRequest request;
    request.set_cloud_unique_id(cloud_unique_id_);
    request.add_index_ids(identity_.stream_id());
    request.set_db_id(identity_.base_db_id());
    request.set_table_id(identity_.base_table_id());
    request.set_object_type(IndexObjectTypePB::TABLE_STREAM);
    request.set_stream_db_id(identity_.stream_db_id());
    request.set_expiration(0);
    IndexResponse response;
    brpc::Controller controller;
    service_->drop_index(&controller, &request, &response, nullptr);
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.status().msg();

    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string value;
    ASSERT_EQ(txn->get(recycle_index_key({instance_id_, identity_.stream_id()}), &value),
              TxnErrorCode::TXN_OK);
    RecycleIndexPB recycle_index;
    ASSERT_TRUE(recycle_index.ParseFromString(value));
    EXPECT_EQ(recycle_index.state(), RecycleIndexPB::DROPPED);
    EXPECT_EQ(recycle_index.object_type(), IndexObjectTypePB::TABLE_STREAM);
    EXPECT_EQ(recycle_index.db_id(), identity_.base_db_id());
    EXPECT_EQ(recycle_index.table_id(), identity_.base_table_id());
    EXPECT_EQ(recycle_index.stream_db_id(), identity_.stream_db_id());
}

TEST_F(MetaServiceTableStreamTest, DropWritesTypedOperationLogInVersionedMode) {
    set_multi_version_status(MULTI_VERSION_READ_WRITE);
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    IndexRequest request;
    request.set_cloud_unique_id(cloud_unique_id_);
    request.add_index_ids(identity_.stream_id());
    request.set_db_id(identity_.base_db_id());
    request.set_table_id(identity_.base_table_id());
    request.set_object_type(IndexObjectTypePB::TABLE_STREAM);
    request.set_stream_db_id(identity_.stream_db_id());
    request.set_expiration(0);
    IndexResponse response;
    brpc::Controller controller;
    service_->drop_index(&controller, &request, &response, nullptr);
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.status().msg();

    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    OperationLogPB operation_log;
    Versionstamp log_version;
    ASSERT_EQ(read_operation_log(txn.get(), versioned::log_key({instance_id_}), &log_version,
                                 &operation_log),
              TxnErrorCode::TXN_OK);
    ASSERT_TRUE(operation_log.has_drop_index());
    ASSERT_TRUE(operation_log.has_min_timestamp());
    EXPECT_EQ(operation_log.min_timestamp(),
              static_cast<uint64_t>(std::numeric_limits<int64_t>::max()));
    const DropIndexLogPB& drop_index = operation_log.drop_index();
    EXPECT_EQ(drop_index.object_type(), IndexObjectTypePB::TABLE_STREAM);
    EXPECT_EQ(drop_index.db_id(), identity_.base_db_id());
    EXPECT_EQ(drop_index.table_id(), identity_.base_table_id());
    EXPECT_EQ(drop_index.stream_db_id(), identity_.stream_db_id());
    ASSERT_EQ(drop_index.index_ids_size(), 1);
    EXPECT_EQ(drop_index.index_ids(0), identity_.stream_id());

    std::string value;
    EXPECT_EQ(txn->get(recycle_index_key({instance_id_, identity_.stream_id()}), &value, true),
              TxnErrorCode::TXN_KEY_NOT_FOUND);
}

TEST_F(MetaServiceTableStreamTest, DropRetryAfterMetaServiceRestartConverges) {
    const bool old_force_immediate_recycle = config::force_immediate_recycle;
    config::force_immediate_recycle = true;
    DORIS_CLOUD_DEFER {
        config::force_immediate_recycle = old_force_immediate_recycle;
    };

    set_multi_version_status(MULTI_VERSION_READ_WRITE);
    constexpr int64_t kPartitionId = 2001;
    put_auto_versioned_offset(instance_id_, identity_, kPartitionId, 100, true);

    IndexRequest request;
    request.set_cloud_unique_id(cloud_unique_id_);
    request.add_index_ids(identity_.stream_id());
    request.set_db_id(identity_.base_db_id());
    request.set_table_id(identity_.base_table_id());
    request.set_object_type(IndexObjectTypePB::TABLE_STREAM);
    request.set_stream_db_id(identity_.stream_db_id());
    request.set_expiration(0);

    // The first DROP succeeds, but its response is lost before the caller observes it.
    IndexResponse lost_response;
    brpc::Controller first_drop_controller;
    service_->drop_index(&first_drop_controller, &request, &lost_response, nullptr);
    ASSERT_EQ(lost_response.status().code(), MetaServiceCode::OK) << lost_response.status().msg();
    const std::string operation_log_prefix = versioned::log_key({instance_id_});
    const size_t first_drop_log_count =
            count_keys(operation_log_prefix, lexical_end(operation_log_prefix));
    ASSERT_GT(first_drop_log_count, 0);

    auto restarted_service = restart_meta_service();
    IndexResponse retry_response;
    brpc::Controller retry_drop_controller;
    restarted_service->drop_index(&retry_drop_controller, &request, &retry_response, nullptr);
    ASSERT_EQ(retry_response.status().code(), MetaServiceCode::OK) << retry_response.status().msg();
    EXPECT_GE(count_keys(operation_log_prefix, lexical_end(operation_log_prefix)),
              first_drop_log_count);

    InstanceInfoPB instance;
    instance.set_instance_id(instance_id_);
    instance.set_status(InstanceInfoPB::NORMAL);
    instance.set_multi_version_status(MULTI_VERSION_READ_WRITE);
    InstanceRecycler recycler(service_->txn_kv(), instance, RecyclerThreadPoolGroup {},
                              std::make_shared<TxnLazyCommitter>(service_->txn_kv()));
    ASSERT_EQ(recycler.init(), 0);
    ASSERT_EQ(recycler.recycle_operation_logs(), 0);
    EXPECT_EQ(count_keys(operation_log_prefix, lexical_end(operation_log_prefix)), 0);

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string value;
    ASSERT_EQ(txn->get(recycle_index_key({instance_id_, identity_.stream_id()}), &value, true),
              TxnErrorCode::TXN_OK);
    RecycleIndexPB recycle_index;
    ASSERT_TRUE(recycle_index.ParseFromString(value));
    EXPECT_EQ(recycle_index.state(), RecycleIndexPB::DROPPED);
    EXPECT_EQ(recycle_index.object_type(), IndexObjectTypePB::TABLE_STREAM);
    ASSERT_EQ(recycler.recycle_indexes(), 0);

    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    EXPECT_EQ(txn->get(recycle_index_key({instance_id_, identity_.stream_id()}), &value, true),
              TxnErrorCode::TXN_KEY_NOT_FOUND);
    const std::string latest_offset_prefix = table_stream_offset_key_prefix(
            instance_id_, identity_.base_db_id(), identity_.base_table_id(),
            identity_.stream_db_id(), identity_.stream_id());
    const std::string versioned_offset_prefix = versioned::table_stream_offset_key_prefix(
            instance_id_, identity_.base_db_id(), identity_.base_table_id(),
            identity_.stream_db_id(), identity_.stream_id());
    EXPECT_EQ(count_keys(latest_offset_prefix, lexical_end(latest_offset_prefix)), 0);
    EXPECT_EQ(count_keys(versioned_offset_prefix, lexical_end(versioned_offset_prefix)), 0);
}

TEST_F(MetaServiceTableStreamTest, DropUsesEarliestLocalVersionedOffset) {
    const std::string source_instance_id = "table_stream_drop_source";
    const Versionstamp source_version =
            put_auto_versioned_offset(source_instance_id, identity_, 2001, 10);
    set_clone_source(source_instance_id, Versionstamp(source_version.version() + 1, 0));
    // Partition 2002 sorts after partition 2001 but contains the earliest local version.
    const Versionstamp min_local_version =
            put_auto_versioned_offset(instance_id_, identity_, 2002, 30);
    put_auto_versioned_offset(instance_id_, identity_, 2001, 10);
    put_auto_versioned_offset(instance_id_, identity_, 2001, 20);

    IndexRequest request;
    request.set_cloud_unique_id(cloud_unique_id_);
    request.add_index_ids(identity_.stream_id());
    request.set_db_id(identity_.base_db_id());
    request.set_table_id(identity_.base_table_id());
    request.set_object_type(IndexObjectTypePB::TABLE_STREAM);
    request.set_stream_db_id(identity_.stream_db_id());
    request.set_expiration(0);
    IndexResponse response;
    brpc::Controller controller;
    service_->drop_index(&controller, &request, &response, nullptr);
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.status().msg();

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    OperationLogPB operation_log;
    Versionstamp log_version;
    ASSERT_EQ(read_operation_log(txn.get(), versioned::log_key({instance_id_}), &log_version,
                                 &operation_log),
              TxnErrorCode::TXN_OK);
    ASSERT_TRUE(operation_log.has_drop_index());
    ASSERT_TRUE(operation_log.has_min_timestamp());
    EXPECT_LT(source_version.version(), min_local_version.version());
    EXPECT_EQ(operation_log.min_timestamp(), min_local_version.version());
    EXPECT_LT(operation_log.min_timestamp(), log_version.version());
}

TEST_F(MetaServiceTableStreamTest, DropFindsMinimumInLargeVersionedOffsetHistory) {
    set_multi_version_status(MULTI_VERSION_READ_WRITE);
    const int default_full_range_batch_size = FullRangeGetOptions().batch_limit;

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    TableStreamOffsetPB offset;
    offset.set_state(TABLE_STREAM_OFFSET_CONSUMED);
    offset.set_offset_tso(100);
    for (int i = 0; i < default_full_range_batch_size; ++i) {
        offset.set_partition_id(2001);
        versioned_put(
                txn.get(),
                versioned::table_stream_offset_key(
                        {instance_id_, identity_.base_db_id(), identity_.base_table_id(),
                         identity_.stream_db_id(), identity_.stream_id(), offset.partition_id()}),
                Versionstamp(1000 + i, 0), offset.SerializeAsString());
    }
    // This key sorts after all partition 2001 history but carries the global minimum version.
    offset.set_partition_id(2002);
    versioned_put(txn.get(),
                  versioned::table_stream_offset_key(
                          {instance_id_, identity_.base_db_id(), identity_.base_table_id(),
                           identity_.stream_db_id(), identity_.stream_id(), offset.partition_id()}),
                  Versionstamp(50, 0), offset.SerializeAsString());
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    IndexRequest request;
    request.set_cloud_unique_id(cloud_unique_id_);
    request.add_index_ids(identity_.stream_id());
    request.set_db_id(identity_.base_db_id());
    request.set_table_id(identity_.base_table_id());
    request.set_object_type(IndexObjectTypePB::TABLE_STREAM);
    request.set_stream_db_id(identity_.stream_db_id());
    request.set_expiration(0);
    IndexResponse response;
    brpc::Controller controller;
    service_->drop_index(&controller, &request, &response, nullptr);
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.status().msg();

    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    OperationLogPB operation_log;
    Versionstamp log_version;
    ASSERT_EQ(read_operation_log(txn.get(), versioned::log_key({instance_id_}), &log_version,
                                 &operation_log),
              TxnErrorCode::TXN_OK);
    ASSERT_TRUE(operation_log.has_drop_index());
    ASSERT_TRUE(operation_log.has_min_timestamp());
    EXPECT_EQ(operation_log.min_timestamp(), 50);
}

TEST_F(MetaServiceTableStreamTest, DropAndRecycleCloneChildOffsets) {
    const bool old_force_immediate_recycle = config::force_immediate_recycle;
    config::force_immediate_recycle = true;
    DORIS_CLOUD_DEFER {
        config::force_immediate_recycle = old_force_immediate_recycle;
    };

    const std::string source_instance_id = "table_stream_drop_recycle_source";
    TableStreamIdentityPB local_identity = identity_;
    TableStreamIdentityPB inherited_identity = identity_;
    inherited_identity.set_stream_id(identity_.stream_id() + 1);
    put_auto_versioned_offset(source_instance_id, local_identity, 2001, 90);
    const Versionstamp source_version =
            put_auto_versioned_offset(source_instance_id, inherited_identity, 2001, 80);
    const Versionstamp source_snapshot = Versionstamp::next(source_version);
    set_clone_source(source_instance_id, source_snapshot);
    put_auto_versioned_offset(instance_id_, local_identity, 2001, 100, true);

    auto drop_stream = [&](const TableStreamIdentityPB& identity) {
        IndexRequest request;
        request.set_cloud_unique_id(cloud_unique_id_);
        request.add_index_ids(identity.stream_id());
        request.set_db_id(identity.base_db_id());
        request.set_table_id(identity.base_table_id());
        request.set_object_type(IndexObjectTypePB::TABLE_STREAM);
        request.set_stream_db_id(identity.stream_db_id());
        request.set_expiration(0);
        IndexResponse response;
        brpc::Controller controller;
        service_->drop_index(&controller, &request, &response, nullptr);
        EXPECT_EQ(response.status().code(), MetaServiceCode::OK) << response.status().msg();
    };
    drop_stream(local_identity);
    drop_stream(inherited_identity);

    InstanceInfoPB child_instance;
    child_instance.set_instance_id(instance_id_);
    child_instance.set_status(InstanceInfoPB::NORMAL);
    child_instance.set_multi_version_status(MULTI_VERSION_READ_WRITE);
    child_instance.set_source_instance_id(source_instance_id);
    child_instance.set_source_snapshot_id(SnapshotManager::serialize_snapshot_id(source_snapshot));
    InstanceRecycler recycler(service_->txn_kv(), child_instance, RecyclerThreadPoolGroup {},
                              std::make_shared<TxnLazyCommitter>(service_->txn_kv()));
    ASSERT_EQ(recycler.init(), 0);
    ASSERT_EQ(recycler.recycle_operation_logs(), 0);

    auto key_exists = [&](std::string_view key) {
        std::unique_ptr<Transaction> txn;
        EXPECT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string value;
        return txn->get(key, &value, true) == TxnErrorCode::TXN_OK;
    };
    EXPECT_TRUE(key_exists(recycle_index_key({instance_id_, local_identity.stream_id()})));
    EXPECT_TRUE(key_exists(recycle_index_key({instance_id_, inherited_identity.stream_id()})));

    ASSERT_EQ(recycler.recycle_indexes(), 0);
    const auto offset_count = [&](const std::string& prefix) {
        std::unique_ptr<Transaction> txn;
        EXPECT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::unique_ptr<RangeGetIterator> iter;
        EXPECT_EQ(txn->get(prefix, lexical_end(prefix), &iter, true, 0), TxnErrorCode::TXN_OK);
        return iter->size();
    };
    EXPECT_EQ(offset_count(table_stream_offset_key_prefix(
                      instance_id_, local_identity.base_db_id(), local_identity.base_table_id(),
                      local_identity.stream_db_id(), local_identity.stream_id())),
              0);
    EXPECT_EQ(offset_count(versioned::table_stream_offset_key_prefix(
                      instance_id_, local_identity.base_db_id(), local_identity.base_table_id(),
                      local_identity.stream_db_id(), local_identity.stream_id())),
              0);
    EXPECT_EQ(offset_count(versioned::table_stream_offset_key_prefix(
                      instance_id_, inherited_identity.base_db_id(),
                      inherited_identity.base_table_id(), inherited_identity.stream_db_id(),
                      inherited_identity.stream_id())),
              0);
    EXPECT_FALSE(key_exists(recycle_index_key({instance_id_, local_identity.stream_id()})));
    EXPECT_FALSE(key_exists(recycle_index_key({instance_id_, inherited_identity.stream_id()})));
    EXPECT_GT(
            offset_count(versioned::table_stream_offset_key_prefix(
                    source_instance_id, local_identity.base_db_id(), local_identity.base_table_id(),
                    local_identity.stream_db_id(), local_identity.stream_id())),
            0);
    EXPECT_GT(offset_count(versioned::table_stream_offset_key_prefix(
                      source_instance_id, inherited_identity.base_db_id(),
                      inherited_identity.base_table_id(), inherited_identity.stream_db_id(),
                      inherited_identity.stream_id())),
              0);
}

} // namespace doris::cloud
