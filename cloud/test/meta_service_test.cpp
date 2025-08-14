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

#include "meta-service/meta_service.h"

#include <brpc/controller.h>
#include <bvar/window.h>
#include <fmt/core.h>
#include <gen_cpp/cloud.pb.h>
#include <gen_cpp/olap_file.pb.h>
#include <google/protobuf/repeated_field.h>
#include <gtest/gtest.h>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <random>
#include <string>
#include <thread>

#include "common/config.h"
#include "common/logging.h"
#include "common/util.h"
#include "cpp/sync_point.h"
#include "meta-service/meta_service_helper.h"
#include "meta-store/document_message.h"
#include "meta-store/keys.h"
#include "meta-store/mem_txn_kv.h"
#include "meta-store/txn_kv.h"
#include "meta-store/txn_kv_error.h"
#include "meta-store/versioned_value.h"
#include "mock_resource_manager.h"
#include "rate-limiter/rate_limiter.h"
#include "resource-manager/resource_manager.h"

int main(int argc, char** argv) {
    const std::string conf_file = "doris_cloud.conf";
    if (!doris::cloud::config::init(conf_file.c_str(), true)) {
        std::cerr << "failed to init config file, conf=" << conf_file << std::endl;
        return -1;
    }

    config::enable_retry_txn_conflict = false;
    config::enable_txn_store_retry = true;
    config::txn_store_retry_base_intervals_ms = 1;
    config::txn_store_retry_times = 20;
    config::enable_check_instance_id = false;

    if (!doris::cloud::init_glog("meta_service_test")) {
        std::cerr << "failed to init glog" << std::endl;
        return -1;
    }
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace doris::cloud {

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

std::unique_ptr<MetaServiceProxy> get_meta_service() {
    return get_meta_service(true);
}

std::unique_ptr<MetaServiceProxy> get_fdb_meta_service() {
    config::fdb_cluster_file_path = "fdb.cluster";
    static auto txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<FdbTxnKv>());
    static std::atomic<bool> init {false};
    bool tmp = false;
    if (init.compare_exchange_strong(tmp, true)) {
        int ret = txn_kv->init();
        [&] {
            ASSERT_EQ(ret, 0);
            ASSERT_NE(txn_kv.get(), nullptr);
        }();
    }
    auto rs = std::make_shared<MockResourceManager>(txn_kv);
    auto rl = std::make_shared<RateLimiter>();
    auto meta_service = std::make_unique<MetaServiceImpl>(txn_kv, rs, rl);
    return std::make_unique<MetaServiceProxy>(std::move(meta_service));
}

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
    req.set_db_id(1); // default db_id
    add_tablet(req, table_id, index_id, partition_id, tablet_id);
    meta_service->create_tablets(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << tablet_id;
}

static void create_tablet_with_db_id(MetaServiceProxy* meta_service, int64_t db_id,
                                     int64_t table_id, int64_t index_id, int64_t partition_id,
                                     int64_t tablet_id) {
    brpc::Controller cntl;
    CreateTabletsRequest req;
    CreateTabletsResponse res;
    req.set_db_id(db_id);
    add_tablet(req, table_id, index_id, partition_id, tablet_id);
    meta_service->create_tablets(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << tablet_id;
}

static void begin_txn(MetaServiceProxy* meta_service, int64_t db_id, const std::string& label,
                      int64_t table_id, int64_t& txn_id) {
    brpc::Controller cntl;
    BeginTxnRequest req;
    BeginTxnResponse res;
    auto txn_info = req.mutable_txn_info();
    txn_info->set_db_id(db_id);
    txn_info->set_label(label);
    txn_info->add_table_ids(table_id);
    txn_info->set_timeout_ms(36000);
    meta_service->begin_txn(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << label;
    ASSERT_TRUE(res.has_txn_id()) << label;
    txn_id = res.txn_id();
}

static void commit_txn(MetaServiceProxy* meta_service, int64_t db_id, int64_t txn_id,
                       const std::string& label) {
    brpc::Controller cntl;
    CommitTxnRequest req;
    CommitTxnResponse res;
    req.set_db_id(db_id);
    req.set_txn_id(txn_id);
    meta_service->commit_txn(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << label;
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

static void prepare_rowset(MetaServiceProxy* meta_service, const doris::RowsetMetaCloudPB& rowset,
                           CreateRowsetResponse& res) {
    brpc::Controller cntl;
    auto arena = res.GetArena();
    auto req = google::protobuf::Arena::CreateMessage<CreateRowsetRequest>(arena);
    req->mutable_rowset_meta()->CopyFrom(rowset);
    meta_service->prepare_rowset(&cntl, req, &res, nullptr);
    if (!arena) delete req;
}

void commit_rowset(MetaServiceProxy* meta_service, const doris::RowsetMetaCloudPB& rowset,
                   CreateRowsetResponse& res) {
    brpc::Controller cntl;
    auto arena = res.GetArena();
    auto req = google::protobuf::Arena::CreateMessage<CreateRowsetRequest>(arena);
    req->mutable_rowset_meta()->CopyFrom(rowset);
    meta_service->commit_rowset(&cntl, req, &res, nullptr);
    if (!arena) delete req;
}

static void update_tmp_rowset(MetaServiceProxy* meta_service,
                              const doris::RowsetMetaCloudPB& rowset, CreateRowsetResponse& res) {
    brpc::Controller cntl;
    auto arena = res.GetArena();
    auto req = google::protobuf::Arena::CreateMessage<CreateRowsetRequest>(arena);
    req->mutable_rowset_meta()->CopyFrom(rowset);
    meta_service->update_tmp_rowset(&cntl, req, &res, nullptr);
    if (!arena) delete req;
}

static void get_delete_bitmap_update_lock(MetaServiceProxy* meta_service,
                                          GetDeleteBitmapUpdateLockResponse& res, int64_t db_id,
                                          int64_t table_id, int64_t index_id,
                                          const std::vector<std::array<int64_t, 2>>& tablet_idxes,
                                          int64_t expiration, int64_t lock_id, int64_t initiator,
                                          bool require_compaction_stats) {
    brpc::Controller cntl;
    GetDeleteBitmapUpdateLockRequest req;
    req.set_cloud_unique_id("test_cloud_unique_id");
    req.set_table_id(table_id);
    for (const auto& [partition_id, _] : tablet_idxes) {
        req.add_partition_ids(partition_id);
    }
    req.set_expiration(expiration);
    req.set_lock_id(lock_id);
    req.set_initiator(initiator);
    req.set_require_compaction_stats(require_compaction_stats);
    for (const auto& [partition_id, tablet_id] : tablet_idxes) {
        TabletIndexPB* idx = req.add_tablet_indexes();
        idx->set_db_id(db_id);
        idx->set_table_id(table_id);
        idx->set_index_id(index_id);
        idx->set_partition_id(partition_id);
        idx->set_tablet_id(tablet_id);
    }
    meta_service->get_delete_bitmap_update_lock(
            reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
}

void insert_rowset(MetaServiceProxy* meta_service, int64_t db_id, const std::string& label,
                   int64_t table_id, int64_t partition_id, int64_t tablet_id) {
    int64_t txn_id = 0;
    ASSERT_NO_FATAL_FAILURE(begin_txn(meta_service, db_id, label, table_id, txn_id));
    CreateRowsetResponse res;
    auto rowset = create_rowset(txn_id, tablet_id, partition_id);
    prepare_rowset(meta_service, rowset, res);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << label;
    res.Clear();
    ASSERT_NO_FATAL_FAILURE(commit_rowset(meta_service, rowset, res));
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << label;
    commit_txn(meta_service, db_id, txn_id, label);
}

static void add_tablet_metas(MetaServiceProxy* meta_service, std::string instance_id,
                             int64_t table_id, int64_t index_id,
                             const std::vector<std::array<int64_t, 2>>& tablet_idxes) {
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);

    for (const auto& idx : tablet_idxes) {
        int64_t partition_id = idx[0];
        int64_t tablet_id = idx[1];
        std::string stats_key =
                stats_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
        TabletStatsPB stats;
        stats.set_base_compaction_cnt(10);
        stats.set_cumulative_compaction_cnt(20);
        stats.set_cumulative_point(30);
        txn->put(stats_key, stats.SerializeAsString());

        doris::TabletMetaCloudPB tablet_pb;
        tablet_pb.set_table_id(table_id);
        tablet_pb.set_index_id(index_id);
        tablet_pb.set_partition_id(partition_id);
        tablet_pb.set_tablet_id(tablet_id);
        tablet_pb.set_tablet_state(doris::TabletStatePB::PB_RUNNING);
        auto tablet_meta_key =
                meta_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
        auto tablet_meta_val = tablet_pb.SerializeAsString();
        txn->put(tablet_meta_key, tablet_meta_val);
    }
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
}

static void start_compaction_job(MetaService* meta_service, int64_t tablet_id,
                                 const std::string& job_id, const std::string& initiator,
                                 int base_compaction_cnt, int cumu_compaction_cnt,
                                 TabletCompactionJobPB::CompactionType type,
                                 StartTabletJobResponse& res,
                                 std::pair<int64_t, int64_t> input_version = {0, 0}) {
    brpc::Controller cntl;
    StartTabletJobRequest req;
    req.mutable_job()->mutable_idx()->set_tablet_id(tablet_id);
    auto compaction = req.mutable_job()->add_compaction();
    compaction->set_id(job_id);
    compaction->set_initiator(initiator);
    compaction->set_base_compaction_cnt(base_compaction_cnt);
    compaction->set_cumulative_compaction_cnt(cumu_compaction_cnt);
    compaction->set_type(type);
    long now = time(nullptr);
    compaction->set_expiration(now + 12);
    compaction->set_lease(now + 3);
    if (input_version.second > 0) {
        compaction->add_input_versions(input_version.first);
        compaction->add_input_versions(input_version.second);
        compaction->set_check_input_versions_range(true);
    }
    meta_service->start_tablet_job(&cntl, &req, &res, nullptr);
};

TEST(MetaServiceTest, GetInstanceIdTest) {
    extern std::string get_instance_id(const std::shared_ptr<ResourceManager>& rc_mgr,
                                       const std::string& cloud_unique_id);
    auto meta_service = get_meta_service();
    auto sp = SyncPoint::get_instance();
    DORIS_CLOUD_DEFER {
        SyncPoint::get_instance()->clear_all_call_backs();
    };
    sp->set_call_back("get_instance_id_err", [&](auto&& args) {
        std::string* err = try_any_cast<std::string*>(args[0]);
        *err = "can't find node from cache";
    });
    sp->enable_processing();

    auto instance_id =
            get_instance_id(meta_service->resource_mgr(), "1:ALBJLH4Q:m-n3qdpyal27rh8iprxx");
    ASSERT_EQ(instance_id, "ALBJLH4Q");

    // version not support
    instance_id = get_instance_id(meta_service->resource_mgr(), "2:ALBJLH4Q:m-n3qdpyal27rh8iprxx");
    ASSERT_EQ(instance_id, "");

    // degraded format err
    instance_id = get_instance_id(meta_service->resource_mgr(), "1:ALBJLH4Q");
    ASSERT_EQ(instance_id, "");

    // std::invalid_argument
    instance_id = get_instance_id(meta_service->resource_mgr(),
                                  "invalid_version:ALBJLH4Q:m-n3qdpyal27rh8iprxx");
    ASSERT_EQ(instance_id, "");

    // std::out_of_range
    instance_id = get_instance_id(meta_service->resource_mgr(),
                                  "12345678901:ALBJLH4Q:m-n3qdpyal27rh8iprxx");
    ASSERT_EQ(instance_id, "");

    config::enable_check_instance_id = true;
    auto ms = get_meta_service(false);
    instance_id =
            get_instance_id(ms->resource_mgr(), "1:ALBJLH4Q-check-invalid:m-n3qdpyal27rh8iprxx");
    ASSERT_EQ(instance_id, "");

    sp->set_call_back("is_instance_id_registered", [&](auto&& args) {
        TxnErrorCode* c0 = try_any_cast<TxnErrorCode*>(args[0]);
        *c0 = TxnErrorCode::TXN_OK;
    });
    instance_id =
            get_instance_id(ms->resource_mgr(), "1:ALBJLH4Q-check-invalid:m-n3qdpyal27rh8iprxx");
    ASSERT_EQ(instance_id, "ALBJLH4Q-check-invalid");
    config::enable_check_instance_id = false;

    sp->clear_all_call_backs();
    sp->clear_trace();
    sp->disable_processing();
}

TEST(MetaServiceTest, CreateInstanceTest) {
    auto meta_service = get_meta_service();

    // case: normal create instance with obj info
    {
        brpc::Controller cntl;
        CreateInstanceRequest req;
        req.set_instance_id("test_instance");
        req.set_user_id("test_user");
        req.set_name("test_name");
        ObjectStoreInfoPB obj;
        obj.set_ak("123");
        obj.set_sk("321");
        obj.set_bucket("456");
        obj.set_prefix("654");
        obj.set_endpoint("789");
        obj.set_region("987");
        obj.set_external_endpoint("888");
        obj.set_provider(ObjectStoreInfoPB::BOS);
        req.mutable_obj_info()->CopyFrom(obj);

        auto sp = SyncPoint::get_instance();
        sp->set_call_back("encrypt_ak_sk:get_encryption_key", [](auto&& args) {
            auto* ret = try_any_cast<int*>(args[0]);
            *ret = 0;
            auto* key = try_any_cast<std::string*>(args[1]);
            *key = "test";
            auto* key_id = try_any_cast<int64_t*>(args[2]);
            *key_id = 1;
        });
        sp->enable_processing();
        CreateInstanceResponse res;
        meta_service->create_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        sp->clear_all_call_backs();
        sp->clear_trace();
        sp->disable_processing();
    }

    // case: normal create instance without obj info and hdfs info
    {
        brpc::Controller cntl;
        CreateInstanceRequest req;
        req.set_instance_id("test_instance_without_hdfs_and_obj");
        req.set_user_id("test_user");
        req.set_name("test_name");

        auto sp = SyncPoint::get_instance();
        sp->set_call_back("encrypt_ak_sk:get_encryption_key", [](auto&& args) {
            auto* ret = try_any_cast<int*>(args[0]);
            *ret = 0;
            auto* key = try_any_cast<std::string*>(args[1]);
            *key = "test";
            auto* key_id = try_any_cast<int64_t*>(args[2]);
            *key_id = 1;
        });
        sp->enable_processing();
        CreateInstanceResponse res;
        meta_service->create_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        sp->clear_all_call_backs();
        sp->clear_trace();
        sp->disable_processing();
    }

    // case: normal create instance with hdfs info
    {
        brpc::Controller cntl;
        CreateInstanceRequest req;
        req.set_instance_id("test_instance_with_hdfs_info");
        req.set_user_id("test_user");
        req.set_name("test_name");
        HdfsVaultInfo hdfs;
        HdfsBuildConf conf;
        conf.set_fs_name("hdfs://127.0.0.1:8020");
        conf.set_user("test_user");
        hdfs.mutable_build_conf()->CopyFrom(conf);
        StorageVaultPB vault;
        vault.mutable_hdfs_info()->CopyFrom(hdfs);
        req.mutable_vault()->CopyFrom(vault);

        auto sp = SyncPoint::get_instance();
        sp->set_call_back("encrypt_ak_sk:get_encryption_key", [](auto&& args) {
            auto* ret = try_any_cast<int*>(args[0]);
            *ret = 0;
            auto* key = try_any_cast<std::string*>(args[1]);
            *key = "test";
            auto* key_id = try_any_cast<int64_t*>(args[2]);
            *key_id = 1;
        });
        sp->enable_processing();
        CreateInstanceResponse res;
        meta_service->create_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        sp->clear_all_call_backs();
        sp->clear_trace();
        sp->disable_processing();
    }

    // case: request has invalid argument
    {
        brpc::Controller cntl;
        CreateInstanceRequest req;
        CreateInstanceResponse res;
        meta_service->create_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    }

    // case: normal drop instance
    {
        brpc::Controller cntl;
        AlterInstanceRequest req;
        AlterInstanceResponse res;
        req.set_op(AlterInstanceRequest::DROP);
        req.set_instance_id("test_instance");
        meta_service->alter_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        InstanceKeyInfo key_info {"test_instance"};
        std::string key;
        std::string val;
        instance_key(key_info, &key);
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        ASSERT_EQ(txn->get(key, &val), TxnErrorCode::TXN_OK);
        InstanceInfoPB instance;
        instance.ParseFromString(val);
        ASSERT_EQ(instance.status(), InstanceInfoPB::DELETED);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // case: normal refresh instance
    {
        brpc::Controller cntl;
        AlterInstanceRequest req;
        AlterInstanceResponse res;
        req.set_op(AlterInstanceRequest::REFRESH);
        req.set_instance_id("test_instance");
        meta_service->alter_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // case: rpc get instance
    {
        brpc::Controller cntl;
        GetInstanceRequest req;
        GetInstanceResponse res;
        meta_service->get_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                   &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
        req.set_cloud_unique_id("1:test_instance:m-n3qdpyal27rh8iprxx");
        meta_service->get_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                   &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }
}

TEST(MetaServiceTest, AlterS3StorageVaultTest) {
    auto meta_service = get_meta_service();

    auto sp = SyncPoint::get_instance();
    sp->enable_processing();
    sp->set_call_back("encrypt_ak_sk:get_encryption_key", [](auto&& args) {
        auto* ret = try_any_cast<int*>(args[0]);
        *ret = 0;
        auto* key = try_any_cast<std::string*>(args[1]);
        *key = "selectdbselectdbselectdbselectdb";
        auto* key_id = try_any_cast<int64_t*>(args[2]);
        *key_id = 1;
    });
    std::pair<std::string, std::string> pair;
    sp->set_call_back("extract_object_storage_info:get_aksk_pair", [&](auto&& args) {
        auto* ret = try_any_cast<std::pair<std::string, std::string>*>(args[0]);
        pair = *ret;
    });

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string key;
    std::string val;
    InstanceKeyInfo key_info {"test_instance"};
    instance_key(key_info, &key);

    ObjectStoreInfoPB obj_info;
    obj_info.set_id("1");
    obj_info.set_ak("ak");
    obj_info.set_sk("sk");
    StorageVaultPB vault;
    constexpr char vault_name[] = "test_alter_s3_vault";
    vault.mutable_obj_info()->MergeFrom(obj_info);
    vault.set_name(vault_name);
    vault.set_id("2");
    InstanceInfoPB instance;
    instance.add_storage_vault_names(vault.name());
    instance.add_resource_ids(vault.id());
    instance.set_instance_id("GetObjStoreInfoTestInstance");
    val = instance.SerializeAsString();
    txn->put(key, val);
    txn->put(storage_vault_key({instance.instance_id(), "2"}), vault.SerializeAsString());
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    txn = nullptr;

    auto get_test_instance = [&](InstanceInfoPB& i) {
        std::string key;
        std::string val;
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        InstanceKeyInfo key_info {"test_instance"};
        instance_key(key_info, &key);
        ASSERT_EQ(txn->get(key, &val), TxnErrorCode::TXN_OK);
        i.ParseFromString(val);
    };

    {
        AlterObjStoreInfoRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_op(AlterObjStoreInfoRequest::ALTER_S3_VAULT);
        StorageVaultPB vault;
        vault.mutable_obj_info()->set_ak("new_ak");
        vault.set_name(vault_name);
        req.mutable_vault()->CopyFrom(vault);

        brpc::Controller cntl;
        AlterObjStoreInfoResponse res;
        meta_service->alter_storage_vault(
                reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
        ASSERT_NE(res.status().code(), MetaServiceCode::OK) << res.status().msg();
        InstanceInfoPB instance;
        get_test_instance(instance);

        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string val;
        ASSERT_EQ(txn->get(storage_vault_key({instance.instance_id(), "2"}), &val),
                  TxnErrorCode::TXN_OK);
        StorageVaultPB get_obj;
        get_obj.ParseFromString(val);
        ASSERT_EQ(get_obj.obj_info().ak(), "ak") << get_obj.obj_info().ak();
    }

    {
        AlterObjStoreInfoRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_op(AlterObjStoreInfoRequest::ALTER_S3_VAULT);
        StorageVaultPB vault;
        ObjectStoreInfoPB obj;
        obj_info.set_ak("new_ak");
        vault.mutable_obj_info()->MergeFrom(obj);
        vault.set_name("test_alter_s3_vault_non_exist");
        req.mutable_vault()->CopyFrom(vault);

        brpc::Controller cntl;
        AlterObjStoreInfoResponse res;
        meta_service->alter_storage_vault(
                reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
        ASSERT_NE(res.status().code(), MetaServiceCode::OK) << res.status().msg();
    }

    {
        AlterObjStoreInfoRequest req;
        constexpr char new_vault_name[] = "@!#vault_name";
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_op(AlterObjStoreInfoRequest::ALTER_S3_VAULT);
        StorageVaultPB vault;
        vault.set_alter_name(new_vault_name);
        ObjectStoreInfoPB obj;
        obj_info.set_ak("new_ak");
        vault.mutable_obj_info()->MergeFrom(obj);
        vault.set_name(vault_name);
        req.mutable_vault()->CopyFrom(vault);

        brpc::Controller cntl;
        AlterObjStoreInfoResponse res;
        meta_service->alter_storage_vault(
                reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
        ASSERT_NE(res.status().code(), MetaServiceCode::OK) << res.status().msg();
        ASSERT_TRUE(res.status().msg().find("invalid storage vault name") != std::string::npos)
                << res.status().msg();
    }

    {
        AlterObjStoreInfoRequest req;
        constexpr char new_vault_name[] = "new_test_alter_s3_vault";
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_op(AlterObjStoreInfoRequest::ALTER_S3_VAULT);
        StorageVaultPB vault;
        vault.set_alter_name(new_vault_name);
        ObjectStoreInfoPB obj;
        obj_info.set_ak("new_ak");
        obj_info.set_sk("new_sk");
        vault.mutable_obj_info()->MergeFrom(obj);
        vault.set_name(vault_name);
        req.mutable_vault()->CopyFrom(vault);

        brpc::Controller cntl;
        AlterObjStoreInfoResponse res;
        meta_service->alter_storage_vault(
                reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();

        {
            AlterObjStoreInfoRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            req.set_op(AlterObjStoreInfoRequest::ALTER_S3_VAULT);
            StorageVaultPB vault;
            vault.set_alter_name(new_vault_name);
            ObjectStoreInfoPB obj;
            obj_info.set_ak("new_ak");
            obj_info.set_sk("new_sk");
            vault.mutable_obj_info()->MergeFrom(obj);
            vault.set_name(new_vault_name);
            req.mutable_vault()->CopyFrom(vault);
            meta_service->alter_storage_vault(
                    reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res,
                    nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::ALREADY_EXISTED) << res.status().msg();
        }

        InstanceInfoPB instance;
        get_test_instance(instance);

        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string val;
        ASSERT_EQ(txn->get(storage_vault_key({instance.instance_id(), "2"}), &val),
                  TxnErrorCode::TXN_OK);
        StorageVaultPB get_obj;
        get_obj.ParseFromString(val);
        ASSERT_EQ(get_obj.name(), new_vault_name) << get_obj.obj_info().ak();
    }

    SyncPoint::get_instance()->disable_processing();
    SyncPoint::get_instance()->clear_all_call_backs();
}

TEST(MetaServiceTest, AlterHdfsStorageVaultTest) {
    auto meta_service = get_meta_service();

    auto sp = SyncPoint::get_instance();
    sp->enable_processing();
    sp->set_call_back("encrypt_ak_sk:get_encryption_key", [](auto&& args) {
        auto* ret = try_any_cast<int*>(args[0]);
        *ret = 0;
        auto* key = try_any_cast<std::string*>(args[1]);
        *key = "selectdbselectdbselectdbselectdb";
        auto* key_id = try_any_cast<int64_t*>(args[2]);
        *key_id = 1;
    });
    std::pair<std::string, std::string> pair;
    sp->set_call_back("extract_object_storage_info:get_aksk_pair", [&](auto&& args) {
        auto* ret = try_any_cast<std::pair<std::string, std::string>*>(args[0]);
        pair = *ret;
    });

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string key;
    std::string val;
    InstanceKeyInfo key_info {"test_instance"};
    instance_key(key_info, &key);

    HdfsBuildConf hdfs_build_conf;
    hdfs_build_conf.set_fs_name("fs_name");
    hdfs_build_conf.set_user("root");
    HdfsVaultInfo hdfs_info;
    hdfs_info.set_prefix("root_path");
    hdfs_info.mutable_build_conf()->MergeFrom(hdfs_build_conf);
    StorageVaultPB vault;
    constexpr char vault_name[] = "test_alter_hdfs_vault";
    vault.mutable_hdfs_info()->MergeFrom(hdfs_info);
    vault.set_name(vault_name);
    vault.set_id("2");
    InstanceInfoPB instance;
    instance.add_storage_vault_names(vault.name());
    instance.add_resource_ids(vault.id());
    instance.set_instance_id("GetObjStoreInfoTestInstance");
    val = instance.SerializeAsString();
    txn->put(key, val);
    txn->put(storage_vault_key({instance.instance_id(), "2"}), vault.SerializeAsString());
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    txn = nullptr;

    auto get_test_instance = [&](InstanceInfoPB& i) {
        std::string key;
        std::string val;
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        InstanceKeyInfo key_info {"test_instance"};
        instance_key(key_info, &key);
        ASSERT_EQ(txn->get(key, &val), TxnErrorCode::TXN_OK);
        i.ParseFromString(val);
    };

    {
        AlterObjStoreInfoRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_op(AlterObjStoreInfoRequest::ALTER_HDFS_VAULT);
        StorageVaultPB vault;
        vault.mutable_hdfs_info()->mutable_build_conf()->set_user("hadoop");
        vault.set_name(vault_name);
        req.mutable_vault()->CopyFrom(vault);

        brpc::Controller cntl;
        AlterObjStoreInfoResponse res;
        meta_service->alter_storage_vault(
                reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();

        InstanceInfoPB instance;
        get_test_instance(instance);

        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string val;
        ASSERT_EQ(txn->get(storage_vault_key({instance.instance_id(), "2"}), &val),
                  TxnErrorCode::TXN_OK);
        StorageVaultPB get_obj;
        get_obj.ParseFromString(val);
        ASSERT_EQ(get_obj.hdfs_info().build_conf().user(), "hadoop")
                << get_obj.hdfs_info().build_conf().fs_name();
    }

    {
        AlterObjStoreInfoRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_op(AlterObjStoreInfoRequest::ALTER_HDFS_VAULT);
        StorageVaultPB vault;
        auto* hdfs = vault.mutable_hdfs_info();
        hdfs->set_prefix("fake_one");
        vault.set_name("test_alter_hdfs_vault_non_exist");
        req.mutable_vault()->CopyFrom(vault);

        brpc::Controller cntl;
        AlterObjStoreInfoResponse res;
        meta_service->alter_storage_vault(
                reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
        ASSERT_NE(res.status().code(), MetaServiceCode::OK) << res.status().msg();
    }

    {
        AlterObjStoreInfoRequest req;
        constexpr char new_vault_name[] = "Thi213***@fakeVault";
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_op(AlterObjStoreInfoRequest::ALTER_HDFS_VAULT);
        StorageVaultPB vault;
        vault.mutable_hdfs_info()->mutable_build_conf()->set_user("hadoop");
        vault.set_name(vault_name);
        vault.set_alter_name(new_vault_name);
        req.mutable_vault()->CopyFrom(vault);

        brpc::Controller cntl;
        AlterObjStoreInfoResponse res;
        meta_service->alter_storage_vault(
                reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
        ASSERT_NE(res.status().code(), MetaServiceCode::OK) << res.status().msg();
        ASSERT_TRUE(res.status().msg().find("invalid storage vault name") != std::string::npos)
                << res.status().msg();
    }

    {
        AlterObjStoreInfoRequest req;
        constexpr char new_vault_name[] = "new_test_alter_hdfs_vault";
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_op(AlterObjStoreInfoRequest::ALTER_HDFS_VAULT);
        StorageVaultPB vault;
        vault.mutable_hdfs_info()->mutable_build_conf()->set_user("hadoop");
        vault.set_name(vault_name);
        vault.set_alter_name(new_vault_name);
        req.mutable_vault()->CopyFrom(vault);

        brpc::Controller cntl;
        AlterObjStoreInfoResponse res;
        meta_service->alter_storage_vault(
                reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();

        {
            AlterObjStoreInfoRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            req.set_op(AlterObjStoreInfoRequest::ALTER_HDFS_VAULT);
            StorageVaultPB vault;
            vault.mutable_hdfs_info()->mutable_build_conf()->set_user("hadoop");
            vault.set_name(new_vault_name);
            vault.set_alter_name(new_vault_name);
            req.mutable_vault()->CopyFrom(vault);

            brpc::Controller cntl;
            AlterObjStoreInfoResponse res;
            meta_service->alter_storage_vault(
                    reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res,
                    nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::ALREADY_EXISTED) << res.status().msg();
        }

        InstanceInfoPB instance;
        get_test_instance(instance);

        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string val;
        ASSERT_EQ(txn->get(storage_vault_key({instance.instance_id(), "2"}), &val),
                  TxnErrorCode::TXN_OK);
        StorageVaultPB get_obj;
        get_obj.ParseFromString(val);
        ASSERT_EQ(get_obj.name(), new_vault_name) << get_obj.obj_info().ak();
    }

    SyncPoint::get_instance()->disable_processing();
    SyncPoint::get_instance()->clear_all_call_backs();
}

TEST(MetaServiceTest, AlterClusterTest) {
    auto meta_service = get_meta_service();
    ASSERT_NE(meta_service, nullptr);

    // case: normal add cluster
    {
        brpc::Controller cntl;
        AlterClusterRequest req;
        req.set_instance_id(mock_instance);
        req.mutable_cluster()->set_cluster_name(mock_cluster_name);
        req.set_op(AlterClusterRequest::ADD_CLUSTER);
        AlterClusterResponse res;
        meta_service->alter_cluster(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // case: request has invalid argument
    {
        brpc::Controller cntl;
        AlterClusterRequest req;
        req.set_op(AlterClusterRequest::DROP_CLUSTER);
        AlterClusterResponse res;
        meta_service->alter_cluster(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    }

    // add node
    {
        brpc::Controller cntl;
        AlterClusterRequest req;
        req.set_instance_id(mock_instance);
        req.set_op(AlterClusterRequest::ADD_NODE);
        req.mutable_cluster()->set_cluster_name(mock_cluster_name);
        req.mutable_cluster()->set_cluster_id(mock_cluster_id);
        req.mutable_cluster()->set_type(ClusterPB::COMPUTE);
        auto node = req.mutable_cluster()->add_nodes();
        node->set_ip("127.0.0.1");
        node->set_heartbeat_port(9999);
        AlterClusterResponse res;
        meta_service->alter_cluster(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // drop node
    {
        brpc::Controller cntl;
        AlterClusterRequest req;
        req.set_instance_id(mock_instance);
        req.set_op(AlterClusterRequest::DROP_NODE);
        req.mutable_cluster()->set_cluster_name(mock_cluster_name);
        req.mutable_cluster()->set_cluster_id(mock_cluster_id);
        req.mutable_cluster()->set_type(ClusterPB::COMPUTE);
        auto node = req.mutable_cluster()->add_nodes();
        node->set_ip("127.0.0.1");
        node->set_heartbeat_port(9999);
        AlterClusterResponse res;
        meta_service->alter_cluster(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // rename cluster
    {
        brpc::Controller cntl;
        AlterClusterRequest req;
        req.set_instance_id(mock_instance);
        req.mutable_cluster()->set_cluster_id(mock_cluster_id);
        req.mutable_cluster()->set_cluster_name("rename_cluster_name");
        req.set_op(AlterClusterRequest::RENAME_CLUSTER);
        AlterClusterResponse res;
        meta_service->alter_cluster(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // set cluster status
    {
        brpc::Controller cntl;
        AlterClusterRequest req;
        req.set_instance_id(mock_instance);
        req.mutable_cluster()->set_cluster_id(mock_cluster_id);
        req.mutable_cluster()->set_cluster_status(ClusterStatus::SUSPENDED);
        req.set_op(AlterClusterRequest::SET_CLUSTER_STATUS);
        AlterClusterResponse res;
        meta_service->alter_cluster(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // set UPDATE_CLUSTER_MYSQL_USER_NAME
    {
        brpc::Controller cntl;
        AlterClusterRequest req;
        req.set_instance_id(mock_instance);
        req.mutable_cluster()->set_cluster_id(mock_cluster_id);
        req.mutable_cluster()->add_mysql_user_name("test_user");
        req.set_op(AlterClusterRequest::UPDATE_CLUSTER_MYSQL_USER_NAME);
        AlterClusterResponse res;
        meta_service->alter_cluster(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }
}

TEST(MetaServiceTest, GetClusterTest) {
    auto meta_service = get_meta_service();

    // add cluster first
    InstanceKeyInfo key_info {mock_instance};
    std::string key;
    std::string val;
    instance_key(key_info, &key);

    InstanceInfoPB instance;
    instance.set_instance_id(mock_instance);
    ClusterPB c1;
    c1.set_cluster_name(mock_cluster_name);
    c1.set_cluster_id(mock_cluster_id);
    c1.add_mysql_user_name()->append("m1");
    instance.add_clusters()->CopyFrom(c1);
    val = instance.SerializeAsString();

    std::unique_ptr<Transaction> txn;
    std::string get_val;
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->put(key, val);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    // case: normal get
    {
        brpc::Controller cntl;
        GetClusterRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_cluster_id(mock_cluster_id);
        req.set_cluster_name("test_cluster");
        GetClusterResponse res;
        meta_service->get_cluster(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                  &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }
}

TEST(MetaServiceTest, BeginTxnTest) {
    auto meta_service = get_meta_service();
    int64_t db_id = 666;
    int64_t table_id = 123;
    const std::string& label = "test_label";
    int64_t timeout_ms = 60 * 1000;

    // test invalid argument
    {
        brpc::Controller cntl;
        BeginTxnRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        BeginTxnResponse res;
        meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    }

    {
        brpc::Controller cntl;
        BeginTxnRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");

        TxnInfoPB txn_info;
        txn_info.set_db_id(db_id);
        txn_info.add_table_ids(table_id);
        txn_info.set_timeout_ms(timeout_ms);
        req.mutable_txn_info()->CopyFrom(txn_info);
        BeginTxnResponse res;
        meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    }

    {
        brpc::Controller cntl;
        BeginTxnRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");

        TxnInfoPB txn_info;
        txn_info.set_db_id(db_id);
        txn_info.set_label(label);
        txn_info.add_table_ids(table_id);
        txn_info.set_timeout_ms(timeout_ms);
        req.mutable_txn_info()->CopyFrom(txn_info);
        BeginTxnResponse res;
        meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // case: label already used
    {
        brpc::Controller cntl;
        BeginTxnRequest req;
        auto label_already_in_use = "test_label_already_in_use";

        req.set_cloud_unique_id("test_cloud_unique_id");
        TxnInfoPB txn_info;
        txn_info.set_db_id(888);
        txn_info.set_label(label_already_in_use);
        txn_info.add_table_ids(456);
        txn_info.set_timeout_ms(36000);
        req.mutable_txn_info()->CopyFrom(txn_info);
        BeginTxnResponse res;
        meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

        meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::TXN_LABEL_ALREADY_USED);
        auto found = res.status().msg().find(fmt::format(
                "Label [{}] has already been used, relate to txn", label_already_in_use));
        ASSERT_NE(found, std::string::npos);
    }

    // case: dup begin txn request
    {
        brpc::Controller cntl;
        BeginTxnRequest req;

        req.set_cloud_unique_id("test_cloud_unique_id");
        TxnInfoPB txn_info;
        txn_info.set_db_id(999);
        txn_info.set_label("test_label_dup_request");
        txn_info.add_table_ids(789);
        UniqueIdPB unique_id_pb;
        unique_id_pb.set_hi(100);
        unique_id_pb.set_lo(10);
        txn_info.mutable_request_id()->CopyFrom(unique_id_pb);
        txn_info.set_timeout_ms(36000);
        req.mutable_txn_info()->CopyFrom(txn_info);
        BeginTxnResponse res;
        meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

        meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::TXN_DUPLICATED_REQ);
    }

    {
        // ===========================================================================
        // threads concurrent execution with sequence in begin_txn with same label:
        //
        //      thread1              thread2
        //         |                    |
        //         |                commit_txn1
        //         |                    |
        //         |                    |
        //         |                    |
        //       commit_txn2            |
        //         |                    |
        //         v                    v
        //

        std::mutex go_mutex;
        std::condition_variable go_cv;
        bool go = false;
        auto sp = SyncPoint::get_instance();
        DORIS_CLOUD_DEFER {
            SyncPoint::get_instance()->clear_all_call_backs();
        };

        std::atomic<int32_t> count_txn1 = {0};
        std::atomic<int32_t> count_txn2 = {0};
        std::atomic<int32_t> count_txn3 = {0};

        int64_t db_id = 1928354123;
        int64_t table_id = 12131231231;
        std::string test_label = "test_race_with_same_label";

        std::atomic<int32_t> success_txn = {0};

        sp->set_call_back("begin_txn:before:commit_txn:1", [&](auto&& args) {
            const auto& label = *try_any_cast<std::string*>(args[0]);
            std::unique_lock<std::mutex> _lock(go_mutex);
            count_txn1++;
            LOG(INFO) << "count_txn1:" << count_txn1 << " label=" << label;
            if (count_txn1 == 1) {
                {
                    LOG(INFO) << "count_txn1:" << count_txn1 << " label=" << label << " go=" << go;
                    go_cv.wait(_lock);
                }
            }

            if (count_txn1 == 2) {
                {
                    LOG(INFO) << "count_txn1:" << count_txn1 << " label=" << label << " go=" << go;
                    go_cv.notify_all();
                }
            }
        });

        sp->set_call_back("begin_txn:after:commit_txn:1", [&](auto&& args) {
            const auto& label = *try_any_cast<std::string*>(args[0]);
            std::unique_lock<std::mutex> _lock(go_mutex);
            count_txn2++;
            LOG(INFO) << "count_txn2:" << count_txn2 << " label=" << label;
            if (count_txn2 == 1) {
                {
                    LOG(INFO) << "count_txn2:" << count_txn2 << " label=" << label << " go=" << go;
                    go_cv.wait(_lock);
                }
            }

            if (count_txn2 == 2) {
                {
                    LOG(INFO) << "count_txn2:" << count_txn2 << " label=" << label << " go=" << go;
                    go_cv.notify_all();
                }
            }
        });

        sp->set_call_back("begin_txn:after:commit_txn:2", [&](auto&& args) {
            int64_t txn_id = *try_any_cast<int64_t*>(args[0]);
            count_txn3++;
            LOG(INFO) << "count_txn3:" << count_txn3 << " txn_id=" << txn_id;
        });

        sp->enable_processing();

        std::thread thread1([&] {
            {
                std::unique_lock<std::mutex> _lock(go_mutex);
                go_cv.wait(_lock, [&] { return go; });
            }
            brpc::Controller cntl;
            BeginTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            TxnInfoPB txn_info;
            txn_info.set_db_id(db_id);
            txn_info.set_label(test_label);
            txn_info.add_table_ids(table_id);
            UniqueIdPB unique_id_pb;
            unique_id_pb.set_hi(1001);
            unique_id_pb.set_lo(11);
            txn_info.mutable_request_id()->CopyFrom(unique_id_pb);
            txn_info.set_timeout_ms(36000);
            req.mutable_txn_info()->CopyFrom(txn_info);
            BeginTxnResponse res;
            meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            if (res.status().code() == MetaServiceCode::OK) {
                success_txn++;
            } else {
                ASSERT_EQ(res.status().code(), MetaServiceCode::KV_TXN_CONFLICT);
            }
        });

        std::thread thread2([&] {
            {
                std::unique_lock<std::mutex> _lock(go_mutex);
                go_cv.wait(_lock, [&] { return go; });
            }
            brpc::Controller cntl;
            BeginTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            TxnInfoPB txn_info;
            txn_info.set_db_id(db_id);
            txn_info.set_label(test_label);
            txn_info.add_table_ids(table_id);
            UniqueIdPB unique_id_pb;
            unique_id_pb.set_hi(100);
            unique_id_pb.set_lo(10);
            txn_info.mutable_request_id()->CopyFrom(unique_id_pb);
            txn_info.set_timeout_ms(36000);
            req.mutable_txn_info()->CopyFrom(txn_info);
            BeginTxnResponse res;
            meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            if (res.status().code() == MetaServiceCode::OK) {
                success_txn++;
            } else {
                ASSERT_EQ(res.status().code(), MetaServiceCode::KV_TXN_CONFLICT);
            }
        });

        std::unique_lock<std::mutex> go_lock(go_mutex);
        go = true;
        go_lock.unlock();
        go_cv.notify_all();

        thread1.join();
        thread2.join();
        sp->clear_all_call_backs();
        sp->clear_trace();
        sp->disable_processing();
        ASSERT_EQ(success_txn.load(), 1);
    }
    {
        // ===========================================================================
        // threads concurrent execution with sequence in begin_txn with different label:
        //
        //      thread1              thread2
        //         |                    |
        //         |                commit_txn1
        //         |                    |
        //         |                    |
        //         |                    |
        //       commit_txn2            |
        //         |                    |
        //         v                    v

        std::mutex go_mutex;
        std::condition_variable go_cv;
        bool go = false;
        auto sp = SyncPoint::get_instance();
        DORIS_CLOUD_DEFER {
            SyncPoint::get_instance()->clear_all_call_backs();
        };

        std::atomic<int32_t> count_txn1 = {0};
        std::atomic<int32_t> count_txn2 = {0};
        std::mutex flow_mutex_1;
        std::condition_variable flow_cv_1;

        int64_t db_id = 19541231112;
        int64_t table_id = 312312321211;
        std::string test_label1 = "test_race_with_diff_label1";
        std::string test_label2 = "test_race_with_diff_label2";

        std::atomic<int32_t> success_txn = {0};

        sp->set_call_back("begin_txn:before:commit_txn:1", [&](auto&& args) {
            std::string label = *try_any_cast<std::string*>(args[0]);
            if (count_txn1.load() == 1) {
                std::unique_lock<std::mutex> flow_lock_1(flow_mutex_1);
                flow_cv_1.wait(flow_lock_1);
            }
            count_txn1++;
            LOG(INFO) << "count_txn1:" << count_txn1 << " label=" << label;
        });

        sp->set_call_back("begin_txn:after:commit_txn:2", [&](auto&& args) {
            int64_t txn_id = *try_any_cast<int64_t*>(args[0]);
            while (count_txn2.load() == 0 && count_txn1.load() == 1) {
                sleep(1);
                flow_cv_1.notify_all();
            }
            count_txn2++;
            LOG(INFO) << "count_txn2:" << count_txn2 << " txn_id=" << txn_id;
        });
        sp->enable_processing();

        std::thread thread1([&] {
            {
                std::unique_lock<std::mutex> _lock(go_mutex);
                go_cv.wait(_lock, [&] { return go; });
            }
            brpc::Controller cntl;
            BeginTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            TxnInfoPB txn_info;
            txn_info.set_db_id(db_id);
            txn_info.set_label(test_label1);
            txn_info.add_table_ids(table_id);
            UniqueIdPB unique_id_pb;
            unique_id_pb.set_hi(1001);
            unique_id_pb.set_lo(11);
            txn_info.mutable_request_id()->CopyFrom(unique_id_pb);
            txn_info.set_timeout_ms(36000);
            req.mutable_txn_info()->CopyFrom(txn_info);
            BeginTxnResponse res;
            meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            if (res.status().code() == MetaServiceCode::OK) {
                success_txn++;
            } else {
                ASSERT_EQ(res.status().code(), MetaServiceCode::TXN_LABEL_ALREADY_USED);
            }
        });

        std::thread thread2([&] {
            {
                std::unique_lock<std::mutex> _lock(go_mutex);
                go_cv.wait(_lock, [&] { return go; });
            }
            brpc::Controller cntl;
            BeginTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            TxnInfoPB txn_info;
            txn_info.set_db_id(db_id);
            txn_info.set_label(test_label2);
            txn_info.add_table_ids(table_id);
            txn_info.set_timeout_ms(36000);
            UniqueIdPB unique_id_pb;
            unique_id_pb.set_hi(100);
            unique_id_pb.set_lo(10);
            txn_info.mutable_request_id()->CopyFrom(unique_id_pb);
            req.mutable_txn_info()->CopyFrom(txn_info);
            BeginTxnResponse res;
            meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            if (res.status().code() == MetaServiceCode::OK) {
                success_txn++;
            } else {
                ASSERT_EQ(res.status().code(), MetaServiceCode::TXN_LABEL_ALREADY_USED);
            }
        });

        std::unique_lock<std::mutex> go_lock(go_mutex);
        go = true;
        go_lock.unlock();
        go_cv.notify_all();

        thread1.join();
        thread2.join();
        sp->clear_all_call_backs();
        sp->clear_trace();
        sp->disable_processing();
        ASSERT_EQ(success_txn.load(), 2);
    }
    {
        // test reuse label
        // 1. beigin_txn
        // 2. abort_txn
        // 3. begin_txn again can successfully

        std::string cloud_unique_id = "test_cloud_unique_id";
        int64_t db_id = 124343989;
        int64_t table_id = 1231311;
        int64_t txn_id = -1;
        std::string label = "test_reuse_label";
        {
            brpc::Controller cntl;
            BeginTxnRequest req;
            req.set_cloud_unique_id(cloud_unique_id);
            TxnInfoPB txn_info;
            txn_info.set_db_id(db_id);
            txn_info.set_label(label);
            txn_info.add_table_ids(table_id);
            txn_info.set_timeout_ms(36000);
            UniqueIdPB unique_id_pb;
            unique_id_pb.set_hi(100);
            unique_id_pb.set_lo(10);
            txn_info.mutable_request_id()->CopyFrom(unique_id_pb);
            req.mutable_txn_info()->CopyFrom(txn_info);
            BeginTxnResponse res;
            meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            txn_id = res.txn_id();
        }
        // abort txn
        {
            brpc::Controller cntl;
            AbortTxnRequest req;
            req.set_cloud_unique_id(cloud_unique_id);
            ASSERT_GT(txn_id, 0);
            req.set_txn_id(txn_id);
            req.set_reason("test");
            AbortTxnResponse res;
            meta_service->abort_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            ASSERT_EQ(res.txn_info().status(), TxnStatusPB::TXN_STATUS_ABORTED);
        }
        {
            brpc::Controller cntl;
            BeginTxnRequest req;
            req.set_cloud_unique_id(cloud_unique_id);
            TxnInfoPB txn_info;
            txn_info.set_db_id(db_id);
            txn_info.set_label(label);
            txn_info.add_table_ids(table_id);
            UniqueIdPB unique_id_pb;
            unique_id_pb.set_hi(100);
            unique_id_pb.set_lo(10);
            txn_info.mutable_request_id()->CopyFrom(unique_id_pb);
            txn_info.set_timeout_ms(36000);
            req.mutable_txn_info()->CopyFrom(txn_info);
            BeginTxnResponse res;
            meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            ASSERT_GT(res.txn_id(), txn_id);
        }
    }

    {
        // test reuse label exceed max_num_aborted_txn

        std::string cloud_unique_id = "test_cloud_unique_id";
        int64_t db_id = 124343989;
        int64_t table_id = 12897811;
        int64_t txn_id = -1;
        std::string label = "test_max_num_aborted_txn_label";
        for (int i = 0; i < config::max_num_aborted_txn; i++) {
            {
                brpc::Controller cntl;
                BeginTxnRequest req;
                req.set_cloud_unique_id(cloud_unique_id);
                TxnInfoPB txn_info;
                txn_info.set_db_id(db_id);
                txn_info.set_label(label);
                txn_info.add_table_ids(table_id);
                txn_info.set_timeout_ms(36000);
                UniqueIdPB unique_id_pb;
                unique_id_pb.set_hi(100);
                unique_id_pb.set_lo(10);
                txn_info.mutable_request_id()->CopyFrom(unique_id_pb);
                req.mutable_txn_info()->CopyFrom(txn_info);
                BeginTxnResponse res;
                meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);
                ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
                txn_id = res.txn_id();
            }
            // abort txn
            {
                brpc::Controller cntl;
                AbortTxnRequest req;
                req.set_cloud_unique_id(cloud_unique_id);
                ASSERT_GT(txn_id, 0);
                req.set_txn_id(txn_id);
                req.set_reason("test");
                AbortTxnResponse res;
                meta_service->abort_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);
                ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
                ASSERT_EQ(res.txn_info().status(), TxnStatusPB::TXN_STATUS_ABORTED);
            }
        }
        {
            brpc::Controller cntl;
            BeginTxnRequest req;
            req.set_cloud_unique_id(cloud_unique_id);
            TxnInfoPB txn_info;
            txn_info.set_db_id(db_id);
            txn_info.set_label(label);
            txn_info.add_table_ids(table_id);
            UniqueIdPB unique_id_pb;
            unique_id_pb.set_hi(100);
            unique_id_pb.set_lo(10);
            txn_info.mutable_request_id()->CopyFrom(unique_id_pb);
            txn_info.set_timeout_ms(36000);
            req.mutable_txn_info()->CopyFrom(txn_info);
            BeginTxnResponse res;
            meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
            ASSERT_TRUE(res.status().msg().find("too many aborted txn for label") !=
                        std::string::npos);
        }
    }
}

TEST(MetaServiceTest, PrecommitTest1) {
    // PrecommitTestCase1: only use db_id for precommit_txn
    auto meta_service = get_meta_service();
    const int64_t db_id = 563413;
    const int64_t table_id = 417417878;
    const std::string& label = "label_123dae121das";
    int64_t txn_id = -1;
    {
        brpc::Controller cntl;
        BeginTxnRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        TxnInfoPB txn_info;
        txn_info.set_db_id(db_id);
        txn_info.set_label(label);
        txn_info.add_table_ids(table_id);
        txn_info.set_timeout_ms(36000);
        req.mutable_txn_info()->CopyFrom(txn_info);
        BeginTxnResponse res;
        meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        txn_id = res.txn_id();
        ASSERT_GT(txn_id, -1);
    }

    {
        brpc::Controller cntl;
        PrecommitTxnRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_precommit_timeout_ms(36000);
        PrecommitTxnResponse res;
        meta_service->precommit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    }

    {
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = meta_service->txn_kv()->create_txn(&txn);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);

        const std::string info_key = txn_info_key({mock_instance, db_id, txn_id});
        std::string info_val;
        ASSERT_EQ(txn->get(info_key, &info_val), TxnErrorCode::TXN_OK);
        TxnInfoPB txn_info;
        txn_info.ParseFromString(info_val);
        ASSERT_EQ(txn_info.status(), TxnStatusPB::TXN_STATUS_PREPARED);

        brpc::Controller cntl;
        PrecommitTxnRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_txn_id(txn_id);
        req.set_precommit_timeout_ms(36000);
        PrecommitTxnResponse res;
        meta_service->precommit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

        err = meta_service->txn_kv()->create_txn(&txn);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        ASSERT_EQ(txn->get(info_key, &info_val), TxnErrorCode::TXN_OK);
        txn_info.ParseFromString(info_val);
        ASSERT_EQ(txn_info.status(), TxnStatusPB::TXN_STATUS_PRECOMMITTED);
    }
}

TEST(MetaServiceTest, PrecommitTxnTest2) {
    auto meta_service = get_meta_service();
    const int64_t db_id = 563413;
    const int64_t table_id = 417417878;
    const std::string& label = "label_123dae121das";
    int64_t txn_id = -1;
    // begin txn first
    {
        brpc::Controller cntl;
        BeginTxnRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        TxnInfoPB txn_info;
        txn_info.set_db_id(db_id);
        txn_info.set_label(label);
        txn_info.add_table_ids(table_id);
        txn_info.set_timeout_ms(36000);
        req.mutable_txn_info()->CopyFrom(txn_info);
        BeginTxnResponse res;
        meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        txn_id = res.txn_id();
        ASSERT_GT(txn_id, -1);
    }

    // case: txn's status should be TXN_STATUS_PRECOMMITTED
    {
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = meta_service->txn_kv()->create_txn(&txn);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);

        const std::string info_key = txn_info_key({mock_instance, db_id, txn_id});
        std::string info_val;
        ASSERT_EQ(txn->get(info_key, &info_val), TxnErrorCode::TXN_OK);
        TxnInfoPB txn_info;
        txn_info.ParseFromString(info_val);
        // before call precommit_txn, txn's status is TXN_STATUS_PREPARED
        ASSERT_EQ(txn_info.status(), TxnStatusPB::TXN_STATUS_PREPARED);

        brpc::Controller cntl;
        PrecommitTxnRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_db_id(db_id);
        req.set_txn_id(txn_id);
        req.set_precommit_timeout_ms(36000);
        PrecommitTxnResponse res;
        meta_service->precommit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

        err = meta_service->txn_kv()->create_txn(&txn);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        ASSERT_EQ(txn->get(info_key, &info_val), TxnErrorCode::TXN_OK);
        txn_info.ParseFromString(info_val);
        // after call precommit_txn, txn's status is TXN_STATUS_PRECOMMITTED
        ASSERT_EQ(txn_info.status(), TxnStatusPB::TXN_STATUS_PRECOMMITTED);
    }

    // case: when txn's status is TXN_STATUS_ABORTED/TXN_STATUS_VISIBLE/TXN_STATUS_PRECOMMITTED
    {
        // TXN_STATUS_ABORTED
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = meta_service->txn_kv()->create_txn(&txn);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);

        const std::string info_key = txn_info_key({mock_instance, db_id, txn_id});
        std::string info_val;
        ASSERT_EQ(txn->get(info_key, &info_val), TxnErrorCode::TXN_OK);
        TxnInfoPB txn_info;
        txn_info.ParseFromString(info_val);
        txn_info.set_status(TxnStatusPB::TXN_STATUS_ABORTED);
        info_val.clear();
        txn_info.SerializeToString(&info_val);
        txn->put(info_key, info_val);
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        brpc::Controller cntl;
        PrecommitTxnRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_db_id(db_id);
        req.set_txn_id(txn_id);
        req.set_precommit_timeout_ms(36000);
        PrecommitTxnResponse res;
        meta_service->precommit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::TXN_ALREADY_ABORTED);

        // TXN_STATUS_VISIBLE
        txn_info.set_status(TxnStatusPB::TXN_STATUS_VISIBLE);
        info_val.clear();
        txn_info.SerializeToString(&info_val);
        txn->put(info_key, info_val);
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
        meta_service->precommit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::TXN_ALREADY_VISIBLE);

        // TXN_STATUS_PRECOMMITTED
        txn_info.set_status(TxnStatusPB::TXN_STATUS_PRECOMMITTED);
        info_val.clear();
        txn_info.SerializeToString(&info_val);
        txn->put(info_key, info_val);
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
        meta_service->precommit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::TXN_ALREADY_PRECOMMITED);
    }
}

TEST(MetaServiceTest, CommitTxnTest) {
    auto meta_service = get_meta_service();
    int64_t table_id = 1234;
    int64_t index_id = 1235;
    int64_t partition_id = 1236;

    // case: first version of rowset
    {
        int64_t txn_id = -1;
        // begin txn
        {
            brpc::Controller cntl;
            BeginTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            TxnInfoPB txn_info_pb;
            txn_info_pb.set_db_id(666);
            txn_info_pb.set_label("test_label");
            txn_info_pb.add_table_ids(1234);
            txn_info_pb.set_timeout_ms(36000);
            req.mutable_txn_info()->CopyFrom(txn_info_pb);
            BeginTxnResponse res;
            meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            txn_id = res.txn_id();
        }

        // mock rowset and tablet
        int64_t tablet_id_base = 1103;
        for (int i = 0; i < 5; ++i) {
            create_tablet(meta_service.get(), table_id, index_id, partition_id, tablet_id_base + i);
            auto tmp_rowset = create_rowset(txn_id, tablet_id_base + i, partition_id);
            CreateRowsetResponse res;
            commit_rowset(meta_service.get(), tmp_rowset, res);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        }

        // precommit txn
        {
            brpc::Controller cntl;
            PrecommitTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            req.set_db_id(666);
            req.set_txn_id(txn_id);
            req.set_precommit_timeout_ms(36000);
            PrecommitTxnResponse res;
            meta_service->precommit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        }

        // commit txn
        {
            brpc::Controller cntl;
            CommitTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            req.set_db_id(666);
            req.set_txn_id(txn_id);
            CommitTxnResponse res;
            meta_service->commit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        }

        // doubly commit txn
        {
            brpc::Controller cntl;
            CommitTxnRequest req;
            auto db_id = 666;
            req.set_cloud_unique_id("test_cloud_unique_id");
            req.set_db_id(db_id);
            req.set_txn_id(txn_id);
            CommitTxnResponse res;
            meta_service->commit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            auto found = res.status().msg().find(fmt::format(
                    "transaction is already visible: db_id={} txn_id={}", db_id, txn_id));
            ASSERT_TRUE(found != std::string::npos);
        }

        // doubly commit txn(2pc)
        {
            brpc::Controller cntl;
            CommitTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            req.set_db_id(666);
            req.set_txn_id(txn_id);
            req.set_is_2pc(true);
            CommitTxnResponse res;
            meta_service->commit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::TXN_ALREADY_VISIBLE);
            auto found = res.status().msg().find(
                    fmt::format("transaction [{}] is already visible, not pre-committed.", txn_id));
            ASSERT_TRUE(found != std::string::npos);
        }
    }
}

TEST(MetaServiceTest, CommitTxnExpiredTest) {
    auto meta_service = get_meta_service();

    int64_t table_id = 1234789234;
    int64_t index_id = 1235;
    int64_t partition_id = 1236;

    // case: first version of rowset
    {
        int64_t txn_id = -1;
        int64_t db_id = 713232132;
        // begin txn
        {
            brpc::Controller cntl;
            BeginTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            TxnInfoPB txn_info_pb;
            txn_info_pb.set_db_id(db_id);
            txn_info_pb.set_label("test_commit_txn_expired");
            txn_info_pb.add_table_ids(1234789234);
            txn_info_pb.set_timeout_ms(1);
            req.mutable_txn_info()->CopyFrom(txn_info_pb);
            BeginTxnResponse res;
            meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            txn_id = res.txn_id();
        }

        // mock rowset and tablet
        int64_t tablet_id_base = 1103;
        for (int i = 0; i < 5; ++i) {
            create_tablet(meta_service.get(), table_id, index_id, partition_id, tablet_id_base + i);
            auto tmp_rowset = create_rowset(txn_id, tablet_id_base + i, partition_id);
            CreateRowsetResponse res;
            commit_rowset(meta_service.get(), tmp_rowset, res);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        }
        // sleep 1 second for txn timeout
        sleep(1);
        // commit txn
        {
            brpc::Controller cntl;
            CommitTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            req.set_db_id(db_id);
            req.set_txn_id(txn_id);
            CommitTxnResponse res;
            meta_service->commit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::UNDEFINED_ERR);
            ASSERT_TRUE(res.status().msg().find("txn is expired, not allow to commit txn_id=") !=
                        std::string::npos);
        }
    }
}

void create_and_commit_rowset(MetaServiceProxy* meta_service, int64_t table_id, int64_t index_id,
                              int64_t partition_id, int64_t tablet_id, int64_t txn_id) {
    create_tablet(meta_service, table_id, index_id, partition_id, tablet_id);
    auto tmp_rowset = create_rowset(txn_id, tablet_id, partition_id);
    CreateRowsetResponse res;
    commit_rowset(meta_service, tmp_rowset, res);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
}

TEST(MetaServiceTest, CommitTxnWithSubTxnTest) {
    auto meta_service = get_meta_service();
    int64_t db_id = 98131;
    int64_t txn_id = -1;
    int64_t t1 = 10;
    int64_t t1_index = 100;
    int64_t t1_p1 = 11;
    int64_t t1_p1_t1 = 12;
    int64_t t1_p1_t2 = 13;
    int64_t t1_p2 = 14;
    int64_t t1_p2_t1 = 15;
    int64_t t2 = 16;
    int64_t t2_index = 101;
    int64_t t2_p3 = 17;
    int64_t t2_p3_t1 = 18;
    [[maybe_unused]] int64_t t2_p4 = 19;
    [[maybe_unused]] int64_t t2_p4_t1 = 20;
    std::string label = "test_label";
    std::string label2 = "test_label_0";
    // begin txn
    {
        brpc::Controller cntl;
        BeginTxnRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        TxnInfoPB txn_info_pb;
        txn_info_pb.set_db_id(db_id);
        txn_info_pb.set_label(label);
        txn_info_pb.add_table_ids(t1);
        txn_info_pb.set_timeout_ms(36000);
        req.mutable_txn_info()->CopyFrom(txn_info_pb);
        BeginTxnResponse res;
        meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        txn_id = res.txn_id();
    }

    // mock rowset and tablet: for sub_txn1
    int64_t sub_txn_id1 = txn_id;
    create_and_commit_rowset(meta_service.get(), t1, t1_index, t1_p1, t1_p1_t1, sub_txn_id1);
    create_and_commit_rowset(meta_service.get(), t1, t1_index, t1_p1, t1_p1_t2, sub_txn_id1);
    create_and_commit_rowset(meta_service.get(), t1, t1_index, t1_p2, t1_p2_t1, sub_txn_id1);

    // begin_sub_txn2
    int64_t sub_txn_id2 = -1;
    {
        brpc::Controller cntl;
        BeginSubTxnRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_txn_id(txn_id);
        req.set_sub_txn_num(0);
        req.set_db_id(db_id);
        req.set_label(label2);
        req.mutable_table_ids()->Add(t1);
        req.mutable_table_ids()->Add(t2);
        BeginSubTxnResponse res;
        meta_service->begin_sub_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(res.txn_info().table_ids().size(), 2);
        ASSERT_EQ(res.txn_info().sub_txn_ids().size(), 1);
        ASSERT_TRUE(res.has_sub_txn_id());
        sub_txn_id2 = res.sub_txn_id();
        ASSERT_EQ(sub_txn_id2, res.txn_info().sub_txn_ids()[0]);
    }
    // mock rowset and tablet: for sub_txn3
    create_and_commit_rowset(meta_service.get(), t2, t2_index, t2_p3, t2_p3_t1, sub_txn_id2);

    // begin_sub_txn3
    int64_t sub_txn_id3 = -1;
    {
        brpc::Controller cntl;
        BeginSubTxnRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_txn_id(txn_id);
        req.set_sub_txn_num(1);
        req.set_db_id(db_id);
        req.set_label("test_label_1");
        req.mutable_table_ids()->Add(t1);
        req.mutable_table_ids()->Add(t2);
        req.mutable_table_ids()->Add(t1);
        BeginSubTxnResponse res;
        meta_service->begin_sub_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(res.txn_info().table_ids().size(), 3);
        ASSERT_EQ(res.txn_info().sub_txn_ids().size(), 2);
        ASSERT_TRUE(res.has_sub_txn_id());
        sub_txn_id3 = res.sub_txn_id();
        ASSERT_EQ(sub_txn_id3, res.txn_info().sub_txn_ids()[1]);
    }
    // mock rowset and tablet: for sub_txn3
    create_and_commit_rowset(meta_service.get(), t1, t1_index, t1_p1, t1_p1_t1, sub_txn_id3);
    create_and_commit_rowset(meta_service.get(), t1, t1_index, t1_p1, t1_p1_t2, sub_txn_id3);

    // commit txn
    CommitTxnRequest req;
    {
        brpc::Controller cntl;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_db_id(666);
        req.set_txn_id(txn_id);
        req.set_is_txn_load(true);

        SubTxnInfo sub_txn_info1;
        sub_txn_info1.set_sub_txn_id(sub_txn_id1);
        sub_txn_info1.set_table_id(t1);
        sub_txn_info1.mutable_base_tablet_ids()->Add(t1_p1_t1);
        sub_txn_info1.mutable_base_tablet_ids()->Add(t1_p1_t2);
        sub_txn_info1.mutable_base_tablet_ids()->Add(t1_p2_t1);

        SubTxnInfo sub_txn_info2;
        sub_txn_info2.set_sub_txn_id(sub_txn_id2);
        sub_txn_info2.set_table_id(t2);
        sub_txn_info2.mutable_base_tablet_ids()->Add(t2_p3_t1);

        SubTxnInfo sub_txn_info3;
        sub_txn_info3.set_sub_txn_id(sub_txn_id3);
        sub_txn_info3.set_table_id(t1);
        sub_txn_info3.mutable_base_tablet_ids()->Add(t1_p1_t1);
        sub_txn_info3.mutable_base_tablet_ids()->Add(t1_p1_t2);

        req.mutable_sub_txn_infos()->Add(std::move(sub_txn_info1));
        req.mutable_sub_txn_infos()->Add(std::move(sub_txn_info2));
        req.mutable_sub_txn_infos()->Add(std::move(sub_txn_info3));
        CommitTxnResponse res;
        meta_service->commit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                 &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        // std::cout << res.DebugString() << std::endl;
        ASSERT_EQ(res.table_ids().size(), 3);

        ASSERT_EQ(res.table_ids()[0], t2);
        ASSERT_EQ(res.partition_ids()[0], t2_p3);
        ASSERT_EQ(res.versions()[0], 2);

        ASSERT_EQ(res.table_ids()[1], t1);
        ASSERT_EQ(res.partition_ids()[1], t1_p2);
        ASSERT_EQ(res.versions()[1], 2);

        ASSERT_EQ(res.table_ids()[2], t1);
        ASSERT_EQ(res.partition_ids()[2], t1_p1) << res.ShortDebugString();
        ASSERT_EQ(res.versions()[2], 3) << res.ShortDebugString();
    }

    // doubly commit txn
    {
        brpc::Controller cntl;
        CommitTxnResponse res;
        meta_service->commit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                 &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        auto found = res.status().msg().find(
                fmt::format("transaction is already visible: db_id={} txn_id={}", db_id, txn_id));
        ASSERT_TRUE(found != std::string::npos);
    }

    // check kv
    {
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = meta_service->txn_kv()->create_txn(&txn);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);

        // txn_info
        std::string info_key = txn_info_key({mock_instance, db_id, txn_id});
        std::string info_val;
        ASSERT_EQ(txn->get(info_key, &info_val), TxnErrorCode::TXN_OK);
        TxnInfoPB txn_info;
        txn_info.ParseFromString(info_val);
        ASSERT_EQ(txn_info.status(), TxnStatusPB::TXN_STATUS_VISIBLE);

        info_key = txn_info_key({mock_instance, db_id, sub_txn_id2});
        ASSERT_EQ(txn->get(info_key, &info_val), TxnErrorCode::TXN_KEY_NOT_FOUND);

        // txn_index
        std::string index_key = txn_index_key({mock_instance, txn_id});
        std::string index_val;
        ASSERT_EQ(txn->get(index_key, &index_val), TxnErrorCode::TXN_OK);
        TxnIndexPB txn_index;
        txn_index.ParseFromString(index_val);
        ASSERT_TRUE(txn_index.has_tablet_index());

        index_key = txn_index_key({mock_instance, sub_txn_id3});
        ASSERT_EQ(txn->get(index_key, &index_val), TxnErrorCode::TXN_OK);
        txn_index.ParseFromString(index_val);
        ASSERT_TRUE(txn_index.has_tablet_index());

        // txn_label
        std::string label_key = txn_label_key({mock_instance, db_id, label});
        std::string label_val;
        ASSERT_EQ(txn->get(label_key, &label_val), TxnErrorCode::TXN_OK);

        label_key = txn_label_key({mock_instance, db_id, label2});
        ASSERT_EQ(txn->get(label_key, &label_val), TxnErrorCode::TXN_KEY_NOT_FOUND);

        // txn_running
        std::string running_key = txn_running_key({mock_instance, db_id, txn_id});
        std::string running_val;
        ASSERT_EQ(txn->get(running_key, &running_val), TxnErrorCode::TXN_KEY_NOT_FOUND);

        running_key = txn_running_key({mock_instance, db_id, sub_txn_id3});
        ASSERT_EQ(txn->get(running_key, &running_val), TxnErrorCode::TXN_KEY_NOT_FOUND);

        // tmp rowset
        int64_t ids[] = {txn_id, sub_txn_id1, sub_txn_id2, sub_txn_id3};
        for (auto id : ids) {
            MetaRowsetTmpKeyInfo rs_tmp_key_info0 {mock_instance, id, 0};
            MetaRowsetTmpKeyInfo rs_tmp_key_info1 {mock_instance, id + 1, 0};
            std::string rs_tmp_key0;
            std::string rs_tmp_key1;
            meta_rowset_tmp_key(rs_tmp_key_info0, &rs_tmp_key0);
            meta_rowset_tmp_key(rs_tmp_key_info1, &rs_tmp_key1);
            std::unique_ptr<RangeGetIterator> it;
            ASSERT_EQ(txn->get(rs_tmp_key0, rs_tmp_key1, &it, true), TxnErrorCode::TXN_OK);
            ASSERT_FALSE(it->has_next());
        }

        // partition version
        std::string ver_key = partition_version_key({mock_instance, db_id, t2, t2_p3});
        std::string ver_val;
        ASSERT_EQ(txn->get(ver_key, &ver_val), TxnErrorCode::TXN_OK);
        VersionPB version;
        version.ParseFromString(ver_val);
        ASSERT_EQ(version.version(), 2);

        ver_key = partition_version_key({mock_instance, db_id, t1, t1_p2});
        ASSERT_EQ(txn->get(ver_key, &ver_val), TxnErrorCode::TXN_OK);
        version.ParseFromString(ver_val);
        ASSERT_EQ(version.version(), 2);

        ver_key = partition_version_key({mock_instance, db_id, t1, t1_p1});
        ASSERT_EQ(txn->get(ver_key, &ver_val), TxnErrorCode::TXN_OK);
        version.ParseFromString(ver_val);
        ASSERT_EQ(version.version(), 3);

        // table version
        std::string table_ver_key = table_version_key({mock_instance, db_id, t1});
        std::string table_ver_val;
        ASSERT_EQ(txn->get(table_ver_key, &table_ver_val), TxnErrorCode::TXN_OK);
        auto val_int = *reinterpret_cast<const int64_t*>(table_ver_val.data());
        ASSERT_EQ(val_int, 1);

        table_version_key({mock_instance, db_id, t2});
        ASSERT_EQ(txn->get(table_ver_key, &table_ver_val), TxnErrorCode::TXN_OK);
        val_int = *reinterpret_cast<const int64_t*>(table_ver_val.data());
        ASSERT_EQ(val_int, 1);
    }
}

TEST(MetaServiceTest, CommitTxnWithSubTxnTest2) {
    auto meta_service = get_meta_service();
    int64_t db_id = 99131;
    int64_t txn_id = -1;
    int64_t t1 = 20;
    int64_t t1_index = 200;
    int64_t t1_p1 = 21;
    int64_t t1_p1_t1 = 22;
    int64_t t1_p1_t2 = 23;
    int64_t t1_p2 = 24;
    int64_t t1_p2_t1 = 25;
    int64_t t2 = 26;
    int64_t t2_index = 201;
    int64_t t2_p3 = 27;
    int64_t t2_p3_t1 = 28;
    [[maybe_unused]] int64_t t2_p4 = 29;
    [[maybe_unused]] int64_t t2_p4_t1 = 30;
    std::string label = "test_label_10";
    std::string label2 = "test_label_11";
    // begin txn
    {
        brpc::Controller cntl;
        BeginTxnRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        TxnInfoPB txn_info_pb;
        txn_info_pb.set_db_id(db_id);
        txn_info_pb.set_label(label);
        txn_info_pb.add_table_ids(t1);
        txn_info_pb.set_timeout_ms(36000);
        req.mutable_txn_info()->CopyFrom(txn_info_pb);
        BeginTxnResponse res;
        meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        txn_id = res.txn_id();
    }

    std::vector<SubTxnInfo> sub_txn_infos;
    for (int i = 0; i < 500; i++) {
        int64_t sub_txn_id1 = -1;
        if (i == 0) {
            sub_txn_id1 = txn_id;
        } else {
            brpc::Controller cntl;
            BeginSubTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            req.set_txn_id(txn_id);
            req.set_sub_txn_num(sub_txn_infos.size() - 1);
            req.set_db_id(db_id);
            req.set_label(label2);
            req.mutable_table_ids()->Add(t1);
            if (i > 0) {
                req.mutable_table_ids()->Add(t2);
            }
            BeginSubTxnResponse res;
            meta_service->begin_sub_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            ASSERT_EQ(res.txn_info().table_ids().size(), i == 0 ? 1 : 2);
            ASSERT_EQ(res.txn_info().sub_txn_ids().size(), sub_txn_infos.size());
            ASSERT_TRUE(res.has_sub_txn_id());
            sub_txn_id1 = res.sub_txn_id();
            ASSERT_EQ(sub_txn_id1,
                      res.txn_info().sub_txn_ids()[res.txn_info().sub_txn_ids().size() - 1]);
        }
        // mock rowset and tablet: for
        create_and_commit_rowset(meta_service.get(), t1, t1_index, t1_p1, t1_p1_t1, sub_txn_id1);
        create_and_commit_rowset(meta_service.get(), t1, t1_index, t1_p1, t1_p1_t2, sub_txn_id1);
        create_and_commit_rowset(meta_service.get(), t1, t1_index, t1_p2, t1_p2_t1, sub_txn_id1);
        // generate sub_txn_info
        {
            SubTxnInfo sub_txn_info1;
            sub_txn_info1.set_sub_txn_id(sub_txn_id1);
            sub_txn_info1.set_table_id(t1);
            sub_txn_info1.mutable_base_tablet_ids()->Add(t1_p1_t1);
            sub_txn_info1.mutable_base_tablet_ids()->Add(t1_p1_t2);
            sub_txn_info1.mutable_base_tablet_ids()->Add(t1_p2_t1);
            sub_txn_infos.push_back(sub_txn_info1);
        }

        // begin_sub_txn2
        int64_t sub_txn_id2 = -1;
        {
            brpc::Controller cntl;
            BeginSubTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            req.set_txn_id(txn_id);
            req.set_sub_txn_num(sub_txn_infos.size() - 1);
            req.set_db_id(db_id);
            req.set_label(label2);
            req.mutable_table_ids()->Add(t1);
            req.mutable_table_ids()->Add(t2);
            BeginSubTxnResponse res;
            meta_service->begin_sub_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            ASSERT_EQ(res.txn_info().table_ids().size(), 2);
            ASSERT_EQ(res.txn_info().sub_txn_ids().size(), sub_txn_infos.size());
            ASSERT_TRUE(res.has_sub_txn_id());
            sub_txn_id2 = res.sub_txn_id();
        }
        // mock rowset and tablet: for sub_txn3
        create_and_commit_rowset(meta_service.get(), t2, t2_index, t2_p3, t2_p3_t1, sub_txn_id2);
        {
            SubTxnInfo sub_txn_info2;
            sub_txn_info2.set_sub_txn_id(sub_txn_id2);
            sub_txn_info2.set_table_id(t2);
            sub_txn_info2.mutable_base_tablet_ids()->Add(t2_p3_t1);
            sub_txn_infos.push_back(sub_txn_info2);
        }

        // begin_sub_txn3
        int64_t sub_txn_id3 = -1;
        {
            brpc::Controller cntl;
            BeginSubTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            req.set_txn_id(txn_id);
            req.set_sub_txn_num(sub_txn_infos.size() - 1);
            req.set_db_id(db_id);
            req.set_label(label2);
            req.mutable_table_ids()->Add(t1);
            req.mutable_table_ids()->Add(t2);
            BeginSubTxnResponse res;
            meta_service->begin_sub_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            ASSERT_EQ(res.txn_info().table_ids().size(), 2);
            ASSERT_EQ(res.txn_info().sub_txn_ids().size(), sub_txn_infos.size());
            ASSERT_TRUE(res.has_sub_txn_id());
            sub_txn_id3 = res.sub_txn_id();
        }
        // mock rowset and tablet: for sub_txn3
        create_and_commit_rowset(meta_service.get(), t1, t1_index, t1_p1, t1_p1_t1, sub_txn_id3);
        create_and_commit_rowset(meta_service.get(), t1, t1_index, t1_p1, t1_p1_t2, sub_txn_id3);
        {
            SubTxnInfo sub_txn_info3;
            sub_txn_info3.set_sub_txn_id(sub_txn_id3);
            sub_txn_info3.set_table_id(t1);
            sub_txn_info3.mutable_base_tablet_ids()->Add(t1_p1_t1);
            sub_txn_info3.mutable_base_tablet_ids()->Add(t1_p1_t2);
            sub_txn_infos.push_back(sub_txn_info3);
        }
    }

    // commit txn
    CommitTxnRequest req;
    {
        brpc::Controller cntl;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_db_id(666);
        req.set_txn_id(txn_id);
        req.set_is_txn_load(true);

        for (const auto& sub_txn_info : sub_txn_infos) {
            req.add_sub_txn_infos()->CopyFrom(sub_txn_info);
        }
        CommitTxnResponse res;
        meta_service->commit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                 &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        std::cout << res.DebugString() << std::endl;
        ASSERT_EQ(res.table_ids().size(), 3);

        ASSERT_EQ(res.table_ids()[0], t2);
        ASSERT_EQ(res.partition_ids()[0], t2_p3);
        ASSERT_EQ(res.versions()[0], 501);

        ASSERT_EQ(res.table_ids()[1], t1);
        ASSERT_EQ(res.partition_ids()[1], t1_p2);
        ASSERT_EQ(res.versions()[1], 501);

        ASSERT_EQ(res.table_ids()[2], t1);
        ASSERT_EQ(res.partition_ids()[2], t1_p1);
        ASSERT_EQ(res.versions()[2], 1001);
    }
}

TEST(MetaServiceTest, BeginAndAbortSubTxnTest) {
    auto meta_service = get_meta_service();
    long db_id = 98762;
    int64_t txn_id = -1;
    // begin txn
    {
        brpc::Controller cntl;
        BeginTxnRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        TxnInfoPB txn_info_pb;
        txn_info_pb.set_db_id(db_id);
        txn_info_pb.set_label("test_label");
        txn_info_pb.add_table_ids(1234);
        txn_info_pb.set_timeout_ms(36000);
        req.mutable_txn_info()->CopyFrom(txn_info_pb);
        BeginTxnResponse res;
        meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        txn_id = res.txn_id();
    }
    // case: begin 2 sub txn
    int64_t sub_txn_id1 = -1;
    int64_t sub_txn_id2 = -1;
    for (int i = 0; i < 2; i++) {
        {
            brpc::Controller cntl;
            BeginSubTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            req.set_txn_id(txn_id);
            req.set_sub_txn_num(i);
            req.set_db_id(db_id);
            req.set_label("test_label_" + std::to_string(i));
            req.mutable_table_ids()->Add(1234);
            req.mutable_table_ids()->Add(1235);
            if (i == 1) {
                req.mutable_table_ids()->Add(1235);
            }
            BeginSubTxnResponse res;
            meta_service->begin_sub_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            ASSERT_EQ(res.txn_info().table_ids().size(), i == 0 ? 2 : 3);
            ASSERT_EQ(res.txn_info().sub_txn_ids().size(), i == 0 ? 1 : 2);
            ASSERT_TRUE(res.has_sub_txn_id());
            if (i == 0) {
                sub_txn_id1 = res.sub_txn_id();
                ASSERT_EQ(sub_txn_id1, res.txn_info().sub_txn_ids()[0]);
            } else {
                sub_txn_id2 = res.sub_txn_id();
                ASSERT_EQ(sub_txn_id1, res.txn_info().sub_txn_ids()[0]);
                ASSERT_EQ(sub_txn_id2, res.txn_info().sub_txn_ids()[1]);
            }
        }
        // get txn state
        {
            brpc::Controller cntl;
            GetTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            req.set_txn_id(txn_id);
            GetTxnResponse res;
            meta_service->get_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                  &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            ASSERT_EQ(res.txn_info().table_ids().size(), i == 0 ? 2 : 3);
            ASSERT_EQ(res.txn_info().table_ids()[0], 1234);
            ASSERT_EQ(res.txn_info().table_ids()[1], 1235);
            if (i == 1) {
                ASSERT_EQ(res.txn_info().table_ids()[2], 1235);
            }
        }
    }
    // case: abort sub txn2 twice
    {
        for (int i = 0; i < 2; i++) {
            brpc::Controller cntl;
            AbortSubTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            req.set_txn_id(txn_id);
            req.set_sub_txn_id(sub_txn_id2);
            req.set_sub_txn_num(2);
            req.set_db_id(db_id);
            req.mutable_table_ids()->Add(1234);
            req.mutable_table_ids()->Add(1235);
            AbortSubTxnResponse res;
            meta_service->abort_sub_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            // check txn state
            ASSERT_EQ(res.txn_info().table_ids().size(), 2);
            ASSERT_EQ(res.txn_info().sub_txn_ids().size(), 2);
            ASSERT_EQ(sub_txn_id1, res.txn_info().sub_txn_ids()[0]);
            ASSERT_EQ(sub_txn_id2, res.txn_info().sub_txn_ids()[1]);
        }
        // get txn state
        {
            brpc::Controller cntl;
            GetTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            req.set_txn_id(txn_id);
            GetTxnResponse res;
            meta_service->get_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                  &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            ASSERT_EQ(res.txn_info().table_ids().size(), 2);
            ASSERT_EQ(res.txn_info().table_ids()[0], 1234);
            ASSERT_EQ(res.txn_info().table_ids()[1], 1235);
        }
    }
    // check label key does not exist
    for (int i = 0; i < 2; i++) {
        std::string key =
                txn_label_key({"test_instance", db_id, "test_label_" + std::to_string(i)});
        std::string val;
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        ASSERT_EQ(txn->get(key, &val), TxnErrorCode::TXN_KEY_NOT_FOUND);
    }
    // check txn index key exist
    for (auto i : {sub_txn_id1, sub_txn_id2}) {
        std::string key = txn_index_key({"test_instance", i});
        std::string val;
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        ASSERT_EQ(txn->get(key, &val), TxnErrorCode::TXN_OK);
    }
}

TEST(MetaServiceTest, AbortTxnTest) {
    auto meta_service = get_meta_service();

    // case: abort txn by txn_id
    {
        int64_t db_id = 666;
        int64_t table_id = 12345;
        std::string label = "abort_txn_by_txn_id";
        std::string cloud_unique_id = "test_cloud_unique_id";
        int64_t tablet_id_base = 1104;
        int64_t txn_id = -1;
        // begin txn
        {
            brpc::Controller cntl;
            BeginTxnRequest req;
            req.set_cloud_unique_id(cloud_unique_id);
            TxnInfoPB txn_info_pb;
            txn_info_pb.set_db_id(db_id);
            txn_info_pb.set_label(label);
            txn_info_pb.add_table_ids(table_id);
            txn_info_pb.set_timeout_ms(36000);
            req.mutable_txn_info()->CopyFrom(txn_info_pb);
            BeginTxnResponse res;
            meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            txn_id = res.txn_id();
        }

        // mock rowset and tablet
        for (int i = 0; i < 5; ++i) {
            create_tablet(meta_service.get(), 12345, 1235, 1236, tablet_id_base + i);
            auto tmp_rowset = create_rowset(txn_id, tablet_id_base + i);
            CreateRowsetResponse res;
            commit_rowset(meta_service.get(), tmp_rowset, res);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        }

        // abort txn by txn_id
        {
            brpc::Controller cntl;
            AbortTxnRequest req;
            req.set_cloud_unique_id(cloud_unique_id);
            req.set_txn_id(txn_id);
            req.set_reason("test");
            AbortTxnResponse res;
            meta_service->abort_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            ASSERT_EQ(res.txn_info().status(), TxnStatusPB::TXN_STATUS_ABORTED);
        }
    }

    // case: abort txn by db_id + label
    {
        int64_t db_id = 66631313131;
        int64_t table_id = 12345;
        std::string label = "abort_txn_by_db_id_and_label";
        std::string cloud_unique_id = "test_cloud_unique_id";
        int64_t tablet_id_base = 1104;
        int64_t txn_id = -1;
        // begin txn
        {
            brpc::Controller cntl;
            BeginTxnRequest req;
            req.set_cloud_unique_id(cloud_unique_id);
            TxnInfoPB txn_info_pb;
            txn_info_pb.set_db_id(db_id);
            txn_info_pb.set_label(label);
            txn_info_pb.add_table_ids(table_id);
            txn_info_pb.set_timeout_ms(36000);
            req.mutable_txn_info()->CopyFrom(txn_info_pb);
            BeginTxnResponse res;
            meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            txn_id = res.txn_id();
        }

        // mock rowset and tablet
        for (int i = 0; i < 5; ++i) {
            create_tablet(meta_service.get(), table_id, 1235, 1236, tablet_id_base + i);
            auto tmp_rowset = create_rowset(txn_id, tablet_id_base + i);
            CreateRowsetResponse res;
            commit_rowset(meta_service.get(), tmp_rowset, res);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        }

        // abort txn by db_id and label
        {
            brpc::Controller cntl;
            AbortTxnRequest req;
            req.set_cloud_unique_id(cloud_unique_id);
            req.set_db_id(db_id);
            req.set_label(label);
            req.set_reason("test");
            AbortTxnResponse res;
            meta_service->abort_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            ASSERT_EQ(res.txn_info().status(), TxnStatusPB::TXN_STATUS_ABORTED);

            std::string recycle_txn_key_;
            std::string recycle_txn_val;
            RecycleTxnKeyInfo recycle_txn_key_info {mock_instance, db_id, txn_id};
            recycle_txn_key(recycle_txn_key_info, &recycle_txn_key_);
            std::unique_ptr<Transaction> txn;
            ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
            ASSERT_EQ(txn->get(recycle_txn_key_, &recycle_txn_val), TxnErrorCode::TXN_OK);
            ASSERT_NE(txn_id, -1);
        }
    }
}

TEST(MetaServiceTest, GetCurrentMaxTxnIdTest) {
    auto meta_service = get_meta_service();

    const int64_t db_id = 123;
    const std::string label = "test_label123";
    const std::string cloud_unique_id = "test_cloud_unique_id";

    brpc::Controller begin_txn_cntl;
    BeginTxnRequest begin_txn_req;
    BeginTxnResponse begin_txn_res;
    TxnInfoPB txn_info_pb;

    begin_txn_req.set_cloud_unique_id(cloud_unique_id);
    txn_info_pb.set_db_id(db_id);
    txn_info_pb.set_label(label);
    txn_info_pb.add_table_ids(12345);
    txn_info_pb.set_timeout_ms(36000);
    begin_txn_req.mutable_txn_info()->CopyFrom(txn_info_pb);

    meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&begin_txn_cntl),
                            &begin_txn_req, &begin_txn_res, nullptr);
    ASSERT_EQ(begin_txn_res.status().code(), MetaServiceCode::OK);

    brpc::Controller max_txn_id_cntl;
    GetCurrentMaxTxnRequest max_txn_id_req;
    GetCurrentMaxTxnResponse max_txn_id_res;

    max_txn_id_req.set_cloud_unique_id(cloud_unique_id);

    meta_service->get_current_max_txn_id(
            reinterpret_cast<::google::protobuf::RpcController*>(&max_txn_id_cntl), &max_txn_id_req,
            &max_txn_id_res, nullptr);

    ASSERT_EQ(max_txn_id_res.status().code(), MetaServiceCode::OK);
    ASSERT_GE(max_txn_id_res.current_max_txn_id(), begin_txn_res.txn_id());
}

TEST(MetaServiceTest, AbortTxnWithCoordinatorTest) {
    auto meta_service = get_meta_service();

    const int64_t db_id = 666;
    const int64_t table_id = 777;
    const std::string label = "test_label";
    const std::string cloud_unique_id = "test_cloud_unique_id";
    const int64_t coordinator_id = 15623;
    int64_t cur_time = std::chrono::duration_cast<std::chrono::seconds>(
                               std::chrono::steady_clock::now().time_since_epoch())
                               .count();
    std::string host = "127.0.0.1:15586";
    int64_t txn_id = -1;

    brpc::Controller begin_txn_cntl;
    BeginTxnRequest begin_txn_req;
    BeginTxnResponse begin_txn_res;
    TxnInfoPB txn_info_pb;
    TxnCoordinatorPB coordinator;

    begin_txn_req.set_cloud_unique_id(cloud_unique_id);
    txn_info_pb.set_db_id(db_id);
    txn_info_pb.set_label(label);
    txn_info_pb.add_table_ids(table_id);
    txn_info_pb.set_timeout_ms(36000);
    coordinator.set_id(coordinator_id);
    coordinator.set_ip(host);
    coordinator.set_sourcetype(::doris::cloud::TxnSourceTypePB::TXN_SOURCE_TYPE_BE);
    coordinator.set_start_time(cur_time);
    txn_info_pb.mutable_coordinator()->CopyFrom(coordinator);
    begin_txn_req.mutable_txn_info()->CopyFrom(txn_info_pb);

    meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&begin_txn_cntl),
                            &begin_txn_req, &begin_txn_res, nullptr);
    ASSERT_EQ(begin_txn_res.status().code(), MetaServiceCode::OK);
    txn_id = begin_txn_res.txn_id();
    ASSERT_GT(txn_id, -1);

    brpc::Controller abort_txn_cntl;
    AbortTxnWithCoordinatorRequest abort_txn_req;
    AbortTxnWithCoordinatorResponse abort_txn_resp;

    abort_txn_req.set_id(coordinator_id);
    abort_txn_req.set_ip(host);
    abort_txn_req.set_start_time(cur_time + 3600);

    // first time to check txn conflict
    meta_service->abort_txn_with_coordinator(
            reinterpret_cast<::google::protobuf::RpcController*>(&begin_txn_cntl), &abort_txn_req,
            &abort_txn_resp, nullptr);
    ASSERT_EQ(abort_txn_resp.status().code(), MetaServiceCode::OK);

    brpc::Controller abort_txn_conflict_cntl;
    CheckTxnConflictRequest check_txn_conflict_req;
    CheckTxnConflictResponse check_txn_conflict_res;

    check_txn_conflict_req.set_cloud_unique_id(cloud_unique_id);
    check_txn_conflict_req.set_db_id(db_id);
    check_txn_conflict_req.set_end_txn_id(txn_id + 1);
    check_txn_conflict_req.add_table_ids(table_id);

    // first time to check txn conflict
    meta_service->check_txn_conflict(
            reinterpret_cast<::google::protobuf::RpcController*>(&begin_txn_cntl),
            &check_txn_conflict_req, &check_txn_conflict_res, nullptr);

    ASSERT_EQ(check_txn_conflict_res.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(check_txn_conflict_res.finished(), true);
    ASSERT_EQ(check_txn_conflict_res.conflict_txns_size(), 0);
}

TEST(MetaServiceTest, CheckTxnConflictTest) {
    auto meta_service = get_meta_service();

    const int64_t db_id = 666;
    const int64_t table_id = 777;
    const std::string label = "test_label";
    const std::string cloud_unique_id = "test_cloud_unique_id";
    int64_t txn_id = -1;

    brpc::Controller begin_txn_cntl;
    BeginTxnRequest begin_txn_req;
    BeginTxnResponse begin_txn_res;
    TxnInfoPB txn_info_pb;

    begin_txn_req.set_cloud_unique_id(cloud_unique_id);
    txn_info_pb.set_db_id(db_id);
    txn_info_pb.set_label(label);
    txn_info_pb.add_table_ids(table_id);
    txn_info_pb.set_timeout_ms(36000);
    begin_txn_req.mutable_txn_info()->CopyFrom(txn_info_pb);

    meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&begin_txn_cntl),
                            &begin_txn_req, &begin_txn_res, nullptr);
    ASSERT_EQ(begin_txn_res.status().code(), MetaServiceCode::OK);
    txn_id = begin_txn_res.txn_id();
    ASSERT_GT(txn_id, -1);

    brpc::Controller check_txn_conflict_cntl;
    CheckTxnConflictRequest check_txn_conflict_req;
    CheckTxnConflictResponse check_txn_conflict_res;

    check_txn_conflict_req.set_cloud_unique_id(cloud_unique_id);
    check_txn_conflict_req.set_db_id(db_id);
    check_txn_conflict_req.set_end_txn_id(txn_id + 1);
    check_txn_conflict_req.add_table_ids(table_id);

    // first time to check txn conflict
    meta_service->check_txn_conflict(
            reinterpret_cast<::google::protobuf::RpcController*>(&begin_txn_cntl),
            &check_txn_conflict_req, &check_txn_conflict_res, nullptr);

    ASSERT_EQ(check_txn_conflict_res.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(check_txn_conflict_res.finished(), false);
    ASSERT_EQ(check_txn_conflict_res.conflict_txns_size(), 1);
    check_txn_conflict_res.clear_conflict_txns();

    // mock rowset and tablet
    int64_t tablet_id_base = 123456;
    for (int i = 0; i < 5; ++i) {
        create_tablet(meta_service.get(), table_id, 1235, 1236, tablet_id_base + i);
        auto tmp_rowset = create_rowset(txn_id, tablet_id_base + i);
        CreateRowsetResponse res;
        commit_rowset(meta_service.get(), tmp_rowset, res);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    brpc::Controller commit_txn_cntl;
    CommitTxnRequest commit_txn_req;
    commit_txn_req.set_cloud_unique_id(cloud_unique_id);
    commit_txn_req.set_db_id(db_id);
    commit_txn_req.set_txn_id(txn_id);
    CommitTxnResponse commit_txn_res;
    meta_service->commit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&commit_txn_cntl),
                             &commit_txn_req, &commit_txn_res, nullptr);
    ASSERT_EQ(commit_txn_res.status().code(), MetaServiceCode::OK);

    // second time to check txn conflict
    meta_service->check_txn_conflict(
            reinterpret_cast<::google::protobuf::RpcController*>(&check_txn_conflict_cntl),
            &check_txn_conflict_req, &check_txn_conflict_res, nullptr);

    ASSERT_EQ(check_txn_conflict_res.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(check_txn_conflict_res.finished(), true);
    ASSERT_EQ(check_txn_conflict_res.conflict_txns_size(), 0);

    {
        std::string running_key = txn_running_key({mock_instance, db_id, txn_id});
        std::string running_value;
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = meta_service->txn_kv()->create_txn(&txn);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        ASSERT_EQ(txn->get(running_key, &running_value), TxnErrorCode::TXN_KEY_NOT_FOUND);
    }
}

TEST(MetaServiceTest, CheckNotTimeoutTxnConflictTest) {
    auto meta_service = get_meta_service();

    const int64_t db_id = 666;
    const int64_t table_id = 777;
    const std::string label = "test_label";
    const std::string cloud_unique_id = "test_cloud_unique_id";
    int64_t txn_id = -1;

    brpc::Controller begin_txn_cntl;
    BeginTxnRequest begin_txn_req;
    BeginTxnResponse begin_txn_res;
    TxnInfoPB txn_info_pb;

    begin_txn_req.set_cloud_unique_id(cloud_unique_id);
    txn_info_pb.set_db_id(db_id);
    txn_info_pb.set_label(label);
    txn_info_pb.add_table_ids(table_id);
    txn_info_pb.set_timeout_ms(3);
    begin_txn_req.mutable_txn_info()->CopyFrom(txn_info_pb);

    meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&begin_txn_cntl),
                            &begin_txn_req, &begin_txn_res, nullptr);
    ASSERT_EQ(begin_txn_res.status().code(), MetaServiceCode::OK);
    txn_id = begin_txn_res.txn_id();
    ASSERT_GT(txn_id, -1);

    brpc::Controller check_txn_conflict_cntl;
    CheckTxnConflictRequest check_txn_conflict_req;
    CheckTxnConflictResponse check_txn_conflict_res;

    check_txn_conflict_req.set_cloud_unique_id(cloud_unique_id);
    check_txn_conflict_req.set_db_id(db_id);
    check_txn_conflict_req.set_end_txn_id(txn_id + 1);
    check_txn_conflict_req.add_table_ids(table_id);

    // wait txn timeout
    sleep(5);
    // first time to check txn conflict
    meta_service->check_txn_conflict(
            reinterpret_cast<::google::protobuf::RpcController*>(&begin_txn_cntl),
            &check_txn_conflict_req, &check_txn_conflict_res, nullptr);

    ASSERT_EQ(check_txn_conflict_res.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(check_txn_conflict_res.finished(), true);
}

TEST(MetaServiceTest, CheckTxnConflictWithAbortLabelTest) {
    int ret = 0;

    auto txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<MemTxnKv>());
    if (txn_kv != nullptr) {
        ret = txn_kv->init();
        [&] { ASSERT_EQ(ret, 0); }();
    }
    [&] { ASSERT_NE(txn_kv.get(), nullptr); }();

    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        txn->remove("\x00", "\xfe"); // This is dangerous if the fdb is not correctly set
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    auto rs = std::make_shared<MockResourceManager>(txn_kv);
    auto rl = std::make_shared<RateLimiter>();
    auto meta_service =
            std::make_unique<MetaServiceProxy>(std::make_unique<MetaServiceImpl>(txn_kv, rs, rl));

    const int64_t db_id = 666;
    const int64_t table_id = 777;
    const std::string label = "test_label";
    const std::string cloud_unique_id = "test_cloud_unique_id";
    int64_t txn_id = -1;

    brpc::Controller begin_txn_cntl;
    BeginTxnRequest begin_txn_req;
    BeginTxnResponse begin_txn_res;
    TxnInfoPB txn_info_pb;

    begin_txn_req.set_cloud_unique_id(cloud_unique_id);
    txn_info_pb.set_db_id(db_id);
    txn_info_pb.set_label(label);
    txn_info_pb.add_table_ids(table_id);
    txn_info_pb.set_timeout_ms(36000);
    begin_txn_req.mutable_txn_info()->CopyFrom(txn_info_pb);

    meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&begin_txn_cntl),
                            &begin_txn_req, &begin_txn_res, nullptr);
    ASSERT_EQ(begin_txn_res.status().code(), MetaServiceCode::OK);
    txn_id = begin_txn_res.txn_id();
    ASSERT_GT(txn_id, -1);

    brpc::Controller check_txn_conflict_cntl;
    CheckTxnConflictRequest check_txn_conflict_req;
    CheckTxnConflictResponse check_txn_conflict_res;

    check_txn_conflict_req.set_cloud_unique_id(cloud_unique_id);
    check_txn_conflict_req.set_db_id(db_id);
    check_txn_conflict_req.set_end_txn_id(txn_id + 1);
    check_txn_conflict_req.add_table_ids(table_id);

    // first time to check txn conflict
    meta_service->check_txn_conflict(
            reinterpret_cast<::google::protobuf::RpcController*>(&begin_txn_cntl),
            &check_txn_conflict_req, &check_txn_conflict_res, nullptr);

    ASSERT_EQ(check_txn_conflict_res.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(check_txn_conflict_res.finished(), false);

    std::string running_key;
    std::string running_val;
    txn_running_key({mock_instance, db_id, txn_id}, &running_key);
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        ASSERT_EQ(txn->get(running_key, &running_val), TxnErrorCode::TXN_OK);
    }

    brpc::Controller abort_txn_cntl;
    AbortTxnRequest abort_txn_req;
    abort_txn_req.set_cloud_unique_id(cloud_unique_id);
    abort_txn_req.set_db_id(db_id);
    abort_txn_req.set_label(label);
    AbortTxnResponse abort_txn_res;
    meta_service->abort_txn(reinterpret_cast<::google::protobuf::RpcController*>(&abort_txn_cntl),
                            &abort_txn_req, &abort_txn_res, nullptr);
    ASSERT_EQ(abort_txn_res.status().code(), MetaServiceCode::OK);

    // second time to check txn conflict
    meta_service->check_txn_conflict(
            reinterpret_cast<::google::protobuf::RpcController*>(&check_txn_conflict_cntl),
            &check_txn_conflict_req, &check_txn_conflict_res, nullptr);

    ASSERT_EQ(check_txn_conflict_res.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(check_txn_conflict_res.finished(), true);

    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        ASSERT_EQ(txn->get(running_key, &running_val), TxnErrorCode::TXN_KEY_NOT_FOUND);
    }
}

TEST(MetaServiceTest, CleanTxnLabelTest) {
    int ret = 0;
    auto txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<MemTxnKv>());
    if (txn_kv != nullptr) {
        ret = txn_kv->init();
        [&] { ASSERT_EQ(ret, 0); }();
    }
    [&] { ASSERT_NE(txn_kv.get(), nullptr); }();

    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        txn->remove("\x00", "\xfe"); // This is dangerous if the fdb is not correctly set
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    auto rs = std::make_shared<MockResourceManager>(txn_kv);
    auto rl = std::make_shared<RateLimiter>();
    auto meta_service =
            std::make_unique<MetaServiceProxy>(std::make_unique<MetaServiceImpl>(txn_kv, rs, rl));

    // clean txn label by db_id and label
    {
        int64_t txn_id = -1;
        int64_t db_id = 1987211;
        const std::string& label = "test_clean_label";

        {
            brpc::Controller cntl;
            CleanTxnLabelRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            req.set_db_id(db_id);
            req.add_labels(label);
            CleanTxnLabelResponse res;
            meta_service->clean_txn_label(
                    reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res,
                    nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        }

        // begin txn
        {
            brpc::Controller cntl;
            BeginTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            TxnInfoPB txn_info_pb;
            txn_info_pb.set_db_id(db_id);
            txn_info_pb.set_label(label);
            txn_info_pb.add_table_ids(1234);
            txn_info_pb.set_timeout_ms(36000);
            req.mutable_txn_info()->CopyFrom(txn_info_pb);
            BeginTxnResponse res;
            meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            txn_id = res.txn_id();
        }

        const std::string info_key = txn_info_key({mock_instance, db_id, txn_id});
        std::string info_val;

        const std::string label_key = txn_label_key({mock_instance, db_id, label});
        std::string label_val;

        const std::string index_key = txn_index_key({mock_instance, txn_id});
        std::string index_val;

        const std::string running_key = txn_running_key({mock_instance, db_id, txn_id});
        std::string running_val;

        const std::string recycle_key = recycle_txn_key({mock_instance, db_id, txn_id});
        std::string recycle_val;

        {
            std::unique_ptr<Transaction> txn;
            ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
            TxnErrorCode err = txn->get(info_key, &info_val);
            ASSERT_EQ(err, TxnErrorCode::TXN_OK);
            err = txn->get(label_key, &label_val);
            ASSERT_EQ(err, TxnErrorCode::TXN_OK);
            err = txn->get(index_key, &index_val);
            ASSERT_EQ(err, TxnErrorCode::TXN_OK);
            err = txn->get(running_key, &running_val);
            ASSERT_EQ(err, TxnErrorCode::TXN_OK);
            err = txn->get(recycle_key, &recycle_val);
            ASSERT_EQ(err, TxnErrorCode::TXN_KEY_NOT_FOUND);
        }

        // mock rowset and tablet
        int64_t tablet_id_base = 110313131;
        for (int i = 0; i < 2; ++i) {
            create_tablet(meta_service.get(), 1234, 1235, 1236, tablet_id_base + i);
            auto tmp_rowset = create_rowset(txn_id, tablet_id_base + i);
            CreateRowsetResponse res;
            commit_rowset(meta_service.get(), tmp_rowset, res);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        }

        // commit txn
        {
            brpc::Controller cntl;
            CommitTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            req.set_db_id(db_id);
            req.set_txn_id(txn_id);
            CommitTxnResponse res;
            meta_service->commit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        }

        // begin txn
        {
            brpc::Controller cntl;
            BeginTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            TxnInfoPB txn_info_pb;
            txn_info_pb.set_db_id(db_id);
            txn_info_pb.set_label(label);
            txn_info_pb.add_table_ids(1234);
            txn_info_pb.set_timeout_ms(36000);
            req.mutable_txn_info()->CopyFrom(txn_info_pb);
            BeginTxnResponse res;
            meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::TXN_LABEL_ALREADY_USED);
        }

        // clean txn label
        {
            brpc::Controller cntl;
            CleanTxnLabelRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            CleanTxnLabelResponse res;
            meta_service->clean_txn_label(
                    reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res,
                    nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
        }

        // clean txn label
        {
            brpc::Controller cntl;
            CleanTxnLabelRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            req.set_db_id(db_id);
            req.add_labels(label);
            CleanTxnLabelResponse res;
            meta_service->clean_txn_label(
                    reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res,
                    nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        }

        {
            std::unique_ptr<Transaction> txn;
            ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
            TxnErrorCode err = txn->get(info_key, &info_val);
            ASSERT_EQ(err, TxnErrorCode::TXN_KEY_NOT_FOUND);
            err = txn->get(label_key, &label_val);
            ASSERT_EQ(err, TxnErrorCode::TXN_KEY_NOT_FOUND);
            err = txn->get(index_key, &index_val);
            ASSERT_EQ(err, TxnErrorCode::TXN_KEY_NOT_FOUND);
            err = txn->get(running_key, &running_val);
            ASSERT_EQ(err, TxnErrorCode::TXN_KEY_NOT_FOUND);
            err = txn->get(recycle_key, &recycle_val);
            ASSERT_EQ(err, TxnErrorCode::TXN_KEY_NOT_FOUND);
        }

        {
            brpc::Controller cntl;
            BeginTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            TxnInfoPB txn_info_pb;
            txn_info_pb.set_db_id(db_id);
            txn_info_pb.set_label(label);
            txn_info_pb.add_table_ids(1234);
            txn_info_pb.set_timeout_ms(36000);
            req.mutable_txn_info()->CopyFrom(txn_info_pb);
            BeginTxnResponse res;
            meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            txn_id = res.txn_id();
        }

        // abort txn
        {
            brpc::Controller cntl;
            AbortTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            ASSERT_GT(txn_id, 0);
            req.set_txn_id(txn_id);
            req.set_reason("test");
            AbortTxnResponse res;
            meta_service->abort_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            ASSERT_EQ(res.txn_info().status(), TxnStatusPB::TXN_STATUS_ABORTED);
        }

        {
            brpc::Controller cntl;
            BeginTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            TxnInfoPB txn_info_pb;
            txn_info_pb.set_db_id(db_id);
            txn_info_pb.set_label(label);
            txn_info_pb.add_table_ids(1234);
            txn_info_pb.set_timeout_ms(36000);
            req.mutable_txn_info()->CopyFrom(txn_info_pb);
            BeginTxnResponse res;
            meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            txn_id = res.txn_id();
        }

        // clean txn label
        {
            brpc::Controller cntl;
            CleanTxnLabelRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            req.set_db_id(db_id);
            req.add_labels(label);
            CleanTxnLabelResponse res;
            meta_service->clean_txn_label(
                    reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res,
                    nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        }

        // abort txn
        {
            brpc::Controller cntl;
            AbortTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            ASSERT_GT(txn_id, 0);
            req.set_txn_id(txn_id);
            req.set_reason("test");
            AbortTxnResponse res;
            meta_service->abort_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            ASSERT_EQ(res.txn_info().status(), TxnStatusPB::TXN_STATUS_ABORTED);
        }

        // clean txn label
        {
            brpc::Controller cntl;
            CleanTxnLabelRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            req.set_db_id(db_id);
            req.add_labels(label);
            CleanTxnLabelResponse res;
            meta_service->clean_txn_label(
                    reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res,
                    nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        }

        {
            const std::string info_key = txn_info_key({mock_instance, db_id, txn_id});
            std::string info_val;

            const std::string label_key = txn_label_key({mock_instance, db_id, label});
            std::string label_val;

            const std::string index_key = txn_index_key({mock_instance, txn_id});
            std::string index_val;

            const std::string running_key = txn_running_key({mock_instance, db_id, txn_id});
            std::string running_val;

            const std::string recycle_key = recycle_txn_key({mock_instance, db_id, txn_id});
            std::string recycle_val;

            std::unique_ptr<Transaction> txn;
            ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
            TxnErrorCode ret = txn->get(info_key, &info_val);
            ASSERT_EQ(ret, TxnErrorCode::TXN_KEY_NOT_FOUND);
            ret = txn->get(label_key, &label_val);
            ASSERT_EQ(ret, TxnErrorCode::TXN_KEY_NOT_FOUND);
            ret = txn->get(index_key, &index_val);
            ASSERT_EQ(ret, TxnErrorCode::TXN_KEY_NOT_FOUND);
            ret = txn->get(running_key, &running_val);
            ASSERT_EQ(ret, TxnErrorCode::TXN_KEY_NOT_FOUND);
            ret = txn->get(recycle_key, &recycle_val);
            ASSERT_EQ(ret, TxnErrorCode::TXN_KEY_NOT_FOUND);
        }
    }
    // clean txn label only by db_id
    {
        int64_t db_id = 1987211123;
        const std::string& label = "test_clean_label";

        TxnInfoPB txn_info_pb;
        txn_info_pb.set_db_id(db_id);
        txn_info_pb.add_table_ids(1234);
        txn_info_pb.set_timeout_ms(36000);
        BeginTxnRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");

        //clean not exist label
        {
            brpc::Controller cntl;
            CleanTxnLabelRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            req.set_db_id(db_id);
            CleanTxnLabelResponse res;
            meta_service->clean_txn_label(
                    reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res,
                    nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        }

        // inject internal_clean_label err = TXN_CONFLICT
        {
            auto sp = SyncPoint::get_instance();
            sp->set_call_back("internal_clean_label:err", [&](auto&& args) {
                auto* err = try_any_cast<TxnErrorCode*>(args[0]);
                *err = TxnErrorCode::TXN_CONFLICT;
            });
            sp->enable_processing();
            int64_t txn_id = -1;
            for (int i = 100; i < 101; i++) {
                {
                    std::stringstream label_ss;
                    label_ss << label << i;
                    brpc::Controller cntl;
                    txn_info_pb.set_label(label_ss.str());
                    req.mutable_txn_info()->CopyFrom(txn_info_pb);
                    BeginTxnResponse res;
                    meta_service->begin_txn(
                            reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res,
                            nullptr);
                    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
                    txn_id = res.txn_id();
                }

                {
                    // mock rowset and tablet
                    int64_t tablet_id_base = 110313131;
                    for (int i = 0; i < 1; ++i) {
                        create_tablet(meta_service.get(), 1234, 1235, 1236, tablet_id_base + i);
                        auto tmp_rowset = create_rowset(txn_id, tablet_id_base + i);
                        CreateRowsetResponse res;
                        commit_rowset(meta_service.get(), tmp_rowset, res);
                        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
                    }
                }

                // commit txn
                {
                    brpc::Controller cntl;
                    CommitTxnRequest req;
                    req.set_cloud_unique_id("test_cloud_unique_id");
                    req.set_db_id(db_id);
                    req.set_txn_id(txn_id);
                    CommitTxnResponse res;
                    meta_service->commit_txn(
                            reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res,
                            nullptr);
                    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
                }
            }

            {
                brpc::Controller cntl;
                CleanTxnLabelRequest req;
                req.set_cloud_unique_id("test_cloud_unique_id");
                req.set_db_id(db_id);
                CleanTxnLabelResponse res;
                meta_service->clean_txn_label(
                        reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res,
                        nullptr);
                ASSERT_EQ(res.status().code(), MetaServiceCode::KV_TXN_CONFLICT);
            }
            sp->clear_all_call_backs();
            sp->clear_trace();
            sp->disable_processing();
        }

        // create 12 committed txns and clean label by id
        {
            auto sp = SyncPoint::get_instance();
            sp->set_call_back("clean_txn_label:limit", [](auto&& args) {
                int* limit = try_any_cast<int*>(args[0]);
                *limit = 5;
            });
            sp->enable_processing();

            int64_t txn_id = -1;
            for (int i = 0; i < 12; i++) {
                {
                    std::stringstream label_ss;
                    label_ss << label << i;
                    brpc::Controller cntl;
                    txn_info_pb.set_label(label_ss.str());
                    req.mutable_txn_info()->CopyFrom(txn_info_pb);
                    BeginTxnResponse res;
                    meta_service->begin_txn(
                            reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res,
                            nullptr);
                    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
                    txn_id = res.txn_id();
                }

                {
                    // mock rowset and tablet
                    int64_t tablet_id_base = 110313131;
                    for (int i = 0; i < 1; ++i) {
                        create_tablet(meta_service.get(), 1234, 1235, 1236, tablet_id_base + i);
                        auto tmp_rowset = create_rowset(txn_id, tablet_id_base + i);
                        CreateRowsetResponse res;
                        commit_rowset(meta_service.get(), tmp_rowset, res);
                        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
                    }
                }

                // commit txn
                {
                    brpc::Controller cntl;
                    CommitTxnRequest req;
                    req.set_cloud_unique_id("test_cloud_unique_id");
                    req.set_db_id(db_id);
                    req.set_txn_id(txn_id);
                    CommitTxnResponse res;
                    meta_service->commit_txn(
                            reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res,
                            nullptr);
                    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
                }
            }

            {
                brpc::Controller cntl;
                CleanTxnLabelRequest req;
                req.set_cloud_unique_id("test_cloud_unique_id");
                req.set_db_id(db_id);
                CleanTxnLabelResponse res;
                meta_service->clean_txn_label(
                        reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res,
                        nullptr);
                ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            }
            sp->clear_all_call_backs();
            sp->clear_trace();
            sp->disable_processing();
        }
    }
}

TEST(MetaServiceTest, GetTxnTest) {
    int ret = 0;
    auto txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<MemTxnKv>());
    if (txn_kv != nullptr) {
        ret = txn_kv->init();
        [&] { ASSERT_EQ(ret, 0); }();
    }
    [&] { ASSERT_NE(txn_kv.get(), nullptr); }();

    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        txn->remove("\x00", "\xfe"); // This is dangerous if the fdb is not correctly set
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    auto rs = std::make_shared<MockResourceManager>(txn_kv);
    auto rl = std::make_shared<RateLimiter>();
    auto meta_service =
            std::make_unique<MetaServiceProxy>(std::make_unique<MetaServiceImpl>(txn_kv, rs, rl));

    {
        int64_t txn_id = -1;
        int64_t db_id = 34521431231;
        const std::string& label = "test_get_txn";

        // begin txn
        {
            brpc::Controller cntl;
            BeginTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            TxnInfoPB txn_info_pb;
            txn_info_pb.set_db_id(db_id);
            txn_info_pb.set_label(label);
            txn_info_pb.add_table_ids(1234);
            txn_info_pb.set_timeout_ms(36000);
            req.mutable_txn_info()->CopyFrom(txn_info_pb);
            BeginTxnResponse res;
            meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            txn_id = res.txn_id();
        }

        {
            brpc::Controller cntl;
            GetTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            req.set_db_id(db_id);
            req.set_txn_id(-1);
            GetTxnResponse res;
            meta_service->get_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                  &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
        }

        {
            brpc::Controller cntl;
            GetTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            req.set_db_id(db_id);
            req.set_txn_id(txn_id);
            GetTxnResponse res;
            meta_service->get_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                  &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        }

        {
            brpc::Controller cntl;
            GetTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            req.set_txn_id(txn_id);
            GetTxnResponse res;
            meta_service->get_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                  &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        }
    }
}
//

TEST(MetaServiceTest, CopyJobTest) {
    auto meta_service = get_meta_service();
    brpc::Controller cntl;
    auto cloud_unique_id = "test_cloud_unique_id";
    auto stage_id = "test_stage_id";
    int64_t table_id = 100;
    std::string instance_id = "copy_job_test_instance_id";

    [[maybe_unused]] auto sp = SyncPoint::get_instance();
    DORIS_CLOUD_DEFER {
        SyncPoint::get_instance()->clear_all_call_backs();
    };
    sp->set_call_back("get_instance_id", [&](auto&& args) {
        auto* ret = try_any_cast_ret<std::string>(args);
        ret->first = instance_id;
        ret->second = true;
    });
    sp->enable_processing();

    // generate a begin copy request
    BeginCopyRequest begin_copy_request;
    begin_copy_request.set_cloud_unique_id(cloud_unique_id);
    begin_copy_request.set_stage_id(stage_id);
    begin_copy_request.set_stage_type(StagePB::EXTERNAL);
    begin_copy_request.set_table_id(table_id);
    begin_copy_request.set_copy_id("test_copy_id");
    begin_copy_request.set_group_id(0);
    begin_copy_request.set_start_time_ms(200);
    begin_copy_request.set_timeout_time_ms(300);
    for (int i = 0; i < 20; ++i) {
        ObjectFilePB object_file_pb;
        object_file_pb.set_relative_path("obj_" + std::to_string(i));
        object_file_pb.set_etag("obj_" + std::to_string(i) + "_etag");
        begin_copy_request.add_object_files()->CopyFrom(object_file_pb);
    }

    // generate a finish copy request
    FinishCopyRequest finish_copy_request;
    finish_copy_request.set_cloud_unique_id(cloud_unique_id);
    finish_copy_request.set_stage_id(stage_id);
    finish_copy_request.set_stage_type(StagePB::EXTERNAL);
    finish_copy_request.set_table_id(table_id);
    finish_copy_request.set_copy_id("test_copy_id");
    finish_copy_request.set_group_id(0);
    finish_copy_request.set_action(FinishCopyRequest::COMMIT);

    // generate a get copy files request
    GetCopyFilesRequest get_copy_file_req;
    get_copy_file_req.set_cloud_unique_id(cloud_unique_id);
    get_copy_file_req.set_stage_id(stage_id);
    get_copy_file_req.set_table_id(table_id);

    // generate a get copy job request
    GetCopyJobRequest get_copy_job_request;
    get_copy_job_request.set_cloud_unique_id(cloud_unique_id);
    get_copy_job_request.set_stage_id(stage_id);
    get_copy_job_request.set_table_id(table_id);
    get_copy_job_request.set_copy_id("test_copy_id");
    get_copy_job_request.set_group_id(0);

    // get copy job
    {
        GetCopyJobResponse res;
        meta_service->get_copy_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                   &get_copy_job_request, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(res.has_copy_job(), false);
    }
    // begin copy
    {
        BeginCopyResponse res;
        meta_service->begin_copy(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                 &begin_copy_request, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(res.filtered_object_files_size(), 20);
    }
    // get copy files
    {
        GetCopyFilesResponse res;
        meta_service->get_copy_files(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &get_copy_file_req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(res.object_files_size(), 20);
    }
    // get copy job
    {
        GetCopyJobResponse res;
        meta_service->get_copy_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                   &get_copy_job_request, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(res.copy_job().object_files().size(), 20);
    }
    // begin copy with duplicate files
    {
        begin_copy_request.set_copy_id("test_copy_id_1");
        begin_copy_request.clear_object_files();
        for (int i = 15; i < 30; ++i) {
            ObjectFilePB object_file_pb;
            object_file_pb.set_relative_path("obj_" + std::to_string(i));
            object_file_pb.set_etag("obj_" + std::to_string(i) + "_etag");
            begin_copy_request.add_object_files()->CopyFrom(object_file_pb);
        }

        BeginCopyResponse res;
        meta_service->begin_copy(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                 &begin_copy_request, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(res.filtered_object_files_size(), 10);
    }
    // get copy files
    {
        GetCopyFilesResponse res;
        meta_service->get_copy_files(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &get_copy_file_req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(res.object_files_size(), 30);
    }
    // finish the first copy job
    {
        FinishCopyResponse res;
        meta_service->finish_copy(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                  &finish_copy_request, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }
    // get copy files
    {
        GetCopyFilesResponse res;
        meta_service->get_copy_files(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &get_copy_file_req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(res.object_files_size(), 30);
    }
    // abort the second copy job
    {
        finish_copy_request.set_copy_id("test_copy_id_1");
        finish_copy_request.set_action(FinishCopyRequest::ABORT);

        FinishCopyResponse res;
        meta_service->finish_copy(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                  &finish_copy_request, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }
    // get copy files
    {
        GetCopyFilesResponse res;
        meta_service->get_copy_files(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &get_copy_file_req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(res.object_files_size(), 20);
    }
    {
        // begin a copy job whose files are all loaded, the copy job key should not be created
        begin_copy_request.set_copy_id("tmp_id");
        begin_copy_request.clear_object_files();
        for (int i = 0; i < 20; ++i) {
            ObjectFilePB object_file_pb;
            object_file_pb.set_relative_path("obj_" + std::to_string(i));
            object_file_pb.set_etag("obj_" + std::to_string(i) + "_etag");
            begin_copy_request.add_object_files()->CopyFrom(object_file_pb);
        }
        BeginCopyResponse res;
        meta_service->begin_copy(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                 &begin_copy_request, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(res.filtered_object_files_size(), 0);
        // get copy job
        get_copy_job_request.set_copy_id("tmp_id");
        GetCopyJobResponse res2;
        meta_service->get_copy_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                   &get_copy_job_request, &res2, nullptr);
        ASSERT_EQ(res2.status().code(), MetaServiceCode::OK);
        ASSERT_FALSE(res2.has_copy_job());
    }
    // scan fdb
    {
        std::unique_ptr<Transaction> txn;
        std::string get_val;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        // 20 copy files
        {
            CopyFileKeyInfo key_info0 {instance_id, stage_id, table_id, "", ""};
            CopyFileKeyInfo key_info1 {instance_id, stage_id, table_id + 1, "", ""};
            std::string key0;
            std::string key1;
            copy_file_key(key_info0, &key0);
            copy_file_key(key_info1, &key1);
            std::unique_ptr<RangeGetIterator> it;
            ASSERT_EQ(txn->get(key0, key1, &it), TxnErrorCode::TXN_OK);
            int file_cnt = 0;
            do {
                ASSERT_EQ(txn->get(key0, key1, &it), TxnErrorCode::TXN_OK);
                while (it->has_next()) {
                    auto [k, v] = it->next();
                    CopyFilePB copy_file;
                    ASSERT_TRUE(copy_file.ParseFromArray(v.data(), v.size()));
                    ASSERT_EQ(copy_file.copy_id(), "test_copy_id");
                    ++file_cnt;
                    if (!it->has_next()) {
                        key0 = k;
                    }
                }
                key0.push_back('\x00');
            } while (it->more());
            ASSERT_EQ(file_cnt, 20);
        }
        // 1 copy job with finish status
        {
            CopyJobKeyInfo key_info0 {instance_id, stage_id, table_id, "", 0};
            CopyJobKeyInfo key_info1 {instance_id, stage_id, table_id + 1, "", 0};
            std::string key0;
            std::string key1;
            copy_job_key(key_info0, &key0);
            copy_job_key(key_info1, &key1);
            std::unique_ptr<RangeGetIterator> it;
            int job_cnt = 0;
            do {
                ASSERT_EQ(txn->get(key0, key1, &it), TxnErrorCode::TXN_OK);
                while (it->has_next()) {
                    auto [k, v] = it->next();
                    CopyJobPB copy_job;
                    ASSERT_EQ(copy_job.ParseFromArray(v.data(), v.size()), true);
                    ASSERT_EQ(copy_job.object_files_size(), 20);
                    ASSERT_EQ(copy_job.job_status(), CopyJobPB::FINISH);
                    ++job_cnt;
                    if (!it->has_next()) {
                        key0 = k;
                    }
                }
                key0.push_back('\x00');
            } while (it->more());
            ASSERT_EQ(job_cnt, 1);
        }
    }
}

TEST(MetaServiceTest, FilterCopyFilesTest) {
    auto meta_service = get_meta_service();
    brpc::Controller cntl;
    auto cloud_unique_id = "test_cloud_unique_id";
    std::string instance_id = "stage_test_instance_id";
    auto stage_id = "test_stage_id";
    int64_t table_id = 100;
    [[maybe_unused]] auto sp = SyncPoint::get_instance();
    sp->set_call_back("get_instance_id", [&](auto&& args) {
        auto* ret = try_any_cast_ret<std::string>(args);
        ret->first = instance_id;
        ret->second = true;
    });
    sp->set_call_back("encrypt_ak_sk:get_encryption_key", [](auto&& args) {
        auto* ret = try_any_cast<int*>(args[0]);
        *ret = 0;
        auto* key = try_any_cast<std::string*>(args[1]);
        *key = "test";
        auto* key_id = try_any_cast<int64_t*>(args[2]);
        *key_id = 1;
    });
    sp->set_call_back("decrypt_ak_sk:get_encryption_key", [](auto&& args) {
        auto* key = try_any_cast<std::string*>(args[0]);
        *key = "test";
        auto* ret = try_any_cast<int*>(args[1]);
        *ret = 0;
    });
    sp->enable_processing();

    FilterCopyFilesRequest request;
    request.set_cloud_unique_id(cloud_unique_id);
    request.set_stage_id(stage_id);
    request.set_table_id(table_id);
    for (int i = 0; i < 10; ++i) {
        ObjectFilePB object_file;
        object_file.set_relative_path("file" + std::to_string(i));
        object_file.set_etag("etag" + std::to_string(i));
        request.add_object_files()->CopyFrom(object_file);
    }

    // all files are not loaded
    {
        FilterCopyFilesResponse res;
        meta_service->filter_copy_files(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &request, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(res.object_files().size(), 10);
    }

    // some files are loaded
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        for (int i = 0; i < 4; ++i) {
            CopyFileKeyInfo key_info {instance_id, stage_id, table_id, "file" + std::to_string(i),
                                      "etag" + std::to_string(i)};
            std::string key;
            copy_file_key(key_info, &key);
            CopyFilePB copy_file;
            copy_file.set_copy_id("test_copy_id");
            std::string val;
            copy_file.SerializeToString(&val);
            txn->put(key, val);
        }
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
        FilterCopyFilesResponse res;
        meta_service->filter_copy_files(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &request, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(res.object_files().size(), 6);
        ASSERT_EQ(res.object_files().at(0).relative_path(), "file4");
    }

    // all files are loaded
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        for (int i = 4; i < 10; ++i) {
            CopyFileKeyInfo key_info {instance_id, stage_id, table_id, "file" + std::to_string(i),
                                      "etag" + std::to_string(i)};
            std::string key;
            copy_file_key(key_info, &key);
            CopyFilePB copy_file;
            copy_file.set_copy_id("test_copy_id");
            std::string val;
            copy_file.SerializeToString(&val);
            txn->put(key, val);
        }
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
        FilterCopyFilesResponse res;
        meta_service->filter_copy_files(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &request, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(res.object_files().size(), 0);
    }
}

extern std::vector<std::pair<int64_t, int64_t>> calc_sync_versions(
        int64_t req_bc_cnt, int64_t bc_cnt, int64_t req_cc_cnt, int64_t cc_cnt, int64_t req_cp,
        int64_t cp, int64_t req_start, int64_t req_end);

TEST(MetaServiceTest, CalcSyncVersionsTest) {
    using Versions = std::vector<std::pair<int64_t, int64_t>>;
    // * no compaction happened
    // req_cc_cnt == ms_cc_cnt && req_bc_cnt == ms_bc_cnt && req_cp == ms_cp
    // BE  [=][=][=][=][=====][=][=]<.......>
    //                  ^~~~~ req_cp
    // BE  [=][=][=][=][=====][=][=][=][=][=][=]
    //                  ^~~~~ ms_cp
    //                               ^_____^ versions_return: [req_start, req_end]
    {
        auto [req_bc_cnt, bc_cnt] = std::tuple {0, 0};
        auto [req_cc_cnt, cc_cnt] = std::tuple {1, 1};
        auto [req_cp, cp] = std::tuple {5, 5};
        auto [req_start, req_end] = std::tuple {8, 12};
        auto versions = calc_sync_versions(req_bc_cnt, bc_cnt, req_cc_cnt, cc_cnt, req_cp, cp,
                                           req_start, req_end);
        ASSERT_EQ(versions, (Versions {{8, 12}}));
    }
    // * only one CC happened and CP changed
    // req_cc_cnt == ms_cc_cnt - 1 && req_bc_cnt == ms_bc_cnt && req_cp < ms_cp
    // BE  [=][=][=][=][=====][=][=]<.......>
    //                  ^~~~~ req_cp
    // MS  [=][=][=][=][xxxxxxxxxxxxxx][=======][=][=]
    //                                  ^~~~~~~ ms_cp
    //                  ^__________________^ versions_return: [req_cp, ms_cp - 1] v [req_start, req_end]
    {
        auto [req_bc_cnt, bc_cnt] = std::tuple {0, 0};
        auto [req_cc_cnt, cc_cnt] = std::tuple {1, 2};
        auto [req_cp, cp] = std::tuple {5, 10};
        auto [req_start, req_end] = std::tuple {8, 12};
        auto versions = calc_sync_versions(req_bc_cnt, bc_cnt, req_cc_cnt, cc_cnt, req_cp, cp,
                                           req_start, req_end);
        ASSERT_EQ(versions, (Versions {{5, 12}})); // [5, 9] v [8, 12]
    }
    {
        auto [req_bc_cnt, bc_cnt] = std::tuple {0, 0};
        auto [req_cc_cnt, cc_cnt] = std::tuple {1, 2};
        auto [req_cp, cp] = std::tuple {5, 15};
        auto [req_start, req_end] = std::tuple {8, 12};
        auto versions = calc_sync_versions(req_bc_cnt, bc_cnt, req_cc_cnt, cc_cnt, req_cp, cp,
                                           req_start, req_end);
        ASSERT_EQ(versions, (Versions {{5, 14}})); // [5, 14] v [8, 12]
    }
    // * only one CC happened and CP remain unchanged
    // req_cc_cnt == ms_cc_cnt - 1 && req_bc_cnt == ms_bc_cnt && req_cp == ms_cp
    // BE  [=][=][=][=][=====][=][=]<.......>
    //                  ^~~~~ req_cp
    // MS  [=][=][=][=][xxxxxxxxxxxxxx][=][=][=][=][=]
    //                  ^~~~~~~~~~~~~~ ms_cp
    //                  ^__________________^ versions_return: [req_cp, max] v [req_start, req_end]
    //
    {
        auto [req_bc_cnt, bc_cnt] = std::tuple {0, 0};
        auto [req_cc_cnt, cc_cnt] = std::tuple {1, 2};
        auto [req_cp, cp] = std::tuple {5, 5};
        auto [req_start, req_end] = std::tuple {8, 12};
        auto versions = calc_sync_versions(req_bc_cnt, bc_cnt, req_cc_cnt, cc_cnt, req_cp, cp,
                                           req_start, req_end);
        ASSERT_EQ(versions, (Versions {{5, INT64_MAX - 1}})); // [5, max] v [8, 12]
    }
    // * more than one CC happened and CP remain unchanged
    // req_cc_cnt < ms_cc_cnt - 1 && req_bc_cnt == ms_bc_cnt && req_cp == ms_cp
    // BE  [=][=][=][=][=====][=][=]<.......>
    //                  ^~~~~ req_cp
    // MS  [=][=][=][=][xxxxxxxxxxxxxx][xxxxxxx][=][=]
    //                  ^~~~~~~~~~~~~~ ms_cp
    //                  ^_____________________^ versions_return: [req_cp, max] v [req_start, req_end]
    {
        auto [req_bc_cnt, bc_cnt] = std::tuple {0, 0};
        auto [req_cc_cnt, cc_cnt] = std::tuple {1, 3};
        auto [req_cp, cp] = std::tuple {5, 5};
        auto [req_start, req_end] = std::tuple {8, 12};
        auto versions = calc_sync_versions(req_bc_cnt, bc_cnt, req_cc_cnt, cc_cnt, req_cp, cp,
                                           req_start, req_end);
        ASSERT_EQ(versions, (Versions {{5, INT64_MAX - 1}})); // [5, max] v [8, 12]
    }
    // * more than one CC happened and CP changed
    // BE  [=][=][=][=][=====][=][=]
    //                  ^~~~~ req_cp
    // MS  [=][=][=][=][xxxxxxxxxxxxxx][xxxxxxx][=][=]
    //                                  ^~~~~~~ ms_cp
    //                  ^_____________________^ related_versions: [req_cp, max] v [req_start, req_end]
    {
        auto [req_bc_cnt, bc_cnt] = std::tuple {0, 0};
        auto [req_cc_cnt, cc_cnt] = std::tuple {1, 3};
        auto [req_cp, cp] = std::tuple {5, 15};
        auto [req_start, req_end] = std::tuple {8, 12};
        auto versions = calc_sync_versions(req_bc_cnt, bc_cnt, req_cc_cnt, cc_cnt, req_cp, cp,
                                           req_start, req_end);
        ASSERT_EQ(versions, (Versions {{5, INT64_MAX - 1}})); // [5, max] v [8, 12]
    }
    // * for any BC happended
    // req_bc_cnt < ms_bc_cnt
    // BE  [=][=][=][=][=====][=][=]<.......>
    //                  ^~~~~ req_cp
    // MS  [xxxxxxxxxx][xxxxxxxxxxxxxx][=======][=][=]
    //                                  ^~~~~~~ ms_cp
    //     ^_________________________^ versions_return: [0, ms_cp - 1] v versions_return_in_above_case
    {
        auto [req_bc_cnt, bc_cnt] = std::tuple {0, 1};
        auto [req_cc_cnt, cc_cnt] = std::tuple {1, 1};
        auto [req_cp, cp] = std::tuple {5, 5};
        auto [req_start, req_end] = std::tuple {8, 12};
        auto versions = calc_sync_versions(req_bc_cnt, bc_cnt, req_cc_cnt, cc_cnt, req_cp, cp,
                                           req_start, req_end);
        ASSERT_EQ(versions, (Versions {{0, 4}, {8, 12}}));
    }
    {
        auto [req_bc_cnt, bc_cnt] = std::tuple {0, 1};
        auto [req_cc_cnt, cc_cnt] = std::tuple {1, 1};
        auto [req_cp, cp] = std::tuple {8, 8};
        auto [req_start, req_end] = std::tuple {8, 12};
        auto versions = calc_sync_versions(req_bc_cnt, bc_cnt, req_cc_cnt, cc_cnt, req_cp, cp,
                                           req_start, req_end);
        ASSERT_EQ(versions, (Versions {{0, 12}})); // [0, 7] v [8, 12]
    }
    {
        auto [req_bc_cnt, bc_cnt] = std::tuple {0, 1};
        auto [req_cc_cnt, cc_cnt] = std::tuple {1, 2};
        auto [req_cp, cp] = std::tuple {5, 10};
        auto [req_start, req_end] = std::tuple {8, 12};
        auto versions = calc_sync_versions(req_bc_cnt, bc_cnt, req_cc_cnt, cc_cnt, req_cp, cp,
                                           req_start, req_end);
        ASSERT_EQ(versions, (Versions {{0, 12}})); // [0, 4] v [5, 9] v [8, 12]
    }
    {
        auto [req_bc_cnt, bc_cnt] = std::tuple {0, 1};
        auto [req_cc_cnt, cc_cnt] = std::tuple {1, 2};
        auto [req_cp, cp] = std::tuple {5, 15};
        auto [req_start, req_end] = std::tuple {8, 12};
        auto versions = calc_sync_versions(req_bc_cnt, bc_cnt, req_cc_cnt, cc_cnt, req_cp, cp,
                                           req_start, req_end);
        ASSERT_EQ(versions, (Versions {{0, 14}})); // [0, 4] v [5, 14] v [8, 12]
    }
    {
        auto [req_bc_cnt, bc_cnt] = std::tuple {0, 1};
        auto [req_cc_cnt, cc_cnt] = std::tuple {1, 2};
        auto [req_cp, cp] = std::tuple {5, 5};
        auto [req_start, req_end] = std::tuple {8, 12};
        auto versions = calc_sync_versions(req_bc_cnt, bc_cnt, req_cc_cnt, cc_cnt, req_cp, cp,
                                           req_start, req_end);
        // [0, 4] v [5, max] v [8, 12]
        ASSERT_EQ(versions, (Versions {{0, INT64_MAX - 1}}));
    }
}

TEST(MetaServiceTest, StageTest) {
    auto meta_service = get_meta_service();
    brpc::Controller cntl;
    auto cloud_unique_id = "test_cloud_unique_id";
    std::string instance_id = "stage_test_instance_id";
    [[maybe_unused]] auto sp = SyncPoint::get_instance();
    DORIS_CLOUD_DEFER {
        SyncPoint::get_instance()->clear_all_call_backs();
    };
    sp->set_call_back("get_instance_id", [&](auto&& args) {
        auto* ret = try_any_cast_ret<std::string>(args);
        ret->first = instance_id;
        ret->second = true;
    });
    sp->set_call_back("encrypt_ak_sk:get_encryption_key", [](auto&& args) {
        auto* ret = try_any_cast<int*>(args[0]);
        *ret = 0;
        auto* key = try_any_cast<std::string*>(args[1]);
        *key = "test";
        auto* key_id = try_any_cast<int64_t*>(args[2]);
        *key_id = 1;
    });
    sp->set_call_back("decrypt_ak_sk:get_encryption_key", [](auto&& args) {
        auto* key = try_any_cast<std::string*>(args[0]);
        *key = "test";
        auto* ret = try_any_cast<int*>(args[1]);
        *ret = 0;
    });
    sp->enable_processing();

    ObjectStoreInfoPB obj;
    obj.set_ak("123");
    obj.set_sk("321");
    obj.set_bucket("456");
    obj.set_prefix("654");
    obj.set_endpoint("789");
    obj.set_region("987");
    obj.set_external_endpoint("888");
    obj.set_provider(ObjectStoreInfoPB::BOS);

    RamUserPB ram_user;
    ram_user.set_user_id("test_user_id");
    ram_user.set_ak("test_ak");
    ram_user.set_sk("test_sk");
    EncryptionInfoPB encry_info;
    encry_info.set_encryption_method("encry_method_test");
    encry_info.set_key_id(1111);
    ram_user.mutable_encryption_info()->CopyFrom(encry_info);

    // create instance
    {
        CreateInstanceRequest req;
        req.set_instance_id(instance_id);
        req.set_user_id("test_user");
        req.set_name("test_name");
        req.mutable_ram_user()->CopyFrom(ram_user);
        req.mutable_obj_info()->CopyFrom(obj);

        CreateInstanceResponse res;
        meta_service->create_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // test create and get internal stage
    {
        // get a non-existent internal stage
        GetStageRequest get_stage_req;
        GetStageResponse res;
        // no cloud_unique_id
        meta_service->get_stage(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                &get_stage_req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);

        get_stage_req.set_cloud_unique_id(cloud_unique_id);
        // no instance_id
        sp->set_call_back("get_instance_id", [&](auto&& args) {
            auto* ret = try_any_cast_ret<std::string>(args);
            ret->first = "";
            ret->second = true;
        });
        meta_service->get_stage(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                &get_stage_req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
        sp->clear_call_back("get_instance_id");
        sp->set_call_back("get_instance_id", [&](auto&& args) {
            auto* ret = try_any_cast_ret<std::string>(args);
            ret->first = instance_id;
            ret->second = true;
        });

        // no stage type
        meta_service->get_stage(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                &get_stage_req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
        get_stage_req.set_type(StagePB::INTERNAL);

        // no internal stage user name
        meta_service->get_stage(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                &get_stage_req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
        get_stage_req.set_mysql_user_name("root");

        // no internal stage user id
        meta_service->get_stage(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                &get_stage_req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
        get_stage_req.set_mysql_user_id("root_id");
        meta_service->get_stage(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                &get_stage_req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::STAGE_NOT_FOUND);

        // create an internal stage
        CreateStageRequest create_stage_request;
        StagePB stage;
        stage.set_type(StagePB::INTERNAL);
        stage.add_mysql_user_name("root");
        stage.add_mysql_user_id("root_id");
        stage.set_stage_id("internal_stage_id");
        create_stage_request.set_cloud_unique_id(cloud_unique_id);
        create_stage_request.mutable_stage()->CopyFrom(stage);
        CreateStageResponse create_stage_response;
        meta_service->create_stage(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                   &create_stage_request, &create_stage_response, nullptr);
        ASSERT_EQ(create_stage_response.status().code(), MetaServiceCode::OK);

        // get existent internal stage
        GetStageResponse res2;
        meta_service->get_stage(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                &get_stage_req, &res2, nullptr);
        ASSERT_EQ(res2.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(1, res2.stage().size());

        // can't find user id's stage
        GetStageResponse res3;
        get_stage_req.set_mysql_user_id("not_root_id_exist");
        meta_service->get_stage(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                &get_stage_req, &res3, nullptr);
        ASSERT_EQ(res3.status().code(), MetaServiceCode::STATE_ALREADY_EXISTED_FOR_USER);
        ASSERT_EQ(1, res3.stage().size());

        // drop internal stage
        DropStageRequest drop_stage_request;
        drop_stage_request.set_cloud_unique_id(cloud_unique_id);
        drop_stage_request.set_type(StagePB::INTERNAL);
        drop_stage_request.set_mysql_user_id("root_id");
        drop_stage_request.set_reason("Drop");
        DropStageResponse drop_stage_response;
        meta_service->drop_stage(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                 &drop_stage_request, &drop_stage_response, nullptr);
        ASSERT_EQ(drop_stage_response.status().code(), MetaServiceCode::OK);
        // scan fdb has recycle_stage key
        {
            RecycleStageKeyInfo key_info0 {instance_id, ""};
            RecycleStageKeyInfo key_info1 {instance_id, "{"};
            std::string key0;
            std::string key1;
            recycle_stage_key(key_info0, &key0);
            recycle_stage_key(key_info1, &key1);
            std::unique_ptr<Transaction> txn;
            std::string get_val;
            ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
            std::unique_ptr<RangeGetIterator> it;
            ASSERT_EQ(txn->get(key0, key1, &it), TxnErrorCode::TXN_OK);
            int stage_cnt = 0;
            do {
                ASSERT_EQ(txn->get(key0, key1, &it), TxnErrorCode::TXN_OK);
                while (it->has_next()) {
                    auto [k, v] = it->next();
                    ++stage_cnt;
                    if (!it->has_next()) {
                        key0 = k;
                    }
                }
                key0.push_back('\x00');
            } while (it->more());
            ASSERT_EQ(stage_cnt, 1);
        }

        // get internal stage
        meta_service->get_stage(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                &get_stage_req, &res2, nullptr);
        ASSERT_EQ(res2.status().code(), MetaServiceCode::STAGE_NOT_FOUND);

        // drop a non-exist internal stage
        drop_stage_request.set_mysql_user_id("root_id2");
        meta_service->drop_stage(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                 &drop_stage_request, &drop_stage_response, nullptr);
        ASSERT_EQ(drop_stage_response.status().code(), MetaServiceCode::STAGE_NOT_FOUND);
    }

    // test create and get external stage
    {
        // get an external stage with name
        GetStageRequest get_stage_req;
        get_stage_req.set_cloud_unique_id(cloud_unique_id);
        get_stage_req.set_type(StagePB::EXTERNAL);
        get_stage_req.set_stage_name("ex_name_1");

        {
            GetStageResponse res;
            meta_service->get_stage(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &get_stage_req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::STAGE_NOT_FOUND);
        }

        // create 4 stages
        for (auto i = 0; i < 4; ++i) {
            StagePB stage;
            stage.set_type(StagePB::EXTERNAL);
            stage.set_stage_id("ex_id_" + std::to_string(i));
            stage.set_name("ex_name_" + std::to_string(i));
            if (i == 2) {
                stage.set_access_type(StagePB::BUCKET_ACL);
            } else if (i == 3) {
                stage.set_access_type(StagePB::IAM);
            }
            stage.mutable_obj_info()->CopyFrom(obj);

            CreateStageRequest create_stage_req;
            create_stage_req.set_cloud_unique_id(cloud_unique_id);
            create_stage_req.mutable_stage()->CopyFrom(stage);

            CreateStageResponse create_stage_res;
            meta_service->create_stage(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                       &create_stage_req, &create_stage_res, nullptr);
            ASSERT_EQ(create_stage_res.status().code(), MetaServiceCode::OK);
        }

        // stages number bigger than config
        {
            config::max_num_stages = 4;
            StagePB stage;
            stage.set_type(StagePB::INTERNAL);
            stage.add_mysql_user_name("root1");
            stage.add_mysql_user_id("root_id1");
            stage.set_stage_id("internal_stage_id1");
            CreateStageRequest create_stage_req;
            create_stage_req.set_cloud_unique_id(cloud_unique_id);
            create_stage_req.mutable_stage()->CopyFrom(stage);

            CreateStageResponse create_stage_res;
            meta_service->create_stage(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                       &create_stage_req, &create_stage_res, nullptr);
            ASSERT_EQ(create_stage_res.status().code(), MetaServiceCode::UNDEFINED_ERR);
        }

        // get an external stage with name
        {
            GetStageResponse res;
            meta_service->get_stage(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &get_stage_req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            ASSERT_EQ(1, res.stage().size());
            ASSERT_EQ("ex_id_1", res.stage().at(0).stage_id());
        }

        // get an external stage with name, type StagePB::BUCKET_ACL
        {
            get_stage_req.set_stage_name("ex_name_2");
            GetStageResponse res;
            meta_service->get_stage(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &get_stage_req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            ASSERT_EQ(1, res.stage().size());
            ASSERT_EQ("ex_id_2", res.stage().at(0).stage_id());
        }

        // get an external stage with name, type StagePB::IAM
        {
            GetStageResponse res;
            get_stage_req.set_stage_name("ex_name_3");
            meta_service->get_stage(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &get_stage_req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            ASSERT_EQ(1, res.stage().size());
            ASSERT_EQ("ex_id_3", res.stage().at(0).stage_id());

            GetStageResponse res1;
            std::unique_ptr<Transaction> txn;
            TxnErrorCode err = meta_service->txn_kv()->create_txn(&txn);
            ASSERT_EQ(err, TxnErrorCode::TXN_OK);

            RamUserPB iam_user;
            txn->put(system_meta_service_arn_info_key(), iam_user.SerializeAsString());
            err = txn->commit();
            ASSERT_EQ(err, TxnErrorCode::TXN_OK);
            LOG_INFO("err=", err);
            meta_service->get_stage(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &get_stage_req, &res1, nullptr);
            ASSERT_EQ(res1.status().code(), MetaServiceCode::OK);
            ASSERT_EQ(1, res1.stage().size());
            ASSERT_EQ("ex_id_3", res1.stage().at(0).stage_id());
        }

        GetStageRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_type(StagePB::EXTERNAL);
        // get all stages
        {
            GetStageResponse res;
            meta_service->get_stage(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            ASSERT_EQ(4, res.stage().size());
            ASSERT_EQ("ex_id_0", res.stage().at(0).stage_id());
            ASSERT_EQ("ex_id_1", res.stage().at(1).stage_id());
        }

        // drop one stage
        {
            DropStageRequest drop_stage_req;
            drop_stage_req.set_cloud_unique_id(cloud_unique_id);
            drop_stage_req.set_type(StagePB::EXTERNAL);
            drop_stage_req.set_stage_name("tmp");
            DropStageResponse res;
            meta_service->drop_stage(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &drop_stage_req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::STAGE_NOT_FOUND);

            drop_stage_req.set_stage_name("ex_name_1");
            meta_service->drop_stage(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &drop_stage_req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

            // get all stage
            GetStageResponse get_stage_res;
            meta_service->get_stage(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &get_stage_res, nullptr);
            ASSERT_EQ(get_stage_res.status().code(), MetaServiceCode::OK);
            ASSERT_EQ(3, get_stage_res.stage().size());
            ASSERT_EQ("ex_name_0", get_stage_res.stage().at(0).name());
        }
    }
    sp->clear_all_call_backs();
    sp->clear_trace();
    sp->disable_processing();
}

TEST(MetaServiceTest, GetIamTest) {
    auto meta_service = get_meta_service();
    brpc::Controller cntl;
    auto cloud_unique_id = "test_cloud_unique_id";
    std::string instance_id = "get_iam_test_instance_id";
    [[maybe_unused]] auto sp = SyncPoint::get_instance();
    DORIS_CLOUD_DEFER {
        SyncPoint::get_instance()->clear_all_call_backs();
    };
    sp->set_call_back("get_instance_id", [&](auto&& args) {
        auto* ret = try_any_cast_ret<std::string>(args);
        ret->first = instance_id;
        ret->second = true;
    });
    sp->set_call_back("encrypt_ak_sk:get_encryption_key", [](auto&& args) {
        auto* ret = try_any_cast<int*>(args[0]);
        *ret = 0;
        auto* key = try_any_cast<std::string*>(args[1]);
        *key = "test";
        auto* key_id = try_any_cast<int64_t*>(args[2]);
        *key_id = 1;
    });
    sp->set_call_back("decrypt_ak_sk:get_encryption_key", [](auto&& args) {
        auto* key = try_any_cast<std::string*>(args[0]);
        *key = "test";
        auto* ret = try_any_cast<int*>(args[1]);
        *ret = 0;
    });
    sp->enable_processing();

    config::arn_id = "iam_arn";
    config::arn_ak = "iam_ak";
    config::arn_sk = "iam_sk";

    // create instance
    {
        ObjectStoreInfoPB obj;
        obj.set_ak("123");
        obj.set_sk("321");
        obj.set_bucket("456");
        obj.set_prefix("654");
        obj.set_endpoint("789");
        obj.set_region("987");
        obj.set_external_endpoint("888");
        obj.set_provider(ObjectStoreInfoPB::BOS);

        RamUserPB ram_user;
        ram_user.set_user_id("test_user_id");
        ram_user.set_ak("test_ak");
        ram_user.set_sk("test_sk");

        CreateInstanceRequest req;
        req.set_instance_id(instance_id);
        req.set_user_id("test_user");
        req.set_name("test_name");
        req.mutable_ram_user()->CopyFrom(ram_user);
        req.mutable_obj_info()->CopyFrom(obj);

        CreateInstanceResponse res;
        meta_service->create_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    GetIamRequest request;
    request.set_cloud_unique_id(cloud_unique_id);
    GetIamResponse response;
    meta_service->get_iam(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &request,
                          &response, nullptr);
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(response.ram_user().user_id(), "test_user_id");
    ASSERT_EQ(response.ram_user().ak(), "test_ak");
    ASSERT_EQ(response.ram_user().sk(), "test_sk");
    ASSERT_TRUE(response.ram_user().external_id().empty());

    ASSERT_EQ(response.iam_user().user_id(), "iam_arn");
    ASSERT_EQ(response.iam_user().external_id(), instance_id);
    ASSERT_EQ(response.iam_user().ak(), "iam_ak");
    ASSERT_EQ(response.iam_user().sk(), "iam_sk");
    sp->clear_all_call_backs();
    sp->clear_trace();
    sp->disable_processing();
}

TEST(MetaServiceTest, AlterRamTest) {
    auto meta_service = get_meta_service();
    brpc::Controller cntl;
    auto cloud_unique_id = "test_cloud_unique_id";
    std::string instance_id = "alter_iam_test_instance_id";
    [[maybe_unused]] auto sp = SyncPoint::get_instance();
    DORIS_CLOUD_DEFER {
        SyncPoint::get_instance()->clear_all_call_backs();
    };
    sp->set_call_back("get_instance_id", [&](auto&& args) {
        auto* ret = try_any_cast_ret<std::string>(args);
        ret->first = instance_id;
        ret->second = true;
    });
    sp->set_call_back("encrypt_ak_sk:get_encryption_key", [](auto&& args) {
        auto* ret = try_any_cast<int*>(args[0]);
        *ret = 0;
        auto* key = try_any_cast<std::string*>(args[1]);
        *key = "test";
        auto* key_id = try_any_cast<int64_t*>(args[2]);
        *key_id = 1;
    });
    sp->set_call_back("decrypt_ak_sk:get_encryption_key", [](auto&& args) {
        auto* key = try_any_cast<std::string*>(args[0]);
        *key = "test";
        auto* ret = try_any_cast<int*>(args[1]);
        *ret = 0;
    });
    sp->enable_processing();

    config::arn_id = "iam_arn";
    config::arn_ak = "iam_ak";
    config::arn_sk = "iam_sk";

    ObjectStoreInfoPB obj;
    obj.set_ak("123");
    obj.set_sk("321");
    obj.set_bucket("456");
    obj.set_prefix("654");
    obj.set_endpoint("789");
    obj.set_region("987");
    obj.set_external_endpoint("888");
    obj.set_provider(ObjectStoreInfoPB::BOS);

    // create instance without ram user
    CreateInstanceRequest create_instance_req;
    create_instance_req.set_instance_id(instance_id);
    create_instance_req.set_user_id("test_user");
    create_instance_req.set_name("test_name");
    create_instance_req.mutable_obj_info()->CopyFrom(obj);
    CreateInstanceResponse create_instance_res;
    meta_service->create_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                  &create_instance_req, &create_instance_res, nullptr);
    ASSERT_EQ(create_instance_res.status().code(), MetaServiceCode::OK);

    // get iam and ram user
    GetIamRequest request;
    request.set_cloud_unique_id(cloud_unique_id);
    GetIamResponse response;
    meta_service->get_iam(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &request,
                          &response, nullptr);
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(response.has_ram_user(), false);
    ASSERT_EQ(response.iam_user().user_id(), "iam_arn");
    ASSERT_EQ(response.iam_user().ak(), "iam_ak");
    ASSERT_EQ(response.iam_user().sk(), "iam_sk");

    // alter ram user
    RamUserPB ram_user;
    ram_user.set_user_id("test_user_id");
    ram_user.set_ak("test_ak");
    ram_user.set_sk("test_sk");
    AlterRamUserRequest alter_ram_user_request;
    alter_ram_user_request.set_instance_id(instance_id);
    alter_ram_user_request.mutable_ram_user()->CopyFrom(ram_user);
    AlterRamUserResponse alter_ram_user_response;
    meta_service->alter_ram_user(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                 &alter_ram_user_request, &alter_ram_user_response, nullptr);

    // get iam and ram user
    meta_service->get_iam(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &request,
                          &response, nullptr);
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(response.has_ram_user(), true);
    ASSERT_EQ(response.ram_user().user_id(), "test_user_id");
    ASSERT_EQ(response.ram_user().ak(), "test_ak");
    ASSERT_EQ(response.ram_user().sk(), "test_sk");
    sp->clear_all_call_backs();
    sp->clear_trace();
    sp->disable_processing();
}

std::string to_raw_string(std::string_view v) {
    std::string ret;
    ret.reserve(v.size() / 1.5);
    while (!v.empty()) {
        if (v[0] == '\\') {
            if (v[1] == 'x') {
                ret.push_back(unhex(std::string_view {v.data() + 2, 2})[0]);
                v.remove_prefix(4);
            } else if (v[1] == '\\') {
                ret.push_back('\\');
                v.remove_prefix(2);
            } else {
                std::abort();
            }
            continue;
        }
        ret.push_back(v[0]);
        v.remove_prefix(1);
    }
    return ret;
}

TEST(MetaServiceTest, DecodeTest) {
    // 504
    std::string v1 =
            R"(\x08\x00\x10\xa0[\x18\xb3[ \xde\xc5\xa4\x8e\xbd\xf0\x97\xc62(\xf4\x96\xe6\xb0\x070\x018\x02@\x02H\x0bX\x05`\xa0\x07h\xa0\x07p\xa0\x01\x88\x01\x00\xa0\x01\x86\x8b\x9a\x9b\x06\xaa\x01\x16\x08\xe6\x9e\x91\xa3\xfb\xbe\xf5\xf0\xc4\x01\x10\xfe\x8b\x90\xa7\xb5\xec\xd5\xc8\xbf\x01\xb0\x01\x01\xba\x0100200000000000071fb4aabb58c570cbcadb10857d3131b97\xc2\x01\x011\xc8\x01\x84\x8b\x9a\x9b\x06\xd0\x01\x85\x8b\x9a\x9b\x06\xda\x01\x04\x0a\x00\x12\x00\xe2\x01\xcd\x02\x08\x02\x121\x08\x00\x12\x06datek1\x1a\x04DATE \x01*\x04NONE0\x01:\x0a2022-01-01@\x00H\x00P\x03X\x03\x80\x01\x01\x12>\x08\x01\x12\x06datek2\x1a\x08DATETIME \x01*\x04NONE0\x01:\x132022-01-01 11:11:11@\x00H\x00P\x08X\x08\x80\x01\x01\x123\x08\x04\x12\x06datev3\x1a\x06DATEV2 \x01*\x04NONE0\x01:\x0a2022-01-01@\x00H\x00P\x04X\x04\x80\x01\x01\x120\x08\x02\x12\x06datev1\x1a\x04DATE \x00*\x03MAX0\x01:\x0a2022-01-01@\x00H\x00P\x03X\x03\x80\x01\x01\x12=\x08\x03\x12\x06datev2\x1a\x08DATETIME \x00*\x03MAX0\x01:\x132022-01-01 11:11:11@\x00H\x00P\x08X\x08\x80\x01\x01\x18\x03 \x80\x08(\x021\x00\x00\x00\x00\x00\x00\x00\x008\x00@\xff\xff\xff\xff\xff\xff\xff\xff\xff\x01H\xff\xff\xff\xff\xff\xff\xff\xff\xff\x01P\x00X\x02`\x05h\x00p\x00\xe8\x01\x85\xae\x9f\x9b\x06\x98\x03\x02)";
    std::string val1 = to_raw_string(v1);
    std::cout << "val1 size " << val1.size() << std::endl;

    // 525
    std::string v2 =
            R"(\x08\x00\x10\xa0[\x18\xb3[ \x80\xb0\x85\xe3\xda\xcc\x8c\x0f(\xf4\x96\xe6\xb0\x070\x018\x01@\x0cH\x0cX\x00`\x00h\x00p\x00\x82\x01\x1e\x08\xff\xff\xff\xff\xff\xff\xff\xff\xff\x01\x12\x11datev3=2022-01-01\x88\x01\x01\x92\x01\x04\x08\x00\x10\x00\xa0\x01\x87\x8b\x9a\x9b\x06\xaa\x01\x16\x08\xe6\x9e\x91\xa3\xfb\xbe\xf5\xf0\xc4\x01\x10\xfe\x8b\x90\xa7\xb5\xec\xd5\xc8\xbf\x01\xb0\x01\x00\xba\x0100200000000000072fb4aabb58c570cbcadb10857d3131b97\xc8\x01\x87\x8b\x9a\x9b\x06\xd0\x01\x87\x8b\x9a\x9b\x06\xe2\x01\xcd\x02\x08\x02\x121\x08\x00\x12\x06datek1\x1a\x04DATE \x01*\x04NONE0\x01:\x0a2022-01-01@\x00H\x00P\x03X\x03\x80\x01\x01\x12>\x08\x01\x12\x06datek2\x1a\x08DATETIME \x01*\x04NONE0\x01:\x132022-01-01 11:11:11@\x00H\x00P\x08X\x08\x80\x01\x01\x123\x08\x04\x12\x06datev3\x1a\x06DATEV2 \x01*\x04NONE0\x01:\x0a2022-01-01@\x00H\x00P\x04X\x04\x80\x01\x01\x120\x08\x02\x12\x06datev1\x1a\x04DATE \x00*\x03MAX0\x01:\x0a2022-01-01@\x00H\x00P\x03X\x03\x80\x01\x01\x12=\x08\x03\x12\x06datev2\x1a\x08DATETIME \x00*\x03MAX0\x01:\x132022-01-01 11:11:11@\x00H\x00P\x08X\x08\x80\x01\x01\x18\x03 \x80\x08(\x021\x00\x00\x00\x00\x00\x00\x00\x008\x00@\xff\xff\xff\xff\xff\xff\xff\xff\xff\x01H\xff\xff\xff\xff\xff\xff\xff\xff\xff\x01P\x00X\x02`\x05h\x00p\x00\xe8\x01\x00\x98\x03\x02)";
    std::string val2 = to_raw_string(v2);
    std::cout << "val2 size " << val2.size() << std::endl;

    [[maybe_unused]] std::string key1(
            "\x01\x10meta\x00\x01\x10selectdb-cloud-"
            "dev\x00\x01\x10rowset\x00\x01\x12\x00\x00\x00\x00\x00\x00-"
            "\xb3\x12\x00\x00\x00\x00\x00\x00\x00\x0b",
            56);
    [[maybe_unused]] std::string key2(
            "\x01\x10meta\x00\x01\x10selectdb-cloud-"
            "dev\x00\x01\x10rowset\x00\x01\x12\x00\x00\x00\x00\x00\x00-"
            "\xb3\x12\x00\x00\x00\x00\x00\x00\x00\x0c",
            56);
    std::cout << "key1 " << key1.size() << " " << hex(key1) << std::endl;
    std::cout << "key2 " << key2.size() << " " << hex(key2) << std::endl;

    doris::RowsetMetaCloudPB rowset1;
    doris::RowsetMetaCloudPB rowset2;

    rowset1.ParseFromString(val1);
    rowset2.ParseFromString(val2);
    std::cout << "rowset1=" << proto_to_json(rowset1) << std::endl;
    std::cout << "rowset2=" << proto_to_json(rowset2) << std::endl;
}

static void get_tablet_stats(MetaServiceProxy* meta_service, int64_t table_id, int64_t index_id,
                             int64_t partition_id, int64_t tablet_id, GetTabletStatsResponse& res) {
    brpc::Controller cntl;
    GetTabletStatsRequest req;
    auto idx = req.add_tablet_idx();
    idx->set_table_id(table_id);
    idx->set_index_id(index_id);
    idx->set_partition_id(partition_id);
    idx->set_tablet_id(tablet_id);
    meta_service->get_tablet_stats(&cntl, &req, &res, nullptr);
}

TEST(MetaServiceTest, UpdateTablet) {
    auto meta_service = get_meta_service();
    std::string cloud_unique_id = "test_cloud_unique_id";
    constexpr auto table_id = 11231, index_id = 11232, partition_id = 11233, tablet_id1 = 11234,
                   tablet_id2 = 21234;
    ASSERT_NO_FATAL_FAILURE(
            create_tablet(meta_service.get(), table_id, index_id, partition_id, tablet_id1));
    ASSERT_NO_FATAL_FAILURE(
            create_tablet(meta_service.get(), table_id, index_id, partition_id, tablet_id2));
    auto get_and_check_tablet_meta = [&](int tablet_id, int64_t ttl_seconds, bool in_memory,
                                         bool is_persistent) {
        brpc::Controller cntl;
        GetTabletRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_tablet_id(tablet_id);
        GetTabletResponse resp;
        meta_service->get_tablet(&cntl, &req, &resp, nullptr);
        ASSERT_EQ(resp.status().code(), MetaServiceCode::OK) << tablet_id;
        EXPECT_EQ(resp.tablet_meta().ttl_seconds(), ttl_seconds);
        EXPECT_EQ(resp.tablet_meta().is_in_memory(), in_memory);
        EXPECT_EQ(resp.tablet_meta().is_persistent(), is_persistent);
    };
    get_and_check_tablet_meta(tablet_id1, 0, false, false);
    get_and_check_tablet_meta(tablet_id2, 0, false, false);
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
    get_and_check_tablet_meta(tablet_id1, 300, false, false);
    get_and_check_tablet_meta(tablet_id2, 3000, false, false);
    {
        brpc::Controller cntl;
        UpdateTabletRequest req;
        UpdateTabletResponse resp;
        req.set_cloud_unique_id(cloud_unique_id);
        TabletMetaInfoPB* tablet_meta_info = req.add_tablet_meta_infos();
        tablet_meta_info->set_tablet_id(tablet_id1);
        tablet_meta_info->set_is_in_memory(true);
        meta_service->update_tablet(&cntl, &req, &resp, nullptr);
        ASSERT_EQ(resp.status().code(), MetaServiceCode::OK);
    }
    {
        brpc::Controller cntl;
        UpdateTabletRequest req;
        UpdateTabletResponse resp;
        req.set_cloud_unique_id(cloud_unique_id);
        TabletMetaInfoPB* tablet_meta_info = req.add_tablet_meta_infos();
        tablet_meta_info->set_tablet_id(tablet_id1);
        tablet_meta_info->set_is_persistent(true);
        meta_service->update_tablet(&cntl, &req, &resp, nullptr);
        ASSERT_EQ(resp.status().code(), MetaServiceCode::OK);
    }
    get_and_check_tablet_meta(tablet_id1, 300, true, true);
}

TEST(MetaServiceTest, GetTabletStatsTest) {
    auto meta_service = get_meta_service();

    constexpr auto table_id = 10001, index_id = 10002, partition_id = 10003, tablet_id = 10004;
    ASSERT_NO_FATAL_FAILURE(
            create_tablet(meta_service.get(), table_id, index_id, partition_id, tablet_id));
    GetTabletStatsResponse res;
    get_tablet_stats(meta_service.get(), table_id, index_id, partition_id, tablet_id, res);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(res.tablet_stats_size(), 1);
    EXPECT_EQ(res.tablet_stats(0).data_size(), 0);
    EXPECT_EQ(res.tablet_stats(0).num_rows(), 0);
    EXPECT_EQ(res.tablet_stats(0).num_rowsets(), 1);
    EXPECT_EQ(res.tablet_stats(0).num_segments(), 0);
    EXPECT_EQ(res.tablet_stats(0).index_size(), 0);
    EXPECT_EQ(res.tablet_stats(0).segment_size(), 0);
    // Insert rowset
    config::split_tablet_stats = false;
    ASSERT_NO_FATAL_FAILURE(
            insert_rowset(meta_service.get(), 10000, "label1", table_id, partition_id, tablet_id));
    ASSERT_NO_FATAL_FAILURE(
            insert_rowset(meta_service.get(), 10000, "label2", table_id, partition_id, tablet_id));
    config::split_tablet_stats = true;
    ASSERT_NO_FATAL_FAILURE(
            insert_rowset(meta_service.get(), 10000, "label3", table_id, partition_id, tablet_id));
    ASSERT_NO_FATAL_FAILURE(
            insert_rowset(meta_service.get(), 10000, "label4", table_id, partition_id, tablet_id));
    // Check tablet stats kv
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string data_size_key, data_size_val;
    stats_tablet_data_size_key({mock_instance, table_id, index_id, partition_id, tablet_id},
                               &data_size_key);
    ASSERT_EQ(txn->get(data_size_key, &data_size_val), TxnErrorCode::TXN_OK);
    EXPECT_EQ(*(int64_t*)data_size_val.data(), 22000);
    std::string index_size_key, index_size_val;
    stats_tablet_index_size_key({mock_instance, table_id, index_id, partition_id, tablet_id},
                                &index_size_key);
    ASSERT_EQ(txn->get(index_size_key, &index_size_val), TxnErrorCode::TXN_OK);
    EXPECT_EQ(*(int64_t*)index_size_val.data(), 2000);
    std::string segment_size_key, segment_size_val;
    stats_tablet_segment_size_key({mock_instance, table_id, index_id, partition_id, tablet_id},
                                  &segment_size_key);
    ASSERT_EQ(txn->get(segment_size_key, &segment_size_val), TxnErrorCode::TXN_OK);
    EXPECT_EQ(*(int64_t*)segment_size_val.data(), 20000);
    std::string num_rows_key, num_rows_val;
    stats_tablet_num_rows_key({mock_instance, table_id, index_id, partition_id, tablet_id},
                              &num_rows_key);
    ASSERT_EQ(txn->get(num_rows_key, &num_rows_val), TxnErrorCode::TXN_OK);
    EXPECT_EQ(*(int64_t*)num_rows_val.data(), 200);
    std::string num_rowsets_key, num_rowsets_val;
    stats_tablet_num_rowsets_key({mock_instance, table_id, index_id, partition_id, tablet_id},
                                 &num_rowsets_key);
    ASSERT_EQ(txn->get(num_rowsets_key, &num_rowsets_val), TxnErrorCode::TXN_OK);
    EXPECT_EQ(*(int64_t*)num_rowsets_val.data(), 2);
    std::string num_segs_key, num_segs_val;
    stats_tablet_num_segs_key({mock_instance, table_id, index_id, partition_id, tablet_id},
                              &num_segs_key);
    ASSERT_EQ(txn->get(num_segs_key, &num_segs_val), TxnErrorCode::TXN_OK);
    EXPECT_EQ(*(int64_t*)num_segs_val.data(), 2);
    // Get tablet stats
    res.Clear();
    get_tablet_stats(meta_service.get(), table_id, index_id, partition_id, tablet_id, res);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(res.tablet_stats_size(), 1);
    EXPECT_EQ(res.tablet_stats(0).data_size(), 44000);
    EXPECT_EQ(res.tablet_stats(0).num_rows(), 400);
    EXPECT_EQ(res.tablet_stats(0).num_rowsets(), 5);
    EXPECT_EQ(res.tablet_stats(0).num_segments(), 4);
    EXPECT_EQ(res.tablet_stats(0).index_size(), 4000);
    EXPECT_EQ(res.tablet_stats(0).segment_size(), 40000);
}

void remove_delete_bitmap_lock(MetaServiceProxy* meta_service, int64_t table_id) {
    std::string lock_key = meta_delete_bitmap_update_lock_key({"test_instance", table_id, -1});
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->remove(lock_key);
    std::string tablet_job_key_begin = mow_tablet_job_key({"test_instance", table_id, 0});
    std::string tablet_job_key_end = mow_tablet_job_key({"test_instance", table_id, INT64_MAX});
    txn->remove(tablet_job_key_begin, tablet_job_key_end);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
}

void testGetDeleteBitmapUpdateLock(int lock_version, int job_lock_id) {
    config::delete_bitmap_lock_v2_white_list = lock_version == 1 ? "" : "*";
    auto meta_service = get_meta_service();
    [[maybe_unused]] auto sp = SyncPoint::get_instance();
    DORIS_CLOUD_DEFER {
        SyncPoint::get_instance()->clear_all_call_backs();
    };
    remove_delete_bitmap_lock(meta_service.get(), 1);
    remove_delete_bitmap_lock(meta_service.get(), 2);
    int64_t table_id = 9;

    // case 1: lock key does not exist, get and remove load lock
    brpc::Controller cntl;
    GetDeleteBitmapUpdateLockRequest req;
    GetDeleteBitmapUpdateLockResponse res;
    req.set_cloud_unique_id("test_cloud_unique_id");
    req.set_table_id(table_id);
    req.add_partition_ids(123);
    req.set_expiration(5);
    req.set_lock_id(888);
    req.set_initiator(-1);
    meta_service->get_delete_bitmap_update_lock(
            reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

    RemoveDeleteBitmapUpdateLockRequest remove_req;
    RemoveDeleteBitmapUpdateLockResponse remove_res;
    remove_req.set_cloud_unique_id("test_cloud_unique_id");
    remove_req.set_table_id(table_id);
    remove_req.set_lock_id(888);
    remove_req.set_initiator(-1);
    meta_service->remove_delete_bitmap_update_lock(
            reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &remove_req, &remove_res,
            nullptr);
    ASSERT_EQ(remove_res.status().code(), MetaServiceCode::OK);

    // case 2: lock key does not exist, get and remove compaction lock
    req.add_partition_ids(123);
    req.set_expiration(600);
    req.set_lock_id(job_lock_id);
    req.set_initiator(100);
    meta_service->get_delete_bitmap_update_lock(
            reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

    remove_req.set_tablet_id(2);
    remove_req.set_lock_id(job_lock_id);
    remove_req.set_initiator(100);
    meta_service->remove_delete_bitmap_update_lock(
            reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &remove_req, &remove_res,
            nullptr);
    ASSERT_EQ(remove_res.status().code(), MetaServiceCode::OK);

    // case 3: lock key owned by load1, load2 get lock
    req.add_partition_ids(123);
    req.set_expiration(600);
    req.set_lock_id(888);
    req.set_initiator(-1);
    meta_service->get_delete_bitmap_update_lock(
            reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

    req.set_lock_id(889);
    meta_service->get_delete_bitmap_update_lock(
            reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::LOCK_CONFLICT);

    // case 4: lock key owned by load1, compaction1 get lock
    req.set_lock_id(job_lock_id);
    req.set_initiator(100);
    meta_service->get_delete_bitmap_update_lock(
            reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::LOCK_CONFLICT);

    remove_req.set_tablet_id(2);
    remove_req.set_lock_id(888);
    remove_req.set_initiator(-1);
    meta_service->remove_delete_bitmap_update_lock(
            reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &remove_req, &remove_res,
            nullptr);
    ASSERT_EQ(remove_res.status().code(), MetaServiceCode::OK);

    // case 5: lock key owned by load1 but expired, load2 get lock
    req.add_partition_ids(123);
    req.set_expiration(1);
    req.set_lock_id(888);
    req.set_initiator(-1);
    meta_service->get_delete_bitmap_update_lock(
            reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

    sleep(2);
    req.set_lock_id(889);
    meta_service->get_delete_bitmap_update_lock(
            reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

    remove_req.set_lock_id(889);
    remove_req.set_initiator(-1);
    meta_service->remove_delete_bitmap_update_lock(
            reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &remove_req, &remove_res,
            nullptr);
    ASSERT_EQ(remove_res.status().code(), MetaServiceCode::OK);

    // case 6: lock key owned by load1 but expired, compaction1 get lock
    req.add_partition_ids(123);
    req.set_expiration(1);
    req.set_lock_id(888);
    req.set_initiator(-1);
    meta_service->get_delete_bitmap_update_lock(
            reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

    sleep(2);
    req.set_lock_id(job_lock_id);
    req.set_initiator(888);
    req.set_expiration(1);
    meta_service->get_delete_bitmap_update_lock(
            reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

    remove_req.set_lock_id(job_lock_id);
    remove_req.set_initiator(888);
    meta_service->remove_delete_bitmap_update_lock(
            reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &remove_req, &remove_res,
            nullptr);
    ASSERT_EQ(remove_res.status().code(), MetaServiceCode::OK);

    // case 7: lock key owned by compaction, new compaction get lock
    req.set_lock_id(job_lock_id);
    req.set_initiator(100);
    req.set_expiration(100);
    meta_service->get_delete_bitmap_update_lock(
            reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

    req.set_lock_id(job_lock_id);
    req.set_initiator(101);
    req.set_expiration(1);
    meta_service->get_delete_bitmap_update_lock(
            reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

    // new compaction get lock again
    meta_service->get_delete_bitmap_update_lock(
            reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

    // case 8: lock key owned by compaction, load1 get lock
    req.set_lock_id(888);
    req.set_initiator(-1);
    req.set_expiration(60);
    meta_service->get_delete_bitmap_update_lock(
            reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::LOCK_CONFLICT);

    remove_req.set_lock_id(job_lock_id);
    remove_req.set_initiator(100);
    meta_service->remove_delete_bitmap_update_lock(
            reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &remove_req, &remove_res,
            nullptr);
    ASSERT_EQ(remove_res.status().code(), MetaServiceCode::OK);

    // case 9: lock key owned by compaction but all expired (101 900), load1 get lock
    req.set_table_id(table_id);
    req.set_lock_id(job_lock_id);
    req.set_initiator(900);
    req.set_expiration(1);
    meta_service->get_delete_bitmap_update_lock(
            reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

    sleep(2);
    req.set_table_id(table_id);
    req.set_lock_id(888);
    req.set_initiator(-1);
    req.set_expiration(60);
    meta_service->get_delete_bitmap_update_lock(
            reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

    remove_req.set_lock_id(888);
    remove_req.set_initiator(-1);
    meta_service->remove_delete_bitmap_update_lock(
            reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &remove_req, &remove_res,
            nullptr);
    ASSERT_EQ(remove_res.status().code(), MetaServiceCode::OK);

    // case 10: lock key owned by compaction but all expired (101 900), schema change get lock
    req.set_table_id(table_id);
    req.set_lock_id(job_lock_id);
    req.set_initiator(900);
    req.set_expiration(1);
    meta_service->get_delete_bitmap_update_lock(
            reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

    sleep(2);
    req.set_table_id(table_id);
    int other_job_lock_id = job_lock_id == COMPACTION_DELETE_BITMAP_LOCK_ID
                                    ? SCHEMA_CHANGE_DELETE_BITMAP_LOCK_ID
                                    : COMPACTION_DELETE_BITMAP_LOCK_ID;
    req.set_lock_id(other_job_lock_id);
    req.set_initiator(100);
    req.set_expiration(1);
    meta_service->get_delete_bitmap_update_lock(
            reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

    remove_req.set_lock_id(other_job_lock_id);
    remove_req.set_initiator(100);
    meta_service->remove_delete_bitmap_update_lock(
            reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &remove_req, &remove_res,
            nullptr);
    ASSERT_EQ(remove_res.status().code(), MetaServiceCode::OK);

    // case 11: lock by schema change but expired, compaction get lock but txn commit conflict, do fast retry
    sp->set_call_back("get_delete_bitmap_update_lock:commit:conflict", [&](auto&& args) {
        auto* first_retry = try_any_cast<bool*>(args[0]);
        auto lock_id = (try_any_cast<const GetDeleteBitmapUpdateLockRequest*>(args[1]))->lock_id();
        if (*first_retry && is_job_delete_bitmap_lock_id(lock_id)) {
            *try_any_cast<TxnErrorCode*>(args[2]) = TxnErrorCode::TXN_CONFLICT;
        } else {
            *try_any_cast<TxnErrorCode*>(args[2]) = TxnErrorCode::TXN_OK;
        }
    });
    sp->enable_processing();
    req.set_lock_id(job_lock_id);
    req.set_initiator(100);
    req.set_expiration(10);
    meta_service->get_delete_bitmap_update_lock(
            reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    remove_req.set_lock_id(job_lock_id);
    remove_req.set_initiator(100);
    meta_service->remove_delete_bitmap_update_lock(
            reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &remove_req, &remove_res,
            nullptr);
    ASSERT_EQ(remove_res.status().code(), MetaServiceCode::OK);

    // case 12: lock by load but expired, compaction get lock but txn commit conflict, do fast retry
    req.set_lock_id(300);
    req.set_initiator(-1);
    req.set_expiration(1);
    meta_service->get_delete_bitmap_update_lock(
            reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

    sleep(2);
    req.set_lock_id(job_lock_id);
    req.set_initiator(100);
    req.set_expiration(10);
    meta_service->get_delete_bitmap_update_lock(
            reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    remove_delete_bitmap_lock(meta_service.get(), table_id);

    // case 13: lock key does not exist, compaction get lock but txn commit conflict, do fast retry
    meta_service->get_delete_bitmap_update_lock(
            reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

    sp->clear_all_call_backs();
    sp->clear_trace();
    sp->disable_processing();
    remove_delete_bitmap_lock(meta_service.get(), table_id);
}

TEST(MetaServiceTest, GetDeleteBitmapUpdateLock) {
    testGetDeleteBitmapUpdateLock(2, COMPACTION_DELETE_BITMAP_LOCK_ID);
    testGetDeleteBitmapUpdateLock(2, SCHEMA_CHANGE_DELETE_BITMAP_LOCK_ID);
    testGetDeleteBitmapUpdateLock(1, COMPACTION_DELETE_BITMAP_LOCK_ID);
    testGetDeleteBitmapUpdateLock(1, SCHEMA_CHANGE_DELETE_BITMAP_LOCK_ID);
}

TEST(MetaServiceTest, GetDeleteBitmapUpdateLockNoReadStats) {
    auto meta_service = get_meta_service();

    brpc::Controller cntl;
    GetDeleteBitmapUpdateLockRequest req;
    GetDeleteBitmapUpdateLockResponse res;
    req.set_cloud_unique_id("test_cloud_unique_id");
    req.set_table_id(111);
    req.add_partition_ids(123);
    req.set_expiration(5);
    req.set_lock_id(888);
    req.set_initiator(-1);
    meta_service->get_delete_bitmap_update_lock(
            reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

    // same lock_id
    meta_service->get_delete_bitmap_update_lock(
            reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

    // different lock_id
    req.set_lock_id(999);
    meta_service->get_delete_bitmap_update_lock(
            reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::LOCK_CONFLICT);

    // lock expired
    req.set_cloud_unique_id("test_cloud_unique_id");
    req.set_table_id(222);
    req.set_expiration(0);
    req.set_lock_id(666);
    meta_service->get_delete_bitmap_update_lock(
            reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    sleep(1);
    req.set_lock_id(667);
    meta_service->get_delete_bitmap_update_lock(
            reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
}

TEST(MetaServiceTest, GetDeleteBitmapUpdateLockTabletStatsNormal) {
    auto meta_service = get_meta_service();

    bool enable_batch_get_mow_tablet_stats_and_meta_vals[] = {false, true};
    for (bool val : enable_batch_get_mow_tablet_stats_and_meta_vals) {
        config::enable_batch_get_mow_tablet_stats_and_meta = val;

        std::string instance_id = "test_get_delete_bitmap_update_lock_normal";
        [[maybe_unused]] auto* sp = SyncPoint::get_instance();
        DORIS_CLOUD_DEFER {
            SyncPoint::get_instance()->disable_processing();
            SyncPoint::get_instance()->clear_all_call_backs();
        };
        sp->set_call_back("get_instance_id", [&](auto&& args) {
            auto* ret = try_any_cast_ret<std::string>(args);
            ret->first = instance_id;
            ret->second = true;
        });
        sp->enable_processing();

        int64_t db_id = 1000;
        int64_t table_id = 2001;
        int64_t index_id = 3001;
        // [(partition_id, tablet_id)]
        std::vector<std::array<int64_t, 2>> tablet_idxes {
                {70001, 12345}, {80001, 3456}, {90001, 6789}};

        add_tablet_metas(meta_service.get(), instance_id, table_id, index_id, tablet_idxes);

        GetDeleteBitmapUpdateLockResponse res;
        get_delete_bitmap_update_lock(meta_service.get(), res, db_id, table_id, index_id,
                                      tablet_idxes, 5, 999999, -1, true);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

        ASSERT_EQ(res.base_compaction_cnts().size(), tablet_idxes.size());
        for (const auto& base_compaction_cnt : res.base_compaction_cnts()) {
            ASSERT_EQ(base_compaction_cnt, 10);
        }
        ASSERT_EQ(res.cumulative_compaction_cnts().size(), tablet_idxes.size());
        for (const auto& cumu_compaction_cnt : res.cumulative_compaction_cnts()) {
            ASSERT_EQ(cumu_compaction_cnt, 20);
        }
        ASSERT_EQ(res.cumulative_points().size(), tablet_idxes.size());
        for (const auto& cumulative_point : res.cumulative_points()) {
            ASSERT_EQ(cumulative_point, 30);
        }
    }
}

TEST(MetaServiceTest, GetDeleteBitmapUpdateLockTabletStatsLockExpired) {
    auto meta_service = get_meta_service();

    bool enable_batch_get_mow_tablet_stats_and_meta_vals[] = {false, true};
    for (bool val : enable_batch_get_mow_tablet_stats_and_meta_vals) {
        config::enable_batch_get_mow_tablet_stats_and_meta = val;
        // 2.1 abnormal path, lock has been expired and taken by another load/compaction during
        // the reading of tablet stats
        std::string instance_id = "test_get_delete_bitmap_update_lock_abnormal1";
        [[maybe_unused]] auto* sp = SyncPoint::get_instance();
        DORIS_CLOUD_DEFER {
            SyncPoint::get_instance()->disable_processing();
            SyncPoint::get_instance()->clear_all_call_backs();
        };
        sp->set_call_back("get_instance_id", [&](auto&& args) {
            auto* ret = try_any_cast_ret<std::string>(args);
            ret->first = instance_id;
            ret->second = true;
        });
        sp->set_call_back("check_delete_bitmap_lock.set_lock_info", [&](auto&& args) {
            auto* lock_info = try_any_cast<DeleteBitmapUpdateLockPB*>(args[0]);
            // simulate that lock_id has been modified by another load
            lock_info->set_lock_id(345);
            LOG(INFO) << "change lock_info.lock_id to 345, lock_info=" << lock_info->DebugString();
        });

        sp->enable_processing();

        int64_t db_id = 1000;
        int64_t table_id = 2001;
        int64_t index_id = 3001;
        // [(partition_id, tablet_id)]
        std::vector<std::array<int64_t, 2>> tablet_idxes {
                {70001, 12345}, {80001, 3456}, {90001, 6789}};

        add_tablet_metas(meta_service.get(), instance_id, table_id, index_id, tablet_idxes);

        GetDeleteBitmapUpdateLockResponse res;
        get_delete_bitmap_update_lock(meta_service.get(), res, db_id, table_id, index_id,
                                      tablet_idxes, 5, 999999, -1, true);
        EXPECT_EQ(res.status().code(), MetaServiceCode::LOCK_EXPIRED);
        EXPECT_EQ(res.base_compaction_cnts().size(), 3);
        EXPECT_EQ(res.cumulative_compaction_cnts().size(), 3);
        EXPECT_EQ(res.cumulative_points().size(), 3);
    }

    for (bool val : enable_batch_get_mow_tablet_stats_and_meta_vals) {
        config::enable_batch_get_mow_tablet_stats_and_meta = val;

        // 2.2 abnormal path, lock has been taken by another load/compaction and been released during
        // the reading of tablet stats
        std::string instance_id = "test_get_delete_bitmap_update_lock_abnormal2";
        [[maybe_unused]] auto* sp = SyncPoint::get_instance();
        DORIS_CLOUD_DEFER {
            SyncPoint::get_instance()->disable_processing();
            SyncPoint::get_instance()->clear_all_call_backs();
        };
        sp->set_call_back("get_instance_id", [&](auto&& args) {
            auto* ret = try_any_cast_ret<std::string>(args);
            ret->first = instance_id;
            ret->second = true;
        });
        sp->set_call_back("check_delete_bitmap_lock.inject_get_lock_key_err", [&](auto&& args) {
            auto* err = try_any_cast<TxnErrorCode*>(args[0]);
            // the lock has been taken by another load and been released,
            // so the delete bitmap update lock KV will be removed
            *err = TxnErrorCode::TXN_KEY_NOT_FOUND;
        });

        sp->enable_processing();

        int64_t db_id = 1000;
        int64_t table_id = 2001;
        int64_t index_id = 3001;
        // [(partition_id, tablet_id)]
        std::vector<std::array<int64_t, 2>> tablet_idxes {
                {70001, 12345}, {80001, 3456}, {90001, 6789}};

        add_tablet_metas(meta_service.get(), instance_id, table_id, index_id, tablet_idxes);

        GetDeleteBitmapUpdateLockResponse res;
        get_delete_bitmap_update_lock(meta_service.get(), res, db_id, table_id, index_id,
                                      tablet_idxes, 5, 999999, -1, true);
        ASSERT_EQ(res.status().code(), MetaServiceCode::LOCK_EXPIRED);
    }
}

TEST(MetaServiceTest, GetDeleteBitmapUpdateLockTabletStatsError) {
    auto meta_service = get_meta_service();

    bool enable_batch_get_mow_tablet_stats_and_meta_vals[] = {false, true};
    for (bool val : enable_batch_get_mow_tablet_stats_and_meta_vals) {
        config::enable_batch_get_mow_tablet_stats_and_meta = val;
        // 2.3 abnormal path, meeting error when reading tablets' stats
        std::string instance_id = "test_get_delete_bitmap_update_lock_abnormal3";
        [[maybe_unused]] auto* sp = SyncPoint::get_instance();
        DORIS_CLOUD_DEFER {
            SyncPoint::get_instance()->disable_processing();
            SyncPoint::get_instance()->clear_all_call_backs();
        };
        sp->set_call_back("get_instance_id", [&](auto&& args) {
            auto* ret = try_any_cast_ret<std::string>(args);
            ret->first = instance_id;
            ret->second = true;
        });

        TxnErrorCode injected_error_code {TxnErrorCode::TXN_KEY_NOT_FOUND};
        sp->set_call_back("get_delete_bitmap_update_lock.get_compaction_cnts_inject_error",
                          [&](auto&& args) {
                              auto* err = try_any_cast<TxnErrorCode*>(args[0]);
                              *err = injected_error_code;
                          });

        sp->enable_processing();

        int64_t db_id = 1000;
        int64_t table_id = 2001;
        int64_t index_id = 3001;
        // [(partition_id, tablet_id)]
        std::vector<std::array<int64_t, 2>> tablet_idxes {
                {70001, 12345}, {80001, 3456}, {90001, 6789}};

        add_tablet_metas(meta_service.get(), instance_id, table_id, index_id, tablet_idxes);

        GetDeleteBitmapUpdateLockResponse res;
        get_delete_bitmap_update_lock(meta_service.get(), res, db_id, table_id, index_id,
                                      tablet_idxes, 5, 999999, -1, true);
        ASSERT_EQ(res.status().code(), MetaServiceCode::KV_TXN_GET_ERR);
    }

    for (bool val : enable_batch_get_mow_tablet_stats_and_meta_vals) {
        config::enable_batch_get_mow_tablet_stats_and_meta = val;
        // 2.4 abnormal path, meeting TXN_TOO_OLD error when reading tablets' stats,
        // this should not fail if lock is not expired
        std::string instance_id = "test_get_delete_bitmap_update_lock_abnormal4";
        [[maybe_unused]] auto* sp = SyncPoint::get_instance();
        DORIS_CLOUD_DEFER {
            SyncPoint::get_instance()->disable_processing();
            SyncPoint::get_instance()->clear_all_call_backs();
        };
        sp->set_call_back("get_instance_id", [&](auto&& args) {
            auto* ret = try_any_cast_ret<std::string>(args);
            ret->first = instance_id;
            ret->second = true;
        });

        int counter = 0;
        sp->set_call_back("get_delete_bitmap_update_lock.get_compaction_cnts_inject_error",
                          [&](auto&& args) {
                              if (counter++ % 2 == 0) {
                                  auto* err = try_any_cast<TxnErrorCode*>(args[0]);
                                  *err = TxnErrorCode::TXN_TOO_OLD;
                              }
                          });

        sp->enable_processing();

        int64_t db_id = 1000;
        int64_t table_id = 2001;
        int64_t index_id = 3001;
        // [(partition_id, tablet_id)]
        std::vector<std::array<int64_t, 2>> tablet_idxes;
        for (int i = 0; i < 20; i++) {
            int64_t partition_id = 70000 + i;
            int64_t tablet_id = 80000 + i;
            tablet_idxes.push_back({partition_id, tablet_id});
        }

        add_tablet_metas(meta_service.get(), instance_id, table_id, index_id, tablet_idxes);

        GetDeleteBitmapUpdateLockResponse res;
        get_delete_bitmap_update_lock(meta_service.get(), res, db_id, table_id, index_id,
                                      tablet_idxes, 5, 999999, -1, true);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(res.base_compaction_cnts().size(), tablet_idxes.size());
        for (const auto& base_compaction_cnt : res.base_compaction_cnts()) {
            ASSERT_EQ(base_compaction_cnt, 10);
        }
        ASSERT_EQ(res.cumulative_compaction_cnts().size(), tablet_idxes.size());
        for (const auto& cumu_compaction_cnt : res.cumulative_compaction_cnts()) {
            ASSERT_EQ(cumu_compaction_cnt, 20);
        }
        ASSERT_EQ(res.cumulative_points().size(), tablet_idxes.size());
        for (const auto& cumulative_point : res.cumulative_points()) {
            ASSERT_EQ(cumulative_point, 30);
        }
    }
}

static std::string generate_random_string(int length) {
    std::string char_set = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    std::random_device rd;
    std::mt19937 generator(rd());
    std::uniform_int_distribution<int> distribution(0, char_set.length() - 1);

    std::string randomString;
    for (int i = 0; i < length; ++i) {
        randomString += char_set[distribution(generator)];
    }
    return randomString;
}

TEST(MetaServiceTest, UpdateDeleteBitmapWithBigKeys) {
    auto meta_service = get_meta_service();
    // get delete bitmap update lock
    brpc::Controller cntl;
    GetDeleteBitmapUpdateLockRequest get_lock_req;
    GetDeleteBitmapUpdateLockResponse get_lock_res;
    get_lock_req.set_cloud_unique_id("test_cloud_unique_id");
    get_lock_req.set_table_id(1999);
    get_lock_req.add_partition_ids(123);
    get_lock_req.set_expiration(5);
    get_lock_req.set_lock_id(-1);
    get_lock_req.set_initiator(100);
    meta_service->get_delete_bitmap_update_lock(
            reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &get_lock_req,
            &get_lock_res, nullptr);
    ASSERT_EQ(get_lock_res.status().code(), MetaServiceCode::OK);
    UpdateDeleteBitmapRequest update_delete_bitmap_req;
    UpdateDeleteBitmapResponse update_delete_bitmap_res;
    update_delete_bitmap_req.set_cloud_unique_id("test_cloud_unique_id");
    update_delete_bitmap_req.set_table_id(1999);
    update_delete_bitmap_req.set_partition_id(123);
    update_delete_bitmap_req.set_lock_id(-1);
    update_delete_bitmap_req.set_initiator(100);
    update_delete_bitmap_req.set_tablet_id(333);
    std::string large_value = generate_random_string(300 * 1000 * 3);
    for (int i = 0; i < 100000; i++) {
        update_delete_bitmap_req.add_rowset_ids("0200000003ea308a3647dbea83220ed4b8897f2288244a91");
        update_delete_bitmap_req.add_segment_ids(0);
        update_delete_bitmap_req.add_versions(i);
        update_delete_bitmap_req.add_segment_delete_bitmaps("1");
    }
    meta_service->update_delete_bitmap(reinterpret_cast<google::protobuf::RpcController*>(&cntl),
                                       &update_delete_bitmap_req, &update_delete_bitmap_res,
                                       nullptr);
    ASSERT_EQ(update_delete_bitmap_res.status().code(), MetaServiceCode::OK);
}

static void set_partition_version(MetaServiceProxy* meta_service, std::string_view instance_id,
                                  int64_t db_id, int64_t table_id, int64_t partition_id,
                                  int64_t version, std::vector<int64_t> pending_txn_ids = {}) {
    std::string ver_key = partition_version_key({instance_id, db_id, table_id, partition_id});
    std::string ver_val;
    VersionPB version_pb;
    version_pb.set_version(version);
    if (!pending_txn_ids.empty()) {
        for (auto txn_id : pending_txn_ids) {
            version_pb.add_pending_txn_ids(txn_id);
        }
    }
    ASSERT_TRUE(version_pb.SerializeToString(&ver_val));
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->put(ver_key, ver_val);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
}

static void begin_txn_and_commit_rowset(MetaServiceProxy* meta_service, const std::string& label,
                                        int64_t db_id, int64_t table_id, int64_t partition_id,
                                        int64_t tablet_id, int64_t* txn_id) {
    begin_txn(meta_service, db_id, label, table_id, *txn_id);
    CreateRowsetResponse res;
    auto rowset = create_rowset(*txn_id, tablet_id, partition_id);
    prepare_rowset(meta_service, rowset, res);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    res.Clear();
    commit_rowset(meta_service, rowset, res);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
}

static void get_delete_bitmap_update_lock(MetaServiceProxy* meta_service, int64_t table_id,
                                          int64_t partition_id, int64_t lock_id,
                                          int64_t initiator) {
    brpc::Controller cntl;
    GetDeleteBitmapUpdateLockRequest get_lock_req;
    GetDeleteBitmapUpdateLockResponse get_lock_res;
    get_lock_req.set_cloud_unique_id("test_cloud_unique_id");
    get_lock_req.set_table_id(table_id);
    get_lock_req.add_partition_ids(partition_id);
    get_lock_req.set_expiration(5);
    get_lock_req.set_lock_id(lock_id);
    get_lock_req.set_initiator(initiator);
    meta_service->get_delete_bitmap_update_lock(
            reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &get_lock_req,
            &get_lock_res, nullptr);
    ASSERT_EQ(get_lock_res.status().code(), MetaServiceCode::OK);
}

static void update_delete_bitmap(MetaServiceProxy* meta_service,
                                 UpdateDeleteBitmapRequest& update_delete_bitmap_req,
                                 UpdateDeleteBitmapResponse& update_delete_bitmap_res,
                                 int64_t table_id, int64_t partition_id, int64_t lock_id,
                                 int64_t initiator, int64_t tablet_id, int64_t txn_id,
                                 int64_t next_visible_version, std::string data = "1111") {
    brpc::Controller cntl;
    update_delete_bitmap_req.set_cloud_unique_id("test_cloud_unique_id");
    update_delete_bitmap_req.set_table_id(table_id);
    update_delete_bitmap_req.set_partition_id(partition_id);
    update_delete_bitmap_req.set_lock_id(lock_id);
    update_delete_bitmap_req.set_initiator(initiator);
    update_delete_bitmap_req.set_tablet_id(tablet_id);
    update_delete_bitmap_req.set_txn_id(txn_id);
    update_delete_bitmap_req.set_next_visible_version(next_visible_version);
    update_delete_bitmap_req.add_rowset_ids("123");
    update_delete_bitmap_req.add_segment_ids(0);
    update_delete_bitmap_req.add_versions(next_visible_version);
    update_delete_bitmap_req.add_segment_delete_bitmaps(data);
    meta_service->update_delete_bitmap(reinterpret_cast<google::protobuf::RpcController*>(&cntl),
                                       &update_delete_bitmap_req, &update_delete_bitmap_res,
                                       nullptr);
}

TEST(MetaServiceTest, UpdateDeleteBitmapCheckPartitionVersion) {
    auto meta_service = get_meta_service();
    brpc::Controller cntl;

    extern std::string get_instance_id(const std::shared_ptr<ResourceManager>& rc_mgr,
                                       const std::string& cloud_unique_id);
    auto instance_id = get_instance_id(meta_service->resource_mgr(), "test_cloud_unique_id");

    {
        // 1. normal path
        // 1.1 has partition version and request version matches
        int64_t db_id = 999;
        int64_t table_id = 1001;
        int64_t index_id = 4001;
        int64_t t1p1 = 2001;
        int64_t tablet_id = 3001;
        int64_t initiator = -1;
        int64_t cur_max_version = 100;
        int64_t txn_id;
        ASSERT_NO_FATAL_FAILURE(create_tablet_with_db_id(meta_service.get(), db_id, table_id,
                                                         index_id, t1p1, tablet_id));
        begin_txn_and_commit_rowset(meta_service.get(), "label11", db_id, table_id, t1p1, tablet_id,
                                    &txn_id);
        int64_t lock_id = txn_id;

        get_delete_bitmap_update_lock(meta_service.get(), table_id, t1p1, lock_id, initiator);
        set_partition_version(meta_service.get(), instance_id, db_id, table_id, t1p1,
                              cur_max_version);

        UpdateDeleteBitmapRequest update_delete_bitmap_req;
        UpdateDeleteBitmapResponse update_delete_bitmap_res;
        update_delete_bitmap(meta_service.get(), update_delete_bitmap_req, update_delete_bitmap_res,
                             table_id, t1p1, lock_id, initiator, tablet_id, txn_id,
                             cur_max_version + 1);
        ASSERT_EQ(update_delete_bitmap_res.status().code(), MetaServiceCode::OK);
    }

    {
        // 1. normal path
        // 1.2 does not have partition version KV and request version matches
        int64_t db_id = 999;
        int64_t table_id = 1002;
        int64_t index_id = 4001;
        int64_t t1p1 = 2001;
        int64_t tablet_id = 3001;
        int64_t initiator = -1;
        int64_t txn_id;
        ASSERT_NO_FATAL_FAILURE(create_tablet_with_db_id(meta_service.get(), db_id, table_id,
                                                         index_id, t1p1, tablet_id));
        begin_txn_and_commit_rowset(meta_service.get(), "label12", db_id, table_id, t1p1, tablet_id,
                                    &txn_id);
        int64_t lock_id = txn_id;

        get_delete_bitmap_update_lock(meta_service.get(), table_id, t1p1, lock_id, initiator);

        UpdateDeleteBitmapRequest update_delete_bitmap_req;
        UpdateDeleteBitmapResponse update_delete_bitmap_res;
        update_delete_bitmap(meta_service.get(), update_delete_bitmap_req, update_delete_bitmap_res,
                             table_id, t1p1, lock_id, initiator, tablet_id, txn_id, 2);
        ASSERT_EQ(update_delete_bitmap_res.status().code(), MetaServiceCode::OK);
    }

    {
        // 1. normal path
        // 1.3 has partition version and pending txn, and request version matches
        int64_t db_id = 999;
        int64_t table_id = 1003;
        int64_t index_id = 4001;
        int64_t t1p1 = 2001;
        int64_t tablet_id = 3001;
        int64_t initiator = -1;
        int64_t cur_max_version = 120;
        int64_t txn_id;
        ASSERT_NO_FATAL_FAILURE(create_tablet_with_db_id(meta_service.get(), db_id, table_id,
                                                         index_id, t1p1, tablet_id));
        begin_txn_and_commit_rowset(meta_service.get(), "label13", db_id, table_id, t1p1, tablet_id,
                                    &txn_id);
        int64_t lock_id = txn_id;

        get_delete_bitmap_update_lock(meta_service.get(), table_id, t1p1, lock_id, initiator);
        set_partition_version(meta_service.get(), instance_id, db_id, table_id, t1p1,
                              cur_max_version, {12345});

        UpdateDeleteBitmapRequest update_delete_bitmap_req;
        UpdateDeleteBitmapResponse update_delete_bitmap_res;
        update_delete_bitmap(meta_service.get(), update_delete_bitmap_req, update_delete_bitmap_res,
                             table_id, t1p1, lock_id, initiator, tablet_id, txn_id,
                             cur_max_version + 2);
        ASSERT_EQ(update_delete_bitmap_res.status().code(), MetaServiceCode::OK);
    }
}

TEST(MetaServiceTest, UpdateDeleteBitmapCheckPartitionVersionFail) {
    auto meta_service = get_meta_service();
    brpc::Controller cntl;

    extern std::string get_instance_id(const std::shared_ptr<ResourceManager>& rc_mgr,
                                       const std::string& cloud_unique_id);
    auto instance_id = get_instance_id(meta_service->resource_mgr(), "test_cloud_unique_id");

    {
        // 2. abnormal path
        // 2.1 has partition version but request version does not match
        int64_t db_id = 999;
        int64_t table_id = 2001;
        int64_t index_id = 4001;
        int64_t t1p1 = 2001;
        int64_t tablet_id = 3001;
        int64_t initiator = -1;
        int64_t cur_max_version = 100;
        int64_t txn_id;
        ASSERT_NO_FATAL_FAILURE(create_tablet_with_db_id(meta_service.get(), db_id, table_id,
                                                         index_id, t1p1, tablet_id));
        begin_txn_and_commit_rowset(meta_service.get(), "label21", db_id, table_id, t1p1, tablet_id,
                                    &txn_id);
        int64_t lock_id = txn_id;

        get_delete_bitmap_update_lock(meta_service.get(), table_id, t1p1, lock_id, initiator);
        set_partition_version(meta_service.get(), instance_id, db_id, table_id, t1p1,
                              cur_max_version);

        UpdateDeleteBitmapRequest update_delete_bitmap_req;
        UpdateDeleteBitmapResponse update_delete_bitmap_res;
        // wrong version
        update_delete_bitmap(meta_service.get(), update_delete_bitmap_req, update_delete_bitmap_res,
                             table_id, t1p1, lock_id, initiator, tablet_id, txn_id,
                             cur_max_version + 2);
        ASSERT_EQ(update_delete_bitmap_res.status().code(), MetaServiceCode::VERSION_NOT_MATCH);
    }

    {
        // 2. abnormal path
        // 2.2 does not have partition version KV and request version does not match
        int64_t db_id = 999;
        int64_t table_id = 2002;
        int64_t index_id = 4001;
        int64_t t1p1 = 2001;
        int64_t tablet_id = 3001;
        int64_t initiator = -1;
        int64_t txn_id;
        ASSERT_NO_FATAL_FAILURE(create_tablet_with_db_id(meta_service.get(), db_id, table_id,
                                                         index_id, t1p1, tablet_id));
        begin_txn_and_commit_rowset(meta_service.get(), "label22", db_id, table_id, t1p1, tablet_id,
                                    &txn_id);
        int64_t lock_id = txn_id;

        get_delete_bitmap_update_lock(meta_service.get(), table_id, t1p1, lock_id, initiator);

        UpdateDeleteBitmapRequest update_delete_bitmap_req;
        UpdateDeleteBitmapResponse update_delete_bitmap_res;
        // first load, wrong version
        update_delete_bitmap(meta_service.get(), update_delete_bitmap_req, update_delete_bitmap_res,
                             table_id, t1p1, lock_id, initiator, tablet_id, txn_id, 10);
        ASSERT_EQ(update_delete_bitmap_res.status().code(), MetaServiceCode::VERSION_NOT_MATCH);
    }

    {
        // 2. abnormal path
        // 2.3 has partition version and pending txn, and request version matches
        int64_t db_id = 999;
        int64_t table_id = 2003;
        int64_t index_id = 4001;
        int64_t t1p1 = 2001;
        int64_t tablet_id = 3001;
        int64_t initiator = -1;
        int64_t cur_max_version = 120;
        int64_t txn_id;
        ASSERT_NO_FATAL_FAILURE(create_tablet_with_db_id(meta_service.get(), db_id, table_id,
                                                         index_id, t1p1, tablet_id));
        begin_txn_and_commit_rowset(meta_service.get(), "label23", db_id, table_id, t1p1, tablet_id,
                                    &txn_id);
        int64_t lock_id = txn_id;

        get_delete_bitmap_update_lock(meta_service.get(), table_id, t1p1, lock_id, initiator);
        set_partition_version(meta_service.get(), instance_id, db_id, table_id, t1p1,
                              cur_max_version, {12345});

        UpdateDeleteBitmapRequest update_delete_bitmap_req;
        UpdateDeleteBitmapResponse update_delete_bitmap_res;
        // wrong version
        update_delete_bitmap(meta_service.get(), update_delete_bitmap_req, update_delete_bitmap_res,
                             table_id, t1p1, lock_id, initiator, tablet_id, txn_id,
                             cur_max_version + 1);
        ASSERT_EQ(update_delete_bitmap_res.status().code(), MetaServiceCode::VERSION_NOT_MATCH);
    }
}

TEST(MetaServiceTest, UpdateDeleteBitmapFailCase) {
    // simulate the situation described in https://github.com/apache/doris/pull/49710
    auto meta_service = get_meta_service();
    brpc::Controller cntl;
    extern std::string get_instance_id(const std::shared_ptr<ResourceManager>& rc_mgr,
                                       const std::string& cloud_unique_id);
    auto instance_id = get_instance_id(meta_service->resource_mgr(), "test_cloud_unique_id");

    int64_t db_id = 1999;
    int64_t table_id = 1001;
    int64_t index_id = 4001;
    int64_t t1p1 = 2001;
    int64_t tablet_id = 3001;
    int64_t initiator = -1;
    int64_t cur_max_version = 100;
    set_partition_version(meta_service.get(), instance_id, db_id, table_id, t1p1, cur_max_version);
    ASSERT_NO_FATAL_FAILURE(create_tablet_with_db_id(meta_service.get(), db_id, table_id, index_id,
                                                     t1p1, tablet_id));

    // txn1 begins
    int64_t txn_id1;
    begin_txn_and_commit_rowset(meta_service.get(), "label31", db_id, table_id, t1p1, tablet_id,
                                &txn_id1);
    int64_t txn1_version_to_publish = cur_max_version + 1;
    // txn1 gains the lock and try to publish with version 101
    int64_t lock_id = txn_id1;
    get_delete_bitmap_update_lock(meta_service.get(), table_id, t1p1, lock_id, initiator);

    // txn1 failed due to calculation timeout and removes the delete bitmap lock
    RemoveDeleteBitmapUpdateLockRequest remove_req;
    RemoveDeleteBitmapUpdateLockResponse remove_res;
    remove_req.set_cloud_unique_id("test_cloud_unique_id");
    remove_req.set_table_id(table_id);
    remove_req.set_lock_id(lock_id);
    remove_req.set_initiator(-1);
    meta_service->remove_delete_bitmap_update_lock(
            reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &remove_req, &remove_res,
            nullptr);
    ASSERT_EQ(remove_res.status().code(), MetaServiceCode::OK);

    // txn2 gains the lock and succeeds to publish with version 101
    int64_t txn_id2;
    begin_txn_and_commit_rowset(meta_service.get(), "label32", db_id, table_id, t1p1, tablet_id,
                                &txn_id2);
    lock_id = txn_id2;
    get_delete_bitmap_update_lock(meta_service.get(), table_id, t1p1, lock_id, initiator);

    int64_t txn2_version_to_publish = cur_max_version + 1;
    UpdateDeleteBitmapRequest update_delete_bitmap_req;
    UpdateDeleteBitmapResponse update_delete_bitmap_res;
    std::string data1 = "1234";
    update_delete_bitmap(meta_service.get(), update_delete_bitmap_req, update_delete_bitmap_res,
                         table_id, t1p1, lock_id, initiator, tablet_id, txn_id2,
                         txn2_version_to_publish, data1);

    CommitTxnRequest req;
    req.set_cloud_unique_id("test_cloud_unique_id");
    req.set_db_id(db_id);
    req.set_txn_id(txn_id2);
    req.add_mow_table_ids(table_id);
    CommitTxnResponse res;
    meta_service->commit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                             &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string ver_key = partition_version_key({instance_id, db_id, table_id, t1p1});
    std::string ver_val;
    VersionPB version_pb;
    auto ret = txn->get(ver_key, &ver_val);
    ASSERT_EQ(ret, TxnErrorCode::TXN_OK);
    ASSERT_TRUE(version_pb.ParseFromString(ver_val));
    ASSERT_EQ(version_pb.version(), cur_max_version + 1);

    std::string lock_key = meta_delete_bitmap_update_lock_key({instance_id, table_id, -1});
    std::string lock_val;
    ret = txn->get(lock_key, &lock_val);
    ASSERT_EQ(ret, TxnErrorCode::TXN_KEY_NOT_FOUND);

    // txn1 retries to publish and gains the lock, try to publish with version 102
    lock_id = txn_id1;
    get_delete_bitmap_update_lock(meta_service.get(), table_id, t1p1, lock_id, initiator);

    // txn1's previous calculation task finshes and try to update delete bitmap with version 101
    std::string data2 = "5678";
    update_delete_bitmap(meta_service.get(), update_delete_bitmap_req, update_delete_bitmap_res,
                         table_id, t1p1, lock_id, initiator, tablet_id, txn_id1,
                         txn1_version_to_publish, data2);
    // this should fail
    ASSERT_EQ(update_delete_bitmap_res.status().code(), MetaServiceCode::VERSION_NOT_MATCH);

    GetDeleteBitmapRequest get_delete_bitmap_req;
    GetDeleteBitmapResponse get_delete_bitmap_res;
    get_delete_bitmap_req.set_cloud_unique_id("test_cloud_unique_id");
    get_delete_bitmap_req.set_tablet_id(tablet_id);
    get_delete_bitmap_req.add_rowset_ids("123");
    get_delete_bitmap_req.add_begin_versions(0);
    get_delete_bitmap_req.add_end_versions(cur_max_version + 1);
    meta_service->get_delete_bitmap(reinterpret_cast<google::protobuf::RpcController*>(&cntl),
                                    &get_delete_bitmap_req, &get_delete_bitmap_res, nullptr);
    ASSERT_EQ(get_delete_bitmap_res.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(get_delete_bitmap_res.rowset_ids_size(), 1);
    ASSERT_EQ(get_delete_bitmap_res.versions_size(), 1);
    ASSERT_EQ(get_delete_bitmap_res.segment_ids_size(), 1);
    ASSERT_EQ(get_delete_bitmap_res.segment_delete_bitmaps_size(), 1);
    ASSERT_EQ(get_delete_bitmap_res.segment_delete_bitmaps(0), data1);
}

TEST(MetaServiceTest, UpdateDeleteBitmapScOverrideExistingKey) {
    auto meta_service = get_meta_service();
    brpc::Controller cntl;
    size_t split_size = 90 * 1000; // see cloud/src/common/util.h

    extern std::string get_instance_id(const std::shared_ptr<ResourceManager>& rc_mgr,
                                       const std::string& cloud_unique_id);
    auto instance_id = get_instance_id(meta_service->resource_mgr(), "test_cloud_unique_id");

    {
        // schema change should use pending delete bitmap to clear previous failed trials
        int64_t db_id = 99999;
        int64_t table_id = 1801;
        int64_t index_id = 4801;
        int64_t t1p1 = 2001;
        int64_t tablet_id = 3001;
        int64_t txn_id;
        ASSERT_NO_FATAL_FAILURE(create_tablet_with_db_id(meta_service.get(), db_id, table_id,
                                                         index_id, t1p1, tablet_id));
        begin_txn_and_commit_rowset(meta_service.get(), "label11", db_id, table_id, t1p1, tablet_id,
                                    &txn_id);
        int64_t lock_id = -2;
        int64_t initiator = 1009;
        int64_t version = 100;

        get_delete_bitmap_update_lock(meta_service.get(), table_id, t1p1, lock_id, initiator);

        {
            UpdateDeleteBitmapRequest update_delete_bitmap_req;
            UpdateDeleteBitmapResponse update_delete_bitmap_res;
            // will be splited and stored in 5 KVs
            std::string data1(split_size * 5, 'c');
            update_delete_bitmap(meta_service.get(), update_delete_bitmap_req,
                                 update_delete_bitmap_res, table_id, t1p1, lock_id, initiator,
                                 tablet_id, txn_id, version, data1);
            ASSERT_EQ(update_delete_bitmap_res.status().code(), MetaServiceCode::OK);

            GetDeleteBitmapRequest get_delete_bitmap_req;
            GetDeleteBitmapResponse get_delete_bitmap_res;
            get_delete_bitmap_req.set_cloud_unique_id("test_cloud_unique_id");
            get_delete_bitmap_req.set_tablet_id(tablet_id);
            get_delete_bitmap_req.add_rowset_ids("123");
            get_delete_bitmap_req.add_begin_versions(0);
            get_delete_bitmap_req.add_end_versions(version);
            meta_service->get_delete_bitmap(
                    reinterpret_cast<google::protobuf::RpcController*>(&cntl),
                    &get_delete_bitmap_req, &get_delete_bitmap_res, nullptr);
            ASSERT_EQ(get_delete_bitmap_res.status().code(), MetaServiceCode::OK);
            ASSERT_EQ(get_delete_bitmap_res.rowset_ids_size(), 1);
            ASSERT_EQ(get_delete_bitmap_res.versions_size(), 1);
            ASSERT_EQ(get_delete_bitmap_res.segment_ids_size(), 1);
            ASSERT_EQ(get_delete_bitmap_res.segment_delete_bitmaps_size(), 1);
            ASSERT_EQ(get_delete_bitmap_res.segment_delete_bitmaps(0), data1);
        }

        {
            std::string pending_key = meta_pending_delete_bitmap_key({instance_id, tablet_id});
            std::string pending_val;
            std::unique_ptr<Transaction> txn;
            ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
            ASSERT_EQ(txn->get(pending_key, &pending_val), TxnErrorCode::TXN_OK);
            PendingDeleteBitmapPB pending_info;
            ASSERT_TRUE(pending_info.ParseFromString(pending_val));
            ASSERT_EQ(pending_info.delete_bitmap_keys_size(), 1);

            std::string_view k1 = pending_info.delete_bitmap_keys(0);
            k1.remove_prefix(1);
            std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
            decode_key(&k1, &out);
            // 0x01 "meta" ${instance_id} "delete_bitmap" ${tablet_id} ${rowset_id} ${version} ${segment_id} -> roaringbitmap
            auto encoded_tablet_id = std::get<std::int64_t>(std::get<0>(out[3]));
            ASSERT_EQ(encoded_tablet_id, tablet_id);
            auto encoded_rowset_id = std::get<std::string>(std::get<0>(out[4]));
            ASSERT_EQ(encoded_rowset_id, "123");
            auto encoded_version = std::get<std::int64_t>(std::get<0>(out[5]));
            ASSERT_EQ(encoded_version, version);
            auto encoded_segment_id = std::get<std::int64_t>(std::get<0>(out[6]));
            ASSERT_EQ(encoded_segment_id, 0);
        }

        {
            UpdateDeleteBitmapRequest update_delete_bitmap_req;
            UpdateDeleteBitmapResponse update_delete_bitmap_res;
            // will be splited and stored in 3 KVs
            // if we don't remove previous splited KVs, will crash when reading
            std::string data2(split_size * 3, 'a');
            update_delete_bitmap(meta_service.get(), update_delete_bitmap_req,
                                 update_delete_bitmap_res, table_id, t1p1, lock_id, initiator,
                                 tablet_id, txn_id, version, data2);
            ASSERT_EQ(update_delete_bitmap_res.status().code(), MetaServiceCode::OK);

            GetDeleteBitmapRequest get_delete_bitmap_req;
            GetDeleteBitmapResponse get_delete_bitmap_res;
            get_delete_bitmap_req.set_cloud_unique_id("test_cloud_unique_id");
            get_delete_bitmap_req.set_tablet_id(tablet_id);
            get_delete_bitmap_req.add_rowset_ids("123");
            get_delete_bitmap_req.add_begin_versions(0);
            get_delete_bitmap_req.add_end_versions(version);
            meta_service->get_delete_bitmap(
                    reinterpret_cast<google::protobuf::RpcController*>(&cntl),
                    &get_delete_bitmap_req, &get_delete_bitmap_res, nullptr);
            ASSERT_EQ(get_delete_bitmap_res.status().code(), MetaServiceCode::OK);
            ASSERT_EQ(get_delete_bitmap_res.rowset_ids_size(), 1);
            ASSERT_EQ(get_delete_bitmap_res.versions_size(), 1);
            ASSERT_EQ(get_delete_bitmap_res.segment_ids_size(), 1);
            ASSERT_EQ(get_delete_bitmap_res.segment_delete_bitmaps_size(), 1);
            ASSERT_EQ(get_delete_bitmap_res.segment_delete_bitmaps(0), data2);
        }
    }
}

void testUpdateDeleteBitmap(int lock_version) {
    config::delete_bitmap_lock_v2_white_list = lock_version == 1 ? "" : "*";
    auto meta_service = get_meta_service();
    remove_delete_bitmap_lock(meta_service.get(), 112);

    // get delete bitmap update lock
    brpc::Controller cntl;
    GetDeleteBitmapUpdateLockRequest get_lock_req;
    GetDeleteBitmapUpdateLockResponse get_lock_res;
    get_lock_req.set_cloud_unique_id("test_cloud_unique_id");
    get_lock_req.set_table_id(112);
    get_lock_req.add_partition_ids(123);
    get_lock_req.set_expiration(5);
    get_lock_req.set_lock_id(888);
    get_lock_req.set_initiator(-1);
    meta_service->get_delete_bitmap_update_lock(
            reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &get_lock_req,
            &get_lock_res, nullptr);
    ASSERT_EQ(get_lock_res.status().code(), MetaServiceCode::OK);

    // first update delete bitmap
    {
        UpdateDeleteBitmapRequest update_delete_bitmap_req;
        UpdateDeleteBitmapResponse update_delete_bitmap_res;
        update_delete_bitmap_req.set_cloud_unique_id("test_cloud_unique_id");
        update_delete_bitmap_req.set_table_id(112);
        update_delete_bitmap_req.set_partition_id(123);
        update_delete_bitmap_req.set_lock_id(888);
        update_delete_bitmap_req.set_initiator(-1);
        update_delete_bitmap_req.set_tablet_id(333);

        update_delete_bitmap_req.add_rowset_ids("123");
        update_delete_bitmap_req.add_segment_ids(1);
        update_delete_bitmap_req.add_versions(2);
        update_delete_bitmap_req.add_segment_delete_bitmaps("abc0");

        update_delete_bitmap_req.add_rowset_ids("123");
        update_delete_bitmap_req.add_segment_ids(0);
        update_delete_bitmap_req.add_versions(3);
        update_delete_bitmap_req.add_segment_delete_bitmaps("abc1");

        update_delete_bitmap_req.add_rowset_ids("123");
        update_delete_bitmap_req.add_segment_ids(1);
        update_delete_bitmap_req.add_versions(3);
        update_delete_bitmap_req.add_segment_delete_bitmaps("abc2");

        update_delete_bitmap_req.add_rowset_ids("124");
        update_delete_bitmap_req.add_segment_ids(0);
        update_delete_bitmap_req.add_versions(2);
        update_delete_bitmap_req.add_segment_delete_bitmaps("abc3");

        std::string large_value = generate_random_string(300 * 1000);
        update_delete_bitmap_req.add_rowset_ids("124");
        update_delete_bitmap_req.add_segment_ids(1);
        update_delete_bitmap_req.add_versions(2);
        update_delete_bitmap_req.add_segment_delete_bitmaps(large_value);

        update_delete_bitmap_req.add_rowset_ids("124");
        update_delete_bitmap_req.add_segment_ids(0);
        update_delete_bitmap_req.add_versions(3);
        update_delete_bitmap_req.add_segment_delete_bitmaps("abc4");

        meta_service->update_delete_bitmap(
                reinterpret_cast<google::protobuf::RpcController*>(&cntl),
                &update_delete_bitmap_req, &update_delete_bitmap_res, nullptr);
        ASSERT_EQ(update_delete_bitmap_res.status().code(), MetaServiceCode::OK);

        // first get delete bitmap
        GetDeleteBitmapRequest get_delete_bitmap_req;
        GetDeleteBitmapResponse get_delete_bitmap_res;
        get_delete_bitmap_req.set_cloud_unique_id("test_cloud_unique_id");
        get_delete_bitmap_req.set_tablet_id(333);

        get_delete_bitmap_req.add_rowset_ids("123");
        get_delete_bitmap_req.add_begin_versions(3);
        get_delete_bitmap_req.add_end_versions(3);

        get_delete_bitmap_req.add_rowset_ids("124");
        get_delete_bitmap_req.add_begin_versions(0);
        get_delete_bitmap_req.add_end_versions(3);

        meta_service->get_delete_bitmap(reinterpret_cast<google::protobuf::RpcController*>(&cntl),
                                        &get_delete_bitmap_req, &get_delete_bitmap_res, nullptr);
        ASSERT_EQ(get_delete_bitmap_res.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(get_delete_bitmap_res.rowset_ids_size(), 5);
        ASSERT_EQ(get_delete_bitmap_res.segment_delete_bitmaps_size(), 5);
        ASSERT_EQ(get_delete_bitmap_res.versions_size(), 5);
        ASSERT_EQ(get_delete_bitmap_res.segment_delete_bitmaps_size(), 5);

        ASSERT_EQ(get_delete_bitmap_res.rowset_ids(0), "123");
        ASSERT_EQ(get_delete_bitmap_res.segment_ids(0), 0);
        ASSERT_EQ(get_delete_bitmap_res.versions(0), 3);
        ASSERT_EQ(get_delete_bitmap_res.segment_delete_bitmaps(0), "abc1");

        ASSERT_EQ(get_delete_bitmap_res.rowset_ids(1), "123");
        ASSERT_EQ(get_delete_bitmap_res.segment_ids(1), 1);
        ASSERT_EQ(get_delete_bitmap_res.versions(1), 3);
        ASSERT_EQ(get_delete_bitmap_res.segment_delete_bitmaps(1), "abc2");

        ASSERT_EQ(get_delete_bitmap_res.rowset_ids(2), "124");
        ASSERT_EQ(get_delete_bitmap_res.segment_ids(2), 0);
        ASSERT_EQ(get_delete_bitmap_res.versions(2), 2);
        ASSERT_EQ(get_delete_bitmap_res.segment_delete_bitmaps(2), "abc3");

        ASSERT_EQ(get_delete_bitmap_res.rowset_ids(3), "124");
        ASSERT_EQ(get_delete_bitmap_res.segment_ids(3), 1);
        ASSERT_EQ(get_delete_bitmap_res.versions(3), 2);
        ASSERT_EQ(get_delete_bitmap_res.segment_delete_bitmaps(3), large_value);

        ASSERT_EQ(get_delete_bitmap_res.rowset_ids(4), "124");
        ASSERT_EQ(get_delete_bitmap_res.segment_ids(4), 0);
        ASSERT_EQ(get_delete_bitmap_res.versions(4), 3);
        ASSERT_EQ(get_delete_bitmap_res.segment_delete_bitmaps(4), "abc4");
    }

    // second update delete bitmap
    {
        UpdateDeleteBitmapRequest update_delete_bitmap_req;
        UpdateDeleteBitmapResponse update_delete_bitmap_res;
        update_delete_bitmap_req.set_cloud_unique_id("test_cloud_unique_id");
        update_delete_bitmap_req.set_table_id(112);
        update_delete_bitmap_req.set_partition_id(123);
        update_delete_bitmap_req.set_lock_id(888);
        update_delete_bitmap_req.set_initiator(-1);
        update_delete_bitmap_req.set_tablet_id(333);

        std::string large_value = generate_random_string(200 * 1000);
        update_delete_bitmap_req.add_rowset_ids("123");
        update_delete_bitmap_req.add_segment_ids(0);
        update_delete_bitmap_req.add_versions(2);
        update_delete_bitmap_req.add_segment_delete_bitmaps(large_value);

        update_delete_bitmap_req.add_rowset_ids("123");
        update_delete_bitmap_req.add_segment_ids(1);
        update_delete_bitmap_req.add_versions(2);
        update_delete_bitmap_req.add_segment_delete_bitmaps("bbb0");

        update_delete_bitmap_req.add_rowset_ids("123");
        update_delete_bitmap_req.add_segment_ids(1);
        update_delete_bitmap_req.add_versions(3);
        update_delete_bitmap_req.add_segment_delete_bitmaps("bbb1");

        update_delete_bitmap_req.add_rowset_ids("124");
        update_delete_bitmap_req.add_segment_ids(1);
        update_delete_bitmap_req.add_versions(3);
        update_delete_bitmap_req.add_segment_delete_bitmaps("bbb2");

        meta_service->update_delete_bitmap(
                reinterpret_cast<google::protobuf::RpcController*>(&cntl),
                &update_delete_bitmap_req, &update_delete_bitmap_res, nullptr);
        ASSERT_EQ(update_delete_bitmap_res.status().code(), MetaServiceCode::OK);

        // second get delete bitmap
        GetDeleteBitmapRequest get_delete_bitmap_req;
        GetDeleteBitmapResponse get_delete_bitmap_res;
        get_delete_bitmap_req.set_cloud_unique_id("test_cloud_unique_id");
        get_delete_bitmap_req.set_tablet_id(333);

        get_delete_bitmap_req.add_rowset_ids("123");
        get_delete_bitmap_req.add_begin_versions(0);
        get_delete_bitmap_req.add_end_versions(3);

        get_delete_bitmap_req.add_rowset_ids("124");
        get_delete_bitmap_req.add_begin_versions(0);
        get_delete_bitmap_req.add_end_versions(3);

        meta_service->get_delete_bitmap(reinterpret_cast<google::protobuf::RpcController*>(&cntl),
                                        &get_delete_bitmap_req, &get_delete_bitmap_res, nullptr);
        ASSERT_EQ(get_delete_bitmap_res.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(get_delete_bitmap_res.rowset_ids_size(), 4);
        ASSERT_EQ(get_delete_bitmap_res.segment_delete_bitmaps_size(), 4);
        ASSERT_EQ(get_delete_bitmap_res.versions_size(), 4);
        ASSERT_EQ(get_delete_bitmap_res.segment_delete_bitmaps_size(), 4);

        ASSERT_EQ(get_delete_bitmap_res.rowset_ids(0), "123");
        ASSERT_EQ(get_delete_bitmap_res.segment_ids(0), 0);
        ASSERT_EQ(get_delete_bitmap_res.versions(0), 2);
        ASSERT_EQ(get_delete_bitmap_res.segment_delete_bitmaps(0), large_value);

        ASSERT_EQ(get_delete_bitmap_res.rowset_ids(1), "123");
        ASSERT_EQ(get_delete_bitmap_res.segment_ids(1), 1);
        ASSERT_EQ(get_delete_bitmap_res.versions(1), 2);
        ASSERT_EQ(get_delete_bitmap_res.segment_delete_bitmaps(1), "bbb0");

        ASSERT_EQ(get_delete_bitmap_res.rowset_ids(2), "123");
        ASSERT_EQ(get_delete_bitmap_res.segment_ids(2), 1);
        ASSERT_EQ(get_delete_bitmap_res.versions(2), 3);
        ASSERT_EQ(get_delete_bitmap_res.segment_delete_bitmaps(2), "bbb1");

        ASSERT_EQ(get_delete_bitmap_res.rowset_ids(3), "124");
        ASSERT_EQ(get_delete_bitmap_res.segment_ids(3), 1);
        ASSERT_EQ(get_delete_bitmap_res.versions(3), 3);
        ASSERT_EQ(get_delete_bitmap_res.segment_delete_bitmaps(3), "bbb2");
    }

    // large size txn
    {
        UpdateDeleteBitmapRequest update_delete_bitmap_req;
        UpdateDeleteBitmapResponse update_delete_bitmap_res;
        update_delete_bitmap_req.set_cloud_unique_id("test_cloud_unique_id");
        update_delete_bitmap_req.set_table_id(112);
        update_delete_bitmap_req.set_partition_id(123);
        update_delete_bitmap_req.set_lock_id(888);
        update_delete_bitmap_req.set_initiator(-1);
        update_delete_bitmap_req.set_tablet_id(333);

        std::string large_value = generate_random_string(300 * 1000);
        for (size_t i = 0; i < 100; ++i) {
            update_delete_bitmap_req.add_rowset_ids("123");
            update_delete_bitmap_req.add_segment_ids(1);
            update_delete_bitmap_req.add_versions(i);
            update_delete_bitmap_req.add_segment_delete_bitmaps(large_value);
        }

        update_delete_bitmap_req.add_rowset_ids("124");
        update_delete_bitmap_req.add_segment_ids(0);
        update_delete_bitmap_req.add_versions(3);
        update_delete_bitmap_req.add_segment_delete_bitmaps("abcd4");

        meta_service->update_delete_bitmap(
                reinterpret_cast<google::protobuf::RpcController*>(&cntl),
                &update_delete_bitmap_req, &update_delete_bitmap_res, nullptr);
        ASSERT_EQ(update_delete_bitmap_res.status().code(), MetaServiceCode::OK);

        GetDeleteBitmapRequest get_delete_bitmap_req;
        GetDeleteBitmapResponse get_delete_bitmap_res;
        get_delete_bitmap_req.set_cloud_unique_id("test_cloud_unique_id");
        get_delete_bitmap_req.set_tablet_id(333);

        get_delete_bitmap_req.add_rowset_ids("123");
        get_delete_bitmap_req.add_begin_versions(0);
        get_delete_bitmap_req.add_end_versions(101);

        get_delete_bitmap_req.add_rowset_ids("124");
        get_delete_bitmap_req.add_begin_versions(0);
        get_delete_bitmap_req.add_end_versions(3);

        meta_service->get_delete_bitmap(reinterpret_cast<google::protobuf::RpcController*>(&cntl),
                                        &get_delete_bitmap_req, &get_delete_bitmap_res, nullptr);
        ASSERT_EQ(get_delete_bitmap_res.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(get_delete_bitmap_res.rowset_ids_size(), 101);
        ASSERT_EQ(get_delete_bitmap_res.segment_delete_bitmaps_size(), 101);
        ASSERT_EQ(get_delete_bitmap_res.versions_size(), 101);
        ASSERT_EQ(get_delete_bitmap_res.segment_delete_bitmaps_size(), 101);

        for (size_t i = 0; i < 100; ++i) {
            ASSERT_EQ(get_delete_bitmap_res.rowset_ids(i), "123");
            ASSERT_EQ(get_delete_bitmap_res.segment_ids(i), 1);
            ASSERT_EQ(get_delete_bitmap_res.versions(i), i);
            ASSERT_EQ(get_delete_bitmap_res.segment_delete_bitmaps(i), large_value);
        }

        ASSERT_EQ(get_delete_bitmap_res.rowset_ids(100), "124");
        ASSERT_EQ(get_delete_bitmap_res.segment_ids(100), 0);
        ASSERT_EQ(get_delete_bitmap_res.versions(100), 3);
        ASSERT_EQ(get_delete_bitmap_res.segment_delete_bitmaps(100), "abcd4");
    }

    // update existing delete bitmap key
    {
        //first update new key
        UpdateDeleteBitmapRequest update_delete_bitmap_req;
        UpdateDeleteBitmapResponse update_delete_bitmap_res;
        update_delete_bitmap_req.set_cloud_unique_id("test_cloud_unique_id");
        update_delete_bitmap_req.set_table_id(112);
        update_delete_bitmap_req.set_partition_id(123);
        update_delete_bitmap_req.set_lock_id(888);
        update_delete_bitmap_req.set_initiator(-1);
        update_delete_bitmap_req.set_tablet_id(333);
        std::string large_value = generate_random_string(300 * 1000 * 3);
        update_delete_bitmap_req.add_rowset_ids("456");
        update_delete_bitmap_req.add_segment_ids(0);
        update_delete_bitmap_req.add_versions(2);
        update_delete_bitmap_req.add_segment_delete_bitmaps(large_value);
        meta_service->update_delete_bitmap(
                reinterpret_cast<google::protobuf::RpcController*>(&cntl),
                &update_delete_bitmap_req, &update_delete_bitmap_res, nullptr);
        ASSERT_EQ(update_delete_bitmap_res.status().code(), MetaServiceCode::OK);

        GetDeleteBitmapRequest get_delete_bitmap_req;
        GetDeleteBitmapResponse get_delete_bitmap_res;
        get_delete_bitmap_req.set_cloud_unique_id("test_cloud_unique_id");
        get_delete_bitmap_req.set_tablet_id(333);

        get_delete_bitmap_req.add_rowset_ids("456");
        get_delete_bitmap_req.add_begin_versions(2);
        get_delete_bitmap_req.add_end_versions(2);

        meta_service->get_delete_bitmap(reinterpret_cast<google::protobuf::RpcController*>(&cntl),
                                        &get_delete_bitmap_req, &get_delete_bitmap_res, nullptr);
        ASSERT_EQ(get_delete_bitmap_res.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(get_delete_bitmap_res.rowset_ids_size(), 1);
        ASSERT_EQ(get_delete_bitmap_res.segment_delete_bitmaps_size(), 1);
        ASSERT_EQ(get_delete_bitmap_res.versions_size(), 1);
        ASSERT_EQ(get_delete_bitmap_res.segment_delete_bitmaps_size(), 1);

        ASSERT_EQ(get_delete_bitmap_res.rowset_ids(0), "456");
        ASSERT_EQ(get_delete_bitmap_res.segment_ids(0), 0);
        ASSERT_EQ(get_delete_bitmap_res.versions(0), 2);
        ASSERT_EQ(get_delete_bitmap_res.segment_delete_bitmaps(0), large_value);
    }

    RemoveDeleteBitmapUpdateLockRequest remove_lock_req;
    RemoveDeleteBitmapUpdateLockResponse remove_lock_res;
    remove_lock_req.set_cloud_unique_id("test_cloud_unique_id");
    remove_lock_req.set_table_id(112);
    remove_lock_req.set_lock_id(888);
    remove_lock_req.set_initiator(-1);
    meta_service->remove_delete_bitmap_update_lock(
            reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &remove_lock_req,
            &remove_lock_res, nullptr);
    ASSERT_EQ(remove_lock_res.status().code(), MetaServiceCode::OK);

    for (int i = 0; i < 2; ++i) {
        auto lock_id =
                i == 0 ? COMPACTION_DELETE_BITMAP_LOCK_ID : SCHEMA_CHANGE_DELETE_BITMAP_LOCK_ID;
        // case: compaction or schema_change update delete bitmap
        get_lock_req.set_lock_id(lock_id);
        get_lock_req.set_initiator(800);
        meta_service->get_delete_bitmap_update_lock(
                reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &get_lock_req,
                &get_lock_res, nullptr);
        ASSERT_EQ(get_lock_res.status().code(), MetaServiceCode::OK);
        // update delete bitmap
        UpdateDeleteBitmapRequest update_delete_bitmap_req;
        UpdateDeleteBitmapResponse update_delete_bitmap_res;
        update_delete_bitmap_req.set_cloud_unique_id("test_cloud_unique_id");
        update_delete_bitmap_req.set_table_id(112);
        update_delete_bitmap_req.set_partition_id(123);
        update_delete_bitmap_req.set_lock_id(lock_id);
        update_delete_bitmap_req.set_initiator(800);
        update_delete_bitmap_req.set_tablet_id(333);
        update_delete_bitmap_req.add_rowset_ids("123");
        update_delete_bitmap_req.add_segment_ids(0);
        update_delete_bitmap_req.add_versions(2);
        update_delete_bitmap_req.add_segment_delete_bitmaps("compaction0");
        meta_service->update_delete_bitmap(
                reinterpret_cast<google::protobuf::RpcController*>(&cntl),
                &update_delete_bitmap_req, &update_delete_bitmap_res, nullptr);
        ASSERT_EQ(update_delete_bitmap_res.status().code(), MetaServiceCode::OK);
        // remove lock
        remove_lock_req.set_lock_id(lock_id);
        remove_lock_req.set_initiator(800);
        meta_service->remove_delete_bitmap_update_lock(
                reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &remove_lock_req,
                &remove_lock_res, nullptr);
        ASSERT_EQ(remove_lock_res.status().code(), MetaServiceCode::OK);

        // case: compaction or schema_change update delete bitmap with lock expired
        get_lock_req.set_lock_id(lock_id);
        get_lock_req.set_initiator(800);
        get_lock_req.set_expiration(1);
        meta_service->get_delete_bitmap_update_lock(
                reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &get_lock_req,
                &get_lock_res, nullptr);
        ASSERT_EQ(get_lock_res.status().code(), MetaServiceCode::OK);
        // load get lock
        sleep(2);
        get_lock_req.set_lock_id(100);
        get_lock_req.set_initiator(-1);
        get_lock_req.set_expiration(1);
        meta_service->get_delete_bitmap_update_lock(
                reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &get_lock_req,
                &get_lock_res, nullptr);
        ASSERT_EQ(get_lock_res.status().code(), MetaServiceCode::OK);
        // compaction update delete bitmap
        meta_service->update_delete_bitmap(
                reinterpret_cast<google::protobuf::RpcController*>(&cntl),
                &update_delete_bitmap_req, &update_delete_bitmap_res, nullptr);
        ASSERT_EQ(update_delete_bitmap_res.status().code(), MetaServiceCode::LOCK_EXPIRED);

        // case: compaction2 or schema_change2 get lock
        sleep(2);
        get_lock_req.set_lock_id(lock_id);
        get_lock_req.set_initiator(810);
        get_lock_req.set_expiration(1);
        meta_service->get_delete_bitmap_update_lock(
                reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &get_lock_req,
                &get_lock_res, nullptr);
        ASSERT_EQ(get_lock_res.status().code(), MetaServiceCode::OK);
        // compaction1 update delete bitmap
        meta_service->update_delete_bitmap(
                reinterpret_cast<google::protobuf::RpcController*>(&cntl),
                &update_delete_bitmap_req, &update_delete_bitmap_res, nullptr);
        ASSERT_EQ(update_delete_bitmap_res.status().code(), MetaServiceCode::LOCK_EXPIRED);
        // remove compaction2 or or schema_change2 lock
        remove_lock_req.set_lock_id(lock_id);
        remove_lock_req.set_initiator(810);
        meta_service->remove_delete_bitmap_update_lock(
                reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &remove_lock_req,
                &remove_lock_res, nullptr);
        ASSERT_EQ(remove_lock_res.status().code(), MetaServiceCode::OK);
    }

    {
        //compaction update delete bitmap without lock
        UpdateDeleteBitmapRequest update_delete_bitmap_req;
        UpdateDeleteBitmapResponse update_delete_bitmap_res;
        update_delete_bitmap_req.set_cloud_unique_id("test_cloud_unique_id");
        update_delete_bitmap_req.set_table_id(112);
        update_delete_bitmap_req.set_partition_id(123);
        update_delete_bitmap_req.set_without_lock(true);
        update_delete_bitmap_req.set_lock_id(-3);
        update_delete_bitmap_req.set_initiator(-1);
        update_delete_bitmap_req.set_tablet_id(333);
        std::string large_value = generate_random_string(300 * 1000);
        update_delete_bitmap_req.add_rowset_ids("456");
        update_delete_bitmap_req.add_segment_ids(0);
        update_delete_bitmap_req.add_versions(2);
        update_delete_bitmap_req.add_segment_delete_bitmaps(large_value);
        meta_service->update_delete_bitmap(
                reinterpret_cast<google::protobuf::RpcController*>(&cntl),
                &update_delete_bitmap_req, &update_delete_bitmap_res, nullptr);
        ASSERT_EQ(update_delete_bitmap_res.status().code(), MetaServiceCode::OK);

        GetDeleteBitmapRequest get_delete_bitmap_req;
        GetDeleteBitmapResponse get_delete_bitmap_res;
        get_delete_bitmap_req.set_cloud_unique_id("test_cloud_unique_id");
        get_delete_bitmap_req.set_tablet_id(333);

        get_delete_bitmap_req.add_rowset_ids("456");
        get_delete_bitmap_req.add_begin_versions(2);
        get_delete_bitmap_req.add_end_versions(2);

        meta_service->get_delete_bitmap(reinterpret_cast<google::protobuf::RpcController*>(&cntl),
                                        &get_delete_bitmap_req, &get_delete_bitmap_res, nullptr);
        ASSERT_EQ(get_delete_bitmap_res.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(get_delete_bitmap_res.rowset_ids_size(), 1);
        ASSERT_EQ(get_delete_bitmap_res.segment_delete_bitmaps_size(), 1);
        ASSERT_EQ(get_delete_bitmap_res.versions_size(), 1);
        ASSERT_EQ(get_delete_bitmap_res.segment_delete_bitmaps_size(), 1);

        ASSERT_EQ(get_delete_bitmap_res.rowset_ids(0), "456");
        ASSERT_EQ(get_delete_bitmap_res.segment_ids(0), 0);
        ASSERT_EQ(get_delete_bitmap_res.versions(0), 2);
        ASSERT_EQ(get_delete_bitmap_res.segment_delete_bitmaps(0), large_value);
    }

    remove_delete_bitmap_lock(meta_service.get(), 112);
}

TEST(MetaServiceTest, UpdateDeleteBitmap) {
    testUpdateDeleteBitmap(2);
    testUpdateDeleteBitmap(1);
}

TEST(MetaServiceTest, UpdateDeleteBitmapWithException) {
    auto meta_service = get_meta_service();
    brpc::Controller cntl;

    {
        UpdateDeleteBitmapRequest update_delete_bitmap_req;
        UpdateDeleteBitmapResponse update_delete_bitmap_res;
        update_delete_bitmap_req.set_cloud_unique_id("test_cloud_unique_id");
        update_delete_bitmap_req.set_table_id(112);
        update_delete_bitmap_req.set_partition_id(123);
        update_delete_bitmap_req.set_lock_id(888);
        update_delete_bitmap_req.set_initiator(-1);
        update_delete_bitmap_req.set_tablet_id(333);

        update_delete_bitmap_req.add_rowset_ids("123");
        update_delete_bitmap_req.add_segment_ids(1);
        update_delete_bitmap_req.add_versions(2);
        update_delete_bitmap_req.add_segment_delete_bitmaps("abc0");

        meta_service->update_delete_bitmap(
                reinterpret_cast<google::protobuf::RpcController*>(&cntl),
                &update_delete_bitmap_req, &update_delete_bitmap_res, nullptr);
        ASSERT_EQ(update_delete_bitmap_res.status().code(), MetaServiceCode::LOCK_EXPIRED);
    }

    // get delete bitmap update lock
    GetDeleteBitmapUpdateLockRequest get_lock_req;
    GetDeleteBitmapUpdateLockResponse get_lock_res;
    get_lock_req.set_cloud_unique_id("test_cloud_unique_id");
    get_lock_req.set_table_id(112);
    get_lock_req.add_partition_ids(123);
    get_lock_req.set_expiration(5);
    get_lock_req.set_lock_id(888);
    get_lock_req.set_initiator(-1);
    meta_service->get_delete_bitmap_update_lock(
            reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &get_lock_req,
            &get_lock_res, nullptr);
    ASSERT_EQ(get_lock_res.status().code(), MetaServiceCode::OK);

    {
        UpdateDeleteBitmapRequest update_delete_bitmap_req;
        UpdateDeleteBitmapResponse update_delete_bitmap_res;
        update_delete_bitmap_req.set_cloud_unique_id("test_cloud_unique_id");
        update_delete_bitmap_req.set_table_id(112);
        update_delete_bitmap_req.set_partition_id(123);
        update_delete_bitmap_req.set_lock_id(222);
        update_delete_bitmap_req.set_initiator(-1);
        update_delete_bitmap_req.set_tablet_id(333);

        update_delete_bitmap_req.add_rowset_ids("123");
        update_delete_bitmap_req.add_segment_ids(1);
        update_delete_bitmap_req.add_versions(2);
        update_delete_bitmap_req.add_segment_delete_bitmaps("abc0");

        meta_service->update_delete_bitmap(
                reinterpret_cast<google::protobuf::RpcController*>(&cntl),
                &update_delete_bitmap_req, &update_delete_bitmap_res, nullptr);
        ASSERT_EQ(update_delete_bitmap_res.status().code(), MetaServiceCode::LOCK_EXPIRED);
    }

    {
        UpdateDeleteBitmapRequest update_delete_bitmap_req;
        UpdateDeleteBitmapResponse update_delete_bitmap_res;
        update_delete_bitmap_req.set_cloud_unique_id("test_cloud_unique_id");
        update_delete_bitmap_req.set_table_id(112);
        update_delete_bitmap_req.set_partition_id(123);
        update_delete_bitmap_req.set_lock_id(888);
        update_delete_bitmap_req.set_initiator(-2);
        update_delete_bitmap_req.set_tablet_id(333);

        update_delete_bitmap_req.add_rowset_ids("123");
        update_delete_bitmap_req.add_segment_ids(1);
        update_delete_bitmap_req.add_versions(2);
        update_delete_bitmap_req.add_segment_delete_bitmaps("abc0");

        meta_service->update_delete_bitmap(
                reinterpret_cast<google::protobuf::RpcController*>(&cntl),
                &update_delete_bitmap_req, &update_delete_bitmap_res, nullptr);
        ASSERT_EQ(update_delete_bitmap_res.status().code(), MetaServiceCode::LOCK_EXPIRED);
    }
}

void update_delete_bitmap_with_remove_pre(MetaServiceProxy* meta_service, int64_t table_id,
                                          int64_t tablet_id, bool inject = false,
                                          bool rowset_non_exist = false) {
    // create rowset, if `rowset_non_exist` enabled, only r4 exists
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        if (rowset_non_exist) {
            std::string rs_key, rs_val;
            doris::RowsetMetaCloudPB rs;
            rs.set_rowset_id(0);

            rs_key = meta_rowset_key({"test_instance", tablet_id, 3});
            rs.set_rowset_id_v2("r2-3");
            ASSERT_TRUE(rs.SerializeToString(&rs_val));
            txn->put(rs_key, rs_val);

            rs_key = meta_rowset_key({"test_instance", tablet_id, 4});
            rs.set_rowset_id_v2("r4");
            ASSERT_TRUE(rs.SerializeToString(&rs_val));
            txn->put(rs_key, rs_val);
        } else {
            for (int i = 2; i <= 4; i++) {
                std::string rs_key, rs_val;
                MetaRowsetKeyInfo rs_key_info {"test_instance", tablet_id, i};
                meta_rowset_key(rs_key_info, &rs_key);
                doris::RowsetMetaCloudPB rs;
                rs.set_rowset_id(0);
                rs.set_rowset_id_v2("r" + std::to_string(i));
                ASSERT_TRUE(rs.SerializeToString(&rs_val));
                txn->put(rs_key, rs_val);
            }
        }
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }
    brpc::Controller cntl;
    // compaction update delete bitmap with remove pre rowset delete bitmaps
    // get update lock
    GetDeleteBitmapUpdateLockRequest get_lock_req;
    GetDeleteBitmapUpdateLockResponse get_lock_res;
    get_lock_req.set_cloud_unique_id("test_cloud_unique_id");
    get_lock_req.set_table_id(table_id);
    get_lock_req.set_lock_id(-1);
    get_lock_req.set_initiator(203);
    get_lock_req.set_expiration(10);
    meta_service->get_delete_bitmap_update_lock(
            reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &get_lock_req,
            &get_lock_res, nullptr);
    ASSERT_EQ(get_lock_res.status().code(), MetaServiceCode::OK);
    // write delete bitmap
    UpdateDeleteBitmapRequest update_delete_bitmap_req;
    UpdateDeleteBitmapResponse update_delete_bitmap_res;
    update_delete_bitmap_req.set_cloud_unique_id("test_cloud_unique_id");
    update_delete_bitmap_req.set_table_id(table_id);
    update_delete_bitmap_req.set_partition_id(201);
    update_delete_bitmap_req.set_tablet_id(tablet_id);
    update_delete_bitmap_req.set_lock_id(-1);
    update_delete_bitmap_req.set_initiator(203);
    std::string large_value = generate_random_string(300 * 1000);
    std::vector<std::tuple<std::string, int64_t, int64_t>> rowset_segment_version_vector = {
            /* r2-0 */ {"r2", 0, 3}, {"r2", 0, 4}, {"r2", 0, 5}, {"r2", 0, 6},
            /* r3-0 */ {"r3", 0, 4}, {"r3", 0, 5}, {"r3", 0, 6},
            /* r3-1 */ {"r3", 1, 4}, {"r3", 1, 5},
            /* r3-2 */ {"r3", 2, 4}, {"r3", 2, 6},
            /* r4-0 */ {"r4", 0, 5}, {"r4", 0, 6}};
    for (const auto& [rowset, segment, version] : rowset_segment_version_vector) {
        update_delete_bitmap_req.add_rowset_ids(rowset);
        update_delete_bitmap_req.add_segment_ids(segment);
        update_delete_bitmap_req.add_versions(version);
        update_delete_bitmap_req.add_segment_delete_bitmaps(large_value);
    }
    meta_service->update_delete_bitmap(reinterpret_cast<google::protobuf::RpcController*>(&cntl),
                                       &update_delete_bitmap_req, &update_delete_bitmap_res,
                                       nullptr);
    ASSERT_EQ(update_delete_bitmap_res.status().code(), MetaServiceCode::OK);
    // remove delete bitmap lock
    RemoveDeleteBitmapUpdateLockRequest remove_lock_req;
    RemoveDeleteBitmapUpdateLockResponse remove_lock_res;
    remove_lock_req.set_cloud_unique_id("test_cloud_unique_id");
    remove_lock_req.set_table_id(table_id);
    remove_lock_req.set_lock_id(-1);
    remove_lock_req.set_initiator(203);
    meta_service->remove_delete_bitmap_update_lock(
            reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &remove_lock_req,
            &remove_lock_res, nullptr);
    ASSERT_EQ(remove_lock_res.status().code(), MetaServiceCode::OK);
    // get delete bitmap
    GetDeleteBitmapRequest get_delete_bitmap_req;
    GetDeleteBitmapResponse get_delete_bitmap_res;
    get_delete_bitmap_req.set_cloud_unique_id("test_cloud_unique_id");
    get_delete_bitmap_req.set_tablet_id(tablet_id);
    std::vector<std::string> rowset_vector = {"r2", "r3", "r4"};
    for (const auto& rowset : rowset_vector) {
        get_delete_bitmap_req.add_rowset_ids(rowset);
        get_delete_bitmap_req.add_begin_versions(0);
        get_delete_bitmap_req.add_end_versions(6);
    }
    meta_service->get_delete_bitmap(reinterpret_cast<google::protobuf::RpcController*>(&cntl),
                                    &get_delete_bitmap_req, &get_delete_bitmap_res, nullptr);
    ASSERT_EQ(get_delete_bitmap_res.status().code(), MetaServiceCode::OK);
    auto size = rowset_segment_version_vector.size();
    ASSERT_EQ(get_delete_bitmap_res.rowset_ids_size(), size);
    ASSERT_EQ(get_delete_bitmap_res.segment_delete_bitmaps_size(), size);
    ASSERT_EQ(get_delete_bitmap_res.versions_size(), size);
    ASSERT_EQ(get_delete_bitmap_res.segment_delete_bitmaps_size(), size);
    // update pre rowset delete bitmap
    update_delete_bitmap_req.clear_rowset_ids();
    update_delete_bitmap_req.clear_segment_ids();
    update_delete_bitmap_req.clear_versions();
    update_delete_bitmap_req.clear_segment_delete_bitmaps();
    update_delete_bitmap_req.set_lock_id(-3);
    update_delete_bitmap_req.set_without_lock(true);
    update_delete_bitmap_req.set_initiator(tablet_id);
    update_delete_bitmap_req.set_pre_rowset_agg_start_version(4);
    update_delete_bitmap_req.set_pre_rowset_agg_end_version(6);
    std::vector<std::tuple<std::string, int64_t, int64_t, int64_t>>
            new_rowset_segment_version_vector = {/* r2-0 */ {"r2", 0, 6, 2},
                                                 /* r3-0 */ {"r3", 0, 6, 3},
                                                 /* r3-1 */ {"r3", 1, 6, 3},
                                                 /* r3-2 */ {"r3", 2, 6, 3},
                                                 /* r4-0 */ {"r4", 0, 6, 4}};
    std::string new_large_value = generate_random_string(300 * 1000);
    for (const auto& [rowset, segment, version, rowset_version] :
         new_rowset_segment_version_vector) {
        update_delete_bitmap_req.add_rowset_ids(rowset);
        update_delete_bitmap_req.add_segment_ids(segment);
        update_delete_bitmap_req.add_versions(version);
        update_delete_bitmap_req.add_segment_delete_bitmaps(new_large_value);
        update_delete_bitmap_req.add_pre_rowset_versions(rowset_version);
    }
    meta_service->update_delete_bitmap(reinterpret_cast<google::protobuf::RpcController*>(&cntl),
                                       &update_delete_bitmap_req, &update_delete_bitmap_res,
                                       nullptr);
    ASSERT_EQ(update_delete_bitmap_res.status().code(),
              inject ? MetaServiceCode::KV_TXN_CONFLICT : MetaServiceCode::OK);
    // get delete bitmap again
    meta_service->get_delete_bitmap(reinterpret_cast<google::protobuf::RpcController*>(&cntl),
                                    &get_delete_bitmap_req, &get_delete_bitmap_res, nullptr);
    ASSERT_EQ(get_delete_bitmap_res.status().code(), MetaServiceCode::OK);
    size = 6;
    if (inject) {
        size = 13;
    } else if (rowset_non_exist) {
        size = 12;
    }
    ASSERT_EQ(get_delete_bitmap_res.rowset_ids_size(), size);
    ASSERT_EQ(get_delete_bitmap_res.segment_delete_bitmaps_size(), size);
    ASSERT_EQ(get_delete_bitmap_res.versions_size(), size);
    ASSERT_EQ(get_delete_bitmap_res.segment_delete_bitmaps_size(), size);
    std::vector<std::tuple<std::string, int64_t, int64_t, std::string>> expected_dm;
    if (inject) {
        expected_dm = {/* r2-0 */ {"r2", 0, 3, large_value},
                       {"r2", 0, 4, large_value},
                       {"r2", 0, 5, large_value},
                       {"r2", 0, 6, new_large_value},
                       /* r3-0 is agg */ {"r3", 0, 4, large_value},
                       {"r3", 1, 4, large_value},
                       {"r3", 2, 4, large_value},
                       {"r3", 0, 5, large_value},
                       {"r3", 1, 5, large_value},
                       {"r3", 0, 6, new_large_value},
                       {"r3", 2, 6, large_value},
                       /* r4-0 */ {"r4", 0, 5, large_value},
                       {"r4", 0, 6, large_value}};
    } else if (rowset_non_exist) {
        expected_dm = {/* r2-0 */ {"r2", 0, 3, large_value},
                       {"r2", 0, 4, large_value},
                       {"r2", 0, 5, large_value},
                       {"r2", 0, 6, large_value},
                       /* r3-0 */ {"r3", 0, 4, large_value},
                       {"r3", 1, 4, large_value},
                       {"r3", 2, 4, large_value},
                       {"r3", 0, 5, large_value},
                       {"r3", 1, 5, large_value},
                       {"r3", 0, 6, large_value},
                       {"r3", 2, 6, large_value},
                       /* r4-0 */ {"r4", 0, 6, new_large_value}};
    } else {
        expected_dm = {/* r2-0 */ {"r2", 0, 3, large_value},
                       {"r2", 0, 6, new_large_value},
                       /* r3-0 */ {"r3", 0, 6, new_large_value},
                       /* r3-1 */ {"r3", 1, 6, new_large_value},
                       /* r3-2 */ {"r3", 2, 6, new_large_value},
                       /* r4-0 */ {"r4", 0, 6, new_large_value}};
    }
    for (size_t i = 0; i < get_delete_bitmap_res.rowset_ids_size(); i++) {
        ASSERT_EQ(get_delete_bitmap_res.rowset_ids(i), std::get<0>(expected_dm[i]));
        ASSERT_EQ(get_delete_bitmap_res.segment_ids(i), std::get<1>(expected_dm[i]));
        ASSERT_EQ(get_delete_bitmap_res.versions(i), std::get<2>(expected_dm[i]));
        ASSERT_EQ(get_delete_bitmap_res.segment_delete_bitmaps(i), std::get<3>(expected_dm[i]));
    }
}

TEST(MetaServiceTest, UpdateDeleteBitmapWithRemovePreDeleteBitmap) {
    auto meta_service = get_meta_service();
    [[maybe_unused]] auto sp = SyncPoint::get_instance();
    DORIS_CLOUD_DEFER {
        SyncPoint::get_instance()->clear_all_call_backs();
    };

    update_delete_bitmap_with_remove_pre(meta_service.get(), 200, 202);

    int64_t max_txn_commit_byte = config::max_txn_commit_byte;
    config::max_txn_commit_byte = 1000;
    update_delete_bitmap_with_remove_pre(meta_service.get(), 300, 302);

    sp->set_call_back("update_delete_bitmap:commit:err", [&](auto&& args) {
        auto initiator = try_any_cast<int64_t>(args[0]);
        auto i = try_any_cast<size_t>(args[1]);
        if (initiator == 402 && i == 2) {
            *try_any_cast<TxnErrorCode*>(args[2]) = TxnErrorCode::TXN_CONFLICT;
        }
    });
    sp->enable_processing();
    update_delete_bitmap_with_remove_pre(meta_service.get(), 400, 402, true);
    sp->clear_all_call_backs();
    sp->clear_trace();
    sp->disable_processing();
    config::max_txn_commit_byte = max_txn_commit_byte;

    update_delete_bitmap_with_remove_pre(meta_service.get(), 500, 502, false, true);
}

TEST(MetaServiceTest, GetDeleteBitmapWithIdx) {
    auto meta_service = get_meta_service();
    extern std::string get_instance_id(const std::shared_ptr<ResourceManager>& rc_mgr,
                                       const std::string& cloud_unique_id);
    auto instance_id = get_instance_id(meta_service->resource_mgr(), "test_cloud_unique_id");
    int64_t db_id = 1;
    int64_t table_id = 1;
    int64_t index_id = 1;
    int64_t partition_id = 1;
    int64_t tablet_id = 123;

    brpc::Controller cntl;
    GetDeleteBitmapRequest req;
    GetDeleteBitmapResponse res;
    req.set_cloud_unique_id("test_cloud_unique_id");
    req.set_tablet_id(tablet_id);
    TabletIndexPB idx;
    idx.set_tablet_id(tablet_id);
    idx.set_index_id(index_id);
    idx.set_db_id(db_id);
    idx.set_partition_id(partition_id);
    idx.set_table_id(table_id);
    *(req.mutable_idx()) = idx;
    req.set_base_compaction_cnt(9);
    req.set_cumulative_compaction_cnt(19);
    req.set_cumulative_point(21);

    meta_service->get_delete_bitmap(reinterpret_cast<google::protobuf::RpcController*>(&cntl), &req,
                                    &res, nullptr);
    EXPECT_EQ(res.status().code(), MetaServiceCode::TABLET_NOT_FOUND);

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string stats_key =
            stats_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
    TabletStatsPB stats;
    stats.set_base_compaction_cnt(9);
    stats.set_cumulative_compaction_cnt(19);
    stats.set_cumulative_point(20);
    txn->put(stats_key, stats.SerializeAsString());
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    meta_service->get_delete_bitmap(reinterpret_cast<google::protobuf::RpcController*>(&cntl), &req,
                                    &res, nullptr);
    EXPECT_EQ(res.status().code(), MetaServiceCode::ROWSETS_EXPIRED);

    req.set_cumulative_point(20);
    meta_service->get_delete_bitmap(reinterpret_cast<google::protobuf::RpcController*>(&cntl), &req,
                                    &res, nullptr);
    EXPECT_EQ(res.status().code(), MetaServiceCode::OK);

    req.add_rowset_ids("1234");
    req.add_begin_versions(1);
    req.add_end_versions(2);
    req.add_end_versions(3);
    meta_service->get_delete_bitmap(reinterpret_cast<google::protobuf::RpcController*>(&cntl), &req,
                                    &res, nullptr);
    EXPECT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
}

TEST(MetaServiceTest, DeleteBimapCommitTxnTest) {
    auto meta_service = get_meta_service();
    extern std::string get_instance_id(const std::shared_ptr<ResourceManager>& rc_mgr,
                                       const std::string& cloud_unique_id);
    auto instance_id = get_instance_id(meta_service->resource_mgr(), "test_cloud_unique_id");

    // case: first version of rowset
    {
        int64_t txn_id = 98765;
        int64_t table_id = 123456; // same as table_id of tmp rowset
        int64_t db_id = 222;
        int64_t tablet_id_base = 8113;
        int64_t partition_id = 1234;
        // begin txn
        {
            brpc::Controller cntl;
            BeginTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            TxnInfoPB txn_info_pb;
            txn_info_pb.set_db_id(db_id);
            txn_info_pb.set_label("test_label");
            txn_info_pb.add_table_ids(table_id);
            txn_info_pb.set_timeout_ms(36000);
            req.mutable_txn_info()->CopyFrom(txn_info_pb);
            BeginTxnResponse res;
            meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            txn_id = res.txn_id();
        }

        // mock rowset and tablet
        for (int i = 0; i < 5; ++i) {
            create_tablet(meta_service.get(), table_id, 1235, partition_id, tablet_id_base + i);
            auto tmp_rowset = create_rowset(txn_id, tablet_id_base + i);
            tmp_rowset.set_partition_id(partition_id);
            CreateRowsetResponse res;
            commit_rowset(meta_service.get(), tmp_rowset, res);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        }

        // update delete bitmap
        {
            // get delete bitmap update lock
            brpc::Controller cntl;
            GetDeleteBitmapUpdateLockRequest get_lock_req;
            GetDeleteBitmapUpdateLockResponse get_lock_res;
            get_lock_req.set_cloud_unique_id("test_cloud_unique_id");
            get_lock_req.set_table_id(table_id);
            get_lock_req.add_partition_ids(partition_id);
            get_lock_req.set_expiration(5);
            get_lock_req.set_lock_id(txn_id);
            get_lock_req.set_initiator(-1);
            meta_service->get_delete_bitmap_update_lock(
                    reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &get_lock_req,
                    &get_lock_res, nullptr);
            ASSERT_EQ(get_lock_res.status().code(), MetaServiceCode::OK);

            // first update delete bitmap
            UpdateDeleteBitmapRequest update_delete_bitmap_req;
            UpdateDeleteBitmapResponse update_delete_bitmap_res;
            update_delete_bitmap_req.set_cloud_unique_id("test_cloud_unique_id");
            update_delete_bitmap_req.set_table_id(table_id);
            update_delete_bitmap_req.set_partition_id(partition_id);
            update_delete_bitmap_req.set_lock_id(txn_id);
            update_delete_bitmap_req.set_initiator(-1);
            update_delete_bitmap_req.set_tablet_id(tablet_id_base);

            update_delete_bitmap_req.add_rowset_ids("123");
            update_delete_bitmap_req.add_segment_ids(1);
            update_delete_bitmap_req.add_versions(2);
            update_delete_bitmap_req.add_segment_delete_bitmaps("abc0");

            meta_service->update_delete_bitmap(
                    reinterpret_cast<google::protobuf::RpcController*>(&cntl),
                    &update_delete_bitmap_req, &update_delete_bitmap_res, nullptr);
            ASSERT_EQ(update_delete_bitmap_res.status().code(), MetaServiceCode::OK);
        }

        // check delete bitmap update lock and pending delete bitmap
        {
            std::unique_ptr<Transaction> txn;
            ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
            std::string lock_key = meta_delete_bitmap_update_lock_key({instance_id, table_id, -1});
            std::string lock_val;
            auto ret = txn->get(lock_key, &lock_val);
            ASSERT_EQ(ret, TxnErrorCode::TXN_OK);
            DeleteBitmapUpdateLockPB lock_info;
            ASSERT_TRUE(lock_info.ParseFromString(lock_val));

            std::string pending_key = meta_pending_delete_bitmap_key({instance_id, tablet_id_base});
            std::string pending_val;
            ret = txn->get(pending_key, &pending_val);
            ASSERT_EQ(ret, TxnErrorCode::TXN_OK);
            PendingDeleteBitmapPB pending_info;
            ASSERT_TRUE(pending_info.ParseFromString(pending_val));
        }

        // commit txn
        {
            brpc::Controller cntl;
            CommitTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            req.set_db_id(db_id);
            req.set_txn_id(txn_id);
            req.add_mow_table_ids(table_id);
            CommitTxnResponse res;
            meta_service->commit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        }

        // check delete bitmap update lock and pending delete bitmap
        {
            std::unique_ptr<Transaction> txn;
            ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
            std::string lock_key = meta_delete_bitmap_update_lock_key({instance_id, table_id, -1});
            std::string lock_val;
            auto ret = txn->get(lock_key, &lock_val);
            ASSERT_EQ(ret, TxnErrorCode::TXN_KEY_NOT_FOUND);

            std::string pending_key = meta_pending_delete_bitmap_key({instance_id, tablet_id_base});
            std::string pending_val;
            ret = txn->get(pending_key, &pending_val);
            ASSERT_EQ(ret, TxnErrorCode::TXN_KEY_NOT_FOUND);
        }
    }
}

TEST(MetaServiceTest, GetDeleteBitmapWithRetryTest1) {
    auto meta_service = get_meta_service();
    SyncPoint::get_instance()->enable_processing();
    size_t index = 0;
    SyncPoint::get_instance()->set_call_back("get_delete_bitmap_code", [&](auto&& args) {
        LOG(INFO) << "GET_DELETE_BITMAP_CODE,index=" << index;
        if (++index < 2) {
            *doris::try_any_cast<MetaServiceCode*>(args[0]) = MetaServiceCode::KV_TXN_TOO_OLD;
        }
    });

    // get delete bitmap update lock
    brpc::Controller cntl;
    GetDeleteBitmapUpdateLockRequest get_lock_req;
    GetDeleteBitmapUpdateLockResponse get_lock_res;
    get_lock_req.set_cloud_unique_id("test_cloud_unique_id");
    get_lock_req.set_table_id(100);
    get_lock_req.add_partition_ids(123);
    get_lock_req.set_expiration(5);
    get_lock_req.set_lock_id(888);
    get_lock_req.set_initiator(-1);
    meta_service->get_delete_bitmap_update_lock(
            reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &get_lock_req,
            &get_lock_res, nullptr);
    ASSERT_EQ(get_lock_res.status().code(), MetaServiceCode::OK);

    //first update new key
    UpdateDeleteBitmapRequest update_delete_bitmap_req;
    UpdateDeleteBitmapResponse update_delete_bitmap_res;
    update_delete_bitmap_req.set_cloud_unique_id("test_cloud_unique_id");
    update_delete_bitmap_req.set_table_id(100);
    update_delete_bitmap_req.set_partition_id(123);
    update_delete_bitmap_req.set_lock_id(888);
    update_delete_bitmap_req.set_initiator(-1);
    update_delete_bitmap_req.set_tablet_id(333);
    std::string large_value = generate_random_string(300 * 1000 * 3);
    update_delete_bitmap_req.add_rowset_ids("456");
    update_delete_bitmap_req.add_segment_ids(0);
    update_delete_bitmap_req.add_versions(2);
    update_delete_bitmap_req.add_segment_delete_bitmaps(large_value);
    meta_service->update_delete_bitmap(reinterpret_cast<google::protobuf::RpcController*>(&cntl),
                                       &update_delete_bitmap_req, &update_delete_bitmap_res,
                                       nullptr);
    ASSERT_EQ(update_delete_bitmap_res.status().code(), MetaServiceCode::OK);

    GetDeleteBitmapRequest get_delete_bitmap_req;
    GetDeleteBitmapResponse get_delete_bitmap_res;
    get_delete_bitmap_req.set_cloud_unique_id("test_cloud_unique_id");
    get_delete_bitmap_req.set_tablet_id(333);

    get_delete_bitmap_req.add_rowset_ids("456");
    get_delete_bitmap_req.add_begin_versions(2);
    get_delete_bitmap_req.add_end_versions(2);

    meta_service->get_delete_bitmap(reinterpret_cast<google::protobuf::RpcController*>(&cntl),
                                    &get_delete_bitmap_req, &get_delete_bitmap_res, nullptr);
    ASSERT_EQ(get_delete_bitmap_res.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(get_delete_bitmap_res.rowset_ids_size(), 1);
    ASSERT_EQ(get_delete_bitmap_res.segment_ids_size(), 1);
    ASSERT_EQ(get_delete_bitmap_res.versions_size(), 1);
    ASSERT_EQ(get_delete_bitmap_res.segment_delete_bitmaps_size(), 1);

    ASSERT_EQ(get_delete_bitmap_res.rowset_ids(0), "456");
    ASSERT_EQ(get_delete_bitmap_res.segment_ids(0), 0);
    ASSERT_EQ(get_delete_bitmap_res.versions(0), 2);
    ASSERT_EQ(get_delete_bitmap_res.segment_delete_bitmaps(0), large_value);

    SyncPoint::get_instance()->disable_processing();
    SyncPoint::get_instance()->clear_all_call_backs();
}

TEST(MetaServiceTest, GetDeleteBitmapWithRetryTest2) {
    auto meta_service = get_meta_service();
    SyncPoint::get_instance()->enable_processing();
    size_t index = 0;
    SyncPoint::get_instance()->set_call_back("get_delete_bitmap_test", [&](auto&& args) {
        auto* test = try_any_cast<bool*>(args[0]);
        *test = true;
        LOG(INFO) << "GET_DELETE_BITMAP_TEST, test=" << *test;
    });
    SyncPoint::get_instance()->set_call_back("get_delete_bitmap_err", [&](auto&& args) {
        auto* round = try_any_cast<int64_t*>(args[0]);
        LOG(INFO) << "GET_DELETE_BITMAP_CODE,index=" << index << ",round=" << *round;
        if (*round > 2 && ++index < 2) {
            *try_any_cast<TxnErrorCode*>(args[1]) = TxnErrorCode::TXN_TOO_OLD;
        }
    });

    // get delete bitmap update lock
    brpc::Controller cntl;
    GetDeleteBitmapUpdateLockRequest get_lock_req;
    GetDeleteBitmapUpdateLockResponse get_lock_res;
    get_lock_req.set_cloud_unique_id("test_cloud_unique_id");
    get_lock_req.set_table_id(100);
    get_lock_req.add_partition_ids(123);
    get_lock_req.set_expiration(5);
    get_lock_req.set_lock_id(888);
    get_lock_req.set_initiator(-1);
    meta_service->get_delete_bitmap_update_lock(
            reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &get_lock_req,
            &get_lock_res, nullptr);
    ASSERT_EQ(get_lock_res.status().code(), MetaServiceCode::OK);

    //first update new key
    UpdateDeleteBitmapRequest update_delete_bitmap_req;
    UpdateDeleteBitmapResponse update_delete_bitmap_res;
    update_delete_bitmap_req.set_cloud_unique_id("test_cloud_unique_id");
    update_delete_bitmap_req.set_table_id(100);
    update_delete_bitmap_req.set_partition_id(123);
    update_delete_bitmap_req.set_lock_id(888);
    update_delete_bitmap_req.set_initiator(-1);
    update_delete_bitmap_req.set_tablet_id(333);
    std::string rowset_id = "456";
    std::string segment_delete_bitmaps[5];
    for (int i = 0; i < 5; i++) {
        segment_delete_bitmaps[i] = generate_random_string(300 * 1000 * 3);
    }
    int count = 5;
    for (int i = 0; i < count; i++) {
        update_delete_bitmap_req.add_rowset_ids(rowset_id);
        update_delete_bitmap_req.add_segment_ids(i);
        update_delete_bitmap_req.add_versions(i + 1);
        update_delete_bitmap_req.add_segment_delete_bitmaps(segment_delete_bitmaps[i]);
    }
    meta_service->update_delete_bitmap(reinterpret_cast<google::protobuf::RpcController*>(&cntl),
                                       &update_delete_bitmap_req, &update_delete_bitmap_res,
                                       nullptr);
    ASSERT_EQ(update_delete_bitmap_res.status().code(), MetaServiceCode::OK);

    GetDeleteBitmapRequest get_delete_bitmap_req;
    GetDeleteBitmapResponse get_delete_bitmap_res;
    get_delete_bitmap_req.set_cloud_unique_id("test_cloud_unique_id");
    get_delete_bitmap_req.set_tablet_id(333);

    get_delete_bitmap_req.add_rowset_ids(rowset_id);
    get_delete_bitmap_req.add_begin_versions(1);
    get_delete_bitmap_req.add_end_versions(count);

    meta_service->get_delete_bitmap(reinterpret_cast<google::protobuf::RpcController*>(&cntl),
                                    &get_delete_bitmap_req, &get_delete_bitmap_res, nullptr);
    ASSERT_EQ(get_delete_bitmap_res.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(get_delete_bitmap_res.rowset_ids_size(), count);
    ASSERT_EQ(get_delete_bitmap_res.segment_ids_size(), count);
    ASSERT_EQ(get_delete_bitmap_res.versions_size(), count);
    ASSERT_EQ(get_delete_bitmap_res.segment_delete_bitmaps_size(), count);

    for (int i = 0; i < count; i++) {
        ASSERT_EQ(get_delete_bitmap_res.rowset_ids(i), rowset_id);
        ASSERT_EQ(get_delete_bitmap_res.segment_ids(i), i);
        ASSERT_EQ(get_delete_bitmap_res.versions(i), i + 1);
        ASSERT_EQ(get_delete_bitmap_res.segment_delete_bitmaps(i), segment_delete_bitmaps[i]);
    }
    SyncPoint::get_instance()->disable_processing();
    SyncPoint::get_instance()->clear_all_call_backs();
}

TEST(MetaServiceTest, GetDeleteBitmapWithRetryTest3) {
    auto meta_service = get_meta_service();
    SyncPoint::get_instance()->enable_processing();
    size_t index = 0;
    SyncPoint::get_instance()->set_call_back("get_delete_bitmap_err", [&](auto&& args) {
        auto* round = try_any_cast<int64_t*>(args[0]);
        LOG(INFO) << "GET_DELETE_BITMAP_CODE,index=" << index << ",round=" << *round;
        if (*round > 2 && ++index < 2) {
            *try_any_cast<TxnErrorCode*>(args[1]) = TxnErrorCode::TXN_TOO_OLD;
        }
    });

    // get delete bitmap update lock
    brpc::Controller cntl;
    GetDeleteBitmapUpdateLockRequest get_lock_req;
    GetDeleteBitmapUpdateLockResponse get_lock_res;
    get_lock_req.set_cloud_unique_id("test_cloud_unique_id");
    get_lock_req.set_table_id(100);
    get_lock_req.add_partition_ids(123);
    get_lock_req.set_expiration(5);
    get_lock_req.set_lock_id(888);
    get_lock_req.set_initiator(-1);
    meta_service->get_delete_bitmap_update_lock(
            reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &get_lock_req,
            &get_lock_res, nullptr);
    ASSERT_EQ(get_lock_res.status().code(), MetaServiceCode::OK);

    //first update new key
    UpdateDeleteBitmapRequest update_delete_bitmap_req;
    UpdateDeleteBitmapResponse update_delete_bitmap_res;
    update_delete_bitmap_req.set_cloud_unique_id("test_cloud_unique_id");
    update_delete_bitmap_req.set_table_id(100);
    update_delete_bitmap_req.set_partition_id(123);
    update_delete_bitmap_req.set_lock_id(888);
    update_delete_bitmap_req.set_initiator(-1);
    update_delete_bitmap_req.set_tablet_id(333);
    std::string rowset_id = "456";
    std::string segment_delete_bitmaps[5];
    for (int i = 0; i < 5; i++) {
        segment_delete_bitmaps[i] = generate_random_string(300 * 1000 * 3);
    }
    int count = 5;
    for (int i = 0; i < count; i++) {
        update_delete_bitmap_req.add_rowset_ids(rowset_id);
        update_delete_bitmap_req.add_segment_ids(i);
        update_delete_bitmap_req.add_versions(i + 1);
        update_delete_bitmap_req.add_segment_delete_bitmaps(segment_delete_bitmaps[i]);
    }
    meta_service->update_delete_bitmap(reinterpret_cast<google::protobuf::RpcController*>(&cntl),
                                       &update_delete_bitmap_req, &update_delete_bitmap_res,
                                       nullptr);
    ASSERT_EQ(update_delete_bitmap_res.status().code(), MetaServiceCode::OK);

    GetDeleteBitmapRequest get_delete_bitmap_req;
    GetDeleteBitmapResponse get_delete_bitmap_res;
    get_delete_bitmap_req.set_cloud_unique_id("test_cloud_unique_id");
    get_delete_bitmap_req.set_tablet_id(333);

    get_delete_bitmap_req.add_rowset_ids(rowset_id);
    get_delete_bitmap_req.add_begin_versions(1);
    get_delete_bitmap_req.add_end_versions(count);

    meta_service->get_delete_bitmap(reinterpret_cast<google::protobuf::RpcController*>(&cntl),
                                    &get_delete_bitmap_req, &get_delete_bitmap_res, nullptr);
    ASSERT_EQ(get_delete_bitmap_res.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(get_delete_bitmap_res.rowset_ids_size(), count);
    ASSERT_EQ(get_delete_bitmap_res.segment_ids_size(), count);
    ASSERT_EQ(get_delete_bitmap_res.versions_size(), count);
    ASSERT_EQ(get_delete_bitmap_res.segment_delete_bitmaps_size(), count);

    for (int i = 0; i < count; i++) {
        ASSERT_EQ(get_delete_bitmap_res.rowset_ids(i), rowset_id);
        ASSERT_EQ(get_delete_bitmap_res.segment_ids(i), i);
        ASSERT_EQ(get_delete_bitmap_res.versions(i), i + 1);
        ASSERT_EQ(get_delete_bitmap_res.segment_delete_bitmaps(i), segment_delete_bitmaps[i]);
    }
    SyncPoint::get_instance()->disable_processing();
    SyncPoint::get_instance()->clear_all_call_backs();
}

TEST(MetaServiceTest, GetVersion) {
    auto service = get_meta_service();

    int64_t table_id = 1;
    int64_t partition_id = 1;
    int64_t tablet_id = 1;

    // INVALID_ARGUMENT
    {
        brpc::Controller ctrl;
        GetVersionRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_table_id(table_id);
        req.set_partition_id(partition_id);

        GetVersionResponse resp;
        service->get_version(&ctrl, &req, &resp, nullptr);

        ASSERT_EQ(resp.status().code(), MetaServiceCode::INVALID_ARGUMENT)
                << " status is " << resp.status().DebugString();
    }

    {
        brpc::Controller ctrl;
        GetVersionRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_db_id(1);
        req.set_table_id(table_id);
        req.set_partition_id(partition_id);

        GetVersionResponse resp;
        service->get_version(&ctrl, &req, &resp, nullptr);

        ASSERT_EQ(resp.status().code(), MetaServiceCode::VERSION_NOT_FOUND)
                << " status is " << resp.status().DebugString();
    }

    create_tablet(service.get(), table_id, 1, partition_id, tablet_id);
    insert_rowset(service.get(), 1, "get_version_label_1", table_id, partition_id, tablet_id);

    {
        brpc::Controller ctrl;
        GetVersionRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_db_id(1);
        req.set_table_id(table_id);
        req.set_partition_id(partition_id);

        GetVersionResponse resp;
        service->get_version(&ctrl, &req, &resp, nullptr);

        ASSERT_EQ(resp.status().code(), MetaServiceCode::OK)
                << " status is " << resp.status().DebugString();
        ASSERT_EQ(resp.version(), 2);
    }
}

TEST(MetaServiceTest, BatchGetVersion) {
    struct TestCase {
        std::vector<int64_t> table_ids;
        std::vector<int64_t> partition_ids;
        std::vector<int64_t> expected_versions;
        std::vector<
                std::tuple<int64_t /*table_id*/, int64_t /*partition_id*/, int64_t /*tablet_id*/>>
                insert_rowsets;
    };

    // table ids: 2, 3, 4, 5
    // partition ids: 6, 7, 8, 9
    std::vector<TestCase> cases = {
            // all version are missing
            {{1, 2, 3, 4}, {6, 7, 8, 9}, {-1, -1, -1, -1}, {}},
            // update table 1, partition 6
            {{1, 2, 3, 4}, {6, 7, 8, 9}, {2, -1, -1, -1}, {{1, 6, 1}}},
            // update table 2, partition 6
            // update table 3, partition 7
            {{1, 2, 3, 4}, {6, 7, 8, 9}, {2, -1, 2, 2}, {{3, 8, 3}, {4, 9, 4}}},
            // update table 1, partition 7 twice
            {{1, 2, 3, 4}, {6, 7, 8, 9}, {2, 3, 2, 2}, {{2, 7, 2}, {2, 7, 2}}},
    };

    auto service = get_meta_service();
    create_tablet(service.get(), 1, 1, 6, 1);
    create_tablet(service.get(), 2, 1, 7, 2);
    create_tablet(service.get(), 3, 1, 8, 3);
    create_tablet(service.get(), 4, 1, 9, 4);

    size_t num_cases = cases.size();
    size_t label_index = 0;
    for (size_t i = 0; i < num_cases; ++i) {
        auto& [table_ids, partition_ids, expected_versions, insert_rowsets] = cases[i];
        for (auto [table_id, partition_id, tablet_id] : insert_rowsets) {
            LOG(INFO) << "insert rowset for table " << table_id << " partition " << partition_id
                      << " tablet_id " << tablet_id;
            insert_rowset(service.get(), 1, std::to_string(++label_index), table_id, partition_id,
                          tablet_id);
        }

        brpc::Controller ctrl;
        GetVersionRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_db_id(-1);
        req.set_table_id(-1);
        req.set_partition_id(-1);
        req.set_batch_mode(true);
        for (size_t i = 0; i < table_ids.size(); ++i) req.add_db_ids(1);
        std::copy(table_ids.begin(), table_ids.end(),
                  google::protobuf::RepeatedFieldBackInserter(req.mutable_table_ids()));
        std::copy(partition_ids.begin(), partition_ids.end(),
                  google::protobuf::RepeatedFieldBackInserter(req.mutable_partition_ids()));

        GetVersionResponse resp;
        service->get_version(&ctrl, &req, &resp, nullptr);

        ASSERT_EQ(resp.status().code(), MetaServiceCode::OK)
                << "case " << i << " status is " << resp.status().msg()
                << ", code=" << resp.status().code();

        std::vector<int64_t> versions(resp.versions().begin(), resp.versions().end());
        EXPECT_EQ(versions, expected_versions) << "case " << i;
    }

    // INVALID_ARGUMENT
    {
        brpc::Controller ctrl;
        GetVersionRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_batch_mode(true);
        GetVersionResponse resp;
        service->get_version(&ctrl, &req, &resp, nullptr);
        ASSERT_EQ(resp.status().code(), MetaServiceCode::INVALID_ARGUMENT)
                << " status is " << resp.status().msg() << ", code=" << resp.status().code();
    }
}

TEST(MetaServiceTest, BatchGetVersionFallback) {
    constexpr size_t N = 100;
    size_t i = 0;
    auto sp = SyncPoint::get_instance();
    DORIS_CLOUD_DEFER {
        SyncPoint::get_instance()->clear_all_call_backs();
    };
    sp->set_call_back("batch_get_version_err", [&](auto&& args) {
        if (i++ == N / 10) {
            *try_any_cast<TxnErrorCode*>(args) = TxnErrorCode::TXN_TOO_OLD;
        }
    });

    sp->enable_processing();

    auto service = get_meta_service();
    for (int64_t i = 1; i <= N; ++i) {
        create_tablet(service.get(), 1, 1, i, i);
        insert_rowset(service.get(), 1, std::to_string(i), 1, i, i);
    }

    brpc::Controller ctrl;
    GetVersionRequest req;
    req.set_cloud_unique_id("test_cloud_unique_id");
    req.set_db_id(-1);
    req.set_table_id(-1);
    req.set_partition_id(-1);
    req.set_batch_mode(true);
    for (size_t i = 1; i <= N; ++i) {
        req.add_db_ids(1);
        req.add_table_ids(1);
        req.add_partition_ids(i);
    }

    GetVersionResponse resp;
    service->get_version(&ctrl, &req, &resp, nullptr);

    ASSERT_EQ(resp.status().code(), MetaServiceCode::OK)
            << "case " << i << " status is " << resp.status().msg()
            << ", code=" << resp.status().code();

    ASSERT_EQ(resp.versions_size(), N);
}

extern bool is_dropped_tablet(Transaction* txn, const std::string& instance_id, int64_t index_id,
                              int64_t partition_id);

TEST(MetaServiceTest, IsDroppedTablet) {
    auto meta_service = get_meta_service();
    std::string instance_id = "IsDroppedTablet";
    auto sp = SyncPoint::get_instance();
    DORIS_CLOUD_DEFER {
        SyncPoint::get_instance()->clear_all_call_backs();
    };
    sp->set_call_back("get_instance_id", [&](auto&& args) {
        auto* ret = try_any_cast_ret<std::string>(args);
        ret->first = instance_id;
        ret->second = true;
    });
    sp->enable_processing();

    meta_service = get_meta_service();
    auto reset_meta_service = [&meta_service] { meta_service = get_meta_service(); };

    constexpr int64_t index_id = 10002;
    constexpr int64_t partition_id = 10003;

    std::unique_ptr<Transaction> txn;
    RecycleIndexPB index_pb;
    auto index_key = recycle_index_key({instance_id, index_id});
    RecyclePartitionPB partition_pb;
    auto partition_key = recycle_partition_key({instance_id, partition_id});
    std::string val;
    // No recycle index and partition kv
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    EXPECT_FALSE(is_dropped_tablet(txn.get(), instance_id, index_id, partition_id));
    // Tablet in PREPARED index
    index_pb.set_state(RecycleIndexPB::PREPARED);
    val = index_pb.SerializeAsString();
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->put(index_key, val);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    EXPECT_FALSE(is_dropped_tablet(txn.get(), instance_id, index_id, partition_id));
    // Tablet in DROPPED/RECYCLING index
    reset_meta_service();
    index_pb.set_state(RecycleIndexPB::DROPPED);
    val = index_pb.SerializeAsString();
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->put(index_key, val);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    EXPECT_TRUE(is_dropped_tablet(txn.get(), instance_id, index_id, partition_id));
    reset_meta_service();
    index_pb.set_state(RecycleIndexPB::RECYCLING);
    val = index_pb.SerializeAsString();
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->put(index_key, val);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    EXPECT_TRUE(is_dropped_tablet(txn.get(), instance_id, index_id, partition_id));
    // Tablet in PREPARED partition
    reset_meta_service();
    partition_pb.set_state(RecyclePartitionPB::PREPARED);
    val = partition_pb.SerializeAsString();
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->put(partition_key, val);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    EXPECT_FALSE(is_dropped_tablet(txn.get(), instance_id, index_id, partition_id));
    // Tablet in DROPPED/RECYCLING partition
    reset_meta_service();
    partition_pb.set_state(RecyclePartitionPB::DROPPED);
    val = partition_pb.SerializeAsString();
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->put(partition_key, val);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    EXPECT_TRUE(is_dropped_tablet(txn.get(), instance_id, index_id, partition_id));
    reset_meta_service();
    partition_pb.set_state(RecyclePartitionPB::RECYCLING);
    val = partition_pb.SerializeAsString();
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->put(partition_key, val);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    EXPECT_TRUE(is_dropped_tablet(txn.get(), instance_id, index_id, partition_id));
}

TEST(MetaServiceTest, IndexRequest) {
    auto meta_service = get_meta_service();
    std::string instance_id = "IndexRequest";
    auto sp = SyncPoint::get_instance();
    DORIS_CLOUD_DEFER {
        SyncPoint::get_instance()->clear_all_call_backs();
    };
    sp->set_call_back("get_instance_id", [&](auto&& args) {
        auto* ret = try_any_cast_ret<std::string>(args);
        ret->first = instance_id;
        ret->second = true;
    });
    sp->enable_processing();

    auto reset_meta_service = [&meta_service] { meta_service = get_meta_service(); };
    constexpr int64_t table_id = 10001;
    constexpr int64_t index_id = 10002;
    constexpr int64_t partition_id = 10003;
    constexpr int64_t tablet_id = 10004;

    std::unique_ptr<Transaction> txn;
    doris::TabletMetaCloudPB tablet_pb;
    tablet_pb.set_table_id(table_id);
    tablet_pb.set_index_id(index_id);
    tablet_pb.set_partition_id(partition_id);
    tablet_pb.set_tablet_id(tablet_id);
    auto tablet_key = meta_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
    auto tablet_val = tablet_pb.SerializeAsString();
    RecycleIndexPB index_pb;
    auto index_key = recycle_index_key({instance_id, index_id});
    int64_t val_int = 0;
    auto tbl_version_key = table_version_key({instance_id, 1, table_id});
    std::string val;

    // ------------Test prepare index------------
    brpc::Controller ctrl;
    IndexRequest req;
    IndexResponse res;
    meta_service->prepare_index(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    req.set_db_id(1);
    req.set_table_id(table_id);
    req.add_index_ids(index_id);
    req.set_is_new_table(true);
    // Last state UNKNOWN
    res.Clear();
    meta_service->prepare_index(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(index_key, &val), TxnErrorCode::TXN_OK);
    ASSERT_TRUE(index_pb.ParseFromString(val));
    ASSERT_EQ(index_pb.state(), RecycleIndexPB::PREPARED);
    // Last state PREPARED
    res.Clear();
    meta_service->prepare_index(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(index_key, &val), TxnErrorCode::TXN_OK);
    ASSERT_TRUE(index_pb.ParseFromString(val));
    ASSERT_EQ(index_pb.state(), RecycleIndexPB::PREPARED);
    // Last state DROPPED
    reset_meta_service();
    index_pb.set_state(RecycleIndexPB::DROPPED);
    val = index_pb.SerializeAsString();
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->put(index_key, val);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    res.Clear();
    meta_service->prepare_index(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(index_key, &val), TxnErrorCode::TXN_OK);
    ASSERT_TRUE(index_pb.ParseFromString(val));
    ASSERT_EQ(index_pb.state(), RecycleIndexPB::DROPPED);
    // Last state RECYCLING
    reset_meta_service();
    index_pb.set_state(RecycleIndexPB::RECYCLING);
    val = index_pb.SerializeAsString();
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->put(index_key, val);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    res.Clear();
    meta_service->prepare_index(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(index_key, &val), TxnErrorCode::TXN_OK);
    ASSERT_TRUE(index_pb.ParseFromString(val));
    ASSERT_EQ(index_pb.state(), RecycleIndexPB::RECYCLING);
    // Last state UNKNOWN but tablet meta existed
    reset_meta_service();
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->remove(index_key);
    txn->put(tablet_key, tablet_val);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    res.Clear();
    meta_service->prepare_index(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::ALREADY_EXISTED);
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(index_key, &val), TxnErrorCode::TXN_KEY_NOT_FOUND);
    // Prepare index should not init table version
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(tbl_version_key, &val), TxnErrorCode::TXN_KEY_NOT_FOUND);
    // ------------Test commit index------------
    reset_meta_service();
    req.Clear();
    meta_service->commit_index(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    req.set_db_id(1);
    req.set_table_id(table_id);
    req.add_index_ids(index_id);
    req.set_is_new_table(true);
    // Last state UNKNOWN
    res.Clear();
    meta_service->commit_index(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(index_key, &val), TxnErrorCode::TXN_KEY_NOT_FOUND);
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(tbl_version_key, &val), TxnErrorCode::TXN_KEY_NOT_FOUND);
    // Last state PREPARED
    reset_meta_service();
    index_pb.set_state(RecycleIndexPB::PREPARED);
    val = index_pb.SerializeAsString();
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->put(index_key, val);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    res.Clear();
    meta_service->commit_index(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(index_key, &val), TxnErrorCode::TXN_KEY_NOT_FOUND);
    // First commit index should init table version
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(tbl_version_key, &val), TxnErrorCode::TXN_OK);
    val_int = *reinterpret_cast<const int64_t*>(val.data());
    ASSERT_EQ(val_int, 1);
    // Last state DROPPED
    reset_meta_service();
    index_pb.set_state(RecycleIndexPB::DROPPED);
    val = index_pb.SerializeAsString();
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->put(index_key, val);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    res.Clear();
    meta_service->commit_index(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(index_key, &val), TxnErrorCode::TXN_OK);
    ASSERT_TRUE(index_pb.ParseFromString(val));
    ASSERT_EQ(index_pb.state(), RecycleIndexPB::DROPPED);
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(tbl_version_key, &val), TxnErrorCode::TXN_KEY_NOT_FOUND);
    // Last state RECYCLING
    reset_meta_service();
    index_pb.set_state(RecycleIndexPB::RECYCLING);
    val = index_pb.SerializeAsString();
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->put(index_key, val);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    res.Clear();
    meta_service->commit_index(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(index_key, &val), TxnErrorCode::TXN_OK);
    ASSERT_TRUE(index_pb.ParseFromString(val));
    ASSERT_EQ(index_pb.state(), RecycleIndexPB::RECYCLING);
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(tbl_version_key, &val), TxnErrorCode::TXN_KEY_NOT_FOUND);
    // Last state UNKNOWN but tablet meta existed
    reset_meta_service();
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->put(tablet_key, tablet_val);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    res.Clear();
    meta_service->commit_index(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(index_key, &val), TxnErrorCode::TXN_KEY_NOT_FOUND);
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(tbl_version_key, &val), TxnErrorCode::TXN_KEY_NOT_FOUND);
    // ------------Test drop index------------
    reset_meta_service();
    req.Clear();
    meta_service->drop_index(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    req.set_db_id(1);
    req.set_table_id(table_id);
    req.add_index_ids(index_id);
    req.set_is_new_table(true);
    // Last state UNKNOWN
    res.Clear();
    meta_service->drop_index(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(index_key, &val), TxnErrorCode::TXN_OK);
    ASSERT_TRUE(index_pb.ParseFromString(val));
    ASSERT_EQ(index_pb.state(), RecycleIndexPB::DROPPED);
    // Last state PREPARED
    reset_meta_service();
    index_pb.set_state(RecycleIndexPB::PREPARED);
    val = index_pb.SerializeAsString();
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->put(index_key, val);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    res.Clear();
    meta_service->drop_index(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(index_key, &val), TxnErrorCode::TXN_OK);
    ASSERT_TRUE(index_pb.ParseFromString(val));
    ASSERT_EQ(index_pb.state(), RecycleIndexPB::DROPPED);
    // Last state DROPPED
    reset_meta_service();
    index_pb.set_state(RecycleIndexPB::DROPPED);
    val = index_pb.SerializeAsString();
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->put(index_key, val);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    res.Clear();
    meta_service->drop_index(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(index_key, &val), TxnErrorCode::TXN_OK);
    ASSERT_TRUE(index_pb.ParseFromString(val));
    ASSERT_EQ(index_pb.state(), RecycleIndexPB::DROPPED);
    // Last state RECYCLING
    reset_meta_service();
    index_pb.set_state(RecycleIndexPB::RECYCLING);
    val = index_pb.SerializeAsString();
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->put(index_key, val);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    res.Clear();
    meta_service->drop_index(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(index_key, &val), TxnErrorCode::TXN_OK);
    ASSERT_TRUE(index_pb.ParseFromString(val));
    ASSERT_EQ(index_pb.state(), RecycleIndexPB::RECYCLING);
    // Drop index should not init table version
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(tbl_version_key, &val), TxnErrorCode::TXN_KEY_NOT_FOUND);
}

TEST(MetaServiceTest, PartitionRequest) {
    auto meta_service = get_meta_service();
    std::string instance_id = "PartitionRequest";
    auto sp = SyncPoint::get_instance();
    DORIS_CLOUD_DEFER {
        SyncPoint::get_instance()->clear_all_call_backs();
    };
    sp->set_call_back("get_instance_id", [&](auto&& args) {
        auto* ret = try_any_cast_ret<std::string>(args);
        ret->first = instance_id;
        ret->second = true;
    });
    sp->enable_processing();

    auto reset_meta_service = [&meta_service] { meta_service = get_meta_service(); };
    constexpr int64_t table_id = 10001;
    constexpr int64_t index_id = 10002;
    constexpr int64_t partition_id = 10003;
    constexpr int64_t tablet_id = 10004;

    std::unique_ptr<Transaction> txn;
    doris::TabletMetaCloudPB tablet_pb;
    tablet_pb.set_table_id(table_id);
    tablet_pb.set_index_id(index_id);
    tablet_pb.set_partition_id(partition_id);
    tablet_pb.set_tablet_id(tablet_id);
    auto tablet_key = meta_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
    auto tablet_val = tablet_pb.SerializeAsString();
    RecyclePartitionPB partition_pb;
    auto partition_key = recycle_partition_key({instance_id, partition_id});
    int64_t val_int = 0;
    auto tbl_version_key = table_version_key({instance_id, 1, table_id});
    std::string val;
    // ------------Test prepare partition------------
    brpc::Controller ctrl;
    PartitionRequest req;
    PartitionResponse res;
    meta_service->prepare_partition(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    req.set_db_id(1);
    req.set_table_id(table_id);
    req.add_index_ids(index_id);
    req.add_partition_ids(partition_id);
    // Last state UNKNOWN
    res.Clear();
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->atomic_add(tbl_version_key, 1);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    meta_service->prepare_partition(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(partition_key, &val), TxnErrorCode::TXN_OK);
    ASSERT_TRUE(partition_pb.ParseFromString(val));
    ASSERT_EQ(partition_pb.state(), RecyclePartitionPB::PREPARED);
    // Prepare partition should not update table version
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(tbl_version_key, &val), TxnErrorCode::TXN_OK);
    val_int = *reinterpret_cast<const int64_t*>(val.data());
    ASSERT_EQ(val_int, 1);
    // Last state PREPARED
    res.Clear();
    meta_service->prepare_partition(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(partition_key, &val), TxnErrorCode::TXN_OK);
    ASSERT_TRUE(partition_pb.ParseFromString(val));
    ASSERT_EQ(partition_pb.state(), RecyclePartitionPB::PREPARED);
    // Last state DROPPED
    reset_meta_service();
    partition_pb.set_state(RecyclePartitionPB::DROPPED);
    val = partition_pb.SerializeAsString();
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->put(partition_key, val);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    res.Clear();
    meta_service->prepare_partition(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(partition_key, &val), TxnErrorCode::TXN_OK);
    ASSERT_TRUE(partition_pb.ParseFromString(val));
    ASSERT_EQ(partition_pb.state(), RecyclePartitionPB::DROPPED);
    // Last state RECYCLING
    reset_meta_service();
    partition_pb.set_state(RecyclePartitionPB::RECYCLING);
    val = partition_pb.SerializeAsString();
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->put(partition_key, val);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    res.Clear();
    meta_service->prepare_partition(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(partition_key, &val), TxnErrorCode::TXN_OK);
    ASSERT_TRUE(partition_pb.ParseFromString(val));
    ASSERT_EQ(partition_pb.state(), RecyclePartitionPB::RECYCLING);
    // Last state UNKNOWN but tablet meta existed
    reset_meta_service();
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->remove(partition_key);
    txn->put(tablet_key, tablet_val);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    res.Clear();
    meta_service->prepare_partition(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::ALREADY_EXISTED);
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(partition_key, &val), TxnErrorCode::TXN_KEY_NOT_FOUND);
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(tbl_version_key, &val), TxnErrorCode::TXN_KEY_NOT_FOUND);
    // ------------Test commit partition------------
    reset_meta_service();
    req.Clear();
    meta_service->commit_partition(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    req.set_db_id(1);
    req.set_table_id(table_id);
    req.add_index_ids(index_id);
    req.add_partition_ids(partition_id);
    // Last state UNKNOWN
    res.Clear();
    meta_service->commit_partition(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(partition_key, &val), TxnErrorCode::TXN_KEY_NOT_FOUND);
    // Last state PREPARED
    reset_meta_service();
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->atomic_add(tbl_version_key, 1);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    partition_pb.set_state(RecyclePartitionPB::PREPARED);
    val = partition_pb.SerializeAsString();
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->put(partition_key, val);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    res.Clear();
    meta_service->commit_partition(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(partition_key, &val), TxnErrorCode::TXN_KEY_NOT_FOUND);
    // Commit partition should update table version
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(tbl_version_key, &val), TxnErrorCode::TXN_OK);
    val_int = *reinterpret_cast<const int64_t*>(val.data());
    ASSERT_EQ(val_int, 2);
    // Last state DROPPED
    reset_meta_service();
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->atomic_add(tbl_version_key, 1);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    partition_pb.set_state(RecyclePartitionPB::DROPPED);
    val = partition_pb.SerializeAsString();
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->put(partition_key, val);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    res.Clear();
    meta_service->commit_partition(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(partition_key, &val), TxnErrorCode::TXN_OK);
    ASSERT_TRUE(partition_pb.ParseFromString(val));
    ASSERT_EQ(partition_pb.state(), RecyclePartitionPB::DROPPED);
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(tbl_version_key, &val), TxnErrorCode::TXN_OK);
    val_int = *reinterpret_cast<const int64_t*>(val.data());
    ASSERT_EQ(val_int, 1);
    // Last state RECYCLING
    reset_meta_service();
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->atomic_add(tbl_version_key, 1);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    partition_pb.set_state(RecyclePartitionPB::RECYCLING);
    val = partition_pb.SerializeAsString();
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->put(partition_key, val);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    res.Clear();
    meta_service->commit_partition(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(partition_key, &val), TxnErrorCode::TXN_OK);
    ASSERT_TRUE(partition_pb.ParseFromString(val));
    ASSERT_EQ(partition_pb.state(), RecyclePartitionPB::RECYCLING);
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(tbl_version_key, &val), TxnErrorCode::TXN_OK);
    val_int = *reinterpret_cast<const int64_t*>(val.data());
    ASSERT_EQ(val_int, 1);
    // Last state UNKNOWN but tablet meta existed
    reset_meta_service();
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->atomic_add(tbl_version_key, 1);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->put(tablet_key, tablet_val);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    res.Clear();
    meta_service->commit_partition(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(partition_key, &val), TxnErrorCode::TXN_KEY_NOT_FOUND);
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(tbl_version_key, &val), TxnErrorCode::TXN_OK);
    val_int = *reinterpret_cast<const int64_t*>(val.data());
    ASSERT_EQ(val_int, 1);
    // Last state UNKNOWN and tablet meta existed, but request has no index ids
    reset_meta_service();
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->put(tablet_key, tablet_val);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    res.Clear();
    req.clear_index_ids();
    meta_service->commit_partition(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(partition_key, &val), TxnErrorCode::TXN_KEY_NOT_FOUND);
    req.add_index_ids(index_id);
    // ------------Test check partition-----------
    // Normal
    req.set_db_id(1);
    req.set_table_id(table_id + 1);
    req.add_index_ids(index_id + 1);
    req.add_partition_ids(partition_id + 1);
    meta_service->prepare_partition(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    meta_service->commit_partition(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    CheckKVRequest req_check;
    CheckKVResponse res_check;
    meta_service->check_kv(&ctrl, &req_check, &res_check, nullptr);
    ASSERT_EQ(res_check.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    res_check.Clear();
    req_check.set_op(CheckKVRequest::CREATE_PARTITION_AFTER_FE_COMMIT);
    CheckKeyInfos check_keys_pb;
    check_keys_pb.add_table_ids(table_id + 1);
    check_keys_pb.add_index_ids(index_id + 1);
    check_keys_pb.add_partition_ids(partition_id + 1);
    req_check.mutable_check_keys()->CopyFrom(check_keys_pb);
    meta_service->check_kv(&ctrl, &req_check, &res_check, nullptr);
    ASSERT_EQ(res_check.status().code(), MetaServiceCode::OK);
    res_check.Clear();
    // AbNomal not commit
    req.Clear();
    req.set_db_id(1);
    req.set_table_id(table_id + 2);
    req.add_index_ids(index_id + 2);
    req.add_partition_ids(partition_id + 2);
    meta_service->prepare_partition(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    req_check.Clear();
    req_check.set_op(CheckKVRequest::CREATE_PARTITION_AFTER_FE_COMMIT);
    check_keys_pb.Clear();
    check_keys_pb.add_table_ids(table_id + 2);
    check_keys_pb.add_index_ids(index_id + 2);
    check_keys_pb.add_partition_ids(partition_id + 2);
    req_check.mutable_check_keys()->CopyFrom(check_keys_pb);
    meta_service->check_kv(&ctrl, &req_check, &res_check, nullptr);
    ASSERT_EQ(res_check.status().code(), MetaServiceCode::UNDEFINED_ERR);

    // ------------Test check index-----------
    // Normal
    IndexRequest req_index;
    IndexResponse res_index;
    req_index.set_db_id(1);
    req_index.set_table_id(table_id + 3);
    req_index.add_index_ids(index_id + 3);
    meta_service->prepare_index(&ctrl, &req_index, &res_index, nullptr);
    ASSERT_EQ(res_index.status().code(), MetaServiceCode::OK);
    meta_service->commit_index(&ctrl, &req_index, &res_index, nullptr);
    ASSERT_EQ(res_index.status().code(), MetaServiceCode::OK);
    req_check.Clear();
    res_check.Clear();
    req_check.set_op(CheckKVRequest::CREATE_INDEX_AFTER_FE_COMMIT);
    check_keys_pb.Clear();
    check_keys_pb.add_table_ids(table_id + 3);
    check_keys_pb.add_index_ids(index_id + 3);
    req_check.mutable_check_keys()->CopyFrom(check_keys_pb);
    meta_service->check_kv(&ctrl, &req_check, &res_check, nullptr);
    ASSERT_EQ(res_check.status().code(), MetaServiceCode::OK);
    res_check.Clear();

    // ------------Test drop partition------------
    reset_meta_service();
    req.Clear();
    meta_service->drop_partition(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    req.set_db_id(1);
    req.set_table_id(table_id);
    req.add_index_ids(index_id);
    req.add_partition_ids(partition_id);
    req.set_need_update_table_version(true);
    // Last state UNKNOWN
    res.Clear();
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->atomic_add(tbl_version_key, 1);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    meta_service->drop_partition(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(partition_key, &val), TxnErrorCode::TXN_OK);
    ASSERT_TRUE(partition_pb.ParseFromString(val));
    ASSERT_EQ(partition_pb.state(), RecyclePartitionPB::DROPPED);
    // Drop partition should update table version
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(tbl_version_key, &val), TxnErrorCode::TXN_OK);
    val_int = *reinterpret_cast<const int64_t*>(val.data());
    ASSERT_EQ(val_int, 2);
    // Last state PREPARED
    reset_meta_service();
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->atomic_add(tbl_version_key, 1);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    partition_pb.set_state(RecyclePartitionPB::PREPARED);
    val = partition_pb.SerializeAsString();
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->put(partition_key, val);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    res.Clear();
    meta_service->drop_partition(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(partition_key, &val), TxnErrorCode::TXN_OK);
    ASSERT_TRUE(partition_pb.ParseFromString(val));
    ASSERT_EQ(partition_pb.state(), RecyclePartitionPB::DROPPED);
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(tbl_version_key, &val), TxnErrorCode::TXN_OK);
    val_int = *reinterpret_cast<const int64_t*>(val.data());
    ASSERT_EQ(val_int, 2);
    // Last state PREPARED but drop an empty partition
    req.set_need_update_table_version(false);
    reset_meta_service();
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->atomic_add(tbl_version_key, 1);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    partition_pb.set_state(RecyclePartitionPB::PREPARED);
    val = partition_pb.SerializeAsString();
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->put(partition_key, val);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    res.Clear();
    meta_service->drop_partition(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(partition_key, &val), TxnErrorCode::TXN_OK);
    ASSERT_TRUE(partition_pb.ParseFromString(val));
    ASSERT_EQ(partition_pb.state(), RecyclePartitionPB::DROPPED);
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(tbl_version_key, &val), TxnErrorCode::TXN_OK);
    val_int = *reinterpret_cast<const int64_t*>(val.data());
    ASSERT_EQ(val_int, 1);
    // Last state DROPPED
    reset_meta_service();
    req.set_need_update_table_version(true);
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->atomic_add(tbl_version_key, 1);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    partition_pb.set_state(RecyclePartitionPB::DROPPED);
    val = partition_pb.SerializeAsString();
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->put(partition_key, val);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    res.Clear();
    meta_service->drop_partition(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(partition_key, &val), TxnErrorCode::TXN_OK);
    ASSERT_TRUE(partition_pb.ParseFromString(val));
    ASSERT_EQ(partition_pb.state(), RecyclePartitionPB::DROPPED);
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(tbl_version_key, &val), TxnErrorCode::TXN_OK);
    val_int = *reinterpret_cast<const int64_t*>(val.data());
    ASSERT_EQ(val_int, 1);
    // Last state RECYCLING
    reset_meta_service();
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->atomic_add(tbl_version_key, 1);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    partition_pb.set_state(RecyclePartitionPB::RECYCLING);
    val = partition_pb.SerializeAsString();
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->put(partition_key, val);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    res.Clear();
    meta_service->drop_partition(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(partition_key, &val), TxnErrorCode::TXN_OK);
    ASSERT_TRUE(partition_pb.ParseFromString(val));
    ASSERT_EQ(partition_pb.state(), RecyclePartitionPB::RECYCLING);
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(tbl_version_key, &val), TxnErrorCode::TXN_OK);
    val_int = *reinterpret_cast<const int64_t*>(val.data());
    ASSERT_EQ(val_int, 1);
}

TEST(MetaServiceTxnStoreRetryableTest, MockGetVersion) {
    size_t index = 0;
    SyncPoint::get_instance()->set_call_back("get_version_code", [&](auto&& args) {
        LOG(INFO) << "GET_VERSION_CODE";
        if (++index < 2) {
            *doris::try_any_cast<MetaServiceCode*>(args[0]) =
                    MetaServiceCode::KV_TXN_STORE_GET_RETRYABLE;
        }
    });
    SyncPoint::get_instance()->enable_processing();

    auto service = get_meta_service();
    create_tablet(service.get(), 1, 1, 1, 1);
    insert_rowset(service.get(), 1, std::to_string(1), 1, 1, 1);

    brpc::Controller ctrl;
    GetVersionRequest req;
    req.set_cloud_unique_id("test_cloud_unique_id");
    req.set_db_id(1);
    req.set_table_id(1);
    req.set_partition_id(1);

    GetVersionResponse resp;
    service->get_version(&ctrl, &req, &resp, nullptr);

    ASSERT_EQ(resp.status().code(), MetaServiceCode::OK)
            << " status is " << resp.status().msg() << ", code=" << resp.status().code();
    EXPECT_EQ(resp.version(), 2);
    EXPECT_GE(index, 2);

    SyncPoint::get_instance()->disable_processing();
    SyncPoint::get_instance()->clear_all_call_backs();
}

TEST(MetaServiceTxnStoreRetryableTest, DoNotReturnRetryableCode) {
    SyncPoint::get_instance()->set_call_back("get_version_code", [&](auto&& args) {
        *doris::try_any_cast<MetaServiceCode*>(args[0]) =
                MetaServiceCode::KV_TXN_STORE_GET_RETRYABLE;
    });
    SyncPoint::get_instance()->enable_processing();
    int32_t retry_times = config::txn_store_retry_times;
    config::txn_store_retry_times = 3;

    auto service = get_meta_service();
    create_tablet(service.get(), 1, 1, 1, 1);
    insert_rowset(service.get(), 1, std::to_string(1), 1, 1, 1);

    brpc::Controller ctrl;
    GetVersionRequest req;
    req.set_cloud_unique_id("test_cloud_unique_id");
    req.set_db_id(1);
    req.set_table_id(1);
    req.set_partition_id(1);

    GetVersionResponse resp;
    service->get_version(&ctrl, &req, &resp, nullptr);

    ASSERT_EQ(resp.status().code(), MetaServiceCode::KV_TXN_GET_ERR)
            << " status is " << resp.status().msg() << ", code=" << resp.status().code();

    SyncPoint::get_instance()->disable_processing();
    SyncPoint::get_instance()->clear_all_call_backs();
    config::txn_store_retry_times = retry_times;
}

TEST(MetaServiceTest, GetClusterStatusTest) {
    auto meta_service = get_meta_service();

    // add cluster first
    InstanceKeyInfo key_info {mock_instance};
    std::string key;
    std::string val;
    instance_key(key_info, &key);

    InstanceInfoPB instance;
    instance.set_instance_id(mock_instance);
    ClusterPB c1;
    c1.set_type(ClusterPB::COMPUTE);
    c1.set_cluster_name(mock_cluster_name);
    c1.set_cluster_id(mock_cluster_id);
    c1.add_mysql_user_name()->append("m1");
    c1.set_cluster_status(ClusterStatus::NORMAL);
    ClusterPB c2;
    c2.set_type(ClusterPB::COMPUTE);
    c2.set_cluster_name(mock_cluster_name + "2");
    c2.set_cluster_id(mock_cluster_id + "2");
    c2.add_mysql_user_name()->append("m2");
    c2.set_cluster_status(ClusterStatus::SUSPENDED);
    ClusterPB c3;
    c3.set_type(ClusterPB::COMPUTE);
    c3.set_cluster_name(mock_cluster_name + "3");
    c3.set_cluster_id(mock_cluster_id + "3");
    c3.add_mysql_user_name()->append("m3");
    c3.set_cluster_status(ClusterStatus::TO_RESUME);
    instance.add_clusters()->CopyFrom(c1);
    instance.add_clusters()->CopyFrom(c2);
    instance.add_clusters()->CopyFrom(c3);
    val = instance.SerializeAsString();

    std::unique_ptr<Transaction> txn;
    std::string get_val;
    TxnErrorCode err = meta_service->txn_kv()->create_txn(&txn);
    ASSERT_EQ(err, TxnErrorCode::TXN_OK);
    txn->put(key, val);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    // case: get all cluster
    {
        brpc::Controller cntl;
        GetClusterStatusRequest req;
        req.add_instance_ids(mock_instance);
        GetClusterStatusResponse res;
        meta_service->get_cluster_status(
                reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(res.details().at(0).clusters().size(), 3);
    }

    // get normal cluster
    {
        brpc::Controller cntl;
        GetClusterStatusRequest req;
        req.add_instance_ids(mock_instance);
        req.set_status(NORMAL);
        GetClusterStatusResponse res;
        meta_service->get_cluster_status(
                reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(res.details().at(0).clusters().size(), 1);
    }
}

TEST(MetaServiceTest, DecryptInfoTest) {
    auto meta_service = get_meta_service();
    auto sp = SyncPoint::get_instance();
    sp->enable_processing();
    sp->set_call_back("decrypt_ak_sk:get_encryption_key", [](auto&& args) {
        auto* key = try_any_cast<std::string*>(args[0]);
        *key = "selectdbselectdbselectdbselectdb";
        auto* ret = try_any_cast<int*>(args[1]);
        *ret = 0;
    });
    InstanceInfoPB instance;

    EncryptionInfoPB encryption_info;
    encryption_info.set_encryption_method("AES_256_ECB");
    encryption_info.set_key_id(1);

    std::string cipher_sk = "JUkuTDctR+ckJtnPkLScWaQZRcOtWBhsLLpnCRxQLxr734qB8cs6gNLH6grE1FxO";
    std::string plain_sk = "Hx60p12123af234541nsVsffdfsdfghsdfhsdf34t";
    ObjectStoreInfoPB obj_info;
    obj_info.mutable_encryption_info()->CopyFrom(encryption_info);
    obj_info.set_ak("akak1");
    obj_info.set_sk(cipher_sk);
    instance.add_obj_info()->CopyFrom(obj_info);

    RamUserPB ram_user;
    ram_user.set_ak("akak2");
    ram_user.set_sk(cipher_sk);
    ram_user.mutable_encryption_info()->CopyFrom(encryption_info);
    instance.mutable_ram_user()->CopyFrom(ram_user);

    StagePB stage;
    stage.mutable_obj_info()->CopyFrom(obj_info);
    instance.add_stages()->CopyFrom(stage);

    auto checkcheck = [&](const InstanceInfoPB& instance) {
        ASSERT_EQ(instance.obj_info(0).ak(), "akak1");
        ASSERT_EQ(instance.obj_info(0).sk(), plain_sk);

        ASSERT_EQ(instance.ram_user().ak(), "akak2");
        ASSERT_EQ(instance.ram_user().sk(), plain_sk);

        ASSERT_EQ(instance.stages(0).obj_info().ak(), "akak1");
        ASSERT_EQ(instance.stages(0).obj_info().sk(), plain_sk);
    };

    std::string instance_id = "i1";
    MetaServiceCode code;
    std::string msg;
    // No system_meta_service_arn_info_key
    {
        std::unique_ptr<Transaction> txn0;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn0), TxnErrorCode::TXN_OK);
        std::shared_ptr<Transaction> txn(txn0.release());
        InstanceInfoPB decrypt_instance;
        decrypt_instance.CopyFrom(instance);
        int ret = decrypt_instance_info(decrypt_instance, instance_id, code, msg, txn);
        ASSERT_EQ(ret, 0);
        checkcheck(decrypt_instance);
        ASSERT_EQ(decrypt_instance.iam_user().user_id(), config::arn_id);
        ASSERT_EQ(decrypt_instance.iam_user().external_id(), instance_id);
        ASSERT_EQ(decrypt_instance.iam_user().ak(), config::arn_ak);
        ASSERT_EQ(decrypt_instance.iam_user().sk(), config::arn_sk);
    }

    // With system_meta_service_arn_info_key
    {
        std::string key = system_meta_service_arn_info_key();
        std::string val;
        std::unique_ptr<Transaction> txn2;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn2), TxnErrorCode::TXN_OK);
        RamUserPB iam_user;
        iam_user.set_user_id("1234");
        iam_user.set_ak("aksk3");
        iam_user.set_sk(cipher_sk);
        iam_user.set_external_id(instance_id);
        iam_user.mutable_encryption_info()->CopyFrom(encryption_info);
        val = iam_user.SerializeAsString();
        txn2->put(key, val);
        ASSERT_EQ(txn2->commit(), TxnErrorCode::TXN_OK);
        std::unique_ptr<Transaction> txn0;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn0), TxnErrorCode::TXN_OK);
        std::shared_ptr<Transaction> txn(txn0.release());
        InstanceInfoPB decrypt_instance;
        decrypt_instance.CopyFrom(instance);
        int ret = decrypt_instance_info(decrypt_instance, instance_id, code, msg, txn);
        ASSERT_EQ(ret, 0);
        checkcheck(decrypt_instance);
        ASSERT_EQ(decrypt_instance.iam_user().user_id(), "1234");
        ASSERT_EQ(decrypt_instance.iam_user().external_id(), instance_id);
        ASSERT_EQ(decrypt_instance.iam_user().ak(), "aksk3");
        ASSERT_EQ(decrypt_instance.iam_user().sk(), plain_sk);
    }
    SyncPoint::get_instance()->disable_processing();
    SyncPoint::get_instance()->clear_all_call_backs();
}

TEST(MetaServiceTest, LegacyUpdateAkSkTest) {
    auto meta_service = get_meta_service();

    auto sp = SyncPoint::get_instance();
    sp->enable_processing();

    sp->set_call_back("encrypt_ak_sk:get_encryption_key", [](auto&& args) {
        auto* ret = try_any_cast<int*>(args[0]);
        *ret = 0;
        auto* key = try_any_cast<std::string*>(args[1]);
        *key = "selectdbselectdbselectdbselectdb";
        auto* key_id = try_any_cast<int64_t*>(args[2]);
        *key_id = 1;
    });

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string key;
    std::string val;
    InstanceKeyInfo key_info {"test_instance"};
    instance_key(key_info, &key);

    ObjectStoreInfoPB obj_info;
    obj_info.set_id("1");
    obj_info.set_ak("ak");
    obj_info.set_sk("sk");
    InstanceInfoPB instance;
    instance.add_obj_info()->CopyFrom(obj_info);
    val = instance.SerializeAsString();
    txn->put(key, val);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    auto get_test_instance = [&](InstanceInfoPB& i) {
        std::string key;
        std::string val;
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        InstanceKeyInfo key_info {"test_instance"};
        instance_key(key_info, &key);
        ASSERT_EQ(txn->get(key, &val), TxnErrorCode::TXN_OK);
        i.ParseFromString(val);
    };

    std::string cipher_sk = "JUkuTDctR+ckJtnPkLScWaQZRcOtWBhsLLpnCRxQLxr734qB8cs6gNLH6grE1FxO";
    std::string plain_sk = "Hx60p12123af234541nsVsffdfsdfghsdfhsdf34t";

    // update failed
    {
        AlterObjStoreInfoRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_op(AlterObjStoreInfoRequest::LEGACY_UPDATE_AK_SK);
        req.mutable_obj()->set_id("2");
        req.mutable_obj()->set_ak("new_ak");
        req.mutable_obj()->set_sk(plain_sk);

        brpc::Controller cntl;
        AlterObjStoreInfoResponse res;
        meta_service->alter_obj_store_info(
                reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
        InstanceInfoPB instance;
        get_test_instance(instance);
        ASSERT_EQ(instance.obj_info(0).id(), "1");
        ASSERT_EQ(instance.obj_info(0).ak(), "ak");
        ASSERT_EQ(instance.obj_info(0).sk(), "sk");
    }

    // update successful
    {
        AlterObjStoreInfoRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_op(AlterObjStoreInfoRequest::LEGACY_UPDATE_AK_SK);
        req.mutable_obj()->set_id("1");
        req.mutable_obj()->set_ak("new_ak");
        req.mutable_obj()->set_sk(plain_sk);

        brpc::Controller cntl;
        AlterObjStoreInfoResponse res;
        meta_service->alter_obj_store_info(
                reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        InstanceInfoPB instance;
        get_test_instance(instance);
        ASSERT_EQ(instance.obj_info(0).id(), "1");
        ASSERT_EQ(instance.obj_info(0).ak(), "new_ak");
        ASSERT_EQ(instance.obj_info(0).sk(), cipher_sk);
    }

    SyncPoint::get_instance()->disable_processing();
    SyncPoint::get_instance()->clear_all_call_backs();
}

namespace detail {
bool normalize_hdfs_prefix(std::string& prefix);
bool normalize_hdfs_fs_name(std::string& fs_name);
} // namespace detail

TEST(MetaServiceTest, NormalizeHdfsConfTest) {
    using namespace detail;
    std::string prefix = "hdfs://127.0.0.1:8020/test";
    EXPECT_FALSE(normalize_hdfs_prefix(prefix));
    prefix = "test";
    EXPECT_TRUE(normalize_hdfs_prefix(prefix));
    EXPECT_EQ(prefix, "test");
    prefix = "   test ";
    EXPECT_TRUE(normalize_hdfs_prefix(prefix));
    EXPECT_EQ(prefix, "test");
    prefix = "  /test// ";
    EXPECT_TRUE(normalize_hdfs_prefix(prefix));
    EXPECT_EQ(prefix, "test");
    prefix = "/";
    EXPECT_TRUE(normalize_hdfs_prefix(prefix));
    EXPECT_EQ(prefix, "");

    std::string fs_name;
    EXPECT_FALSE(normalize_hdfs_fs_name(prefix));
    fs_name = " hdfs://127.0.0.1:8020/  ";
    EXPECT_TRUE(normalize_hdfs_fs_name(fs_name));
    EXPECT_EQ(fs_name, "hdfs://127.0.0.1:8020");
}

TEST(MetaServiceTest, AddObjInfoTest) {
    auto meta_service = get_meta_service();

    auto sp = SyncPoint::get_instance();
    sp->enable_processing();

    sp->set_call_back("encrypt_ak_sk:get_encryption_key", [](auto&& args) {
        auto* ret = try_any_cast<int*>(args[0]);
        *ret = 0;
        auto* key = try_any_cast<std::string*>(args[1]);
        *key = "selectdbselectdbselectdbselectdb";
        auto* key_id = try_any_cast<int64_t*>(args[2]);
        *key_id = 1;
    });

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string key;
    std::string val;
    InstanceKeyInfo key_info {"test_instance"};
    instance_key(key_info, &key);

    InstanceInfoPB instance;
    val = instance.SerializeAsString();
    txn->put(key, val);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    auto get_test_instance = [&](InstanceInfoPB& i) {
        std::string key;
        std::string val;
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        InstanceKeyInfo key_info {"test_instance"};
        instance_key(key_info, &key);
        ASSERT_EQ(txn->get(key, &val), TxnErrorCode::TXN_OK);
        i.ParseFromString(val);
    };

    // update failed
    {
        AlterObjStoreInfoRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_op(AlterObjStoreInfoRequest::ADD_OBJ_INFO);

        brpc::Controller cntl;
        AlterObjStoreInfoResponse res;
        meta_service->alter_obj_store_info(
                reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT) << res.status().msg();
    }

    // update successful
    {
        AlterObjStoreInfoRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_op(AlterObjStoreInfoRequest::ADD_OBJ_INFO);
        auto sp = SyncPoint::get_instance();
        sp->set_call_back("create_object_info_with_encrypt", [](auto&& args) {
            auto* ret = try_any_cast<int*>(args[0]);
            *ret = 0;
        });
        sp->enable_processing();

        ObjectStoreInfoPB obj_info;
        obj_info.set_ak("ak");
        obj_info.set_sk("sk");
        obj_info.set_bucket("bucket");
        obj_info.set_prefix("prefix");
        obj_info.set_endpoint("endpoint");
        obj_info.set_region("region");
        obj_info.set_provider(ObjectStoreInfoPB::Provider::ObjectStoreInfoPB_Provider_COS);
        obj_info.set_external_endpoint("external");
        req.mutable_obj()->MergeFrom(obj_info);

        brpc::Controller cntl;
        AlterObjStoreInfoResponse res;
        meta_service->alter_obj_store_info(
                reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();

        InstanceInfoPB instance;
        get_test_instance(instance);
        const auto& obj = instance.obj_info().at(0);
        ASSERT_EQ(obj.id(), "1");

        sp->clear_all_call_backs();
        sp->clear_trace();
        sp->disable_processing();
    }

    SyncPoint::get_instance()->disable_processing();
    SyncPoint::get_instance()->clear_all_call_backs();
}

TEST(MetaServiceTest, AddHdfsInfoTest) {
    auto meta_service = get_meta_service();

    auto sp = SyncPoint::get_instance();
    sp->enable_processing();
    sp->set_call_back("encrypt_ak_sk:get_encryption_key", [](auto&& args) {
        auto* ret = try_any_cast<int*>(args[0]);
        *ret = 0;
        auto* key = try_any_cast<std::string*>(args[1]);
        *key = "selectdbselectdbselectdbselectdb";
        auto* key_id = try_any_cast<int64_t*>(args[2]);
        *key_id = 1;
    });

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string key;
    std::string val;
    InstanceKeyInfo key_info {"test_instance"};
    instance_key(key_info, &key);

    ObjectStoreInfoPB obj_info;
    obj_info.set_id("1");
    obj_info.set_ak("ak");
    obj_info.set_sk("sk");
    InstanceInfoPB instance;
    instance.add_obj_info()->CopyFrom(obj_info);
    val = instance.SerializeAsString();
    txn->put(key, val);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    auto get_test_instance = [&](InstanceInfoPB& i) {
        std::string key;
        std::string val;
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        InstanceKeyInfo key_info {"test_instance"};
        instance_key(key_info, &key);
        ASSERT_EQ(txn->get(key, &val), TxnErrorCode::TXN_OK);
        i.ParseFromString(val);
    };

    // update failed
    {
        AlterObjStoreInfoRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_op(AlterObjStoreInfoRequest::ADD_HDFS_INFO);

        brpc::Controller cntl;
        AlterObjStoreInfoResponse res;
        meta_service->alter_obj_store_info(
                reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT) << res.status().msg();
    }

    // update successful
    {
        AlterObjStoreInfoRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_op(AlterObjStoreInfoRequest::ADD_HDFS_INFO);
        StorageVaultPB hdfs;
        hdfs.set_name("test_alter_add_hdfs_info");
        HdfsVaultInfo params;

        hdfs.mutable_hdfs_info()->CopyFrom(params);
        req.mutable_vault()->CopyFrom(hdfs);

        brpc::Controller cntl;
        AlterObjStoreInfoResponse res;
        meta_service->alter_storage_vault(
                reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
        // Invalid fs name
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT) << res.status().msg();
        req.mutable_vault()->mutable_hdfs_info()->mutable_build_conf()->set_fs_name(
                "hdfs://ip:port");
        meta_service->alter_storage_vault(
                reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();

        InstanceInfoPB instance;
        get_test_instance(instance);
        ASSERT_EQ(*(instance.resource_ids().begin()), "2");
        ASSERT_EQ(*(instance.storage_vault_names().begin()), "test_alter_add_hdfs_info");
    }

    // update failed because duplicate name
    {
        AlterObjStoreInfoRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_op(AlterObjStoreInfoRequest::ADD_HDFS_INFO);
        StorageVaultPB hdfs;
        hdfs.set_name("test_alter_add_hdfs_info");
        HdfsVaultInfo params;
        params.mutable_build_conf()->set_fs_name("hdfs://ip:port");

        hdfs.mutable_hdfs_info()->CopyFrom(params);
        req.mutable_vault()->CopyFrom(hdfs);

        brpc::Controller cntl;
        AlterObjStoreInfoResponse res;
        meta_service->alter_storage_vault(
                reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::ALREADY_EXISTED) << res.status().msg();
    }

    // to test if the vault id is expected
    {
        AlterObjStoreInfoRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_op(AlterObjStoreInfoRequest::ADD_HDFS_INFO);
        StorageVaultPB hdfs;
        hdfs.set_name("test_alter_add_hdfs_info_1");
        HdfsVaultInfo params;
        params.mutable_build_conf()->set_fs_name("hdfs://ip:port");

        hdfs.mutable_hdfs_info()->CopyFrom(params);
        req.mutable_vault()->CopyFrom(hdfs);

        brpc::Controller cntl;
        AlterObjStoreInfoResponse res;
        meta_service->alter_storage_vault(
                reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();
        InstanceInfoPB instance;
        get_test_instance(instance);
        ASSERT_EQ(*(instance.resource_ids().begin() + 1), "3");
        ASSERT_EQ(*(instance.storage_vault_names().begin() + 1), "test_alter_add_hdfs_info_1");
    }

    SyncPoint::get_instance()->disable_processing();
    SyncPoint::get_instance()->clear_all_call_backs();
}

TEST(MetaServiceTest, DropHdfsInfoTest) {
    auto meta_service = get_meta_service();

    auto sp = SyncPoint::get_instance();
    sp->enable_processing();
    sp->set_call_back("encrypt_ak_sk:get_encryption_key", [](auto&& args) {
        auto* ret = try_any_cast<int*>(args[0]);
        *ret = 0;
        auto* key = try_any_cast<std::string*>(args[1]);
        *key = "selectdbselectdbselectdbselectdb";
        auto* key_id = try_any_cast<int64_t*>(args[2]);
        *key_id = 1;
    });

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string key;
    std::string val;
    InstanceKeyInfo key_info {"test_instance"};
    instance_key(key_info, &key);

    ObjectStoreInfoPB obj_info;
    obj_info.set_id("1");
    obj_info.set_ak("ak");
    obj_info.set_sk("sk");
    StorageVaultPB vault;
    vault.set_name("test_hdfs_vault");
    vault.set_id("2");
    InstanceInfoPB instance;
    instance.add_obj_info()->CopyFrom(obj_info);
    instance.add_storage_vault_names(vault.name());
    instance.add_resource_ids(vault.id());
    val = instance.SerializeAsString();
    txn->put(key, val);
    txn->put(storage_vault_key({instance.instance_id(), "2"}), vault.SerializeAsString());
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    txn = nullptr;

    auto get_test_instance = [&](InstanceInfoPB& i) {
        std::string key;
        std::string val;
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        InstanceKeyInfo key_info {"test_instance"};
        instance_key(key_info, &key);
        ASSERT_EQ(txn->get(key, &val), TxnErrorCode::TXN_OK);
        i.ParseFromString(val);
    };

    // update failed because has no storage vault set
    {
        AlterObjStoreInfoRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_op(AlterObjStoreInfoRequest::DROP_HDFS_INFO);

        brpc::Controller cntl;
        AlterObjStoreInfoResponse res;
        meta_service->alter_storage_vault(
                reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT) << res.status().msg();
    }

    // update failed because vault name does not exist
    {
        AlterObjStoreInfoRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_op(AlterObjStoreInfoRequest::DROP_HDFS_INFO);
        StorageVaultPB hdfs;
        hdfs.set_name("test_hdfs_vault_not_found");
        HdfsVaultInfo params;

        hdfs.mutable_hdfs_info()->CopyFrom(params);
        req.mutable_vault()->CopyFrom(hdfs);

        brpc::Controller cntl;
        AlterObjStoreInfoResponse res;
        meta_service->alter_storage_vault(
                reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::STORAGE_VAULT_NOT_FOUND)
                << res.status().msg();
    }

    // update successfully
    {
        AlterObjStoreInfoRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_op(AlterObjStoreInfoRequest::DROP_HDFS_INFO);
        StorageVaultPB hdfs;
        hdfs.set_name("test_hdfs_vault");
        HdfsVaultInfo params;

        hdfs.mutable_hdfs_info()->CopyFrom(params);
        req.mutable_vault()->CopyFrom(hdfs);

        brpc::Controller cntl;
        AlterObjStoreInfoResponse res;
        meta_service->alter_storage_vault(
                reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();
        InstanceInfoPB instance;
        get_test_instance(instance);
        ASSERT_EQ(instance.resource_ids().size(), 0);
        ASSERT_EQ(instance.storage_vault_names().size(), 0);
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        // To test we can not read the storage vault anymore
        std::string vault_key = storage_vault_key({instance.instance_id(), "2"});
        std::string vault_value;
        auto code = txn->get(vault_key, &vault_value);
        ASSERT_TRUE(code != TxnErrorCode::TXN_OK);
    }

    {
        // Try to add one new hdfs info and then check the vault id is expected
        AlterObjStoreInfoRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_op(AlterObjStoreInfoRequest::ADD_HDFS_INFO);
        StorageVaultPB hdfs;
        hdfs.set_name("test_alter_add_hdfs_info");
        HdfsVaultInfo params;
        HdfsBuildConf conf;
        conf.set_fs_name("hdfs://127.0.0.1:8020");
        params.mutable_build_conf()->MergeFrom(conf);

        hdfs.mutable_hdfs_info()->CopyFrom(params);
        req.mutable_vault()->CopyFrom(hdfs);

        brpc::Controller cntl;
        AlterObjStoreInfoResponse res;
        meta_service->alter_storage_vault(
                reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();
        InstanceInfoPB instance;
        get_test_instance(instance);
        ASSERT_EQ(*(instance.resource_ids().begin()), "2");
        ASSERT_EQ(*(instance.storage_vault_names().begin()), "test_alter_add_hdfs_info");
    }

    // Add two more vaults
    {
        // Try to add one new hdfs info and then check the vault id is expected
        AlterObjStoreInfoRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_op(AlterObjStoreInfoRequest::ADD_HDFS_INFO);
        StorageVaultPB hdfs;
        hdfs.set_name("test_alter_add_hdfs_info_1");
        HdfsVaultInfo params;
        HdfsBuildConf conf;
        conf.set_fs_name("hdfs://127.0.0.1:8020");
        params.mutable_build_conf()->MergeFrom(conf);

        hdfs.mutable_hdfs_info()->CopyFrom(params);
        req.mutable_vault()->CopyFrom(hdfs);

        brpc::Controller cntl;
        AlterObjStoreInfoResponse res;
        meta_service->alter_storage_vault(
                reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();
        InstanceInfoPB instance;
        get_test_instance(instance);
        ASSERT_EQ(instance.resource_ids().at(1), "3");
        ASSERT_EQ(instance.storage_vault_names().at(1), "test_alter_add_hdfs_info_1");
    }

    {
        // Try to add one new hdfs info and then check the vault id is expected
        AlterObjStoreInfoRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_op(AlterObjStoreInfoRequest::ADD_HDFS_INFO);
        StorageVaultPB hdfs;
        hdfs.set_name("test_alter_add_hdfs_info_2");
        HdfsVaultInfo params;
        HdfsBuildConf conf;
        conf.set_fs_name("hdfs://127.0.0.1:8020");
        params.mutable_build_conf()->MergeFrom(conf);

        hdfs.mutable_hdfs_info()->CopyFrom(params);
        req.mutable_vault()->CopyFrom(hdfs);

        brpc::Controller cntl;
        AlterObjStoreInfoResponse res;
        meta_service->alter_storage_vault(
                reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();
        InstanceInfoPB instance;
        get_test_instance(instance);
        ASSERT_EQ(instance.resource_ids().at(2), "4");
        ASSERT_EQ(instance.storage_vault_names().at(2), "test_alter_add_hdfs_info_2");
    }

    // Remove one vault among three vaults
    {
        AlterObjStoreInfoRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_op(AlterObjStoreInfoRequest::DROP_HDFS_INFO);
        StorageVaultPB hdfs;
        hdfs.set_name("test_alter_add_hdfs_info_1");
        HdfsVaultInfo params;

        hdfs.mutable_hdfs_info()->CopyFrom(params);
        req.mutable_vault()->CopyFrom(hdfs);

        brpc::Controller cntl;
        AlterObjStoreInfoResponse res;
        meta_service->alter_storage_vault(
                reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();
        InstanceInfoPB instance;
        get_test_instance(instance);
        ASSERT_EQ(instance.resource_ids().size(), 2);
        ASSERT_EQ(instance.storage_vault_names().size(), 2);
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        // To test we can not read the storage vault anymore
        std::string vault_key = storage_vault_key({instance.instance_id(), "3"});
        std::string vault_value;
        auto code = txn->get(vault_key, &vault_value);
        ASSERT_TRUE(code != TxnErrorCode::TXN_OK);
        ASSERT_EQ(2, instance.resource_ids().size());
        ASSERT_EQ(2, instance.storage_vault_names().size());
        ASSERT_EQ(instance.resource_ids().at(0), "2");
        ASSERT_EQ(instance.storage_vault_names().at(0), "test_alter_add_hdfs_info");
        ASSERT_EQ(instance.resource_ids().at(1), "4");
        ASSERT_EQ(instance.storage_vault_names().at(1), "test_alter_add_hdfs_info_2");
    }

    {
        // Try to add one new hdfs info and then check the vault id is expected
        AlterObjStoreInfoRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_op(AlterObjStoreInfoRequest::ADD_HDFS_INFO);
        StorageVaultPB hdfs;
        hdfs.set_name("test_alter_add_hdfs_info_3");
        HdfsVaultInfo params;
        HdfsBuildConf conf;
        conf.set_fs_name("hdfs://127.0.0.1:8020");
        params.mutable_build_conf()->MergeFrom(conf);

        hdfs.mutable_hdfs_info()->CopyFrom(params);
        req.mutable_vault()->CopyFrom(hdfs);

        brpc::Controller cntl;
        AlterObjStoreInfoResponse res;
        meta_service->alter_storage_vault(
                reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();
        InstanceInfoPB instance;
        get_test_instance(instance);
        ASSERT_EQ(instance.resource_ids().at(2), "5");
        ASSERT_EQ(instance.storage_vault_names().at(2), "test_alter_add_hdfs_info_3");
    }

    SyncPoint::get_instance()->disable_processing();
    SyncPoint::get_instance()->clear_all_call_backs();
}

TEST(MetaServiceTest, GetDefaultVaultTest) {
    auto meta_service = get_meta_service();

    auto get_test_instance = [&](InstanceInfoPB& i, std::string instance_id) {
        std::string key;
        std::string val;
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        InstanceKeyInfo key_info {std::move(instance_id)};
        instance_key(key_info, &key);
        ASSERT_EQ(txn->get(key, &val), TxnErrorCode::TXN_OK);
        i.ParseFromString(val);
    };

    // case: normal create instance with hdfs info
    {
        brpc::Controller cntl;
        CreateInstanceRequest req;
        std::string instance_id = "test_instance_with_hdfs_info";
        req.set_instance_id(instance_id);
        req.set_user_id("test_user");
        req.set_name("test_name");
        HdfsVaultInfo hdfs;
        HdfsBuildConf conf;
        conf.set_fs_name("hdfs://127.0.0.1:8020");
        conf.set_user("test_user");
        hdfs.mutable_build_conf()->CopyFrom(conf);
        StorageVaultPB vault;
        vault.mutable_hdfs_info()->CopyFrom(hdfs);
        req.mutable_vault()->CopyFrom(vault);

        auto sp = SyncPoint::get_instance();
        sp->set_call_back("create_object_info_with_encrypt", [](auto&& args) {
            auto* ret = try_any_cast<int*>(args[0]);
            *ret = 0;
        });
        sp->enable_processing();
        CreateInstanceResponse res;
        meta_service->create_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        InstanceInfoPB i;
        get_test_instance(i, instance_id);
        // It wouldn't be set
        ASSERT_EQ(i.default_storage_vault_id(), "");
        ASSERT_EQ(i.default_storage_vault_name(), "");
        ASSERT_EQ(i.resource_ids().at(0), "1");
        ASSERT_EQ(i.storage_vault_names().at(0), "built_in_storage_vault");
        sp->clear_all_call_backs();
        sp->clear_trace();
        sp->disable_processing();
    }

    // case: normal create instance with obj info
    {
        brpc::Controller cntl;
        CreateInstanceRequest req;
        std::string instance_id = "test_instance_with_s3_info";
        req.set_instance_id(instance_id);
        req.set_user_id("test_user");
        req.set_name("test_name");
        InstanceInfoPB instance;

        EncryptionInfoPB encryption_info;
        encryption_info.set_encryption_method("AES_256_ECB");
        encryption_info.set_key_id(1);

        std::string cipher_sk = "JUkuTDctR+ckJtnPkLScWaQZRcOtWBhsLLpnCRxQLxr734qB8cs6gNLH6grE1FxO";
        std::string plain_sk = "Hx60p12123af234541nsVsffdfsdfghsdfhsdf34t";
        ObjectStoreInfoPB obj_info;
        obj_info.mutable_encryption_info()->CopyFrom(encryption_info);
        obj_info.set_ak("akak1");
        obj_info.set_sk(cipher_sk);
        obj_info.set_endpoint("selectdb");
        obj_info.set_external_endpoint("velodb");
        obj_info.set_bucket("gavin");
        obj_info.set_region("American");
        obj_info.set_provider(ObjectStoreInfoPB::Provider::ObjectStoreInfoPB_Provider_S3);
        instance.add_obj_info()->CopyFrom(obj_info);
        req.mutable_obj_info()->CopyFrom(obj_info);

        auto sp = SyncPoint::get_instance();
        sp->set_call_back("create_object_info_with_encrypt", [](auto&& args) {
            auto* ret = try_any_cast<int*>(args[0]);
            *ret = 0;
            auto* code = try_any_cast<MetaServiceCode*>(args[1]);
            *code = MetaServiceCode::OK;
            auto* msg = try_any_cast<std::string*>(args[2]);
            *msg = "";
        });
        sp->enable_processing();
        CreateInstanceResponse res;
        meta_service->create_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        InstanceInfoPB i;
        get_test_instance(i, instance_id);
        sp->clear_all_call_backs();
        sp->clear_trace();
        sp->disable_processing();
    }
}

TEST(MetaServiceTest, SetDefaultVaultTest) {
    auto meta_service = get_meta_service();
    std::string instance_id = "test_instance";

    auto get_test_instance = [&](InstanceInfoPB& i) {
        std::string key;
        std::string val;
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        InstanceKeyInfo key_info {instance_id};
        instance_key(key_info, &key);
        ASSERT_EQ(txn->get(key, &val), TxnErrorCode::TXN_OK);
        i.ParseFromString(val);
    };

    brpc::Controller cntl;
    CreateInstanceRequest req;
    req.set_instance_id(instance_id);
    req.set_user_id("test_user");
    req.set_name("test_name");
    HdfsVaultInfo hdfs;
    HdfsBuildConf conf;
    conf.set_fs_name("hdfs://127.0.0.1:8020");
    conf.set_user("test_user");
    hdfs.mutable_build_conf()->CopyFrom(conf);
    StorageVaultPB vault;
    vault.mutable_hdfs_info()->CopyFrom(hdfs);
    req.mutable_vault()->CopyFrom(vault);

    auto sp = SyncPoint::get_instance();
    sp->set_call_back("encrypt_ak_sk:get_encryption_key", [](auto&& args) {
        auto* ret = try_any_cast<int*>(args[0]);
        *ret = 0;
        auto* key = try_any_cast<std::string*>(args[1]);
        *key = "test";
        auto* key_id = try_any_cast<int64_t*>(args[2]);
        *key_id = 1;
    });
    sp->enable_processing();
    CreateInstanceResponse res;
    meta_service->create_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                  &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    InstanceInfoPB i;
    get_test_instance(i);
    ASSERT_EQ(i.default_storage_vault_id(), "");
    ASSERT_EQ(i.default_storage_vault_name(), "");
    ASSERT_EQ(i.resource_ids().at(0), "1");
    ASSERT_EQ(i.storage_vault_names().at(0), "built_in_storage_vault");

    for (size_t i = 0; i < 20; i++) {
        AlterObjStoreInfoRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_op(AlterObjStoreInfoRequest::ADD_HDFS_INFO);
        StorageVaultPB hdfs;
        auto name = fmt::format("test_alter_add_hdfs_info_{}", i);
        hdfs.set_name(name);
        HdfsVaultInfo params;
        HdfsBuildConf conf;
        conf.set_fs_name("hdfs://127.0.0.1:8020");
        params.mutable_build_conf()->MergeFrom(conf);

        hdfs.mutable_hdfs_info()->CopyFrom(params);
        req.mutable_vault()->CopyFrom(hdfs);

        brpc::Controller cntl;
        AlterObjStoreInfoResponse res;
        meta_service->alter_storage_vault(
                reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();

        AlterObjStoreInfoRequest set_default_req;
        set_default_req.set_cloud_unique_id("test_cloud_unique_id");
        set_default_req.set_op(AlterObjStoreInfoRequest::SET_DEFAULT_VAULT);
        set_default_req.mutable_vault()->CopyFrom(hdfs);
        AlterObjStoreInfoResponse set_default_res;
        meta_service->alter_storage_vault(
                reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &set_default_req,
                &set_default_res, nullptr);
        ASSERT_EQ(set_default_res.status().code(), MetaServiceCode::OK)
                << set_default_res.status().msg();

        InstanceInfoPB instance;
        get_test_instance(instance);
        ASSERT_EQ(std::to_string(i + 2), instance.default_storage_vault_id());
    }

    // Try to set one non-existent vault as default
    {
        StorageVaultPB hdfs;
        auto name = "test_alter_add_hdfs_info_no";
        hdfs.set_name(name);
        HdfsVaultInfo params;

        hdfs.mutable_hdfs_info()->CopyFrom(params);
        AlterObjStoreInfoRequest set_default_req;
        set_default_req.set_cloud_unique_id("test_cloud_unique_id");
        set_default_req.set_op(AlterObjStoreInfoRequest::SET_DEFAULT_VAULT);
        set_default_req.mutable_vault()->CopyFrom(hdfs);
        AlterObjStoreInfoResponse set_default_res;
        meta_service->alter_storage_vault(
                reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &set_default_req,
                &set_default_res, nullptr);
        ASSERT_NE(set_default_res.status().code(), MetaServiceCode::OK)
                << set_default_res.status().msg();
    }

    sp->clear_all_call_backs();
    sp->clear_trace();
    sp->disable_processing();
}

TEST(MetaServiceTest, GetObjStoreInfoTest) {
    auto meta_service = get_meta_service();

    auto sp = SyncPoint::get_instance();
    sp->enable_processing();
    sp->set_call_back("encrypt_ak_sk:get_encryption_key", [](auto&& args) {
        auto* ret = try_any_cast<int*>(args[0]);
        *ret = 0;
        auto* key = try_any_cast<std::string*>(args[1]);
        *key = "selectdbselectdbselectdbselectdb";
        auto* key_id = try_any_cast<int64_t*>(args[2]);
        *key_id = 1;
    });

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string key;
    std::string val;
    InstanceKeyInfo key_info {"test_instance"};
    instance_key(key_info, &key);

    ObjectStoreInfoPB obj_info;
    obj_info.set_id("1");
    obj_info.set_ak("ak");
    obj_info.set_sk("sk");
    StorageVaultPB vault;
    vault.set_name("test_hdfs_vault");
    vault.set_id("2");
    InstanceInfoPB instance;
    instance.add_obj_info()->CopyFrom(obj_info);
    instance.add_storage_vault_names(vault.name());
    instance.add_resource_ids(vault.id());
    instance.set_instance_id("GetObjStoreInfoTestInstance");
    val = instance.SerializeAsString();
    txn->put(key, val);
    txn->put(storage_vault_key({instance.instance_id(), "2"}), vault.SerializeAsString());
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    txn = nullptr;

    auto get_test_instance = [&](InstanceInfoPB& i) {
        std::string key;
        std::string val;
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        InstanceKeyInfo key_info {"test_instance"};
        instance_key(key_info, &key);
        ASSERT_EQ(txn->get(key, &val), TxnErrorCode::TXN_OK);
        i.ParseFromString(val);
    };

    for (size_t i = 0; i < 20; i++) {
        AlterObjStoreInfoRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_op(AlterObjStoreInfoRequest::ADD_HDFS_INFO);
        StorageVaultPB hdfs;
        auto name = fmt::format("test_alter_add_hdfs_info_{}", i);
        hdfs.set_name(name);
        HdfsVaultInfo params;
        HdfsBuildConf conf;
        conf.set_fs_name("hdfs://127.0.0.1:8020");
        params.mutable_build_conf()->MergeFrom(conf);

        hdfs.mutable_hdfs_info()->CopyFrom(params);
        req.mutable_vault()->CopyFrom(hdfs);

        brpc::Controller cntl;
        AlterObjStoreInfoResponse res;
        meta_service->alter_storage_vault(
                reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();
        InstanceInfoPB instance;
        get_test_instance(instance);
        ASSERT_TRUE(std::find_if(instance.resource_ids().begin(), instance.resource_ids().end(),
                                 [&](const auto& id) { return id == std::to_string(i + 3); }) !=
                    instance.resource_ids().end());
        ASSERT_TRUE(std::find_if(instance.storage_vault_names().begin(),
                                 instance.storage_vault_names().end(), [&](const auto& vault_name) {
                                     return name == vault_name;
                                 }) != instance.storage_vault_names().end());
    }

    {
        brpc::Controller cntl;
        GetObjStoreInfoRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        GetObjStoreInfoResponse res;
        meta_service->get_obj_store_info(
                reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();
        ASSERT_EQ(res.storage_vault().size(), 21);
        const auto& vaults = res.storage_vault();
        for (size_t i = 0; i < 20; i++) {
            auto id = std::to_string(i + 3);
            auto name = fmt::format("test_alter_add_hdfs_info_{}", i);
            ASSERT_TRUE(std::find_if(vaults.begin(), vaults.end(), [&](const auto& vault) {
                            return id == vault.id();
                        }) != vaults.end());
            ASSERT_TRUE(std::find_if(vaults.begin(), vaults.end(), [&](const auto& vault) {
                            return name == vault.name();
                        }) != vaults.end());
        }
    }

    SyncPoint::get_instance()->disable_processing();
    SyncPoint::get_instance()->clear_all_call_backs();
}

TEST(MetaServiceTest, CreateVersionedTablet) {
    auto meta_service = get_meta_service(false);

    auto sp = SyncPoint::get_instance();
    sp->enable_processing();
    sp->set_call_back("encrypt_ak_sk:get_encryption_key", [](auto&& args) {
        auto* ret = try_any_cast<int*>(args[0]);
        *ret = 0;
        auto* key = try_any_cast<std::string*>(args[1]);
        *key = "selectdbselectdbselectdbselectdb";
        auto* key_id = try_any_cast<int64_t*>(args[2]);
        *key_id = 1;
    });

    std::string instance_id = "test_instance";
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string key;
        std::string val;
        InstanceKeyInfo key_info {"test_instance"};
        instance_key(key_info, &key);

        InstanceInfoPB instance;
        instance.set_multi_version_status(MultiVersionStatus::MULTI_VERSION_WRITE_ONLY);
        val = instance.SerializeAsString();
        txn->put(key, val);
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        meta_service->resource_mgr()->refresh_instance(instance_id);
    }

    int64_t db_id = 1, table_id = 2, index_id = 3, partition_id = 4, tablet_id = 5;

    {
        brpc::Controller cntl;
        CreateTabletsRequest req;
        CreateTabletsResponse res;
        req.set_cloud_unique_id(fmt::format("1:{}:1", instance_id));
        req.set_db_id(db_id);
        add_tablet(req, table_id, index_id, partition_id, tablet_id);
        meta_service->create_tablets(&cntl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << tablet_id;
    }

    Versionstamp commit_versionstamp;
    {
        // verify versioned tablet meta is written
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string val;
        std::string key = versioned::meta_tablet_key({instance_id, tablet_id});
        ASSERT_EQ(versioned_get(txn.get(), key, &commit_versionstamp, &val), TxnErrorCode::TXN_OK)
                << hex(key);
        TabletMetaCloudPB versioned_tablet_meta;
        versioned_tablet_meta.ParseFromString(val);
        ASSERT_EQ(versioned_tablet_meta.table_id(), table_id);
        ASSERT_EQ(versioned_tablet_meta.index_id(), index_id)
                << versioned_tablet_meta.ShortDebugString();
        ASSERT_EQ(versioned_tablet_meta.partition_id(), partition_id);
        ASSERT_EQ(versioned_tablet_meta.tablet_id(), tablet_id);
    }

    {
        // verify versioned tablet index/inverted index is written
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string key = versioned::tablet_index_key({instance_id, tablet_id});
        std::string val;
        ASSERT_EQ(txn->get(key, &val), TxnErrorCode::TXN_OK);
        TabletIndexPB tablet_index;
        tablet_index.ParseFromString(val);
        ASSERT_EQ(tablet_index.db_id(), db_id);
        ASSERT_EQ(tablet_index.table_id(), table_id);
        ASSERT_EQ(tablet_index.index_id(), index_id);
        ASSERT_EQ(tablet_index.partition_id(), partition_id);
        ASSERT_EQ(tablet_index.tablet_id(), tablet_id);

        key = versioned::tablet_inverted_index_key(
                {instance_id, db_id, table_id, index_id, partition_id, tablet_id});
        ASSERT_EQ(txn->get(key, &val), TxnErrorCode::TXN_OK);
    }

    {
        // verify the first versioned rowset meta is written
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        int64_t end_version = 1; // from add_tablet
        std::string key = versioned::meta_rowset_load_key({instance_id, tablet_id, end_version});
        RowsetMetaCloudPB rowset_meta;
        Versionstamp versionstamp;
        ASSERT_EQ(versioned::document_get(txn.get(), key, &rowset_meta, &versionstamp),
                  TxnErrorCode::TXN_OK);
        ASSERT_EQ(versionstamp, commit_versionstamp);
    }

    {
        // verify the tablet load stats is written
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string key = versioned::tablet_load_stats_key({instance_id, tablet_id});
        TabletStatsPB load_stats;
        Versionstamp versionstamp;
        ASSERT_EQ(versioned::document_get(txn.get(), key, &load_stats, &versionstamp),
                  TxnErrorCode::TXN_OK);
        ASSERT_EQ(versionstamp, commit_versionstamp);
    }

    {
        // verify the tablet compact stats is written
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string key = versioned::tablet_compact_stats_key({instance_id, tablet_id});
        TabletStatsPB compact_stats;
        Versionstamp versionstamp;
        ASSERT_EQ(versioned::document_get(txn.get(), key, &compact_stats, &versionstamp),
                  TxnErrorCode::TXN_OK);
        ASSERT_EQ(versionstamp, commit_versionstamp);
        EXPECT_EQ(compact_stats.cumulative_point(), 2);
    }

    SyncPoint::get_instance()->disable_processing();
    SyncPoint::get_instance()->clear_all_call_backs();
}

TEST(MetaServiceTest, CreateTabletsVaultsTest) {
    auto meta_service = get_meta_service();

    auto sp = SyncPoint::get_instance();
    sp->enable_processing();
    sp->set_call_back("encrypt_ak_sk:get_encryption_key", [](auto&& args) {
        auto* ret = try_any_cast<int*>(args[0]);
        *ret = 0;
        auto* key = try_any_cast<std::string*>(args[1]);
        *key = "selectdbselectdbselectdbselectdb";
        auto* key_id = try_any_cast<int64_t*>(args[2]);
        *key_id = 1;
    });

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string key;
    std::string val;
    InstanceKeyInfo key_info {"test_instance"};
    instance_key(key_info, &key);

    InstanceInfoPB instance;
    instance.set_enable_storage_vault(true);
    val = instance.SerializeAsString();
    txn->put(key, val);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    auto get_test_instance = [&](InstanceInfoPB& i) {
        std::string key;
        std::string val;
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        InstanceKeyInfo key_info {"test_instance"};
        instance_key(key_info, &key);
        ASSERT_EQ(txn->get(key, &val), TxnErrorCode::TXN_OK);
        i.ParseFromString(val);
    };

    // tablet_metas_size is 0
    {
        CreateTabletsRequest request;
        request.set_cloud_unique_id("test_cloud_unique_id");

        brpc::Controller cntl;
        CreateTabletsResponse response;
        meta_service->create_tablets(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &request, &response, nullptr);
        ASSERT_EQ(response.status().code(), MetaServiceCode::INVALID_ARGUMENT)
                << response.status().msg();
    }

    // try to use default
    {
        CreateTabletsRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_storage_vault_name("");
        req.add_tablet_metas();

        brpc::Controller cntl;
        CreateTabletsResponse res;
        meta_service->create_tablets(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        // failed because no default
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT) << res.status().msg();
    }

    // Create One Hdfs info as built_in vault
    {
        AlterObjStoreInfoRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_op(AlterObjStoreInfoRequest::ADD_HDFS_INFO);
        StorageVaultPB hdfs;
        hdfs.set_name("built_in_storage_vault");
        HdfsVaultInfo params;
        params.mutable_build_conf()->set_fs_name("hdfs://ip:port");

        hdfs.mutable_hdfs_info()->CopyFrom(params);
        req.mutable_vault()->CopyFrom(hdfs);

        brpc::Controller cntl;
        AlterObjStoreInfoResponse res;
        meta_service->alter_storage_vault(
                reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();

        InstanceInfoPB i;
        get_test_instance(i);
        ASSERT_EQ(i.default_storage_vault_id(), "");
        ASSERT_EQ(i.default_storage_vault_name(), "");
        ASSERT_EQ(i.resource_ids().at(0), "1");
        ASSERT_EQ(i.storage_vault_names().at(0), "built_in_storage_vault");
    }

    // Try to set built_in_storage_vault vault as default
    {
        StorageVaultPB hdfs;
        std::string name = "built_in_storage_vault";
        hdfs.set_name(std::move(name));
        HdfsVaultInfo params;

        hdfs.mutable_hdfs_info()->CopyFrom(params);
        AlterObjStoreInfoRequest set_default_req;
        set_default_req.set_cloud_unique_id("test_cloud_unique_id");
        set_default_req.set_op(AlterObjStoreInfoRequest::SET_DEFAULT_VAULT);
        set_default_req.mutable_vault()->CopyFrom(hdfs);
        AlterObjStoreInfoResponse set_default_res;
        brpc::Controller cntl;
        meta_service->alter_storage_vault(
                reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &set_default_req,
                &set_default_res, nullptr);
        ASSERT_EQ(set_default_res.status().code(), MetaServiceCode::OK)
                << set_default_res.status().msg();
    }

    // try to use default vault
    {
        CreateTabletsRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_storage_vault_name("");
        req.add_tablet_metas();

        auto sp = SyncPoint::get_instance();
        sp->set_call_back("create_tablets",
                          [](auto&& args) { *try_any_cast<bool*>(args.back()) = true; });
        sp->enable_processing();

        brpc::Controller cntl;
        CreateTabletsResponse res;
        meta_service->create_tablets(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();
        ASSERT_EQ(res.storage_vault_id(), "1");
        ASSERT_EQ(res.storage_vault_name(), "built_in_storage_vault");

        sp->clear_call_back("create_tablets");
    }

    // try to use one non-existent vault
    {
        CreateTabletsRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_storage_vault_name("non-existent");
        req.add_tablet_metas();

        auto sp = SyncPoint::get_instance();
        SyncPoint::CallbackGuard guard;
        sp->set_call_back(
                "create_tablets", [](auto&& args) { *try_any_cast<bool*>(args.back()) = true; },
                &guard);
        sp->enable_processing();

        brpc::Controller cntl;
        CreateTabletsResponse res;
        meta_service->create_tablets(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT) << res.status().msg();
    }

    SyncPoint::get_instance()->disable_processing();
    SyncPoint::get_instance()->clear_all_call_backs();
}

TEST(MetaServiceTest, UpdateAkSkTest) {
    auto meta_service = get_meta_service();

    auto sp = SyncPoint::get_instance();
    sp->enable_processing();
    sp->set_call_back("encrypt_ak_sk:get_encryption_key", [](auto&& args) {
        auto* ret = try_any_cast<int*>(args[0]);
        *ret = 0;
        auto* key = try_any_cast<std::string*>(args[1]);
        *key = "selectdbselectdbselectdbselectdb";
        auto* key_id = try_any_cast<int64_t*>(args[2]);
        *key_id = 1;
    });

    auto get_test_instance = [&](InstanceInfoPB& i) {
        std::string key;
        std::string val;
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        InstanceKeyInfo key_info {"test_instance"};
        instance_key(key_info, &key);
        ASSERT_EQ(txn->get(key, &val), TxnErrorCode::TXN_OK);
        i.ParseFromString(val);
    };

    std::string cipher_sk = "JUkuTDctR+ckJtnPkLScWaQZRcOtWBhsLLpnCRxQLxr734qB8cs6gNLH6grE1FxO";
    std::string plain_sk = "Hx60p12123af234541nsVsffdfsdfghsdfhsdf34t";

    auto update = [&](bool with_user_id, bool with_wrong_user_id) {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string key;
        std::string val;
        InstanceKeyInfo key_info {"test_instance"};
        instance_key(key_info, &key);

        ObjectStoreInfoPB obj_info;
        if (with_user_id) {
            obj_info.set_user_id("111");
        }
        obj_info.set_ak("ak");
        obj_info.set_sk("sk");
        InstanceInfoPB instance;
        instance.add_obj_info()->CopyFrom(obj_info);
        val = instance.SerializeAsString();
        txn->put(key, val);
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        UpdateAkSkRequest req;
        req.set_instance_id("test_instance");
        RamUserPB ram_user;
        if (with_wrong_user_id) {
            ram_user.set_user_id("222");
        } else {
            ram_user.set_user_id("111");
        }
        ram_user.set_ak("new_ak");
        ram_user.set_sk(plain_sk);
        req.add_internal_bucket_user()->CopyFrom(ram_user);

        brpc::Controller cntl;
        UpdateAkSkResponse res;
        meta_service->update_ak_sk(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                   &req, &res, nullptr);
        if (with_wrong_user_id) {
            ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
        } else {
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            InstanceInfoPB update_instance;
            get_test_instance(update_instance);
            ASSERT_EQ(update_instance.obj_info(0).user_id(), "111");
            ASSERT_EQ(update_instance.obj_info(0).ak(), "new_ak");
            ASSERT_EQ(update_instance.obj_info(0).sk(), cipher_sk);
        }
    };

    update(false, false);
    update(true, false);
    update(true, true);
}

TEST(MetaServiceTest, AlterIamTest) {
    auto meta_service = get_meta_service();

    auto sp = SyncPoint::get_instance();
    sp->enable_processing();
    sp->set_call_back("encrypt_ak_sk:get_encryption_key", [](auto&& args) {
        auto* ret = try_any_cast<int*>(args[0]);
        *ret = 0;
        auto* key = try_any_cast<std::string*>(args[1]);
        *key = "selectdbselectdbselectdbselectdb";
        auto* key_id = try_any_cast<int64_t*>(args[2]);
        *key_id = 1;
    });

    std::string cipher_sk = "JUkuTDctR+ckJtnPkLScWaQZRcOtWBhsLLpnCRxQLxr734qB8cs6gNLH6grE1FxO";
    std::string plain_sk = "Hx60p12123af234541nsVsffdfsdfghsdfhsdf34t";

    auto get_arn_info_key = [&](RamUserPB& i) {
        std::string val;
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        ASSERT_EQ(txn->get(system_meta_service_arn_info_key(), &val), TxnErrorCode::TXN_OK);
        i.ParseFromString(val);
    };

    // add new system_meta_service_arn_info_key
    {
        AlterIamRequest req;
        req.set_account_id("123");
        req.set_ak("ak1");
        req.set_sk(plain_sk);

        brpc::Controller cntl;
        AlterIamResponse res;
        meta_service->alter_iam(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        RamUserPB ram_user;
        get_arn_info_key(ram_user);
        ASSERT_EQ(ram_user.user_id(), "123");
        ASSERT_EQ(ram_user.ak(), "ak1");
        ASSERT_EQ(ram_user.sk(), cipher_sk);
    }
    // with old system_meta_service_arn_info_key
    {
        AlterIamRequest req;
        req.set_account_id("321");
        req.set_ak("ak2");
        req.set_sk(plain_sk);

        brpc::Controller cntl;
        AlterIamResponse res;
        meta_service->alter_iam(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        RamUserPB ram_user;
        get_arn_info_key(ram_user);
        ASSERT_EQ(ram_user.user_id(), "321");
        ASSERT_EQ(ram_user.ak(), "ak2");
        ASSERT_EQ(ram_user.sk(), cipher_sk);
    }

    SyncPoint::get_instance()->disable_processing();
    SyncPoint::get_instance()->clear_all_call_backs();
}

TEST(MetaServiceTest, UpdateTmpRowsetTest) {
    auto meta_service = get_meta_service();

    std::string instance_id = "update_rowset_meta_test_instance_id";
    auto sp = SyncPoint::get_instance();
    DORIS_CLOUD_DEFER {
        SyncPoint::get_instance()->clear_all_call_backs();
    };
    sp->set_call_back("get_instance_id", [&](auto&& args) {
        auto* ret = try_any_cast_ret<std::string>(args);
        ret->first = instance_id;
        ret->second = true;
    });
    sp->enable_processing();

    {
        // 1. normal path
        constexpr auto db_id = 10000, table_id = 10001, index_id = 10002, partition_id = 10003,
                       tablet_id = 10004;
        int64_t txn_id = 0;
        std::string label = "update_rowset_meta_test_label1";
        CreateRowsetResponse res;

        ASSERT_NO_FATAL_FAILURE(
                create_tablet(meta_service.get(), table_id, index_id, partition_id, tablet_id));

        ASSERT_NO_FATAL_FAILURE(begin_txn(meta_service.get(), db_id, label, table_id, txn_id));
        auto rowset = create_rowset(txn_id, tablet_id, partition_id);
        ASSERT_NO_FATAL_FAILURE(prepare_rowset(meta_service.get(), rowset, res));
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << label;
        res.Clear();

        rowset.set_rowset_state(doris::BEGIN_PARTIAL_UPDATE);
        ASSERT_NO_FATAL_FAILURE(commit_rowset(meta_service.get(), rowset, res));
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << label;
        res.Clear();

        // simulate that there are new segments added to this rowset
        rowset.set_num_segments(rowset.num_segments() + 3);
        rowset.set_num_rows(rowset.num_rows() + 1000);
        rowset.set_total_disk_size(rowset.total_disk_size() + 11000);
        rowset.set_index_disk_size(rowset.index_disk_size() + 1000);
        rowset.set_data_disk_size(rowset.data_disk_size() + 10000);

        ASSERT_NO_FATAL_FAILURE(update_tmp_rowset(meta_service.get(), rowset, res));
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << label;

        std::string key;
        std::string val;
        MetaRowsetTmpKeyInfo key_info {instance_id, txn_id, tablet_id};
        meta_rowset_tmp_key(key_info, &key);
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        ASSERT_EQ(txn->get(key, &val), TxnErrorCode::TXN_OK);
        doris::RowsetMetaCloudPB fetchedRowsetMeta;
        ASSERT_TRUE(fetchedRowsetMeta.ParseFromString(val));

        ASSERT_EQ(doris::BEGIN_PARTIAL_UPDATE, fetchedRowsetMeta.rowset_state());
        ASSERT_EQ(rowset.num_segments(), fetchedRowsetMeta.num_segments());
        ASSERT_EQ(rowset.num_rows(), fetchedRowsetMeta.num_rows());
        ASSERT_EQ(rowset.total_disk_size(), fetchedRowsetMeta.total_disk_size());
        ASSERT_EQ(rowset.index_disk_size(), fetchedRowsetMeta.index_disk_size());
        ASSERT_EQ(rowset.data_disk_size(), fetchedRowsetMeta.data_disk_size());

        ASSERT_NO_FATAL_FAILURE(commit_txn(meta_service.get(), db_id, txn_id, label));
    }

    {
        // 2. rpc retryies due to network error will success
        constexpr auto db_id = 20000, table_id = 20001, index_id = 20002, partition_id = 20003,
                       tablet_id = 20004;
        int64_t txn_id = 0;
        std::string label = "update_rowset_meta_test_label2";
        CreateRowsetResponse res;

        ASSERT_NO_FATAL_FAILURE(
                create_tablet(meta_service.get(), table_id, index_id, partition_id, tablet_id));

        ASSERT_NO_FATAL_FAILURE(begin_txn(meta_service.get(), db_id, label, table_id, txn_id));
        auto rowset = create_rowset(txn_id, tablet_id, partition_id);
        ASSERT_NO_FATAL_FAILURE(prepare_rowset(meta_service.get(), rowset, res));
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << label;
        res.Clear();

        rowset.set_rowset_state(doris::BEGIN_PARTIAL_UPDATE);
        ASSERT_NO_FATAL_FAILURE(commit_rowset(meta_service.get(), rowset, res));
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << label;
        res.Clear();

        // simulate that there are new segments added to this rowset
        rowset.set_num_segments(rowset.num_segments() + 3);
        rowset.set_num_rows(rowset.num_rows() + 1000);
        rowset.set_total_disk_size(rowset.total_disk_size() + 11000);
        rowset.set_index_disk_size(rowset.index_disk_size() + 1000);
        rowset.set_data_disk_size(rowset.data_disk_size() + 10000);

        // repeated calls to update_tmp_rowset will all success
        ASSERT_NO_FATAL_FAILURE(update_tmp_rowset(meta_service.get(), rowset, res));
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << label;
        ASSERT_NO_FATAL_FAILURE(update_tmp_rowset(meta_service.get(), rowset, res));
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << label;
        ASSERT_NO_FATAL_FAILURE(update_tmp_rowset(meta_service.get(), rowset, res));
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << label;
        ASSERT_NO_FATAL_FAILURE(update_tmp_rowset(meta_service.get(), rowset, res));
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << label;

        std::string key;
        std::string val;
        MetaRowsetTmpKeyInfo key_info {instance_id, txn_id, tablet_id};
        meta_rowset_tmp_key(key_info, &key);
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        ASSERT_EQ(txn->get(key, &val), TxnErrorCode::TXN_OK);
        doris::RowsetMetaCloudPB fetchedRowsetMeta;
        ASSERT_TRUE(fetchedRowsetMeta.ParseFromString(val));

        ASSERT_EQ(doris::BEGIN_PARTIAL_UPDATE, fetchedRowsetMeta.rowset_state());
        ASSERT_EQ(rowset.num_segments(), fetchedRowsetMeta.num_segments());
        ASSERT_EQ(rowset.num_rows(), fetchedRowsetMeta.num_rows());
        ASSERT_EQ(rowset.total_disk_size(), fetchedRowsetMeta.total_disk_size());
        ASSERT_EQ(rowset.index_disk_size(), fetchedRowsetMeta.index_disk_size());
        ASSERT_EQ(rowset.data_disk_size(), fetchedRowsetMeta.data_disk_size());

        ASSERT_NO_FATAL_FAILURE(commit_txn(meta_service.get(), db_id, txn_id, label));
    }

    {
        // 3. call update_tmp_rowset without commit_rowset first will fail
        constexpr auto db_id = 30000, table_id = 30001, index_id = 30002, partition_id = 30003,
                       tablet_id = 30004;
        int64_t txn_id = 0;
        std::string label = "update_rowset_meta_test_label1";
        CreateRowsetResponse res;

        ASSERT_NO_FATAL_FAILURE(
                create_tablet(meta_service.get(), table_id, index_id, partition_id, tablet_id));

        ASSERT_NO_FATAL_FAILURE(begin_txn(meta_service.get(), db_id, label, table_id, txn_id));
        auto rowset = create_rowset(txn_id, tablet_id, partition_id);
        ASSERT_NO_FATAL_FAILURE(prepare_rowset(meta_service.get(), rowset, res));
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << label;
        res.Clear();

        // simulate that there are new segments added to this rowset
        rowset.set_num_segments(rowset.num_segments() + 3);
        rowset.set_num_rows(rowset.num_rows() + 1000);
        rowset.set_total_disk_size(rowset.total_disk_size() + 11000);
        rowset.set_index_disk_size(rowset.index_disk_size() + 1000);
        rowset.set_data_disk_size(rowset.data_disk_size() + 10000);

        ASSERT_NO_FATAL_FAILURE(update_tmp_rowset(meta_service.get(), rowset, res));
        ASSERT_EQ(res.status().code(), MetaServiceCode::ROWSET_META_NOT_FOUND) << label;
    }
}

TEST(MetaServiceTest, CreateS3VaultWithIamRole) {
    auto sp = SyncPoint::get_instance();
    sp->enable_processing();
    sp->set_call_back("encrypt_ak_sk:get_encryption_key", [](auto&& args) {
        auto* ret = try_any_cast<int*>(args[0]);
        *ret = 0;
        auto* key = try_any_cast<std::string*>(args[1]);
        *key = "selectdbselectdbselectdbselectdb";
        auto* key_id = try_any_cast<int64_t*>(args[2]);
        *key_id = 1;
    });
    std::pair<std::string, std::string> pair;
    sp->set_call_back("extract_object_storage_info:get_aksk_pair", [&](auto&& args) {
        auto* ret = try_any_cast<std::pair<std::string, std::string>*>(args[0]);
        pair = *ret;
    });

    auto meta_service = get_meta_service();

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string key;
    std::string val;
    InstanceKeyInfo key_info {"test_instance"};
    instance_key(key_info, &key);

    ObjectStoreInfoPB obj_info;
    obj_info.set_id("1");
    obj_info.set_ak("ak");
    obj_info.set_sk("sk");
    StorageVaultPB vault;
    constexpr char vault_name[] = "test_alter_s3_vault";
    vault.mutable_obj_info()->MergeFrom(obj_info);
    vault.set_name(vault_name);
    vault.set_id("2");
    InstanceInfoPB instance;
    instance.add_storage_vault_names(vault.name());
    instance.add_resource_ids(vault.id());
    instance.set_instance_id("GetObjStoreInfoTestInstance");
    instance.set_enable_storage_vault(true);
    val = instance.SerializeAsString();
    txn->put(key, val);
    txn->put(storage_vault_key({instance.instance_id(), "2"}), vault.SerializeAsString());
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    txn = nullptr;

    auto get_test_instance = [&](InstanceInfoPB& i) {
        std::string key;
        std::string val;
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        InstanceKeyInfo key_info {"test_instance"};
        instance_key(key_info, &key);
        ASSERT_EQ(txn->get(key, &val), TxnErrorCode::TXN_OK);
        i.ParseFromString(val);
    };

    {
        AlterObjStoreInfoRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_op(AlterObjStoreInfoRequest::ADD_S3_VAULT);
        StorageVaultPB vault;
        vault.mutable_obj_info()->set_endpoint("s3.us-east-1.amazonaws.com");
        vault.mutable_obj_info()->set_region("us-east-1");
        vault.mutable_obj_info()->set_bucket("test_bucket");
        vault.mutable_obj_info()->set_prefix("test_prefix");
        vault.mutable_obj_info()->set_ak("new_ak");
        vault.mutable_obj_info()->set_sk("new_sk");
        vault.mutable_obj_info()->set_provider(
                ObjectStoreInfoPB::Provider::ObjectStoreInfoPB_Provider_S3);

        vault.set_name("ak_sk_s3_vault");
        req.mutable_vault()->CopyFrom(vault);

        brpc::Controller cntl;
        AlterObjStoreInfoResponse res;
        meta_service->alter_storage_vault(
                reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

        {
            InstanceInfoPB instance;
            get_test_instance(instance);
            std::unique_ptr<Transaction> txn;
            ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
            std::string val;
            ASSERT_EQ(txn->get(storage_vault_key({instance.instance_id(), "2"}), &val),
                      TxnErrorCode::TXN_OK);
            StorageVaultPB get_obj;
            get_obj.ParseFromString(val);
            ASSERT_EQ(get_obj.obj_info().ak(), "ak") << get_obj.obj_info().ak();
        }

        {
            InstanceInfoPB instance;
            get_test_instance(instance);
            std::unique_ptr<Transaction> txn;
            ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
            std::string val;
            ASSERT_EQ(txn->get(storage_vault_key({instance.instance_id(), "3"}), &val),
                      TxnErrorCode::TXN_OK);
            StorageVaultPB get_obj;
            get_obj.ParseFromString(val);
            ASSERT_EQ(get_obj.obj_info().ak(), "new_ak") << get_obj.obj_info().ak();
            ASSERT_NE(get_obj.obj_info().sk(), "new_sk") << get_obj.obj_info().sk();
        }
    }

    {
        AlterObjStoreInfoRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_op(AlterObjStoreInfoRequest::ADD_S3_VAULT);
        StorageVaultPB vault;
        vault.mutable_obj_info()->set_endpoint("s3.us-east-1.amazonaws.com");
        vault.mutable_obj_info()->set_region("us-east-1");
        vault.mutable_obj_info()->set_bucket("test_bucket");
        vault.mutable_obj_info()->set_prefix("test_prefix");
        vault.mutable_obj_info()->set_role_arn("arn:aws:iam::123456789012:role/test-role");
        vault.mutable_obj_info()->set_external_id("external_id");
        vault.mutable_obj_info()->set_provider(
                ObjectStoreInfoPB::Provider::ObjectStoreInfoPB_Provider_S3);
        vault.mutable_obj_info()->set_cred_provider_type(CredProviderTypePB::INSTANCE_PROFILE);

        vault.set_name("ak_sk_s3_vault_with_role");
        req.mutable_vault()->CopyFrom(vault);

        brpc::Controller cntl;
        AlterObjStoreInfoResponse res;
        meta_service->alter_storage_vault(
                reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

        {
            InstanceInfoPB instance;
            get_test_instance(instance);
            std::unique_ptr<Transaction> txn;
            ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
            std::string val;
            ASSERT_EQ(txn->get(storage_vault_key({instance.instance_id(), "4"}), &val),
                      TxnErrorCode::TXN_OK);
            StorageVaultPB get_obj;
            get_obj.ParseFromString(val);
            ASSERT_EQ(get_obj.obj_info().ak().empty(), true) << get_obj.obj_info().ak();
            ASSERT_EQ(get_obj.obj_info().sk().empty(), true) << get_obj.obj_info().sk();

            ASSERT_EQ(get_obj.obj_info().role_arn(), "arn:aws:iam::123456789012:role/test-role")
                    << get_obj.obj_info().role_arn();
            ASSERT_EQ(get_obj.obj_info().external_id(), "external_id")
                    << get_obj.obj_info().external_id();

            ASSERT_EQ(get_obj.obj_info().endpoint(), "s3.us-east-1.amazonaws.com")
                    << get_obj.obj_info().endpoint();
            ASSERT_EQ(get_obj.obj_info().region(), "us-east-1") << get_obj.obj_info().region();
            ASSERT_EQ(get_obj.obj_info().bucket(), "test_bucket") << get_obj.obj_info().bucket();
            ASSERT_EQ(get_obj.obj_info().prefix(), "test_prefix") << get_obj.obj_info().prefix();
            ASSERT_EQ(get_obj.obj_info().provider(),
                      ObjectStoreInfoPB::Provider::ObjectStoreInfoPB_Provider_S3)
                    << get_obj.obj_info().provider();
            ASSERT_EQ(get_obj.name(), "ak_sk_s3_vault_with_role") << get_obj.name();
            ASSERT_EQ(get_obj.id(), "4") << get_obj.id();
            ASSERT_EQ(get_obj.obj_info().cred_provider_type(), CredProviderTypePB::INSTANCE_PROFILE)
                    << get_obj.obj_info().cred_provider_type();
        }
    }

    LOG(INFO) << "instance:" << instance.ShortDebugString();
    SyncPoint::get_instance()->disable_processing();
    SyncPoint::get_instance()->clear_all_call_backs();
}

TEST(MetaServiceTest, AddObjInfoWithRole) {
    auto meta_service = get_meta_service();

    auto sp = SyncPoint::get_instance();
    sp->enable_processing();

    sp->set_call_back("encrypt_ak_sk:get_encryption_key", [](auto&& args) {
        auto* ret = try_any_cast<int*>(args[0]);
        *ret = 0;
        auto* key = try_any_cast<std::string*>(args[1]);
        *key = "selectdbselectdbselectdbselectdb";
        auto* key_id = try_any_cast<int64_t*>(args[2]);
        *key_id = 1;
    });

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string key;
    std::string val;
    InstanceKeyInfo key_info {"test_instance"};
    instance_key(key_info, &key);

    InstanceInfoPB instance;
    val = instance.SerializeAsString();
    txn->put(key, val);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    auto get_test_instance = [&](InstanceInfoPB& i) {
        std::string key;
        std::string val;
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        InstanceKeyInfo key_info {"test_instance"};
        instance_key(key_info, &key);
        ASSERT_EQ(txn->get(key, &val), TxnErrorCode::TXN_OK);
        i.ParseFromString(val);
    };

    {
        AlterObjStoreInfoRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_op(AlterObjStoreInfoRequest::ADD_OBJ_INFO);
        auto sp = SyncPoint::get_instance();
        sp->set_call_back("create_object_info_with_encrypt", [](auto&& args) {
            auto* ret = try_any_cast<int*>(args[0]);
            *ret = 0;
        });
        sp->enable_processing();

        ObjectStoreInfoPB obj_info;
        obj_info.set_endpoint("s3.us-east-1.amazonaws.com");
        obj_info.set_region("us-east-1");
        obj_info.set_bucket("test_bucket");
        obj_info.set_prefix("test_prefix");
        obj_info.set_role_arn("arn:aws:iam::123456789012:role/test-role");
        obj_info.set_external_id("external_id");
        obj_info.set_provider(ObjectStoreInfoPB::Provider::ObjectStoreInfoPB_Provider_S3);
        obj_info.set_cred_provider_type(CredProviderTypePB::INSTANCE_PROFILE);

        req.mutable_obj()->MergeFrom(obj_info);

        brpc::Controller cntl;
        AlterObjStoreInfoResponse res;
        meta_service->alter_obj_store_info(
                reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();

        InstanceInfoPB instance;
        get_test_instance(instance);
        const auto& obj = instance.obj_info().at(0);
        ASSERT_EQ(obj.id(), "1");
        ASSERT_EQ(obj.ak().empty(), true) << obj.ak();
        ASSERT_EQ(obj.sk().empty(), true) << obj.sk();

        ASSERT_EQ(obj.role_arn(), "arn:aws:iam::123456789012:role/test-role") << obj.role_arn();
        ASSERT_EQ(obj.external_id(), "external_id") << obj.external_id();

        ASSERT_EQ(obj.endpoint(), "s3.us-east-1.amazonaws.com") << obj.endpoint();
        ASSERT_EQ(obj.region(), "us-east-1") << obj.region();
        ASSERT_EQ(obj.bucket(), "test_bucket") << obj.bucket();
        ASSERT_EQ(obj.prefix(), "test_prefix") << obj.prefix();
        ASSERT_EQ(obj.provider(), ObjectStoreInfoPB::Provider::ObjectStoreInfoPB_Provider_S3)
                << obj.provider();
        ASSERT_EQ(obj.cred_provider_type(), CredProviderTypePB::INSTANCE_PROFILE)
                << obj.cred_provider_type();

        sp->clear_all_call_backs();
        sp->clear_trace();
        sp->disable_processing();
    }

    SyncPoint::get_instance()->disable_processing();
    SyncPoint::get_instance()->clear_all_call_backs();
}

TEST(MetaServiceTest, CheckJobExisted) {
    auto meta_service = get_meta_service();

    std::string instance_id = "check_job_existed_instance_id";
    auto sp = SyncPoint::get_instance();
    DORIS_CLOUD_DEFER {
        SyncPoint::get_instance()->clear_all_call_backs();
    };
    sp->set_call_back("get_instance_id", [&](auto&& args) {
        auto* ret = try_any_cast_ret<std::string>(args);
        ret->first = instance_id;
        ret->second = true;
    });
    sp->enable_processing();

    // OK
    {
        constexpr auto table_id = 952701, index_id = 952702, partition_id = 952703,
                       tablet_id = 952704;
        int64_t txn_id = 952705;
        std::string label = "update_rowset_meta_test_label1";
        CreateRowsetResponse res;

        ASSERT_NO_FATAL_FAILURE(
                create_tablet(meta_service.get(), table_id, index_id, partition_id, tablet_id));

        auto rowset = create_rowset(txn_id, tablet_id, partition_id);

        {
            StartTabletJobResponse res;
            start_compaction_job(meta_service.get(), tablet_id, "compaction1", "ip:port", 0, 0,
                                 TabletCompactionJobPB::BASE, res);
        }

        brpc::Controller cntl;
        auto arena = res.GetArena();
        auto req = google::protobuf::Arena::CreateMessage<CreateRowsetRequest>(arena);
        req->set_tablet_job_id("compaction1");
        req->mutable_rowset_meta()->CopyFrom(rowset);
        meta_service->prepare_rowset(&cntl, req, &res, nullptr);
        if (!arena) delete req;

        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();
        res.Clear();
    }

    // job does not exist,
    {
        constexpr auto table_id = 952801, index_id = 952802, partition_id = 952803,
                       tablet_id = 952804;
        int64_t txn_id = 952805;
        std::string label = "update_rowset_meta_test_label1";
        CreateRowsetResponse res;

        ASSERT_NO_FATAL_FAILURE(
                create_tablet(meta_service.get(), table_id, index_id, partition_id, tablet_id));

        auto rowset = create_rowset(txn_id, tablet_id, partition_id);

        brpc::Controller cntl;
        auto arena = res.GetArena();
        auto req = google::protobuf::Arena::CreateMessage<CreateRowsetRequest>(arena);
        req->set_tablet_job_id("compaction1");
        req->mutable_rowset_meta()->CopyFrom(rowset);
        meta_service->prepare_rowset(&cntl, req, &res, nullptr);
        if (!arena) delete req;

        ASSERT_EQ(res.status().code(), MetaServiceCode::STALE_PREPARE_ROWSET) << res.status().msg();
        res.Clear();
    }

    // compaction job exists, job id not match
    {
        constexpr auto table_id = 952901, index_id = 952902, partition_id = 952903,
                       tablet_id = 952904;
        int64_t txn_id = 952905;
        std::string label = "update_rowset_meta_test_label1";
        CreateRowsetResponse res;

        ASSERT_NO_FATAL_FAILURE(
                create_tablet(meta_service.get(), table_id, index_id, partition_id, tablet_id));

        auto rowset = create_rowset(txn_id, tablet_id, partition_id);

        {
            StartTabletJobResponse res;
            start_compaction_job(meta_service.get(), tablet_id, "compaction1", "ip:port", 0, 0,
                                 TabletCompactionJobPB::BASE, res);
        }

        brpc::Controller cntl;
        auto arena = res.GetArena();
        auto req = google::protobuf::Arena::CreateMessage<CreateRowsetRequest>(arena);
        req->set_tablet_job_id("compaction2");
        req->mutable_rowset_meta()->CopyFrom(rowset);
        meta_service->prepare_rowset(&cntl, req, &res, nullptr);
        if (!arena) delete req;

        ASSERT_EQ(res.status().code(), MetaServiceCode::STALE_PREPARE_ROWSET) << res.status().msg();
        res.Clear();
    }

    // do not set job id
    {
        constexpr auto table_id = 953501, index_id = 953502, partition_id = 953503,
                       tablet_id = 953504;
        int64_t txn_id = 953505;
        std::string label = "update_rowset_meta_test_label1";
        CreateRowsetResponse res;

        ASSERT_NO_FATAL_FAILURE(
                create_tablet(meta_service.get(), table_id, index_id, partition_id, tablet_id));

        auto rowset = create_rowset(txn_id, tablet_id, partition_id);

        {
            StartTabletJobResponse res;
            start_compaction_job(meta_service.get(), tablet_id, "compaction1", "ip:port", 0, 0,
                                 TabletCompactionJobPB::BASE, res);
        }

        brpc::Controller cntl;
        auto arena = res.GetArena();
        auto req = google::protobuf::Arena::CreateMessage<CreateRowsetRequest>(arena);
        req->mutable_rowset_meta()->CopyFrom(rowset);
        meta_service->prepare_rowset(&cntl, req, &res, nullptr);
        if (!arena) delete req;

        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();
        res.Clear();
    }

    // job id is empty string
    {
        constexpr auto table_id = 953601, index_id = 953602, partition_id = 953603,
                       tablet_id = 953604;
        int64_t txn_id = 953605;
        std::string label = "update_rowset_meta_test_label1";
        CreateRowsetResponse res;

        ASSERT_NO_FATAL_FAILURE(
                create_tablet(meta_service.get(), table_id, index_id, partition_id, tablet_id));

        auto rowset = create_rowset(txn_id, tablet_id, partition_id);

        {
            StartTabletJobResponse res;
            start_compaction_job(meta_service.get(), tablet_id, "compaction1", "ip:port", 0, 0,
                                 TabletCompactionJobPB::BASE, res);
        }

        brpc::Controller cntl;
        auto arena = res.GetArena();
        auto req = google::protobuf::Arena::CreateMessage<CreateRowsetRequest>(arena);
        req->set_tablet_job_id("");
        req->mutable_rowset_meta()->CopyFrom(rowset);
        meta_service->prepare_rowset(&cntl, req, &res, nullptr);
        if (!arena) delete req;

        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();
        res.Clear();
    }

    // commit rowset OK
    {
        constexpr auto table_id = 953001, index_id = 953002, partition_id = 953003,
                       tablet_id = 953004;
        int64_t txn_id = 953005;
        std::string label = "update_rowset_meta_test_label1";
        CreateRowsetResponse res;

        ASSERT_NO_FATAL_FAILURE(
                create_tablet(meta_service.get(), table_id, index_id, partition_id, tablet_id));

        auto rowset = create_rowset(txn_id, tablet_id, partition_id);

        {
            StartTabletJobResponse res;
            start_compaction_job(meta_service.get(), tablet_id, "compaction1", "ip:port", 0, 0,
                                 TabletCompactionJobPB::BASE, res);
        }

        brpc::Controller cntl;
        auto arena = res.GetArena();
        auto req = google::protobuf::Arena::CreateMessage<CreateRowsetRequest>(arena);
        req->set_tablet_job_id("compaction1");
        req->mutable_rowset_meta()->CopyFrom(rowset);
        meta_service->commit_rowset(&cntl, req, &res, nullptr);
        if (!arena) delete req;

        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();
        res.Clear();
    }

    // commit rowset, job does not exist,
    {
        constexpr auto table_id = 953101, index_id = 953102, partition_id = 953103,
                       tablet_id = 953104;
        int64_t txn_id = 952805;
        std::string label = "update_rowset_meta_test_label1";
        CreateRowsetResponse res;

        ASSERT_NO_FATAL_FAILURE(
                create_tablet(meta_service.get(), table_id, index_id, partition_id, tablet_id));

        auto rowset = create_rowset(txn_id, tablet_id, partition_id);

        brpc::Controller cntl;
        auto arena = res.GetArena();
        auto req = google::protobuf::Arena::CreateMessage<CreateRowsetRequest>(arena);
        req->set_tablet_job_id("compaction1");
        req->mutable_rowset_meta()->CopyFrom(rowset);
        meta_service->commit_rowset(&cntl, req, &res, nullptr);
        if (!arena) delete req;

        ASSERT_EQ(res.status().code(), MetaServiceCode::STALE_PREPARE_ROWSET) << res.status().msg();
        res.Clear();
    }

    // commit rowset, compaction job exists, job id not match
    {
        constexpr auto table_id = 953201, index_id = 953202, partition_id = 953203,
                       tablet_id = 953204;
        int64_t txn_id = 952905;
        std::string label = "update_rowset_meta_test_label1";
        CreateRowsetResponse res;

        ASSERT_NO_FATAL_FAILURE(
                create_tablet(meta_service.get(), table_id, index_id, partition_id, tablet_id));

        auto rowset = create_rowset(txn_id, tablet_id, partition_id);

        {
            StartTabletJobResponse res;
            start_compaction_job(meta_service.get(), tablet_id, "compaction1", "ip:port", 0, 0,
                                 TabletCompactionJobPB::BASE, res);
        }

        brpc::Controller cntl;
        auto arena = res.GetArena();
        auto req = google::protobuf::Arena::CreateMessage<CreateRowsetRequest>(arena);
        req->set_tablet_job_id("compaction2");
        req->mutable_rowset_meta()->CopyFrom(rowset);
        meta_service->commit_rowset(&cntl, req, &res, nullptr);
        if (!arena) delete req;

        ASSERT_EQ(res.status().code(), MetaServiceCode::STALE_PREPARE_ROWSET) << res.status().msg();
        res.Clear();
    }

    // do not set job id when commit rowset
    {
        constexpr auto table_id = 953301, index_id = 953302, partition_id = 953303,
                       tablet_id = 953304;
        int64_t txn_id = 953305;
        std::string label = "update_rowset_meta_test_label1";
        CreateRowsetResponse res;

        ASSERT_NO_FATAL_FAILURE(
                create_tablet(meta_service.get(), table_id, index_id, partition_id, tablet_id));

        auto rowset = create_rowset(txn_id, tablet_id, partition_id);

        {
            StartTabletJobResponse res;
            start_compaction_job(meta_service.get(), tablet_id, "compaction1", "ip:port", 0, 0,
                                 TabletCompactionJobPB::BASE, res);
        }

        brpc::Controller cntl;
        auto arena = res.GetArena();
        auto req = google::protobuf::Arena::CreateMessage<CreateRowsetRequest>(arena);
        req->mutable_rowset_meta()->CopyFrom(rowset);
        meta_service->commit_rowset(&cntl, req, &res, nullptr);
        if (!arena) delete req;

        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();
        res.Clear();
    }

    // job id is empty string when commit rowset
    {
        constexpr auto table_id = 953401, index_id = 953402, partition_id = 953403,
                       tablet_id = 953404;
        int64_t txn_id = 953405;
        std::string label = "update_rowset_meta_test_label1";
        CreateRowsetResponse res;

        ASSERT_NO_FATAL_FAILURE(
                create_tablet(meta_service.get(), table_id, index_id, partition_id, tablet_id));

        auto rowset = create_rowset(txn_id, tablet_id, partition_id);

        {
            StartTabletJobResponse res;
            start_compaction_job(meta_service.get(), tablet_id, "compaction1", "ip:port", 0, 0,
                                 TabletCompactionJobPB::BASE, res);
        }

        brpc::Controller cntl;
        auto arena = res.GetArena();
        auto req = google::protobuf::Arena::CreateMessage<CreateRowsetRequest>(arena);
        req->set_tablet_job_id("");
        req->mutable_rowset_meta()->CopyFrom(rowset);
        meta_service->commit_rowset(&cntl, req, &res, nullptr);
        if (!arena) delete req;

        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();
        res.Clear();
    }
}

TEST(MetaServiceTest, StalePrepareRowset) {
    auto meta_service = get_meta_service();

    int64_t table_id = 1;
    int64_t partition_id = 1;
    int64_t tablet_id = 1;
    int64_t db_id = 100201;
    std::string label = "test_prepare_rowset";
    create_tablet(meta_service.get(), table_id, 1, partition_id, tablet_id);

    int64_t txn_id = 0;
    ASSERT_NO_FATAL_FAILURE(begin_txn(meta_service.get(), db_id, label, table_id, txn_id));
    CreateRowsetResponse res;
    auto rowset = create_rowset(txn_id, tablet_id, partition_id);
    rowset.mutable_load_id()->set_hi(123);
    rowset.mutable_load_id()->set_lo(456);
    prepare_rowset(meta_service.get(), rowset, res);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << label;
    res.Clear();
    ASSERT_NO_FATAL_FAILURE(commit_rowset(meta_service.get(), rowset, res));
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << label;

    prepare_rowset(meta_service.get(), rowset, res);
    ASSERT_TRUE(res.status().msg().find("rowset already exists") != std::string::npos)
            << res.status().msg();
    ASSERT_EQ(res.status().code(), MetaServiceCode::ALREADY_EXISTED) << res.status().code();

    commit_txn(meta_service.get(), db_id, txn_id, label);
    prepare_rowset(meta_service.get(), rowset, res);
    ASSERT_TRUE(res.status().msg().find("txn is not in") != std::string::npos)
            << res.status().msg();
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT) << res.status().code();
}

TEST(MetaServiceTest, StaleCommitRowset) {
    auto meta_service = get_meta_service();

    int64_t table_id = 1;
    int64_t partition_id = 1;
    int64_t tablet_id = 1;
    int64_t db_id = 100201;
    std::string label = "test_prepare_rowset";
    create_tablet(meta_service.get(), table_id, 1, partition_id, tablet_id);

    int64_t txn_id = 0;
    ASSERT_NO_FATAL_FAILURE(begin_txn(meta_service.get(), db_id, label, table_id, txn_id));
    CreateRowsetResponse res;
    auto rowset = create_rowset(txn_id, tablet_id, partition_id);
    rowset.mutable_load_id()->set_hi(123);
    rowset.mutable_load_id()->set_lo(456);
    prepare_rowset(meta_service.get(), rowset, res);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << label;
    res.Clear();
    ASSERT_NO_FATAL_FAILURE(commit_rowset(meta_service.get(), rowset, res));
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << label;

    ASSERT_NO_FATAL_FAILURE(commit_rowset(meta_service.get(), rowset, res));
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << label;

    commit_txn(meta_service.get(), db_id, txn_id, label);
    ASSERT_NO_FATAL_FAILURE(commit_rowset(meta_service.get(), rowset, res));
    ASSERT_TRUE(res.status().msg().find("txn is not in") != std::string::npos)
            << res.status().msg();
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT) << res.status().code();
}

TEST(MetaServiceTest, AlterObjInfoTest) {
    auto meta_service = get_meta_service();

    auto sp = SyncPoint::get_instance();
    sp->enable_processing();

    sp->set_call_back("encrypt_ak_sk:get_encryption_key", [](auto&& args) {
        auto* ret = try_any_cast<int*>(args[0]);
        *ret = 0;
        auto* key = try_any_cast<std::string*>(args[1]);
        *key = "selectdbselectdbselectdbselectdb";
        auto* key_id = try_any_cast<int64_t*>(args[2]);
        *key_id = 1;
    });

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string key;
    std::string val;
    InstanceKeyInfo key_info {"test_instance"};
    instance_key(key_info, &key);

    ObjectStoreInfoPB obj_info;
    obj_info.set_id("1");
    obj_info.set_ak("access_key_132131");
    obj_info.set_sk("secret_key_434124");
    obj_info.set_provider(ObjectStoreInfoPB::Provider::ObjectStoreInfoPB_Provider_S3);
    InstanceInfoPB instance;
    instance.add_obj_info()->CopyFrom(obj_info);
    val = instance.SerializeAsString();
    txn->put(key, val);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    auto get_test_instance = [&](InstanceInfoPB& i) {
        std::string key;
        std::string val;
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        InstanceKeyInfo key_info {"test_instance"};
        instance_key(key_info, &key);
        ASSERT_EQ(txn->get(key, &val), TxnErrorCode::TXN_OK);
        i.ParseFromString(val);
    };

    std::string cipher_sk = "JUkuTDctR+ckJtnPkLScWaQZRcOtWBhsLLpnCRxQLxr734qB8cs6gNLH6grE1FxO";
    std::string plain_sk = "Hx60p12123af234541nsVsffdfsdfghsdfhsdf34t";

    // update failed
    {
        AlterObjStoreInfoRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_op(AlterObjStoreInfoRequest::ALTER_OBJ_INFO);
        req.mutable_obj()->set_id("2");
        req.mutable_obj()->set_ak("new_ak");
        req.mutable_obj()->set_sk(plain_sk);

        brpc::Controller cntl;
        AlterObjStoreInfoResponse res;
        meta_service->alter_obj_store_info(
                reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
        InstanceInfoPB instance;
        get_test_instance(instance);
        ASSERT_EQ(instance.obj_info(0).id(), "1");
        ASSERT_EQ(instance.obj_info(0).ak(), "access_key_132131");
        ASSERT_EQ(instance.obj_info(0).sk(), "secret_key_434124");
    }

    // update ak/sk successful
    {
        AlterObjStoreInfoRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_op(AlterObjStoreInfoRequest::ALTER_OBJ_INFO);
        req.mutable_obj()->set_id("1");
        req.mutable_obj()->set_ak("new_access_key_132131");
        req.mutable_obj()->set_sk(plain_sk);

        brpc::Controller cntl;
        AlterObjStoreInfoResponse res;
        meta_service->alter_obj_store_info(
                reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        InstanceInfoPB instance;
        get_test_instance(instance);
        LOG(INFO) << "instance:" << instance.ShortDebugString();
        ASSERT_EQ(instance.obj_info(0).id(), "1");
        ASSERT_EQ(instance.obj_info(0).ak(), "new_access_key_132131");
        ASSERT_EQ(instance.obj_info(0).sk(), cipher_sk);
    }

    // update from ak/sk to role_arn
    {
        AlterObjStoreInfoRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_op(AlterObjStoreInfoRequest::ALTER_OBJ_INFO);
        req.mutable_obj()->set_id("1");
        req.mutable_obj()->set_role_arn("arn:aws:iam::1453123012:role/test-role");
        req.mutable_obj()->set_external_id("external_id_13123");
        req.mutable_obj()->set_cred_provider_type(CredProviderTypePB::INSTANCE_PROFILE);
        req.mutable_obj()->set_provider(ObjectStoreInfoPB::Provider::ObjectStoreInfoPB_Provider_S3);

        brpc::Controller cntl;
        AlterObjStoreInfoResponse res;
        meta_service->alter_obj_store_info(
                reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        InstanceInfoPB instance;
        get_test_instance(instance);
        LOG(INFO) << "instance:" << instance.ShortDebugString();
        ASSERT_EQ(instance.obj_info(0).id(), "1");
        ASSERT_EQ(instance.obj_info(0).role_arn(), "arn:aws:iam::1453123012:role/test-role");
        ASSERT_EQ(instance.obj_info(0).external_id(), "external_id_13123");
        ASSERT_EQ(instance.obj_info(0).provider(),
                  ObjectStoreInfoPB::Provider::ObjectStoreInfoPB_Provider_S3);
        ASSERT_EQ(instance.obj_info(0).cred_provider_type(), CredProviderTypePB::INSTANCE_PROFILE);
        ASSERT_TRUE(instance.obj_info(0).ak().empty());
        ASSERT_TRUE(instance.obj_info(0).sk().empty());
        ASSERT_FALSE(instance.obj_info(0).has_encryption_info());
    }

    // update from role_arn to ak/sk
    {
        AlterObjStoreInfoRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_op(AlterObjStoreInfoRequest::ALTER_OBJ_INFO);
        req.mutable_obj()->set_id("1");
        req.mutable_obj()->set_ak("new_access_key_132131");
        req.mutable_obj()->set_sk(plain_sk);

        brpc::Controller cntl;
        AlterObjStoreInfoResponse res;
        meta_service->alter_obj_store_info(
                reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        InstanceInfoPB instance;
        get_test_instance(instance);
        LOG(INFO) << "instance:" << instance.ShortDebugString();
        ASSERT_EQ(instance.obj_info(0).id(), "1");
        ASSERT_EQ(instance.obj_info(0).ak(), "new_access_key_132131");
        ASSERT_EQ(instance.obj_info(0).sk(), cipher_sk);
        ASSERT_EQ(instance.obj_info(0).provider(),
                  ObjectStoreInfoPB::Provider::ObjectStoreInfoPB_Provider_S3);
        ASSERT_FALSE(instance.obj_info(0).has_cred_provider_type());
        ASSERT_FALSE(instance.obj_info(0).has_role_arn());
        ASSERT_FALSE(instance.obj_info(0).has_external_id());
    }

    SyncPoint::get_instance()->disable_processing();
    SyncPoint::get_instance()->clear_all_call_backs();
}

TEST(MetaServiceTest, AlterS3StorageVaultWithRoleArnTest) {
    auto meta_service = get_meta_service();

    auto sp = SyncPoint::get_instance();
    sp->enable_processing();
    sp->set_call_back("encrypt_ak_sk:get_encryption_key", [](auto&& args) {
        auto* ret = try_any_cast<int*>(args[0]);
        *ret = 0;
        auto* key = try_any_cast<std::string*>(args[1]);
        *key = "selectdbselectdbselectdbselectdb";
        auto* key_id = try_any_cast<int64_t*>(args[2]);
        *key_id = 1;
    });
    std::pair<std::string, std::string> pair;
    sp->set_call_back("extract_object_storage_info:get_aksk_pair", [&](auto&& args) {
        auto* ret = try_any_cast<std::pair<std::string, std::string>*>(args[0]);
        pair = *ret;
    });

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string key;
    std::string val;
    InstanceKeyInfo key_info {"test_instance"};
    instance_key(key_info, &key);

    ObjectStoreInfoPB obj_info;
    obj_info.set_id("1");
    obj_info.set_ak("123456ab");
    obj_info.set_sk("@ak$");
    obj_info.set_provider(ObjectStoreInfoPB::Provider::ObjectStoreInfoPB_Provider_S3);
    StorageVaultPB vault;
    constexpr char vault_name[] = "test_alter_s3_vault_111";
    vault.mutable_obj_info()->MergeFrom(obj_info);
    vault.set_name(vault_name);
    vault.set_id("2");
    InstanceInfoPB instance;
    instance.add_storage_vault_names(vault.name());
    instance.add_resource_ids(vault.id());
    instance.set_instance_id("GetObjStoreInfoTestInstance");
    val = instance.SerializeAsString();
    txn->put(key, val);
    txn->put(storage_vault_key({instance.instance_id(), "2"}), vault.SerializeAsString());
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    txn = nullptr;

    auto get_test_instance = [&](InstanceInfoPB& i) {
        std::string key;
        std::string val;
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        InstanceKeyInfo key_info {"test_instance"};
        instance_key(key_info, &key);
        ASSERT_EQ(txn->get(key, &val), TxnErrorCode::TXN_OK);
        i.ParseFromString(val);
    };

    // update from ak/sk to role_arn
    {
        AlterObjStoreInfoRequest req;
        constexpr char new_vault_name[] = "new_test_alter_s3_vault_111";
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_op(AlterObjStoreInfoRequest::ALTER_S3_VAULT);
        StorageVaultPB vault;
        vault.set_alter_name(new_vault_name);
        ObjectStoreInfoPB obj;
        obj.set_role_arn("arn:aws:iam::12311321:role/test-alter-role");
        obj.set_external_id("external_id_123123");
        vault.mutable_obj_info()->MergeFrom(obj);
        vault.set_name(vault_name);
        req.mutable_vault()->CopyFrom(vault);

        brpc::Controller cntl;
        AlterObjStoreInfoResponse res;
        meta_service->alter_storage_vault(
                reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();

        InstanceInfoPB instance;
        get_test_instance(instance);
        LOG(INFO) << "instance:" << instance.ShortDebugString();

        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string val;
        ASSERT_EQ(txn->get(storage_vault_key({instance.instance_id(), "2"}), &val),
                  TxnErrorCode::TXN_OK);
        StorageVaultPB get_obj;
        get_obj.ParseFromString(val);
        ASSERT_EQ(get_obj.id(), "2");
        ASSERT_EQ(get_obj.obj_info().role_arn(), "arn:aws:iam::12311321:role/test-alter-role");
        ASSERT_EQ(get_obj.obj_info().external_id(), "external_id_123123");
        ASSERT_EQ(get_obj.obj_info().provider(),
                  ObjectStoreInfoPB::Provider::ObjectStoreInfoPB_Provider_S3);
        ASSERT_EQ(get_obj.obj_info().cred_provider_type(), CredProviderTypePB::INSTANCE_PROFILE);
        ASSERT_TRUE(get_obj.obj_info().ak().empty());
        ASSERT_TRUE(get_obj.obj_info().sk().empty());
        ASSERT_FALSE(get_obj.obj_info().has_encryption_info());
        ASSERT_EQ(get_obj.name(), new_vault_name) << get_obj.obj_info().ShortDebugString();
    }

    std::string cipher_sk = "JUkuTDctR+ckJtnPkLScWaQZRcOtWBhsLLpnCRxQLxr734qB8cs6gNLH6grE1FxO";
    std::string plain_sk = "Hx60p12123af234541nsVsffdfsdfghsdfhsdf34t";

    // update from role_arn to ak_sk
    {
        AlterObjStoreInfoRequest req;
        constexpr char new_vault_name[] = "new_test_alter_s3_vault_111";
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_op(AlterObjStoreInfoRequest::ALTER_S3_VAULT);
        StorageVaultPB vault;
        ObjectStoreInfoPB obj;
        obj.set_ak("123456ab");
        obj.set_sk(plain_sk);
        vault.mutable_obj_info()->MergeFrom(obj);
        vault.set_name(new_vault_name);
        req.mutable_vault()->CopyFrom(vault);

        brpc::Controller cntl;
        AlterObjStoreInfoResponse res;
        meta_service->alter_storage_vault(
                reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();

        InstanceInfoPB instance;
        get_test_instance(instance);
        LOG(INFO) << "instance:" << instance.ShortDebugString();

        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string val;
        ASSERT_EQ(txn->get(storage_vault_key({instance.instance_id(), "2"}), &val),
                  TxnErrorCode::TXN_OK);
        StorageVaultPB get_obj;
        get_obj.ParseFromString(val);
        ASSERT_EQ(get_obj.id(), "2");
        ASSERT_EQ(get_obj.obj_info().provider(),
                  ObjectStoreInfoPB::Provider::ObjectStoreInfoPB_Provider_S3);
        ASSERT_EQ(get_obj.obj_info().ak(), "123456ab");
        ASSERT_EQ(get_obj.obj_info().sk(), cipher_sk);
        ASSERT_TRUE(get_obj.obj_info().role_arn().empty());
        ASSERT_TRUE(get_obj.obj_info().external_id().empty());
        ASSERT_TRUE(get_obj.obj_info().has_encryption_info());
        ASSERT_FALSE(get_obj.obj_info().has_cred_provider_type());
        ASSERT_EQ(get_obj.name(), new_vault_name) << get_obj.obj_info().ShortDebugString();
    }

    SyncPoint::get_instance()->disable_processing();
    SyncPoint::get_instance()->clear_all_call_backs();
}

void scan_restore_job_rowset(
        Transaction* txn, const std::string& instance_id, int64_t tablet_id, MetaServiceCode& code,
        std::string& msg,
        std::vector<std::pair<std::string, doris::RowsetMetaCloudPB>>* restore_job_rs_metas);

TEST(MetaServiceTest, RestoreJobTest) {
    auto meta_service = get_meta_service();
    ASSERT_NE(meta_service, nullptr);

    std::string instance_id = "test_prepare_restore_job_instance_id";
    auto sp = SyncPoint::get_instance();
    std::unique_ptr<int, std::function<void(int*)>> defer(
            (int*)0x01, [](int*) { SyncPoint::get_instance()->clear_all_call_backs(); });
    sp->set_call_back("get_instance_id", [&](auto&& args) {
        auto* ret = try_any_cast_ret<std::string>(args);
        ret->first = instance_id;
        ret->second = true;
    });
    sp->enable_processing();

    auto reset_meta_service = [&meta_service] { meta_service = get_meta_service(); };
    constexpr int64_t table_id = 10001;
    constexpr int64_t index_id = 10002;
    constexpr int64_t partition_id = 10003;
    constexpr int64_t tablet_id = 10004;
    constexpr int64_t version = 1;
    constexpr int64_t txn_id = 0;

    TabletIndexPB tablet_idx;
    tablet_idx.set_table_id(table_id);
    tablet_idx.set_index_id(index_id);
    tablet_idx.set_partition_id(partition_id);
    tablet_idx.set_tablet_id(tablet_id);
    std::string tablet_idx_val;
    tablet_idx.SerializeToString(&tablet_idx_val);

    std::unique_ptr<Transaction> txn;
    brpc::Controller cntl;
    RestoreJobRequest req;
    RestoreJobResponse res;

    // ------------Test prepare restore job------------
    // invalid args prepare restore job
    {
        reset_meta_service();
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        txn->put(meta_tablet_idx_key({instance_id, tablet_id}), tablet_idx_val);
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        // empty tablet id
        meta_service->prepare_restore_job(&cntl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_EQ(res.status().msg(), "empty tablet_id");

        // restore with empty tablet meta
        req.set_tablet_id(tablet_id);
        res.Clear();
        meta_service->prepare_restore_job(&cntl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_EQ(res.status().msg(), "no tablet meta");
        req.Clear();
        res.Clear();
    }
    // normal prepare restore job
    {
        reset_meta_service();
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        txn->put(meta_tablet_idx_key({instance_id, tablet_id}), tablet_idx_val);
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
        req.set_tablet_id(tablet_id);
        req.set_expiration(time(nullptr) + 3600);

        // set tablet meta
        auto* tablet_meta = req.mutable_tablet_meta();
        tablet_meta->set_table_id(table_id);
        tablet_meta->set_index_id(index_id);
        tablet_meta->set_partition_id(partition_id);
        tablet_meta->set_tablet_id(tablet_id);
        tablet_meta->set_schema_version(1);
        auto* rs_meta = tablet_meta->add_rs_metas();
        *rs_meta = create_rowset(txn_id, tablet_id, partition_id, version);

        meta_service->prepare_restore_job(&cntl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();
        std::string restore_job_key = job_restore_tablet_key({instance_id, tablet_id});
        std::string val;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        ASSERT_EQ(txn->get(restore_job_key, &val), TxnErrorCode::TXN_OK);

        RestoreJobCloudPB restore_job_pb;
        ASSERT_TRUE(restore_job_pb.ParseFromString(val));
        ASSERT_EQ(restore_job_pb.tablet_id(), tablet_id);
        ASSERT_EQ(restore_job_pb.state(), RestoreJobCloudPB::PREPARED);
        ASSERT_EQ(restore_job_pb.tablet_meta().schema_version(), 1);

        std::string restore_job_rs_key = job_restore_rowset_key({instance_id, tablet_id, version});
        ASSERT_EQ(txn->get(restore_job_rs_key, &val), TxnErrorCode::TXN_OK);
        RowsetMetaCloudPB rs_meta_pb;
        ASSERT_TRUE(rs_meta_pb.ParseFromString(val));
        ASSERT_EQ(rs_meta_pb.tablet_id(), tablet_id);
        ASSERT_EQ(rs_meta_pb.rowset_id_v2(), rs_meta->rowset_id_v2());
        req.Clear();
        res.Clear();
    }
    // duplicate prepare restore job
    {
        reset_meta_service();
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        txn->put(meta_tablet_idx_key({instance_id, tablet_id}), tablet_idx_val);
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
        req.set_tablet_id(tablet_id);

        // set tablet meta
        auto* tablet_meta = req.mutable_tablet_meta();
        tablet_meta->set_table_id(table_id);
        tablet_meta->set_index_id(index_id);
        tablet_meta->set_partition_id(partition_id);
        tablet_meta->set_tablet_id(tablet_id);
        tablet_meta->set_schema_version(1);
        auto* rs_meta = tablet_meta->add_rs_metas();
        *rs_meta = create_rowset(txn_id, tablet_id, partition_id, version);

        // first request
        meta_service->prepare_restore_job(&cntl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();
        std::string restore_job_key = job_restore_tablet_key({instance_id, tablet_id});
        std::string val;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        ASSERT_EQ(txn->get(restore_job_key, &val), TxnErrorCode::TXN_OK);

        RestoreJobCloudPB restore_job_pb;
        ASSERT_TRUE(restore_job_pb.ParseFromString(val));
        ASSERT_EQ(restore_job_pb.tablet_id(), tablet_id);
        ASSERT_EQ(restore_job_pb.state(), RestoreJobCloudPB::PREPARED);
        ASSERT_EQ(restore_job_pb.version(), 0);

        // second request
        meta_service->prepare_restore_job(&cntl, &req, &res, nullptr);
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        ASSERT_EQ(txn->get(restore_job_key, &val), TxnErrorCode::TXN_OK);

        restore_job_pb.Clear();
        ASSERT_TRUE(restore_job_pb.ParseFromString(val));
        ASSERT_EQ(restore_job_pb.tablet_id(), tablet_id);
        ASSERT_EQ(restore_job_pb.state(), RestoreJobCloudPB::PREPARED);
        ASSERT_EQ(restore_job_pb.version(), 1);
        req.Clear();
        res.Clear();
    }
    // ------------Test commit restore job------------
    // invalid args commit restore job
    {
        reset_meta_service();
        // empty tablet_id
        meta_service->commit_restore_job(&cntl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_EQ(res.status().msg(), "empty tablet_id");
        req.Clear();
        res.Clear();
    }
    // commit restore job not exits
    {
        reset_meta_service();
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        txn->put(meta_tablet_idx_key({instance_id, tablet_id}), tablet_idx_val);
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        req.set_tablet_id(tablet_id);
        meta_service->commit_restore_job(&cntl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_EQ(res.status().msg(), "restore job not exists or has been recycled");
        req.Clear();
        res.Clear();
    }
    // normal commit restore job
    {
        reset_meta_service();
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        txn->put(meta_tablet_idx_key({instance_id, tablet_id}), tablet_idx_val);
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        // prepare restore job
        RestoreJobRequest make_req;
        RestoreJobResponse make_res;
        make_req.set_tablet_id(tablet_id);
        make_req.set_expiration(time(nullptr) + 3600);
        auto* tablet_meta = make_req.mutable_tablet_meta();
        tablet_meta->set_table_id(table_id);
        tablet_meta->set_index_id(index_id);
        tablet_meta->set_partition_id(partition_id);
        tablet_meta->set_tablet_id(tablet_id);
        tablet_meta->set_schema_version(1);
        auto* rs_meta = tablet_meta->add_rs_metas();
        *rs_meta = create_rowset(txn_id, tablet_id, partition_id, version);
        auto* delete_bitmap = tablet_meta->mutable_delete_bitmap();
        delete_bitmap->add_rowset_ids(rs_meta->rowset_id_v2());
        delete_bitmap->add_versions(1);
        delete_bitmap->add_segment_ids(1);
        delete_bitmap->add_segment_delete_bitmaps("test_bitmap");

        meta_service->prepare_restore_job(&cntl, &make_req, &make_res, nullptr);
        ASSERT_EQ(make_res.status().code(), MetaServiceCode::OK);
        std::string restore_job_key = job_restore_tablet_key({instance_id, tablet_id});
        std::string val;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        ASSERT_EQ(txn->get(restore_job_key, &val), TxnErrorCode::TXN_OK);
        std::string restore_job_rs_key = job_restore_rowset_key({instance_id, tablet_id, version});
        ASSERT_EQ(txn->get(restore_job_rs_key, &val), TxnErrorCode::TXN_OK);

        // commit_restore_job
        req.set_tablet_id(tablet_id);
        meta_service->commit_restore_job(&cntl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();
        std::string tablet_key =
                meta_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        ASSERT_EQ(txn->get(tablet_key, &val), TxnErrorCode::TXN_OK);
        TabletMetaCloudPB saved_tablet_meta;
        ASSERT_TRUE(saved_tablet_meta.ParseFromString(val));
        ASSERT_EQ(saved_tablet_meta.tablet_id(), tablet_id);
        ASSERT_EQ(saved_tablet_meta.schema_version(), 1);
        std::string rs_key = meta_rowset_key({instance_id, tablet_id, version});
        ASSERT_EQ(txn->get(rs_key, &val), TxnErrorCode::TXN_OK);
        RowsetMetaCloudPB saved_rs_meta;
        ASSERT_TRUE(saved_rs_meta.ParseFromString(val));
        ASSERT_EQ(saved_rs_meta.tablet_id(), tablet_id);
        ASSERT_EQ(saved_rs_meta.rowset_id_v2(), rs_meta->rowset_id_v2());
        std::string bitmap_key =
                meta_delete_bitmap_key({instance_id, tablet_id, rs_meta->rowset_id_v2(), 1, 1});
        ASSERT_EQ(txn->get(bitmap_key, &val), TxnErrorCode::TXN_OK);
        ASSERT_EQ(val, "test_bitmap");

        // ths restore job key should not be removed, restore job rowset key should be found
        ASSERT_EQ(txn->get(restore_job_key, &val), TxnErrorCode::TXN_OK);
        std::vector<std::pair<std::string, doris::RowsetMetaCloudPB>> restore_job_rs_metas;
        MetaServiceCode code;
        std::string msg;
        scan_restore_job_rowset(txn.get(), instance_id, tablet_id, code, msg,
                                &restore_job_rs_metas);
        ASSERT_EQ(code, MetaServiceCode::OK) << msg;
        ASSERT_EQ(restore_job_rs_metas.size(), 1);
        req.Clear();
        res.Clear();
    }
    // large commit restore job request with 10000 rowset meta
    {
        reset_meta_service();
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        txn->put(meta_tablet_idx_key({instance_id, tablet_id}), tablet_idx_val);
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        // prepare restore job
        RestoreJobRequest make_req;
        RestoreJobResponse make_res;
        make_req.set_tablet_id(tablet_id);
        make_req.set_expiration(time(nullptr) + 3600);

        auto* tablet_meta = make_req.mutable_tablet_meta();
        tablet_meta->set_table_id(table_id);
        tablet_meta->set_index_id(index_id);
        tablet_meta->set_partition_id(partition_id);
        tablet_meta->set_tablet_id(tablet_id);
        tablet_meta->set_schema_version(1);

        // add 10000 rowset meta
        constexpr int LARGE_ROWSET_COUNT = 10000;
        for (int64_t ver = 1; ver <= LARGE_ROWSET_COUNT; ver++) {
            auto* rs_meta = tablet_meta->add_rs_metas();
            *rs_meta = create_rowset(txn_id, tablet_id, partition_id, ver);
        }

        meta_service->prepare_restore_job(&cntl, &make_req, &make_res, nullptr);
        ASSERT_EQ(make_res.status().code(), MetaServiceCode::OK) << make_res.status().msg();
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        for (int64_t ver = 1; ver <= LARGE_ROWSET_COUNT; ver++) {
            std::string restore_job_rs_key = job_restore_rowset_key({instance_id, tablet_id, ver});
            std::string val;
            ASSERT_EQ(txn->get(restore_job_rs_key, &val), TxnErrorCode::TXN_OK);
            RowsetMetaCloudPB rs_meta_pb;
            ASSERT_TRUE(rs_meta_pb.ParseFromString(val));
            ASSERT_EQ(rs_meta_pb.tablet_id(), tablet_id);
            ASSERT_EQ(rs_meta_pb.start_version(), ver);
            ASSERT_EQ(rs_meta_pb.end_version(), ver);
        }

        // commit_restore_job
        req.set_tablet_id(tablet_id);
        meta_service->commit_restore_job(&cntl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        for (int64_t ver = 1; ver <= LARGE_ROWSET_COUNT; ver++) {
            std::string rs_key = meta_rowset_key({instance_id, tablet_id, ver});
            std::string val;
            ASSERT_EQ(txn->get(rs_key, &val), TxnErrorCode::TXN_OK);
            RowsetMetaCloudPB saved_rs_meta;
            ASSERT_TRUE(saved_rs_meta.ParseFromString(val));
            ASSERT_EQ(saved_rs_meta.tablet_id(), tablet_id);
            ASSERT_EQ(saved_rs_meta.start_version(), ver);
            ASSERT_EQ(saved_rs_meta.end_version(), ver);
        }

        // ths restore job key should not be removed, restore job rowset key should be found
        std::string restore_job_key = job_restore_tablet_key({instance_id, tablet_id});
        std::string val;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        ASSERT_EQ(txn->get(restore_job_key, &val), TxnErrorCode::TXN_OK);
        std::vector<std::pair<std::string, doris::RowsetMetaCloudPB>> restore_job_rs_metas;
        MetaServiceCode code;
        std::string msg;
        scan_restore_job_rowset(txn.get(), instance_id, tablet_id, code, msg,
                                &restore_job_rs_metas);
        ASSERT_EQ(code, MetaServiceCode::OK) << msg;
        ASSERT_EQ(restore_job_rs_metas.size(), 10000);
        req.Clear();
        res.Clear();
    }
    // ------------Test finish restore job------------
    // invalid args finish restore job
    {
        reset_meta_service();
        // empty tablet_id
        meta_service->finish_restore_job(&cntl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_EQ(res.status().msg(), "empty tablet_id");
        req.Clear();
        res.Clear();
    }
    // finish restore job not exists
    {
        reset_meta_service();
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        txn->put(meta_tablet_idx_key({instance_id, tablet_id}), tablet_idx_val);
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        req.set_tablet_id(tablet_id);
        meta_service->finish_restore_job(&cntl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_EQ(res.status().msg(), "restore job not exists or has been recycled");
        req.Clear();
        res.Clear();
    }
    // finish restore job COMMITTED -> COMPLETED
    {
        reset_meta_service();
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        txn->put(meta_tablet_idx_key({instance_id, tablet_id}), tablet_idx_val);
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        // prepare restore job
        RestoreJobRequest make_req;
        RestoreJobResponse make_res;
        make_req.set_tablet_id(tablet_id);
        make_req.set_expiration(time(nullptr) + 3600);

        auto* tablet_meta = make_req.mutable_tablet_meta();
        tablet_meta->set_table_id(table_id);
        tablet_meta->set_index_id(index_id);
        tablet_meta->set_partition_id(partition_id);
        tablet_meta->set_tablet_id(tablet_id);
        tablet_meta->set_schema_version(1);
        auto* rs_meta = tablet_meta->add_rs_metas();
        *rs_meta = create_rowset(txn_id, tablet_id, partition_id, version);
        auto* delete_bitmap = tablet_meta->mutable_delete_bitmap();
        delete_bitmap->add_rowset_ids(rs_meta->rowset_id_v2());
        delete_bitmap->add_versions(1);
        delete_bitmap->add_segment_ids(1);
        delete_bitmap->add_segment_delete_bitmaps("test_bitmap");

        meta_service->prepare_restore_job(&cntl, &make_req, &make_res, nullptr);
        ASSERT_EQ(make_res.status().code(), MetaServiceCode::OK);
        std::string restore_job_key = job_restore_tablet_key({instance_id, tablet_id});
        std::string val;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        ASSERT_EQ(txn->get(restore_job_key, &val), TxnErrorCode::TXN_OK);
        std::string restore_job_rs_key = job_restore_rowset_key({instance_id, tablet_id, version});
        ASSERT_EQ(txn->get(restore_job_rs_key, &val), TxnErrorCode::TXN_OK);

        req.set_tablet_id(tablet_id);
        meta_service->commit_restore_job(&cntl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        ASSERT_EQ(txn->get(restore_job_key, &val), TxnErrorCode::TXN_OK);
        ASSERT_EQ(txn->get(restore_job_rs_key, &val), TxnErrorCode::TXN_OK);

        // finish_restore_job to COMPLETED
        req.set_tablet_id(tablet_id);
        req.set_is_completed(true);
        meta_service->finish_restore_job(&cntl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();

        // this restore job key should be in COMPLETED state
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        ASSERT_EQ(txn->get(restore_job_key, &val), TxnErrorCode::TXN_OK);
        RestoreJobCloudPB restore_job_pb;
        ASSERT_TRUE(restore_job_pb.ParseFromString(val));
        ASSERT_EQ(restore_job_pb.state(), RestoreJobCloudPB::COMPLETED);
        ASSERT_EQ(restore_job_pb.need_recycle_data(), false);
        std::vector<std::pair<std::string, doris::RowsetMetaCloudPB>> restore_job_rs_metas;
        MetaServiceCode code;
        std::string msg;
        scan_restore_job_rowset(txn.get(), instance_id, tablet_id, code, msg,
                                &restore_job_rs_metas);
        ASSERT_EQ(code, MetaServiceCode::OK) << msg;
        ASSERT_EQ(restore_job_rs_metas.size(), 1);
        req.Clear();
        res.Clear();
    }
    // finish restore job state PREPARED -> DROPPED
    {
        reset_meta_service();
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        txn->put(meta_tablet_idx_key({instance_id, tablet_id}), tablet_idx_val);
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        // prepare restore job
        RestoreJobRequest make_req;
        RestoreJobResponse make_res;
        make_req.set_tablet_id(tablet_id);
        make_req.set_expiration(time(nullptr) + 3600);

        auto* tablet_meta = make_req.mutable_tablet_meta();
        tablet_meta->set_table_id(table_id);
        tablet_meta->set_index_id(index_id);
        tablet_meta->set_partition_id(partition_id);
        tablet_meta->set_tablet_id(tablet_id);
        tablet_meta->set_schema_version(1);
        auto* rs_meta = tablet_meta->add_rs_metas();
        *rs_meta = create_rowset(txn_id, tablet_id, partition_id, version);

        meta_service->prepare_restore_job(&cntl, &make_req, &make_res, nullptr);
        ASSERT_EQ(make_res.status().code(), MetaServiceCode::OK);
        std::string restore_job_key = job_restore_tablet_key({instance_id, tablet_id});
        std::string val;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        ASSERT_EQ(txn->get(restore_job_key, &val), TxnErrorCode::TXN_OK);
        std::string restore_job_rs_key = job_restore_rowset_key({instance_id, tablet_id, version});
        ASSERT_EQ(txn->get(restore_job_rs_key, &val), TxnErrorCode::TXN_OK);

        // finish_restore_job to DROPPED
        req.set_tablet_id(tablet_id);
        req.set_is_completed(false);
        meta_service->finish_restore_job(&cntl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();

        // this restore job key should be in DROPPED state
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        ASSERT_EQ(txn->get(restore_job_key, &val), TxnErrorCode::TXN_OK);
        RestoreJobCloudPB restore_job_pb;
        ASSERT_TRUE(restore_job_pb.ParseFromString(val));
        ASSERT_EQ(restore_job_pb.state(), RestoreJobCloudPB::DROPPED);
        ASSERT_EQ(restore_job_pb.need_recycle_data(), true);
        std::vector<std::pair<std::string, doris::RowsetMetaCloudPB>> restore_job_rs_metas;
        MetaServiceCode code;
        std::string msg;
        scan_restore_job_rowset(txn.get(), instance_id, tablet_id, code, msg,
                                &restore_job_rs_metas);
        ASSERT_EQ(code, MetaServiceCode::OK) << msg;
        ASSERT_EQ(restore_job_rs_metas.size(), 1);
        req.Clear();
        res.Clear();
    }
    // finish restore job invalid state PREPARED -> COMPLETED
    {
        reset_meta_service();
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        txn->put(meta_tablet_idx_key({instance_id, tablet_id}), tablet_idx_val);
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        // prepare restore job
        RestoreJobRequest make_req;
        RestoreJobResponse make_res;
        make_req.set_tablet_id(tablet_id);
        make_req.set_expiration(time(nullptr) + 3600);

        // set tablet meta
        auto* tablet_meta = make_req.mutable_tablet_meta();
        tablet_meta->set_table_id(table_id);
        tablet_meta->set_index_id(index_id);
        tablet_meta->set_partition_id(partition_id);
        tablet_meta->set_tablet_id(tablet_id);
        tablet_meta->set_schema_version(1);
        auto* rs_meta = tablet_meta->add_rs_metas();
        *rs_meta = create_rowset(txn_id, tablet_id, partition_id, version);

        meta_service->prepare_restore_job(&cntl, &make_req, &make_res, nullptr);
        ASSERT_EQ(make_res.status().code(), MetaServiceCode::OK);

        // finish_restore_job to COMPLETED
        req.set_tablet_id(tablet_id);
        req.set_is_completed(true);
        meta_service->finish_restore_job(&cntl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_TRUE(res.status().msg().find("invalid state to complete") != std::string::npos);
    }
}

} // namespace doris::cloud
