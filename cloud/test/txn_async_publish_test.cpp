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
#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <string>

#include "common/config.h"
#include "common/logging.h"
#include "meta-service/meta_service.h"
#include "meta-service/meta_service_helper.h"
#include "meta-store/keys.h"
#include "meta-store/mem_txn_kv.h"
#include "meta-store/txn_kv_error.h"
#include "mock_resource_manager.h"
#include "rate-limiter/rate_limiter.h"
#include "resource-manager/resource_manager.h"

using namespace doris::cloud;

int main(int argc, char** argv) {
    const std::string conf_file = "doris_cloud.conf";
    if (!doris::cloud::config::init(conf_file.c_str(), true)) {
        std::cerr << "failed to init config file, conf=" << conf_file << std::endl;
        return -1;
    }
    if (!doris::cloud::init_glog("txn_async_publish_test")) {
        std::cerr << "failed to init glog" << std::endl;
        return -1;
    }
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace doris::cloud {

static std::shared_ptr<TxnKv> get_mem_txn_kv() {
    auto txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<MemTxnKv>());
    int ret = txn_kv->init();
    [&] { ASSERT_EQ(ret, 0); }();
    [&] { ASSERT_NE(txn_kv.get(), nullptr); }();
    return txn_kv;
}

static std::unique_ptr<MetaServiceProxy> get_meta_service(std::shared_ptr<TxnKv> txn_kv) {
    std::unique_ptr<Transaction> txn;
    EXPECT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->remove("\x00", "\xfe");
    EXPECT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    auto rs = std::make_shared<MockResourceManager>(txn_kv);
    auto rl = std::make_shared<RateLimiter>();
    auto snapshot = std::make_shared<SnapshotManager>(txn_kv);
    auto meta_service = std::make_unique<MetaServiceImpl>(txn_kv, rs, rl, snapshot);
    return std::make_unique<MetaServiceProxy>(std::move(meta_service));
}

static std::string next_rowset_id() {
    static int cnt = 0;
    return std::to_string(++cnt);
}

static void add_tablet(CreateTabletsRequest& req, int64_t table_id, int64_t index_id,
                       int64_t partition_id, int64_t tablet_id) {
    auto tablet = req.add_tablet_metas();
    tablet->set_table_id(table_id);
    tablet->set_index_id(index_id);
    tablet->set_partition_id(partition_id);
    tablet->set_tablet_id(tablet_id);
    auto schema = tablet->mutable_schema();
    schema->set_schema_version(0);
    auto first_rowset = tablet->add_rs_metas();
    first_rowset->set_rowset_id(0);
    first_rowset->set_rowset_id_v2(next_rowset_id());
    first_rowset->set_start_version(0);
    first_rowset->set_end_version(1);
    first_rowset->mutable_tablet_schema()->CopyFrom(*schema);
}

static void create_tablet(MetaServiceProxy* meta_service, int64_t db_id, int64_t table_id,
                          int64_t index_id, int64_t partition_id, int64_t tablet_id) {
    brpc::Controller cntl;
    CreateTabletsRequest req;
    CreateTabletsResponse res;
    req.set_db_id(db_id);
    add_tablet(req, table_id, index_id, partition_id, tablet_id);
    meta_service->create_tablets(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << tablet_id;
}

static doris::RowsetMetaCloudPB create_rowset(int64_t txn_id, int64_t tablet_id, int64_t index_id,
                                              int64_t partition_id) {
    doris::RowsetMetaCloudPB rowset;
    rowset.set_rowset_id(0);
    rowset.set_rowset_id_v2(next_rowset_id());
    rowset.set_tablet_id(tablet_id);
    rowset.set_partition_id(partition_id);
    rowset.set_index_id(index_id);
    rowset.set_txn_id(txn_id);
    rowset.set_num_segments(1);
    rowset.set_num_rows(100);
    rowset.set_data_disk_size(1024);
    rowset.set_index_disk_size(128);
    rowset.set_total_disk_size(1024 + 128); // total = data + index
    rowset.mutable_tablet_schema()->set_schema_version(0);
    rowset.set_txn_expiration(::time(nullptr));
    return rowset;
}

static void prepare_rowset(MetaServiceProxy* meta_service, const doris::RowsetMetaCloudPB& rowset) {
    brpc::Controller cntl;
    CreateRowsetRequest req;
    CreateRowsetResponse res;
    req.mutable_rowset_meta()->CopyFrom(rowset);
    meta_service->prepare_rowset(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
}

static void commit_rowset(MetaServiceProxy* meta_service, const doris::RowsetMetaCloudPB& rowset) {
    brpc::Controller cntl;
    CreateRowsetRequest req;
    CreateRowsetResponse res;
    req.mutable_rowset_meta()->CopyFrom(rowset);
    meta_service->commit_rowset(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
}

static int64_t begin_txn(MetaServiceProxy* meta_service, int64_t db_id, int64_t table_id,
                         const std::string& label) {
    brpc::Controller cntl;
    BeginTxnRequest req;
    req.set_cloud_unique_id("test_cloud_unique_id");
    TxnInfoPB txn_info_pb;
    txn_info_pb.set_db_id(db_id);
    txn_info_pb.set_label(label);
    txn_info_pb.add_table_ids(table_id);
    txn_info_pb.set_timeout_ms(600000);
    req.mutable_txn_info()->CopyFrom(txn_info_pb);
    BeginTxnResponse res;
    meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res,
                            nullptr);
    EXPECT_EQ(res.status().code(), MetaServiceCode::OK);
    return res.txn_id();
}

// Helper: build a CommitTxnRequest for async publish commit phase
static CommitTxnRequest build_async_publish_commit_req(
        int64_t db_id, int64_t txn_id,
        const std::vector<std::tuple<int64_t, int64_t, int64_t, int64_t>>& tablets) {
    // tablets: vector of (tablet_id, table_id, index_id, partition_id)
    CommitTxnRequest req;
    req.set_cloud_unique_id("test_cloud_unique_id");
    req.set_db_id(db_id);
    req.set_txn_id(txn_id);
    req.set_enable_mow_async_publish(true);
    for (auto& [tablet_id, table_id, index_id, partition_id] : tablets) {
        auto* ti = req.add_involved_tablets();
        ti->set_tablet_id(tablet_id);
        ti->set_table_id(table_id);
        ti->set_index_id(index_id);
        ti->set_partition_id(partition_id);
        ti->set_be_cloud_unique_id("be_1");
        ti->set_be_endpoint("127.0.0.1:8040");
    }
    return req;
}

// Helper: read partition commit version from KV
static int64_t get_partition_commit_version(std::shared_ptr<TxnKv>& txn_kv,
                                            const std::string& instance_id, int64_t db_id,
                                            int64_t table_id, int64_t partition_id) {
    std::unique_ptr<Transaction> txn;
    EXPECT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string key = partition_commit_version_key({instance_id, db_id, table_id, partition_id});
    std::string val;
    auto err = txn->get(key, &val);
    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) return -1;
    EXPECT_EQ(err, TxnErrorCode::TXN_OK);
    VersionPB version_pb;
    EXPECT_TRUE(version_pb.ParseFromString(val));
    return version_pb.version();
}

// Helper: read TxnInfoPB from KV
static TxnInfoPB get_txn_info(std::shared_ptr<TxnKv>& txn_kv, const std::string& instance_id,
                              int64_t db_id, int64_t txn_id) {
    std::unique_ptr<Transaction> txn;
    EXPECT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string key = txn_info_key({instance_id, db_id, txn_id});
    std::string val;
    EXPECT_EQ(txn->get(key, &val), TxnErrorCode::TXN_OK);
    TxnInfoPB txn_info;
    EXPECT_TRUE(txn_info.ParseFromString(val));
    return txn_info;
}

// Helper: check if txn_running_key exists
static bool txn_running_key_exists(std::shared_ptr<TxnKv>& txn_kv, const std::string& instance_id,
                                   int64_t db_id, int64_t txn_id) {
    std::unique_ptr<Transaction> txn;
    EXPECT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string key = txn_running_key({instance_id, db_id, txn_id});
    std::string val;
    return txn->get(key, &val) == TxnErrorCode::TXN_OK;
}

static const std::string mock_instance = "test_instance";

// ==================== Test Cases ====================

// Basic: single partition, single tablet async publish commit
TEST(TxnAsyncPublishTest, CommitBasic) {
    auto txn_kv = get_mem_txn_kv();
    auto meta_service = get_meta_service(txn_kv);

    int64_t db_id = 1001;
    int64_t table_id = 2001;
    int64_t index_id = 3001;
    int64_t partition_id = 4001;
    int64_t tablet_id = 5001;

    create_tablet(meta_service.get(), db_id, table_id, index_id, partition_id, tablet_id);

    int64_t txn_id = begin_txn(meta_service.get(), db_id, table_id, "label_basic");

    auto rowset = create_rowset(txn_id, tablet_id, index_id, partition_id);
    prepare_rowset(meta_service.get(), rowset);
    commit_rowset(meta_service.get(), rowset);

    // Async publish commit
    auto req = build_async_publish_commit_req(db_id, txn_id,
                                              {{tablet_id, table_id, index_id, partition_id}});
    CommitTxnResponse res;
    brpc::Controller cntl;
    meta_service->commit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                             &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();

    // Verify response: partition_ids and commit_versions
    ASSERT_EQ(res.partition_ids_size(), 1);
    ASSERT_EQ(res.partition_ids(0), partition_id);
    ASSERT_EQ(res.commit_versions_size(), 1);
    ASSERT_EQ(res.commit_versions(0), 2); // initial=1, +1=2

    // Verify TxnInfoPB in response
    ASSERT_TRUE(res.has_txn_info());
    ASSERT_EQ(res.txn_info().status(), TxnStatusPB::TXN_STATUS_COMMITTED);
    ASSERT_TRUE(res.txn_info().mow_async_publish());

    // Verify KV: partition commit version
    int64_t commit_ver =
            get_partition_commit_version(txn_kv, mock_instance, db_id, table_id, partition_id);
    ASSERT_EQ(commit_ver, 2);

    // Verify KV: TxnInfoPB persisted
    auto txn_info = get_txn_info(txn_kv, mock_instance, db_id, txn_id);
    ASSERT_EQ(txn_info.status(), TxnStatusPB::TXN_STATUS_COMMITTED);
    ASSERT_TRUE(txn_info.mow_async_publish());
    ASSERT_EQ(txn_info.committed_partition_ids_size(), 1);
    ASSERT_EQ(txn_info.committed_partition_ids(0), partition_id);
    ASSERT_EQ(txn_info.committed_versions_size(), 1);
    ASSERT_EQ(txn_info.committed_versions(0), 2);
    ASSERT_EQ(txn_info.involved_tablets_size(), 1);
    ASSERT_EQ(txn_info.involved_tablets(0).tablet_id(), tablet_id);

    // Verify KV: txn_running_key deleted
    ASSERT_FALSE(txn_running_key_exists(txn_kv, mock_instance, db_id, txn_id));
}

// Multi-partition test: 2 partitions, each with 1 tablet
TEST(TxnAsyncPublishTest, CommitMultiPartition) {
    auto txn_kv = get_mem_txn_kv();
    auto meta_service = get_meta_service(txn_kv);

    int64_t db_id = 1002;
    int64_t table_id = 2002;
    int64_t index_id = 3002;
    int64_t partition_id_1 = 4002;
    int64_t partition_id_2 = 4003;
    int64_t tablet_id_1 = 5002;
    int64_t tablet_id_2 = 5003;

    create_tablet(meta_service.get(), db_id, table_id, index_id, partition_id_1, tablet_id_1);
    create_tablet(meta_service.get(), db_id, table_id, index_id, partition_id_2, tablet_id_2);

    int64_t txn_id = begin_txn(meta_service.get(), db_id, table_id, "label_multi_part");

    auto rowset1 = create_rowset(txn_id, tablet_id_1, index_id, partition_id_1);
    auto rowset2 = create_rowset(txn_id, tablet_id_2, index_id, partition_id_2);
    prepare_rowset(meta_service.get(), rowset1);
    commit_rowset(meta_service.get(), rowset1);
    prepare_rowset(meta_service.get(), rowset2);
    commit_rowset(meta_service.get(), rowset2);

    auto req = build_async_publish_commit_req(db_id, txn_id,
                                              {{tablet_id_1, table_id, index_id, partition_id_1},
                                               {tablet_id_2, table_id, index_id, partition_id_2}});
    CommitTxnResponse res;
    brpc::Controller cntl;
    meta_service->commit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                             &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();

    // Both partitions should get commit version 2
    ASSERT_EQ(res.commit_versions_size(), 2);
    for (int i = 0; i < res.commit_versions_size(); ++i) {
        ASSERT_EQ(res.commit_versions(i), 2);
    }

    // Verify KV
    ASSERT_EQ(get_partition_commit_version(txn_kv, mock_instance, db_id, table_id, partition_id_1),
              2);
    ASSERT_EQ(get_partition_commit_version(txn_kv, mock_instance, db_id, table_id, partition_id_2),
              2);
}

// Idempotent: calling commit twice returns OK with same versions
TEST(TxnAsyncPublishTest, CommitIdempotent) {
    auto txn_kv = get_mem_txn_kv();
    auto meta_service = get_meta_service(txn_kv);

    int64_t db_id = 1003;
    int64_t table_id = 2003;
    int64_t index_id = 3003;
    int64_t partition_id = 4004;
    int64_t tablet_id = 5004;

    create_tablet(meta_service.get(), db_id, table_id, index_id, partition_id, tablet_id);
    int64_t txn_id = begin_txn(meta_service.get(), db_id, table_id, "label_idempotent");

    auto rowset = create_rowset(txn_id, tablet_id, index_id, partition_id);
    prepare_rowset(meta_service.get(), rowset);
    commit_rowset(meta_service.get(), rowset);

    auto req = build_async_publish_commit_req(db_id, txn_id,
                                              {{tablet_id, table_id, index_id, partition_id}});

    // First commit
    {
        CommitTxnResponse res;
        brpc::Controller cntl;
        meta_service->commit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                 &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();
        ASSERT_EQ(res.commit_versions(0), 2);
    }

    // Second commit (idempotent)
    {
        CommitTxnResponse res;
        brpc::Controller cntl;
        meta_service->commit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                 &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();
        // Should return same committed versions from TxnInfoPB
        ASSERT_EQ(res.commit_versions_size(), 1);
        ASSERT_EQ(res.commit_versions(0), 2);
    }

    // Partition commit version should still be 2, not 3
    ASSERT_EQ(get_partition_commit_version(txn_kv, mock_instance, db_id, table_id, partition_id),
              2);
}

// Already aborted: commit should return TXN_ALREADY_ABORTED
TEST(TxnAsyncPublishTest, CommitAlreadyAborted) {
    auto txn_kv = get_mem_txn_kv();
    auto meta_service = get_meta_service(txn_kv);

    int64_t db_id = 1004;
    int64_t table_id = 2004;
    int64_t index_id = 3004;
    int64_t partition_id = 4005;
    int64_t tablet_id = 5005;

    create_tablet(meta_service.get(), db_id, table_id, index_id, partition_id, tablet_id);
    int64_t txn_id = begin_txn(meta_service.get(), db_id, table_id, "label_aborted");

    // Abort the txn
    {
        brpc::Controller cntl;
        AbortTxnRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_txn_id(txn_id);
        req.set_reason("test abort");
        AbortTxnResponse res;
        meta_service->abort_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // Try async publish commit
    auto req = build_async_publish_commit_req(db_id, txn_id,
                                              {{tablet_id, table_id, index_id, partition_id}});
    CommitTxnResponse res;
    brpc::Controller cntl;
    meta_service->commit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                             &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::TXN_ALREADY_ABORTED);
}

// Sequential commits on same partition: version increments correctly
TEST(TxnAsyncPublishTest, SequentialCommitsSamePartition) {
    auto txn_kv = get_mem_txn_kv();
    auto meta_service = get_meta_service(txn_kv);

    int64_t db_id = 1005;
    int64_t table_id = 2005;
    int64_t index_id = 3005;
    int64_t partition_id = 4006;
    int64_t tablet_id = 5006;

    create_tablet(meta_service.get(), db_id, table_id, index_id, partition_id, tablet_id);

    // 3 sequential commits on the same partition
    for (int i = 0; i < 3; ++i) {
        int64_t txn_id =
                begin_txn(meta_service.get(), db_id, table_id, "label_seq_" + std::to_string(i));

        auto rowset = create_rowset(txn_id, tablet_id, index_id, partition_id);
        prepare_rowset(meta_service.get(), rowset);
        commit_rowset(meta_service.get(), rowset);

        auto req = build_async_publish_commit_req(db_id, txn_id,
                                                  {{tablet_id, table_id, index_id, partition_id}});
        CommitTxnResponse res;
        brpc::Controller cntl;
        meta_service->commit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                 &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();

        // commit versions: 2, 3, 4 (initial=1, each +1)
        int64_t expected_version = 2 + i;
        ASSERT_EQ(res.commit_versions(0), expected_version)
                << "iteration=" << i << " expected=" << expected_version;
    }

    // Final partition commit version should be 4
    ASSERT_EQ(get_partition_commit_version(txn_kv, mock_instance, db_id, table_id, partition_id),
              4);
}

// Verify commit version and visible version are independent
TEST(TxnAsyncPublishTest, CommitVersionIndependent) {
    auto txn_kv = get_mem_txn_kv();
    auto meta_service = get_meta_service(txn_kv);

    int64_t db_id = 1006;
    int64_t table_id = 2006;
    int64_t index_id = 3006;
    int64_t partition_id = 4007;
    int64_t tablet_id = 5007;

    create_tablet(meta_service.get(), db_id, table_id, index_id, partition_id, tablet_id);

    int64_t txn_id = begin_txn(meta_service.get(), db_id, table_id, "label_independent");

    auto rowset = create_rowset(txn_id, tablet_id, index_id, partition_id);
    prepare_rowset(meta_service.get(), rowset);
    commit_rowset(meta_service.get(), rowset);

    auto req = build_async_publish_commit_req(db_id, txn_id,
                                              {{tablet_id, table_id, index_id, partition_id}});
    CommitTxnResponse res;
    brpc::Controller cntl;
    meta_service->commit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                             &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();

    // Commit version should be 2
    ASSERT_EQ(get_partition_commit_version(txn_kv, mock_instance, db_id, table_id, partition_id),
              2);

    // Visible version key should NOT exist (no publish has happened yet)
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string vis_key = partition_version_key({mock_instance, db_id, table_id, partition_id});
        std::string vis_val;
        auto err = txn->get(vis_key, &vis_val);
        ASSERT_EQ(err, TxnErrorCode::TXN_KEY_NOT_FOUND);
    }
}

// Verify tmp rowset and formal rowset are NOT touched by async publish commit
TEST(TxnAsyncPublishTest, NoRowsetConversion) {
    auto txn_kv = get_mem_txn_kv();
    auto meta_service = get_meta_service(txn_kv);

    int64_t db_id = 1007;
    int64_t table_id = 2007;
    int64_t index_id = 3007;
    int64_t partition_id = 4008;
    int64_t tablet_id = 5008;

    create_tablet(meta_service.get(), db_id, table_id, index_id, partition_id, tablet_id);

    int64_t txn_id = begin_txn(meta_service.get(), db_id, table_id, "label_no_rowset_conv");

    auto rowset = create_rowset(txn_id, tablet_id, index_id, partition_id);
    prepare_rowset(meta_service.get(), rowset);
    commit_rowset(meta_service.get(), rowset);

    auto req = build_async_publish_commit_req(db_id, txn_id,
                                              {{tablet_id, table_id, index_id, partition_id}});
    CommitTxnResponse res;
    brpc::Controller cntl;
    meta_service->commit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                             &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();

    // tmp rowset should still exist (not deleted by commit phase)
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string tmp_key = meta_rowset_tmp_key({mock_instance, txn_id, tablet_id});
        std::string tmp_val;
        ASSERT_EQ(txn->get(tmp_key, &tmp_val), TxnErrorCode::TXN_OK)
                << "tmp rowset should still exist after async publish commit";
    }

    // formal rowset at version=2 should NOT exist (not created by commit phase)
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string rowset_key = meta_rowset_key({mock_instance, tablet_id, 2});
        std::string rowset_val;
        ASSERT_EQ(txn->get(rowset_key, &rowset_val), TxnErrorCode::TXN_KEY_NOT_FOUND)
                << "formal rowset should NOT exist after async publish commit";
    }
}

// Verify load_schema_param is persisted in TxnInfoPB
TEST(TxnAsyncPublishTest, LoadSchemaParamPersisted) {
    auto txn_kv = get_mem_txn_kv();
    auto meta_service = get_meta_service(txn_kv);

    int64_t db_id = 1008;
    int64_t table_id = 2008;
    int64_t index_id = 3008;
    int64_t partition_id = 4009;
    int64_t tablet_id = 5009;

    create_tablet(meta_service.get(), db_id, table_id, index_id, partition_id, tablet_id);
    int64_t txn_id = begin_txn(meta_service.get(), db_id, table_id, "label_schema_param");

    auto rowset = create_rowset(txn_id, tablet_id, index_id, partition_id);
    prepare_rowset(meta_service.get(), rowset);
    commit_rowset(meta_service.get(), rowset);

    auto req = build_async_publish_commit_req(db_id, txn_id,
                                              {{tablet_id, table_id, index_id, partition_id}});
    // Set mock load_schema_param bytes
    std::string mock_schema_bytes = "mock_thrift_serialized_schema_param_bytes";
    req.set_load_schema_param(mock_schema_bytes);

    CommitTxnResponse res;
    brpc::Controller cntl;
    meta_service->commit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                             &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();

    // Verify TxnInfoPB has load_schema_param
    auto txn_info = get_txn_info(txn_kv, mock_instance, db_id, txn_id);
    ASSERT_TRUE(txn_info.has_load_schema_param());
    ASSERT_EQ(txn_info.load_schema_param(), mock_schema_bytes);
}

// ==================== ConvertTmpRowset Tests ====================

// Helper: read formal rowset from KV
static std::optional<doris::RowsetMetaCloudPB> get_formal_rowset(std::shared_ptr<TxnKv>& txn_kv,
                                                                  const std::string& instance_id,
                                                                  int64_t tablet_id, int64_t version) {
    std::unique_ptr<Transaction> txn;
    EXPECT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string rowset_key = meta_rowset_key({instance_id, tablet_id, version});
    std::string rowset_val;
    auto err = txn->get(rowset_key, &rowset_val);
    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) return std::nullopt;
    EXPECT_EQ(err, TxnErrorCode::TXN_OK);
    doris::RowsetMetaCloudPB rowset_meta;
    EXPECT_TRUE(rowset_meta.ParseFromString(rowset_val));
    return rowset_meta;
}

// Helper: read tmp rowset from KV
static std::optional<doris::RowsetMetaCloudPB> get_tmp_rowset(std::shared_ptr<TxnKv>& txn_kv,
                                                              const std::string& instance_id,
                                                              int64_t txn_id, int64_t tablet_id) {
    std::unique_ptr<Transaction> txn;
    EXPECT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string tmp_key = meta_rowset_tmp_key({instance_id, txn_id, tablet_id});
    std::string tmp_val;
    auto err = txn->get(tmp_key, &tmp_val);
    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) return std::nullopt;
    EXPECT_EQ(err, TxnErrorCode::TXN_OK);
    doris::RowsetMetaCloudPB rowset_meta;
    EXPECT_TRUE(rowset_meta.ParseFromString(tmp_val));
    return rowset_meta;
}

// Basic: single tmp rowset conversion
TEST(TxnAsyncPublishTest, ConvertTmpRowsetBasic) {
    auto txn_kv = get_mem_txn_kv();
    auto meta_service = get_meta_service(txn_kv);

    int64_t db_id = 2001;
    int64_t table_id = 2002;
    int64_t index_id = 3002;
    int64_t partition_id = 4002;
    int64_t tablet_id = 5002;

    create_tablet(meta_service.get(), db_id, table_id, index_id, partition_id, tablet_id);

    int64_t txn_id = begin_txn(meta_service.get(), db_id, table_id, "label_convert_basic");

    auto rowset = create_rowset(txn_id, tablet_id, index_id, partition_id);
    prepare_rowset(meta_service.get(), rowset);
    commit_rowset(meta_service.get(), rowset);

    // Call convert_tmp_rowset
    brpc::Controller cntl;
    ConvertTmpRowsetRequest req;
    ConvertTmpRowsetResponse res;
    req.set_cloud_unique_id("test_cloud_unique_id");
    req.set_txn_id(txn_id);
    req.set_tablet_id(tablet_id);
    req.set_version(2); // commit version
    req.set_db_id(db_id);
    req.set_table_id(table_id);
    req.set_index_id(index_id);
    req.set_partition_id(partition_id);

    meta_service->convert_tmp_rowset(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();

    // Verify response contains rowset_meta with correct fields
    ASSERT_TRUE(res.has_rowset_meta());
    ASSERT_EQ(res.rowset_meta().tablet_id(), tablet_id);
    ASSERT_EQ(res.rowset_meta().start_version(), 2);
    ASSERT_EQ(res.rowset_meta().end_version(), 2);
    ASSERT_GT(res.rowset_meta().visible_ts_ms(), 0);
    ASSERT_EQ(res.rowset_meta().num_segments(), 1); // verify segments count from rowset

    // Verify formal rowset exists in KV
    auto formal_rowset = get_formal_rowset(txn_kv, mock_instance, tablet_id, 2);
    ASSERT_TRUE(formal_rowset.has_value());
    ASSERT_EQ(formal_rowset->rowset_id_v2(), rowset.rowset_id_v2());
    ASSERT_EQ(formal_rowset->start_version(), 2);
    ASSERT_EQ(formal_rowset->end_version(), 2);

    // Verify tmp rowset deleted
    auto tmp_rowset = get_tmp_rowset(txn_kv, mock_instance, txn_id, tablet_id);
    ASSERT_FALSE(tmp_rowset.has_value()) << "tmp rowset should be deleted after conversion";

    // Verify tablet stats updated by reading KV directly
    if (config::split_tablet_stats) {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

        std::string num_rows_key, num_rows_val;
        stats_tablet_num_rows_key({mock_instance, table_id, index_id, partition_id, tablet_id},
                                  &num_rows_key);
        ASSERT_EQ(txn->get(num_rows_key, &num_rows_val), TxnErrorCode::TXN_OK);
        EXPECT_EQ(*(int64_t*)num_rows_val.data(), 100);

        std::string data_size_key, data_size_val;
        stats_tablet_data_size_key({mock_instance, table_id, index_id, partition_id, tablet_id},
                                   &data_size_key);
        ASSERT_EQ(txn->get(data_size_key, &data_size_val), TxnErrorCode::TXN_OK);
        EXPECT_EQ(*(int64_t*)data_size_val.data(), 1152); // total_disk_size = data + index

        std::string num_segs_key, num_segs_val;
        stats_tablet_num_segs_key({mock_instance, table_id, index_id, partition_id, tablet_id},
                                   &num_segs_key);
        ASSERT_EQ(txn->get(num_segs_key, &num_segs_val), TxnErrorCode::TXN_OK);
        EXPECT_EQ(*(int64_t*)num_segs_val.data(), 1);

        std::string num_rowsets_key, num_rowsets_val;
        stats_tablet_num_rowsets_key({mock_instance, table_id, index_id, partition_id, tablet_id},
                                     &num_rowsets_key);
        ASSERT_EQ(txn->get(num_rowsets_key, &num_rowsets_val), TxnErrorCode::TXN_OK);
        EXPECT_GT(*(int64_t*)num_rowsets_val.data(), 0); // Just verify it's updated
    }
}

// Idempotent: second call should succeed
TEST(TxnAsyncPublishTest, ConvertTmpRowsetIdempotent) {
    auto txn_kv = get_mem_txn_kv();
    auto meta_service = get_meta_service(txn_kv);

    int64_t db_id = 2002;
    int64_t table_id = 2003;
    int64_t index_id = 3003;
    int64_t partition_id = 4003;
    int64_t tablet_id = 5003;

    create_tablet(meta_service.get(), db_id, table_id, index_id, partition_id, tablet_id);

    int64_t txn_id = begin_txn(meta_service.get(), db_id, table_id, "label_convert_idempotent");

    auto rowset = create_rowset(txn_id, tablet_id, index_id, partition_id);
    prepare_rowset(meta_service.get(), rowset);
    commit_rowset(meta_service.get(), rowset);

    // First call
    brpc::Controller cntl;
    ConvertTmpRowsetRequest req;
    ConvertTmpRowsetResponse res;
    req.set_cloud_unique_id("test_cloud_unique_id");
    req.set_txn_id(txn_id);
    req.set_tablet_id(tablet_id);
    req.set_version(2);
    req.set_db_id(db_id);
    req.set_table_id(table_id);
    req.set_index_id(index_id);
    req.set_partition_id(partition_id);

    meta_service->convert_tmp_rowset(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();

    // Second call (idempotent)
    brpc::Controller cntl2;
    ConvertTmpRowsetResponse res2;
    meta_service->convert_tmp_rowset(reinterpret_cast<::google::protobuf::RpcController*>(&cntl2),
                                     &req, &res2, nullptr);
    ASSERT_EQ(res2.status().code(), MetaServiceCode::OK) << res2.status().msg();
    ASSERT_TRUE(res2.has_rowset_meta());
    ASSERT_EQ(res2.rowset_meta().rowset_id_v2(), rowset.rowset_id_v2());

    // Verify only one formal rowset exists
    auto formal_rowset = get_formal_rowset(txn_kv, mock_instance, tablet_id, 2);
    ASSERT_TRUE(formal_rowset.has_value());
}

// Error: tmp rowset not found
TEST(TxnAsyncPublishTest, ConvertTmpRowsetNotFound) {
    auto txn_kv = get_mem_txn_kv();
    auto meta_service = get_meta_service(txn_kv);

    int64_t db_id = 2003;
    int64_t table_id = 2004;
    int64_t index_id = 3004;
    int64_t partition_id = 4004;
    int64_t tablet_id = 5004;

    create_tablet(meta_service.get(), db_id, table_id, index_id, partition_id, tablet_id);

    // Call convert_tmp_rowset without creating tmp rowset
    brpc::Controller cntl;
    ConvertTmpRowsetRequest req;
    ConvertTmpRowsetResponse res;
    req.set_cloud_unique_id("test_cloud_unique_id");
    req.set_txn_id(99999); // non-existent txn
    req.set_tablet_id(tablet_id);
    req.set_version(2);
    req.set_db_id(db_id);
    req.set_table_id(table_id);
    req.set_index_id(index_id);
    req.set_partition_id(partition_id);

    meta_service->convert_tmp_rowset(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
}

// Error: invalid parameters
TEST(TxnAsyncPublishTest, ConvertTmpRowsetInvalidParams) {
    auto txn_kv = get_mem_txn_kv();
    auto meta_service = get_meta_service(txn_kv);

    // Test with invalid txn_id
    {
        brpc::Controller cntl;
        ConvertTmpRowsetRequest req;
        ConvertTmpRowsetResponse res;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_txn_id(0); // invalid
        req.set_tablet_id(5005);
        req.set_version(2);

        meta_service->convert_tmp_rowset(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                         &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    }

    // Test with invalid tablet_id
    {
        brpc::Controller cntl;
        ConvertTmpRowsetRequest req;
        ConvertTmpRowsetResponse res;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_txn_id(12345);
        req.set_tablet_id(0); // invalid
        req.set_version(2);

        meta_service->convert_tmp_rowset(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                         &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    }

    // Test with invalid version
    {
        brpc::Controller cntl;
        ConvertTmpRowsetRequest req;
        ConvertTmpRowsetResponse res;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_txn_id(12345);
        req.set_tablet_id(5005);
        req.set_version(0); // invalid

        meta_service->convert_tmp_rowset(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                         &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    }
}

// Error: invalid instance_id (cloud_unique_id)
TEST(TxnAsyncPublishTest, ConvertTmpRowsetInvalidInstance) {
    auto txn_kv = get_mem_txn_kv();
    auto meta_service = get_meta_service(txn_kv);

    brpc::Controller cntl;
    ConvertTmpRowsetRequest req;
    ConvertTmpRowsetResponse res;
    req.set_cloud_unique_id("invalid_instance_id"); // won't resolve to instance_id
    req.set_txn_id(12345);
    req.set_tablet_id(5006);
    req.set_version(2);

    meta_service->convert_tmp_rowset(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
}

// Error: tmp rowset recycled (transaction aborted)
TEST(TxnAsyncPublishTest, ConvertTmpRowsetRecycled) {
    auto txn_kv = get_mem_txn_kv();
    auto meta_service = get_meta_service(txn_kv);

    int64_t db_id = 2004;
    int64_t table_id = 2005;
    int64_t index_id = 3005;
    int64_t partition_id = 4005;
    int64_t tablet_id = 5007;

    create_tablet(meta_service.get(), db_id, table_id, index_id, partition_id, tablet_id);

    int64_t txn_id = begin_txn(meta_service.get(), db_id, table_id, "label_convert_recycled");

    auto rowset = create_rowset(txn_id, tablet_id, index_id, partition_id);
    prepare_rowset(meta_service.get(), rowset);
    commit_rowset(meta_service.get(), rowset);

    // Manually mark tmp rowset as recycled
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string tmp_key = meta_rowset_tmp_key({mock_instance, txn_id, tablet_id});
        std::string tmp_val;
        ASSERT_EQ(txn->get(tmp_key, &tmp_val), TxnErrorCode::TXN_OK);
        doris::RowsetMetaCloudPB rs_meta;
        ASSERT_TRUE(rs_meta.ParseFromString(tmp_val));
        rs_meta.set_is_recycled(true);
        tmp_val = rs_meta.SerializeAsString();
        txn->put(tmp_key, tmp_val);
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Try to convert recycled tmp rowset
    brpc::Controller cntl;
    ConvertTmpRowsetRequest req;
    ConvertTmpRowsetResponse res;
    req.set_cloud_unique_id("test_cloud_unique_id");
    req.set_txn_id(txn_id);
    req.set_tablet_id(tablet_id);
    req.set_version(2);
    req.set_db_id(db_id);
    req.set_table_id(table_id);
    req.set_index_id(index_id);
    req.set_partition_id(partition_id);

    meta_service->convert_tmp_rowset(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::TXN_ALREADY_ABORTED);
}

// Verify version conflict: different rowset at same version
TEST(TxnAsyncPublishTest, ConvertTmpRowsetVersionConflict) {
    auto txn_kv = get_mem_txn_kv();
    auto meta_service = get_meta_service(txn_kv);

    int64_t db_id = 2005;
    int64_t table_id = 2006;
    int64_t index_id = 3006;
    int64_t partition_id = 4006;
    int64_t tablet_id = 5008;

    create_tablet(meta_service.get(), db_id, table_id, index_id, partition_id, tablet_id);

    int64_t txn_id = begin_txn(meta_service.get(), db_id, table_id, "label_convert_conflict");

    auto rowset = create_rowset(txn_id, tablet_id, index_id, partition_id);
    prepare_rowset(meta_service.get(), rowset);
    commit_rowset(meta_service.get(), rowset);

    // Manually create a formal rowset at version 2 with different rowset_id
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string rowset_key = meta_rowset_key({mock_instance, tablet_id, 2});
        doris::RowsetMetaCloudPB conflicting_rowset;
        conflicting_rowset.set_rowset_id(0);  // Required field
        conflicting_rowset.set_rowset_id_v2("conflicting_rowset_id");
        conflicting_rowset.set_tablet_id(tablet_id);
        conflicting_rowset.set_partition_id(partition_id);
        conflicting_rowset.set_index_id(index_id);
        conflicting_rowset.set_start_version(2);
        conflicting_rowset.set_end_version(2);
        conflicting_rowset.set_num_segments(1);
        conflicting_rowset.mutable_tablet_schema()->set_schema_version(0);
        std::string val = conflicting_rowset.SerializeAsString();
        txn->put(rowset_key, val);
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Try to convert, should fail with version conflict
    brpc::Controller cntl;
    ConvertTmpRowsetRequest req;
    ConvertTmpRowsetResponse res;
    req.set_cloud_unique_id("test_cloud_unique_id");
    req.set_txn_id(txn_id);
    req.set_tablet_id(tablet_id);
    req.set_version(2);
    req.set_db_id(db_id);
    req.set_table_id(table_id);
    req.set_index_id(index_id);
    req.set_partition_id(partition_id);

    meta_service->convert_tmp_rowset(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::ALREADY_EXISTED);
}

// Multi-tablet: convert multiple tablets independently
TEST(TxnAsyncPublishTest, ConvertTmpRowsetMultiTablet) {
    auto txn_kv = get_mem_txn_kv();
    auto meta_service = get_meta_service(txn_kv);

    int64_t db_id = 2006;
    int64_t table_id = 2007;
    int64_t index_id = 3007;
    int64_t partition_id = 4007;
    int64_t tablet_id_1 = 5009;
    int64_t tablet_id_2 = 5010;

    create_tablet(meta_service.get(), db_id, table_id, index_id, partition_id, tablet_id_1);
    create_tablet(meta_service.get(), db_id, table_id, index_id, partition_id, tablet_id_2);

    int64_t txn_id = begin_txn(meta_service.get(), db_id, table_id, "label_convert_multi");

    auto rowset1 = create_rowset(txn_id, tablet_id_1, index_id, partition_id);
    prepare_rowset(meta_service.get(), rowset1);
    commit_rowset(meta_service.get(), rowset1);

    auto rowset2 = create_rowset(txn_id, tablet_id_2, index_id, partition_id);
    prepare_rowset(meta_service.get(), rowset2);
    commit_rowset(meta_service.get(), rowset2);

    // Convert first tablet
    {
        brpc::Controller cntl;
        ConvertTmpRowsetRequest req;
        ConvertTmpRowsetResponse res;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_txn_id(txn_id);
        req.set_tablet_id(tablet_id_1);
        req.set_version(2);
        req.set_db_id(db_id);
        req.set_table_id(table_id);
        req.set_index_id(index_id);
        req.set_partition_id(partition_id);

        meta_service->convert_tmp_rowset(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                         &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // Convert second tablet
    {
        brpc::Controller cntl;
        ConvertTmpRowsetRequest req;
        ConvertTmpRowsetResponse res;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_txn_id(txn_id);
        req.set_tablet_id(tablet_id_2);
        req.set_version(2);
        req.set_db_id(db_id);
        req.set_table_id(table_id);
        req.set_index_id(index_id);
        req.set_partition_id(partition_id);

        meta_service->convert_tmp_rowset(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                         &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // Verify both formal rowsets exist
    auto formal1 = get_formal_rowset(txn_kv, mock_instance, tablet_id_1, 2);
    auto formal2 = get_formal_rowset(txn_kv, mock_instance, tablet_id_2, 2);
    ASSERT_TRUE(formal1.has_value());
    ASSERT_TRUE(formal2.has_value());
    ASSERT_EQ(formal1->rowset_id_v2(), rowset1.rowset_id_v2());
    ASSERT_EQ(formal2->rowset_id_v2(), rowset2.rowset_id_v2());

    // Verify both tmp rowsets deleted
    auto tmp1 = get_tmp_rowset(txn_kv, mock_instance, txn_id, tablet_id_1);
    auto tmp2 = get_tmp_rowset(txn_kv, mock_instance, txn_id, tablet_id_2);
    ASSERT_FALSE(tmp1.has_value());
    ASSERT_FALSE(tmp2.has_value());
}

} // namespace doris::cloud
