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
#include <bvar/window.h>
#include <fmt/core.h>
#include <gen_cpp/cloud.pb.h>
#include <gen_cpp/olap_file.pb.h>
#include <google/protobuf/repeated_field.h>
#include <gtest/gtest.h>

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <random>
#include <string>
#include <thread>

#include "common/config.h"
#include "common/logging.h"
#include "common/util.h"
#include "cpp/sync_point.h"
#include "meta-service/keys.h"
#include "meta-service/mem_txn_kv.h"
#include "meta-service/meta_service.h"
#include "meta-service/meta_service_helper.h"
#include "meta-service/txn_kv_error.h"
#include "mock_resource_manager.h"
#include "rate-limiter/rate_limiter.h"
#include "recycler/recycler.h"
#include "resource-manager/resource_manager.h"

using namespace doris::cloud;

namespace doris::cloud {
void repair_tablet_index(
        std::shared_ptr<TxnKv>& txn_kv, MetaServiceCode& code, std::string& msg,
        const std::string& instance_id, int64_t db_id, int64_t txn_id,
        const std::vector<std::pair<std::string, doris::RowsetMetaCloudPB>>& tmp_rowsets_meta);
};

static doris::cloud::RecyclerThreadPoolGroup thread_group;

int main(int argc, char** argv) {
    const std::string conf_file = "doris_cloud.conf";
    if (!doris::cloud::config::init(conf_file.c_str(), true)) {
        std::cerr << "failed to init config file, conf=" << conf_file << std::endl;
        return -1;
    }

    config::enable_cloud_txn_lazy_commit = true;
    config::txn_lazy_commit_rowsets_thresold = 2;
    config::txn_lazy_max_rowsets_per_batch = 2;
    config::txn_lazy_commit_num_threads = 2;
    config::max_tablet_index_num_per_batch = 2;

    config::enable_txn_store_retry = false;

    config::label_keep_max_second = 0;

    if (!doris::cloud::init_glog("txn_lazy_commit_test")) {
        std::cerr << "failed to init glog" << std::endl;
        return -1;
    }
    ::testing::InitGoogleTest(&argc, argv);

    auto s3_producer_pool = std::make_shared<SimpleThreadPool>(config::recycle_pool_parallelism);
    s3_producer_pool->start();
    auto recycle_tablet_pool = std::make_shared<SimpleThreadPool>(config::recycle_pool_parallelism);
    recycle_tablet_pool->start();
    auto group_recycle_function_pool =
            std::make_shared<SimpleThreadPool>(config::recycle_pool_parallelism);
    group_recycle_function_pool->start();
    thread_group =
            RecyclerThreadPoolGroup(std::move(s3_producer_pool), std::move(recycle_tablet_pool),
                                    std::move(group_recycle_function_pool));

    return RUN_ALL_TESTS();
}
namespace doris::cloud {

std::unique_ptr<MetaServiceProxy> get_meta_service(std::shared_ptr<TxnKv> txn_kv,
                                                   bool mock_resource_mgr) {
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
    first_rowset->set_rowset_id(0); // required
    first_rowset->set_rowset_id_v2(next_rowset_id());
    first_rowset->set_start_version(0);
    first_rowset->set_end_version(1);
    first_rowset->mutable_tablet_schema()->CopyFrom(*schema);
}

static void create_tablet_without_db_id(MetaServiceProxy* meta_service, int64_t table_id,
                                        int64_t index_id, int64_t partition_id, int64_t tablet_id) {
    brpc::Controller cntl;
    CreateTabletsRequest req;
    CreateTabletsResponse res;
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

static doris::RowsetMetaCloudPB create_rowset(int64_t txn_id, int64_t tablet_id, int partition_id,
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
    rowset.mutable_tablet_schema()->set_schema_version(0);
    rowset.set_txn_expiration(::time(nullptr)); // Required by DCHECK
    return rowset;
}

static void commit_rowset(MetaServiceProxy* meta_service, const doris::RowsetMetaCloudPB& rowset,
                          CreateRowsetResponse& res) {
    brpc::Controller cntl;
    auto arena = res.GetArena();
    auto req = google::protobuf::Arena::CreateMessage<CreateRowsetRequest>(arena);
    req->mutable_rowset_meta()->CopyFrom(rowset);
    meta_service->commit_rowset(&cntl, req, &res, nullptr);
    if (!arena) {
        delete req;
    }
}

static std::shared_ptr<TxnKv> get_mem_txn_kv() {
    int ret = 0;
    // MemKv
    auto txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<MemTxnKv>());
    if (txn_kv != nullptr) {
        ret = txn_kv->init();
        [&] { ASSERT_EQ(ret, 0); }();
    }
    [&] { ASSERT_NE(txn_kv.get(), nullptr); }();
    return txn_kv;
}

static void check_tablet_idx_db_id(std::unique_ptr<Transaction>& txn, int64_t db_id,
                                   int64_t tablet_id) {
    std::string mock_instance = "test_instance";
    std::string key = meta_tablet_idx_key({mock_instance, tablet_id});
    std::string val;
    ASSERT_EQ(txn->get(key, &val), TxnErrorCode::TXN_OK);
    TabletIndexPB tablet_idx_pb;
    tablet_idx_pb.ParseFromString(val);
    ASSERT_EQ(tablet_idx_pb.db_id(), db_id);
}

static void check_tablet_idx_without_db_id(std::unique_ptr<Transaction>& txn, int64_t tablet_id) {
    std::string mock_instance = "test_instance";
    std::string key = meta_tablet_idx_key({mock_instance, tablet_id});
    std::string val;
    ASSERT_EQ(txn->get(key, &val), TxnErrorCode::TXN_OK);
    TabletIndexPB tablet_idx_pb;
    tablet_idx_pb.ParseFromString(val);
    ASSERT_FALSE(tablet_idx_pb.has_db_id());
}

static void check_tmp_rowset_exist(std::unique_ptr<Transaction>& txn, int64_t tablet_id,
                                   int64_t txn_id) {
    std::string mock_instance = "test_instance";
    std::string key = meta_rowset_tmp_key({mock_instance, txn_id, tablet_id});
    std::string val;
    ASSERT_EQ(txn->get(key, &val), TxnErrorCode::TXN_OK);
}

static void check_tmp_rowset_not_exist(std::unique_ptr<Transaction>& txn, int64_t tablet_id,
                                       int64_t txn_id) {
    std::string mock_instance = "test_instance";
    std::string key = meta_rowset_tmp_key({mock_instance, txn_id, tablet_id});
    std::string val;
    ASSERT_EQ(txn->get(key, &val), TxnErrorCode::TXN_KEY_NOT_FOUND);
}

static void check_rowset_meta_exist(std::unique_ptr<Transaction>& txn, int64_t tablet_id,
                                    int64_t end_version) {
    std::string mock_instance = "test_instance";
    std::string rowset_key = meta_rowset_key({mock_instance, tablet_id, end_version});
    std::string rowset_val;
    ASSERT_EQ(txn->get(rowset_key, &rowset_val), TxnErrorCode::TXN_OK);
}

static void check_rowset_meta_not_exist(std::unique_ptr<Transaction>& txn, int64_t tablet_id,
                                        int64_t end_version) {
    std::string mock_instance = "test_instance";
    std::string rowset_key = meta_rowset_key({mock_instance, tablet_id, end_version});
    std::string rowset_val;
    ASSERT_EQ(txn->get(rowset_key, &rowset_val), TxnErrorCode::TXN_KEY_NOT_FOUND);
}

[[maybe_unused]] static void check_txn_visible(std::unique_ptr<Transaction>& txn, int64_t db_id,
                                               int64_t txn_id, std::string label) {
    std::string mock_instance = "test_instance";
    std::string info_key, info_val;
    TxnInfoKeyInfo txn_info_key_info {mock_instance, db_id, txn_id};
    txn_info_key(txn_info_key_info, &info_key);
    ASSERT_EQ(txn->get(info_key, &info_val), TxnErrorCode::TXN_OK);
    TxnInfoPB txn_info_pb;
    txn_info_pb.ParseFromString(info_val);
    ASSERT_EQ(txn_info_pb.status(), TxnStatusPB::TXN_STATUS_VISIBLE);

    std::string label_key, label_val;
    txn_label_key({mock_instance, db_id, label}, &label_key);
    ASSERT_EQ(txn->get(label_key, &label_val), TxnErrorCode::TXN_OK);

    std::string index_key, index_val;
    index_key = txn_index_key({mock_instance, txn_id});
    ASSERT_EQ(txn->get(index_key, &index_val), TxnErrorCode::TXN_OK);

    std::string running_key, running_value;
    TxnRunningKeyInfo running_key_info {mock_instance, db_id, txn_id};
    txn_running_key(running_key_info, &running_key);
    ASSERT_EQ(txn->get(running_key, &running_value), TxnErrorCode::TXN_KEY_NOT_FOUND);

    std::string rec_txn_key, rec_txn_val;
    RecycleTxnKeyInfo recycle_txn_key_info {mock_instance, db_id, txn_id};
    recycle_txn_key(recycle_txn_key_info, &rec_txn_key);
    ASSERT_EQ(txn->get(rec_txn_key, &rec_txn_val), TxnErrorCode::TXN_OK);
}

[[maybe_unused]] static void check_txn_committed(std::unique_ptr<Transaction>& txn, int64_t db_id,
                                                 int64_t txn_id, std::string label) {
    std::string mock_instance = "test_instance";
    std::string info_key, info_val;
    TxnInfoKeyInfo txn_info_key_info {mock_instance, db_id, txn_id};
    txn_info_key(txn_info_key_info, &info_key);
    ASSERT_EQ(txn->get(info_key, &info_val), TxnErrorCode::TXN_OK);
    TxnInfoPB txn_info_pb;
    txn_info_pb.ParseFromString(info_val);
    ASSERT_EQ(txn_info_pb.status(), TxnStatusPB::TXN_STATUS_COMMITTED);

    std::string label_key, label_val;
    txn_label_key({mock_instance, db_id, label}, &label_key);
    ASSERT_EQ(txn->get(label_key, &label_val), TxnErrorCode::TXN_OK);

    std::string index_key, index_val;
    index_key = txn_index_key({mock_instance, txn_id});
    ASSERT_EQ(txn->get(index_key, &index_val), TxnErrorCode::TXN_OK);

    std::string running_key, running_value;
    TxnRunningKeyInfo running_key_info {mock_instance, db_id, txn_id};
    txn_running_key(running_key_info, &running_key);
    ASSERT_EQ(txn->get(running_key, &running_value), TxnErrorCode::TXN_OK);

    std::string rec_txn_key, rec_txn_val;
    RecycleTxnKeyInfo recycle_txn_key_info {mock_instance, db_id, txn_id};
    recycle_txn_key(recycle_txn_key_info, &rec_txn_key);
    ASSERT_EQ(txn->get(rec_txn_key, &rec_txn_val), TxnErrorCode::TXN_KEY_NOT_FOUND);
}

static void check_txn_not_exist(std::unique_ptr<Transaction>& txn, int64_t db_id, int64_t txn_id,
                                std::string label) {
    std::string mock_instance = "test_instance";

    std::string info_key, info_val;
    TxnInfoKeyInfo txn_info_key_info {mock_instance, db_id, txn_id};
    txn_info_key(txn_info_key_info, &info_key);
    ASSERT_EQ(txn->get(info_key, &info_val), TxnErrorCode::TXN_KEY_NOT_FOUND);

    std::string label_key, label_val;
    txn_label_key({mock_instance, db_id, label}, &label_key);
    ASSERT_EQ(txn->get(label_key, &label_val), TxnErrorCode::TXN_KEY_NOT_FOUND);

    std::string index_key, index_val;
    index_key = txn_index_key({mock_instance, txn_id});
    ASSERT_EQ(txn->get(index_key, &index_val), TxnErrorCode::TXN_KEY_NOT_FOUND);

    std::string running_key, running_value;
    TxnRunningKeyInfo running_key_info {mock_instance, db_id, txn_id};
    txn_running_key(running_key_info, &running_key);
    ASSERT_EQ(txn->get(running_key, &running_value), TxnErrorCode::TXN_KEY_NOT_FOUND);

    std::string rec_txn_key, rec_txn_val;
    RecycleTxnKeyInfo recycle_txn_key_info {mock_instance, db_id, txn_id};
    recycle_txn_key(recycle_txn_key_info, &rec_txn_key);
    ASSERT_EQ(txn->get(rec_txn_key, &rec_txn_val), TxnErrorCode::TXN_KEY_NOT_FOUND);
}

TEST(TxnLazyCommitTest, CreateTabletWithDbIdTest) {
    auto txn_kv = get_mem_txn_kv();
    auto meta_service = get_meta_service(txn_kv, true);
    int db_id = 1000313313;
    int table_id = 1001414121;
    int index_id = 1002316473;
    int partition_id = 10035151;

    // mock rowset and tablet
    int64_t tablet_id_base = 11414703;
    for (int i = 0; i < 5; ++i) {
        create_tablet_with_db_id(meta_service.get(), db_id, table_id, index_id, partition_id,
                                 tablet_id_base + i);
    }

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    for (int i = 0; i < 5; ++i) {
        int64_t tablet_id = tablet_id_base + i;
        check_tablet_idx_db_id(txn, db_id, tablet_id);
    }
}

TEST(TxnLazyCommitTest, CreateTabletWithoutDbIdTest) {
    auto txn_kv = get_mem_txn_kv();
    auto meta_service = get_meta_service(txn_kv, true);
    int table_id = 3131;
    int index_id = 4131;
    int partition_id = 3131;

    // mock rowset and tablet
    int64_t tablet_id_base = 42411890;
    for (int i = 0; i < 5; ++i) {
        create_tablet_without_db_id(meta_service.get(), table_id, index_id, partition_id,
                                    tablet_id_base + i);
    }

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    for (int i = 0; i < 5; ++i) {
        int64_t tablet_id = tablet_id_base + i;
        check_tablet_idx_without_db_id(txn, tablet_id);
    }
}

TEST(TxnLazyCommitTest, RepairTabletIndexTest) {
    auto txn_kv = get_mem_txn_kv();
    auto meta_service = get_meta_service(txn_kv, true);
    int64_t db_id = 21318977;
    int64_t table_id = 46714;
    int64_t index_id = 83645;
    int64_t partition_id = 123131;
    std::string label = "test_repair_tablet_index";
    int64_t txn_id = 0;
    std::string mock_instance = "test_instance";

    {
        brpc::Controller cntl;
        BeginTxnRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");

        TxnInfoPB txn_info;
        txn_info.set_db_id(db_id);
        txn_info.set_label(label);
        txn_info.add_table_ids(table_id);
        txn_info.set_timeout_ms(6000);
        req.mutable_txn_info()->CopyFrom(txn_info);
        BeginTxnResponse res;
        meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        txn_id = res.txn_id();
    }

    ASSERT_GT(txn_id, 0);

    // mock rowset and tablet
    int64_t tablet_id_base = 87134121;
    std::vector<std::pair<std::string, doris::RowsetMetaCloudPB>> tmp_rowsets_meta;
    for (int i = 0; i < 5; ++i) {
        create_tablet_without_db_id(meta_service.get(), table_id, index_id, partition_id,
                                    tablet_id_base + i);
        auto tmp_rowset = create_rowset(txn_id, tablet_id_base + i, partition_id);
        tmp_rowsets_meta.push_back(std::make_pair("mock_tmp_rowset_key", tmp_rowset));
        CreateRowsetResponse res;
        commit_rowset(meta_service.get(), tmp_rowset, res);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        for (int i = 0; i < 5; ++i) {
            int64_t tablet_id = tablet_id_base + i;
            check_tablet_idx_without_db_id(txn, tablet_id);
        }
    }

    MetaServiceCode code = MetaServiceCode::UNDEFINED_ERR;
    std::string msg;
    repair_tablet_index(txn_kv, code, msg, mock_instance, db_id, txn_id, tmp_rowsets_meta);
    ASSERT_EQ(code, MetaServiceCode::OK);

    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        for (int i = 0; i < 5; ++i) {
            int64_t tablet_id = tablet_id_base + i;
            check_tablet_idx_db_id(txn, db_id, tablet_id);
        }
    }
}

TEST(TxnLazyCommitTest, CommitTxnEventuallyWithoutDbIdTest) {
    auto txn_kv = get_mem_txn_kv();

    int64_t db_id = 3131397513;
    int64_t table_id = 3213867;
    int64_t index_id = 123513;
    int64_t partition_id = 113123;
    bool commit_txn_eventually_finish_hit = false;
    bool last_pending_txn_id_hit = false;
    int repair_tablet_idx_count = 0;

    auto sp = SyncPoint::get_instance();
    sp->set_call_back("commit_txn_eventually::need_repair_tablet_idx", [&](auto&& args) {
        bool need_repair_tablet_idx = *try_any_cast<bool*>(args[0]);
        LOG(INFO) << "zhangleixxx2" << need_repair_tablet_idx;
        if (repair_tablet_idx_count == 0) {
            ASSERT_TRUE(need_repair_tablet_idx);
            repair_tablet_idx_count++;
        } else {
            ASSERT_FALSE(need_repair_tablet_idx);
        }
    });

    sp->set_call_back("commit_txn_eventually::last_pending_txn_id", [&](auto&& args) {
        int64_t last_pending_txn_id = *try_any_cast<int64_t*>(args[0]);
        ASSERT_EQ(last_pending_txn_id, 0);
        last_pending_txn_id_hit = true;
    });

    sp->set_call_back("commit_txn_eventually::finish", [&](auto&& args) {
        MetaServiceCode code = *try_any_cast<MetaServiceCode*>(args[0]);
        ASSERT_EQ(code, MetaServiceCode::OK);
        commit_txn_eventually_finish_hit = true;
    });
    sp->enable_processing();

    auto meta_service = get_meta_service(txn_kv, true);
    brpc::Controller cntl;
    BeginTxnRequest req;
    req.set_cloud_unique_id("test_cloud_unique_id");
    TxnInfoPB txn_info_pb;
    txn_info_pb.set_db_id(db_id);
    txn_info_pb.set_label("test_label_commit_txn_eventually");
    txn_info_pb.add_table_ids(table_id);
    txn_info_pb.set_timeout_ms(36000);
    req.mutable_txn_info()->CopyFrom(txn_info_pb);
    BeginTxnResponse res;
    meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res,
                            nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    int64_t txn_id = res.txn_id();

    // mock rowset and tablet
    int64_t tablet_id_base = 1103;
    for (int i = 0; i < 5; ++i) {
        create_tablet_without_db_id(meta_service.get(), table_id, index_id, partition_id,
                                    tablet_id_base + i);
        auto tmp_rowset = create_rowset(txn_id, tablet_id_base + i, partition_id);
        CreateRowsetResponse res;
        commit_rowset(meta_service.get(), tmp_rowset, res);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        for (int i = 0; i < 5; ++i) {
            int64_t tablet_id = tablet_id_base + i;
            check_tmp_rowset_exist(txn, tablet_id, txn_id);
        }
    }

    {
        brpc::Controller cntl;
        CommitTxnRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_db_id(db_id);
        req.set_txn_id(txn_id);
        req.set_is_2pc(false);
        req.set_enable_txn_lazy_commit(true);
        for (int i = 0; i < 5; ++i) {
            int64_t tablet_id = tablet_id_base + i;
            req.add_base_tablet_ids(tablet_id);
        }
        CommitTxnResponse res;
        meta_service->commit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                 &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_GE(repair_tablet_idx_count, 0);
        ASSERT_TRUE(last_pending_txn_id_hit);
        ASSERT_TRUE(commit_txn_eventually_finish_hit);
    }

    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string mock_instance = "test_instance";
        for (int i = 0; i < 5; ++i) {
            int64_t tablet_id = tablet_id_base + i;
            check_tablet_idx_db_id(txn, db_id, tablet_id);
            check_tmp_rowset_not_exist(txn, tablet_id, txn_id);
            check_rowset_meta_exist(txn, tablet_id, 2);
        }
    }

    sp->clear_all_call_backs();
    sp->clear_trace();
    sp->disable_processing();
}

TEST(TxnLazyCommitTest, CommitTxnEventuallyWithAbortedTest) {
    auto txn_kv = get_mem_txn_kv();
    int64_t db_id = 55432134;
    int64_t table_id = 326843;
    int64_t index_id = 34345678;
    int64_t partition_id = 212343;

    auto meta_service = get_meta_service(txn_kv, true);
    brpc::Controller cntl;
    BeginTxnRequest req;
    req.set_cloud_unique_id("test_cloud_unique_id");
    TxnInfoPB txn_info_pb;
    txn_info_pb.set_db_id(db_id);
    txn_info_pb.set_label("test_label_commit_txn_eventually");
    txn_info_pb.add_table_ids(table_id);
    txn_info_pb.set_timeout_ms(36000);
    req.mutable_txn_info()->CopyFrom(txn_info_pb);
    BeginTxnResponse res;
    meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res,
                            nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    int64_t txn_id = res.txn_id();

    // mock rowset and tablet
    int64_t tablet_id_base = 372323;
    for (int i = 0; i < 5; ++i) {
        create_tablet_without_db_id(meta_service.get(), table_id, index_id, partition_id,
                                    tablet_id_base + i);
        auto tmp_rowset = create_rowset(txn_id, tablet_id_base + i, partition_id);
        CreateRowsetResponse res;
        commit_rowset(meta_service.get(), tmp_rowset, res);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        for (int i = 0; i < 5; ++i) {
            int64_t tablet_id = tablet_id_base + i;
            check_tmp_rowset_exist(txn, tablet_id, txn_id);
        }
    }

    {
        brpc::Controller cntl;
        AbortTxnRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_txn_id(txn_id);
        req.set_reason("test");
        AbortTxnResponse res;
        meta_service->abort_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(res.txn_info().status(), TxnStatusPB::TXN_STATUS_ABORTED);
    }

    {
        brpc::Controller cntl;
        CommitTxnRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_db_id(db_id);
        req.set_txn_id(txn_id);
        req.set_is_2pc(false);
        req.set_enable_txn_lazy_commit(true);
        for (int i = 0; i < 5; ++i) {
            int64_t tablet_id = tablet_id_base + i;
            req.add_base_tablet_ids(tablet_id);
        }
        CommitTxnResponse res;
        meta_service->commit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                 &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::TXN_ALREADY_ABORTED);
    }

    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        for (int i = 0; i < 5; ++i) {
            int64_t tablet_id = tablet_id_base + i;
            check_tablet_idx_db_id(txn, db_id, tablet_id);
            check_tmp_rowset_exist(txn, tablet_id, txn_id);
            check_rowset_meta_not_exist(txn, tablet_id, 2);
        }
    }
}

TEST(TxnLazyCommitTest, CommitTxnEventuallyWithDbIdTest) {
    auto txn_kv = get_mem_txn_kv();
    int64_t db_id = 7651485414;
    int64_t table_id = 31478952181;
    int64_t index_id = 89894141;
    int64_t partition_id = 1241241;
    bool commit_txn_eventually_finish_hit = false;
    bool last_pending_txn_id_hit = false;
    int repair_tablet_idx_count = 0;

    auto sp = SyncPoint::get_instance();
    sp->set_call_back("commit_txn_eventually::need_repair_tablet_idx", [&](auto&& args) {
        bool need_repair_tablet_idx = *try_any_cast<bool*>(args[0]);
        ASSERT_FALSE(need_repair_tablet_idx);
        repair_tablet_idx_count++;
    });

    sp->set_call_back("commit_txn_eventually::last_pending_txn_id", [&](auto&& args) {
        int64_t last_pending_txn_id = *try_any_cast<int64_t*>(args[0]);
        ASSERT_EQ(last_pending_txn_id, 0);
        last_pending_txn_id_hit = true;
    });

    sp->set_call_back("commit_txn_eventually::finish", [&](auto&& args) {
        MetaServiceCode code = *try_any_cast<MetaServiceCode*>(args[0]);
        ASSERT_EQ(code, MetaServiceCode::OK);
        commit_txn_eventually_finish_hit = true;
    });
    sp->enable_processing();

    auto meta_service = get_meta_service(txn_kv, true);
    brpc::Controller cntl;
    BeginTxnRequest req;
    req.set_cloud_unique_id("test_cloud_unique_id");
    TxnInfoPB txn_info_pb;
    txn_info_pb.set_db_id(db_id);
    txn_info_pb.set_label("test_label_commit_txn_eventually2");
    txn_info_pb.add_table_ids(table_id);
    txn_info_pb.set_timeout_ms(36000);
    req.mutable_txn_info()->CopyFrom(txn_info_pb);
    BeginTxnResponse res;
    meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res,
                            nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    int64_t txn_id = res.txn_id();

    // mock rowset and tablet
    int64_t tablet_id_base = 3131124;
    for (int i = 0; i < 5; ++i) {
        create_tablet_with_db_id(meta_service.get(), db_id, table_id, index_id, partition_id,
                                 tablet_id_base + i);
        auto tmp_rowset = create_rowset(txn_id, tablet_id_base + i, partition_id);
        CreateRowsetResponse res;
        commit_rowset(meta_service.get(), tmp_rowset, res);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    {
        brpc::Controller cntl;
        CommitTxnRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_db_id(db_id);
        req.set_txn_id(txn_id);
        req.set_is_2pc(false);
        req.set_enable_txn_lazy_commit(true);
        CommitTxnResponse res;
        meta_service->commit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                 &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_GE(repair_tablet_idx_count, 0);
        ASSERT_TRUE(last_pending_txn_id_hit);
        ASSERT_TRUE(commit_txn_eventually_finish_hit);
    }

    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string mock_instance = "test_instance";
        for (int i = 0; i < 5; ++i) {
            int64_t tablet_id = tablet_id_base + i;
            check_tablet_idx_db_id(txn, db_id, tablet_id);
            check_tmp_rowset_not_exist(txn, tablet_id, txn_id);
            check_rowset_meta_exist(txn, tablet_id, 2);
        }
    }

    sp->clear_all_call_backs();
    sp->clear_trace();
    sp->disable_processing();
}

TEST(TxnLazyCommitTest, CommitTxnImmediatelyTest) {
    auto txn_kv = get_mem_txn_kv();

    int64_t db_id = 983153141;
    int64_t table_id = 71419093;
    int64_t index_id = 80124;
    int64_t partition_id = 8989313;
    bool commit_txn_immediatelly_hit = false;

    auto sp = SyncPoint::get_instance();
    sp->set_call_back("commit_txn_immediately::finish", [&](auto&& args) {
        MetaServiceCode code = *try_any_cast<MetaServiceCode*>(args[0]);
        ASSERT_EQ(code, MetaServiceCode::OK);
        commit_txn_immediatelly_hit = true;
    });
    sp->enable_processing();

    auto meta_service = get_meta_service(txn_kv, true);
    brpc::Controller cntl;
    BeginTxnRequest req;
    req.set_cloud_unique_id("test_cloud_unique_id");
    TxnInfoPB txn_info_pb;
    txn_info_pb.set_db_id(db_id);
    txn_info_pb.set_label("test_commit_txn_immediatelly");
    txn_info_pb.add_table_ids(table_id);
    txn_info_pb.set_timeout_ms(36000);
    req.mutable_txn_info()->CopyFrom(txn_info_pb);
    BeginTxnResponse res;
    meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res,
                            nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    int64_t txn_id = res.txn_id();

    // mock rowset and tablet
    int64_t tablet_id_base = 31311414;
    for (int i = 0; i < config::txn_lazy_commit_rowsets_thresold; ++i) {
        create_tablet_without_db_id(meta_service.get(), table_id, index_id, partition_id,
                                    tablet_id_base + i);
        auto tmp_rowset = create_rowset(txn_id, tablet_id_base + i, partition_id);
        CreateRowsetResponse res;
        commit_rowset(meta_service.get(), tmp_rowset, res);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    {
        brpc::Controller cntl;
        CommitTxnRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_db_id(db_id);
        req.set_txn_id(txn_id);
        req.set_is_2pc(false);
        req.set_enable_txn_lazy_commit(true);
        CommitTxnResponse res;
        meta_service->commit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                 &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_TRUE(commit_txn_immediatelly_hit);
    }

    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string mock_instance = "test_instance";
        for (int i = 0; i < config::txn_lazy_commit_rowsets_thresold; ++i) {
            int64_t tablet_id = tablet_id_base + i;
            check_tablet_idx_without_db_id(txn, tablet_id);
            check_tmp_rowset_not_exist(txn, tablet_id, txn_id);
            check_rowset_meta_exist(txn, tablet_id, 2);
        }
    }

    sp->clear_all_call_backs();
    sp->clear_trace();
    sp->disable_processing();
}

TEST(TxnLazyCommitTest, NotFallThroughCommitTxnEventuallyTest) {
    auto txn_kv = get_mem_txn_kv();
    int64_t db_id = 415413556;
    int64_t table_id = 34184234;
    int64_t index_id = 9059444;
    int64_t partition_id = 8934984;
    bool commit_txn_immediatelly_hit = false;
    bool commit_txn_eventually_finish_hit = false;

    auto sp = SyncPoint::get_instance();
    sp->set_call_back("commit_txn_immediately::before_commit", [&](auto&& args) {
        TxnErrorCode* err = try_any_cast<TxnErrorCode*>(args[0]);
        *err = TxnErrorCode::TXN_BYTES_TOO_LARGE;

        MetaServiceCode* code = try_any_cast<MetaServiceCode*>(args[1]);
        *code = cast_as<ErrCategory::COMMIT>(*err);

        bool* pred = try_any_cast<bool*>(args.back());
        *pred = true;
        commit_txn_immediatelly_hit = true;
    });

    sp->set_call_back("commit_txn_eventually::finish", [&](auto&& args) {
        MetaServiceCode code = *try_any_cast<MetaServiceCode*>(args[0]);
        ASSERT_EQ(code, MetaServiceCode::OK);
        commit_txn_eventually_finish_hit = true;
    });
    sp->enable_processing();

    auto meta_service = get_meta_service(txn_kv, true);
    brpc::Controller cntl;
    BeginTxnRequest req;
    req.set_cloud_unique_id("test_cloud_unique_id");
    TxnInfoPB txn_info_pb;
    txn_info_pb.set_db_id(db_id);
    txn_info_pb.set_label("test_label_not_fallthrough_commit_txn_eventually");
    txn_info_pb.add_table_ids(table_id);
    txn_info_pb.set_timeout_ms(36000);
    req.mutable_txn_info()->CopyFrom(txn_info_pb);
    BeginTxnResponse res;
    meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res,
                            nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    int64_t txn_id = res.txn_id();

    // mock rowset and tablet
    int64_t tablet_id_base = 783426908;
    for (int i = 0; i < config::txn_lazy_commit_rowsets_thresold; ++i) {
        create_tablet_with_db_id(meta_service.get(), db_id, table_id, index_id, partition_id,
                                 tablet_id_base + i);
        auto tmp_rowset = create_rowset(txn_id, tablet_id_base + i, partition_id);
        CreateRowsetResponse res;
        commit_rowset(meta_service.get(), tmp_rowset, res);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    {
        brpc::Controller cntl;
        CommitTxnRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_db_id(db_id);
        req.set_txn_id(txn_id);
        req.set_is_2pc(false);
        CommitTxnResponse res;
        meta_service->commit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                 &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_TRUE(commit_txn_immediatelly_hit);
        ASSERT_FALSE(commit_txn_eventually_finish_hit);
    }

    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        for (int i = 0; i < config::txn_lazy_commit_rowsets_thresold; ++i) {
            int64_t tablet_id = tablet_id_base + i;
            check_tablet_idx_db_id(txn, db_id, tablet_id);
            check_tmp_rowset_exist(txn, tablet_id, txn_id);
            check_rowset_meta_not_exist(txn, tablet_id, 2);
        }
    }

    sp->clear_all_call_backs();
    sp->clear_trace();
    sp->disable_processing();
}

TEST(TxnLazyCommitTest, FallThroughCommitTxnEventuallyTest) {
    auto txn_kv = get_mem_txn_kv();

    int64_t db_id = 903437831;
    int64_t table_id = 13845693;
    int64_t index_id = 2366023843;
    int64_t partition_id = 210486436;
    bool commit_txn_immediatelly_hit = false;
    bool commit_txn_eventually_finish_hit = false;

    auto sp = SyncPoint::get_instance();
    sp->set_call_back("commit_txn_immediately::before_commit", [&](auto&& args) {
        TxnErrorCode* err = try_any_cast<TxnErrorCode*>(args[0]);
        *err = TxnErrorCode::TXN_BYTES_TOO_LARGE;

        MetaServiceCode* code = try_any_cast<MetaServiceCode*>(args[1]);
        *code = cast_as<ErrCategory::COMMIT>(*err);

        bool* pred = try_any_cast<bool*>(args.back());
        *pred = true;
        commit_txn_immediatelly_hit = true;
    });

    sp->set_call_back("commit_txn_eventually::finish", [&](auto&& args) {
        MetaServiceCode code = *try_any_cast<MetaServiceCode*>(args[0]);
        ASSERT_EQ(code, MetaServiceCode::OK);
        commit_txn_eventually_finish_hit = true;
    });
    sp->enable_processing();

    auto meta_service = get_meta_service(txn_kv, true);
    brpc::Controller cntl;
    BeginTxnRequest req;
    req.set_cloud_unique_id("test_cloud_unique_id");
    TxnInfoPB txn_info_pb;
    txn_info_pb.set_db_id(db_id);
    txn_info_pb.set_label("test_label_fallthrough_commit_txn_eventually");
    txn_info_pb.add_table_ids(table_id);
    txn_info_pb.set_timeout_ms(36000);
    req.mutable_txn_info()->CopyFrom(txn_info_pb);
    BeginTxnResponse res;
    meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res,
                            nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    int64_t txn_id = res.txn_id();

    // mock rowset and tablet
    int64_t tablet_id_base = 1908462;
    for (int i = 0; i < config::txn_lazy_commit_rowsets_thresold; ++i) {
        create_tablet_with_db_id(meta_service.get(), db_id, table_id, index_id, partition_id,
                                 tablet_id_base + i);
        auto tmp_rowset = create_rowset(txn_id, tablet_id_base + i, partition_id);
        CreateRowsetResponse res;
        commit_rowset(meta_service.get(), tmp_rowset, res);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    {
        brpc::Controller cntl;
        CommitTxnRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_db_id(db_id);
        req.set_txn_id(txn_id);
        req.set_is_2pc(false);
        req.set_enable_txn_lazy_commit(true);
        CommitTxnResponse res;
        meta_service->commit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                 &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_TRUE(commit_txn_immediatelly_hit);
        ASSERT_TRUE(commit_txn_eventually_finish_hit);
    }

    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string mock_instance = "test_instance";
        for (int i = 0; i < config::txn_lazy_commit_rowsets_thresold; ++i) {
            int64_t tablet_id = tablet_id_base + i;

            check_tablet_idx_db_id(txn, db_id, tablet_id);
            check_tmp_rowset_not_exist(txn, tablet_id, txn_id);
            check_rowset_meta_exist(txn, tablet_id, 2);
        }
    }

    sp->clear_all_call_backs();
    sp->clear_trace();
    sp->disable_processing();
}

TEST(TxnLazyCommitTest, ConcurrentCommitTxnEventuallyCase1Test) {
    // ===========================================================================
    // threads concurrent execution flow:
    //
    //          thread1                       thread2
    //             |                             |
    //  commit_txn_eventually begin   commit_txn_eventually begin
    //             |                             |
    //       lazy commit wait                    |
    //             |                             |
    //             |                      advance last txn
    //             |                             |
    //             |                           finish
    //             |                             |
    //          finish                           |
    //             |                             |
    //             |                             |
    //             v                             v

    auto txn_kv = get_mem_txn_kv();
    int64_t db_id = 315477;
    int64_t table_id = 31752134;
    int64_t index_id = 3458532;
    int64_t partition_id = 26328765;

    std::mutex go_mutex;
    std::condition_variable go_cv;

    bool go = false;
    std::atomic<int32_t> commit_txn_eventually_begin_count = {0};
    std::atomic<int32_t> last_pending_txn_id_count = {0};
    std::atomic<int32_t> txn_lazy_committer_wait_count = {0};
    std::atomic<int32_t> finish_count = {0};

    auto sp = SyncPoint::get_instance();

    int64_t first_txn_id = 0;
    sp->set_call_back("commit_txn_eventually:begin", [&](auto&& args) {
        std::unique_lock<std::mutex> _lock(go_mutex);
        commit_txn_eventually_begin_count++;
        if (commit_txn_eventually_begin_count == 1) {
            first_txn_id = *try_any_cast<int64_t*>(args[0]);
        }
        if (commit_txn_eventually_begin_count == 2) {
            {
                go_cv.wait(_lock, [&] { return txn_lazy_committer_wait_count == 1; });
            }
        }
    });

    sp->set_call_back("commit_txn_eventually::advance_last_pending_txn_id", [&](auto&& args) {
        std::unique_lock<std::mutex> _lock(go_mutex);
        last_pending_txn_id_count++;
        if (last_pending_txn_id_count == 1) {
            int64_t last_pending_txn_id = *try_any_cast<int64_t*>(args[0]);
            ASSERT_EQ(last_pending_txn_id, first_txn_id);
        }
    });

    sp->set_call_back("commit_txn_eventually::txn_lazy_committer_wait", [&](auto&& args) {
        std::unique_lock<std::mutex> _lock(go_mutex);
        txn_lazy_committer_wait_count++;
        if (txn_lazy_committer_wait_count == 1) {
            go_cv.notify_all();
            go_cv.wait(_lock, [&] { return finish_count == 1; });
        }
    });

    int64_t second_txn_id = 0;
    sp->set_call_back("commit_txn_eventually::finish", [&](auto&& args) {
        MetaServiceCode code = *try_any_cast<MetaServiceCode*>(args[0]);
        int64_t txn_id = *try_any_cast<int64_t*>(args[1]);
        std::unique_lock<std::mutex> _lock(go_mutex);
        finish_count++;
        if (finish_count == 1) {
            second_txn_id = txn_id;
            ASSERT_NE(second_txn_id, first_txn_id);
            go_cv.notify_all();
        }
        ASSERT_EQ(code, MetaServiceCode::OK);
    });

    sp->enable_processing();

    auto meta_service = get_meta_service(txn_kv, true);
    // mock rowset and tablet
    int64_t tablet_id_base = 1908462;
    for (int i = 0; i < 10; ++i) {
        create_tablet_with_db_id(meta_service.get(), db_id, table_id, index_id, partition_id,
                                 tablet_id_base + i);
    }

    int64_t txn_id1 = 0;
    std::thread thread1([&] {
        {
            std::unique_lock<std::mutex> _lock(go_mutex);
            go_cv.wait(_lock, [&] { return go; });
        }
        {
            brpc::Controller cntl;
            BeginTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            TxnInfoPB txn_info_pb;
            txn_info_pb.set_db_id(db_id);
            txn_info_pb.set_label("test_label_concurrent_commit_txn_eventually2313");
            txn_info_pb.add_table_ids(table_id);
            txn_info_pb.set_timeout_ms(36000);
            req.mutable_txn_info()->CopyFrom(txn_info_pb);
            BeginTxnResponse res;
            meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            txn_id1 = res.txn_id();
            ASSERT_GT(txn_id1, 0);
        }
        {
            for (int i = 0; i < 10; ++i) {
                auto tmp_rowset = create_rowset(txn_id1, tablet_id_base + i, partition_id);
                CreateRowsetResponse res;
                commit_rowset(meta_service.get(), tmp_rowset, res);
                ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            }
        }

        {
            brpc::Controller cntl;
            CommitTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            req.set_db_id(db_id);
            req.set_txn_id(txn_id1);
            req.set_is_2pc(false);
            req.set_enable_txn_lazy_commit(true);
            CommitTxnResponse res;
            meta_service->commit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        }
    });

    int64_t txn_id2 = 0;
    std::thread thread2([&] {
        {
            std::unique_lock<std::mutex> _lock(go_mutex);
            go_cv.wait(_lock, [&] { return go; });
        }
        {
            brpc::Controller cntl;
            BeginTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            TxnInfoPB txn_info_pb;
            txn_info_pb.set_db_id(db_id);
            txn_info_pb.set_label("test_label_concurrent_commit_txn_eventually234142");
            txn_info_pb.add_table_ids(table_id);
            txn_info_pb.set_timeout_ms(36000);
            req.mutable_txn_info()->CopyFrom(txn_info_pb);
            BeginTxnResponse res;
            meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            txn_id2 = res.txn_id();
            ASSERT_GT(txn_id2, 0);
        }
        {
            for (int i = 0; i < 10; ++i) {
                auto tmp_rowset = create_rowset(txn_id2, tablet_id_base + i, partition_id);
                CreateRowsetResponse res;
                commit_rowset(meta_service.get(), tmp_rowset, res);
                ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            }
        }

        {
            brpc::Controller cntl;
            CommitTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            req.set_db_id(db_id);
            req.set_txn_id(txn_id2);
            req.set_is_2pc(false);
            req.set_enable_txn_lazy_commit(true);
            CommitTxnResponse res;
            meta_service->commit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
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
    ASSERT_EQ(commit_txn_eventually_begin_count, 3);
    ASSERT_EQ(last_pending_txn_id_count, 1);
    ASSERT_EQ(finish_count, 2);

    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string mock_instance = "test_instance";
        for (int i = 0; i < 10; ++i) {
            int64_t tablet_id = tablet_id_base + i;
            std::string key = meta_tablet_idx_key({mock_instance, tablet_id});
            std::string val;
            ASSERT_EQ(txn->get(key, &val), TxnErrorCode::TXN_OK);
            TabletIndexPB tablet_idx_pb;
            tablet_idx_pb.ParseFromString(val);
            ASSERT_EQ(tablet_idx_pb.db_id(), db_id);

            std::string tmp_rowset_key =
                    meta_rowset_tmp_key({mock_instance, first_txn_id, tablet_id});
            std::string tmp_rowset_val;
            ASSERT_EQ(txn->get(tmp_rowset_key, &tmp_rowset_val), TxnErrorCode::TXN_KEY_NOT_FOUND);

            std::string rowset_key = meta_rowset_key({mock_instance, tablet_id, 2});
            std::string rowset_val;
            ASSERT_EQ(txn->get(rowset_key, &rowset_val), TxnErrorCode::TXN_OK);

            tmp_rowset_key = meta_rowset_tmp_key({mock_instance, second_txn_id, tablet_id});
            tmp_rowset_val.clear();
            ASSERT_EQ(txn->get(tmp_rowset_key, &tmp_rowset_val), TxnErrorCode::TXN_KEY_NOT_FOUND);

            rowset_key = meta_rowset_key({mock_instance, tablet_id, 3});
            rowset_val.clear();
            ASSERT_EQ(txn->get(rowset_key, &rowset_val), TxnErrorCode::TXN_OK);
        }
    }
}

TEST(TxnLazyCommitTest, ConcurrentCommitTxnEventuallyCase2Test) {
    // ===========================================================================
    // threads concurrent execution flow:
    //
    //           thread1                           thread2
    //              |                                 |
    //   commit_txn_eventually begin      commit_txn_immediately begin
    //              |                                 |
    //       lazy commit wait                         |
    //              |                                 |
    //              |                         advance last txn
    //              |                                 |
    //              |                               finish
    //              |                                 |
    //           finish                               |
    //              |                                 |
    //              |                                 |
    //              v                                 v

    auto txn_kv = get_mem_txn_kv();
    int64_t db_id = 134179141;
    int64_t table_id = 3243234;
    int64_t index_id = 8098324;
    int64_t partition_id = 32895321;

    std::mutex go_mutex;
    std::condition_variable go_cv;
    bool go = false;

    std::atomic<int32_t> commit_txn_immediately_begin_count = {0};
    std::atomic<int32_t> last_pending_txn_id_count = {0};
    std::atomic<int32_t> txn_lazy_committer_wait_count = {0};
    std::atomic<int32_t> immediately_finish_count = {0};
    std::atomic<int32_t> eventually_finish_count = {0};

    auto sp = SyncPoint::get_instance();

    int64_t first_txn_id = 0;
    sp->set_call_back("commit_txn_immediately:begin", [&](auto&& args) {
        std::unique_lock<std::mutex> _lock(go_mutex);
        commit_txn_immediately_begin_count++;
        if (commit_txn_immediately_begin_count == 1) {
            {
                first_txn_id = *try_any_cast<int64_t*>(args[0]);
                go_cv.wait(_lock, [&] { return txn_lazy_committer_wait_count == 1; });
            }
        }
    });

    int64_t second_txn_id = 0;
    sp->set_call_back("commit_txn_eventually::txn_lazy_committer_wait", [&](auto&& args) {
        std::unique_lock<std::mutex> _lock(go_mutex);
        txn_lazy_committer_wait_count++;
        if (txn_lazy_committer_wait_count == 1) {
            int64_t txn_id = *try_any_cast<int64_t*>(args[0]);
            second_txn_id = txn_id;
            go_cv.notify_all();
            go_cv.wait(_lock, [&] { return immediately_finish_count == 1; });
        }
    });

    sp->set_call_back("commit_txn_immediately::advance_last_pending_txn_id", [&](auto&& args) {
        std::unique_lock<std::mutex> _lock(go_mutex);
        last_pending_txn_id_count++;
        if (last_pending_txn_id_count == 1) {
            int64_t last_pending_txn_id = *try_any_cast<int64_t*>(args[0]);
            ASSERT_EQ(last_pending_txn_id, second_txn_id);
        }
    });

    sp->set_call_back("commit_txn_immediately::finish", [&](auto&& args) {
        MetaServiceCode code = *try_any_cast<MetaServiceCode*>(args[0]);
        ASSERT_EQ(code, MetaServiceCode::OK);
        std::unique_lock<std::mutex> _lock(go_mutex);
        immediately_finish_count++;
        if (immediately_finish_count == 1) {
            go_cv.notify_all();
        }
    });

    sp->set_call_back("commit_txn_eventually::finish", [&](auto&& args) {
        MetaServiceCode code = *try_any_cast<MetaServiceCode*>(args[0]);
        ASSERT_EQ(code, MetaServiceCode::OK);
        eventually_finish_count++;
    });

    sp->enable_processing();

    auto meta_service = get_meta_service(txn_kv, true);
    // mock rowset and tablet
    int64_t tablet_id_base = 1908462;
    for (int i = 0; i < 10; ++i) {
        create_tablet_with_db_id(meta_service.get(), db_id, table_id, index_id, partition_id,
                                 tablet_id_base + i);
    }

    int64_t txn_id1 = 0;
    std::thread thread1([&] {
        {
            std::unique_lock<std::mutex> _lock(go_mutex);
            go_cv.wait(_lock, [&] { return go; });
        }
        {
            brpc::Controller cntl;
            BeginTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            TxnInfoPB txn_info_pb;
            txn_info_pb.set_db_id(db_id);
            txn_info_pb.set_label("test_label_concurrent_commit_txn_eventually3441");
            txn_info_pb.add_table_ids(table_id);
            txn_info_pb.set_timeout_ms(36000);
            req.mutable_txn_info()->CopyFrom(txn_info_pb);
            BeginTxnResponse res;
            meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            txn_id1 = res.txn_id();
            ASSERT_GT(txn_id1, 0);
        }
        {
            for (int i = 0; i < 10; ++i) {
                auto tmp_rowset = create_rowset(txn_id1, tablet_id_base + i, partition_id);
                CreateRowsetResponse res;
                commit_rowset(meta_service.get(), tmp_rowset, res);
                ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            }
        }

        {
            brpc::Controller cntl;
            CommitTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            req.set_db_id(db_id);
            req.set_txn_id(txn_id1);
            req.set_is_2pc(false);
            req.set_enable_txn_lazy_commit(true);
            CommitTxnResponse res;
            meta_service->commit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        }
    });

    int64_t txn_id2 = 0;
    std::thread thread2([&] {
        {
            std::unique_lock<std::mutex> _lock(go_mutex);
            go_cv.wait(_lock, [&] { return go; });
        }
        {
            brpc::Controller cntl;
            BeginTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            TxnInfoPB txn_info_pb;
            txn_info_pb.set_db_id(db_id);
            txn_info_pb.set_label("test_label_concurrent_commit_txn_eventually3245232");
            txn_info_pb.add_table_ids(table_id);
            txn_info_pb.set_timeout_ms(36000);
            req.mutable_txn_info()->CopyFrom(txn_info_pb);
            BeginTxnResponse res;
            meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            txn_id2 = res.txn_id();
            ASSERT_GT(txn_id2, 0);
        }
        {
            for (int i = 0; i < 10; ++i) {
                auto tmp_rowset = create_rowset(txn_id2, tablet_id_base + i, partition_id);
                CreateRowsetResponse res;
                commit_rowset(meta_service.get(), tmp_rowset, res);
                ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            }
        }

        {
            brpc::Controller cntl;
            CommitTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            req.set_db_id(db_id);
            req.set_txn_id(txn_id2);
            req.set_is_2pc(false);
            req.set_enable_txn_lazy_commit(false);
            CommitTxnResponse res;
            meta_service->commit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
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
    ASSERT_EQ(commit_txn_immediately_begin_count, 2);
    ASSERT_EQ(last_pending_txn_id_count, 1);
    ASSERT_EQ(immediately_finish_count, 1);
    ASSERT_EQ(eventually_finish_count, 1);

    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        for (int i = 0; i < 10; ++i) {
            int64_t tablet_id = tablet_id_base + i;
            check_tablet_idx_db_id(txn, db_id, tablet_id);

            check_tmp_rowset_not_exist(txn, tablet_id, first_txn_id);
            check_rowset_meta_exist(txn, tablet_id, 2);

            check_tmp_rowset_not_exist(txn, tablet_id, second_txn_id);
            check_rowset_meta_exist(txn, tablet_id, 3);
        }
    }
}

TEST(TxnLazyCommitTest, ConcurrentCommitTxnEventuallyCase3Test) {
    // ===========================================================================
    // threads concurrent execution flow:
    //
    //            thread1              thread2
    //               |                     |
    //  commit_txn_eventually begin  get rowset begin
    //               |                     |
    //   lazy commit wait                  |
    //               |                     |
    //               |              advance last txn
    //               |                     |
    //               |                   finish
    //               |                     |
    //            finish                   |
    //               |                     |
    //               |                     |
    //               v                     v

    auto txn_kv = get_mem_txn_kv();

    int64_t db_id = 6544345;
    int64_t table_id = 32431068334;
    int64_t index_id = 132433;
    int64_t partition_id = 956120248;

    auto meta_service = get_meta_service(txn_kv, true);
    // mock rowset and tablet
    int64_t tablet_id_base = 19201262;

    for (int i = 0; i < 10; ++i) {
        create_tablet_with_db_id(meta_service.get(), db_id, table_id, index_id, partition_id,
                                 tablet_id_base + i);
    }
    {
        int tmp_txn_id = 0;
        {
            brpc::Controller cntl;
            BeginTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            TxnInfoPB txn_info_pb;
            txn_info_pb.set_db_id(db_id);
            txn_info_pb.set_label("test_label_32ae213dasf2");
            txn_info_pb.add_table_ids(table_id);
            txn_info_pb.set_timeout_ms(36000);
            req.mutable_txn_info()->CopyFrom(txn_info_pb);
            BeginTxnResponse res;
            meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            tmp_txn_id = res.txn_id();
            ASSERT_GT(res.txn_id(), 0);
        }

        {
            for (int i = 0; i < 10; ++i) {
                auto tmp_rowset = create_rowset(tmp_txn_id, tablet_id_base + i, partition_id);
                CreateRowsetResponse res;
                commit_rowset(meta_service.get(), tmp_rowset, res);
                ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            }
        }

        {
            brpc::Controller cntl;
            CommitTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            req.set_db_id(db_id);
            req.set_txn_id(tmp_txn_id);
            req.set_is_2pc(false);
            req.set_enable_txn_lazy_commit(true);
            CommitTxnResponse res;
            meta_service->commit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        }
    }

    std::mutex go_mutex;
    std::condition_variable go_cv;
    bool go = false;

    std::atomic<int32_t> get_rowset_begin_count = {0};
    std::atomic<int32_t> last_pending_txn_id_count = {0};
    std::atomic<int32_t> txn_lazy_committer_submit_count = {0};
    std::atomic<int32_t> get_rowset_finish_count = {0};
    std::atomic<int32_t> eventually_finish_count = {0};

    auto sp = SyncPoint::get_instance();

    int64_t get_rowset_tablet_id = 0;
    sp->set_call_back("get_rowset:begin", [&](auto&& args) {
        std::unique_lock<std::mutex> _lock(go_mutex);
        get_rowset_begin_count++;
        if (get_rowset_begin_count == 1) {
            {
                get_rowset_tablet_id = *try_any_cast<int64_t*>(args[0]);
                go_cv.wait(_lock, [&] { return txn_lazy_committer_submit_count == 1; });
            }
        }
    });

    int64_t eventually_txn_id = 0;
    sp->set_call_back("commit_txn_eventually::txn_lazy_committer_submit", [&](auto&& args) {
        std::unique_lock<std::mutex> _lock(go_mutex);
        txn_lazy_committer_submit_count++;
        if (txn_lazy_committer_submit_count == 1) {
            eventually_txn_id = *try_any_cast<int64_t*>(args[0]);
            go_cv.notify_all();
            go_cv.wait(_lock, [&] { return get_rowset_finish_count == 1; });
        }
    });

    sp->set_call_back("get_rowset::advance_last_pending_txn_id", [&](auto&& args) {
        std::unique_lock<std::mutex> _lock(go_mutex);
        last_pending_txn_id_count++;
        if (last_pending_txn_id_count == 1) {
            auto version_pb = *try_any_cast<VersionPB*>(args[0]);
            ASSERT_EQ(version_pb.pending_txn_ids(0), eventually_txn_id);
            ASSERT_GT(version_pb.pending_txn_ids(0), 0);
        }
    });

    sp->set_call_back("get_rowset::finish", [&](auto&& args) {
        std::unique_lock<std::mutex> _lock(go_mutex);
        get_rowset_finish_count++;
        if (get_rowset_finish_count == 1) {
            go_cv.notify_all();
        }
    });

    sp->set_call_back("commit_txn_eventually::finish", [&](auto&& args) {
        MetaServiceCode code = *try_any_cast<MetaServiceCode*>(args[0]);
        ASSERT_EQ(code, MetaServiceCode::OK);
        eventually_finish_count++;
    });

    sp->enable_processing();

    int64_t txn_id1 = 0;
    std::thread thread1([&] {
        {
            std::unique_lock<std::mutex> _lock(go_mutex);
            go_cv.wait(_lock, [&] { return go; });
        }
        {
            brpc::Controller cntl;
            BeginTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            TxnInfoPB txn_info_pb;
            txn_info_pb.set_db_id(db_id);
            txn_info_pb.set_label("test_label_315322242");
            txn_info_pb.add_table_ids(table_id);
            txn_info_pb.set_timeout_ms(36000);
            req.mutable_txn_info()->CopyFrom(txn_info_pb);
            BeginTxnResponse res;
            meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            txn_id1 = res.txn_id();
            ASSERT_GT(txn_id1, 0);
        }

        {
            for (int i = 0; i < 10; ++i) {
                auto tmp_rowset = create_rowset(txn_id1, tablet_id_base + i, partition_id);
                CreateRowsetResponse res;
                commit_rowset(meta_service.get(), tmp_rowset, res);
                ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            }
        }

        {
            brpc::Controller cntl;
            CommitTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            req.set_db_id(db_id);
            req.set_txn_id(txn_id1);
            req.set_is_2pc(false);
            req.set_enable_txn_lazy_commit(true);
            CommitTxnResponse res;
            meta_service->commit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        }
    });

    std::thread thread2([&] {
        {
            std::unique_lock<std::mutex> _lock(go_mutex);
            go_cv.wait(_lock, [&] { return go; });
        }
        {
            brpc::Controller cntl;
            GetRowsetRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            auto tablet_idx = req.mutable_idx();
            tablet_idx->set_table_id(table_id);
            tablet_idx->set_index_id(index_id);
            tablet_idx->set_partition_id(partition_id);
            tablet_idx->set_tablet_id(tablet_id_base);
            req.set_start_version(0);
            req.set_end_version(-1);
            req.set_cumulative_compaction_cnt(0);
            req.set_base_compaction_cnt(0);
            req.set_cumulative_point(2);

            GetRowsetResponse res;
            meta_service->get_rowset(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
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

    ASSERT_EQ(get_rowset_begin_count, 2);
    ASSERT_EQ(last_pending_txn_id_count, 1);
    ASSERT_EQ(txn_lazy_committer_submit_count, 1);
    ASSERT_EQ(get_rowset_finish_count, 1);
    ASSERT_EQ(eventually_finish_count, 1);
}

TEST(TxnLazyCommitTest, ConcurrentCommitTxnEventuallyCase4Test) {
    // ===========================================================================
    // threads concurrent execution flow:
    //
    //   meta-service           recycler
    //         |                    |
    //       begin                  |
    //         |                    |
    //   lazy commit submit         |
    //         |                    |
    //       dead                   |
    //         |                    |
    //         |         abort_timeout_txn begin
    //         |                    |
    //         |             advance last txn
    //         |                    |
    //                              |
    //         |                  finish
    //         |                    |
    //         v                    v

    auto txn_kv = get_mem_txn_kv();

    int64_t db_id = 77623430234;
    int64_t table_id = 96545043;
    int64_t index_id = 2381203456;
    int64_t partition_id = 450976544;
    std::string mock_instance = "test_instance";
    const std::string label = "test_label_6787230013";

    bool commit_txn_eventullay_hit = false;
    bool abort_timeout_txn_hit = false;

    auto sp = SyncPoint::get_instance();

    sp->set_call_back("commit_txn_eventually::txn_lazy_committer_submit", [&](auto&& args) {
        commit_txn_eventullay_hit = true;
        bool* pred = try_any_cast<bool*>(args.back());
        *pred = true;
    });

    TxnInfoPB txn_info_pb;
    sp->set_call_back("abort_timeout_txn::advance_last_pending_txn_id", [&](auto&& args) {
        abort_timeout_txn_hit = true;
        txn_info_pb = *try_any_cast<TxnInfoPB*>(args[0]);
        {
            std::unique_ptr<Transaction> txn;
            ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
            check_txn_committed(txn, db_id, txn_info_pb.txn_id(), label);
        }
    });

    sp->enable_processing();

    auto meta_service = get_meta_service(txn_kv, true);
    // mock rowset and tablet
    int64_t tablet_id_base = 213430076554;
    for (int i = 0; i < 10; ++i) {
        create_tablet_with_db_id(meta_service.get(), db_id, table_id, index_id, partition_id,
                                 tablet_id_base + i);
    }
    int txn_id = 0;
    {
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
            meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            txn_id = res.txn_id();
            ASSERT_GT(txn_id, 0);
        }

        {
            for (int i = 0; i < 10; ++i) {
                auto tmp_rowset = create_rowset(txn_id, tablet_id_base + i, partition_id);
                CreateRowsetResponse res;
                commit_rowset(meta_service.get(), tmp_rowset, res);
                ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            }
        }

        {
            brpc::Controller cntl;
            CommitTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            req.set_db_id(db_id);
            req.set_txn_id(txn_id);
            req.set_is_2pc(false);
            req.set_enable_txn_lazy_commit(true);
            CommitTxnResponse res;
            meta_service->commit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        }
    }

    InstanceInfoPB instance;
    instance.set_instance_id(mock_instance);
    InstanceRecycler recycler(txn_kv, instance, thread_group,
                              std::make_shared<TxnLazyCommitter>(txn_kv));

    ASSERT_EQ(recycler.init(), 0);
    ASSERT_EQ(recycler.abort_timeout_txn(), 0);
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        check_txn_visible(txn, db_id, txn_id, label);
    }
    sleep(1);
    ASSERT_EQ(recycler.recycle_expired_txn_label(), 0);
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        check_txn_not_exist(txn, db_id, txn_id, label);
    }

    sp->clear_all_call_backs();
    sp->clear_trace();
    sp->disable_processing();
    ASSERT_TRUE(commit_txn_eventullay_hit);
    ASSERT_TRUE(abort_timeout_txn_hit);
    ASSERT_EQ(txn_id, txn_info_pb.txn_id());
}

TEST(TxnLazyCommitTest, RowsetMetaSizeExceedTest) {
    auto txn_kv = get_mem_txn_kv();

    int64_t db_id = 5252025;
    int64_t table_id = 35201043384;
    int64_t index_id = 256439;
    int64_t partition_id = 732536259;

    auto meta_service = get_meta_service(txn_kv, true);
    int64_t tablet_id = 25910248;

    {
        create_tablet_with_db_id(meta_service.get(), db_id, table_id, index_id, partition_id,
                                 tablet_id);
    }
    {
        int tmp_txn_id = 0;
        {
            brpc::Controller cntl;
            BeginTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            TxnInfoPB txn_info_pb;
            txn_info_pb.set_db_id(db_id);
            txn_info_pb.set_label("test_label_32ae213dasg3");
            txn_info_pb.add_table_ids(table_id);
            txn_info_pb.set_timeout_ms(36000);
            req.mutable_txn_info()->CopyFrom(txn_info_pb);
            BeginTxnResponse res;
            meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            tmp_txn_id = res.txn_id();
            ASSERT_GT(res.txn_id(), 0);
        }
        {
            auto tmp_rowset = create_rowset(tmp_txn_id, tablet_id, partition_id);
            CreateRowsetResponse res;
            commit_rowset(meta_service.get(), tmp_rowset, res);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        }
        {
            brpc::Controller cntl;
            CommitTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            req.set_db_id(db_id);
            req.set_txn_id(tmp_txn_id);
            req.set_is_2pc(false);
            req.set_enable_txn_lazy_commit(true);
            CommitTxnResponse res;
            meta_service->commit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        }
    }

    auto* sp = SyncPoint::get_instance();
    sp->set_call_back("get_rowset:meta_exceed_limit", [](auto&& args) {
        auto* byte_size = try_any_cast<size_t*>(args[0]);
        *byte_size = std::numeric_limits<int32_t>::max();
        ++(*byte_size);
    });

    sp->enable_processing();
    {
        brpc::Controller cntl;
        GetRowsetRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        auto* tablet_idx = req.mutable_idx();
        tablet_idx->set_table_id(table_id);
        tablet_idx->set_index_id(index_id);
        tablet_idx->set_partition_id(partition_id);
        tablet_idx->set_tablet_id(tablet_id);
        req.set_start_version(0);
        req.set_end_version(-1);
        req.set_cumulative_compaction_cnt(0);
        req.set_base_compaction_cnt(0);
        req.set_cumulative_point(2);

        GetRowsetResponse res;
        meta_service->get_rowset(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                 &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::PROTOBUF_PARSE_ERR);
    }
}

} // namespace doris::cloud
