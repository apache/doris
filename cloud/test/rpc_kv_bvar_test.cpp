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

//#define private public
#include "common/bvars.h"
#include "meta-service/meta_service.h"
//#undef private

#include <brpc/controller.h>
#include <bvar/window.h>
#include <gen_cpp/cloud.pb.h>
#include <gen_cpp/olap_file.pb.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <random>
#include <thread>

#include "common/config.h"
#include "common/logging.h"
#include "common/util.h"
#include "cpp/sync_point.h"
#include "meta-service/keys.h"
#include "meta-service/mem_txn_kv.h"
#include "meta-service/meta_service_helper.h"
#include "meta-service/txn_kv.h"
#include "meta-service/txn_kv_error.h"
#include "mock_resource_manager.h"
#include "rate-limiter/rate_limiter.h"
#include "resource-manager/resource_manager.h"

int main(int argc, char** argv) {
    const std::string conf_file = "/mnt/disk2/lihao/doris/cloud/conf/doris_cloud.conf";
    if (!doris::cloud::config::init(conf_file.c_str(), true)) {
        std::cerr << "failed to init config file, conf=" << conf_file << std::endl;
        return -1;
    }

    if (!doris::cloud::init_glog("rpc_kv_bvar_test")) {
        std::cerr << "failed to init glog" << std::endl;
        return -1;
    }

    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace doris::cloud {
using Status = MetaServiceResponseStatus;

void start_compaction_job(MetaService* meta_service, int64_t tablet_id, const std::string& job_id,
                          const std::string& initiator, int base_compaction_cnt,
                          int cumu_compaction_cnt, TabletCompactionJobPB::CompactionType type,
                          StartTabletJobResponse& res,
                          std::pair<int64_t, int64_t> input_version = {0, 0}) {
    brpc::Controller cntl;
    StartTabletJobRequest req;
    req.mutable_job()->mutable_idx()->set_tablet_id(tablet_id);
    auto* compaction = req.mutable_job()->add_compaction();
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

std::unique_ptr<MetaServiceProxy> get_meta_service(bool mock_resource_mgr) {
    int ret = 0;
    // MemKv
    auto txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<MemTxnKv>());
    if (txn_kv != nullptr) {
        ret = txn_kv->init();
        [&] { ASSERT_EQ(ret, 0); }();
    }
    [&] { ASSERT_NE(txn_kv.get(), nullptr); }();

    std::unique_ptr<Transaction> txn;
    EXPECT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->remove("\x00", "\xfe"); // This is dangerous if the fdb is not correctly set
    EXPECT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    auto rs = std::make_shared<MockResourceManager>(txn_kv);
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

static void commit_rowset(MetaServiceProxy* meta_service, const doris::RowsetMetaCloudPB& rowset,
                          CreateRowsetResponse& res) {
    brpc::Controller cntl;
    auto* arena = res.GetArena();
    auto* req = google::protobuf::Arena::CreateMessage<CreateRowsetRequest>(arena);
    req->mutable_rowset_meta()->CopyFrom(rowset);
    meta_service->commit_rowset(&cntl, req, &res, nullptr);
    if (!arena) {
        delete req;
    }
}

static std::string next_rowset_id() {
    static int cnt = 0;
    return std::to_string(++cnt);
}

static void fill_schema(doris::TabletSchemaCloudPB* schema, int32_t schema_version) {
    schema->set_schema_version(schema_version);
    for (int i = 0; i < 10; ++i) {
        auto* column = schema->add_column();
        column->set_unique_id(20000 + i);
        column->set_type("INT");
    }
}

static void add_tablet(CreateTabletsRequest& req, int64_t table_id, int64_t index_id,
                       int64_t partition_id, int64_t tablet_id, const std::string& rowset_id,
                       int32_t schema_version) {
    auto* tablet = req.add_tablet_metas();
    tablet->set_table_id(table_id);
    tablet->set_index_id(index_id);
    tablet->set_partition_id(partition_id);
    tablet->set_tablet_id(tablet_id);
    auto* schema = tablet->mutable_schema();
    fill_schema(schema, schema_version);
    auto* first_rowset = tablet->add_rs_metas();
    first_rowset->set_rowset_id(0); // required
    first_rowset->set_rowset_id_v2(rowset_id);
    first_rowset->set_start_version(0);
    first_rowset->set_end_version(1);
    first_rowset->mutable_tablet_schema()->CopyFrom(*schema);
}

static void add_tablet(CreateTabletsRequest& req, int64_t table_id, int64_t index_id,
                       int64_t partition_id, int64_t tablet_id) {
    auto* tablet = req.add_tablet_metas();
    tablet->set_table_id(table_id);
    tablet->set_index_id(index_id);
    tablet->set_partition_id(partition_id);
    tablet->set_tablet_id(tablet_id);
    auto* schema = tablet->mutable_schema();
    schema->set_schema_version(0);
    auto* first_rowset = tablet->add_rs_metas();
    first_rowset->set_rowset_id(0); // required
    first_rowset->set_rowset_id_v2(next_rowset_id());
    first_rowset->set_start_version(0);
    first_rowset->set_end_version(1);
    first_rowset->mutable_tablet_schema()->CopyFrom(*schema);
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

static void create_tablet(MetaServiceProxy* meta_service, int64_t table_id, int64_t index_id,
                          int64_t partition_id, int64_t tablet_id, const std::string& rowset_id,
                          int32_t schema_version) {
    brpc::Controller cntl;
    CreateTabletsRequest req;
    CreateTabletsResponse res;
    add_tablet(req, table_id, index_id, partition_id, tablet_id, rowset_id, schema_version);
    meta_service->create_tablets(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << tablet_id;
}

static void create_tablet(MetaServiceProxy* meta_service, int64_t table_id, int64_t index_id,
                          int64_t partition_id, int64_t tablet_id) {
    brpc::Controller cntl;
    CreateTabletsRequest req;
    CreateTabletsResponse res;
    add_tablet(req, table_id, index_id, partition_id, tablet_id);
    meta_service->create_tablets(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << tablet_id;
}

static void begin_txn(MetaServiceProxy* meta_service, int64_t db_id, const std::string& label,
                      int64_t table_id, int64_t& txn_id) {
    brpc::Controller cntl;
    BeginTxnRequest req;
    BeginTxnResponse res;
    auto* txn_info = req.mutable_txn_info();
    txn_info->set_db_id(db_id);
    txn_info->set_label(label);
    txn_info->add_table_ids(table_id);
    txn_info->set_timeout_ms(36000);
    meta_service->begin_txn(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << label;
    ASSERT_TRUE(res.has_txn_id()) << label;
    txn_id = res.txn_id();
}

static doris::RowsetMetaCloudPB create_rowset(int64_t txn_id, int64_t tablet_id,
                                              int partition_id = 10, int64_t version = -1,
                                              int num_rows = 100) {
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

static void check_get_tablet(MetaServiceProxy* meta_service, int64_t tablet_id,
                             int32_t schema_version) {
    brpc::Controller cntl;
    GetTabletRequest req;
    GetTabletResponse res;
    req.set_tablet_id(tablet_id);
    meta_service->get_tablet(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << tablet_id;
    ASSERT_TRUE(res.has_tablet_meta()) << tablet_id;
    EXPECT_TRUE(res.tablet_meta().has_schema()) << tablet_id;
    EXPECT_EQ(res.tablet_meta().schema_version(), schema_version) << tablet_id;
};

static void prepare_rowset(MetaServiceProxy* meta_service, const doris::RowsetMetaCloudPB& rowset,
                           CreateRowsetResponse& res) {
    brpc::Controller cntl;
    auto* arena = res.GetArena();
    auto* req = google::protobuf::Arena::CreateMessage<CreateRowsetRequest>(arena);
    req->mutable_rowset_meta()->CopyFrom(rowset);
    meta_service->prepare_rowset(&cntl, req, &res, nullptr);
    if (!arena) {
        delete req;
    }
}

static void get_tablet_stats(MetaService* meta_service, int64_t table_id, int64_t index_id,
                             int64_t partition_id, int64_t tablet_id, GetTabletStatsResponse& res) {
    brpc::Controller cntl;
    GetTabletStatsRequest req;
    auto* idx = req.add_tablet_idx();
    idx->set_table_id(table_id);
    idx->set_index_id(index_id);
    idx->set_partition_id(partition_id);
    idx->set_tablet_id(tablet_id);
    meta_service->get_tablet_stats(&cntl, &req, &res, nullptr);
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

static void insert_rowset(MetaServiceProxy* meta_service, int64_t db_id, const std::string& label,
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

static void get_rowset(MetaServiceProxy* meta_service, int64_t table_id, int64_t index_id,
                       int64_t partition_id, int64_t tablet_id, GetRowsetResponse& res) {
    brpc::Controller cntl;
    GetRowsetRequest req;
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
    meta_service->get_rowset(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << tablet_id;
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

// create_tablets
TEST(RpcKvBvarTest, DISABLED_CreateTablets) {
    auto meta_service = get_meta_service();
    auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());
    constexpr auto table_id = 10021, index_id = 10022, partition_id = 10023, tablet_id = 10024;

    mem_kv->read_count_ = 0;
    mem_kv->write_count_ = 0;

    create_tablet(meta_service.get(), table_id, index_id, partition_id, tablet_id);
    LOG(INFO) << "CreateTablets: " << mem_kv->read_count_ << ", " << mem_kv->write_count_;
    ASSERT_EQ(mem_kv->read_count_, g_bvar_rpc_kv_create_tablets_read_counter.get_value());
    ASSERT_EQ(mem_kv->write_count_, g_bvar_rpc_kv_create_tablets_write_counter.get_value());
}

// get_tablet
TEST(RpcKvBvarTest, DISABLED_GetTablet) {
    std::string cloud_unique_id = "test_cloud_unique_id";
    auto meta_service = get_meta_service();
    auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());
    constexpr auto table_id = 10021, index_id = 10022, partition_id = 10023, tablet_id = 10024;
    create_tablet(meta_service.get(), table_id, index_id, partition_id, tablet_id);

    mem_kv->read_count_ = 0;
    mem_kv->write_count_ = 0;

    brpc::Controller cntl;
    GetTabletRequest req;
    req.set_cloud_unique_id(cloud_unique_id);
    req.set_tablet_id(tablet_id);
    GetTabletResponse resp;

    meta_service->get_tablet(&cntl, &req, &resp, nullptr);
    LOG(INFO) << "GetTablet: " << mem_kv->read_count_ << ", " << mem_kv->write_count_;
    ASSERT_EQ(mem_kv->read_count_, g_bvar_rpc_kv_get_tablet_read_counter.get_value());
    ASSERT_EQ(mem_kv->write_count_, g_bvar_rpc_kv_get_tablet_write_counter.get_value());
}

// get_tablet_stats
TEST(RpcKvBvarTest, DISABLED_GetTabletStats) {
    std::string cloud_unique_id = "test_cloud_unique_id";
    auto meta_service = get_meta_service();
    auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());
    constexpr auto table_id = 10021, index_id = 10022, partition_id = 10023, tablet_id = 10024;
    create_tablet(meta_service.get(), table_id, index_id, partition_id, tablet_id);

    mem_kv->read_count_ = 0;
    mem_kv->write_count_ = 0;

    GetTabletStatsResponse res;
    get_tablet_stats(meta_service.get(), table_id, index_id, partition_id, tablet_id, res);

    LOG(INFO) << "GetTabletStats: " << mem_kv->read_count_ << ", " << mem_kv->write_count_;
    ASSERT_EQ(mem_kv->read_count_, g_bvar_rpc_kv_get_tablet_stats_read_counter.get_value());
    ASSERT_EQ(mem_kv->write_count_, g_bvar_rpc_kv_get_tablet_stats_write_counter.get_value());
}

// update_tablet
TEST(RpcKvBvarTest, DISABLED_UpdateTablet) {
    auto meta_service = get_meta_service();
    auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());
    constexpr auto table_id = 10001, index_id = 10002, partition_id = 10003, tablet_id = 10004;
    create_tablet(meta_service.get(), table_id, index_id, partition_id, tablet_id);

    brpc::Controller cntl;
    UpdateTabletRequest req;
    UpdateTabletResponse resp;
    req.set_cloud_unique_id("test_cloud_unique_id");
    TabletMetaInfoPB* tablet_meta_info = req.add_tablet_meta_infos();
    tablet_meta_info->set_tablet_id(tablet_id);
    tablet_meta_info->set_is_in_memory(true);

    mem_kv->read_count_ = 0;
    mem_kv->write_count_ = 0;

    meta_service->update_tablet(&cntl, &req, &resp, nullptr);

    LOG(INFO) << "UpdateTablet: " << mem_kv->read_count_ << ", " << mem_kv->write_count_;
    ASSERT_EQ(mem_kv->read_count_, g_bvar_rpc_kv_update_tablet_read_counter.get_value());
    ASSERT_EQ(mem_kv->write_count_, g_bvar_rpc_kv_update_tablet_write_counter.get_value());
}

// update_tablet_schema
// should not call update_tablet_schema
// TEST(RpcKvBvarTest, DISABLED_UpdateTabletSchema) {
//     auto meta_service = get_meta_service();
//     auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());
//     constexpr auto table_id = 10001, index_id = 10002, partition_id = 10003, tablet_id = 10004;
//     create_tablet(meta_service.get(), table_id, index_id, partition_id, tablet_id);

//     brpc::Controller cntl;
//     UpdateTabletSchemaRequest req;
//     UpdateTabletSchemaResponse resp;
//     req.set_tablet_id(tablet_id);
//     req.set_cloud_unique_id("test_cloud_unique_id");

//     mem_kv->read_count_ = 0;
//     mem_kv->write_count_ = 0;

//     meta_service->update_tablet_schema(&cntl, &req, &resp, nullptr);

//     LOG(INFO) << "UpdateTabletSchema: " << mem_kv->read_count_ << ", "
//               << mem_kv->write_count_;
//     ASSERT_EQ(mem_kv->read_count_,
//               g_bvar_rpc_kv_update_tablet_schema_read_counter.get_value());
//     ASSERT_EQ(mem_kv->write_count_,
//               g_bvar_rpc_kv_update_tablet_schema_write_counter.get_value());
// }

// prepare_rowset
TEST(RpcKvBvarTest, DISABLED_PrepareRowset) {
    int64_t db_id = 100201;
    std::string label = "test_prepare_rowset";
    auto meta_service = get_meta_service();
    auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());
    constexpr auto table_id = 10021, index_id = 10022, partition_id = 10023, tablet_id = 10024;
    create_tablet(meta_service.get(), table_id, index_id, partition_id, tablet_id);

    int64_t txn_id = 0;
    ASSERT_NO_FATAL_FAILURE(begin_txn(meta_service.get(), db_id, label, table_id, txn_id));
    CreateRowsetResponse res;
    auto rowset = create_rowset(txn_id, tablet_id, partition_id);
    rowset.mutable_load_id()->set_hi(123);
    rowset.mutable_load_id()->set_lo(456);

    mem_kv->read_count_ = 0;
    mem_kv->write_count_ = 0;

    prepare_rowset(meta_service.get(), rowset, res);

    LOG(INFO) << "PrepareRowset: " << mem_kv->read_count_ << ", " << mem_kv->write_count_;
    ASSERT_EQ(mem_kv->read_count_, g_bvar_rpc_kv_prepare_rowset_read_counter.get_value());
    ASSERT_EQ(mem_kv->write_count_, g_bvar_rpc_kv_prepare_rowset_write_counter.get_value());
}

// get_rowset
TEST(RpcKvBvarTest, DISABLED_GetRowset) {
    auto meta_service = get_meta_service();
    auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());

    constexpr auto table_id = 10001, index_id = 10002, partition_id = 10003, tablet_id = 10004;
    config::write_schema_kv = true;
    ASSERT_NO_FATAL_FAILURE(create_tablet(meta_service.get(), table_id, index_id, partition_id,
                                          tablet_id, next_rowset_id(), 1));
    // check get tablet response
    check_get_tablet(meta_service.get(), tablet_id, 1);

    mem_kv->read_count_ = 0;
    mem_kv->write_count_ = 0;

    // check get rowset response
    GetRowsetResponse get_rowset_res;
    get_rowset(meta_service.get(), table_id, index_id, partition_id, tablet_id, get_rowset_res);

    LOG(INFO) << "GetRowset: " << mem_kv->read_count_ << ", " << mem_kv->write_count_;
    ASSERT_EQ(mem_kv->read_count_, g_bvar_rpc_kv_get_rowset_read_counter.get_value());
    ASSERT_EQ(mem_kv->write_count_, g_bvar_rpc_kv_get_rowset_write_counter.get_value());
}

// update_tmp_rowset
TEST(RpcKvBvarTest, DISABLED_UpdateTmpRowset) {
    auto meta_service = get_meta_service();
    auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());
    constexpr auto table_id = 10001, index_id = 10002, partition_id = 10003, tablet_id = 10004;
    int64_t txn_id = -1;
    create_tablet(meta_service.get(), table_id, index_id, partition_id, tablet_id);
    auto rowset = create_rowset(txn_id, tablet_id, partition_id);
    rowset.set_num_segments(rowset.num_segments() + 3);
    rowset.set_num_rows(rowset.num_rows() + 1000);
    rowset.set_total_disk_size(rowset.total_disk_size() + 11000);
    rowset.set_index_disk_size(rowset.index_disk_size() + 1000);
    rowset.set_data_disk_size(rowset.data_disk_size() + 10000);

    std::unique_ptr<Transaction> txn;
    std::string update_key;
    brpc::Controller cntl;
    CreateRowsetResponse res;
    auto* arena = res.GetArena();
    auto* req = google::protobuf::Arena::CreateMessage<CreateRowsetRequest>(arena);
    std::string instance_id = get_instance_id(meta_service->resource_mgr(), req->cloud_unique_id());
    MetaRowsetTmpKeyInfo key_info {instance_id, txn_id, tablet_id};
    meta_rowset_tmp_key(key_info, &update_key);
    EXPECT_EQ(mem_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->put(update_key, "update_tmp_rowset_val");
    EXPECT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    req->mutable_rowset_meta()->CopyFrom(rowset);

    mem_kv->read_count_ = 0;
    mem_kv->write_count_ = 0;

    meta_service->update_tmp_rowset(&cntl, req, &res, nullptr);

    if (!arena) {
        delete req;
    }

    LOG(INFO) << "UpdateTmpRowset: " << mem_kv->read_count_ << ", " << mem_kv->write_count_;
    ASSERT_EQ(mem_kv->read_count_, g_bvar_rpc_kv_update_tmp_rowset_read_counter.get_value());
    ASSERT_EQ(mem_kv->write_count_, g_bvar_rpc_kv_update_tmp_rowset_write_counter.get_value());
}

// commit_rowset
TEST(RpcKvBvarTest, DISABLED_CommitRowset) {
    auto meta_service = get_meta_service();
    auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());

    constexpr auto table_id = 10001, index_id = 10002, partition_id = 10003, tablet_id = 10004;
    int64_t txn_id = -1;
    create_tablet(meta_service.get(), table_id, index_id, partition_id, tablet_id);
    auto tmp_rowset = create_rowset(txn_id, tablet_id, partition_id);

    mem_kv->read_count_ = 0;
    mem_kv->write_count_ = 0;

    CreateRowsetResponse res;
    commit_rowset(meta_service.get(), tmp_rowset, res);

    LOG(INFO) << "CommitRowset: " << mem_kv->read_count_ << ", " << mem_kv->write_count_;
    ASSERT_EQ(mem_kv->read_count_, g_bvar_rpc_kv_commit_rowset_read_counter.get_value());
    ASSERT_EQ(mem_kv->write_count_, g_bvar_rpc_kv_commit_rowset_write_counter.get_value());
}

// get_version
TEST(RpcKvBvarTest, DISABLED_GetVersion) {
    auto meta_service = get_meta_service();
    auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());

    constexpr auto table_id = 10001, partition_id = 10003, tablet_id = 10004;
    create_tablet(meta_service.get(), table_id, 1, partition_id, tablet_id);
    insert_rowset(meta_service.get(), 1, "get_version_label_1", table_id, partition_id, tablet_id);

    brpc::Controller ctrl;
    GetVersionRequest req;
    req.set_cloud_unique_id("test_cloud_unique_id");
    req.set_db_id(1);
    req.set_table_id(table_id);
    req.set_partition_id(partition_id);

    mem_kv->read_count_ = 0;
    mem_kv->write_count_ = 0;

    GetVersionResponse resp;
    meta_service->get_version(&ctrl, &req, &resp, nullptr);

    LOG(INFO) << "GetVersion: " << mem_kv->read_count_ << ", " << mem_kv->write_count_;
    ASSERT_EQ(mem_kv->read_count_, g_bvar_rpc_kv_get_version_read_counter.get_value());
    ASSERT_EQ(mem_kv->write_count_, g_bvar_rpc_kv_get_version_write_counter.get_value());
}

// get_schema_dict
TEST(RpcKvBvarTest, DISABLED_GetSchemaDict) {
    auto meta_service = get_meta_service();
    auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());
    constexpr auto table_id = 10001, index_id = 10002, partition_id = 10003, tablet_id = 10004;
    create_tablet(meta_service.get(), table_id, index_id, partition_id, tablet_id);

    brpc::Controller ctrl;
    GetSchemaDictRequest req;
    req.set_cloud_unique_id("test_cloud_unique_id");
    req.set_index_id(index_id);

    std::unique_ptr<Transaction> txn;
    std::string instance_id = get_instance_id(meta_service->resource_mgr(), req.cloud_unique_id());
    std::string dict_key = meta_schema_pb_dictionary_key({instance_id, req.index_id()});
    EXPECT_EQ(mem_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->put(dict_key, "dict_val");
    EXPECT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    mem_kv->read_count_ = 0;
    mem_kv->write_count_ = 0;

    GetSchemaDictResponse resp;
    meta_service->get_schema_dict(&ctrl, &req, &resp, nullptr);

    LOG(INFO) << "GetSchemaDict: " << mem_kv->read_count_ << ", " << mem_kv->write_count_;
    ASSERT_EQ(mem_kv->read_count_, g_bvar_rpc_kv_get_schema_dict_read_counter.get_value());
    ASSERT_EQ(mem_kv->write_count_, g_bvar_rpc_kv_get_schema_dict_write_counter.get_value());
}

// get_delete_bitmap_update_lock
TEST(RpcKvBvarTest, DISABLED_GetDeleteBitmapUpdateLock) {
    auto meta_service = get_meta_service();
    auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());

    int64_t db_id = 99999;
    int64_t table_id = 1801;
    int64_t index_id = 4801;
    int64_t t1p1 = 2001;
    int64_t tablet_id = 3001;
    int64_t txn_id;
    ASSERT_NO_FATAL_FAILURE(create_tablet_with_db_id(meta_service.get(), db_id, table_id, index_id,
                                                     t1p1, tablet_id));
    begin_txn_and_commit_rowset(meta_service.get(), "label11", db_id, table_id, t1p1, tablet_id,
                                &txn_id);
    int64_t lock_id = -2;
    int64_t initiator = 1009;

    mem_kv->read_count_ = 0;
    mem_kv->write_count_ = 0;

    get_delete_bitmap_update_lock(meta_service.get(), table_id, t1p1, lock_id, initiator);

    LOG(INFO) << "GetDeleteBitmapUpdateLock: " << mem_kv->read_count_ << ", "
              << mem_kv->write_count_;
    ASSERT_EQ(mem_kv->read_count_,
              g_bvar_rpc_kv_get_delete_bitmap_update_lock_read_counter.get_value());
    ASSERT_EQ(mem_kv->write_count_,
              g_bvar_rpc_kv_get_delete_bitmap_update_lock_write_counter.get_value());
}

// update_delete_bitmap
TEST(RpcKvBvarTest, DISABLED_UpdateDeleteBitmap) {
    auto meta_service = get_meta_service();
    auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());

    int64_t db_id = 99999;
    int64_t table_id = 1801;
    int64_t index_id = 4801;
    int64_t t1p1 = 2001;
    int64_t tablet_id = 3001;
    int64_t txn_id;
    size_t split_size = 90 * 1000; // see cloud/src/common/util.h
    ASSERT_NO_FATAL_FAILURE(create_tablet_with_db_id(meta_service.get(), db_id, table_id, index_id,
                                                     t1p1, tablet_id));
    begin_txn_and_commit_rowset(meta_service.get(), "label11", db_id, table_id, t1p1, tablet_id,
                                &txn_id);
    int64_t lock_id = -2;
    int64_t initiator = 1009;
    int64_t version = 100;
    get_delete_bitmap_update_lock(meta_service.get(), table_id, t1p1, lock_id, initiator);
    UpdateDeleteBitmapRequest update_delete_bitmap_req;
    UpdateDeleteBitmapResponse update_delete_bitmap_res;
    // will be splited and stored in 5 KVs
    std::string data1(split_size * 5, 'c');

    mem_kv->read_count_ = 0;
    mem_kv->write_count_ = 0;

    update_delete_bitmap(meta_service.get(), update_delete_bitmap_req, update_delete_bitmap_res,
                         table_id, t1p1, lock_id, initiator, tablet_id, txn_id, version, data1);

    LOG(INFO) << "UpdateDeleteBitmap: " << mem_kv->read_count_ << ", " << mem_kv->write_count_;
    ASSERT_EQ(mem_kv->read_count_, g_bvar_rpc_kv_update_delete_bitmap_read_counter.get_value());
    ASSERT_EQ(mem_kv->write_count_, g_bvar_rpc_kv_update_delete_bitmap_write_counter.get_value());
}

// get_delete_bitmap
TEST(RpcKvBvarTest, DISABLED_GetDeleteBitmap) {
    auto meta_service = get_meta_service();
    auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());
    int64_t db_id = 99999;
    int64_t table_id = 1801;
    int64_t index_id = 4801;
    int64_t t1p1 = 2001;
    int64_t tablet_id = 3001;
    int64_t txn_id;
    size_t split_size = 90 * 1000; // see cloud/src/common/util.h
    ASSERT_NO_FATAL_FAILURE(create_tablet_with_db_id(meta_service.get(), db_id, table_id, index_id,
                                                     t1p1, tablet_id));
    begin_txn_and_commit_rowset(meta_service.get(), "label11", db_id, table_id, t1p1, tablet_id,
                                &txn_id);
    int64_t lock_id = -2;
    int64_t initiator = 1009;
    int64_t version = 100;
    get_delete_bitmap_update_lock(meta_service.get(), table_id, t1p1, lock_id, initiator);
    UpdateDeleteBitmapRequest update_delete_bitmap_req;
    UpdateDeleteBitmapResponse update_delete_bitmap_res;
    // will be splited and stored in 5 KVs
    std::string data1(split_size * 5, 'c');
    update_delete_bitmap(meta_service.get(), update_delete_bitmap_req, update_delete_bitmap_res,
                         table_id, t1p1, lock_id, initiator, tablet_id, txn_id, version, data1);

    brpc::Controller ctrl;
    GetDeleteBitmapRequest get_delete_bitmap_req;
    GetDeleteBitmapResponse get_delete_bitmap_res;
    get_delete_bitmap_req.set_cloud_unique_id("test_cloud_unique_id");
    get_delete_bitmap_req.set_tablet_id(tablet_id);
    get_delete_bitmap_req.add_rowset_ids("123");
    get_delete_bitmap_req.add_begin_versions(0);
    get_delete_bitmap_req.add_end_versions(version);

    mem_kv->read_count_ = 0;
    mem_kv->write_count_ = 0;

    meta_service->get_delete_bitmap(reinterpret_cast<google::protobuf::RpcController*>(&ctrl),
                                    &get_delete_bitmap_req, &get_delete_bitmap_res, nullptr);

    LOG(INFO) << "GetDeleteBitmap: " << mem_kv->read_count_ << ", " << mem_kv->write_count_;
    ASSERT_EQ(mem_kv->read_count_, g_bvar_rpc_kv_get_delete_bitmap_read_counter.get_value());
    ASSERT_EQ(mem_kv->write_count_, g_bvar_rpc_kv_get_delete_bitmap_write_counter.get_value());
}

// remove_delete_bitmap_update_lock
TEST(RpcKvBvarTest, DISABLED_RemoveDeleteBitmapUpdateLock) {
    auto meta_service = get_meta_service();
    auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());

    int64_t db_id = 99999;
    int64_t table_id = 1801;
    int64_t index_id = 4801;
    int64_t t1p1 = 2001;
    int64_t tablet_id = 3001;
    int64_t txn_id;
    ASSERT_NO_FATAL_FAILURE(create_tablet_with_db_id(meta_service.get(), db_id, table_id, index_id,
                                                     t1p1, tablet_id));
    begin_txn_and_commit_rowset(meta_service.get(), "label11", db_id, table_id, t1p1, tablet_id,
                                &txn_id);
    int64_t lock_id = -2;
    int64_t initiator = 1009;

    get_delete_bitmap_update_lock(meta_service.get(), table_id, t1p1, lock_id, initiator);
    brpc::Controller cntl;
    RemoveDeleteBitmapUpdateLockRequest remove_req;
    RemoveDeleteBitmapUpdateLockResponse remove_res;

    remove_req.set_cloud_unique_id("test_cloud_unique_id");
    remove_req.set_table_id(table_id);
    remove_req.set_lock_id(lock_id);
    remove_req.set_initiator(initiator);

    mem_kv->read_count_ = 0;
    mem_kv->write_count_ = 0;

    meta_service->remove_delete_bitmap_update_lock(
            reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &remove_req, &remove_res,
            nullptr);

    LOG(INFO) << "RemoveDeleteBitmapUpdateLock: " << mem_kv->read_count_ << ", "
              << mem_kv->write_count_;
    ASSERT_EQ(mem_kv->read_count_,
              g_bvar_rpc_kv_remove_delete_bitmap_update_lock_read_counter.get_value());
    ASSERT_EQ(mem_kv->write_count_,
              g_bvar_rpc_kv_remove_delete_bitmap_update_lock_write_counter.get_value());
}

// remove_delete_bitmap
TEST(RpcKvBvarTest, DISABLED_RemoveDeleteBitmap) {
    auto meta_service = get_meta_service();
    auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());
    int64_t db_id = 99999;
    int64_t table_id = 1801;
    int64_t index_id = 4801;
    int64_t t1p1 = 2001;
    int64_t tablet_id = 3001;
    int64_t txn_id;
    size_t split_size = 90 * 1000; // see cloud/src/common/util.h
    ASSERT_NO_FATAL_FAILURE(create_tablet_with_db_id(meta_service.get(), db_id, table_id, index_id,
                                                     t1p1, tablet_id));
    begin_txn_and_commit_rowset(meta_service.get(), "label1", db_id, table_id, t1p1, tablet_id,
                                &txn_id);
    int64_t lock_id = -2;
    int64_t initiator = 1009;
    get_delete_bitmap_update_lock(meta_service.get(), table_id, t1p1, lock_id, initiator);
    int64_t version = 100;
    UpdateDeleteBitmapRequest update_delete_bitmap_req;
    UpdateDeleteBitmapResponse update_delete_bitmap_res;
    // will be splited and stored in 5 KVs
    std::string data1(split_size * 5, 'c');
    update_delete_bitmap(meta_service.get(), update_delete_bitmap_req, update_delete_bitmap_res,
                         table_id, t1p1, lock_id, initiator, tablet_id, txn_id, version, data1);

    brpc::Controller ctrl;
    RemoveDeleteBitmapRequest req;
    RemoveDeleteBitmapResponse resp;
    req.add_begin_versions(version);
    req.add_end_versions(version);
    req.add_rowset_ids("rowset_ids");
    req.set_cloud_unique_id("test_cloud_unique_id");

    mem_kv->read_count_ = 0;
    mem_kv->write_count_ = 0;

    meta_service->remove_delete_bitmap(&ctrl, &req, &resp, nullptr);

    LOG(INFO) << "RemoveDeleteBitmap: " << mem_kv->read_count_ << ", " << mem_kv->write_count_;
    ASSERT_EQ(mem_kv->read_count_, g_bvar_rpc_kv_remove_delete_bitmap_read_counter.get_value());
    ASSERT_EQ(mem_kv->write_count_, g_bvar_rpc_kv_remove_delete_bitmap_write_counter.get_value());
}

// start_tablet_job
TEST(RpcKvBvarTest, DISABLED_StartTabletJob) {
    auto meta_service = get_meta_service();
    auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());
    constexpr int64_t table_id = 10001;
    constexpr int64_t index_id = 10002;
    constexpr int64_t partition_id = 10003;
    constexpr int64_t tablet_id = 10004;
    create_tablet(meta_service.get(), table_id, index_id, partition_id, tablet_id);
    StartTabletJobResponse res;

    mem_kv->read_count_ = 0;
    mem_kv->write_count_ = 0;

    start_compaction_job(meta_service.get(), tablet_id, "compaction1", "ip:port", 0, 0,
                         TabletCompactionJobPB::BASE, res);

    LOG(INFO) << "StartTabletJob: " << mem_kv->read_count_ << ", " << mem_kv->write_count_;
    ASSERT_EQ(mem_kv->read_count_, g_bvar_rpc_kv_start_tablet_job_read_counter.get_value());
    ASSERT_EQ(mem_kv->write_count_, g_bvar_rpc_kv_start_tablet_job_write_counter.get_value());
}

// finish_tablet_job

// prepare_index
// commit_index
// drop_index
// prepare_partition
// commit_partition
// drop_partition
// check_kv
// get_obj_store_info
// alter_storage_vault
// alter_obj_store_info
// update_ak_sk
// create_instance
// get_instance
// alter_cluster
// get_cluster
// create_stage
// get_stage
// get_iam
// alter_iam
// alter_ram_user
// begin_copy
// finish_copy
// get_copy_job
// get_copy_files
// filter_copy_files
// get_cluster_status
// begin_txn
// precommit_txn
// get_rl_task_commit_attach
// reset_rl_progress
// commit_txn
// abort_txn
// get_txn
// get_current_max_txn_id
// begin_sub_txn
// abort_sub_txn
// abort_txn_with_coordinator
// check_txn_conflict
// clean_txn_label
// get_txn_id

} // namespace doris::cloud
