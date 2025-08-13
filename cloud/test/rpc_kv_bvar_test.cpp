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
#include <cstdint>
#include <memory>

#include "common/config.h"
#include "common/logging.h"
#include "common/util.h"
#include "cpp/sync_point.h"
#include "meta-service/meta_service_helper.h"
#include "meta-store/keys.h"
#include "meta-store/mem_txn_kv.h"
#include "meta-store/txn_kv.h"
#include "meta-store/txn_kv_error.h"
#include "mock_resource_manager.h"
#include "rate-limiter/rate_limiter.h"

int main(int argc, char** argv) {
    const std::string conf_file = "doris_cloud.conf";
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

std::unique_ptr<MetaServiceProxy> get_meta_service() {
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

void create_tablet(MetaService* meta_service, int64_t table_id, int64_t index_id,
                   int64_t partition_id, int64_t tablet_id, bool enable_mow,
                   bool not_ready = false) {
    brpc::Controller cntl;
    CreateTabletsRequest req;
    CreateTabletsResponse res;
    auto* tablet = req.add_tablet_metas();
    tablet->set_tablet_state(not_ready ? doris::TabletStatePB::PB_NOTREADY
                                       : doris::TabletStatePB::PB_RUNNING);
    tablet->set_table_id(table_id);
    tablet->set_index_id(index_id);
    tablet->set_partition_id(partition_id);
    tablet->set_tablet_id(tablet_id);
    tablet->set_enable_unique_key_merge_on_write(enable_mow);
    auto* schema = tablet->mutable_schema();
    schema->set_schema_version(0);
    auto* first_rowset = tablet->add_rs_metas();
    first_rowset->set_rowset_id(0); // required
    first_rowset->set_rowset_id_v2(next_rowset_id());
    first_rowset->set_start_version(0);
    first_rowset->set_end_version(1);
    first_rowset->mutable_tablet_schema()->CopyFrom(*schema);
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

static doris::RowsetMetaCloudPB create_rowset(int64_t txn_id, int64_t tablet_id, int index_id,
                                              int partition_id, int64_t version = -1,
                                              int num_rows = 100) {
    doris::RowsetMetaCloudPB rowset;
    rowset.set_rowset_id(0); // required
    rowset.set_rowset_id_v2(next_rowset_id());
    rowset.set_tablet_id(tablet_id);
    rowset.set_partition_id(partition_id);
    rowset.set_index_id(index_id);
    rowset.set_txn_id(txn_id);
    if (version > 0) {
        rowset.set_start_version(version);
        rowset.set_end_version(version);
    }
    rowset.set_num_segments(0);
    rowset.set_num_rows(0);
    rowset.set_data_disk_size(0);
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

static void create_and_commit_rowset(MetaServiceProxy* meta_service, int64_t table_id,
                                     int64_t index_id, int64_t partition_id, int64_t tablet_id,
                                     int64_t txn_id) {
    create_tablet(meta_service, table_id, index_id, partition_id, tablet_id);
    auto tmp_rowset = create_rowset(txn_id, tablet_id, partition_id);
    CreateRowsetResponse res;
    commit_rowset(meta_service, tmp_rowset, res);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
}

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

void start_schema_change_job(MetaServiceProxy* meta_service, int64_t table_id, int64_t index_id,
                             int64_t partition_id, int64_t tablet_id, int64_t new_tablet_id,
                             const std::string& job_id, const std::string& initiator,
                             StartTabletJobResponse& res, int64_t alter_version = -1) {
    brpc::Controller cntl;
    StartTabletJobRequest req;
    req.mutable_job()->mutable_idx()->set_tablet_id(tablet_id);
    auto* sc = req.mutable_job()->mutable_schema_change();
    sc->set_id(job_id);
    sc->set_initiator(initiator);
    sc->mutable_new_tablet_idx()->set_tablet_id(new_tablet_id);
    if (alter_version != -1) {
        sc->set_alter_version(alter_version);
    }
    long now = time(nullptr);
    sc->set_expiration(now + 12);
    meta_service->start_tablet_job(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK)
            << job_id << ' ' << initiator << ' ' << res.status().msg();
};

void finish_schema_change_job(
        MetaService* meta_service, int64_t tablet_id, int64_t new_tablet_id,
        const std::string& job_id, const std::string& initiator,
        const std::vector<doris::RowsetMetaCloudPB>& output_rowsets, FinishTabletJobResponse& res,
        FinishTabletJobRequest_Action action = FinishTabletJobRequest::COMMIT) {
    brpc::Controller cntl;
    FinishTabletJobRequest req;
    req.set_action(action);
    req.mutable_job()->mutable_idx()->set_tablet_id(tablet_id);
    auto* sc = req.mutable_job()->mutable_schema_change();
    sc->mutable_new_tablet_idx()->set_tablet_id(new_tablet_id);
    if (output_rowsets.empty()) {
        sc->set_alter_version(0);
    } else {
        sc->set_alter_version(output_rowsets.back().end_version());
        for (const auto& rowset : output_rowsets) {
            sc->add_txn_ids(rowset.txn_id());
            sc->add_output_versions(rowset.end_version());
            sc->set_num_output_rows(sc->num_output_rows() + rowset.num_rows());
            sc->set_num_output_segments(sc->num_output_segments() + rowset.num_segments());
            sc->set_size_output_rowsets(sc->size_output_rowsets() + rowset.total_disk_size());
            sc->set_index_size_output_rowsets(sc->index_size_output_rowsets() +
                                              rowset.index_disk_size());
            sc->set_segment_size_output_rowsets(sc->segment_size_output_rowsets() +
                                                rowset.data_disk_size());
        }
        sc->set_num_output_rowsets(output_rowsets.size());
    }
    sc->set_id(job_id);
    sc->set_initiator(initiator);
    sc->set_delete_bitmap_lock_initiator(12345);
    meta_service->finish_tablet_job(&cntl, &req, &res, nullptr);
}

void clear_memkv_count_bytes(MemTxnKv* memkv) {
    memkv->get_count_ = memkv->put_count_ = memkv->del_count_ = 0;
    memkv->get_bytes_ = memkv->put_bytes_ = memkv->del_bytes_ = 0;
}

// create_tablets
TEST(RpcKvBvarTest, CreateTablets) {
    auto meta_service = get_meta_service();
    auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());
    constexpr auto table_id = 10021, index_id = 10022, partition_id = 10023, tablet_id = 10024;

    clear_memkv_count_bytes(mem_kv.get());

    create_tablet(meta_service.get(), table_id, index_id, partition_id, tablet_id);
    LOG(INFO) << "CreateTablets: " << mem_kv->get_count_ << ", " << mem_kv->put_count_ << ", "
              << mem_kv->del_count_ << ", " << mem_kv->get_bytes_ << ", " << mem_kv->put_bytes_
              << ", " << mem_kv->del_bytes_;
    ASSERT_EQ(mem_kv->get_bytes_, g_bvar_rpc_kv_create_tablets_get_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->put_bytes_, g_bvar_rpc_kv_create_tablets_put_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->get_count_, g_bvar_rpc_kv_create_tablets_get_counter.get({mock_instance}));
    ASSERT_EQ(mem_kv->put_count_, g_bvar_rpc_kv_create_tablets_put_counter.get({mock_instance}));
}

// get_tablet
TEST(RpcKvBvarTest, GetTablet) {
    std::string cloud_unique_id = "test_cloud_unique_id";
    auto meta_service = get_meta_service();
    auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());
    constexpr auto table_id = 10021, index_id = 10022, partition_id = 10023, tablet_id = 10024;
    create_tablet(meta_service.get(), table_id, index_id, partition_id, tablet_id);

    clear_memkv_count_bytes(mem_kv.get());

    brpc::Controller cntl;
    GetTabletRequest req;
    req.set_cloud_unique_id(cloud_unique_id);
    req.set_tablet_id(tablet_id);
    GetTabletResponse resp;

    meta_service->get_tablet(&cntl, &req, &resp, nullptr);
    LOG(INFO) << "GetTablet: " << mem_kv->get_count_ << ", " << mem_kv->put_count_ << ", "
              << mem_kv->del_count_ << ", " << mem_kv->get_bytes_ << ", " << mem_kv->put_bytes_
              << ", " << mem_kv->del_bytes_;
    ASSERT_EQ(mem_kv->get_bytes_, g_bvar_rpc_kv_get_tablet_get_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->get_count_, g_bvar_rpc_kv_get_tablet_get_counter.get({mock_instance}));
}

// get_tablet_stats
TEST(RpcKvBvarTest, GetTabletStats) {
    std::string cloud_unique_id = "test_cloud_unique_id";
    auto meta_service = get_meta_service();
    auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());
    constexpr auto table_id = 10021, index_id = 10022, partition_id = 10023, tablet_id = 10024;
    create_tablet(meta_service.get(), table_id, index_id, partition_id, tablet_id);

    clear_memkv_count_bytes(mem_kv.get());

    GetTabletStatsResponse res;
    get_tablet_stats(meta_service.get(), table_id, index_id, partition_id, tablet_id, res);

    LOG(INFO) << "GetTabletStats: " << mem_kv->get_count_ << ", " << mem_kv->put_count_ << ", "
              << mem_kv->del_count_ << ", " << mem_kv->get_bytes_ << ", " << mem_kv->put_bytes_
              << ", " << mem_kv->del_bytes_;

    ASSERT_EQ(mem_kv->get_bytes_, g_bvar_rpc_kv_get_tablet_stats_get_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->get_count_, g_bvar_rpc_kv_get_tablet_stats_get_counter.get({mock_instance}));
}

// update_tablet
TEST(RpcKvBvarTest, UpdateTablet) {
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

    clear_memkv_count_bytes(mem_kv.get());

    meta_service->update_tablet(&cntl, &req, &resp, nullptr);

    LOG(INFO) << "UpdateTablet: " << mem_kv->get_count_ << ", " << mem_kv->put_count_ << ", "
              << mem_kv->del_count_ << ", " << mem_kv->get_bytes_ << ", " << mem_kv->put_bytes_
              << ", " << mem_kv->del_bytes_;

    ASSERT_EQ(mem_kv->get_bytes_, g_bvar_rpc_kv_update_tablet_get_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->put_bytes_, g_bvar_rpc_kv_update_tablet_put_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->get_count_, g_bvar_rpc_kv_update_tablet_get_counter.get({mock_instance}));
    ASSERT_EQ(mem_kv->put_count_, g_bvar_rpc_kv_update_tablet_put_counter.get({mock_instance}));
}

// update_tablet_schema
// should not call update_tablet_schema
// TEST(RpcKvBvarTest, UpdateTabletSchema) {
//     auto meta_service = get_meta_service();
//     auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());
//     constexpr auto table_id = 10001, index_id = 10002, partition_id = 10003, tablet_id = 10004;
//     create_tablet(meta_service.get(), table_id, index_id, partition_id, tablet_id);

//     brpc::Controller cntl;
//     UpdateTabletSchemaRequest req;
//     UpdateTabletSchemaResponse resp;
//     req.set_tablet_id(tablet_id);
//     req.set_cloud_unique_id("test_cloud_unique_id");

//     mem_kv->get_count_ = 0;
//     mem_kv->put_count_ = 0;
//     mem_kv->del_count_ = 0;

//     meta_service->update_tablet_schema(&cntl, &req, &resp, nullptr);

//     LOG(INFO) << "UpdateTabletSchema: " << mem_kv->get_count_ << ", "
//               << mem_kv->put_count_ << ", " << mem_kv->del_count_;
//     ASSERT_EQ(mem_kv->get_count_,
//               g_bvar_rpc_kv_update_tablet_schema_get_counter.get({mock_instance}));
//     ASSERT_EQ(mem_kv->put_count_,
//               g_bvar_rpc_kv_update_tablet_schema_put_counter.get({mock_instance}));
//     ASSERT_EQ(mem_kv->del_count_,
//               g_bvar_rpc_kv_update_tablet_schema_del_counter.get({mock_instance}));
// }

// begin_txn
TEST(RpcKvBvarTest, BeginTxn) {
    auto meta_service = get_meta_service();
    auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());
    int64_t db_id = 100201;
    std::string label = "test_prepare_rowset";
    constexpr auto table_id = 10021, index_id = 10022, partition_id = 10023, tablet_id = 10024;
    create_tablet(meta_service.get(), table_id, index_id, partition_id, tablet_id);

    int64_t txn_id = 0;

    clear_memkv_count_bytes(mem_kv.get());

    ASSERT_NO_FATAL_FAILURE(begin_txn(meta_service.get(), db_id, label, table_id, txn_id));

    LOG(INFO) << "BeginTxn: " << mem_kv->get_count_ << ", " << mem_kv->put_count_ << ", "
              << mem_kv->del_count_ << ", " << mem_kv->get_bytes_ << ", " << mem_kv->put_bytes_
              << ", " << mem_kv->del_bytes_;

    ASSERT_EQ(mem_kv->get_bytes_, g_bvar_rpc_kv_begin_txn_get_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->put_bytes_, g_bvar_rpc_kv_begin_txn_put_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->get_count_, g_bvar_rpc_kv_begin_txn_get_counter.get({mock_instance}));
    ASSERT_EQ(mem_kv->put_count_, g_bvar_rpc_kv_begin_txn_put_counter.get({mock_instance}));
}

// commit_txn
TEST(RpcKvBvarTest, CommitTxn) {
    auto meta_service = get_meta_service();
    auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());
    int64_t db_id = 100201;
    std::string label = "test_prepare_rowset";
    constexpr auto table_id = 10021, index_id = 10022, partition_id = 10023, tablet_id = 10024;
    create_tablet(meta_service.get(), table_id, index_id, partition_id, tablet_id);
    int64_t txn_id = 0;
    ASSERT_NO_FATAL_FAILURE(begin_txn(meta_service.get(), db_id, label, table_id, txn_id));

    clear_memkv_count_bytes(mem_kv.get());

    commit_txn(meta_service.get(), db_id, txn_id, label);

    LOG(INFO) << "CommitTxn: " << mem_kv->get_count_ << ", " << mem_kv->put_count_ << ", "
              << mem_kv->del_count_ << ", " << mem_kv->get_bytes_ << ", " << mem_kv->put_bytes_
              << ", " << mem_kv->del_bytes_;

    ASSERT_EQ(mem_kv->get_bytes_, g_bvar_rpc_kv_commit_txn_get_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->put_bytes_, g_bvar_rpc_kv_commit_txn_put_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->del_bytes_, g_bvar_rpc_kv_commit_txn_del_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->get_count_, g_bvar_rpc_kv_commit_txn_get_counter.get({mock_instance}));
    ASSERT_EQ(mem_kv->put_count_, g_bvar_rpc_kv_commit_txn_put_counter.get({mock_instance}));
    ASSERT_EQ(mem_kv->del_count_, g_bvar_rpc_kv_commit_txn_del_counter.get({mock_instance}));
}

// precommit_txn
TEST(RpcKvBvarTest, PrecommitTxn) {
    auto meta_service = get_meta_service();
    auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());
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

    clear_memkv_count_bytes(mem_kv.get());

    meta_service->precommit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

    LOG(INFO) << "PrecommitTxn: " << mem_kv->get_count_ << ", " << mem_kv->put_count_ << ", "
              << mem_kv->del_count_ << ", " << mem_kv->get_bytes_ << ", " << mem_kv->put_bytes_
              << ", " << mem_kv->del_bytes_;

    ASSERT_EQ(mem_kv->get_bytes_, g_bvar_rpc_kv_precommit_txn_get_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->put_bytes_, g_bvar_rpc_kv_precommit_txn_put_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->get_count_, g_bvar_rpc_kv_precommit_txn_get_counter.get({mock_instance}));
    ASSERT_EQ(mem_kv->put_count_, g_bvar_rpc_kv_precommit_txn_put_counter.get({mock_instance}));
}

// abort_txn
TEST(RpcKvBvarTest, AbortTxn) {
    auto meta_service = get_meta_service();
    auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());
    int64_t db_id = 100201;
    std::string label = "test_prepare_rowset";
    constexpr auto table_id = 10021, index_id = 10022, partition_id = 10023, tablet_id = 10024;
    create_tablet(meta_service.get(), table_id, index_id, partition_id, tablet_id);
    int64_t txn_id = 0;
    ASSERT_NO_FATAL_FAILURE(begin_txn(meta_service.get(), db_id, label, table_id, txn_id));

    clear_memkv_count_bytes(mem_kv.get());

    brpc::Controller cntl;
    AbortTxnRequest req;
    req.set_cloud_unique_id("test_cloud_unique_id");
    req.set_txn_id(txn_id);
    req.set_reason("test");
    AbortTxnResponse res;
    meta_service->abort_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res,
                            nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

    LOG(INFO) << "AbortTxn: " << mem_kv->get_count_ << ", " << mem_kv->put_count_ << ", "
              << mem_kv->del_count_ << ", " << mem_kv->get_bytes_ << ", " << mem_kv->put_bytes_
              << ", " << mem_kv->del_bytes_;

    ASSERT_EQ(mem_kv->get_bytes_, g_bvar_rpc_kv_abort_txn_get_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->put_bytes_, g_bvar_rpc_kv_abort_txn_put_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->del_bytes_, g_bvar_rpc_kv_abort_txn_del_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->get_count_, g_bvar_rpc_kv_abort_txn_get_counter.get({mock_instance}));
    ASSERT_EQ(mem_kv->put_count_, g_bvar_rpc_kv_abort_txn_put_counter.get({mock_instance}));
    ASSERT_EQ(mem_kv->del_count_, g_bvar_rpc_kv_abort_txn_del_counter.get({mock_instance}));
}

// get_txn
TEST(RpcKvBvarTest, GetTxn) {
    auto meta_service = get_meta_service();
    auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());
    int64_t db_id = 100201;
    std::string label = "test_prepare_rowset";
    constexpr auto table_id = 10021, index_id = 10022, partition_id = 10023, tablet_id = 10024;
    create_tablet(meta_service.get(), table_id, index_id, partition_id, tablet_id);
    int64_t txn_id = 0;
    ASSERT_NO_FATAL_FAILURE(begin_txn(meta_service.get(), db_id, label, table_id, txn_id));

    clear_memkv_count_bytes(mem_kv.get());

    brpc::Controller cntl;
    GetTxnRequest req;
    req.set_cloud_unique_id("test_cloud_unique_id");
    req.set_txn_id(txn_id);
    req.set_db_id(db_id);
    GetTxnResponse res;
    meta_service->get_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res,
                          nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

    LOG(INFO) << "GetTxn: " << mem_kv->get_count_ << ", " << mem_kv->put_count_ << ", "
              << mem_kv->del_count_ << ", " << mem_kv->get_bytes_ << ", " << mem_kv->put_bytes_
              << ", " << mem_kv->del_bytes_;

    ASSERT_EQ(mem_kv->get_bytes_, g_bvar_rpc_kv_get_txn_get_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->get_count_, g_bvar_rpc_kv_get_txn_get_counter.get({mock_instance}));
}

// get_txn_id
TEST(RpcKvBvarTest, GetTxnId) {
    auto meta_service = get_meta_service();
    auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());
    int64_t db_id = 100201;
    std::string label = "test_prepare_rowset";
    constexpr auto table_id = 10021, index_id = 10022, partition_id = 10023, tablet_id = 10024;
    create_tablet(meta_service.get(), table_id, index_id, partition_id, tablet_id);

    int64_t txn_id = 0;

    ASSERT_NO_FATAL_FAILURE(begin_txn(meta_service.get(), db_id, label, table_id, txn_id));

    brpc::Controller cntl;
    GetTxnIdRequest req;
    GetTxnIdResponse res;
    req.set_cloud_unique_id("test_cloud_unique_id");
    req.set_label(label);
    req.set_db_id(db_id);

    clear_memkv_count_bytes(mem_kv.get());

    meta_service->get_txn_id(&cntl, &req, &res, nullptr);

    LOG(INFO) << "GetTxnId: " << mem_kv->get_count_ << ", " << mem_kv->put_count_ << ", "
              << mem_kv->del_count_ << ", " << mem_kv->get_bytes_ << ", " << mem_kv->put_bytes_
              << ", " << mem_kv->del_bytes_;

    ASSERT_EQ(mem_kv->get_bytes_, g_bvar_rpc_kv_get_txn_id_get_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->get_count_, g_bvar_rpc_kv_get_txn_id_get_counter.get({mock_instance}));
}

// prepare_rowset
TEST(RpcKvBvarTest, PrepareRowset) {
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

    clear_memkv_count_bytes(mem_kv.get());

    prepare_rowset(meta_service.get(), rowset, res);

    LOG(INFO) << "PrepareRowset: " << mem_kv->get_count_ << ", " << mem_kv->put_count_ << ", "
              << mem_kv->del_count_ << ", " << mem_kv->get_bytes_ << ", " << mem_kv->put_bytes_
              << ", " << mem_kv->del_bytes_;

    ASSERT_EQ(mem_kv->get_bytes_, g_bvar_rpc_kv_prepare_rowset_get_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->put_bytes_, g_bvar_rpc_kv_prepare_rowset_put_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->get_count_, g_bvar_rpc_kv_prepare_rowset_get_counter.get({mock_instance}));
    ASSERT_EQ(mem_kv->put_count_, g_bvar_rpc_kv_prepare_rowset_put_counter.get({mock_instance}));
}

// get_rowset
TEST(RpcKvBvarTest, GetRowset) {
    auto meta_service = get_meta_service();
    auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());

    constexpr auto table_id = 10001, index_id = 10002, partition_id = 10003, tablet_id = 10004;
    config::write_schema_kv = true;
    ASSERT_NO_FATAL_FAILURE(create_tablet(meta_service.get(), table_id, index_id, partition_id,
                                          tablet_id, next_rowset_id(), 1));
    // check get tablet response
    check_get_tablet(meta_service.get(), tablet_id, 1);

    clear_memkv_count_bytes(mem_kv.get());

    // check get rowset response
    GetRowsetResponse get_rowset_res;
    get_rowset(meta_service.get(), table_id, index_id, partition_id, tablet_id, get_rowset_res);

    LOG(INFO) << "GetRowset: " << mem_kv->get_count_ << ", " << mem_kv->put_count_ << ", "
              << mem_kv->del_count_ << ", " << mem_kv->get_bytes_ << ", " << mem_kv->put_bytes_
              << ", " << mem_kv->del_bytes_;

    ASSERT_EQ(mem_kv->get_bytes_, g_bvar_rpc_kv_get_rowset_get_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->get_count_, g_bvar_rpc_kv_get_rowset_get_counter.get({mock_instance}));
}

// update_tmp_rowset
TEST(RpcKvBvarTest, UpdateTmpRowset) {
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

    clear_memkv_count_bytes(mem_kv.get());
    meta_service->update_tmp_rowset(&cntl, req, &res, nullptr);

    if (!arena) {
        delete req;
    }

    LOG(INFO) << "UpdateTmpRowset: " << mem_kv->get_count_ << ", " << mem_kv->put_count_ << ", "
              << mem_kv->del_count_ << ", " << mem_kv->get_bytes_ << ", " << mem_kv->put_bytes_
              << ", " << mem_kv->del_bytes_;

    ASSERT_EQ(mem_kv->get_bytes_, g_bvar_rpc_kv_update_tmp_rowset_get_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->put_bytes_, g_bvar_rpc_kv_update_tmp_rowset_put_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->get_count_, g_bvar_rpc_kv_update_tmp_rowset_get_counter.get({mock_instance}));
    ASSERT_EQ(mem_kv->put_count_, g_bvar_rpc_kv_update_tmp_rowset_put_counter.get({mock_instance}));
}

// commit_rowset
TEST(RpcKvBvarTest, CommitRowset) {
    auto meta_service = get_meta_service();
    auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());

    constexpr auto table_id = 10001, index_id = 10002, partition_id = 10003, tablet_id = 10004;
    int64_t txn_id = -1;
    create_tablet(meta_service.get(), table_id, index_id, partition_id, tablet_id);
    auto tmp_rowset = create_rowset(txn_id, tablet_id, partition_id);

    clear_memkv_count_bytes(mem_kv.get());

    CreateRowsetResponse res;
    commit_rowset(meta_service.get(), tmp_rowset, res);

    LOG(INFO) << "CommitRowset: " << mem_kv->get_count_ << ", " << mem_kv->put_count_ << ", "
              << mem_kv->del_count_ << ", " << mem_kv->get_bytes_ << ", " << mem_kv->put_bytes_
              << ", " << mem_kv->del_bytes_;

    ASSERT_EQ(mem_kv->get_bytes_, g_bvar_rpc_kv_commit_rowset_get_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->put_bytes_, g_bvar_rpc_kv_commit_rowset_put_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->del_bytes_, g_bvar_rpc_kv_commit_rowset_del_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->get_count_, g_bvar_rpc_kv_commit_rowset_get_counter.get({mock_instance}));
    ASSERT_EQ(mem_kv->put_count_, g_bvar_rpc_kv_commit_rowset_put_counter.get({mock_instance}));
    ASSERT_EQ(mem_kv->del_count_, g_bvar_rpc_kv_commit_rowset_del_counter.get({mock_instance}));
}

// get_version
TEST(RpcKvBvarTest, GetVersion) {
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

    clear_memkv_count_bytes(mem_kv.get());

    GetVersionResponse resp;
    meta_service->get_version(&ctrl, &req, &resp, nullptr);

    LOG(INFO) << "GetVersion: " << mem_kv->get_count_ << ", " << mem_kv->put_count_ << ", "
              << mem_kv->del_count_ << ", " << mem_kv->get_bytes_ << ", " << mem_kv->put_bytes_
              << ", " << mem_kv->del_bytes_;

    ASSERT_EQ(mem_kv->get_bytes_, g_bvar_rpc_kv_get_version_get_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->get_count_, g_bvar_rpc_kv_get_version_get_counter.get({mock_instance}));
}

// get_delete_bitmap_update_lock
TEST(RpcKvBvarTest, GetDeleteBitmapUpdateLock) {
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

    clear_memkv_count_bytes(mem_kv.get());

    get_delete_bitmap_update_lock(meta_service.get(), table_id, t1p1, lock_id, initiator);

    LOG(INFO) << "GetDeleteBitmapUpdateLock: " << mem_kv->get_count_ << ", " << mem_kv->put_count_
              << ", " << mem_kv->del_count_ << ", " << mem_kv->get_bytes_ << ", "
              << mem_kv->put_bytes_ << ", " << mem_kv->del_bytes_;

    ASSERT_EQ(mem_kv->get_bytes_,
              g_bvar_rpc_kv_get_delete_bitmap_update_lock_get_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->put_bytes_,
              g_bvar_rpc_kv_get_delete_bitmap_update_lock_put_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->del_bytes_,
              g_bvar_rpc_kv_get_delete_bitmap_update_lock_del_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->get_count_,
              g_bvar_rpc_kv_get_delete_bitmap_update_lock_get_counter.get({mock_instance}));
    ASSERT_EQ(mem_kv->put_count_,
              g_bvar_rpc_kv_get_delete_bitmap_update_lock_put_counter.get({mock_instance}));
    ASSERT_EQ(mem_kv->del_count_,
              g_bvar_rpc_kv_get_delete_bitmap_update_lock_del_counter.get({mock_instance}));
}

// update_delete_bitmap
TEST(RpcKvBvarTest, UpdateDeleteBitmap) {
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

    clear_memkv_count_bytes(mem_kv.get());

    update_delete_bitmap(meta_service.get(), update_delete_bitmap_req, update_delete_bitmap_res,
                         table_id, t1p1, lock_id, initiator, tablet_id, txn_id, version, data1);

    LOG(INFO) << "UpdateDeleteBitmap: " << mem_kv->get_count_ << ", " << mem_kv->put_count_ << ", "
              << mem_kv->del_count_ << ", " << mem_kv->get_bytes_ << ", " << mem_kv->put_bytes_
              << ", " << mem_kv->del_bytes_;

    ASSERT_EQ(mem_kv->get_bytes_,
              g_bvar_rpc_kv_update_delete_bitmap_get_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->put_bytes_,
              g_bvar_rpc_kv_update_delete_bitmap_put_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->del_bytes_,
              g_bvar_rpc_kv_update_delete_bitmap_del_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->get_count_,
              g_bvar_rpc_kv_update_delete_bitmap_get_counter.get({mock_instance}));
    ASSERT_EQ(mem_kv->put_count_,
              g_bvar_rpc_kv_update_delete_bitmap_put_counter.get({mock_instance}));
    ASSERT_EQ(mem_kv->del_count_,
              g_bvar_rpc_kv_update_delete_bitmap_del_counter.get({mock_instance}));
}

// get_delete_bitmap
TEST(RpcKvBvarTest, GetDeleteBitmap) {
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

    clear_memkv_count_bytes(mem_kv.get());

    meta_service->get_delete_bitmap(reinterpret_cast<google::protobuf::RpcController*>(&ctrl),
                                    &get_delete_bitmap_req, &get_delete_bitmap_res, nullptr);

    LOG(INFO) << "GetDeleteBitmap: " << mem_kv->get_count_ << ", " << mem_kv->put_count_ << ", "
              << mem_kv->del_count_ << ", " << mem_kv->get_bytes_ << ", " << mem_kv->put_bytes_
              << ", " << mem_kv->del_bytes_;

    ASSERT_EQ(mem_kv->get_bytes_, g_bvar_rpc_kv_get_delete_bitmap_get_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->get_count_, g_bvar_rpc_kv_get_delete_bitmap_get_counter.get({mock_instance}));
}

// remove_delete_bitmap_update_lock
TEST(RpcKvBvarTest, RemoveDeleteBitmapUpdateLock) {
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

    clear_memkv_count_bytes(mem_kv.get());

    meta_service->remove_delete_bitmap_update_lock(
            reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &remove_req, &remove_res,
            nullptr);

    LOG(INFO) << "RemoveDeleteBitmapUpdateLock: " << mem_kv->get_count_ << ", "
              << mem_kv->put_count_ << ", " << mem_kv->del_count_ << ", " << mem_kv->get_bytes_
              << ", " << mem_kv->put_bytes_ << ", " << mem_kv->del_bytes_;

    ASSERT_EQ(mem_kv->get_bytes_,
              g_bvar_rpc_kv_remove_delete_bitmap_update_lock_get_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->put_bytes_,
              g_bvar_rpc_kv_remove_delete_bitmap_update_lock_put_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->del_bytes_,
              g_bvar_rpc_kv_remove_delete_bitmap_update_lock_del_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->get_count_,
              g_bvar_rpc_kv_remove_delete_bitmap_update_lock_get_counter.get({mock_instance}));
    ASSERT_EQ(mem_kv->put_count_,
              g_bvar_rpc_kv_remove_delete_bitmap_update_lock_put_counter.get({mock_instance}));
    ASSERT_EQ(mem_kv->del_count_,
              g_bvar_rpc_kv_remove_delete_bitmap_update_lock_del_counter.get({mock_instance}));
}

// remove_delete_bitmap
TEST(RpcKvBvarTest, RemoveDeleteBitmap) {
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

    clear_memkv_count_bytes(mem_kv.get());

    meta_service->remove_delete_bitmap(&ctrl, &req, &resp, nullptr);

    LOG(INFO) << "RemoveDeleteBitmap: " << mem_kv->get_count_ << ", " << mem_kv->put_count_ << ", "
              << mem_kv->del_count_ << ", " << mem_kv->get_bytes_ << ", " << mem_kv->put_bytes_
              << ", " << mem_kv->del_bytes_;

    ASSERT_EQ(mem_kv->del_bytes_,
              g_bvar_rpc_kv_remove_delete_bitmap_del_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->del_count_,
              g_bvar_rpc_kv_remove_delete_bitmap_del_counter.get({mock_instance}));
}

// start_tablet_job
TEST(RpcKvBvarTest, StartTabletJob) {
    auto meta_service = get_meta_service();
    auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());
    constexpr int64_t table_id = 10001;
    constexpr int64_t index_id = 10002;
    constexpr int64_t partition_id = 10003;
    constexpr int64_t tablet_id = 10004;
    create_tablet(meta_service.get(), table_id, index_id, partition_id, tablet_id);
    StartTabletJobResponse res;

    clear_memkv_count_bytes(mem_kv.get());

    start_compaction_job(meta_service.get(), tablet_id, "compaction1", "ip:port", 0, 0,
                         TabletCompactionJobPB::BASE, res);

    LOG(INFO) << "StartTabletJob: " << mem_kv->get_count_ << ", " << mem_kv->put_count_ << ", "
              << mem_kv->del_count_ << ", " << mem_kv->get_bytes_ << ", " << mem_kv->put_bytes_
              << ", " << mem_kv->del_bytes_;

    ASSERT_EQ(mem_kv->get_bytes_, g_bvar_rpc_kv_start_tablet_job_get_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->put_bytes_, g_bvar_rpc_kv_start_tablet_job_put_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->get_count_, g_bvar_rpc_kv_start_tablet_job_get_counter.get({mock_instance}));
    ASSERT_EQ(mem_kv->put_count_, g_bvar_rpc_kv_start_tablet_job_put_counter.get({mock_instance}));
}

// finish_tablet_job
TEST(RpcKvBvarTest, FinishTabletJob) {
    auto meta_service = get_meta_service();
    auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());
    brpc::Controller cntl;

    int64_t table_id = 1;
    int64_t index_id = 2;
    int64_t partition_id = 3;
    int64_t tablet_id = 4;

    ASSERT_NO_FATAL_FAILURE(
            create_tablet(meta_service.get(), table_id, index_id, partition_id, tablet_id, false));

    StartTabletJobResponse res;
    start_compaction_job(meta_service.get(), tablet_id, "job1", "BE1", 0, 0,
                         TabletCompactionJobPB::CUMULATIVE, res);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    res.Clear();

    int64_t new_tablet_id = 11;
    ASSERT_NO_FATAL_FAILURE(create_tablet(meta_service.get(), table_id, index_id, partition_id,
                                          new_tablet_id, false, true));
    StartTabletJobResponse sc_res;
    ASSERT_NO_FATAL_FAILURE(start_schema_change_job(meta_service.get(), table_id, index_id,
                                                    partition_id, tablet_id, new_tablet_id, "job2",
                                                    "BE1", sc_res));

    long now = time(nullptr);
    FinishTabletJobRequest req;
    FinishTabletJobResponse finish_res_2;
    req.set_action(FinishTabletJobRequest::LEASE);
    auto* compaction = req.mutable_job()->add_compaction();
    compaction->set_id("job1");
    compaction->set_initiator("BE1");
    compaction->set_lease(now + 10);
    req.mutable_job()->mutable_idx()->set_tablet_id(tablet_id);

    clear_memkv_count_bytes(mem_kv.get());

    meta_service->finish_tablet_job(&cntl, &req, &finish_res_2, nullptr);

    LOG(INFO) << "FinishTabletJob: " << mem_kv->get_count_ << ", " << mem_kv->put_count_ << ", "
              << mem_kv->del_count_ << ", " << mem_kv->get_bytes_ << ", " << mem_kv->put_bytes_
              << ", " << mem_kv->del_bytes_;

    ASSERT_EQ(mem_kv->get_bytes_, g_bvar_rpc_kv_finish_tablet_job_get_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->put_bytes_, g_bvar_rpc_kv_finish_tablet_job_put_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->del_bytes_, g_bvar_rpc_kv_finish_tablet_job_del_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->get_count_, g_bvar_rpc_kv_finish_tablet_job_get_counter.get({mock_instance}));
    ASSERT_EQ(mem_kv->put_count_, g_bvar_rpc_kv_finish_tablet_job_put_counter.get({mock_instance}));
    ASSERT_EQ(mem_kv->del_count_, g_bvar_rpc_kv_finish_tablet_job_del_counter.get({mock_instance}));
}

// prepare_index
TEST(RpcKvBvarTest, PrepareIndex) {
    auto meta_service = get_meta_service();
    auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());
    std::string instance_id = "test_cloud_instance_id";

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
    auto tbl_version_key = table_version_key({instance_id, 1, table_id});
    std::string val;

    brpc::Controller ctrl;
    IndexRequest req;
    req.set_db_id(1);
    req.set_table_id(table_id);
    req.add_index_ids(index_id);
    req.set_is_new_table(true);
    IndexResponse res;

    clear_memkv_count_bytes(mem_kv.get());

    meta_service->prepare_index(&ctrl, &req, &res, nullptr);

    LOG(INFO) << "PrepareIndex: " << mem_kv->get_count_ << ", " << mem_kv->put_count_ << ", "
              << mem_kv->del_count_ << ", " << mem_kv->get_bytes_ << ", " << mem_kv->put_bytes_
              << ", " << mem_kv->del_bytes_;

    ASSERT_EQ(mem_kv->get_bytes_, g_bvar_rpc_kv_prepare_index_get_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->put_bytes_, g_bvar_rpc_kv_prepare_index_put_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->get_count_, g_bvar_rpc_kv_prepare_index_get_counter.get({mock_instance}));
    ASSERT_EQ(mem_kv->put_count_, g_bvar_rpc_kv_prepare_index_put_counter.get({mock_instance}));
}

// commit_index
TEST(RpcKvBvarTest, CommitIndex) {
    auto meta_service = get_meta_service();
    auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());
    std::string instance_id = "test_cloud_instance_id";

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
    auto tbl_version_key = table_version_key({instance_id, 1, table_id});
    std::string val;

    brpc::Controller ctrl;
    IndexRequest req;
    req.set_db_id(1);
    req.set_table_id(table_id);
    req.add_index_ids(index_id);
    req.set_is_new_table(true);
    IndexResponse res;
    meta_service->prepare_index(&ctrl, &req, &res, nullptr);
    res.Clear();

    clear_memkv_count_bytes(mem_kv.get());

    meta_service->commit_index(&ctrl, &req, &res, nullptr);

    LOG(INFO) << "CommitIndex: " << mem_kv->get_count_ << ", " << mem_kv->put_count_ << ", "
              << mem_kv->del_count_ << ", " << mem_kv->get_bytes_ << ", " << mem_kv->put_bytes_
              << ", " << mem_kv->del_bytes_;

    ASSERT_EQ(mem_kv->get_bytes_, g_bvar_rpc_kv_commit_index_get_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->put_bytes_, g_bvar_rpc_kv_commit_index_put_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->del_bytes_, g_bvar_rpc_kv_commit_index_del_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->get_count_, g_bvar_rpc_kv_commit_index_get_counter.get({mock_instance}));
    ASSERT_EQ(mem_kv->put_count_, g_bvar_rpc_kv_commit_index_put_counter.get({mock_instance}));
    ASSERT_EQ(mem_kv->del_count_, g_bvar_rpc_kv_commit_index_del_counter.get({mock_instance}));
}

// drop_index
TEST(RpcKvBvarTest, DropIndex) {
    auto meta_service = get_meta_service();
    auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());
    int64_t db_id = 4524364;
    int64_t table_id = 65354;
    int64_t index_id = 658432;
    int64_t partition_id = 76553;
    std::string mock_instance = "test_instance";
    const std::string label = "test_label_67a34e2q1231";

    int64_t tablet_id_base = 2313324;
    for (int i = 0; i < 10; ++i) {
        create_tablet_with_db_id(meta_service.get(), db_id, table_id, index_id, partition_id,
                                 tablet_id_base + i);
    }
    int txn_id {};
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
        ASSERT_GT(txn_id, 0);
    }
    {
        for (int i = 0; i < 10; ++i) {
            auto tmp_rowset =
                    create_rowset(txn_id, tablet_id_base + i, index_id, partition_id, -1, 100);
            CreateRowsetResponse res;
            commit_rowset(meta_service.get(), tmp_rowset, res);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        }
    }

    brpc::Controller cntl;
    IndexRequest req;
    req.set_cloud_unique_id("test_cloud_unique_id");

    req.set_db_id(1);
    req.set_table_id(table_id);
    req.add_index_ids(index_id);
    IndexResponse res;

    clear_memkv_count_bytes(mem_kv.get());

    meta_service->drop_index(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                             &res, nullptr);

    LOG(INFO) << "DropIndex: " << mem_kv->get_count_ << ", " << mem_kv->put_count_ << ", "
              << mem_kv->del_count_ << ", " << mem_kv->get_bytes_ << ", " << mem_kv->put_bytes_
              << ", " << mem_kv->del_bytes_;

    ASSERT_EQ(mem_kv->get_bytes_, g_bvar_rpc_kv_drop_index_get_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->put_bytes_, g_bvar_rpc_kv_drop_index_put_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->get_count_, g_bvar_rpc_kv_drop_index_get_counter.get({mock_instance}));
    ASSERT_EQ(mem_kv->put_count_, g_bvar_rpc_kv_drop_index_put_counter.get({mock_instance}));
}

// prepare_partition
TEST(RpcKvBvarTest, PreparePartition) {
    auto meta_service = get_meta_service();
    auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());
    std::string instance_id = "test_cloud_instance_id";
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
    auto tbl_version_key = table_version_key({instance_id, 1, table_id});
    std::string val;
    brpc::Controller ctrl;
    PartitionRequest req;
    PartitionResponse res;
    req.set_db_id(1);
    req.set_table_id(table_id);
    req.add_index_ids(index_id);
    req.add_partition_ids(partition_id);
    res.Clear();
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->atomic_add(tbl_version_key, 1);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    clear_memkv_count_bytes(mem_kv.get());

    meta_service->prepare_partition(&ctrl, &req, &res, nullptr);

    LOG(INFO) << "PreparePartition: " << mem_kv->get_count_ << ", " << mem_kv->put_count_ << ", "
              << mem_kv->del_count_ << ", " << mem_kv->get_bytes_ << ", " << mem_kv->put_bytes_
              << ", " << mem_kv->del_bytes_;

    ASSERT_EQ(mem_kv->get_bytes_, g_bvar_rpc_kv_prepare_partition_get_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->put_bytes_, g_bvar_rpc_kv_prepare_partition_put_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->get_count_, g_bvar_rpc_kv_prepare_partition_get_counter.get({mock_instance}));
    ASSERT_EQ(mem_kv->put_count_, g_bvar_rpc_kv_prepare_partition_put_counter.get({mock_instance}));
}

// commit_partition
TEST(RpcKvBvarTest, CommitPartition) {
    auto meta_service = get_meta_service();
    auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());
    std::string instance_id = "test_cloud_instance_id";
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
    auto tbl_version_key = table_version_key({instance_id, 1, table_id});
    std::string val;
    brpc::Controller ctrl;
    PartitionRequest req;
    PartitionResponse res;
    req.set_db_id(1);
    req.set_table_id(table_id);
    req.add_index_ids(index_id);
    req.add_partition_ids(partition_id);
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->atomic_add(tbl_version_key, 1);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    meta_service->prepare_partition(&ctrl, &req, &res, nullptr);

    clear_memkv_count_bytes(mem_kv.get());

    meta_service->commit_partition(&ctrl, &req, &res, nullptr);

    LOG(INFO) << "CommitPartition: " << mem_kv->get_count_ << ", " << mem_kv->put_count_ << ", "
              << mem_kv->del_count_ << ", " << mem_kv->get_bytes_ << ", " << mem_kv->put_bytes_
              << ", " << mem_kv->del_bytes_;

    ASSERT_EQ(mem_kv->get_bytes_, g_bvar_rpc_kv_commit_partition_get_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->put_bytes_, g_bvar_rpc_kv_commit_partition_put_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->del_bytes_, g_bvar_rpc_kv_commit_partition_del_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->get_count_, g_bvar_rpc_kv_commit_partition_get_counter.get({mock_instance}));
    ASSERT_EQ(mem_kv->put_count_, g_bvar_rpc_kv_commit_partition_put_counter.get({mock_instance}));
    ASSERT_EQ(mem_kv->del_count_, g_bvar_rpc_kv_commit_partition_del_counter.get({mock_instance}));
}

// check_kv
TEST(RpcKvBvarTest, CheckKv) {
    auto meta_service = get_meta_service();
    auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());
    std::string instance_id = "test_instance";
    constexpr int64_t table_id = 10001;
    constexpr int64_t index_id = 10002;
    constexpr int64_t partition_id = 10003;

    std::unique_ptr<Transaction> txn;
    RecyclePartitionPB partition_pb;
    auto partition_key = recycle_partition_key({instance_id, 10004});
    auto tbl_version_key = table_version_key({instance_id, 1, table_id});
    brpc::Controller ctrl;
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->put(partition_key, "val");
    txn->atomic_add(tbl_version_key, 1);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    CheckKVRequest req_check;
    CheckKVResponse res_check;
    req_check.set_op(CheckKVRequest::CREATE_PARTITION_AFTER_FE_COMMIT);
    CheckKeyInfos check_keys_pb;
    check_keys_pb.add_table_ids(table_id + 1);
    check_keys_pb.add_index_ids(index_id + 1);
    check_keys_pb.add_partition_ids(partition_id + 1);
    req_check.mutable_check_keys()->CopyFrom(check_keys_pb);

    clear_memkv_count_bytes(mem_kv.get());

    meta_service->check_kv(&ctrl, &req_check, &res_check, nullptr);

    LOG(INFO) << "CheckKv: " << mem_kv->get_count_ << ", " << mem_kv->put_count_ << ", "
              << mem_kv->del_count_ << ", " << mem_kv->get_bytes_ << ", " << mem_kv->put_bytes_
              << ", " << mem_kv->del_bytes_;

    ASSERT_EQ(mem_kv->get_bytes_, g_bvar_rpc_kv_check_kv_get_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->get_count_, g_bvar_rpc_kv_check_kv_get_counter.get({mock_instance}));
}

// drop_partition
TEST(RpcKvBvarTest, DropPartition) {
    auto meta_service = get_meta_service();
    auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());
    std::string instance_id = "test_instance";
    constexpr int64_t table_id = 10001;
    constexpr int64_t index_id = 10002;
    constexpr int64_t partition_id = 10003;
    auto tbl_version_key = table_version_key({instance_id, 1, table_id});

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->atomic_add(tbl_version_key, 1);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    brpc::Controller ctrl;
    PartitionRequest req;
    PartitionResponse res;
    req.set_db_id(1);
    req.set_table_id(table_id);
    req.add_index_ids(index_id);
    req.add_partition_ids(partition_id);
    req.set_need_update_table_version(true);

    clear_memkv_count_bytes(mem_kv.get());

    meta_service->drop_partition(&ctrl, &req, &res, nullptr);

    LOG(INFO) << "DropPartition: " << mem_kv->get_count_ << ", " << mem_kv->put_count_ << ", "
              << mem_kv->del_count_ << ", " << mem_kv->get_bytes_ << ", " << mem_kv->put_bytes_
              << ", " << mem_kv->del_bytes_;

    ASSERT_EQ(mem_kv->get_bytes_, g_bvar_rpc_kv_drop_partition_get_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->put_bytes_, g_bvar_rpc_kv_drop_partition_put_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->get_count_, g_bvar_rpc_kv_drop_partition_get_counter.get({mock_instance}));
    ASSERT_EQ(mem_kv->put_count_, g_bvar_rpc_kv_drop_partition_put_counter.get({mock_instance}));
}

// get_obj_store_info
TEST(RpcKvBvarTest, GetObjStoreInfo) {
    auto meta_service = get_meta_service();
    auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());

    auto rate_limiter = std::make_shared<cloud::RateLimiter>();

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    InstanceKeyInfo key_info {"test_instance"};
    std::string key;
    instance_key(key_info, &key);
    txn->put(key, "val");
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    brpc::Controller cntl;
    GetObjStoreInfoResponse res;
    GetObjStoreInfoRequest req;
    req.set_cloud_unique_id("test_cloud_unique_id");

    clear_memkv_count_bytes(mem_kv.get());

    meta_service->get_obj_store_info(&cntl, &req, &res, nullptr);

    LOG(INFO) << "GetObjStoreInfo: " << mem_kv->get_count_ << ", " << mem_kv->put_count_ << ", "
              << mem_kv->del_count_ << ", " << mem_kv->get_bytes_ << ", " << mem_kv->put_bytes_
              << ", " << mem_kv->del_bytes_;

    ASSERT_EQ(mem_kv->get_bytes_, g_bvar_rpc_kv_get_obj_store_info_get_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->get_count_,
              g_bvar_rpc_kv_get_obj_store_info_get_counter.get({mock_instance}));
}

// alter_storage_vault
TEST(RpcKvBvarTest, AlterStorageVault) {
    auto meta_service = get_meta_service();
    auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());

    constexpr char vault_name[] = "test_alter_s3_vault";

    InstanceKeyInfo key_info {"test_instance"};
    std::string key;
    instance_key(key_info, &key);
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->put(key, "val");
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    AlterObjStoreInfoRequest req;
    req.set_cloud_unique_id("test_cloud_unique_id");
    req.set_op(AlterObjStoreInfoRequest::ALTER_S3_VAULT);
    StorageVaultPB vault;
    vault.mutable_obj_info()->set_ak("new_ak");
    vault.set_name(vault_name);
    req.mutable_vault()->CopyFrom(vault);

    brpc::Controller cntl;
    AlterObjStoreInfoResponse res;

    clear_memkv_count_bytes(mem_kv.get());

    meta_service->alter_storage_vault(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);

    LOG(INFO) << "AlterStorageVault: " << mem_kv->get_count_ << ", " << mem_kv->put_count_ << ", "
              << mem_kv->del_count_ << ", " << mem_kv->get_bytes_ << ", " << mem_kv->put_bytes_
              << ", " << mem_kv->del_bytes_;

    ASSERT_EQ(mem_kv->get_bytes_, g_bvar_rpc_kv_alter_storage_vault_get_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->put_bytes_, g_bvar_rpc_kv_alter_storage_vault_put_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->del_bytes_, g_bvar_rpc_kv_alter_storage_vault_del_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->get_count_,
              g_bvar_rpc_kv_alter_storage_vault_get_counter.get({mock_instance}));
    ASSERT_EQ(mem_kv->put_count_,
              g_bvar_rpc_kv_alter_storage_vault_put_counter.get({mock_instance}));
    ASSERT_EQ(mem_kv->del_count_,
              g_bvar_rpc_kv_alter_storage_vault_del_counter.get({mock_instance}));
}

// alter_obj_store_info
TEST(RpcKvBvarTest, AlterObjStoreInfo) {
    auto meta_service = get_meta_service();
    auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());

    auto* sp = SyncPoint::get_instance();
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

    std::string plain_sk = "Hx60p12123af234541nsVsffdfsdfghsdfhsdf34t";

    AlterObjStoreInfoRequest req;
    req.set_cloud_unique_id("test_cloud_unique_id");
    req.set_op(AlterObjStoreInfoRequest::LEGACY_UPDATE_AK_SK);
    req.mutable_obj()->set_id("1");
    req.mutable_obj()->set_ak("new_ak");
    req.mutable_obj()->set_sk(plain_sk);

    brpc::Controller cntl;
    AlterObjStoreInfoResponse res;

    clear_memkv_count_bytes(mem_kv.get());

    meta_service->alter_obj_store_info(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                       &req, &res, nullptr);

    LOG(INFO) << "AlterObjStoreInfo: " << mem_kv->get_count_ << ", " << mem_kv->put_count_ << ", "
              << mem_kv->del_count_ << ", " << mem_kv->get_bytes_ << ", " << mem_kv->put_bytes_
              << ", " << mem_kv->del_bytes_;

    ASSERT_EQ(mem_kv->get_bytes_,
              g_bvar_rpc_kv_alter_obj_store_info_get_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->put_bytes_,
              g_bvar_rpc_kv_alter_obj_store_info_put_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->get_count_,
              g_bvar_rpc_kv_alter_obj_store_info_get_counter.get({mock_instance}));
    ASSERT_EQ(mem_kv->put_count_,
              g_bvar_rpc_kv_alter_obj_store_info_put_counter.get({mock_instance}));
    SyncPoint::get_instance()->disable_processing();
    SyncPoint::get_instance()->clear_all_call_backs();
}

// update_ak_sk
TEST(RpcKvBvarTest, UpdateAkSk) {
    auto meta_service = get_meta_service();
    auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());

    auto* sp = SyncPoint::get_instance();
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

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string key;
    std::string val;
    InstanceKeyInfo key_info {"test_instance"};
    instance_key(key_info, &key);

    ObjectStoreInfoPB obj_info;

    obj_info.set_user_id("111");

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
    ram_user.set_user_id("111");

    ram_user.set_ak("new_ak");
    ram_user.set_sk(plain_sk);
    req.add_internal_bucket_user()->CopyFrom(ram_user);

    brpc::Controller cntl;
    UpdateAkSkResponse res;

    clear_memkv_count_bytes(mem_kv.get());

    meta_service->update_ak_sk(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                               &res, nullptr);

    LOG(INFO) << "UpdateAkSk: " << mem_kv->get_count_ << ", " << mem_kv->put_count_ << ", "
              << mem_kv->del_count_ << ", " << mem_kv->get_bytes_ << ", " << mem_kv->put_bytes_
              << ", " << mem_kv->del_bytes_;

    ASSERT_EQ(mem_kv->get_bytes_, g_bvar_rpc_kv_update_ak_sk_get_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->put_bytes_, g_bvar_rpc_kv_update_ak_sk_put_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->get_count_, g_bvar_rpc_kv_update_ak_sk_get_counter.get({mock_instance}));
    ASSERT_EQ(mem_kv->put_count_, g_bvar_rpc_kv_update_ak_sk_put_counter.get({mock_instance}));

    SyncPoint::get_instance()->disable_processing();
    SyncPoint::get_instance()->clear_all_call_backs();
}

// create_instance
TEST(RpcKvBvarTest, CreateInstance) {
    auto meta_service = get_meta_service();
    auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());

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

    auto* sp = SyncPoint::get_instance();
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

    clear_memkv_count_bytes(mem_kv.get());

    meta_service->create_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                  &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

    LOG(INFO) << "CreateInstance: " << mem_kv->get_count_ << ", " << mem_kv->put_count_ << ", "
              << mem_kv->del_count_ << ", " << mem_kv->get_bytes_ << ", " << mem_kv->put_bytes_
              << ", " << mem_kv->del_bytes_;

    ASSERT_EQ(mem_kv->get_bytes_, g_bvar_rpc_kv_create_instance_get_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->get_count_, g_bvar_rpc_kv_create_instance_get_counter.get({mock_instance}));

    sp->clear_all_call_backs();
    sp->clear_trace();
    sp->disable_processing();
}

// get_instance
TEST(RpcKvBvarTest, GetInstance) {
    auto meta_service = get_meta_service();
    auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());

    auto* sp = SyncPoint::get_instance();
    sp->set_call_back("encrypt_ak_sk:get_encryption_key", [](auto&& args) {
        auto* ret = try_any_cast<int*>(args[0]);
        *ret = 0;
        auto* key = try_any_cast<std::string*>(args[1]);
        *key = "test";
        auto* key_id = try_any_cast<int64_t*>(args[2]);
        *key_id = 1;
    });
    sp->enable_processing();
    brpc::Controller cntl;
    {
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

        CreateInstanceResponse res;

        meta_service->create_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
    }
    GetInstanceRequest req;
    GetInstanceResponse res;

    clear_memkv_count_bytes(mem_kv.get());

    req.set_cloud_unique_id("1:test_instance:m-n3qdpyal27rh8iprxx");
    meta_service->get_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                               &res, nullptr);

    LOG(INFO) << "GetInstance: " << mem_kv->get_count_ << ", " << mem_kv->put_count_ << ", "
              << mem_kv->del_count_ << ", " << mem_kv->get_bytes_ << ", " << mem_kv->put_bytes_
              << ", " << mem_kv->del_bytes_;

    ASSERT_EQ(mem_kv->get_bytes_, g_bvar_rpc_kv_get_instance_get_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->get_count_, g_bvar_rpc_kv_get_instance_get_counter.get({mock_instance}));
    sp->clear_all_call_backs();
    sp->clear_trace();
    sp->disable_processing();
}

// alter_cluster
// TEST(RpcKvBvarTest, AlterCluster) {
//     auto meta_service = get_meta_service();
//     auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());

//     brpc::Controller cntl;
//     AlterClusterRequest req;
//     req.set_instance_id(mock_instance);
//     req.mutable_cluster()->set_cluster_name(mock_cluster_name);
//     req.set_op(AlterClusterRequest::ADD_CLUSTER);
//     AlterClusterResponse res;

//     mem_kv->get_count_ = 0;
//     mem_kv->put_count_ = 0;
//     mem_kv->del_count_ = 0;

//     meta_service->alter_cluster(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
//                                 &res, nullptr);

//     LOG(INFO) << "AlterCluster: " << mem_kv->get_count_ << ", " << mem_kv->put_count_ << ", "
//               << mem_kv->del_count_;
//     ASSERT_EQ(mem_kv->get_count_, g_bvar_rpc_kv_alter_cluster_get_counter.get({mock_instance}));
// }

// get_cluster
TEST(RpcKvBvarTest, GetCluster) {
    auto meta_service = get_meta_service();
    auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());
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

    brpc::Controller cntl;
    GetClusterRequest req;
    req.set_cloud_unique_id("test_cloud_unique_id");
    req.set_cluster_id(mock_cluster_id);
    req.set_cluster_name("test_cluster");
    GetClusterResponse res;

    clear_memkv_count_bytes(mem_kv.get());

    meta_service->get_cluster(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                              &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

    LOG(INFO) << "GetCluster: " << mem_kv->get_count_ << ", " << mem_kv->put_count_ << ", "
              << mem_kv->del_count_ << ", " << mem_kv->get_bytes_ << ", " << mem_kv->put_bytes_
              << ", " << mem_kv->del_bytes_;

    ASSERT_EQ(mem_kv->get_bytes_, g_bvar_rpc_kv_get_cluster_get_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->put_bytes_, g_bvar_rpc_kv_get_cluster_put_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->get_count_, g_bvar_rpc_kv_get_cluster_get_counter.get({mock_instance}));
    ASSERT_EQ(mem_kv->put_count_, g_bvar_rpc_kv_get_cluster_put_counter.get({mock_instance}));
}

// create_stage
TEST(RpcKvBvarTest, CreateStage) {
    auto meta_service = get_meta_service();
    auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());

    brpc::Controller cntl;
    const auto* cloud_unique_id = "test_cloud_unique_id";
    std::string instance_id = "stage_test_instance_id";
    [[maybe_unused]] auto* sp = SyncPoint::get_instance();
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
    clear_memkv_count_bytes(mem_kv.get());

    meta_service->create_stage(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                               &create_stage_request, &create_stage_response, nullptr);

    LOG(INFO) << "CreateStage: " << mem_kv->get_count_ << ", " << mem_kv->put_count_ << ", "
              << mem_kv->del_count_ << ", " << mem_kv->get_bytes_ << ", " << mem_kv->put_bytes_
              << ", " << mem_kv->del_bytes_;
    ASSERT_EQ(mem_kv->get_bytes_, g_bvar_rpc_kv_create_stage_get_bytes.get({instance_id}));
    ASSERT_EQ(mem_kv->put_bytes_, g_bvar_rpc_kv_create_stage_put_bytes.get({instance_id}));
    ASSERT_EQ(mem_kv->get_count_, g_bvar_rpc_kv_create_stage_get_counter.get({instance_id}));
    ASSERT_EQ(mem_kv->put_count_, g_bvar_rpc_kv_create_stage_put_counter.get({instance_id}));
    sp->clear_all_call_backs();
    sp->clear_trace();
    sp->disable_processing();
}

// get_stage
TEST(RpcKvBvarTest, GetStage) {
    auto meta_service = get_meta_service();
    auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());

    brpc::Controller cntl;
    const auto* cloud_unique_id = "test_cloud_unique_id";
    std::string instance_id = "stage_test_instance_id";
    [[maybe_unused]] auto* sp = SyncPoint::get_instance();
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

    GetStageRequest get_stage_req;
    get_stage_req.set_type(StagePB::INTERNAL);
    get_stage_req.set_cloud_unique_id(cloud_unique_id);
    get_stage_req.set_mysql_user_name("root");
    get_stage_req.set_mysql_user_id("root_id");

    // get existent internal stage
    GetStageResponse res2;

    clear_memkv_count_bytes(mem_kv.get());

    meta_service->get_stage(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                            &get_stage_req, &res2, nullptr);

    LOG(INFO) << "GetStage: " << mem_kv->get_count_ << ", " << mem_kv->put_count_ << ", "
              << mem_kv->del_count_ << ", " << mem_kv->get_bytes_ << ", " << mem_kv->put_bytes_
              << ", " << mem_kv->del_bytes_;
    ASSERT_EQ(mem_kv->get_bytes_, g_bvar_rpc_kv_get_stage_get_bytes.get({instance_id}));
    ASSERT_EQ(mem_kv->get_count_, g_bvar_rpc_kv_get_stage_get_counter.get({instance_id}));
    sp->clear_all_call_backs();
    sp->clear_trace();
    sp->disable_processing();
}

// get_iam
TEST(RpcKvBvarTest, GetIam) {
    auto meta_service = get_meta_service();
    auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());

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

    clear_memkv_count_bytes(mem_kv.get());
    meta_service->get_iam(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &request,
                          &response, nullptr);
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK);

    LOG(INFO) << "GetIam: " << mem_kv->get_count_ << ", " << mem_kv->put_count_ << ", "
              << mem_kv->del_count_ << ", " << mem_kv->get_bytes_ << ", " << mem_kv->put_bytes_
              << ", " << mem_kv->del_bytes_;
    ASSERT_EQ(mem_kv->get_bytes_, g_bvar_rpc_kv_get_iam_get_bytes.get({instance_id}));
    ASSERT_EQ(mem_kv->get_count_, g_bvar_rpc_kv_get_iam_get_counter.get({instance_id}));
    sp->clear_all_call_backs();
    sp->clear_trace();
    sp->disable_processing();
}

// alter_iam
TEST(RpcKvBvarTest, AlterIam) {
    auto meta_service = get_meta_service();
    auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());

    auto* sp = SyncPoint::get_instance();
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

    AlterIamRequest req;
    req.set_account_id("123");
    req.set_ak("ak1");
    req.set_sk(plain_sk);

    brpc::Controller cntl;
    AlterIamResponse res;

    clear_memkv_count_bytes(mem_kv.get());

    meta_service->alter_iam(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res,
                            nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

    LOG(INFO) << "AlterIam: " << mem_kv->get_count_ << ", " << mem_kv->put_count_ << ", "
              << mem_kv->del_count_ << ", " << mem_kv->get_bytes_ << ", " << mem_kv->put_bytes_
              << ", " << mem_kv->del_bytes_;
    ASSERT_EQ(mem_kv->get_bytes_, g_bvar_rpc_kv_alter_iam_get_bytes.get({"alter_iam_instance"}));
    ASSERT_EQ(mem_kv->put_bytes_, g_bvar_rpc_kv_alter_iam_put_bytes.get({"alter_iam_instance"}));
    ASSERT_EQ(mem_kv->get_count_, g_bvar_rpc_kv_alter_iam_get_counter.get({"alter_iam_instance"}));
    ASSERT_EQ(mem_kv->put_count_, g_bvar_rpc_kv_alter_iam_put_counter.get({"alter_iam_instance"}));
}

// alter_ram_user
TEST(RpcKvBvarTest, AlterRamUser) {
    auto meta_service = get_meta_service();
    auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());
    brpc::Controller cntl;
    std::string instance_id = "alter_ram_user_instance_id";
    [[maybe_unused]] auto* sp = SyncPoint::get_instance();
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

    // alter ram user
    RamUserPB ram_user;
    ram_user.set_user_id("test_user_id");
    ram_user.set_ak("test_ak");
    ram_user.set_sk("test_sk");
    AlterRamUserRequest alter_ram_user_request;
    alter_ram_user_request.set_instance_id(instance_id);
    alter_ram_user_request.mutable_ram_user()->CopyFrom(ram_user);
    AlterRamUserResponse alter_ram_user_response;
    clear_memkv_count_bytes(mem_kv.get());
    meta_service->alter_ram_user(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                 &alter_ram_user_request, &alter_ram_user_response, nullptr);

    LOG(INFO) << "AlterRamUser: " << mem_kv->get_count_ << ", " << mem_kv->put_count_ << ", "
              << mem_kv->del_count_ << ", " << mem_kv->get_bytes_ << ", " << mem_kv->put_bytes_
              << ", " << mem_kv->del_bytes_;
    ASSERT_EQ(mem_kv->get_bytes_, g_bvar_rpc_kv_alter_ram_user_get_bytes.get({instance_id}));
    ASSERT_EQ(mem_kv->put_bytes_, g_bvar_rpc_kv_alter_ram_user_put_bytes.get({instance_id}));
    ASSERT_EQ(mem_kv->get_count_, g_bvar_rpc_kv_alter_ram_user_get_counter.get({instance_id}));
    ASSERT_EQ(mem_kv->put_count_, g_bvar_rpc_kv_alter_ram_user_put_counter.get({instance_id}));
    sp->clear_all_call_backs();
    sp->clear_trace();
    sp->disable_processing();
}

// begin_copy
TEST(RpcKvBvarTest, BeginCopy) {
    auto meta_service = get_meta_service();
    auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());
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
    BeginCopyResponse res;

    clear_memkv_count_bytes(mem_kv.get());

    meta_service->begin_copy(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                             &begin_copy_request, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

    LOG(INFO) << "BeginCopy: " << mem_kv->get_count_ << ", " << mem_kv->put_count_ << ", "
              << mem_kv->del_count_ << ", " << mem_kv->get_bytes_ << ", " << mem_kv->put_bytes_
              << ", " << mem_kv->del_bytes_;
    ASSERT_EQ(mem_kv->get_bytes_, g_bvar_rpc_kv_begin_copy_get_bytes.get({instance_id}));
    ASSERT_EQ(mem_kv->put_bytes_, g_bvar_rpc_kv_begin_copy_put_bytes.get({instance_id}));
    ASSERT_EQ(mem_kv->get_count_, g_bvar_rpc_kv_begin_copy_get_counter.get({instance_id}));
    ASSERT_EQ(mem_kv->put_count_, g_bvar_rpc_kv_begin_copy_put_counter.get({instance_id}));
    sp->clear_all_call_backs();
    sp->clear_trace();
    sp->disable_processing();
}

// get_copy_job
TEST(RpcKvBvarTest, GetCopyJob) {
    auto meta_service = get_meta_service();
    auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());
    brpc::Controller cntl;
    const char* cloud_unique_id = "test_cloud_unique_id";
    const char* stage_id = "test_stage_id";
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
    {
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
        BeginCopyResponse res;
        meta_service->begin_copy(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                 &begin_copy_request, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    GetCopyJobRequest get_copy_job_request;
    get_copy_job_request.set_cloud_unique_id(cloud_unique_id);
    get_copy_job_request.set_stage_id(stage_id);
    get_copy_job_request.set_table_id(table_id);
    get_copy_job_request.set_copy_id("test_copy_id");
    get_copy_job_request.set_group_id(0);

    clear_memkv_count_bytes(mem_kv.get());

    GetCopyJobResponse res;
    meta_service->get_copy_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                               &get_copy_job_request, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

    LOG(INFO) << "GetCopyJob: " << mem_kv->get_count_ << ", " << mem_kv->put_count_ << ", "
              << mem_kv->del_count_ << ", " << mem_kv->get_bytes_ << ", " << mem_kv->put_bytes_
              << ", " << mem_kv->del_bytes_;
    ASSERT_EQ(mem_kv->get_bytes_, g_bvar_rpc_kv_get_copy_job_get_bytes.get({instance_id}));
    ASSERT_EQ(mem_kv->get_count_, g_bvar_rpc_kv_get_copy_job_get_counter.get({instance_id}));
    sp->clear_all_call_backs();
    sp->clear_trace();
    sp->disable_processing();
}

// finish_copy
TEST(RpcKvBvarTest, FinishCopy) {
    auto meta_service = get_meta_service();
    auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());
    brpc::Controller cntl;
    const char* cloud_unique_id = "test_cloud_unique_id";
    const char* stage_id = "test_stage_id";
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
    {
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
        BeginCopyResponse res;
        meta_service->begin_copy(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                 &begin_copy_request, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }
    FinishCopyRequest finish_copy_request;
    finish_copy_request.set_cloud_unique_id(cloud_unique_id);
    finish_copy_request.set_stage_id(stage_id);
    finish_copy_request.set_stage_type(StagePB::EXTERNAL);
    finish_copy_request.set_table_id(table_id);
    finish_copy_request.set_copy_id("test_copy_id");
    finish_copy_request.set_group_id(0);
    finish_copy_request.set_action(FinishCopyRequest::COMMIT);

    clear_memkv_count_bytes(mem_kv.get());

    FinishCopyResponse res;
    meta_service->finish_copy(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                              &finish_copy_request, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

    LOG(INFO) << "FinishCopy: " << mem_kv->get_count_ << ", " << mem_kv->put_count_ << ", "
              << mem_kv->del_count_ << ", " << mem_kv->get_bytes_ << ", " << mem_kv->put_bytes_
              << ", " << mem_kv->del_bytes_;
    ASSERT_EQ(mem_kv->get_bytes_, g_bvar_rpc_kv_finish_copy_get_bytes.get({instance_id}));
    ASSERT_EQ(mem_kv->put_bytes_, g_bvar_rpc_kv_finish_copy_put_bytes.get({instance_id}));
    ASSERT_EQ(mem_kv->del_bytes_, g_bvar_rpc_kv_finish_copy_del_bytes.get({instance_id}));
    ASSERT_EQ(mem_kv->get_count_, g_bvar_rpc_kv_finish_copy_get_counter.get({instance_id}));
    ASSERT_EQ(mem_kv->put_count_, g_bvar_rpc_kv_finish_copy_put_counter.get({instance_id}));
    ASSERT_EQ(mem_kv->del_count_, g_bvar_rpc_kv_finish_copy_del_counter.get({instance_id}));
    sp->clear_all_call_backs();
    sp->clear_trace();
    sp->disable_processing();
}

// get_copy_files
TEST(RpcKvBvarTest, GetCopyFiles) {
    auto meta_service = get_meta_service();
    auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());
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
    {
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
        BeginCopyResponse res;
        meta_service->begin_copy(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                 &begin_copy_request, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }
    GetCopyFilesRequest get_copy_file_req;
    get_copy_file_req.set_cloud_unique_id(cloud_unique_id);
    get_copy_file_req.set_stage_id(stage_id);
    get_copy_file_req.set_table_id(table_id);

    clear_memkv_count_bytes(mem_kv.get());

    GetCopyFilesResponse res;
    meta_service->get_copy_files(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                 &get_copy_file_req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

    LOG(INFO) << "GetCopyFiles: " << mem_kv->get_count_ << ", " << mem_kv->put_count_ << ", "
              << mem_kv->del_count_ << ", " << mem_kv->get_bytes_ << ", " << mem_kv->put_bytes_
              << ", " << mem_kv->del_bytes_;
    ASSERT_EQ(mem_kv->get_bytes_, g_bvar_rpc_kv_get_copy_files_get_bytes.get({instance_id}));
    ASSERT_EQ(mem_kv->get_count_, g_bvar_rpc_kv_get_copy_files_get_counter.get({instance_id}));
    sp->clear_all_call_backs();
    sp->clear_trace();
    sp->disable_processing();
}

// filter_copy_files
TEST(RpcKvBvarTest, FilterCopyFiles) {
    auto meta_service = get_meta_service();
    auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());
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
    {
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
        BeginCopyResponse res;
        meta_service->begin_copy(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                 &begin_copy_request, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

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
    FilterCopyFilesResponse res;
    clear_memkv_count_bytes(mem_kv.get());

    meta_service->filter_copy_files(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &request, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

    LOG(INFO) << "FilterCopyFiles: " << mem_kv->get_count_ << ", " << mem_kv->put_count_ << ", "
              << mem_kv->del_count_ << ", " << mem_kv->get_bytes_ << ", " << mem_kv->put_bytes_
              << ", " << mem_kv->del_bytes_;
    ASSERT_EQ(mem_kv->get_bytes_, g_bvar_rpc_kv_filter_copy_files_get_bytes.get({instance_id}));
    ASSERT_EQ(mem_kv->get_count_, g_bvar_rpc_kv_filter_copy_files_get_counter.get({instance_id}));
    sp->clear_all_call_backs();
    sp->clear_trace();
    sp->disable_processing();
}

// get_cluster_status
TEST(RpcKvBvarTest, GetClusterStatus) {
    auto meta_service = get_meta_service();
    auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());

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

    brpc::Controller cntl;
    GetClusterStatusRequest req;
    req.add_instance_ids(mock_instance);
    GetClusterStatusResponse res;

    clear_memkv_count_bytes(mem_kv.get());

    meta_service->get_cluster_status(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(res.details().at(0).clusters().size(), 3);

    LOG(INFO) << "GetClusterStatus: " << mem_kv->get_count_ << ", " << mem_kv->put_count_ << ", "
              << mem_kv->del_count_ << ", " << mem_kv->get_bytes_ << ", " << mem_kv->put_bytes_
              << ", " << mem_kv->del_bytes_;
    ASSERT_EQ(mem_kv->get_bytes_, g_bvar_rpc_kv_get_cluster_status_get_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->get_count_,
              g_bvar_rpc_kv_get_cluster_status_get_counter.get({mock_instance}));
}

// get_current_max_txn_id
TEST(RpcKvBvarTest, GetCurrentMaxTxnId) {
    auto meta_service = get_meta_service();
    auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());
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

    std::unique_ptr<Transaction> txn;
    EXPECT_EQ(mem_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->put("schema change", "val");
    EXPECT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    max_txn_id_req.set_cloud_unique_id(cloud_unique_id);

    clear_memkv_count_bytes(mem_kv.get());

    meta_service->get_current_max_txn_id(
            reinterpret_cast<::google::protobuf::RpcController*>(&max_txn_id_cntl), &max_txn_id_req,
            &max_txn_id_res, nullptr);

    LOG(INFO) << "GetCurrentMaxTxnId: " << mem_kv->get_count_ << ", " << mem_kv->put_count_ << ", "
              << mem_kv->del_count_ << ", " << mem_kv->get_bytes_ << ", " << mem_kv->put_bytes_
              << ", " << mem_kv->del_bytes_;
    ASSERT_EQ(mem_kv->get_bytes_,
              g_bvar_rpc_kv_get_current_max_txn_id_get_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->get_count_,
              g_bvar_rpc_kv_get_current_max_txn_id_get_counter.get({mock_instance}));
}

// begin_sub_txn
TEST(RpcKvBvarTest, BeginSubTxn) {
    auto meta_service = get_meta_service();
    auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());
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

    clear_memkv_count_bytes(mem_kv.get());

    meta_service->begin_sub_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                &res, nullptr);

    LOG(INFO) << "BeginSubTxn: " << mem_kv->get_count_ << ", " << mem_kv->put_count_ << ", "
              << mem_kv->del_count_ << ", " << mem_kv->get_bytes_ << ", " << mem_kv->put_bytes_
              << ", " << mem_kv->del_bytes_;
    ASSERT_EQ(mem_kv->get_bytes_, g_bvar_rpc_kv_begin_sub_txn_get_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->put_bytes_, g_bvar_rpc_kv_begin_sub_txn_put_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->del_bytes_, g_bvar_rpc_kv_begin_sub_txn_del_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->get_count_, g_bvar_rpc_kv_begin_sub_txn_get_counter.get({mock_instance}));
    ASSERT_EQ(mem_kv->put_count_, g_bvar_rpc_kv_begin_sub_txn_put_counter.get({mock_instance}));
    ASSERT_EQ(mem_kv->del_count_, g_bvar_rpc_kv_begin_sub_txn_del_counter.get({mock_instance}));
}

// abort_sub_txn
TEST(RpcKvBvarTest, AbortSubTxn) {
    auto meta_service = get_meta_service();
    auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());
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
    brpc::Controller cntl;
    {
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
    }

    AbortSubTxnRequest req;
    req.set_cloud_unique_id("test_cloud_unique_id");
    req.set_txn_id(txn_id);
    req.set_sub_txn_num(2);
    req.set_sub_txn_id(sub_txn_id1);
    req.set_db_id(db_id);
    req.mutable_table_ids()->Add(t1);
    req.mutable_table_ids()->Add(t2);
    AbortSubTxnResponse res;

    clear_memkv_count_bytes(mem_kv.get());

    meta_service->abort_sub_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                &res, nullptr);

    LOG(INFO) << "AbortSubTxn: " << mem_kv->get_count_ << ", " << mem_kv->put_count_ << ", "
              << mem_kv->del_count_ << ", " << mem_kv->get_bytes_ << ", " << mem_kv->put_bytes_
              << ", " << mem_kv->del_bytes_;
    ASSERT_EQ(mem_kv->get_bytes_, g_bvar_rpc_kv_abort_sub_txn_get_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->put_bytes_, g_bvar_rpc_kv_abort_sub_txn_put_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->get_count_, g_bvar_rpc_kv_abort_sub_txn_get_counter.get({mock_instance}));
    ASSERT_EQ(mem_kv->put_count_, g_bvar_rpc_kv_abort_sub_txn_put_counter.get({mock_instance}));
}

// abort_txn_with_coordinator
TEST(RpcKvBvarTest, AbortTxnWithCoordinator) {
    auto meta_service = get_meta_service();
    auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());
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

    clear_memkv_count_bytes(mem_kv.get());

    meta_service->abort_txn_with_coordinator(
            reinterpret_cast<::google::protobuf::RpcController*>(&begin_txn_cntl), &abort_txn_req,
            &abort_txn_resp, nullptr);

    LOG(INFO) << "AbortTxnWithCoordinator: " << mem_kv->get_count_ << ", " << mem_kv->put_count_
              << ", " << mem_kv->del_count_ << ", " << mem_kv->get_bytes_ << ", "
              << mem_kv->put_bytes_ << ", " << mem_kv->del_bytes_;
    ASSERT_EQ(mem_kv->get_bytes_,
              g_bvar_rpc_kv_abort_txn_with_coordinator_get_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->get_count_,
              g_bvar_rpc_kv_abort_txn_with_coordinator_get_counter.get({mock_instance}));
}

// check_txn_conflict
TEST(RpcKvBvarTest, CheckTxnConflict) {
    auto meta_service = get_meta_service();
    auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());

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

    clear_memkv_count_bytes(mem_kv.get());

    meta_service->check_txn_conflict(
            reinterpret_cast<::google::protobuf::RpcController*>(&begin_txn_cntl),
            &check_txn_conflict_req, &check_txn_conflict_res, nullptr);

    LOG(INFO) << "CheckTxnConflict: " << mem_kv->get_count_ << ", " << mem_kv->put_count_ << ", "
              << mem_kv->del_count_ << ", " << mem_kv->get_bytes_ << ", " << mem_kv->put_bytes_
              << ", " << mem_kv->del_bytes_;
    ASSERT_EQ(mem_kv->get_bytes_, g_bvar_rpc_kv_check_txn_conflict_get_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->get_count_,
              g_bvar_rpc_kv_check_txn_conflict_get_counter.get({mock_instance}));
}

// clean_txn_label
TEST(RpcKvBvarTest, CleanTxnLabel) {
    auto meta_service = get_meta_service();
    auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());
    int64_t db_id = 1987211;
    const std::string& label = "test_clean_label";
    brpc::Controller cntl;
    {
        BeginTxnRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        TxnInfoPB txn_info_pb;
        txn_info_pb.set_db_id(db_id);
        txn_info_pb.set_label(label);
        txn_info_pb.add_table_ids(1234);
        txn_info_pb.set_timeout_ms(36000);
        req.mutable_txn_info()->CopyFrom(txn_info_pb);
        BeginTxnResponse res;
        meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                &res, nullptr);
    }
    CleanTxnLabelRequest req;
    req.set_cloud_unique_id("test_cloud_unique_id");
    req.set_db_id(db_id);
    req.add_labels(label);
    CleanTxnLabelResponse res;

    clear_memkv_count_bytes(mem_kv.get());

    meta_service->clean_txn_label(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                  &res, nullptr);

    LOG(INFO) << "CleanTxnLabel: " << mem_kv->get_count_ << ", " << mem_kv->put_count_ << ", "
              << mem_kv->del_count_ << ", " << mem_kv->get_bytes_ << ", " << mem_kv->put_bytes_
              << ", " << mem_kv->del_bytes_;
    ASSERT_EQ(mem_kv->get_count_, g_bvar_rpc_kv_clean_txn_label_get_counter.get({mock_instance}));
    ASSERT_EQ(mem_kv->put_count_, g_bvar_rpc_kv_clean_txn_label_put_counter.get({mock_instance}));
    ASSERT_EQ(mem_kv->del_count_, g_bvar_rpc_kv_clean_txn_label_del_counter.get({mock_instance}));
    ASSERT_EQ(mem_kv->get_bytes_, g_bvar_rpc_kv_clean_txn_label_get_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->put_bytes_, g_bvar_rpc_kv_clean_txn_label_put_bytes.get({mock_instance}));
    ASSERT_EQ(mem_kv->del_bytes_, g_bvar_rpc_kv_clean_txn_label_del_bytes.get({mock_instance}));
}
} // namespace doris::cloud
