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
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <string>

#include "common/defer.h"
#include "common/util.h"
#include "cpp/sync_point.h"
#include "meta-service/meta_service.h"
#include "meta-store/codec.h"
#include "meta-store/document_message.h"
#include "meta-store/document_message_get_range.h"
#include "meta-store/keys.h"
#include "meta-store/mem_txn_kv.h"
#include "meta-store/txn_kv.h"
#include "meta-store/txn_kv_error.h"
#include "meta-store/versioned_value.h"
#include "mock_accessor.h"
#include "rate-limiter/rate_limiter.h"
#include "recycler/checker.h"
#include "recycler/recycler.h"
#include "recycler/storage_vault_accessor.h"
#include "recycler/util.h"

using namespace doris;
using namespace doris::cloud;

extern doris::cloud::RecyclerThreadPoolGroup thread_group;

namespace {

constexpr int DATA_DISK_SIZE_CONST = 100;
constexpr int INDEX_DISK_SIZE_CONST = 10;
constexpr int DISK_SIZE_CONST = 110;
constexpr std::string_view RESOURCE_ID = "mock_resource_id";

std::unique_ptr<MetaServiceProxy> get_meta_service() {
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

    auto rs = std::make_shared<ResourceManager>(txn_kv);
    auto rl = std::make_shared<RateLimiter>();
    auto meta_service = std::make_unique<MetaServiceImpl>(txn_kv, rs, rl);
    return std::make_unique<MetaServiceProxy>(std::move(meta_service));
}

// Create a MULTI_VERSION_READ_WRITE instance and refresh the resource manager.
void create_and_refresh_instance(MetaServiceProxy* service, std::string instance_id) {
    // write instance
    InstanceInfoPB instance_info;
    instance_info.set_instance_id(instance_id);
    instance_info.set_multi_version_status(MULTI_VERSION_READ_WRITE);
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->put(instance_key(instance_id), instance_info.SerializeAsString());
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    service->resource_mgr()->refresh_instance(instance_id);
    ASSERT_TRUE(service->resource_mgr()->is_version_write_enabled(instance_id));
}

void prepare_and_commit_index(MetaServiceProxy* service, const std::string& cloud_unique_id,
                              int64_t db_id, int64_t table_id, int64_t index_id) {
    IndexRequest request;
    request.set_cloud_unique_id(cloud_unique_id);
    request.set_db_id(db_id);
    request.set_table_id(table_id);
    request.add_index_ids(index_id);

    IndexResponse response;
    brpc::Controller cntl;
    service->prepare_index(&cntl, &request, &response, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.ShortDebugString();

    // Commit index
    service->commit_index(&cntl, &request, &response, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.ShortDebugString();
}

void drop_index(MetaServiceProxy* service, const std::string& cloud_unique_id, int64_t db_id,
                int64_t table_id, int64_t index_id) {
    IndexRequest request;
    request.set_cloud_unique_id(cloud_unique_id);
    request.set_db_id(db_id);
    request.set_table_id(table_id);
    request.add_index_ids(index_id);

    IndexResponse response;
    brpc::Controller cntl;
    service->drop_index(&cntl, &request, &response, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.ShortDebugString();
}

void prepare_and_commit_partition(MetaServiceProxy* service, const std::string& cloud_unique_id,
                                  int64_t db_id, int64_t table_id, int64_t partition_id,
                                  int64_t index_id) {
    PartitionRequest request;
    request.set_cloud_unique_id(cloud_unique_id);
    request.set_db_id(db_id);
    request.set_table_id(table_id);
    request.add_partition_ids(partition_id);
    request.add_index_ids(index_id);

    PartitionResponse response;
    brpc::Controller cntl;
    service->prepare_partition(&cntl, &request, &response, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.ShortDebugString();

    // Commit partition
    service->commit_partition(&cntl, &request, &response, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.ShortDebugString();
}

void drop_partition(MetaServiceProxy* service, const std::string& cloud_unique_id, int64_t db_id,
                    int64_t table_id, int64_t partition_id) {
    PartitionRequest request;
    request.set_cloud_unique_id(cloud_unique_id);
    request.set_db_id(db_id);
    request.set_table_id(table_id);
    request.add_partition_ids(partition_id);

    PartitionResponse response;
    brpc::Controller cntl;
    service->drop_partition(&cntl, &request, &response, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.ShortDebugString();
}

std::string next_rowset_id() {
    static int cnt = 0;
    return std::to_string(++cnt);
}

void add_tablet(CreateTabletsRequest& req, int64_t table_id, int64_t index_id, int64_t partition_id,
                int64_t tablet_id, TabletStatePB state = TabletStatePB::PB_RUNNING) {
    auto tablet = req.add_tablet_metas();
    tablet->set_table_id(table_id);
    tablet->set_index_id(index_id);
    tablet->set_partition_id(partition_id);
    tablet->set_tablet_id(tablet_id);
    tablet->set_tablet_state(state);
    auto schema = tablet->mutable_schema();
    schema->set_schema_version(0);
    auto first_rowset = tablet->add_rs_metas();
    first_rowset->set_rowset_id(0); // required
    first_rowset->set_rowset_id_v2(next_rowset_id());
    first_rowset->set_start_version(0);
    first_rowset->set_end_version(1);
    first_rowset->mutable_tablet_schema()->CopyFrom(*schema);
}

void create_tablet(MetaServiceProxy* meta_service, const std::string& cloud_unique_id,
                   int64_t db_id, int64_t table_id, int64_t index_id, int64_t partition_id,
                   int64_t tablet_id, TabletStatePB state = TabletStatePB::PB_RUNNING) {
    brpc::Controller cntl;
    CreateTabletsRequest req;
    CreateTabletsResponse res;
    req.set_db_id(db_id);
    req.set_cloud_unique_id(cloud_unique_id);
    add_tablet(req, table_id, index_id, partition_id, tablet_id, state);
    meta_service->create_tablets(&cntl, &req, &res, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << tablet_id;
}

void begin_txn(MetaServiceProxy* meta_service, const std::string& cloud_unique_id, int64_t db_id,
               const std::string& label, int64_t table_id, int64_t& txn_id) {
    brpc::Controller cntl;
    BeginTxnRequest req;
    BeginTxnResponse res;
    req.set_cloud_unique_id(cloud_unique_id);
    auto txn_info = req.mutable_txn_info();
    txn_info->set_db_id(db_id);
    txn_info->set_label(label);
    txn_info->add_table_ids(table_id);
    txn_info->set_timeout_ms(36000);
    meta_service->begin_txn(&cntl, &req, &res, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << label;
    ASSERT_TRUE(res.has_txn_id()) << label;
    txn_id = res.txn_id();
}

void commit_txn(MetaServiceProxy* meta_service, const std::string& cloud_unique_id, int64_t db_id,
                int64_t txn_id, const std::string& label) {
    brpc::Controller cntl;
    CommitTxnRequest req;
    CommitTxnResponse res;
    req.set_cloud_unique_id(cloud_unique_id);
    req.set_db_id(db_id);
    req.set_txn_id(txn_id);
    meta_service->commit_txn(&cntl, &req, &res, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
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
    rowset.set_resource_id(std::string(RESOURCE_ID));
    rowset.set_num_segments(1);
    rowset.set_num_rows(num_rows);
    rowset.set_data_disk_size(num_rows * DATA_DISK_SIZE_CONST);
    rowset.set_index_disk_size(num_rows * INDEX_DISK_SIZE_CONST);
    rowset.set_total_disk_size(num_rows * DISK_SIZE_CONST);
    rowset.mutable_tablet_schema()->set_schema_version(0);
    rowset.set_txn_expiration(::time(nullptr)); // Required by DCHECK
    return rowset;
}

void prepare_rowset(MetaServiceProxy* meta_service, const std::string& cloud_unique_id,
                    const doris::RowsetMetaCloudPB& rowset) {
    brpc::Controller cntl;
    CreateRowsetRequest req;
    CreateRowsetResponse resp;
    req.set_cloud_unique_id(cloud_unique_id);
    req.mutable_rowset_meta()->CopyFrom(rowset);
    meta_service->prepare_rowset(&cntl, &req, &resp, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ(resp.status().code(), MetaServiceCode::OK) << rowset.ShortDebugString();
}

void commit_rowset(MetaServiceProxy* meta_service, const std::string& cloud_unique_id,
                   const doris::RowsetMetaCloudPB& rowset) {
    brpc::Controller cntl;
    CreateRowsetRequest req;
    CreateRowsetResponse resp;
    req.set_cloud_unique_id(cloud_unique_id);
    req.mutable_rowset_meta()->CopyFrom(rowset);
    meta_service->commit_rowset(&cntl, &req, &resp, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ(resp.status().code(), MetaServiceCode::OK) << rowset.ShortDebugString();
}

void insert_rowset(MetaServiceProxy* meta_service, const std::string& cloud_unique_id,
                   int64_t db_id, const std::string& label, int64_t table_id, int64_t partition_id,
                   int64_t tablet_id, std::string* rowset_id = nullptr) {
    int64_t txn_id = 0;
    ASSERT_NO_FATAL_FAILURE(
            begin_txn(meta_service, cloud_unique_id, db_id, label, table_id, txn_id));
    auto rowset = create_rowset(txn_id, tablet_id, partition_id);
    if (rowset_id) {
        *rowset_id = rowset.rowset_id_v2();
    }
    ASSERT_NO_FATAL_FAILURE(prepare_rowset(meta_service, cloud_unique_id, rowset));
    ASSERT_NO_FATAL_FAILURE(commit_rowset(meta_service, cloud_unique_id, rowset));
    ASSERT_NO_FATAL_FAILURE(commit_txn(meta_service, cloud_unique_id, db_id, txn_id, label));
}

void insert_rowsets(MetaServiceProxy* meta_service, const std::string& cloud_unique_id,
                    int64_t db_id, const std::string& label, int64_t table_id, int64_t partition_id,
                    std::vector<int64_t> tablet_ids) {
    int64_t txn_id = 0;
    ASSERT_NO_FATAL_FAILURE(
            begin_txn(meta_service, cloud_unique_id, db_id, label, table_id, txn_id));
    for (auto tablet_id : tablet_ids) {
        auto rowset = create_rowset(txn_id, tablet_id, partition_id);
        ASSERT_NO_FATAL_FAILURE(prepare_rowset(meta_service, cloud_unique_id, rowset));
        ASSERT_NO_FATAL_FAILURE(commit_rowset(meta_service, cloud_unique_id, rowset));
    }
    ASSERT_NO_FATAL_FAILURE(commit_txn(meta_service, cloud_unique_id, db_id, txn_id, label));
}

void get_tablet_stats(MetaService* meta_service, const std::string& cloud_unique_id,
                      int64_t tablet_id, TabletStatsPB& stats) {
    brpc::Controller cntl;
    GetTabletStatsRequest req;
    GetTabletStatsResponse res;
    req.set_cloud_unique_id(cloud_unique_id);
    auto idx = req.add_tablet_idx();
    idx->set_tablet_id(tablet_id);
    meta_service->get_tablet_stats(&cntl, &req, &res, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << tablet_id;
    stats = res.tablet_stats(0);
}

void start_compaction_job(MetaService* meta_service, const std::string& cloud_unique_id,
                          int64_t tablet_id, const std::string& job_id,
                          const std::string& initiator, int base_compaction_cnt,
                          int cumu_compaction_cnt, TabletCompactionJobPB::CompactionType type,
                          std::pair<int64_t, int64_t> input_version = {0, 0}) {
    brpc::Controller cntl;
    StartTabletJobRequest req;
    StartTabletJobResponse res;
    req.set_cloud_unique_id(cloud_unique_id);
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
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.ShortDebugString();
}

void finish_compaction_job(MetaService* meta_service, const std::string& cloud_unique_id,
                           int64_t tablet_id, const std::string& job_id,
                           const std::string& initiator, int base_compaction_cnt,
                           int cumu_compaction_cnt, TabletCompactionJobPB::CompactionType type,
                           const std::string& output_rowset_id,
                           std::pair<int64_t, int64_t> input_version, int64_t txn_id) {
    brpc::Controller cntl;
    FinishTabletJobRequest req;
    FinishTabletJobResponse res;
    req.set_cloud_unique_id(cloud_unique_id);
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
    compaction->add_input_versions(input_version.first);
    compaction->add_input_versions(input_version.second);
    compaction->add_output_versions(input_version.second); // [first, second]
    compaction->set_check_input_versions_range(true);
    compaction->add_output_rowset_ids(output_rowset_id);
    compaction->add_txn_id(txn_id);
    req.set_action(FinishTabletJobRequest::COMMIT);
    meta_service->finish_tablet_job(&cntl, &req, &res, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.ShortDebugString();
}

void compact_rowsets_cumulative(MetaServiceProxy* meta_service, const std::string& cloud_unique_id,
                                int64_t db_id, const std::string& label, int64_t table_id,
                                int64_t partition_id, int64_t tablet_id, int64_t start_version,
                                int64_t end_version, int num_rows) {
    TabletStatsPB stats;
    ASSERT_NO_FATAL_FAILURE(get_tablet_stats(meta_service, cloud_unique_id, tablet_id, stats));
    int base_compaction_cnt = stats.base_compaction_cnt();
    int cumu_compaction_cnt = stats.cumulative_compaction_cnt();
    std::string job_id = fmt::format("compaction_{}_{}_{}", tablet_id, start_version, end_version);
    ASSERT_NO_FATAL_FAILURE(start_compaction_job(
            meta_service, cloud_unique_id, tablet_id, job_id, "test_case", base_compaction_cnt,
            cumu_compaction_cnt, TabletCompactionJobPB::CUMULATIVE, {start_version, end_version}));
    int64_t txn_id = 123321;
    doris::RowsetMetaCloudPB compact_rowset =
            create_rowset(txn_id, tablet_id, partition_id, start_version, num_rows);
    std::string output_rowset_id = compact_rowset.rowset_id_v2();
    compact_rowset.set_end_version(end_version);
    ASSERT_NO_FATAL_FAILURE(prepare_rowset(meta_service, cloud_unique_id, compact_rowset));
    ASSERT_NO_FATAL_FAILURE(commit_rowset(meta_service, cloud_unique_id, compact_rowset));
    ASSERT_NO_FATAL_FAILURE(finish_compaction_job(
            meta_service, cloud_unique_id, tablet_id, job_id, "test_case", base_compaction_cnt,
            cumu_compaction_cnt, TabletCompactionJobPB::CUMULATIVE, output_rowset_id,
            {start_version, end_version}, txn_id));
}

void start_schema_change_job(MetaServiceProxy* meta_service, const std::string& cloud_unique_id,
                             int64_t table_id, int64_t index_id, int64_t partition_id,
                             int64_t tablet_id, int64_t new_tablet_id, const std::string& job_id,
                             const std::string& initiator, int64_t alter_version = -1) {
    brpc::Controller cntl;
    StartTabletJobRequest req;
    StartTabletJobResponse res;
    req.set_cloud_unique_id(cloud_unique_id);
    req.mutable_job()->mutable_idx()->set_tablet_id(tablet_id);
    auto sc = req.mutable_job()->mutable_schema_change();
    sc->set_id(job_id);
    sc->set_initiator(initiator);
    sc->mutable_new_tablet_idx()->set_tablet_id(new_tablet_id);
    if (alter_version != -1) {
        sc->set_alter_version(alter_version);
    }
    long now = time(nullptr);
    sc->set_expiration(now + 12);
    meta_service->start_tablet_job(&cntl, &req, &res, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK)
            << job_id << ' ' << initiator << ' ' << res.status().msg();
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK)
            << job_id << ' ' << initiator;
};

void finish_schema_change_job(MetaService* meta_service, const std::string& cloud_unique_id,
                              int64_t tablet_id, int64_t new_tablet_id, const std::string& job_id,
                              const std::string& initiator,
                              const std::vector<doris::RowsetMetaCloudPB>& output_rowsets,
                              FinishTabletJobRequest_Action action = FinishTabletJobRequest::COMMIT,
                              int64_t delete_bitmap_lock_initiator = 12345) {
    brpc::Controller cntl;
    FinishTabletJobRequest req;
    FinishTabletJobResponse res;
    req.set_cloud_unique_id(cloud_unique_id);
    req.set_action(action);
    req.mutable_job()->mutable_idx()->set_tablet_id(tablet_id);
    auto sc = req.mutable_job()->mutable_schema_change();
    sc->mutable_new_tablet_idx()->set_tablet_id(new_tablet_id);
    if (output_rowsets.empty()) {
        sc->set_alter_version(0);
    } else {
        sc->set_alter_version(output_rowsets.back().end_version());
        for (auto& rowset : output_rowsets) {
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
    sc->set_delete_bitmap_lock_initiator(delete_bitmap_lock_initiator);
    meta_service->finish_tablet_job(&cntl, &req, &res, nullptr);
}

void get_rowsets(MetaServiceProxy* meta_service, const std::string& cloud_unique_id,
                 int64_t tablet_id, int64_t start_version, int64_t end_version,
                 std::vector<doris::RowsetMetaCloudPB>& rowsets) {
    TabletStatsPB stats;
    ASSERT_NO_FATAL_FAILURE(get_tablet_stats(meta_service, cloud_unique_id, tablet_id, stats));

    brpc::Controller cntl;
    GetRowsetRequest req;
    GetRowsetResponse res;
    req.set_cloud_unique_id(cloud_unique_id);
    req.set_base_compaction_cnt(stats.base_compaction_cnt());
    req.set_cumulative_compaction_cnt(stats.cumulative_compaction_cnt());
    if (stats.has_full_compaction_cnt()) {
        req.set_full_compaction_cnt(stats.full_compaction_cnt());
    }
    req.set_cumulative_point(stats.cumulative_point());
    req.set_start_version(start_version);
    req.set_end_version(end_version);
    req.mutable_idx()->set_tablet_id(tablet_id);
    meta_service->get_rowset(&cntl, &req, &res, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.ShortDebugString();
    for (int i = 0; i < res.rowset_meta_size(); i++) {
        rowsets.push_back(res.rowset_meta(i));
    }
}

void get_instance(MetaServiceProxy* meta_service, const std::string& cloud_unique_id,
                  InstanceInfoPB& instance_info) {
    brpc::Controller cntl;
    GetInstanceRequest req;
    GetInstanceResponse res;
    req.set_cloud_unique_id(cloud_unique_id);
    meta_service->get_instance(&cntl, &req, &res, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.ShortDebugString();
    ASSERT_TRUE(res.has_instance()) << res.ShortDebugString();
    instance_info.CopyFrom(res.instance());
}

void drop_instance(MetaServiceProxy* meta_service, const std::string& instance_id) {
    brpc::Controller cntl;
    AlterInstanceRequest req;
    AlterInstanceResponse res;
    req.set_instance_id(instance_id);
    req.set_op(AlterInstanceRequest::DROP);
    meta_service->alter_instance(&cntl, &req, &res, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.ShortDebugString();
}

std::unique_ptr<InstanceRecycler> get_instance_recycler(
        MetaServiceProxy* meta_service, const InstanceInfoPB& instance_info,
        std::shared_ptr<StorageVaultAccessor> accessor) {
    auto txn_lazy_committer = std::make_shared<TxnLazyCommitter>(meta_service->txn_kv());
    auto recycler = std::make_unique<InstanceRecycler>(meta_service->txn_kv(), instance_info,
                                                       thread_group, txn_lazy_committer);
    recycler->TEST_add_accessor(RESOURCE_ID, accessor);
    return recycler;
}

std::unique_ptr<InstanceRecycler> get_instance_recycler(MetaServiceProxy* meta_service,
                                                        const InstanceInfoPB& instance_info) {
    std::shared_ptr<StorageVaultAccessor> accessor = std::make_shared<MockAccessor>();
    return get_instance_recycler(meta_service, instance_info, accessor);
}

// Convert a string to a hex-escaped string.
// A non-displayed character is represented as \xHH where HH is the hexadecimal value of the character.
// A displayed character is represented as itself.
std::string escape_hex(std::string_view data) {
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

size_t count_range(TxnKv* txn_kv, std::string_view begin = "", std::string_view end = "\xFF") {
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

std::string dump_range(TxnKv* txn_kv, std::string_view begin = "", std::string_view end = "\xFF") {
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

void check_rowset_meta(TxnKv* txn_kv, std::string_view instance_id, int64_t tablet_id,
                       int64_t start_version, int64_t end_version) {
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

    // 1. there is no overlap
    std::map<int64_t, RowsetMetaCloudPB> rowset_graph;
    {
        std::string start_key =
                versioned::meta_rowset_load_key({instance_id, tablet_id, start_version});
        std::string end_key =
                versioned::meta_rowset_load_key({instance_id, tablet_id, end_version});

        // [start, end]
        versioned::ReadDocumentMessagesOptions options;
        options.exclude_begin_key = false;
        options.exclude_end_key = false;

        auto iter = versioned::document_get_range<RowsetMetaCloudPB>(txn.get(), start_key, end_key,
                                                                     options);
        for (auto&& kvp = iter->next(); kvp.has_value(); kvp = iter->next()) {
            auto&& [key, version, rowset_meta] = *kvp;
            auto result = rowset_graph.emplace(rowset_meta.end_version(), std::move(rowset_meta));
            EXPECT_TRUE(result.second) << "insert load rowset meta: " << rowset_meta.end_version();
        }
        ASSERT_TRUE(iter->is_valid())
                << "failed to get loaded rowset metas, error_code=" << iter->error_code();
    }

    {
        std::string start_key =
                versioned::meta_rowset_compact_key({instance_id, tablet_id, start_version});
        std::string end_key =
                versioned::meta_rowset_compact_key({instance_id, tablet_id, end_version});

        // [start, end]
        versioned::ReadDocumentMessagesOptions options;
        options.exclude_begin_key = false;
        options.exclude_end_key = false;

        int64_t last_start_version = std::numeric_limits<int64_t>::max();
        auto iter = versioned::document_get_range<RowsetMetaCloudPB>(txn.get(), start_key, end_key,
                                                                     options);
        for (auto&& kvp = iter->next(); kvp.has_value(); kvp = iter->next()) {
            auto&& [key, version, rowset_meta] = *kvp;
            int64_t start_version = rowset_meta.start_version();
            int64_t end_version = rowset_meta.end_version();
            EXPECT_GT(last_start_version, start_version)
                    << "This compact rowset has been covered by a large compact rowset";

            last_start_version = start_version;
            // erase the rowsets that are covered by this compact rowset
            auto lower = rowset_graph.lower_bound(start_version);
            auto upper = rowset_graph.upper_bound(end_version);
            EXPECT_EQ(lower, upper)
                    << "There is overlap between rowsets, start_version=" << start_version
                    << ", end_version=" << end_version;
            rowset_graph.erase(lower, upper);
            auto result = rowset_graph.emplace(end_version, std::move(rowset_meta));
            EXPECT_TRUE(result.second) << "insert load rowset meta: " << rowset_meta.end_version();
        }
        ASSERT_TRUE(iter->is_valid())
                << "failed to get compact rowset metas, error_code=" << iter->error_code();
    }

    // 2. there is no hole
    int64_t expect_version = start_version;
    for (auto&& [end_version, rowset_meta] : rowset_graph) {
        EXPECT_EQ(rowset_meta.start_version(), expect_version)
                << "There is a hole between rowsets, expected start_version=" << expect_version
                << ", but got " << rowset_meta.start_version();
        expect_version = end_version + 1;
    }

    // 3. the data ref count is correct
    for (auto&& [end_version, rowset_meta] : rowset_graph) {
        std::string data_reference_key = versioned::data_rowset_ref_count_key(
                {instance_id, tablet_id, rowset_meta.rowset_id_v2()});
        std::string data_reference_value;
        auto rc = txn->get(data_reference_key, &data_reference_value);
        ASSERT_EQ(rc, TxnErrorCode::TXN_OK)
                << "failed to get data reference key: " << data_reference_key;
    }
}

void check_no_specified_rowset_meta(TxnKv* txn_kv, std::string_view instance_id, int64_t tablet_id,
                                    const std::vector<std::string>& rowset_ids) {
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    for (auto rowset_id : rowset_ids) {
        std::string data_reference_key =
                versioned::data_rowset_ref_count_key({instance_id, tablet_id, rowset_id});
        std::string data_reference_value;
        auto rc = txn->get(data_reference_key, &data_reference_value);
        ASSERT_EQ(rc, TxnErrorCode::TXN_KEY_NOT_FOUND)
                << "failed to get data reference key: " << data_reference_key;
    }
}

void versioned_get_all(TxnKv* txn_kv, std::string key,
                       std::vector<std::pair<std::string, Versionstamp>>& values) {
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string begin_key = encode_versioned_key(key, Versionstamp::min());
    std::string end_key = encode_versioned_key(key, Versionstamp::max());

    FullRangeGetOptions opts;
    auto iter = txn->full_range_get(begin_key, end_key, opts);
    for (auto&& kvp = iter->next(); kvp.has_value(); kvp = iter->next()) {
        auto&& [key, value] = *kvp;
        std::string_view key_part(key);
        Versionstamp version;
        EXPECT_EQ(decode_tailing_versionstamp_end(&key_part), 0);
        EXPECT_EQ(decode_tailing_versionstamp(&key_part, &version), 0);
        values.emplace_back(value, version);
    }
    ASSERT_TRUE(iter->is_valid()) << "failed to get all versioned values, error_code="
                                  << iter->error_code();
}

void check_partition_resources(TxnKv* txn_kv, std::string_view instance_id, int64_t table_id,
                               int64_t index_id, int64_t partition_id) {
    // 1. check table version
    std::vector<std::pair<std::string, Versionstamp>> values;
    std::string table_version_key = versioned::table_version_key({instance_id, table_id});
    ASSERT_NO_FATAL_FAILURE(versioned_get_all(txn_kv, table_version_key, values));
    ASSERT_EQ(values.size(), 1) << "There should be only one table version";

    // 2. check partition version
    values.clear();
    std::string partition_version_key =
            versioned::partition_version_key({instance_id, partition_id});
    ASSERT_NO_FATAL_FAILURE(versioned_get_all(txn_kv, partition_version_key, values));
    ASSERT_EQ(values.size(), 1) << "There should be only one partition version";

    // 3. check index meta
    values.clear();
    std::string index_meta_key = versioned::meta_index_key({instance_id, index_id});
    ASSERT_NO_FATAL_FAILURE(versioned_get_all(txn_kv, index_meta_key, values));
    ASSERT_EQ(values.size(), 1) << "There should be only one index meta";

    // 4. check partition meta
    values.clear();
    std::string partition_meta_key = versioned::meta_partition_key({instance_id, partition_id});
    ASSERT_NO_FATAL_FAILURE(versioned_get_all(txn_kv, partition_meta_key, values));
    ASSERT_EQ(values.size(), 1) << "There should be only one partition meta";
}

void check_tablet_meta(TxnKv* txn_kv, std::string_view instance_id, int64_t tablet_id) {
    // 1. check tablet meta
    std::vector<std::pair<std::string, Versionstamp>> values;
    std::string tablet_meta_key = versioned::meta_tablet_key({instance_id, tablet_id});
    ASSERT_NO_FATAL_FAILURE(versioned_get_all(txn_kv, tablet_meta_key, values));
    ASSERT_EQ(values.size(), 1) << "There should be only one tablet meta, " << dump_range(txn_kv);

    // 2. check tablet load stats
    values.clear();
    std::string tablet_stats_key = versioned::tablet_load_stats_key({instance_id, tablet_id});
    ASSERT_NO_FATAL_FAILURE(versioned_get_all(txn_kv, tablet_stats_key, values));
    ASSERT_EQ(values.size(), 1) << "There should be only one load stats key";

    // 3. check tablet compaction stats
    values.clear();
    std::string tablet_compaction_stats_key =
            versioned::tablet_compact_stats_key({instance_id, tablet_id});
    ASSERT_NO_FATAL_FAILURE(versioned_get_all(txn_kv, tablet_compaction_stats_key, values));
    ASSERT_EQ(values.size(), 1) << "There should be only one compaction stats key";
}

} // namespace

#define MOCK_GET_INSTANCE_ID(instance_id)                                          \
    DORIS_CLOUD_DEFER {                                                            \
        SyncPoint::get_instance()->clear_all_call_backs();                         \
    };                                                                             \
    SyncPoint::get_instance()->set_call_back("get_instance_id", [&](auto&& args) { \
        auto* ret = try_any_cast_ret<std::string>(args);                           \
        ret->first = instance_id;                                                  \
        ret->second = true;                                                        \
    });                                                                            \
    SyncPoint::get_instance()->enable_processing();

// A test that simulates a compaction operation and recycles the input rowsets.
TEST(RecycleVersionedKeysTest, RecycleRowset_Compaction) {
    auto meta_service = get_meta_service();
    auto txn_kv = meta_service->txn_kv();
    std::string instance_id = "recycle_rowset_test_instance";
    std::string cloud_unique_id = fmt::format("1:{}:0", instance_id);
    MOCK_GET_INSTANCE_ID(instance_id);
    ASSERT_NO_FATAL_FAILURE(create_and_refresh_instance(meta_service.get(), instance_id));

    int64_t db_id = 1, table_id = 2, index_id = 3, partition_id = 4, tablet_id = 5;

    {
        // create partition/index/tablet
        ASSERT_NO_FATAL_FAILURE(prepare_and_commit_index(meta_service.get(), cloud_unique_id, db_id,
                                                         table_id, index_id));
        ASSERT_NO_FATAL_FAILURE(prepare_and_commit_partition(
                meta_service.get(), cloud_unique_id, db_id, table_id, partition_id, index_id));
        ASSERT_NO_FATAL_FAILURE(create_tablet(meta_service.get(), cloud_unique_id, db_id, table_id,
                                              index_id, partition_id, tablet_id));
    }

    size_t num_input_rowsets = 3;
    std::vector<int64_t> input_versions;
    std::vector<std::string> input_rowset_ids;
    {
        // insert some rowsets
        for (size_t i = 0; i < num_input_rowsets; ++i) {
            std::string rowset_id;
            ASSERT_NO_FATAL_FAILURE(insert_rowset(meta_service.get(), cloud_unique_id, db_id,
                                                  fmt::format("label_{}", i), table_id,
                                                  partition_id, tablet_id, &rowset_id));
            input_versions.push_back(i + 2); // versions start from 2
            input_rowset_ids.push_back(rowset_id);
        }

        // insert other rowsets
        for (size_t i = 0; i < 2; ++i) {
            ASSERT_NO_FATAL_FAILURE(insert_rowset(meta_service.get(), cloud_unique_id, db_id,
                                                  fmt::format("other_label_{}", i), table_id,
                                                  partition_id, tablet_id));
        }
    }

    int64_t start_version = input_versions.front();
    int64_t end_version = input_versions.back();
    int64_t last_version = end_version + 2;
    {
        // simulate a compaction operation and recycle the input rowsets
        ASSERT_NO_FATAL_FAILURE(compact_rowsets_cumulative(
                meta_service.get(), cloud_unique_id, db_id, "compaction_label", table_id,
                partition_id, tablet_id, start_version, end_version,
                300)); // num_rows for output rowset
    }

    {
        // recycle the rowsets
        config::force_immediate_recycle = true;
        DORIS_CLOUD_DEFER {
            config::force_immediate_recycle = false;
        };

        InstanceInfoPB instance_info;
        ASSERT_NO_FATAL_FAILURE(get_instance(meta_service.get(), cloud_unique_id, instance_info));
        auto recycler = get_instance_recycler(meta_service.get(), instance_info);
        ASSERT_EQ(recycler->recycle_operation_logs(), 0);
        ASSERT_EQ(recycler->recycle_rowsets(), 0);
    }

    ASSERT_NO_FATAL_FAILURE({
        // The input rowsets are recycled.
        check_no_specified_rowset_meta(txn_kv.get(), instance_id, tablet_id, input_rowset_ids);
        check_rowset_meta(txn_kv.get(), instance_id, tablet_id, start_version, last_version);
        check_partition_resources(txn_kv.get(), instance_id, table_id, index_id, partition_id);
        check_tablet_meta(txn_kv.get(), instance_id, tablet_id);
    });
}

// A test that simulates a schema change operation
TEST(RecycleVersionedKeysTest, RecycleRowset_SchemaChange) {
    auto meta_service = get_meta_service();
    auto txn_kv = meta_service->txn_kv();
    std::string instance_id = "recycle_schema_change_test_instance";
    std::string cloud_unique_id = fmt::format("1:{}:0", instance_id);
    MOCK_GET_INSTANCE_ID(instance_id);
    ASSERT_NO_FATAL_FAILURE(create_and_refresh_instance(meta_service.get(), instance_id));

    int64_t db_id = 1, table_id = 2, index_id = 3, partition_id = 4, tablet_id = 5;
    int64_t new_tablet_id = 6;

    {
        // create partition/index/tablet
        ASSERT_NO_FATAL_FAILURE(prepare_and_commit_index(meta_service.get(), cloud_unique_id, db_id,
                                                         table_id, index_id));
        ASSERT_NO_FATAL_FAILURE(prepare_and_commit_partition(
                meta_service.get(), cloud_unique_id, db_id, table_id, partition_id, index_id));
        ASSERT_NO_FATAL_FAILURE(create_tablet(meta_service.get(), cloud_unique_id, db_id, table_id,
                                              index_id, partition_id, tablet_id));
    }

    size_t num_input_rowsets = 4;
    std::vector<int64_t> input_versions;
    {
        // insert some rowsets
        for (size_t i = 0; i < num_input_rowsets; ++i) {
            ASSERT_NO_FATAL_FAILURE(insert_rowset(meta_service.get(), cloud_unique_id, db_id,
                                                  fmt::format("label_{}", i), table_id,
                                                  partition_id, tablet_id));
            input_versions.push_back(i + 2); // versions start from 2
        }
    }

    {
        // Create new tablet and insert some rowsets to both new and old tablet.
        ASSERT_NO_FATAL_FAILURE(create_tablet(meta_service.get(), cloud_unique_id, db_id, table_id,
                                              index_id, partition_id, new_tablet_id,
                                              TabletStatePB::PB_NOTREADY));
        for (size_t i = 0; i < 2; ++i) {
            ASSERT_NO_FATAL_FAILURE(insert_rowsets(meta_service.get(), cloud_unique_id, db_id,
                                                   fmt::format("new_label_{}", i), table_id,
                                                   partition_id, {new_tablet_id, tablet_id}));
        }
    }

    std::vector<doris::RowsetMetaCloudPB> output_rowsets;
    {
        // simulate a schema change operation and recycle the input rowsets
        std::string job_id = fmt::format("schema_change_{}_{}", tablet_id, new_tablet_id);
        int64_t alter_version = input_versions.back();

        ASSERT_NO_FATAL_FAILURE(start_schema_change_job(
                meta_service.get(), cloud_unique_id, table_id, index_id, partition_id, tablet_id,
                new_tablet_id, job_id, "test_case", alter_version));

        // Create output rowsets for new tablet
        for (auto version : input_versions) {
            int64_t txn_id = 100000 + version; // Use different txn_id for schema change output
            auto output_rowset = create_rowset(txn_id, new_tablet_id, partition_id, version, 100);
            output_rowsets.push_back(output_rowset);
            ASSERT_NO_FATAL_FAILURE(
                    prepare_rowset(meta_service.get(), cloud_unique_id, output_rowset));
            ASSERT_NO_FATAL_FAILURE(
                    commit_rowset(meta_service.get(), cloud_unique_id, output_rowset));
        }

        ASSERT_NO_FATAL_FAILURE(finish_schema_change_job(meta_service.get(), cloud_unique_id,
                                                         tablet_id, new_tablet_id, job_id,
                                                         "test_case", output_rowsets));
    }

    {
        // recycle the rowsets
        config::force_immediate_recycle = true;
        DORIS_CLOUD_DEFER {
            config::force_immediate_recycle = false;
        };

        InstanceInfoPB instance_info;
        ASSERT_NO_FATAL_FAILURE(get_instance(meta_service.get(), cloud_unique_id, instance_info));
        auto recycler = get_instance_recycler(meta_service.get(), instance_info);
        ASSERT_EQ(recycler->recycle_operation_logs(), 0);
        ASSERT_EQ(recycler->recycle_rowsets(), 0);
    }

    ASSERT_NO_FATAL_FAILURE({
        int64_t start_version = input_versions.front();
        int64_t end_version = input_versions.back() + 2;
        check_rowset_meta(txn_kv.get(), instance_id, tablet_id, start_version, end_version);
        check_rowset_meta(txn_kv.get(), instance_id, new_tablet_id, start_version, end_version);
        check_tablet_meta(txn_kv.get(), instance_id, tablet_id);
        check_tablet_meta(txn_kv.get(), instance_id, new_tablet_id);
        check_partition_resources(txn_kv.get(), instance_id, table_id, index_id, partition_id);
    });

    {
        std::vector<RowsetMetaCloudPB> rowsets;
        ASSERT_NO_FATAL_FAILURE(get_rowsets(meta_service.get(), cloud_unique_id, new_tablet_id,
                                            input_versions.front(), input_versions.back(),
                                            rowsets));
        ASSERT_EQ(rowsets.size(), output_rowsets.size());
        for (size_t i = 0; i < rowsets.size(); ++i) {
            ASSERT_EQ(rowsets[i].rowset_id_v2(), output_rowsets[i].rowset_id_v2());
        }
    }
}

// A test that simulates a schema change operation and recycles the input rowsets.
TEST(RecycleVersionedKeysTest, RecycleRowset_SchemaChangeOverwrite) {
    auto meta_service = get_meta_service();
    auto txn_kv = meta_service->txn_kv();
    std::string instance_id = "recycle_schema_change_overwrite_test_instance";
    std::string cloud_unique_id = fmt::format("1:{}:0", instance_id);
    MOCK_GET_INSTANCE_ID(instance_id);
    ASSERT_NO_FATAL_FAILURE(create_and_refresh_instance(meta_service.get(), instance_id));

    int64_t db_id = 1, table_id = 2, index_id = 3, partition_id = 4, tablet_id = 5;
    int64_t new_tablet_id = 6;

    {
        // create partition/index/tablet
        ASSERT_NO_FATAL_FAILURE(prepare_and_commit_index(meta_service.get(), cloud_unique_id, db_id,
                                                         table_id, index_id));
        ASSERT_NO_FATAL_FAILURE(prepare_and_commit_partition(
                meta_service.get(), cloud_unique_id, db_id, table_id, partition_id, index_id));
        ASSERT_NO_FATAL_FAILURE(create_tablet(meta_service.get(), cloud_unique_id, db_id, table_id,
                                              index_id, partition_id, tablet_id));
    }

    int64_t start_version = 2, end_version = 1;
    {
        // insert some rowsets
        for (size_t i = 0; i < 4; ++i) {
            ASSERT_NO_FATAL_FAILURE(insert_rowset(meta_service.get(), cloud_unique_id, db_id,
                                                  fmt::format("label_{}", i), table_id,
                                                  partition_id, tablet_id));
            end_version += 1;
        }
    }

    {
        // Create new tablet and insert some rowsets to both new and old tablet.
        ASSERT_NO_FATAL_FAILURE(create_tablet(meta_service.get(), cloud_unique_id, db_id, table_id,
                                              index_id, partition_id, new_tablet_id,
                                              TabletStatePB::PB_NOTREADY));
        for (size_t i = 0; i < 2; ++i) {
            ASSERT_NO_FATAL_FAILURE(insert_rowsets(meta_service.get(), cloud_unique_id, db_id,
                                                   fmt::format("new_label_{}", i), table_id,
                                                   partition_id, {new_tablet_id, tablet_id}));
            end_version += 1;
        }
    }

    std::vector<doris::RowsetMetaCloudPB> output_rowsets;
    {
        // simulate a schema change operation and recycle the input rowsets
        std::string job_id = fmt::format("schema_change_{}_{}", tablet_id, new_tablet_id);
        int64_t alter_version = end_version;

        ASSERT_NO_FATAL_FAILURE(start_schema_change_job(
                meta_service.get(), cloud_unique_id, table_id, index_id, partition_id, tablet_id,
                new_tablet_id, job_id, "test_case", alter_version));

        // Create output rowsets for new tablet
        for (int64_t version = start_version; version <= end_version; ++version) {
            int64_t txn_id = 100000 + version; // Use different txn_id for schema change output
            auto output_rowset = create_rowset(txn_id, new_tablet_id, partition_id, version, 100);
            output_rowsets.push_back(output_rowset);
            ASSERT_NO_FATAL_FAILURE(
                    prepare_rowset(meta_service.get(), cloud_unique_id, output_rowset));
            ASSERT_NO_FATAL_FAILURE(
                    commit_rowset(meta_service.get(), cloud_unique_id, output_rowset));
        }

        ASSERT_NO_FATAL_FAILURE(finish_schema_change_job(meta_service.get(), cloud_unique_id,
                                                         tablet_id, new_tablet_id, job_id,
                                                         "test_case", output_rowsets));
    }

    {
        // recycle the rowsets
        config::force_immediate_recycle = true;
        DORIS_CLOUD_DEFER {
            config::force_immediate_recycle = false;
        };

        InstanceInfoPB instance_info;
        ASSERT_NO_FATAL_FAILURE(get_instance(meta_service.get(), cloud_unique_id, instance_info));
        auto recycler = get_instance_recycler(meta_service.get(), instance_info);
        ASSERT_EQ(recycler->recycle_operation_logs(), 0);
        ASSERT_EQ(recycler->recycle_rowsets(), 0);
    }

    ASSERT_NO_FATAL_FAILURE({
        check_rowset_meta(txn_kv.get(), instance_id, tablet_id, start_version, end_version);
        check_rowset_meta(txn_kv.get(), instance_id, new_tablet_id, start_version, end_version);
        check_tablet_meta(txn_kv.get(), instance_id, tablet_id);
        check_tablet_meta(txn_kv.get(), instance_id, new_tablet_id);
        check_partition_resources(txn_kv.get(), instance_id, table_id, index_id, partition_id);
    });

    {
        std::vector<RowsetMetaCloudPB> rowsets;
        ASSERT_NO_FATAL_FAILURE(get_rowsets(meta_service.get(), cloud_unique_id, new_tablet_id,
                                            start_version, end_version, rowsets));
        ASSERT_EQ(rowsets.size(), output_rowsets.size());
        for (size_t i = 0; i < rowsets.size(); ++i) {
            ASSERT_EQ(rowsets[i].rowset_id_v2(), output_rowsets[i].rowset_id_v2());
        }
    }
}

// A test that simulates a schema change operation, a compaction and recycles the input rowsets.
TEST(RecycleVersionedKeysTest, RecycleRowset_SchemaChangeAndCompaction) {
    auto meta_service = get_meta_service();
    auto txn_kv = meta_service->txn_kv();
    std::string instance_id = "recycle_schema_change_compaction_test_instance";
    std::string cloud_unique_id = fmt::format("1:{}:0", instance_id);
    MOCK_GET_INSTANCE_ID(instance_id);
    ASSERT_NO_FATAL_FAILURE(create_and_refresh_instance(meta_service.get(), instance_id));

    int64_t db_id = 1, table_id = 2, index_id = 3, partition_id = 4, tablet_id = 5;
    int64_t new_tablet_id = 6;

    {
        // create partition/index/tablet
        ASSERT_NO_FATAL_FAILURE(prepare_and_commit_index(meta_service.get(), cloud_unique_id, db_id,
                                                         table_id, index_id));
        ASSERT_NO_FATAL_FAILURE(prepare_and_commit_partition(
                meta_service.get(), cloud_unique_id, db_id, table_id, partition_id, index_id));
        ASSERT_NO_FATAL_FAILURE(create_tablet(meta_service.get(), cloud_unique_id, db_id, table_id,
                                              index_id, partition_id, tablet_id));
    }

    size_t num_input_rowsets = 5;
    std::vector<int64_t> input_versions;
    {
        // insert some rowsets
        for (size_t i = 0; i < num_input_rowsets; ++i) {
            ASSERT_NO_FATAL_FAILURE(insert_rowset(meta_service.get(), cloud_unique_id, db_id,
                                                  fmt::format("label_{}", i), table_id,
                                                  partition_id, tablet_id));
            input_versions.push_back(i + 2); // versions start from 2
        }
    }

    std::vector<doris::RowsetMetaCloudPB> schema_change_output_rowsets;
    {
        // simulate a schema change operation and recycle the input rowsets
        std::string job_id = fmt::format("schema_change_{}_{}", tablet_id, new_tablet_id);
        int64_t alter_version = input_versions.back();

        ASSERT_NO_FATAL_FAILURE(create_tablet(meta_service.get(), cloud_unique_id, db_id, table_id,
                                              index_id, partition_id, new_tablet_id,
                                              TabletStatePB::PB_NOTREADY));

        ASSERT_NO_FATAL_FAILURE(start_schema_change_job(
                meta_service.get(), cloud_unique_id, table_id, index_id, partition_id, tablet_id,
                new_tablet_id, job_id, "test_case", alter_version));

        // Create output rowsets for new tablet
        for (auto version : input_versions) {
            int64_t txn_id = 100000 + version; // Use different txn_id for schema change output
            auto output_rowset = create_rowset(txn_id, new_tablet_id, partition_id, version, 100);
            schema_change_output_rowsets.push_back(output_rowset);
            ASSERT_NO_FATAL_FAILURE(
                    prepare_rowset(meta_service.get(), cloud_unique_id, output_rowset));
            ASSERT_NO_FATAL_FAILURE(
                    commit_rowset(meta_service.get(), cloud_unique_id, output_rowset));
        }

        ASSERT_NO_FATAL_FAILURE(finish_schema_change_job(
                meta_service.get(), cloud_unique_id, tablet_id, new_tablet_id, job_id, "test_case",
                schema_change_output_rowsets));
    }

    int64_t compaction_start_version = input_versions[1]; // Start from version 3
    int64_t compaction_end_version = input_versions[3];   // End at version 5
    {
        // simulate a compaction operation and recycle the input rowsets
        ASSERT_NO_FATAL_FAILURE(compact_rowsets_cumulative(
                meta_service.get(), cloud_unique_id, db_id, "compaction_label", table_id,
                partition_id, new_tablet_id, compaction_start_version, compaction_end_version,
                300)); // num_rows for output rowset
    }

    {
        // recycle the rowsets
        config::force_immediate_recycle = true;
        DORIS_CLOUD_DEFER {
            config::force_immediate_recycle = false;
        };

        InstanceInfoPB instance_info;
        ASSERT_NO_FATAL_FAILURE(get_instance(meta_service.get(), cloud_unique_id, instance_info));
        auto recycler = get_instance_recycler(meta_service.get(), instance_info);
        ASSERT_EQ(recycler->recycle_operation_logs(), 0);
        ASSERT_EQ(recycler->recycle_rowsets(), 0);
    }

    ASSERT_NO_FATAL_FAILURE({
        int64_t start_version = input_versions.front();
        int64_t end_version = input_versions.back();
        check_rowset_meta(txn_kv.get(), instance_id, tablet_id, start_version, end_version);
        check_rowset_meta(txn_kv.get(), instance_id, new_tablet_id, start_version, end_version);
        check_tablet_meta(txn_kv.get(), instance_id, tablet_id);
        check_tablet_meta(txn_kv.get(), instance_id, new_tablet_id);
        check_partition_resources(txn_kv.get(), instance_id, table_id, index_id, partition_id);
    });
}

// TODO: pass this test
// A test that simulates a drop index operation.
TEST(RecycleVersionedKeysTest, DISABLED_RecycleIndex) {
    auto meta_service = get_meta_service();
    auto txn_kv = meta_service->txn_kv();
    std::string instance_id = "recycle_index_test_instance";
    std::string cloud_unique_id = fmt::format("1:{}:0", instance_id);
    MOCK_GET_INSTANCE_ID(instance_id);
    ASSERT_NO_FATAL_FAILURE(create_and_refresh_instance(meta_service.get(), instance_id));

    int64_t db_id = 1, table_id = 2, index_id = 3, partition_id = 4, tablet_id = 5;
    {
        // prepare index, partition, and tablets
        ASSERT_NO_FATAL_FAILURE(prepare_and_commit_index(meta_service.get(), cloud_unique_id, db_id,
                                                         table_id, index_id));
        ASSERT_NO_FATAL_FAILURE(prepare_and_commit_partition(
                meta_service.get(), cloud_unique_id, db_id, table_id, partition_id, index_id));
        ASSERT_NO_FATAL_FAILURE(create_tablet(meta_service.get(), cloud_unique_id, db_id, table_id,
                                              index_id, partition_id, tablet_id));
    }

    size_t num_rowsets = 5;
    {
        // insert some rowsets
        for (size_t i = 0; i < num_rowsets; ++i) {
            ASSERT_NO_FATAL_FAILURE(insert_rowset(meta_service.get(), cloud_unique_id, db_id,
                                                  fmt::format("label_{}", i), table_id,
                                                  partition_id, tablet_id));
        }
    }

    {
        // drop index
        ASSERT_NO_FATAL_FAILURE(
                drop_index(meta_service.get(), cloud_unique_id, db_id, table_id, index_id));
    }

    {
        // run recycler to recycle the dropped index
        config::force_immediate_recycle = true;
        DORIS_CLOUD_DEFER {
            config::force_immediate_recycle = false;
        };

        InstanceInfoPB instance_info;
        ASSERT_NO_FATAL_FAILURE(get_instance(meta_service.get(), cloud_unique_id, instance_info));
        auto recycler = get_instance_recycler(meta_service.get(), instance_info);
        ASSERT_EQ(recycler->recycle_operation_logs(), 0);
        ASSERT_EQ(recycler->recycle_indexes(), 0);
    }

    {
        // check the rowsets and tablets are recycled
        std::string meta_key = versioned::meta_key_prefix(instance_id);
        std::string meta_key_end = versioned::meta_key_prefix(instance_id + '\x00');
        ASSERT_EQ(count_range(txn_kv.get(), meta_key, meta_key_end), 0)
                << dump_range(txn_kv.get(), meta_key, meta_key_end);

        std::string data_key = versioned::data_key_prefix(instance_id);
        std::string data_key_end = versioned::data_key_prefix(instance_id + '\x00');
        ASSERT_EQ(count_range(txn_kv.get(), data_key, data_key_end), 0)
                << dump_range(txn_kv.get(), data_key, data_key_end);

        std::string index_key = versioned::index_key_prefix(instance_id);
        std::string index_key_end = versioned::index_key_prefix(instance_id + '\x00');
        ASSERT_EQ(count_range(txn_kv.get(), index_key, index_key_end), 0)
                << dump_range(txn_kv.get(), index_key, index_key_end);

        std::string stats_key = versioned::stats_key_prefix(instance_id);
        std::string stats_key_end = versioned::stats_key_prefix(instance_id + '\x00');
        ASSERT_EQ(count_range(txn_kv.get(), stats_key, stats_key_end), 0)
                << dump_range(txn_kv.get(), stats_key, stats_key_end);
    }
}

// A test that simulates a drop partition operation.
TEST(RecycleVersionedKeysTest, DISABLED_RecyclePartition) {
    auto meta_service = get_meta_service();
    auto txn_kv = meta_service->txn_kv();
    std::string instance_id = "recycle_partition_test_instance";
    std::string cloud_unique_id = fmt::format("1:{}:0", instance_id);
    MOCK_GET_INSTANCE_ID(instance_id);
    ASSERT_NO_FATAL_FAILURE(create_and_refresh_instance(meta_service.get(), instance_id));

    int64_t db_id = 1, table_id = 2, index_id = 3, partition_id = 4, tablet_id = 5;
    {
        // create partition/index/tablet
        ASSERT_NO_FATAL_FAILURE(prepare_and_commit_index(meta_service.get(), cloud_unique_id, db_id,
                                                         table_id, index_id));
        ASSERT_NO_FATAL_FAILURE(prepare_and_commit_partition(
                meta_service.get(), cloud_unique_id, db_id, table_id, partition_id, index_id));
        ASSERT_NO_FATAL_FAILURE(create_tablet(meta_service.get(), cloud_unique_id, db_id, table_id,
                                              index_id, partition_id, tablet_id));
    }

    size_t num_rowsets = 3;
    {
        // insert some rowsets
        for (size_t i = 0; i < num_rowsets; ++i) {
            ASSERT_NO_FATAL_FAILURE(insert_rowset(meta_service.get(), cloud_unique_id, db_id,
                                                  fmt::format("label_{}", i), table_id,
                                                  partition_id, tablet_id));
        }
    }

    {
        // drop partition
        ASSERT_NO_FATAL_FAILURE(
                drop_partition(meta_service.get(), cloud_unique_id, db_id, table_id, partition_id));
    }

    {
        // run recycler to recycle the dropped partition
        InstanceInfoPB instance_info;
        ASSERT_NO_FATAL_FAILURE(get_instance(meta_service.get(), cloud_unique_id, instance_info));
        auto recycler = get_instance_recycler(meta_service.get(), instance_info);
        ASSERT_EQ(recycler->recycle_operation_logs(), 0);
        ASSERT_EQ(recycler->recycle_partitions(), 0);
    }

    {
        // check the rowsets and tablets are recycled
        std::string meta_key = versioned::meta_key_prefix(instance_id);
        std::string meta_key_end = versioned::meta_key_prefix(instance_id + '\x00');
        ASSERT_EQ(count_range(txn_kv.get(), meta_key, meta_key_end), 0) << dump_range(txn_kv.get());

        std::string data_key = versioned::data_key_prefix(instance_id);
        std::string data_key_end = versioned::data_key_prefix(instance_id + '\x00');
        ASSERT_EQ(count_range(txn_kv.get(), data_key, data_key_end), 0) << dump_range(txn_kv.get());

        std::string index_key = versioned::index_key_prefix(instance_id);
        std::string index_key_end = versioned::index_key_prefix(instance_id + '\x00');
        ASSERT_EQ(count_range(txn_kv.get(), index_key, index_key_end), 0)
                << dump_range(txn_kv.get());

        std::string stats_key = versioned::stats_key_prefix(instance_id);
        std::string stats_key_end = versioned::stats_key_prefix(instance_id + '\x00');
        ASSERT_EQ(count_range(txn_kv.get(), stats_key, stats_key_end), 0)
                << dump_range(txn_kv.get());
    }
}

// A test that simulates a recycle operation on a deleted instance.
TEST(RecycleVersionedKeysTest, RecycleDeletedInstance) {
    auto meta_service = get_meta_service();
    auto txn_kv = meta_service->txn_kv();
    std::string instance_id = "recycle_deleted_instance_test_instance";
    std::string cloud_unique_id = fmt::format("1:{}:0", instance_id);
    MOCK_GET_INSTANCE_ID(instance_id);
    ASSERT_NO_FATAL_FAILURE(create_and_refresh_instance(meta_service.get(), instance_id));

    int64_t db_id = 1, table_id = 2, index_id = 3, partition_id = 4, tablet_id = 5;

    {
        // prepare index, partition, and tablets
        ASSERT_NO_FATAL_FAILURE(prepare_and_commit_index(meta_service.get(), cloud_unique_id, db_id,
                                                         table_id, index_id));
        ASSERT_NO_FATAL_FAILURE(prepare_and_commit_partition(
                meta_service.get(), cloud_unique_id, db_id, table_id, partition_id, index_id));
        ASSERT_NO_FATAL_FAILURE(create_tablet(meta_service.get(), cloud_unique_id, db_id, table_id,
                                              index_id, partition_id, tablet_id));
    }

    size_t num_rowsets = 4;
    {
        // insert some rowsets
        for (size_t i = 0; i < num_rowsets; ++i) {
            ASSERT_NO_FATAL_FAILURE(insert_rowset(meta_service.get(), cloud_unique_id, db_id,
                                                  fmt::format("label_{}", i), table_id,
                                                  partition_id, tablet_id));
        }
    }

    {
        // mark the instance as deleted
        ASSERT_NO_FATAL_FAILURE(drop_instance(meta_service.get(), instance_id));
    }

    {
        // Recycle deleted instance
        InstanceInfoPB instance_info;
        ASSERT_NO_FATAL_FAILURE(get_instance(meta_service.get(), cloud_unique_id, instance_info));
        auto recycler = get_instance_recycler(meta_service.get(), instance_info);
        ASSERT_EQ(recycler->recycle_deleted_instance(), 0);
    }

    {
        // Assert all datas of the instance are recycled.
        std::string meta_key = versioned::meta_key_prefix(instance_id);
        std::string meta_key_end = versioned::meta_key_prefix(instance_id + '\x00');
        ASSERT_EQ(count_range(txn_kv.get(), meta_key, meta_key_end), 0) << dump_range(txn_kv.get());

        std::string data_key = versioned::data_key_prefix(instance_id);
        std::string data_key_end = versioned::data_key_prefix(instance_id + '\x00');
        ASSERT_EQ(count_range(txn_kv.get(), data_key, data_key_end), 0) << dump_range(txn_kv.get());

        std::string index_key = versioned::index_key_prefix(instance_id);
        std::string index_key_end = versioned::index_key_prefix(instance_id + '\x00');
        ASSERT_EQ(count_range(txn_kv.get(), index_key, index_key_end), 0)
                << dump_range(txn_kv.get());

        std::string stats_key = versioned::stats_key_prefix(instance_id);
        std::string stats_key_end = versioned::stats_key_prefix(instance_id + '\x00');
        ASSERT_EQ(count_range(txn_kv.get(), stats_key, stats_key_end), 0)
                << dump_range(txn_kv.get());

        std::string version_key = versioned::version_key_prefix(instance_id);
        std::string version_key_end = versioned::version_key_prefix(instance_id + '\x00');
        ASSERT_EQ(count_range(txn_kv.get(), version_key, version_key_end), 0)
                << dump_range(txn_kv.get());

        std::string log_key = versioned::log_key_prefix(instance_id);
        std::string log_key_end = versioned::log_key_prefix(instance_id + '\x00');
        ASSERT_EQ(count_range(txn_kv.get(), log_key, log_key_end), 0) << dump_range(txn_kv.get());
    }
}
