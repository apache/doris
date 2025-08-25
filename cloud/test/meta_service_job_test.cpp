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
#include <cstdlib>
#include <ctime>
#include <functional>
#include <limits>
#include <memory>
#include <random>
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
extern void insert_rowset(MetaServiceProxy* meta_service, int64_t db_id, const std::string& label,
                          int64_t table_id, int64_t partition_id, int64_t tablet_id);
extern void add_tablet(CreateTabletsRequest& req, int64_t table_id, int64_t index_id,
                       int64_t partition_id, int64_t tablet_id);
extern void get_tablet_stats(MetaServiceProxy* meta_service, int64_t table_id, int64_t index_id,
                             int64_t partition_id, int64_t tablet_id, GetTabletStatsResponse& res);
extern void create_and_commit_rowset(MetaServiceProxy* meta_service, int64_t table_id,
                                     int64_t index_id, int64_t partition_id, int64_t tablet_id,
                                     int64_t txn_id);

// Create a MULTI_VERSION_READ_WRITE instance and refresh the resource manager.
static void create_and_refresh_instance(MetaServiceProxy* service, std::string instance_id) {
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

namespace {
const std::string instance_id = "MetaServiceJobTest";

void start_compaction_job(MetaService* meta_service, int64_t tablet_id, const std::string& job_id,
                          const std::string& initiator, int base_compaction_cnt,
                          int cumu_compaction_cnt, TabletCompactionJobPB::CompactionType type,
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

void get_tablet_stats(MetaService* meta_service, int64_t tablet_id, TabletStatsPB& stats) {
    brpc::Controller cntl;
    GetTabletStatsRequest req;
    GetTabletStatsResponse res;
    auto idx = req.add_tablet_idx();
    idx->set_tablet_id(tablet_id);
    meta_service->get_tablet_stats(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << tablet_id;
    stats = res.tablet_stats(0);
}

std::string next_rowset_id() {
    static int cnt = 0;
    return fmt::format("{:04}", ++cnt);
}

doris::RowsetMetaCloudPB create_rowset(int64_t tablet_id, int64_t start_version,
                                       int64_t end_version, int num_rows = 100) {
    doris::RowsetMetaCloudPB rowset;
    rowset.set_rowset_id(0); // required
    rowset.set_rowset_id_v2(next_rowset_id());
    rowset.set_tablet_id(tablet_id);
    rowset.set_txn_id(start_version << 32 | end_version);
    rowset.set_start_version(start_version);
    rowset.set_end_version(end_version);
    rowset.set_num_segments(1);
    rowset.set_num_rows(num_rows);
    rowset.set_total_disk_size(num_rows * 100);
    rowset.set_data_disk_size(num_rows * 50);
    rowset.set_index_disk_size(num_rows * 50);
    rowset.mutable_tablet_schema()->set_schema_version(0);
    rowset.set_txn_expiration(::time(nullptr)); // Required by DCHECK
    return rowset;
}

void commit_rowset(MetaService* meta_service, const doris::RowsetMetaCloudPB& rowset,
                   CreateRowsetResponse& res, int txn_id = 1) {
    brpc::Controller cntl;
    CreateRowsetRequest req;
    req.set_txn_id(txn_id);
    req.mutable_rowset_meta()->CopyFrom(rowset);
    meta_service->commit_rowset(&cntl, &req, &res, nullptr);
}

void insert_rowsets(TxnKv* txn_kv, int64_t table_id, int64_t index_id, int64_t partition_id,
                    int64_t tablet_id, const std::vector<doris::RowsetMetaCloudPB>& rowsets) {
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK) << tablet_id;
    std::deque<std::string> buffer;
    int data_size = 0, num_rows = 0, num_seg = 0, index_size = 0, segment_size = 0;
    for (auto& rowset : rowsets) {
        data_size += rowset.total_disk_size();
        num_rows += rowset.num_rows();
        num_seg += rowset.num_segments();
        index_size += rowset.index_disk_size();
        segment_size += rowset.data_disk_size();
        auto& key = buffer.emplace_back();
        auto& val = buffer.emplace_back();
        meta_rowset_key({instance_id, tablet_id, rowset.end_version()}, &key);
        ASSERT_TRUE(rowset.SerializeToString(&val)) << tablet_id;
        txn->put(key, val);
    }
    StatsTabletKeyInfo info {instance_id, table_id, index_id, partition_id, tablet_id};
    std::string data_size_key;
    stats_tablet_data_size_key(info, &data_size_key);
    txn->atomic_add(data_size_key, data_size);
    std::string num_rows_key;
    stats_tablet_num_rows_key(info, &num_rows_key);
    txn->atomic_add(num_rows_key, num_rows);
    std::string num_rowsets_key;
    stats_tablet_num_rowsets_key(info, &num_rowsets_key);
    txn->atomic_add(num_rowsets_key, rowsets.size());
    std::string num_segs_key;
    stats_tablet_num_segs_key(info, &num_segs_key);
    txn->atomic_add(num_segs_key, num_seg);
    std::string index_size_key;
    stats_tablet_index_size_key(info, &index_size_key);
    txn->atomic_add(index_size_key, index_size);
    std::string segment_size_key;
    stats_tablet_segment_size_key(info, &segment_size_key);
    txn->atomic_add(segment_size_key, segment_size);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK) << tablet_id;
}

MetaServiceCode get_delete_bitmap_lock(MetaServiceProxy* meta_service, int64_t table_id,
                                       int64_t lock_id, int64_t initor, int64_t expiration = 5,
                                       std::string cloud_unique_id = "test_cloud_unique_id") {
    brpc::Controller cntl;
    GetDeleteBitmapUpdateLockRequest req;
    GetDeleteBitmapUpdateLockResponse res;
    req.set_cloud_unique_id(cloud_unique_id);
    req.set_table_id(table_id);
    req.set_expiration(expiration);
    req.set_lock_id(lock_id);
    req.set_initiator(initor);
    meta_service->get_delete_bitmap_update_lock(
            reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
    return res.status().code();
}

MetaServiceCode remove_delete_bitmap_lock(MetaServiceProxy* meta_service, int64_t table_id,
                                          int64_t lock_id, int64_t initiator) {
    brpc::Controller cntl;
    RemoveDeleteBitmapUpdateLockRequest req;
    RemoveDeleteBitmapUpdateLockResponse res;
    req.set_cloud_unique_id("test_cloud_unique_id");
    req.set_table_id(table_id);
    req.set_lock_id(lock_id);
    req.set_initiator(initiator);
    meta_service->remove_delete_bitmap_update_lock(
            reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
    return res.status().code();
}

MetaServiceCode update_delete_bitmap(MetaServiceProxy* meta_service, int64_t table_id,
                                     int64_t partition_id, int64_t tablet_id, int64_t lock_id,
                                     int64_t initor,
                                     std::string cloud_unique_id = "test_cloud_unique_id") {
    brpc::Controller cntl;
    UpdateDeleteBitmapRequest update_delete_bitmap_req;
    UpdateDeleteBitmapResponse update_delete_bitmap_res;
    update_delete_bitmap_req.set_cloud_unique_id(cloud_unique_id);
    update_delete_bitmap_req.set_table_id(table_id);
    update_delete_bitmap_req.set_partition_id(partition_id);
    update_delete_bitmap_req.set_lock_id(lock_id);
    update_delete_bitmap_req.set_initiator(initor);
    update_delete_bitmap_req.set_tablet_id(tablet_id);
    for (int i = 0; i < 3; i++) {
        update_delete_bitmap_req.add_rowset_ids("0200000003ea308a3647dbea83220ed4b8897f2288244a91");
        update_delete_bitmap_req.add_segment_ids(0);
        update_delete_bitmap_req.add_versions(i);
        update_delete_bitmap_req.add_segment_delete_bitmaps("1");
    }
    meta_service->update_delete_bitmap(reinterpret_cast<google::protobuf::RpcController*>(&cntl),
                                       &update_delete_bitmap_req, &update_delete_bitmap_res,
                                       nullptr);
    return update_delete_bitmap_res.status().code();
}

void remove_delete_bitmap_lock(MetaServiceProxy* meta_service, int64_t table_id) {
    std::string lock_key = meta_delete_bitmap_update_lock_key({instance_id, table_id, -1});
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->remove(lock_key);
    std::string tablet_job_key_begin = mow_tablet_job_key({instance_id, table_id, 0});
    std::string tablet_job_key_end = mow_tablet_job_key({instance_id, table_id, INT64_MAX});
    txn->remove(tablet_job_key_begin, tablet_job_key_end);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
}

void create_tablet(MetaService* meta_service, int64_t table_id, int64_t index_id,
                   int64_t partition_id, int64_t tablet_id, bool enable_mow,
                   bool not_ready = false) {
    brpc::Controller cntl;
    CreateTabletsRequest req;
    CreateTabletsResponse res;
    req.set_db_id(1);
    auto tablet = req.add_tablet_metas();
    tablet->set_tablet_state(not_ready ? doris::TabletStatePB::PB_NOTREADY
                                       : doris::TabletStatePB::PB_RUNNING);
    tablet->set_table_id(table_id);
    tablet->set_index_id(index_id);
    tablet->set_partition_id(partition_id);
    tablet->set_tablet_id(tablet_id);
    tablet->set_enable_unique_key_merge_on_write(enable_mow);
    auto schema = tablet->mutable_schema();
    schema->set_schema_version(0);
    auto first_rowset = tablet->add_rs_metas();
    first_rowset->set_rowset_id(0); // required
    first_rowset->set_rowset_id_v2(next_rowset_id());
    first_rowset->set_start_version(0);
    first_rowset->set_end_version(1);
    first_rowset->mutable_tablet_schema()->CopyFrom(*schema);
    meta_service->create_tablets(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << tablet_id;
}

void start_schema_change_job(MetaServiceProxy* meta_service, int64_t table_id, int64_t index_id,
                             int64_t partition_id, int64_t tablet_id, int64_t new_tablet_id,
                             const std::string& job_id, const std::string& initiator,
                             StartTabletJobResponse& res, int64_t alter_version = -1) {
    brpc::Controller cntl;
    StartTabletJobRequest req;
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
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK)
            << job_id << ' ' << initiator << ' ' << res.status().msg();
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK)
            << job_id << ' ' << initiator;
    auto job_key = job_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
    std::string job_val;
    ASSERT_EQ(txn->get(job_key, &job_val), TxnErrorCode::TXN_OK) << job_id << ' ' << initiator;
    TabletJobInfoPB job_pb;
    ASSERT_TRUE(job_pb.ParseFromString(job_val)) << job_id << ' ' << initiator;
    ASSERT_TRUE(job_pb.has_schema_change()) << job_id << ' ' << initiator;
    EXPECT_EQ(job_pb.schema_change().id(), job_id) << ' ' << initiator;
};

void finish_schema_change_job(MetaService* meta_service, int64_t tablet_id, int64_t new_tablet_id,
                              const std::string& job_id, const std::string& initiator,
                              const std::vector<doris::RowsetMetaCloudPB>& output_rowsets,
                              FinishTabletJobResponse& res,
                              FinishTabletJobRequest_Action action = FinishTabletJobRequest::COMMIT,
                              int64_t delete_bitmap_lock_initiator = 12345) {
    brpc::Controller cntl;
    FinishTabletJobRequest req;
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

void finish_schema_change_job(MetaService* meta_service, int64_t tablet_id, int64_t new_tablet_id,
                              const std::string& job_id, const std::string& initiator,
                              const std::vector<doris::RowsetMetaCloudPB>& output_rowsets,
                              FinishTabletJobResponse& res, int64_t delete_bitmap_lock_initiator) {
    finish_schema_change_job(meta_service, tablet_id, new_tablet_id, job_id, initiator,
                             output_rowsets, res, FinishTabletJobRequest::COMMIT,
                             delete_bitmap_lock_initiator);
}
} // namespace

TEST(MetaServiceJobTest, StartCompactionArguments) {
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

    auto meta_service = get_meta_service();
    brpc::Controller cntl;
    StartTabletJobRequest req;
    StartTabletJobResponse res;
    meta_service->start_tablet_job(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT) << res.status().msg();
    EXPECT_NE(res.status().msg().find("no valid job"), std::string::npos) << res.status().msg();

    auto* job = req.mutable_job();
    auto* compaction = job->add_compaction();
    meta_service->start_tablet_job(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT) << res.status().msg();
    EXPECT_NE(res.status().msg().find("no valid tablet_id"), std::string::npos)
            << res.status().msg();

    auto* idx = job->mutable_idx();
    constexpr int64_t table_id = 10001;
    constexpr int64_t index_id = 10002;
    constexpr int64_t partition_id = 10003;
    constexpr int64_t tablet_id = 10004;
    idx->set_tablet_id(tablet_id);
    meta_service->start_tablet_job(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::TABLET_NOT_FOUND) << res.status().msg();

    create_tablet(meta_service.get(), table_id, index_id, partition_id, tablet_id, false);
    meta_service->start_tablet_job(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT) << res.status().msg();
    EXPECT_NE(res.status().msg().find("no job id"), std::string::npos) << res.status().msg();

    compaction->set_id("compaction1");
    meta_service->start_tablet_job(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT) << res.status().msg();
    EXPECT_NE(res.status().msg().find("no valid compaction_cnt"), std::string::npos)
            << res.status().msg();

    compaction->set_base_compaction_cnt(0);
    compaction->set_cumulative_compaction_cnt(0);
    meta_service->start_tablet_job(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT) << res.status().msg();
    EXPECT_NE(res.status().msg().find("no valid expiration"), std::string::npos)
            << res.status().msg();

    compaction->set_expiration(114115);
    meta_service->start_tablet_job(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT) << res.status().msg();
    EXPECT_NE(res.status().msg().find("no valid lease"), std::string::npos) << res.status().msg();

    compaction->set_lease(114115);
    meta_service->start_tablet_job(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();
}

TEST(MetaServiceJobTest, StartFullCompaction) {
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

    auto meta_service = get_meta_service();
    constexpr int64_t table_id = 10001;
    constexpr int64_t index_id = 10002;
    constexpr int64_t partition_id = 10003;
    constexpr int64_t tablet_id = 10004;
    create_tablet(meta_service.get(), table_id, index_id, partition_id, tablet_id, false);

    StartTabletJobResponse res;
    {
        start_compaction_job(meta_service.get(), tablet_id, "compaction1", "ip:port", 0, 0,
                             TabletCompactionJobPB::BASE, res);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

        start_compaction_job(meta_service.get(), tablet_id, "compaction2", "ip:port", 0, 0,
                             TabletCompactionJobPB::CUMULATIVE, res);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

        start_compaction_job(meta_service.get(), tablet_id, "compaction3", "ip:port", 0, 0,
                             TabletCompactionJobPB::BASE, res);
        ASSERT_EQ(res.status().code(), MetaServiceCode::JOB_TABLET_BUSY);

        start_compaction_job(meta_service.get(), tablet_id, "compaction4", "ip:port", 0, 0,
                             TabletCompactionJobPB::FULL, res);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

        start_compaction_job(meta_service.get(), tablet_id, "compaction5", "ip:port", 0, 0,
                             TabletCompactionJobPB::BASE, res);
        ASSERT_EQ(res.status().code(), MetaServiceCode::JOB_TABLET_BUSY);
    }
    {
        start_compaction_job(meta_service.get(), tablet_id, "compaction6", "ip:port", 0, 0,
                             TabletCompactionJobPB::FULL, res, {1, 20});
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

        start_compaction_job(meta_service.get(), tablet_id, "compaction7", "ip:port", 0, 0,
                             TabletCompactionJobPB::CUMULATIVE, res, {18, 22});
        ASSERT_EQ(res.status().code(), MetaServiceCode::JOB_TABLET_BUSY);

        start_compaction_job(meta_service.get(), tablet_id, "compaction8", "ip:port", 0, 0,
                             TabletCompactionJobPB::CUMULATIVE, res, {21, 26});
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }
}

TEST(MetaServiceJobTest, StartSchemaChangeArguments) {
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

    auto meta_service = get_meta_service();
    brpc::Controller cntl;
    StartTabletJobRequest req;
    StartTabletJobResponse res;
    meta_service->start_tablet_job(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT) << res.status().msg();
    EXPECT_NE(res.status().msg().find("no valid job"), std::string::npos) << res.status().msg();

    auto* job = req.mutable_job();
    auto* sc = job->mutable_schema_change();
    meta_service->start_tablet_job(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT) << res.status().msg();
    EXPECT_NE(res.status().msg().find("no valid tablet_id"), std::string::npos)
            << res.status().msg();

    auto* idx = job->mutable_idx();
    constexpr int64_t table_id = 10001;
    constexpr int64_t index_id = 10002;
    constexpr int64_t partition_id = 10003;
    constexpr int64_t tablet_id = 10004;
    idx->set_tablet_id(tablet_id);
    meta_service->start_tablet_job(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::TABLET_NOT_FOUND) << res.status().msg();

    create_tablet(meta_service.get(), table_id, index_id, partition_id, tablet_id, false);
    meta_service->start_tablet_job(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT) << res.status().msg();
    EXPECT_NE(res.status().msg().find("no job id"), std::string::npos) << res.status().msg();

    sc->set_id("sc1");
    meta_service->start_tablet_job(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT) << res.status().msg();
    EXPECT_NE(res.status().msg().find("no initiator"), std::string::npos) << res.status().msg();

    sc->set_initiator("BE1");
    //     meta_service->start_tablet_job(&cntl, &req, &res, nullptr);
    //     ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT) << res.status().msg();
    //     EXPECT_NE(res.status().msg().find("no valid expiration"), std::string::npos)
    //             << res.status().msg();

    sc->set_expiration(114115);
    meta_service->start_tablet_job(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT) << res.status().msg();
    EXPECT_NE(res.status().msg().find("no valid new_tablet_id"), std::string::npos)
            << res.status().msg();

    auto* new_idx = sc->mutable_new_tablet_idx();
    new_idx->set_tablet_id(tablet_id);
    meta_service->start_tablet_job(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT) << res.status().msg();
    EXPECT_NE(res.status().msg().find("new_tablet_id same with base_tablet_id"), std::string::npos)
            << res.status().msg();

    constexpr int64_t new_index_id = 10005;
    constexpr int64_t new_tablet_id = 10006;
    new_idx->set_tablet_id(new_tablet_id);
    meta_service->start_tablet_job(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::TABLET_NOT_FOUND) << res.status().msg();

    create_tablet(meta_service.get(), table_id, new_index_id, partition_id, new_tablet_id, false);
    meta_service->start_tablet_job(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::JOB_ALREADY_SUCCESS) << res.status().msg();

    // Reset tablet state
    auto tablet_key =
            meta_tablet_key({instance_id, table_id, new_index_id, partition_id, new_tablet_id});
    std::string tablet_val;
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(tablet_key, &tablet_val), TxnErrorCode::TXN_OK);
    doris::TabletMetaCloudPB tablet_meta;
    ASSERT_TRUE(tablet_meta.ParseFromString(tablet_val));
    tablet_meta.clear_tablet_state();
    tablet_val = tablet_meta.SerializeAsString();
    txn->put(tablet_key, tablet_val);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    meta_service->start_tablet_job(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT) << res.status().msg();
    EXPECT_NE(res.status().msg().find("invalid new tablet state"), std::string::npos)
            << res.status().msg();

    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(tablet_key, &tablet_val), TxnErrorCode::TXN_OK);
    ASSERT_TRUE(tablet_meta.ParseFromString(tablet_val));
    tablet_meta.set_tablet_state(doris::TabletStatePB::PB_NOTREADY);
    tablet_val = tablet_meta.SerializeAsString();
    txn->put(tablet_key, tablet_val);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    meta_service->start_tablet_job(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();
}

TEST(MetaServiceJobTest, ProcessCompactionArguments) {
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

    auto meta_service = get_meta_service();
    brpc::Controller cntl;
    FinishTabletJobRequest req;
    FinishTabletJobResponse res;
    meta_service->finish_tablet_job(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT) << res.status().msg();
    EXPECT_NE(res.status().msg().find("no valid job"), std::string::npos) << res.status().msg();

    auto* job = req.mutable_job();
    auto* compaction = job->add_compaction();
    meta_service->finish_tablet_job(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT) << res.status().msg();
    EXPECT_NE(res.status().msg().find("no valid tablet_id"), std::string::npos)
            << res.status().msg();

    auto* idx = job->mutable_idx();
    constexpr int64_t table_id = 10001;
    constexpr int64_t index_id = 10002;
    constexpr int64_t partition_id = 10003;
    constexpr int64_t tablet_id = 10004;
    idx->set_tablet_id(tablet_id);
    meta_service->finish_tablet_job(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::TABLET_NOT_FOUND) << res.status().msg();

    create_tablet(meta_service.get(), table_id, index_id, partition_id, tablet_id, false);
    meta_service->finish_tablet_job(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT) << res.status().msg();
    EXPECT_NE(res.status().msg().find("job not found"), std::string::npos) << res.status().msg();

    auto job_key = job_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
    TabletJobInfoPB recorded_job;
    auto job_val = recorded_job.SerializeAsString();
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->put(job_key, job_val);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    meta_service->finish_tablet_job(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT) << res.status().msg();
    EXPECT_NE(res.status().msg().find("no running compaction"), std::string::npos)
            << res.status().msg();

    auto* recorded_compaction = recorded_job.add_compaction();
    recorded_compaction->set_id("compaction1");
    recorded_compaction->set_expiration(114115);
    job_val = recorded_job.SerializeAsString();
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->put(job_key, job_val);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    meta_service->finish_tablet_job(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT) << res.status().msg();
    EXPECT_NE(res.status().msg().find("unmatched job id"), std::string::npos) << res.status().msg();

    compaction->set_id("compaction1");
    meta_service->finish_tablet_job(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::JOB_EXPIRED) << res.status().msg();

    // Prepare job kv
    recorded_compaction->set_expiration(::time(nullptr) + 10);
    job_val = recorded_job.SerializeAsString();
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->put(job_key, job_val);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    meta_service->finish_tablet_job(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT) << res.status().msg();
    EXPECT_NE(res.status().msg().find("unsupported action"), std::string::npos)
            << res.status().msg();

    req.set_action(FinishTabletJobRequest::LEASE);
    meta_service->finish_tablet_job(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT) << res.status().msg();
    EXPECT_NE(res.status().msg().find("invalid lease"), std::string::npos) << res.status().msg();

    compaction->set_lease(::time(nullptr) + 5);
    meta_service->finish_tablet_job(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();

    req.set_action(FinishTabletJobRequest::COMMIT);
    meta_service->finish_tablet_job(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT) << res.status().msg();
    EXPECT_NE(res.status().msg().find("invalid compaction type"), std::string::npos)
            << res.status().msg();

    compaction->set_type(TabletCompactionJobPB::EMPTY_CUMULATIVE);
    meta_service->finish_tablet_job(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();
}

TEST(MetaServiceJobTest, ProcessSchemaChangeArguments) {
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

    auto meta_service = get_meta_service();
    brpc::Controller cntl;
    FinishTabletJobRequest req;
    FinishTabletJobResponse res;
    meta_service->finish_tablet_job(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT) << res.status().msg();
    EXPECT_NE(res.status().msg().find("no valid job"), std::string::npos) << res.status().msg();

    auto* job = req.mutable_job();
    auto* sc = job->mutable_schema_change();
    meta_service->finish_tablet_job(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT) << res.status().msg();
    EXPECT_NE(res.status().msg().find("no valid tablet_id"), std::string::npos)
            << res.status().msg();

    auto* idx = job->mutable_idx();
    constexpr int64_t table_id = 10001;
    constexpr int64_t index_id = 10002;
    constexpr int64_t partition_id = 10003;
    constexpr int64_t tablet_id = 10004;
    idx->set_tablet_id(tablet_id);
    meta_service->finish_tablet_job(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::TABLET_NOT_FOUND) << res.status().msg();

    create_tablet(meta_service.get(), table_id, index_id, partition_id, tablet_id, false);
    meta_service->finish_tablet_job(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT) << res.status().msg();
    EXPECT_NE(res.status().msg().find("job not found"), std::string::npos) << res.status().msg();

    auto job_key = job_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
    TabletJobInfoPB recorded_job;
    auto job_val = recorded_job.SerializeAsString();
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->put(job_key, job_val);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    meta_service->finish_tablet_job(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT) << res.status().msg();
    EXPECT_NE(res.status().msg().find("no valid new_tablet_id"), std::string::npos)
            << res.status().msg();

    auto* new_idx = sc->mutable_new_tablet_idx();
    new_idx->set_tablet_id(tablet_id);
    meta_service->finish_tablet_job(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT) << res.status().msg();
    EXPECT_NE(res.status().msg().find("new_tablet_id same with base_tablet_id"), std::string::npos)
            << res.status().msg();

    constexpr int64_t new_index_id = 10005;
    constexpr int64_t new_tablet_id = 10006;
    new_idx->set_tablet_id(new_tablet_id);
    meta_service->finish_tablet_job(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::TABLET_NOT_FOUND) << res.status().msg();

    create_tablet(meta_service.get(), table_id, new_index_id, partition_id, new_tablet_id, false);
    meta_service->finish_tablet_job(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::JOB_ALREADY_SUCCESS) << res.status().msg();

    // Reset tablet state
    auto tablet_key =
            meta_tablet_key({instance_id, table_id, new_index_id, partition_id, new_tablet_id});
    std::string tablet_val;
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(tablet_key, &tablet_val), TxnErrorCode::TXN_OK);
    doris::TabletMetaCloudPB tablet_meta;
    ASSERT_TRUE(tablet_meta.ParseFromString(tablet_val));
    tablet_meta.clear_tablet_state();
    tablet_val = tablet_meta.SerializeAsString();
    txn->put(tablet_key, tablet_val);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    meta_service->finish_tablet_job(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT) << res.status().msg();
    EXPECT_NE(res.status().msg().find("invalid new tablet state"), std::string::npos)
            << res.status().msg();

    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(tablet_key, &tablet_val), TxnErrorCode::TXN_OK);
    ASSERT_TRUE(tablet_meta.ParseFromString(tablet_val));
    tablet_meta.set_tablet_state(doris::TabletStatePB::PB_NOTREADY);
    tablet_val = tablet_meta.SerializeAsString();
    txn->put(tablet_key, tablet_val);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    meta_service->finish_tablet_job(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT) << res.status().msg();
    EXPECT_NE(res.status().msg().find("no running schema_change"), std::string::npos)
            << res.status().msg();

    auto* recorded_sc = recorded_job.mutable_schema_change();
    recorded_sc->set_expiration(114115);
    job_val = recorded_job.SerializeAsString();
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->put(job_key, job_val);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    meta_service->finish_tablet_job(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::JOB_EXPIRED) << res.status().msg();

    recorded_sc->set_expiration(::time(nullptr) + 10);
    recorded_sc->set_id("sc1");
    recorded_sc->set_initiator("BE1");
    job_val = recorded_job.SerializeAsString();
    auto new_job_key =
            job_tablet_key({instance_id, table_id, new_index_id, partition_id, new_tablet_id});
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->put(job_key, job_val);
    txn->put(new_job_key, job_val);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    meta_service->finish_tablet_job(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT) << res.status().msg();
    EXPECT_NE(res.status().msg().find("unmatched job id or initiator"), std::string::npos)
            << res.status().msg();

    sc->set_id("sc1");
    meta_service->finish_tablet_job(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT) << res.status().msg();
    EXPECT_NE(res.status().msg().find("unmatched job id or initiator"), std::string::npos)
            << res.status().msg();

    sc->set_initiator("BE1");
    meta_service->finish_tablet_job(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT) << res.status().msg();
    EXPECT_NE(res.status().msg().find("unsupported action"), std::string::npos)
            << res.status().msg();

    req.set_action(FinishTabletJobRequest::ABORT);
    meta_service->finish_tablet_job(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();
}

TEST(MetaServiceJobTest, CompactionJobTest) {
    auto meta_service = get_meta_service();
    // meta_service->resource_mgr().reset(); // Do not use resource manager

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

    brpc::Controller cntl;

    // Start compaction job
    auto test_start_compaction_job = [&](int64_t table_id, int64_t index_id, int64_t partition_id,
                                         int64_t tablet_id,
                                         TabletCompactionJobPB::CompactionType type) {
        StartTabletJobResponse res;
        std::string job_id = "job_id123";

        start_compaction_job(meta_service.get(), 0, job_id, "ip:port", 0, 0, type, res);
        ASSERT_NE(res.status().msg().find("no valid tablet_id given"), std::string::npos);

        start_compaction_job(meta_service.get(), tablet_id, job_id, "ip:port", 0, 0, type, res);
        ASSERT_EQ(res.status().code(), MetaServiceCode::TABLET_NOT_FOUND);

        auto index_key = meta_tablet_idx_key({instance_id, tablet_id});
        TabletIndexPB idx_pb;
        idx_pb.set_table_id(1);
        idx_pb.set_index_id(2);
        idx_pb.set_partition_id(3);
        idx_pb.set_tablet_id(tablet_id + 1); // error, tablet_id not match
        std::string idx_val = idx_pb.SerializeAsString();
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        txn->put(index_key, idx_val);
        std::string stats_key =
                stats_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
        TabletStatsPB stats;
        stats.set_base_compaction_cnt(9);
        stats.set_cumulative_compaction_cnt(19);
        txn->put(stats_key, stats.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
        start_compaction_job(meta_service.get(), tablet_id, job_id, "ip:port", 0, 0, type, res);
        ASSERT_NE(res.status().msg().find("internal error"), std::string::npos);
        idx_pb.set_tablet_id(tablet_id); // Correct tablet_id
        idx_val = idx_pb.SerializeAsString();
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        txn->put(index_key, idx_val);
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
        start_compaction_job(meta_service.get(), tablet_id, job_id, "ip:port", 9, 18, type, res);
        ASSERT_EQ(res.status().code(), MetaServiceCode::STALE_TABLET_CACHE);
        start_compaction_job(meta_service.get(), tablet_id, job_id, "ip:port", 9, 19, type, res);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        auto job_key = job_tablet_key({instance_id, idx_pb.table_id(), idx_pb.index_id(),
                                       idx_pb.partition_id(), idx_pb.tablet_id()});
        std::string job_val;
        ASSERT_EQ(txn->get(job_key, &job_val), TxnErrorCode::TXN_OK);
        TabletJobInfoPB job_pb;
        ASSERT_TRUE(job_pb.ParseFromString(job_val));
        start_compaction_job(meta_service.get(), tablet_id, job_id, "ip:port", 9, 19, type, res);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK); // Same job_id, return OK
    };

    auto test_commit_compaction_job = [&](int64_t table_id, int64_t index_id, int64_t partition_id,
                                          int64_t tablet_id,
                                          TabletCompactionJobPB::CompactionType type) {
        FinishTabletJobRequest req;
        FinishTabletJobResponse res;
        std::string job_id = "job_id123";

        auto compaction = req.mutable_job()->add_compaction();
        compaction->set_id(job_id);
        compaction->set_initiator("ip:port");
        meta_service->finish_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);
        ASSERT_NE(res.status().msg().find("no valid tablet_id given"), std::string::npos);

        req.mutable_job()->mutable_idx()->set_table_id(table_id);
        req.mutable_job()->mutable_idx()->set_index_id(index_id);
        req.mutable_job()->mutable_idx()->set_partition_id(partition_id);
        req.mutable_job()->mutable_idx()->set_tablet_id(tablet_id);
        compaction->set_base_compaction_cnt(10);
        compaction->set_cumulative_compaction_cnt(20);
        // Action is not set
        meta_service->finish_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);
        ASSERT_NE(res.status().msg().find("unsupported action"), std::string::npos);

        //======================================================================
        // Test commit
        //======================================================================
        req.set_action(FinishTabletJobRequest::COMMIT);

        auto tablet_meta_key =
                meta_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        doris::TabletMetaCloudPB tablet_meta_pb;
        tablet_meta_pb.set_table_id(table_id);
        tablet_meta_pb.set_index_id(index_id);
        tablet_meta_pb.set_partition_id(partition_id);
        tablet_meta_pb.set_tablet_id(tablet_id);
        tablet_meta_pb.set_cumulative_layer_point(50);
        std::string tablet_meta_val = tablet_meta_pb.SerializeAsString();
        ASSERT_FALSE(tablet_meta_val.empty());
        txn->put(tablet_meta_key, tablet_meta_val);
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        meta_service->finish_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);

        // Create create tablet stats, compation job will will update stats
        auto tablet_stats_key =
                stats_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
        TabletStatsPB tablet_stats_pb;
        tablet_stats_pb.mutable_idx()->set_table_id(table_id);
        tablet_stats_pb.mutable_idx()->set_index_id(index_id);
        tablet_stats_pb.mutable_idx()->set_partition_id(partition_id);
        tablet_stats_pb.mutable_idx()->set_tablet_id(tablet_id);

        std::mt19937 rng(std::chrono::system_clock::now().time_since_epoch().count());
        std::uniform_int_distribution<int> dist(1, 10000); // Positive numbers

        compaction->set_output_cumulative_point(tablet_stats_pb.cumulative_point() + dist(rng));
        compaction->set_num_output_rows(dist(rng));
        compaction->set_num_output_rowsets(dist(rng));
        compaction->set_num_output_segments(dist(rng));
        compaction->set_num_input_rows(dist(rng));
        compaction->set_num_input_rowsets(dist(rng));
        compaction->set_num_input_segments(dist(rng));
        compaction->set_size_input_rowsets(dist(rng));
        compaction->set_size_output_rowsets(dist(rng));
        compaction->set_index_size_input_rowsets(dist(rng));
        compaction->set_segment_size_output_rowsets(dist(rng));
        compaction->set_index_size_input_rowsets(dist(rng));
        compaction->set_segment_size_output_rowsets(dist(rng));
        compaction->set_type(type);

        tablet_stats_pb.set_cumulative_compaction_cnt(dist(rng));
        tablet_stats_pb.set_base_compaction_cnt(dist(rng));
        tablet_stats_pb.set_cumulative_point(tablet_meta_pb.cumulative_layer_point());
        // MUST let data stats be larger than input data size
        tablet_stats_pb.set_num_rows(dist(rng) + compaction->num_input_rows());
        tablet_stats_pb.set_data_size(dist(rng) + compaction->size_input_rowsets());
        tablet_stats_pb.set_num_rowsets(dist(rng) + compaction->num_input_rowsets());
        tablet_stats_pb.set_num_segments(dist(rng) + compaction->num_input_segments());
        tablet_stats_pb.set_index_size(dist(rng) + compaction->index_size_input_rowsets());
        tablet_stats_pb.set_segment_size(dist(rng) + compaction->segment_size_input_rowsets());

        std::string tablet_stats_val = tablet_stats_pb.SerializeAsString();
        ASSERT_FALSE(tablet_stats_val.empty());
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        txn->put(tablet_stats_key, tablet_stats_val);
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        // Input rowset not valid
        meta_service->finish_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);
        ASSERT_NE(res.status().msg().find("invalid input"), std::string::npos);

        // Provide input and output rowset info
        int64_t input_version_start = dist(rng);
        int64_t input_version_end = input_version_start + 100;
        compaction->add_input_versions(input_version_start);
        compaction->add_input_versions(input_version_end);
        compaction->add_output_versions(input_version_end);
        compaction->add_output_rowset_ids("output rowset id");

        // Input rowsets must exist, and more than 0
        // Check number input rowsets
        sp->set_call_back("process_compaction_job::loop_input_done", [](auto&& args) {
            auto* num_input_rowsets = try_any_cast<int*>(args[0]);
            ASSERT_EQ(*num_input_rowsets, 0); // zero existed rowsets
        });
        sp->set_call_back("process_compaction_job::too_few_rowsets", [](auto&& args) {
            auto* need_commit = try_any_cast<bool*>(args[0]);
            ASSERT_EQ(*need_commit, true);
            *need_commit = false; // Donot remove tablet job in order to continue test
        });

        meta_service->finish_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);
        ASSERT_NE(res.status().msg().find("too few input rowsets"), std::string::npos);

        // Provide input rowset KVs, boundary test, 5 input rowsets
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        // clang-format off
        std::vector<std::string> input_rowset_keys = {
                meta_rowset_key({instance_id, tablet_id, input_version_start - 1}),
                meta_rowset_key({instance_id, tablet_id, input_version_start}),
                meta_rowset_key({instance_id, tablet_id, input_version_start + 1}),
                meta_rowset_key({instance_id, tablet_id, (input_version_start + input_version_end) / 2}),
                meta_rowset_key({instance_id, tablet_id, input_version_end - 1}),
                meta_rowset_key({instance_id, tablet_id, input_version_end}),
                meta_rowset_key({instance_id, tablet_id, input_version_end + 1}),
        };
        // clang-format on
        std::vector<std::unique_ptr<std::string>> input_rowset_vals;
        for (auto& i : input_rowset_keys) {
            doris::RowsetMetaCloudPB rs_pb;
            rs_pb.set_rowset_id(0);
            rs_pb.set_rowset_id_v2(hex(i));
            input_rowset_vals.emplace_back(new std::string(rs_pb.SerializeAsString()));
            txn->put(i, *input_rowset_vals.back());
        }
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        // Check number input rowsets
        sp->set_call_back("process_compaction_job::loop_input_done", [](auto&& args) {
            auto* num_input_rowsets = try_any_cast<int*>(args[0]);
            ASSERT_EQ(*num_input_rowsets, 5);
        });
        // No tmp rowset key (output rowset)
        meta_service->finish_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);
        ASSERT_NE(res.status().msg().find("invalid txn_id"), std::string::npos);

        int64_t txn_id = dist(rng);
        compaction->add_txn_id(txn_id);

        meta_service->finish_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);
        ASSERT_NE(res.status().msg().find("failed to get tmp rowset key"), std::string::npos);

        // Provide invalid output rowset meta
        auto tmp_rowset_key = meta_rowset_tmp_key({instance_id, txn_id, tablet_id});
        doris::RowsetMetaCloudPB tmp_rs_pb;
        tmp_rs_pb.set_rowset_id(0);
        auto tmp_rowset_val = tmp_rs_pb.SerializeAsString();
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        txn->put(tmp_rowset_key, tmp_rowset_val);
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        meta_service->finish_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);
        ASSERT_NE(res.status().msg().find("invalid txn_id in output tmp rowset meta"),
                  std::string::npos);

        // Provide txn_id in output rowset meta
        tmp_rs_pb.set_txn_id(10086);
        tmp_rowset_val = tmp_rs_pb.SerializeAsString();
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        txn->put(tmp_rowset_key, tmp_rowset_val);
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        meta_service->finish_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

        //=====================================================================
        // All branch tests done, we are done commit a compaction job
        //=====================================================================
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        tablet_stats_val.clear();
        ASSERT_EQ(txn->get(tablet_stats_key, &tablet_stats_val), TxnErrorCode::TXN_OK);
        TabletStatsPB stats;
        ASSERT_TRUE(stats.ParseFromString(tablet_stats_val));

        // clang-format off
        EXPECT_EQ(stats.base_compaction_cnt()      , tablet_stats_pb.base_compaction_cnt() + (req.job().compaction(0).type() == TabletCompactionJobPB::BASE));
        EXPECT_EQ(stats.cumulative_compaction_cnt(), tablet_stats_pb.cumulative_compaction_cnt() + (req.job().compaction(0).type() == TabletCompactionJobPB::CUMULATIVE));
        EXPECT_EQ(stats.cumulative_point()         , type == TabletCompactionJobPB::BASE ? 50 : req.job().compaction(0).output_cumulative_point());
        EXPECT_EQ(stats.num_rows()                 , tablet_stats_pb.num_rows() + (req.job().compaction(0).num_output_rows() - req.job().compaction(0).num_input_rows()));
        EXPECT_EQ(stats.data_size()                , tablet_stats_pb.data_size() + (req.job().compaction(0).size_output_rowsets() - req.job().compaction(0).size_input_rowsets()));
        EXPECT_EQ(stats.num_rowsets()              , tablet_stats_pb.num_rowsets() + (req.job().compaction(0).num_output_rowsets() - req.job().compaction(0).num_input_rowsets()));
        EXPECT_EQ(stats.num_segments()             , tablet_stats_pb.num_segments() + (req.job().compaction(0).num_output_segments() - req.job().compaction(0).num_input_segments()));
        EXPECT_EQ(stats.index_size()               , tablet_stats_pb.index_size() + (req.job().compaction(0).index_size_output_rowsets() - req.job().compaction(0).index_size_input_rowsets()));
        EXPECT_EQ(stats.segment_size()             , tablet_stats_pb.segment_size() + (req.job().compaction(0).segment_size_output_rowsets() - req.job().compaction(0).segment_size_input_rowsets()));
        // clang-format on

        // Check job removed, tablet meta updated
        auto job_key = job_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
        std::string job_val;
        ASSERT_EQ(txn->get(job_key, &job_val), TxnErrorCode::TXN_OK);
        TabletJobInfoPB job_pb;
        ASSERT_TRUE(job_pb.ParseFromString(job_val));
        ASSERT_TRUE(job_pb.compaction().empty());
        tablet_meta_val.clear();

        // Check tmp rowset removed
        ASSERT_EQ(txn->get(tmp_rowset_key, &tmp_rowset_val), TxnErrorCode::TXN_KEY_NOT_FOUND);
        // Check input rowsets removed, the largest version remains
        for (int i = 1; i < input_rowset_keys.size() - 2; ++i) {
            std::string val;
            EXPECT_EQ(txn->get(input_rowset_keys[i], &val), TxnErrorCode::TXN_KEY_NOT_FOUND)
                    << hex(input_rowset_keys[i]);
        }
        // Check recycle rowsets added
        for (int i = 1; i < input_rowset_vals.size() - 1; ++i) {
            doris::RowsetMetaCloudPB rs;
            ASSERT_TRUE(rs.ParseFromString(*input_rowset_vals[i]));
            auto key = recycle_rowset_key({instance_id, tablet_id, rs.rowset_id_v2()});
            std::string val;
            EXPECT_EQ(txn->get(key, &val), TxnErrorCode::TXN_OK) << hex(key);
        }
        // Check output rowset added
        auto rowset_key = meta_rowset_key({instance_id, tablet_id, input_version_end});
        std::string rowset_val;
        EXPECT_EQ(txn->get(rowset_key, &rowset_val), TxnErrorCode::TXN_OK) << hex(rowset_key);
    };

    auto test_abort_compaction_job = [&](int64_t table_id, int64_t index_id, int64_t partition_id,
                                         int64_t tablet_id) {
        FinishTabletJobRequest req;
        FinishTabletJobResponse res;
        std::string job_id = "job_id123";

        auto compaction = req.mutable_job()->add_compaction();
        compaction->set_id(job_id);
        compaction->set_initiator("ip:port");
        req.mutable_job()->mutable_idx()->set_tablet_id(tablet_id);
        req.set_action(FinishTabletJobRequest::ABORT);
        meta_service->finish_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);

        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        auto job_key = job_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
        std::string job_val;
        ASSERT_EQ(txn->get(job_key, &job_val), TxnErrorCode::TXN_OK);
        TabletJobInfoPB job_pb;
        ASSERT_TRUE(job_pb.ParseFromString(job_val));
        ASSERT_TRUE(job_pb.compaction().empty());
    };
    ASSERT_NO_FATAL_FAILURE(
            test_start_compaction_job(1, 2, 3, 4, TabletCompactionJobPB::CUMULATIVE));
    ASSERT_NO_FATAL_FAILURE(
            test_commit_compaction_job(1, 2, 3, 4, TabletCompactionJobPB::CUMULATIVE));
    ASSERT_NO_FATAL_FAILURE(
            test_start_compaction_job(1, 2, 3, 5, TabletCompactionJobPB::CUMULATIVE));
    ASSERT_NO_FATAL_FAILURE(test_abort_compaction_job(1, 2, 3, 5));
    ASSERT_NO_FATAL_FAILURE(test_start_compaction_job(1, 2, 3, 6, TabletCompactionJobPB::BASE));
    ASSERT_NO_FATAL_FAILURE(test_commit_compaction_job(1, 2, 3, 6, TabletCompactionJobPB::BASE));
    ASSERT_NO_FATAL_FAILURE(test_start_compaction_job(1, 2, 3, 7, TabletCompactionJobPB::BASE));
    ASSERT_NO_FATAL_FAILURE(test_abort_compaction_job(1, 2, 3, 7));
}

TEST(MetaServiceJobVersionedReadTest, CompactionJobTest) {
    auto meta_service = get_meta_service(false);
    std::string instance_id = "test_cloud_instance_id";
    std::string cloud_unique_id = "1:test_cloud_unique_id:1";
    MOCK_GET_INSTANCE_ID(instance_id);
    create_and_refresh_instance(meta_service.get(), instance_id);

    int64_t table_id = 1, index_id = 2, partition_id = 3, tablet_id = 4;
    {
        // Create tablet
        create_tablet(meta_service.get(), table_id, index_id, partition_id, tablet_id, true);
    }

    {
        // Create rowsets
        insert_rowset(meta_service.get(), 1, "commit_rowset_1", table_id, partition_id, tablet_id);
        insert_rowset(meta_service.get(), 1, "commit_rowset_2", table_id, partition_id, tablet_id);
        insert_rowset(meta_service.get(), 1, "commit_rowset_3", table_id, partition_id, tablet_id);
        insert_rowset(meta_service.get(), 1, "commit_rowset_4", table_id, partition_id, tablet_id);
        insert_rowset(meta_service.get(), 1, "commit_rowset_5", table_id, partition_id, tablet_id);
        insert_rowset(meta_service.get(), 1, "commit_rowset_6", table_id, partition_id, tablet_id);
        insert_rowset(meta_service.get(), 1, "commit_rowset_7", table_id, partition_id, tablet_id);
        insert_rowset(meta_service.get(), 1, "commit_rowset_8", table_id, partition_id, tablet_id);
    }

    struct TestCase {
        TabletCompactionJobPB::CompactionType type;
        int64_t start_version, end_version;
        int64_t num_input_rowsets;
    };

    std::vector<TestCase> cases = {
            {TabletCompactionJobPB::CUMULATIVE, 7, 8, 2},
            {TabletCompactionJobPB::BASE, 2, 5, 4},
            {TabletCompactionJobPB::CUMULATIVE, 6, 8, 2},
    };

    auto get_tablet_stats = [&]() -> TabletStatsPB {
        GetTabletStatsRequest get_tablet_stats_req;
        get_tablet_stats_req.set_cloud_unique_id(cloud_unique_id);
        auto* tablet_idx = get_tablet_stats_req.add_tablet_idx();
        tablet_idx->set_tablet_id(tablet_id);
        tablet_idx->set_db_id(1);
        tablet_idx->set_index_id(index_id);
        tablet_idx->set_partition_id(partition_id);
        tablet_idx->set_table_id(table_id);
        GetTabletStatsResponse get_tablet_stats_resp;
        brpc::Controller cntl;
        meta_service->get_tablet_stats(&cntl, &get_tablet_stats_req, &get_tablet_stats_resp,
                                       nullptr);
        EXPECT_EQ(get_tablet_stats_resp.status().code(), MetaServiceCode::OK);
        EXPECT_EQ(get_tablet_stats_resp.tablet_stats_size(), 1);
        return get_tablet_stats_resp.tablet_stats(0);
    };

    int base_cnt = 0, cumu_cnt = 0;
    int64_t txn_id = 123321;
    for (auto& tc : cases) {
        std::string job_id = fmt::format("job_{}_{}", base_cnt, cumu_cnt);
        TabletCompactionJobPB::CompactionType type = tc.type;

        {
            // Start compaction job
            StartTabletJobResponse res;
            start_compaction_job(meta_service.get(), tablet_id, job_id, "ip:port", base_cnt,
                                 cumu_cnt, type, res);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        }

        txn_id += 1;
        {
            // Put tmp rowset
            auto tmp_rowset = create_rowset(tablet_id, tc.start_version, tc.end_version, 100);
            tmp_rowset.set_txn_id(txn_id);
            CreateRowsetResponse res;
            commit_rowset(meta_service.get(), tmp_rowset, res, txn_id);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        }

        auto tablet_stats_pb = get_tablet_stats();

        {
            // Commit compaction job.
            FinishTabletJobRequest req;
            FinishTabletJobResponse res;

            req.set_action(FinishTabletJobRequest::COMMIT);
            req.mutable_job()->mutable_idx()->set_table_id(table_id);
            req.mutable_job()->mutable_idx()->set_index_id(index_id);
            req.mutable_job()->mutable_idx()->set_partition_id(partition_id);
            req.mutable_job()->mutable_idx()->set_tablet_id(tablet_id);
            auto compaction = req.mutable_job()->add_compaction();
            compaction->set_id(job_id);
            compaction->set_initiator("ip:port");
            compaction->set_base_compaction_cnt(base_cnt);
            compaction->set_cumulative_compaction_cnt(cumu_cnt);
            compaction->add_txn_id(txn_id);

            std::mt19937 rng(std::chrono::system_clock::now().time_since_epoch().count());
            std::uniform_int_distribution<int> dist(1, 10000); // Positive numbers

            compaction->set_output_cumulative_point(2);
            compaction->set_num_output_rows(100);
            compaction->set_num_output_rowsets(1);
            compaction->set_num_output_segments(1);
            compaction->set_num_input_rows(tc.num_input_rowsets * 100);
            compaction->set_num_input_rowsets(tc.num_input_rowsets);
            compaction->set_num_input_segments(tc.num_input_rowsets);
            compaction->set_size_input_rowsets(tc.num_input_rowsets * 100 * 110);
            compaction->set_size_output_rowsets(100 * 110);
            compaction->set_index_size_input_rowsets(tc.num_input_rowsets * 100 * 10);
            compaction->set_segment_size_output_rowsets(100 * 110);
            compaction->set_index_size_input_rowsets(tc.num_input_rowsets * 100 * 10);
            compaction->set_segment_size_output_rowsets(100 * 110);
            compaction->set_type(type);
            compaction->add_input_versions(tc.start_version);
            compaction->add_input_versions(tc.end_version);
            compaction->add_output_versions(tc.end_version);
            compaction->add_output_rowset_ids("output rowset id");

            SyncPoint::get_instance()->set_call_back(
                    "process_compaction_job::loop_input_done", [&](auto&& args) {
                        auto* num_input_rowsets = try_any_cast<int*>(args[0]);
                        ASSERT_EQ(*num_input_rowsets, tc.num_input_rowsets);
                    });

            brpc::Controller cntl;
            meta_service->finish_tablet_job(&cntl, &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

            if (type == TabletCompactionJobPB::BASE) {
                base_cnt++;
            } else {
                cumu_cnt++;
            }
            auto stats = get_tablet_stats();

            EXPECT_EQ(stats.base_compaction_cnt(),
                      tablet_stats_pb.base_compaction_cnt() +
                              (req.job().compaction(0).type() == TabletCompactionJobPB::BASE));
            EXPECT_EQ(
                    stats.cumulative_compaction_cnt(),
                    tablet_stats_pb.cumulative_compaction_cnt() +
                            (req.job().compaction(0).type() == TabletCompactionJobPB::CUMULATIVE));
            EXPECT_EQ(stats.cumulative_point(), req.job().compaction(0).output_cumulative_point());
            EXPECT_EQ(stats.num_rows(),
                      tablet_stats_pb.num_rows() + (req.job().compaction(0).num_output_rows() -
                                                    req.job().compaction(0).num_input_rows()));
            EXPECT_EQ(stats.data_size(),
                      tablet_stats_pb.data_size() + (req.job().compaction(0).size_output_rowsets() -
                                                     req.job().compaction(0).size_input_rowsets()));
            EXPECT_EQ(stats.num_rowsets(), tablet_stats_pb.num_rowsets() +
                                                   (req.job().compaction(0).num_output_rowsets() -
                                                    req.job().compaction(0).num_input_rowsets()));
            EXPECT_EQ(stats.num_segments(), tablet_stats_pb.num_segments() +
                                                    (req.job().compaction(0).num_output_segments() -
                                                     req.job().compaction(0).num_input_segments()));
            EXPECT_EQ(stats.index_size(),
                      tablet_stats_pb.index_size() +
                              (req.job().compaction(0).index_size_output_rowsets() -
                               req.job().compaction(0).index_size_input_rowsets()));
            EXPECT_EQ(stats.segment_size(),
                      tablet_stats_pb.segment_size() +
                              (req.job().compaction(0).segment_size_output_rowsets() -
                               req.job().compaction(0).segment_size_input_rowsets()));
        }
    }
}

void check_delete_bitmap_lock(MetaServiceProxy* meta_service, std::string instance_id,
                              int64_t table_id, int64_t lock_id, bool exist) {
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string lock_key = meta_delete_bitmap_update_lock_key({instance_id, table_id, -1});
    std::string lock_val;
    DeleteBitmapUpdateLockPB lock_info;
    TxnErrorCode err = txn->get(lock_key, &lock_val);
    if (exist) {
        ASSERT_EQ(err, TxnErrorCode::TXN_OK) << ", lock_id=" << lock_id << ", exist=" << exist;
    } else {
        ASSERT_EQ(err, TxnErrorCode::TXN_KEY_NOT_FOUND)
                << ", lock_id=" << lock_id << ", exist=" << exist;
        return;
    }
    ASSERT_TRUE(lock_info.ParseFromString(lock_val));
    ASSERT_EQ(lock_info.lock_id(), lock_id) << ", exist=" << exist;
}

void check_job_key(MetaServiceProxy* meta_service, std::string instance_id, int64_t table_id,
                   int64_t initiator, bool exist) {
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string tablet_job_key = mow_tablet_job_key({instance_id, table_id, initiator});
    std::string tablet_job_val;
    TxnErrorCode err = txn->get(tablet_job_key, &tablet_job_val);
    LOG(INFO) << "check_job_key instance_id=" << instance_id << ",initiator=" << initiator
              << ",exist=" << exist << ",err=" << err;
    if (exist) {
        ASSERT_TRUE(err == TxnErrorCode::TXN_OK);
    } else {
        ASSERT_TRUE(err == TxnErrorCode::TXN_KEY_NOT_FOUND);
    }
}

TEST(MetaServiceJobTest, DeleteBitmapUpdateLockCompatibilityTest) {
    auto meta_service = get_meta_service();
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

    brpc::Controller cntl;

    auto test_start_compaction_job = [&](int64_t table_id, int64_t index_id, int64_t partition_id,
                                         int64_t tablet_id,
                                         TabletCompactionJobPB::CompactionType type,
                                         std::string job_id = "job_id123",
                                         std::string instance_id = "MetaServiceJobTest") {
        StartTabletJobResponse res;

        auto index_key = meta_tablet_idx_key({instance_id, tablet_id});
        TabletIndexPB idx_pb;
        idx_pb.set_table_id(table_id);
        idx_pb.set_index_id(index_id);
        idx_pb.set_partition_id(partition_id);
        idx_pb.set_tablet_id(tablet_id);
        std::string idx_val = idx_pb.SerializeAsString();
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        txn->put(index_key, idx_val);
        std::string stats_key =
                stats_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
        TabletStatsPB stats;
        stats.set_base_compaction_cnt(9);
        stats.set_cumulative_compaction_cnt(19);
        txn->put(stats_key, stats.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
        start_compaction_job(meta_service.get(), tablet_id, job_id, "ip:port", 9, 19, type, res);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    };
    FinishTabletJobResponse res;
    auto test_commit_compaction_job = [&](int64_t table_id, int64_t index_id, int64_t partition_id,
                                          int64_t tablet_id,
                                          TabletCompactionJobPB::CompactionType type,
                                          int64_t initiator = 12345,
                                          std::string job_id = "job_id123",
                                          std::string instance_id = "MetaServiceJobTest") {
        FinishTabletJobRequest req;

        auto compaction = req.mutable_job()->add_compaction();
        compaction->set_id(job_id);
        compaction->set_initiator("ip:port");
        compaction->set_base_compaction_cnt(10);
        compaction->set_cumulative_compaction_cnt(20);
        compaction->set_delete_bitmap_lock_initiator(initiator);
        req.mutable_job()->mutable_idx()->set_table_id(table_id);
        req.mutable_job()->mutable_idx()->set_index_id(index_id);
        req.mutable_job()->mutable_idx()->set_partition_id(partition_id);
        req.mutable_job()->mutable_idx()->set_tablet_id(tablet_id);
        req.set_action(FinishTabletJobRequest::COMMIT);
        req.set_cloud_unique_id("test_cloud_unique_id");

        auto tablet_meta_key =
                meta_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        doris::TabletMetaCloudPB tablet_meta_pb;
        tablet_meta_pb.set_table_id(table_id);
        tablet_meta_pb.set_index_id(index_id);
        tablet_meta_pb.set_partition_id(partition_id);
        tablet_meta_pb.set_tablet_id(tablet_id);
        tablet_meta_pb.set_cumulative_layer_point(50);
        std::string tablet_meta_val = tablet_meta_pb.SerializeAsString();
        ASSERT_FALSE(tablet_meta_val.empty());
        txn->put(tablet_meta_key, tablet_meta_val);
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        // Create tablet stats, compaction job will update stats
        auto tablet_stats_key =
                stats_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
        TabletStatsPB tablet_stats_pb;
        tablet_stats_pb.mutable_idx()->set_table_id(table_id);
        tablet_stats_pb.mutable_idx()->set_index_id(index_id);
        tablet_stats_pb.mutable_idx()->set_partition_id(partition_id);
        tablet_stats_pb.mutable_idx()->set_tablet_id(tablet_id);

        std::mt19937 rng(std::chrono::system_clock::now().time_since_epoch().count());
        std::uniform_int_distribution<int> dist(1, 10000); // Positive numbers

        compaction->set_output_cumulative_point(tablet_stats_pb.cumulative_point() + dist(rng));
        compaction->set_num_output_rows(dist(rng));
        compaction->set_num_output_rowsets(dist(rng));
        compaction->set_num_output_segments(dist(rng));
        compaction->set_num_input_rows(dist(rng));
        compaction->set_num_input_rowsets(dist(rng));
        compaction->set_num_input_segments(dist(rng));
        compaction->set_size_input_rowsets(dist(rng));
        compaction->set_size_output_rowsets(dist(rng));
        compaction->set_type(type);

        tablet_stats_pb.set_cumulative_compaction_cnt(dist(rng));
        tablet_stats_pb.set_base_compaction_cnt(dist(rng));
        tablet_stats_pb.set_cumulative_point(tablet_meta_pb.cumulative_layer_point());
        // MUST let data stats be larger than input data size
        tablet_stats_pb.set_num_rows(dist(rng) + compaction->num_input_rows());
        tablet_stats_pb.set_data_size(dist(rng) + compaction->size_input_rowsets());
        tablet_stats_pb.set_num_rowsets(dist(rng) + compaction->num_input_rowsets());
        tablet_stats_pb.set_num_segments(dist(rng) + compaction->num_input_segments());
        tablet_stats_pb.set_index_size(dist(rng) + compaction->index_size_input_rowsets());
        tablet_stats_pb.set_segment_size(dist(rng) + compaction->segment_size_input_rowsets());

        std::string tablet_stats_val = tablet_stats_pb.SerializeAsString();
        ASSERT_FALSE(tablet_stats_val.empty());
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        txn->put(tablet_stats_key, tablet_stats_val);
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        // Provide input and output rowset info
        int64_t input_version_start = dist(rng);
        int64_t input_version_end = input_version_start + 100;
        compaction->add_input_versions(input_version_start);
        compaction->add_input_versions(input_version_end);
        compaction->add_output_versions(input_version_end);
        compaction->add_output_rowset_ids("output rowset id");

        // Provide input rowset KVs, boundary test, 5 input rowsets
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        // clang-format off
        std::vector<std::string> input_rowset_keys = {
                meta_rowset_key({instance_id, tablet_id, input_version_start - 1}),
                meta_rowset_key({instance_id, tablet_id, input_version_start}),
                meta_rowset_key({instance_id, tablet_id, input_version_start + 1}),
                meta_rowset_key({instance_id, tablet_id, (input_version_start + input_version_end) / 2}),
                meta_rowset_key({instance_id, tablet_id, input_version_end - 1}),
                meta_rowset_key({instance_id, tablet_id, input_version_end}),
                meta_rowset_key({instance_id, tablet_id, input_version_end + 1}),
        };
        // clang-format on
        std::vector<std::unique_ptr<std::string>> input_rowset_vals;
        for (auto& i : input_rowset_keys) {
            doris::RowsetMetaCloudPB rs_pb;
            rs_pb.set_rowset_id(0);
            rs_pb.set_rowset_id_v2(hex(i));
            input_rowset_vals.emplace_back(new std::string(rs_pb.SerializeAsString()));
            txn->put(i, *input_rowset_vals.back());
        }
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        int64_t txn_id = dist(rng);
        compaction->add_txn_id(txn_id);

        // Provide output rowset meta
        auto tmp_rowset_key = meta_rowset_tmp_key({instance_id, txn_id, tablet_id});
        doris::RowsetMetaCloudPB tmp_rs_pb;
        tmp_rs_pb.set_rowset_id(0);
        tmp_rs_pb.set_txn_id(10086);
        auto tmp_rowset_val = tmp_rs_pb.SerializeAsString();
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        txn->put(tmp_rowset_key, tmp_rowset_val);
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        meta_service->finish_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);
    };

    auto test_abort_compaction_job = [&](int64_t table_id, int64_t index_id, int64_t partition_id,
                                         int64_t tablet_id, int64_t initiator = 12345,
                                         std::string job_id = "job_id123") {
        FinishTabletJobRequest req;
        FinishTabletJobResponse res;

        auto compaction = req.mutable_job()->add_compaction();
        compaction->set_id(job_id);
        compaction->set_initiator("ip:port");
        compaction->set_delete_bitmap_lock_initiator(initiator);
        req.mutable_job()->mutable_idx()->set_tablet_id(tablet_id);
        req.set_action(FinishTabletJobRequest::ABORT);
        meta_service->finish_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);

        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        auto job_key = job_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
        std::string job_val;
        ASSERT_EQ(txn->get(job_key, &job_val), TxnErrorCode::TXN_OK);
        TabletJobInfoPB job_pb;
        ASSERT_TRUE(job_pb.ParseFromString(job_val));
        ASSERT_TRUE(job_pb.compaction().empty());
    };

    auto clear_rowsets = [&](int64_t tablet_id) {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string key1 = meta_rowset_key({instance_id, tablet_id, 1});
        std::string key2 = meta_rowset_key({instance_id, tablet_id, 10001});
        txn->remove(key1, key2);
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    };

    config::use_delete_bitmap_lock_random_version = false;
    int64_t table_id = 111;
    remove_delete_bitmap_lock(meta_service.get(), table_id);

    // test compaction and schema change
    std::set<std::tuple<int, int, int, int, MetaServiceCode>> use_versions = {
            {1, 1, 1, 1, MetaServiceCode::OK},
            {1, 1, 1, 2, MetaServiceCode::OK},
            {1, 1, 2, 1, MetaServiceCode::OK},
            {1, 1, 2, 2, MetaServiceCode::OK},
            {1, 2, 1, 1, MetaServiceCode::LOCK_CONFLICT},
            {1, 2, 1, 2, MetaServiceCode::LOCK_CONFLICT},
            {1, 2, 2, 1, MetaServiceCode::OK},
            {1, 2, 2, 2, MetaServiceCode::OK},
            {2, 1, 1, 1, MetaServiceCode::OK},
            {2, 1, 1, 2, MetaServiceCode::OK},
            {2, 1, 2, 1, MetaServiceCode::OK},
            {2, 1, 2, 2, MetaServiceCode::OK},
            {2, 2, 1, 1, MetaServiceCode::LOCK_CONFLICT},
            {2, 2, 1, 2, MetaServiceCode::LOCK_CONFLICT},
            {2, 2, 2, 1, MetaServiceCode::OK},
            {2, 2, 2, 2, MetaServiceCode::OK}};
    for (auto& use_version : use_versions) {
        int64_t version1 = std::get<0>(use_version);
        int64_t version2 = std::get<1>(use_version);
        int64_t version3 = std::get<2>(use_version);
        int64_t version4 = std::get<3>(use_version);
        LOG(INFO) << "version1=" << version1 << ", version2=" << version2
                  << ", version3=" << version3 << ", version4=" << version4;

        config::delete_bitmap_lock_v2_white_list = version1 == 1 ? "" : "*";
        // 1. compaction1 get lock, but not commit job, the lock is timeout
        auto tablet_id = 5;
        auto new_tablet_id = 6;
        test_start_compaction_job(table_id, 2, 3, tablet_id, TabletCompactionJobPB::BASE);
        auto res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 12345, 1);
        ASSERT_EQ(res_code, MetaServiceCode::OK);
        sleep(2);
        // 2. schema change1 get lock (commit; or timeout)
        config::delete_bitmap_lock_v2_white_list = version2 == 1 ? "" : "*";
        ASSERT_NO_FATAL_FAILURE(
                create_tablet(meta_service.get(), table_id, 2, 3, new_tablet_id, true, true));
        StartTabletJobResponse start_sc_res;
        ASSERT_NO_FATAL_FAILURE(start_schema_change_job(meta_service.get(), table_id, 2, 3,
                                                        tablet_id, new_tablet_id, "job1", "be1",
                                                        start_sc_res));
        res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -2, 12346);
        ASSERT_EQ(res_code, MetaServiceCode::OK);
        std::vector<doris::RowsetMetaCloudPB> output_rowsets;
        for (int64_t i = 0; i < 5; ++i) {
            output_rowsets.push_back(create_rowset(new_tablet_id, i + 2, i + 2));
            CreateRowsetResponse create_rowset_response;
            commit_rowset(meta_service.get(), output_rowsets.back(), create_rowset_response);
            ASSERT_EQ(create_rowset_response.status().code(), MetaServiceCode::OK) << i;
        }
        FinishTabletJobResponse finish_sc_res;
        finish_schema_change_job(meta_service.get(), tablet_id, new_tablet_id, "job1", "be1",
                                 output_rowsets, finish_sc_res, 12346);
        ASSERT_EQ(finish_sc_res.status().code(), MetaServiceCode::OK);
        // 3. compaction2 get lock, compaction1 start commit
        config::delete_bitmap_lock_v2_white_list = version3 == 1 ? "" : "*";
        res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 12347);
        ASSERT_EQ(res_code, std::get<4>(use_version));
        config::delete_bitmap_lock_v2_white_list = version4 == 1 ? "" : "*";
        test_commit_compaction_job(table_id, 2, 3, tablet_id, TabletCompactionJobPB::BASE);
        ASSERT_EQ(res.status().code(), MetaServiceCode::LOCK_EXPIRED);
        clear_rowsets(table_id);
        clear_rowsets(new_tablet_id);
        remove_delete_bitmap_lock(meta_service.get(), table_id);
    }

    // case 1: lock key does not exist, get and remove load lock in new way, success
    config::delete_bitmap_lock_v2_white_list = "*";
    auto res_code = get_delete_bitmap_lock(meta_service.get(), table_id, 123, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    res_code = remove_delete_bitmap_lock(meta_service.get(), table_id, 123, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);

    // case 2: lock key does not exist, get and remove load lock in old way, success
    config::delete_bitmap_lock_v2_white_list = "";
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, 123, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    res_code = remove_delete_bitmap_lock(meta_service.get(), table_id, 123, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);

    // case 3: lock key does not exist, get and remove compaction lock in new way, success
    config::delete_bitmap_lock_v2_white_list = "*";
    test_start_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE);
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 12345);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    res_code = update_delete_bitmap(meta_service.get(), table_id, 3, 5, -1, 12345);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    test_commit_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

    // case 4: lock key does not exist, get and remove compaction lock in old way, success
    config::delete_bitmap_lock_v2_white_list = "";
    test_start_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE);
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 12345);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    res_code = update_delete_bitmap(meta_service.get(), table_id, 3, 5, -1, 12345);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    test_commit_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

    // case 5:
    // 5.1 lock key does not exist
    remove_delete_bitmap_lock(meta_service.get(), table_id);
    clear_rowsets(table_id);
    // 5.2 load get lock in new way
    config::delete_bitmap_lock_v2_white_list = "*";
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, 222, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    // 5.3 compaction get lock in new way, failed
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 111);
    ASSERT_EQ(res_code, MetaServiceCode::LOCK_CONFLICT);
    // 5.4 load remove lock in old way, success
    config::delete_bitmap_lock_v2_white_list = "";
    res_code = remove_delete_bitmap_lock(meta_service.get(), table_id, 222, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);

    // case 6:
    // 6.1 lock key does not exist
    remove_delete_bitmap_lock(meta_service.get(), table_id);
    clear_rowsets(table_id);
    // 6.2 load get lock in old way
    config::delete_bitmap_lock_v2_white_list = "";
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, 222, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    // 6.3 compaction get lock in old way, failed
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 111);
    ASSERT_EQ(res_code, MetaServiceCode::LOCK_CONFLICT);
    // 6.4 load remove lock in new way, success
    config::delete_bitmap_lock_v2_white_list = "*";
    res_code = remove_delete_bitmap_lock(meta_service.get(), table_id, 222, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);

    // case 7:
    // 7.1 lock key does not exist
    remove_delete_bitmap_lock(meta_service.get(), table_id);
    clear_rowsets(table_id);
    // 7.2 compaction get lock in new way
    config::delete_bitmap_lock_v2_white_list = "*";
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 777);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    // 7.3 load get lock in new way
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, 123, -1);
    ASSERT_EQ(res_code, MetaServiceCode::LOCK_CONFLICT);
    // 7.4 compaction update delete bitmap in old way failed, abort compaction job
    config::delete_bitmap_lock_v2_white_list = "";
    test_start_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE);
    res_code = update_delete_bitmap(meta_service.get(), table_id, 3, 5, -1, 777);
    ASSERT_EQ(res_code, MetaServiceCode::LOCK_EXPIRED);
    test_abort_compaction_job(table_id, 2, 3, 5, 777);
    // 7.5 load get lock fail
    std::string white_lists[] = {"", "*"};
    for (auto& white_list : white_lists) {
        config::delete_bitmap_lock_v2_white_list = white_list;
        res_code = get_delete_bitmap_lock(meta_service.get(), table_id, 123, -1);
        ASSERT_EQ(res_code, MetaServiceCode::LOCK_CONFLICT);
    }

    // case 8:
    // 8.1 lock key does not exist
    remove_delete_bitmap_lock(meta_service.get(), table_id);
    clear_rowsets(table_id);
    // 8.2 compaction get lock in old way
    config::delete_bitmap_lock_v2_white_list = "";
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 888);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    // 8.3 load get lock in old way
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, 124, -1);
    ASSERT_EQ(res_code, MetaServiceCode::LOCK_CONFLICT);
    // 8.4 compaction update delete bitmap in new way failed, abort compaction job
    config::delete_bitmap_lock_v2_white_list = "*";
    test_start_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE);
    res_code = update_delete_bitmap(meta_service.get(), table_id, 3, 5, -1, 888);
    ASSERT_EQ(res_code, MetaServiceCode::LOCK_EXPIRED);
    test_abort_compaction_job(table_id, 2, 3, 5, 888);
    // 8.5 load get lock success
    for (auto& white_list : white_lists) {
        config::delete_bitmap_lock_v2_white_list = white_list;
        res_code = get_delete_bitmap_lock(meta_service.get(), table_id, 123, -1);
        ASSERT_EQ(res_code, MetaServiceCode::OK);
    }

    // case 9:
    // 9.1 lock key does not exist
    remove_delete_bitmap_lock(meta_service.get(), table_id);
    clear_rowsets(table_id);
    // 9.2 compaction1 get lock in new way
    config::delete_bitmap_lock_v2_white_list = "*";
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 901);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    // 9.3 compaction2 get and remove lock in old way
    config::delete_bitmap_lock_v2_white_list = "";
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 902);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    test_start_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE);
    res_code = update_delete_bitmap(meta_service.get(), table_id, 3, 5, -1, 902);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    test_commit_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE, 902);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    // 9.4 load get and remove lock in old way
    config::delete_bitmap_lock_v2_white_list = "";
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, 199, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    res_code = remove_delete_bitmap_lock(meta_service.get(), table_id, 199, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    // 9.5 compaction3 get lock in new way
    config::delete_bitmap_lock_v2_white_list = "*";
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 903);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    // 9.6 compaction1 update delete bitmap in new way failed, abort job
    test_start_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE);
    res_code = update_delete_bitmap(meta_service.get(), table_id, 3, 5, -1, 901);
    ASSERT_EQ(res_code, MetaServiceCode::LOCK_EXPIRED);
    test_abort_compaction_job(table_id, 2, 3, 5, 901);

    // case 10:
    // 10.1 lock key does not exist
    remove_delete_bitmap_lock(meta_service.get(), table_id);
    clear_rowsets(table_id);
    // 10.2 compaction1 get lock in new way
    config::delete_bitmap_lock_v2_white_list = "*";
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 1001);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    // 10.3 compaction2 get and remove lock in old way
    config::delete_bitmap_lock_v2_white_list = "";
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 1002);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    test_start_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE);
    res_code = update_delete_bitmap(meta_service.get(), table_id, 3, 5, -1, 1002);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    test_commit_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE, 1002);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    // 10.4 load get and remove lock in old way
    config::delete_bitmap_lock_v2_white_list = "";
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, 1910, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    res_code = remove_delete_bitmap_lock(meta_service.get(), table_id, 1910, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    // 10.5 compaction3 get lock in old way
    config::delete_bitmap_lock_v2_white_list = "";
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 1003);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    // 10.6 compaction1 update delete bitmap in new way success, commit job failed
    config::delete_bitmap_lock_v2_white_list = "*";
    test_start_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE);
    res_code = update_delete_bitmap(meta_service.get(), table_id, 3, 5, -1, 1001);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    test_commit_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE, 1001);
    ASSERT_EQ(res.status().code(), MetaServiceCode::LOCK_EXPIRED);

    // case 11:
    // 11.1 lock key does not exist
    remove_delete_bitmap_lock(meta_service.get(), table_id);
    clear_rowsets(table_id);
    // 11.2 compaction1 get lock in new way
    config::delete_bitmap_lock_v2_white_list = "*";
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 1101);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    // 11.3 compaction2 get and remove lock in old way
    config::delete_bitmap_lock_v2_white_list = "";
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 1102);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    test_start_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE);
    res_code = update_delete_bitmap(meta_service.get(), table_id, 3, 5, -1, 1102);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    test_commit_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE, 1102);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    // 11.4 sc get and remove lock in old way
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -2, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    res_code = remove_delete_bitmap_lock(meta_service.get(), table_id, -2, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    // 11.5 compaction3 get lock in new way
    config::delete_bitmap_lock_v2_white_list = "*";
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 1103);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    // 11.6 compaction1 update delete bitmap in new way failed, abort job
    config::delete_bitmap_lock_v2_white_list = "*";
    test_start_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE);
    res_code = update_delete_bitmap(meta_service.get(), table_id, 3, 5, -1, 1101);
    ASSERT_EQ(res_code, MetaServiceCode::LOCK_EXPIRED);
    test_abort_compaction_job(table_id, 2, 3, 5, 1101);

    // case 12:
    // 12.1 lock key does not exist
    remove_delete_bitmap_lock(meta_service.get(), table_id);
    clear_rowsets(table_id);
    // 12.2 compaction1 get lock in new way
    config::delete_bitmap_lock_v2_white_list = "*";
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 1201);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    // 12.3 compaction2 get and remove lock in old way
    config::delete_bitmap_lock_v2_white_list = "";
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 1202);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    test_start_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE);
    res_code = update_delete_bitmap(meta_service.get(), table_id, 3, 5, -1, 1202);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    test_commit_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE, 1202);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    // 12.4 sc get and remove lock in old way
    config::delete_bitmap_lock_v2_white_list = "";
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -2, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    res_code = remove_delete_bitmap_lock(meta_service.get(), table_id, -2, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    // 12.5 compaction3 get lock in old way
    config::delete_bitmap_lock_v2_white_list = "";
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 1203);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    // 12.6 compaction1 update delete bitmap in new way success, commit job failed
    config::delete_bitmap_lock_v2_white_list = "*";
    test_start_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE);
    res_code = update_delete_bitmap(meta_service.get(), table_id, 3, 5, -1, 1201);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    test_commit_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE, 1201);
    ASSERT_EQ(res.status().code(), MetaServiceCode::LOCK_EXPIRED);

    // case 13:
    // 13.1 lock key does not exist
    remove_delete_bitmap_lock(meta_service.get(), table_id);
    clear_rowsets(table_id);
    // 13.2 compaction1 get lock in new way
    config::delete_bitmap_lock_v2_white_list = "*";
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 1301);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    // 13.3 compaction2 get and remove lock in old way
    config::delete_bitmap_lock_v2_white_list = "";
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 1302);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    test_start_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE);
    res_code = update_delete_bitmap(meta_service.get(), table_id, 3, 5, -1, 1302);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    test_commit_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE, 1302);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    // 13.4 load get and remove lock in old way
    config::delete_bitmap_lock_v2_white_list = "";
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, 1390, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    res_code = remove_delete_bitmap_lock(meta_service.get(), table_id, 1390, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    // 13.5 compaction3 get lock in new way
    config::delete_bitmap_lock_v2_white_list = "*";
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 1303);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    // 13.6 compaction1 update delete bitmap in new way failed, abort job
    config::delete_bitmap_lock_v2_white_list = "*";
    test_start_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE);
    res_code = update_delete_bitmap(meta_service.get(), table_id, 3, 5, -1, 1301);
    ASSERT_EQ(res_code, MetaServiceCode::LOCK_EXPIRED);
    test_abort_compaction_job(table_id, 2, 3, 5, 1301);

    // case 14:
    // 14.1 lock key does not exist
    remove_delete_bitmap_lock(meta_service.get(), table_id);
    clear_rowsets(table_id);
    // 14.2 compaction1 get lock in new way
    config::delete_bitmap_lock_v2_white_list = "*";
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 1401);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    // 14.3 compaction2 get and remove lock in old way
    config::delete_bitmap_lock_v2_white_list = "";
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 1402);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    test_start_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE);
    res_code = update_delete_bitmap(meta_service.get(), table_id, 3, 5, -1, 1402);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    test_commit_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE, 1402);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    // 14.4 load get and remove lock in old way
    config::delete_bitmap_lock_v2_white_list = "";
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, 1490, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    res_code = remove_delete_bitmap_lock(meta_service.get(), table_id, 1490, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    // 14.5 compaction3 get lock in old way
    config::delete_bitmap_lock_v2_white_list = "";
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 1403);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    // 14.6 compaction1 update delete bitmap in old way failed, abort job
    config::delete_bitmap_lock_v2_white_list = "";
    test_start_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE);
    res_code = update_delete_bitmap(meta_service.get(), table_id, 3, 5, -1, 1401);
    ASSERT_EQ(res_code, MetaServiceCode::LOCK_EXPIRED);
    test_abort_compaction_job(table_id, 2, 3, 5, 1401);

    // case 15:
    // 15.1 lock key does not exist
    remove_delete_bitmap_lock(meta_service.get(), table_id);
    clear_rowsets(table_id);
    // 15.2 compaction1 get lock in new way
    config::delete_bitmap_lock_v2_white_list = "*";
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 1501);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    // 15.3 compaction2 get and remove lock in old way
    config::delete_bitmap_lock_v2_white_list = "";
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 1502);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    test_start_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE);
    res_code = update_delete_bitmap(meta_service.get(), table_id, 3, 5, -1, 1502);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    test_commit_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE, 1502);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    // 15.4 sc get and remove lock in old way
    config::delete_bitmap_lock_v2_white_list = "";
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -2, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    res_code = remove_delete_bitmap_lock(meta_service.get(), table_id, -2, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    // 15.5 compaction3 get lock in new way
    config::delete_bitmap_lock_v2_white_list = "*";
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 1503);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    // 15.6 compaction1 update delete bitmap in old way failed, abort job
    config::delete_bitmap_lock_v2_white_list = "";
    test_start_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE);
    res_code = update_delete_bitmap(meta_service.get(), table_id, 3, 5, -1, 1501);
    ASSERT_EQ(res_code, MetaServiceCode::LOCK_EXPIRED);
    test_abort_compaction_job(table_id, 2, 3, 5, 1501);

    // case 16:
    // 16.1 lock key does not exist
    remove_delete_bitmap_lock(meta_service.get(), table_id);
    clear_rowsets(table_id);
    // 16.2 compaction1 get lock in new way
    config::delete_bitmap_lock_v2_white_list = "*";
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 1601);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    // 16.3 compaction2 get and remove lock in old way
    config::delete_bitmap_lock_v2_white_list = "";
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 1602);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    test_start_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE);
    res_code = update_delete_bitmap(meta_service.get(), table_id, 3, 5, -1, 1602);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    test_commit_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE, 1602);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    // 16.4 sc get and remove lock in old way
    config::delete_bitmap_lock_v2_white_list = "";
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -2, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    res_code = remove_delete_bitmap_lock(meta_service.get(), table_id, -2, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    // 16.5 compaction3 get lock in old way
    config::delete_bitmap_lock_v2_white_list = "";
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 1603);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    // 16.6 compaction1 update delete bitmap in old way failed, abort job
    config::delete_bitmap_lock_v2_white_list = "";
    test_start_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE);
    res_code = update_delete_bitmap(meta_service.get(), table_id, 3, 5, -1, 1601);
    ASSERT_EQ(res_code, MetaServiceCode::LOCK_EXPIRED);
    test_abort_compaction_job(table_id, 2, 3, 5, 1601);

    // case 17:
    // 17.1 lock key does not exist
    remove_delete_bitmap_lock(meta_service.get(), table_id);
    clear_rowsets(table_id);
    // 17.2 compaction1 get lock in old way
    config::delete_bitmap_lock_v2_white_list = "";
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 1701);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    // 17.3 load get and remove lock in new way
    config::delete_bitmap_lock_v2_white_list = "*";
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, 1793, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    res_code = remove_delete_bitmap_lock(meta_service.get(), table_id, 1793, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    // 17.4 compaction2 get lock in new way
    config::delete_bitmap_lock_v2_white_list = "*";
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 1702);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    // 17.5 compaction1 update delete bitmap in new way failed, abort job
    config::delete_bitmap_lock_v2_white_list = "*";
    test_start_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE);
    res_code = update_delete_bitmap(meta_service.get(), table_id, 3, 5, -1, 1701);
    ASSERT_EQ(res_code, MetaServiceCode::LOCK_EXPIRED);
    test_abort_compaction_job(table_id, 2, 3, 5, 1701);

    // case 18:
    // 18.1 lock key does not exist
    remove_delete_bitmap_lock(meta_service.get(), table_id);
    clear_rowsets(table_id);
    // 18.2 compaction1 get lock in old way
    config::delete_bitmap_lock_v2_white_list = "";
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 1801);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    // 18.3 load get and remove lock in new way
    config::delete_bitmap_lock_v2_white_list = "*";
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, 1893, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    res_code = remove_delete_bitmap_lock(meta_service.get(), table_id, 1893, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    // 18.4 compaction2 get lock in old way
    config::delete_bitmap_lock_v2_white_list = "";
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 1802);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    // 18.5 compaction1 update delete bitmap in new way failed, abort job
    config::delete_bitmap_lock_v2_white_list = "*";
    test_start_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE);
    res_code = update_delete_bitmap(meta_service.get(), table_id, 3, 5, -1, 1801);
    ASSERT_EQ(res_code, MetaServiceCode::LOCK_EXPIRED);
    test_abort_compaction_job(table_id, 2, 3, 5, 1801);

    // case 19:
    // 19.1 lock key does not exist
    remove_delete_bitmap_lock(meta_service.get(), table_id);
    clear_rowsets(table_id);
    // 19.2 compaction1 get lock in old way
    config::delete_bitmap_lock_v2_white_list = "";
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 1901);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    // 19.3 sc get and remove lock in new way
    config::delete_bitmap_lock_v2_white_list = "*";
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -2, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    res_code = remove_delete_bitmap_lock(meta_service.get(), table_id, -2, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    // 19.4 compaction2 get lock in new way
    config::delete_bitmap_lock_v2_white_list = "*";
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 1902);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    // 19.5 compaction1 update delete bitmap in new way failed, abort job
    config::delete_bitmap_lock_v2_white_list = "*";
    test_start_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE);
    res_code = update_delete_bitmap(meta_service.get(), table_id, 3, 5, -1, 1901);
    ASSERT_EQ(res_code, MetaServiceCode::LOCK_EXPIRED);
    test_abort_compaction_job(table_id, 2, 3, 5, 1901);

    // case 20:
    // 20.1 lock key does not exist
    remove_delete_bitmap_lock(meta_service.get(), table_id);
    clear_rowsets(table_id);
    // 20.2 compaction1 get lock in old way
    config::delete_bitmap_lock_v2_white_list = "";
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 2001);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    // 20.3 sc get and remove lock in new way
    config::delete_bitmap_lock_v2_white_list = "*";
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -2, -1, 1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    res_code = remove_delete_bitmap_lock(meta_service.get(), table_id, -2, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    // 20.4 compaction2 get lock in old way
    sleep(2);
    config::delete_bitmap_lock_v2_white_list = "";
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 2002);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    // 20.5 compaction1 update delete bitmap in new way failed, abort job
    config::delete_bitmap_lock_v2_white_list = "*";
    test_start_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE);
    res_code = update_delete_bitmap(meta_service.get(), table_id, 3, 5, -1, 2001);
    ASSERT_EQ(res_code, MetaServiceCode::LOCK_EXPIRED);
    test_abort_compaction_job(table_id, 2, 3, 5, 2001);

    // case 21:
    // 21.1 lock key does not exist
    remove_delete_bitmap_lock(meta_service.get(), table_id);
    clear_rowsets(table_id);
    // 21.2 compaction1 get lock in old way
    config::delete_bitmap_lock_v2_white_list = "";
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 2101);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    // 21.3 load get and remove lock in new way
    config::delete_bitmap_lock_v2_white_list = "*";
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, 2190, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    res_code = remove_delete_bitmap_lock(meta_service.get(), table_id, 2190, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    // 21.4 compaction2 get lock in new way
    config::delete_bitmap_lock_v2_white_list = "*";
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 2102);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    // 21.5 compaction1 update delete bitmap in old way failed, abort job
    config::delete_bitmap_lock_v2_white_list = "";
    test_start_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE);
    res_code = update_delete_bitmap(meta_service.get(), table_id, 3, 5, -1, 2101);
    ASSERT_EQ(res_code, MetaServiceCode::LOCK_EXPIRED);
    test_abort_compaction_job(table_id, 2, 3, 5, 2101);

    // case 22:
    // 22.1 lock key does not exist
    remove_delete_bitmap_lock(meta_service.get(), table_id);
    clear_rowsets(table_id);
    // 22.2 compaction1 get lock in old way
    config::delete_bitmap_lock_v2_white_list = "";
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 2201);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    // 22.3 load get and remove lock in new way
    config::delete_bitmap_lock_v2_white_list = "*";
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, 2290, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    res_code = remove_delete_bitmap_lock(meta_service.get(), table_id, 2290, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    // 22.4 compaction2 get lock in old way
    config::delete_bitmap_lock_v2_white_list = "";
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 2202);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    // 22.5 compaction1 update delete bitmap in old way failed, abort job
    config::delete_bitmap_lock_v2_white_list = "";
    test_start_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE);
    res_code = update_delete_bitmap(meta_service.get(), table_id, 3, 5, -1, 2201);
    ASSERT_EQ(res_code, MetaServiceCode::LOCK_EXPIRED);
    test_abort_compaction_job(table_id, 2, 3, 5, 2201);

    // case 23:
    // 23.1 lock key does not exist
    remove_delete_bitmap_lock(meta_service.get(), table_id);
    clear_rowsets(table_id);
    // 23.2 compaction1 get lock in old way
    config::delete_bitmap_lock_v2_white_list = "";
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 2301);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    // 23.3 sc get and remove lock in new way
    config::delete_bitmap_lock_v2_white_list = "*";
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, 2390, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    res_code = remove_delete_bitmap_lock(meta_service.get(), table_id, 2390, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    // 23.4 compaction2 get lock in new way
    config::delete_bitmap_lock_v2_white_list = "*";
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 2302);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    // 23.5 compaction1 update delete bitmap in old way failed, abort job
    config::delete_bitmap_lock_v2_white_list = "";
    test_start_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE);
    res_code = update_delete_bitmap(meta_service.get(), table_id, 3, 5, -1, 2301);
    ASSERT_EQ(res_code, MetaServiceCode::LOCK_EXPIRED);
    test_abort_compaction_job(table_id, 2, 3, 5, 2301);

    // case 24:
    // 24.1 lock key does not exist
    remove_delete_bitmap_lock(meta_service.get(), table_id);
    clear_rowsets(table_id);
    // 24.2 compaction1 get lock in old way
    config::delete_bitmap_lock_v2_white_list = "";
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 2401);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    // 24.3 sc get and remove lock in new way
    config::delete_bitmap_lock_v2_white_list = "*";
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -2, -1, 1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    res_code = remove_delete_bitmap_lock(meta_service.get(), table_id, -2, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    // 24.4 compaction2 get lock in old way
    sleep(2);
    config::delete_bitmap_lock_v2_white_list = "";
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 2402);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    // 24.5 compaction1 update delete bitmap in old way failed, abort job
    config::delete_bitmap_lock_v2_white_list = "";
    test_start_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE);
    res_code = update_delete_bitmap(meta_service.get(), table_id, 3, 5, -1, 2401);
    ASSERT_EQ(res_code, MetaServiceCode::LOCK_EXPIRED);
    test_abort_compaction_job(table_id, 2, 3, 5, 2401);

    // fuzzy case 1:
    // 1.1 lock key does not exist
    // 1.2 compaction1 get lock in old way
    // 1.3 sc、load、compaction get and remove lock in new or old way
    // 1.4 compaction2 get lock in old way
    // 1.5 if sc or load succeed，compaction1 remove lock in old way failed

    // fuzzy case 2:
    // 2.1 lock key does not exist
    // 2.2 compaction1 get lock in new way
    // 2.3 sc、load、compaction get and remove lock in new or old way
    // 2.4 compaction2 get lock in new way
    // 2.5 if sc or load succeed，compaction1 remove lock in new way failed
    table_id = 222;
    for (int i = 1; i <= 2; i++) {
        std::string lock_version = "v1";
        if (i == 1) {
            lock_version = "v1";
        } else {
            lock_version = "v2";
        }
        // 1 lock key does not exist
        remove_delete_bitmap_lock(meta_service.get(), table_id);
        clear_rowsets(table_id);
        // 2 compaction1 get lock in old/new way
        if (lock_version == "v1") {
            config::delete_bitmap_lock_v2_white_list = "";
        } else {
            config::delete_bitmap_lock_v2_white_list = "*";
        }
        res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 2501);
        ASSERT_EQ(res_code, MetaServiceCode::OK);
        // 3 sc、load、compaction get and remove lock in new or old way
        bool load_or_sc_succeed = false;
        bool compaction_succeed = false;
        std::srand(std::time(0));
        for (int i = 0; i < 10; i++) {
            int num = std::rand() % 3;
            std::string use_version = (std::rand() % 2 == 0 ? "v2" : "v1");
            if (use_version == "v1") {
                config::delete_bitmap_lock_v2_white_list = "";
            } else {
                config::delete_bitmap_lock_v2_white_list = "*";
            }
            LOG(INFO) << "i=" << i << ",num=" << num << ",use_version=" << use_version;
            switch (num) {
            case 0: {
                res_code = get_delete_bitmap_lock(meta_service.get(), table_id, 2590, -1);
                if (res_code == MetaServiceCode::OK) {
                    res_code = remove_delete_bitmap_lock(meta_service.get(), table_id, 2590, -1);
                    if (res_code == MetaServiceCode::OK) {
                        load_or_sc_succeed = true;
                    }
                }
                LOG(INFO) << "i=" << i << ",load_or_sc_succeed=" << load_or_sc_succeed
                          << ",compaction_succeed=" << compaction_succeed;
                break;
            }
            case 1: {
                res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -2, -1, 1);
                if (res_code == MetaServiceCode::OK) {
                    res_code = remove_delete_bitmap_lock(meta_service.get(), table_id, -2, -1);
                    if (res_code == MetaServiceCode::OK) {
                        load_or_sc_succeed = true;
                    }
                }
                LOG(INFO) << "i=" << i << ",load_or_sc_succeed=" << load_or_sc_succeed
                          << ",compaction_succeed=" << compaction_succeed;
                sleep(2);
                break;
            }
            case 2: {
                res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 6600 + i);
                if (res_code == MetaServiceCode::OK) {
                    test_start_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE);
                    res_code =
                            update_delete_bitmap(meta_service.get(), table_id, 3, 5, -1, 6600 + i);
                    if (res.status().code() == MetaServiceCode::OK) {
                        test_commit_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE,
                                                   6600 + i);
                        if (res.status().code() == MetaServiceCode::OK) {
                            compaction_succeed = true;
                        }
                    }
                }
                LOG(INFO) << "i=" << i << ",load_or_sc_succeed=" << load_or_sc_succeed
                          << ",compaction_succeed=" << compaction_succeed;
                break;
            }
            }
        }
        // 4 compaction2 get lock in old/new way
        if (lock_version == "v1") {
            config::delete_bitmap_lock_v2_white_list = "";
        } else {
            config::delete_bitmap_lock_v2_white_list = "*";
        }
        res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 2502);
        ASSERT_EQ(res_code, MetaServiceCode::OK);
        // 5 if sc or load succeed，compaction1 remove lock in old/new way failed
        if (lock_version == "v1") {
            config::delete_bitmap_lock_v2_white_list = "";
        } else {
            config::delete_bitmap_lock_v2_white_list = "*";
        }
        test_start_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE);
        test_commit_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE, 2501);
        if (load_or_sc_succeed) {
            ASSERT_EQ(res.status().code(), MetaServiceCode::LOCK_EXPIRED);
        }
    }

    //white list test
    //1. normal test
    config::delete_bitmap_lock_v2_white_list =
            "instance_id1;instance_id1;instance_id2;仓库6;Instance_id7;Instance_id*;instance_id10$;"
            "instance_id11 ;;instance_id12";
    //after parse config, the following instance will use v2:
    //instance_id1,instance_id2,instance_id11,instance_id12
    table_id = 7890;
    //compaction
    remove_delete_bitmap_lock(meta_service.get(), table_id);
    std::string instance_id_x = "instance_id1";
    sp->set_call_back("get_instance_id", [&](auto&& args) {
        auto* ret = try_any_cast_ret<std::string>(args);
        ret->first = instance_id_x;
        ret->second = true;
    });
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 101);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, -1, true);
    check_job_key(meta_service.get(), instance_id_x, table_id, 101, true);
    test_start_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE, "job_id123",
                              instance_id_x);
    res_code = update_delete_bitmap(meta_service.get(), table_id, 3, 5, -1, 101);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    test_commit_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE, 101, "job_id123",
                               instance_id_x);
    check_job_key(meta_service.get(), instance_id_x, table_id, 101, false);

    instance_id_x = "instance_id2";
    sp->set_call_back("get_instance_id", [&](auto&& args) {
        auto* ret = try_any_cast_ret<std::string>(args);
        ret->first = instance_id_x;
        ret->second = true;
    });
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 102);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, -1, true);
    check_job_key(meta_service.get(), instance_id_x, table_id, 102, true);
    test_start_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE, "job_id123",
                              instance_id_x);
    res_code = update_delete_bitmap(meta_service.get(), table_id, 3, 5, -1, 102);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    test_commit_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE, 102, "job_id123",
                               instance_id_x);
    check_job_key(meta_service.get(), instance_id_x, table_id, 102, false);

    instance_id_x = "instance_id3";
    sp->set_call_back("get_instance_id", [&](auto&& args) {
        auto* ret = try_any_cast_ret<std::string>(args);
        ret->first = instance_id_x;
        ret->second = true;
    });
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 103);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, -1, true);
    test_start_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE, "job_id123",
                              instance_id_x);
    res_code = update_delete_bitmap(meta_service.get(), table_id, 3, 5, -1, 103);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    test_commit_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE, 103, "job_id123",
                               instance_id_x);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, -1, false);

    instance_id_x = "instance_id6";
    sp->set_call_back("get_instance_id", [&](auto&& args) {
        auto* ret = try_any_cast_ret<std::string>(args);
        ret->first = instance_id_x;
        ret->second = true;
    });
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 106);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, -1, true);
    test_start_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE, "job_id123",
                              instance_id_x);
    res_code = update_delete_bitmap(meta_service.get(), table_id, 3, 5, -1, 106);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    test_commit_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE, 106, "job_id123",
                               instance_id_x);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, -1, false);

    instance_id_x = "instance_id7";
    sp->set_call_back("get_instance_id", [&](auto&& args) {
        auto* ret = try_any_cast_ret<std::string>(args);
        ret->first = instance_id_x;
        ret->second = true;
    });
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 107);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, -1, true);
    test_start_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE, "job_id123",
                              instance_id_x);
    res_code = update_delete_bitmap(meta_service.get(), table_id, 3, 5, -1, 107);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    test_commit_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE, 107, "job_id123",
                               instance_id_x);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, -1, false);

    instance_id_x = "instance_id10";
    sp->set_call_back("get_instance_id", [&](auto&& args) {
        auto* ret = try_any_cast_ret<std::string>(args);
        ret->first = instance_id_x;
        ret->second = true;
    });
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 1010);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, -1, true);
    test_start_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE, "job_id123",
                              instance_id_x);
    res_code = update_delete_bitmap(meta_service.get(), table_id, 3, 5, -1, 1010);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    test_commit_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE, 1010, "job_id123",
                               instance_id_x);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, -1, false);

    instance_id_x = "instance_id11";
    sp->set_call_back("get_instance_id", [&](auto&& args) {
        auto* ret = try_any_cast_ret<std::string>(args);
        ret->first = instance_id_x;
        ret->second = true;
    });
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 1011);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, -1, true);
    check_job_key(meta_service.get(), instance_id_x, table_id, 1011, true);
    test_start_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE, "job_id123",
                              instance_id_x);
    res_code = update_delete_bitmap(meta_service.get(), table_id, 3, 5, -1, 1011);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    test_commit_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE, 1011, "job_id123",
                               instance_id_x);
    check_job_key(meta_service.get(), instance_id_x, table_id, 1011, false);

    instance_id_x = "instance_id12";
    sp->set_call_back("get_instance_id", [&](auto&& args) {
        auto* ret = try_any_cast_ret<std::string>(args);
        ret->first = instance_id_x;
        ret->second = true;
    });
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 1012);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, -1, true);
    check_job_key(meta_service.get(), instance_id_x, table_id, 1012, true);
    test_start_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE, "job_id123",
                              instance_id_x);
    res_code = update_delete_bitmap(meta_service.get(), table_id, 3, 5, -1, 1012);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    test_commit_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE, 1012, "job_id123",
                               instance_id_x);
    check_job_key(meta_service.get(), instance_id_x, table_id, 1012, false);

    //load
    instance_id_x = "instance_id1";
    sp->set_call_back("get_instance_id", [&](auto&& args) {
        auto* ret = try_any_cast_ret<std::string>(args);
        ret->first = instance_id_x;
        ret->second = true;
    });
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, 1121, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, 1121, true);
    res_code = remove_delete_bitmap_lock(meta_service.get(), table_id, 1121, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, 1121, false);

    instance_id_x = "instance_id2";
    sp->set_call_back("get_instance_id", [&](auto&& args) {
        auto* ret = try_any_cast_ret<std::string>(args);
        ret->first = instance_id_x;
        ret->second = true;
    });
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, 1122, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, 1122, true);
    res_code = remove_delete_bitmap_lock(meta_service.get(), table_id, 1122, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, 1122, false);

    instance_id_x = "instance_id3";
    sp->set_call_back("get_instance_id", [&](auto&& args) {
        auto* ret = try_any_cast_ret<std::string>(args);
        ret->first = instance_id_x;
        ret->second = true;
    });
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, 1123, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, 1123, true);
    res_code = remove_delete_bitmap_lock(meta_service.get(), table_id, 1123, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, 1123, false);

    instance_id_x = "instance_id6";
    sp->set_call_back("get_instance_id", [&](auto&& args) {
        auto* ret = try_any_cast_ret<std::string>(args);
        ret->first = instance_id_x;
        ret->second = true;
    });
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, 1126, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, 1126, true);
    res_code = remove_delete_bitmap_lock(meta_service.get(), table_id, 1126, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, 1126, false);

    instance_id_x = "instance_id7";
    sp->set_call_back("get_instance_id", [&](auto&& args) {
        auto* ret = try_any_cast_ret<std::string>(args);
        ret->first = instance_id_x;
        ret->second = true;
    });
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, 1127, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, 1127, true);
    res_code = remove_delete_bitmap_lock(meta_service.get(), table_id, 1127, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, 1127, false);

    instance_id_x = "instance_id10";
    sp->set_call_back("get_instance_id", [&](auto&& args) {
        auto* ret = try_any_cast_ret<std::string>(args);
        ret->first = instance_id_x;
        ret->second = true;
    });
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, 1130, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, 1130, true);
    res_code = remove_delete_bitmap_lock(meta_service.get(), table_id, 1130, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, 1130, false);

    instance_id_x = "instance_id11";
    sp->set_call_back("get_instance_id", [&](auto&& args) {
        auto* ret = try_any_cast_ret<std::string>(args);
        ret->first = instance_id_x;
        ret->second = true;
    });
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, 1131, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, 1131, true);
    res_code = remove_delete_bitmap_lock(meta_service.get(), table_id, 1131, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, 1131, false);

    instance_id_x = "instance_id12";
    sp->set_call_back("get_instance_id", [&](auto&& args) {
        auto* ret = try_any_cast_ret<std::string>(args);
        ret->first = instance_id_x;
        ret->second = true;
    });
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, 1132, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, 1132, true);
    res_code = remove_delete_bitmap_lock(meta_service.get(), table_id, 1132, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, 1132, false);

    //sc
    instance_id_x = "instance_id1";
    sp->set_call_back("get_instance_id", [&](auto&& args) {
        auto* ret = try_any_cast_ret<std::string>(args);
        ret->first = instance_id_x;
        ret->second = true;
    });
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -2, 1301);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, -2, true);
    res_code = remove_delete_bitmap_lock(meta_service.get(), table_id, -2, 1301);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, -2, true);

    instance_id_x = "instance_id2";
    sp->set_call_back("get_instance_id", [&](auto&& args) {
        auto* ret = try_any_cast_ret<std::string>(args);
        ret->first = instance_id_x;
        ret->second = true;
    });
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -2, 1302);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, -2, true);
    res_code = remove_delete_bitmap_lock(meta_service.get(), table_id, -2, 1302);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, -2, true);

    instance_id_x = "instance_id3";
    sp->set_call_back("get_instance_id", [&](auto&& args) {
        auto* ret = try_any_cast_ret<std::string>(args);
        ret->first = instance_id_x;
        ret->second = true;
    });
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -2, 1303);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, -2, true);
    res_code = remove_delete_bitmap_lock(meta_service.get(), table_id, -2, 1303);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, -2, false);

    instance_id_x = "instance_id6";
    sp->set_call_back("get_instance_id", [&](auto&& args) {
        auto* ret = try_any_cast_ret<std::string>(args);
        ret->first = instance_id_x;
        ret->second = true;
    });
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -2, 1306);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, -2, true);
    res_code = remove_delete_bitmap_lock(meta_service.get(), table_id, -2, 1306);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, -2, false);

    instance_id_x = "instance_id7";
    sp->set_call_back("get_instance_id", [&](auto&& args) {
        auto* ret = try_any_cast_ret<std::string>(args);
        ret->first = instance_id_x;
        ret->second = true;
    });
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -2, 1307);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, -2, true);
    res_code = remove_delete_bitmap_lock(meta_service.get(), table_id, -2, 1307);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, -2, false);

    instance_id_x = "instance_id10";
    sp->set_call_back("get_instance_id", [&](auto&& args) {
        auto* ret = try_any_cast_ret<std::string>(args);
        ret->first = instance_id_x;
        ret->second = true;
    });
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -2, 1310);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, -2, true);
    res_code = remove_delete_bitmap_lock(meta_service.get(), table_id, -2, 1310);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, -2, false);

    instance_id_x = "instance_id11";
    sp->set_call_back("get_instance_id", [&](auto&& args) {
        auto* ret = try_any_cast_ret<std::string>(args);
        ret->first = instance_id_x;
        ret->second = true;
    });
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -2, 1311);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, -2, true);
    res_code = remove_delete_bitmap_lock(meta_service.get(), table_id, -2, 1311);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, -2, true);

    instance_id_x = "instance_id12";
    sp->set_call_back("get_instance_id", [&](auto&& args) {
        auto* ret = try_any_cast_ret<std::string>(args);
        ret->first = instance_id_x;
        ret->second = true;
    });
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -2, 1312);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, -2, true);
    res_code = remove_delete_bitmap_lock(meta_service.get(), table_id, -2, 1312);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, -2, true);

    //2. empty white list
    config::delete_bitmap_lock_v2_white_list = "";
    // compaction
    instance_id_x = "instance_id20";
    sp->set_call_back("get_instance_id", [&](auto&& args) {
        auto* ret = try_any_cast_ret<std::string>(args);
        ret->first = instance_id_x;
        ret->second = true;
    });
    //compaction
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 2000);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, -1, true);
    test_start_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE, "job_id123",
                              instance_id_x);
    res_code = update_delete_bitmap(meta_service.get(), table_id, 3, 5, -1, 2000);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    test_commit_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE, 2000, "job_id123",
                               instance_id_x);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, -1, false);
    // load
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, 2001, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, 2001, true);
    res_code = remove_delete_bitmap_lock(meta_service.get(), table_id, 2001, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, 2001, false);
    // sc
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -2, 2002);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, -2, true);
    res_code = remove_delete_bitmap_lock(meta_service.get(), table_id, -2, 2002);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, -2, false);

    //3. 1000 item white list
    std::stringstream ss;
    ss << "instance_id0;";
    for (int i = 1; i < 1000; i++) {
        ss << "instance_id" << i << ";";
    }
    ss << "instance_id1000";
    config::delete_bitmap_lock_v2_white_list = ss.str();
    instance_id_x = "instance_id0";
    sp->set_call_back("get_instance_id", [&](auto&& args) {
        auto* ret = try_any_cast_ret<std::string>(args);
        ret->first = instance_id_x;
        ret->second = true;
    });
    //compaction
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 3000);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, -1, true);
    check_job_key(meta_service.get(), instance_id_x, table_id, 3000, true);
    test_start_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE, "job_id123",
                              instance_id_x);
    res_code = update_delete_bitmap(meta_service.get(), table_id, 3, 5, -1, 3000);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    test_commit_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE, 3000, "job_id123",
                               instance_id_x);
    check_job_key(meta_service.get(), instance_id_x, table_id, 3000, false);
    // load
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, 3001, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, 3001, true);
    res_code = remove_delete_bitmap_lock(meta_service.get(), table_id, 3001, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, 3001, false);
    // sc
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -2, 3002);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, -2, true);
    res_code = remove_delete_bitmap_lock(meta_service.get(), table_id, -2, 3002);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, -2, true);

    //4.config single item
    config::delete_bitmap_lock_v2_white_list = "instance_id40";
    instance_id_x = "instance_id40";
    sp->set_call_back("get_instance_id", [&](auto&& args) {
        auto* ret = try_any_cast_ret<std::string>(args);
        ret->first = instance_id_x;
        ret->second = true;
    });

    //instance_id40->v2
    //compaction
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 4000);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, -1, true);
    check_job_key(meta_service.get(), instance_id_x, table_id, 4000, true);
    test_start_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE, "job_id123",
                              instance_id_x);
    res_code = update_delete_bitmap(meta_service.get(), table_id, 3, 5, -1, 4000);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    test_commit_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE, 4000, "job_id123",
                               instance_id_x);
    check_job_key(meta_service.get(), instance_id_x, table_id, 4000, false);
    // load
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, 4001, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, 4001, true);
    res_code = remove_delete_bitmap_lock(meta_service.get(), table_id, 4001, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, 4001, false);
    // sc
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -2, 4002);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, -2, true);
    res_code = remove_delete_bitmap_lock(meta_service.get(), table_id, -2, 4002);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, -2, true);

    //instance_id41->v1
    instance_id_x = "instance_id41";
    sp->set_call_back("get_instance_id", [&](auto&& args) {
        auto* ret = try_any_cast_ret<std::string>(args);
        ret->first = instance_id_x;
        ret->second = true;
    });
    //compaction
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 4100);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, -1, true);
    test_start_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE, "job_id123",
                              instance_id_x);
    res_code = update_delete_bitmap(meta_service.get(), table_id, 3, 5, -1, 4100);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    test_commit_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE, 4100, "job_id123",
                               instance_id_x);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, -1, false);
    // load
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, 4101, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, 4101, true);
    res_code = remove_delete_bitmap_lock(meta_service.get(), table_id, 4101, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, 4101, false);
    // sc
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -2, 4102);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, -2, true);
    res_code = remove_delete_bitmap_lock(meta_service.get(), table_id, -2, 4102);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, -2, false);

    //5.config *
    //instance_id50->v2
    config::delete_bitmap_lock_v2_white_list = "*";
    instance_id_x = "instance_id50";
    sp->set_call_back("get_instance_id", [&](auto&& args) {
        auto* ret = try_any_cast_ret<std::string>(args);
        ret->first = instance_id_x;
        ret->second = true;
    });

    //compaction
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 5000);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, -1, true);
    check_job_key(meta_service.get(), instance_id_x, table_id, 5000, true);
    test_start_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE, "job_id123",
                              instance_id_x);
    res_code = update_delete_bitmap(meta_service.get(), table_id, 3, 5, -1, 5000);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    test_commit_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE, 5000, "job_id123",
                               instance_id_x);
    check_job_key(meta_service.get(), instance_id_x, table_id, 5000, false);
    // load
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, 5001, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, 5001, true);
    res_code = remove_delete_bitmap_lock(meta_service.get(), table_id, 5001, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, 5001, false);
    // sc
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -2, 5002);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, -2, true);
    res_code = remove_delete_bitmap_lock(meta_service.get(), table_id, -2, 5002);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, -2, true);

    //instance_id51->v2
    instance_id_x = "instance_id51";
    sp->set_call_back("get_instance_id", [&](auto&& args) {
        auto* ret = try_any_cast_ret<std::string>(args);
        ret->first = instance_id_x;
        ret->second = true;
    });

    //compaction
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 5100);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, -1, true);
    check_job_key(meta_service.get(), instance_id_x, table_id, 5100, true);
    test_start_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE, "job_id123",
                              instance_id_x);
    res_code = update_delete_bitmap(meta_service.get(), table_id, 3, 5, -1, 5100);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    test_commit_compaction_job(table_id, 2, 3, 5, TabletCompactionJobPB::BASE, 5100, "job_id123",
                               instance_id_x);
    check_job_key(meta_service.get(), instance_id_x, table_id, 5100, false);
    // load
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, 5101, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, 5101, true);
    res_code = remove_delete_bitmap_lock(meta_service.get(), table_id, 5101, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, 5101, false);
    // sc
    res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -2, 5102);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, -2, true);
    res_code = remove_delete_bitmap_lock(meta_service.get(), table_id, -2, 5102);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    check_delete_bitmap_lock(meta_service.get(), instance_id_x, table_id, -2, true);
}

void testCompactionJobWithMoWTest(int lock_version) {
    config::delete_bitmap_lock_v2_white_list = lock_version == 1 ? "" : "*";
    auto meta_service = get_meta_service();
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

    brpc::Controller cntl;

    // Start compaction job
    auto test_start_compaction_job = [&](int64_t table_id, int64_t index_id, int64_t partition_id,
                                         int64_t tablet_id,
                                         TabletCompactionJobPB::CompactionType type) {
        StartTabletJobResponse res;
        std::string job_id = "job_id123";

        auto index_key = meta_tablet_idx_key({instance_id, tablet_id});
        TabletIndexPB idx_pb;
        idx_pb.set_table_id(table_id);
        idx_pb.set_index_id(index_id);
        idx_pb.set_partition_id(partition_id);
        idx_pb.set_tablet_id(tablet_id);
        std::string idx_val = idx_pb.SerializeAsString();
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        txn->put(index_key, idx_val);
        std::string stats_key =
                stats_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
        TabletStatsPB stats;
        stats.set_base_compaction_cnt(9);
        stats.set_cumulative_compaction_cnt(19);
        txn->put(stats_key, stats.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
        start_compaction_job(meta_service.get(), tablet_id, job_id, "ip:port", 9, 19, type, res);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    };

    FinishTabletJobResponse res;
    auto test_commit_compaction_job = [&](int64_t table_id, int64_t index_id, int64_t partition_id,
                                          int64_t tablet_id,
                                          TabletCompactionJobPB::CompactionType type) {
        FinishTabletJobRequest req;
        std::string job_id = "job_id123";

        auto compaction = req.mutable_job()->add_compaction();
        compaction->set_id(job_id);
        compaction->set_initiator("ip:port");
        compaction->set_base_compaction_cnt(10);
        compaction->set_cumulative_compaction_cnt(20);
        compaction->set_delete_bitmap_lock_initiator(12345);
        req.mutable_job()->mutable_idx()->set_table_id(table_id);
        req.mutable_job()->mutable_idx()->set_index_id(index_id);
        req.mutable_job()->mutable_idx()->set_partition_id(partition_id);
        req.mutable_job()->mutable_idx()->set_tablet_id(tablet_id);
        req.set_action(FinishTabletJobRequest::COMMIT);

        auto tablet_meta_key =
                meta_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        doris::TabletMetaCloudPB tablet_meta_pb;
        tablet_meta_pb.set_table_id(table_id);
        tablet_meta_pb.set_index_id(index_id);
        tablet_meta_pb.set_partition_id(partition_id);
        tablet_meta_pb.set_tablet_id(tablet_id);
        tablet_meta_pb.set_cumulative_layer_point(50);
        std::string tablet_meta_val = tablet_meta_pb.SerializeAsString();
        ASSERT_FALSE(tablet_meta_val.empty());
        txn->put(tablet_meta_key, tablet_meta_val);
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        // Create create tablet stats, compation job will will update stats
        auto tablet_stats_key =
                stats_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
        TabletStatsPB tablet_stats_pb;
        tablet_stats_pb.mutable_idx()->set_table_id(table_id);
        tablet_stats_pb.mutable_idx()->set_index_id(index_id);
        tablet_stats_pb.mutable_idx()->set_partition_id(partition_id);
        tablet_stats_pb.mutable_idx()->set_tablet_id(tablet_id);

        std::mt19937 rng(std::chrono::system_clock::now().time_since_epoch().count());
        std::uniform_int_distribution<int> dist(1, 10000); // Positive numbers

        compaction->set_output_cumulative_point(tablet_stats_pb.cumulative_point() + dist(rng));
        compaction->set_num_output_rows(dist(rng));
        compaction->set_num_output_rowsets(dist(rng));
        compaction->set_num_output_segments(dist(rng));
        compaction->set_num_input_rows(dist(rng));
        compaction->set_num_input_rowsets(dist(rng));
        compaction->set_num_input_segments(dist(rng));
        compaction->set_size_input_rowsets(dist(rng));
        compaction->set_size_output_rowsets(dist(rng));
        compaction->set_type(type);

        tablet_stats_pb.set_cumulative_compaction_cnt(dist(rng));
        tablet_stats_pb.set_base_compaction_cnt(dist(rng));
        tablet_stats_pb.set_cumulative_point(tablet_meta_pb.cumulative_layer_point());
        // MUST let data stats be larger than input data size
        tablet_stats_pb.set_num_rows(dist(rng) + compaction->num_input_rows());
        tablet_stats_pb.set_data_size(dist(rng) + compaction->size_input_rowsets());
        tablet_stats_pb.set_num_rowsets(dist(rng) + compaction->num_input_rowsets());
        tablet_stats_pb.set_num_segments(dist(rng) + compaction->num_input_segments());
        tablet_stats_pb.set_index_size(dist(rng) + compaction->index_size_input_rowsets());
        tablet_stats_pb.set_segment_size(dist(rng) + compaction->segment_size_input_rowsets());

        std::string tablet_stats_val = tablet_stats_pb.SerializeAsString();
        ASSERT_FALSE(tablet_stats_val.empty());
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        txn->put(tablet_stats_key, tablet_stats_val);
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        // Provide input and output rowset info
        int64_t input_version_start = dist(rng);
        int64_t input_version_end = input_version_start + 100;
        compaction->add_input_versions(input_version_start);
        compaction->add_input_versions(input_version_end);
        compaction->add_output_versions(input_version_end);
        compaction->add_output_rowset_ids("output rowset id");

        // Input rowsets must exist, and more than 0
        // Check number input rowsets
        sp->set_call_back("process_compaction_job::loop_input_done", [](auto&& args) {
            auto* num_input_rowsets = try_any_cast<int*>(args[0]);
            ASSERT_EQ(*num_input_rowsets, 0); // zero existed rowsets
        });
        sp->set_call_back("process_compaction_job::too_few_rowsets", [](auto&& args) {
            auto* need_commit = try_any_cast<bool*>(args[0]);
            ASSERT_EQ(*need_commit, true);
            *need_commit = false; // Donot remove tablet job in order to continue test
        });

        // Provide input rowset KVs, boundary test, 5 input rowsets
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        // clang-format off
        std::vector<std::string> input_rowset_keys = {
                meta_rowset_key({instance_id, tablet_id, input_version_start - 1}),
                meta_rowset_key({instance_id, tablet_id, input_version_start}),
                meta_rowset_key({instance_id, tablet_id, input_version_start + 1}),
                meta_rowset_key({instance_id, tablet_id, (input_version_start + input_version_end) / 2}),
                meta_rowset_key({instance_id, tablet_id, input_version_end - 1}),
                meta_rowset_key({instance_id, tablet_id, input_version_end}),
                meta_rowset_key({instance_id, tablet_id, input_version_end + 1}),
        };
        // clang-format on
        std::vector<std::unique_ptr<std::string>> input_rowset_vals;
        for (auto& i : input_rowset_keys) {
            doris::RowsetMetaCloudPB rs_pb;
            rs_pb.set_rowset_id(0);
            rs_pb.set_rowset_id_v2(hex(i));
            input_rowset_vals.emplace_back(new std::string(rs_pb.SerializeAsString()));
            txn->put(i, *input_rowset_vals.back());
        }
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        // Check number input rowsets
        sp->set_call_back("process_compaction_job::loop_input_done", [](auto&& args) {
            auto* num_input_rowsets = try_any_cast<int*>(args[0]);
            ASSERT_EQ(*num_input_rowsets, 5);
        });
        int64_t txn_id = dist(rng);
        compaction->add_txn_id(txn_id);

        // Provide output rowset meta
        auto tmp_rowset_key = meta_rowset_tmp_key({instance_id, txn_id, tablet_id});
        doris::RowsetMetaCloudPB tmp_rs_pb;
        tmp_rs_pb.set_rowset_id(0);
        tmp_rs_pb.set_txn_id(10086);
        auto tmp_rowset_val = tmp_rs_pb.SerializeAsString();
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        txn->put(tmp_rowset_key, tmp_rowset_val);
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        meta_service->finish_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);
    };

    auto test_abort_compaction_job = [&](int64_t table_id, int64_t index_id, int64_t partition_id,
                                         int64_t tablet_id) {
        FinishTabletJobRequest req;
        FinishTabletJobResponse res;
        std::string job_id = "job_id123";

        auto compaction = req.mutable_job()->add_compaction();
        compaction->set_id(job_id);
        compaction->set_initiator("ip:port");
        compaction->set_delete_bitmap_lock_initiator(12345);
        req.mutable_job()->mutable_idx()->set_tablet_id(tablet_id);
        req.set_action(FinishTabletJobRequest::ABORT);
        meta_service->finish_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);

        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        auto job_key = job_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
        std::string job_val;
        ASSERT_EQ(txn->get(job_key, &job_val), TxnErrorCode::TXN_OK);
        TabletJobInfoPB job_pb;
        ASSERT_TRUE(job_pb.ParseFromString(job_val));
        ASSERT_TRUE(job_pb.compaction().empty());
    };

    auto clear_rowsets = [&](int64_t tablet_id) {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string key1 = meta_rowset_key({instance_id, tablet_id, 1});
        std::string key2 = meta_rowset_key({instance_id, tablet_id, 10001});
        txn->remove(key1, key2);
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    };

    remove_delete_bitmap_lock(meta_service.get(), 1);
    remove_delete_bitmap_lock(meta_service.get(), 2);
    test_start_compaction_job(1, 2, 3, 4, TabletCompactionJobPB::CUMULATIVE);
    test_commit_compaction_job(1, 2, 3, 4, TabletCompactionJobPB::CUMULATIVE);
    ASSERT_EQ(res.status().code(), MetaServiceCode::KV_TXN_GET_ERR);
    clear_rowsets(4);

    auto res_code = get_delete_bitmap_lock(meta_service.get(), 1, 1, 1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    test_commit_compaction_job(1, 2, 3, 4, TabletCompactionJobPB::CUMULATIVE);
    ASSERT_EQ(res.status().code(), MetaServiceCode::LOCK_EXPIRED);
    res_code = remove_delete_bitmap_lock(meta_service.get(), 1, 1, 1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    clear_rowsets(4);

    res_code = get_delete_bitmap_lock(meta_service.get(), 1, -1, 1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    test_commit_compaction_job(1, 2, 3, 4, TabletCompactionJobPB::CUMULATIVE);
    ASSERT_EQ(res.status().code(), MetaServiceCode::LOCK_EXPIRED);
    res_code = remove_delete_bitmap_lock(meta_service.get(), 1, -1, 1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    clear_rowsets(4);

    res_code = get_delete_bitmap_lock(meta_service.get(), 1, -1, 12345);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    test_commit_compaction_job(1, 2, 3, 4, TabletCompactionJobPB::CUMULATIVE);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    clear_rowsets(4);

    test_start_compaction_job(2, 2, 3, 5, TabletCompactionJobPB::BASE);
    res_code = get_delete_bitmap_lock(meta_service.get(), 2, -1, 12345);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    res_code = get_delete_bitmap_lock(meta_service.get(), 2, -1, 2345);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    test_commit_compaction_job(2, 2, 3, 5, TabletCompactionJobPB::BASE);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    res_code = remove_delete_bitmap_lock(meta_service.get(), 2, -1, 2345);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    clear_rowsets(5);

    test_start_compaction_job(2, 2, 3, 6, TabletCompactionJobPB::BASE);
    res_code = get_delete_bitmap_lock(meta_service.get(), 2, -1, 12345);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    res_code = get_delete_bitmap_lock(meta_service.get(), 2, 123, -1);
    ASSERT_EQ(res_code, MetaServiceCode::LOCK_CONFLICT);
    test_abort_compaction_job(2, 2, 3, 6);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    res_code = get_delete_bitmap_lock(meta_service.get(), 2, 123, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    res_code = remove_delete_bitmap_lock(meta_service.get(), 2, 123, -1);
    ASSERT_EQ(res_code, MetaServiceCode::OK);
    clear_rowsets(6);
}

TEST(MetaServiceJobTest, CompactionJobWithMoWTest) {
    testCompactionJobWithMoWTest(2);
    testCompactionJobWithMoWTest(1);
}

TEST(MetaServiceJobTest, SchemaChangeJobTest) {
    auto meta_service = get_meta_service();
    // meta_service->resource_mgr().reset(); // Do not use resource manager

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

    brpc::Controller cntl;

    int64_t table_id = 1;
    int64_t index_id = 2;
    int64_t partition_id = 3;
    int64_t tablet_id = 4;
    ASSERT_NO_FATAL_FAILURE(
            create_tablet(meta_service.get(), table_id, index_id, partition_id, tablet_id, false));

    // commit schema_change job with alter_version == 1
    {
        int64_t new_tablet_id = 14;
        ASSERT_NO_FATAL_FAILURE(create_tablet(meta_service.get(), table_id, index_id, partition_id,
                                              new_tablet_id, false, true));
        StartTabletJobResponse sc_res;
        ASSERT_NO_FATAL_FAILURE(start_schema_change_job(meta_service.get(), table_id, index_id,
                                                        partition_id, tablet_id, new_tablet_id,
                                                        "job1", "be1", sc_res));
        FinishTabletJobResponse res;
        finish_schema_change_job(meta_service.get(), tablet_id, new_tablet_id, "job2", "be1", {},
                                 res);
        ASSERT_NE(res.status().code(), MetaServiceCode::OK);
        ASSERT_NE(res.status().msg().find("unmatched job id or initiator"), std::string::npos);
        res.Clear();
        finish_schema_change_job(meta_service.get(), tablet_id, new_tablet_id, "job1", "be2", {},
                                 res);
        ASSERT_NE(res.status().code(), MetaServiceCode::OK);
        ASSERT_NE(res.status().msg().find("unmatched job id or initiator"), std::string::npos);
        res.Clear();
        finish_schema_change_job(meta_service.get(), tablet_id, new_tablet_id, "job1", "be1", {},
                                 res);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        res.Clear();
        finish_schema_change_job(meta_service.get(), tablet_id, new_tablet_id, "job1", "be1", {},
                                 res);
        ASSERT_EQ(res.status().code(), MetaServiceCode::JOB_ALREADY_SUCCESS);
    }

    // commit schema_change job with txn_ids
    {
        int64_t new_tablet_id = 24;
        ASSERT_NO_FATAL_FAILURE(create_tablet(meta_service.get(), table_id, index_id, partition_id,
                                              new_tablet_id, false, true));
        StartTabletJobResponse sc_res;
        ASSERT_NO_FATAL_FAILURE(start_schema_change_job(meta_service.get(), table_id, index_id,
                                                        partition_id, tablet_id, new_tablet_id,
                                                        "job2", "be1", sc_res));

        std::vector<doris::RowsetMetaCloudPB> output_rowsets;
        for (int64_t i = 0; i < 5; ++i) {
            output_rowsets.push_back(create_rowset(new_tablet_id, i + 2, i + 2));
            CreateRowsetResponse res;
            commit_rowset(meta_service.get(), output_rowsets.back(), res);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << i;
        }

        FinishTabletJobResponse res;
        finish_schema_change_job(meta_service.get(), tablet_id, new_tablet_id, "job2", "be1",
                                 output_rowsets, res);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_TRUE(res.has_stats());
        EXPECT_EQ(res.stats().num_rows(), 500);
        EXPECT_EQ(res.stats().num_rowsets(), 6);
        EXPECT_EQ(res.stats().num_segments(), 5);
        EXPECT_EQ(res.stats().data_size(), 50000);
        EXPECT_EQ(res.stats().index_size(), 25000);
        EXPECT_EQ(res.stats().segment_size(), 25000);
        TabletStatsPB tablet_stats;
        get_tablet_stats(meta_service.get(), new_tablet_id, tablet_stats);
        EXPECT_EQ(tablet_stats.num_rows(), 500);
        EXPECT_EQ(tablet_stats.num_rowsets(), 6);
        EXPECT_EQ(tablet_stats.num_segments(), 5);
        EXPECT_EQ(tablet_stats.data_size(), 50000);
        EXPECT_EQ(tablet_stats.index_size(), 25000);
        EXPECT_EQ(tablet_stats.segment_size(), 25000);

        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        // check tablet state
        auto tablet_key =
                meta_tablet_key({instance_id, table_id, index_id, partition_id, new_tablet_id});
        std::string tablet_val;
        ASSERT_EQ(txn->get(tablet_key, &tablet_val), TxnErrorCode::TXN_OK);
        doris::TabletMetaCloudPB tablet_pb;
        ASSERT_TRUE(tablet_pb.ParseFromString(tablet_val));
        ASSERT_EQ(tablet_pb.tablet_state(), doris::TabletStatePB::PB_RUNNING);
        // check visible rowsets
        std::unique_ptr<RangeGetIterator> it;
        auto rs_start = meta_rowset_key({instance_id, new_tablet_id, 0});
        auto rs_end = meta_rowset_key({instance_id, new_tablet_id, 100});
        ASSERT_EQ(txn->get(rs_start, rs_end, &it), TxnErrorCode::TXN_OK);
        ASSERT_EQ(it->size(), 6);
        auto [k, v] = it->next();
        doris::RowsetMetaCloudPB saved_rowset;
        ASSERT_TRUE(saved_rowset.ParseFromArray(v.data(), v.size()));
        EXPECT_EQ(saved_rowset.start_version(), 0);
        EXPECT_EQ(saved_rowset.end_version(), 1);
        for (auto& rs : output_rowsets) {
            auto [k, v] = it->next();
            ASSERT_TRUE(saved_rowset.ParseFromArray(v.data(), v.size()));
            EXPECT_EQ(saved_rowset.rowset_id_v2(), rs.rowset_id_v2());
            EXPECT_EQ(saved_rowset.start_version(), rs.start_version());
            EXPECT_EQ(saved_rowset.end_version(), rs.end_version());
        }
    }

    // commit schema_change job with rowsets which overlapped with visible rowsets
    {
        int64_t new_tablet_id = 34;
        ASSERT_NO_FATAL_FAILURE(create_tablet(meta_service.get(), table_id, index_id, partition_id,
                                              new_tablet_id, false, true));
        StartTabletJobResponse sc_res;
        ASSERT_NO_FATAL_FAILURE(start_schema_change_job(meta_service.get(), table_id, index_id,
                                                        partition_id, tablet_id, new_tablet_id,
                                                        "job3", "be1", sc_res));
        // provide existed rowsets
        std::vector<doris::RowsetMetaCloudPB> existed_rowsets;
        for (int i = 0; i < 5; ++i) {
            existed_rowsets.push_back(create_rowset(new_tablet_id, i + 11, i + 11));
        }
        ASSERT_NO_FATAL_FAILURE(insert_rowsets(meta_service->txn_kv().get(), table_id, index_id,
                                               partition_id, new_tablet_id, existed_rowsets));

        std::vector<doris::RowsetMetaCloudPB> output_rowsets;
        output_rowsets.push_back(create_rowset(new_tablet_id, 2, 8));
        output_rowsets.push_back(create_rowset(new_tablet_id, 9, 12));
        output_rowsets.push_back(create_rowset(new_tablet_id, 13, 13));
        for (auto& rs : output_rowsets) {
            CreateRowsetResponse res;
            commit_rowset(meta_service.get(), rs, res);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << rs.end_version();
        }

        FinishTabletJobResponse res;
        finish_schema_change_job(meta_service.get(), tablet_id, new_tablet_id, "job3", "be1",
                                 output_rowsets, res);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_TRUE(res.has_stats());
        // overwrite [11-13], retain [14-15]
        EXPECT_EQ(res.stats().num_rows(), 500);
        EXPECT_EQ(res.stats().num_rowsets(), 6);
        EXPECT_EQ(res.stats().num_segments(), 5);
        EXPECT_EQ(res.stats().data_size(), 50000);
        EXPECT_EQ(res.stats().index_size(), 25000);
        EXPECT_EQ(res.stats().segment_size(), 25000);
        TabletStatsPB tablet_stats;
        get_tablet_stats(meta_service.get(), new_tablet_id, tablet_stats);
        EXPECT_EQ(tablet_stats.num_rows(), 500);
        EXPECT_EQ(tablet_stats.num_rowsets(), 6);
        EXPECT_EQ(tablet_stats.num_segments(), 5);
        EXPECT_EQ(tablet_stats.data_size(), 50000);
        EXPECT_EQ(tablet_stats.index_size(), 25000);
        EXPECT_EQ(tablet_stats.segment_size(), 25000);

        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        // check tablet state
        auto tablet_key =
                meta_tablet_key({instance_id, table_id, index_id, partition_id, new_tablet_id});
        std::string tablet_val;
        ASSERT_EQ(txn->get(tablet_key, &tablet_val), TxnErrorCode::TXN_OK);
        doris::TabletMetaCloudPB tablet_pb;
        ASSERT_TRUE(tablet_pb.ParseFromString(tablet_val));
        ASSERT_EQ(tablet_pb.tablet_state(), doris::TabletStatePB::PB_RUNNING);

        // check visible rowsets
        std::unique_ptr<RangeGetIterator> it;
        auto rs_start = meta_rowset_key({instance_id, new_tablet_id, 0});
        auto rs_end = meta_rowset_key({instance_id, new_tablet_id, 100});
        ASSERT_EQ(txn->get(rs_start, rs_end, &it), TxnErrorCode::TXN_OK);
        // [0-1][2-8][9-12][13-13][14-14][15-15]
        EXPECT_EQ(it->size(), 6);
        auto [k, v] = it->next();
        doris::RowsetMetaCloudPB saved_rowset;
        ASSERT_TRUE(saved_rowset.ParseFromArray(v.data(), v.size()));
        EXPECT_EQ(saved_rowset.start_version(), 0);
        EXPECT_EQ(saved_rowset.end_version(), 1);
        for (auto& rs : output_rowsets) {
            auto [k, v] = it->next();
            ASSERT_TRUE(saved_rowset.ParseFromArray(v.data(), v.size()));
            EXPECT_EQ(saved_rowset.start_version(), rs.start_version());
            EXPECT_EQ(saved_rowset.end_version(), rs.end_version());
            EXPECT_EQ(saved_rowset.rowset_id_v2(), rs.rowset_id_v2());
        }
        for (int i = 3; i < 5; ++i) { // [14-14][15-15]
            auto [k, v] = it->next();
            ASSERT_TRUE(saved_rowset.ParseFromArray(v.data(), v.size()));
            EXPECT_EQ(saved_rowset.start_version(), existed_rowsets[i].start_version());
            EXPECT_EQ(saved_rowset.end_version(), existed_rowsets[i].end_version());
            EXPECT_EQ(saved_rowset.rowset_id_v2(), existed_rowsets[i].rowset_id_v2());
        }

        // check recycled rowsets
        auto recycl_rs_start = recycle_rowset_key({instance_id, new_tablet_id, ""});
        auto recycl_rs_end = recycle_rowset_key({instance_id, new_tablet_id, "\xff"});
        ASSERT_EQ(txn->get(recycl_rs_start, recycl_rs_end, &it), TxnErrorCode::TXN_OK);
        // [11-11], [12-12], old[13-13]
        ASSERT_EQ(it->size(), 3);
        for (int i = 0; i < 3; ++i) {
            auto [k, v] = it->next();
            k.remove_prefix(1);
            std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
            decode_key(&k, &out);
            // 0x01 "recycle" ${instance_id} "rowset" ${tablet_id} ${rowset_id} -> RecycleRowsetPB
            const auto& rowset_id = std::get<std::string>(std::get<0>(out[4]));
            EXPECT_EQ(rowset_id, existed_rowsets[i].rowset_id_v2());
        }
    }
}

TEST(MetaServiceJobTest, RetrySchemaChangeJobTest) {
    auto meta_service = get_meta_service();
    // meta_service->resource_mgr().reset(); // Do not use resource manager

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

    brpc::Controller cntl;

    int64_t table_id = 1;
    int64_t index_id = 2;
    int64_t partition_id = 3;
    int64_t tablet_id = 4;
    ASSERT_NO_FATAL_FAILURE(
            create_tablet(meta_service.get(), table_id, index_id, partition_id, tablet_id, false));
    int64_t new_tablet_id = 14;
    // start "job1" on BE1
    ASSERT_NO_FATAL_FAILURE(create_tablet(meta_service.get(), table_id, index_id, partition_id,
                                          new_tablet_id, false, true));
    StartTabletJobResponse sc_res;
    ASSERT_NO_FATAL_FAILURE(start_schema_change_job(meta_service.get(), table_id, index_id,
                                                    partition_id, tablet_id, new_tablet_id, "job1",
                                                    "be1", sc_res));
    // provide existed rowsets
    std::vector<doris::RowsetMetaCloudPB> existed_rowsets;
    for (int i = 0; i < 5; ++i) {
        existed_rowsets.push_back(create_rowset(new_tablet_id, i + 11, i + 11));
    }
    ASSERT_NO_FATAL_FAILURE(insert_rowsets(meta_service->txn_kv().get(), table_id, index_id,
                                           partition_id, new_tablet_id, existed_rowsets));
    sc_res.Clear();
    // FE canceled "job1" and starts "job2" on BE1, should preempt previous "job1"
    ASSERT_NO_FATAL_FAILURE(start_schema_change_job(meta_service.get(), table_id, index_id,
                                                    partition_id, tablet_id, new_tablet_id, "job2",
                                                    "be1", sc_res));
    sc_res.Clear();
    // retry "job2" on BE1
    ASSERT_NO_FATAL_FAILURE(start_schema_change_job(meta_service.get(), table_id, index_id,
                                                    partition_id, tablet_id, new_tablet_id, "job2",
                                                    "be1", sc_res));
    // BE1 output_versions=[2-8][9-9][10-10][11-11]
    std::vector<doris::RowsetMetaCloudPB> be1_output_rowsets;
    be1_output_rowsets.push_back(create_rowset(new_tablet_id, 2, 8));
    be1_output_rowsets.push_back(create_rowset(new_tablet_id, 9, 9));
    be1_output_rowsets.push_back(create_rowset(new_tablet_id, 10, 10));
    be1_output_rowsets.push_back(create_rowset(new_tablet_id, 11, 11));
    for (auto& rs : be1_output_rowsets) {
        CreateRowsetResponse res;
        commit_rowset(meta_service.get(), rs, res);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << rs.end_version();
    }
    sc_res.Clear();
    // FE thinks BE1 is not alive and retries "job2" on BE2, should preempt "job2" created by BE1
    ASSERT_NO_FATAL_FAILURE(start_schema_change_job(meta_service.get(), table_id, index_id,
                                                    partition_id, tablet_id, new_tablet_id, "job2",
                                                    "be2", sc_res));
    // BE2 output_versions=[2-8][9-12][13-13]
    std::vector<doris::RowsetMetaCloudPB> be2_output_rowsets;
    {
        CreateRowsetResponse res;
        // [2-8] has committed by BE1
        commit_rowset(meta_service.get(), create_rowset(new_tablet_id, 2, 8), res);
        ASSERT_EQ(res.status().code(), MetaServiceCode::ALREADY_EXISTED);
        ASSERT_TRUE(res.has_existed_rowset_meta());
        ASSERT_EQ(res.existed_rowset_meta().rowset_id_v2(), be1_output_rowsets[0].rowset_id_v2());
        be2_output_rowsets.push_back(res.existed_rowset_meta());
        res.Clear();
        be2_output_rowsets.push_back(create_rowset(new_tablet_id, 9, 12));
        commit_rowset(meta_service.get(), be2_output_rowsets.back(), res);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        res.Clear();
        be2_output_rowsets.push_back(create_rowset(new_tablet_id, 13, 13));
        commit_rowset(meta_service.get(), be2_output_rowsets.back(), res);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // BE1 commit job, but check initiator failed
    FinishTabletJobResponse res;
    finish_schema_change_job(meta_service.get(), tablet_id, new_tablet_id, "job2", "be1",
                             be1_output_rowsets, res);
    ASSERT_NE(res.status().code(), MetaServiceCode::OK);
    ASSERT_NE(res.status().msg().find("unmatched job id or initiator"), std::string::npos);
    // BE2 commit job
    res.Clear();
    finish_schema_change_job(meta_service.get(), tablet_id, new_tablet_id, "job2", "be2",
                             be2_output_rowsets, res);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    ASSERT_TRUE(res.has_stats());
    // [0-1][2-8][9-12][13-13][14-14][15-15]
    EXPECT_EQ(res.stats().num_rows(), 500);
    EXPECT_EQ(res.stats().num_rowsets(), 6);
    EXPECT_EQ(res.stats().num_segments(), 5);
    EXPECT_EQ(res.stats().data_size(), 50000);
    EXPECT_EQ(res.stats().index_size(), 25000);
    EXPECT_EQ(res.stats().segment_size(), 25000);
    TabletStatsPB tablet_stats;
    get_tablet_stats(meta_service.get(), new_tablet_id, tablet_stats);
    EXPECT_EQ(tablet_stats.num_rows(), 500);
    EXPECT_EQ(tablet_stats.num_rowsets(), 6);
    EXPECT_EQ(tablet_stats.num_segments(), 5);
    EXPECT_EQ(tablet_stats.data_size(), 50000);
    EXPECT_EQ(tablet_stats.index_size(), 25000);
    EXPECT_EQ(tablet_stats.segment_size(), 25000);

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    // check tablet state
    auto tablet_key =
            meta_tablet_key({instance_id, table_id, index_id, partition_id, new_tablet_id});
    std::string tablet_val;
    ASSERT_EQ(txn->get(tablet_key, &tablet_val), TxnErrorCode::TXN_OK);
    doris::TabletMetaCloudPB tablet_pb;
    ASSERT_TRUE(tablet_pb.ParseFromString(tablet_val));
    ASSERT_EQ(tablet_pb.tablet_state(), doris::TabletStatePB::PB_RUNNING);

    // check visible rowsets
    std::unique_ptr<RangeGetIterator> it;
    auto rs_start = meta_rowset_key({instance_id, new_tablet_id, 0});
    auto rs_end = meta_rowset_key({instance_id, new_tablet_id, 100});
    ASSERT_EQ(txn->get(rs_start, rs_end, &it), TxnErrorCode::TXN_OK);
    EXPECT_EQ(it->size(), 6);
    auto [k, v] = it->next();
    doris::RowsetMetaCloudPB saved_rowset;
    ASSERT_TRUE(saved_rowset.ParseFromArray(v.data(), v.size()));
    EXPECT_EQ(saved_rowset.start_version(), 0);
    EXPECT_EQ(saved_rowset.end_version(), 1);
    for (auto& rs : be2_output_rowsets) {
        auto [k, v] = it->next();
        ASSERT_TRUE(saved_rowset.ParseFromArray(v.data(), v.size()));
        EXPECT_EQ(saved_rowset.start_version(), rs.start_version());
        EXPECT_EQ(saved_rowset.end_version(), rs.end_version());
        EXPECT_EQ(saved_rowset.rowset_id_v2(), rs.rowset_id_v2());
    }
    for (int i = 3; i < 5; ++i) { // [14-14][15-15]
        auto [k, v] = it->next();
        ASSERT_TRUE(saved_rowset.ParseFromArray(v.data(), v.size()));
        EXPECT_EQ(saved_rowset.start_version(), existed_rowsets[i].start_version());
        EXPECT_EQ(saved_rowset.end_version(), existed_rowsets[i].end_version());
        EXPECT_EQ(saved_rowset.rowset_id_v2(), existed_rowsets[i].rowset_id_v2());
    }

    // check recycled rowsets
    auto recycl_rs_start = recycle_rowset_key({instance_id, new_tablet_id, ""});
    auto recycl_rs_end = recycle_rowset_key({instance_id, new_tablet_id, "\xff"});
    ASSERT_EQ(txn->get(recycl_rs_start, recycl_rs_end, &it), TxnErrorCode::TXN_OK);
    // [11-11], [12-12], old[13-13]
    ASSERT_EQ(it->size(), 3);
    for (int i = 0; i < 3; ++i) {
        auto [k, v] = it->next();
        k.remove_prefix(1);
        std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
        decode_key(&k, &out);
        // 0x01 "recycle" ${instance_id} "rowset" ${tablet_id} ${rowset_id} -> RecycleRowsetPB
        const auto& rowset_id = std::get<std::string>(std::get<0>(out[4]));
        EXPECT_EQ(rowset_id, existed_rowsets[i].rowset_id_v2());
    }
}

void testSchemaChangeJobWithMoWTest(int lock_version) {
    config::delete_bitmap_lock_v2_white_list = lock_version == 1 ? "" : "*";
    auto meta_service = get_meta_service();
    // meta_service->resource_mgr().reset(); // Do not use resource manager

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

    brpc::Controller cntl;

    int64_t table_id = 1;
    int64_t index_id = 2;
    int64_t partition_id = 3;
    int64_t tablet_id = 4;
    ASSERT_NO_FATAL_FAILURE(
            create_tablet(meta_service.get(), table_id, index_id, partition_id, tablet_id, true));
    remove_delete_bitmap_lock(meta_service.get(), table_id);
    {
        int64_t new_tablet_id = 14;
        ASSERT_NO_FATAL_FAILURE(create_tablet(meta_service.get(), table_id, index_id, partition_id,
                                              new_tablet_id, true, true));
        StartTabletJobResponse sc_res;
        ASSERT_NO_FATAL_FAILURE(start_schema_change_job(meta_service.get(), table_id, index_id,
                                                        partition_id, tablet_id, new_tablet_id,
                                                        "job1", "be1", sc_res));
        std::vector<doris::RowsetMetaCloudPB> output_rowsets;
        for (int64_t i = 0; i < 5; ++i) {
            output_rowsets.push_back(create_rowset(new_tablet_id, i + 2, i + 2));
            CreateRowsetResponse res;
            commit_rowset(meta_service.get(), output_rowsets.back(), res);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << i;
        }
        FinishTabletJobResponse res;
        finish_schema_change_job(meta_service.get(), tablet_id, new_tablet_id, "job1", "be1",
                                 output_rowsets, res);
        ASSERT_EQ(res.status().code(), MetaServiceCode::KV_TXN_GET_ERR);
        res.Clear();

        auto res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -1, 2345);
        ASSERT_EQ(res_code, MetaServiceCode::OK);
        finish_schema_change_job(meta_service.get(), tablet_id, new_tablet_id, "job1", "be1",
                                 output_rowsets, res);
        ASSERT_EQ(res.status().code(), MetaServiceCode::LOCK_EXPIRED);
        ASSERT_NE(res.status().msg().find("lock id not match"), std::string::npos);
        res_code = remove_delete_bitmap_lock(meta_service.get(), table_id, -1, 2345);
        ASSERT_EQ(res_code, MetaServiceCode::OK);
        res.Clear();

        res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -2, 2345);
        ASSERT_EQ(res_code, MetaServiceCode::OK);
        finish_schema_change_job(meta_service.get(), tablet_id, new_tablet_id, "job1", "be1",
                                 output_rowsets, res);
        ASSERT_EQ(res.status().code(), MetaServiceCode::LOCK_EXPIRED);
        ASSERT_NE(res.status().msg().find("lock initiator 12345 not exist"), std::string::npos);
        res_code = remove_delete_bitmap_lock(meta_service.get(), table_id, -2, 2345);
        ASSERT_EQ(res_code, MetaServiceCode::OK);
        res.Clear();

        res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -2, 12345);
        ASSERT_EQ(res_code, MetaServiceCode::OK);

        std::string pending_key = meta_pending_delete_bitmap_key({instance_id, new_tablet_id});
        std::string pending_val;
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        ASSERT_EQ(txn->get(pending_key, &pending_val), TxnErrorCode::TXN_KEY_NOT_FOUND);

        res_code = update_delete_bitmap(meta_service.get(), table_id, partition_id, new_tablet_id,
                                        -2, 12345);
        ASSERT_EQ(res_code, MetaServiceCode::OK);

        // schema change job should write pending delete bitmap key
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        ASSERT_EQ(txn->get(pending_key, &pending_val), TxnErrorCode::TXN_OK);
        PendingDeleteBitmapPB pending_info;
        ASSERT_TRUE(pending_info.ParseFromString(pending_val));
        ASSERT_EQ(pending_info.delete_bitmap_keys_size(), 3);
        for (int i = 0; i < 3; ++i) {
            std::string_view k1 = pending_info.delete_bitmap_keys(i);
            k1.remove_prefix(1);
            std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
            decode_key(&k1, &out);
            // 0x01 "meta" ${instance_id} "delete_bitmap" ${tablet_id} ${rowset_id} ${version} ${segment_id} -> roaringbitmap
            ASSERT_EQ(std::get<std::int64_t>(std::get<0>(out[3])), new_tablet_id);
            ASSERT_EQ(std::get<std::string>(std::get<0>(out[4])),
                      "0200000003ea308a3647dbea83220ed4b8897f2288244a91");
            ASSERT_EQ(std::get<std::int64_t>(std::get<0>(out[5])), i);
            ASSERT_EQ(std::get<std::int64_t>(std::get<0>(out[6])), 0);
        }

        finish_schema_change_job(meta_service.get(), tablet_id, new_tablet_id, "job1", "be1",
                                 output_rowsets, res);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        res_code = remove_delete_bitmap_lock(meta_service.get(), table_id, -2, 12345);
        ASSERT_EQ(res_code,
                  lock_version == 1 ? MetaServiceCode::LOCK_EXPIRED : MetaServiceCode::OK);
        res.Clear();

        // pending delete bitmap key on new tablet should be removed after schema change job finishes
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        ASSERT_EQ(txn->get(pending_key, &pending_val), TxnErrorCode::TXN_KEY_NOT_FOUND);
    }

    {
        int64_t new_tablet_id = 15;
        ASSERT_NO_FATAL_FAILURE(create_tablet(meta_service.get(), table_id, index_id, partition_id,
                                              new_tablet_id, true, true));
        StartTabletJobResponse sc_res;
        ASSERT_NO_FATAL_FAILURE(start_schema_change_job(meta_service.get(), table_id, index_id,
                                                        partition_id, tablet_id, new_tablet_id,
                                                        "job2", "be1", sc_res));
        std::vector<doris::RowsetMetaCloudPB> output_rowsets;
        for (int64_t i = 0; i < 5; ++i) {
            output_rowsets.push_back(create_rowset(new_tablet_id, i + 2, i + 2));
            CreateRowsetResponse res;
            commit_rowset(meta_service.get(), output_rowsets.back(), res);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << i;
        }
        auto res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -2, 12345);
        ASSERT_EQ(res_code, MetaServiceCode::OK);
        res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -2, 12346);
        ASSERT_EQ(res_code, MetaServiceCode::OK);
        FinishTabletJobResponse res;
        finish_schema_change_job(meta_service.get(), tablet_id, new_tablet_id, "job2", "be1",
                                 output_rowsets, res);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        res_code = remove_delete_bitmap_lock(meta_service.get(), table_id, -2, 12345);
        ASSERT_EQ(res_code,
                  lock_version == 1 ? MetaServiceCode::LOCK_EXPIRED : MetaServiceCode::OK);
        res_code = remove_delete_bitmap_lock(meta_service.get(), table_id, -2, 12346);
        ASSERT_EQ(res_code, MetaServiceCode::OK);
        res.Clear();
    }

    // alter version < 2
    {
        int64_t new_tablet_id = 16;
        ASSERT_NO_FATAL_FAILURE(create_tablet(meta_service.get(), table_id, index_id, partition_id,
                                              new_tablet_id, true, true));
        StartTabletJobResponse sc_res;
        ASSERT_NO_FATAL_FAILURE(start_schema_change_job(meta_service.get(), table_id, index_id,
                                                        partition_id, tablet_id, new_tablet_id,
                                                        "job2", "be1", sc_res));
        std::vector<doris::RowsetMetaCloudPB> output_rowsets;
        auto res_code = get_delete_bitmap_lock(meta_service.get(), table_id, -2, 12345);
        ASSERT_EQ(res_code, MetaServiceCode::OK);
        FinishTabletJobResponse res;
        finish_schema_change_job(meta_service.get(), tablet_id, new_tablet_id, "job2", "be1",
                                 output_rowsets, res);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        res_code = get_delete_bitmap_lock(meta_service.get(), table_id, 100, -1);
        ASSERT_EQ(res_code, MetaServiceCode::OK);
        res_code = remove_delete_bitmap_lock(meta_service.get(), table_id, 100, -1);
        ASSERT_EQ(res_code, MetaServiceCode::OK);
        res.Clear();
    }
}

TEST(MetaServiceJobTest, SchemaChangeJobWithMoWTest) {
    testSchemaChangeJobWithMoWTest(2);
    testSchemaChangeJobWithMoWTest(1);
}

TEST(MetaServiceJobTest, ConcurrentCompactionTest) {
    auto meta_service = get_meta_service();
    // meta_service->resource_mgr().reset(); // Do not use resource manager

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
    start_compaction_job(meta_service.get(), tablet_id, "job1", "BE1", 0, 0,
                         TabletCompactionJobPB::CUMULATIVE, res);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK); // Same job id, return OK
    res.Clear();
    start_compaction_job(meta_service.get(), tablet_id, "job2", "BE1", 0, 0,
                         TabletCompactionJobPB::CUMULATIVE, res);
    ASSERT_EQ(res.status().code(), MetaServiceCode::JOB_TABLET_BUSY);
    ::sleep(5);
    res.Clear();
    start_compaction_job(meta_service.get(), tablet_id, "job3", "BE2", 0, 0,
                         TabletCompactionJobPB::CUMULATIVE, res);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    res.Clear();
    start_compaction_job(meta_service.get(), tablet_id, "job4", "BE1", 0, 0,
                         TabletCompactionJobPB::BASE, res);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    res.Clear();
    start_compaction_job(meta_service.get(), tablet_id, "job4", "BE1", 0, 0,
                         TabletCompactionJobPB::BASE, res);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK); // Same job id, return OK
    res.Clear();
    start_compaction_job(meta_service.get(), tablet_id, "job5", "BE1", 0, 0,
                         TabletCompactionJobPB::BASE, res);
    ASSERT_EQ(res.status().code(), MetaServiceCode::JOB_TABLET_BUSY);

    // check job kv
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string job_key =
            job_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
    std::string job_val;
    TabletJobInfoPB job_pb;
    ASSERT_EQ(txn->get(job_key, &job_val), TxnErrorCode::TXN_OK);
    ASSERT_TRUE(job_pb.ParseFromString(job_val));
    ASSERT_EQ(job_pb.compaction_size(), 2);
    ASSERT_EQ(job_pb.compaction(0).id(), "job3");
    ASSERT_EQ(job_pb.compaction(0).initiator(), "BE2");
    ASSERT_EQ(job_pb.compaction(1).id(), "job4");
    ASSERT_EQ(job_pb.compaction(1).initiator(), "BE1");

    // BE2 abort job3
    {
        FinishTabletJobRequest req;
        FinishTabletJobResponse res;
        req.set_action(FinishTabletJobRequest::ABORT);
        auto compaction = req.mutable_job()->add_compaction();
        compaction->set_id("job3");
        compaction->set_initiator("BE2");
        compaction->set_type(TabletCompactionJobPB::CUMULATIVE);
        req.mutable_job()->mutable_idx()->set_tablet_id(tablet_id);
        meta_service->finish_tablet_job(&cntl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        meta_service->finish_tablet_job(&cntl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_NE(res.status().msg().find("unmatched job id"), std::string::npos);
    }

    // BE1 lease job4
    long now = time(nullptr);
    {
        FinishTabletJobRequest req;
        FinishTabletJobResponse res;
        req.set_action(FinishTabletJobRequest::LEASE);
        auto compaction = req.mutable_job()->add_compaction();
        compaction->set_id("job4");
        compaction->set_initiator("BE1");
        compaction->set_lease(now + 10);
        req.mutable_job()->mutable_idx()->set_tablet_id(tablet_id);
        meta_service->finish_tablet_job(&cntl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(job_key, &job_val), TxnErrorCode::TXN_OK);
    job_pb.Clear();
    ASSERT_TRUE(job_pb.ParseFromString(job_val));
    ASSERT_EQ(job_pb.compaction_size(), 1);
    ASSERT_EQ(job_pb.compaction(0).id(), "job4");
    ASSERT_EQ(job_pb.compaction(0).initiator(), "BE1");
    ASSERT_EQ(job_pb.compaction(0).lease(), now + 10);

    res.Clear();
    start_compaction_job(meta_service.get(), tablet_id, "job5", "BE2", 0, 0,
                         TabletCompactionJobPB::CUMULATIVE, res);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(job_key, &job_val), TxnErrorCode::TXN_OK);
    ASSERT_TRUE(job_pb.ParseFromString(job_val));
    ASSERT_EQ(job_pb.compaction_size(), 2);
    ASSERT_EQ(job_pb.compaction(0).id(), "job4");
    ASSERT_EQ(job_pb.compaction(0).initiator(), "BE1");
    ASSERT_EQ(job_pb.compaction(1).id(), "job5");
    ASSERT_EQ(job_pb.compaction(1).initiator(), "BE2");

    // Provide existed rowsets
    std::vector<doris::RowsetMetaCloudPB> existed_rowsets;
    for (int i = 0; i < 10; ++i) { // [2-11]
        existed_rowsets.push_back(create_rowset(tablet_id, i + 2, i + 2));
    }
    insert_rowsets(meta_service->txn_kv().get(), table_id, index_id, partition_id, tablet_id,
                   existed_rowsets);

    // BE2 commit job5
    {
        // Provide output rowset
        auto output_rowset = create_rowset(tablet_id, 5, 10);
        CreateRowsetResponse rowset_res;
        commit_rowset(meta_service.get(), output_rowset, rowset_res);
        ASSERT_EQ(rowset_res.status().code(), MetaServiceCode::OK);

        FinishTabletJobRequest req;
        FinishTabletJobResponse res;
        req.set_action(FinishTabletJobRequest::COMMIT);
        req.mutable_job()->mutable_idx()->set_tablet_id(tablet_id);
        auto compaction = req.mutable_job()->add_compaction();
        compaction->set_id("job5");
        compaction->set_initiator("BE2");
        compaction->set_type(TabletCompactionJobPB::CUMULATIVE);
        compaction->add_input_versions(5);
        compaction->add_input_versions(10);
        compaction->add_txn_id(output_rowset.txn_id());
        compaction->add_output_versions(10);
        compaction->add_output_rowset_ids(output_rowset.rowset_id_v2());
        compaction->set_output_cumulative_point(11);
        compaction->set_size_input_rowsets(60000);
        compaction->set_index_size_input_rowsets(30000);
        compaction->set_segment_size_input_rowsets(30000);
        compaction->set_num_input_rows(600);
        compaction->set_num_input_rowsets(6);
        compaction->set_num_input_segments(6);
        compaction->set_size_output_rowsets(10000);
        compaction->set_index_size_output_rowsets(5000);
        compaction->set_segment_size_output_rowsets(5000);
        compaction->set_num_output_rows(100);
        compaction->set_num_output_rowsets(1);
        compaction->set_num_output_segments(1);
        meta_service->finish_tablet_job(&cntl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_TRUE(res.has_stats());
        EXPECT_EQ(res.stats().cumulative_point(), 11);
        // [0-1][2][3][4][5-10][11]
        EXPECT_EQ(res.stats().num_rows(), 500);
        EXPECT_EQ(res.stats().num_rowsets(), 6);
        EXPECT_EQ(res.stats().num_segments(), 5);
        EXPECT_EQ(res.stats().data_size(), 50000);
        EXPECT_EQ(res.stats().index_size(), 25000);
        EXPECT_EQ(res.stats().segment_size(), 25000);
        TabletStatsPB tablet_stats;
        get_tablet_stats(meta_service.get(), tablet_id, tablet_stats);
        EXPECT_EQ(tablet_stats.num_rows(), 500);
        EXPECT_EQ(tablet_stats.num_rowsets(), 6);
        EXPECT_EQ(tablet_stats.num_segments(), 5);
        EXPECT_EQ(tablet_stats.data_size(), 50000);
        EXPECT_EQ(tablet_stats.index_size(), 25000);
        EXPECT_EQ(tablet_stats.segment_size(), 25000);

        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        // Check tmp rowsets
        std::string tmp_rs_key, tmp_rs_val;
        meta_rowset_tmp_key({instance_id, output_rowset.txn_id(), tablet_id}, &tmp_rs_val);
        ASSERT_EQ(txn->get(tmp_rs_key, &tmp_rs_val), TxnErrorCode::TXN_KEY_NOT_FOUND);
        // Check visible rowsets
        std::unique_ptr<RangeGetIterator> it;
        auto rs_start = meta_rowset_key({instance_id, table_id, 0});
        auto rs_end = meta_rowset_key({instance_id, tablet_id, 100});
        ASSERT_EQ(txn->get(rs_start, rs_end, &it), TxnErrorCode::TXN_OK);
        ASSERT_EQ(it->size(), 6);
        auto [k, v] = it->next(); // [0-1]
        doris::RowsetMetaCloudPB visible_rowset;
        ASSERT_TRUE(visible_rowset.ParseFromArray(v.data(), v.size()));
        EXPECT_EQ(visible_rowset.start_version(), 0);
        EXPECT_EQ(visible_rowset.end_version(), 1);
        for (int i = 0; i < 3; ++i) { // [2][3][4]
            std::tie(k, v) = it->next();
            ASSERT_TRUE(visible_rowset.ParseFromArray(v.data(), v.size()));
            EXPECT_EQ(visible_rowset.start_version(), i + 2);
            EXPECT_EQ(visible_rowset.end_version(), i + 2);
            EXPECT_EQ(visible_rowset.rowset_id_v2(), existed_rowsets[i].rowset_id_v2());
        }
        std::tie(k, v) = it->next(); // [5-10]
        ASSERT_TRUE(visible_rowset.ParseFromArray(v.data(), v.size()));
        EXPECT_EQ(visible_rowset.start_version(), 5);
        EXPECT_EQ(visible_rowset.end_version(), 10);
        EXPECT_EQ(visible_rowset.rowset_id_v2(), output_rowset.rowset_id_v2());
        std::tie(k, v) = it->next(); // [11]
        ASSERT_TRUE(visible_rowset.ParseFromArray(v.data(), v.size()));
        EXPECT_EQ(visible_rowset.start_version(), 11);
        EXPECT_EQ(visible_rowset.end_version(), 11);
        EXPECT_EQ(visible_rowset.rowset_id_v2(), existed_rowsets[9].rowset_id_v2());

        // Check recycle rowsets
        auto recycl_rs_start = recycle_rowset_key({instance_id, tablet_id, ""});
        auto recycl_rs_end = recycle_rowset_key({instance_id, tablet_id, "\xff"});
        ASSERT_EQ(txn->get(recycl_rs_start, recycl_rs_end, &it), TxnErrorCode::TXN_OK);
        // [5][6][7][8][9][10]
        ASSERT_EQ(it->size(), 6);
        for (int i = 0; i < 6; ++i) {
            auto [k, v] = it->next();
            k.remove_prefix(1);
            std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
            decode_key(&k, &out);
            // 0x01 "recycle" ${instance_id} "rowset" ${tablet_id} ${rowset_id} -> RecycleRowsetPB
            const auto& rowset_id = std::get<std::string>(std::get<0>(out[4]));
            EXPECT_EQ(rowset_id, existed_rowsets[i + 3].rowset_id_v2());
        }
    }

    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(job_key, &job_val), TxnErrorCode::TXN_OK);
    ASSERT_TRUE(job_pb.ParseFromString(job_val));
    ASSERT_EQ(job_pb.compaction_size(), 1);
    ASSERT_EQ(job_pb.compaction(0).id(), "job4");
    ASSERT_EQ(job_pb.compaction(0).initiator(), "BE1");

    // BE1 commit job4
    {
        // Provide output rowset
        auto output_rowset = create_rowset(tablet_id, 2, 4);
        CreateRowsetResponse rowset_res;
        commit_rowset(meta_service.get(), output_rowset, rowset_res);
        ASSERT_EQ(rowset_res.status().code(), MetaServiceCode::OK);

        FinishTabletJobRequest req;
        FinishTabletJobResponse res;
        req.set_action(FinishTabletJobRequest::COMMIT);
        req.mutable_job()->mutable_idx()->set_tablet_id(tablet_id);
        auto compaction = req.mutable_job()->add_compaction();
        compaction->set_id("job4");
        compaction->set_initiator("BE1");
        compaction->set_type(TabletCompactionJobPB::BASE);
        compaction->add_input_versions(2);
        compaction->add_input_versions(4);
        compaction->add_txn_id(output_rowset.txn_id());
        compaction->add_output_versions(4);
        compaction->add_output_rowset_ids(output_rowset.rowset_id_v2());
        compaction->set_output_cumulative_point(5);
        compaction->set_size_input_rowsets(30000);
        compaction->set_index_size_input_rowsets(15000);
        compaction->set_segment_size_input_rowsets(15000);
        compaction->set_num_input_rows(300);
        compaction->set_num_input_rowsets(3);
        compaction->set_num_input_segments(3);
        compaction->set_size_output_rowsets(10000);
        compaction->set_index_size_output_rowsets(5000);
        compaction->set_segment_size_output_rowsets(5000);
        compaction->set_num_output_rows(100);
        compaction->set_num_output_rowsets(1);
        compaction->set_num_output_segments(1);
        meta_service->finish_tablet_job(&cntl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_TRUE(res.has_stats());
        EXPECT_EQ(res.stats().cumulative_point(), 11);
        // [0-1][2-4][5-10][11]
        EXPECT_EQ(res.stats().num_rows(), 300);
        EXPECT_EQ(res.stats().num_rowsets(), 4);
        EXPECT_EQ(res.stats().num_segments(), 3);
        EXPECT_EQ(res.stats().data_size(), 30000);
        EXPECT_EQ(res.stats().index_size(), 15000);
        EXPECT_EQ(res.stats().segment_size(), 15000);
        TabletStatsPB tablet_stats;
        get_tablet_stats(meta_service.get(), tablet_id, tablet_stats);
        EXPECT_EQ(tablet_stats.num_rows(), 300);
        EXPECT_EQ(tablet_stats.num_rowsets(), 4);
        EXPECT_EQ(tablet_stats.num_segments(), 3);
        EXPECT_EQ(tablet_stats.data_size(), 30000);
        EXPECT_EQ(tablet_stats.index_size(), 15000);
        EXPECT_EQ(tablet_stats.segment_size(), 15000);

        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        // Check tmp rowsets
        std::string tmp_rs_key, tmp_rs_val;
        meta_rowset_tmp_key({instance_id, output_rowset.txn_id(), tablet_id}, &tmp_rs_val);
        ASSERT_EQ(txn->get(tmp_rs_key, &tmp_rs_val), TxnErrorCode::TXN_KEY_NOT_FOUND);
        // Check visible rowsets
        std::unique_ptr<RangeGetIterator> it;
        auto rs_start = meta_rowset_key({instance_id, table_id, 0});
        auto rs_end = meta_rowset_key({instance_id, tablet_id, 100});
        ASSERT_EQ(txn->get(rs_start, rs_end, &it), TxnErrorCode::TXN_OK);
        ASSERT_EQ(it->size(), 4);
        auto [k, v] = it->next();
        doris::RowsetMetaCloudPB visible_rowset;
        ASSERT_TRUE(visible_rowset.ParseFromArray(v.data(), v.size()));
        EXPECT_EQ(visible_rowset.start_version(), 0);
        EXPECT_EQ(visible_rowset.end_version(), 1);
        std::tie(k, v) = it->next();
        ASSERT_TRUE(visible_rowset.ParseFromArray(v.data(), v.size()));
        EXPECT_EQ(visible_rowset.start_version(), 2);
        EXPECT_EQ(visible_rowset.end_version(), 4);
        EXPECT_EQ(visible_rowset.rowset_id_v2(), output_rowset.rowset_id_v2());
        std::tie(k, v) = it->next();
        ASSERT_TRUE(visible_rowset.ParseFromArray(v.data(), v.size()));
        EXPECT_EQ(visible_rowset.start_version(), 5);
        EXPECT_EQ(visible_rowset.end_version(), 10);
        std::tie(k, v) = it->next();
        ASSERT_TRUE(visible_rowset.ParseFromArray(v.data(), v.size()));
        EXPECT_EQ(visible_rowset.start_version(), 11);
        EXPECT_EQ(visible_rowset.end_version(), 11);
        EXPECT_EQ(visible_rowset.rowset_id_v2(), existed_rowsets[9].rowset_id_v2());

        // Check recycle rowsets
        auto recycl_rs_start = recycle_rowset_key({instance_id, tablet_id, ""});
        auto recycl_rs_end = recycle_rowset_key({instance_id, tablet_id, "\xff"});
        ASSERT_EQ(txn->get(recycl_rs_start, recycl_rs_end, &it), TxnErrorCode::TXN_OK);
        // [2][3][4][5][6][7][8][9][10]
        ASSERT_EQ(it->size(), 9);
        for (int i = 0; i < 9; ++i) {
            auto [k, v] = it->next();
            k.remove_prefix(1);
            std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
            decode_key(&k, &out);
            // 0x01 "recycle" ${instance_id} "rowset" ${tablet_id} ${rowset_id} -> RecycleRowsetPB
            const auto& rowset_id = std::get<std::string>(std::get<0>(out[4]));
            EXPECT_EQ(rowset_id, existed_rowsets[i].rowset_id_v2());
        }
    }

    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(job_key, &job_val), TxnErrorCode::TXN_OK);
    ASSERT_TRUE(job_pb.ParseFromString(job_val));
    ASSERT_EQ(job_pb.compaction_size(), 0);
}

TEST(MetaServiceJobTest, ParallelCumuCompactionTest) {
    auto meta_service = get_meta_service();
    // meta_service->resource_mgr().reset(); // Do not use resource manager

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
    start_compaction_job(meta_service.get(), tablet_id, "job2", "BE1", 0, 0,
                         TabletCompactionJobPB::CUMULATIVE, res);
    ASSERT_EQ(res.status().code(), MetaServiceCode::JOB_TABLET_BUSY);
    // Don't return `version_in_compaction` to disable parallel cumu compaction
    EXPECT_EQ(res.version_in_compaction_size(), 0);
    res.Clear();
    start_compaction_job(meta_service.get(), tablet_id, "job3", "BE2", 0, 0,
                         TabletCompactionJobPB::CUMULATIVE, res, {5, 10});
    ASSERT_EQ(res.status().code(), MetaServiceCode::JOB_TABLET_BUSY);
    // Don't return `version_in_compaction` to disable parallel cumu compaction
    EXPECT_EQ(res.version_in_compaction_size(), 0);

    ::sleep(5); // Wait for job1 expired

    res.Clear();
    start_compaction_job(meta_service.get(), tablet_id, "job4", "BE1", 0, 0,
                         TabletCompactionJobPB::CUMULATIVE, res, {5, 10});
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    res.Clear();
    start_compaction_job(meta_service.get(), tablet_id, "job5", "BE1", 0, 0,
                         TabletCompactionJobPB::CUMULATIVE, res, {12, 15});
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    res.Clear();
    start_compaction_job(meta_service.get(), tablet_id, "job6", "BE1", 0, 0,
                         TabletCompactionJobPB::CUMULATIVE, res, {6, 9});
    ASSERT_EQ(res.status().code(), MetaServiceCode::JOB_TABLET_BUSY);
    ASSERT_EQ(res.version_in_compaction_size(), 4); // [5-10][11-15]
    EXPECT_EQ(res.version_in_compaction(0), 5);
    EXPECT_EQ(res.version_in_compaction(1), 10);
    EXPECT_EQ(res.version_in_compaction(2), 12);
    EXPECT_EQ(res.version_in_compaction(3), 15);
    res.Clear();
    start_compaction_job(meta_service.get(), tablet_id, "job7", "BE2", 0, 0,
                         TabletCompactionJobPB::CUMULATIVE, res, {5, 9});
    ASSERT_EQ(res.status().code(), MetaServiceCode::JOB_TABLET_BUSY);
    ASSERT_EQ(res.version_in_compaction_size(), 4); // [5-10][11-15]
    EXPECT_EQ(res.version_in_compaction(0), 5);
    EXPECT_EQ(res.version_in_compaction(1), 10);
    EXPECT_EQ(res.version_in_compaction(2), 12);
    EXPECT_EQ(res.version_in_compaction(3), 15);
    res.Clear();
    start_compaction_job(meta_service.get(), tablet_id, "job8", "BE2", 0, 0,
                         TabletCompactionJobPB::CUMULATIVE, res, {6, 10});
    ASSERT_EQ(res.status().code(), MetaServiceCode::JOB_TABLET_BUSY);
    ASSERT_EQ(res.version_in_compaction_size(), 4); // [5-10][11-15]
    EXPECT_EQ(res.version_in_compaction(0), 5);
    EXPECT_EQ(res.version_in_compaction(1), 10);
    EXPECT_EQ(res.version_in_compaction(2), 12);
    EXPECT_EQ(res.version_in_compaction(3), 15);
    res.Clear();
    start_compaction_job(meta_service.get(), tablet_id, "job9", "BE2", 0, 0,
                         TabletCompactionJobPB::CUMULATIVE, res, {4, 11});
    ASSERT_EQ(res.status().code(), MetaServiceCode::JOB_TABLET_BUSY);
    ASSERT_EQ(res.version_in_compaction_size(), 4); // [5-10][11-15]
    EXPECT_EQ(res.version_in_compaction(0), 5);
    EXPECT_EQ(res.version_in_compaction(1), 10);
    EXPECT_EQ(res.version_in_compaction(2), 12);
    EXPECT_EQ(res.version_in_compaction(3), 15);
    res.Clear();
    start_compaction_job(meta_service.get(), tablet_id, "job10", "BE2", 0, 0,
                         TabletCompactionJobPB::CUMULATIVE, res, {3, 5});
    ASSERT_EQ(res.status().code(), MetaServiceCode::JOB_TABLET_BUSY);
    ASSERT_EQ(res.version_in_compaction_size(), 4); // [5-10][11-15]
    EXPECT_EQ(res.version_in_compaction(0), 5);
    EXPECT_EQ(res.version_in_compaction(1), 10);
    EXPECT_EQ(res.version_in_compaction(2), 12);
    EXPECT_EQ(res.version_in_compaction(3), 15);
    res.Clear();
    start_compaction_job(meta_service.get(), tablet_id, "job11", "BE2", 0, 0,
                         TabletCompactionJobPB::CUMULATIVE, res, {10, 11});
    ASSERT_EQ(res.status().code(), MetaServiceCode::JOB_TABLET_BUSY);
    ASSERT_EQ(res.version_in_compaction_size(), 4); // [5-10][11-15]
    EXPECT_EQ(res.version_in_compaction(0), 5);
    EXPECT_EQ(res.version_in_compaction(1), 10);
    EXPECT_EQ(res.version_in_compaction(2), 12);
    EXPECT_EQ(res.version_in_compaction(3), 15);
    res.Clear();
    start_compaction_job(meta_service.get(), tablet_id, "job12", "BE1", 0, 0,
                         TabletCompactionJobPB::CUMULATIVE, res, {2, 4});
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    res.Clear();
    start_compaction_job(meta_service.get(), tablet_id, "job13", "BE1", 0, 0,
                         TabletCompactionJobPB::CUMULATIVE, res);
    ASSERT_EQ(res.status().code(), MetaServiceCode::JOB_TABLET_BUSY);
    // Don't return `version_in_compaction` to disable parallel cumu compaction
    EXPECT_EQ(res.version_in_compaction_size(), 0);
    res.Clear();
    start_compaction_job(meta_service.get(), tablet_id, "job14", "BE1", 0, 0,
                         TabletCompactionJobPB::CUMULATIVE, res, {11, 11});
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
}

TEST(MetaServiceJobTest, SchemaChangeJobPersistTest) {
    auto meta_service = get_meta_service();

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

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string job_key =
            job_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
    std::string job_val;
    TabletJobInfoPB job_pb;
    ASSERT_EQ(txn->get(job_key, &job_val), TxnErrorCode::TXN_OK);
    ASSERT_TRUE(job_pb.ParseFromString(job_val));
    ASSERT_EQ(job_pb.compaction_size(), 1);
    ASSERT_EQ(job_pb.compaction(0).id(), "job1");
    ASSERT_EQ(job_pb.compaction(0).initiator(), "BE1");

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
    meta_service->finish_tablet_job(&cntl, &req, &finish_res_2, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

    FinishTabletJobResponse finish_res;
    finish_schema_change_job(meta_service.get(), tablet_id, new_tablet_id, "job1", "BE1", {},
                             finish_res);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
}

TEST(MetaServiceJobTest, DoCompactionWhenSC) {
    auto meta_service = get_meta_service();

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

    brpc::Controller cntl;

    int64_t table_id = 5421;
    int64_t index_id = 5422;
    int64_t partition_id = 5423;
    int64_t tablet_id = 5424;
    int64_t new_tablet_id = 54211;

    ASSERT_NO_FATAL_FAILURE(
            create_tablet(meta_service.get(), table_id, index_id, partition_id, tablet_id, false));

    ASSERT_NO_FATAL_FAILURE(create_tablet(meta_service.get(), table_id, index_id, partition_id,
                                          new_tablet_id, false, true));

    StartTabletJobResponse sc_res;
    ASSERT_NO_FATAL_FAILURE(start_schema_change_job(meta_service.get(), table_id, index_id,
                                                    partition_id, tablet_id, new_tablet_id,
                                                    "job_sc", "BE1", sc_res, 8));

    StartTabletJobResponse res;
    start_compaction_job(meta_service.get(), tablet_id, "job1", "BE1", 0, 7,
                         TabletCompactionJobPB::CUMULATIVE, res, {7, 10});
    ASSERT_EQ(res.status().code(), MetaServiceCode::JOB_CHECK_ALTER_VERSION);
    res.Clear();

    start_compaction_job(meta_service.get(), tablet_id, "job1", "BE1", 0, 7,
                         TabletCompactionJobPB::BASE, res, {0, 10});
    ASSERT_EQ(res.status().code(), MetaServiceCode::JOB_CHECK_ALTER_VERSION);
    res.Clear();

    start_compaction_job(meta_service.get(), tablet_id, "job1", "BE1", 0, 7,
                         TabletCompactionJobPB::BASE, res, {0, 7});
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    res.Clear();

    start_compaction_job(meta_service.get(), new_tablet_id, "job2", "BE1", 0, 7,
                         TabletCompactionJobPB::CUMULATIVE, res, {9, 10});
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    res.Clear();

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string job_key =
            job_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
    std::string job_val;
    TabletJobInfoPB job_pb;
    ASSERT_EQ(txn->get(job_key, &job_val), TxnErrorCode::TXN_OK);
    ASSERT_TRUE(job_pb.ParseFromString(job_val));
    ASSERT_EQ(job_pb.compaction_size(), 1);
    ASSERT_EQ(job_pb.compaction(0).id(), "job1");
    ASSERT_EQ(job_pb.compaction(0).initiator(), "BE1");
    ASSERT_EQ(job_pb.compaction(0).input_versions(0), 0);
    ASSERT_EQ(job_pb.compaction(0).input_versions(1), 7);
    ASSERT_EQ(job_pb.schema_change().alter_version(), 8);

    std::string new_job_key =
            job_tablet_key({instance_id, table_id, index_id, partition_id, new_tablet_id});
    std::string new_job_val;
    TabletJobInfoPB new_job_pb;
    ASSERT_EQ(txn->get(new_job_key, &new_job_val), TxnErrorCode::TXN_OK);
    ASSERT_TRUE(new_job_pb.ParseFromString(new_job_val));
    ASSERT_EQ(new_job_pb.compaction_size(), 1);
    ASSERT_EQ(new_job_pb.compaction(0).id(), "job2");
    ASSERT_EQ(new_job_pb.compaction(0).initiator(), "BE1");
    ASSERT_EQ(new_job_pb.compaction(0).input_versions(0), 9);
    ASSERT_EQ(new_job_pb.compaction(0).input_versions(1), 10);
    ASSERT_EQ(new_job_pb.schema_change().alter_version(), 8);

    FinishTabletJobResponse finish_res;
    finish_schema_change_job(meta_service.get(), tablet_id, new_tablet_id, "job_sc", "BE1", {},
                             finish_res);
    ASSERT_EQ(finish_res.status().code(), MetaServiceCode::OK);
}

TEST(MetaServiceJobTest, ReStartSC) {
    auto meta_service = get_meta_service();

    auto* sp = SyncPoint::get_instance();
    DORIS_CLOUD_DEFER {
        SyncPoint::get_instance()->clear_all_call_backs();
    };
    sp->set_call_back("get_instance_id", [&](auto&& args) {
        auto* ret = try_any_cast_ret<std::string>(args);
        ret->first = instance_id;
        ret->second = true;
    });
    bool use_origin_job = false;
    sp->set_call_back("restart_compaction_job", [&](auto&&) { use_origin_job = true; });
    sp->enable_processing();

    brpc::Controller cntl;

    int64_t table_id = 5331;
    int64_t index_id = 5332;
    int64_t partition_id = 5333;
    int64_t tablet_id = 5334;
    int64_t new_tablet_id = 53311;

    ASSERT_NO_FATAL_FAILURE(
            create_tablet(meta_service.get(), table_id, index_id, partition_id, tablet_id, false));

    ASSERT_NO_FATAL_FAILURE(create_tablet(meta_service.get(), table_id, index_id, partition_id,
                                          new_tablet_id, false, true));

    StartTabletJobResponse sc_res;
    ASSERT_NO_FATAL_FAILURE(start_schema_change_job(meta_service.get(), table_id, index_id,
                                                    partition_id, tablet_id, new_tablet_id,
                                                    "job_sc", "BE1", sc_res, 8));
    sc_res.Clear();
    ASSERT_NO_FATAL_FAILURE(start_schema_change_job(meta_service.get(), table_id, index_id,
                                                    partition_id, tablet_id, new_tablet_id,
                                                    "job_sc", "BE1", sc_res, 8));
    ASSERT_TRUE(use_origin_job);
    ASSERT_EQ(sc_res.alter_version(), 8);
    FinishTabletJobResponse finish_res;
    finish_schema_change_job(meta_service.get(), tablet_id, new_tablet_id, "job_sc", "BE1", {},
                             finish_res);
    ASSERT_EQ(finish_res.status().code(), MetaServiceCode::OK);
}

TEST(MetaServiceJobTest, CancelSC) {
    auto meta_service = get_meta_service();

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

    brpc::Controller cntl;

    int64_t table_id = 5731;
    int64_t index_id = 5732;
    int64_t partition_id = 5733;
    int64_t tablet_id = 5734;
    int64_t new_tablet_id = 57311;
    int64_t new_tablet_id1 = 57322;

    ASSERT_NO_FATAL_FAILURE(
            create_tablet(meta_service.get(), table_id, index_id, partition_id, tablet_id, false));

    ASSERT_NO_FATAL_FAILURE(create_tablet(meta_service.get(), table_id, index_id, partition_id,
                                          new_tablet_id, false, true));

    ASSERT_NO_FATAL_FAILURE(create_tablet(meta_service.get(), table_id, index_id, partition_id,
                                          new_tablet_id1, false, true));

    StartTabletJobResponse sc_res;
    ASSERT_NO_FATAL_FAILURE(start_schema_change_job(meta_service.get(), table_id, index_id,
                                                    partition_id, tablet_id, new_tablet_id,
                                                    "job_sc", "BE1", sc_res, 8));
    {
        FinishTabletJobResponse finish_res;
        finish_schema_change_job(meta_service.get(), tablet_id, new_tablet_id, "job_sc", "BE1", {},
                                 finish_res, FinishTabletJobRequest::ABORT);
        ASSERT_EQ(finish_res.status().code(), MetaServiceCode::OK);
    }
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string job_key =
                job_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
        std::string job_val;
        TabletJobInfoPB job_pb;
        ASSERT_EQ(txn->get(job_key, &job_val), TxnErrorCode::TXN_OK);
        ASSERT_TRUE(job_pb.ParseFromString(job_val));
        ASSERT_FALSE(job_pb.has_schema_change());
    }
    sc_res.Clear();
    ASSERT_NO_FATAL_FAILURE(start_schema_change_job(meta_service.get(), table_id, index_id,
                                                    partition_id, tablet_id, new_tablet_id1,
                                                    "job_sc1", "BE1", sc_res, 8));
    {
        FinishTabletJobResponse finish_res;
        finish_schema_change_job(meta_service.get(), tablet_id, new_tablet_id, "job_sc", "BE1", {},
                                 finish_res, FinishTabletJobRequest::ABORT);
        ASSERT_NE(finish_res.status().msg().find("unmatched job id or initiator"),
                  std::string::npos);
    }
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string job_key =
                job_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
        std::string job_val;
        TabletJobInfoPB job_pb;
        ASSERT_EQ(txn->get(job_key, &job_val), TxnErrorCode::TXN_OK);
        ASSERT_TRUE(job_pb.ParseFromString(job_val));
        ASSERT_TRUE(job_pb.has_schema_change());
    }
}

} // namespace doris::cloud
