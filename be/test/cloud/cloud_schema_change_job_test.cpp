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

#include "cloud/cloud_schema_change_job.h"

#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest.h>

#include <memory>

#include "cloud/cloud_cluster_info.h"
#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "cloud/cloud_tablet_mgr.h"
#include "common/status.h"
#include "cpp/sync_point.h"
#include "json2pb/json_to_pb.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/tablet/tablet_meta.h"
#include "util/uid_util.h"

namespace doris {

class CloudSchemaChangeJobTest : public testing::Test {
public:
    CloudSchemaChangeJobTest() : _engine(CloudStorageEngine(EngineOptions {})) {}

    void SetUp() override {
        _cluster_info = std::make_shared<CloudClusterInfo>();
        _cluster_info->_is_in_standby = false;
        ExecEnv::GetInstance()->_cluster_info = _cluster_info.get();

        _json_rowset_meta = R"({
            "rowset_id": 540081,
            "tablet_id": 10001,
            "txn_id": 4042,
            "tablet_schema_hash": 567997577,
            "rowset_type": "BETA_ROWSET",
            "rowset_state": "VISIBLE",
            "start_version": 2,
            "end_version": 2,
            "num_rows": 100,
            "total_disk_size": 41,
            "data_disk_size": 41,
            "index_disk_size": 235,
            "empty": false,
            "load_id": {
                "hi": -5350970832824939812,
                "lo": -6717994719194512122
            },
            "creation_time": 1553765670,
            "num_segments": 0
        })";
    }

    void TearDown() override {
        auto* sp = SyncPoint::get_instance();
        sp->disable_processing();
        sp->clear_all_call_backs();
    }

protected:
    RowsetSharedPtr create_rowset(TabletSchemaSPtr schema, int64_t tablet_id, int64_t start,
                                  int64_t end) {
        RowsetMetaPB pb;
        json2pb::JsonToProtoMessage(_json_rowset_meta, &pb);
        pb.set_tablet_id(tablet_id);
        pb.set_start_version(start);
        pb.set_end_version(end);
        auto rs_meta = std::make_shared<RowsetMeta>();
        rs_meta->init_from_pb(pb);
        rs_meta->set_tablet_schema(schema);
        RowsetSharedPtr rowset;
        static_cast<void>(RowsetFactory::create_rowset(schema, "", rs_meta, &rowset));
        return rowset;
    }

    std::string _json_rowset_meta;
    CloudStorageEngine _engine;
    std::shared_ptr<CloudClusterInfo> _cluster_info;
};

// Test: cross-V1 compaction detected → abort SC job → return SC_COMPACTION_CONFLICT
TEST_F(CloudSchemaChangeJobTest, CrossV1CompactionDetected) {
    int64_t base_tablet_id = 10001;
    int64_t new_tablet_id = 10002;

    // Create tablet metas
    TabletMetaSharedPtr base_meta(new TabletMeta(
            1, 2, base_tablet_id, base_tablet_id + 100, 4, 5, TTabletSchema(), 6, {{7, 8}},
            UniqueId(9, 10), TTabletType::TABLET_TYPE_DISK, TCompressionType::LZ4F));
    TabletMetaSharedPtr new_meta(new TabletMeta(
            1, 2, new_tablet_id, new_tablet_id + 100, 4, 5, TTabletSchema(), 6, {{7, 8}},
            UniqueId(11, 12), TTabletType::TABLET_TYPE_DISK, TCompressionType::LZ4F));

    auto base_tablet = std::make_shared<CloudTablet>(_engine, std::move(base_meta));
    auto new_tablet = std::make_shared<CloudTablet>(_engine, std::move(new_meta));

    // New tablet must be NOTREADY for SC (TABLET_RUNNING would short-circuit)
    static_cast<void>(new_tablet->set_tablet_state(TABLET_NOTREADY));

    // Note: process_alter_tablet calls get_tablet() which creates new CloudTablet
    // instances from meta. Rowsets are injected via the sync_tablet_rowsets mock below.

    // Set up SyncPoint mocks
    auto* sp = SyncPoint::get_instance();
    sp->clear_all_call_backs();
    sp->enable_processing();

    // Mock get_tablet_meta → return the pre-built tablet meta
    sp->set_call_back("CloudMetaMgr::get_tablet_meta", [&](auto&& args) {
        auto tablet_id = try_any_cast<int64_t>(args[0]);
        auto* meta_ptr = try_any_cast<TabletMetaSharedPtr*>(args[1]);
        if (tablet_id == base_tablet_id) {
            *meta_ptr = base_tablet->tablet_meta();
        } else if (tablet_id == new_tablet_id) {
            *meta_ptr = new_tablet->tablet_meta();
        }
        try_any_cast_ret<Status>(args)->second = true;
    });

    // Mock sync_tablet_rowsets → OK, and inject cross-V1 rowset into new tablet
    auto cross_v1_rowset = create_rowset(new_tablet->tablet_schema(), new_tablet_id, 5, 10);
    sp->set_call_back("CloudMetaMgr::sync_tablet_rowsets",
                      [new_tablet_id, cross_v1_rowset](auto&& outcome) {
                          // Inject cross-V1 rowset when syncing the new tablet
                          auto* tablet = try_any_cast<CloudTablet*>(outcome[0]);
                          if (tablet->tablet_id() == new_tablet_id) {
                              std::unique_lock lock(tablet->get_header_lock());
                              tablet->add_rowsets({cross_v1_rowset}, false, lock);
                          }
                          try_any_cast_ret<Status>(outcome)->second = true;
                      });

    // Mock prepare_tablet_job → OK with alter_version=6
    sp->set_call_back("CloudMetaMgr::prepare_tablet_job", [](auto&& outcome) {
        auto* pairs = try_any_cast_ret<Status>(outcome);
        pairs->second = true;
        pairs->first = Status::OK();

        auto* resp = try_any_cast<cloud::StartTabletJobResponse*>(outcome[1]);
        resp->mutable_status()->set_code(cloud::MetaServiceCode::OK);
        resp->set_alter_version(6); // V1=6, rowset [5-10] crosses this
    });

    // Mock abort_tablet_job → OK
    bool abort_called = false;
    sp->set_call_back("CloudMetaMgr::abort_tablet_job", [&abort_called](auto&& outcome) {
        abort_called = true;
        auto* pairs = try_any_cast_ret<Status>(outcome);
        pairs->second = true;
        pairs->first = Status::OK();
    });

    // Build ALTER request
    TAlterTabletReqV2 request;
    request.base_tablet_id = base_tablet_id;
    request.new_tablet_id = new_tablet_id;
    request.alter_version = 4; // V0

    // Execute
    CloudSchemaChangeJob sc_job(_engine, "test_job_1", 9999999999);
    auto status = sc_job.process_alter_tablet(request);

    // Should fail with SC_COMPACTION_CONFLICT
    ASSERT_FALSE(status.ok());
    ASSERT_TRUE(status.is<ErrorCode::SC_COMPACTION_CONFLICT>()) << status.to_string();

    // abort_tablet_job should have been called to clean up the SC job
    ASSERT_TRUE(abort_called);
}

// Test: cross-V1 detected but abort_tablet_job fails → return non-retryable INTERNAL_ERROR
// (not SC_COMPACTION_CONFLICT) to avoid FE burning retries on a stale meta-service job.
TEST_F(CloudSchemaChangeJobTest, CrossV1CompactionAbortFailed) {
    int64_t base_tablet_id = 20001;
    int64_t new_tablet_id = 20002;

    TabletMetaSharedPtr base_meta(new TabletMeta(
            1, 2, base_tablet_id, base_tablet_id + 100, 4, 5, TTabletSchema(), 6, {{7, 8}},
            UniqueId(9, 10), TTabletType::TABLET_TYPE_DISK, TCompressionType::LZ4F));
    TabletMetaSharedPtr new_meta(new TabletMeta(
            1, 2, new_tablet_id, new_tablet_id + 100, 4, 5, TTabletSchema(), 6, {{7, 8}},
            UniqueId(11, 12), TTabletType::TABLET_TYPE_DISK, TCompressionType::LZ4F));

    auto base_tablet = std::make_shared<CloudTablet>(_engine, std::move(base_meta));
    auto new_tablet = std::make_shared<CloudTablet>(_engine, std::move(new_meta));
    static_cast<void>(new_tablet->set_tablet_state(TABLET_NOTREADY));

    auto* sp = SyncPoint::get_instance();
    sp->clear_all_call_backs();
    sp->enable_processing();

    sp->set_call_back("CloudMetaMgr::get_tablet_meta", [&](auto&& args) {
        auto tablet_id = try_any_cast<int64_t>(args[0]);
        auto* meta_ptr = try_any_cast<TabletMetaSharedPtr*>(args[1]);
        if (tablet_id == base_tablet_id) {
            *meta_ptr = base_tablet->tablet_meta();
        } else if (tablet_id == new_tablet_id) {
            *meta_ptr = new_tablet->tablet_meta();
        }
        try_any_cast_ret<Status>(args)->second = true;
    });

    auto cross_v1_rowset = create_rowset(new_tablet->tablet_schema(), new_tablet_id, 5, 10);
    sp->set_call_back("CloudMetaMgr::sync_tablet_rowsets",
                      [new_tablet_id, cross_v1_rowset](auto&& outcome) {
                          auto* tablet = try_any_cast<CloudTablet*>(outcome[0]);
                          if (tablet->tablet_id() == new_tablet_id) {
                              std::unique_lock lock(tablet->get_header_lock());
                              tablet->add_rowsets({cross_v1_rowset}, false, lock);
                          }
                          try_any_cast_ret<Status>(outcome)->second = true;
                      });

    sp->set_call_back("CloudMetaMgr::prepare_tablet_job", [](auto&& outcome) {
        auto* pairs = try_any_cast_ret<Status>(outcome);
        pairs->second = true;
        pairs->first = Status::OK();
        auto* resp = try_any_cast<cloud::StartTabletJobResponse*>(outcome[1]);
        resp->mutable_status()->set_code(cloud::MetaServiceCode::OK);
        resp->set_alter_version(6);
    });

    // Mock abort_tablet_job → FAIL (simulates meta-service RPC failure)
    sp->set_call_back("CloudMetaMgr::abort_tablet_job", [](auto&& outcome) {
        auto* pairs = try_any_cast_ret<Status>(outcome);
        pairs->second = true;
        pairs->first = Status::InternalError("mock abort failed");
    });

    TAlterTabletReqV2 request;
    request.base_tablet_id = base_tablet_id;
    request.new_tablet_id = new_tablet_id;
    request.alter_version = 4;

    CloudSchemaChangeJob sc_job(_engine, "test_job_2", 9999999999);
    auto status = sc_job.process_alter_tablet(request);

    // Should fail with INTERNAL_ERROR (non-retryable), NOT SC_COMPACTION_CONFLICT
    ASSERT_FALSE(status.ok());
    ASSERT_FALSE(status.is<ErrorCode::SC_COMPACTION_CONFLICT>()) << status.to_string();
    ASSERT_TRUE(status.is<ErrorCode::INTERNAL_ERROR>()) << status.to_string();
    ASSERT_TRUE(status.to_string().find("failed to abort SC job") != std::string::npos)
            << status.to_string();
}

// Test: abort_tablet_job RPC replay sees INVALID_ARGUMENT "no running schema_change"
// (first ABORT committed but reply lost). Should treat as idempotent success and
// return SC_COMPACTION_CONFLICT so FE retries.
TEST_F(CloudSchemaChangeJobTest, CrossV1CompactionAbortIdempotentReplay) {
    int64_t base_tablet_id = 30001;
    int64_t new_tablet_id = 30002;

    TabletMetaSharedPtr base_meta(new TabletMeta(
            1, 2, base_tablet_id, base_tablet_id + 100, 4, 5, TTabletSchema(), 6, {{7, 8}},
            UniqueId(9, 10), TTabletType::TABLET_TYPE_DISK, TCompressionType::LZ4F));
    TabletMetaSharedPtr new_meta(new TabletMeta(
            1, 2, new_tablet_id, new_tablet_id + 100, 4, 5, TTabletSchema(), 6, {{7, 8}},
            UniqueId(11, 12), TTabletType::TABLET_TYPE_DISK, TCompressionType::LZ4F));

    auto base_tablet = std::make_shared<CloudTablet>(_engine, std::move(base_meta));
    auto new_tablet = std::make_shared<CloudTablet>(_engine, std::move(new_meta));
    static_cast<void>(new_tablet->set_tablet_state(TABLET_NOTREADY));

    auto* sp = SyncPoint::get_instance();
    sp->clear_all_call_backs();
    sp->enable_processing();

    sp->set_call_back("CloudMetaMgr::get_tablet_meta", [&](auto&& args) {
        auto tablet_id = try_any_cast<int64_t>(args[0]);
        auto* meta_ptr = try_any_cast<TabletMetaSharedPtr*>(args[1]);
        if (tablet_id == base_tablet_id) {
            *meta_ptr = base_tablet->tablet_meta();
        } else if (tablet_id == new_tablet_id) {
            *meta_ptr = new_tablet->tablet_meta();
        }
        try_any_cast_ret<Status>(args)->second = true;
    });

    auto cross_v1_rowset = create_rowset(new_tablet->tablet_schema(), new_tablet_id, 5, 10);
    sp->set_call_back("CloudMetaMgr::sync_tablet_rowsets",
                      [new_tablet_id, cross_v1_rowset](auto&& outcome) {
                          auto* tablet = try_any_cast<CloudTablet*>(outcome[0]);
                          if (tablet->tablet_id() == new_tablet_id) {
                              std::unique_lock lock(tablet->get_header_lock());
                              tablet->add_rowsets({cross_v1_rowset}, false, lock);
                          }
                          try_any_cast_ret<Status>(outcome)->second = true;
                      });

    sp->set_call_back("CloudMetaMgr::prepare_tablet_job", [](auto&& outcome) {
        auto* pairs = try_any_cast_ret<Status>(outcome);
        pairs->second = true;
        pairs->first = Status::OK();
        auto* resp = try_any_cast<cloud::StartTabletJobResponse*>(outcome[1]);
        resp->mutable_status()->set_code(cloud::MetaServiceCode::OK);
        resp->set_alter_version(6);
    });

    // Mock abort_tablet_job → INVALID_ARGUMENT with "no running schema_change"
    // (simulates retry_rpc replay after first ABORT already committed)
    sp->set_call_back("CloudMetaMgr::abort_tablet_job", [](auto&& outcome) {
        auto* pairs = try_any_cast_ret<Status>(outcome);
        pairs->second = true;
        pairs->first = Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "failed to abort tablet job: there is no running schema_change, tablet_id=30001");
    });

    TAlterTabletReqV2 request;
    request.base_tablet_id = base_tablet_id;
    request.new_tablet_id = new_tablet_id;
    request.alter_version = 4;

    CloudSchemaChangeJob sc_job(_engine, "test_job_3", 9999999999);
    auto status = sc_job.process_alter_tablet(request);

    // Should treat idempotent replay as success → return SC_COMPACTION_CONFLICT (retryable)
    ASSERT_FALSE(status.ok());
    ASSERT_TRUE(status.is<ErrorCode::SC_COMPACTION_CONFLICT>()) << status.to_string();
}

} // namespace doris
