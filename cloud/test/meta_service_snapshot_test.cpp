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
#include <thread>

#include "common/config.h"
#include "common/defer.h"
#include "cpp/sync_point.h"
#include "meta-service/meta_service.h"
#include "meta-service/txn_lazy_committer.h"
#include "meta-store/keys.h"
#include "meta-store/mem_txn_kv.h"
#include "meta-store/txn_kv.h"
#include "meta-store/txn_kv_error.h"
#include "meta-store/versioned_value.h"
#include "mock_accessor.h"
#include "recycler/checker.h"
#include "recycler/recycler.h"
#include "recycler/snapshot_chain_compactor.h"
#include "recycler/snapshot_data_migrator.h"
#include "snapshot/doris_snapshot_manager.h"

namespace doris::cloud {

extern std::unique_ptr<MetaServiceProxy> get_meta_service(bool mock_resource_mgr);

// Helper: create a test instance via RPC with default ObjectStoreInfoPB.
static void create_test_instance_via_rpc(MetaServiceProxy* meta_service,
                                         const std::string& instance_id,
                                         const std::string& name = "test_name") {
    brpc::Controller cntl;
    CreateInstanceRequest req;
    req.set_instance_id(instance_id);
    req.set_user_id("test_user");
    req.set_name(name);
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
    meta_service->create_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                  &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
}

TEST(MetaServiceSnapshotTest, BeginSnapshotTest) {
    auto meta_service = get_meta_service(true);
    const char* const cloud_unique_id = "test_cloud_unique_id";

    // Setup SyncPoint for encryption
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

    // Cleanup SyncPoint when test finishes
    DORIS_CLOUD_DEFER {
        sp->disable_processing();
        sp->clear_all_call_backs();
    };

    // Create test instance first
    create_test_instance_via_rpc(meta_service.get(), "test_instance");

    // test invalid argument - empty cloud_unique_id
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    }

    // test normal begin snapshot
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_timeout_seconds(3600);
        req.set_auto_snapshot(true);
        req.set_ttl_seconds(7200);
        req.set_snapshot_label("test_snapshot");
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_FALSE(res.image_url().empty());
        ASSERT_FALSE(res.snapshot_id().empty());
        ASSERT_TRUE(res.image_url().find("/snapshot/") != std::string::npos);
    }

    // test begin snapshot with custom parameters
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_timeout_seconds(1800);
        req.set_auto_snapshot(false);
        req.set_ttl_seconds(14400);
        req.set_snapshot_label("custom_snapshot");
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_FALSE(res.image_url().empty());
        ASSERT_FALSE(res.snapshot_id().empty());
    }

    // test invalid timeout_seconds - zero
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_timeout_seconds(0);
        req.set_ttl_seconds(7200);
        req.set_snapshot_label("test_snapshot");
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    }

    // test invalid timeout_seconds - negative
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_timeout_seconds(-100);
        req.set_ttl_seconds(7200);
        req.set_snapshot_label("test_snapshot");
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    }

    // test invalid ttl_seconds - zero
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_timeout_seconds(3600);
        req.set_ttl_seconds(0);
        req.set_snapshot_label("test_snapshot");
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    }

    // test invalid ttl_seconds - negative
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_timeout_seconds(3600);
        req.set_ttl_seconds(-500);
        req.set_snapshot_label("test_snapshot");
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    }

    // test empty snapshot_label
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_timeout_seconds(3600);
        req.set_ttl_seconds(7200);
        req.set_snapshot_label("");
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    }

    // test valid IPv4 address
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_timeout_seconds(3600);
        req.set_ttl_seconds(7200);
        req.set_snapshot_label("test_snapshot");
        req.set_request_ip("192.168.1.100");
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // test valid IPv6 address
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_timeout_seconds(3600);
        req.set_ttl_seconds(7200);
        req.set_snapshot_label("test_snapshot");
        req.set_request_ip("2001:db8::1");
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // test invalid IP address format
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_timeout_seconds(3600);
        req.set_ttl_seconds(7200);
        req.set_snapshot_label("test_snapshot");
        req.set_request_ip("invalid.ip.address");
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    }

    // test invalid IP address - out of range
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_timeout_seconds(3600);
        req.set_ttl_seconds(7200);
        req.set_snapshot_label("test_snapshot");
        req.set_request_ip("256.256.256.256");
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    }

    // test empty IP address (should pass - IP is optional)
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_timeout_seconds(3600);
        req.set_ttl_seconds(7200);
        req.set_snapshot_label("test_snapshot");
        req.set_request_ip("");
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }
}

// Full snapshot lifecycle: begin -> update -> commit -> list -> drop -> list
TEST(MetaServiceSnapshotTest, SnapshotLifecycleTest) {
    auto meta_service = get_meta_service(true);
    const char* const cloud_unique_id = "test_cloud_unique_id";

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
    DORIS_CLOUD_DEFER {
        sp->disable_processing();
        sp->clear_all_call_backs();
    };

    // Create test instance
    create_test_instance_via_rpc(meta_service.get(), "lifecycle_instance");

    std::string snapshot_id;

    // Step 1: Begin snapshot
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_timeout_seconds(3600);
        req.set_ttl_seconds(86400);
        req.set_snapshot_label("lifecycle_snapshot");
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_FALSE(res.snapshot_id().empty());
        ASSERT_FALSE(res.image_url().empty());
        snapshot_id = res.snapshot_id();
    }

    // Step 2: Update snapshot (simulate file upload metadata)
    {
        brpc::Controller cntl;
        UpdateSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_snapshot_id(snapshot_id);
        req.set_upload_file("data/snapshot_00001.dat");
        req.set_upload_id("upload-abc-123");
        UpdateSnapshotResponse res;
        meta_service->update_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // Step 3: Commit snapshot
    {
        brpc::Controller cntl;
        CommitSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_snapshot_id(snapshot_id);
        req.set_last_journal_id(1000);
        req.set_snapshot_meta_image_size(4096);
        req.set_snapshot_logical_data_size(1048576);
        CommitSnapshotResponse res;
        meta_service->commit_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // Step 4: List snapshots - should see the committed snapshot
    {
        brpc::Controller cntl;
        ListSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        ListSnapshotResponse res;
        meta_service->list_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_GE(res.snapshots_size(), 1);
        bool found = false;
        for (const auto& snap : res.snapshots()) {
            if (snap.snapshot_id() == snapshot_id) {
                found = true;
                ASSERT_EQ(snap.snapshot_label(), "lifecycle_snapshot");
                ASSERT_EQ(snap.journal_id(), 1000);
                ASSERT_EQ(snap.snapshot_meta_image_size(), 4096);
                ASSERT_EQ(snap.snapshot_logical_data_size(), 1048576);
                break;
            }
        }
        ASSERT_TRUE(found) << "committed snapshot not found in list";
    }

    // Step 5: Drop snapshot
    {
        brpc::Controller cntl;
        DropSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_snapshot_id(snapshot_id);
        DropSnapshotResponse res;
        meta_service->drop_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // Step 6: List snapshots - dropped snapshot should be filtered out
    {
        brpc::Controller cntl;
        ListSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        ListSnapshotResponse res;
        meta_service->list_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        for (const auto& snap : res.snapshots()) {
            ASSERT_NE(snap.snapshot_id(), snapshot_id)
                    << "dropped snapshot should not appear in list";
        }
    }
}

// Abort snapshot test
TEST(MetaServiceSnapshotTest, AbortSnapshotTest) {
    auto meta_service = get_meta_service(true);
    const char* const cloud_unique_id = "test_cloud_unique_id";

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
    DORIS_CLOUD_DEFER {
        sp->disable_processing();
        sp->clear_all_call_backs();
    };

    // Create test instance
    create_test_instance_via_rpc(meta_service.get(), "abort_instance");

    std::string snapshot_id;

    // Begin snapshot
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_timeout_seconds(3600);
        req.set_ttl_seconds(86400);
        req.set_snapshot_label("abort_test_snapshot");
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        snapshot_id = res.snapshot_id();
    }

    // Abort snapshot
    {
        brpc::Controller cntl;
        AbortSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_snapshot_id(snapshot_id);
        req.set_reason("test abort reason");
        AbortSnapshotResponse res;
        meta_service->abort_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // Abort again (idempotent) - should also succeed
    {
        brpc::Controller cntl;
        AbortSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_snapshot_id(snapshot_id);
        req.set_reason("test abort reason again");
        AbortSnapshotResponse res;
        meta_service->abort_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // Cannot commit an aborted snapshot
    {
        brpc::Controller cntl;
        CommitSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_snapshot_id(snapshot_id);
        CommitSnapshotResponse res;
        meta_service->commit_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_NE(res.status().code(), MetaServiceCode::OK);
    }

    // List - aborted snapshot should be filtered out
    {
        brpc::Controller cntl;
        ListSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        ListSnapshotResponse res;
        meta_service->list_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        for (const auto& snap : res.snapshots()) {
            ASSERT_NE(snap.snapshot_id(), snapshot_id)
                    << "aborted snapshot should not appear in list";
        }
    }
}

// Invalid snapshot_id tests for update/commit/abort/drop
TEST(MetaServiceSnapshotTest, InvalidSnapshotIdTest) {
    auto meta_service = get_meta_service(true);
    const char* const cloud_unique_id = "test_cloud_unique_id";

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
    DORIS_CLOUD_DEFER {
        sp->disable_processing();
        sp->clear_all_call_backs();
    };

    // Create test instance
    create_test_instance_via_rpc(meta_service.get(), "invalid_id_instance");

    // Update with invalid snapshot_id
    {
        brpc::Controller cntl;
        UpdateSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_snapshot_id("invalid_hex_id");
        UpdateSnapshotResponse res;
        meta_service->update_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_NE(res.status().code(), MetaServiceCode::OK);
    }

    // Commit with empty snapshot_id
    {
        brpc::Controller cntl;
        CommitSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_snapshot_id("");
        CommitSnapshotResponse res;
        meta_service->commit_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_NE(res.status().code(), MetaServiceCode::OK);
    }

    // Drop with non-existent snapshot_id (valid hex but no match)
    {
        brpc::Controller cntl;
        DropSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_snapshot_id("00000000000000000000"); // valid 20-char hex
        DropSnapshotResponse res;
        meta_service->drop_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_NE(res.status().code(), MetaServiceCode::OK);
    }
}

// List with required_snapshot_id
TEST(MetaServiceSnapshotTest, ListSpecificSnapshotTest) {
    auto meta_service = get_meta_service(true);
    const char* const cloud_unique_id = "test_cloud_unique_id";

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
    DORIS_CLOUD_DEFER {
        sp->disable_processing();
        sp->clear_all_call_backs();
    };

    // Create test instance
    create_test_instance_via_rpc(meta_service.get(), "list_specific_instance");

    std::string snapshot_id;

    // Begin and commit a snapshot
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_timeout_seconds(3600);
        req.set_ttl_seconds(86400);
        req.set_snapshot_label("specific_list_snapshot");
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        snapshot_id = res.snapshot_id();
    }
    {
        brpc::Controller cntl;
        CommitSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_snapshot_id(snapshot_id);
        req.set_last_journal_id(500);
        CommitSnapshotResponse res;
        meta_service->commit_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // List with required_snapshot_id - should return exactly one
    {
        brpc::Controller cntl;
        ListSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_required_snapshot_id(snapshot_id);
        ListSnapshotResponse res;
        meta_service->list_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(res.snapshots_size(), 1);
        ASSERT_EQ(res.snapshots(0).snapshot_id(), snapshot_id);
        ASSERT_EQ(res.snapshots(0).snapshot_label(), "specific_list_snapshot");
    }

    // List non-existent snapshot
    {
        brpc::Controller cntl;
        ListSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_required_snapshot_id("00000000000000000000");
        ListSnapshotResponse res;
        meta_service->list_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        // Should return INVALID_ARGUMENT since snapshot not found
        ASSERT_NE(res.status().code(), MetaServiceCode::OK);
    }
}

// ============================================================================
// Phase 2 Tests: DorisSnapshotManager Operational Methods
// ============================================================================

// Helper: write a snapshot KV entry directly into TxnKv
static void write_test_snapshot(TxnKv* txn_kv, const std::string& instance_id,
                                const Versionstamp& vs, const SnapshotPB& pb) {
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string key_prefix = versioned::snapshot_full_key({instance_id});
    std::string full_key = encode_versioned_key(key_prefix, vs);
    txn->put(full_key, pb.SerializeAsString());
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
}

// Helper: write instance info into TxnKv
static void write_test_instance(TxnKv* txn_kv, const InstanceInfoPB& instance) {
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string key = instance_key({instance.instance_id()});
    txn->put(key, instance.SerializeAsString());
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
}

// Helper: check if a snapshot key exists in TxnKv
static bool snapshot_key_exists(TxnKv* txn_kv, const std::string& instance_id,
                                const Versionstamp& vs) {
    std::unique_ptr<Transaction> txn;
    if (txn_kv->create_txn(&txn) != TxnErrorCode::TXN_OK) return false;
    std::string key_prefix = versioned::snapshot_full_key({instance_id});
    std::string full_key = encode_versioned_key(key_prefix, vs);
    std::string value;
    return txn->get(full_key, &value) == TxnErrorCode::TXN_OK;
}

// --- set_multi_version_status ---

TEST(DorisSnapshotManagerTest, SetMultiVersionStatusForwardTransition) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    // Write instance with DISABLED status
    InstanceInfoPB instance;
    instance.set_instance_id("mvs_test_instance");
    instance.set_user_id("test_user");
    instance.set_multi_version_status(MultiVersionStatus::MULTI_VERSION_DISABLED);
    write_test_instance(txn_kv.get(), instance);

    DorisSnapshotManager mgr(txn_kv);

    // DISABLED → WRITE_ONLY (forward)
    auto [code1, msg1] = mgr.set_multi_version_status("mvs_test_instance",
                                                      MultiVersionStatus::MULTI_VERSION_WRITE_ONLY);
    ASSERT_EQ(code1, MetaServiceCode::OK);

    // WRITE_ONLY → READ_WRITE (forward)
    auto [code2, msg2] = mgr.set_multi_version_status("mvs_test_instance",
                                                      MultiVersionStatus::MULTI_VERSION_READ_WRITE);
    ASSERT_EQ(code2, MetaServiceCode::OK);

    // READ_WRITE → ENABLED (forward)
    auto [code3, msg3] = mgr.set_multi_version_status("mvs_test_instance",
                                                      MultiVersionStatus::MULTI_VERSION_ENABLED);
    ASSERT_EQ(code3, MetaServiceCode::OK);
}

TEST(DorisSnapshotManagerTest, SetMultiVersionStatusNoChange) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    InstanceInfoPB instance;
    instance.set_instance_id("mvs_noop_instance");
    instance.set_user_id("test_user");
    instance.set_multi_version_status(MultiVersionStatus::MULTI_VERSION_WRITE_ONLY);
    write_test_instance(txn_kv.get(), instance);

    DorisSnapshotManager mgr(txn_kv);
    auto [code, msg] = mgr.set_multi_version_status("mvs_noop_instance",
                                                    MultiVersionStatus::MULTI_VERSION_WRITE_ONLY);
    ASSERT_EQ(code, MetaServiceCode::OK);
    ASSERT_EQ(msg, "no change");
}

TEST(DorisSnapshotManagerTest, SetMultiVersionStatusBackwardRejected) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    InstanceInfoPB instance;
    instance.set_instance_id("mvs_backward_instance");
    instance.set_user_id("test_user");
    instance.set_multi_version_status(MultiVersionStatus::MULTI_VERSION_ENABLED);
    write_test_instance(txn_kv.get(), instance);

    DorisSnapshotManager mgr(txn_kv);
    auto [code, msg] = mgr.set_multi_version_status("mvs_backward_instance",
                                                    MultiVersionStatus::MULTI_VERSION_WRITE_ONLY);
    ASSERT_EQ(code, MetaServiceCode::INVALID_ARGUMENT);
}

// --- recycle_snapshot_meta_and_data ---

TEST(DorisSnapshotManagerTest, RecycleSnapshotMetaAndData) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    std::string inst_id = "recycle_meta_data_instance";
    Versionstamp vs(100, 0);

    // Write a snapshot KV
    SnapshotPB pb;
    pb.set_status(SnapshotStatus::SNAPSHOT_RECYCLED);
    pb.set_image_url("snapshot/recycle_meta_data_instance/snap001/image/");
    pb.set_upload_file("data/part1.dat");
    pb.set_upload_id("upload-abc");
    write_test_snapshot(txn_kv.get(), inst_id, vs, pb);
    ASSERT_TRUE(snapshot_key_exists(txn_kv.get(), inst_id, vs));

    // Create a mock accessor
    auto accessor = std::make_shared<MockAccessor>();
    accessor->put_file(pb.image_url() + "file1.img", "");

    DorisSnapshotManager mgr(txn_kv);
    int ret = mgr.recycle_snapshot_meta_and_data(inst_id, "res1", accessor.get(), vs, pb);
    ASSERT_EQ(ret, 0);

    // Verify snapshot key is deleted
    ASSERT_FALSE(snapshot_key_exists(txn_kv.get(), inst_id, vs));
}

TEST(DorisSnapshotManagerTest, RecycleSnapshotMetaAndDataNullAccessor) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    std::string inst_id = "recycle_null_accessor";
    Versionstamp vs(200, 0);

    SnapshotPB pb;
    pb.set_status(SnapshotStatus::SNAPSHOT_ABORTED);
    write_test_snapshot(txn_kv.get(), inst_id, vs, pb);

    DorisSnapshotManager mgr(txn_kv);
    // null accessor should still succeed (skip object storage ops)
    int ret = mgr.recycle_snapshot_meta_and_data(inst_id, "", nullptr, vs, pb);
    ASSERT_EQ(ret, 0);
    ASSERT_FALSE(snapshot_key_exists(txn_kv.get(), inst_id, vs));
}

// --- check_snapshots ---

TEST(DorisSnapshotManagerTest, CheckSnapshotsEmpty) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    DorisSnapshotManager mgr(txn_kv);
    InstanceChecker checker(txn_kv, "check_empty_instance");
    auto mock_acc = std::make_shared<MockAccessor>();
    checker.TEST_add_accessor("res1", mock_acc);

    // No snapshots → return 0
    ASSERT_EQ(mgr.check_snapshots(&checker), 0);
}

TEST(DorisSnapshotManagerTest, CheckSnapshotsAllPresent) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    std::string inst_id = "check_present_instance";
    Versionstamp vs1(300, 0);
    Versionstamp vs2(301, 0);

    // Write two NORMAL snapshots with image URLs
    SnapshotPB pb1;
    pb1.set_status(SnapshotStatus::SNAPSHOT_NORMAL);
    pb1.set_image_url("snapshot/check_present_instance/snap1/image/");
    pb1.set_resource_id("res1");
    write_test_snapshot(txn_kv.get(), inst_id, vs1, pb1);

    SnapshotPB pb2;
    pb2.set_status(SnapshotStatus::SNAPSHOT_NORMAL);
    pb2.set_image_url("snapshot/check_present_instance/snap2/image/");
    pb2.set_resource_id("res1");
    write_test_snapshot(txn_kv.get(), inst_id, vs2, pb2);

    // Mock accessor with both files present
    auto mock_acc = std::make_shared<MockAccessor>();
    mock_acc->put_file(pb1.image_url(), "");
    mock_acc->put_file(pb2.image_url(), "");

    DorisSnapshotManager mgr(txn_kv);
    InstanceChecker checker(txn_kv, inst_id);
    checker.TEST_add_accessor("res1", mock_acc);

    ASSERT_EQ(mgr.check_snapshots(&checker), 0);
}

TEST(DorisSnapshotManagerTest, CheckSnapshotsMissingFile) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    std::string inst_id = "check_missing_instance";
    Versionstamp vs(400, 0);

    SnapshotPB pb;
    pb.set_status(SnapshotStatus::SNAPSHOT_NORMAL);
    pb.set_image_url("snapshot/check_missing_instance/snap1/image/");
    pb.set_resource_id("res1");
    write_test_snapshot(txn_kv.get(), inst_id, vs, pb);

    // Mock accessor WITHOUT the file
    auto mock_acc = std::make_shared<MockAccessor>();

    DorisSnapshotManager mgr(txn_kv);
    InstanceChecker checker(txn_kv, inst_id);
    checker.TEST_add_accessor("res1", mock_acc);

    // File missing → return 1 (data loss)
    ASSERT_EQ(mgr.check_snapshots(&checker), 1);
}

TEST(DorisSnapshotManagerTest, CheckSnapshotsSkipNonNormal) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    std::string inst_id = "check_skip_nonnormal";
    Versionstamp vs(500, 0);

    // ABORTED snapshot should be skipped
    SnapshotPB pb;
    pb.set_status(SnapshotStatus::SNAPSHOT_ABORTED);
    pb.set_image_url("snapshot/check_skip_nonnormal/snap1/image/");
    write_test_snapshot(txn_kv.get(), inst_id, vs, pb);

    auto mock_acc = std::make_shared<MockAccessor>();
    DorisSnapshotManager mgr(txn_kv);
    InstanceChecker checker(txn_kv, inst_id);
    checker.TEST_add_accessor("res1", mock_acc);

    // Non-NORMAL snapshots are skipped → return 0
    ASSERT_EQ(mgr.check_snapshots(&checker), 0);
}

// --- inverted_check_snapshots ---

TEST(DorisSnapshotManagerTest, InvertedCheckSnapshotsNoOrphan) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    std::string inst_id = "inv_check_clean";
    Versionstamp vs(600, 0);

    // Known snapshot
    SnapshotPB pb;
    pb.set_status(SnapshotStatus::SNAPSHOT_NORMAL);
    pb.set_image_url("snapshot/inv_check_clean/snap1/image/");
    write_test_snapshot(txn_kv.get(), inst_id, vs, pb);

    // Mock accessor lists exactly the known file
    auto mock_acc = std::make_shared<MockAccessor>();
    mock_acc->put_file("snapshot/inv_check_clean/snap1/image/", "");

    DorisSnapshotManager mgr(txn_kv);
    InstanceChecker checker(txn_kv, inst_id);
    checker.TEST_add_accessor("res1", mock_acc);

    ASSERT_EQ(mgr.inverted_check_snapshots(&checker), 0);
}

TEST(DorisSnapshotManagerTest, InvertedCheckSnapshotsOrphanDetected) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    std::string inst_id = "inv_check_orphan";

    // No snapshots in KV → any file in storage is orphan
    auto mock_acc = std::make_shared<MockAccessor>();
    mock_acc->put_file("snapshot/inv_check_orphan/leaked_file.img", "");

    DorisSnapshotManager mgr(txn_kv);
    InstanceChecker checker(txn_kv, inst_id);
    checker.TEST_add_accessor("res1", mock_acc);

    ASSERT_EQ(mgr.inverted_check_snapshots(&checker), 1);
}

// --- check_mvcc_meta_key ---

TEST(DorisSnapshotManagerTest, CheckMvccMetaKeyValid) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    std::string inst_id = "mvcc_valid_instance";
    Versionstamp vs(700, 0);

    SnapshotPB pb;
    pb.set_status(SnapshotStatus::SNAPSHOT_NORMAL);
    pb.set_image_url("test/image");
    write_test_snapshot(txn_kv.get(), inst_id, vs, pb);

    DorisSnapshotManager mgr(txn_kv);
    InstanceChecker checker(txn_kv, inst_id);

    ASSERT_EQ(mgr.check_mvcc_meta_key(&checker), 0);
}

TEST(DorisSnapshotManagerTest, CheckMvccMetaKeyEmpty) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    DorisSnapshotManager mgr(txn_kv);
    InstanceChecker checker(txn_kv, "mvcc_empty_instance");

    // No keys → return 0 (no issues)
    ASSERT_EQ(mgr.check_mvcc_meta_key(&checker), 0);
}

// --- inverted_check_mvcc_meta_key ---

TEST(DorisSnapshotManagerTest, InvertedCheckMvccMetaKeyValid) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    std::string inst_id = "inv_mvcc_valid";
    Versionstamp vs(800, 0);

    SnapshotPB pb;
    pb.set_status(SnapshotStatus::SNAPSHOT_NORMAL);
    write_test_snapshot(txn_kv.get(), inst_id, vs, pb);

    DorisSnapshotManager mgr(txn_kv);
    InstanceChecker checker(txn_kv, inst_id);

    ASSERT_EQ(mgr.inverted_check_mvcc_meta_key(&checker), 0);
}

// --- migrate_to_versioned_keys ---

TEST(DorisSnapshotManagerTest, MigrateToVersionedKeysDisabledSkip) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    InstanceInfoPB instance;
    instance.set_instance_id("migrate_disabled_instance");
    instance.set_multi_version_status(MultiVersionStatus::MULTI_VERSION_DISABLED);

    InstanceDataMigrator migrator(txn_kv, instance);
    DorisSnapshotManager mgr(txn_kv);

    // DISABLED → skip migration, return 0
    ASSERT_EQ(mgr.migrate_to_versioned_keys(&migrator), 0);
}

TEST(DorisSnapshotManagerTest, MigrateToVersionedKeysWithData) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    std::string inst_id = "migrate_data_instance";

    // Write non-versioned tablet keys
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        for (int i = 1; i <= 5; i++) {
            std::string key = meta_tablet_key({inst_id, /*table_id=*/100,
                                               /*index_id=*/200, /*partition_id=*/300,
                                               /*tablet_id=*/static_cast<int64_t>(1000 + i)});
            txn->put(key, "tablet_data_" + std::to_string(i));
        }
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Write non-versioned rowset keys
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        for (int i = 1; i <= 3; i++) {
            std::string key = meta_rowset_key({inst_id, /*tablet_id=*/1001,
                                               /*version=*/static_cast<int64_t>(i)});
            txn->put(key, "rowset_data_" + std::to_string(i));
        }
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    InstanceInfoPB instance;
    instance.set_instance_id(inst_id);
    instance.set_multi_version_status(MultiVersionStatus::MULTI_VERSION_WRITE_ONLY);

    InstanceDataMigrator migrator(txn_kv, instance);
    DorisSnapshotManager mgr(txn_kv);

    ASSERT_EQ(mgr.migrate_to_versioned_keys(&migrator), 0);
}

// --- compact_snapshot_chains ---

TEST(DorisSnapshotManagerTest, CompactSnapshotChainsNotCloned) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    // Instance without source_instance_id (not a clone)
    InstanceInfoPB instance;
    instance.set_instance_id("compact_not_cloned");

    InstanceChainCompactor compactor(txn_kv, instance);
    DorisSnapshotManager mgr(txn_kv);

    // Not cloned → skip, return 0
    ASSERT_EQ(mgr.compact_snapshot_chains(&compactor), 0);
}

TEST(DorisSnapshotManagerTest, CompactSnapshotChainsValidChain) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    std::string source_instance = "compact_source_instance";
    std::string clone_instance = "compact_clone_instance";
    Versionstamp snapshot_vs(900, 0);
    std::string snapshot_id = SnapshotManager::serialize_snapshot_id(snapshot_vs);

    // Write source snapshot (NORMAL)
    SnapshotPB source_pb;
    source_pb.set_status(SnapshotStatus::SNAPSHOT_NORMAL);
    source_pb.set_image_url("snapshot/source/snap1/image/");
    write_test_snapshot(txn_kv.get(), source_instance, snapshot_vs, source_pb);

    // Write reference key for the clone
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        versioned::SnapshotReferenceKeyInfo ref_info {source_instance, snapshot_vs, clone_instance};
        std::string ref_key = versioned::snapshot_reference_key(ref_info);
        txn->put(ref_key, "ref_value");
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Setup clone instance info
    InstanceInfoPB clone_info;
    clone_info.set_instance_id(clone_instance);
    clone_info.set_source_instance_id(source_instance);
    clone_info.set_source_snapshot_id(snapshot_id);

    InstanceChainCompactor compactor(txn_kv, clone_info);
    DorisSnapshotManager mgr(txn_kv);

    ASSERT_EQ(mgr.compact_snapshot_chains(&compactor), 0);
}

TEST(DorisSnapshotManagerTest, CompactSnapshotChainsInvalidSnapshotId) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    InstanceInfoPB clone_info;
    clone_info.set_instance_id("compact_bad_id");
    clone_info.set_source_instance_id("some_source");
    clone_info.set_source_snapshot_id("invalid_hex"); // not a valid 20-char hex

    InstanceChainCompactor compactor(txn_kv, clone_info);
    DorisSnapshotManager mgr(txn_kv);

    // Invalid snapshot_id → return -1
    ASSERT_EQ(mgr.compact_snapshot_chains(&compactor), -1);
}

TEST(DorisSnapshotManagerTest, CompactSnapshotChainsSourceNotFound) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    Versionstamp snapshot_vs(950, 0);
    std::string snapshot_id = SnapshotManager::serialize_snapshot_id(snapshot_vs);

    // Clone instance pointing to non-existent source snapshot
    InstanceInfoPB clone_info;
    clone_info.set_instance_id("compact_no_source");
    clone_info.set_source_instance_id("nonexistent_source");
    clone_info.set_source_snapshot_id(snapshot_id);

    InstanceChainCompactor compactor(txn_kv, clone_info);
    DorisSnapshotManager mgr(txn_kv);

    // Source snapshot not found → return -1
    ASSERT_EQ(mgr.compact_snapshot_chains(&compactor), -1);
}

// ============================================================================
// Phase 3 Tests: Clone Instance via RPC
// ============================================================================

TEST(MetaServiceSnapshotTest, CloneInstanceReadOnly) {
    auto meta_service = get_meta_service(true);
    const char* const cloud_unique_id = "test_cloud_unique_id";

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
    DORIS_CLOUD_DEFER {
        sp->disable_processing();
        sp->clear_all_call_backs();
    };

    // Create source instance
    create_test_instance_via_rpc(meta_service.get(), "clone_ro_source", "source_name");

    // Begin + commit a snapshot on source
    std::string snapshot_id;
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_timeout_seconds(3600);
        req.set_ttl_seconds(86400);
        req.set_snapshot_label("clone_ro_snap");
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        snapshot_id = res.snapshot_id();
    }
    {
        brpc::Controller cntl;
        CommitSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_snapshot_id(snapshot_id);
        req.set_last_journal_id(100);
        CommitSnapshotResponse res;
        meta_service->commit_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // Clone as READ_ONLY
    {
        brpc::Controller cntl;
        CloneInstanceRequest req;
        req.set_from_snapshot_id(snapshot_id);
        req.set_from_instance_id("clone_ro_source");
        req.set_new_instance_id("clone_ro_target");
        req.set_clone_type(CloneInstanceRequest::READ_ONLY);
        CloneInstanceResponse res;
        meta_service->clone_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_TRUE(res.has_image_url());
    }

    // Clone same target again should fail (instance already exists)
    {
        brpc::Controller cntl;
        CloneInstanceRequest req;
        req.set_from_snapshot_id(snapshot_id);
        req.set_from_instance_id("clone_ro_source");
        req.set_new_instance_id("clone_ro_target");
        req.set_clone_type(CloneInstanceRequest::READ_ONLY);
        CloneInstanceResponse res;
        meta_service->clone_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_NE(res.status().code(), MetaServiceCode::OK);
    }
}

TEST(MetaServiceSnapshotTest, CloneInstanceWritable) {
    auto meta_service = get_meta_service(true);
    const char* const cloud_unique_id = "test_cloud_unique_id";

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
    DORIS_CLOUD_DEFER {
        sp->disable_processing();
        sp->clear_all_call_backs();
    };

    // Create source instance
    {
        brpc::Controller cntl;
        CreateInstanceRequest req;
        req.set_instance_id("clone_wr_source");
        req.set_user_id("test_user");
        req.set_name("source_name");
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
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    std::string snapshot_id;
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_timeout_seconds(3600);
        req.set_ttl_seconds(86400);
        req.set_snapshot_label("clone_wr_snap");
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        snapshot_id = res.snapshot_id();
    }
    {
        brpc::Controller cntl;
        CommitSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_snapshot_id(snapshot_id);
        req.set_last_journal_id(200);
        CommitSnapshotResponse res;
        meta_service->commit_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // Clone as WRITABLE
    {
        brpc::Controller cntl;
        CloneInstanceRequest req;
        req.set_from_snapshot_id(snapshot_id);
        req.set_from_instance_id("clone_wr_source");
        req.set_new_instance_id("clone_wr_target");
        req.set_clone_type(CloneInstanceRequest::WRITABLE);
        // Provide new obj_info for writable clone
        ObjectStoreInfoPB obj;
        obj.set_ak("new_ak");
        obj.set_sk("new_sk");
        obj.set_bucket("new_bucket");
        obj.set_prefix("new_prefix");
        obj.set_endpoint("new_endpoint");
        obj.set_region("new_region");
        obj.set_provider(ObjectStoreInfoPB::BOS);
        req.mutable_obj_info()->CopyFrom(obj);
        CloneInstanceResponse res;
        meta_service->clone_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_TRUE(res.has_image_url());
    }
}

TEST(MetaServiceSnapshotTest, CloneInstanceInvalidArgs) {
    auto meta_service = get_meta_service(true);

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
    DORIS_CLOUD_DEFER {
        sp->disable_processing();
        sp->clear_all_call_backs();
    };

    // Missing clone_type
    {
        brpc::Controller cntl;
        CloneInstanceRequest req;
        req.set_from_snapshot_id("00000000000000000000");
        req.set_from_instance_id("some_source");
        req.set_new_instance_id("some_target");
        CloneInstanceResponse res;
        meta_service->clone_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_NE(res.status().code(), MetaServiceCode::OK);
    }

    // UNKNOWN clone_type
    {
        brpc::Controller cntl;
        CloneInstanceRequest req;
        req.set_from_snapshot_id("00000000000000000000");
        req.set_from_instance_id("some_source");
        req.set_new_instance_id("some_target");
        req.set_clone_type(CloneInstanceRequest::UNKNOWN);
        CloneInstanceResponse res;
        meta_service->clone_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_NE(res.status().code(), MetaServiceCode::OK);
    }

    // Missing from_instance_id
    {
        brpc::Controller cntl;
        CloneInstanceRequest req;
        req.set_from_snapshot_id("00000000000000000000");
        req.set_new_instance_id("some_target");
        req.set_clone_type(CloneInstanceRequest::READ_ONLY);
        CloneInstanceResponse res;
        meta_service->clone_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_NE(res.status().code(), MetaServiceCode::OK);
    }

    // Missing new_instance_id
    {
        brpc::Controller cntl;
        CloneInstanceRequest req;
        req.set_from_snapshot_id("00000000000000000000");
        req.set_from_instance_id("some_source");
        req.set_clone_type(CloneInstanceRequest::READ_ONLY);
        CloneInstanceResponse res;
        meta_service->clone_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_NE(res.status().code(), MetaServiceCode::OK);
    }
}

// ============================================================================
// Phase 4 Tests: Recycle Snapshots
// ============================================================================

TEST(DorisSnapshotManagerTest, RecycleSnapshotsPrepareTimeout) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    std::string inst_id = "recycle_timeout_instance";
    Versionstamp vs(1100, 0);

    // Write a PREPARE snapshot that is already timed out
    SnapshotPB pb;
    pb.set_status(SnapshotStatus::SNAPSHOT_PREPARE);
    pb.set_create_at(1000);     // created at t=1000
    pb.set_timeout_seconds(60); // timeout after 60s
    pb.set_image_url("snapshot/recycle_timeout/snap1/image/");
    write_test_snapshot(txn_kv.get(), inst_id, vs, pb);

    // Create instance and recycler
    InstanceInfoPB instance;
    instance.set_instance_id(inst_id);
    auto obj_info = instance.add_obj_info();
    obj_info->set_id("res1");
    write_test_instance(txn_kv.get(), instance);

    RecyclerThreadPoolGroup thread_group;
    InstanceRecycler recycler(txn_kv, instance, thread_group,
                              std::make_shared<TxnLazyCommitter>(txn_kv));
    ASSERT_EQ(recycler.init(), 0);

    DorisSnapshotManager mgr(txn_kv);
    int ret = mgr.recycle_snapshots(&recycler);
    // Should succeed - the timed-out PREPARE snapshot gets marked ABORTED and recycled
    ASSERT_EQ(ret, 0);

    // Verify the snapshot key is cleaned up
    ASSERT_FALSE(snapshot_key_exists(txn_kv.get(), inst_id, vs));
}

TEST(DorisSnapshotManagerTest, RecycleSnapshotsTTLExpired) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    std::string inst_id = "recycle_ttl_instance";
    Versionstamp vs(1200, 0);

    // Write a NORMAL snapshot whose TTL has expired
    SnapshotPB pb;
    pb.set_status(SnapshotStatus::SNAPSHOT_NORMAL);
    pb.set_create_at(1000); // created at t=1000
    pb.set_ttl_seconds(60); // TTL of 60s (long expired)
    pb.set_image_url("snapshot/recycle_ttl/snap1/image/");
    pb.set_resource_id("res1");
    write_test_snapshot(txn_kv.get(), inst_id, vs, pb);

    InstanceInfoPB instance;
    instance.set_instance_id(inst_id);
    auto obj_info = instance.add_obj_info();
    obj_info->set_id("res1");
    write_test_instance(txn_kv.get(), instance);

    RecyclerThreadPoolGroup thread_group;
    InstanceRecycler recycler(txn_kv, instance, thread_group,
                              std::make_shared<TxnLazyCommitter>(txn_kv));
    ASSERT_EQ(recycler.init(), 0);

    DorisSnapshotManager mgr(txn_kv);
    ASSERT_EQ(mgr.recycle_snapshots(&recycler), 0);

    // Verify the TTL-expired snapshot is cleaned up
    ASSERT_FALSE(snapshot_key_exists(txn_kv.get(), inst_id, vs));
}

TEST(DorisSnapshotManagerTest, RecycleSnapshotsMaxReserved) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    std::string inst_id = "recycle_maxres_instance";
    int64_t now = std::chrono::duration_cast<std::chrono::seconds>(
                          std::chrono::system_clock::now().time_since_epoch())
                          .count();

    // Write 5 NORMAL snapshots with no TTL
    std::vector<Versionstamp> vss;
    for (int i = 0; i < 5; i++) {
        Versionstamp vs(1300 + i, 0);
        vss.push_back(vs);
        SnapshotPB pb;
        pb.set_status(SnapshotStatus::SNAPSHOT_NORMAL);
        pb.set_create_at(now - 3600 + i * 100); // staggered creation times
        pb.set_image_url("snapshot/recycle_maxres/snap" + std::to_string(i) + "/image/");
        pb.set_resource_id("res1");
        write_test_snapshot(txn_kv.get(), inst_id, vs, pb);
    }

    // Set max_reserved to 2 via config
    auto old_max = config::snapshot_max_reserved_num;
    config::snapshot_max_reserved_num = 2;
    DORIS_CLOUD_DEFER {
        config::snapshot_max_reserved_num = old_max;
    };

    InstanceInfoPB instance;
    instance.set_instance_id(inst_id);
    auto obj_info = instance.add_obj_info();
    obj_info->set_id("res1");
    write_test_instance(txn_kv.get(), instance);

    RecyclerThreadPoolGroup thread_group;
    InstanceRecycler recycler(txn_kv, instance, thread_group,
                              std::make_shared<TxnLazyCommitter>(txn_kv));
    ASSERT_EQ(recycler.init(), 0);

    DorisSnapshotManager mgr(txn_kv);
    ASSERT_EQ(mgr.recycle_snapshots(&recycler), 0);

    // 3 oldest should be removed, 2 newest should remain
    int remaining = 0;
    for (auto& vs : vss) {
        if (snapshot_key_exists(txn_kv.get(), inst_id, vs)) {
            remaining++;
        }
    }
    ASSERT_EQ(remaining, 2) << "max_reserved=2, should keep only 2 newest snapshots";
}

TEST(DorisSnapshotManagerTest, RecycleSnapshotsEmpty) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    std::string inst_id = "recycle_empty_instance";
    InstanceInfoPB instance;
    instance.set_instance_id(inst_id);
    write_test_instance(txn_kv.get(), instance);

    RecyclerThreadPoolGroup thread_group;
    InstanceRecycler recycler(txn_kv, instance, thread_group,
                              std::make_shared<TxnLazyCommitter>(txn_kv));
    ASSERT_EQ(recycler.init(), 0);

    DorisSnapshotManager mgr(txn_kv);
    // No snapshots → return 0
    ASSERT_EQ(mgr.recycle_snapshots(&recycler), 0);
}

TEST(DorisSnapshotManagerTest, RecycleSnapshotsKeepsActiveNormal) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    std::string inst_id = "recycle_keep_active";
    int64_t now = std::chrono::duration_cast<std::chrono::seconds>(
                          std::chrono::system_clock::now().time_since_epoch())
                          .count();
    Versionstamp vs(1400, 0);

    // NORMAL snapshot with long TTL (not expired)
    SnapshotPB pb;
    pb.set_status(SnapshotStatus::SNAPSHOT_NORMAL);
    pb.set_create_at(now);
    pb.set_ttl_seconds(999999); // not expired
    pb.set_image_url("snapshot/keep_active/snap1/image/");
    write_test_snapshot(txn_kv.get(), inst_id, vs, pb);

    // High max_reserved
    auto old_max = config::snapshot_max_reserved_num;
    config::snapshot_max_reserved_num = 100;
    DORIS_CLOUD_DEFER {
        config::snapshot_max_reserved_num = old_max;
    };

    InstanceInfoPB instance;
    instance.set_instance_id(inst_id);
    write_test_instance(txn_kv.get(), instance);

    RecyclerThreadPoolGroup thread_group;
    InstanceRecycler recycler(txn_kv, instance, thread_group,
                              std::make_shared<TxnLazyCommitter>(txn_kv));
    ASSERT_EQ(recycler.init(), 0);

    DorisSnapshotManager mgr(txn_kv);
    ASSERT_EQ(mgr.recycle_snapshots(&recycler), 0);

    // Active snapshot should still exist
    ASSERT_TRUE(snapshot_key_exists(txn_kv.get(), inst_id, vs));
}

// ============================================================================
// Phase 5 Tests: Dedicated Update/Commit/Drop tests
// ============================================================================

TEST(MetaServiceSnapshotTest, UpdateSnapshotTest) {
    auto meta_service = get_meta_service(true);
    const char* const cloud_unique_id = "test_cloud_unique_id";

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
    DORIS_CLOUD_DEFER {
        sp->disable_processing();
        sp->clear_all_call_backs();
    };

    // Create test instance
    {
        brpc::Controller cntl;
        CreateInstanceRequest req;
        req.set_instance_id("update_test_instance");
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
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    std::string snapshot_id;
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_timeout_seconds(3600);
        req.set_ttl_seconds(86400);
        req.set_snapshot_label("update_test_snap");
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        snapshot_id = res.snapshot_id();
    }

    // Update snapshot
    {
        brpc::Controller cntl;
        UpdateSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_snapshot_id(snapshot_id);
        req.set_upload_file("image_20260101.dat");
        req.set_upload_id("upload-xyz-789");
        UpdateSnapshotResponse res;
        meta_service->update_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // Update again (should overwrite)
    {
        brpc::Controller cntl;
        UpdateSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_snapshot_id(snapshot_id);
        req.set_upload_file("image_20260102.dat");
        req.set_upload_id("upload-xyz-790");
        UpdateSnapshotResponse res;
        meta_service->update_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // Cannot update a non-existent snapshot
    {
        brpc::Controller cntl;
        UpdateSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_snapshot_id("00000000000000000000");
        req.set_upload_file("file.dat");
        UpdateSnapshotResponse res;
        meta_service->update_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_NE(res.status().code(), MetaServiceCode::OK);
    }
}

TEST(MetaServiceSnapshotTest, CommitSnapshotStateValidation) {
    auto meta_service = get_meta_service(true);
    const char* const cloud_unique_id = "test_cloud_unique_id";

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
    DORIS_CLOUD_DEFER {
        sp->disable_processing();
        sp->clear_all_call_backs();
    };

    {
        brpc::Controller cntl;
        CreateInstanceRequest req;
        req.set_instance_id("commit_state_instance");
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
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    std::string snapshot_id;
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_timeout_seconds(3600);
        req.set_ttl_seconds(86400);
        req.set_snapshot_label("commit_state_snap");
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        snapshot_id = res.snapshot_id();
    }

    // Commit with metadata
    {
        brpc::Controller cntl;
        CommitSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_snapshot_id(snapshot_id);
        req.set_last_journal_id(5000);
        req.set_snapshot_meta_image_size(8192);
        req.set_snapshot_logical_data_size(2097152);
        CommitSnapshotResponse res;
        meta_service->commit_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // Double commit should fail (already NORMAL)
    {
        brpc::Controller cntl;
        CommitSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_snapshot_id(snapshot_id);
        CommitSnapshotResponse res;
        meta_service->commit_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_NE(res.status().code(), MetaServiceCode::OK);
    }

    // Verify via list
    {
        brpc::Controller cntl;
        ListSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_required_snapshot_id(snapshot_id);
        ListSnapshotResponse res;
        meta_service->list_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(res.snapshots_size(), 1);
        ASSERT_EQ(res.snapshots(0).journal_id(), 5000);
        ASSERT_EQ(res.snapshots(0).snapshot_meta_image_size(), 8192);
        ASSERT_EQ(res.snapshots(0).snapshot_logical_data_size(), 2097152);
    }
}

TEST(MetaServiceSnapshotTest, DropSnapshotTest) {
    auto meta_service = get_meta_service(true);
    const char* const cloud_unique_id = "test_cloud_unique_id";

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
    DORIS_CLOUD_DEFER {
        sp->disable_processing();
        sp->clear_all_call_backs();
    };

    {
        brpc::Controller cntl;
        CreateInstanceRequest req;
        req.set_instance_id("drop_test_instance");
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
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    std::string snapshot_id;
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_timeout_seconds(3600);
        req.set_ttl_seconds(86400);
        req.set_snapshot_label("drop_test_snap");
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        snapshot_id = res.snapshot_id();
    }
    {
        brpc::Controller cntl;
        CommitSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_snapshot_id(snapshot_id);
        req.set_last_journal_id(300);
        CommitSnapshotResponse res;
        meta_service->commit_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // Drop the snapshot
    {
        brpc::Controller cntl;
        DropSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_snapshot_id(snapshot_id);
        DropSnapshotResponse res;
        meta_service->drop_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // Drop again (idempotent — already in non-NORMAL state)
    {
        brpc::Controller cntl;
        DropSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_snapshot_id(snapshot_id);
        DropSnapshotResponse res;
        meta_service->drop_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        // Either OK (idempotent) or error — depends on implementation
    }

    // List should not show dropped snapshot
    {
        brpc::Controller cntl;
        ListSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        ListSnapshotResponse res;
        meta_service->list_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        for (const auto& snap : res.snapshots()) {
            ASSERT_NE(snap.snapshot_id(), snapshot_id)
                    << "dropped snapshot should not appear in list";
        }
    }
}

} // namespace doris::cloud