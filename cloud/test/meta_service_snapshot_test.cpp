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
#include <tuple>

#include "common/defer.h"
#include "cpp/sync_point.h"
#include "meta-service/meta_service.h"
#include "meta-store/keys.h"
#include "meta-store/mem_txn_kv.h"
#include "rate-limiter/rate_limiter.h"
#include "resource-manager/resource_manager.h"
#include "snapshot/snapshot_manager.h"

namespace doris::cloud {

extern std::unique_ptr<MetaServiceProxy> get_meta_service(bool mock_resource_mgr);

TEST(MetaServiceSnapshotTest, CheckInstanceRecycleCompletedForInstanceChain) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);
    auto resource_mgr = std::make_shared<ResourceManager>(txn_kv);
    auto rate_limiter = std::make_shared<RateLimiter>();
    auto snapshot_manager = std::make_shared<SnapshotManager>(txn_kv);
    MetaServiceImpl meta_service(txn_kv, resource_mgr, rate_limiter, snapshot_manager);

    InstanceInfoPB original_instance;
    original_instance.set_instance_id("original_instance");
    original_instance.set_status(InstanceInfoPB::DELETED);
    original_instance.set_successor_instance_id("middle_instance");
    original_instance.set_recycled_state(
            InstanceRecycleState::INSTANCE_RECYCLE_STATE_DATA_CLEANUP_COMPLETED);

    InstanceInfoPB middle_instance;
    middle_instance.set_instance_id("middle_instance");
    middle_instance.set_status(InstanceInfoPB::DELETED);
    middle_instance.set_original_instance_id(original_instance.instance_id());
    middle_instance.set_successor_instance_id("tail_instance");
    middle_instance.set_recycled_state(
            InstanceRecycleState::INSTANCE_RECYCLE_STATE_DATA_CLEANUP_COMPLETED);

    InstanceInfoPB tail_instance;
    tail_instance.set_instance_id("tail_instance");
    tail_instance.set_status(InstanceInfoPB::DELETED);
    tail_instance.set_original_instance_id(original_instance.instance_id());
    tail_instance.set_recycled_state(
            InstanceRecycleState::INSTANCE_RECYCLE_STATE_DATA_CLEANUP_COMPLETED);

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->put(instance_key({original_instance.instance_id()}),
             original_instance.SerializeAsString());
    txn->put(instance_key({middle_instance.instance_id()}), middle_instance.SerializeAsString());
    txn->put(instance_key({tail_instance.instance_id()}), tail_instance.SerializeAsString());
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    bool finished = true;
    std::string reason;
    auto [code, msg] = meta_service.check_instance_recycle_completed(tail_instance.instance_id(),
                                                                     finished, reason);
    ASSERT_EQ(code, MetaServiceCode::OK) << msg;
    ASSERT_FALSE(finished);
    ASSERT_EQ(reason,
              "instance has not completed recycling, instance_id=tail_instance, "
              "recycled_state=INSTANCE_RECYCLE_STATE_DATA_CLEANUP_COMPLETED");

    tail_instance.set_recycled_state(
            InstanceRecycleState::INSTANCE_RECYCLE_STATE_CLEANUP_COMPLETED);
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->put(instance_key({tail_instance.instance_id()}), tail_instance.SerializeAsString());
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    std::tie(code, msg) = meta_service.check_instance_recycle_completed(tail_instance.instance_id(),
                                                                        finished, reason);
    ASSERT_EQ(code, MetaServiceCode::OK) << msg;
    ASSERT_FALSE(finished);
    ASSERT_EQ(reason,
              "referenced instance has not completed recycling, "
              "predecessor_instance_id=original_instance, "
              "recycled_state=INSTANCE_RECYCLE_STATE_DATA_CLEANUP_COMPLETED");

    original_instance.set_recycled_state(
            InstanceRecycleState::INSTANCE_RECYCLE_STATE_CLEANUP_COMPLETED);
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->put(instance_key({original_instance.instance_id()}),
             original_instance.SerializeAsString());
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    std::tie(code, msg) = meta_service.check_instance_recycle_completed(tail_instance.instance_id(),
                                                                        finished, reason);
    ASSERT_EQ(code, MetaServiceCode::OK) << msg;
    ASSERT_FALSE(finished);
    ASSERT_EQ(reason,
              "referenced instance has not completed recycling, "
              "predecessor_instance_id=middle_instance, "
              "recycled_state=INSTANCE_RECYCLE_STATE_DATA_CLEANUP_COMPLETED");

    middle_instance.set_recycled_state(
            InstanceRecycleState::INSTANCE_RECYCLE_STATE_CLEANUP_COMPLETED);
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->put(instance_key({middle_instance.instance_id()}), middle_instance.SerializeAsString());
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    std::tie(code, msg) = meta_service.check_instance_recycle_completed(tail_instance.instance_id(),
                                                                        finished, reason);
    ASSERT_EQ(code, MetaServiceCode::OK) << msg;
    ASSERT_TRUE(finished);
    ASSERT_EQ(reason,
              "instance recycling has completed, instance_id=tail_instance, "
              "recycled_state=INSTANCE_RECYCLE_STATE_CLEANUP_COMPLETED");

    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->remove(instance_key({tail_instance.instance_id()}));
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    finished = false;
    std::tie(code, msg) = meta_service.check_instance_recycle_completed(tail_instance.instance_id(),
                                                                        finished, reason);
    ASSERT_EQ(code, MetaServiceCode::OK) << msg;
    ASSERT_TRUE(finished);
    ASSERT_EQ(reason,
              "instance recycling is considered completed because the instance key does not "
              "exist, instance_id=tail_instance");
}

TEST(MetaServiceSnapshotTest, DISABLED_BeginSnapshotTest) {
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

        CreateInstanceResponse res;
        meta_service->create_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

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
} // namespace doris::cloud
