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
#include "meta-service/meta_service.h"
//#undef private

#include <brpc/controller.h>
#include <bvar/window.h>
#include <gen_cpp/cloud.pb.h>
#include <gen_cpp/olap_file.pb.h>
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
#include "meta-service/txn_kv_error.h"
#include "rate-limiter/rate_limiter.h"
#include "resource-manager/resource_manager.h"

int main(int argc, char** argv) {
    const std::string conf_file = "doris_cloud.conf";
    if (!doris::cloud::config::init(conf_file.c_str(), true)) {
        std::cerr << "failed to init config file, conf=" << conf_file << std::endl;
        return -1;
    }

    if (!doris::cloud::init_glog("resource_test")) {
        std::cerr << "failed to init glog" << std::endl;
        return -1;
    }

    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace doris::cloud {

static std::shared_ptr<TxnKv> create_txn_kv() {
    // MemKv
    int ret = 0;
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
    return txn_kv;
}

std::unique_ptr<MetaServiceProxy> get_meta_service(std::shared_ptr<TxnKv> txn_kv = {}) {
    if (!txn_kv) {
        txn_kv = create_txn_kv();
    }

    auto rs = std::make_shared<ResourceManager>(txn_kv);
    EXPECT_EQ(rs->init(), 0);
    auto rl = std::make_shared<RateLimiter>();
    auto meta_service = std::make_unique<MetaServiceImpl>(txn_kv, rs, rl);
    return std::make_unique<MetaServiceProxy>(std::move(meta_service));
}

static void create_args_to_add(std::vector<NodeInfo>* to_add, std::vector<NodeInfo>* to_del,
                               bool is_host = false) {
    to_add->clear();
    to_del->clear();
    auto ni_1 = NodeInfo {.role = Role::COMPUTE_NODE,
                          .instance_id = "test-resource-instance",
                          .cluster_name = "cluster_name_1",
                          .cluster_id = "cluster_id_1",
                          .node_info = NodeInfoPB {}};
    is_host ? ni_1.node_info.set_host("host1") : ni_1.node_info.set_ip("127.0.0.1");
    ni_1.node_info.set_cloud_unique_id("test_cloud_unique_id_1");
    ni_1.node_info.set_heartbeat_port(9999);
    to_add->push_back(ni_1);

    auto ni_2 = NodeInfo {.role = Role::COMPUTE_NODE,
                          .instance_id = "test-resource-instance",
                          .cluster_name = "cluster_name_1",
                          .cluster_id = "cluster_id_1",
                          .node_info = NodeInfoPB {}};
    is_host ? ni_2.node_info.set_host("host2") : ni_2.node_info.set_ip("127.0.0.2");
    ni_2.node_info.set_cloud_unique_id("test_cloud_unique_id_1");
    ni_2.node_info.set_heartbeat_port(9999);
    to_add->push_back(ni_2);

    auto ni_3 = NodeInfo {.role = Role::COMPUTE_NODE,
                          .instance_id = "test-resource-instance",
                          .cluster_name = "cluster_name_2",
                          .cluster_id = "cluster_id_2",
                          .node_info = NodeInfoPB {}};
    is_host ? ni_3.node_info.set_host("host3") : ni_3.node_info.set_ip("127.0.0.3");
    ni_3.node_info.set_cloud_unique_id("test_cloud_unique_id_2");
    ni_3.node_info.set_heartbeat_port(9999);
    to_add->push_back(ni_3);
}

static void create_args_to_del(std::vector<NodeInfo>* to_add, std::vector<NodeInfo>* to_del,
                               bool is_host = false) {
    to_add->clear();
    to_del->clear();
    auto ni_1 = NodeInfo {.role = Role::COMPUTE_NODE,
                          .instance_id = "test-resource-instance",
                          .cluster_name = "cluster_name_1",
                          .cluster_id = "cluster_id_1",
                          .node_info = NodeInfoPB {}};
    is_host ? ni_1.node_info.set_host("host2") : ni_1.node_info.set_ip("127.0.0.2");
    ni_1.node_info.set_cloud_unique_id("test_cloud_unique_id_1");
    ni_1.node_info.set_heartbeat_port(9999);
    to_del->push_back(ni_1);
}

static void get_instance_info(MetaServiceProxy* ms, InstanceInfoPB* instance,
                              std::string_view instance_id = "test-resource-instance") {
    InstanceKeyInfo key_info {instance_id};
    std::string key;
    std::string val;
    instance_key(key_info, &key);
    std::unique_ptr<Transaction> txn;
    EXPECT_EQ(ms->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    EXPECT_EQ(txn->get(key, &val), TxnErrorCode::TXN_OK);
    instance->ParseFromString(val);
}

static void create_instance(MetaServiceProxy* ms, const std::string& instance_id) {
    brpc::Controller ctrl;
    CreateInstanceRequest req;
    req.set_instance_id(instance_id);
    req.set_user_id("test_user");
    req.set_name(instance_id + "name");
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
    CreateInstanceResponse resp;
    ms->create_instance(&ctrl, &req, &resp, brpc::DoNothing());
    ASSERT_FALSE(ctrl.Failed());
    ASSERT_EQ(resp.status().code(), MetaServiceCode::OK);
}

static void create_cluster(MetaServiceProxy* ms, const std::string& instance_id,
                           const std::string& cluster_id, const std::string& cluster_name,
                           ClusterPB_Type type) {
    brpc::Controller cntl;
    AlterClusterRequest req;
    req.set_instance_id(instance_id);
    req.mutable_cluster()->set_cluster_id(cluster_id);
    req.mutable_cluster()->set_cluster_name(cluster_name);
    req.mutable_cluster()->set_type(type);
    if (type == ClusterPB::SQL) {
        auto* node = req.mutable_cluster()->add_nodes();
        node->set_node_type(NodeInfoPB::FE_MASTER);
        node->set_ip("127.0.0.1");
        node->set_edit_log_port(10000);
        node->set_name("sql_node");
    } else {
        auto* node = req.mutable_cluster()->add_nodes();
        node->set_ip("127.0.0.1");
        node->set_heartbeat_port(10000);
        node->set_name("sql_node");
    }
    req.set_op(AlterClusterRequest::ADD_CLUSTER);
    AlterClusterResponse res;
    ms->alter_cluster(&cntl, &req, &res, brpc::DoNothing());
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
}

static void drop_cluster(MetaServiceProxy* ms, const std::string& instance_id,
                         const std::string& cluster_id) {
    brpc::Controller cntl;
    AlterClusterRequest req;
    req.set_instance_id(instance_id);
    req.mutable_cluster()->set_cluster_id(cluster_id);
    req.set_op(AlterClusterRequest::DROP_CLUSTER);
    AlterClusterResponse res;
    ms->alter_cluster(&cntl, &req, &res, brpc::DoNothing());
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
}

// test cluster's node addr use ip
TEST(ResourceTest, ModifyNodesIpTest) {
    auto meta_service = get_meta_service();
    std::vector<NodeInfo> to_add = {};
    std::vector<NodeInfo> to_del = {};
    auto ins = InstanceInfoPB {};
    create_args_to_add(&to_add, &to_del);
    auto sp = SyncPoint::get_instance();
    sp->set_call_back("modify_nodes:get_instance", [&](auto&& args) {
        *try_any_cast<TxnErrorCode*>(args[0]) = TxnErrorCode::TXN_OK;
        ins.set_instance_id("test-resource-instance");
        ins.set_status(InstanceInfoPB::NORMAL);
        auto* c = ins.mutable_clusters()->Add();
        c->set_cluster_name("cluster_name_1");
        c->set_cluster_id("cluster_id_1");
        auto* c1 = ins.mutable_clusters()->Add();
        c1->set_cluster_name("cluster_name_2");
        c1->set_cluster_id("cluster_id_2");
        *try_any_cast<InstanceInfoPB*>(args[1]) = ins;
    });
    sp->enable_processing();

    // test cluster add nodes
    auto r = meta_service->resource_mgr()->modify_nodes("test-resource-instance", to_add, to_del);
    ASSERT_EQ(r, "");
    InstanceInfoPB instance;
    get_instance_info(meta_service.get(), &instance);
    std::cout << "after to add = " << proto_to_json(instance) << std::endl;
    ASSERT_EQ(instance.clusters().size(), 2);
    // after add assert cluster_name_1 has 2 nodes
    ASSERT_EQ(instance.clusters(0).nodes().size(), 2);
    // after add assert cluster_name_2 has 1 nodes
    ASSERT_EQ(instance.clusters(1).nodes().size(), 1);
    sp->clear_all_call_backs();
    sp->clear_trace();
    sp->disable_processing();

    sp->set_call_back("modify_nodes:get_instance", [&](auto&& args) {
        *try_any_cast<TxnErrorCode*>(args[0]) = TxnErrorCode::TXN_OK;
        *try_any_cast<InstanceInfoPB*>(args[1]) = instance;
    });
    sp->enable_processing();
    create_args_to_del(&to_add, &to_del);
    // test cluster del node
    r = meta_service->resource_mgr()->modify_nodes("test-resource-instance", to_add, to_del);
    InstanceInfoPB instance1;
    get_instance_info(meta_service.get(), &instance1);
    ASSERT_EQ(r, "");
    std::cout << "after to del = " << proto_to_json(instance1) << std::endl;
    ASSERT_EQ(instance1.clusters().size(), 2);
    // after del assert cluster_name_1 has 1 nodes
    ASSERT_EQ(instance1.clusters(0).nodes().size(), 1);
    // after del assert cluster_name_2 has 1 nodes
    ASSERT_EQ(instance1.clusters(1).nodes().size(), 1);
    sp->clear_all_call_backs();
    sp->clear_trace();
    sp->disable_processing();
}

// test cluster's node addr use host
TEST(ResourceTest, ModifyNodesHostTest) {
    auto meta_service = get_meta_service();
    std::vector<NodeInfo> to_add = {};
    std::vector<NodeInfo> to_del = {};
    auto ins = InstanceInfoPB {};
    create_args_to_add(&to_add, &to_del, true);
    auto sp = SyncPoint::get_instance();
    sp->set_call_back("modify_nodes:get_instance", [&](auto&& args) {
        *try_any_cast<TxnErrorCode*>(args[0]) = TxnErrorCode::TXN_OK;
        ins.set_instance_id("test-resource-instance");
        ins.set_status(InstanceInfoPB::NORMAL);
        auto* c = ins.mutable_clusters()->Add();
        c->set_cluster_name("cluster_name_1");
        c->set_cluster_id("cluster_id_1");
        auto* c1 = ins.mutable_clusters()->Add();
        c1->set_cluster_name("cluster_name_2");
        c1->set_cluster_id("cluster_id_2");
        *try_any_cast<InstanceInfoPB*>(args[1]) = ins;
    });
    sp->enable_processing();

    // test cluster add nodes
    auto r = meta_service->resource_mgr()->modify_nodes("test-resource-instance", to_add, to_del);
    ASSERT_EQ(r, "");
    InstanceInfoPB instance;
    get_instance_info(meta_service.get(), &instance);
    std::cout << "after to add = " << proto_to_json(instance) << std::endl;
    ASSERT_EQ(instance.clusters().size(), 2);
    // after add assert cluster_name_1 has 2 nodes
    ASSERT_EQ(instance.clusters(0).nodes().size(), 2);
    // after add assert cluster_name_2 has 1 nodes
    ASSERT_EQ(instance.clusters(1).nodes().size(), 1);
    sp->clear_all_call_backs();
    sp->clear_trace();
    sp->disable_processing();

    sp->set_call_back("modify_nodes:get_instance", [&](auto&& args) {
        *try_any_cast<TxnErrorCode*>(args[0]) = TxnErrorCode::TXN_OK;
        *try_any_cast<InstanceInfoPB*>(args[1]) = instance;
    });
    sp->enable_processing();
    create_args_to_del(&to_add, &to_del, true);
    r = meta_service->resource_mgr()->modify_nodes("test-resource-instance", to_add, to_del);
    InstanceInfoPB instance1;
    get_instance_info(meta_service.get(), &instance1);
    ASSERT_EQ(r, "");
    std::cout << "after to del = " << proto_to_json(instance1) << std::endl;
    ASSERT_EQ(instance1.clusters().size(), 2);
    // after del assert cluster_name_1 has 1 nodes
    ASSERT_EQ(instance1.clusters(0).nodes().size(), 1);
    // after del assert cluster_name_2 has 1 nodes
    ASSERT_EQ(instance1.clusters(1).nodes().size(), 1);
    sp->clear_all_call_backs();
    sp->clear_trace();
    sp->disable_processing();
}

// test restart meta service
TEST(ResourceTest, RestartResourceManager) {
    auto sp = SyncPoint::get_instance();
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

    auto txn_kv = create_txn_kv();

    {
        auto meta_service = get_meta_service(txn_kv);
        create_instance(meta_service.get(), "test_instance_id");
        create_instance(meta_service.get(), "test_instance_id_2");
        create_cluster(meta_service.get(), "test_instance_id", "cluster_id", "cluster_name",
                       ClusterPB::SQL);
    }

    {
        auto meta_service = get_meta_service(txn_kv);
        {
            InstanceInfoPB info;
            auto [code, msg] =
                    meta_service->resource_mgr()->get_instance(nullptr, "test_instance_id", &info);
            ASSERT_EQ(code, TxnErrorCode::TXN_OK) << msg;
            ASSERT_EQ(info.name(), "test_instance_idname");
        }
        {
            InstanceInfoPB info;
            auto [code, msg] = meta_service->resource_mgr()->get_instance(
                    nullptr, "test_instance_id_2", &info);
            ASSERT_EQ(code, TxnErrorCode::TXN_OK) << msg;
            ASSERT_EQ(info.name(), "test_instance_id_2name");
        }
    }
    sp->disable_processing();
    sp->clear_all_call_backs();
}

// test add/drop cluster
TEST(ResourceTest, AddDropCluster) {
    auto sp = SyncPoint::get_instance();
    sp->set_call_back("encrypt_ak_sk:get_encryption_key", [](auto&& args) {
        auto* ret = try_any_cast<int*>(args[0]);
        *ret = 0;
        auto* key = try_any_cast<std::string*>(args[1]);
        *key = "selectdbselectdbselectdbselectdb";
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

    auto meta_service = get_meta_service();
    create_instance(meta_service.get(), "test_instance_id");
    create_cluster(meta_service.get(), "test_instance_id", "sql_id", "sql_cluster", ClusterPB::SQL);
    create_cluster(meta_service.get(), "test_instance_id", "compute_id", "compute_cluster",
                   ClusterPB::COMPUTE);

    InstanceInfoPB info;
    get_instance_info(meta_service.get(), &info, "test_instance_id");
    ASSERT_EQ(info.clusters_size(), 2);

    drop_cluster(meta_service.get(), "test_instance_id", "sql_id");

    get_instance_info(meta_service.get(), &info, "test_instance_id");
    ASSERT_EQ(info.clusters_size(), 1);
    ASSERT_EQ(info.clusters(0).cluster_name(), "compute_cluster");
    ASSERT_EQ(info.clusters(0).cluster_id(), "compute_id");

    sp->disable_processing();
    sp->clear_all_call_backs();
}

TEST(ResourceTest, InitScanRetry) {
    auto sp = SyncPoint::get_instance();
    sp->set_call_back("encrypt_ak_sk:get_encryption_key", [](auto&& args) {
        auto* ret = try_any_cast<int*>(args[0]);
        *ret = 0;
        auto* key = try_any_cast<std::string*>(args[1]);
        *key = "selectdbselectdbselectdbselectdb";
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

    constexpr size_t NUM_BATCH_SIZE = 100;
    sp->set_call_back("ResourceManager:init:limit",
                      [&](auto&& args) { *try_any_cast<int*>(args[0]) = NUM_BATCH_SIZE; });

    auto txn_kv = create_txn_kv();
    {
        auto meta_service = get_meta_service(txn_kv);
        for (size_t i = 0; i < NUM_BATCH_SIZE * 2; i++) {
            std::string instance_id = "test_instance_id_" + std::to_string(i);
            create_instance(meta_service.get(), instance_id);
        }
    }

    {
        size_t count = 0;
        sp->set_call_back("ResourceManager:init:get_err", [&](auto&& args) {
            if (++count == 2) {
                *try_any_cast<TxnErrorCode*>(args[0]) = TxnErrorCode::TXN_TOO_OLD;
            }
        });
        auto meta_service = get_meta_service(txn_kv);
        ASSERT_GT(count, 1) << count;
        for (size_t i = 0; i < NUM_BATCH_SIZE * 2; i++) {
            InstanceInfoPB info;
            std::string instance_id = "test_instance_id_" + std::to_string(i);
            get_instance_info(meta_service.get(), &info, instance_id);
        }
    }

    sp->disable_processing();
    sp->clear_all_call_backs();
}

} // namespace doris::cloud
