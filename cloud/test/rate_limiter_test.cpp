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

#include "rate-limiter/rate_limiter.h"

#include <gen_cpp/cloud.pb.h>
#include <gtest/gtest.h>

#include <cstddef>

#include "common/config.h"
#include "common/util.h"
#include "meta-service/keys.h"
#include "meta-service/mem_txn_kv.h"
#include "meta-service/meta_service.h"
#include "meta-service/txn_kv_error.h"
#include "mock_resource_manager.h"
#include "resource-manager/resource_manager.h"

int main(int argc, char** argv) {
    doris::cloud::config::init(nullptr, true);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

using namespace doris::cloud;

const std::string mock_instance_0 = "mock_instance_0";
const std::string mock_instance_1 = "mock_instance_1";
const std::string mock_cluster_0 = "mock_cluster_0";
const std::string mock_cluster_1 = "mock_cluster_1";
const std::string mock_cluster_id_0 = "mock_cluster_id_0";
const std::string mock_cluster_id_1 = "mock_cluster_id_1";
const std::string mock_cloud_unique_id_0 = "mock_cloud_unique_id_0";
const std::string mock_cloud_unique_id_1 = "mock_cloud_unique_id_1";

class MockMultiInstanceRsMgr : public MockResourceManager {
public:
    using MockResourceManager::MockResourceManager;

    std::string get_node(const std::string& cloud_unique_id,
                         std::vector<NodeInfo>* nodes) override {
        if (cloud_unique_id == mock_cloud_unique_id_0) {
            nodes->emplace_back(Role::COMPUTE_NODE, mock_instance_0, mock_cluster_0,
                                mock_cluster_id_0);
        } else if (cloud_unique_id == mock_cloud_unique_id_1) {
            nodes->emplace_back(Role::COMPUTE_NODE, mock_instance_1, mock_cluster_1,
                                mock_cluster_id_1);
        }
        return {};
    };
};

std::unique_ptr<MetaServiceProxy> get_meta_service() {
    auto txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<MemTxnKv>());
    [&] { ASSERT_NE(txn_kv.get(), nullptr); }();
    auto rs = std::make_shared<MockMultiInstanceRsMgr>(txn_kv);
    auto rl = std::make_shared<RateLimiter>();
    auto meta_service = std::make_unique<MetaServiceImpl>(txn_kv, rs, rl);
    return std::make_unique<MetaServiceProxy>(std::move(meta_service));
}

void mock_add_cluster(MetaServiceProxy& meta_service, std::string instance_id) {
    // add cluster first
    InstanceKeyInfo key_info {instance_id};
    std::string key;
    std::string val;
    instance_key(key_info, &key);

    InstanceInfoPB instance;
    instance.set_instance_id(instance_id);
    ClusterPB c1;
    c1.set_cluster_name(mock_cluster_name);
    c1.set_cluster_id(mock_cluster_id);
    c1.add_mysql_user_name()->append("name1");
    instance.add_clusters()->CopyFrom(c1);
    val = instance.SerializeAsString();

    std::unique_ptr<Transaction> txn;
    std::string get_val;
    ASSERT_EQ(meta_service.txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->put(key, val);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
}

void mock_get_cluster(MetaServiceProxy& meta_service, const std::string& cloud_uid,
                      MetaServiceCode code) {
    GetClusterRequest req;
    req.set_cloud_unique_id(cloud_uid);
    req.set_cluster_id(mock_cluster_id);
    req.set_cluster_name(mock_cluster_name);
    brpc::Controller cntl;
    GetClusterResponse res;
    meta_service.get_cluster(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                             &res, nullptr);

    ASSERT_EQ(res.status().code(), code);
}

template <typename Rpc>
void mock_parallel_rpc(Rpc rpc, MetaServiceProxy* meta_service, const std::string& cloud_uid,
                       MetaServiceCode expected, size_t times) {
    std::vector<std::thread> threads;
    for (size_t i = 0; i < times; ++i) {
        threads.emplace_back([&]() { rpc(*meta_service, cloud_uid, expected); });
    }
    for (auto& t : threads) {
        t.join();
    }
}

TEST(RateLimiterTest, RateLimitGetClusterTest) {
    auto meta_service = get_meta_service();
    mock_add_cluster(*meta_service, mock_instance_0);

    mock_parallel_rpc(mock_get_cluster, meta_service.get(), mock_cloud_unique_id_0,
                      MetaServiceCode::OK, 20);

    std::this_thread::sleep_for(std::chrono::seconds(1));
    meta_service->rate_limiter()
            ->get_rpc_rate_limiter("get_cluster")
            ->qps_limiter_[mock_instance_0]
            ->max_qps_limit_ = 1;
    mock_parallel_rpc(mock_get_cluster, meta_service.get(), mock_cloud_unique_id_0,
                      MetaServiceCode::MAX_QPS_LIMIT, 1);

    std::this_thread::sleep_for(std::chrono::seconds(1));
    meta_service->rate_limiter()
            ->get_rpc_rate_limiter("get_cluster")
            ->qps_limiter_[mock_instance_0]
            ->max_qps_limit_ = 10000;
    mock_parallel_rpc(mock_get_cluster, meta_service.get(), mock_cloud_unique_id_0,
                      MetaServiceCode::OK, 1);
}

TEST(RateLimiterTest, AdjustLimitInfluenceTest) {
    auto meta_service = get_meta_service();
    mock_add_cluster(*meta_service, mock_instance_0);
    mock_add_cluster(*meta_service, mock_instance_1);

    mock_parallel_rpc(mock_get_cluster, meta_service.get(), mock_cloud_unique_id_0,
                      MetaServiceCode::OK, 1);
    std::this_thread::sleep_for(std::chrono::seconds(1));
    {
        ASSERT_TRUE(meta_service->rate_limiter()->set_instance_rate_limit(1, mock_instance_1));
        ASSERT_TRUE(
                meta_service->rate_limiter()->set_rate_limit(100, "get_cluster", mock_instance_0));

        auto limit = meta_service->rate_limiter()
                             ->get_rpc_rate_limiter("get_cluster")
                             ->qps_limiter_.at(mock_instance_0)
                             ->max_qps_limit();
        ASSERT_EQ(limit, 100);
        limit = meta_service->rate_limiter()
                        ->get_rpc_rate_limiter("get_cluster")
                        ->qps_limiter_.at(mock_instance_1)
                        ->max_qps_limit();
        ASSERT_EQ(limit, 1);
    }
    {
        ASSERT_TRUE(meta_service->rate_limiter()->set_rate_limit(1));
        auto limit =
                meta_service->rate_limiter()->get_rpc_rate_limiter("commit_txn")->max_qps_limit();
        ASSERT_EQ(limit, 1);
        limit = meta_service->rate_limiter()->get_rpc_rate_limiter("get_cluster")->max_qps_limit();
        ASSERT_EQ(limit, 5000000);
    }
    {
        ASSERT_TRUE(meta_service->rate_limiter()->set_rate_limit(5000, "get_cluster"));
        auto limit =
                meta_service->rate_limiter()->get_rpc_rate_limiter("commit_txn")->max_qps_limit();
        ASSERT_EQ(limit, 1);
        ASSERT_TRUE(meta_service->rate_limiter()->set_rate_limit(1000));
        limit = meta_service->rate_limiter()->get_rpc_rate_limiter("get_cluster")->max_qps_limit();
        ASSERT_EQ(limit, 5000);
        limit = meta_service->rate_limiter()->get_rpc_rate_limiter("commit_txn")->max_qps_limit();
        ASSERT_EQ(limit, 1000);
    }
    {
        ASSERT_TRUE(meta_service->rate_limiter()->set_rate_limit(3000, "commit_txn"));
        auto limit =
                meta_service->rate_limiter()->get_rpc_rate_limiter("commit_txn")->max_qps_limit();
        ASSERT_EQ(limit, 3000);
        limit = meta_service->rate_limiter()->get_rpc_rate_limiter("get_cluster")->max_qps_limit();
        ASSERT_EQ(limit, 5000);
    }
    {
        auto limit = meta_service->rate_limiter()
                             ->get_rpc_rate_limiter("get_cluster")
                             ->qps_limiter_.at(mock_instance_0)
                             ->max_qps_limit();
        ASSERT_EQ(limit, 100);
        limit = meta_service->rate_limiter()
                        ->get_rpc_rate_limiter("get_cluster")
                        ->qps_limiter_.at(mock_instance_1)
                        ->max_qps_limit();
        ASSERT_EQ(limit, 1);
    }
    {
        ASSERT_TRUE(meta_service->rate_limiter()->set_instance_rate_limit(200, mock_instance_1));
        auto limit = meta_service->rate_limiter()
                             ->get_rpc_rate_limiter("get_cluster")
                             ->qps_limiter_.at(mock_instance_0)
                             ->max_qps_limit();
        ASSERT_EQ(limit, 100);
        limit = meta_service->rate_limiter()
                        ->get_rpc_rate_limiter("get_cluster")
                        ->qps_limiter_.at(mock_instance_1)
                        ->max_qps_limit();
        ASSERT_EQ(limit, 200);
    }
}

TEST(RateLimiterTest, AdjustLimitMockRPCTest) {
    auto meta_service = get_meta_service();
    mock_add_cluster(*meta_service, mock_instance_0);
    mock_add_cluster(*meta_service, mock_instance_1);
    mock_parallel_rpc(mock_get_cluster, meta_service.get(), mock_cloud_unique_id_0,
                      MetaServiceCode::OK, 20);
    mock_parallel_rpc(mock_get_cluster, meta_service.get(), mock_cloud_unique_id_1,
                      MetaServiceCode::OK, 20);
    std::this_thread::sleep_for(std::chrono::seconds(1));

    {
        ASSERT_TRUE(meta_service->rate_limiter()->set_rate_limit(10000, "get_cluster"));
        ASSERT_TRUE(meta_service->rate_limiter()->set_rate_limit(1));
        mock_parallel_rpc(mock_get_cluster, meta_service.get(), mock_cloud_unique_id_0,
                          MetaServiceCode::OK, 20);
        auto limit =
                meta_service->rate_limiter()->get_rpc_rate_limiter("get_cluster")->max_qps_limit();
        ASSERT_EQ(limit, 10000);
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    {
        ASSERT_TRUE(meta_service->rate_limiter()->set_rate_limit(1, "get_cluster"));
        ASSERT_TRUE(meta_service->rate_limiter()->set_rate_limit(1));
        mock_parallel_rpc(mock_get_cluster, meta_service.get(), mock_cloud_unique_id_0,
                          MetaServiceCode::MAX_QPS_LIMIT, 1);
        auto limit =
                meta_service->rate_limiter()->get_rpc_rate_limiter("get_cluster")->max_qps_limit();
        ASSERT_EQ(limit, 1);
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    {
        ASSERT_TRUE(meta_service->rate_limiter()->set_rate_limit(10000, "get_cluster",
                                                                 mock_instance_0));
        ASSERT_TRUE(meta_service->rate_limiter()->set_rate_limit(10000, "get_cluster",
                                                                 mock_instance_1));
        mock_parallel_rpc(mock_get_cluster, meta_service.get(), mock_cloud_unique_id_0,
                          MetaServiceCode::OK, 20);
        mock_parallel_rpc(mock_get_cluster, meta_service.get(), mock_cloud_unique_id_1,
                          MetaServiceCode::OK, 20);
        auto limit = meta_service->rate_limiter()
                             ->get_rpc_rate_limiter("get_cluster")
                             ->qps_limiter_.at(mock_instance_0)
                             ->max_qps_limit();
        ASSERT_EQ(limit, 10000);
        limit = meta_service->rate_limiter()
                        ->get_rpc_rate_limiter("get_cluster")
                        ->qps_limiter_.at(mock_instance_1)
                        ->max_qps_limit();
        ASSERT_EQ(limit, 10000);
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    {
        ASSERT_TRUE(meta_service->rate_limiter()->set_instance_rate_limit(1, mock_instance_0));
        mock_parallel_rpc(mock_get_cluster, meta_service.get(), mock_cloud_unique_id_0,
                          MetaServiceCode::MAX_QPS_LIMIT, 1);
        mock_parallel_rpc(mock_get_cluster, meta_service.get(), mock_cloud_unique_id_1,
                          MetaServiceCode::OK, 20);
        auto limit = meta_service->rate_limiter()
                             ->get_rpc_rate_limiter("get_cluster")
                             ->qps_limiter_.at(mock_instance_0)
                             ->max_qps_limit();
        ASSERT_EQ(limit, 1);
        limit = meta_service->rate_limiter()
                        ->get_rpc_rate_limiter("get_cluster")
                        ->qps_limiter_.at(mock_instance_1)
                        ->max_qps_limit();
        ASSERT_EQ(limit, 10000);
    }
}
