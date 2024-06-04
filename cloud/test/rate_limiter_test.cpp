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

#include <gtest/gtest.h>

#include "common/config.h"
#include "common/util.h"
#include "meta-service/keys.h"
#include "meta-service/mem_txn_kv.h"
#include "meta-service/meta_service.h"
#include "meta-service/txn_kv_error.h"
#include "mock_resource_manager.h"

int main(int argc, char** argv) {
    doris::cloud::config::init(nullptr, true);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

using namespace doris::cloud;

std::unique_ptr<MetaServiceProxy> get_meta_service() {
    auto txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<MemTxnKv>());
    [&] { ASSERT_NE(txn_kv.get(), nullptr); }();
    auto rs = std::make_shared<MockResourceManager>(txn_kv);
    auto rl = std::make_shared<RateLimiter>();
    auto meta_service = std::make_unique<MetaServiceImpl>(txn_kv, rs, rl);
    return std::make_unique<MetaServiceProxy>(std::move(meta_service));
}

TEST(RateLimiterTest, RateLimitGetClusterTest) {
    auto meta_service = get_meta_service();
    // add cluster first
    InstanceKeyInfo key_info {mock_instance};
    std::string key;
    std::string val;
    instance_key(key_info, &key);

    InstanceInfoPB instance;
    instance.set_instance_id(mock_instance);
    ClusterPB c1;
    c1.set_cluster_name(mock_cluster_name);
    c1.set_cluster_id(mock_cluster_id);
    c1.add_mysql_user_name()->append("name1");
    instance.add_clusters()->CopyFrom(c1);
    val = instance.SerializeAsString();

    std::unique_ptr<Transaction> txn;
    std::string get_val;
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->put(key, val);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    auto get_cluster = [&](MetaServiceCode code) {
        GetClusterRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_cluster_id(mock_cluster_id);
        req.set_cluster_name(mock_cluster_name);
        brpc::Controller cntl;
        GetClusterResponse res;
        meta_service->get_cluster(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                  &res, nullptr);

        ASSERT_EQ(res.status().code(), code);
    };
    std::vector<std::thread> threads;
    for (int i = 0; i < 20; ++i) {
        threads.emplace_back(get_cluster, MetaServiceCode::OK);
    }
    for (auto& t : threads) {
        t.join();
    }
    threads.clear();

    std::this_thread::sleep_for(std::chrono::seconds(1));
    meta_service->rate_limiter()
            ->get_rpc_rate_limiter("get_cluster")
            ->qps_limiter_[mock_instance]
            ->max_qps_limit_ = 1;
    threads.emplace_back(get_cluster, MetaServiceCode::MAX_QPS_LIMIT);
    for (auto& t : threads) {
        t.join();
    }
    threads.clear();

    std::this_thread::sleep_for(std::chrono::seconds(1));
    meta_service->rate_limiter()
            ->get_rpc_rate_limiter("get_cluster")
            ->qps_limiter_[mock_instance]
            ->max_qps_limit_ = 10000;
    threads.emplace_back(get_cluster, MetaServiceCode::OK);
    for (auto& t : threads) {
        t.join();
    }
    threads.clear();
}