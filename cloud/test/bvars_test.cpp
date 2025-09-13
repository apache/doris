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

#include "common/bvars.h"

#include <brpc/server.h>
#include <bthread/bthread.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <iostream>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "common/configbase.h"
#include "common/defer.h"
#include "common/logging.h"
#include "meta-service/meta_service.h"
#include "meta-store/mem_txn_kv.h"

namespace doris::cloud {

int main(int argc, char** argv) {
    if (!cloud::init_glog("bvars_test")) {
        std::cerr << "failed to init glog" << std::endl;
        return -1;
    }

    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

class BvarsTest : public ::testing::Test {
public:
    void SetUp() override {
        if (server.Start("0.0.0.0:0", &options) == -1) {
            perror("Start brpc server");
        }
    }
    void TearDown() override {
        server.Stop(0);
        server.Join();
    }
    brpc::ServerOptions options;
    brpc::Server server;
};

TEST(BvarsTest, MutiThreadRecordMetrics) {
    const int THREAD_NUM = 1000;
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);
    std::vector<std::thread> threads;
    std::atomic_int64_t start_count = 0;
    std::atomic_int64_t update_count = 0;
    std::atomic_int64_t already_start_count = 0;
    int interval_s = 15;
    auto* sp = SyncPoint::get_instance();
    DORIS_CLOUD_DEFER {
        SyncPoint::get_instance()->clear_all_call_backs();
    };
    sp->set_call_back("ScheduledLatencyUpdater::start", [&](auto&&) { ++start_count; });
    sp->set_call_back("ScheduledLatencyUpdater::update", [&](auto&&) { ++update_count; });
    sp->set_call_back("ScheduledLatencyUpdater::already_start",
                      [&](auto&&) { ++already_start_count; });
    sp->set_call_back("mBvarLatencyRecorderWithStatus::put", [&interval_s](auto&& args) {
        auto* vault = try_any_cast<int*>(args[0]);
        *vault = interval_s;
    });
    sp->enable_processing();

    for (int i = 0; i < THREAD_NUM; ++i) {
        threads.emplace_back([i, &txn_kv]() {
            std::unique_ptr<Transaction> txn;
            ASSERT_EQ(TxnErrorCode::TXN_OK, txn_kv->create_txn(&txn));

            std::string instance_id = "instance_" + std::to_string(i);
            int64_t partition_count = 10 + i;
            int64_t tablet_count = 50 + i;
            int64_t txn_id = 1000 + i;

            record_txn_commit_stats(txn.get(), instance_id, partition_count, tablet_count, txn_id);
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // tablet and partition metrics, so it will double
    ASSERT_EQ(start_count, 2 * THREAD_NUM);
    ASSERT_EQ(already_start_count, 0);

    threads.clear();

    for (int i = 0; i < THREAD_NUM; ++i) {
        threads.emplace_back([i, &txn_kv]() {
            std::unique_ptr<Transaction> txn;
            ASSERT_EQ(TxnErrorCode::TXN_OK, txn_kv->create_txn(&txn));

            std::string instance_id = "instance_" + std::to_string(i);
            int64_t partition_count = 10 + i;
            int64_t tablet_count = 50 + i;
            int64_t txn_id = 1000 + i;

            record_txn_commit_stats(txn.get(), instance_id, partition_count, tablet_count, txn_id);
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // tablet and partition metrics, so it will double
    ASSERT_EQ(already_start_count, 2 * THREAD_NUM);

    sleep(interval_s);

    // tablet and partition metrics, so it will double
    ASSERT_EQ(update_count, 2 * THREAD_NUM);
}

} // namespace doris::cloud