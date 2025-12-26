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
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

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
TEST(BvarsTest, MultiThreadRecordMetrics) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    std::vector<std::thread> threads;
    std::atomic_long update_count = 0;
    MBvarLatencyRecorderWithStatus<60> mbvarlr_partition_test("partition_test", {"instance_id"});
    MBvarLatencyRecorderWithStatus<60> mbvarlr_tablet_test("tablet_test", {"instance_id"});

    int interval_s = 60;
    auto* sp = SyncPoint::get_instance();
    sp->set_call_back("mBvarLatencyRecorderWithStatus::put", [&interval_s](auto&& args) {
        auto* vault = try_any_cast<int*>(args[0]);
        *vault = interval_s;
    });
    sp->set_call_back("mBvarLatencyRecorderWithStatus::update",
                      [&update_count](auto&& args) { update_count++; });
    sp->enable_processing();

    for (int i = 0; i < 20; ++i) {
        threads.emplace_back([&]() {
            for (size_t j = 0; j < 1000; j++) {
                std::unique_ptr<Transaction> txn;
                ASSERT_EQ(TxnErrorCode::TXN_OK, txn_kv->create_txn(&txn));

                std::string instance_id = "instance_" + std::to_string(j % 100);
                int64_t partition_count = 10 + j;
                int64_t tablet_count = 50 + j;

                mbvarlr_partition_test.put({instance_id}, partition_count);
                mbvarlr_tablet_test.put({instance_id}, tablet_count);

                std::this_thread::sleep_for(std::chrono::microseconds(1000));
            }
        });
    }

    for (int i = 0; i < 20; ++i) {
        threads.emplace_back([&]() {
            for (size_t j = 0; j < 1000; j++) {
                std::unique_ptr<Transaction> txn;
                ASSERT_EQ(TxnErrorCode::TXN_OK, txn_kv->create_txn(&txn));

                std::string instance_id = "instance_" + std::to_string(j % 100);

                mbvarlr_partition_test.get_count({instance_id});
                mbvarlr_partition_test.get_avg({instance_id});
                mbvarlr_partition_test.get_max({instance_id});
                mbvarlr_tablet_test.get_count({instance_id});
                mbvarlr_tablet_test.get_avg({instance_id});
                mbvarlr_tablet_test.get_max({instance_id});

                std::this_thread::sleep_for(std::chrono::microseconds(1000));
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    sleep(interval_s * 3);

    ASSERT_GT(update_count.load(), 200);
}

} // namespace doris::cloud