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

#include "cloud/cloud_meta_mgr.h"

#include <gen_cpp/cloud.pb.h>
#include <gtest/gtest.h>

#include <chrono>
#include <memory>

namespace doris {
using namespace cloud;
using namespace std::chrono;

class CloudMetaMgrTest : public testing::Test {
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(CloudMetaMgrTest, bthread_fork_join_test) {
    // clang-format off
    std::vector<std::function<Status()>> tasks {
        []{ bthread_usleep(20000); return Status::OK(); },
        []{ bthread_usleep(20000); return Status::OK(); },
        []{ bthread_usleep(20000); return Status::OK(); },
        []{ bthread_usleep(20000); return Status::OK(); },
        []{ bthread_usleep(20000); return Status::OK(); },
        []{ bthread_usleep(20000); return Status::OK(); },
        []{ bthread_usleep(20000); return Status::OK(); },
    };
    {
        auto start = steady_clock::now();
        EXPECT_TRUE(bthread_fork_join(tasks, 3).ok());
        auto end = steady_clock::now();
        auto elapsed = duration_cast<milliseconds>(end - start).count();
        EXPECT_GT(elapsed, 40); // at least 2 rounds running for 7 tasks
    }
    {
        std::future<Status> fut;
        auto start = steady_clock::now();
        auto t = tasks;
        EXPECT_TRUE(bthread_fork_join(std::move(t), 3, &fut).ok()); // return immediately
        auto end = steady_clock::now();
        auto elapsed = duration_cast<milliseconds>(end - start).count();
        EXPECT_LE(elapsed, 40); // async
        EXPECT_TRUE(fut.get().ok());
        end = steady_clock::now();
        elapsed = duration_cast<milliseconds>(end - start).count();
        EXPECT_GT(elapsed, 40); // at least 2 rounds running for 7 tasks
    }

    // make the first batch fail fast
    tasks.insert(tasks.begin(), []{ bthread_usleep(20000); return Status::InternalError<false>("error"); });
    {
        auto start = steady_clock::now();
        EXPECT_FALSE(bthread_fork_join(tasks, 3).ok());
        auto end = steady_clock::now();
        auto elapsed = duration_cast<milliseconds>(end - start).count();
        EXPECT_LE(elapsed, 40); // at most 1 round running for 7 tasks
    }
    {
        std::future<Status> fut;
        auto start = steady_clock::now();
        auto t = tasks;
        EXPECT_TRUE(bthread_fork_join(std::move(t), 3, &fut).ok()); // return immediately
        auto end = steady_clock::now();
        auto elapsed = duration_cast<milliseconds>(end - start).count();
        EXPECT_LE(elapsed, 40); // async
        EXPECT_FALSE(fut.get().ok());
        end = steady_clock::now();
        elapsed = duration_cast<milliseconds>(end - start).count();
        EXPECT_LE(elapsed, 40); // at most 1 round running for 7 tasks
    }
    // clang-format on
}

TEST_F(CloudMetaMgrTest, hide_ak_sk_when_get_storage_vault_info) {
    // 创建测试响应对象
    CloudMetaMgr mgr;
    GetObjStoreInfoResponse resp;

    // 添加普通对象存储信息
    auto* obj1 = resp.add_obj_info();
    obj1->set_ak("abcdefghijklmnopqrstuvwxyz1234567890");
    obj1->set_sk("abcdefghijklmnopqrstuvwxyz1234567890");

    auto* obj2 = resp.add_obj_info();
    obj2->set_ak("abc");
    obj2->set_sk("abc");

    auto* obj3 = resp.add_obj_info();
    obj3->set_ak("ab");
    obj3->set_sk("ab");

    auto* obj4 = resp.add_obj_info();
    obj4->set_ak("a");
    obj4->set_sk("a");

    auto* vault1 = resp.add_storage_vault();
    auto* vault_obj1 = vault1->mutable_obj_info();
    vault_obj1->set_ak("abcdefghijklmnopqrstuvwxyz1234567890");
    vault_obj1->set_sk("abcdefghijklmnopqrstuvwxyz1234567890");

    auto* vault2 = resp.add_storage_vault();
    auto* vault_obj2 = vault2->mutable_obj_info();
    vault_obj2->set_ak("abc");
    vault_obj2->set_sk("abc");

    auto* vault3 = resp.add_storage_vault();
    auto* vault_obj3 = vault3->mutable_obj_info();
    vault_obj3->set_ak("ab");
    vault_obj3->set_sk("ab");

    auto* vault4 = resp.add_storage_vault();
    auto* vault_obj4 = vault4->mutable_obj_info();
    vault_obj4->set_ak("a");
    vault_obj4->set_sk("a");

    mgr.hide_access_key(&resp);

    EXPECT_EQ(resp.obj_info(0).ak(), "xxxxxxxxxxxxxxxpqrstuxxxxxxxxxxxxxxx");
    EXPECT_EQ(resp.obj_info(1).ak(), "xbx");
    EXPECT_EQ(resp.obj_info(2).ak(), "xx");
    EXPECT_EQ(resp.obj_info(3).ak(), "x");

    EXPECT_EQ(resp.obj_info(0).sk(), "abxxx");
    EXPECT_EQ(resp.obj_info(1).sk(), "abxxx");
    EXPECT_EQ(resp.obj_info(2).sk(), "abxxx");
    EXPECT_EQ(resp.obj_info(3).sk(), "axxx");

    EXPECT_EQ(resp.storage_vault(0).obj_info().ak(), "xxxxxxxxxxxxxxxpqrstuxxxxxxxxxxxxxxx");
    EXPECT_EQ(resp.storage_vault(1).obj_info().ak(), "xbx");
    EXPECT_EQ(resp.storage_vault(2).obj_info().ak(), "xx");
    EXPECT_EQ(resp.storage_vault(3).obj_info().ak(), "x");

    EXPECT_EQ(resp.storage_vault(0).obj_info().sk(), "abxxx");
    EXPECT_EQ(resp.storage_vault(1).obj_info().sk(), "abxxx");
    EXPECT_EQ(resp.storage_vault(2).obj_info().sk(), "abxxx");
    EXPECT_EQ(resp.storage_vault(3).obj_info().sk(), "axxx");
}

} // namespace doris
