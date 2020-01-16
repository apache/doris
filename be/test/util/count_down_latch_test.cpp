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

#include <gtest/gtest.h>
#include "common/config.h"
#include "util/count_down_latch.hpp"
#include "util/logging.h"

#include <memory>

namespace doris {

class CountDownLatchTest : public testing::Test {
public:
    CountDownLatchTest() {
    }

protected:
    virtual void SetUp() {
    }
    virtual void TearDown() {
    }
};

struct WokerCtx {
    WokerCtx(int count_param) :
            latch(CountDownLatch(count_param)), 
            u_sleep_time(0),
            count(count_param) {
    }

    ~WokerCtx() {
    }

    CountDownLatch latch;
    int u_sleep_time;
    int count;
};

void* worker(void* param) {
    std::shared_ptr<WokerCtx> ctx = *(std::shared_ptr<WokerCtx>*)param;
    usleep(ctx->u_sleep_time);
    ctx->latch.count_down(ctx->count);
    return nullptr;
}

TEST_F(CountDownLatchTest, Normal) {
    std::shared_ptr<WokerCtx> ctx(new WokerCtx(10));
    ctx->u_sleep_time = 100;

    pthread_t id;
    pthread_create(&id, nullptr, worker, &ctx);
    ctx->latch.await();
    pthread_join(id, nullptr);
}

TEST_F(CountDownLatchTest, Timed) {
    std::shared_ptr<WokerCtx> ctx(new WokerCtx(10));
    ctx->u_sleep_time = 100;

    pthread_t id;
    pthread_create(&id, nullptr, worker, &ctx);
    ASSERT_EQ(0, ctx->latch.await(1000));
    pthread_join(id, nullptr);
}

TEST_F(CountDownLatchTest, Timeout) {
    std::shared_ptr<WokerCtx> ctx(new WokerCtx(10));
    ctx->u_sleep_time = 2000000;

    pthread_t id;
    pthread_create(&id, nullptr, worker, &ctx);
    ASSERT_EQ(-1, ctx->latch.await(1000));
    pthread_join(id, nullptr);
}

}

int main(int argc, char** argv) {
    std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    if (!doris::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    doris::init_glog("be-test");
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
