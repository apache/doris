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

#include "util/counter_cond_variable.hpp"

#include <gtest/gtest.h>

#include <thread>

#include "common/logging.h"

namespace doris {

CounterCondVariable g_cond;
std::mutex g_io_mu;

class CounterCondVariableTest : public testing::Test {
public:
    CounterCondVariableTest() {}
    virtual ~CounterCondVariableTest() {}
};

void submitter() {
    for (int i = 0; i < 10; ++i) {
        g_cond.inc();
    }
}

void worker() {
    for (int i = 0; i < 10; ++i) {
        {
            std::unique_lock<std::mutex> lock(g_io_mu);
            std::cout << "worker " << i << std::endl;
        }
        usleep(100);
        g_cond.dec();
    }
}

void waiter() {
    g_cond.block_wait();
    std::cout << "wait finished" << std::endl;
}

TEST_F(CounterCondVariableTest, test) {
    g_cond.block_wait();
    g_cond.inc(10);
    g_cond.dec(10);
    g_cond.block_wait();

    std::thread submit(submitter);
    std::thread wait1(waiter);
    std::thread wait2(waiter);
    std::thread work(worker);

    submit.join();
    wait1.join();
    wait2.join();
    work.join();

    g_cond.block_wait();
}

} // namespace doris

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
