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

#include "runtime/thread_resource_mgr.h"

#include <gtest/gtest.h>

#include <boost/bind.hpp>
#include <string>

#include "util/cpu_info.h"

namespace doris {

class NotifiedCounter {
public:
    NotifiedCounter() : _counter(0) {}

    void Notify(ThreadResourceMgr::ResourcePool* consumer) {
        DCHECK(consumer != NULL);
        DCHECK_LT(consumer->num_threads(), consumer->quota());
        ++_counter;
    }

    int counter() const { return _counter; }

private:
    int _counter;
};

TEST(ThreadResourceMgr, BasicTest) {
    ThreadResourceMgr mgr(5);
    NotifiedCounter counter1;
    NotifiedCounter counter2;

    ThreadResourceMgr::ResourcePool* c1 = mgr.register_pool();
    c1->set_thread_available_cb(
            boost::bind<void>(boost::mem_fn(&NotifiedCounter::Notify), &counter1, _1));
    c1->acquire_thread_token();
    c1->acquire_thread_token();
    c1->acquire_thread_token();
    EXPECT_EQ(c1->num_threads(), 3);
    EXPECT_EQ(c1->num_required_threads(), 3);
    EXPECT_EQ(c1->num_optional_threads(), 0);
    EXPECT_EQ(counter1.counter(), 0);
    c1->release_thread_token(true);
    EXPECT_EQ(c1->num_threads(), 2);
    EXPECT_EQ(c1->num_required_threads(), 2);
    EXPECT_EQ(c1->num_optional_threads(), 0);
    EXPECT_EQ(counter1.counter(), 1);
    EXPECT_TRUE(c1->try_acquire_thread_token());
    EXPECT_TRUE(c1->try_acquire_thread_token());
    EXPECT_TRUE(c1->try_acquire_thread_token());
    EXPECT_FALSE(c1->try_acquire_thread_token());
    EXPECT_EQ(c1->num_threads(), 5);
    EXPECT_EQ(c1->num_required_threads(), 2);
    EXPECT_EQ(c1->num_optional_threads(), 3);
    c1->release_thread_token(true);
    c1->release_thread_token(false);
    EXPECT_EQ(counter1.counter(), 3);

    // Register a new consumer, quota is cut in half
    ThreadResourceMgr::ResourcePool* c2 = mgr.register_pool();
    c2->set_thread_available_cb(
            boost::bind<void>(boost::mem_fn(&NotifiedCounter::Notify), &counter2, _1));
    EXPECT_FALSE(c1->try_acquire_thread_token());
    EXPECT_EQ(c1->num_threads(), 3);
    c1->acquire_thread_token();
    EXPECT_EQ(c1->num_threads(), 4);
    EXPECT_EQ(c1->num_required_threads(), 2);
    EXPECT_EQ(c1->num_optional_threads(), 2);

    mgr.unregister_pool(c1);
    mgr.unregister_pool(c2);
    EXPECT_EQ(counter1.counter(), 3);
    EXPECT_EQ(counter2.counter(), 1);
}

} // namespace doris

int main(int argc, char** argv) {
    std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    if (!doris::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    init_glog("be-test");
    ::testing::InitGoogleTest(&argc, argv);
    doris::CpuInfo::Init();
    return RUN_ALL_TESTS();
}
