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

#include "runtime/mem_tracker.h"
#include "util/logging.h"
#include "util/metrics.h"

namespace doris {

TEST(MemTrackerTest, SingleTrackerNoLimit) {
    auto t = MemTracker::CreateTracker();
    EXPECT_FALSE(t->has_limit());
    t->Consume(10);
    EXPECT_EQ(t->consumption(), 10);
    t->Consume(10);
    EXPECT_EQ(t->consumption(), 20);
    t->Release(15);
    EXPECT_EQ(t->consumption(), 5);
    EXPECT_FALSE(t->LimitExceeded(MemLimit::HARD));
    t->Release(5);
}

TEST(MemTestTest, SingleTrackerWithLimit) {
    auto t = MemTracker::CreateTracker(11, "limit tracker");
    EXPECT_TRUE(t->has_limit());
    t->Consume(10);
    EXPECT_EQ(t->consumption(), 10);
    EXPECT_FALSE(t->LimitExceeded(MemLimit::HARD));
    t->Consume(10);
    EXPECT_EQ(t->consumption(), 20);
    EXPECT_TRUE(t->LimitExceeded(MemLimit::HARD));
    t->Release(15);
    EXPECT_EQ(t->consumption(), 5);
    EXPECT_FALSE(t->LimitExceeded(MemLimit::HARD));
    t->Release(5);
}

TEST(MemTestTest, TrackerHierarchy) {
    auto p = MemTracker::CreateTracker(100);
    auto c1 = MemTracker::CreateTracker(80, "c1", p);
    auto c2 = MemTracker::CreateTracker(50, "c2", p);

    // everything below limits
    c1->Consume(60);
    EXPECT_EQ(c1->consumption(), 60);
    EXPECT_FALSE(c1->LimitExceeded(MemLimit::HARD));
    EXPECT_FALSE(c1->AnyLimitExceeded(MemLimit::HARD));
    EXPECT_EQ(c2->consumption(), 0);
    EXPECT_FALSE(c2->LimitExceeded(MemLimit::HARD));
    EXPECT_FALSE(c2->AnyLimitExceeded(MemLimit::HARD));
    EXPECT_EQ(p->consumption(), 60);
    EXPECT_FALSE(p->LimitExceeded(MemLimit::HARD));
    EXPECT_FALSE(p->AnyLimitExceeded(MemLimit::HARD));

    // p goes over limit
    c2->Consume(50);
    EXPECT_EQ(c1->consumption(), 60);
    EXPECT_FALSE(c1->LimitExceeded(MemLimit::HARD));
    EXPECT_TRUE(c1->AnyLimitExceeded(MemLimit::HARD));
    EXPECT_EQ(c2->consumption(), 50);
    EXPECT_FALSE(c2->LimitExceeded(MemLimit::HARD));
    EXPECT_TRUE(c2->AnyLimitExceeded(MemLimit::HARD));
    EXPECT_EQ(p->consumption(), 110);
    EXPECT_TRUE(p->LimitExceeded(MemLimit::HARD));

    // c2 goes over limit, p drops below limit
    c1->Release(20);
    c2->Consume(10);
    EXPECT_EQ(c1->consumption(), 40);
    EXPECT_FALSE(c1->LimitExceeded(MemLimit::HARD));
    EXPECT_FALSE(c1->AnyLimitExceeded(MemLimit::HARD));
    EXPECT_EQ(c2->consumption(), 60);
    EXPECT_TRUE(c2->LimitExceeded(MemLimit::HARD));
    EXPECT_TRUE(c2->AnyLimitExceeded(MemLimit::HARD));
    EXPECT_EQ(p->consumption(), 100);
    EXPECT_FALSE(p->LimitExceeded(MemLimit::HARD));
    c1->Release(40);
    c2->Release(60);
}

TEST(MemTestTest, TrackerHierarchyTryConsume) {
    auto p = MemTracker::CreateTracker(100);
    auto c1 = MemTracker::CreateTracker(80, "c1", p);
    auto c2 = MemTracker::CreateTracker(50, "c2", p);

    // everything below limits
    bool consumption = c1->TryConsume(60).ok();
    EXPECT_EQ(consumption, true);
    EXPECT_EQ(c1->consumption(), 60);
    EXPECT_FALSE(c1->LimitExceeded(MemLimit::HARD));
    EXPECT_FALSE(c1->AnyLimitExceeded(MemLimit::HARD));
    EXPECT_EQ(c2->consumption(), 0);
    EXPECT_FALSE(c2->LimitExceeded(MemLimit::HARD));
    EXPECT_FALSE(c2->AnyLimitExceeded(MemLimit::HARD));
    EXPECT_EQ(p->consumption(), 60);
    EXPECT_FALSE(p->LimitExceeded(MemLimit::HARD));
    EXPECT_FALSE(p->AnyLimitExceeded(MemLimit::HARD));

    // p goes over limit
    consumption = c2->TryConsume(50).ok();
    EXPECT_EQ(consumption, false);
    EXPECT_EQ(c1->consumption(), 60);
    EXPECT_FALSE(c1->LimitExceeded(MemLimit::HARD));
    EXPECT_FALSE(c1->AnyLimitExceeded(MemLimit::HARD));
    EXPECT_EQ(c2->consumption(), 0);
    EXPECT_FALSE(c2->LimitExceeded(MemLimit::HARD));
    EXPECT_FALSE(c2->AnyLimitExceeded(MemLimit::HARD));
    EXPECT_EQ(p->consumption(), 60);
    EXPECT_FALSE(p->LimitExceeded(MemLimit::HARD));
    EXPECT_FALSE(p->AnyLimitExceeded(MemLimit::HARD));

    // c2 goes over limit, p drops below limit
    c1->Release(20);
    c2->Consume(10);
    EXPECT_EQ(c1->consumption(), 40);
    EXPECT_FALSE(c1->LimitExceeded(MemLimit::HARD));
    EXPECT_FALSE(c1->AnyLimitExceeded(MemLimit::HARD));
    EXPECT_EQ(c2->consumption(), 10);
    EXPECT_FALSE(c2->LimitExceeded(MemLimit::HARD));
    EXPECT_FALSE(c2->AnyLimitExceeded(MemLimit::HARD));
    EXPECT_EQ(p->consumption(), 50);
    EXPECT_FALSE(p->LimitExceeded(MemLimit::HARD));

    c1->Release(40);
    c2->Release(10);
}

} // end namespace doris

int main(int argc, char** argv) {
    std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    if (!doris::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    doris::init_glog("be-test");
    ::testing::InitGoogleTest(&argc, argv);
    doris::MemInfo::init();
    return RUN_ALL_TESTS();
}
