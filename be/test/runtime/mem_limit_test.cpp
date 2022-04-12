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
    auto t = MemTracker::create_tracker();
    EXPECT_FALSE(t->has_limit());
    t->consume(10);
    EXPECT_EQ(t->consumption(), 10);
    t->consume(10);
    EXPECT_EQ(t->consumption(), 20);
    t->release(15);
    EXPECT_EQ(t->consumption(), 5);
    EXPECT_FALSE(t->limit_exceeded());
    t->release(5);
}

TEST(MemTestTest, SingleTrackerWithLimit) {
    auto t = MemTracker::create_tracker(11, "limit tracker");
    EXPECT_TRUE(t->has_limit());
    t->consume(10);
    EXPECT_EQ(t->consumption(), 10);
    EXPECT_FALSE(t->limit_exceeded());
    t->consume(10);
    EXPECT_EQ(t->consumption(), 20);
    EXPECT_TRUE(t->limit_exceeded());
    t->release(15);
    EXPECT_EQ(t->consumption(), 5);
    EXPECT_FALSE(t->limit_exceeded());
    t->release(5);
}

TEST(MemTestTest, TrackerHierarchy) {
    auto p = MemTracker::create_tracker(100);
    auto c1 = MemTracker::create_tracker(80, "c1", p);
    auto c2 = MemTracker::create_tracker(50, "c2", p);

    // everything below limits
    c1->consume(60);
    EXPECT_EQ(c1->consumption(), 60);
    EXPECT_FALSE(c1->limit_exceeded());
    EXPECT_FALSE(c1->any_limit_exceeded());
    EXPECT_EQ(c2->consumption(), 0);
    EXPECT_FALSE(c2->limit_exceeded());
    EXPECT_FALSE(c2->any_limit_exceeded());
    EXPECT_EQ(p->consumption(), 60);
    EXPECT_FALSE(p->limit_exceeded());
    EXPECT_FALSE(p->any_limit_exceeded());

    // p goes over limit
    c2->consume(50);
    EXPECT_EQ(c1->consumption(), 60);
    EXPECT_FALSE(c1->limit_exceeded());
    EXPECT_TRUE(c1->any_limit_exceeded());
    EXPECT_EQ(c2->consumption(), 50);
    EXPECT_FALSE(c2->limit_exceeded());
    EXPECT_TRUE(c2->any_limit_exceeded());
    EXPECT_EQ(p->consumption(), 110);
    EXPECT_TRUE(p->limit_exceeded());

    // c2 goes over limit, p drops below limit
    c1->release(20);
    c2->consume(10);
    EXPECT_EQ(c1->consumption(), 40);
    EXPECT_FALSE(c1->limit_exceeded());
    EXPECT_FALSE(c1->any_limit_exceeded());
    EXPECT_EQ(c2->consumption(), 60);
    EXPECT_TRUE(c2->limit_exceeded());
    EXPECT_TRUE(c2->any_limit_exceeded());
    EXPECT_EQ(p->consumption(), 100);
    EXPECT_FALSE(p->limit_exceeded());
    c1->release(40);
    c2->release(60);
}

TEST(MemTestTest, TrackerHierarchyTryConsume) {
    auto p = MemTracker::create_tracker(100);
    auto c1 = MemTracker::create_tracker(80, "c1", p);
    auto c2 = MemTracker::create_tracker(50, "c2", p);

    // everything below limits
    bool consumption = c1->try_consume(60).ok();
    EXPECT_EQ(consumption, true);
    EXPECT_EQ(c1->consumption(), 60);
    EXPECT_FALSE(c1->limit_exceeded());
    EXPECT_FALSE(c1->any_limit_exceeded());
    EXPECT_EQ(c2->consumption(), 0);
    EXPECT_FALSE(c2->limit_exceeded());
    EXPECT_FALSE(c2->any_limit_exceeded());
    EXPECT_EQ(p->consumption(), 60);
    EXPECT_FALSE(p->limit_exceeded());
    EXPECT_FALSE(p->any_limit_exceeded());

    // p goes over limit
    consumption = c2->try_consume(50).ok();
    EXPECT_EQ(consumption, false);
    EXPECT_EQ(c1->consumption(), 60);
    EXPECT_FALSE(c1->limit_exceeded());
    EXPECT_FALSE(c1->any_limit_exceeded());
    EXPECT_EQ(c2->consumption(), 0);
    EXPECT_FALSE(c2->limit_exceeded());
    EXPECT_FALSE(c2->any_limit_exceeded());
    EXPECT_EQ(p->consumption(), 60);
    EXPECT_FALSE(p->limit_exceeded());
    EXPECT_FALSE(p->any_limit_exceeded());

    // c2 goes over limit, p drops below limit
    c1->release(20);
    c2->consume(10);
    EXPECT_EQ(c1->consumption(), 40);
    EXPECT_FALSE(c1->limit_exceeded());
    EXPECT_FALSE(c1->any_limit_exceeded());
    EXPECT_EQ(c2->consumption(), 10);
    EXPECT_FALSE(c2->limit_exceeded());
    EXPECT_FALSE(c2->any_limit_exceeded());
    EXPECT_EQ(p->consumption(), 50);
    EXPECT_FALSE(p->limit_exceeded());

    c1->release(40);
    c2->release(10);
}

} // end namespace doris
