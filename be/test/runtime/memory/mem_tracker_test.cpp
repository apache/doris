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

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <memory>

#include "gtest/gtest_pred_impl.h"
#include "runtime/memory/mem_tracker_limiter.h"

namespace doris {

TEST(MemTrackerTest, SingleTrackerNoLimit) {
    auto t = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::GLOBAL);
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

TEST(MemTrackerTest, SingleTrackerWithLimit) {
    auto t = std::make_unique<MemTrackerLimiter>(MemTrackerLimiter::Type::GLOBAL, "limit tracker",
                                                 11);
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

} // end namespace doris
