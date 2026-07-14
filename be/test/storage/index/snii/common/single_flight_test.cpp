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

#include "storage/index/snii/common/single_flight.h"

#include <gtest/gtest.h>

#include <atomic>
#include <latch>
#include <memory>
#include <thread>
#include <utility>
#include <vector>

namespace doris::snii {

// A lone caller leads, a second concurrent caller follows, and the follower reuses the
// leader's published result. The in-flight entry is cleared on publish.
TEST(SniiSingleFlight, LeadFollowReuse) {
    SingleFlight<int> sf;

    auto leader = sf.join_or_lead("k");
    EXPECT_FALSE(leader.has_value()); // first caller leads
    EXPECT_EQ(sf.inflight_size(), 1U);

    auto follower = sf.join_or_lead("k");
    ASSERT_TRUE(follower.has_value()); // second caller follows the in-flight leader
    EXPECT_EQ(sf.inflight_size(), 1U); // still one in-flight key

    sf.publish("k", 7);
    EXPECT_EQ(follower->get(), 7); // follower reuses the leader's result
    EXPECT_EQ(sf.inflight_size(), 0U);
}

// Different keys execute independently -- each first caller leads.
TEST(SniiSingleFlight, DistinctKeysLeadIndependently) {
    SingleFlight<int> sf;
    EXPECT_FALSE(sf.join_or_lead("a").has_value());
    EXPECT_FALSE(sf.join_or_lead("b").has_value());
    EXPECT_EQ(sf.inflight_size(), 2U);
    sf.publish("a", 1);
    sf.publish("b", 2);
    EXPECT_EQ(sf.inflight_size(), 0U);
}

// After a key is published, the next caller of the same key leads a fresh execution.
TEST(SniiSingleFlight, ReLeadAfterPublish) {
    SingleFlight<int> sf;
    EXPECT_FALSE(sf.join_or_lead("k").has_value());
    sf.publish("k", 1);
    EXPECT_EQ(sf.inflight_size(), 0U);
    EXPECT_FALSE(sf.join_or_lead("k").has_value()); // leads again
    EXPECT_EQ(sf.inflight_size(), 1U);
    sf.publish("k", 2);
}

// The motivating scenario: many threads issue the same key concurrently. Exactly one leads;
// all followers receive the single shared result (one shared_ptr payload, shared read-only).
TEST(SniiSingleFlight, ConcurrentCollapsesToOneLeader) {
    constexpr int kThreads = 16;
    SingleFlight<std::shared_ptr<int>> sf;

    std::atomic<int> leader_count {0};
    std::latch joined(kThreads);
    std::vector<std::shared_ptr<int>> results(kThreads);
    auto payload = std::make_shared<int>(42);

    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int i = 0; i < kThreads; ++i) {
        threads.emplace_back([&, i] {
            auto follow = sf.join_or_lead("hot-segment");
            const bool is_leader = !follow.has_value();
            joined.count_down();
            if (is_leader) {
                leader_count.fetch_add(1, std::memory_order_relaxed);
                joined.wait(); // ensure every thread has joined before we publish
                sf.publish("hot-segment", payload);
            } else {
                results[i] = follow->get();
            }
        });
    }
    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(leader_count.load(), 1); // only one open/execute happened
    EXPECT_EQ(sf.inflight_size(), 0U);
    for (int i = 0; i < kThreads; ++i) {
        if (results[i] != nullptr) {
            EXPECT_EQ(results[i], payload); // same shared object, not a recomputed copy
            EXPECT_EQ(*results[i], 42);
        }
    }
}

// A leader that fails publishes its failure; followers observe it (and would then fall back
// to computing independently in the production path).
TEST(SniiSingleFlight, ErrorResultPropagates) {
    SingleFlight<std::pair<bool, int>> sf;
    auto leader = sf.join_or_lead("k");
    ASSERT_FALSE(leader.has_value());
    auto follower = sf.join_or_lead("k");
    ASSERT_TRUE(follower.has_value());

    sf.publish("k", std::make_pair(false, 0)); // leader failed
    auto [ok, value] = follower->get();
    EXPECT_FALSE(ok);
    EXPECT_EQ(value, 0);
}

} // namespace doris::snii
