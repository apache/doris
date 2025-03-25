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

#include <gen_cpp/RuntimeProfile_types.h>
#include <gtest/gtest.h>

#include <atomic>
#include <cstdlib>
#include <thread>

#include "util/runtime_profile.h"

namespace doris {
class RuntimeProfileProfileLevelTest : public testing::Test {
public:
    RuntimeProfileProfileLevelTest() = default;
    ~RuntimeProfileProfileLevelTest() override = default;
};

TEST_F(RuntimeProfileProfileLevelTest, EmptyProfile) {
    RuntimeProfile profile("test");
    TRuntimeProfileTree tprofile;
    // Empty RuntimeProfile it self will have a ROOT_COUNTER.
    profile.to_thrift(&tprofile, 0);
    ASSERT_EQ(tprofile.nodes.size(), 1);
    const TRuntimeProfileNode& tnode = tprofile.nodes[0];
    ASSERT_EQ(tnode.counters.size(), 0);
    ASSERT_EQ(tnode.child_counters_map.size(), 0);

    tprofile.nodes.clear();
    profile.to_thrift(&tprofile, 1);
    ASSERT_EQ(tprofile.nodes.size(), 1);
    const TRuntimeProfileNode& tnode1 = tprofile.nodes[0];
    ASSERT_EQ(tnode1.counters.size(), 0);
    ASSERT_EQ(tnode1.child_counters_map.size(), 0);
}
TEST_F(RuntimeProfileProfileLevelTest, BasicTestNoChildCounter) {
    /*
     ""
        counter01-level-0
        counter02-level-0
        counter11-level-1
        counter12-level-1
        counter21-default-levle-2
        counter22-default-levle-2
      */
    // Create RuntimeProfile with different counter with different level
    RuntimeProfile profile("test");
    [[maybe_unused]] auto* counter01 = profile.add_counter("counter01-level-0", TUnit::UNIT, "", 0);
    [[maybe_unused]] auto* counter02 = profile.add_counter("counter02-level-0", TUnit::UNIT, "", 0);

    [[maybe_unused]] auto* counter11 = profile.add_counter("counter11-level-1", TUnit::UNIT, "", 1);
    [[maybe_unused]] auto* counter12 = profile.add_counter("counter12-level-1", TUnit::UNIT, "", 1);

    [[maybe_unused]] auto* counter21 =
            profile.add_counter("counter21-default-levle-2", TUnit::UNIT);
    [[maybe_unused]] auto* counter22 =
            profile.add_counter("counter22-default-levle-2", TUnit::UNIT);

    // Call to_thrift will different profile level.
    TRuntimeProfileTree tprofile;
    profile.to_thrift(&tprofile, 0);
    ASSERT_EQ(tprofile.nodes.size(), 1);
    TRuntimeProfileNode& tnode = tprofile.nodes[0];
    ASSERT_EQ(tnode.counters.size(), 2);
    ASSERT_EQ(tnode.child_counters_map.size(), 1);
    auto entry = tnode.child_counters_map.begin();
    ASSERT_TRUE(entry->first == RuntimeProfile::ROOT_COUNTER);
    // After prune, counters with level 1 and 2 should be removed.
    ASSERT_EQ(entry->second.size(), 2);
    for (const auto& counter : tnode.counters) {
        ASSERT_TRUE(entry->second.contains(counter.name));
    }
    for (const auto& child_counter_name : entry->second) {
        ASSERT_TRUE(child_counter_name == "counter01-level-0" ||
                    child_counter_name == "counter02-level-0");
    }

    tprofile.nodes.clear();
    profile.to_thrift(&tprofile, 1);

    // No child RuntimeProfile, so we just have one node.
    ASSERT_EQ(tprofile.nodes.size(), 1);
    tnode = tprofile.nodes[0];
    // counter21-default-levle-2
    // counter22-default-levle-2
    // should be removed.
    ASSERT_EQ(tnode.counters.size(), 4);
    // Only one parent counter, so child_counter_map should have only one entry.
    ASSERT_EQ(tnode.child_counters_map.size(), 1);
    entry = tnode.child_counters_map.begin();
    // ROOT_COUNTER is the only parent counter.
    ASSERT_TRUE(entry->first == RuntimeProfile::ROOT_COUNTER);
    // ROOT_COUNTER has four children.
    ASSERT_EQ(entry->second.size(), 4);

    for (const auto& counter : tnode.counters) {
        ASSERT_TRUE(entry->second.contains(counter.name));
    }
    for (const auto& child_counter_name : entry->second) {
        ASSERT_TRUE(child_counter_name == "counter01-level-0" ||
                    child_counter_name == "counter02-level-0" ||
                    child_counter_name == "counter11-level-1" ||
                    child_counter_name == "counter12-level-1");
    }

    tprofile.nodes.clear();
    profile.to_thrift(&tprofile, 2);
    ASSERT_EQ(tprofile.nodes.size(), 1);
    const TRuntimeProfileNode& tnode2 = tprofile.nodes[0];
    ASSERT_EQ(tnode2.counters.size(), 6);
    ASSERT_EQ(tnode2.child_counters_map.size(), 1);
    const auto entry2 = tnode2.child_counters_map.begin();
    ASSERT_TRUE(entry2->first == RuntimeProfile::ROOT_COUNTER);
    ASSERT_EQ(entry2->second.size(), 6);
    for (const auto& counter : tnode2.counters) {
        ASSERT_TRUE(entry2->second.contains(counter.name));
    }
    for (const auto& child_counter_name : entry2->second) {
        ASSERT_TRUE(child_counter_name == "counter01-level-0" ||
                    child_counter_name == "counter02-level-0" ||
                    child_counter_name == "counter11-level-1" ||
                    child_counter_name == "counter12-level-1" ||
                    child_counter_name == "counter21-default-levle-2" ||
                    child_counter_name == "counter22-default-levle-2");
    }

    tprofile.nodes.clear();
    profile.to_thrift(&tprofile, 3);
    ASSERT_EQ(tprofile.nodes.size(), 1);
    const TRuntimeProfileNode& tnode3 = tprofile.nodes[0];
    ASSERT_EQ(tnode3.counters.size(), 6);
    ASSERT_EQ(tnode3.child_counters_map.size(), 1);
    const auto entry3 = tnode3.child_counters_map.begin();
    ASSERT_TRUE(entry3->first == RuntimeProfile::ROOT_COUNTER);
    ASSERT_EQ(entry3->second.size(), 6);
    for (const auto& counter : tnode3.counters) {
        ASSERT_TRUE(entry3->second.contains(counter.name));
    }

    for (const auto& child_counter_name : entry3->second) {
        ASSERT_TRUE(child_counter_name == "counter01-level-0" ||
                    child_counter_name == "counter02-level-0" ||
                    child_counter_name == "counter11-level-1" ||
                    child_counter_name == "counter12-level-1" ||
                    child_counter_name == "counter21-default-levle-2" ||
                    child_counter_name == "counter22-default-levle-2");
    }
}

TEST_F(RuntimeProfileProfileLevelTest, BasicTestWithChildCounter) {
    /*
        ""
                counter01
                        counter01_child01
                        counter01_child02
                counter02
                        counter02_child01
      */
    // Create RuntimeProfile with different counter with different level
    // They must have other counter as its parent counter.
    RuntimeProfile profile("test");
    [[maybe_unused]] auto* counter01 =
            profile.add_counter("counter01", TUnit::UNIT, RuntimeProfile::ROOT_COUNTER, 0);
    [[maybe_unused]] auto* counter02 =
            profile.add_counter("counter02", TUnit::UNIT, RuntimeProfile::ROOT_COUNTER, 0);

    [[maybe_unused]] auto* counter01_child01 =
            profile.add_counter("counter01_child01", TUnit::UNIT, "counter01", 0);
    [[maybe_unused]] auto* counter01_child02 =
            profile.add_counter("counter01_child02", TUnit::UNIT, "counter01", 0);
    [[maybe_unused]] auto* counter02_child01 =
            profile.add_counter("counter02_child01", TUnit::UNIT, "counter02", 0);

    TRuntimeProfileTree tprofile;
    profile.to_thrift(&tprofile, 0);
    ASSERT_EQ(tprofile.nodes.size(), 1);
    TRuntimeProfileNode& tnode = tprofile.nodes[0];
    ASSERT_EQ(tnode.name, "test");
    ASSERT_EQ(tnode.counters.size(), 5);
    ASSERT_EQ(tnode.child_counters_map.size(), 3);

    std::set<std::string> root_counter_children =
            tnode.child_counters_map[RuntimeProfile::ROOT_COUNTER];
    ASSERT_EQ(root_counter_children.size(), 2);

    std::set<std::string> child01_children = tnode.child_counters_map["counter01"];
    ASSERT_EQ(child01_children.size(), 2);

    std::set<std::string> child02_children = tnode.child_counters_map["counter02"];
    ASSERT_EQ(child02_children.size(), 1);

    [[maybe_unused]] auto* counter01_child11 =
            profile.add_counter("counter01_child11", TUnit::UNIT, "counter01", 1);
    [[maybe_unused]] auto* counter01_child12 =
            profile.add_counter("counter01_child12", TUnit::UNIT, "counter01", 1);
    [[maybe_unused]] auto* counter01_child13 =
            profile.add_counter("counter01_child13", TUnit::UNIT, "counter02", 1);

    tprofile.nodes.clear();
    profile.to_thrift(&tprofile, 1);
    ASSERT_EQ(tprofile.nodes.size(), 1);
    const TRuntimeProfileNode& tnode1 = tprofile.nodes[0];
    ASSERT_EQ(tnode1.counters.size(), 8);
    ASSERT_EQ(tnode1.child_counters_map.size(), 3);
    root_counter_children = tnode.child_counters_map[RuntimeProfile::ROOT_COUNTER];
    ASSERT_EQ(root_counter_children.size(), 2);

    child01_children = tnode.child_counters_map["counter01"];
    ASSERT_EQ(child01_children.size(), 4);

    child02_children = tnode.child_counters_map["counter02"];
    ASSERT_EQ(child02_children.size(), 2);
}

TEST_F(RuntimeProfileProfileLevelTest, ConcurrentTest) {
    RuntimeProfile profile("ConcurrentTest");
    std::atomic_bool stop {false};

    std::thread add_counter_thread([&profile, &stop]() {
        int idx = 0;
        while (!stop) {
            profile.add_counter("counter" + std::to_string(idx), TUnit::UNIT, "", 1);
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            idx++;
        }
    });

    std::thread to_thrift_thread([&profile, &stop]() {
        while (!stop) {
            TRuntimeProfileTree tprofile;
            profile.to_thrift(&tprofile, 2);
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    });

    std::this_thread::sleep_for(std::chrono::seconds(1));

    stop = true;

    add_counter_thread.join();
    to_thrift_thread.join();
}
} // namespace doris
