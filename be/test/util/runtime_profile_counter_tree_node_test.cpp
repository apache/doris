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

#include "util/runtime_profile_counter_tree_node.h"

#include <gtest/gtest.h>

#include "util/runtime_profile.h"

namespace doris {

class RuntimeProfileCounterTreeNodeTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Setup code if needed
    }

    void TearDown() override {
        // Cleanup code if needed
    }
};

TEST_F(RuntimeProfileCounterTreeNodeTest, TestEmpty) {
    RuntimeProfile::CounterMap counterMap;
    RuntimeProfile::ChildCounterMap childCounterMap;

    RuntimeProfileCounterTreeNode rootNode =
            RuntimeProfileCounterTreeNode::from_map(counterMap, childCounterMap, "root");

    ASSERT_EQ(rootNode.name, "root");
    ASSERT_EQ(rootNode.counter, nullptr);
    ASSERT_TRUE(rootNode.children.empty());
}

TEST_F(RuntimeProfileCounterTreeNodeTest, TestEmptyToThrift) {
    RuntimeProfileCounterTreeNode rootNode;

    std::vector<TCounter> tcounter;
    std::map<std::string, std::set<std::string>> child_counter_map;
    rootNode.to_thrift(tcounter, child_counter_map);

    ASSERT_TRUE(tcounter.empty());
    ASSERT_TRUE(child_counter_map.empty());
}

TEST_F(RuntimeProfileCounterTreeNodeTest, TestFromMap) {
    RuntimeProfile::CounterMap counterMap;
    RuntimeProfile::ChildCounterMap childCounterMap;

    // Create some mock counters
    auto rootCounter = std::make_unique<RuntimeProfile::Counter>(TUnit::UNIT);
    auto childCounter = std::make_unique<RuntimeProfile::Counter>(TUnit::UNIT);

    counterMap["root"] = rootCounter.get();
    counterMap["child"] = childCounter.get();

    childCounterMap["root"].insert("child");

    RuntimeProfileCounterTreeNode rootNode =
            RuntimeProfileCounterTreeNode::from_map(counterMap, childCounterMap, "root");

    ASSERT_EQ(rootNode.name, "root");
    ASSERT_EQ(*rootNode.counter, *rootCounter);
    ASSERT_EQ(rootNode.children.size(), 1);
    ASSERT_EQ(rootNode.children[0].name, "child");
    ASSERT_EQ(*rootNode.children[0].counter, *childCounter);
}

TEST_F(RuntimeProfileCounterTreeNodeTest, TestPruneTheTree) {
    RuntimeProfile::CounterMap counterMap;
    RuntimeProfile::ChildCounterMap childCounterMap;

    // Create some mock counters
    auto rootCounter = std::make_unique<RuntimeProfile::Counter>(TUnit::UNIT);
    auto childCounter = std::make_unique<RuntimeProfile::Counter>(TUnit::UNIT);
    auto grandChildCounter = std::make_unique<RuntimeProfile::Counter>(TUnit::UNIT);

    counterMap["root"] = rootCounter.get();
    counterMap["child"] = childCounter.get();
    counterMap["grandchild"] = grandChildCounter.get();

    childCounterMap["root"].insert("child");
    childCounterMap["child"].insert("grandchild");

    RuntimeProfileCounterTreeNode rootNode =
            RuntimeProfileCounterTreeNode::from_map(counterMap, childCounterMap, "root");

    // Set levels
    rootCounter->set_level(0);
    childCounter->set_level(1);
    grandChildCounter->set_level(2);

    RuntimeProfileCounterTreeNode prunedNode =
            RuntimeProfileCounterTreeNode::prune_the_tree(rootNode, 1);

    ASSERT_EQ(prunedNode.name, "root");
    ASSERT_EQ(prunedNode.children.size(), 1);
    ASSERT_EQ(prunedNode.children[0].name, "child");
    ASSERT_TRUE(prunedNode.children[0].children.empty());
}

TEST_F(RuntimeProfileCounterTreeNodeTest, TestToThrift) {
    RuntimeProfile::CounterMap counterMap;
    RuntimeProfile::ChildCounterMap childCounterMap;

    // Create some mock counters
    auto rootCounter = std::make_unique<RuntimeProfile::Counter>(TUnit::UNIT);
    auto childCounter = std::make_unique<RuntimeProfile::Counter>(TUnit::UNIT);

    counterMap["root"] = rootCounter.get();
    counterMap["child"] = childCounter.get();

    childCounterMap["root"].insert("child");

    RuntimeProfileCounterTreeNode rootNode =
            RuntimeProfileCounterTreeNode::from_map(counterMap, childCounterMap, "root");

    std::vector<TCounter> tcounter;
    std::map<std::string, std::set<std::string>> child_counter_map;

    rootNode.to_thrift(tcounter, child_counter_map);

    ASSERT_EQ(tcounter.size(), 2);
    ASSERT_EQ(tcounter[0].name, "root");
    ASSERT_EQ(tcounter[1].name, "child");

    ASSERT_EQ(child_counter_map.size(), 1);
    ASSERT_EQ(child_counter_map["root"].size(), 1);
    ASSERT_EQ(*child_counter_map["root"].begin(), "child");
}

TEST_F(RuntimeProfileCounterTreeNodeTest, HighWaterMarkCounterToThrift) {
    RuntimeProfile::CounterMap counterMap;
    RuntimeProfile::ChildCounterMap childCounterMap;
    /*
    ""
        "root"
            "child"
            "childPeak"
        "rootPeak"
    */

    // Create some mock counters
    auto rootCounter = std::make_unique<RuntimeProfile::HighWaterMarkCounter>(
            TUnit::UNIT, 0, RuntimeProfile::ROOT_COUNTER, 0, 0);
    auto childCounter =
            std::make_unique<RuntimeProfile::HighWaterMarkCounter>(TUnit::UNIT, 1, "root", 0, 0);

    counterMap["root"] = rootCounter.get();
    counterMap["child"] = childCounter.get();

    childCounterMap[RuntimeProfile::ROOT_COUNTER].insert("root");
    childCounterMap["root"].insert("child");

    rootCounter->add(10);
    rootCounter->set(5);

    childCounter->add(100);
    childCounter->set(50);

    RuntimeProfileCounterTreeNode rootNode = RuntimeProfileCounterTreeNode::from_map(
            counterMap, childCounterMap, RuntimeProfile::ROOT_COUNTER);

    std::vector<TCounter> tcounter;
    std::map<std::string, std::set<std::string>> child_counter_map;

    rootNode.to_thrift(tcounter, child_counter_map);

    /*
    ROOT_COUNTER
        root
            child
    */

    /* 
    tcounter: root, rootPeak, child, childPeak
    child_counter_map:
        ROOT_COUNTER -> {root, rootPeak}
        root -> {child, childPeak}
    */

    ASSERT_EQ(tcounter.size(), 4);
    EXPECT_EQ(tcounter[0].name, "root");
    EXPECT_EQ(tcounter[0].value, 5);
    EXPECT_EQ(tcounter[1].name, "rootPeak");
    EXPECT_EQ(tcounter[1].value, 10);
    EXPECT_EQ(tcounter[2].name, "child");
    EXPECT_EQ(tcounter[2].value, 50);
    EXPECT_EQ(tcounter[3].name, "childPeak");
    EXPECT_EQ(tcounter[3].value, 100);

    for (const auto& entry : child_counter_map) {
        std::cout << entry.first << " -> {";
        for (const auto& child : entry.second) {
            std::cout << child << ", ";
        }
        std::cout << "}" << std::endl;
    }

    ASSERT_EQ(child_counter_map.size(), 2);
    ASSERT_EQ(child_counter_map[RuntimeProfile::ROOT_COUNTER].size(), 2);
    for (const auto& counter_name : child_counter_map[RuntimeProfile::ROOT_COUNTER]) {
        ASSERT_TRUE(counter_name == "root" || counter_name == "rootPeak");
    }
    ASSERT_EQ(child_counter_map["root"].size(), 2);
    for (const auto& counter_name : child_counter_map["root"]) {
        ASSERT_TRUE(counter_name == "child" || counter_name == "childPeak");
    }
}

TEST_F(RuntimeProfileCounterTreeNodeTest, NonZeroCounterToThrfit) {
    RuntimeProfile::CounterMap counterMap;
    RuntimeProfile::ChildCounterMap childCounterMap;
    /*
    ""
        "root"
            "child"
    */

    // Create some mock counters
    auto rootCounter = std::make_unique<RuntimeProfile::NonZeroCounter>(
            TUnit::UNIT, 0, RuntimeProfile::ROOT_COUNTER, 0);
    auto childCounter = std::make_unique<RuntimeProfile::NonZeroCounter>(TUnit::UNIT, 1, "root", 0);

    counterMap["root"] = rootCounter.get();
    counterMap["child"] = childCounter.get();

    childCounterMap[RuntimeProfile::ROOT_COUNTER].insert("root");
    childCounterMap["root"].insert("child");

    rootCounter->update(-10);
    childCounter->update(10);

    RuntimeProfileCounterTreeNode rootNode = RuntimeProfileCounterTreeNode::from_map(
            counterMap, childCounterMap, RuntimeProfile::ROOT_COUNTER);

    std::vector<TCounter> tcounter;
    std::map<std::string, std::set<std::string>> child_counter_map;

    rootNode.to_thrift(tcounter, child_counter_map);

    /*
    ROOT_COUNTER
        root(-11)
            child(10)
    */

    /* 
    tcounter: empty
    child_counter_map:
        ROOT_COUNTER -> {}
    */

    ASSERT_EQ(tcounter.size(), 0);

    for (const auto& entry : child_counter_map) {
        std::cout << entry.first << " -> {";
        for (const auto& child : entry.second) {
            std::cout << child << ", ";
        }
        std::cout << "}" << std::endl;
    }

    ASSERT_EQ(child_counter_map.size(), 1);
    ASSERT_EQ(child_counter_map[RuntimeProfile::ROOT_COUNTER].size(), 0);
}

TEST_F(RuntimeProfileCounterTreeNodeTest, DescriptionCounter) {
    RuntimeProfile::CounterMap counterMap;
    RuntimeProfile::ChildCounterMap childCounterMap;
    /*
    ""
        "root"
            "description_entry"
    */

    auto rootCounter = std::make_unique<RuntimeProfile::Counter>(TUnit::UNIT);
    auto descriptionEntry = std::make_unique<RuntimeProfile::DescriptionEntry>(
            "description_entry", "Updated description");

    counterMap["root"] = rootCounter.get();
    counterMap["description_entry"] = descriptionEntry.get();

    childCounterMap[RuntimeProfile::ROOT_COUNTER].insert("root");
    childCounterMap["root"].insert("description_entry");

    RuntimeProfileCounterTreeNode rootNode = RuntimeProfileCounterTreeNode::from_map(
            counterMap, childCounterMap, RuntimeProfile::ROOT_COUNTER);

    std::vector<TCounter> tcounter;
    std::map<std::string, std::set<std::string>> child_counter_map;

    rootNode.to_thrift(tcounter, child_counter_map);

    /*
    ROOT_COUNTER
        root
            description_entry
    */

    /* 
    tcounter: root, description_entry
    child_counter_map:
        ROOT_COUNTER -> {root}
        root -> {description_entry}
    */

    for (const auto& counter : tcounter) {
        std::cout << "Counter: " << counter.name;
        if (counter.name == "description_entry") {
            EXPECT_TRUE(counter.__isset.description);
            EXPECT_EQ(counter.description, "Updated description");
        }
        if (counter.__isset.description) {
            std::cout << ", Description: " << counter.description;
        }
        std::cout << std::endl;
    }

    ASSERT_EQ(tcounter.size(), 2);
    EXPECT_EQ(tcounter[0].name, "root");
    EXPECT_EQ(tcounter[1].name, "description_entry");

    ASSERT_TRUE(tcounter[1].__isset.description);
    EXPECT_EQ(tcounter[1].description, "Updated description");
    EXPECT_EQ(tcounter[1].level, 2);

    ASSERT_EQ(child_counter_map.size(), 2);
    ASSERT_EQ(child_counter_map[RuntimeProfile::ROOT_COUNTER].size(), 1);
    ASSERT_EQ(child_counter_map["root"].size(), 1);
    ASSERT_EQ(*child_counter_map["root"].begin(), "description_entry");
}

TEST_F(RuntimeProfileCounterTreeNodeTest, ConditionCounterTest) {
    auto min_counter = std::make_unique<RuntimeProfile::ConditionCounter>(
            TUnit::UNIT, [](int64_t _c, int64_t c) { return c < _c; }, 100000, 1000000);

    min_counter->conditional_update(100, 1);
    ASSERT_EQ(min_counter->value(), 1);
    min_counter->conditional_update(200, 2);
    ASSERT_EQ(min_counter->value(), 1);
    min_counter->conditional_update(10, 3);
    ASSERT_EQ(min_counter->value(), 3);

    auto max_counter = std::make_unique<RuntimeProfile::ConditionCounter>(
            TUnit::UNIT, [](int64_t _c, int64_t c) { return c > _c; });

    max_counter->conditional_update(10, 4);
    ASSERT_EQ(max_counter->value(), 4);
    max_counter->conditional_update(1, 5);
    ASSERT_EQ(max_counter->value(), 4);
    max_counter->conditional_update(100, 6);
    ASSERT_EQ(max_counter->value(), 6);
    max_counter->conditional_update(10, 7);
    ASSERT_EQ(max_counter->value(), 6);
}
} // namespace doris