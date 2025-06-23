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

#include <gen_cpp/RuntimeProfile_types.h>

#include <string>

#include "util/runtime_profile.h"

namespace doris {

RuntimeProfileCounterTreeNode RuntimeProfileCounterTreeNode::from_map(
        const CounterMap& counterMap, const ChildCounterMap& childCounterMap,
        const std::string& rootName) {
    RuntimeProfileCounterTreeNode rootNode;
    rootNode.name = rootName;
    if (childCounterMap.empty() || childCounterMap.empty()) {
        return rootNode;
    }
    // ROOT_COUNTER is a special case, it does not have any counter.
    if (rootName == RuntimeProfile::ROOT_COUNTER) {
        rootNode.counter = nullptr;
    } else {
        rootNode.counter = counterMap.at(rootName);
    }

    auto it = childCounterMap.find(rootName);
    if (it != childCounterMap.end()) {
        for (const auto& childName : it->second) {
            rootNode.children.emplace_back(from_map(counterMap, childCounterMap, childName));
        }
    }

    return rootNode;
}

// Prune the tree by:
// 1. Remove all leaf nodes whose level is greater than the given level.
// 2. Remove all nodes whose children are all pruned.
RuntimeProfileCounterTreeNode RuntimeProfileCounterTreeNode::prune_the_tree(
        RuntimeProfileCounterTreeNode node, int level) {
    // Iterate through the children and prune them recursively
    for (auto it = node.children.begin(); it != node.children.end();) {
        *it = prune_the_tree(*it, level);
        if (it->children.empty() && it->counter->level() > level) {
            it = node.children.erase(it);
        } else {
            ++it;
        }
    }
    return node;
}

void RuntimeProfileCounterTreeNode::to_thrift(
        std::vector<TCounter>& tcounter,
        std::map<std::string, std::set<std::string>>& child_counter_map) const {
    if (name != RuntimeProfile::ROOT_COUNTER && counter != nullptr) {
        if (auto* highWaterMarkCounter =
                    dynamic_cast<RuntimeProfile::HighWaterMarkCounter*>(counter)) {
            // HighWaterMarkCounter will convert itself to two counters, one is current, the other is peak.
            tcounter.push_back(highWaterMarkCounter->to_thrift(name));
            tcounter.push_back(highWaterMarkCounter->to_thrift_peak(name + "Peak"));
            child_counter_map[highWaterMarkCounter->parent_name()].insert(name + "Peak");
        } else if (auto* nonZeroCounter = dynamic_cast<RuntimeProfile::NonZeroCounter*>(counter)) {
            if (nonZeroCounter->value() > 0) {
                tcounter.push_back(to_thrift());
            } else {
                // Erase non-zero counter from its parent's child counter map.
                child_counter_map[nonZeroCounter->parent_name()].erase(name);
                // Adn skipping all child conter of this counter.
                return;
            }
        } else {
            tcounter.push_back(to_thrift());
        }
    }

    for (const auto& child : children) {
        // insert before child doing to_thrift, so that we can remove it if it is zero.
        (child_counter_map)[name].insert(child.name);
        child.to_thrift(tcounter, child_counter_map);
    }
}

TCounter RuntimeProfileCounterTreeNode::to_thrift() const {
    TCounter tcounter;
    if (counter != nullptr) {
        tcounter = counter->to_thrift(name);
    } else {
        tcounter.name = name;
    }
    return tcounter;
}
} // namespace doris
