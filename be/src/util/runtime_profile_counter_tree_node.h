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

#pragma once
#include <gen_cpp/RuntimeProfile_types.h>

#include <string>

#include "util/runtime_profile.h"

namespace doris {

class RuntimeProfileCounterTreeNode {
public:
    using CounterMap = std::map<std::string, RuntimeProfile::Counter*>;
    using ChildCounterMap = std::map<std::string, std::set<std::string>>;

    RuntimeProfileCounterTreeNode() = default;
    ~RuntimeProfileCounterTreeNode() {
        // counter is not owned by this class
        counter = nullptr;
    };

    // Create a tree node from a counter map and a child counter map.
    static RuntimeProfileCounterTreeNode from_map(const CounterMap& counterMap,
                                                  const ChildCounterMap& childCounterMap,
                                                  const std::string& rootName);

    // Prune the tree by:
    // 1. Remove all leaf nodes whose level is greater than the given level.
    // 2. Remove all nodes whose children are all pruned.
    static RuntimeProfileCounterTreeNode prune_the_tree(RuntimeProfileCounterTreeNode node,
                                                        int64_t level);

    // Convert the counter tree to a list of TCounter objects and a map of tree topology.
    void to_thrift(std::vector<TCounter>& tcounter,
                   std::map<std::string, std::set<std::string>>& child_counter_map) const;

    void to_proto(
            google::protobuf::RepeatedPtrField<PProfileCounter>* proto_counters,
            google::protobuf::Map<std::string, PProfileChildCounterSet>* child_counter_map) const;

    // Conver this node to a TCounter object.
    TCounter to_thrift() const;

    PProfileCounter to_proto() const;

private:
    std::string name;
    // counter is not owned by this class
    RuntimeProfile::Counter* counter = nullptr;

    std::vector<RuntimeProfileCounterTreeNode> children;
};

} // namespace doris
