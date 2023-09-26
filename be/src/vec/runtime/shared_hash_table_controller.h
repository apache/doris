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

#include <gen_cpp/Types_types.h>

#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <vector>

#include "common/status.h"

namespace doris {

class RuntimeState;
class MinMaxFuncBase;
class HybridSetBase;
class BloomFilterFuncBase;
class BitmapFilterFuncBase;

namespace pipeline {
class SharedHashTableDependency;
}

namespace vectorized {

class Arena;
class Block;

struct SharedRuntimeFilterContext {
    std::shared_ptr<MinMaxFuncBase> minmax_func;
    std::shared_ptr<HybridSetBase> hybrid_set;
    std::shared_ptr<BloomFilterFuncBase> bloom_filter_func;
    std::shared_ptr<BitmapFilterFuncBase> bitmap_filter_func;
};

struct SharedHashTableContext {
    SharedHashTableContext()
            : hash_table_variants(nullptr),
              signaled(false),
              short_circuit_for_null_in_probe_side(false) {}

    Status status;
    std::shared_ptr<Arena> arena;
    std::shared_ptr<void> hash_table_variants;
    std::shared_ptr<std::vector<Block>> blocks;
    std::map<int, SharedRuntimeFilterContext> runtime_filters;
    bool signaled;
    bool short_circuit_for_null_in_probe_side;
};

using SharedHashTableContextPtr = std::shared_ptr<SharedHashTableContext>;

class SharedHashTableController {
public:
    /// set hash table builder's fragment instance id and consumers' fragment instance id
    void set_builder_and_consumers(TUniqueId builder, int node_id);
    TUniqueId get_builder_fragment_instance_id(int my_node_id);
    SharedHashTableContextPtr get_context(int my_node_id);
    void signal(int my_node_id);
    void signal(int my_node_id, Status status);
    Status wait_for_signal(RuntimeState* state, const SharedHashTableContextPtr& context);
    bool should_build_hash_table(const TUniqueId& fragment_instance_id, int my_node_id);
    void set_pipeline_engine_enabled(bool enabled) { _pipeline_engine_enabled = enabled; }
    void append_dependency(int node_id, std::shared_ptr<pipeline::SharedHashTableDependency> dep) {
        std::lock_guard<std::mutex> lock(_mutex);
        _dependencies[node_id].push_back(dep);
    }

private:
    bool _pipeline_engine_enabled = false;
    std::mutex _mutex;
    // For pipelineX, we update all dependencies once hash table is built;
    std::map<int /*node id*/, std::vector<std::shared_ptr<pipeline::SharedHashTableDependency>>>
            _dependencies;
    std::condition_variable _cv;
    std::map<int /*node id*/, TUniqueId /*fragment instance id*/> _builder_fragment_ids;
    std::map<int /*node id*/, SharedHashTableContextPtr> _shared_contexts;
};

} // namespace vectorized
} // namespace doris
