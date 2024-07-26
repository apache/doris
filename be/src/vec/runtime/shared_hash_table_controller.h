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
#include "vec/core/block.h"

namespace doris {

class RuntimeState;
class MinMaxFuncBase;
class HybridSetBase;
class BloomFilterFuncBase;
class BitmapFilterFuncBase;

namespace pipeline {
class Dependency;
}

struct RuntimeFilterContext {
    std::shared_ptr<MinMaxFuncBase> minmax_func;
    std::shared_ptr<HybridSetBase> hybrid_set;
    std::shared_ptr<BloomFilterFuncBase> bloom_filter_func;
    std::shared_ptr<BitmapFilterFuncBase> bitmap_filter_func;
    bool ignored = false;
};

using RuntimeFilterContextSPtr = std::shared_ptr<RuntimeFilterContext>;

namespace vectorized {

class Arena;

struct SharedHashTableContext {
    SharedHashTableContext()
            : hash_table_variants(nullptr), block(std::make_shared<vectorized::Block>()) {}

    Status status;
    std::shared_ptr<Arena> arena;
    std::shared_ptr<void> hash_table_variants;
    std::shared_ptr<Block> block;
    std::shared_ptr<std::vector<uint32_t>> build_indexes_null;
    std::map<int, RuntimeFilterContextSPtr> runtime_filters;
    std::atomic<bool> signaled = false;
    bool short_circuit_for_null_in_probe_side = false;
};

using SharedHashTableContextPtr = std::shared_ptr<SharedHashTableContext>;

class SharedHashTableController {
public:
    /// set hash table builder's fragment instance id and consumers' fragment instance id
    void set_builder_and_consumers(TUniqueId builder, int node_id);
    TUniqueId get_builder_fragment_instance_id(int my_node_id);
    SharedHashTableContextPtr get_context(int my_node_id);
    void signal(int my_node_id);
    void signal_finish(int my_node_id);
    void append_dependency(int node_id, std::shared_ptr<pipeline::Dependency> dep,
                           std::shared_ptr<pipeline::Dependency> finish_dep) {
        std::lock_guard<std::mutex> lock(_mutex);
        if (!_dependencies.contains(node_id)) {
            _dependencies.insert({node_id, {}});
            _finish_dependencies.insert({node_id, {}});
        }
        _dependencies[node_id].push_back(dep);
        _finish_dependencies[node_id].push_back(finish_dep);
    }

private:
    std::mutex _mutex;
    // For pipelineX, we update all dependencies once hash table is built;
    std::map<int /*node id*/, std::vector<std::shared_ptr<pipeline::Dependency>>> _dependencies;
    std::map<int /*node id*/, std::vector<std::shared_ptr<pipeline::Dependency>>>
            _finish_dependencies;
    std::condition_variable _cv;
    std::map<int /*node id*/, TUniqueId /*fragment instance id*/> _builder_fragment_ids;
    std::map<int /*node id*/, SharedHashTableContextPtr> _shared_contexts;
};

} // namespace vectorized
} // namespace doris
