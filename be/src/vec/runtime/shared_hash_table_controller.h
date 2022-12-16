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

#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <vector>

#include "vec/core/block.h"

namespace doris {

class RuntimeState;
class TUniqueId;
class MinMaxFuncBase;
class HybridSetBase;
class BloomFilterFuncBase;
class BitmapFilterFuncBase;

namespace vectorized {

class VExprContext;

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
    TUniqueId get_builder_fragment_instance_id(int my_node_id);
    SharedHashTableContextPtr get_context(int my_node_id);
    void signal(int my_node_id);
    Status wait_for_signal(RuntimeState* state, const SharedHashTableContextPtr& context);
    bool should_build_hash_table(const TUniqueId& fragment_instance_id, int my_node_id);

private:
    std::mutex _mutex;
    std::condition_variable _cv;
    std::map<int /*node id*/, TUniqueId /*fragment instance id*/> _builder_fragment_ids;
    std::map<int /*node id*/, SharedHashTableContextPtr> _shared_contexts;
};

} // namespace vectorized
} // namespace doris