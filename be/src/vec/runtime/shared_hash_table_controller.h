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
#include "runtime_filter/runtime_filter_definitions.h"
#include "runtime_filter/runtime_filter_wrapper.h"
#include "vec/core/block.h"

namespace doris {
#include "common/compile_check_begin.h"

class RuntimeState;
class MinMaxFuncBase;
class HybridSetBase;
class BloomFilterFuncBase;
class BitmapFilterFuncBase;

namespace pipeline {
class Dependency;
}
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
    std::map<int, std::shared_ptr<RuntimeFilterWrapper>> runtime_filters;
    std::atomic<bool> signaled = false;
    bool short_circuit_for_null_in_probe_side = false;

    std::mutex mutex;
    std::vector<std::shared_ptr<pipeline::Dependency>> finish_dependencies;
};

using SharedHashTableContextPtr = std::shared_ptr<SharedHashTableContext>;

} // namespace vectorized
} // namespace doris

#include "common/compile_check_end.h"
