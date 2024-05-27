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

#include "shared_hash_table_controller.h"

#include <glog/logging.h>
#include <runtime/runtime_state.h>
// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <utility>

#include "pipeline/exec/hashjoin_build_sink.h"

namespace doris::vectorized {

void SharedHashTableController::set_builder_and_consumers(TUniqueId builder, int node_id) {
    // Only need to set builder and consumers with pipeline engine enabled.
    std::lock_guard<std::mutex> lock(_mutex);
    DCHECK(_builder_fragment_ids.find(node_id) == _builder_fragment_ids.cend());
    _builder_fragment_ids.insert({node_id, builder});
}

SharedHashTableContextPtr SharedHashTableController::get_context(int my_node_id) {
    std::lock_guard<std::mutex> lock(_mutex);
    if (!_shared_contexts.contains(my_node_id)) {
        _shared_contexts.insert({my_node_id, std::make_shared<SharedHashTableContext>()});
    }
    return _shared_contexts[my_node_id];
}

void SharedHashTableController::signal(int my_node_id) {
    std::lock_guard<std::mutex> lock(_mutex);
    auto it = _shared_contexts.find(my_node_id);
    if (it != _shared_contexts.cend()) {
        it->second->signaled = true;
        _shared_contexts.erase(it);
    }
    for (auto& dep : _dependencies[my_node_id]) {
        dep->set_ready();
    }
    _cv.notify_all();
}

void SharedHashTableController::signal_finish(int my_node_id) {
    std::lock_guard<std::mutex> lock(_mutex);
    for (auto& dep : _finish_dependencies[my_node_id]) {
        dep->set_ready();
    }
    _cv.notify_all();
}

TUniqueId SharedHashTableController::get_builder_fragment_instance_id(int my_node_id) {
    std::lock_guard<std::mutex> lock(_mutex);
    auto it = _builder_fragment_ids.find(my_node_id);
    if (it == _builder_fragment_ids.cend()) {
        return TUniqueId {};
    }
    return it->second;
}

} // namespace doris::vectorized
