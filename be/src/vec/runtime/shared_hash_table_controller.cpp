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

#include <runtime/runtime_state.h>

namespace doris {
namespace vectorized {

bool SharedHashTableController::should_build_hash_table(const TUniqueId& fragment_instance_id,
                                                        int my_node_id) {
    std::lock_guard<std::mutex> lock(_mutex);
    auto it = _builder_fragment_ids.find(my_node_id);
    if (it == _builder_fragment_ids.cend()) {
        _builder_fragment_ids.insert({my_node_id, fragment_instance_id});
        return true;
    }
    return false;
}

SharedHashTableContextPtr SharedHashTableController::get_context(int my_node_id) {
    std::lock_guard<std::mutex> lock(_mutex);
    auto it = _shared_contexts.find(my_node_id);
    if (it == _shared_contexts.cend()) {
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

Status SharedHashTableController::wait_for_signal(RuntimeState* state,
                                                  const SharedHashTableContextPtr& context) {
    std::unique_lock<std::mutex> lock(_mutex);
    // maybe builder signaled before other instances waiting,
    // so here need to check value of `signaled`
    while (!context->signaled) {
        _cv.wait_for(lock, std::chrono::milliseconds(400), [&]() { return context->signaled; });
        // return if the instances is cancelled(eg. query timeout)
        RETURN_IF_CANCELLED(state);
    }
    return context->status;
}

} // namespace vectorized
} // namespace doris
