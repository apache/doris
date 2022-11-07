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

#include "shared_hashtable_controller.h"

#include <runtime/runtime_state.h>

namespace doris {
namespace vectorized {

bool SharedHashTableController::should_build_hash_table(RuntimeState* state, int my_node_id) {
    std::lock_guard<std::mutex> lock(_mutex);
    auto it = _builder_fragment_ids.find(my_node_id);
    if (it == _builder_fragment_ids.cend()) {
        _builder_fragment_ids[my_node_id] = state->fragment_instance_id();
        return true;
    }
    return false;
}

void SharedHashTableController::put_hash_table(SharedHashTableEntry&& entry, int my_node_id) {
    std::lock_guard<std::mutex> lock(_mutex);
    DCHECK(_hash_table_entries.find(my_node_id) == _hash_table_entries.cend());
    _hash_table_entries.insert({my_node_id, std::move(entry)});
    _cv.notify_all();
}

SharedHashTableEntry& SharedHashTableController::wait_for_hash_table(int my_node_id) {
    std::unique_lock<std::mutex> lock(_mutex);
    auto it = _hash_table_entries.find(my_node_id);
    if (it == _hash_table_entries.cend()) {
        _cv.wait(lock, [this, &it, my_node_id]() {
            it = _hash_table_entries.find(my_node_id);
            return it != _hash_table_entries.cend();
        });
    }
    return it->second;
}

void SharedHashTableController::acquire_ref_count(RuntimeState* state, int my_node_id) {
    std::unique_lock<std::mutex> lock(_mutex);
    _ref_fragments[my_node_id].emplace_back(state->fragment_instance_id());
}

Status SharedHashTableController::release_ref_count(RuntimeState* state, int my_node_id) {
    std::unique_lock<std::mutex> lock(_mutex);
    RETURN_IF_CANCELLED(state);
    auto id = state->fragment_instance_id();
    auto it = std::find(_ref_fragments[my_node_id].begin(), _ref_fragments[my_node_id].end(), id);
    CHECK(it != _ref_fragments[my_node_id].end());
    _ref_fragments[my_node_id].erase(it);
    _cv.notify_all();
    return Status::OK();
}

Status SharedHashTableController::release_ref_count_if_need(TUniqueId fragment_id) {
    std::unique_lock<std::mutex> lock(_mutex);
    bool need_to_notify = false;
    for (auto& ref : _ref_fragments) {
        auto it = std::find(ref.second.begin(), ref.second.end(), fragment_id);
        if (it == ref.second.end()) continue;
        ref.second.erase(it);
        need_to_notify = true;
        LOG(INFO) << "release_ref_count in node: " << ref.first
                  << " for fragment id: " << fragment_id;
    }
    if (need_to_notify) _cv.notify_all();
    return Status::OK();
}

Status SharedHashTableController::wait_for_closable(RuntimeState* state, int my_node_id) {
    std::unique_lock<std::mutex> lock(_mutex);
    RETURN_IF_CANCELLED(state);
    if (!_ref_fragments[my_node_id].empty()) {
        _cv.wait(lock, [&]() { return _ref_fragments[my_node_id].empty(); });
    }
    return Status::OK();
}

} // namespace vectorized
} // namespace doris