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

template <typename ExprCtxType>
class RuntimeFilterSlotsBase;

namespace vectorized {

class VExprContext;

struct SharedHashTableEntry {
    SharedHashTableEntry(Status status_, void* hash_table_ptr_, std::vector<Block>* blocks_,
                         RuntimeFilterSlotsBase<VExprContext>* runtime_filter_slots_)
            : status(status_),
              hash_table_ptr(hash_table_ptr_),
              blocks(blocks_),
              runtime_filter_slots(runtime_filter_slots_) {}
    SharedHashTableEntry(SharedHashTableEntry&& entry)
            : status(entry.status),
              hash_table_ptr(entry.hash_table_ptr),
              blocks(entry.blocks),
              runtime_filter_slots(entry.runtime_filter_slots) {}

    static SharedHashTableEntry empty_entry_with_status(const Status& status) {
        return SharedHashTableEntry(status, nullptr, nullptr, nullptr);
    }

    Status status;
    void* hash_table_ptr;
    std::vector<Block>* blocks;
    RuntimeFilterSlotsBase<VExprContext>* runtime_filter_slots;
};

class SharedHashTableController {
public:
    bool should_build_hash_table(RuntimeState* state, int my_node_id);
    bool supposed_to_build_hash_table(RuntimeState* state, int my_node_id);
    void acquire_ref_count(RuntimeState* state, int my_node_id);
    SharedHashTableEntry& wait_for_hash_table(int my_node_id);
    Status release_ref_count(RuntimeState* state, int my_node_id);
    Status release_ref_count_if_need(TUniqueId fragment_id, Status status);
    void put_hash_table(SharedHashTableEntry&& entry, int my_node_id);
    Status wait_for_closable(RuntimeState* state, int my_node_id);

private:
    // If the fragment instance was supposed to build hash table, but it didn't build.
    // To avoid deadlocking other fragment instances,
    // here need to put an empty SharedHashTableEntry with canceled status.
    void _put_an_empty_entry_if_need(Status status, TUniqueId fragment_id, int node_id);

private:
    std::mutex _mutex;
    std::condition_variable _cv;
    std::map<int /*node id*/, TUniqueId /*fragment id*/> _builder_fragment_ids;
    std::map<int /*node id*/, SharedHashTableEntry> _hash_table_entries;
    std::map<int /*node id*/, std::vector<TUniqueId>> _ref_fragments;
};

} // namespace vectorized
} // namespace doris