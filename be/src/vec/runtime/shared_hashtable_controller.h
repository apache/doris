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

namespace vectorized {

class VExprContext;

struct SharedHashTableEntry {
    SharedHashTableEntry(void* hash_table_ptr_, std::vector<Block>& blocks_,
                         std::unordered_map<const Block*, std::vector<int>>& inserted_rows_,
                         const std::vector<VExprContext*>& exprs)
            : hash_table_ptr(hash_table_ptr_),
              blocks(blocks_),
              inserted_rows(inserted_rows_),
              build_exprs(exprs) {}
    SharedHashTableEntry(SharedHashTableEntry&& entry)
            : hash_table_ptr(entry.hash_table_ptr),
              blocks(entry.blocks),
              inserted_rows(entry.inserted_rows),
              build_exprs(entry.build_exprs) {}
    void* hash_table_ptr;
    std::vector<Block>& blocks;
    std::unordered_map<const Block*, std::vector<int>>& inserted_rows;
    std::vector<VExprContext*> build_exprs;
};

class SharedHashTableController {
public:
    bool should_build_hash_table(RuntimeState* state, int my_node_id);
    void acquire_ref_count(RuntimeState* state, int my_node_id);
    SharedHashTableEntry& wait_for_hash_table(int my_node_id);
    Status release_ref_count(RuntimeState* state, int my_node_id);
    Status release_ref_count_if_need(TUniqueId fragment_id);
    void put_hash_table(SharedHashTableEntry&& entry, int my_node_id);
    Status wait_for_closable(RuntimeState* state, int my_node_id);

private:
    std::mutex _mutex;
    std::condition_variable _cv;
    std::map<int /*node id*/, TUniqueId /*fragment id*/> _builder_fragment_ids;
    std::map<int /*node id*/, SharedHashTableEntry> _hash_table_entries;
    std::map<int /*node id*/, std::vector<TUniqueId>> _ref_fragments;
};

} // namespace vectorized
} // namespace doris