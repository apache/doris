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

#include <atomic>
#include <cstdint>
#include <deque>
#include <memory>
#include <vector>

#include "common/status.h"
#include "common/thread_safety_annotations.h"
#include "core/block/block.h"

namespace doris {
#include "common/compile_check_begin.h"

class Dependency;

// Per child sub-queue. Groups all parallel state so that the lock/field
// relationship is explicit and can be checked by clang -Wthread-safety.
struct SubQueue {
    // Protects the `blocks` deque and serializes high-level state
    // transitions (push/pop/finish/cancel) on this sub-queue.
    AnnotatedMutex queue_lock;
    std::deque<std::unique_ptr<Block>> blocks GUARDED_BY(queue_lock);

    // The following fields are only accessed while holding queue_lock.
    int64_t bytes_in_queue GUARDED_BY(queue_lock) = 0;
    bool is_finished GUARDED_BY(queue_lock) = false;

    // Protects the `free_blocks` deque only.
    AnnotatedMutex free_lock;
    std::deque<std::unique_ptr<Block>> free_blocks GUARDED_BY(free_lock);

    // blocks_in_queue is readable from lock-free fast paths (remaining_has_data),
    // so it remains atomic and is intentionally not GUARDED_BY.
    std::atomic_uint32_t blocks_in_queue {0};

    // Maximum number of blocks allowed in this sub-queue before the sink is blocked.
    // Updated by DataQueue::set_max_blocks_in_sub_queue / set_low_memory_mode.
    std::atomic_int64_t max_blocks_in_queue {1};

    // Set once during init via set_sink_dependency, then read-only.
    Dependency* sink_dependency = nullptr;

    // Pop a block under queue_lock.
    // Notifies sink_dependency->set_ready() (outside the lock) if the queue becomes empty.
    // output_block is null if the queue was empty.
    void try_pop(std::unique_ptr<Block>* output_block);

    // Push a block under queue_lock and atomically increment total_counter.
    // Returns false (without incrementing) if already finished.
    // Calls sink_dependency->block() (outside the lock) if the queue exceeds max_blocks_in_queue.
    bool try_push(std::unique_ptr<Block> block, std::atomic_uint32_t& total_counter);

    // Mark this sub-queue finished. Returns false if already finished (idempotent).
    // Decrements unfinished_counter and may set all_finished within queue_lock.
    bool mark_finished(std::atomic_uint32_t& unfinished_counter, std::atomic_bool& all_finished);

    // Clear all pending blocks under queue_lock.
    // Calls sink_dependency->set_always_ready() (outside the lock) if any blocks were cleared.
    void clear_blocks();
};

class DataQueue {
public:
    //always one is enough, but in union node it's has more children
    DataQueue(int child_count = 1);
    ~DataQueue() = default;

    Status get_block_from_queue(std::unique_ptr<Block>* block, int* child_idx = nullptr);
    Status push_block(std::unique_ptr<Block> block, int child_idx = 0);

    std::unique_ptr<Block> get_free_block(int child_idx = 0);
    void push_free_block(std::unique_ptr<Block> output_block, int child_idx = 0);

    void set_finish(int child_idx = 0);
    bool is_all_finish();

    // This function is not thread safe, should be called in Operator::get_block()
    bool remaining_has_data();
    bool has_more_data() const;

    void set_source_dependency(std::shared_ptr<Dependency> source_dependency)
            NO_THREAD_SAFETY_ANALYSIS;
    void set_sink_dependency(Dependency* sink_dependency, int child_idx);
    void set_max_blocks_in_sub_queue(int64_t max_blocks);
    void set_low_memory_mode();

    void terminate();

private:
    void clear_free_blocks();
    void set_source_ready();
    void set_source_block();

    std::vector<std::unique_ptr<SubQueue>> _sub_queues;

    //how many deque will be init, always will be one
    int _child_count = 0;
    std::atomic_uint32_t _un_finished_counter = 0;
    std::atomic_bool _is_all_finished = false;
    std::atomic_uint32_t _cur_blocks_total_nums = 0;

    //this will be indicate which queue has data, it's useful when have many queues
    std::atomic_int _flag_queue_idx = 0;
    // only used by streaming agg source operator

    std::atomic_bool _is_low_memory_mode = false;

    // _source_dependency is written once during initialization (set_source_dependency)
    // and read/used only while holding _source_lock thereafter.
    std::shared_ptr<Dependency> _source_dependency GUARDED_BY(_source_lock) = nullptr;
    AnnotatedMutex _source_lock;
};

#include "common/compile_check_end.h"
} // namespace doris
