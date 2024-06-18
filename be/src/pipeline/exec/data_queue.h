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

#include <stdint.h>

#include <atomic>
#include <deque>
#include <memory>
#include <mutex>
#include <vector>

#include "common/status.h"
#include "util/spinlock.h"
#include "vec/core/block.h"

namespace doris::pipeline {

class Dependency;

class DataQueue {
public:
    //always one is enough, but in union node it's has more children
    DataQueue(int child_count = 1);
    ~DataQueue() = default;

    Status get_block_from_queue(std::unique_ptr<vectorized::Block>* block,
                                int* child_idx = nullptr);

    void push_block(std::unique_ptr<vectorized::Block> block, int child_idx = 0);

    std::unique_ptr<vectorized::Block> get_free_block(int child_idx = 0);

    void push_free_block(std::unique_ptr<vectorized::Block> output_block, int child_idx = 0);

    void set_finish(int child_idx = 0);
    void set_canceled(int child_idx = 0); // should set before finish
    bool is_finish(int child_idx = 0);
    bool is_all_finish();

    bool has_enough_space_to_push();
    bool has_data_or_finished(int child_idx = 0);
    bool remaining_has_data();

    int64_t max_bytes_in_queue() const { return _max_bytes_in_queue; }
    int64_t max_size_of_queue() const { return _max_size_of_queue; }

    bool data_exhausted() const { return _data_exhausted; }
    void set_source_dependency(std::shared_ptr<Dependency> source_dependency) {
        _source_dependency = source_dependency;
    }
    void set_sink_dependency(Dependency* sink_dependency, int child_idx) {
        _sink_dependencies[child_idx] = sink_dependency;
    }

    void set_source_ready();
    void set_source_block();

    void set_max_blocks_in_sub_queue(int64_t max_blocks) { _max_blocks_in_sub_queue = max_blocks; }

private:
    std::vector<std::unique_ptr<std::mutex>> _queue_blocks_lock;
    std::vector<std::deque<std::unique_ptr<vectorized::Block>>> _queue_blocks;

    std::vector<std::unique_ptr<std::mutex>> _free_blocks_lock;
    std::vector<std::deque<std::unique_ptr<vectorized::Block>>> _free_blocks;

    //how many deque will be init, always will be one
    int _child_count = 0;
    std::vector<std::atomic_bool> _is_finished;
    std::atomic_uint32_t _un_finished_counter;
    std::atomic_bool _is_all_finished = false;
    std::vector<std::atomic_bool> _is_canceled;
    // int64_t just for counter of profile
    std::vector<std::atomic_int64_t> _cur_bytes_in_queue;
    std::vector<std::atomic_uint32_t> _cur_blocks_nums_in_queue;
    std::atomic_uint32_t _cur_blocks_total_nums = 0;

    //this will be indicate which queue has data, it's useful when have many queues
    std::atomic_int _flag_queue_idx = 0;
    // only used by streaming agg source operator
    bool _data_exhausted = false;

    int64_t _max_blocks_in_sub_queue = 1;

    //this only use to record the queue[0] for profile
    int64_t _max_bytes_in_queue = 0;
    int64_t _max_size_of_queue = 0;
    static constexpr int64_t MAX_BYTE_OF_QUEUE = 1024l * 1024 * 1024 / 10;

    // data queue is multi sink one source
    std::shared_ptr<Dependency> _source_dependency = nullptr;
    std::vector<Dependency*> _sink_dependencies;
    SpinLock _source_lock;
};

} // namespace doris::pipeline
