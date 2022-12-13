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
#include <deque>
#include <memory>
#include <mutex>
#include <vector>

namespace doris {
namespace vectorized {
class Block;
}
namespace pipeline {

class DataQueue {
public:
    DataQueue(int child_count);
    ~DataQueue() = default;

    std::unique_ptr<vectorized::Block> get_block_from_queue();

    void push_block(int child_idx, std::unique_ptr<vectorized::Block> block);

    bool remaining_has_data();

private:
    std::vector<std::unique_ptr<std::mutex>> _queue_blocks_lock;
    std::vector<std::deque<std::unique_ptr<vectorized::Block>>> _queue_blocks;
    int _child_count = 0;
    std::atomic<int> _flag_queue_idx = 0;
};

} // namespace pipeline
} // namespace doris