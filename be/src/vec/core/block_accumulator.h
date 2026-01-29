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

#include <memory>
#include <vector>

#include "block.h"

namespace doris::vectorized {

// 用来累积Block，尽可能保证每个Block的行数达到batch_size。
// 非线程安全
// 使用的时候，可以一直push，最后调用finish，表示数据已经push完毕。
// 然后可以调用has_output和pull接口获取累积好的Block。
class BlockAccumulator {
public:
    BlockAccumulator(size_t batch_size) : _batch_size(batch_size) {}

    Status push(Block&& block);

    Status set_finish();

    bool has_output() const { return !_queue.empty(); }

    bool pull(Block& block);

    void close();

private:
    Status push_tmp_block_to_queue();

    std::list<Block> _queue;

    MutableBlock _tmp_block;

    const size_t _batch_size;
};

// 用于pipeline模式下的累积，和BlockAccumulator不同的是，不可以一直push数据
class PipelineBlockAccumulator {
public:
    PipelineBlockAccumulator(size_t batch_size) : _batch_size(batch_size) {}

    Status push(Block&& block);

    Status push_with_selector(Block&& block, const IColumn::Selector& selector);

    BlockUPtr pull();

    bool has_output();

    bool need_input();

    void set_finish();

    bool is_finished();

    void close() {
        _in_block = nullptr;
        _out_block = nullptr;
    }

private:
    std::unique_ptr<MutableBlock> _in_block;
    std::unique_ptr<Block> _out_block;

    const size_t _batch_size;
    bool _is_finished = false;
};

}; // namespace doris::vectorized
