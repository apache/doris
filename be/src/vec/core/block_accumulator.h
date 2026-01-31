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

// Used to accumulate Blocks, ensuring each Block's row count reaches batch_size as much as possible.
// Not thread-safe.
// Usage: continuously push, finally call finish to indicate all data has been pushed.
// Then call has_output and pull interfaces to get the accumulated Block.
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

// Used for accumulation in pipeline mode. Unlike BlockAccumulator, it cannot continuously push data.
// Use has_output() to determine if data can be pulled.
// Use need_input() to determine if data can be pushed.
// The correct usage flow is: keep pushing until need_input() returns false, then pull data.
// Currently used in StatefulOperator: need_input() is called in need_more_input_data(), push() in push(), pull() in pull().
// After set_finish() is called, it indicates all data has been pushed and no more data can be pushed.
// Not thread-safe.

///TODO: Consider memory reuse?
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
