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
#include <queue>

#include "runtime/result_writer.h"
#include "vec/exprs/vexpr_fwd.h"

namespace doris {
class ObjectPool;
class RowDescriptor;
class RuntimeState;
class RuntimeProfile;
class TDataSink;
class TExpr;

namespace vectorized {
class Block;
/*
 *  In the pipeline execution engine, there are usually a large number of io operations on the sink side that
 *  will block the limited execution threads of the pipeline execution engine, resulting in a sharp performance
 *  degradation of the pipeline execution engine when there are import tasks.
 *
 *  So all ResultWriter in Sink should use AsyncResultWriter to do the real IO task in thread pool to keep the
 *  pipeline execution engine performance.
 *
 *  The Sub class of AsyncResultWriter need to impl two virtual function
 *     * Status open() the first time IO work like: create file/ connect networking
 *     * Status append_block() do the real IO work for block 
 */
class AsyncResultWriter : public ResultWriter {
public:
    AsyncResultWriter(const VExprContextSPtrs& output_expr_ctxs);

    Status close() override { return Status::OK(); }

    void force_close();

    virtual bool in_transaction() { return false; }

    bool need_normal_close() { return _need_normal_close; }

    Status init(RuntimeState* state) override { return Status::OK(); }

    virtual Status open(RuntimeState* state, RuntimeProfile* profile) = 0;

    Status write(std::unique_ptr<Block> block) { return append_block(*block); }

    bool can_write() {
        std::lock_guard l(_m);
        return _data_queue.size() < QUEUE_SIZE || !_writer_status.ok() || _eos;
    }

    [[nodiscard]] bool is_pending_finish() const { return !_writer_thread_closed; }

    void process_block(RuntimeState* state, RuntimeProfile* profile);

    // sink the block date to date queue
    Status sink(Block* block, bool eos);

    std::unique_ptr<Block> get_block_from_queue();

    // Add the IO thread task process block() to thread pool to dispose the IO
    void start_writer(RuntimeState* state, RuntimeProfile* profile);

protected:
    Status _projection_block(Block& input_block, Block* output_block);
    const VExprContextSPtrs& _vec_output_expr_ctxs;

private:
    static constexpr auto QUEUE_SIZE = 3;
    std::mutex _m;
    std::condition_variable _cv;
    std::deque<std::unique_ptr<Block>> _data_queue;
    Status _writer_status = Status::OK();
    bool _eos = false;
    bool _force_close = false;
    bool _need_normal_close = true;
    bool _writer_thread_closed = false;
};

} // namespace vectorized
} // namespace doris
