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
#include <concurrentqueue.h>

#include <condition_variable>
#include <queue> // IWYU pragma: keep

#include "runtime/result_writer.h"
#include "util/runtime_profile.h"
#include "vec/exprs/vexpr_fwd.h"

namespace doris {
class ObjectPool;
class RowDescriptor;
class RuntimeState;
class TDataSink;
class TExpr;

namespace pipeline {
class Dependency;
class PipelineTask;

} // namespace pipeline

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
 *     * Status open() the first time IO work like: create file/ connect network
 *     * Status write() do the real IO work for block 
 */
class BlockingWriter : public ResultWriter {
public:
    BlockingWriter(const VExprContextSPtrs& output_expr_ctxs);

    Status init(RuntimeState* state) override { return Status::OK(); }

    virtual Status open(RuntimeState* state, RuntimeProfile* operator_profile);

    // sink the block data to data queue, it is async
    Status sink(RuntimeState* state, Block* block, bool eos, RuntimeProfile* operator_profile);

    void set_low_memory_mode();

protected:
    Status _projection_block(Block& input_block, Block* output_block);
    const VExprContextSPtrs& _vec_output_expr_ctxs;
    RuntimeProfile* _operator_profile = nullptr; // not owned, set when open

    std::unique_ptr<Block> _get_free_block(Block*, size_t rows);

private:
    void _return_free_block(std::unique_ptr<Block>);
    bool _eos = false;
    std::atomic_bool _low_memory_mode = false;
    moodycamel::ConcurrentQueue<std::unique_ptr<Block>> _free_blocks;
    RuntimeProfile::Counter* _memory_used_counter = nullptr;
};

} // namespace vectorized
} // namespace doris
