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

#include "async_result_writer.h"

#include "pipeline/pipeline_x/dependency.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "runtime/runtime_state.h"
#include "vec/core/block.h"
#include "vec/core/materialize_block.h"
#include "vec/exprs/vexpr_context.h"

namespace doris {
class ObjectPool;
class RowDescriptor;
class TExpr;

namespace vectorized {

AsyncResultWriter::AsyncResultWriter(const doris::vectorized::VExprContextSPtrs& output_expr_ctxs)
        : _vec_output_expr_ctxs(output_expr_ctxs),
          _dependency(nullptr),
          _finish_dependency(nullptr) {}

void AsyncResultWriter::set_dependency(pipeline::AsyncWriterDependency* dep,
                                       pipeline::Dependency* finish_dep) {
    _dependency = dep;
    _finish_dependency = finish_dep;
}

Status AsyncResultWriter::sink(Block* block, bool eos) {
    auto rows = block->rows();
    auto status = Status::OK();
    std::unique_ptr<Block> add_block;
    if (rows) {
        add_block = _get_free_block(block, rows);
    }

    // if io task failed, just return error status to
    // end the query

    std::lock_guard l(_m);

    if (_is_closed) {
        return Status::Cancelled("Already closed");
    }

    _eos = eos;
    if (_dependency && _is_finished()) {
        _dependency->set_ready();
    }
    if (rows) {
        _data_queue.emplace_back(std::move(add_block));
        if (_dependency && !_data_queue_is_available() && !_is_finished()) {
            _dependency->block();
        }
    } else if (_eos && _data_queue.empty()) {
        status = Status::EndOfFile("Run out of sink data");
    }

    _cv.notify_one();
    return status;
}

std::unique_ptr<Block> AsyncResultWriter::_get_block_from_queue() {
    std::lock_guard l(_m);
    DCHECK(!_data_queue.empty());
    auto block = std::move(_data_queue.front());
    _data_queue.pop_front();
    if (_dependency && _data_queue_is_available()) {
        _dependency->set_ready();
    }
    return block;
}

void AsyncResultWriter::start_writer(RuntimeState* state, RuntimeProfile* profile) {
    static_cast<void>(ExecEnv::GetInstance()->fragment_mgr()->get_thread_pool()->submit_func(
            [this, state, profile]() { this->process_block(state, profile); }));
}

void AsyncResultWriter::process_block(RuntimeState* state, RuntimeProfile* profile) {
    if (auto status = open(state, profile); !status.ok()) {
        auto close_st = close(status);
        return;
    }

    while (true) {
        std::unique_lock<std::mutex> lock(_m);

        while (!_is_closed && !_eos && _data_queue.empty()) {
            _cv.wait_for(lock, std::chrono::seconds(1));
        }

        if (_is_closed) {
            break;
        }

        if (_eos && _data_queue.empty()) {
            break;
        }

        // if _eos == true && !_data_queue.empty()
        // we will write block to downstream one by one until _data_queue is empty
        // and loop will be breaked then.

        // release lock before IO
        lock.unlock();

        auto block = _get_block_from_queue();
        // for VFileResultWriter, we have three types of transformer:
        // 1. VCSVTransformer
        // 2. VParquetTransformer
        // 3. FORMAT_ORC
        // what if method write blocks for a very long time? seems that we cannot
        // notify them to stop unless they are using async writing pattern.
        auto write_st = write(block);

        if (write_st.ok()) {
            _return_free_block(std::move(block));
            continue;
        }

        lock.lock();
        // from this point on, there will be no further block added to _data_queue
        _is_closed = true;

        if (_dependency && _is_finished()) {
            _dependency->set_ready();
        }

        lock.unlock();
        break;
    }

    std::lock_guard<std::mutex> lg(_m);
    // we need this to notify try_close to return.
    _writer_thread_closed = true;
    _data_queue.clear();

    if (_finish_dependency) {
        _finish_dependency->set_ready();
    }
    // notify try_close thread
    _cv.notify_one();

    return;
}

Status AsyncResultWriter::_projection_block(doris::vectorized::Block& input_block,
                                            doris::vectorized::Block* output_block) {
    Status status = Status::OK();
    if (input_block.rows() == 0) {
        return status;
    }
    RETURN_IF_ERROR(vectorized::VExprContext::get_output_block_after_execute_exprs(
            _vec_output_expr_ctxs, input_block, output_block));
    materialize_block_inplace(*output_block);
    return status;
}

void AsyncResultWriter::_return_free_block(std::unique_ptr<Block> b) {
    _free_blocks.enqueue(std::move(b));
}

std::unique_ptr<Block> AsyncResultWriter::_get_free_block(doris::vectorized::Block* block,
                                                          int rows) {
    std::unique_ptr<Block> b;
    if (!_free_blocks.try_dequeue(b)) {
        b = block->create_same_struct_block(rows, true);
    }
    b->swap(*block);
    return b;
}

} // namespace vectorized
} // namespace doris
