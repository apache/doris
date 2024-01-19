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

    std::lock_guard l(_m);
    // if io task failed, just return error status to
    // end the query
    if (!_writer_status.ok()) {
        return _writer_status;
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
        force_close(status);
    }

    if (_writer_status.ok()) {
        while (true) {
            if (!_eos && _data_queue.empty() && _writer_status.ok()) {
                std::unique_lock l(_m);
                while (!_eos && _data_queue.empty() && _writer_status.ok()) {
                    _cv.wait(l);
                }
            }

            if ((_eos && _data_queue.empty()) || !_writer_status.ok()) {
                _data_queue.clear();
                break;
            }

            auto block = _get_block_from_queue();
            auto status = write(*block);
            if (!status.ok()) [[unlikely]] {
                std::unique_lock l(_m);
                _writer_status = status;
                if (_dependency && _is_finished()) {
                    _dependency->set_ready();
                }
                break;
            }

            _return_free_block(std::move(block));
        }
    }

    // If the last block is sent successfuly, then call finish to clear the buffer or commit
    // transactions.
    // Using lock to make sure the writer status is not modified
    // There is a unique ptr err_msg in Status, if it is modified, the unique ptr
    // maybe released. And it will core because use after free.
    std::lock_guard l(_m);
    if (_writer_status.ok() && _eos) {
        _writer_status = finish(state);
    }

    if (_writer_status.ok()) {
        _writer_status = close(_writer_status);
    } else {
        // If it is already failed before, then not update the write status so that we could get
        // the real reason.
        static_cast<void>(close(_writer_status));
    }
    _writer_thread_closed = true;
    if (_finish_dependency) {
        _finish_dependency->set_ready();
    }
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

void AsyncResultWriter::force_close(Status s) {
    std::lock_guard l(_m);
    _writer_status = s;
    if (_dependency && _is_finished()) {
        _dependency->set_ready();
    }
    _cv.notify_one();
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
