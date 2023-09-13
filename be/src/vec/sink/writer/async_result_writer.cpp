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
        : _vec_output_expr_ctxs(output_expr_ctxs) {};

Status AsyncResultWriter::sink(Block* block, bool eos) {
    auto rows = block->rows();
    auto status = Status::OK();
    std::unique_ptr<Block> add_block;
    if (rows) {
        add_block = block->create_same_struct_block(0);
    }

    std::lock_guard l(_m);
    // if io task failed, just return error status to
    // end the query
    if (!_writer_status.ok()) {
        return _writer_status;
    }

    _eos = eos;
    if (rows) {
        RETURN_IF_ERROR(MutableBlock::build_mutable_block(add_block.get()).merge(*block));
        _data_queue.emplace_back(std::move(add_block));
    } else if (_eos && _data_queue.empty()) {
        status = Status::EndOfFile("Run out of sink data");
    }

    _cv.notify_one();
    return status;
}

std::unique_ptr<Block> AsyncResultWriter::get_block_from_queue() {
    std::lock_guard l(_m);
    DCHECK(!_data_queue.empty());
    auto block = std::move(_data_queue.front());
    _data_queue.pop_front();
    return block;
}

void AsyncResultWriter::start_writer(RuntimeState* state, RuntimeProfile* profile) {
    ExecEnv::GetInstance()->fragment_mgr()->get_thread_pool()->submit_func(
            [this, state, profile]() { this->process_block(state, profile); });
}

void AsyncResultWriter::process_block(RuntimeState* state, RuntimeProfile* profile) {
    _writer_status = open(state, profile);
    if (_writer_status.ok()) {
        while (true) {
            {
                std::unique_lock l(_m);
                while (!_eos && _data_queue.empty() && !_force_close) {
                    _cv.wait(l);
                }
            }

            if ((_eos && _data_queue.empty()) || _force_close) {
                _data_queue.clear();
                break;
            }

            auto status = write(get_block_from_queue());
            std::unique_lock l(_m);
            _writer_status = status;
            if (!status.ok()) {
                break;
            }
        }
    }

    // if not in transaction or status is in error or force close we can do close in
    // async IO thread
    if (!_writer_status.ok() || _force_close || !in_transaction()) {
        close();
        _need_normal_close = false;
    }
    _writer_thread_closed = true;
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

void AsyncResultWriter::force_close() {
    std::lock_guard l(_m);
    _force_close = true;
    _cv.notify_one();
}

} // namespace vectorized
} // namespace doris
