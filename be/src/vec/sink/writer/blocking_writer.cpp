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

#include "blocking_writer.h"

#include "common/status.h"
#include "pipeline/dependency.h"
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
#include "common/compile_check_begin.h"

BlockingWriter::BlockingWriter(const doris::vectorized::VExprContextSPtrs& output_expr_ctxs)
        : _vec_output_expr_ctxs(output_expr_ctxs) {}

Status BlockingWriter::sink(RuntimeState* state, Block* block, bool eos,
                            RuntimeProfile* operator_profile) {
    auto rows = block->rows();
    if (rows) {
        std::unique_ptr<Block> add_block = _get_free_block(block, rows);
        RETURN_IF_ERROR(write(state, *add_block));
        _return_free_block(std::move(add_block));
    }
    _eos = eos;
    if (eos) {
        RETURN_IF_ERROR(finish(state));
    }
    return Status::OK();
}

Status BlockingWriter::open(RuntimeState* state, RuntimeProfile* operator_profile) {
    _operator_profile = operator_profile;
    DCHECK(_operator_profile->get_child("CommonCounters") != nullptr);
    _memory_used_counter =
            _operator_profile->get_child("CommonCounters")->get_counter("MemoryUsage");
    DCHECK(_memory_used_counter != nullptr);
    return Status::OK();
}

Status BlockingWriter::_projection_block(doris::vectorized::Block& input_block,
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

void BlockingWriter::_return_free_block(std::unique_ptr<Block> b) {
    if (_low_memory_mode) {
        return;
    }

    const auto allocated_bytes = b->allocated_bytes();
    if (_free_blocks.enqueue(std::move(b))) {
        _memory_used_counter->update(allocated_bytes);
    }
}

std::unique_ptr<Block> BlockingWriter::_get_free_block(doris::vectorized::Block* block,
                                                       size_t rows) {
    std::unique_ptr<Block> b;
    if (!_free_blocks.try_dequeue(b)) {
        b = block->create_same_struct_block(rows, true);
    } else {
        _memory_used_counter->update(-b->allocated_bytes());
    }
    b->swap(*block);
    return b;
}

template <typename T>
void clear_blocks(moodycamel::ConcurrentQueue<T>& blocks,
                  RuntimeProfile::Counter* memory_used_counter = nullptr);
void BlockingWriter::set_low_memory_mode() {
    _low_memory_mode = true;
    clear_blocks(_free_blocks, _memory_used_counter);
}

void BlockingWriter::on_finish() const {
    _finish_dependency->set_ready();
}
} // namespace vectorized
} // namespace doris
