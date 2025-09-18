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

AsyncResultWriter::AsyncResultWriter(const doris::vectorized::VExprContextSPtrs& output_expr_ctxs,
                                     std::shared_ptr<pipeline::Dependency> dep,
                                     std::shared_ptr<pipeline::Dependency> fin_dep)
        : _vec_output_expr_ctxs(output_expr_ctxs), _dependency(dep), _finish_dependency(fin_dep) {}

Status AsyncResultWriter::sink(Block* block, bool eos) {
    auto rows = block->rows();
    std::unique_ptr<Block> add_block;
    if (rows) {
        add_block = _get_free_block(block, rows);
    }

    std::lock_guard l(_m);
    // if io task failed, just return error status to
    // end the query
    if (!_writer_status.ok()) {
        return _writer_status.status();
    }

    DCHECK(_dependency);
    if (_is_finished()) {
        _dependency->set_ready();
    }
    if (rows) {
        _memory_used_counter->update(add_block->allocated_bytes());
        _data_queue.emplace_back(std::move(add_block));
        if (!_data_queue_is_available() && !_is_finished()) {
            _dependency->block();
        }
    }
    // in 'process block' we check _eos first and _data_queue second so here
    // in the lock. must modify the _eos after change _data_queue to make sure
    // not lead the logic error in multi thread
    _eos = eos;

    _cv.notify_one();
    return Status::OK();
}

std::unique_ptr<Block> AsyncResultWriter::_get_block_from_queue() {
    std::lock_guard l(_m);
    DCHECK(!_data_queue.empty());
    auto block = std::move(_data_queue.front());
    _data_queue.pop_front();
    DCHECK(_dependency);
    if (_data_queue_is_available()) {
        _dependency->set_ready();
    }
    _memory_used_counter->update(-block->allocated_bytes());
    return block;
}

Status AsyncResultWriter::start_writer(RuntimeState* state, RuntimeProfile* operator_profile) {
    // Attention!!!
    // AsyncResultWriter::open is called asynchronously,
    // so we need to setupt the operator_profile and memory counter here,
    // or else the counter can be nullptr when AsyncResultWriter::sink is called.
    _operator_profile = operator_profile;
    DCHECK(_operator_profile->get_child("CommonCounters") != nullptr);
    _memory_used_counter =
            _operator_profile->get_child("CommonCounters")->get_counter("MemoryUsage");
    DCHECK(_memory_used_counter != nullptr);
    // Should set to false here, to
    DCHECK(_finish_dependency);
    _finish_dependency->block();
    // This is a async thread, should lock the task ctx, to make sure runtimestate and operator_profile
    // not deconstructed before the thread exit.
    auto task_ctx = state->get_task_execution_context();
    RETURN_IF_ERROR(ExecEnv::GetInstance()->fragment_mgr()->get_thread_pool()->submit_func(
            [this, state, operator_profile, task_ctx]() {
                SCOPED_ATTACH_TASK(state);
                auto task_lock = task_ctx.lock();
                if (task_lock == nullptr) {
                    return;
                }
                this->process_block(state, operator_profile);
                task_lock.reset();
            }));
    return Status::OK();
}

void AsyncResultWriter::process_block(RuntimeState* state, RuntimeProfile* operator_profile) {
    if (auto status = open(state, operator_profile); !status.ok()) {
        force_close(status);
    }

    if (state && state->get_query_ctx() && state->get_query_ctx()->workload_group()) {
        if (auto cg_ctl_sptr =
                    state->get_query_ctx()->workload_group()->get_cgroup_cpu_ctl_wptr().lock()) {
            Status ret = cg_ctl_sptr->add_thread_to_cgroup();
            if (ret.ok()) {
                std::string wg_tname =
                        "asyc_wr_" + state->get_query_ctx()->workload_group()->name();
                Thread::set_self_name(wg_tname);
            }
        }
    }

    DCHECK(_dependency);
    while (_writer_status.ok()) {
        ThreadCpuStopWatch cpu_time_stop_watch;
        cpu_time_stop_watch.start();
        Defer defer {[&]() {
            if (state && state->get_query_ctx()) {
                state->get_query_ctx()->resource_ctx()->cpu_context()->update_cpu_cost_ms(
                        cpu_time_stop_watch.elapsed_time());
            }
        }};

        //1) wait scan operator write data
        {
            std::unique_lock l(_m);
            // When the query is cancelled, _writer_status may be set to error status in force_close method.
            // When the BE process is exit gracefully, the fragment mgr's thread pool will be shutdown,
            // and the async thread will be exit.
            while (!_eos && _data_queue.empty() && _writer_status.ok() &&
                   !ExecEnv::GetInstance()->fragment_mgr()->shutting_down()) {
                // Add 1s to check to avoid lost signal
                _cv.wait_for(l, std::chrono::seconds(1));
            }
            // If writer status is not ok, then we should not change its status to avoid lost the actual error status.
            if (ExecEnv::GetInstance()->fragment_mgr()->shutting_down() && _writer_status.ok()) {
                _writer_status.update(Status::InternalError<false>("FragmentMgr is shutting down"));
            }

            //check if eos or writer error
            if ((_eos && _data_queue.empty()) || !_writer_status.ok()) {
                _data_queue.clear();
                break;
            }
        }

        //2) get the block from  data queue and write to downstream
        auto block = _get_block_from_queue();
        auto status = write(state, *block);
        if (!status.ok()) [[unlikely]] {
            std::unique_lock l(_m);
            _writer_status.update(status);
            if (_is_finished()) {
                _dependency->set_ready();
            }
            break;
        }

        _return_free_block(std::move(block));
    }

    bool need_finish = false;
    {
        // If the last block is sent successfuly, then call finish to clear the buffer or commit
        // transactions.
        // Using lock to make sure the writer status is not modified
        // There is a unique ptr err_msg in Status, if it is modified, the unique ptr
        // maybe released. And it will core because use after free.
        std::lock_guard l(_m);
        if (_writer_status.ok() && _eos) {
            need_finish = true;
        }
    }
    // eos only means the last block is input to the queue and there is no more block to be added,
    // it is not sure that the block is written to stream.
    if (need_finish) {
        // Should not call finish in lock because it may hang, and it will lock _m too long.
        // And get_writer_status will also need this lock, it will block pipeline exec thread.
        Status st = finish(state);
        _writer_status.update(st);
    }
    Status st = Status::OK();
    { st = _writer_status.status(); }

    Status close_st = close(st);
    {
        // If it is already failed before, then not update the write status so that we could get
        // the real reason.
        std::lock_guard l(_m);
        if (_writer_status.ok()) {
            _writer_status.update(close_st);
        }
    }
    // should set _finish_dependency first, as close function maybe blocked by wait_close of execution_timeout
    _set_ready_to_finish();
}

void AsyncResultWriter::_set_ready_to_finish() {
    DCHECK(_finish_dependency);
    _finish_dependency->set_ready();
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
    _writer_status.update(s);
    DCHECK(_dependency);
    if (_is_finished()) {
        _dependency->set_ready();
    }
    _cv.notify_one();
}

void AsyncResultWriter::_return_free_block(std::unique_ptr<Block> b) {
    if (_low_memory_mode) {
        return;
    }

    const auto allocated_bytes = b->allocated_bytes();
    if (_free_blocks.enqueue(std::move(b))) {
        _memory_used_counter->update(allocated_bytes);
    }
}

std::unique_ptr<Block> AsyncResultWriter::_get_free_block(doris::vectorized::Block* block,
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
void AsyncResultWriter::set_low_memory_mode() {
    _low_memory_mode = true;
    clear_blocks(_free_blocks, _memory_used_counter);
}
} // namespace vectorized
} // namespace doris
