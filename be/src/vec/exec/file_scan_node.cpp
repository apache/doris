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

#include "vec/exec/file_scan_node.h"

#include "common/config.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/runtime_filter_mgr.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"
#include "runtime/tuple.h"
#include "runtime/tuple_row.h"
#include "util/priority_thread_pool.hpp"
#include "util/runtime_profile.h"
#include "util/thread.h"
#include "util/types.h"
#include "vec/exec/file_arrow_scanner.h"
#include "vec/exec/file_text_scanner.h"
#include "vec/exprs/vcompound_pred.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"

namespace doris::vectorized {

FileScanNode::FileScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ScanNode(pool, tnode, descs),
          _tuple_id(tnode.file_scan_node.tuple_id),
          _runtime_state(nullptr),
          _tuple_desc(nullptr),
          _num_running_scanners(0),
          _scan_finished(false),
          _max_buffered_batches(32),
          _wait_scanner_timer(nullptr),
          _runtime_filter_descs(tnode.runtime_filters) {
    LOG(WARNING) << "file scan node runtime filter size=" << _runtime_filter_descs.size();
}

Status FileScanNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ScanNode::init(tnode, state));
    auto& file_scan_node = tnode.file_scan_node;

    if (file_scan_node.__isset.pre_filter_exprs) {
        _pre_filter_texprs = file_scan_node.pre_filter_exprs;
    }

    int filter_size = _runtime_filter_descs.size();
    _runtime_filter_ctxs.resize(filter_size);
    _runtime_filter_ready_flag.resize(filter_size);
    for (int i = 0; i < filter_size; ++i) {
        IRuntimeFilter* runtime_filter = nullptr;
        const auto& filter_desc = _runtime_filter_descs[i];
        RETURN_IF_ERROR(state->runtime_filter_mgr()->regist_filter(
                RuntimeFilterRole::CONSUMER, filter_desc, state->query_options(), id()));
        RETURN_IF_ERROR(state->runtime_filter_mgr()->get_consume_filter(filter_desc.filter_id,
                                                                        &runtime_filter));

        _runtime_filter_ctxs[i].runtimefilter = runtime_filter;
        _runtime_filter_ready_flag[i] = false;
        _rf_locks.push_back(std::make_unique<std::mutex>());
    }

    return Status::OK();
}

Status FileScanNode::prepare(RuntimeState* state) {
    VLOG_QUERY << "FileScanNode prepare";
    RETURN_IF_ERROR(ScanNode::prepare(state));
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker());
    // get tuple desc
    _runtime_state = state;
    _tuple_desc = state->desc_tbl().get_tuple_descriptor(_tuple_id);
    if (_tuple_desc == nullptr) {
        std::stringstream ss;
        ss << "Failed to get tuple descriptor, _tuple_id=" << _tuple_id;
        return Status::InternalError(ss.str());
    }

    // Initialize slots map
    for (auto slot : _tuple_desc->slots()) {
        auto pair = _slots_map.emplace(slot->col_name(), slot);
        if (!pair.second) {
            std::stringstream ss;
            ss << "Failed to insert slot, col_name=" << slot->col_name();
            return Status::InternalError(ss.str());
        }
    }

    // Profile
    _wait_scanner_timer = ADD_TIMER(runtime_profile(), "WaitScannerTime");
    _filter_timer = ADD_TIMER(runtime_profile(), "PredicateFilteredTime");
    _num_rows_filtered = ADD_COUNTER(runtime_profile(), "PredicateFilteredRows", TUnit::UNIT);
    _num_scanners = ADD_COUNTER(runtime_profile(), "NumScanners", TUnit::UNIT);

    return Status::OK();
}

Status FileScanNode::open(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker());
    RETURN_IF_CANCELLED(state);

    RETURN_IF_ERROR(_acquire_and_build_runtime_filter(state));

    RETURN_IF_ERROR(start_scanners());

    return Status::OK();
}

Status FileScanNode::_acquire_and_build_runtime_filter(RuntimeState* state) {
    // acquire runtime filter
    _runtime_filter_ctxs.resize(_runtime_filter_descs.size());
    for (size_t i = 0; i < _runtime_filter_descs.size(); ++i) {
        auto& filter_desc = _runtime_filter_descs[i];
        IRuntimeFilter* runtime_filter = nullptr;
        state->runtime_filter_mgr()->get_consume_filter(filter_desc.filter_id, &runtime_filter);
        DCHECK(runtime_filter != nullptr);
        if (runtime_filter == nullptr) {
            continue;
        }
        bool ready = runtime_filter->is_ready();
        if (!ready) {
            ready = runtime_filter->await();
        }
        if (ready) {
            _runtime_filter_ctxs[i].apply_mark = true;
            _runtime_filter_ctxs[i].runtimefilter = runtime_filter;

            // TODO: currently, after calling get_push_expr_ctxs(), the func ptr in runtime_filter
            // will be released, and it will not be used again for building vexpr.
            //
            // std::list<ExprContext*> expr_context;
            // RETURN_IF_ERROR(runtime_filter->get_push_expr_ctxs(&expr_context));
            // for (auto ctx : expr_context) {
            //     ctx->prepare(state, row_desc());
            //     ctx->open(state);
            //     int index = _conjunct_ctxs.size();
            //     _conjunct_ctxs.push_back(ctx);
            //     // it's safe to store address from a fix-resized vector
            //     _conjunctid_to_runtime_filter_ctxs[index] = &_runtime_filter_ctxs[i];
            // }
        }
    }

    // rebuild vexpr
    for (int i = 0; i < _runtime_filter_ctxs.size(); ++i) {
        if (!_runtime_filter_ctxs[i].apply_mark) {
            continue;
        }
        IRuntimeFilter* runtime_filter = _runtime_filter_ctxs[i].runtimefilter;
        std::vector<VExpr*> vexprs;
        runtime_filter->get_prepared_vexprs(&vexprs, row_desc());
        if (vexprs.empty()) {
            continue;
        }
        auto last_expr = _vconjunct_ctx_ptr ? (*_vconjunct_ctx_ptr)->root() : vexprs[0];
        for (size_t j = _vconjunct_ctx_ptr ? 0 : 1; j < vexprs.size(); j++) {
            TExprNode texpr_node;
            texpr_node.__set_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
            texpr_node.__set_node_type(TExprNodeType::COMPOUND_PRED);
            texpr_node.__set_opcode(TExprOpcode::COMPOUND_AND);
            VExpr* new_node = _pool->add(new VcompoundPred(texpr_node));
            new_node->add_child(last_expr);
            new_node->add_child(vexprs[j]);
            last_expr = new_node;
        }
        auto new_vconjunct_ctx_ptr = _pool->add(new VExprContext(last_expr));
        auto expr_status = new_vconjunct_ctx_ptr->prepare(state, row_desc());
        if (UNLIKELY(!expr_status.OK())) {
            LOG(WARNING) << "Something wrong for runtime filters: " << expr_status;
            vexprs.clear();
            break;
        }

        expr_status = new_vconjunct_ctx_ptr->open(state);
        if (UNLIKELY(!expr_status.OK())) {
            LOG(WARNING) << "Something wrong for runtime filters: " << expr_status;
            vexprs.clear();
            break;
        }
        if (_vconjunct_ctx_ptr) {
            _stale_vexpr_ctxs.push_back(std::move(_vconjunct_ctx_ptr));
        }
        _vconjunct_ctx_ptr.reset(new doris::vectorized::VExprContext*);
        *(_vconjunct_ctx_ptr.get()) = new_vconjunct_ctx_ptr;
        _runtime_filter_ready_flag[i] = true;
    }
    return Status::OK();
}

Status FileScanNode::start_scanners() {
    {
        std::unique_lock<std::mutex> l(_batch_queue_lock);
        _num_running_scanners = _scan_ranges.size();
    }

    _scanners_status.resize(_scan_ranges.size());
    COUNTER_UPDATE(_num_scanners, _scan_ranges.size());
    ThreadPoolToken* thread_token = _runtime_state->get_query_fragments_ctx()->get_token();
    PriorityThreadPool* thread_pool = _runtime_state->exec_env()->scan_thread_pool();
    for (int i = 0; i < _scan_ranges.size(); ++i) {
        Status submit_status = Status::OK();
        if (thread_token != nullptr) {
            submit_status = thread_token->submit_func(std::bind(&FileScanNode::scanner_worker, this,
                                                                i, _scan_ranges.size(),
                                                                std::ref(_scanners_status[i])));
        } else {
            PriorityThreadPool::WorkFunction task =
                    std::bind(&FileScanNode::scanner_worker, this, i, _scan_ranges.size(),
                              std::ref(_scanners_status[i]));
            if (!thread_pool->offer(task)) {
                submit_status = Status::Cancelled("Failed to submit scan task");
            }
        }
        if (!submit_status.ok()) {
            LOG(WARNING) << "Failed to assign file scanner task to thread pool! "
                         << submit_status.get_error_msg();
            _scanners_status[i].set_value(submit_status);
            for (int j = i + 1; j < _scan_ranges.size(); ++j) {
                _scanners_status[j].set_value(Status::Cancelled("Cancelled"));
            }
            {
                std::lock_guard<std::mutex> l(_batch_queue_lock);
                update_status(submit_status);
                _num_running_scanners -= _scan_ranges.size() - i;
            }
            _queue_writer_cond.notify_all();
            break;
        }
    }
    return Status::OK();
}

Status FileScanNode::get_next(RuntimeState* state, vectorized::Block* block, bool* eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    // check if CANCELLED.
    if (state->is_cancelled()) {
        std::unique_lock<std::mutex> l(_batch_queue_lock);
        if (update_status(Status::Cancelled("Cancelled"))) {
            // Notify all scanners
            _queue_writer_cond.notify_all();
        }
    }

    if (_scan_finished.load()) {
        *eos = true;
        return Status::OK();
    }

    const int batch_size = _runtime_state->batch_size();
    while (true) {
        std::shared_ptr<vectorized::Block> scanner_block;
        {
            std::unique_lock<std::mutex> l(_batch_queue_lock);
            while (_process_status.ok() && !_runtime_state->is_cancelled() &&
                   _num_running_scanners > 0 && _block_queue.empty()) {
                SCOPED_TIMER(_wait_scanner_timer);
                _queue_reader_cond.wait_for(l, std::chrono::seconds(1));
            }
            if (!_process_status.ok()) {
                // Some scanner process failed.
                return _process_status;
            }
            if (_runtime_state->is_cancelled()) {
                if (update_status(Status::Cancelled("Cancelled"))) {
                    _queue_writer_cond.notify_all();
                }
                return _process_status;
            }
            if (!_block_queue.empty()) {
                scanner_block = _block_queue.front();
                _block_queue.pop_front();
            }
        }

        // All scanner has been finished, and all cached batch has been read
        if (!scanner_block) {
            if (_mutable_block && !_mutable_block->empty()) {
                *block = _mutable_block->to_block();
                reached_limit(block, eos);
                LOG_IF(INFO, *eos) << "FileScanNode ReachedLimit.";
            }
            _scan_finished.store(true);
            *eos = true;
            return Status::OK();
        }
        // notify one scanner
        _queue_writer_cond.notify_one();

        if (UNLIKELY(!_mutable_block)) {
            _mutable_block.reset(new MutableBlock(scanner_block->clone_empty()));
        }

        if (_mutable_block->rows() + scanner_block->rows() < batch_size) {
            // merge scanner_block into _mutable_block
            _mutable_block->add_rows(scanner_block.get(), 0, scanner_block->rows());
            continue;
        } else {
            if (_mutable_block->empty()) {
                // directly use scanner_block
                *block = *scanner_block;
            } else {
                // copy _mutable_block firstly, then merge scanner_block into _mutable_block for next.
                *block = _mutable_block->to_block();
                _mutable_block->set_muatable_columns(scanner_block->clone_empty_columns());
                _mutable_block->add_rows(scanner_block.get(), 0, scanner_block->rows());
            }
            break;
        }
    }

    reached_limit(block, eos);
    if (*eos) {
        _scan_finished.store(true);
        _queue_writer_cond.notify_all();
        LOG(INFO) << "FileScanNode ReachedLimit.";
    } else {
        *eos = false;
    }

    return Status::OK();
}

Status FileScanNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    _scan_finished.store(true);
    _queue_writer_cond.notify_all();
    _queue_reader_cond.notify_all();
    {
        std::unique_lock<std::mutex> l(_batch_queue_lock);
        _queue_reader_cond.wait(l, [this] { return _num_running_scanners == 0; });
    }
    for (int i = 0; i < _scanners_status.size(); i++) {
        std::future<Status> f = _scanners_status[i].get_future();
        RETURN_IF_ERROR(f.get());
    }
    // Close
    _batch_queue.clear();

    for (auto& filter_desc : _runtime_filter_descs) {
        IRuntimeFilter* runtime_filter = nullptr;
        state->runtime_filter_mgr()->get_consume_filter(filter_desc.filter_id, &runtime_filter);
        DCHECK(runtime_filter != nullptr);
        runtime_filter->consumer_close();
    }

    for (auto& ctx : _stale_vexpr_ctxs) {
        (*ctx)->close(state);
    }

    return ExecNode::close(state);
}

Status FileScanNode::scanner_scan(const TFileScanRange& scan_range, ScannerCounter* counter) {
    //create scanner object and open
    std::unique_ptr<FileScanner> scanner = create_scanner(scan_range, counter);
    RETURN_IF_ERROR(scanner->open());
    bool scanner_eof = false;
    while (!scanner_eof) {
        RETURN_IF_CANCELLED(_runtime_state);
        // If we have finished all works
        if (_scan_finished.load() || !_process_status.ok()) {
            return Status::OK();
        }

        std::shared_ptr<vectorized::Block> block(new vectorized::Block());
        RETURN_IF_ERROR(scanner->get_next(block.get(), &scanner_eof));
        if (block->rows() == 0) {
            continue;
        }
        auto old_rows = block->rows();
        {
            SCOPED_TIMER(_filter_timer);
            RETURN_IF_ERROR(VExprContext::filter_block(_vconjunct_ctx_ptr, block.get(),
                                                       _tuple_desc->slots().size()));
        }
        counter->num_rows_unselected += old_rows - block->rows();
        if (block->rows() == 0) {
            continue;
        }

        std::unique_lock<std::mutex> l(_batch_queue_lock);
        while (_process_status.ok() && !_scan_finished.load() && !_runtime_state->is_cancelled() &&
               // stop pushing more batch if
               // 1. too many batches in queue, or
               // 2. at least one batch in queue and memory exceed limit.
               (_block_queue.size() >= _max_buffered_batches ||
                (thread_context()
                         ->_thread_mem_tracker_mgr->limiter_mem_tracker()
                         ->any_limit_exceeded() &&
                 !_block_queue.empty()))) {
            _queue_writer_cond.wait_for(l, std::chrono::seconds(1));
        }
        // Process already set failed, so we just return OK
        if (!_process_status.ok()) {
            return Status::OK();
        }
        // Scan already finished, just return
        if (_scan_finished.load()) {
            return Status::OK();
        }
        // Runtime state is canceled, just return cancel
        if (_runtime_state->is_cancelled()) {
            return Status::Cancelled("Cancelled");
        }
        // Queue size Must be smaller than _max_buffered_batches
        _block_queue.push_back(block);

        // Notify reader to process
        _queue_reader_cond.notify_one();
    }
    return Status::OK();
}

void FileScanNode::scanner_worker(int start_idx, int length, std::promise<Status>& p_status) {
    Thread::set_self_name("file_scanner");
    Status status = Status::OK();
    ScannerCounter counter;
    const TFileScanRange& scan_range =
            _scan_ranges[start_idx].scan_range.ext_scan_range.file_scan_range;
    status = scanner_scan(scan_range, &counter);
    if (!status.ok()) {
        LOG(WARNING) << "Scanner[" << start_idx
                     << "] process failed. status=" << status.get_error_msg();
    }

    // Update stats
    _runtime_state->update_num_rows_load_filtered(counter.num_rows_filtered);
    _runtime_state->update_num_rows_load_unselected(counter.num_rows_unselected);
    COUNTER_UPDATE(_num_rows_filtered, counter.num_rows_unselected);

    // scanner is going to finish
    {
        std::lock_guard<std::mutex> l(_batch_queue_lock);
        if (!status.ok()) {
            update_status(status);
        }
        // This scanner will finish
        _num_running_scanners--;
    }
    _queue_reader_cond.notify_all();
    // If one scanner failed, others don't need scan any more
    if (!status.ok()) {
        _queue_writer_cond.notify_all();
    }
    p_status.set_value(status);
}

std::unique_ptr<FileScanner> FileScanNode::create_scanner(const TFileScanRange& scan_range,
                                                          ScannerCounter* counter) {
    FileScanner* scan = nullptr;
    switch (scan_range.params.format_type) {
    case TFileFormatType::FORMAT_PARQUET:
        scan = new VFileParquetScanner(_runtime_state, runtime_profile(), scan_range.params,
                                       scan_range.ranges, _pre_filter_texprs, counter);
        break;
    case TFileFormatType::FORMAT_ORC:
        scan = new VFileORCScanner(_runtime_state, runtime_profile(), scan_range.params,
                                   scan_range.ranges, _pre_filter_texprs, counter);
        break;

    default:
        scan = new FileTextScanner(_runtime_state, runtime_profile(), scan_range.params,
                                   scan_range.ranges, _pre_filter_texprs, counter);
    }
    scan->reg_conjunct_ctxs(_tuple_id, _conjunct_ctxs);
    std::unique_ptr<FileScanner> scanner(scan);
    return scanner;
}

// This function is called after plan node has been prepared.
Status FileScanNode::set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) {
    int max_scanners = config::doris_scanner_thread_pool_thread_num;
    if (scan_ranges.size() <= max_scanners) {
        _scan_ranges = scan_ranges;
    } else {
        // There is no need for the number of scanners to exceed the number of threads in thread pool.
        _scan_ranges.clear();
        auto range_iter = scan_ranges.begin();
        for (int i = 0; i < max_scanners && range_iter != scan_ranges.end(); ++i, ++range_iter) {
            _scan_ranges.push_back(*range_iter);
        }
        for (int i = 0; range_iter != scan_ranges.end(); ++i, ++range_iter) {
            if (i == max_scanners) {
                i = 0;
            }
            auto& ranges = _scan_ranges[i].scan_range.ext_scan_range.file_scan_range.ranges;
            auto& merged_ranges = range_iter->scan_range.ext_scan_range.file_scan_range.ranges;
            ranges.insert(ranges.end(), merged_ranges.begin(), merged_ranges.end());
        }
        _scan_ranges.shrink_to_fit();
        LOG(INFO) << "Merge " << scan_ranges.size() << " scan ranges to " << _scan_ranges.size();
    }
    return Status::OK();
}

void FileScanNode::debug_string(int ident_level, std::stringstream* out) const {
    (*out) << "FileScanNode";
}

} // namespace doris::vectorized
