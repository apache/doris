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

#include "vec/exec/volap_scan_node.h"

#include "common/resource_tls.h"
#include "exec/scan_node.h"
#include "gen_cpp/PlanNodes_types.h"
#include "olap/storage_engine.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/large_int_value.h"
#include "runtime/runtime_filter_mgr.h"
#include "util/priority_thread_pool.hpp"
#include "util/to_string.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/exec/volap_scanner.h"
#include "vec/exprs/vbloom_predicate.h"
#include "vec/exprs/vcompound_pred.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vruntimefilter_wrapper.h"
#include "vec/functions/in.h"

namespace doris::vectorized {
using doris::operator<<;

#define RETURN_IF_PUSH_DOWN(stmt) \
    if (!push_down) {             \
        stmt;                     \
    } else {                      \
        return;                   \
    }

VOlapScanNode::VOlapScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ScanNode(pool, tnode, descs),
          _tuple_id(tnode.olap_scan_node.tuple_id),
          _olap_scan_node(tnode.olap_scan_node),
          _tuple_desc(nullptr),
          _tuple_idx(0),
          _eos(false),
          _max_materialized_row_batches(config::doris_scanner_queue_size),
          _start(false),
          _scanner_done(false),
          _transfer_done(false),
          _status(Status::OK()),
          _resource_info(nullptr),
          _buffered_bytes(0),
          _eval_conjuncts_fn(nullptr),
          _runtime_filter_descs(tnode.runtime_filters),
          _max_materialized_blocks(config::doris_scanner_queue_size) {
    _materialized_blocks.reserve(_max_materialized_blocks);
    _free_blocks.reserve(_max_materialized_blocks);
    // if sort_info is set, push _limit to each olap scanner
    if (_olap_scan_node.__isset.sort_info && _olap_scan_node.__isset.sort_limit) {
        _limit_per_scanner = _olap_scan_node.sort_limit;
    }
}

Status VOlapScanNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));

    const TQueryOptions& query_options = state->query_options();
    if (query_options.__isset.max_scan_key_num) {
        _max_scan_key_num = query_options.max_scan_key_num;
    } else {
        _max_scan_key_num = config::doris_max_scan_key_num;
    }

    if (query_options.__isset.max_pushdown_conditions_per_column) {
        _max_pushdown_conditions_per_column = query_options.max_pushdown_conditions_per_column;
    } else {
        _max_pushdown_conditions_per_column = config::max_pushdown_conditions_per_column;
    }

    _max_scanner_queue_size_bytes = query_options.mem_limit / 20; //TODO: session variable percent

    /// TODO: could one filter used in the different scan_node ?
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
    }

    return Status::OK();
}

void VOlapScanNode::init_scan_profile() {
    std::string scanner_profile_name = "VOlapScanner";
    if (_olap_scan_node.__isset.table_name) {
        scanner_profile_name = fmt::format("VOlapScanner({0})", _olap_scan_node.table_name);
    }
    _scanner_profile.reset(new RuntimeProfile(scanner_profile_name));
    runtime_profile()->add_child(_scanner_profile.get(), true, nullptr);

    _segment_profile.reset(new RuntimeProfile("SegmentIterator"));
    _scanner_profile->add_child(_segment_profile.get(), true, nullptr);
}

void VOlapScanNode::_init_counter(RuntimeState* state) {
    ADD_TIMER(_scanner_profile, "ShowHintsTime_V1");

    _reader_init_timer = ADD_TIMER(_scanner_profile, "ReaderInitTime");
    _read_compressed_counter = ADD_COUNTER(_segment_profile, "CompressedBytesRead", TUnit::BYTES);
    _read_uncompressed_counter =
            ADD_COUNTER(_segment_profile, "UncompressedBytesRead", TUnit::BYTES);
    _block_load_timer = ADD_TIMER(_segment_profile, "BlockLoadTime");
    _block_load_counter = ADD_COUNTER(_segment_profile, "BlocksLoad", TUnit::UNIT);
    _block_fetch_timer = ADD_TIMER(_scanner_profile, "BlockFetchTime");
    _raw_rows_counter = ADD_COUNTER(_segment_profile, "RawRowsRead", TUnit::UNIT);
    _block_convert_timer = ADD_TIMER(_scanner_profile, "BlockConvertTime");
    // Will be delete after non-vectorized code is removed
    _block_seek_timer = ADD_TIMER(_segment_profile, "BlockSeekTime");
    _block_seek_counter = ADD_COUNTER(_segment_profile, "BlockSeekCount", TUnit::UNIT);
    _block_init_timer = ADD_TIMER(_segment_profile, "BlockInitTime");
    _block_init_seek_timer = ADD_TIMER(_segment_profile, "BlockInitSeekTime");
    _block_init_seek_counter = ADD_COUNTER(_segment_profile, "BlockInitSeekCount", TUnit::UNIT);

    _rows_vec_cond_counter = ADD_COUNTER(_segment_profile, "RowsVectorPredFiltered", TUnit::UNIT);
    _vec_cond_timer = ADD_TIMER(_segment_profile, "VectorPredEvalTime");
    _short_cond_timer = ADD_TIMER(_segment_profile, "ShortPredEvalTime");
    _first_read_timer = ADD_TIMER(_segment_profile, "FirstReadTime");
    _first_read_seek_timer = ADD_TIMER(_segment_profile, "FirstReadSeekTime");
    _first_read_seek_counter = ADD_COUNTER(_segment_profile, "FirstReadSeekCount", TUnit::UNIT);

    _lazy_read_timer = ADD_TIMER(_segment_profile, "LazyReadTime");
    _lazy_read_seek_timer = ADD_TIMER(_segment_profile, "LazyReadSeekTime");
    _lazy_read_seek_counter = ADD_COUNTER(_segment_profile, "LazyReadSeekCount", TUnit::UNIT);

    _output_col_timer = ADD_TIMER(_segment_profile, "OutputColumnTime");

    _stats_filtered_counter = ADD_COUNTER(_segment_profile, "RowsStatsFiltered", TUnit::UNIT);
    _bf_filtered_counter = ADD_COUNTER(_segment_profile, "RowsBloomFilterFiltered", TUnit::UNIT);
    _del_filtered_counter = ADD_COUNTER(_scanner_profile, "RowsDelFiltered", TUnit::UNIT);
    _conditions_filtered_counter =
            ADD_COUNTER(_segment_profile, "RowsConditionsFiltered", TUnit::UNIT);
    _key_range_filtered_counter =
            ADD_COUNTER(_segment_profile, "RowsKeyRangeFiltered", TUnit::UNIT);

    _io_timer = ADD_TIMER(_segment_profile, "IOTimer");
    _decompressor_timer = ADD_TIMER(_segment_profile, "DecompressorTimer");
    _index_load_timer = ADD_TIMER(_segment_profile, "IndexLoadTime_V1");

    _scan_timer = ADD_TIMER(_scanner_profile, "ScanTime");
    _scan_cpu_timer = ADD_TIMER(_scanner_profile, "ScanCpuTime");

    _total_pages_num_counter = ADD_COUNTER(_segment_profile, "TotalPagesNum", TUnit::UNIT);
    _cached_pages_num_counter = ADD_COUNTER(_segment_profile, "CachedPagesNum", TUnit::UNIT);

    _bitmap_index_filter_counter =
            ADD_COUNTER(_segment_profile, "RowsBitmapIndexFiltered", TUnit::UNIT);
    _bitmap_index_filter_timer = ADD_TIMER(_segment_profile, "BitmapIndexFilterTimer");

    _num_scanners = ADD_COUNTER(_runtime_profile, "NumScanners", TUnit::UNIT);

    _filtered_segment_counter = ADD_COUNTER(_segment_profile, "NumSegmentFiltered", TUnit::UNIT);
    _total_segment_counter = ADD_COUNTER(_segment_profile, "NumSegmentTotal", TUnit::UNIT);

    // time of transfer thread to wait for row batch from scan thread
    _scanner_wait_batch_timer = ADD_TIMER(_runtime_profile, "ScannerBatchWaitTime");
    // time of scan thread to wait for worker thread of the thread pool
    _scanner_wait_worker_timer = ADD_TIMER(_runtime_profile, "ScannerWorkerWaitTime");

    // time of node to wait for batch/block queue
    _olap_wait_batch_queue_timer = ADD_TIMER(_runtime_profile, "BatchQueueWaitTime");

    // for the purpose of debugging or profiling
    for (int i = 0; i < GENERAL_DEBUG_COUNT; ++i) {
        char name[64];
        snprintf(name, sizeof(name), "GeneralDebugTimer%d", i);
        _general_debug_timer[i] = ADD_TIMER(_segment_profile, name);
    }
}

Status VOlapScanNode::prepare(RuntimeState* state) {
    init_scan_profile();
    RETURN_IF_ERROR(ScanNode::prepare(state));
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker());
    // create scanner profile
    // create timer
    _tablet_counter = ADD_COUNTER(runtime_profile(), "TabletCount ", TUnit::UNIT);
    _scanner_sched_counter = ADD_COUNTER(runtime_profile(), "ScannerSchedCount ", TUnit::UNIT);

    _rows_pushed_cond_filtered_counter =
            ADD_COUNTER(_scanner_profile, "RowsPushedCondFiltered", TUnit::UNIT);
    _init_counter(state);
    _tuple_desc = state->desc_tbl().get_tuple_descriptor(_tuple_id);

    if (_tuple_desc == nullptr) {
        // TODO: make sure we print all available diagnostic output to our error log
        return Status::InternalError("Failed to get tuple descriptor.");
    }

    const std::vector<SlotDescriptor*>& slots = _tuple_desc->slots();

    for (int i = 0; i < slots.size(); ++i) {
        if (!slots[i]->is_materialized()) {
            continue;
        }

        if (slots[i]->type().is_collection_type()) {
            _collection_slots.push_back(slots[i]);
        }

        if (slots[i]->type().is_string_type()) {
            _string_slots.push_back(slots[i]);
        }
    }

    _runtime_state = state;
    for (size_t i = 0; i < _runtime_filter_descs.size(); ++i) {
        _runtime_filter_ctxs[i].runtimefilter->init_profile(_runtime_profile.get());
    }
    return Status::OK();
}

Status VOlapScanNode::open(RuntimeState* state) {
    START_AND_SCOPE_SPAN(state->get_tracer(), span, "VOlapScanNode::open");
    VLOG_CRITICAL << "VOlapScanNode::Open";
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(ExecNode::open(state));
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker());

    _resource_info = ResourceTls::get_resource_tls();

    // acquire runtime filter
    _runtime_filter_ctxs.resize(_runtime_filter_descs.size());

    std::vector<VExpr*> vexprs;
    for (size_t i = 0; i < _runtime_filter_descs.size(); ++i) {
        IRuntimeFilter* runtime_filter = _runtime_filter_ctxs[i].runtimefilter;
        bool ready = runtime_filter->is_ready();
        if (!ready) {
            ready = runtime_filter->await();
        }
        if (ready) {
            RETURN_IF_ERROR(runtime_filter->get_push_expr_ctxs(&vexprs));
            _runtime_filter_ctxs[i].apply_mark = true;
            _runtime_filter_ctxs[i].runtimefilter = runtime_filter;
        }
    }
    RETURN_IF_ERROR(_append_rf_into_conjuncts(state, vexprs));

    return Status::OK();
}

void VOlapScanNode::transfer_thread(RuntimeState* state) {
    // scanner open pushdown to scanThread
    START_AND_SCOPE_SPAN(state->get_tracer(), span, "VOlapScanNode::transfer_thread");
    SCOPED_ATTACH_TASK(state);
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker());
    Status status = Status::OK();

    if (_vconjunct_ctx_ptr) {
        for (auto scanner : _volap_scanners) {
            status = (*_vconjunct_ctx_ptr)->clone(state, scanner->vconjunct_ctx_ptr());
            if (!status.ok()) {
                std::lock_guard<SpinLock> guard(_status_mutex);
                _status = status;
                break;
            }
        }
    }

    /*********************************
     * 优先级调度基本策略:
     * 1. 通过查询拆分的Range个数来确定初始nice值
     *    Range个数越多，越倾向于认定为大查询，nice值越小
     * 2. 通过查询累计读取的数据量来调整nice值
     *    读取的数据越多，越倾向于认定为大查询，nice值越小
     * 3. 通过nice值来判断查询的优先级
     *    nice值越大的，越优先获得的查询资源
     * 4. 定期提高队列内残留任务的优先级，避免大查询完全饿死
     *********************************/
    _total_assign_num = 0;
    _nice = 18 + std::max(0, 2 - (int)_volap_scanners.size() / 5);

    auto doris_scanner_row_num =
            _limit == -1 ? config::doris_scanner_row_num
                         : std::min(static_cast<int64_t>(config::doris_scanner_row_num), _limit);
    _block_size = _limit == -1 ? state->batch_size()
                               : std::min(static_cast<int64_t>(state->batch_size()), _limit);
    auto block_per_scanner = (doris_scanner_row_num + (_block_size - 1)) / _block_size;
    auto pre_block_count =
            std::min(_volap_scanners.size(),
                     static_cast<size_t>(config::doris_scanner_thread_pool_thread_num)) *
            block_per_scanner;

    for (int i = 0; i < pre_block_count; ++i) {
        auto block = new Block(_tuple_desc->slots(), _block_size);
        _free_blocks.emplace_back(block);
        _buffered_bytes += block->allocated_bytes();
    }

    // read from scanner
    while (LIKELY(status.ok())) {
        int assigned_thread_num = _start_scanner_thread_task(state, block_per_scanner);

        std::vector<Block*> blocks;
        {
            // 1 scanner idle task not empty, assign new scanner task
            std::unique_lock<std::mutex> l(_scan_blocks_lock);

            // scanner_row_num = 16k
            // 16k * 10 * 12 * 8 = 15M(>2s)  --> nice=10
            // 16k * 20 * 22 * 8 = 55M(>6s)  --> nice=0
            while (_nice > 0 && _total_assign_num > (22 - _nice) * (20 - _nice) * 6) {
                --_nice;
            }

            // 2 wait when all scanner are running & no result in queue
            while (UNLIKELY(_running_thread == assigned_thread_num && _scan_blocks.empty() &&
                            !_scanner_done)) {
                SCOPED_TIMER(_scanner_wait_batch_timer);
                _scan_block_added_cv.wait(l);
            }

            // 3 transfer result block when queue is not empty
            if (LIKELY(!_scan_blocks.empty())) {
                blocks.swap(_scan_blocks);
                for (auto b : blocks) {
                    _scan_row_batches_bytes -= b->allocated_bytes();
                }
                // delete scan_block if transfer thread should be stopped
                // because scan_block wouldn't be useful anymore
                if (UNLIKELY(_transfer_done)) {
                    std::for_each(blocks.begin(), blocks.end(), std::default_delete<Block>());
                    blocks.clear();
                }
            } else {
                if (_scanner_done) {
                    // We should close eof scanners before transfer done, otherwise,
                    // they are closed until scannode is closed. Because plan is closed
                    // after the plan is finished, so query profile would leak stats from
                    // scanners closed by scannode::close.
                    while (!_volap_scanners.empty()) {
                        auto scanner = _volap_scanners.front();
                        _volap_scanners.pop_front();
                        DCHECK(scanner->need_to_close());
                        scanner->close(state);
                    }
                    break;
                }
            }
        }

        if (!blocks.empty()) {
            _add_blocks(blocks);
        }
    }

    VLOG_CRITICAL << "TransferThread finish.";
    {
        std::unique_lock<std::mutex> l(_blocks_lock);
        _transfer_done = true;
    }
    _block_added_cv.notify_all();
    {
        std::unique_lock<std::mutex> l(_scan_blocks_lock);
        _scan_thread_exit_cv.wait(l, [this] { return _running_thread == 0; });
    }
    VLOG_CRITICAL << "Scanner threads have been exited. TransferThread exit.";
}

void VOlapScanNode::scanner_thread(VOlapScanner* scanner) {
    SCOPED_ATTACH_TASK(_runtime_state->scanner_mem_tracker(),
                       ThreadContext::query_to_task_type(_runtime_state->query_type()),
                       print_id(_runtime_state->query_id()),
                       _runtime_state->fragment_instance_id());
    Thread::set_self_name("volap_scanner");
    int64_t wait_time = scanner->update_wait_worker_timer();
    // Do not use ScopedTimer. There is no guarantee that, the counter
    // (_scan_cpu_timer, the class member) is not destroyed after `_running_thread==0`.
    ThreadCpuStopWatch cpu_watch;
    cpu_watch.start();
    Status status = Status::OK();
    bool eos = false;
    RuntimeState* state = scanner->runtime_state();
    DCHECK(nullptr != state);
    if (!scanner->is_open()) {
        status = scanner->open();
        if (!status.ok()) {
            std::lock_guard<SpinLock> guard(_status_mutex);
            _status = status;
            eos = true;
        }
        scanner->set_opened();
    }

    std::vector<VExpr*> vexprs;
    auto& scanner_filter_apply_marks = *scanner->mutable_runtime_filter_marks();
    DCHECK(scanner_filter_apply_marks.size() == _runtime_filter_descs.size());
    for (size_t i = 0; i < scanner_filter_apply_marks.size(); i++) {
        if (!scanner_filter_apply_marks[i] && !_runtime_filter_ctxs[i].apply_mark) {
            /// When runtime filters are ready during running, we should use them to filter data
            /// in VOlapScanner.
            /// New arrival rf will be processed as below:
            /// 1. convert these runtime filters to vectorized expressions
            /// 2. if this is the first scanner thread to receive this rf, construct a new
            /// VExprContext and update `_vconjunct_ctx_ptr` in scan node. Notice that we use
            /// `_runtime_filter_ready_flag` to ensure `_vconjunct_ctx_ptr` will be updated only
            /// once after any runtime_filters are ready.
            /// 3. finally, just copy this new VExprContext to scanner and use it to filter data.
            IRuntimeFilter* runtime_filter = _runtime_filter_ctxs[i].runtimefilter;
            DCHECK(runtime_filter != nullptr);
            bool ready = runtime_filter->is_ready();
            if (ready) {
                runtime_filter->get_prepared_vexprs(&vexprs, _row_descriptor);
                scanner_filter_apply_marks[i] = true;
                if (!_runtime_filter_ready_flag[i] && !vexprs.empty()) {
                    std::lock_guard<std::shared_mutex> l(_rf_lock);
                    if (!_runtime_filter_ready_flag[i]) {
                        // Use all conjuncts and new arrival runtime filters to construct a new
                        // expression tree here.
                        _append_rf_into_conjuncts(state, vexprs);
                        _runtime_filter_ready_flag[i] = true;
                    }
                }
            }
        }
    }

    if (!vexprs.empty()) {
        if (*scanner->vconjunct_ctx_ptr()) {
            scanner->discard_conjuncts();
        }
        {
            std::shared_lock<std::shared_mutex> l(_rf_lock);
            WARN_IF_ERROR((*_vconjunct_ctx_ptr)->clone(state, scanner->vconjunct_ctx_ptr()),
                          "Something wrong for runtime filters: ");
        }
    }

    std::vector<Block*> blocks;

    // Because we use thread pool to scan data from storage. One scanner can't
    // use this thread too long, this can starve other query's scanner. So, we
    // need yield this thread when we do enough work. However, OlapStorage read
    // data in pre-aggregate mode, then we can't use storage returned data to
    // judge if we need to yield. So we record all raw data read in this round
    // scan, if this exceed row number or bytes threshold, we yield this thread.
    int64_t raw_rows_read = scanner->raw_rows_read();
    int64_t raw_rows_threshold = raw_rows_read + config::doris_scanner_row_num;
    int64_t raw_bytes_read = 0;
    int64_t raw_bytes_threshold = config::doris_scanner_row_bytes;
    bool get_free_block = true;
    int num_rows_in_block = 0;

    // Has to wait at least one full block, or it will cause a lot of schedule task in priority
    // queue, it will affect query latency and query concurrency for example ssb 3.3.
    while (!eos && raw_bytes_read < raw_bytes_threshold &&
           ((raw_rows_read < raw_rows_threshold && get_free_block) ||
            num_rows_in_block < _runtime_state->batch_size())) {
        if (UNLIKELY(_transfer_done)) {
            eos = true;
            status = Status::Cancelled(
                    "Scan thread cancelled, cause query done, maybe reach limit.");
            break;
        }

        auto block = _alloc_block(get_free_block);
        status = scanner->get_block(_runtime_state, block, &eos);
        VLOG_ROW << "VOlapScanNode input rows: " << block->rows();
        if (!status.ok()) {
            LOG(WARNING) << "Scan thread read VOlapScanner failed: " << status.to_string();
            // Add block ptr in blocks, prevent mem leak in read failed
            blocks.push_back(block);
            eos = true;
            break;
        }

        raw_bytes_read += block->bytes();
        num_rows_in_block += block->rows();
        // 4. if status not ok, change status_.
        if (UNLIKELY(block->rows() == 0)) {
            std::lock_guard<std::mutex> l(_free_blocks_lock);
            _free_blocks.emplace_back(block);
        } else {
            if (!blocks.empty() &&
                blocks.back()->rows() + block->rows() <= _runtime_state->batch_size()) {
                MutableBlock(blocks.back()).merge(*block);
                block->clear_column_data();
                std::lock_guard<std::mutex> l(_free_blocks_lock);
                _free_blocks.emplace_back(block);
            } else {
                blocks.push_back(block);
            }
        }
        raw_rows_read = scanner->raw_rows_read();
    }

    {
        // if we failed, check status.
        if (UNLIKELY(!status.ok())) {
            _transfer_done = true;
            std::lock_guard<SpinLock> guard(_status_mutex);
            if (LIKELY(_status.ok())) {
                _status = status;
            }
        }

        bool global_status_ok = false;
        {
            std::lock_guard<SpinLock> guard(_status_mutex);
            global_status_ok = _status.ok();
        }
        if (UNLIKELY(!global_status_ok)) {
            eos = true;
            std::for_each(blocks.begin(), blocks.end(), std::default_delete<Block>());
        } else {
            std::lock_guard<std::mutex> l(_scan_blocks_lock);
            _scan_blocks.insert(_scan_blocks.end(), blocks.begin(), blocks.end());
            for (auto b : blocks) {
                _scan_row_batches_bytes += b->allocated_bytes();
            }
        }
        // If eos is true, we will process out of this lock block.
        if (eos) {
            scanner->mark_to_need_to_close();
        }
        std::lock_guard<std::mutex> l(_volap_scanners_lock);
        _volap_scanners.push_front(scanner);
    }
    if (eos) {
        std::lock_guard<std::mutex> l(_scan_blocks_lock);
        _progress.update(1);
        if (_progress.done()) {
            // this is the right out
            _scanner_done = true;
        }
    }
    _scan_cpu_timer->update(cpu_watch.elapsed_time());
    _scanner_wait_worker_timer->update(wait_time);

    std::unique_lock<std::mutex> l(_scan_blocks_lock);
    _running_thread--;

    // The transfer thead will wait for `_running_thread==0`, to make sure all scanner threads won't access class members.
    // Do not access class members after this code.
    _scan_block_added_cv.notify_one();
    _scan_thread_exit_cv.notify_one();
}

Status VOlapScanNode::_add_blocks(std::vector<Block*>& block) {
    {
        std::unique_lock<std::mutex> l(_blocks_lock);

        // check queue limit for both block queue size and bytes
        while (UNLIKELY((_materialized_blocks.size() >= _max_materialized_blocks ||
                         _materialized_row_batches_bytes >= _max_scanner_queue_size_bytes / 2) &&
                        !_transfer_done)) {
            _block_consumed_cv.wait(l);
        }

        VLOG_CRITICAL << "Push block to materialized_blocks";
        _materialized_blocks.insert(_materialized_blocks.end(), block.cbegin(), block.cend());
        for (auto b : block) {
            _materialized_row_batches_bytes += b->allocated_bytes();
        }
    }
    // remove one block, notify main thread
    _block_added_cv.notify_one();
    return Status::OK();
}

Status VOlapScanNode::normalize_conjuncts() {
    std::vector<SlotDescriptor*> slots = _tuple_desc->slots();

    for (int slot_idx = 0; slot_idx < slots.size(); ++slot_idx) {
        switch (slots[slot_idx]->type().type) {
#define M(NAME)                                                                \
    case TYPE_##NAME: {                                                        \
        ColumnValueRange<TYPE_##NAME> range(slots[slot_idx]->col_name(),       \
                                            slots[slot_idx]->type().precision, \
                                            slots[slot_idx]->type().scale);    \
        _id_to_slot_column_value_range[slots[slot_idx]->id()] =                \
                std::pair {slots[slot_idx], range};                            \
        break;                                                                 \
    }
#define APPLY_FOR_PRIMITIVE_TYPE(M) \
    M(TINYINT)                      \
    M(SMALLINT)                     \
    M(INT)                          \
    M(BIGINT)                       \
    M(LARGEINT)                     \
    M(CHAR)                         \
    M(DATE)                         \
    M(DATETIME)                     \
    M(DATEV2)                       \
    M(DATETIMEV2)                   \
    M(VARCHAR)                      \
    M(STRING)                       \
    M(HLL)                          \
    M(DECIMAL32)                    \
    M(DECIMAL64)                    \
    M(DECIMAL128)                   \
    M(DECIMALV2)                    \
    M(BOOLEAN)
            APPLY_FOR_PRIMITIVE_TYPE(M)
#undef M
        default: {
            VLOG_CRITICAL << "Unsupported Normalize Slot [ColName=" << slots[slot_idx]->col_name()
                          << "]";
            break;
        }
        }
    }
    if (_vconjunct_ctx_ptr) {
        if ((*_vconjunct_ctx_ptr)->root()) {
            VExpr* new_root = _normalize_predicate(_runtime_state, (*_vconjunct_ctx_ptr)->root());
            if (new_root) {
                (*_vconjunct_ctx_ptr)->set_root(new_root);
            } else {
                (*(_vconjunct_ctx_ptr.get()))->mark_as_stale();
                _stale_vexpr_ctxs.push_back(std::move(_vconjunct_ctx_ptr));
                _vconjunct_ctx_ptr.reset(nullptr);
            }
        }
    }
    for (auto& it : _id_to_slot_column_value_range) {
        std::visit(
                [&](auto&& range) {
                    if (range.is_empty_value_range()) {
                        _eos = true;
                    }
                },
                it.second.second);
        _column_value_ranges[it.second.first->col_name()] = it.second.second;
    }

    return Status::OK();
}

static std::string olap_filter_to_string(const doris::TCondition& condition) {
    auto op_name = condition.condition_op;
    if (condition.condition_op == "*=") {
        op_name = "IN";
    } else if (condition.condition_op == "!*=") {
        op_name = "NOT IN";
    }
    return fmt::format("{{{} {} {}}}", condition.column_name, op_name,
                       to_string(condition.condition_values));
}

static std::string olap_filters_to_string(const std::vector<doris::TCondition>& filters) {
    std::string filters_string;
    filters_string += "[";
    for (auto it = filters.cbegin(); it != filters.cend(); it++) {
        if (it != filters.cbegin()) {
            filters_string += ",";
        }
        filters_string += olap_filter_to_string(*it);
    }
    filters_string += "]";
    return filters_string;
}

Status VOlapScanNode::build_key_ranges_and_filters() {
    const std::vector<std::string>& column_names = _olap_scan_node.key_column_name;
    const std::vector<TPrimitiveType::type>& column_types = _olap_scan_node.key_column_type;
    DCHECK(column_types.size() == column_names.size());

    // 1. construct scan key except last olap engine short key
    _scan_keys.set_is_convertible(limit() == -1);

    // we use `exact_range` to identify a key range is an exact range or not when we convert
    // it to `_scan_keys`. If `exact_range` is true, we can just discard it from `_olap_filter`.
    bool exact_range = true;
    for (int column_index = 0; column_index < column_names.size() && !_scan_keys.has_range_value();
         ++column_index) {
        auto iter = _column_value_ranges.find(column_names[column_index]);
        if (_column_value_ranges.end() == iter) {
            break;
        }

        RETURN_IF_ERROR(std::visit(
                [&](auto&& range) {
                    RETURN_IF_ERROR(
                            _scan_keys.extend_scan_key(range, _max_scan_key_num, &exact_range));
                    if (exact_range) {
                        _column_value_ranges.erase(iter->first);
                    }
                    return Status::OK();
                },
                iter->second));
    }

    for (auto& iter : _column_value_ranges) {
        std::vector<TCondition> filters;
        std::visit([&](auto&& range) { range.to_olap_filter(filters); }, iter.second);

        for (const auto& filter : filters) {
            _olap_filter.push_back(std::move(filter));
        }
    }

    _runtime_profile->add_info_string("PushdownPredicate", olap_filters_to_string(_olap_filter));

    _runtime_profile->add_info_string("KeyRanges", _scan_keys.debug_string());

    VLOG_CRITICAL << _scan_keys.debug_string();

    return Status::OK();
}

Status VOlapScanNode::start_scan(RuntimeState* state) {
    RETURN_IF_CANCELLED(state);

    VLOG_CRITICAL << "NormalizeConjuncts";
    RETURN_IF_ERROR(normalize_conjuncts());

    if (_eos) {
        return Status::OK();
    }

    VLOG_CRITICAL << "BuildKeyRangesAndFilters";
    RETURN_IF_ERROR(build_key_ranges_and_filters());

    VLOG_CRITICAL << "StartScanThread";
    RETURN_IF_ERROR(start_scan_thread(state));

    return Status::OK();
}

static bool ignore_cast(SlotDescriptor* slot, VExpr* expr) {
    if (slot->type().is_date_type() && expr->type().is_date_type()) {
        return true;
    }
    if (slot->type().is_string_type() && expr->type().is_string_type()) {
        return true;
    }
    return false;
}

template <bool IsFixed, PrimitiveType PrimitiveType, typename ChangeFixedValueRangeFunc>
Status VOlapScanNode::change_value_range(ColumnValueRange<PrimitiveType>& temp_range, void* value,
                                         const ChangeFixedValueRangeFunc& func,
                                         const std::string& fn_name, int slot_ref_child) {
    if constexpr (PrimitiveType == TYPE_DATE) {
        DateTimeValue date_value;
        reinterpret_cast<VecDateTimeValue*>(value)->convert_vec_dt_to_dt(&date_value);
        if constexpr (IsFixed) {
            if (!date_value.check_loss_accuracy_cast_to_date()) {
                func(temp_range,
                     reinterpret_cast<typename PrimitiveTypeTraits<PrimitiveType>::CppType*>(
                             &date_value));
            }
        } else {
            if (date_value.check_loss_accuracy_cast_to_date()) {
                if (fn_name == "lt" || fn_name == "ge") {
                    ++date_value;
                }
            }
            func(temp_range, to_olap_filter_type(fn_name, slot_ref_child),
                 reinterpret_cast<typename PrimitiveTypeTraits<PrimitiveType>::CppType*>(
                         &date_value));
        }
    } else if constexpr (PrimitiveType == TYPE_DATETIME) {
        DateTimeValue date_value;
        reinterpret_cast<VecDateTimeValue*>(value)->convert_vec_dt_to_dt(&date_value);
        if constexpr (IsFixed) {
            func(temp_range,
                 reinterpret_cast<typename PrimitiveTypeTraits<PrimitiveType>::CppType*>(
                         &date_value));
        } else {
            func(temp_range, to_olap_filter_type(fn_name, slot_ref_child),
                 reinterpret_cast<typename PrimitiveTypeTraits<PrimitiveType>::CppType*>(
                         reinterpret_cast<char*>(&date_value)));
        }
    } else if constexpr ((PrimitiveType == TYPE_DECIMALV2) || (PrimitiveType == TYPE_CHAR) ||
                         (PrimitiveType == TYPE_VARCHAR) || (PrimitiveType == TYPE_HLL) ||
                         (PrimitiveType == TYPE_DATETIMEV2) || (PrimitiveType == TYPE_TINYINT) ||
                         (PrimitiveType == TYPE_SMALLINT) || (PrimitiveType == TYPE_INT) ||
                         (PrimitiveType == TYPE_BIGINT) || (PrimitiveType == TYPE_LARGEINT) ||
                         (PrimitiveType == TYPE_DECIMAL32) || (PrimitiveType == TYPE_DECIMAL64) ||
                         (PrimitiveType == TYPE_DECIMAL128) || (PrimitiveType == TYPE_STRING) ||
                         (PrimitiveType == TYPE_BOOLEAN) || (PrimitiveType == TYPE_DATEV2)) {
        if constexpr (IsFixed) {
            func(temp_range,
                 reinterpret_cast<typename PrimitiveTypeTraits<PrimitiveType>::CppType*>(value));
        } else {
            func(temp_range, to_olap_filter_type(fn_name, slot_ref_child),
                 reinterpret_cast<typename PrimitiveTypeTraits<PrimitiveType>::CppType*>(value));
        }
    } else {
        static_assert(always_false_v<PrimitiveType>);
    }

    return Status::OK();
}

bool VOlapScanNode::is_key_column(const std::string& key_name) {
    // all column in dup_keys table or unique_keys with merge on write table olap scan node threat
    // as key column
    if (_olap_scan_node.keyType == TKeysType::DUP_KEYS ||
        (_olap_scan_node.keyType == TKeysType::UNIQUE_KEYS &&
         _olap_scan_node.__isset.enable_unique_key_merge_on_write &&
         _olap_scan_node.enable_unique_key_merge_on_write)) {
        return true;
    }

    auto res = std::find(_olap_scan_node.key_column_name.begin(),
                         _olap_scan_node.key_column_name.end(), key_name);
    return res != _olap_scan_node.key_column_name.end();
}

Status VOlapScanNode::start_scan_thread(RuntimeState* state) {
    if (_scan_ranges.empty()) {
        _transfer_done = true;
        return Status::OK();
    }

    // ranges constructed from scan keys
    std::vector<std::unique_ptr<OlapScanRange>> cond_ranges;
    RETURN_IF_ERROR(_scan_keys.get_key_range(&cond_ranges));
    // if we can't get ranges from conditions, we give it a total range
    if (cond_ranges.empty()) {
        cond_ranges.emplace_back(new OlapScanRange());
    }
    bool need_split = true;
    // If we have ranges more than 64, there is no need to call
    // ShowHint to split ranges
    if (limit() != -1 || cond_ranges.size() > 64) {
        need_split = false;
    }
    int scanners_per_tablet = std::max(1, 64 / (int)_scan_ranges.size());

    std::unordered_set<std::string> disk_set;
    for (auto& scan_range : _scan_ranges) {
        auto tablet_id = scan_range->tablet_id;
        std::string err;
        TabletSharedPtr tablet =
                StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, true, &err);
        if (tablet == nullptr) {
            std::stringstream ss;
            ss << "failed to get tablet: " << tablet_id << ", reason: " << err;
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }

        std::vector<std::unique_ptr<OlapScanRange>>* ranges = &cond_ranges;
        std::vector<std::unique_ptr<OlapScanRange>> split_ranges;
        if (need_split && !tablet->all_beta()) {
            auto st = get_hints(tablet, *scan_range, config::doris_scan_range_row_count,
                                _scan_keys.begin_include(), _scan_keys.end_include(), cond_ranges,
                                &split_ranges, _runtime_profile.get());
            if (st.ok()) {
                ranges = &split_ranges;
            }
        }
        int size_based_scanners_per_tablet = 1;

        if (config::doris_scan_range_max_mb > 0) {
            size_based_scanners_per_tablet = std::max(
                    1, (int)(tablet->tablet_footprint() / (config::doris_scan_range_max_mb << 20)));
        }

        int ranges_per_scanner =
                std::max(1, (int)ranges->size() /
                                    std::min(scanners_per_tablet, size_based_scanners_per_tablet));
        int num_ranges = ranges->size();
        for (int i = 0; i < num_ranges;) {
            std::vector<OlapScanRange*> scanner_ranges;
            scanner_ranges.push_back((*ranges)[i].get());
            ++i;
            for (int j = 1; i < num_ranges && j < ranges_per_scanner &&
                            (*ranges)[i]->end_include == (*ranges)[i - 1]->end_include;
                 ++j, ++i) {
                scanner_ranges.push_back((*ranges)[i].get());
            }
            VOlapScanner* scanner = new VOlapScanner(state, this, _olap_scan_node.is_preaggregation,
                                                     _need_agg_finalize, *scan_range);
            // add scanner to pool before doing prepare.
            // so that scanner can be automatically deconstructed if prepare failed.
            _scanner_pool.add(scanner);
            RETURN_IF_ERROR(scanner->prepare(*scan_range, scanner_ranges, _olap_filter,
                                             _bloom_filters_push_down, _push_down_functions));

            _volap_scanners.push_back(scanner);
            disk_set.insert(scanner->scan_disk());
        }
    }
    COUNTER_SET(_num_disks_accessed_counter, static_cast<int64_t>(disk_set.size()));
    COUNTER_SET(_num_scanners, static_cast<int64_t>(_volap_scanners.size()));

    // init progress
    std::stringstream ss;
    ss << "ScanThread complete (node=" << id() << "):";
    _progress = ProgressUpdater(ss.str(), _volap_scanners.size(), 1);

    _transfer_thread.reset(new std::thread(
            [this, state, parent_span = opentelemetry::trace::Tracer::GetCurrentSpan()] {
                opentelemetry::trace::Scope scope {parent_span};
                transfer_thread(state);
            }));

    return Status::OK();
}

Status VOlapScanNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    START_AND_SCOPE_SPAN(state->get_tracer(), span, "VOlapScanNode::close");
    // change done status
    {
        std::unique_lock<std::mutex> l(_blocks_lock);
        _transfer_done = true;
    }
    // notify all scanner thread
    _block_consumed_cv.notify_all();
    _block_added_cv.notify_all();
    _scan_block_added_cv.notify_all();

    // join transfer thread
    if (_transfer_thread) {
        _transfer_thread->join();
    }

    // clear some block in queue
    // TODO: The presence of transfer_thread here may cause Block's memory alloc and be released not in a thread,
    // which may lead to potential performance problems. we should rethink whether to delete the transfer thread
    std::for_each(_materialized_blocks.begin(), _materialized_blocks.end(),
                  std::default_delete<Block>());
    _materialized_row_batches_bytes = 0;
    std::for_each(_scan_blocks.begin(), _scan_blocks.end(), std::default_delete<Block>());
    _scan_row_batches_bytes = 0;
    std::for_each(_free_blocks.begin(), _free_blocks.end(), std::default_delete<Block>());

    // OlapScanNode terminate by exception
    // so that initiative close the Scanner
    for (auto scanner : _volap_scanners) {
        scanner->close(state);
    }

    for (auto& filter_ctx : _runtime_filter_ctxs) {
        filter_ctx.runtimefilter->consumer_close();
    }

    for (auto& ctx : _stale_vexpr_ctxs) {
        (*ctx)->close(state);
    }

    VLOG_CRITICAL << "VOlapScanNode::close()";
    return ScanNode::close(state);
}

Status VOlapScanNode::get_next(RuntimeState* state, Block* block, bool* eos) {
    INIT_AND_SCOPE_GET_NEXT_SPAN(state->get_tracer(), _get_next_span, "VOlapScanNode::get_next");
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker());

    // check if Canceled.
    if (state->is_cancelled()) {
        std::unique_lock<std::mutex> l(_blocks_lock);
        _transfer_done = true;
        std::lock_guard<SpinLock> guard(_status_mutex);
        if (LIKELY(_status.ok())) {
            _status = Status::Cancelled("Cancelled");
        }
        return _status;
    }

    // check if started.
    if (!_start) {
        Status status = start_scan(state);

        if (!status.ok()) {
            LOG(ERROR) << "StartScan Failed cause " << status.get_error_msg();
            *eos = true;
            return status;
        }

        _start = true;
    }

    // some conjuncts will be disposed in start_scan function, so
    // we should check _eos after call start_scan
    if (_eos) {
        *eos = true;
        return Status::OK();
    }

    // wait for block from queue
    Block* materialized_block = nullptr;
    {
        std::unique_lock<std::mutex> l(_blocks_lock);
        SCOPED_TIMER(_olap_wait_batch_queue_timer);
        while (_materialized_blocks.empty() && !_transfer_done) {
            if (state->is_cancelled()) {
                _transfer_done = true;
            }

            // use wait_for, not wait, in case to capture the state->is_cancelled()
            _block_added_cv.wait_for(l, std::chrono::seconds(1));
        }

        if (!_materialized_blocks.empty()) {
            materialized_block = _materialized_blocks.back();
            DCHECK(materialized_block != nullptr);
            _materialized_blocks.pop_back();
            _materialized_row_batches_bytes -= materialized_block->allocated_bytes();
        }
    }

    // return block
    if (nullptr != materialized_block) {
        // notify scanner
        _block_consumed_cv.notify_one();
        // get scanner's block memory
        block->swap(*materialized_block);
        VLOG_ROW << "VOlapScanNode output rows: " << block->rows();
        reached_limit(block, eos);

        // reach scan node limit
        if (*eos) {
            {
                std::unique_lock<std::mutex> l(_blocks_lock);
                _transfer_done = true;
            }

            _block_consumed_cv.notify_all();
            *eos = true;
        } else {
            *eos = false;
        }

        {
            // ReThink whether the SpinLock Better
            std::lock_guard<std::mutex> l(_free_blocks_lock);
            _free_blocks.emplace_back(materialized_block);
        }
        return Status::OK();
    }

    // all scanner done, change *eos to true
    *eos = true;
    std::lock_guard<SpinLock> guard(_status_mutex);
    return _status;
}

Block* VOlapScanNode::_alloc_block(bool& get_free_block) {
    {
        std::lock_guard<std::mutex> l(_free_blocks_lock);
        if (!_free_blocks.empty()) {
            auto block = _free_blocks.back();
            _free_blocks.pop_back();
            return block;
        }
    }

    get_free_block = false;

    auto block = new Block(_tuple_desc->slots(), _block_size);
    _buffered_bytes += block->allocated_bytes();
    return block;
}

int VOlapScanNode::_start_scanner_thread_task(RuntimeState* state, int block_per_scanner) {
    std::list<VOlapScanner*> olap_scanners;
    int assigned_thread_num = _running_thread;
    size_t max_thread = config::doris_scanner_queue_size;
    if (config::doris_scanner_row_num > state->batch_size()) {
        max_thread /= config::doris_scanner_row_num / state->batch_size();
        if (max_thread <= 0) max_thread = 1;
    }
    // copy to local
    {
        // How many thread can apply to this query
        size_t thread_slot_num = 0;
        {
            if (_scan_row_batches_bytes < _max_scanner_queue_size_bytes / 2) {
                std::lock_guard<std::mutex> l(_free_blocks_lock);
                thread_slot_num = _free_blocks.size() / block_per_scanner;
                thread_slot_num += (_free_blocks.size() % block_per_scanner != 0);
                thread_slot_num = std::min(thread_slot_num, max_thread - assigned_thread_num);
                if (thread_slot_num <= 0) {
                    thread_slot_num = 1;
                }
            } else {
                std::lock_guard<std::mutex> l(_scan_blocks_lock);
                if (_scan_blocks.empty()) {
                    // Just for notify if _scan_blocks is empty and no running thread
                    if (assigned_thread_num == 0) {
                        thread_slot_num = 1;
                        // NOTE: if olap_scanners_ is empty, scanner_done_ should be true
                    }
                }
            }
        }

        {
            std::lock_guard<std::mutex> l(_volap_scanners_lock);
            thread_slot_num = std::min(thread_slot_num, _volap_scanners.size());
            for (int i = 0; i < thread_slot_num && !_volap_scanners.empty();) {
                auto scanner = _volap_scanners.front();
                _volap_scanners.pop_front();

                if (scanner->need_to_close()) {
                    scanner->close(state);
                } else {
                    olap_scanners.push_back(scanner);
                    _running_thread++;
                    assigned_thread_num++;
                    i++;
                }
            }
        }
    }

    // post volap scanners to thread-pool
    ThreadPoolToken* thread_token = state->get_query_fragments_ctx()->get_token();
    auto iter = olap_scanners.begin();
    if (thread_token != nullptr) {
        while (iter != olap_scanners.end()) {
            auto s = thread_token->submit_func(
                    [this, scanner = *iter] { this->scanner_thread(scanner); });
            if (s.ok()) {
                (*iter)->start_wait_worker_timer();
                COUNTER_UPDATE(_scanner_sched_counter, 1);
                olap_scanners.erase(iter++);
            } else {
                LOG(FATAL) << "Failed to assign scanner task to thread pool! " << s.get_error_msg();
            }
            ++_total_assign_num;
        }
    } else {
        PriorityThreadPool* thread_pool = state->exec_env()->scan_thread_pool();
        PriorityThreadPool* remote_thread_pool = state->exec_env()->remote_scan_thread_pool();
        while (iter != olap_scanners.end()) {
            PriorityThreadPool::Task task;
            task.work_function = [this, scanner = *iter] { this->scanner_thread(scanner); };
            task.priority = _nice;
            task.queue_id = state->exec_env()->store_path_to_index((*iter)->scan_disk());
            (*iter)->start_wait_worker_timer();

            TabletStorageType type = (*iter)->get_storage_type();
            bool ret = false;
            COUNTER_UPDATE(_scanner_sched_counter, 1);
            if (type == TabletStorageType::STORAGE_TYPE_LOCAL) {
                ret = thread_pool->offer(task);
            } else {
                ret = remote_thread_pool->offer(task);
            }

            if (ret) {
                olap_scanners.erase(iter++);
            } else {
                LOG(FATAL) << "Failed to assign scanner task to thread pool!";
            }
            ++_total_assign_num;
        }
    }

    return assigned_thread_num;
}

// PlanFragmentExecutor will call this method to set scan range
// Doris scan range is defined in thrift file like this
// struct TPaloScanRange {
//  1: required list<Types.TNetworkAddress> hosts
//  2: required string schema_hash
//  3: required string version
//  5: required Types.TTabletId tablet_id
//  6: required string db_name
//  7: optional list<TKeyRange> partition_column_ranges
//  8: optional string index_name
//  9: optional string table_name
//}
// every doris_scan_range is related with one tablet so that one olap scan node contains multiple tablet
Status VOlapScanNode::set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) {
    for (auto& scan_range : scan_ranges) {
        DCHECK(scan_range.scan_range.__isset.palo_scan_range);
        _scan_ranges.emplace_back(new TPaloScanRange(scan_range.scan_range.palo_scan_range));
        COUNTER_UPDATE(_tablet_counter, 1);
    }

    return Status::OK();
}

Status VOlapScanNode::get_hints(TabletSharedPtr table, const TPaloScanRange& scan_range,
                                int block_row_count, bool is_begin_include, bool is_end_include,
                                const std::vector<std::unique_ptr<OlapScanRange>>& scan_key_range,
                                std::vector<std::unique_ptr<OlapScanRange>>* sub_scan_range,
                                RuntimeProfile* profile) {
    RuntimeProfile::Counter* show_hints_timer = profile->get_counter("ShowHintsTime_V1");
    std::vector<std::vector<OlapTuple>> ranges;
    bool have_valid_range = false;
    for (auto& key_range : scan_key_range) {
        if (key_range->begin_scan_range.size() == 1 &&
            key_range->begin_scan_range.get_value(0) == NEGATIVE_INFINITY) {
            continue;
        }
        SCOPED_TIMER(show_hints_timer);

        Status res = Status::OK();
        std::vector<OlapTuple> range;
        res = table->split_range(key_range->begin_scan_range, key_range->end_scan_range,
                                 block_row_count, &range);
        if (!res.ok()) {
            return Status::InternalError("fail to show hints");
        }
        ranges.emplace_back(std::move(range));
        have_valid_range = true;
    }

    if (!have_valid_range) {
        std::vector<OlapTuple> range;
        auto res = table->split_range({}, {}, block_row_count, &range);
        if (!res.ok()) {
            return Status::InternalError("fail to show hints");
        }
        ranges.emplace_back(std::move(range));
    }

    for (int i = 0; i < ranges.size(); ++i) {
        for (int j = 0; j < ranges[i].size(); j += 2) {
            std::unique_ptr<OlapScanRange> range(new OlapScanRange);
            range->begin_scan_range.reset();
            range->begin_scan_range = ranges[i][j];
            range->end_scan_range.reset();
            range->end_scan_range = ranges[i][j + 1];

            if (0 == j) {
                range->begin_include = is_begin_include;
            } else {
                range->begin_include = true;
            }

            if (j + 2 == ranges[i].size()) {
                range->end_include = is_end_include;
            } else {
                range->end_include = false;
            }

            sub_scan_range->emplace_back(std::move(range));
        }
    }

    return Status::OK();
}

template <bool IsNotIn>
bool VOlapScanNode::_should_push_down_in_predicate(VInPredicate* pred, VExprContext* expr_ctx) {
    if (pred->is_not_in() != IsNotIn) {
        return false;
    }
    InState* state = reinterpret_cast<InState*>(
            expr_ctx->fn_context(pred->fn_context_index())
                    ->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    HybridSetBase* set = state->hybrid_set.get();

    // if there are too many elements in InPredicate, exceed the limit,
    // we will not push any condition of this column to storage engine.
    // because too many conditions pushed down to storage engine may even
    // slow down the query process.
    // ATTN: This is just an experience value. You may need to try
    // different thresholds to improve performance.
    if (set->size() > _max_pushdown_conditions_per_column) {
        VLOG_NOTICE << "Predicate value num " << set->size() << " exceed limit "
                    << _max_pushdown_conditions_per_column;
        return false;
    }
    return true;
}

bool VOlapScanNode::_should_push_down_function_filter(VectorizedFnCall* fn_call,
                                                      VExprContext* expr_ctx,
                                                      StringVal* constant_str,
                                                      doris_udf::FunctionContext** fn_ctx) {
    // Now only `like` function filters is supported to push down
    if (fn_call->fn().name.function_name != "like") {
        return false;
    }

    const auto& children = fn_call->children();
    doris_udf::FunctionContext* func_cxt = expr_ctx->fn_context(fn_call->fn_context_index());
    DCHECK(func_cxt != nullptr);
    DCHECK(children.size() == 2);
    for (size_t i = 0; i < children.size(); i++) {
        if (VExpr::expr_without_cast(children[i])->node_type() != TExprNodeType::SLOT_REF) {
            // not a slot ref(column)
            continue;
        }
        if (!children[1 - i]->is_constant()) {
            // only handle constant value
            return false;
        } else {
            DCHECK(children[1 - i]->type().is_string_type());
            if (const ColumnConst* const_column = check_and_get_column<ColumnConst>(
                        children[1 - i]->get_const_col(expr_ctx)->column_ptr)) {
                *constant_str = const_column->get_data_at(0).to_string_val();
            } else {
                return false;
            }
        }
    }
    *fn_ctx = func_cxt;
    return true;
}

bool VOlapScanNode::_should_push_down_binary_predicate(
        VectorizedFnCall* fn_call, VExprContext* expr_ctx, StringRef* constant_val,
        int* slot_ref_child, const std::function<bool(const std::string&)>& fn_checker) {
    if (!fn_checker(fn_call->fn().name.function_name)) {
        return false;
    }

    const auto& children = fn_call->children();
    DCHECK(children.size() == 2);
    for (size_t i = 0; i < children.size(); i++) {
        if (VExpr::expr_without_cast(children[i])->node_type() != TExprNodeType::SLOT_REF) {
            // not a slot ref(column)
            continue;
        }
        if (!children[1 - i]->is_constant()) {
            // only handle constant value
            return false;
        } else {
            if (const ColumnConst* const_column = check_and_get_column<ColumnConst>(
                        children[1 - i]->get_const_col(expr_ctx)->column_ptr)) {
                *slot_ref_child = i;
                *constant_val = const_column->get_data_at(0);
            } else {
                return false;
            }
        }
    }
    return true;
}

bool VOlapScanNode::_is_predicate_acting_on_slot(
        VExpr* expr,
        const std::function<bool(const std::vector<VExpr*>&, const VSlotRef**, VExpr**)>& checker,
        SlotDescriptor** slot_desc, ColumnValueRangeType** range) {
    const VSlotRef* slot_ref = nullptr;
    VExpr* child_contains_slot = nullptr;
    if (!checker(expr->children(), &slot_ref, &child_contains_slot)) {
        // not a slot ref(column)
        return false;
    }

    auto entry = _id_to_slot_column_value_range.find(slot_ref->slot_id());
    if (_id_to_slot_column_value_range.end() == entry) {
        return false;
    }
    *slot_desc = entry->second.first;
    DCHECK(child_contains_slot != nullptr);
    if (child_contains_slot->type().type != (*slot_desc)->type().type) {
        if (!ignore_cast(*slot_desc, child_contains_slot)) {
            // the type of predicate not match the slot's type
            return false;
        }
    }
    *range = &(entry->second.second);
    return true;
}

template <PrimitiveType T>
Status VOlapScanNode::_normalize_in_and_eq_predicate(VExpr* expr, VExprContext* expr_ctx,
                                                     SlotDescriptor* slot,
                                                     ColumnValueRange<T>& range, bool* push_down) {
    auto temp_range = ColumnValueRange<T>::create_empty_column_value_range(slot->type().precision,
                                                                           slot->type().scale);
    bool effect = false;
    // 1. Normalize in conjuncts like 'where col in (v1, v2, v3)'
    if (TExprNodeType::IN_PRED == expr->node_type()) {
        VInPredicate* pred = static_cast<VInPredicate*>(expr);
        if (!_should_push_down_in_predicate<false>(pred, expr_ctx)) {
            return Status::OK();
        }

        // begin to push InPredicate value into ColumnValueRange
        InState* state = reinterpret_cast<InState*>(
                expr_ctx->fn_context(pred->fn_context_index())
                        ->get_function_state(FunctionContext::FRAGMENT_LOCAL));
        HybridSetBase::IteratorBase* iter = state->hybrid_set->begin();
        auto fn_name = std::string("");
        while (iter->has_next()) {
            // column in (nullptr) is always false so continue to
            // dispose next item
            if (nullptr == iter->get_value()) {
                iter->next();
                continue;
            }
            auto value = const_cast<void*>(iter->get_value());
            RETURN_IF_ERROR(change_value_range<true>(
                    temp_range, value, ColumnValueRange<T>::add_fixed_value_range, fn_name));
            iter->next();
        }

        range.intersection(temp_range);
        effect = true;
    } else if (TExprNodeType::BINARY_PRED == expr->node_type()) {
        DCHECK(expr->children().size() == 2);
        auto eq_checker = [](const std::string& fn_name) { return fn_name == "eq"; };

        StringRef value;
        int slot_ref_child = -1;
        if (_should_push_down_binary_predicate(reinterpret_cast<VectorizedFnCall*>(expr), expr_ctx,
                                               &value, &slot_ref_child, eq_checker)) {
            DCHECK(slot_ref_child >= 0);
            // where A = nullptr should return empty result set
            auto fn_name = std::string("");
            if (value.data != nullptr) {
                if constexpr (T == TYPE_CHAR || T == TYPE_VARCHAR || T == TYPE_STRING ||
                              T == TYPE_HLL) {
                    auto val = StringValue(value.data, value.size);
                    RETURN_IF_ERROR(change_value_range<true>(
                            temp_range, reinterpret_cast<void*>(&val),
                            ColumnValueRange<T>::add_fixed_value_range, fn_name));
                } else {
                    RETURN_IF_ERROR(change_value_range<true>(
                            temp_range, reinterpret_cast<void*>(const_cast<char*>(value.data)),
                            ColumnValueRange<T>::add_fixed_value_range, fn_name));
                }
                range.intersection(temp_range);
                effect = true;
            }
        }
    }

    // exceed limit, no conditions will be pushed down to storage engine.
    if (range.get_fixed_value_size() > _max_pushdown_conditions_per_column) {
        range.set_whole_value_range();
    } else {
        *push_down = effect;
    }
    return Status::OK();
}

template <PrimitiveType T>
Status VOlapScanNode::_normalize_not_in_and_not_eq_predicate(VExpr* expr, VExprContext* expr_ctx,
                                                             SlotDescriptor* slot,
                                                             ColumnValueRange<T>& range,
                                                             bool* push_down) {
    bool is_fixed_range = range.is_fixed_value_range();
    auto not_in_range = ColumnValueRange<T>::create_empty_column_value_range(range.column_name());
    bool effect = false;
    // 1. Normalize in conjuncts like 'where col in (v1, v2, v3)'
    if (TExprNodeType::IN_PRED == expr->node_type()) {
        VInPredicate* pred = static_cast<VInPredicate*>(expr);
        if (!_should_push_down_in_predicate<true>(pred, expr_ctx)) {
            return Status::OK();
        }

        // begin to push InPredicate value into ColumnValueRange
        InState* state = reinterpret_cast<InState*>(
                expr_ctx->fn_context(pred->fn_context_index())
                        ->get_function_state(FunctionContext::FRAGMENT_LOCAL));
        HybridSetBase::IteratorBase* iter = state->hybrid_set->begin();
        auto fn_name = std::string("");
        while (iter->has_next()) {
            // column not in (nullptr) is always true
            if (nullptr == iter->get_value()) {
                continue;
            }
            auto value = const_cast<void*>(iter->get_value());
            if (is_fixed_range) {
                RETURN_IF_ERROR(change_value_range<true>(
                        range, value, ColumnValueRange<T>::remove_fixed_value_range, fn_name));
            } else {
                RETURN_IF_ERROR(change_value_range<true>(
                        not_in_range, value, ColumnValueRange<T>::add_fixed_value_range, fn_name));
            }
            iter->next();
        }
        effect = true;
    } else if (TExprNodeType::BINARY_PRED == expr->node_type()) {
        DCHECK(expr->children().size() == 2);

        auto ne_checker = [](const std::string& fn_name) { return fn_name == "ne"; };
        StringRef value;
        int slot_ref_child = -1;
        if (_should_push_down_binary_predicate(reinterpret_cast<VectorizedFnCall*>(expr), expr_ctx,
                                               &value, &slot_ref_child, ne_checker)) {
            DCHECK(slot_ref_child >= 0);
            // where A = nullptr should return empty result set
            if (value.data != nullptr) {
                auto fn_name = std::string("");
                if constexpr (T == TYPE_CHAR || T == TYPE_VARCHAR || T == TYPE_STRING ||
                              T == TYPE_HLL) {
                    auto val = StringValue(value.data, value.size);
                    if (is_fixed_range) {
                        RETURN_IF_ERROR(change_value_range<true>(
                                range, reinterpret_cast<void*>(&val),
                                ColumnValueRange<T>::remove_fixed_value_range, fn_name));
                    } else {
                        RETURN_IF_ERROR(change_value_range<true>(
                                not_in_range, reinterpret_cast<void*>(&val),
                                ColumnValueRange<T>::add_fixed_value_range, fn_name));
                    }
                } else {
                    if (is_fixed_range) {
                        RETURN_IF_ERROR(change_value_range<true>(
                                range, reinterpret_cast<void*>(const_cast<char*>(value.data)),
                                ColumnValueRange<T>::remove_fixed_value_range, fn_name));
                    } else {
                        RETURN_IF_ERROR(change_value_range<true>(
                                not_in_range,
                                reinterpret_cast<void*>(const_cast<char*>(value.data)),
                                ColumnValueRange<T>::add_fixed_value_range, fn_name));
                    }
                }
                effect = true;
            }
        }
    }

    if (is_fixed_range ||
        not_in_range.get_fixed_value_size() <= _max_pushdown_conditions_per_column) {
        if (!is_fixed_range) {
            // push down not in condition to storage engine
            not_in_range.to_in_condition(_olap_filter, false);
        }
        *push_down = effect;
    }
    return Status::OK();
}

template <PrimitiveType T>
Status VOlapScanNode::_normalize_is_null_predicate(VExpr* expr, VExprContext* expr_ctx,
                                                   SlotDescriptor* slot, ColumnValueRange<T>& range,
                                                   bool* push_down) {
    if (TExprNodeType::FUNCTION_CALL == expr->node_type()) {
        if (reinterpret_cast<VectorizedFnCall*>(expr)->fn().name.function_name == "is_null_pred") {
            auto temp_range = ColumnValueRange<T>::create_empty_column_value_range(
                    slot->type().precision, slot->type().scale);
            temp_range.set_contain_null(true);
            range.intersection(temp_range);
            *push_down = true;
        } else if (reinterpret_cast<VectorizedFnCall*>(expr)->fn().name.function_name ==
                   "is_not_null_pred") {
            auto temp_range = ColumnValueRange<T>::create_empty_column_value_range(
                    slot->type().precision, slot->type().scale);
            temp_range.set_contain_null(false);
            range.intersection(temp_range);
            *push_down = true;
        }
    }
    return Status::OK();
}

template <PrimitiveType T>
Status VOlapScanNode::_normalize_noneq_binary_predicate(VExpr* expr, VExprContext* expr_ctx,
                                                        SlotDescriptor* slot,
                                                        ColumnValueRange<T>& range,
                                                        bool* push_down) {
    if (TExprNodeType::BINARY_PRED == expr->node_type()) {
        DCHECK(expr->children().size() == 2);

        auto noneq_checker = [](const std::string& fn_name) {
            return fn_name != "ne" && fn_name != "eq";
        };
        StringRef value;
        int slot_ref_child = -1;
        if (_should_push_down_binary_predicate(reinterpret_cast<VectorizedFnCall*>(expr), expr_ctx,
                                               &value, &slot_ref_child, noneq_checker)) {
            DCHECK(slot_ref_child >= 0);
            const std::string& fn_name =
                    reinterpret_cast<VectorizedFnCall*>(expr)->fn().name.function_name;

            // where A = nullptr should return empty result set
            if (value.data != nullptr) {
                *push_down = true;
                if constexpr (T == TYPE_CHAR || T == TYPE_VARCHAR || T == TYPE_STRING ||
                              T == TYPE_HLL) {
                    auto val = StringValue(value.data, value.size);
                    RETURN_IF_ERROR(change_value_range<false>(range, reinterpret_cast<void*>(&val),
                                                              ColumnValueRange<T>::add_value_range,
                                                              fn_name, slot_ref_child));
                } else {
                    RETURN_IF_ERROR(change_value_range<false>(
                            range, reinterpret_cast<void*>(const_cast<char*>(value.data)),
                            ColumnValueRange<T>::add_value_range, fn_name, slot_ref_child));
                }
            }
        }
    }
    return Status::OK();
}

Status VOlapScanNode::_normalize_bloom_filter(VExpr* expr, VExprContext* expr_ctx,
                                              SlotDescriptor* slot, bool* push_down) {
    if (TExprNodeType::BLOOM_PRED == expr->node_type()) {
        DCHECK(expr->children().size() == 1);
        _bloom_filters_push_down.emplace_back(slot->col_name(), expr->get_bloom_filter_func());
        *push_down = true;
    }
    return Status::OK();
}

Status VOlapScanNode::_normalize_function_filters(VExpr* expr, VExprContext* expr_ctx,
                                                  SlotDescriptor* slot, bool* push_down) {
    bool opposite = false;
    VExpr* fn_expr = expr;
    if (TExprNodeType::COMPOUND_PRED == expr->node_type() &&
        expr->fn().name.function_name == "not") {
        fn_expr = fn_expr->children()[0];
        opposite = true;
    }

    if (TExprNodeType::FUNCTION_CALL == fn_expr->node_type()) {
        doris_udf::FunctionContext* fn_ctx = nullptr;
        StringVal val;
        if (_should_push_down_function_filter(reinterpret_cast<VectorizedFnCall*>(fn_expr),
                                              expr_ctx, &val, &fn_ctx)) {
            std::string col = slot->col_name();
            _push_down_functions.emplace_back(opposite, col, fn_ctx, val);
            *push_down = true;
        }
    }
    return Status::OK();
}

void VOlapScanNode::eval_const_conjuncts(VExpr* vexpr, VExprContext* expr_ctx, bool* push_down) {
    char* constant_val = nullptr;
    if (vexpr->is_constant()) {
        if (const ColumnConst* const_column =
                    check_and_get_column<ColumnConst>(vexpr->get_const_col(expr_ctx)->column_ptr)) {
            constant_val = const_cast<char*>(const_column->get_data_at(0).data);
            if (constant_val == nullptr || *reinterpret_cast<bool*>(constant_val) == false) {
                *push_down = true;
                _eos = true;
            }
        } else if (const ColumnVector<UInt8>* bool_column =
                           check_and_get_column<ColumnVector<UInt8>>(
                                   vexpr->get_const_col(expr_ctx)->column_ptr)) {
            // TODO: If `vexpr->is_constant()` is true, a const column is expected here.
            //  But now we still don't cover all predicates for const expression.
            //  For example, for query `SELECT col FROM tbl WHERE 'PROMOTION' LIKE 'AAA%'`,
            //  predicate `like` will return a ColumnVector<UInt8> which contains a single value.
            LOG(WARNING) << "Expr[" << vexpr->debug_string()
                         << "] should return a const column but actually is "
                         << vexpr->get_const_col(expr_ctx)->column_ptr->get_name();
            DCHECK_EQ(bool_column->size(), 1);
            if (bool_column->size() == 1) {
                constant_val = const_cast<char*>(bool_column->get_data_at(0).data);
                if (constant_val == nullptr || *reinterpret_cast<bool*>(constant_val) == false) {
                    *push_down = true;
                    _eos = true;
                }
            } else {
                LOG(WARNING) << "Constant predicate in scan node should return a bool column with "
                                "`size == 1` but actually is "
                             << bool_column->size();
            }
        } else {
            LOG(WARNING) << "Expr[" << vexpr->debug_string()
                         << "] should return a boolean column but actually is "
                         << vexpr->get_const_col(expr_ctx)->column_ptr->get_name();
        }
    }
}

VExpr* VOlapScanNode::_normalize_predicate(RuntimeState* state, VExpr* conjunct_expr_root) {
    static constexpr auto is_leaf = [](VExpr* expr) { return !expr->is_and_expr(); };
    auto in_predicate_checker = [](const std::vector<VExpr*>& children, const VSlotRef** slot,
                                   VExpr** child_contains_slot) {
        if (children.empty() ||
            VExpr::expr_without_cast(children[0])->node_type() != TExprNodeType::SLOT_REF) {
            // not a slot ref(column)
            return false;
        }
        *slot = reinterpret_cast<const VSlotRef*>(VExpr::expr_without_cast(children[0]));
        *child_contains_slot = children[0];
        return true;
    };
    auto eq_predicate_checker = [](const std::vector<VExpr*>& children, const VSlotRef** slot,
                                   VExpr** child_contains_slot) {
        for (const VExpr* child : children) {
            if (VExpr::expr_without_cast(child)->node_type() != TExprNodeType::SLOT_REF) {
                // not a slot ref(column)
                continue;
            }
            *slot = reinterpret_cast<const VSlotRef*>(VExpr::expr_without_cast(child));
            *child_contains_slot = const_cast<VExpr*>(child);
            return true;
        }
        return false;
    };

    if (conjunct_expr_root != nullptr) {
        if (is_leaf(conjunct_expr_root)) {
            auto impl = conjunct_expr_root->get_impl();
            VExpr* cur_expr = impl ? const_cast<VExpr*>(impl) : conjunct_expr_root;
            SlotDescriptor* slot = nullptr;
            ColumnValueRangeType* range = nullptr;
            bool push_down = false;
            eval_const_conjuncts(cur_expr, *(_vconjunct_ctx_ptr.get()), &push_down);
            if (push_down) {
                return nullptr;
            }
            if (_is_predicate_acting_on_slot(cur_expr, in_predicate_checker, &slot, &range) ||
                _is_predicate_acting_on_slot(cur_expr, eq_predicate_checker, &slot, &range)) {
                std::visit(
                        [&](auto& value_range) {
                            RETURN_IF_PUSH_DOWN(_normalize_in_and_eq_predicate(
                                    cur_expr, *(_vconjunct_ctx_ptr.get()), slot, value_range,
                                    &push_down));
                            RETURN_IF_PUSH_DOWN(_normalize_not_in_and_not_eq_predicate(
                                    cur_expr, *(_vconjunct_ctx_ptr.get()), slot, value_range,
                                    &push_down));
                            RETURN_IF_PUSH_DOWN(_normalize_is_null_predicate(
                                    cur_expr, *(_vconjunct_ctx_ptr.get()), slot, value_range,
                                    &push_down));
                            RETURN_IF_PUSH_DOWN(_normalize_noneq_binary_predicate(
                                    cur_expr, *(_vconjunct_ctx_ptr.get()), slot, value_range,
                                    &push_down));
                            if (is_key_column(slot->col_name())) {
                                RETURN_IF_PUSH_DOWN(_normalize_bloom_filter(
                                        cur_expr, *(_vconjunct_ctx_ptr.get()), slot, &push_down));
                                if (state->enable_function_pushdown()) {
                                    RETURN_IF_PUSH_DOWN(_normalize_function_filters(
                                            cur_expr, *(_vconjunct_ctx_ptr.get()), slot,
                                            &push_down));
                                }
                            }
                        },
                        *range);
            }
            if (push_down && is_key_column(slot->col_name())) {
                return nullptr;
            } else {
                return conjunct_expr_root;
            }
        } else {
            VExpr* left_child = _normalize_predicate(state, conjunct_expr_root->children()[0]);
            VExpr* right_child = _normalize_predicate(state, conjunct_expr_root->children()[1]);

            if (left_child != nullptr && right_child != nullptr) {
                conjunct_expr_root->set_children({left_child, right_child});
                return conjunct_expr_root;
            } else {
                // here only close the and expr self, do not close the child
                conjunct_expr_root->set_children({});
                conjunct_expr_root->close(state, *_vconjunct_ctx_ptr,
                                          (*_vconjunct_ctx_ptr)->get_function_state_scope());
            }

            // here do not close Expr* now
            return left_child != nullptr ? left_child : right_child;
        }
    }
    return conjunct_expr_root;
}

Status VOlapScanNode::_append_rf_into_conjuncts(RuntimeState* state, std::vector<VExpr*>& vexprs) {
    if (!vexprs.empty()) {
        VExpr* last_expr = nullptr;
        if (_vconjunct_ctx_ptr) {
            last_expr = (*_vconjunct_ctx_ptr)->root();
        } else {
            DCHECK(_rf_vexpr_set.find(vexprs[0]) == _rf_vexpr_set.end());
            last_expr = vexprs[0];
            _rf_vexpr_set.insert(vexprs[0]);
        }
        for (size_t j = _vconjunct_ctx_ptr ? 0 : 1; j < vexprs.size(); j++) {
            if (_rf_vexpr_set.find(vexprs[j]) != _rf_vexpr_set.end()) {
                continue;
            }
            TFunction fn;
            TFunctionName fn_name;
            fn_name.__set_db_name("");
            fn_name.__set_function_name("and");
            fn.__set_name(fn_name);
            fn.__set_binary_type(TFunctionBinaryType::BUILTIN);
            std::vector<TTypeDesc> arg_types;
            arg_types.push_back(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
            arg_types.push_back(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
            fn.__set_arg_types(arg_types);
            fn.__set_ret_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
            fn.__set_has_var_args(false);

            TExprNode texpr_node;
            texpr_node.__set_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
            texpr_node.__set_node_type(TExprNodeType::COMPOUND_PRED);
            texpr_node.__set_opcode(TExprOpcode::COMPOUND_AND);
            texpr_node.__set_fn(fn);
            texpr_node.__set_is_nullable(last_expr->is_nullable() || vexprs[j]->is_nullable());
            VExpr* new_node = _pool->add(new VcompoundPred(texpr_node));
            new_node->add_child(last_expr);
            DCHECK((vexprs[j])->get_impl() != nullptr);
            new_node->add_child(vexprs[j]);
            last_expr = new_node;
            _rf_vexpr_set.insert(vexprs[j]);
        }
        auto new_vconjunct_ctx_ptr = _pool->add(new VExprContext(last_expr));
        if (_vconjunct_ctx_ptr) {
            (*_vconjunct_ctx_ptr)->clone_fn_contexts(new_vconjunct_ctx_ptr);
        }
        RETURN_IF_ERROR(new_vconjunct_ctx_ptr->prepare(state, _row_descriptor));
        RETURN_IF_ERROR(new_vconjunct_ctx_ptr->open(state));
        if (_vconjunct_ctx_ptr) {
            (*(_vconjunct_ctx_ptr.get()))->mark_as_stale();
            _stale_vexpr_ctxs.push_back(std::move(_vconjunct_ctx_ptr));
        }
        _vconjunct_ctx_ptr.reset(new doris::vectorized::VExprContext*);
        *(_vconjunct_ctx_ptr.get()) = new_vconjunct_ctx_ptr;
    }
    return Status::OK();
}

std::string VOlapScanNode::get_name() {
    return fmt::format("VOlapScanNode({0})", _olap_scan_node.table_name);
}

} // namespace doris::vectorized
