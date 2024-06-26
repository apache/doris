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

#include "exec/olap_scan_node.h"

#include <algorithm>
#include <iostream>
#include <string>
#include <utility>

#include "agent/cgroups_mgr.h"
#include "common/logging.h"
#include "common/resource_tls.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "exprs/runtime_filter.h"
#include "gen_cpp/PlanNodes_types.h"
#include "olap/storage_engine.h"
#include "runtime/exec_env.h"
#include "runtime/large_int_value.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_filter_mgr.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"
#include "runtime/tuple_row.h"
#include "util/priority_thread_pool.hpp"
#include "util/runtime_profile.h"
#include "util/thread.h"
#include "util/to_string.h"

namespace doris {

#define DS_SUCCESS(x) ((x) >= 0)

OlapScanNode::OlapScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
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
          _runtime_filter_descs(tnode.runtime_filters) {}

Status OlapScanNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    _direct_conjunct_size = _conjunct_ctxs.size();

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
    for (int i = 0; i < filter_size; ++i) {
        IRuntimeFilter* runtime_filter = nullptr;
        const auto& filter_desc = _runtime_filter_descs[i];
        RETURN_IF_ERROR(state->runtime_filter_mgr()->register_filter(
                RuntimeFilterRole::CONSUMER, filter_desc, state->query_options(), id()));
        RETURN_IF_ERROR(state->runtime_filter_mgr()->get_consume_filter(filter_desc.filter_id,
                                                                        &runtime_filter));

        _runtime_filter_ctxs[i].runtimefilter = runtime_filter;
    }
    _batch_size = _limit == -1 ? state->batch_size()
                               : std::min(static_cast<int64_t>(state->batch_size()), _limit);
    return Status::OK();
}

void OlapScanNode::init_scan_profile() {
    std::string scanner_profile_name = "OlapScanner";
    if (_olap_scan_node.__isset.table_name) {
        scanner_profile_name = fmt::format("OlapScanner({0})", _olap_scan_node.table_name);
    }
    _scanner_profile.reset(new RuntimeProfile(scanner_profile_name));
    runtime_profile()->add_child(_scanner_profile.get(), true, nullptr);

    _segment_profile.reset(new RuntimeProfile("SegmentIterator"));
    _scanner_profile->add_child(_segment_profile.get(), true, nullptr);
}

void OlapScanNode::_init_counter(RuntimeState* state) {
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
    _block_seek_timer = ADD_TIMER(_segment_profile, "BlockSeekTime");
    _block_seek_counter = ADD_COUNTER(_segment_profile, "BlockSeekCount", TUnit::UNIT);

    _rows_vec_cond_counter = ADD_COUNTER(_segment_profile, "RowsVectorPredFiltered", TUnit::UNIT);
    _vec_cond_timer = ADD_TIMER(_segment_profile, "VectorPredEvalTime");
    _short_cond_timer = ADD_TIMER(_segment_profile, "ShortPredEvalTime");
    _first_read_timer = ADD_TIMER(_segment_profile, "FirstReadTime");
    _lazy_read_timer = ADD_TIMER(_segment_profile, "LazyReadTime");
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

Status OlapScanNode::prepare(RuntimeState* state) {
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

    _scanner_mem_tracker = std::make_shared<MemTracker>("Scanners");

    if (_tuple_desc == nullptr) {
        // TODO: make sure we print all available diagnostic output to our error log
        return Status::InternalError("Failed to get tuple descriptor.");
    }

    _runtime_profile->add_info_string("Table", _tuple_desc->table_desc()->name());

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
        IRuntimeFilter* runtime_filter = nullptr;
        state->runtime_filter_mgr()->get_consume_filter(_runtime_filter_descs[i].filter_id,
                                                        &runtime_filter);
        DCHECK(runtime_filter != nullptr);
        runtime_filter->init_profile(_runtime_profile.get());
    }
    return Status::OK();
}

Status OlapScanNode::open(RuntimeState* state) {
    VLOG_CRITICAL << "OlapScanNode::Open";
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(ExecNode::open(state));
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker());

    _resource_info = ResourceTls::get_resource_tls();

    // acquire runtime filter
    _runtime_filter_ctxs.resize(_runtime_filter_descs.size());

    for (size_t i = 0; i < _runtime_filter_descs.size(); ++i) {
        auto& filter_desc = _runtime_filter_descs[i];
        IRuntimeFilter* runtime_filter = nullptr;
        state->runtime_filter_mgr()->get_consume_filter(filter_desc.filter_id, &runtime_filter);
        DCHECK(runtime_filter != nullptr);
        if (auto bf = runtime_filter->get_bloomfilter()) {
            RETURN_IF_ERROR(bf->init_with_fixed_length());
        }
        if (runtime_filter == nullptr) {
            continue;
        }
        bool ready = runtime_filter->is_ready();
        if (!ready) {
            ready = runtime_filter->await();
        }
        if (ready) {
            std::list<ExprContext*> expr_context;
            RETURN_IF_ERROR(runtime_filter->get_push_expr_ctxs(&expr_context));
            _runtime_filter_ctxs[i].apply_mark = true;
            _runtime_filter_ctxs[i].runtimefilter = runtime_filter;

            for (auto ctx : expr_context) {
                ctx->prepare(state, row_desc());
                ctx->open(state);
                int index = _conjunct_ctxs.size();
                _conjunct_ctxs.push_back(ctx);
                // it's safe to store address from a fix-resized vector
                _conjunctid_to_runtime_filter_ctxs[index] = &_runtime_filter_ctxs[i];
            }
        }
    }

    return Status::OK();
}

Status OlapScanNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker());
    // check if Canceled.
    if (state->is_cancelled()) {
        std::unique_lock<std::mutex> l(_row_batches_lock);
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
            LOG(ERROR) << "StartScan Failed cause " << status;
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

    // wait for batch from queue
    RowBatch* materialized_batch = nullptr;
    {
        std::unique_lock<std::mutex> l(_row_batches_lock);
        SCOPED_TIMER(_olap_wait_batch_queue_timer);
        while (_materialized_row_batches.empty() && !_transfer_done) {
            if (state->is_cancelled()) {
                _transfer_done = true;
            }

            // use wait_for, not wait, in case to capture the state->is_cancelled()
            _row_batch_added_cv.wait_for(l, std::chrono::seconds(1));
        }

        if (!_materialized_row_batches.empty()) {
            materialized_batch = _materialized_row_batches.front();
            DCHECK(materialized_batch != nullptr);
            _materialized_row_batches.pop_front();
            _materialized_row_batches_bytes -=
                    materialized_batch->tuple_data_pool()->total_reserved_bytes();
        }
    }

    // return batch
    if (nullptr != materialized_batch) {
        // notify scanner
        _row_batch_consumed_cv.notify_one();
        // get scanner's batch memory
        row_batch->acquire_state(materialized_batch);
        _num_rows_returned += row_batch->num_rows();
        COUNTER_SET(_rows_returned_counter, _num_rows_returned);

        // reach scan node limit
        if (reached_limit()) {
            int num_rows_over = _num_rows_returned - _limit;
            row_batch->set_num_rows(row_batch->num_rows() - num_rows_over);
            _num_rows_returned -= num_rows_over;
            COUNTER_SET(_rows_returned_counter, _num_rows_returned);

            {
                std::unique_lock<std::mutex> l(_row_batches_lock);
                _transfer_done = true;
            }

            _row_batch_consumed_cv.notify_all();
            *eos = true;
            VLOG_QUERY << "OlapScanNode ReachedLimit. fragment id="
                       << print_id(_runtime_state->fragment_instance_id());
        } else {
            *eos = false;
        }

        if (VLOG_ROW_IS_ON) {
            for (int i = 0; i < row_batch->num_rows(); ++i) {
                TupleRow* row = row_batch->get_row(i);
                VLOG_ROW << "OlapScanNode output row: "
                         << Tuple::to_string(row->get_tuple(0), *_tuple_desc);
            }
        }

        delete materialized_batch;
        return Status::OK();
    }

    // all scanner done, change *eos to true
    *eos = true;
    std::lock_guard<SpinLock> guard(_status_mutex);
    return _status;
}

Status OlapScanNode::collect_query_statistics(QueryStatistics* statistics) {
    RETURN_IF_ERROR(ExecNode::collect_query_statistics(statistics));
    statistics->add_scan_bytes(_read_compressed_counter->value());
    statistics->add_scan_rows(_raw_rows_counter->value());
    statistics->add_cpu_ms(_scan_cpu_timer->value() / NANOS_PER_MILLIS);
    return Status::OK();
}

Status OlapScanNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    // change done status
    {
        std::unique_lock<std::mutex> l(_row_batches_lock);
        _transfer_done = true;
    }
    // notify all scanner thread
    _row_batch_consumed_cv.notify_all();
    _row_batch_added_cv.notify_all();
    _scan_batch_added_cv.notify_all();

    // _transfer_thread
    // _transfer_thread may not be initialized. So need to check it
    if (_transfer_thread != nullptr) {
        _transfer_thread->join();
    }

    // clear some row batch in queue
    for (auto row_batch : _materialized_row_batches) {
        delete row_batch;
    }

    _materialized_row_batches.clear();
    _materialized_row_batches_bytes = 0;

    for (auto row_batch : _scan_row_batches) {
        delete row_batch;
    }

    _scan_row_batches.clear();
    _scan_row_batches_bytes = 0;

    // OlapScanNode terminate by exception
    // so that initiative close the Scanner
    for (auto scanner : _olap_scanners) {
        scanner->close(state);
    }

    for (auto& filter_desc : _runtime_filter_descs) {
        IRuntimeFilter* runtime_filter = nullptr;
        state->runtime_filter_mgr()->get_consume_filter(filter_desc.filter_id, &runtime_filter);
        DCHECK(runtime_filter != nullptr);
        runtime_filter->consumer_close();
    }

    VLOG_CRITICAL << "OlapScanNode::close()";
    // pushed functions close
    Expr::close(_pushed_func_conjunct_ctxs, state);

    return ScanNode::close(state);
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
Status OlapScanNode::set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) {
    for (auto& scan_range : scan_ranges) {
        DCHECK(scan_range.scan_range.__isset.palo_scan_range);
        _scan_ranges.emplace_back(new TPaloScanRange(scan_range.scan_range.palo_scan_range));
        COUNTER_UPDATE(_tablet_counter, 1);
    }

    return Status::OK();
}

Status OlapScanNode::start_scan(RuntimeState* state) {
    RETURN_IF_CANCELLED(state);

    VLOG_CRITICAL << "Eval Const Conjuncts";
    // 1. Eval const conjuncts to find whether eos = true
    eval_const_conjuncts();

    VLOG_CRITICAL << "NormalizeConjuncts";
    // 2. Convert conjuncts to ColumnValueRange in each column, some conjuncts may
    // set eos = true
    RETURN_IF_ERROR(normalize_conjuncts());

    // 1 and 2 step dispose find conjuncts set eos = true, return directly
    if (_eos) {
        return Status::OK();
    }

    VLOG_CRITICAL << "BuildKeyRangesAndFilters";
    // 3.1 Using ColumnValueRange to Build StorageEngine filters
    RETURN_IF_ERROR(build_key_ranges_and_filters());
    // 3.2 Function pushdown
    if (state->enable_function_pushdown()) RETURN_IF_ERROR(build_function_filters());

    VLOG_CRITICAL << "Filter idle conjuncts";
    // 4. Filter idle conjunct which already trans to olap filters
    // this must be after build_scan_key, it will free the StringValue memory
    remove_pushed_conjuncts(state);

    VLOG_CRITICAL << "StartScanThread";
    // 5. Start multi thread to read several `Sub Sub ScanRange`
    RETURN_IF_ERROR(start_scan_thread(state));

    return Status::OK();
}

bool OlapScanNode::is_key_column(const std::string& key_name) {
    // all column in dup_keys table olap scan node threat
    // as key column
    if (_olap_scan_node.keyType == TKeysType::DUP_KEYS) {
        return true;
    }

    auto res = std::find(_olap_scan_node.key_column_name.begin(),
                         _olap_scan_node.key_column_name.end(), key_name);
    return res != _olap_scan_node.key_column_name.end();
}

void OlapScanNode::remove_pushed_conjuncts(RuntimeState* state) {
    if (_pushed_conjuncts_index.empty() && _pushed_func_conjuncts_index.empty()) {
        return;
    }

    // dispose direct conjunct first
    std::vector<ExprContext*> new_conjunct_ctxs;
    for (int i = 0; i < _direct_conjunct_size; ++i) {
        if (!_pushed_conjuncts_index.empty() && _pushed_conjuncts_index.count(i)) {
            _conjunct_ctxs[i]->close(state); // pushed condition, just close
        } else if (!_pushed_func_conjuncts_index.empty() && _pushed_func_conjuncts_index.count(i)) {
            _pushed_func_conjunct_ctxs.emplace_back(
                    _conjunct_ctxs[i]); // pushed functions, need keep ctxs
        } else {
            new_conjunct_ctxs.emplace_back(_conjunct_ctxs[i]);
        }
    }

    auto new_direct_conjunct_size = new_conjunct_ctxs.size();

    // dispose hash join push down conjunct second
    for (int i = _direct_conjunct_size; i < _conjunct_ctxs.size(); ++i) {
        if (!_pushed_conjuncts_index.empty() && _pushed_conjuncts_index.count(i)) {
            _conjunct_ctxs[i]->close(state); // pushed condition, just close
        } else if (!_pushed_func_conjuncts_index.empty() && _pushed_func_conjuncts_index.count(i)) {
            _pushed_func_conjunct_ctxs.emplace_back(
                    _conjunct_ctxs[i]); // pushed functions, need keep ctxs
        } else {
            new_conjunct_ctxs.emplace_back(_conjunct_ctxs[i]);
        }
    }

    _conjunct_ctxs = std::move(new_conjunct_ctxs);
    _direct_conjunct_size = new_direct_conjunct_size;

    // TODO: support vbloom_filter_predicate/vbinary_predicate and merge unpushed predicate to _vconjunct_ctx
    for (auto push_down_ctx : _pushed_conjuncts_index) {
        auto iter = _conjunctid_to_runtime_filter_ctxs.find(push_down_ctx);
        if (iter != _conjunctid_to_runtime_filter_ctxs.end()) {
            iter->second->runtimefilter->set_push_down_profile();
        }
    }

    // set vconjunct_ctx is empty, if all conjunct
    if (_direct_conjunct_size == 0) {
        if (_vconjunct_ctx_ptr != nullptr) {
            (*_vconjunct_ctx_ptr)->close(state);
            _vconjunct_ctx_ptr = nullptr;
        }
    }

    // filter idle conjunct in vexpr_contexts
    auto checker = [&](int index) { return _pushed_conjuncts_index.count(index); };
    _peel_pushed_vconjunct(state, checker);
}

void OlapScanNode::eval_const_conjuncts() {
    for (int conj_idx = 0; conj_idx < _conjunct_ctxs.size(); ++conj_idx) {
        // if conjunct is constant, compute direct and set eos = true
        if (_conjunct_ctxs[conj_idx]->root()->is_constant()) {
            void* value = _conjunct_ctxs[conj_idx]->get_value(nullptr);
            if (value == nullptr || *reinterpret_cast<bool*>(value) == false) {
                _eos = true;
                break;
            }
        }
    }
}

Status OlapScanNode::normalize_conjuncts() {
    std::vector<SlotDescriptor*> slots = _tuple_desc->slots();

    for (int slot_idx = 0; slot_idx < slots.size(); ++slot_idx) {
        switch (slots[slot_idx]->type().type) {
        case TYPE_TINYINT: {
            ColumnValueRange<TYPE_TINYINT> range(slots[slot_idx]->col_name());
            normalize_predicate(range, slots[slot_idx]);
            break;
        }

        case TYPE_SMALLINT: {
            ColumnValueRange<TYPE_SMALLINT> range(slots[slot_idx]->col_name());
            normalize_predicate(range, slots[slot_idx]);
            break;
        }

        case TYPE_INT: {
            ColumnValueRange<TYPE_INT> range(slots[slot_idx]->col_name());
            normalize_predicate(range, slots[slot_idx]);
            break;
        }

        case TYPE_BIGINT: {
            ColumnValueRange<TYPE_BIGINT> range(slots[slot_idx]->col_name());
            normalize_predicate(range, slots[slot_idx]);
            break;
        }

        case TYPE_LARGEINT: {
            ColumnValueRange<TYPE_LARGEINT> range(slots[slot_idx]->col_name());
            normalize_predicate(range, slots[slot_idx]);
            break;
        }

        case TYPE_CHAR: {
            ColumnValueRange<TYPE_CHAR> range(slots[slot_idx]->col_name());
            normalize_predicate(range, slots[slot_idx]);
            break;
        }
        case TYPE_VARCHAR: {
            ColumnValueRange<TYPE_VARCHAR> range(slots[slot_idx]->col_name());
            normalize_predicate(range, slots[slot_idx]);
            break;
        }
        case TYPE_HLL: {
            ColumnValueRange<TYPE_HLL> range(slots[slot_idx]->col_name());
            normalize_predicate(range, slots[slot_idx]);
            break;
        }
        case TYPE_STRING: {
            ColumnValueRange<TYPE_STRING> range(slots[slot_idx]->col_name());
            normalize_predicate(range, slots[slot_idx]);
            break;
        }

        case TYPE_DATE: {
            ColumnValueRange<TYPE_DATE> range(slots[slot_idx]->col_name());
            normalize_predicate(range, slots[slot_idx]);
            break;
        }
        case TYPE_DATETIME: {
            ColumnValueRange<TYPE_DATETIME> range(slots[slot_idx]->col_name());
            normalize_predicate(range, slots[slot_idx]);
            break;
        }

        case TYPE_DATEV2: {
            ColumnValueRange<TYPE_DATEV2> range(slots[slot_idx]->col_name());
            normalize_predicate(range, slots[slot_idx]);
            break;
        }

        case TYPE_DECIMALV2: {
            ColumnValueRange<TYPE_DECIMALV2> range(slots[slot_idx]->col_name());
            normalize_predicate(range, slots[slot_idx]);
            break;
        }

        case TYPE_BOOLEAN: {
            ColumnValueRange<TYPE_BOOLEAN> range(slots[slot_idx]->col_name());
            normalize_predicate(range, slots[slot_idx]);
            break;
        }

        default: {
            VLOG_CRITICAL << "Unsupported Normalize Slot [ColName=" << slots[slot_idx]->col_name()
                          << "]";
            break;
        }
        }
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
    // std::vector<std::string> filters_string;
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

Status OlapScanNode::build_function_filters() {
    for (int conj_idx = 0; conj_idx < _conjunct_ctxs.size(); ++conj_idx) {
        ExprContext* ex_ctx = _conjunct_ctxs[conj_idx];
        Expr* fn_expr = ex_ctx->root();
        bool opposite = false;

        if (TExprNodeType::COMPOUND_PRED == fn_expr->node_type() &&
            TExprOpcode::COMPOUND_NOT == fn_expr->op()) {
            fn_expr = fn_expr->get_child(0);
            opposite = true;
        }

        // currently only support like / not like
        if (TExprNodeType::FUNCTION_CALL == fn_expr->node_type() &&
            "like" == fn_expr->fn().name.function_name) {
            doris_udf::FunctionContext* func_cxt =
                    ex_ctx->fn_context(fn_expr->get_fn_context_index());

            if (!func_cxt) {
                continue;
            }
            if (fn_expr->children().size() != 2) {
                continue;
            }
            SlotRef* slot_ref = nullptr;
            Expr* literal_expr = nullptr;

            if (TExprNodeType::SLOT_REF == fn_expr->get_child(0)->node_type()) {
                literal_expr = fn_expr->get_child(1);
                slot_ref = (SlotRef*)(fn_expr->get_child(0));
            } else if (TExprNodeType::SLOT_REF == fn_expr->get_child(1)->node_type()) {
                literal_expr = fn_expr->get_child(0);
                slot_ref = (SlotRef*)(fn_expr->get_child(1));
            } else {
                continue;
            }

            if (TExprNodeType::STRING_LITERAL != literal_expr->node_type()) continue;

            const SlotDescriptor* slot_desc = nullptr;
            std::vector<SlotId> slot_ids;
            slot_ref->get_slot_ids(&slot_ids);
            for (SlotDescriptor* slot : _tuple_desc->slots()) {
                if (slot->id() == slot_ids[0]) {
                    slot_desc = slot;
                    break;
                }
            }

            if (!slot_desc) {
                continue;
            }
            std::string col = slot_desc->col_name();
            StringVal val = literal_expr->get_string_val(ex_ctx, nullptr);
            _push_down_functions.emplace_back(opposite, col, func_cxt, val);
            _pushed_func_conjuncts_index.insert(conj_idx);
        }
    }
    return Status::OK();
}

Status OlapScanNode::build_key_ranges_and_filters() {
    const std::vector<std::string>& column_names = _olap_scan_node.key_column_name;
    const std::vector<TPrimitiveType::type>& column_types = _olap_scan_node.key_column_type;
    DCHECK(column_types.size() == column_names.size());

    // 1. construct scan key except last olap engine short key
    _scan_keys.set_is_convertible(limit() == -1);

    // we use `exact_range` to identify a key range is an exact range or not when we convert
    // it to `_scan_keys`. If `exact_range` is true, we can just discard it from `_olap_filter`.
    bool exact_range = true;
    bool eos = false;
    for (int column_index = 0;
         column_index < column_names.size() && !_scan_keys.has_range_value() && !eos;
         ++column_index) {
        auto iter = _column_value_ranges.find(column_names[column_index]);
        if (_column_value_ranges.end() == iter) {
            break;
        }

        RETURN_IF_ERROR(std::visit(
                [&](auto&& range) {
                    // make a copy or range and pass to extend_scan_key, keep the range unchanged
                    // because extend_scan_key method may change the first parameter.
                    // but the original range may be converted to olap filters, if it's not an exact_range.
                    // related pr https://github.com/apache/doris/pull/13530
                    auto temp_range = range;
                    RETURN_IF_ERROR(_scan_keys.extend_scan_key(temp_range, _max_scan_key_num,
                                                               &exact_range, &eos));
                    if (exact_range) {
                        _column_value_ranges.erase(iter->first);
                    }
                    return Status::OK();
                },
                iter->second));
    }
    _eos |= eos;

    for (auto& iter : _column_value_ranges) {
        std::vector<TCondition> filters;
        std::visit([&](auto&& range) { range.to_olap_filter(filters); }, iter.second);

        for (auto& filter : filters) {
            _olap_filter.push_back(std::move(filter));
        }
    }

    _runtime_profile->add_info_string("PushdownPredicate", olap_filters_to_string(_olap_filter));

    _runtime_profile->add_info_string("KeyRanges", _scan_keys.debug_string());

    VLOG_CRITICAL << _scan_keys.debug_string();

    return Status::OK();
}

Status OlapScanNode::get_hints(TabletSharedPtr table, const TPaloScanRange& scan_range,
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

Status OlapScanNode::start_scan_thread(RuntimeState* state) {
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
        int32_t schema_hash = strtoul(scan_range->schema_hash.c_str(), nullptr, 10);
        std::string err;
        TabletSharedPtr tablet =
                StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, true, &err);
        if (tablet == nullptr) {
            std::stringstream ss;
            ss << "failed to get tablet: " << tablet_id << " with schema hash: " << schema_hash
               << ", reason: " << err;
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
        // In order to avoid the problem of too many scanners caused by small tablets,
        // in addition to scanRange, we also need to consider the size of the tablet when
        // creating the scanner. One scanner is used for every 1Gb, and the final scanner_per_tablet
        // takes the minimum value calculated by scanrange and size.
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
            OlapScanner* scanner =
                    new OlapScanner(state, this, _olap_scan_node.is_preaggregation,
                                    _need_agg_finalize, *scan_range, _scanner_mem_tracker);
            scanner->set_batch_size(_batch_size);
            // add scanner to pool before doing prepare.
            // so that scanner can be automatically deconstructed if prepare failed.
            _scanner_pool.add(scanner);
            RETURN_IF_ERROR(scanner->prepare(*scan_range, scanner_ranges, _olap_filter,
                                             _bloom_filters_push_down, _push_down_functions));

            _olap_scanners.push_back(scanner);
            disk_set.insert(scanner->scan_disk());
        }
    }
    COUNTER_SET(_num_disks_accessed_counter, static_cast<int64_t>(disk_set.size()));
    COUNTER_SET(_num_scanners, static_cast<int64_t>(_olap_scanners.size()));

    // PAIN_LOG(_olap_scanners.size());
    // init progress
    std::stringstream ss;
    ss << "ScanThread complete (node=" << id() << "):";
    _progress = ProgressUpdater(ss.str(), _olap_scanners.size(), 1);

    _transfer_thread = std::make_shared<std::thread>(&OlapScanNode::transfer_thread, this, state);

    return Status::OK();
}

template <PrimitiveType T>
Status OlapScanNode::normalize_predicate(ColumnValueRange<T>& range, SlotDescriptor* slot) {
    // 1. Normalize InPredicate, add to ColumnValueRange
    RETURN_IF_ERROR(normalize_in_and_eq_predicate(slot, &range));

    // 2. Normalize NotInPredicate, add to ColumnValueRange
    RETURN_IF_ERROR(normalize_not_in_and_not_eq_predicate(slot, &range));

    // 3. Normalize BinaryPredicate , add to ColumnValueRange
    RETURN_IF_ERROR(normalize_noneq_binary_predicate(slot, &range));

    // 3. Normalize BloomFilterPredicate, push down by hash join node
    RETURN_IF_ERROR(normalize_bloom_filter_predicate(slot));

    // 4. Check whether range is empty, set _eos
    if (range.is_empty_value_range()) _eos = true;

    // 5. Add range to Column->ColumnValueRange map
    _column_value_ranges[slot->col_name()] = range;

    return Status::OK();
}

static bool ignore_cast(SlotDescriptor* slot, Expr* expr) {
    if (slot->type().is_date_type() && expr->type().is_date_type()) {
        return true;
    }
    if (slot->type().is_string_type() && expr->type().is_string_type()) {
        return true;
    }
    return false;
}

bool OlapScanNode::should_push_down_in_predicate(doris::SlotDescriptor* slot,
                                                 doris::InPredicate* pred) {
    if (Expr::type_without_cast(pred->get_child(0)) != TExprNodeType::SLOT_REF) {
        // not a slot ref(column)
        return false;
    }

    std::vector<SlotId> slot_ids;
    if (pred->get_child(0)->get_slot_ids(&slot_ids) != 1) {
        // not a single column predicate
        return false;
    }

    if (slot_ids[0] != slot->id()) {
        // predicate not related to current column
        return false;
    }

    if (pred->get_child(0)->type().type != slot->type().type) {
        if (!ignore_cast(slot, pred->get_child(0))) {
            // the type of predicate not match the slot's type
            return false;
        }
    }

    VLOG_CRITICAL << slot->col_name() << " fixed_values add num: " << pred->hybrid_set()->size();

    // if there are too many elements in InPredicate, exceed the limit,
    // we will not push any condition of this column to storage engine.
    // because too many conditions pushed down to storage engine may even
    // slow down the query process.
    // ATTN: This is just an experience value. You may need to try
    // different thresholds to improve performance.
    if (pred->hybrid_set()->size() > _max_pushdown_conditions_per_column) {
        VLOG_NOTICE << "Predicate value num " << pred->hybrid_set()->size() << " exceed limit "
                    << _max_pushdown_conditions_per_column;
        return false;
    }

    return true;
}

std::pair<bool, void*> OlapScanNode::should_push_down_eq_predicate(doris::SlotDescriptor* slot,
                                                                   doris::Expr* pred, int conj_idx,
                                                                   int child_idx) {
    auto result_pair = std::make_pair<bool, void*>(false, nullptr);

    // Do not get slot_ref of column, should not push_down to Storage Engine
    if (Expr::type_without_cast(pred->get_child(child_idx)) != TExprNodeType::SLOT_REF) {
        return result_pair;
    }

    std::vector<SlotId> slot_ids;
    if (pred->get_child(child_idx)->get_slot_ids(&slot_ids) != 1) {
        // not a single column predicate
        return result_pair;
    }

    if (slot_ids[0] != slot->id()) {
        // predicate not related to current column
        return result_pair;
    }

    if (pred->get_child(child_idx)->type().type != slot->type().type) {
        if (!ignore_cast(slot, pred->get_child(child_idx))) {
            // the type of predicate not match the slot's type
            return result_pair;
        }
    }

    Expr* expr = pred->get_child(1 - child_idx);
    if (!expr->is_constant()) {
        // only handle constant value
        return result_pair;
    }

    // get value in result pair
    result_pair = std::make_pair(true, _conjunct_ctxs[conj_idx]->get_value(expr, nullptr));

    return result_pair;
}

template <PrimitiveType primitive_type, typename ChangeFixedValueRangeFunc>
Status OlapScanNode::change_fixed_value_range(ColumnValueRange<primitive_type>& temp_range,
                                              void* value, const ChangeFixedValueRangeFunc& func) {
    switch (primitive_type) {
    case TYPE_DATE: {
        DateTimeValue date_value = *reinterpret_cast<DateTimeValue*>(value);
        // There is must return empty data in olap_scan_node,
        // Because data value loss accuracy
        if (!date_value.check_loss_accuracy_cast_to_date()) {
            func(temp_range,
                 reinterpret_cast<typename PrimitiveTypeTraits<primitive_type>::CppType*>(
                         &date_value));
        }
        break;
    }
    case TYPE_DECIMALV2:
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_HLL:
    case TYPE_DATETIME:
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
    case TYPE_LARGEINT:
    case TYPE_STRING: {
        func(temp_range,
             reinterpret_cast<typename PrimitiveTypeTraits<primitive_type>::CppType*>(value));
        break;
    }
    case TYPE_BOOLEAN: {
        bool v = *reinterpret_cast<bool*>(value);
        func(temp_range,
             reinterpret_cast<typename PrimitiveTypeTraits<primitive_type>::CppType*>(&v));
        break;
    }
    default: {
        LOG(WARNING) << "Normalize filter fail, Unsupported Primitive type. [type="
                     << primitive_type << "]";
        return Status::InternalError("Normalize filter fail, Unsupported Primitive type");
    }
    }
    return Status::OK();
}

// Construct the ColumnValueRange for one specified column
// It will only handle the InPredicate and eq BinaryPredicate in _conjunct_ctxs.
// It will try to push down conditions of that column as much as possible,
// But if the number of conditions exceeds the limit, none of conditions will be pushed down.
template <PrimitiveType T>
Status OlapScanNode::normalize_in_and_eq_predicate(SlotDescriptor* slot,
                                                   ColumnValueRange<T>* range) {
    std::vector<uint32_t> filter_conjuncts_index;
    for (int conj_idx = 0; conj_idx < _conjunct_ctxs.size(); ++conj_idx) {
        // create empty range as temp range, temp range should do intersection on range
        auto temp_range = ColumnValueRange<T>::create_empty_column_value_range();

        // 1. Normalize in conjuncts like 'where col in (v1, v2, v3)'
        if (TExprOpcode::FILTER_IN == _conjunct_ctxs[conj_idx]->root()->op()) {
            InPredicate* pred = static_cast<InPredicate*>(_conjunct_ctxs[conj_idx]->root());
            if (!should_push_down_in_predicate(slot, pred)) {
                continue;
            }

            // begin to push InPredicate value into ColumnValueRange
            HybridSetBase::IteratorBase* iter = pred->hybrid_set()->begin();
            while (iter->has_next()) {
                // column in (nullptr) is always false so continue to
                // dispose next item
                if (nullptr == iter->get_value()) {
                    iter->next();
                    continue;
                }
                auto value = const_cast<void*>(iter->get_value());
                RETURN_IF_ERROR(change_fixed_value_range(
                        temp_range, value, ColumnValueRange<T>::add_fixed_value_range));
                iter->next();
            }

            if (is_key_column(slot->col_name())) {
                filter_conjuncts_index.emplace_back(conj_idx);
            }
            range->intersection(temp_range);
        } // end of handle in predicate
        // 2. Normalize eq conjuncts like 'where col = value'
        else if (TExprNodeType::BINARY_PRED == _conjunct_ctxs[conj_idx]->root()->node_type() &&
                 FILTER_IN == to_olap_filter_type(_conjunct_ctxs[conj_idx]->root()->op(), false)) {
            Expr* pred = _conjunct_ctxs[conj_idx]->root();
            DCHECK(pred->get_num_children() == 2);

            for (int child_idx = 0; child_idx < 2; ++child_idx) {
                // TODO: should use C++17 structured bindlings to refactor this code in the future:
                // 'auto [should_push_down, value] = should_push_down_eq_predicate(slot, pred, conj_idx, child_idx);'
                // make code tidier and readabler
                auto result_pair = should_push_down_eq_predicate(slot, pred, conj_idx, child_idx);
                if (!result_pair.first) {
                    continue;
                }

                auto value = result_pair.second;
                // where A = nullptr should return empty result set
                if (value != nullptr) {
                    RETURN_IF_ERROR(change_fixed_value_range(
                            temp_range, value, ColumnValueRange<T>::add_fixed_value_range));
                }

                if (is_key_column(slot->col_name())) {
                    filter_conjuncts_index.emplace_back(conj_idx);
                }
                range->intersection(temp_range);
            } // end for each binary predicate child
        }     // end of handling eq binary predicate
    }

    // exceed limit, no conditions will be pushed down to storage engine.
    if (range->get_fixed_value_size() > _max_pushdown_conditions_per_column) {
        range->set_whole_value_range();
    } else {
        std::copy(filter_conjuncts_index.cbegin(), filter_conjuncts_index.cend(),
                  std::inserter(_pushed_conjuncts_index, _pushed_conjuncts_index.begin()));
    }
    return Status::OK();
}

// Construct the ColumnValueRange for one specified column
// It will only handle the NotInPredicate and not eq BinaryPredicate in _conjunct_ctxs.
// It will try to push down conditions of that column as much as possible,
// But if the number of conditions exceeds the limit, none of conditions will be pushed down.
template <PrimitiveType T>
Status OlapScanNode::normalize_not_in_and_not_eq_predicate(SlotDescriptor* slot,
                                                           ColumnValueRange<T>* range) {
    // If the conjunct of slot is fixed value, will change the fixed value set of column value range
    // else add value to not in range and push down predicate directly
    bool is_fixed_range = range->is_fixed_value_range();
    auto not_in_range = ColumnValueRange<T>::create_empty_column_value_range(range->column_name());

    std::vector<uint32_t> filter_conjuncts_index;
    for (int conj_idx = 0; conj_idx < _conjunct_ctxs.size(); ++conj_idx) {
        // 1. Normalize in conjuncts like 'where col not in (v1, v2, v3)'
        if (TExprOpcode::FILTER_NOT_IN == _conjunct_ctxs[conj_idx]->root()->op()) {
            InPredicate* pred = static_cast<InPredicate*>(_conjunct_ctxs[conj_idx]->root());
            if (!should_push_down_in_predicate(slot, pred)) {
                continue;
            }

            // begin to push InPredicate value into ColumnValueRange
            auto iter = pred->hybrid_set()->begin();
            while (iter->has_next()) {
                // column not in (nullptr) is always true
                if (nullptr == iter->get_value()) {
                    continue;
                }
                auto value = const_cast<void*>(iter->get_value());
                if (is_fixed_range) {
                    RETURN_IF_ERROR(change_fixed_value_range(
                            *range, value, ColumnValueRange<T>::remove_fixed_value_range));
                } else {
                    RETURN_IF_ERROR(change_fixed_value_range(
                            not_in_range, value, ColumnValueRange<T>::add_fixed_value_range));
                }
                iter->next();
            }

            // only where a in ('a', 'b', nullptr) contain nullptr will
            // clear temp_range to whole range, no need do intersection
            if (is_key_column(slot->col_name())) {
                filter_conjuncts_index.emplace_back(conj_idx);
            }
        } // end of handle not in predicate

        // 2. Normalize eq conjuncts like 'where col != value'
        if (TExprNodeType::BINARY_PRED == _conjunct_ctxs[conj_idx]->root()->node_type() &&
            FILTER_NOT_IN == to_olap_filter_type(_conjunct_ctxs[conj_idx]->root()->op(), false)) {
            Expr* pred = _conjunct_ctxs[conj_idx]->root();
            DCHECK(pred->get_num_children() == 2);

            for (int child_idx = 0; child_idx < 2; ++child_idx) {
                // TODO: should use C++17 structured bindlings to refactor this code in the future:
                // 'auto [should_push_down, value] = should_push_down_eq_predicate(slot, pred, conj_idx, child_idx);'
                // make code tidier and readabler
                auto result_pair = should_push_down_eq_predicate(slot, pred, conj_idx, child_idx);
                if (!result_pair.first) {
                    continue;
                }
                auto value = result_pair.second;

                if (is_fixed_range) {
                    RETURN_IF_ERROR(change_fixed_value_range(
                            *range, value, ColumnValueRange<T>::remove_fixed_value_range));
                } else {
                    RETURN_IF_ERROR(change_fixed_value_range(
                            not_in_range, value, ColumnValueRange<T>::add_fixed_value_range));
                }

                if (is_key_column(slot->col_name())) {
                    filter_conjuncts_index.emplace_back(conj_idx);
                }
            } // end for each binary predicate child
        }     // end of handling eq binary predicate
    }

    // exceed limit, no conditions will be pushed down to storage engine.
    if (is_fixed_range ||
        not_in_range.get_fixed_value_size() <= _max_pushdown_conditions_per_column) {
        if (!is_fixed_range) {
            // push down not in condition to storage engine
            not_in_range.to_in_condition(_olap_filter, false);
        }
        std::copy(filter_conjuncts_index.cbegin(), filter_conjuncts_index.cend(),
                  std::inserter(_pushed_conjuncts_index, _pushed_conjuncts_index.begin()));
    }
    return Status::OK();
}

template <PrimitiveType T>
bool OlapScanNode::normalize_is_null_predicate(Expr* expr, SlotDescriptor* slot,
                                               const std::string& is_null_str,
                                               ColumnValueRange<T>* range) {
    if (expr->node_type() != TExprNodeType::SLOT_REF) {
        return false;
    }

    std::vector<SlotId> slot_ids;
    if (1 != expr->get_slot_ids(&slot_ids)) {
        return false;
    }

    if (slot_ids[0] != slot->id()) {
        return false;
    }

    auto temp_range = ColumnValueRange<T>::create_empty_column_value_range();
    temp_range.set_contain_null(is_null_str == "null");
    range->intersection(temp_range);

    return true;
}

template <PrimitiveType T>
Status OlapScanNode::normalize_noneq_binary_predicate(SlotDescriptor* slot,
                                                      ColumnValueRange<T>* range) {
    std::vector<uint32_t> filter_conjuncts_index;

    for (int conj_idx = 0; conj_idx < _conjunct_ctxs.size(); ++conj_idx) {
        Expr* root_expr = _conjunct_ctxs[conj_idx]->root();
        if (TExprNodeType::BINARY_PRED != root_expr->node_type() ||
            FILTER_IN == to_olap_filter_type(root_expr->op(), false) ||
            FILTER_NOT_IN == to_olap_filter_type(root_expr->op(), false)) {
            if (TExprNodeType::FUNCTION_CALL == root_expr->node_type()) {
                std::string is_null_str;
                // 1. dispose the where pred "A is null" and "A is not null"
                if (root_expr->is_null_scalar_function(is_null_str) &&
                    normalize_is_null_predicate(root_expr->get_child(0), slot, is_null_str,
                                                range)) {
                    // if column is key column should push down conjunct storage engine
                    if (is_key_column(slot->col_name())) {
                        filter_conjuncts_index.emplace_back(conj_idx);
                    }
                }
            }
            continue;
        }

        // 2. dispose the where pred "A <,<=" and "A >,>="
        Expr* pred = _conjunct_ctxs[conj_idx]->root();
        DCHECK(pred->get_num_children() == 2);

        for (int child_idx = 0; child_idx < 2; ++child_idx) {
            if (Expr::type_without_cast(pred->get_child(child_idx)) != TExprNodeType::SLOT_REF) {
                continue;
            }
            if (pred->get_child(child_idx)->type().type != slot->type().type) {
                if (!ignore_cast(slot, pred->get_child(child_idx))) {
                    continue;
                }
            }

            std::vector<SlotId> slot_ids;

            if (1 == pred->get_child(child_idx)->get_slot_ids(&slot_ids)) {
                if (slot_ids[0] != slot->id()) {
                    continue;
                }

                Expr* expr = pred->get_child(1 - child_idx);

                // for case: where col_a > col_b
                if (!expr->is_constant()) {
                    continue;
                }

                void* value = _conjunct_ctxs[conj_idx]->get_value(expr, nullptr);
                // for case: where col > null
                if (value == nullptr) {
                    continue;
                }

                switch (slot->type().type) {
                case TYPE_DATE: {
                    DateTimeValue date_value = *reinterpret_cast<DateTimeValue*>(value);
                    // NOTE: Datetime may be truncated to a date column, so we call ++operator for date_value
                    //  for example: '2010-01-01 00:00:01' will be truncate to '2010-01-01'
                    if (date_value.check_loss_accuracy_cast_to_date()) {
                        if (pred->op() == TExprOpcode::LT || pred->op() == TExprOpcode::GE) {
                            ++date_value;
                        }
                    }
                    range->add_range(to_olap_filter_type(pred->op(), child_idx),
                                     *reinterpret_cast<typename PrimitiveTypeTraits<T>::CppType*>(
                                             &date_value));
                    break;
                }
                case TYPE_TINYINT:
                case TYPE_DECIMALV2:
                case TYPE_CHAR:
                case TYPE_VARCHAR:
                case TYPE_HLL:
                case TYPE_DATETIME:
                case TYPE_SMALLINT:
                case TYPE_INT:
                case TYPE_BIGINT:
                case TYPE_LARGEINT:
                case TYPE_BOOLEAN:
                case TYPE_STRING: {
                    range->add_range(
                            to_olap_filter_type(pred->op(), child_idx),
                            *reinterpret_cast<typename PrimitiveTypeTraits<T>::CppType*>(value));
                    break;
                }

                default: {
                    LOG(WARNING) << "Normalize filter fail, Unsupported Primitive type. [type="
                                 << expr->type() << "]";
                    return Status::InternalError(
                            "Normalize filter fail, Unsupported Primitive type");
                }
                }

                if (is_key_column(slot->col_name())) {
                    filter_conjuncts_index.emplace_back(conj_idx);
                }

                VLOG_CRITICAL << slot->col_name() << " op: "
                              << static_cast<int>(to_olap_filter_type(pred->op(), child_idx))
                              << " value: "
                              << *reinterpret_cast<typename PrimitiveTypeTraits<T>::CppType*>(
                                         value);
            }
        }
    }

    std::copy(filter_conjuncts_index.cbegin(), filter_conjuncts_index.cend(),
              std::inserter(_pushed_conjuncts_index, _pushed_conjuncts_index.begin()));

    return Status::OK();
}

Status OlapScanNode::normalize_bloom_filter_predicate(SlotDescriptor* slot) {
    std::vector<uint32_t> filter_conjuncts_index;

    for (int conj_idx = _direct_conjunct_size; conj_idx < _conjunct_ctxs.size(); ++conj_idx) {
        Expr* root_expr = _conjunct_ctxs[conj_idx]->root();
        if (TExprNodeType::BLOOM_PRED != root_expr->node_type()) continue;

        Expr* pred = _conjunct_ctxs[conj_idx]->root();
        DCHECK(pred->get_num_children() == 1);

        if (Expr::type_without_cast(pred->get_child(0)) != TExprNodeType::SLOT_REF) {
            continue;
        }
        if (pred->get_child(0)->type().type != slot->type().type) {
            if (!ignore_cast(slot, pred->get_child(0))) {
                continue;
            }
        }

        std::vector<SlotId> slot_ids;

        if (1 == pred->get_child(0)->get_slot_ids(&slot_ids)) {
            if (slot_ids[0] != slot->id()) {
                continue;
            }
            // only key column of bloom filter will push down to storage engine
            if (is_key_column(slot->col_name())) {
                filter_conjuncts_index.emplace_back(conj_idx);
                _bloom_filters_push_down.emplace_back(
                        slot->col_name(),
                        (reinterpret_cast<BloomFilterPredicate*>(pred))->get_bloom_filter_func());
            }
        }
    }

    std::copy(filter_conjuncts_index.cbegin(), filter_conjuncts_index.cend(),
              std::inserter(_pushed_conjuncts_index, _pushed_conjuncts_index.begin()));

    return Status::OK();
}

void OlapScanNode::transfer_thread(RuntimeState* state) {
    // scanner open pushdown to scanThread
    SCOPED_ATTACH_TASK(state);
    Status status = Status::OK();
    for (auto scanner : _olap_scanners) {
        status = Expr::clone_if_not_exists(_conjunct_ctxs, state, scanner->conjunct_ctxs());
        if (!status.ok()) {
            std::lock_guard<SpinLock> guard(_status_mutex);
            _status = status;
            break;
        }
    }

    /*********************************
     * The basic strategy of priority scheduling:
     * 1. Determine the initial nice value by querying the number of split ranges
     *    The more the number of Ranges, the more likely it is to be recognized as a large query, and the smaller the nice value
     * 2. Adjust the nice value by querying the accumulated data volume
     *    The more data read, the more likely it is to be regarded as a large query, and the smaller the nice value
     * 3. Judge the priority of the query by the nice value
     *    The larger the nice value, the more preferentially obtained query resources
     * 4. Regularly increase the priority of the remaining tasks in the queue to avoid starvation for large queries
     *********************************/
    // after merge #15604, we no long support thread token to non-vec olap scan node,
    // so keep thread_token as null
    ThreadPoolToken* thread_token = nullptr;
    PriorityThreadPool* thread_pool = state->exec_env()->scan_thread_pool();
    PriorityThreadPool* remote_thread_pool = state->exec_env()->remote_scan_thread_pool();
    _total_assign_num = 0;
    _nice = 18 + std::max(0, 2 - (int)_olap_scanners.size() / 5);
    std::list<OlapScanner*> olap_scanners;

    int64_t mem_consume = _scanner_mem_tracker->consumption();
    int max_thread = _max_materialized_row_batches;
    if (config::doris_scanner_row_num > state->batch_size()) {
        max_thread /= config::doris_scanner_row_num / state->batch_size();
        if (max_thread <= 0) max_thread = 1;
    }
    // read from scanner
    while (LIKELY(status.ok())) {
        // When query cancel, _transfer_done is set to true at OlapScanNode::close,
        // and the loop is exited at this time, and the current thread exits after
        // waiting for _running_thread to decrease to 0.
        if (UNLIKELY(_transfer_done)) {
            LOG(INFO) << "Transfer thread cancelled, wait for the end of scan thread.";
            break;
        }
        int assigned_thread_num = 0;
        // copy to local
        {
            std::unique_lock<std::mutex> l(_scan_batches_lock);
            assigned_thread_num = _running_thread;
            // How many thread can apply to this query
            size_t thread_slot_num = 0;
            mem_consume = _scanner_mem_tracker->consumption();
            // check limit for total memory and _scan_row_batches memory
            if (mem_consume < (state->query_mem_tracker()->limit() * 6) / 10 &&
                _scan_row_batches_bytes < _max_scanner_queue_size_bytes / 2) {
                thread_slot_num = max_thread - assigned_thread_num;
            } else {
                // Memory already exceed
                if (_scan_row_batches.empty()) {
                    // NOTE(zc): here need to lock row_batches_lock_
                    //  be worried about dead lock, so don't check here
                    // if (materialized_row_batches_.empty()) {
                    //     LOG(FATAL) << "Scan_row_batches_ and materialized_row_batches_"
                    //         " are empty when memory exceed";
                    // }
                    // Just for notify if scan_row_batches_ is empty and no running thread
                    if (assigned_thread_num == 0) {
                        thread_slot_num = 1;
                        // NOTE: if olap_scanners_ is empty, scanner_done_ should be true
                    }
                }
            }
            thread_slot_num = std::min(thread_slot_num, _olap_scanners.size());
            for (int i = 0; i < thread_slot_num; ++i) {
                olap_scanners.push_back(_olap_scanners.front());
                _olap_scanners.pop_front();
                _running_thread++;
                assigned_thread_num++;
            }
        }

        auto iter = olap_scanners.begin();
        if (thread_token != nullptr) {
            while (iter != olap_scanners.end()) {
                auto s = thread_token->submit_func(
                        std::bind(&OlapScanNode::scanner_thread, this, *iter));
                if (s.ok()) {
                    (*iter)->start_wait_worker_timer();
                    COUNTER_UPDATE(_scanner_sched_counter, 1);
                    olap_scanners.erase(iter++);
                } else {
                    LOG(FATAL) << "Failed to assign scanner task to thread pool! " << s;
                }
                ++_total_assign_num;
            }
        } else {
            while (iter != olap_scanners.end()) {
                PriorityThreadPool::Task task;
                task.work_function = std::bind(&OlapScanNode::scanner_thread, this, *iter);
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

        RowBatch* scan_batch = nullptr;
        {
            // 1 scanner idle task not empty, assign new scanner task
            std::unique_lock<std::mutex> l(_scan_batches_lock);

            // scanner_row_num = 16k
            // 16k * 10 * 12 * 8 = 15M(>2s)  --> nice=10
            // 16k * 20 * 22 * 8 = 55M(>6s)  --> nice=0
            while (_nice > 0 && _total_assign_num > (22 - _nice) * (20 - _nice) * 6) {
                --_nice;
            }

            // 2 wait when all scanner are running & no result in queue
            while (UNLIKELY(_running_thread == assigned_thread_num && _scan_row_batches.empty() &&
                            !_scanner_done)) {
                SCOPED_TIMER(_scanner_wait_batch_timer);
                _scan_batch_added_cv.wait(l);
            }

            // 3 transfer result row batch when queue is not empty
            if (LIKELY(!_scan_row_batches.empty())) {
                scan_batch = _scan_row_batches.front();
                _scan_row_batches.pop_front();
                _scan_row_batches_bytes -= scan_batch->tuple_data_pool()->total_reserved_bytes();

                // delete scan_batch if transfer thread should be stopped
                // because scan_batch wouldn't be useful anymore
                if (UNLIKELY(_transfer_done)) {
                    delete scan_batch;
                    scan_batch = nullptr;
                }
            } else {
                if (_scanner_done) {
                    break;
                }
            }
        }

        if (nullptr != scan_batch) {
            add_one_batch(scan_batch);
        }
    } // end of transfer while

    VLOG_CRITICAL << "TransferThread finish.";
    {
        std::unique_lock<std::mutex> l(_row_batches_lock);
        _transfer_done = true;
        _row_batch_added_cv.notify_all();
    }

    std::unique_lock<std::mutex> l(_scan_batches_lock);
    _scan_thread_exit_cv.wait(l, [this] { return _running_thread == 0; });
    VLOG_CRITICAL << "Scanner threads have been exited. TransferThread exit.";
}

void OlapScanNode::scanner_thread(OlapScanner* scanner) {
    Thread::set_self_name("olap_scanner");
    if (UNLIKELY(_transfer_done)) {
        _scanner_done = true;
        std::unique_lock<std::mutex> l(_scan_batches_lock);
        _running_thread--;
        // We need to make sure the scanner is closed because the query has been closed or cancelled.
        scanner->close(scanner->runtime_state());
        _scan_batch_added_cv.notify_one();
        _scan_thread_exit_cv.notify_one();
        LOG(INFO) << "Scan thread cancelled, cause query done, scan thread started to exit";
        return;
    }
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

    std::vector<ExprContext*> contexts;
    auto& scanner_filter_apply_marks = *scanner->mutable_runtime_filter_marks();
    DCHECK(scanner_filter_apply_marks.size() == _runtime_filter_descs.size());
    for (size_t i = 0; i < scanner_filter_apply_marks.size(); i++) {
        if (!scanner_filter_apply_marks[i] && !_runtime_filter_ctxs[i].apply_mark) {
            IRuntimeFilter* runtime_filter = nullptr;
            state->runtime_filter_mgr()->get_consume_filter(_runtime_filter_descs[i].filter_id,
                                                            &runtime_filter);
            DCHECK(runtime_filter != nullptr);
            bool ready = runtime_filter->is_ready();
            if (ready) {
                runtime_filter->get_prepared_context(&contexts, row_desc());
                scanner_filter_apply_marks[i] = true;
            }
        }
    }

    if (!contexts.empty()) {
        std::vector<ExprContext*> new_contexts;
        auto& scanner_conjunct_ctxs = *scanner->conjunct_ctxs();
        Expr::clone_if_not_exists(contexts, state, &new_contexts);
        scanner_conjunct_ctxs.insert(scanner_conjunct_ctxs.end(), new_contexts.begin(),
                                     new_contexts.end());
        scanner->set_use_pushdown_conjuncts(true);
    }

    // apply to cgroup
    if (_resource_info != nullptr) {
        CgroupsMgr::apply_cgroup(_resource_info->user, _resource_info->group);
    }

    std::vector<RowBatch*> row_batchs;

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
    while (!eos && raw_rows_read < raw_rows_threshold && raw_bytes_read < raw_bytes_threshold) {
        if (UNLIKELY(_transfer_done)) {
            eos = true;
            status = Status::Cancelled("Cancelled");
            VLOG_QUERY << "Scan thread cancelled, cause query done, maybe reach limit."
                       << ", fragment id=" << print_id(_runtime_state->fragment_instance_id());
            break;
        }
        RowBatch* row_batch = new RowBatch(this->row_desc(), _batch_size);
        row_batch->set_scanner_id(scanner->id());
        status = scanner->get_batch(_runtime_state, row_batch, &eos);
        if (!status.ok()) {
            LOG(WARNING) << "Scan thread read OlapScanner failed: " << status;
            eos = true;
            break;
        }
        // 4. if status not ok, change status_.
        if (UNLIKELY(row_batch->num_rows() == 0)) {
            // may be failed, push already, scan node delete this batch.
            delete row_batch;
            row_batch = nullptr;
        } else {
            row_batchs.push_back(row_batch);
            raw_bytes_read += row_batch->tuple_data_pool()->total_reserved_bytes();
        }
        raw_rows_read = scanner->raw_rows_read();
        if (limit() != -1 && raw_rows_read >= limit()) {
            eos = true;
            break;
        }
    }

    {
        std::unique_lock<std::mutex> l(_scan_batches_lock);
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
            for (auto rb : row_batchs) {
                delete rb;
            }
        } else {
            for (auto rb : row_batchs) {
                _scan_row_batches.push_back(rb);
                _scan_row_batches_bytes += rb->tuple_data_pool()->total_reserved_bytes();
            }
        }
        // If eos is true, we will process out of this lock block.
        if (!eos) {
            _olap_scanners.push_front(scanner);
        }
    }
    if (eos) {
        // close out of batches lock. we do this before _progress update
        // that can assure this object can keep live before we finish.
        scanner->close(_runtime_state);

        std::unique_lock<std::mutex> l(_scan_batches_lock);
        _progress.update(1);
        if (_progress.done()) {
            // this is the right out
            _scanner_done = true;
        }
    }

    _scan_cpu_timer->update(cpu_watch.elapsed_time());
    _scanner_wait_worker_timer->update(wait_time);

    // The transfer thead will wait for `_running_thread==0`, to make sure all scanner threads won't access class members.
    // Do not access class members after this code.
    std::unique_lock<std::mutex> l(_scan_batches_lock);
    _running_thread--;
    // Both cv of _scan_batch_added_cv and _scan_thread_exit_cv should be notify after
    // change the value of _running_thread, because transfer thread lock will check the value
    // of _running_thread after be notify. Otherwise there could be dead lock between scanner_thread
    // and transfer thread
    _scan_batch_added_cv.notify_one();
    _scan_thread_exit_cv.notify_one();
}

Status OlapScanNode::add_one_batch(RowBatch* row_batch) {
    {
        std::unique_lock<std::mutex> l(_row_batches_lock);

        // check queue limit for both both batch size and bytes
        while (UNLIKELY((_materialized_row_batches.size() >= _max_materialized_row_batches ||
                         _materialized_row_batches_bytes >= _max_scanner_queue_size_bytes / 2) &&
                        !_transfer_done)) {
            _row_batch_consumed_cv.wait(l);
        }

        VLOG_CRITICAL << "Push row_batch to materialized_row_batches";
        _materialized_row_batches.push_back(row_batch);
        _materialized_row_batches_bytes += row_batch->tuple_data_pool()->total_reserved_bytes();
    }
    // remove one batch, notify main thread
    _row_batch_added_cv.notify_one();
    return Status::OK();
}
} // namespace doris
