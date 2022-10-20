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

#include "common/status.h"
#include "exprs/expr_context.h"
#include "olap/tablet.h"
#include "runtime/runtime_state.h"
#include "vec/exprs/vexpr_context.h"

namespace doris::vectorized {

class Block;
class VScanNode;

// Counter for load
struct ScannerCounter {
    ScannerCounter() : num_rows_filtered(0), num_rows_unselected(0) {}

    int64_t num_rows_filtered;   // unqualified rows (unmatched the dest schema, or no partition)
    int64_t num_rows_unselected; // rows filtered by predicates
};

class VScanner {
public:
    VScanner(RuntimeState* state, VScanNode* parent, int64_t limit);

    virtual ~VScanner() {}

    virtual Status open(RuntimeState* state) { return Status::OK(); }

    Status get_block(RuntimeState* state, Block* block, bool* eos);

    virtual Status close(RuntimeState* state);

    // Subclass must implement this to return the current rows read
    virtual int64_t raw_rows_read() { return 0; }

protected:
    // Subclass should implement this to return data.
    virtual Status _get_block_impl(RuntimeState* state, Block* block, bool* eof) = 0;

    // Update the counters before closing this scanner
    virtual void _update_counters_before_close();

    // Filter the output block finally.
    Status _filter_output_block(Block* block);

public:
    VScanNode* get_parent() { return _parent; }

    Status try_append_late_arrival_runtime_filter();

    // Call start_wait_worker_timer() when submit the scanner to the thread pool.
    // And call update_wait_worker_timer() when it is actually being executed.
    void start_wait_worker_timer() {
        _watch.reset();
        _watch.start();
    }

    void update_wait_worker_timer() { _scanner_wait_worker_timer += _watch.elapsed_time(); }

    RuntimeState* runtime_state() { return _state; }

    bool is_open() { return _is_open; }
    void set_opened() { _is_open = true; }

    int queue_id() { return _state->exec_env()->store_path_to_index("xxx"); }

    doris::TabletStorageType get_storage_type() {
        return doris::TabletStorageType::STORAGE_TYPE_LOCAL;
    }

    bool need_to_close() { return _need_to_close; }

    void mark_to_need_to_close() {
        _update_counters_before_close();
        _need_to_close = true;
    }

    void set_status_on_failure(const Status& st) { _status = st; }

    VExprContext** vconjunct_ctx_ptr() { return &_vconjunct_ctx; }

    void reg_conjunct_ctxs(const std::vector<ExprContext*>& conjunct_ctxs) {
        _conjunct_ctxs = conjunct_ctxs;
    }

    // return false if _is_counted_down is already true,
    // otherwise, set _is_counted_down to true and return true.
    bool set_counted_down() {
        if (_is_counted_down) {
            return false;
        }
        _is_counted_down = true;
        return true;
    }

protected:
    void _discard_conjuncts() {
        if (_vconjunct_ctx) {
            _vconjunct_ctx->mark_as_stale();
            _stale_vexpr_ctxs.push_back(_vconjunct_ctx);
            _vconjunct_ctx = nullptr;
        }
    }

protected:
    RuntimeState* _state;
    VScanNode* _parent;
    // Set if scan node has sort limit info
    int64_t _limit = -1;

    const TupleDescriptor* _input_tuple_desc = nullptr;
    const TupleDescriptor* _output_tuple_desc = nullptr;
    const TupleDescriptor* _real_tuple_desc = nullptr;

    // If _input_tuple_desc is set, the scanner will read data into
    // this _input_block first, then convert to the output block.
    Block _input_block;
    // If _input_tuple_desc is set, this will point to _input_block,
    // otherwise, it will point to the output block.
    Block* _input_block_ptr;

    bool _is_open = false;
    bool _is_closed = false;
    bool _need_to_close = false;
    Status _status;

    // If _applied_rf_num == _total_rf_num
    // means all runtime filters are arrived and applied.
    int _applied_rf_num = 0;
    int _total_rf_num = 0;
    // Cloned from _vconjunct_ctx of scan node.
    // It includes predicate in SQL and runtime filters.
    VExprContext* _vconjunct_ctx = nullptr;
    // Late arriving runtime filters will update _vconjunct_ctx.
    // The old _vconjunct_ctx will be temporarily placed in _stale_vexpr_ctxs
    // and will be destroyed at the end.
    std::vector<VExprContext*> _stale_vexpr_ctxs;

    // num of rows read from scanner
    int64_t _num_rows_read = 0;

    // Set true after counter is updated finally
    bool _has_updated_counter = false;

    // watch to count the time wait for scanner thread
    MonotonicStopWatch _watch;
    int64_t _scanner_wait_worker_timer = 0;

    // File formats based push down predicate
    std::vector<ExprContext*> _conjunct_ctxs;

    bool _is_load = false;
    // set to true after decrease the "_num_unfinished_scanners" in scanner context
    bool _is_counted_down = false;

    ScannerCounter _counter;
};

} // namespace doris::vectorized
