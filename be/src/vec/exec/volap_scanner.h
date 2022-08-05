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

#include "exec/exec_node.h"
#include "exec/olap_utils.h"
#include "gen_cpp/PaloInternalService_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "olap/tablet.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/olap/block_reader.h"

namespace doris {
class RuntimeProfile;
class Field;
class RowBatch;

namespace vectorized {
class VOlapScanNode;

class VOlapScanner {
public:
    VOlapScanner(RuntimeState* runtime_state, VOlapScanNode* parent, bool aggregation,
                 bool need_agg_finalize, const TPaloScanRange& scan_range, MemTracker* tracker);
    virtual ~VOlapScanner() = default;

    Status prepare(const TPaloScanRange& scan_range, const std::vector<OlapScanRange*>& key_ranges,
                   const std::vector<TCondition>& filters,
                   const std::vector<std::pair<std::string, std::shared_ptr<IBloomFilterFuncBase>>>&
                           bloom_filters,
                   const std::vector<FunctionFilter>& function_filters);

    Status open();

    Status get_block(RuntimeState* state, vectorized::Block* block, bool* eof);

    Status close(RuntimeState* state);

    Status get_batch(RuntimeState* state, RowBatch* row_batch, bool* eos) {
        return Status::NotSupported("Not Implemented VOlapScanNode Node::get_next scalar");
    }

    RuntimeState* runtime_state() { return _runtime_state; }

    std::vector<ExprContext*>* conjunct_ctxs() { return &_conjunct_ctxs; }

    VExprContext** vconjunct_ctx_ptr() { return &_vconjunct_ctx; }

    void mark_to_need_to_close() { _need_to_close = true; }

    bool need_to_close() { return _need_to_close; }

    int id() const { return _id; }
    void set_id(int id) { _id = id; }
    bool is_open() const { return _is_open; }
    void set_opened() { _is_open = true; }

    int64_t raw_rows_read() const { return _raw_rows_read; }

    void update_counter();

    const std::string& scan_disk() const { return _tablet->data_dir()->path(); }

    void start_wait_worker_timer() {
        _watcher.reset();
        _watcher.start();
    }

    int64_t update_wait_worker_timer() const { return _watcher.elapsed_time(); }

    void set_use_pushdown_conjuncts(bool has_pushdown_conjuncts) {
        _use_pushdown_conjuncts = has_pushdown_conjuncts;
    }

    std::vector<bool>* mutable_runtime_filter_marks() { return &_runtime_filter_marks; }

    TabletStorageType get_storage_type();

private:
    Status _init_tablet_reader_params(
            const std::vector<OlapScanRange*>& key_ranges, const std::vector<TCondition>& filters,
            const std::vector<std::pair<string, std::shared_ptr<IBloomFilterFuncBase>>>&
                    bloom_filters,
            const std::vector<FunctionFilter>& function_filters);
    Status _init_return_columns(bool need_seq_col);

    // Update profile that need to be reported in realtime.
    void _update_realtime_counter();

    virtual void set_tablet_reader() { _tablet_reader = std::make_unique<BlockReader>(); }

    RuntimeState* _runtime_state;
    VOlapScanNode* _parent;
    const TupleDescriptor* _tuple_desc; /**< tuple descriptor */

    std::vector<ExprContext*> _conjunct_ctxs;
    // to record which runtime filters have been used
    std::vector<bool> _runtime_filter_marks;

    int _id;
    bool _is_open;
    bool _aggregation;
    bool _need_agg_finalize = true;
    bool _has_update_counter = false;
    bool _use_pushdown_conjuncts = false;

    TabletReader::ReaderParams _tablet_reader_params;
    std::unique_ptr<TabletReader> _tablet_reader;

    TabletSharedPtr _tablet;
    int64_t _version;

    std::vector<uint32_t> _return_columns;
    std::unordered_set<uint32_t> _tablet_columns_convert_to_null_set;

    // time costed and row returned statistics
    int64_t _num_rows_read = 0;
    int64_t _raw_rows_read = 0;
    int64_t _compressed_bytes_read = 0;

    // number rows filtered by pushed condition
    int64_t _num_rows_pushed_cond_filtered = 0;

    bool _is_closed = false;

    MonotonicStopWatch _watcher;

    MemTracker* _mem_tracker;

    VExprContext* _vconjunct_ctx = nullptr;
    bool _need_to_close = false;

    TabletSchemaSPtr _tablet_schema;
};

} // namespace vectorized
} // namespace doris
