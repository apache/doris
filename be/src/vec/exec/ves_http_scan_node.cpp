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

#include "vec/exec/ves_http_scan_node.h"

#include "gen_cpp/PlanNodes_types.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"
#include "runtime/tuple.h"
#include "runtime/tuple_row.h"
#include "util/runtime_profile.h"
#include "util/types.h"
#include "vec/exprs/vexpr_context.h"

namespace doris::vectorized {

VEsHttpScanNode::VEsHttpScanNode(ObjectPool* pool, const TPlanNode& tnode,
                                 const DescriptorTbl& descs)
        : EsHttpScanNode(pool, tnode, descs) {
    _vectorized = true;
}

VEsHttpScanNode::~VEsHttpScanNode() {}

Status VEsHttpScanNode::get_next(RuntimeState* state, vectorized::Block* block, bool* eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    if (state->is_cancelled()) {
        std::unique_lock<std::mutex> l(_block_queue_lock);
        if (update_status(Status::Cancelled("Cancelled"))) {
            _queue_writer_cond.notify_all();
        }
    }

    if (_scan_finished.load() || _eos) {
        *eos = true;
        return Status::OK();
    }

    std::shared_ptr<vectorized::Block> scanner_block;
    {
        std::unique_lock<std::mutex> l(_block_queue_lock);
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
    if (scanner_block == nullptr) {
        _scan_finished.store(true);
        *eos = true;
        return Status::OK();
    }

    // notify one scanner
    _queue_writer_cond.notify_one();

    reached_limit(scanner_block.get(), eos);
    *block = *scanner_block;

    // This is first time reach limit.
    // Only valid when query 'select * from table1 limit 20'
    if (*eos) {
        _scan_finished.store(true);
        _queue_writer_cond.notify_all();
        LOG(INFO) << "VEsHttpScanNode ReachedLimit.";
        *eos = true;
    } else {
        *eos = false;
    }

    return Status::OK();
}

Status VEsHttpScanNode::scanner_scan(std::unique_ptr<VEsHttpScanner> scanner) {
    RETURN_IF_ERROR(scanner->open());
    bool scanner_eof = false;

    const int batch_size = _runtime_state->batch_size();
    std::unique_ptr<MemPool> tuple_pool(new MemPool(mem_tracker().get()));
    size_t slot_num = _tuple_desc->slots().size();

    while (!scanner_eof) {
        std::shared_ptr<vectorized::Block> block(new vectorized::Block());
        std::vector<vectorized::MutableColumnPtr> columns(slot_num);
        for (int i = 0; i < slot_num; i++) {
            columns[i] = _tuple_desc->slots()[i]->get_empty_mutable_column();
        }
        while (columns[0]->size() < batch_size && !scanner_eof) {
            RETURN_IF_CANCELLED(_runtime_state);

            // If we have finished all works
            if (_scan_finished.load()) {
                return Status::OK();
            }

            // Get from scanner
            RETURN_IF_ERROR(
                    scanner->get_next(columns, tuple_pool.get(), &scanner_eof, _docvalue_context));
        }

        if (columns[0]->size() > 0) {
            auto n_columns = 0;
            for (const auto slot_desc : _tuple_desc->slots()) {
                block->insert(ColumnWithTypeAndName(std::move(columns[n_columns++]),
                                                    slot_desc->get_data_type_ptr(),
                                                    slot_desc->col_name()));
            }

            RETURN_IF_ERROR(VExprContext::filter_block(_vconjunct_ctx_ptr, block.get(),
                                                       _tuple_desc->slots().size()));

            std::unique_lock<std::mutex> l(_block_queue_lock);
            while (_process_status.ok() && !_scan_finished.load() &&
                   !_runtime_state->is_cancelled() &&
                   _block_queue.size() >= _max_buffered_batches) {
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
            _block_queue.push_back(block);

            // Notify reader to
            _queue_reader_cond.notify_one();
        }
    }

    return Status::OK();
}

Status VEsHttpScanNode::close(RuntimeState* state) {
    EsHttpScanNode::close(state);
    _block_queue.clear();
    return _process_status;
}

} // namespace doris::vectorized