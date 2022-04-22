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

#include "vec/exec/volap_scanner.h"

#include <memory>

#include "runtime/runtime_state.h"
#include "vec/columns/column_complex.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/core/block.h"
#include "vec/exec/volap_scan_node.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris::vectorized {

VOlapScanner::VOlapScanner(RuntimeState* runtime_state, VOlapScanNode* parent, bool aggregation,
                           bool need_agg_finalize, const TPaloScanRange& scan_range,
                           std::shared_ptr<MemTracker> tracker)
        : OlapScanner(runtime_state, parent, aggregation, need_agg_finalize, scan_range, tracker) {}

Status VOlapScanner::get_block(RuntimeState* state, vectorized::Block* block, bool* eof) {
    // only empty block should be here
    DCHECK(block->rows() == 0);
    SCOPED_SWITCH_TASK_THREAD_LOCAL_EXISTED_MEM_TRACKER(_mem_tracker);

    int64_t raw_rows_threshold = raw_rows_read() + config::doris_scanner_row_num;
    int64_t raw_bytes_threshold = config::doris_scanner_row_bytes;
    if (!block->mem_reuse()) {
        for (const auto slot_desc : _tuple_desc->slots()) {
            block->insert(ColumnWithTypeAndName(slot_desc->get_empty_mutable_column(),
                                                slot_desc->get_data_type_ptr(),
                                                slot_desc->col_name()));
        }
    }

    {
        SCOPED_TIMER(_parent->_scan_timer);
        do {
            // Read one block from block reader
            auto res = _tablet_reader->next_block_with_aggregation(block, nullptr, nullptr, eof);
            if (!res) {
                std::stringstream ss;
                ss << "Internal Error: read storage fail. res=" << res
                   << ", tablet=" << _tablet->full_name()
                   << ", backend=" << BackendOptions::get_localhost();
                return res;
            }
            _num_rows_read += block->rows();
            _update_realtime_counter();

            RETURN_IF_ERROR(
                    VExprContext::filter_block(_vconjunct_ctx, block, _tuple_desc->slots().size()));
        } while (block->rows() == 0 && !(*eof) && raw_rows_read() < raw_rows_threshold &&
                 block->allocated_bytes() < raw_bytes_threshold);
    }
    // NOTE:
    // There is no need to check raw_bytes_threshold since block->rows() == 0 is checked first.
    // But checking raw_bytes_threshold is still added here for consistency with raw_rows_threshold
    // and olap_scanner.cpp.

    return Status::OK();
}

void VOlapScanner::set_tablet_reader() {
    _tablet_reader = std::make_unique<BlockReader>();
}
} // namespace doris::vectorized
