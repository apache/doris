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

#include "vec/exec/tablefunction/vnumbers_tbf_scannode.h"

#include <sstream>
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"
#include "runtime/tuple_row.h"
#include "util/runtime_profile.h"

namespace doris::vectorized {

VNumbersTBFScanNode::VNumbersTBFScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ScanNode(pool, tnode, descs),
          _is_init(false),
          _total_numbers(tnode.numbers_scan_node.totalNumbers),  
          _tuple_id(tnode.numbers_scan_node.tuple_id),
          _tuple_desc(nullptr) {}

VNumbersTBFScanNode::~VNumbersTBFScanNode() {}

Status VNumbersTBFScanNode::prepare(RuntimeState* state) {
    VLOG_CRITICAL << "VNumbersTBFScanNode::Prepare";

    if (_is_init) {
        return Status::OK();
    }

    if (nullptr == state) {
        return Status::InternalError("input pointer is nullptr.");
    }

    RETURN_IF_ERROR(ScanNode::prepare(state));
    // get tuple desc
    _tuple_desc = state->desc_tbl().get_tuple_descriptor(_tuple_id);

    if (nullptr == _tuple_desc) {
        return Status::InternalError("Failed to get tuple descriptor.");
    }

    if (_tuple_desc->slots().size() != 1) {
        LOG(FATAL) << "number of slots should be 1 but it is " << _tuple_desc->slots().size();
    }

    _is_init = true;

    return Status::OK();
}

Status VNumbersTBFScanNode::open(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::open(state));
    VLOG_CRITICAL << "VNumbersTBFScanNode::Open";

    if (nullptr == state) {
        return Status::InternalError("input pointer is nullptr.");
    }

    if (!_is_init) {
        return Status::InternalError("used before initialize.");
    }

    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::OPEN));
    RETURN_IF_CANCELLED(state);
    SCOPED_TIMER(_runtime_profile->total_time_counter());

    return Status::OK();
}

Status VNumbersTBFScanNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
    LOG(FATAL) << "VNumbersTBFScanNode only support vectorized execution";
    return Status::OK();
}


Status VNumbersTBFScanNode::get_next(RuntimeState* state, vectorized::Block* block, bool* eos) {
    VLOG_CRITICAL << "VNumbersTBFScanNode::GetNext";
    if (state == NULL || block == NULL || eos == NULL)
        return Status::InternalError("input is NULL pointer");
    if (!_is_init) return Status::InternalError("used before initialize.");
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::GETNEXT));
    RETURN_IF_CANCELLED(state);
    bool mem_reuse = block->mem_reuse();
    DCHECK(block->rows() == 0);
    std::vector<vectorized::MutableColumnPtr> columns(_slot_num);

    do {
        for (int i = 0; i < _slot_num; ++i) {
            if (mem_reuse) {
                columns[i] = std::move(*block->get_by_position(i).column).mutate();
            } else {
                columns[i] = _tuple_desc->slots()[i]->get_empty_mutable_column();
            }
        }
        while (true) {
            RETURN_IF_CANCELLED(state);
            int batch_size = state->batch_size();
            if (columns[0]->size() == batch_size) {
                break;
            }
            reinterpret_cast<vectorized::ColumnVector<vectorized::Int64>*>(columns[0].get())->insert_value(_cur_offset ++);
            if (_cur_offset > _total_numbers) {
                *eos = true;
                break;
            }
        }
        auto n_columns = 0;
        if (!mem_reuse) {
            for (const auto slot_desc : _tuple_desc->slots()) {
                block->insert(ColumnWithTypeAndName(std::move(columns[n_columns++]),
                                                    slot_desc->get_data_type_ptr(),
                                                    slot_desc->col_name()));
            }
        } else {
            columns.clear();
        }
        VLOG_ROW << "VNumbersTBFScanNode output rows: " << block->rows();
    } while (block->rows() == 0 && !(*eos));
    RETURN_IF_ERROR(VExprContext::filter_block(_vconjunct_ctx_ptr, block, block->columns()));
    reached_limit(block, eos);
    return Status::OK();
}


Status VNumbersTBFScanNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::CLOSE));
    SCOPED_TIMER(_runtime_profile->total_time_counter());

    return ExecNode::close(state);
}

Status VNumbersTBFScanNode::set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) {
    return Status::OK();
}

} // namespace doris
