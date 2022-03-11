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

VNumbersTBF::VNumbersTBF(VTableValuedFunctionScanNode& tbf_scan_node)
        : VTableValuedFunctionInf(tbf_scan_node) {
}

VNumbersTBF::~VNumbersTBF() {}

Status VNumbersTBF::get_next(RuntimeState* state, vectorized::Block* block, bool* eos) {
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
    } while (block->rows() == 0 && !(*eos));
    RETURN_IF_ERROR(VExprContext::filter_block(_vconjunct_ctx_ptr, block, block->columns()));
    reached_limit(block, eos);
    return Status::OK();
}

Status VNumbersTBF::set_scan_ranges(const std::vector<TScanRangeParams>& scan_range_params) {
    _total_numbers = scan_range_params[0].scan_range.numbers_tvf_scan_range.totalNumbers;
    return Status::OK();
}

} // namespace doris
