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

#include "vec/exec/data_gen_functions/vnumbers_tvf.h"

#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <glog/logging.h>

#include <utility>

#include "common/status.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "vec/columns/column.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/data_types/data_type.h"

namespace doris::vectorized {

VNumbersTVF::VNumbersTVF(TupleId tuple_id, const TupleDescriptor* tuple_desc)
        : VDataGenFunctionInf(tuple_id, tuple_desc) {}

Status VNumbersTVF::get_next(RuntimeState* state, vectorized::Block* block, bool* eos) {
    bool mem_reuse = block->mem_reuse();
    DCHECK(block->rows() == 0);
    std::vector<vectorized::MutableColumnPtr> columns(_slot_num);

    do {
        for (int i = 0; i < _slot_num; ++i) {
            if (mem_reuse) {
                columns[i] = std::move(*(block->get_by_position(i).column)).mutate();
            } else {
                columns[i] = _tuple_desc->slots()[i]->get_empty_mutable_column();
            }
        }
        while (true) {
            RETURN_IF_CANCELLED(state);
            int batch_size = state->batch_size();
            if (columns[0]->size() == batch_size) {
                // what if batch_size < _total_numbers, should we set *eos？
                break;
            }
            // if _total_numbers == 0, so we can break loop at now.
            if (_cur_offset >= _total_numbers) {
                *eos = true;
                break;
            }
            columns[0]->insert_data(reinterpret_cast<const char*>(&_cur_offset),
                                    sizeof(_cur_offset));
            ++_cur_offset;
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
    return Status::OK();
}

Status VNumbersTVF::set_scan_ranges(const std::vector<TScanRangeParams>& scan_range_params) {
    // Currently we do not support multi-threads numbers function, so there is no need to
    // use more than one scan_range_param.
    DCHECK(scan_range_params.size() == 1);
    _total_numbers =
            scan_range_params[0].scan_range.data_gen_scan_range.numbers_params.totalNumbers;
    return Status::OK();
}

} // namespace doris::vectorized
