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

#include "vec/exec/vodbc_scan_node.h"

#include "exec/text_converter.h"
#include "exec/text_converter.hpp"

namespace doris {
namespace vectorized {

VOdbcScanNode::VOdbcScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : OdbcScanNode(pool, tnode, descs, "VOdbcScanNode") {}
VOdbcScanNode::~VOdbcScanNode() {}

Status VOdbcScanNode::get_next(RuntimeState* state, Block* block, bool* eos) {
    VLOG_CRITICAL << get_scan_node_type() << "::GetNext";

    if (nullptr == state || nullptr == block || nullptr == eos) {
        return Status::InternalError("input is NULL pointer");
    }

    if (!is_init()) {
        return Status::InternalError("used before initialize.");
    }

    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::GETNEXT));
    RETURN_IF_CANCELLED(state);

    auto odbc_scanner = get_odbc_scanner();
    auto tuple_desc = get_tuple_desc();
    auto text_converter = get_text_converter();

    auto column_size = tuple_desc->slots().size();
    std::vector<MutableColumnPtr> columns(column_size);

    bool mem_reuse = block->mem_reuse();
    // only empty block should be here
    DCHECK(block->rows() == 0);

    // Indicates whether there are more rows to process. Set in _odbc_scanner.next().
    bool odbc_eos = false;

    do {
        RETURN_IF_CANCELLED(state);

        for (auto i = 0; i < column_size; i++) {
            if (mem_reuse) {
                columns[i] = std::move(*block->get_by_position(i).column).mutate();
            } else {
                columns[i] = tuple_desc->slots()[i]->get_empty_mutable_column();
            }
        }

        for (int row_index = 0; true; row_index++) {
            // block is full, break
            if (state->batch_size() <= columns[0]->size()) {
                break;
            }

            RETURN_IF_ERROR(odbc_scanner->get_next_row(&odbc_eos));

            if (odbc_eos) {
                *eos = true;
                break;
            }

            // Read one row from reader

            for (int column_index = 0, materialized_column_index = 0; column_index < column_size;
                 ++column_index) {
                auto slot_desc = tuple_desc->slots()[column_index];
                // because the fe planner filter the non_materialize column
                if (!slot_desc->is_materialized()) {
                    continue;
                }
                const auto& column_data = odbc_scanner->get_column_data(materialized_column_index);

                char* value_data = static_cast<char*>(column_data.target_value_ptr);
                int value_len = column_data.strlen_or_ind;

                if (!text_converter->write_column(slot_desc, &columns[column_index], value_data,
                                                  value_len, true, false)) {
                    std::stringstream ss;
                    ss << "Fail to convert odbc value:'" << value_data << "' to "
                       << slot_desc->type() << " on column:`" << slot_desc->col_name() + "`";
                    return Status::InternalError(ss.str());
                }
                materialized_column_index++;
            }
        }

        // Before really use the Block, muse clear other ptr of column in block
        // So here need do std::move and clear in `columns`
        if (!mem_reuse) {
            int column_index = 0;
            for (const auto slot_desc : tuple_desc->slots()) {
                block->insert(ColumnWithTypeAndName(std::move(columns[column_index++]),
                                                    slot_desc->get_data_type_ptr(),
                                                    slot_desc->col_name()));
            }
        } else {
            columns.clear();
        }
        VLOG_ROW << "VOdbcScanNode output rows: " << block->rows();
    } while (block->rows() == 0 && !(*eos));

    RETURN_IF_ERROR(VExprContext::filter_block(_vconjunct_ctx_ptr, block, block->columns()));
    reached_limit(block, eos);

    return Status::OK();
}

} // namespace vectorized
} // namespace doris
