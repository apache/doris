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

#include "vec/exec/vmysql_scan_node.h"

#include "exec/text_converter.h"
#include "exec/text_converter.hpp"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"
#include "runtime/tuple_row.h"
#include "util/runtime_profile.h"
#include "util/types.h"
namespace doris::vectorized {

VMysqlScanNode::VMysqlScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : MysqlScanNode(pool, tnode, descs) {}

VMysqlScanNode::~VMysqlScanNode() {}

Status VMysqlScanNode::get_next(RuntimeState* state, vectorized::Block* block, bool* eos) {
    VLOG_CRITICAL << "VMysqlScanNode::GetNext";
    if (state == NULL || block == NULL || eos == NULL)
        return Status::InternalError("input is NULL pointer");
    if (!_is_init) return Status::InternalError("used before initialize.");
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::GETNEXT));
    RETURN_IF_CANCELLED(state);
    bool mem_reuse = block->mem_reuse();
    DCHECK(block->rows() == 0);
    std::vector<vectorized::MutableColumnPtr> columns(_slot_num);
    bool mysql_eos = false;

    do {
        columns.resize(_slot_num);
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

            char** data = NULL;
            unsigned long* length = NULL;
            RETURN_IF_ERROR(_mysql_scanner->get_next_row(&data, &length, &mysql_eos));

            if (mysql_eos) {
                *eos = true;
                break;
            }
            int j = 0;
            for (int i = 0; i < _slot_num; ++i) {
                auto slot_desc = _tuple_desc->slots()[i];
                if (!slot_desc->is_materialized()) {
                    continue;
                }

                if (data[j] == nullptr) {
                    if (slot_desc->is_nullable()) {
                        auto* nullable_column =
                                reinterpret_cast<vectorized::ColumnNullable*>(columns[i].get());
                        nullable_column->insert_data(nullptr, 0);
                    } else {
                        std::stringstream ss;
                        ss << "nonnull column contains NULL. table=" << _table_name
                           << ", column=" << slot_desc->col_name();
                        return Status::InternalError(ss.str());
                    }
                } else {
                    RETURN_IF_ERROR(
                            write_text_column(data[j], length[j], slot_desc, &columns[i], state));
                }
                j++;
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
        VLOG_ROW << "VMYSQLScanNode output rows: " << block->rows();
    } while (block->rows() == 0 && !(*eos));

    reached_limit(block, eos);
    return Status::OK();
}

Status VMysqlScanNode::write_text_column(char* value, int value_length, SlotDescriptor* slot,
                                         vectorized::MutableColumnPtr* column_ptr,
                                         RuntimeState* state) {
    if (!_text_converter->write_column(slot, column_ptr, value, value_length, true, false)) {
        std::stringstream ss;
        ss << "Fail to convert mysql value:'" << value << "' to " << slot->type() << " on column:`"
           << slot->col_name() + "`";
        return Status::InternalError(ss.str());
    }
    return Status::OK();
}
} // namespace doris::vectorized