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

#include "vec/exec/vschema_scan_node.h"

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

VSchemaScanNode::VSchemaScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : SchemaScanNode(pool, tnode, descs), _src_single_tuple(nullptr), _dest_single_tuple(nullptr) {}

VSchemaScanNode::~VSchemaScanNode() {
    delete[] reinterpret_cast<char*>(_src_single_tuple);
    _src_single_tuple = NULL;

    delete[] reinterpret_cast<char*>(_dest_single_tuple);
    _dest_single_tuple = NULL;
}

Status VSchemaScanNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(SchemaScanNode::prepare(state));

    _src_single_tuple = reinterpret_cast<doris::Tuple*>(new (std::nothrow) char[_src_tuple_desc->byte_size()]);
    if (NULL == _src_single_tuple) {
        return Status::InternalError("new src single tuple failed.");
    }

    _dest_single_tuple = reinterpret_cast<doris::Tuple*>(new (std::nothrow) char[_dest_tuple_desc->byte_size()]);
    if (NULL == _dest_single_tuple) {
        return Status::InternalError("new desc single tuple failed.");
    }

    return Status::OK();
}

Status VSchemaScanNode::get_next(RuntimeState* state, vectorized::Block* block, bool* eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());

    VLOG_CRITICAL << "VSchemaScanNode::GetNext";
    if (state == NULL || block == NULL || eos == NULL)
        return Status::InternalError("input is NULL pointer");
    if (!_is_init) return Status::InternalError("used before initialize.");
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::GETNEXT));
    RETURN_IF_CANCELLED(state);
    std::vector<vectorized::MutableColumnPtr> columns(_slot_num);
    bool schema_eos = false;

    do {
        bool mem_reuse = block->mem_reuse();
        DCHECK(block->rows() == 0);

        columns.resize(_slot_num);
        for (int i = 0; i < _slot_num; ++i) {
            if (mem_reuse) {
                columns[i] = std::move(*block->get_by_position(i).column).mutate();
            } else {
                columns[i] = _dest_tuple_desc->slots()[i]->get_empty_mutable_column();
            }
        }
        while (true) {
            RETURN_IF_CANCELLED(state);

            // get all slots from schema table.
            RETURN_IF_ERROR(_schema_scanner->get_next_row(_src_single_tuple, _tuple_pool.get(),
                                                          &schema_eos));
            if (schema_eos) {
                *eos = true;
                break;
            }
            // tuple project
            project_tuple();

            for (int i = 0; i < _slot_num; ++i) {
                auto slot_desc = _dest_tuple_desc->slots()[i];
                if (!slot_desc->is_materialized()) {
                    continue;
                }

                if (_dest_single_tuple->is_null(slot_desc->null_indicator_offset())) {
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
                            write_slot_to_vectorized_column(_dest_single_tuple->get_slot(slot_desc->tuple_offset()), slot_desc, &columns[i]));
                }
            }
            if (columns[0]->size() == state->batch_size()) {
                break;
            }
        }
        if (!columns.empty() && !columns[0]->empty()) {
            auto n_columns = 0;
            if (!mem_reuse) {
                for (const auto slot_desc : _dest_tuple_desc->slots()) {
                    block->insert(ColumnWithTypeAndName(std::move(columns[n_columns++]),
                                                        slot_desc->get_data_type_ptr(),
                                                        slot_desc->col_name()));
                }
            } else {
                columns.clear();
            }
            RETURN_IF_ERROR(VExprContext::filter_block(_vconjunct_ctx_ptr, block, _dest_tuple_desc->slots().size()));
            VLOG_ROW << "VSchemaScanNode output rows: " << block->rows();
        }
    } while (block->rows() == 0 && !(*eos));

    reached_limit(block, eos);
    return Status::OK();
}

Status VSchemaScanNode::write_slot_to_vectorized_column(void* slot, 
                                                        SlotDescriptor* slot_desc,
                                                        vectorized::MutableColumnPtr* column_ptr) {
    vectorized::IColumn* col_ptr = column_ptr->get();
    if (slot_desc->is_nullable()) {
        auto* nullable_column = reinterpret_cast<vectorized::ColumnNullable*>(column_ptr->get());
        nullable_column->get_null_map_data().push_back(0);
        col_ptr = &nullable_column->get_nested_column();
    }
    switch (slot_desc->type().type) {
        case TYPE_HLL: {
            HyperLogLog* hll_slot = reinterpret_cast<HyperLogLog*>(slot);
            reinterpret_cast<vectorized::ColumnHLL*>(col_ptr)->get_data().emplace_back(*hll_slot);
            break;            
        }
        case TYPE_VARCHAR:
        case TYPE_CHAR:
        case TYPE_STRING: {
            StringValue* str_slot = reinterpret_cast<StringValue*>(slot);
            reinterpret_cast<vectorized::ColumnString*>(col_ptr)->insert_data(str_slot->ptr, str_slot->len);
            break;
        }

        case TYPE_BOOLEAN: {
            uint8_t num = *reinterpret_cast<bool*>(slot);
            reinterpret_cast<vectorized::ColumnVector<vectorized::UInt8>*>(col_ptr)->insert_value(num);
            break;
        }

        case TYPE_TINYINT: {
            int8_t num = *reinterpret_cast<int8_t*>(slot);
            reinterpret_cast<vectorized::ColumnVector<vectorized::Int8>*>(col_ptr)->insert_value(num);
            break;
        }

        case TYPE_SMALLINT: {
            int16_t num = *reinterpret_cast<int16_t*>(slot);
            reinterpret_cast<vectorized::ColumnVector<vectorized::Int16>*>(col_ptr)->insert_value(num);
            break;
        }

        case TYPE_INT: {
            int32_t num = *reinterpret_cast<int32_t*>(slot);
            reinterpret_cast<vectorized::ColumnVector<vectorized::Int32>*>(col_ptr)->insert_value(num);
            break;
        }

        case TYPE_BIGINT: {
            int64_t num = *reinterpret_cast<int64_t*>(slot);
            reinterpret_cast<vectorized::ColumnVector<vectorized::Int64>*>(col_ptr)->insert_value(num);
            break;
        }

        case TYPE_LARGEINT: {
            __int128 num;
            memcpy(&num, slot, sizeof(__int128));
            reinterpret_cast<vectorized::ColumnVector<vectorized::Int128>*>(col_ptr)->insert_value(num);
            break;
        }

        case TYPE_FLOAT: {
            float num = *reinterpret_cast<float*>(slot);
            reinterpret_cast<vectorized::ColumnVector<vectorized::Float32>*>(col_ptr)->insert_value(num);
            break;
        }

        case TYPE_DOUBLE: {
            double num = *reinterpret_cast<double*>(slot);
            reinterpret_cast<vectorized::ColumnVector<vectorized::Float64>*>(col_ptr)->insert_value(num);
            break;
        }

        case TYPE_DATE: {
            VecDateTimeValue value;
            DateTimeValue* ts_slot = reinterpret_cast<DateTimeValue*>(slot);
            value.convert_dt_to_vec_dt(ts_slot);
            reinterpret_cast<vectorized::ColumnVector<vectorized::Int64>*>(col_ptr)->insert_data(
                reinterpret_cast<char*>(&value), 0);
            break;
        }

        case TYPE_DATETIME: {
            VecDateTimeValue value;
            DateTimeValue* ts_slot = reinterpret_cast<DateTimeValue*>(slot);
            value.convert_dt_to_vec_dt(ts_slot);
            reinterpret_cast<vectorized::ColumnVector<vectorized::Int64>*>(col_ptr)->insert_data(
                reinterpret_cast<char*>(&value), 0);
            break;
        }

        case TYPE_DECIMALV2: {
            __int128 num = (reinterpret_cast<PackedInt128*>(slot))->value;
            reinterpret_cast<vectorized::ColumnVector<doris::PackedInt128>*>(col_ptr)->insert_value(num);
            break;
        }

        default: {
            DCHECK(false) << "bad slot type: " << slot_desc->type();
            std::stringstream ss;
            ss << "Fail to convert schema type:'" << slot_desc->type() << " on column:`"
            << slot_desc->col_name() + "`";
            return Status::InternalError(ss.str());
        }
    }

    return Status::OK();
}

void VSchemaScanNode::project_tuple() {
    memset(_dest_single_tuple, 0, _dest_tuple_desc->num_null_bytes());

    for (int i = 0; i < _slot_num; ++i) {
        if (!_dest_tuple_desc->slots()[i]->is_materialized()) {
            continue;
        }
        int j = _index_map[i];

        if (_src_single_tuple->is_null(_src_tuple_desc->slots()[j]->null_indicator_offset())) {
            _dest_single_tuple->set_null(_dest_tuple_desc->slots()[i]->null_indicator_offset());
        } else {
            void* dest_slot = _dest_single_tuple->get_slot(_dest_tuple_desc->slots()[i]->tuple_offset());
            void* src_slot = _src_single_tuple->get_slot(_src_tuple_desc->slots()[j]->tuple_offset());
            int slot_size = _src_tuple_desc->slots()[j]->type().get_slot_size();
            memcpy(dest_slot, src_slot, slot_size);
        }
    }
}

} // namespace doris::vectorized
