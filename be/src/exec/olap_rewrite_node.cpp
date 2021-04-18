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

#include "exec/olap_rewrite_node.h"

#include <sstream>

#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "runtime/descriptors.h"
#include "runtime/raw_value.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/tuple.h"

namespace doris {

OlapRewriteNode::OlapRewriteNode(ObjectPool* pool, const TPlanNode& tnode,
                                 const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs),
          _child_row_batch(nullptr),
          _child_row_idx(0),
          _child_eos(false) {}

Status OlapRewriteNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    DCHECK(tnode.__isset.olap_rewrite_node);
    // create columns
    RETURN_IF_ERROR(Expr::create_expr_trees(_pool, tnode.olap_rewrite_node.columns, &_columns));
    _column_types = tnode.olap_rewrite_node.column_types;
    _output_tuple_id = tnode.olap_rewrite_node.output_tuple_id;
    return Status::OK();
}

Status OlapRewriteNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::prepare(state));
    RETURN_IF_ERROR(Expr::prepare(_columns, state, child(0)->row_desc(), expr_mem_tracker()));
    _output_tuple_desc = state->desc_tbl().get_tuple_descriptor(_output_tuple_id);
    // _child_row_batch.reset(new RowBatch(child(0)->row_desc(), state->batch_size(), mem_tracker()));
    _child_row_batch.reset(new RowBatch(child(0)->row_desc(), state->batch_size(),
                                        state->fragment_mem_tracker().get()));

    _max_decimal_val.resize(_column_types.size());
    _max_decimalv2_val.resize(_column_types.size());
    for (int i = 0; i < _column_types.size(); ++i) {
        if (_column_types[i].type == TPrimitiveType::DECIMAL) {
            _max_decimal_val[i].to_max_decimal(_column_types[i].precision, _column_types[i].scale);
        } else if (_column_types[i].type == TPrimitiveType::DECIMALV2) {
            _max_decimalv2_val[i].to_max_decimal(_column_types[i].precision,
                                                 _column_types[i].scale);
        }
    }
    return Status::OK();
}

Status OlapRewriteNode::open(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::open(state));
    RETURN_IF_ERROR(Expr::open(_columns, state));
    RETURN_IF_ERROR(child(0)->open(state));
    return Status::OK();
}

Status OlapRewriteNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
    RETURN_IF_CANCELLED(state);
    SCOPED_TIMER(_runtime_profile->total_time_counter());

    if (reached_limit() || (_child_row_idx == _child_row_batch->num_rows() && _child_eos)) {
        // we're already done or we exhausted the last child batch and there won't be any
        // new ones
        *eos = true;
        return Status::OK();
    }

    // start (or continue) consuming row batches from child
    while (true) {
        if (_child_row_idx == _child_row_batch->num_rows()) {
            // fetch next batch
            RETURN_IF_CANCELLED(state);
            row_batch->tuple_data_pool()->acquire_data(_child_row_batch->tuple_data_pool(), false);
            _child_row_batch->reset();
            RETURN_IF_ERROR(child(0)->get_next(state, _child_row_batch.get(), &_child_eos));
            _child_row_idx = 0;
        }

        if (copy_rows(state, row_batch)) {
            *eos = reached_limit() ||
                   (_child_row_idx == _child_row_batch->num_rows() && _child_eos);
            return Status::OK();
        }

        if (_child_eos) {
            // finished w/ last child row batch, and child eos is true
            *eos = true;
            return Status::OK();
        }
    }

    return Status::OK();
}

bool OlapRewriteNode::copy_one_row(TupleRow* src_row, Tuple* tuple, MemPool* pool,
                                   std::stringstream* ss) {
    memset(tuple, 0, _output_tuple_desc->num_null_bytes());
    // check if valid
    for (int i = 0; i < _columns.size(); ++i) {
        void* src_value = _columns[i]->get_value(src_row);
        SlotDescriptor* slot_desc = _output_tuple_desc->slots()[i];
        // support null for insert into statement
        if (!slot_desc->is_nullable()) {
            if (src_value == nullptr) {
                //column in target table satisfy not null constraint
                (*ss) << "column(" << slot_desc->col_name() << ")'s value is null";
                return false;
            }
        } else {
            if (src_value == nullptr) {
                tuple->set_null(slot_desc->null_indicator_offset());
                continue;
            } else {
                tuple->set_not_null(slot_desc->null_indicator_offset());
            }
        }

        const TColumnType& column_type = _column_types[i];
        switch (column_type.type) {
        case TPrimitiveType::CHAR:
        case TPrimitiveType::VARCHAR: {
            // Fixed length string
            StringValue* str_val = (StringValue*)src_value;
            if (str_val->len > column_type.len) {
                (*ss) << "the length of input is too long than schema. "
                      << "column_name: " << slot_desc->col_name() << "; "
                      << "input_str: [" << std::string(str_val->ptr, str_val->len) << "] "
                      << "schema length: " << column_type.len << "; "
                      << "actual length: " << str_val->len << "; ";
                return false;
            }
            StringValue* dst_val = (StringValue*)tuple->get_slot(slot_desc->tuple_offset());
            if (column_type.type == TPrimitiveType::CHAR) {
                dst_val->ptr = (char*)pool->allocate(column_type.len);
                memcpy(dst_val->ptr, str_val->ptr, str_val->len);
                memset(dst_val->ptr + str_val->len, 0, column_type.len - str_val->len);
                dst_val->len = column_type.len;
            } else {
                dst_val->ptr = (char*)pool->allocate(column_type.len);
                memcpy(dst_val->ptr, str_val->ptr, str_val->len);
                dst_val->len = str_val->len;
            }
            break;
        }
        case TPrimitiveType::DECIMAL: {
            DecimalValue* dec_val = (DecimalValue*)src_value;
            DecimalValue* dst_val = (DecimalValue*)tuple->get_slot(slot_desc->tuple_offset());
            if (dec_val->scale() > column_type.scale) {
                int code = dec_val->round(dst_val, column_type.scale, HALF_UP);
                if (code != E_DEC_OK) {
                    (*ss) << "round one decimal failed.value=" << dec_val->to_string();
                    return false;
                }
            } else {
                *dst_val = *dec_val;
            }
            if (*dst_val > _max_decimal_val[i]) {
                dst_val->to_max_decimal(column_type.precision, column_type.scale);
            }
            break;
        }
        case TPrimitiveType::DECIMALV2: {
            DecimalV2Value* dec_val = (DecimalV2Value*)src_value;
            DecimalV2Value* dst_val = (DecimalV2Value*)tuple->get_slot(slot_desc->tuple_offset());
            if (dec_val->greater_than_scale(column_type.scale)) {
                int code = dec_val->round(dst_val, column_type.scale, HALF_UP);
                if (code != E_DEC_OK) {
                    (*ss) << "round one decimal failed.value=" << dec_val->to_string();
                    return false;
                }
            } else {
                *reinterpret_cast<PackedInt128*>(dst_val) =
                        *reinterpret_cast<const PackedInt128*>(dec_val);
            }
            if (*dst_val > _max_decimalv2_val[i]) {
                dst_val->to_max_decimal(column_type.precision, column_type.scale);
            }
            break;
        }
        default: {
            void* dst_val = (void*)tuple->get_slot(slot_desc->tuple_offset());
            RawValue::write(src_value, dst_val, slot_desc->type(), pool);
            break;
        }
        }
    }
    return true;
}

bool OlapRewriteNode::copy_rows(RuntimeState* state, RowBatch* output_batch) {
    Tuple* tuple = nullptr;
    MemPool* pool = output_batch->tuple_data_pool();
    int64_t num_rows_invalid = 0;
    for (; _child_row_idx < _child_row_batch->num_rows(); ++_child_row_idx) {
        // Add a new row to output_batch
        int dst_row_idx = output_batch->add_row();

        if (dst_row_idx == RowBatch::INVALID_ROW_INDEX) {
            return true;
        }

        if (tuple == nullptr) {
            tuple = Tuple::create(_output_tuple_desc->byte_size(), pool);
        }
        TupleRow* src_row = _child_row_batch->get_row(_child_row_idx);

        std::stringstream ss;
        if (copy_one_row(src_row, tuple, pool, &ss)) {
            TupleRow* dst_row = output_batch->get_row(dst_row_idx);
            dst_row->set_tuple(0, tuple);
            tuple = nullptr;
            output_batch->commit_last_row();
            ++_num_rows_returned;
            COUNTER_SET(_rows_returned_counter, _num_rows_returned);

            if (reached_limit()) {
                return true;
            }
        } else {
            num_rows_invalid++;
            state->append_error_msg_to_file("", ss.str());
        }
    }
    if (num_rows_invalid > 0) {
        state->update_num_rows_load_filtered(num_rows_invalid);
    }

    if (VLOG_ROW_IS_ON) {
        for (int i = 0; i < output_batch->num_rows(); ++i) {
            TupleRow* row = output_batch->get_row(i);
            VLOG_ROW << "OlapRewriteNode input row: " << row->to_string(row_desc());
        }
    }

    return output_batch->is_full() || output_batch->at_resource_limit();
}

Status OlapRewriteNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    _child_row_batch.reset();
    // RETURN_IF_ERROR(child(0)->close(state));
    Expr::close(_columns, state);
    return ExecNode::close(state);
}
} // namespace doris
