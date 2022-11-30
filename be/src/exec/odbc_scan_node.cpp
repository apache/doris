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

#include "odbc_scan_node.h"

#include <sstream>

#include "exec/text_converter.hpp"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/tuple_row.h"
#include "util/runtime_profile.h"

namespace doris {

OdbcScanNode::OdbcScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs,
                           std::string scan_node_type)
        : ScanNode(pool, tnode, descs),
          _is_init(false),
          _scan_node_type(std::move(scan_node_type)),
          _table_name(tnode.odbc_scan_node.table_name),
          _connect_string(std::move(tnode.odbc_scan_node.connect_string)),
          _query_string(std::move(tnode.odbc_scan_node.query_string)),
          _tuple_id(tnode.odbc_scan_node.tuple_id),
          _tuple_desc(nullptr),
          _slot_num(0) {}

OdbcScanNode::~OdbcScanNode() {}

Status OdbcScanNode::prepare(RuntimeState* state) {
    VLOG_CRITICAL << _scan_node_type << "::Prepare";

    if (_is_init) {
        return Status::OK();
    }

    if (nullptr == state) {
        return Status::InternalError("input pointer is null.");
    }

    RETURN_IF_ERROR(ScanNode::prepare(state));
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker_growh());
    // get tuple desc
    _tuple_desc = state->desc_tbl().get_tuple_descriptor(_tuple_id);

    if (nullptr == _tuple_desc) {
        return Status::InternalError("Failed to get tuple descriptor.");
    }

    _slot_num = _tuple_desc->slots().size();

    _odbc_param.connect_string = std::move(_connect_string);
    _odbc_param.query_string = std::move(_query_string);
    _odbc_param.tuple_desc = _tuple_desc;

    _odbc_scanner.reset(new (std::nothrow) ODBCConnector(_odbc_param));

    if (_odbc_scanner.get() == nullptr) {
        return Status::InternalError("new a odbc scanner failed.");
    }

    _tuple_pool.reset(new (std::nothrow) MemPool());

    if (_tuple_pool.get() == nullptr) {
        return Status::InternalError("new a mem pool failed.");
    }

    _text_converter.reset(new (std::nothrow) TextConverter('\\'));

    if (_text_converter.get() == nullptr) {
        return Status::InternalError("new a text convertor failed.");
    }

    _is_init = true;

    return Status::OK();
}

Status OdbcScanNode::open(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker_growh());
    VLOG_CRITICAL << _scan_node_type << "::Open";

    if (nullptr == state) {
        return Status::InternalError("input pointer is null.");
    }

    if (!_is_init) {
        return Status::InternalError("used before initialize.");
    }

    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(_odbc_scanner->open(state));
    RETURN_IF_ERROR(_odbc_scanner->query());
    // check materialize slot num

    return Status::OK();
}

Status OdbcScanNode::write_text_slot(char* value, int value_length, SlotDescriptor* slot,
                                     RuntimeState* state) {
    if (!_text_converter->write_slot(slot, _tuple, value, value_length, true, false,
                                     _tuple_pool.get())) {
        std::stringstream ss;
        ss << "Fail to convert odbc value:'" << value << "' to " << slot->type() << " on column:`"
           << slot->col_name() + "`";
        return Status::InternalError(ss.str());
    }

    return Status::OK();
}

Status OdbcScanNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
    VLOG_CRITICAL << _scan_node_type << "::GetNext";

    if (nullptr == state || nullptr == row_batch || nullptr == eos) {
        return Status::InternalError("input is nullptr pointer");
    }

    if (!_is_init) {
        return Status::InternalError("used before initialize.");
    }

    RETURN_IF_CANCELLED(state);
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker_growh());

    if (reached_limit()) {
        *eos = true;
        return Status::OK();
    }

    // create new tuple buffer for row_batch
    int tuple_buffer_size = row_batch->capacity() * _tuple_desc->byte_size();
    void* tuple_buffer = _tuple_pool->allocate(tuple_buffer_size);

    if (nullptr == tuple_buffer) {
        return Status::InternalError("Allocate memory failed.");
    }

    _tuple = reinterpret_cast<Tuple*>(tuple_buffer);
    // Indicates whether there are more rows to process. Set in _odbc_scanner.next().
    bool odbc_eos = false;

    while (true) {
        RETURN_IF_CANCELLED(state);

        if (reached_limit() || row_batch->is_full()) {
            // hang on to last allocated chunk in pool, we'll keep writing into it in the
            // next get_next() call
            row_batch->tuple_data_pool()->acquire_data(_tuple_pool.get(), !reached_limit());
            *eos = reached_limit();
            return Status::OK();
        }

        RETURN_IF_ERROR(_odbc_scanner->get_next_row(&odbc_eos));

        if (odbc_eos) {
            row_batch->tuple_data_pool()->acquire_data(_tuple_pool.get(), false);
            *eos = true;
            return Status::OK();
        }

        int row_idx = row_batch->add_row();
        TupleRow* row = row_batch->get_row(row_idx);
        // scan node is the first tuple of tuple row
        row->set_tuple(0, _tuple);
        memset(_tuple, 0, _tuple_desc->num_null_bytes());
        int j = 0;

        for (int i = 0; i < _slot_num; ++i) {
            auto slot_desc = _tuple_desc->slots()[i];
            // because the fe planner filter the non_materialize column
            if (!slot_desc->is_materialized()) {
                continue;
            }

            const auto& column_data = _odbc_scanner->get_column_data(j);
            if (column_data.strlen_or_ind == SQL_NULL_DATA) {
                if (slot_desc->is_nullable()) {
                    _tuple->set_null(slot_desc->null_indicator_offset());
                } else {
                    return Status::InternalError(
                            "nonnull column contains nullptr. table={}, column={}", _table_name,
                            slot_desc->col_name());
                }
            } else if (column_data.strlen_or_ind > column_data.buffer_length) {
                return Status::InternalError(
                        "column value length longer than buffer length. "
                        "table={}, column={}, buffer_length",
                        _table_name, slot_desc->col_name(), column_data.buffer_length);
            } else {
                RETURN_IF_ERROR(write_text_slot(static_cast<char*>(column_data.target_value_ptr),
                                                column_data.strlen_or_ind, slot_desc, state));
            }
            j++;
        }

        ExprContext* const* ctxs = &_conjunct_ctxs[0];
        int num_ctxs = _conjunct_ctxs.size();

        // ODBC scanner can not filter conjunct with function, need check conjunct again.
        if (ExecNode::eval_conjuncts(ctxs, num_ctxs, row)) {
            row_batch->commit_last_row();
            ++_num_rows_returned;
            COUNTER_SET(_rows_returned_counter, _num_rows_returned);
            char* new_tuple = reinterpret_cast<char*>(_tuple);
            new_tuple += _tuple_desc->byte_size();
            _tuple = reinterpret_cast<Tuple*>(new_tuple);
        }
    }
}

Status OdbcScanNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    SCOPED_TIMER(_runtime_profile->total_time_counter());

    _tuple_pool.reset();

    return ExecNode::close(state);
}

void OdbcScanNode::debug_string(int indentation_level, std::stringstream* out) const {
    *out << string(indentation_level * 2, ' ');
    *out << _scan_node_type << "(tupleid=" << _tuple_id << " table=" << _table_name;
    *out << ")" << std::endl;

    for (int i = 0; i < _children.size(); ++i) {
        _children[i]->debug_string(indentation_level + 1, out);
    }
}

Status OdbcScanNode::set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) {
    return Status::OK();
}

} // namespace doris
