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
#include "runtime/runtime_state.h"
#include "runtime/row_batch.h"
#include "runtime/string_value.h"
#include "runtime/tuple_row.h"
#include "util/runtime_profile.h"

namespace doris {

OdbcScanNode::OdbcScanNode(ObjectPool* pool, const TPlanNode& tnode,
                             const DescriptorTbl& descs)
        : ScanNode(pool, tnode, descs),
          _is_init(false),
          _table_name(tnode.odbc_scan_node.table_name),
          _tuple_id(tnode.odbc_scan_node.tuple_id),
          _columns(tnode.odbc_scan_node.columns),
          _filters(tnode.odbc_scan_node.filters),
          _tuple_desc(nullptr) {
}

OdbcScanNode::~OdbcScanNode() {
}

Status OdbcScanNode::prepare(RuntimeState* state) {
    VLOG(1) << "OdbcScanNode::Prepare";

    if (_is_init) {
        return Status::OK();
    }

    if (NULL == state) {
        return Status::InternalError("input pointer is NULL.");
    }

    RETURN_IF_ERROR(ScanNode::prepare(state));
    // get tuple desc
    _tuple_desc = state->desc_tbl().get_tuple_descriptor(_tuple_id);

    if (NULL == _tuple_desc) {
        return Status::InternalError("Failed to get tuple descriptor.");
    }

    _slot_num = _tuple_desc->slots().size();
    // get odbc table info
    const ODBCTableDescriptor* odbc_table =
            static_cast<const ODBCTableDescriptor*>(_tuple_desc->table_desc());

    if (NULL == odbc_table) {
        return Status::InternalError("odbc table pointer is NULL.");
    }

    _odbc_param.host = odbc_table->host();
    _odbc_param.port = odbc_table->port();
    _odbc_param.user = odbc_table->user();
    _odbc_param.passwd = odbc_table->passwd();
    _odbc_param.db = odbc_table->db();
    _odbc_param.drivier = odbc_table->driver();
    _odbc_param.type = odbc_table->type();
    _odbc_param.tuple_desc = _tuple_desc;

    _odbc_scanner.reset(new (std::nothrow)ODBCScanner(_odbc_param));

    if (_odbc_scanner.get() == nullptr) {
        return Status::InternalError("new a odbc scanner failed.");
    }

    _tuple_pool.reset(new(std::nothrow) MemPool(mem_tracker().get()));

    if (_tuple_pool.get() == NULL) {
        return Status::InternalError("new a mem pool failed.");
    }

    _text_converter.reset(new(std::nothrow) TextConverter('\\'));

    if (_text_converter.get() == NULL) {
        return Status::InternalError("new a text convertor failed.");
    }

    _is_init = true;

    return Status::OK();
}

Status OdbcScanNode::open(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::open(state));
    VLOG(1) << "OdbcScanNode::Open";

    if (NULL == state) {
        return Status::InternalError("input pointer is NULL.");
    }

    if (!_is_init) {
        return Status::InternalError("used before initialize.");
    }

    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::OPEN));
    RETURN_IF_CANCELLED(state);
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(_odbc_scanner->open());
    RETURN_IF_ERROR(_odbc_scanner->query(_table_name, _columns, _filters));
    // check materialize slot num

    return Status::OK();
}

Status OdbcScanNode::write_text_slot(char* value, int value_length,
                                      SlotDescriptor* slot, RuntimeState* state) {
    if (!_text_converter->write_slot(slot, _tuple, value, value_length,
                                     true, false, _tuple_pool.get())) {
        std::stringstream ss;
        ss << "fail to convert odbc value '" << value << "' TO " << slot->type();
        return Status::InternalError(ss.str());
    }

    return Status::OK();
}

Status OdbcScanNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
    VLOG(1) << "OdbcScanNode::GetNext";

    if (NULL == state || NULL == row_batch || NULL == eos) {
        return Status::InternalError("input is NULL pointer");
    }

    if (!_is_init) {
        return Status::InternalError("used before initialize.");
    }

    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::GETNEXT));
    RETURN_IF_CANCELLED(state);
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    SCOPED_TIMER(materialize_tuple_timer());

    if (reached_limit()) {
        *eos = true;
        return Status::OK();
    }

    // create new tuple buffer for row_batch
    int tuple_buffer_size = row_batch->capacity() * _tuple_desc->byte_size();
    void* tuple_buffer = _tuple_pool->allocate(tuple_buffer_size);

    if (NULL == tuple_buffer) {
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
                    std::stringstream ss;
                    ss << "nonnull column contains NULL. table=" << _table_name
                       << ", column=" << slot_desc->col_name();
                    return Status::InternalError(ss.str());
                }
            } else if (column_data.strlen_or_ind > column_data.buffer_length) {
                std::stringstream ss;
                ss << "nonnull column contains NULL. table=" << _table_name
                   << ", column=" << slot_desc->col_name();
                return Status::InternalError(ss.str());
            } else {
                    RETURN_IF_ERROR(
                            write_text_slot(static_cast<char*>(column_data.target_value_ptr), column_data.strlen_or_ind, slot_desc, state));
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

    return Status::OK();
}

Status OdbcScanNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::CLOSE));
    SCOPED_TIMER(_runtime_profile->total_time_counter());

    _tuple_pool.reset();

    return ExecNode::close(state);
}

void OdbcScanNode::debug_string(int indentation_level, stringstream* out) const {
    *out << string(indentation_level * 2, ' ');
    *out << "OdbcScanNode(tupleid=" << _tuple_id << " table=" << _table_name;
    *out << ")" << std::endl;

    for (int i = 0; i < _children.size(); ++i) {
        _children[i]->debug_string(indentation_level + 1, out);
    }
}

Status OdbcScanNode::set_scan_ranges(const vector<TScanRangeParams>& scan_ranges) {
    return Status::OK();
}

}
