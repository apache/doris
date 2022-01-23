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

#include "mysql_scan_node.h"

#include <sstream>

#include "exec/text_converter.hpp"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"
#include "runtime/tuple_row.h"
#include "util/runtime_profile.h"

namespace doris {

MysqlScanNode::MysqlScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ScanNode(pool, tnode, descs),
          _is_init(false),
          _table_name(tnode.mysql_scan_node.table_name),
          _tuple_id(tnode.mysql_scan_node.tuple_id),
          _columns(tnode.mysql_scan_node.columns),
          _filters(tnode.mysql_scan_node.filters),
          _tuple_desc(nullptr),
          _slot_num(0) {}

MysqlScanNode::~MysqlScanNode() {}

Status MysqlScanNode::prepare(RuntimeState* state) {
    VLOG_CRITICAL << "MysqlScanNode::Prepare";

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

    _slot_num = _tuple_desc->slots().size();
    // get mysql info
    const MySQLTableDescriptor* mysql_table =
            static_cast<const MySQLTableDescriptor*>(_tuple_desc->table_desc());

    if (nullptr == mysql_table) {
        return Status::InternalError("mysql table pointer is nullptr.");
    }

    _my_param.host = mysql_table->host();
    _my_param.port = mysql_table->port();
    _my_param.user = mysql_table->user();
    _my_param.passwd = mysql_table->passwd();
    _my_param.db = mysql_table->mysql_db();
    // new one scanner
    _mysql_scanner.reset(new (std::nothrow) MysqlScanner(_my_param));

    if (_mysql_scanner.get() == nullptr) {
        return Status::InternalError("new a mysql scanner failed.");
    }

    _tuple_pool.reset(new (std::nothrow) MemPool(mem_tracker().get()));

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

Status MysqlScanNode::open(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::open(state));
    VLOG_CRITICAL << "MysqlScanNode::Open";

    if (nullptr == state) {
        return Status::InternalError("input pointer is nullptr.");
    }

    if (!_is_init) {
        return Status::InternalError("used before initialize.");
    }

    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::OPEN));
    RETURN_IF_CANCELLED(state);
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(_mysql_scanner->open());
    RETURN_IF_ERROR(_mysql_scanner->query(_table_name, _columns, _filters, _limit));

    // check materialize slot num
    int materialize_num = 0;

    for (int i = 0; i < _tuple_desc->slots().size(); ++i) {
        if (_tuple_desc->slots()[i]->is_materialized()) {
            materialize_num++;
        }
    }

    if (_mysql_scanner->field_num() != materialize_num) {
        return Status::InternalError("input and output not equal.");
    }

    return Status::OK();
}

Status MysqlScanNode::write_text_slot(char* value, int value_length, SlotDescriptor* slot,
                                      RuntimeState* state) {
    if (!_text_converter->write_slot(slot, _tuple, value, value_length, true, false,
                                     _tuple_pool.get())) {
        std::stringstream ss;
        ss << "Fail to convert mysql value:'" << value << "' to " << slot->type() << " on column:`"
           << slot->col_name() + "`";
        return Status::InternalError(ss.str());
    }

    return Status::OK();
}

Status MysqlScanNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
    VLOG_CRITICAL << "MysqlScanNode::GetNext";

    if (nullptr == state || nullptr == row_batch || nullptr == eos) {
        return Status::InternalError("input is nullptr pointer");
    }

    if (!_is_init) {
        return Status::InternalError("used before initialize.");
    }

    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::GETNEXT));
    RETURN_IF_CANCELLED(state);
    SCOPED_TIMER(_runtime_profile->total_time_counter());

    // create new tuple buffer for row_batch
    int tuple_buffer_size = row_batch->capacity() * _tuple_desc->byte_size();
    void* tuple_buffer = _tuple_pool->allocate(tuple_buffer_size);

    if (nullptr == tuple_buffer) {
        return Status::InternalError("Allocate memory failed.");
    }

    _tuple = reinterpret_cast<Tuple*>(tuple_buffer);
    // Indicates whether there are more rows to process. Set in _hbase_scanner.next().
    bool mysql_eos = false;

    while (true) {
        RETURN_IF_CANCELLED(state);

        if (row_batch->is_full()) {
            // hang on to last allocated chunk in pool, we'll keep writing into it in the
            // next get_next() call
            row_batch->tuple_data_pool()->acquire_data(_tuple_pool.get(), !reached_limit());
            return Status::OK();
        }

        // read mysql
        char** data = nullptr;
        unsigned long* length = nullptr;
        RETURN_IF_ERROR(_mysql_scanner->get_next_row(&data, &length, &mysql_eos));

        if (mysql_eos) {
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

            if (data[j] == nullptr) {
                if (slot_desc->is_nullable()) {
                    _tuple->set_null(slot_desc->null_indicator_offset());
                } else {
                    std::stringstream ss;
                    ss << "nonnull column contains nullptr. table=" << _table_name
                       << ", column=" << slot_desc->col_name();
                    return Status::InternalError(ss.str());
                }
            } else {
                RETURN_IF_ERROR(write_text_slot(data[j], length[j], slot_desc, state));
            }

            j++;
        }

        // MySQL has filter all rows, no need check.
        {
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

Status MysqlScanNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::CLOSE));
    SCOPED_TIMER(_runtime_profile->total_time_counter());

    _tuple_pool.reset();

    return ExecNode::close(state);
}

void MysqlScanNode::debug_string(int indentation_level, std::stringstream* out) const {
    *out << string(indentation_level * 2, ' ');
    *out << "MysqlScanNode(tupleid=" << _tuple_id << " table=" << _table_name;
    *out << ")" << std::endl;

    for (int i = 0; i < _children.size(); ++i) {
        _children[i]->debug_string(indentation_level + 1, out);
    }
}

Status MysqlScanNode::set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) {
    return Status::OK();
}

} // namespace doris
