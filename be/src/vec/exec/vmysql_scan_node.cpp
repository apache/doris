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
        : ScanNode(pool, tnode, descs),
          _is_init(false),
          _table_name(tnode.mysql_scan_node.table_name),
          _tuple_id(tnode.mysql_scan_node.tuple_id),
          _columns(tnode.mysql_scan_node.columns),
          _filters(tnode.mysql_scan_node.filters),
          _tuple_desc(nullptr),
          _slot_num(0) {}

Status VMysqlScanNode::prepare(RuntimeState* state) {
    VLOG_CRITICAL << "VMysqlScanNode::Prepare";

    if (_is_init) {
        return Status::OK();
    }

    if (nullptr == state) {
        return Status::InternalError("input pointer is nullptr.");
    }

    RETURN_IF_ERROR(ScanNode::prepare(state));
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker_growh());
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
    _my_param.charset = mysql_table->charset();
    // new one scanner
    _mysql_scanner.reset(new (std::nothrow) MysqlScanner(_my_param));

    if (_mysql_scanner.get() == nullptr) {
        return Status::InternalError("new a mysql scanner failed.");
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

Status VMysqlScanNode::open(RuntimeState* state) {
    START_AND_SCOPE_SPAN(state->get_tracer(), span, "VMysqlScanNode::open");
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker_growh());
    VLOG_CRITICAL << "MysqlScanNode::Open";

    if (nullptr == state) {
        return Status::InternalError("input pointer is nullptr.");
    }

    if (!_is_init) {
        return Status::InternalError("used before initialize.");
    }

    RETURN_IF_CANCELLED(state);
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

Status VMysqlScanNode::write_text_slot(char* value, int value_length, SlotDescriptor* slot,
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

Status VMysqlScanNode::get_next(RuntimeState* state, vectorized::Block* block, bool* eos) {
    INIT_AND_SCOPE_GET_NEXT_SPAN(state->get_tracer(), _get_next_span, "VMysqlScanNode::get_next");
    VLOG_CRITICAL << "VMysqlScanNode::GetNext";
    if (state == NULL || block == NULL || eos == NULL)
        return Status::InternalError("input is NULL pointer");
    if (!_is_init) return Status::InternalError("used before initialize.");
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
                        return Status::InternalError(
                                "nonnull column contains NULL. table={}, column={}", _table_name,
                                slot_desc->col_name());
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

Status VMysqlScanNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    START_AND_SCOPE_SPAN(state->get_tracer(), span, "VMysqlScanNode::close");
    SCOPED_TIMER(_runtime_profile->total_time_counter());

    _tuple_pool.reset();

    return ExecNode::close(state);
}

void VMysqlScanNode::debug_string(int indentation_level, std::stringstream* out) const {
    *out << string(indentation_level * 2, ' ');
    *out << "VMysqlScanNode(tupleid=" << _tuple_id << " table=" << _table_name;
    *out << ")" << std::endl;

    for (int i = 0; i < _children.size(); ++i) {
        _children[i]->debug_string(indentation_level + 1, out);
    }
}

Status VMysqlScanNode::set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) {
    return Status::OK();
}
} // namespace doris::vectorized