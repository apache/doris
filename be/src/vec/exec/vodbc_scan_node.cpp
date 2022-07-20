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

VOdbcScanNode::VOdbcScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs,
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

Status VOdbcScanNode::prepare(RuntimeState* state) {
    VLOG_CRITICAL << _scan_node_type << "::Prepare";

    if (_is_init) {
        return Status::OK();
    }

    if (nullptr == state) {
        return Status::InternalError("input pointer is null.");
    }

    RETURN_IF_ERROR(ScanNode::prepare(state));
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker());
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

Status VOdbcScanNode::open(RuntimeState* state) {
    START_AND_SCOPE_SPAN(state->get_tracer(), span, "VOdbcScanNode::open");
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker());
    VLOG_CRITICAL << _scan_node_type << "::Open";

    if (nullptr == state) {
        return Status::InternalError("input pointer is null.");
    }

    if (!_is_init) {
        return Status::InternalError("used before initialize.");
    }

    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(_odbc_scanner->open());
    RETURN_IF_ERROR(_odbc_scanner->query());
    // check materialize slot num

    return Status::OK();
}

Status VOdbcScanNode::write_text_slot(char* value, int value_length, SlotDescriptor* slot,
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

Status VOdbcScanNode::get_next(RuntimeState* state, Block* block, bool* eos) {
    INIT_AND_SCOPE_GET_NEXT_SPAN(state->get_tracer(), _get_next_span, "VOdbcScanNode::get_next");
    VLOG_CRITICAL << get_scan_node_type() << "::GetNext";

    if (nullptr == state || nullptr == block || nullptr == eos) {
        return Status::InternalError("input is NULL pointer");
    }

    if (!is_init()) {
        return Status::InternalError("used before initialize.");
    }
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

        columns.resize(column_size);
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

Status VOdbcScanNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    START_AND_SCOPE_SPAN(state->get_tracer(), span, "VOdbcScanNode::close");
    SCOPED_TIMER(_runtime_profile->total_time_counter());

    _tuple_pool.reset();

    return ExecNode::close(state);
}

void VOdbcScanNode::debug_string(int indentation_level, std::stringstream* out) const {
    *out << string(indentation_level * 2, ' ');
    *out << _scan_node_type << "(tupleid=" << _tuple_id << " table=" << _table_name;
    *out << ")" << std::endl;

    for (int i = 0; i < _children.size(); ++i) {
        _children[i]->debug_string(indentation_level + 1, out);
    }
}

Status VOdbcScanNode::set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) {
    return Status::OK();
}

} // namespace vectorized
} // namespace doris
