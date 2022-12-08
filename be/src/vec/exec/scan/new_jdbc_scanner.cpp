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

#include "new_jdbc_scanner.h"

namespace doris::vectorized {
NewJdbcScanner::NewJdbcScanner(RuntimeState* state, NewJdbcScanNode* parent, int64_t limit,
                               const TupleId& tuple_id, const std::string& query_string)
        : VScanner(state, static_cast<VScanNode*>(parent), limit),
          _is_init(false),
          _jdbc_eos(false),
          _tuple_id(tuple_id),
          _query_string(query_string),
          _tuple_desc(nullptr) {}

Status NewJdbcScanner::prepare(RuntimeState* state) {
    VLOG_CRITICAL << "NewJdbcScanner::Prepare";
    if (_is_init) {
        return Status::OK();
    }

    if (state == nullptr) {
        return Status::InternalError("input pointer is NULL of VJdbcScanNode::prepare.");
    }

    // get tuple desc
    _tuple_desc = state->desc_tbl().get_tuple_descriptor(_tuple_id);
    if (_tuple_desc == nullptr) {
        return Status::InternalError("Failed to get tuple descriptor.");
    }

    // get jdbc table info
    const JdbcTableDescriptor* jdbc_table =
            static_cast<const JdbcTableDescriptor*>(_tuple_desc->table_desc());
    if (jdbc_table == nullptr) {
        return Status::InternalError("jdbc table pointer is NULL of VJdbcScanNode::prepare.");
    }
    _jdbc_param.driver_class = jdbc_table->jdbc_driver_class();
    _jdbc_param.driver_path = jdbc_table->jdbc_driver_url();
    _jdbc_param.resource_name = jdbc_table->jdbc_resource_name();
    _jdbc_param.driver_checksum = jdbc_table->jdbc_driver_checksum();
    _jdbc_param.jdbc_url = jdbc_table->jdbc_url();
    _jdbc_param.user = jdbc_table->jdbc_user();
    _jdbc_param.passwd = jdbc_table->jdbc_passwd();
    _jdbc_param.tuple_desc = _tuple_desc;
    _jdbc_param.query_string = std::move(_query_string);

    _jdbc_connector.reset(new (std::nothrow) JdbcConnector(_jdbc_param));
    if (_jdbc_connector == nullptr) {
        return Status::InternalError("new a jdbc scanner failed.");
    }

    _is_init = true;
    return Status::OK();
}

Status NewJdbcScanner::open(RuntimeState* state) {
    VLOG_CRITICAL << "NewJdbcScanner::open";
    if (state == nullptr) {
        return Status::InternalError("input pointer is NULL of VJdbcScanNode::open.");
    }

    if (!_is_init) {
        return Status::InternalError("used before initialize of VJdbcScanNode::open.");
    }
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(VScanner::open(state));
    RETURN_IF_ERROR(_jdbc_connector->open(state, true));
    RETURN_IF_ERROR(_jdbc_connector->query());
    return Status::OK();
}

Status NewJdbcScanner::_get_block_impl(RuntimeState* state, Block* block, bool* eof) {
    VLOG_CRITICAL << "NewJdbcScanner::_get_block_impl";
    if (nullptr == state || nullptr == block || nullptr == eof) {
        return Status::InternalError("input is NULL pointer");
    }

    if (!_is_init) {
        return Status::InternalError("used before initialize of VJdbcScanNode::get_next.");
    }

    if (_jdbc_eos == true) {
        *eof = true;
        return Status::OK();
    }

    auto column_size = _tuple_desc->slots().size();
    std::vector<MutableColumnPtr> columns(column_size);
    bool mem_reuse = block->mem_reuse();
    // only empty block should be here
    DCHECK(block->rows() == 0);

    do {
        RETURN_IF_CANCELLED(state);

        columns.resize(column_size);
        for (auto i = 0; i < column_size; i++) {
            if (mem_reuse) {
                columns[i] = std::move(*block->get_by_position(i).column).mutate();
            } else {
                columns[i] = _tuple_desc->slots()[i]->get_empty_mutable_column();
            }
        }

        RETURN_IF_ERROR(_jdbc_connector->get_next(&_jdbc_eos, columns, state->batch_size()));

        if (_jdbc_eos == true) {
            if (block->rows() == 0) {
                *eof = true;
            }
            break;
        }

        // Before really use the Block, must clear other ptr of column in block
        // So here need do std::move and clear in `columns`
        if (!mem_reuse) {
            int column_index = 0;
            for (const auto slot_desc : _tuple_desc->slots()) {
                block->insert(ColumnWithTypeAndName(std::move(columns[column_index++]),
                                                    slot_desc->get_data_type_ptr(),
                                                    slot_desc->col_name()));
            }
        } else {
            columns.clear();
        }
        VLOG_ROW << "NewJdbcScanNode output rows: " << block->rows();
    } while (block->rows() == 0 && !(*eof));
    return Status::OK();
}

Status NewJdbcScanner::close(RuntimeState* state) {
    RETURN_IF_ERROR(VScanner::close(state));
    RETURN_IF_ERROR(_jdbc_connector->close());
    return Status::OK();
}
} // namespace doris::vectorized
