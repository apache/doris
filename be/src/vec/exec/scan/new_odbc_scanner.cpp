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

#include "vec/exec/scan/new_odbc_scanner.h"

#include <gen_cpp/PlanNodes_types.h>
#include <sql.h>

#include <algorithm>
#include <new>
#include <ostream>
#include <utility>
#include <vector>

#include "common/logging.h"
#include "common/status.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"
#include "vec/columns/column.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/data_types/data_type.h"
#include "vec/exec/scan/new_odbc_scan_node.h"
#include "vec/exec/scan/vscanner.h"
#include "vec/exprs/vexpr_context.h"

namespace doris {
class RuntimeProfile;
namespace vectorized {
class VScanNode;
} // namespace vectorized
} // namespace doris

static const std::string NEW_SCANNER_TYPE = "NewOdbcScanner";

namespace doris::vectorized {
NewOdbcScanner::NewOdbcScanner(RuntimeState* state, NewOdbcScanNode* parent, int64_t limit,
                               const TOdbcScanNode& odbc_scan_node, RuntimeProfile* profile)
        : VScanner(state, static_cast<VScanNode*>(parent), limit, profile),
          _odbc_eof(false),
          _table_name(odbc_scan_node.table_name),
          _connect_string(odbc_scan_node.connect_string),
          _query_string(odbc_scan_node.query_string),
          _tuple_id(odbc_scan_node.tuple_id),
          _tuple_desc(nullptr) {
    _is_init = false;
}

Status NewOdbcScanner::prepare(RuntimeState* state, const VExprContextSPtrs& conjuncts) {
    VLOG_CRITICAL << NEW_SCANNER_TYPE << "::prepare";
    RETURN_IF_ERROR(VScanner::prepare(state, conjuncts));

    if (_is_init) {
        return Status::OK();
    }

    if (nullptr == state) {
        return Status::InternalError("input pointer is null.");
    }

    // get tuple desc
    _tuple_desc = state->desc_tbl().get_tuple_descriptor(_tuple_id);

    if (nullptr == _tuple_desc) {
        return Status::InternalError("Failed to get tuple descriptor.");
    }

    _odbc_param.connect_string = std::move(_connect_string);
    _odbc_param.query_string = std::move(_query_string);
    _odbc_param.tuple_desc = _tuple_desc;

    _odbc_connector.reset(new (std::nothrow) ODBCConnector(_odbc_param));

    if (_odbc_connector == nullptr) {
        return Status::InternalError("new a odbc scanner failed.");
    }

    _text_converter.reset(new (std::nothrow) TextConverter('\\'));

    if (_text_converter == nullptr) {
        return Status::InternalError("new a text convertor failed.");
    }

    _is_init = true;

    return Status::OK();
}

Status NewOdbcScanner::open(RuntimeState* state) {
    VLOG_CRITICAL << NEW_SCANNER_TYPE << "::open";

    if (nullptr == state) {
        return Status::InternalError("input pointer is null.");
    }

    if (!_is_init) {
        return Status::InternalError("used before initialize.");
    }

    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(VScanner::open(state));
    RETURN_IF_ERROR(_odbc_connector->open(state));
    RETURN_IF_ERROR(_odbc_connector->query());
    // check materialize slot num

    return Status::OK();
}

Status NewOdbcScanner::_get_block_impl(RuntimeState* state, Block* block, bool* eof) {
    VLOG_CRITICAL << NEW_SCANNER_TYPE << "::_get_block_impl";

    if (nullptr == state || nullptr == block || nullptr == eof) {
        return Status::InternalError("input is NULL pointer");
    }

    if (!_is_init) {
        return Status::InternalError("used before initialize.");
    }
    RETURN_IF_CANCELLED(state);

    if (_odbc_eof == true) {
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

        while (true) {
            // block is full, break
            if (state->batch_size() <= columns[0]->size()) {
                break;
            }

            RETURN_IF_ERROR(_odbc_connector->get_next_row(&_odbc_eof));

            if (_odbc_eof == true) {
                if (block->rows() == 0) {
                    *eof = true;
                }
                break;
            }

            // Read one row from reader
            for (int column_index = 0, materialized_column_index = 0; column_index < column_size;
                 ++column_index) {
                auto slot_desc = _tuple_desc->slots()[column_index];
                // because the fe planner filter the non_materialize column
                if (!slot_desc->is_materialized()) {
                    continue;
                }
                const auto& column_data =
                        _odbc_connector->get_column_data(materialized_column_index);

                char* value_data = static_cast<char*>(column_data.target_value_ptr);
                int value_len = column_data.strlen_or_ind;

                if (value_len == SQL_NULL_DATA) {
                    if (slot_desc->is_nullable()) {
                        columns[column_index]->insert_default();
                    } else {
                        return Status::InternalError(
                                "nonnull column contains nullptr. table={}, column={}", _table_name,
                                slot_desc->col_name());
                    }
                } else if (value_len > column_data.buffer_length) {
                    return Status::InternalError(
                            "column value length longer than buffer length. "
                            "table={}, column={}, buffer_length",
                            _table_name, slot_desc->col_name(), column_data.buffer_length);
                } else {
                    if (!_text_converter->write_column(slot_desc, &columns[column_index],
                                                       value_data, value_len, true, false)) {
                        std::stringstream ss;
                        ss << "Fail to convert odbc value:'" << value_data << "' to "
                           << slot_desc->type() << " on column:`" << slot_desc->col_name() + "`";
                        return Status::InternalError(ss.str());
                    }
                }
                materialized_column_index++;
            }
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
        VLOG_ROW << "NewOdbcScanner output rows: " << block->rows();
    } while (block->rows() == 0 && !(*eof));

    return Status::OK();
}

Status NewOdbcScanner::close(RuntimeState* state) {
    RETURN_IF_ERROR(VScanner::close(state));
    return Status::OK();
}
} // namespace doris::vectorized
