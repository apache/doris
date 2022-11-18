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

#include "vec/exec/scan/new_es_scanner.h"

#include "vec/exec/scan/new_es_scan_node.h"

static const std::string NEW_SCANNER_TYPE = "NewEsScanner";

namespace doris::vectorized {

NewEsScanner::NewEsScanner(RuntimeState* state, NewEsScanNode* parent, int64_t limit,
                           TupleId tuple_id, const std::map<std::string, std::string>& properties,
                           const std::map<std::string, std::string>& docvalue_context,
                           bool doc_value_mode)
        : VScanner(state, static_cast<VScanNode*>(parent), limit),
          _is_init(false),
          _es_eof(false),
          _properties(properties),
          _line_eof(false),
          _batch_eof(false),
          _tuple_id(tuple_id),
          _tuple_desc(nullptr),
          _mem_pool(nullptr),
          _es_reader(nullptr),
          _es_scroll_parser(nullptr),
          _docvalue_context(docvalue_context),
          _doc_value_mode(doc_value_mode) {}

Status NewEsScanner::prepare(RuntimeState* state, VExprContext** vconjunct_ctx_ptr) {
    VLOG_CRITICAL << NEW_SCANNER_TYPE << "::prepare";
    if (vconjunct_ctx_ptr != nullptr) {
        // Copy vconjunct_ctx_ptr from scan node to this scanner's _vconjunct_ctx.
        RETURN_IF_ERROR((*vconjunct_ctx_ptr)->clone(_state, &_vconjunct_ctx));
    }

    if (_is_init) {
        return Status::OK();
    }

    if (nullptr == state) {
        return Status::InternalError("input pointer is null.");
    }

    _tuple_desc = state->desc_tbl().get_tuple_descriptor(_tuple_id);
    if (nullptr == _tuple_desc) {
        return Status::InternalError("Failed to get tuple descriptor, tuple_id={}", _tuple_id);
    }

    const std::string& host = _properties.at(ESScanReader::KEY_HOST_PORT);
    _es_reader.reset(new ESScanReader(host, _properties, _doc_value_mode));
    if (_es_reader == nullptr) {
        return Status::InternalError("Es reader construct failed.");
    }

    _is_init = true;
    return Status::OK();
}

Status NewEsScanner::open(RuntimeState* state) {
    VLOG_CRITICAL << NEW_SCANNER_TYPE << "::open";

    if (nullptr == state) {
        return Status::InternalError("input pointer is null.");
    }

    if (!_is_init) {
        return Status::InternalError("used before initialize.");
    }

    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(VScanner::open(state));

    RETURN_IF_ERROR(_es_reader->open());
    _mem_pool.reset(new MemPool());

    return Status::OK();
}

Status NewEsScanner::_get_block_impl(RuntimeState* state, Block* block, bool* eof) {
    VLOG_CRITICAL << NEW_SCANNER_TYPE << "::_get_block_impl";
    if (nullptr == state || nullptr == block || nullptr == eof) {
        return Status::InternalError("input is NULL pointer");
    }

    if (!_is_init) {
        return Status::InternalError("used before initialize.");
    }

    RETURN_IF_CANCELLED(state);

    if (_es_eof == true) {
        *eof = true;
        return Status::OK();
    }

    auto column_size = _tuple_desc->slots().size();
    std::vector<MutableColumnPtr> columns(column_size);

    bool mem_reuse = block->mem_reuse();
    const int batch_size = state->batch_size();
    // only empty block should be here
    DCHECK(block->rows() == 0);

    do {
        columns.resize(column_size);
        for (auto i = 0; i < column_size; i++) {
            if (mem_reuse) {
                columns[i] = std::move(*block->get_by_position(i).column).mutate();
            } else {
                columns[i] = _tuple_desc->slots()[i]->get_empty_mutable_column();
            }
        }

        while (columns[0]->size() < batch_size && !_es_eof) {
            RETURN_IF_CANCELLED(state);
            // Get from scanner
            RETURN_IF_ERROR(_get_next(columns));
        }

        if (_es_eof == true) {
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
        VLOG_ROW << "NewEsScanner output rows: " << block->rows();
    } while (block->rows() == 0 && !(*eof));
    return Status::OK();
}

Status NewEsScanner::_get_next(std::vector<vectorized::MutableColumnPtr>& columns) {
    NewEsScanNode* new_es_scan_node = static_cast<NewEsScanNode*>(_parent);
    SCOPED_TIMER(new_es_scan_node->_read_timer);
    if (_line_eof && _batch_eof) {
        _es_eof = true;
        return Status::OK();
    }

    while (!_batch_eof) {
        if (_line_eof || _es_scroll_parser == nullptr) {
            RETURN_IF_ERROR(_es_reader->get_next(&_batch_eof, _es_scroll_parser));
            if (_batch_eof) {
                _es_eof = true;
                return Status::OK();
            }
        }

        COUNTER_UPDATE(new_es_scan_node->_rows_read_counter, 1);
        SCOPED_TIMER(new_es_scan_node->_materialize_timer);
        RETURN_IF_ERROR(_es_scroll_parser->fill_columns(_tuple_desc, columns, _mem_pool.get(),
                                                        &_line_eof, _docvalue_context));
        if (!_line_eof) {
            break;
        }
    }

    return Status::OK();
}

Status NewEsScanner::close(RuntimeState* state) {
    if (_is_closed) {
        return Status::OK();
    }

    if (_es_reader != nullptr) {
        _es_reader->close();
    }

    RETURN_IF_ERROR(VScanner::close(state));
    return Status::OK();
}
} // namespace doris::vectorized
