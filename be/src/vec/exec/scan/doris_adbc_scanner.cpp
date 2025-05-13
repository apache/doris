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

#include "vec/exec/scan/doris_adbc_scanner.h"

#include <algorithm>
#include <ostream>
#include <utility>

#include "common/logging.h"
#include "pipeline/exec/doris_adbc_scan_operator.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"
#include "vec/columns/column.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"

namespace doris {
namespace vectorized {
class VExprContext;
class VScanNode;
} // namespace vectorized
} // namespace doris

static const std::string NEW_SCANNER_TYPE = "NewDorisAdbcScanner";

namespace doris::vectorized {

NewDorisAdbcScanner::NewDorisAdbcScanner(RuntimeState* state, pipeline::ScanLocalStateBase* local_state,
                           int64_t limit, TupleId tuple_id, const std::string& location_uri,
                           const std::string& ticket,
                           RuntimeProfile* profile)
        : Scanner(state, local_state, limit, profile),
          _doris_eof(false),
          _location_uri(location_uri),
          _ticket(ticket),
          _tuple_id(tuple_id),
          _tuple_desc(nullptr) {
    _is_init = false;
}

Status NewDorisAdbcScanner::prepare(RuntimeState* state, const VExprContextSPtrs& conjuncts) {
    VLOG_CRITICAL << NEW_SCANNER_TYPE << "::prepare";
    RETURN_IF_ERROR(Scanner::prepare(_state, conjuncts));

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

    const DorisTableDescriptor* doris_table =
            static_cast<const DorisTableDescriptor*>(_tuple_desc->table_desc());
    if (doris_table == nullptr) {
        return Status::InternalError("doris table pointer is NULL.");
    }
    _doris_arrow_param.catalog_id = doris_table->catalog_id();
    _doris_arrow_param.user = doris_table->user_name();
    _doris_arrow_param.password = doris_table->passwd();
    _doris_arrow_param.fe_arrow_nodes = &doris_table->fe_arrow_nodes();
    _doris_arrow_param.tuple_desc = _tuple_desc;
    _doris_arrow_param.location_uri = std::move(_location_uri);
    _doris_arrow_param.ticket = std::move(_ticket);

    // TODO later changed to c++ arrow api
    if (_doris_arrow_param.fe_arrow_nodes->empty()) {
        return Status::InternalError("doris fe arrow nodes is empty.");
    }
    _jdbc_param.tuple_desc = _tuple_desc;
    _jdbc_param.use_transaction = false;
    _jdbc_param.table_type = TOdbcTableType::DORIS;
    _jdbc_param.user = _doris_arrow_param.user;
    _jdbc_param.passwd = _doris_arrow_param.password;
    _jdbc_param.arrow_host_port = (*_doris_arrow_param.fe_arrow_nodes)[0];
    _jdbc_param.ticket = _doris_arrow_param.ticket;
    _jdbc_param.location_uri = _doris_arrow_param.location_uri;
    _jdbc_connector.reset(new (std::nothrow) JdbcConnector(_jdbc_param));
    if (_jdbc_connector == nullptr) {
        return Status::InternalError("new a jdbc(adbc) scanner failed.");
    }

    _is_init = true;
    return Status::OK();
}

Status NewDorisAdbcScanner::open(RuntimeState* state) {
    VLOG_CRITICAL << NEW_SCANNER_TYPE << "::open";

    if (nullptr == state) {
        return Status::InternalError("input pointer is null.");
    }

    if (!_is_init) {
        return Status::InternalError("used before initialize.");
    }

    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(Scanner::open(state));
    RETURN_IF_ERROR(_jdbc_connector->open(state, true));
    RETURN_IF_ERROR(_jdbc_connector->query());
    return Status::OK();
}

Status NewDorisAdbcScanner::_get_block_impl(RuntimeState* state, Block* block, bool* eof) {
    VLOG_CRITICAL << NEW_SCANNER_TYPE << "::_get_block_impl";
    if (nullptr == state || nullptr == block || nullptr == eof) {
        return Status::InternalError("input is NULL pointer");
    }

    if (!_is_init) {
        return Status::InternalError("used before initialize.");
    }

    RETURN_IF_CANCELLED(state);

    if (_doris_eof == true) {
        *eof = true;
        return Status::OK();
    }

    auto column_size = _tuple_desc->slots().size();
    std::vector<MutableColumnPtr> columns(column_size);

    bool mem_reuse = block->mem_reuse();
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

        // todo 当前scanner具有以下属性，可用来获取adbc结果
        // location_uri: 远端be节点地址，用于构建Location
        std::string location_uri = _doris_arrow_param.location_uri;
        // ticket: 数据集的唯一标识，用于构建Ticket
        std::string ticket = _doris_arrow_param.ticket;
        // user: 鉴权时可使用
        std::string user = _doris_arrow_param.user;
        // password: 鉴权时可使用
        std::string password = _doris_arrow_param.password;
        // fe_arrow_nodes: 远端目标集群fe_ip及对应arrow_port，鉴权时可使用
        std::vector<std::string> fe_arrow_nodes = *_doris_arrow_param.fe_arrow_nodes;
        LOG(INFO) << "doris arrow flight params: " << location_uri << "," << ticket << "," << user << ","
                  << password << "," << fe_arrow_nodes;

        // TODO later changed to c++ arrow api
        RETURN_IF_ERROR(_jdbc_connector->get_next(&_doris_eof, block, state->batch_size()));

        if (_doris_eof == true) {
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
        VLOG_ROW << "NewDorisAdbcScanner output rows: " << block->rows();
    } while (block->rows() == 0 && !(*eof));
    return Status::OK();
}

Status NewDorisAdbcScanner::close(RuntimeState* state) {
    if (_is_closed) {
        return Status::OK();
    }

    RETURN_IF_ERROR(Scanner::close(state));
    RETURN_IF_ERROR(_jdbc_connector->close());
    return Status::OK();
}
} // namespace doris::vectorized
