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

#include <new>
#include <ostream>
#include <utility>
#include <vector>

#include "common/logging.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"
#include "vec/columns/column.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/data_types/data_type.h"
#include "vec/exec/scan/new_jdbc_scan_node.h"
#include "vec/exec/scan/vscan_node.h"
#include "vec/exec/vjdbc_connector.h"
#include "vec/exprs/vexpr_context.h"

namespace doris::vectorized {
NewJdbcScanner::NewJdbcScanner(RuntimeState* state, NewJdbcScanNode* parent, int64_t limit,
                               const TupleId& tuple_id, const std::string& query_string,
                               TOdbcTableType::type table_type, RuntimeProfile* profile)
        : VScanner(state, static_cast<VScanNode*>(parent), limit, profile),
          _jdbc_eos(false),
          _tuple_id(tuple_id),
          _query_string(query_string),
          _tuple_desc(nullptr),
          _table_type(table_type) {
    _is_init = false;
    _load_jar_timer = ADD_TIMER(get_parent()->_scanner_profile, "LoadJarTime");
    _init_connector_timer = ADD_TIMER(get_parent()->_scanner_profile, "InitConnectorTime");
    _check_type_timer = ADD_TIMER(get_parent()->_scanner_profile, "CheckTypeTime");
    _get_data_timer = ADD_TIMER(get_parent()->_scanner_profile, "GetDataTime");
    _call_jni_next_timer =
            ADD_CHILD_TIMER(get_parent()->_scanner_profile, "CallJniNextTime", "GetDataTime");
    _convert_batch_timer =
            ADD_CHILD_TIMER(get_parent()->_scanner_profile, "ConvertBatchTime", "GetDataTime");
    _execte_read_timer = ADD_TIMER(get_parent()->_scanner_profile, "ExecteReadTime");
    _connector_close_timer = ADD_TIMER(get_parent()->_scanner_profile, "ConnectorCloseTime");
}

NewJdbcScanner::NewJdbcScanner(RuntimeState* state,
                               doris::pipeline::JDBCScanLocalState* local_state, int64_t limit,
                               const TupleId& tuple_id, const std::string& query_string,
                               TOdbcTableType::type table_type, RuntimeProfile* profile)
        : VScanner(state, local_state, limit, profile),
          _jdbc_eos(false),
          _tuple_id(tuple_id),
          _query_string(query_string),
          _tuple_desc(nullptr),
          _table_type(table_type) {
    _is_init = false;
    _load_jar_timer = ADD_TIMER(local_state->_scanner_profile, "LoadJarTime");
    _init_connector_timer = ADD_TIMER(local_state->_scanner_profile, "InitConnectorTime");
    _check_type_timer = ADD_TIMER(local_state->_scanner_profile, "CheckTypeTime");
    _get_data_timer = ADD_TIMER(local_state->_scanner_profile, "GetDataTime");
    _call_jni_next_timer =
            ADD_CHILD_TIMER(local_state->_scanner_profile, "CallJniNextTime", "GetDataTime");
    _convert_batch_timer =
            ADD_CHILD_TIMER(local_state->_scanner_profile, "ConvertBatchTime", "GetDataTime");
    _execte_read_timer = ADD_TIMER(local_state->_scanner_profile, "ExecteReadTime");
    _connector_close_timer = ADD_TIMER(local_state->_scanner_profile, "ConnectorCloseTime");
}

Status NewJdbcScanner::prepare(RuntimeState* state, const VExprContextSPtrs& conjuncts) {
    VLOG_CRITICAL << "NewJdbcScanner::Prepare";
    RETURN_IF_ERROR(VScanner::prepare(state, conjuncts));

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
    _jdbc_param.table_type = _table_type;

    if (get_parent() != nullptr) {
        get_parent()->_scanner_profile->add_info_string("JdbcDriverClass",
                                                        _jdbc_param.driver_class);
        get_parent()->_scanner_profile->add_info_string("JdbcDriverUrl", _jdbc_param.driver_path);
        get_parent()->_scanner_profile->add_info_string("JdbcUrl", _jdbc_param.jdbc_url);
        get_parent()->_scanner_profile->add_info_string("QuerySql", _jdbc_param.query_string);
    } else { //pipelineX
        _local_state->scanner_profile()->add_info_string("JdbcDriverClass",
                                                         _jdbc_param.driver_class);
        _local_state->scanner_profile()->add_info_string("JdbcDriverUrl", _jdbc_param.driver_path);
        _local_state->scanner_profile()->add_info_string("JdbcUrl", _jdbc_param.jdbc_url);
        _local_state->scanner_profile()->add_info_string("QuerySql", _jdbc_param.query_string);
    }

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
        _update_profile();
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

        RETURN_IF_ERROR(_jdbc_connector->get_next(&_jdbc_eos, columns, block, state->batch_size()));

        if (_jdbc_eos == true) {
            if (block->rows() == 0) {
                _update_profile();
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

void NewJdbcScanner::_update_profile() {
    JdbcConnector::JdbcStatistic& jdbc_statistic = _jdbc_connector->get_jdbc_statistic();
    COUNTER_UPDATE(_load_jar_timer, jdbc_statistic._load_jar_timer);
    COUNTER_UPDATE(_init_connector_timer, jdbc_statistic._init_connector_timer);
    COUNTER_UPDATE(_check_type_timer, jdbc_statistic._check_type_timer);
    COUNTER_UPDATE(_get_data_timer, jdbc_statistic._get_data_timer);
    COUNTER_UPDATE(_call_jni_next_timer, jdbc_statistic._call_jni_next_timer);
    COUNTER_UPDATE(_convert_batch_timer, jdbc_statistic._convert_batch_timer);
    COUNTER_UPDATE(_execte_read_timer, jdbc_statistic._execte_read_timer);
    COUNTER_UPDATE(_connector_close_timer, jdbc_statistic._connector_close_timer);
}

Status NewJdbcScanner::close(RuntimeState* state) {
    RETURN_IF_ERROR(VScanner::close(state));
    RETURN_IF_ERROR(_jdbc_connector->close());
    return Status::OK();
}
} // namespace doris::vectorized
