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

#include "jdbc_scanner.h"

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
#include "vec/exec/vjdbc_connector.h"
#include "vec/exprs/vexpr_context.h"

namespace doris::vectorized {

JdbcScanner::JdbcScanner(RuntimeState* state, doris::pipeline::JDBCScanLocalState* local_state,
                         int64_t limit, const TupleId& tuple_id, const std::string& query_string,
                         TOdbcTableType::type table_type, bool is_tvf, RuntimeProfile* profile)
        : Scanner(state, local_state, limit, profile),
          _jdbc_eos(false),
          _tuple_id(tuple_id),
          _query_string(query_string),
          _tuple_desc(nullptr),
          _table_type(table_type),
          _is_tvf(is_tvf) {
    _init_profile(local_state->_scanner_profile);
    _has_prepared = false;
}

Status JdbcScanner::init(RuntimeState* state, const VExprContextSPtrs& conjuncts) {
    VLOG_CRITICAL << "JdbcScanner::init";
    RETURN_IF_ERROR(Scanner::init(state, conjuncts));

    if (state == nullptr) {
        return Status::InternalError("input pointer is NULL of VJdbcScanNode::init.");
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
        return Status::InternalError("jdbc table pointer is NULL of VJdbcScanNode::init.");
    }
    _jdbc_param.catalog_id = jdbc_table->jdbc_catalog_id();
    _jdbc_param.driver_class = jdbc_table->jdbc_driver_class();
    _jdbc_param.driver_path = jdbc_table->jdbc_driver_url();
    _jdbc_param.resource_name = jdbc_table->jdbc_resource_name();
    _jdbc_param.driver_checksum = jdbc_table->jdbc_driver_checksum();
    _jdbc_param.jdbc_url = jdbc_table->jdbc_url();
    _jdbc_param.user = jdbc_table->jdbc_user();
    _jdbc_param.passwd = jdbc_table->jdbc_passwd();
    _jdbc_param.tuple_desc = _tuple_desc;
    _jdbc_param.query_string = std::move(_query_string);
    _jdbc_param.use_transaction = false; // not useful for scanner but only sink.
    _jdbc_param.table_type = _table_type;
    _jdbc_param.is_tvf = _is_tvf;
    _jdbc_param.connection_pool_min_size = jdbc_table->connection_pool_min_size();
    _jdbc_param.connection_pool_max_size = jdbc_table->connection_pool_max_size();
    _jdbc_param.connection_pool_max_life_time = jdbc_table->connection_pool_max_life_time();
    _jdbc_param.connection_pool_max_wait_time = jdbc_table->connection_pool_max_wait_time();
    _jdbc_param.connection_pool_keep_alive = jdbc_table->connection_pool_keep_alive();

    _local_state->scanner_profile()->add_info_string("JdbcDriverClass", _jdbc_param.driver_class);
    _local_state->scanner_profile()->add_info_string("JdbcDriverUrl", _jdbc_param.driver_path);
    _local_state->scanner_profile()->add_info_string("JdbcUrl", _jdbc_param.jdbc_url);
    _local_state->scanner_profile()->add_info_string("QuerySql", _jdbc_param.query_string);

    _jdbc_connector.reset(new (std::nothrow) JdbcConnector(_jdbc_param));
    if (_jdbc_connector == nullptr) {
        return Status::InternalError("new a jdbc scanner failed.");
    }

    return Status::OK();
}

Status JdbcScanner::open(RuntimeState* state) {
    VLOG_CRITICAL << "JdbcScanner::open";
    if (state == nullptr) {
        return Status::InternalError("input pointer is NULL of VJdbcScanNode::open.");
    }

    if (!_has_prepared) {
        return Status::InternalError("used before initialize of VJdbcScanNode::open.");
    }
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(Scanner::open(state));
    RETURN_IF_ERROR(_jdbc_connector->open(state, true));
    RETURN_IF_ERROR(_jdbc_connector->query());
    return Status::OK();
}

Status JdbcScanner::_get_block_impl(RuntimeState* state, Block* block, bool* eof) {
    VLOG_CRITICAL << "JdbcScanner::_get_block_impl";
    if (nullptr == state || nullptr == block || nullptr == eof) {
        return Status::InternalError("input is NULL pointer");
    }

    if (!_has_prepared) {
        return Status::InternalError("used before initialize of VJdbcScanNode::get_next.");
    }

    if (_jdbc_eos == true) {
        *eof = true;
        _update_profile();
        return Status::OK();
    }

    // only empty block should be here
    DCHECK(block->rows() == 0);

    do {
        RETURN_IF_CANCELLED(state);

        RETURN_IF_ERROR(_jdbc_connector->get_next(&_jdbc_eos, block, state->batch_size()));

        if (_jdbc_eos == true) {
            if (block->rows() == 0) {
                _update_profile();
                *eof = true;
            }
            break;
        }

        VLOG_ROW << "NewJdbcScanNode output rows: " << block->rows();
    } while (block->rows() == 0 && !(*eof));
    return Status::OK();
}

void JdbcScanner::_init_profile(const std::shared_ptr<RuntimeProfile>& profile) {
    _load_jar_timer = ADD_TIMER(profile, "LoadJarTime");
    _init_connector_timer = ADD_TIMER(profile, "InitConnectorTime");
    _check_type_timer = ADD_TIMER(profile, "CheckTypeTime");
    _get_data_timer = ADD_TIMER(profile, "GetDataTime");
    _read_and_fill_vector_table_timer =
            ADD_CHILD_TIMER(profile, "ReadAndFillVectorTableTime", "GetDataTime");
    _jni_setup_timer = ADD_CHILD_TIMER(profile, "JniSetupTime", "GetDataTime");
    _has_next_timer = ADD_CHILD_TIMER(profile, "HasNextTime", "GetDataTime");
    _prepare_params_timer = ADD_CHILD_TIMER(profile, "PrepareParamsTime", "GetDataTime");
    _fill_block_timer = ADD_CHILD_TIMER(profile, "FillBlockTime", "GetDataTime");
    _cast_timer = ADD_CHILD_TIMER(profile, "CastTime", "GetDataTime");
    _execte_read_timer = ADD_TIMER(profile, "ExecteReadTime");
    _connector_close_timer = ADD_TIMER(profile, "ConnectorCloseTime");
}

void JdbcScanner::_update_profile() {
    JdbcConnector::JdbcStatistic& jdbc_statistic = _jdbc_connector->get_jdbc_statistic();
    COUNTER_UPDATE(_load_jar_timer, jdbc_statistic._load_jar_timer);
    COUNTER_UPDATE(_init_connector_timer, jdbc_statistic._init_connector_timer);
    COUNTER_UPDATE(_check_type_timer, jdbc_statistic._check_type_timer);
    COUNTER_UPDATE(_get_data_timer, jdbc_statistic._get_data_timer);
    COUNTER_UPDATE(_jni_setup_timer, jdbc_statistic._jni_setup_timer);
    COUNTER_UPDATE(_has_next_timer, jdbc_statistic._has_next_timer);
    COUNTER_UPDATE(_prepare_params_timer, jdbc_statistic._prepare_params_timer);
    COUNTER_UPDATE(_read_and_fill_vector_table_timer,
                   jdbc_statistic._read_and_fill_vector_table_timer);
    COUNTER_UPDATE(_fill_block_timer, jdbc_statistic._fill_block_timer);
    COUNTER_UPDATE(_cast_timer, jdbc_statistic._cast_timer);
    COUNTER_UPDATE(_execte_read_timer, jdbc_statistic._execte_read_timer);
    COUNTER_UPDATE(_connector_close_timer, jdbc_statistic._connector_close_timer);
}

Status JdbcScanner::close(RuntimeState* state) {
    RETURN_IF_ERROR(Scanner::close(state));
    RETURN_IF_ERROR(_jdbc_connector->close());
    return Status::OK();
}
} // namespace doris::vectorized
