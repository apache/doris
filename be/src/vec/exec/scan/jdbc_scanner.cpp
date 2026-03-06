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
#include "util/jdbc_utils.h"
#include "util/runtime_profile.h"
#include "vec/core/block.h"
#include "vec/exec/format/table/jdbc_jni_reader.h"
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
    _has_prepared = false;
}

std::map<std::string, std::string> JdbcScanner::_build_jdbc_params(
        const TupleDescriptor* tuple_desc) {
    const JdbcTableDescriptor* jdbc_table =
            static_cast<const JdbcTableDescriptor*>(tuple_desc->table_desc());

    std::map<std::string, std::string> params;
    params["jdbc_url"] = jdbc_table->jdbc_url();
    params["jdbc_user"] = jdbc_table->jdbc_user();
    params["jdbc_password"] = jdbc_table->jdbc_passwd();
    params["jdbc_driver_class"] = jdbc_table->jdbc_driver_class();
    // Resolve jdbc_driver_url to absolute file:// URL
    // FE sends just the JAR filename; we need to resolve it to a full path.
    std::string driver_url;
    auto resolve_st = JdbcUtils::resolve_driver_url(jdbc_table->jdbc_driver_url(), &driver_url);
    if (!resolve_st.ok()) {
        LOG(WARNING) << "Failed to resolve JDBC driver URL: " << resolve_st.to_string();
        driver_url = jdbc_table->jdbc_driver_url();
    }
    params["jdbc_driver_url"] = driver_url;
    params["query_sql"] = _query_string;
    params["catalog_id"] = std::to_string(jdbc_table->jdbc_catalog_id());
    params["table_type"] = _odbc_table_type_to_string(_table_type);
    params["connection_pool_min_size"] = std::to_string(jdbc_table->connection_pool_min_size());
    params["connection_pool_max_size"] = std::to_string(jdbc_table->connection_pool_max_size());
    params["connection_pool_max_wait_time"] =
            std::to_string(jdbc_table->connection_pool_max_wait_time());
    params["connection_pool_max_life_time"] =
            std::to_string(jdbc_table->connection_pool_max_life_time());
    params["connection_pool_keep_alive"] =
            jdbc_table->connection_pool_keep_alive() ? "true" : "false";
    return params;
}

Status JdbcScanner::init(RuntimeState* state, const VExprContextSPtrs& conjuncts) {
    VLOG_CRITICAL << "JdbcScanner::init";
    RETURN_IF_ERROR(Scanner::init(state, conjuncts));

    if (state == nullptr) {
        return Status::InternalError("input pointer is NULL of JdbcScanner::init.");
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
        return Status::InternalError("jdbc table pointer is NULL of JdbcScanner::init.");
    }

    _local_state->scanner_profile()->add_info_string("JdbcDriverClass",
                                                     jdbc_table->jdbc_driver_class());
    _local_state->scanner_profile()->add_info_string("JdbcDriverUrl",
                                                     jdbc_table->jdbc_driver_url());
    _local_state->scanner_profile()->add_info_string("JdbcUrl", jdbc_table->jdbc_url());
    _local_state->scanner_profile()->add_info_string("QuerySql", _query_string);

    // Build reader params from tuple descriptor
    auto jdbc_params = _build_jdbc_params(_tuple_desc);

    // Get slot descriptors for the reader
    const auto& slots = _tuple_desc->slots();
    std::vector<SlotDescriptor*> slot_descs(slots.begin(), slots.end());

    _jni_reader = JdbcJniReader::create_unique(slot_descs, state, _profile, jdbc_params);

    return Status::OK();
}

Status JdbcScanner::_open_impl(RuntimeState* state) {
    VLOG_CRITICAL << "JdbcScanner::open";
    if (state == nullptr) {
        return Status::InternalError("input pointer is NULL of JdbcScanner::open.");
    }

    if (!_has_prepared) {
        return Status::InternalError("used before initialize of JdbcScanner::open.");
    }
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(Scanner::_open_impl(state));
    RETURN_IF_ERROR(_jni_reader->init_reader());
    return Status::OK();
}

Status JdbcScanner::_get_block_impl(RuntimeState* state, Block* block, bool* eof) {
    VLOG_CRITICAL << "JdbcScanner::_get_block_impl";
    if (nullptr == state || nullptr == block || nullptr == eof) {
        return Status::InternalError("input is NULL pointer");
    }

    if (!_has_prepared) {
        return Status::InternalError("used before initialize of JdbcScanner::get_next.");
    }

    if (_jdbc_eos) {
        *eof = true;
        return Status::OK();
    }

    // only empty block should be here
    DCHECK(block->rows() == 0);

    do {
        RETURN_IF_CANCELLED(state);

        size_t read_rows = 0;
        bool reader_eof = false;
        RETURN_IF_ERROR(_jni_reader->get_next_block(block, &read_rows, &reader_eof));

        if (reader_eof) {
            _jdbc_eos = true;
            if (block->rows() == 0) {
                *eof = true;
            }
            break;
        }

        VLOG_ROW << "JdbcScanner output rows: " << block->rows();
    } while (block->rows() == 0 && !(*eof));
    return Status::OK();
}

Status JdbcScanner::close(RuntimeState* state) {
    if (!_try_close()) {
        return Status::OK();
    }
    RETURN_IF_ERROR(Scanner::close(state));
    if (_jni_reader) {
        RETURN_IF_ERROR(_jni_reader->close());
    }
    return Status::OK();
}
} // namespace doris::vectorized
