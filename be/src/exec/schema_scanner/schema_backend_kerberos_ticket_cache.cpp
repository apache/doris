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

#include "exec/schema_scanner/schema_backend_kerberos_ticket_cache.h"

#include "common/kerberos/kerberos_ticket_mgr.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_factory.hpp"

namespace doris {
#include "common/compile_check_begin.h"

std::vector<SchemaScanner::ColumnDesc> SchemaBackendKerberosTicketCacheScanner::_s_tbls_columns = {
        //   name,       type,          size
        {"BE_ID", TYPE_BIGINT, sizeof(int64_t), true},
        {"BE_IP", TYPE_STRING, sizeof(StringRef), true},
        {"PRINCIPAL", TYPE_STRING, sizeof(StringRef), true},
        {"KEYTAB", TYPE_STRING, sizeof(StringRef), true},
        {"SERVICE_PRINCIPAL", TYPE_STRING, sizeof(StringRef), true},
        {"TICKET_CACHE_PATH", TYPE_STRING, sizeof(StringRef), true},
        {"HASH_CODE", TYPE_STRING, sizeof(StringRef), true},
        {"START_TIME", TYPE_DATETIME, sizeof(int128_t), true},
        {"EXPIRE_TIME", TYPE_DATETIME, sizeof(int128_t), true},
        {"AUTH_TIME", TYPE_DATETIME, sizeof(int128_t), true},
        {"REF_COUNT", TYPE_BIGINT, sizeof(int64_t), true},
        {"REFRESH_INTERVAL_SECOND", TYPE_BIGINT, sizeof(int64_t), true}};

SchemaBackendKerberosTicketCacheScanner::SchemaBackendKerberosTicketCacheScanner()
        : SchemaScanner(_s_tbls_columns, TSchemaTableType::SCH_BACKEND_KERBEROS_TICKET_CACHE) {}

SchemaBackendKerberosTicketCacheScanner::~SchemaBackendKerberosTicketCacheScanner() {}

Status SchemaBackendKerberosTicketCacheScanner::start(RuntimeState* state) {
    _block_rows_limit = state->batch_size();
    return Status::OK();
}

Status SchemaBackendKerberosTicketCacheScanner::get_next_block_internal(vectorized::Block* block,
                                                                        bool* eos) {
    if (!_is_init) {
        return Status::InternalError("Used before initialized.");
    }

    if (nullptr == block || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }

    if (_info_block == nullptr) {
        _info_block = vectorized::Block::create_unique();

        for (int i = 0; i < _s_tbls_columns.size(); ++i) {
            auto data_type = vectorized::DataTypeFactory::instance().create_data_type(
                    _s_tbls_columns[i].type, true);
            _info_block->insert(vectorized::ColumnWithTypeAndName(
                    data_type->create_column(), data_type, _s_tbls_columns[i].name));
        }

        _info_block->reserve(_block_rows_limit);

        ExecEnv::GetInstance()->kerberos_ticket_mgr()->get_ticket_cache_info_block(
                _info_block.get(), _timezone_obj);
        _total_rows = (int)_info_block->rows();
    }

    if (_row_idx == _total_rows) {
        *eos = true;
        return Status::OK();
    }

    int current_batch_rows = std::min(_block_rows_limit, _total_rows - _row_idx);
    vectorized::MutableBlock mblock = vectorized::MutableBlock::build_mutable_block(block);
    RETURN_IF_ERROR(mblock.add_rows(_info_block.get(), _row_idx, current_batch_rows));
    _row_idx += current_batch_rows;

    *eos = _row_idx == _total_rows;
    return Status::OK();
}

} // namespace doris
