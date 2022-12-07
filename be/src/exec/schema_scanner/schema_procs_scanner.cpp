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

#include "exec/schema_scanner/schema_procs_scanner.h"

#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/FrontendService.h>
#include <gen_cpp/HeartbeatService_types.h>
#include <gen_cpp/Types_types.h>

#include "common/config.h"
#include "common/status.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "runtime/primitive_type.h"
#include "runtime/string_value.h"
#include "util/thrift_rpc_helper.h"

namespace doris {

SchemaScanner::ColumnDesc SchemaProcsScanner::_s_procs_columns[] = {
        //   name,       type,          size,     is_null
        {"FuncName", TYPE_VARCHAR, sizeof(StringValue), false},
        {"RetType", TYPE_VARCHAR, sizeof(StringValue), false},
        {"ArgTypes", TYPE_VARCHAR, sizeof(StringValue), false},
        {"Signature", TYPE_VARCHAR, sizeof(StringValue), false},
};

SchemaProcsScanner::SchemaProcsScanner()
        : SchemaScanner(_s_procs_columns,
                        sizeof(_s_procs_columns) / sizeof(SchemaScanner::ColumnDesc),
                        TSchemaTableType::SCH_PROCS),
          _row_idx(0) {}

SchemaProcsScanner::~SchemaProcsScanner() = default;

Status SchemaProcsScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("used before initialized.");
    }
    RETURN_IF_ERROR(_fetch_procs_info());

    return Status::OK();
}

Status SchemaProcsScanner::fill_one_row(Tuple* tuple, MemPool* pool) {
    // set all bit to not null
    memset((void*)tuple, 0, _tuple_desc->num_null_bytes());
    std::string vals[_column_num];
    {
        // add function name
        vals[0] = std::move(_batch_data[_row_idx].name.function_name);
    }
    {
        // add return type
        vals[1] = thrift_to_string(_batch_data[_row_idx].ret_type.types.data()->scalar_type.type);
    }
    {
        // add arguments type
        std::string args;
        for (auto t : _batch_data[_row_idx].arg_types) {
            args += " " + thrift_to_string(t.types.data()->scalar_type.type);
        }
        vals[2] = std::move(args);
    }
    {
        // add signature
        vals[3] = std::move(_batch_data[_row_idx].signature);
    }
    for (size_t idx = 0; idx < _column_num; idx++) {
        RETURN_IF_ERROR(fill_one_col(&vals[idx], pool,
                                     tuple->get_slot(_tuple_desc->slots()[idx]->tuple_offset())));
    }
    _row_idx++;
    return Status::OK();
}

Status SchemaProcsScanner::fill_one_col(const std::string* src, MemPool* pool, void* slot) {
    if (nullptr == slot || nullptr == pool || nullptr == src) {
        return Status::InternalError("input pointer is nullptr.");
    }
    StringValue* str_slot = reinterpret_cast<StringValue*>(slot);
    str_slot->len = src->length();
    str_slot->ptr = (char*)pool->allocate(str_slot->len);
    if (nullptr == str_slot->ptr) {
        return Status::InternalError("Allocate memcpy failed.");
    }
    memcpy(str_slot->ptr, src->c_str(), str_slot->len);
    return Status::OK();
}

Status SchemaProcsScanner::get_next_row(Tuple* tuple, MemPool* pool, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("Used before initialized.");
    }
    if (nullptr == tuple || nullptr == pool || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }

    if (_row_idx >= _batch_data.size()) {
        *eos = true;
        return Status::OK();
    }

    *eos = false;

    return fill_one_row(tuple, pool);
}

Status SchemaProcsScanner::_fetch_procs_info() {
    TFetchProcsDataRequest request;
    request.schema_table_name = TSchemaTableName::PROCS;
    request.__isset.schema_table_name = true;
    TNetworkAddress master_addr = ExecEnv::GetInstance()->master_info()->network_address;
    TFetchProcsDataResult result;

    RETURN_IF_ERROR(ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&request, &result](FrontendServiceConnection& client) {
                client->fetchProcsData(result, request);
            },
            config::txn_commit_rpc_timeout_ms));

    Status status(result.status);
    if (!status.ok()) {
        LOG(WARNING) << "fetch procs from master failed, errmsg=" << status.get_error_msg();
        return status;
    }

    _batch_data = std::move(result.data_batch);
    return Status::OK();
}

} // namespace doris
