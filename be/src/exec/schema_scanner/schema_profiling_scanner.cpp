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

#include "exec/schema_scanner/schema_profiling_scanner.h"

#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/FrontendService_types.h>
#include <stdint.h>

#include "exec/schema_scanner/schema_helper.h"
#include "runtime/define_primitive_type.h"
#include "util/runtime_profile.h"
#include "vec/common/string_ref.h"

namespace doris {
class RuntimeState;
namespace vectorized {
class Block;
} // namespace vectorized

std::vector<SchemaScanner::ColumnDesc> SchemaProfilingScanner::_s_tbls_columns = {
        //   name,       type,          size,     is_null
        {"QUERY_ID", TYPE_INT, sizeof(int), false},
        {"SEQ", TYPE_INT, sizeof(int), false},
        {"STATE", TYPE_VARCHAR, sizeof(StringRef), false},
        {"DURATION", TYPE_DOUBLE, sizeof(double), false},
        {"CPU_USER", TYPE_DOUBLE, sizeof(double), true},
        {"CPU_SYSTEM", TYPE_DOUBLE, sizeof(double), true},
        {"CONTEXT_VOLUNTARY", TYPE_INT, sizeof(int), true},
        {"CONTEXT_INVOLUNTARY", TYPE_INT, sizeof(int), true},
        {"BLOCK_OPS_IN", TYPE_INT, sizeof(int), true},
        {"BLOCK_OPS_OUT", TYPE_INT, sizeof(int), true},
        {"MESSAGES_SENT", TYPE_INT, sizeof(int), true},
        {"MESSAGES_RECEIVED", TYPE_INT, sizeof(int), true},
        {"PAGE_FAULTS_MAJOR", TYPE_INT, sizeof(int), true},
        {"PAGE_FAULTS_MINOR", TYPE_INT, sizeof(int), true},
        {"SWAPS", TYPE_INT, sizeof(int), true},
        {"SOURCE_FUNCTION", TYPE_VARCHAR, sizeof(StringRef), false},
        {"SOURCE_FILE", TYPE_VARCHAR, sizeof(StringRef), false},
        {"SOURCE_LINE", TYPE_INT, sizeof(int), true},
};

SchemaProfilingScanner::SchemaProfilingScanner()
        : SchemaScanner(_s_tbls_columns, TSchemaTableType::SCH_PROFILING) {}

SchemaProfilingScanner::~SchemaProfilingScanner() {}

Status SchemaProfilingScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("used before initialized.");
    }
    SCOPED_TIMER(_get_db_timer);
    TGetDbsParams db_params;
    if (nullptr != _param->common_param->db) {
        db_params.__set_pattern(*(_param->common_param->db));
    }
    if (nullptr != _param->common_param->catalog) {
        db_params.__set_catalog(*(_param->common_param->catalog));
    }
    if (nullptr != _param->common_param->current_user_ident) {
        db_params.__set_current_user_ident(*(_param->common_param->current_user_ident));
    } else {
        if (nullptr != _param->common_param->user) {
            db_params.__set_user(*(_param->common_param->user));
        }
        if (nullptr != _param->common_param->user_ip) {
            db_params.__set_user_ip(*(_param->common_param->user_ip));
        }
    }

    if (nullptr == _param->common_param->ip || 0 == _param->common_param->port) {
        return Status::InternalError("IP or port doesn't exists");
    }
    return Status::OK();
}

Status SchemaProfilingScanner::get_next_block_internal(vectorized::Block* block, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("Used before initialized.");
    }
    if (nullptr == block || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }
    *eos = true;
    return Status::OK();
}

} // namespace doris
