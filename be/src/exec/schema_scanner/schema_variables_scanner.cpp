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

#include "exec/schema_scanner/schema_variables_scanner.h"

#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/FrontendService_types.h>
#include <string.h>

#include <map>
#include <string>
#include <utility>

#include "exec/schema_scanner/schema_helper.h"
#include "runtime/define_primitive_type.h"
#include "util/runtime_profile.h"
#include "vec/common/string_ref.h"

namespace doris {
class RuntimeState;
namespace vectorized {
class Block;
} // namespace vectorized

std::vector<SchemaScanner::ColumnDesc> SchemaVariablesScanner::_s_vars_columns = {
        //   name,       type,          size
        {"VARIABLE_NAME", TYPE_VARCHAR, sizeof(StringRef), false},
        {"VARIABLE_VALUE", TYPE_VARCHAR, sizeof(StringRef), false},
};

SchemaVariablesScanner::SchemaVariablesScanner(TVarType::type type)
        : SchemaScanner(_s_vars_columns, TSchemaTableType::SCH_VARIABLES), _type(type) {}

SchemaVariablesScanner::~SchemaVariablesScanner() {}

Status SchemaVariablesScanner::start(RuntimeState* state) {
    TShowVariableRequest var_params;
    // Use db to save type
    if (_param->common_param->db != nullptr) {
        if (strcmp(_param->common_param->db->c_str(), "GLOBAL") == 0) {
            var_params.__set_varType(TVarType::GLOBAL);
        } else {
            var_params.__set_varType(TVarType::SESSION);
        }
    } else {
        var_params.__set_varType(_type);
    }
    var_params.__set_threadId(_param->common_param->thread_id);

    if (nullptr != _param->common_param->ip && 0 != _param->common_param->port) {
        RETURN_IF_ERROR(SchemaHelper::show_variables(
                *(_param->common_param->ip), _param->common_param->port, var_params, &_var_result));
    } else {
        return Status::InternalError("IP or port doesn't exists");
    }
    return Status::OK();
}

Status SchemaVariablesScanner::get_next_block_internal(vectorized::Block* block, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("call this before initial.");
    }
    if (nullptr == block || nullptr == eos) {
        return Status::InternalError("invalid parameter.");
    }

    *eos = true;
    if (_var_result.variables.empty()) {
        return Status::OK();
    }
    return _fill_block_impl(block);
}

Status SchemaVariablesScanner::_fill_block_impl(vectorized::Block* block) {
    SCOPED_TIMER(_fill_block_timer);
    auto row_num = _var_result.variables.size();
    std::vector<void*> datas(row_num);
    // variables names
    {
        std::vector<StringRef> strs(row_num);
        int idx = 0;
        for (auto& it : _var_result.variables) {
            strs[idx] = StringRef(it.first.c_str(), it.first.size());
            datas[idx] = strs.data() + idx;
            ++idx;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 0, datas));
    }
    // value
    {
        std::vector<StringRef> strs(row_num);
        int idx = 0;
        for (auto& it : _var_result.variables) {
            strs[idx] = StringRef(it.second.c_str(), it.second.size());
            datas[idx] = strs.data() + idx;
            ++idx;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 1, datas));
    }
    return Status::OK();
}

} // namespace doris
