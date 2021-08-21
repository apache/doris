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

#include "exec/schema_scanner/schema_helper.h"
#include "runtime/primitive_type.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"

namespace doris {

SchemaScanner::ColumnDesc SchemaVariablesScanner::_s_vars_columns[] = {
        //   name,       type,          size
        {"VARIABLE_NAME", TYPE_VARCHAR, sizeof(StringValue), false},
        {"VARIABLE_VALUE", TYPE_VARCHAR, sizeof(StringValue), false},
};

SchemaVariablesScanner::SchemaVariablesScanner(TVarType::type type)
        : SchemaScanner(_s_vars_columns,
                        sizeof(_s_vars_columns) / sizeof(SchemaScanner::ColumnDesc)),
          _type(type) {}

SchemaVariablesScanner::~SchemaVariablesScanner() {}

Status SchemaVariablesScanner::start(RuntimeState* state) {
    TShowVariableRequest var_params;
    // Use db to save type
    if (_param->db != nullptr) {
        if (strcmp(_param->db->c_str(), "GLOBAL") == 0) {
            var_params.__set_varType(TVarType::GLOBAL);
        } else {
            var_params.__set_varType(TVarType::SESSION);
        }
    } else {
        var_params.__set_varType(_type);
    }
    var_params.__set_threadId(_param->thread_id);

    if (nullptr != _param->ip && 0 != _param->port) {
        RETURN_IF_ERROR(SchemaHelper::show_variables(*(_param->ip), _param->port, var_params,
                                                     &_var_result));
    } else {
        return Status::InternalError("IP or port doesn't exists");
    }
    _begin = _var_result.variables.begin();
    return Status::OK();
}

Status SchemaVariablesScanner::fill_one_row(Tuple* tuple, MemPool* pool) {
    // variables names
    {
        void* slot = tuple->get_slot(_tuple_desc->slots()[0]->tuple_offset());
        StringValue* str_slot = reinterpret_cast<StringValue*>(slot);
        int len = strlen(_begin->first.c_str());
        str_slot->ptr = (char*)pool->allocate(len + 1);
        if (nullptr == str_slot->ptr) {
            return Status::InternalError("No Memory.");
        }
        memcpy(str_slot->ptr, _begin->first.c_str(), len + 1);
        str_slot->len = len;
    }
    // value
    {
        void* slot = tuple->get_slot(_tuple_desc->slots()[1]->tuple_offset());
        StringValue* str_slot = reinterpret_cast<StringValue*>(slot);
        int len = strlen(_begin->second.c_str());
        str_slot->ptr = (char*)pool->allocate(len + 1);
        if (nullptr == str_slot->ptr) {
            return Status::InternalError("No Memory.");
        }
        memcpy(str_slot->ptr, _begin->second.c_str(), len + 1);
        str_slot->len = len;
    }
    ++_begin;
    return Status::OK();
}

Status SchemaVariablesScanner::get_next_row(Tuple* tuple, MemPool* pool, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("call this before initial.");
    }
    if (nullptr == tuple || nullptr == pool || nullptr == eos) {
        return Status::InternalError("invalid parameter.");
    }
    if (_begin == _var_result.variables.end()) {
        *eos = true;
        return Status::OK();
    }
    *eos = false;
    return fill_one_row(tuple, pool);
}

} // namespace doris
