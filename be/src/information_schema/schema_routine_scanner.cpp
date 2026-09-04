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

#include "information_schema/schema_routine_scanner.h"

#include <vector>

#include "common/status.h"
#include "core/block/block.h"
#include "core/string_ref.h"
#include "runtime/runtime_state.h"

namespace doris {
class RuntimeState;
class Block;

std::vector<SchemaScanner::ColumnDesc> SchemaRoutinesScanner::_s_tbls_columns = {
        {"SPECIFIC_NAME", TYPE_VARCHAR, sizeof(StringRef), true},
        {"ROUTINE_CATALOG", TYPE_VARCHAR, sizeof(StringRef), true},
        {"ROUTINE_SCHEMA", TYPE_VARCHAR, sizeof(StringRef), true},
        {"ROUTINE_NAME", TYPE_VARCHAR, sizeof(StringRef), true},
        {"ROUTINE_TYPE", TYPE_VARCHAR, sizeof(StringRef), true},
        {"DTD_IDENTIFIER", TYPE_VARCHAR, sizeof(StringRef), true},
        {"ROUTINE_BODY", TYPE_VARCHAR, sizeof(StringRef), true},
        {"ROUTINE_DEFINITION", TYPE_VARCHAR, sizeof(StringRef), true},
        {"EXTERNAL_NAME", TYPE_VARCHAR, sizeof(StringRef), true},
        {"EXTERNAL_LANGUAGE", TYPE_VARCHAR, sizeof(StringRef), true},
        {"PARAMETER_STYLE", TYPE_VARCHAR, sizeof(StringRef), true},
        {"IS_DETERMINISTIC", TYPE_VARCHAR, sizeof(StringRef), true},
        {"SQL_DATA_ACCESS", TYPE_VARCHAR, sizeof(StringRef), true},
        {"SQL_PATH", TYPE_VARCHAR, sizeof(StringRef), true},
        {"SECURITY_TYPE", TYPE_VARCHAR, sizeof(StringRef), true},
        {"CREATED", TYPE_DATETIME, sizeof(int64_t), true},
        {"LAST_ALTERED", TYPE_DATETIME, sizeof(int64_t), true},
        {"SQL_MODE", TYPE_VARCHAR, sizeof(StringRef), true},
        {"ROUTINE_COMMENT", TYPE_VARCHAR, sizeof(StringRef), true},
        {"DEFINER", TYPE_VARCHAR, sizeof(StringRef), true},
        {"CHARACTER_SET_CLIENT", TYPE_VARCHAR, sizeof(StringRef), true},
        {"COLLATION_CONNECTION", TYPE_VARCHAR, sizeof(StringRef), true},
        {"DATABASE_COLLATION", TYPE_VARCHAR, sizeof(StringRef), true},
};

SchemaRoutinesScanner::SchemaRoutinesScanner()
        : SchemaScanner(_s_tbls_columns, TSchemaTableType::SCH_PROCEDURES) {}

SchemaRoutinesScanner::~SchemaRoutinesScanner() {}

Status SchemaRoutinesScanner::start(RuntimeState* state) {
    return Status::OK();
}

Status SchemaRoutinesScanner::get_next_block_internal(Block* block, bool* eos) {
    *eos = true;
    return Status::OK();
}

} // namespace doris
