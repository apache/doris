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

#include "information_schema/schema_key_column_usage_scanner.h"

#include "core/block/block.h"
#include "core/string_ref.h"

namespace doris {

// Must stay in the same order and of the same types as the `key_column_usage` entry of
// SchemaTable.TABLE_MAP on the FE, which builds these rows.
std::vector<SchemaScanner::ColumnDesc> SchemaKeyColumnUsageScanner::_s_tbls_columns = {
        {"CONSTRAINT_CATALOG", TYPE_VARCHAR, sizeof(StringRef), true},
        {"CONSTRAINT_SCHEMA", TYPE_VARCHAR, sizeof(StringRef), true},
        {"CONSTRAINT_NAME", TYPE_VARCHAR, sizeof(StringRef), true},
        {"TABLE_CATALOG", TYPE_VARCHAR, sizeof(StringRef), true},
        {"TABLE_SCHEMA", TYPE_VARCHAR, sizeof(StringRef), true},
        {"TABLE_NAME", TYPE_VARCHAR, sizeof(StringRef), true},
        {"COLUMN_NAME", TYPE_VARCHAR, sizeof(StringRef), true},
        {"ORDINAL_POSITION", TYPE_BIGINT, sizeof(int64_t), true},
        {"POSITION_IN_UNIQUE_CONSTRAINT", TYPE_BIGINT, sizeof(int64_t), true},
        {"REFERENCED_TABLE_SCHEMA", TYPE_VARCHAR, sizeof(StringRef), true},
        {"REFERENCED_TABLE_NAME", TYPE_VARCHAR, sizeof(StringRef), true},
        {"REFERENCED_COLUMN_NAME", TYPE_VARCHAR, sizeof(StringRef), true},
};

SchemaKeyColumnUsageScanner::SchemaKeyColumnUsageScanner()
        : SchemaPerDbScanner(_s_tbls_columns, TSchemaTableType::SCH_KEY_COLUMN_USAGE,
                             TSchemaTableName::KEY_COLUMN_USAGE, "key column usage") {}

} // namespace doris
