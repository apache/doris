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

#include "information_schema/schema_table_constraints_scanner.h"

#include "core/block/block.h"
#include "core/string_ref.h"

namespace doris {

// Must stay in the same order and of the same types as the `table_constraints` entry of
// SchemaTable.TABLE_MAP on the FE, which builds these rows.
std::vector<SchemaScanner::ColumnDesc> SchemaTableConstraintsScanner::_s_tbls_columns = {
        {"CONSTRAINT_CATALOG", TYPE_VARCHAR, sizeof(StringRef), true},
        {"CONSTRAINT_SCHEMA", TYPE_VARCHAR, sizeof(StringRef), true},
        {"CONSTRAINT_NAME", TYPE_VARCHAR, sizeof(StringRef), true},
        {"TABLE_SCHEMA", TYPE_VARCHAR, sizeof(StringRef), true},
        {"TABLE_NAME", TYPE_VARCHAR, sizeof(StringRef), true},
        {"CONSTRAINT_TYPE", TYPE_VARCHAR, sizeof(StringRef), true},
};

SchemaTableConstraintsScanner::SchemaTableConstraintsScanner()
        : SchemaPerDbScanner(_s_tbls_columns, TSchemaTableType::SCH_TABLE_CONSTRAINTS,
                             TSchemaTableName::TABLE_CONSTRAINTS, "table constraints") {}

} // namespace doris
