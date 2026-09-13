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

#include "information_schema/schema_statistics_scanner.h"

#include "core/string_ref.h"

namespace doris {

// Must stay in the same order and of the same types as the `statistics` entry of
// SchemaTable.TABLE_MAP on the FE, which builds these rows.
std::vector<SchemaScanner::ColumnDesc> SchemaStatisticsScanner::_s_tbls_columns = {
        {"TABLE_CATALOG", TYPE_VARCHAR, sizeof(StringRef), true},
        {"TABLE_SCHEMA", TYPE_VARCHAR, sizeof(StringRef), true},
        {"TABLE_NAME", TYPE_VARCHAR, sizeof(StringRef), true},
        {"NON_UNIQUE", TYPE_BIGINT, sizeof(int64_t), true},
        {"INDEX_SCHEMA", TYPE_VARCHAR, sizeof(StringRef), true},
        {"INDEX_NAME", TYPE_VARCHAR, sizeof(StringRef), true},
        {"SEQ_IN_INDEX", TYPE_BIGINT, sizeof(int64_t), true},
        {"COLUMN_NAME", TYPE_VARCHAR, sizeof(StringRef), true},
        {"COLLATION", TYPE_VARCHAR, sizeof(StringRef), true},
        {"CARDINALITY", TYPE_BIGINT, sizeof(int64_t), true},
        {"SUB_PART", TYPE_BIGINT, sizeof(int64_t), true},
        {"PACKED", TYPE_VARCHAR, sizeof(StringRef), true},
        {"NULLABLE", TYPE_VARCHAR, sizeof(StringRef), true},
        {"INDEX_TYPE", TYPE_VARCHAR, sizeof(StringRef), true},
        {"COMMENT", TYPE_VARCHAR, sizeof(StringRef), true},
        {"INDEX_COMMENT", TYPE_VARCHAR, sizeof(StringRef), true},
        {"IS_VISIBLE", TYPE_VARCHAR, sizeof(StringRef), true},
        {"EXPRESSION", TYPE_VARCHAR, sizeof(StringRef), true},
};

SchemaStatisticsScanner::SchemaStatisticsScanner()
        : SchemaPerDbScanner(_s_tbls_columns, TSchemaTableType::SCH_STATISTICS,
                             TSchemaTableName::STATISTICS, "statistics") {}

} // namespace doris
