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

#include "information_schema/schema_table_options_scanner.h"

#include "core/string_ref.h"

namespace doris {

std::vector<SchemaScanner::ColumnDesc> SchemaTableOptionsScanner::_s_tbls_columns = {
        {"TABLE_CATALOG", TYPE_VARCHAR, sizeof(StringRef), true},
        {"TABLE_SCHEMA", TYPE_VARCHAR, sizeof(StringRef), true},
        {"TABLE_NAME", TYPE_VARCHAR, sizeof(StringRef), true},
        {"TABLE_MODEL", TYPE_STRING, sizeof(StringRef), true},
        {"TABLE_MODEL_KEY", TYPE_STRING, sizeof(StringRef), true},
        {"DISTRIBUTE_KEY", TYPE_STRING, sizeof(StringRef), true},
        {"DISTRIBUTE_TYPE", TYPE_STRING, sizeof(StringRef), true},
        {"BUCKETS_NUM", TYPE_INT, sizeof(int32_t), true},
        {"PARTITION_NUM", TYPE_INT, sizeof(int32_t), true},
        {"PARTITION_METHOD", TYPE_VARCHAR, sizeof(StringRef), true},
        {"PARTITION_EXPRESSION", TYPE_VARCHAR, sizeof(StringRef), true},
        {"PARTITION_KEY", TYPE_STRING, sizeof(StringRef), true},
        {"RANGE", TYPE_STRING, sizeof(StringRef), true},
};

SchemaTableOptionsScanner::SchemaTableOptionsScanner()
        : SchemaPerDbScanner(_s_tbls_columns, TSchemaTableType::SCH_TABLE_OPTIONS,
                             TSchemaTableName::TABLE_OPTIONS, "table options") {}

} // namespace doris
