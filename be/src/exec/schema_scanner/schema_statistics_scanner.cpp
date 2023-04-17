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

#include "exec/schema_scanner/schema_statistics_scanner.h"

#include <stdint.h>

#include "runtime/define_primitive_type.h"
#include "vec/common/string_ref.h"

namespace doris {

std::vector<SchemaScanner::ColumnDesc> SchemaStatisticsScanner::_s_cols_statistics = {
        //   name,       type,          size,                     is_null
        {"TABLE_CATALOG", TYPE_VARCHAR, sizeof(StringRef), true},
        {"TABLE_SCHEMA", TYPE_VARCHAR, sizeof(StringRef), false},
        {"TABLE_NAME", TYPE_VARCHAR, sizeof(StringRef), false},
        {"NON_UNIQUE", TYPE_BIGINT, sizeof(int64_t), false},
        {"INDEX_SCHEMA", TYPE_VARCHAR, sizeof(StringRef), false},
        {"INDEX_NAME", TYPE_VARCHAR, sizeof(StringRef), false},
        {"SEQ_IN_INDEX", TYPE_BIGINT, sizeof(int64_t), false},
        {"COLUMN_NAME", TYPE_VARCHAR, sizeof(StringRef), false},
        {"COLLATION", TYPE_VARCHAR, sizeof(StringRef), true},
        {"CARDINALITY", TYPE_BIGINT, sizeof(int64_t), true},
        {"SUB_PART", TYPE_BIGINT, sizeof(int64_t), true},
        {"PACKED", TYPE_VARCHAR, sizeof(StringRef), true},
        {"NULLABLE", TYPE_VARCHAR, sizeof(StringRef), false},
        {"INDEX_TYPE", TYPE_VARCHAR, sizeof(StringRef), false},
        {"COMMENT", TYPE_VARCHAR, sizeof(StringRef), true},
};

SchemaStatisticsScanner::SchemaStatisticsScanner() : SchemaScanner(_s_cols_statistics) {}

SchemaStatisticsScanner::~SchemaStatisticsScanner() {}

} // namespace doris
