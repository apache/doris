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

#pragma once

#include <string>

namespace doris {

static const std::string HTTP_DB_KEY = "db";
static const std::string HTTP_TABLE_KEY = "table";
static const std::string HTTP_LABEL_KEY = "label";
static const std::string HTTP_FORMAT_KEY = "format";
static const std::string HTTP_COLUMNS = "columns";
static const std::string HTTP_WHERE = "where";
static const std::string HTTP_COLUMN_SEPARATOR = "column_separator";
static const std::string HTTP_MAX_FILTER_RATIO = "max_filter_ratio";
static const std::string HTTP_TIMEOUT = "timeout";
static const std::string HTTP_PARTITIONS = "partitions";
static const std::string HTTP_TEMP_PARTITIONS = "temporary_partitions";
static const std::string HTTP_NEGATIVE = "negative";
static const std::string HTTP_STRICT_MODE = "strict_mode";
static const std::string HTTP_TIMEZONE = "timezone";
static const std::string HTTP_EXEC_MEM_LIMIT = "exec_mem_limit";
static const std::string HTTP_JSONPATHS = "jsonpaths";
static const std::string HTTP_JSONROOT = "json_root";
static const std::string HTTP_STRIP_OUTER_ARRAY = "strip_outer_array";
static const std::string HTTP_MERGE_TYPE = "merge_type";
static const std::string HTTP_DELETE_CONDITION = "delete";
static const std::string HTTP_FUNCTION_COLUMN = "function_column";
static const std::string HTTP_SEQUENCE_COL = "sequence_col";

static const std::string HTTP_100_CONTINUE = "100-continue";

} // namespace doris
