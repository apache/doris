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
static const std::string HTTP_LINE_DELIMITER = "line_delimiter";
static const std::string HTTP_ENCLOSE = "enclose";
static const std::string HTTP_ESCAPE = "escape";
static const std::string HTTP_MAX_FILTER_RATIO = "max_filter_ratio";
static const std::string HTTP_TIMEOUT = "timeout";
static const std::string HTTP_PARTITIONS = "partitions";
static const std::string HTTP_TEMP_PARTITIONS = "temporary_partitions";
static const std::string HTTP_NEGATIVE = "negative";
static const std::string HTTP_STRICT_MODE = "strict_mode";
static const std::string HTTP_TIMEZONE = "timezone";
static const std::string HTTP_TIME_ZONE = "time_zone";
static const std::string HTTP_EXEC_MEM_LIMIT = "exec_mem_limit";
static const std::string HTTP_JSONPATHS = "jsonpaths";
static const std::string HTTP_JSONROOT = "json_root";
static const std::string HTTP_STRIP_OUTER_ARRAY = "strip_outer_array";
static const std::string HTTP_NUM_AS_STRING = "num_as_string";
static const std::string HTTP_FUZZY_PARSE = "fuzzy_parse";
static const std::string HTTP_READ_JSON_BY_LINE = "read_json_by_line";
static const std::string HTTP_MERGE_TYPE = "merge_type";
static const std::string HTTP_DELETE_CONDITION = "delete";
static const std::string HTTP_FUNCTION_COLUMN = "function_column";
static const std::string HTTP_SEQUENCE_COL = "sequence_col";
static const std::string HTTP_COMPRESS_TYPE = "compress_type";
static const std::string HTTP_SEND_BATCH_PARALLELISM = "send_batch_parallelism";
static const std::string HTTP_LOAD_TO_SINGLE_TABLET = "load_to_single_tablet";
static const std::string HTTP_HIDDEN_COLUMNS = "hidden_columns";
static const std::string HTTP_TRIM_DOUBLE_QUOTES = "trim_double_quotes";
static const std::string HTTP_SKIP_LINES = "skip_lines";
static const std::string HTTP_COMMENT = "comment";
static const std::string HTTP_ENABLE_PROFILE = "enable_profile";
static const std::string HTTP_PARTIAL_COLUMNS = "partial_columns";
static const std::string HTTP_SQL = "sql";
static const std::string HTTP_TWO_PHASE_COMMIT = "two_phase_commit";
static const std::string HTTP_TXN_ID_KEY = "txn_id";
static const std::string HTTP_TXN_OPERATION_KEY = "txn_operation";
static const std::string HTTP_MEMTABLE_ON_SINKNODE = "memtable_on_sink_node";
static const std::string HTTP_LOAD_STREAM_PER_NODE = "load_stream_per_node";
static const std::string HTTP_WAL_ID_KY = "wal_id";
static const std::string HTTP_AUTH_CODE = "auth_code";
static const std::string HTTP_GROUP_COMMIT = "group_commit";
static const std::string HTTP_CLOUD_CLUSTER = "cloud_cluster";

} // namespace doris
