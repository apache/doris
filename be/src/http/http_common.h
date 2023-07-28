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
#include <unordered_set>

#include "http_headers.h"

namespace doris {

static const std::string HTTP_DB_KEY = "db";
static const std::string HTTP_TABLE_KEY = "table";
static const std::string HTTP_LABEL_KEY = "label";
static const std::string HTTP_FORMAT_KEY = "format";
static const std::string HTTP_COLUMNS = "columns";
static const std::string HTTP_WHERE = "where";
static const std::string HTTP_COLUMN_SEPARATOR = "column_separator";
static const std::string HTTP_LINE_DELIMITER = "line_delimiter";
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
static const std::string HTTP_TWO_PHASE_COMMIT = "two_phase_commit";
static const std::string HTTP_TXN_ID_KEY = "txn_id";
static const std::string HTTP_TXN_OPERATION_KEY = "txn_operation";
static const std::string HTTP_TOKEN = "token";

static const std::unordered_set<std::string> kAvailableStreamLoadRequestFields {
        // below are avaiable request fields defined in stream load
        HTTP_LABEL_KEY,
        HTTP_TWO_PHASE_COMMIT,
        HTTP_COMPRESS_TYPE,
        HTTP_FORMAT_KEY,
        HTTP_FORMAT_KEY,
        HTTP_READ_JSON_BY_LINE,
        HTTP_TIMEOUT,
        HTTP_COMMENT,
        HTTP_COLUMNS,
        HTTP_WHERE,
        HTTP_COLUMN_SEPARATOR,
        HTTP_LINE_DELIMITER,
        HTTP_PARTITIONS,
        HTTP_TEMP_PARTITIONS,
        HTTP_NEGATIVE,
        HTTP_STRICT_MODE,
        HTTP_TIMEZONE,
        HTTP_EXEC_MEM_LIMIT,
        HTTP_JSONPATHS,
        HTTP_JSONROOT,
        HTTP_STRIP_OUTER_ARRAY,
        HTTP_NUM_AS_STRING,
        HTTP_FUZZY_PARSE,
        HTTP_READ_JSON_BY_LINE,
        HTTP_FUNCTION_COLUMN + "." + HTTP_SEQUENCE_COL,
        HTTP_SEND_BATCH_PARALLELISM,
        HTTP_LOAD_TO_SINGLE_TABLET,
        HTTP_MERGE_TYPE,
        HTTP_DELETE_CONDITION,
        HTTP_MAX_FILTER_RATIO,
        HTTP_HIDDEN_COLUMNS,
        HTTP_TRIM_DOUBLE_QUOTES,
        HTTP_SKIP_LINES,
        HTTP_ENABLE_PROFILE,
        HTTP_PARTIAL_COLUMNS,
        HTTP_TOKEN,

        // below are http standard request fields
        HttpHeaders::ACCEPT,
        HttpHeaders::ACCEPT_CHARSET,
        HttpHeaders::ACCEPT_ENCODING,
        HttpHeaders::ACCEPT_LANGUAGE,
        HttpHeaders::ACCEPT_RANGES,
        HttpHeaders::ACCEPT_PATCH,
        HttpHeaders::ACCESS_CONTROL_ALLOW_CREDENTIALS,
        HttpHeaders::ACCESS_CONTROL_ALLOW_HEADERS,
        HttpHeaders::ACCESS_CONTROL_ALLOW_METHODS,
        HttpHeaders::ACCESS_CONTROL_ALLOW_ORIGIN,
        HttpHeaders::ACCESS_CONTROL_EXPOSE_HEADERS,
        HttpHeaders::ACCESS_CONTROL_MAX_AGE,
        HttpHeaders::ACCESS_CONTROL_REQUEST_HEADERS,
        HttpHeaders::ACCESS_CONTROL_REQUEST_METHOD,
        HttpHeaders::AGE,
        HttpHeaders::ALLOW,
        HttpHeaders::AUTHORIZATION,
        HttpHeaders::CACHE_CONTROL,
        HttpHeaders::CONNECTION,
        HttpHeaders::CONTENT_BASE,
        HttpHeaders::CONTENT_ENCODING,
        HttpHeaders::CONTENT_LANGUAGE,
        HttpHeaders::CONTENT_LENGTH,
        HttpHeaders::CONTENT_LOCATION,
        HttpHeaders::CONTENT_TRANSFER_ENCODING,
        HttpHeaders::CONTENT_MD5,
        HttpHeaders::CONTENT_RANGE,
        HttpHeaders::CONTENT_TYPE,
        HttpHeaders::COOKIE,
        HttpHeaders::DATE,
        HttpHeaders::ETAG,
        HttpHeaders::EXPECT,
        HttpHeaders::EXPIRES,
        HttpHeaders::FROM,
        HttpHeaders::HOST,
        HttpHeaders::IF_MATCH,
        HttpHeaders::IF_MODIFIED_SINCE,
        HttpHeaders::IF_NONE_MATCH,
        HttpHeaders::IF_RANGE,
        HttpHeaders::IF_UNMODIFIED_SINCE,
        HttpHeaders::LAST_MODIFIED,
        HttpHeaders::LOCATION,
        HttpHeaders::MAX_FORWARDS,
        HttpHeaders::ORIGIN,
        HttpHeaders::PRAGMA,
        HttpHeaders::PROXY_AUTHENTICATE,
        HttpHeaders::PROXY_AUTHORIZATION,
        HttpHeaders::RANGE,
        HttpHeaders::REFERER,
        HttpHeaders::RETRY_AFTER,
        HttpHeaders::SEC_WEBSOCKET_KEY1,
        HttpHeaders::SEC_WEBSOCKET_KEY2,
        HttpHeaders::SEC_WEBSOCKET_LOCATION,
        HttpHeaders::SEC_WEBSOCKET_ORIGIN,
        HttpHeaders::SEC_WEBSOCKET_PROTOCOL,
        HttpHeaders::SEC_WEBSOCKET_VERSION,
        HttpHeaders::SEC_WEBSOCKET_KEY,
        HttpHeaders::SEC_WEBSOCKET_ACCEPT,
        HttpHeaders::SERVER,
        HttpHeaders::SET_COOKIE,
        HttpHeaders::SET_COOKIE2,
        HttpHeaders::TE,
        HttpHeaders::TRAILER,
        HttpHeaders::TRANSFER_ENCODING,
        HttpHeaders::UPGRADE,
        HttpHeaders::USER_AGENT,
        HttpHeaders::VARY,
        HttpHeaders::VIA,
        HttpHeaders::WARNING,
        HttpHeaders::WEBSOCKET_LOCATION,
        HttpHeaders::WEBSOCKET_ORIGIN,
        HttpHeaders::WEBSOCKET_PROTOCOL,
        HttpHeaders::WWW_AUTHENTICATE,
};

} // namespace doris
