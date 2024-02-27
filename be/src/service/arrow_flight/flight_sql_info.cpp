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
// This file is copied from
// https://github.com/apache/arrow/blob/main/cpp/src/arrow/flight/sql/example/sqlite_sql_info.cc
// and modified by Doris

#include "service/arrow_flight/flight_sql_info.h"

#include "arrow/flight/sql/types.h"
#include "arrow/util/config.h"

namespace doris {
namespace flight {
/// \brief Gets the mapping from SQL info ids to arrow::flight::sql::SqlInfoResult instances.
/// \return the cache.
arrow::flight::sql::SqlInfoResultMap GetSqlInfoResultMap() {
    return {{arrow::flight::sql::SqlInfoOptions::SqlInfo::FLIGHT_SQL_SERVER_NAME,
             arrow::flight::sql::SqlInfoResult(std::string("DorisBE"))},
            {arrow::flight::sql::SqlInfoOptions::SqlInfo::FLIGHT_SQL_SERVER_VERSION,
             arrow::flight::sql::SqlInfoResult(std::string("1.0"))},
            {arrow::flight::sql::SqlInfoOptions::SqlInfo::FLIGHT_SQL_SERVER_ARROW_VERSION,
             arrow::flight::sql::SqlInfoResult(std::string(ARROW_VERSION_STRING))},
            {arrow::flight::sql::SqlInfoOptions::SqlInfo::FLIGHT_SQL_SERVER_READ_ONLY,
             arrow::flight::sql::SqlInfoResult(true)},
            {arrow::flight::sql::SqlInfoOptions::SqlInfo::FLIGHT_SQL_SERVER_SQL,
             arrow::flight::sql::SqlInfoResult(true)},
            {arrow::flight::sql::SqlInfoOptions::SqlInfo::FLIGHT_SQL_SERVER_SUBSTRAIT,
             arrow::flight::sql::SqlInfoResult(false)},
            {arrow::flight::sql::SqlInfoOptions::SqlInfo::FLIGHT_SQL_SERVER_TRANSACTION,
             arrow::flight::sql::SqlInfoResult(
                     arrow::flight::sql::SqlInfoOptions::SqlSupportedTransaction::
                             SQL_SUPPORTED_TRANSACTION_NONE)},
            {arrow::flight::sql::SqlInfoOptions::SqlInfo::FLIGHT_SQL_SERVER_CANCEL,
             arrow::flight::sql::SqlInfoResult(false)},
            {arrow::flight::sql::SqlInfoOptions::SqlInfo::SQL_DDL_CATALOG,
             arrow::flight::sql::SqlInfoResult(false)},
            {arrow::flight::sql::SqlInfoOptions::SqlInfo::SQL_DDL_SCHEMA,
             arrow::flight::sql::SqlInfoResult(false)},
            {arrow::flight::sql::SqlInfoOptions::SqlInfo::SQL_DDL_TABLE,
             arrow::flight::sql::SqlInfoResult(false)},
            {arrow::flight::sql::SqlInfoOptions::SqlInfo::SQL_IDENTIFIER_CASE,
             arrow::flight::sql::SqlInfoResult(
                     int64_t(arrow::flight::sql::SqlInfoOptions::SqlSupportedCaseSensitivity::
                                     SQL_CASE_SENSITIVITY_CASE_INSENSITIVE))},
            {arrow::flight::sql::SqlInfoOptions::SqlInfo::SQL_IDENTIFIER_QUOTE_CHAR,
             arrow::flight::sql::SqlInfoResult(std::string("\""))},
            {arrow::flight::sql::SqlInfoOptions::SqlInfo::SQL_QUOTED_IDENTIFIER_CASE,
             arrow::flight::sql::SqlInfoResult(
                     int64_t(arrow::flight::sql::SqlInfoOptions::SqlSupportedCaseSensitivity::
                                     SQL_CASE_SENSITIVITY_CASE_INSENSITIVE))},
            {arrow::flight::sql::SqlInfoOptions::SqlInfo::SQL_ALL_TABLES_ARE_SELECTABLE,
             arrow::flight::sql::SqlInfoResult(true)},
            {arrow::flight::sql::SqlInfoOptions::SqlInfo::SQL_NULL_ORDERING,
             arrow::flight::sql::SqlInfoResult(
                     int64_t(arrow::flight::sql::SqlInfoOptions::SqlNullOrdering::
                                     SQL_NULLS_SORTED_AT_START))},
            {arrow::flight::sql::SqlInfoOptions::SqlInfo::SQL_KEYWORDS,
             arrow::flight::sql::SqlInfoResult(std::vector<std::string>({"ABORT",
                                                                         "ACTION",
                                                                         "ADD",
                                                                         "AFTER",
                                                                         "ALL",
                                                                         "ALTER",
                                                                         "ALWAYS",
                                                                         "ANALYZE",
                                                                         "AND",
                                                                         "AS",
                                                                         "ASC",
                                                                         "ATTACH",
                                                                         "AUTOINCREMENT",
                                                                         "BEFORE",
                                                                         "BEGIN",
                                                                         "BETWEEN",
                                                                         "BY",
                                                                         "CASCADE",
                                                                         "CASE",
                                                                         "CAST",
                                                                         "CHECK",
                                                                         "COLLATE",
                                                                         "COLUMN",
                                                                         "COMMIT",
                                                                         "CONFLICT",
                                                                         "CONSTRAINT",
                                                                         "CREATE",
                                                                         "CROSS",
                                                                         "CURRENT",
                                                                         "CURRENT_DATE",
                                                                         "CURRENT_TIME",
                                                                         "CURRENT_TIMESTAMP",
                                                                         "DATABASE",
                                                                         "DEFAULT",
                                                                         "DEFERRABLE",
                                                                         "DEFERRED",
                                                                         "DELETE",
                                                                         "DESC",
                                                                         "DETACH",
                                                                         "DISTINCT",
                                                                         "DO",
                                                                         "DROP",
                                                                         "EACH",
                                                                         "ELSE",
                                                                         "END",
                                                                         "ESCAPE",
                                                                         "EXCEPT",
                                                                         "EXCLUDE",
                                                                         "EXCLUSIVE",
                                                                         "EXISTS",
                                                                         "EXPLAIN",
                                                                         "FAIL",
                                                                         "FILTER",
                                                                         "FIRST",
                                                                         "FOLLOWING",
                                                                         "FOR",
                                                                         "FOREIGN",
                                                                         "FROM",
                                                                         "FULL",
                                                                         "GENERATED",
                                                                         "GLOB",
                                                                         "GROUP",
                                                                         "GROUPS",
                                                                         "HAVING",
                                                                         "IF",
                                                                         "IGNORE",
                                                                         "IMMEDIATE",
                                                                         "IN",
                                                                         "INDEX",
                                                                         "INDEXED",
                                                                         "INITIALLY",
                                                                         "INNER",
                                                                         "INSERT",
                                                                         "INSTEAD",
                                                                         "INTERSECT",
                                                                         "INTO",
                                                                         "IS",
                                                                         "ISNULL",
                                                                         "JOIN",
                                                                         "KEY",
                                                                         "LAST",
                                                                         "LEFT",
                                                                         "LIKE",
                                                                         "LIMIT",
                                                                         "MATCH",
                                                                         "MATERIALIZED",
                                                                         "NATURAL",
                                                                         "NO",
                                                                         "NOT",
                                                                         "NOTHING",
                                                                         "NOTNULL",
                                                                         "NULL",
                                                                         "NULLS",
                                                                         "OF",
                                                                         "OFFSET",
                                                                         "ON",
                                                                         "OR",
                                                                         "ORDER",
                                                                         "OTHERS",
                                                                         "OUTER",
                                                                         "OVER",
                                                                         "PARTITION",
                                                                         "PLAN",
                                                                         "PRAGMA",
                                                                         "PRECEDING",
                                                                         "PRIMARY",
                                                                         "QUERY",
                                                                         "RAISE",
                                                                         "RANGE",
                                                                         "RECURSIVE",
                                                                         "REFERENCES",
                                                                         "REGEXP",
                                                                         "REINDEX",
                                                                         "RELEASE",
                                                                         "RENAME",
                                                                         "REPLACE",
                                                                         "RESTRICT",
                                                                         "RETURNING",
                                                                         "RIGHT",
                                                                         "ROLLBACK",
                                                                         "ROW",
                                                                         "ROWS",
                                                                         "SAVEPOINT",
                                                                         "SELECT",
                                                                         "SET",
                                                                         "TABLE",
                                                                         "TEMP",
                                                                         "TEMPORARY",
                                                                         "THEN",
                                                                         "TIES",
                                                                         "TO",
                                                                         "TRANSACTION",
                                                                         "TRIGGER",
                                                                         "UNBOUNDED",
                                                                         "UNION",
                                                                         "UNIQUE",
                                                                         "UPDATE",
                                                                         "USING",
                                                                         "VACUUM",
                                                                         "VALUES",
                                                                         "VIEW",
                                                                         "VIRTUAL",
                                                                         "WHEN",
                                                                         "WHERE",
                                                                         "WINDOW",
                                                                         "WITH",
                                                                         "WITHOUT"}))},
            {arrow::flight::sql::SqlInfoOptions::SqlInfo::SQL_NUMERIC_FUNCTIONS,
             arrow::flight::sql::SqlInfoResult(std::vector<std::string>(
                     {"ACOS",    "ACOSH", "ASIN", "ASINH",   "ATAN", "ATAN2", "ATANH", "CEIL",
                      "CEILING", "COS",   "COSH", "DEGREES", "EXP",  "FLOOR", "LN",    "LOG",
                      "LOG",     "LOG10", "LOG2", "MOD",     "PI",   "POW",   "POWER", "RADIANS",
                      "SIN",     "SINH",  "SQRT", "TAN",     "TANH", "TRUNC"}))},
            {arrow::flight::sql::SqlInfoOptions::SqlInfo::SQL_STRING_FUNCTIONS,
             arrow::flight::sql::SqlInfoResult(
                     std::vector<std::string>({"SUBSTR", "TRIM", "LTRIM", "RTRIM", "LENGTH",
                                               "REPLACE", "UPPER", "LOWER", "INSTR"}))},
            {arrow::flight::sql::SqlInfoOptions::SqlInfo::SQL_SUPPORTS_CONVERT,
             arrow::flight::sql::SqlInfoResult(std::unordered_map<int32_t, std::vector<int32_t>>(
                     {{arrow::flight::sql::SqlInfoOptions::SqlSupportsConvert::SQL_CONVERT_BIGINT,
                       std::vector<int32_t>(
                               {arrow::flight::sql::SqlInfoOptions::SqlSupportsConvert::
                                        SQL_CONVERT_INTEGER})}}))}};
}

} // namespace flight
} // namespace doris
