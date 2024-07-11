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

namespace java org.apache.doris.thrift

enum TExprOpcode {
    INVALID_OPCODE,
    COMPOUND_NOT,
    COMPOUND_AND,
    COMPOUND_OR,
    CAST,
    FILTER_IN,
    FILTER_NOT_IN,
    FILTER_NEW_IN,
    FILTER_NEW_NOT_IN,
    EQ,
    NE,
    LT,
    LE,
    GT,
    GE,
    CONDITION_IF,
    CONDITION_NULLIF,
    CONDITION_IFNULL,
    CONDITION_COALESCE,
    TIMESTAMP_DATE_FORMAT,
    TIMESTAMP_DAYOFMONTH,
    TIMESTAMP_DAYOFYEAR,
    TIMESTAMP_DAYS_ADD,
    TIMESTAMP_DAYS_SUB,
    TIMESTAMP_DAY_NAME,
    TIMESTAMP_DIFF,
    TIMESTAMP_FROM_DAYS,
    TIMESTAMP_HOUR,
    TIMESTAMP_HOURS_ADD,
    TIMESTAMP_HOURS_SUB,
    TIMESTAMP_MICROSECOND,
    TIMESTAMP_MICROSECONDS_ADD,
    TIMESTAMP_MICROSECONDS_SUB,
    TIMESTAMP_MINUTE,
    TIMESTAMP_MINUTES_ADD,
    TIMESTAMP_MINUTES_SUB,
    TIMESTAMP_MONTH,
    TIMESTAMP_MONTHS_ADD,
    TIMESTAMP_MONTHS_SUB,
    TIMESTAMP_MONT_NAME,
    TIMESTAMP_NOW,
    TIMESTAMP_SECOND,
    TIMESTAMP_SECONDS_ADD,
    TIMESTAMP_SECONDS_SUB,
    TIMESTAMP_STR_TO_DATE,
    TIMESTAMP_TO_DATE,
    TIMESTAMP_TO_DAYS,
    TIMESTAMP_WEEKOFYEAR,
    TIMESTAMP_WEEKS_ADD,
    TIMESTAMP_WEEKS_SUB,
    TIMESTAMP_YEAR,
    TIMESTAMP_YEARS_ADD,
    TIMESTAMP_YEARS_SUB,
    ADD,
    SUBTRACT,
    MULTIPLY,
    DIVIDE,
    INT_DIVIDE,
    MOD,
    BITAND,
    BITOR,
    BITXOR,
    BITNOT,
    FACTORIAL,
    LAST_OPCODE,
    EQ_FOR_NULL,
    RT_FILTER,
    MATCH_ANY,
    MATCH_ALL,
    MATCH_PHRASE,
    MATCH_ELEMENT_EQ, // DEPRECATED
    MATCH_ELEMENT_LT, // DEPRECATED
    MATCH_ELEMENT_GT, // DEPRECATED
    MATCH_ELEMENT_LE, // DEPRECATED
    MATCH_ELEMENT_GE, // DEPRECATED
    MATCH_PHRASE_PREFIX,
    MATCH_REGEXP,
    MATCH_PHRASE_EDGE,
}
