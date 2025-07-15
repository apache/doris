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
    INVALID_OPCODE = 0,
    COMPOUND_NOT = 1,
    COMPOUND_AND = 2,
    COMPOUND_OR = 3,
    CAST = 4,
    FILTER_IN = 5,
    FILTER_NOT_IN = 6,
    FILTER_NEW_IN = 7,
    FILTER_NEW_NOT_IN = 8,
    EQ = 9,
    NE = 10,
    LT = 11,
    LE = 12,
    GT = 13,
    GE = 14,
    CONDITION_IF = 15,
    CONDITION_NULLIF = 16,
    CONDITION_IFNULL = 17,
    CONDITION_COALESCE = 18,
    TIMESTAMP_DATE_FORMAT = 19,
    TIMESTAMP_DAYOFMONTH = 20,
    TIMESTAMP_DAYOFYEAR = 21,
    TIMESTAMP_DAYS_ADD = 22,
    TIMESTAMP_DAYS_SUB = 23,
    TIMESTAMP_DAY_NAME = 24,
    TIMESTAMP_DIFF = 25,
    TIMESTAMP_FROM_DAYS = 26,
    TIMESTAMP_HOUR = 27,
    TIMESTAMP_HOURS_ADD = 28,
    TIMESTAMP_HOURS_SUB = 29,
    TIMESTAMP_MICROSECOND = 30,
    TIMESTAMP_MICROSECONDS_ADD = 31,
    TIMESTAMP_MICROSECONDS_SUB = 32,
    TIMESTAMP_MINUTE = 33,
    TIMESTAMP_MINUTES_ADD = 34,
    TIMESTAMP_MINUTES_SUB = 35,
    TIMESTAMP_MONTH = 36,
    TIMESTAMP_MONTHS_ADD = 37,
    TIMESTAMP_MONTHS_SUB = 38,
    TIMESTAMP_MONT_NAME = 39,
    TIMESTAMP_NOW = 40,
    TIMESTAMP_SECOND = 41,
    TIMESTAMP_SECONDS_ADD = 42,
    TIMESTAMP_SECONDS_SUB = 43,
    TIMESTAMP_STR_TO_DATE = 44,
    TIMESTAMP_TO_DATE = 45,
    TIMESTAMP_TO_DAYS = 46,
    TIMESTAMP_WEEKOFYEAR = 47,
    TIMESTAMP_WEEKS_ADD = 48,
    TIMESTAMP_WEEKS_SUB = 49,
    TIMESTAMP_YEAR = 50,
    TIMESTAMP_YEARS_ADD = 51,
    TIMESTAMP_YEARS_SUB = 52,
    ADD = 53,
    SUBTRACT = 54,
    MULTIPLY = 55,
    DIVIDE = 56,
    INT_DIVIDE = 57,
    MOD = 58,
    BITAND = 59,
    BITOR = 60,
    BITXOR = 61,
    BITNOT = 62,
    FACTORIAL = 63,
    LAST_OPCODE = 64,
    EQ_FOR_NULL = 65,
    RT_FILTER = 66,
    MATCH_ANY = 67,
    MATCH_ALL = 68,
    MATCH_PHRASE = 69,
    MATCH_ELEMENT_EQ = 70, // DEPRECATED
    MATCH_ELEMENT_LT = 71, // DEPRECATED
    MATCH_ELEMENT_GT = 72, // DEPRECATED
    MATCH_ELEMENT_LE = 73, // DEPRECATED
    MATCH_ELEMENT_GE = 74, // DEPRECATED
    MATCH_PHRASE_PREFIX = 75,
    MATCH_REGEXP = 76,
    MATCH_PHRASE_EDGE = 77,
}
