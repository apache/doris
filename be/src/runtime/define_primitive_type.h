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

namespace doris {
enum PrimitiveType {
    INVALID_TYPE = 0,
    TYPE_NULL,     /* 1 */
    TYPE_BOOLEAN,  /* 2 */
    TYPE_TINYINT,  /* 3 */
    TYPE_SMALLINT, /* 4 */
    TYPE_INT,      /* 5 */
    TYPE_BIGINT,   /* 6 */
    TYPE_LARGEINT, /* 7 */
    TYPE_FLOAT,    /* 8 */
    TYPE_DOUBLE,   /* 9 */
    TYPE_VARCHAR,  /* 10 */
    TYPE_DATE,     /* 11 */
    TYPE_DATETIME, /* 12 */
    TYPE_BINARY,
    /* 13 */                     // Not implemented
    TYPE_DECIMAL [[deprecated]], /* 14 */
    TYPE_CHAR,                   /* 15 */

    TYPE_STRUCT,    /* 16 */
    TYPE_ARRAY,     /* 17 */
    TYPE_MAP,       /* 18 */
    TYPE_HLL,       /* 19 */
    TYPE_DECIMALV2, /* 20 */

    TYPE_TIME,           /* 21 */
    TYPE_OBJECT,         /* 22 */
    TYPE_STRING,         /* 23 */
    TYPE_QUANTILE_STATE, /* 24 */
    TYPE_DATEV2,         /* 25 */
    TYPE_DATETIMEV2,     /* 26 */
    TYPE_TIMEV2,         /* 27 */
    TYPE_DECIMAL32,      /* 28 */
    TYPE_DECIMAL64,      /* 29 */
    TYPE_DECIMAL128I,    /* 30 */
    TYPE_JSONB,          /* 31 */
};

}
