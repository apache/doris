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
    TYPE_IPV4,     /* 13 */
    TYPE_IPV6,     /* 14 */
    TYPE_BINARY,   /* 15 */
    /* 13 */                     // Not implemented
    TYPE_DECIMAL [[deprecated]], /* 16 */
    TYPE_CHAR,                   /* 17 */

    TYPE_STRUCT,    /* 18 */
    TYPE_ARRAY,     /* 19 */
    TYPE_MAP,       /* 20 */
    TYPE_HLL,       /* 21 */
    TYPE_DECIMALV2, /* 22 */

    TYPE_TIME,            /* 23 */
    TYPE_OBJECT,          /* 24 */
    TYPE_STRING,          /* 25 */
    TYPE_QUANTILE_STATE,  /* 26 */
    TYPE_DATEV2,          /* 27 */
    TYPE_DATETIMEV2,      /* 28 */
    TYPE_TIMEV2,          /* 29 */
    TYPE_DECIMAL32,       /* 30 */
    TYPE_DECIMAL64,       /* 31 */
    TYPE_DECIMAL128I,     /* 32 */
    TYPE_JSONB,           /* 33 */
    TYPE_VARIANT,         /* 34 */
    TYPE_LAMBDA_FUNCTION, /* 35 */
    TYPE_AGG_STATE,       /* 36 */
};

}
