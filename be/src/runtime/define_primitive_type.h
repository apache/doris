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

#include <cstdint>

namespace doris {

using PrimitiveNative = uint8_t;

enum PrimitiveType : PrimitiveNative {
    INVALID_TYPE = 0,
    TYPE_NULL,     /* 1 */
    TYPE_BOOLEAN,  /* 2, uint8 */
    TYPE_TINYINT,  /* 3, int8 */
    TYPE_SMALLINT, /* 4, int16 */
    TYPE_INT,      /* 5, int32 */
    TYPE_BIGINT,   /* 6, int64 */
    TYPE_LARGEINT, /* 7, int128 */
    TYPE_FLOAT,    /* 8, float32 */
    TYPE_DOUBLE,   /* 9, float64*/
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
    TYPE_DECIMALV2, /* 20, v2 128bit */

    TYPE_TIME [[deprecated]], /*TYPE_TIMEV2*/

    TYPE_BITMAP,              /* 22, bitmap */
    TYPE_STRING,              /* 23 */
    TYPE_QUANTILE_STATE,      /* 24 */
    TYPE_DATEV2,              /* 25 */
    TYPE_DATETIMEV2,          /* 26 */
    TYPE_TIMEV2,              /* 27 */
    TYPE_DECIMAL32,           /* 28 */
    TYPE_DECIMAL64,           /* 29 */
    TYPE_DECIMAL128I,         /* 30, v3 128bit */
    TYPE_JSONB,               /* 31 */
    TYPE_VARIANT,             /* 32 */
    TYPE_LAMBDA_FUNCTION,     /* 33 */
    TYPE_AGG_STATE,           /* 34 */
    TYPE_DECIMAL256,          /* 35 */
    TYPE_IPV4,                /* 36 */
    TYPE_IPV6,                /* 37 */
    TYPE_UINT32,              /* 38, used as offset */
    TYPE_UINT64,              /* 39, used as offset */
    TYPE_FIXED_LENGTH_OBJECT, /* 40, represent fixed-length object on BE */
    TYPE_VARBINARY            /* 41, varbinary */
};

} // namespace doris
