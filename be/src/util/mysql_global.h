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

#include <float.h>
#include <stdint.h>

namespace doris {

typedef unsigned char uchar;

#define int1store(T, A) *((uint8_t*)(T)) = (uint8_t)(A)
#define int2store(T, A) *((uint16_t*)(T)) = (uint16_t)(A)
#define int3store(T, A)                           \
    do {                                          \
        *(T) = (uchar)((A));                      \
        *(T + 1) = (uchar)(((uint32_t)(A) >> 8)); \
        *(T + 2) = (uchar)(((A) >> 16));          \
    } while (0)
#define int8store(T, A) *((int64_t*)(T)) = (uint64_t)(A)

#define MY_ALIGN(A, L) (((A) + (L)-1) & ~((L)-1))
#define SIZEOF_CHARP 8

#define MAX_TINYINT_WIDTH 3     /* Max width for a TINY w.o. sign */
#define MAX_SMALLINT_WIDTH 5    /* Max width for a SHORT w.o. sign */
#define MAX_MEDIUMINT_WIDTH 8   /* Max width for a INT24 w.o. sign */
#define MAX_INT_WIDTH 10        /* Max width for a LONG w.o. sign */
#define MAX_BIGINT_WIDTH 20     /* Max width for a LONGLONG */
#define MAX_LARGEINT_WIDTH 39   /* Max width for a LARGEINT */
#define MAX_CHAR_WIDTH 255      /* Max length for a CHAR column */
#define MAX_BLOB_WIDTH 16777216 /* Default width for blob */
#define MAX_TIME_WIDTH 10       /* Max width for a TIME HH:MM:SS*/
#define MAX_DECPT_FOR_F_FORMAT DBL_DIG
#define MAX_DATETIME_WIDTH 27 /* YYYY-MM-DD HH:MM:SS.ssssss */
#define MAX_DECIMAL_WIDTH 29  /* Max width for a DECIMAL */

/* -[digits].E+## */
#define MAX_FLOAT_STR_LENGTH 24 // see gutil/strings/numbers.h kFloatToBufferSize
/* -[digits].E+### */
#define MAX_DOUBLE_STR_LENGTH 32 // see gutil/strings/numbers.h kDoubleToBufferSize

/* -[digits].[frac] */
#define MAX_DECIMAL_STR_LENGTH 29

} // namespace doris
