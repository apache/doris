// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

#ifndef BDG_PALO_BE_SRC_RPC_COMPAT_C_H
#define BDG_PALO_BE_SRC_RPC_COMPAT_C_H

/** @defgroup Common Common
 * General purpose utility library.
 * The %Common library contains general purpose utility classes used by all
 * other modules within palo.  It is not dependent on any other library
 * defined in %palo.
 */
/* Portability macros for C code. */

#ifdef __cplusplus
#  define HT_EXTERN_C  extern "C"
#else
#  define HT_EXTERN_C  extern
#endif

/* Calling convention */
#ifdef _MSC_VER
#  define HT_CDECL      __cdecl
#else
#  define HT_CDECL
#endif

#define HT_PUBLIC(ret_type)     ret_type HT_CDECL
#define HT_EXTERN(ret_type)     HT_EXTERN_C HT_PUBLIC(ret_type)

#ifdef __GNUC__
#  define HT_NORETURN __attribute__((__noreturn__))
#  define HT_FORMAT(x) __attribute__((format x))
#  define HT_FUNC __PRETTY_FUNCTION__
#  define HT_COND(x, _prob_) __builtin_expect(x, _prob_)
#else
#  define HT_NORETURN
#  define HT_FORMAT(x)
#  ifndef __attribute__
#    define __attribute__(x)
#  endif
#  define HT_FUNC __func__
#  define HT_COND(x, _prob_) (x)
#endif

#define HT_LIKELY(x) HT_COND(x, 1)
#define HT_UNLIKELY(x) HT_COND(x, 0)
/* We want C limit macros, even when using C++ compilers */
#ifndef __STDC_LIMIT_MACROS
#  define __STDC_LIMIT_MACROS
#endif

#include <stdint.h>
#include <stddef.h>
#include <string.h>
#include <strings.h>
#include <stdlib.h>
#include <unistd.h>

#endif //BDG_PALO_BE_SRC_RPC_COMPAT_C_H
