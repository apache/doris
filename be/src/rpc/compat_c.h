// Copyright (C) 2007-2016 Hypertable, Inc.
//
// This file is part of Hypertable.
// 
// Hypertable is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 3
// of the License, or any later version.
//
// Hypertable is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
// 02110-1301, USA.

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
