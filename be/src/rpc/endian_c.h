// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef BDG_PALO_BE_SRC_RPC_ENDIAN_C_H
#define BDG_PALO_BE_SRC_RPC_ENDIAN_C_H

/* GNU libc offers the helpful header <endian.h> which defines __BYTE_ORDER */
#if defined (__GLIBC__)
# include <endian.h>
# if (__BYTE_ORDER == __LITTLE_ENDIAN)
#  define HT_LITTLE_ENDIAN
# elif (__BYTE_ORDER == __BIG_ENDIAN)
#  define HT_BIG_ENDIAN
# elif (__BYTE_ORDER == __PDP_ENDIAN)
#  define HT_PDP_ENDIAN
# else
#  error Unknown machine endianness detected.
# endif
# define HT_BYTE_ORDER __BYTE_ORDER
#elif defined(__sparc) || defined(__sparc__) \
    || defined(_POWER) || defined(__powerpc__) \
|| defined(__ppc__) || defined(__hpux) \
|| defined(_MIPSEB) || defined(_POWER) \
|| defined(__s390__)

# define HT_BIG_ENDIAN
# define HT_BYTE_ORDER 4321
#elif defined(__i386__) || defined(__alpha__) \
    || defined(__ia64) || defined(__ia64__) \
|| defined(_M_IX86) || defined(_M_IA64) \
|| defined(_M_ALPHA) || defined(__amd64) \
|| defined(__amd64__) || defined(_M_AMD64) \
|| defined(__x86_64) || defined(__x86_64__) \
|| defined(_M_X64)
# define HT_LITTLE_ENDIAN
# define HT_BYTE_ORDER 1234
#else
# error The file endian.h needs to be set up for your CPU type.
#endif

#endif //BDG_PALO_BE_SRC_RPC_ENDIAN_C_H
