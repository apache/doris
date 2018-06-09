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
