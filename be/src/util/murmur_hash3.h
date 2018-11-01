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

#ifndef BDG_PALO_BE_SRC_UTIL_MURMUR_HASH3_H
#define BDG_PALO_BE_SRC_UTIL_MURMUR_HASH3_H

//-----------------------------------------------------------------------------
// Platform-specific functions and macros

// Microsoft Visual Studio

#if defined(_MSC_VER) && (_MSC_VER < 1600)

typedef unsigned char uint8_t;
typedef unsigned int uint32_t;
typedef unsigned __int64 uint64_t;

// Other compilers

#else   // defined(_MSC_VER)

#include <stdint.h>

#endif // !defined(_MSC_VER)

//-----------------------------------------------------------------------------

void murmur_hash3_x86_32(const void* key, int len, uint32_t seed, void* out);

void murmur_hash3_x86_128(const void* key, int len, uint32_t seed, void* out);

void murmur_hash3_x64_128(const void* key, int len, uint32_t seed, void* out);

void murmur_hash3_x64_64(const void* key, int len, uint64_t seed, void* out);

//-----------------------------------------------------------------------------

#endif // BDG_PALO_BE_SRC_UTIL_MURMUR_HASH3_H
