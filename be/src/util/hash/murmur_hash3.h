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
//-----------------------------------------------------------------------------
// MurmurHash3 was written by Austin Appleby, and is placed in the public
// domain. The author hereby disclaims copyright to this source code.

#pragma once

#include <cstdint>
namespace doris {

void murmur_hash3_x86_32(const void* key, int64_t len, uint32_t seed, void* out);

void murmur_hash3_x86_128(const void* key, int len, uint32_t seed, void* out);

void murmur_hash3_x64_process(const void* key, const int len, uint64_t& h1, uint64_t& h2);

void murmur_hash3_x64_128(const void* key, int len, uint32_t seed, void* out);

void murmur_hash3_x64_64_shared(const void* key, const int64_t len, const uint64_t seed, void* out);

void murmur_hash3_x64_64(const void* key, int64_t len, uint64_t seed, void* out);

} // namespace doris