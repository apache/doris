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

#include <string.h>

#include <type_traits>

template <typename T>
inline T unaligned_load(const void* address) {
    T res {};
    memcpy(&res, address, sizeof(res));
    return res;
}

/// We've had troubles before with wrong store size due to integral promotions
/// (e.g., unaligned_store(dest, uint16_t + uint16_t) stores an uint32_t).
/// To prevent this, make the caller specify the stored type explicitly.
/// To disable deduction of T, wrap the argument type with std::enable_if.
template <typename T>
inline void unaligned_store(void* address, const typename std::enable_if<true, T>::type& src) {
    static_assert(std::is_trivially_copyable_v<T>);
    memcpy(address, &src, sizeof(src));
}
