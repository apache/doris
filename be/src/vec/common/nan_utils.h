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

#include <cmath>
#include <limits>
#include <type_traits>

/// To be sure, that this function is zero-cost for non-floating point types.
template <typename T>
inline std::enable_if_t<std::is_floating_point_v<T>, bool> is_nan(T x) {
    return std::isnan(x);
}

template <typename T>
inline std::enable_if_t<!std::is_floating_point_v<T>, bool> is_nan(T) {
    return false;
}

template <typename T>
inline std::enable_if_t<std::is_floating_point_v<T>, bool> is_finite(T x) {
    return std::isfinite(x);
}

template <typename T>
inline std::enable_if_t<!std::is_floating_point_v<T>, bool> is_finite(T) {
    return true;
}

template <typename T>
std::enable_if_t<std::is_floating_point_v<T>, T> nan_or_zero() {
    return std::numeric_limits<T>::quiet_NaN();
}

template <typename T>
std::enable_if_t<std::numeric_limits<T>::is_integer, T> nan_or_zero() {
    return 0;
}

template <typename T>
std::enable_if_t<std::is_class_v<T>, T> nan_or_zero() {
    return T {};
}

#if 1 /// __int128
template <typename T>
std::enable_if_t<std::is_same_v<T, __int128> && !std::numeric_limits<T>::is_integer, __int128>
nan_or_zero() {
    return __int128(0);
}
#endif
