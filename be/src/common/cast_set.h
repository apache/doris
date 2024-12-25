
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

#include <limits>
#include <type_traits>

#include "common/exception.h"
#include "common/status.h"

namespace doris {

template <typename T, typename U>
void check_cast_value(U b) {
    if constexpr (std::is_unsigned_v<U>) {
        if (b > std::numeric_limits<T>::max()) {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                   "value {} cast  to type {} out of range [{},{}]", b,
                                   typeid(T).name(), std::numeric_limits<T>::min(),
                                   std::numeric_limits<T>::max());
        }
    } else if constexpr (std::is_unsigned_v<T>) {
        if (b < 0 || b > std::numeric_limits<T>::max()) {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                   "value {} cast  to type {} out of range [{},{}]", b,
                                   typeid(T).name(), std::numeric_limits<T>::min(),
                                   std::numeric_limits<T>::max());
        }
    } else {
        if (b < std::numeric_limits<T>::min() || b > std::numeric_limits<T>::max()) {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                   "value {} cast  to type {} out of range [{},{}]", b,
                                   typeid(T).name(), std::numeric_limits<T>::min(),
                                   std::numeric_limits<T>::max());
        }
    }
}

template <typename T, typename U, bool need_check_value = true>
    requires std::is_integral_v<T> && std::is_integral_v<U>
void cast_set(T& a, U b) {
    if constexpr (need_check_value) {
        check_cast_value<T>(b);
    }
    a = static_cast<T>(b);
}

template <typename T, typename U>
    requires std::is_floating_point_v<T> and std::is_integral_v<U>
void cast_set(T& a, U b) {
    a = static_cast<T>(b);
}

template <typename T, typename U, bool need_check_value = true>
    requires std::is_integral_v<T> && std::is_integral_v<U>
T cast_set(U b) {
    if constexpr (need_check_value) {
        check_cast_value<T>(b);
    }
    return static_cast<T>(b);
}

template <typename T, typename U>
    requires std::is_floating_point_v<T> and std::is_integral_v<U>
T cast_set(U b) {
    return static_cast<T>(b);
}

} // namespace doris
