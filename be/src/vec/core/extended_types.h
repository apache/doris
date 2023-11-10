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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/base/base/extended_types.h
// and modified by Doris
#pragma once

#include <type_traits>

#include "wide_integer.h"

static_assert(sizeof(wide::Int256) == 32);
static_assert(sizeof(wide::UInt256) == 32);

/// The standard library type traits, such as std::is_arithmetic, with one exception
/// (std::common_type), are "set in stone". Attempting to specialize them causes undefined behavior.
/// So instead of using the std type_traits, we use our own version which allows extension.
namespace wide {
template <class T>
concept is_integer = std::is_integral_v<T> || std::is_same_v<T, wide::Int256> ||
                     std::is_same_v<T, wide::UInt256>;
}
template <>
struct std::is_signed<wide::Int256> {
    static constexpr bool value = true;
};
template <>
struct std::is_signed<wide::UInt256> {
    static constexpr bool value = false;
};

template <>
struct std::is_unsigned<wide::Int256> {
    static constexpr bool value = false;
};
template <>
struct std::is_unsigned<wide::UInt256> {
    static constexpr bool value = true;
};
template <>
struct std::make_unsigned<wide::Int256> {
    using type = wide::UInt256;
};
template <>
struct std::make_unsigned<wide::UInt256> {
    using type = wide::UInt256;
};

template <>
struct std::make_signed<wide::Int256> {
    using type = wide::Int256;
};
template <>
struct std::make_signed<wide::UInt256> {
    using type = wide::Int256;
};