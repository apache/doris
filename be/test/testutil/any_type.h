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

#include <any>
#include <typeinfo>
#include <variant>

namespace doris {

// Typeinfo for __int128 is missing on macOS, we can't use std::any directly.
struct AnyType : public std::variant<std::any, __int128_t> {
    AnyType() = default;

    template <typename T>
    AnyType(T value) {
        this->emplace<std::any>(std::move(value));
    }

    AnyType(__int128_t value) { this->emplace<__int128_t>(value); }

    const std::type_info* type() const {
        if (std::holds_alternative<std::any>(*this)) {
            const auto& type_info = std::get<std::any>(*this).type();
            return &type_info;
        } else {
#ifdef __APPLE__
            return nullptr;
#else
            static const auto& type_info = typeid(__int128_t);
            return &type_info;
#endif
        }
    }

    template <class ValueType>
    friend ValueType any_cast(const AnyType& operand);
    template <class ValueType>
    friend ValueType any_cast(AnyType& operand);
    template <class ValueType>
    friend ValueType any_cast(AnyType&& operand);
};

template <class ValueType>
inline ValueType any_cast(const AnyType& operand) {
    return std::any_cast<ValueType>(std::get<std::any>(operand));
}
template <>
inline __int128_t any_cast(const AnyType& operand) {
    return std::get<__int128_t>(operand);
}

template <class ValueType>
inline ValueType any_cast(AnyType& operand) {
    return std::any_cast<ValueType>(std::get<std::any>(operand));
}
template <>
inline __int128_t any_cast(AnyType& operand) {
    return std::get<__int128_t>(operand);
}

template <class ValueType>
inline ValueType any_cast(AnyType&& operand) {
    return std::any_cast<ValueType>(std::get<std::any>(operand));
}
template <>
inline __int128_t any_cast(AnyType&& operand) {
    return std::get<__int128_t>(operand);
}

} // namespace doris
