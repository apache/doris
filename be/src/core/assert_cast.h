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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/assert_cast.h
// and modified by Doris

#pragma once

#include <type_traits>
#include <typeinfo>

#include "common/demangle.h"
#include "common/exception.h"
#include "common/logging.h"

enum class TypeCheckOnRelease : bool { ENABLE = true, DISABLE = false };

template <typename T>
struct AssertCastNormalizedType {
    using no_ref_t = std::remove_reference_t<T>;
    using no_cv_t = std::remove_cv_t<no_ref_t>;
    using type =
            std::conditional_t<std::is_pointer_v<no_cv_t>,
                               std::add_pointer_t<std::remove_cv_t<std::remove_pointer_t<no_cv_t>>>,
                               no_cv_t>;
};

template <typename T>
using AssertCastNormalizedType_t = typename AssertCastNormalizedType<T>::type;

template <typename T>
using AssertCastClassType_t = std::remove_pointer_t<AssertCastNormalizedType_t<T>>;

/** Perform static_cast in release build when TypeCheckOnRelease is set to DISABLE.
  * Checks type by comparing typeid and throw an exception in all the other situations.
  * The exact match of the type is checked. That is, cast to the ancestor will be unsuccessful.
  */
template <typename To, TypeCheckOnRelease check = TypeCheckOnRelease::ENABLE, typename From>
To assert_cast(From&& from) {
    static_assert(!std::is_same_v<AssertCastNormalizedType_t<To>, AssertCastNormalizedType_t<From>>,
                  "assert_cast is redundant for the same type after removing cv/ref qualifiers");
    static_assert(std::is_class_v<AssertCastClassType_t<To>> &&
                          std::is_class_v<AssertCastClassType_t<From>>,
                  "assert_cast requires casting between class pointer/reference types");
    static_assert(std::is_base_of_v<AssertCastClassType_t<From>, AssertCastClassType_t<To>>,
                  "assert_cast only supports downcast from a base type to a derived type");

    // https://godbolt.org/z/nrsx7nYhs
    // perform_cast will not be compiled to asm in release build with TypeCheckOnRelease::DISABLE
    auto perform_cast = [](auto&& from) -> To {
        if constexpr (std::is_pointer_v<To>) {
            if (typeid(*from) == typeid(std::remove_pointer_t<To>)) {
                return static_cast<To>(from);
            }
            if constexpr (std::is_pointer_v<std::remove_reference_t<From>>) {
                if (auto ptr = dynamic_cast<To>(from); ptr != nullptr) {
                    return ptr;
                }
                throw doris::Exception(doris::Status::FatalError("Bad cast from type:{}* to {}",
                                                                 demangle(typeid(*from).name()),
                                                                 demangle(typeid(To).name())));
            }
        } else {
            if (typeid(from) == typeid(To)) {
                return static_cast<To>(from);
            }
        }
        throw doris::Exception(doris::Status::FatalError("Bad cast from type:{} to {}",
                                                         demangle(typeid(from).name()),
                                                         demangle(typeid(To).name())));
    };

#ifndef NDEBUG
    try {
        return perform_cast(std::forward<From>(from));
    } catch (const std::exception& e) {
        throw doris::Exception(doris::Status::FatalError("assert cast err:{}", e.what()));
    }
#else
    if constexpr (check == TypeCheckOnRelease::ENABLE) {
        try {
            return perform_cast(std::forward<From>(from));
        } catch (const std::exception& e) {
            throw doris::Exception(doris::Status::FatalError("assert cast err:{}", e.what()));
        }
    } else {
        return static_cast<To>(from);
    }
#endif
}
