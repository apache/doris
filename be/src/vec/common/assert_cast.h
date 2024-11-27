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

#include "common/logging.h"
#include "vec/common/demangle.h"

enum class TypeCheckOnRelease : bool { ENABLE = true, DISABLE = false };

/** Perform static_cast in release build when TypeCheckOnRelease is set to DISABLE.
  * Checks type by comparing typeid and throw an exception in all the other situations.
  * The exact match of the type is checked. That is, cast to the ancestor will be unsuccessful.
  */
template <typename To, TypeCheckOnRelease check = TypeCheckOnRelease::ENABLE, typename From>
PURE To assert_cast(From&& from) {
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
                LOG(FATAL) << fmt::format("Bad cast from type:{}* to {}",
                                          demangle(typeid(*from).name()),
                                          demangle(typeid(To).name()));
            }
        } else {
            if (typeid(from) == typeid(To)) {
                return static_cast<To>(from);
            }
        }
        LOG(FATAL) << fmt::format("Bad cast from type:{} to {}", demangle(typeid(from).name()),
                                  demangle(typeid(To).name()));
        __builtin_unreachable();
    };

#ifndef NDEBUG
    try {
        return perform_cast(std::forward<From>(from));
    } catch (const std::exception& e) {
        LOG(FATAL) << "assert cast err:" << e.what();
    }
    __builtin_unreachable();
#else
    if constexpr (check == TypeCheckOnRelease::ENABLE) {
        try {
            return perform_cast(std::forward<From>(from));
        } catch (const std::exception& e) {
            LOG(FATAL) << "assert cast err:" << e.what();
        }
        __builtin_unreachable();
    } else {
        return static_cast<To>(from);
    }
#endif
}
