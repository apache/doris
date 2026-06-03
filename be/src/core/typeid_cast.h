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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/typeid_cast.h
// and modified by Doris

#pragma once

#include <type_traits>
#include <typeinfo>

#include "common/exception.h"
#include "common/status.h"

template <typename T>
struct TypeIdCastNormalizedType {
    using no_ref_t = std::remove_reference_t<T>;
    using no_cv_t = std::remove_cv_t<no_ref_t>;
    using type =
            std::conditional_t<std::is_pointer_v<no_cv_t>,
                               std::add_pointer_t<std::remove_cv_t<std::remove_pointer_t<no_cv_t>>>,
                               no_cv_t>;
};

template <typename T>
using TypeIdCastNormalizedType_t = typename TypeIdCastNormalizedType<T>::type;

template <typename T>
using TypeIdCastClassType_t = std::remove_pointer_t<TypeIdCastNormalizedType_t<T>>;

/** Checks type by comparing typeid.
  * The exact match of the type is checked. That is, cast to the ancestor will be unsuccessful.
  * In the rest, behaves like a dynamic_cast.
  */

template <typename To, typename From>
To typeid_cast(From* from) {
    static_assert(!std::is_same_v<TypeIdCastNormalizedType_t<To>, TypeIdCastNormalizedType_t<From>>,
                  "typeid_cast is redundant for the same type after removing cv/ref qualifiers");
    static_assert(std::is_class_v<TypeIdCastClassType_t<To>> &&
                          std::is_class_v<TypeIdCastClassType_t<From>>,
                  "typeid_cast requires casting between class pointer types");
    static_assert(std::is_base_of_v<TypeIdCastClassType_t<From>, TypeIdCastClassType_t<To>>,
                  "typeid_cast only supports downcast from a base type to a derived type");

#ifndef NDEBUG
    try {
        if (typeid(*from) == typeid(std::remove_pointer_t<To>)) {
            return static_cast<To>(from);
        }
    } catch (const std::exception& e) {
        throw doris::Exception(doris::ErrorCode::BAD_CAST, e.what());
    }
#else
    if (typeid(*from) == typeid(std::remove_pointer_t<To>)) {
        return static_cast<To>(from);
    }
#endif
    return nullptr;
}
