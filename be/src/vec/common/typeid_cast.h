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

#include <string>
#include <type_traits>
#include <typeindex>
#include <typeinfo>

#include "common/status.h"
#include "vec/common/demangle.h"
#include "vec/common/exception.h"

/** Checks type by comparing typeid.
  * The exact match of the type is checked. That is, cast to the ancestor will be unsuccessful.
  * In the rest, behaves like a dynamic_cast.
  */
template <typename To, typename From>
std::enable_if_t<std::is_reference_v<To>, To> typeid_cast(From& from) {
    try {
        if (typeid(from) == typeid(To)) {
            return static_cast<To>(from);
        }
    } catch (const std::exception& e) {
        throw doris::vectorized::Exception(e.what(), doris::TStatusCode::VEC_BAD_CAST);
    }

    throw doris::vectorized::Exception("Bad cast from type " + demangle(typeid(from).name()) +
                                               " to " + demangle(typeid(To).name()),
                                       doris::TStatusCode::VEC_BAD_CAST);
}

template <typename To, typename From>
To typeid_cast(From* from) {
    try {
        if (typeid(*from) == typeid(std::remove_pointer_t<To>)) {
            return static_cast<To>(from);
        } else {
            return nullptr;
        }
    } catch (const std::exception& e) {
        throw doris::vectorized::Exception(e.what(), doris::TStatusCode::VEC_BAD_CAST);
    }
}
