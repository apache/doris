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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/IntDiv.h
// and modified by Doris

#pragma once

#include "common/compiler_util.h"
#include "common/logging.h"
#include "common/status.h"
#include "runtime/decimalv2_value.h"
#include "type_traits"
#include "vec/columns/column_nullable.h"
#include "vec/common/exception.h"
#include "vec/data_types/number_traits.h"

namespace doris::vectorized {

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"
#pragma GCC diagnostic pop

template <typename A, typename B>
struct DivideIntegralImpl {
    using ResultType = typename NumberTraits::ResultOfIntegerDivision<A, B>::Type;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b, NullMap& null_map, size_t index) {
        null_map[index] = b == 0;

        /// Otherwise overflow may occur due to integer promotion. Example: int8_t(-1) / uint64_t(2).
        /// NOTE: overflow is still possible when dividing large signed number to large unsigned number or vice-versa. But it's less harmful.
        if constexpr (std::is_integral_v<A> && std::is_integral_v<B> &&
                      (std::is_signed_v<A> || std::is_signed_v<B>))
            return std::make_signed_t<A>(a) / (std::make_signed_t<B>(b) + (b == 0));
        else
            return a / (b + (b == 0));
    }
};

} // namespace doris::vectorized
