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

#include "vec/common/int_exp.h"
#include "vec/core/types.h"

namespace doris::vectorized {

struct DecimalScaleParams {
    enum ScaleType {
        NOT_INIT,
        NO_SCALE,
        SCALE_UP,
        SCALE_DOWN,
    };
    ScaleType scale_type = ScaleType::NOT_INIT;
    int32_t scale_factor = 1;

    template <typename DecimalPrimitiveType>
    static inline constexpr DecimalPrimitiveType get_scale_factor(int32_t n) {
        if constexpr (std::is_same_v<DecimalPrimitiveType, Int32>) {
            return common::exp10_i32(n);
        } else if constexpr (std::is_same_v<DecimalPrimitiveType, Int64>) {
            return common::exp10_i64(n);
        } else if constexpr (std::is_same_v<DecimalPrimitiveType, Int128>) {
            return common::exp10_i128(n);
        } else {
            return DecimalPrimitiveType(1);
        }
    }
};

} // namespace doris::vectorized
