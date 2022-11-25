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

#include "vec/functions/array/function_array_binary.h"
#include "vec/functions/array/function_array_set.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

struct NameArrayIntersect {
    static constexpr auto name = "array_intersect";
};

template <typename Set, typename Element>
struct IntersectAction {
    // True if set has null element
    bool null_flag = false;
    // True if result_set has null element
    bool result_null_flag = false;
    // True if it should execute the left array first.
    static constexpr auto execute_left_column_first = false;

    // Handle Null element.
    // Return true means this null element should put into result column.
    template <bool is_left>
    bool apply_null() {
        if constexpr (is_left) {
            if (!result_null_flag) {
                result_null_flag = true;
                return null_flag;
            }
        } else {
            if (!null_flag) {
                null_flag = true;
            }
        }
        return false;
    }

    // Handle Non-Null element.
    // Return ture means this Non-Null element should put into result column.
    template <bool is_left>
    bool apply(Set& set, Set& result_set, const Element& elem) {
        if constexpr (is_left) {
            if (set.find(elem) && !result_set.find(elem)) {
                result_set.insert(elem);
                return true;
            }
        } else {
            if (!set.find(elem)) {
                set.insert(elem);
            }
        }
        return false;
    }

    void reset() {
        null_flag = false;
        result_null_flag = false;
    }
};

using FunctionArrayIntersect =
        FunctionArrayBinary<ArraySetImpl<SetOperation::INTERSECT>, NameArrayIntersect>;

void register_function_array_intersect(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionArrayIntersect>();
}

} // namespace doris::vectorized
