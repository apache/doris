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

#include "vec/columns/column_const.h"
#include "vec/common/assert_cast.h"
#include "vec/functions/array/function_array_binary.h"
#include "vec/functions/array/function_array_map.h"
#include "vec/functions/array/function_array_nary.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

#include "common/compile_check_begin.h"
struct NameArrayIntersect {
    static constexpr auto name = "array_intersect";
};

template <typename Map, typename ColumnType>
struct IntersectAction {
    // True if current has null element
    bool current_null_flag = false;
    // True if result_set has null element
    bool result_null_flag = true;
    // True if it should execute the left array first.
    typename Map::mapped_type* value = nullptr;

    // Handle Null element.
    bool apply_null() { return result_null_flag; }

    // Handle Non-Null element.
    void apply(Map& map, const size_t arg_idx, const size_t row_idx,
               const ColumnArrayExecutionData& param) {
        current_null_flag = false;
        size_t start_off = (*param.offsets_ptr)[row_idx - 1];
        size_t end_off = (*param.offsets_ptr)[row_idx];
        for (size_t off = start_off; off < end_off; ++off) {
            if (param.nested_nullmap_data && param.nested_nullmap_data[off]) {
                current_null_flag = true;
            } else {
                if constexpr (std::is_same_v<ColumnString, ColumnType>) {
                    value = &map[param.nested_col->get_data_at(off)];
                } else {
                    auto& data_col = static_cast<const ColumnType&>(*param.nested_col);
                    value = &map[data_col.get_element(off)];
                }
                if (*value == arg_idx) {
                    ++(*value);
                }
            }
        }
        if (!current_null_flag) {
            result_null_flag = false;
        }
    }

    void reset() {
        current_null_flag = false;
        result_null_flag = true;
    }
};

using FunctionArrayIntersect =
        FunctionArrayNary<ArrayMapImpl<MapOperation::INTERSECT>, NameArrayIntersect>;

void register_function_array_intersect(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionArrayIntersect>();
}

} // namespace doris::vectorized
