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
#include "vec/common/string_ref.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/functions/array/function_array_binary.h"
#include "vec/functions/array/function_array_set.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {


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

class FunctionArrayIntersect : public IFunction {
public:
    static constexpr auto name = "array_intersect";
    static FunctionPtr create() { return std::make_shared<FunctionArrayIntersect>(); }

    /// Get function name.
    String get_name() const override { return name; }

    bool is_variadic() const override { return true; }

    bool use_default_implementation_for_constants() const override { return true; }

    size_t get_number_of_arguments() const override { return 0; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DCHECK(arguments.size() >= 2)
                << "function: " << get_name() << ", arguments should not less than 2";
        for (size_t i = 0; i < arguments.size(); ++i) {
            DCHECK(is_array(arguments[i])) << i << "-th element is not array type";
            const auto* array_type = check_and_get_data_type<DataTypeArray>(arguments[i].get());
            DCHECK(array_type) << "function: " << get_name() << " " << i + 1
                               << "-th argument is not array";
        }
        DataTypePtr res_data_type =
                ArraySetImpl<SetOperation::INTERSECT>::get_return_type(arguments);
        return res_data_type;
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        CHECK(arguments.size() >= 2);
        const auto& [left_column, left_const] =
                unpack_if_const(block.get_by_position(arguments[0]).column);
        ColumnPtr res_ptr = left_column;
        ColumnArrayExecutionData left_data;
        ColumnArrayExecutionData right_data;
        for (int i = 1; i < arguments.size(); ++i) {
            // extract array column
            left_data.reset();
            right_data.reset();
            const auto& [right_column, right_const] =
                    unpack_if_const(block.get_by_position(arguments[i]).column);
            if (extract_column_array_info(*res_ptr, left_data) &&
                extract_column_array_info(*right_column, right_data)) {
                if (Status st = ArraySetImpl<SetOperation::INTERSECT>::execute(
                            res_ptr, left_data, right_data, i == 1 ? left_const : false,
                            right_const);
                    st != Status::OK()) {
                    return st;
                }
            } else {
                return Status::RuntimeError(
                        fmt::format("execute failed, unsupported types for function {}({}, {})",
                                    get_name(), res_ptr->get_name(), right_column->get_name()));
            }
        }
        block.replace_by_position(result, std::move(res_ptr));
        return Status::OK();
    }
};

void register_function_array_intersect(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionArrayIntersect>();
}

} // namespace doris::vectorized
