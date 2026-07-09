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

#include <fmt/format.h>
#include <glog/logging.h>

#include <memory>
#include <ostream>
#include <type_traits>
#include <utility>

#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_numbers.h"
#include "core/block/column_with_type_and_name.h"
#include "core/block/columns_with_type_and_name.h"
#include "core/call_on_type_index.h"
#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/column/column_array_view.h"
#include "core/column/column_decimal.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/primitive_type.h"
#include "core/pod_array_fwd.h"
#include "core/types.h"
#include "exprs/function/function.h"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris {

class FunctionArrayRemove : public IFunction {
public:
    static constexpr auto name = "array_remove";
    static FunctionPtr create() { return std::make_shared<FunctionArrayRemove>(); }

    /// Get function name.
    String get_name() const override { return name; }

    bool is_variadic() const override { return false; }

    size_t get_number_of_arguments() const override { return 2; }

    bool use_default_implementation_for_nulls() const override { return false; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return arguments[0];
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        // For default implementation of nulls args
        ColumnsWithTypeAndName args = {block.get_by_position(arguments[0]),
                                       block.get_by_position(arguments[1])};

        auto res_column = _execute_dispatch(args, input_rows_count);
        if (!res_column) {
            return Status::RuntimeError(
                    fmt::format("unsupported types for function {}({}, {})", get_name(),
                                block.get_by_position(arguments[0]).type->get_name(),
                                block.get_by_position(arguments[1]).type->get_name()));
        }
        DCHECK_EQ(args[0].column->size(), res_column->size());
        block.replace_by_position(result, std::move(res_column));
        return Status::OK();
    }

private:
    template <PrimitiveType PType>
    ColumnPtr _execute_view(const IColumn& array_data_column,
                            const ColumnArrayView<PType>& array_view,
                            const ColumnView<PType>& right_view) const {
        auto array_nested_column = array_data_column.clone_empty();
        array_nested_column->reserve(array_data_column.size());

        auto dst_offsets_column = ColumnArray::ColumnOffsets::create();
        auto& dst_offsets = dst_offsets_column->get_data();
        dst_offsets.reserve(array_view.size());

        size_t cur = 0;
        for (size_t row = 0; row < array_view.size(); ++row) {
            const auto array_data = array_view[row];
            size_t cur_count = 0;
            for (size_t pos = 0; pos < array_data.size(); ++pos) {
                // Keep null values unless the remove target is also null.
                if (array_data.is_null_at(pos) && right_view.is_null_at(row)) {
                    continue;
                }
                if (array_data.is_null_at(pos) || right_view.is_null_at(row) ||
                    !(array_data.value_at(pos) == right_view.value_at(row))) {
                    array_nested_column->insert_from(array_data_column, array_data.offset + pos);
                    ++cur_count;
                }
            }

            cur += cur_count;
            dst_offsets.push_back(cur);
        }

        auto dst =
                ColumnArray::create(std::move(array_nested_column), std::move(dst_offsets_column));
        if (!array_view.is_nullable()) {
            return dst;
        }

        auto dst_null_column = ColumnUInt8::create(array_view.size(), 0);
        auto& dst_null_map = dst_null_column->get_data();
        for (size_t row = 0; row < array_view.size(); ++row) {
            dst_null_map[row] = array_view.is_null_at(row);
        }
        return ColumnNullable::create(std::move(dst), std::move(dst_null_column));
    }

    ColumnPtr _execute_dispatch(const ColumnsWithTypeAndName& arguments,
                                size_t input_rows_count) const {
        // check array nested column type and get data
        const auto& [left_column, is_const] = unpack_if_const(arguments[0].column);
        const ColumnArray* array_column = nullptr;
        if (const auto* nullable_array = check_and_get_column<ColumnNullable>(left_column.get())) {
            array_column =
                    reinterpret_cast<const ColumnArray*>(&nullable_array->get_nested_column());
            nullable_array->sanity_check();
        } else {
            array_column = reinterpret_cast<const ColumnArray*>(left_column.get());
        }
        DCHECK(is_const ? array_column->get_offsets().size() == 1
                        : array_column->get_offsets().size() == input_rows_count);
        // execute
        auto array_type = remove_nullable(arguments[0].type);
        auto left_element_type = remove_nullable(
                assert_cast<const DataTypeArray*>(array_type.get())->get_nested_type());
        auto right_type = remove_nullable(arguments[1].type);

        auto left_element_primitive_type = left_element_type->get_primitive_type();
        auto right_primitive_type = right_type->get_primitive_type();
        ColumnPtr res = nullptr;
        if (right_primitive_type == left_element_primitive_type ||
            (is_string_type(right_primitive_type) && is_string_type(left_element_primitive_type))) {
            auto call = [&](const auto& type) -> bool {
                using DispatchType = std::decay_t<decltype(type)>;
                constexpr PrimitiveType PType = DispatchType::PType;
                auto array_view = ColumnArrayView<PType>::create(arguments[0].column);
                auto right_view = ColumnView<PType>::create(arguments[1].column);
                res = _execute_view(array_column->get_data(), array_view, right_view);
                return true;
            };

            dispatch_switch_all(left_element_primitive_type, call);
        }
        return res;
    }
};

} // namespace doris
