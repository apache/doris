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
#include <stddef.h>

#include <memory>
#include <ostream>
#include <string>
#include <utility>

#include "common/status.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/common/assert_cast.h"
#include "vec/common/pod_array_fwd.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/functions/function.h"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris::vectorized {

class FunctionArrayCompact : public IFunction {
public:
    static constexpr auto name = "array_compact";
    static FunctionPtr create() { return std::make_shared<FunctionArrayCompact>(); }
    using NullMapType = PaddedPODArray<UInt8>;

    /// Get function name.
    String get_name() const override { return name; }

    bool is_variadic() const override { return false; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DCHECK(is_array(arguments[0]))
                << "first argument for function: " << name << " should be DataTypeArray"
                << " and arguments[0] is " << arguments[0]->get_name();
        return arguments[0];
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        ColumnPtr src_column =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        const auto& src_column_array = check_and_get_column<ColumnArray>(*src_column);
        if (!src_column_array) {
            return Status::RuntimeError(
                    fmt::format("unsupported types for function {}({})", get_name(),
                                block.get_by_position(arguments[0]).type->get_name()));
        }
        const auto& src_offsets = src_column_array->get_offsets();
        const auto* src_nested_column = &src_column_array->get_data();
        DCHECK(src_nested_column != nullptr);

        DataTypePtr src_column_type = block.get_by_position(arguments[0]).type;
        auto nested_type = assert_cast<const DataTypeArray&>(*src_column_type).get_nested_type();
        auto dest_column_ptr = ColumnArray::create(nested_type->create_column(),
                                                   ColumnArray::ColumnOffsets::create());
        IColumn* dest_nested_column = &dest_column_ptr->get_data();
        auto& dest_offsets = dest_column_ptr->get_offsets();
        DCHECK(dest_nested_column != nullptr);

        auto res_val = _execute(*src_nested_column, src_offsets, *dest_nested_column, dest_offsets);
        if (!res_val) {
            return Status::RuntimeError(
                    fmt::format("execute failed or unsupported types for function {}({})",
                                get_name(), block.get_by_position(arguments[0]).type->get_name()));
        }

        block.replace_by_position(result, std::move(dest_column_ptr));
        return Status::OK();
    }

private:
    bool _execute(const IColumn& src_column, const ColumnArray::Offsets64& src_offsets,
                  IColumn& dest_column, ColumnArray::Offsets64& dest_offsets) const {
        ColumnArray::Offset64 src_offsets_size = src_offsets.size();
        ColumnArray::Offset64 src_pos = 0;
        ColumnArray::Offset64 dest_pos = 0;

        for (size_t i = 0; i < src_offsets_size; ++i) {
            auto src_offset = src_offsets[i];
            if (src_pos < src_offset) {
                // Insert first element
                dest_column.insert_from(src_column, src_pos);

                ++src_pos;
                ++dest_pos;

                // For the rest of elements, insert if the element is different from the previous.
                for (; src_pos < src_offset; ++src_pos) {
                    if (0 != (src_column.compare_at(src_pos - 1, src_pos, src_column, 1))) {
                        dest_column.insert_from(src_column, src_pos);
                        ++dest_pos;
                    }
                }
            }
            dest_offsets.push_back(dest_pos);
        }
        return true;
    }
};

} // namespace doris::vectorized