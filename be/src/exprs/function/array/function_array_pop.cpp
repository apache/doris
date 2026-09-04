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

#include <fmt/format.h>
#include <glog/logging.h>

#include <cstddef>
#include <memory>
#include <ostream>
#include <utility>

#include "common/compiler_util.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_numbers.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_nullable.h"
#include "core/types.h"
#include "exprs/aggregate/aggregate_function.h"
#include "exprs/function/array/function_array_utils.h"
#include "exprs/function/function.h"
#include "exprs/function/simple_function_factory.h"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris {

template <typename PopType>
class FunctionArrayPop : public IFunction {
public:
    static FunctionPtr create() { return std::make_shared<PopType>(); }

    /// Get function name.
    String get_name() const override { return PopType::name; }

    bool is_variadic() const override { return false; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DCHECK(arguments[0]->get_primitive_type() == TYPE_ARRAY)
                << "First argument for function: " << PopType::name
                << " should be DataTypeArray but it has type " << arguments[0]->get_name() << ".";
        return arguments[0];
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        auto array_column =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        // extract src array column
        ColumnArrayExecutionData src;
        if (!extract_column_array_info(*array_column, src)) {
            return Status::RuntimeError(
                    fmt::format("execute failed, unsupported types for function {}({})", get_name(),
                                block.get_by_position(arguments[0]).type->get_name()));
        }
        // prepare dst array column
        bool is_nullable = src.nested_nullmap_data != nullptr;
        ColumnArrayMutableData dst = create_mutable_data(src.nested_col.get(), is_nullable);
        dst.offsets_ptr->reserve(input_rows_count);
        // start from index depending on the PopType::start_offset
        auto offset_column = ColumnInt64::create(array_column->size(), PopType::start_offset);
        // len - 1
        auto length_column = ColumnInt64::create();
        for (size_t row = 0; row < src.offsets_ptr->size(); ++row) {
            size_t off = (*src.offsets_ptr)[row - 1];
            size_t len = (*src.offsets_ptr)[row] - off;
            length_column->insert_value(len - 1);
        }
        slice_array(dst, src, *offset_column, length_column.get());
        ColumnPtr res_column = assemble_column_array(dst);
        block.replace_by_position(result, std::move(res_column));
        return Status::OK();
    }
};

class FunctionArrayPopback : public FunctionArrayPop<FunctionArrayPopback> {
public:
    static constexpr auto name = "array_popback";
    static constexpr int start_offset = 1;
};

class FunctionArrayPopfront : public FunctionArrayPop<FunctionArrayPopfront> {
public:
    static constexpr auto name = "array_popfront";
    static constexpr int start_offset = 2;
};

class FunctionArrayTrim : public IFunction {
public:
    static constexpr auto name = "trim_array";
    static FunctionPtr create() { return std::make_shared<FunctionArrayTrim>(); }

    String get_name() const override { return name; }

    bool is_variadic() const override { return false; }

    size_t get_number_of_arguments() const override { return 2; }

    bool use_default_implementation_for_nulls() const override { return false; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        const auto array_type = remove_nullable(arguments[0]);
        DCHECK(array_type->get_primitive_type() == TYPE_ARRAY)
                << "First argument for function: " << name
                << " should be DataTypeArray but it has type " << arguments[0]->get_name() << ".";
        DCHECK(remove_nullable(arguments[1])->get_primitive_type() == TYPE_BIGINT)
                << "Second argument for function: " << name << " should be BigInt but it has type "
                << arguments[1]->get_name() << ".";
        if (arguments[0]->is_nullable() || arguments[1]->is_nullable()) {
            return make_nullable(array_type);
        }
        return array_type;
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        const auto& [array_column, array_is_const] =
                unpack_if_const(block.get_by_position(arguments[0]).column);
        const auto& [size_column, size_is_const] =
                unpack_if_const(block.get_by_position(arguments[1]).column);

        ColumnArrayExecutionData src;
        if (!extract_column_array_info(*array_column, src)) {
            return Status::RuntimeError(
                    fmt::format("execute failed, unsupported types for function {}({}, {})",
                                get_name(), block.get_by_position(arguments[0]).type->get_name(),
                                block.get_by_position(arguments[1]).type->get_name()));
        }

        const UInt8* size_null_map = nullptr;
        const IColumn* size_data_column = size_column.get();
        if (const auto* nullable_size = check_and_get_column<ColumnNullable>(size_data_column)) {
            size_null_map = nullable_size->get_null_map_data().data();
            size_data_column = &nullable_size->get_nested_column();
        }
        const auto& sizes = assert_cast<const ColumnInt64&>(*size_data_column).get_data();

        auto result_array = ColumnArray::create(src.array_col->get_data_ptr()->clone_empty(),
                                                ColumnArray::ColumnOffsets::create());
        auto& result_data = result_array->get_data();
        auto& result_offsets = result_array->get_offsets();
        result_data.reserve(src.array_col->get_data().size());
        result_offsets.resize(input_rows_count);

        auto result_null_map = ColumnUInt8::create(input_rows_count, 0);
        auto& result_null_map_data = result_null_map->get_data();
        size_t result_offset = 0;
        for (size_t row = 0; row < input_rows_count; ++row) {
            const size_t array_row = index_check_const(row, array_is_const);
            const size_t size_row = index_check_const(row, size_is_const);
            const bool is_null = (src.array_nullmap_data && src.array_nullmap_data[array_row]) ||
                                 (size_null_map && size_null_map[size_row]);
            result_null_map_data[row] = is_null;
            if (is_null) {
                result_offsets[row] = result_offset;
                continue;
            }

            const auto size = sizes[size_row];
            const size_t offset = (*src.offsets_ptr)[array_row - 1];
            const size_t cardinality = (*src.offsets_ptr)[array_row] - offset;
            if (UNLIKELY(size < 0)) {
                return Status::InvalidArgument("size must not be negative: {}", size);
            }
            if (UNLIKELY(static_cast<size_t>(size) > cardinality)) {
                return Status::InvalidArgument("size must not exceed array cardinality {}: {}",
                                               cardinality, size);
            }

            const size_t keep = cardinality - size;
            if (keep > 0) {
                result_data.insert_range_from(src.array_col->get_data(), offset, keep);
            }
            result_offset += keep;
            result_offsets[row] = result_offset;
        }

        if (block.get_by_position(result).type->is_nullable()) {
            block.replace_by_position(result, ColumnNullable::create(std::move(result_array),
                                                                     std::move(result_null_map)));
        } else {
            block.replace_by_position(result, std::move(result_array));
        }
        return Status::OK();
    }
};

void register_function_array_pop(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionArrayPopback>();
    factory.register_function<FunctionArrayPopfront>();
    factory.register_function<FunctionArrayTrim>();
}

} // namespace doris
