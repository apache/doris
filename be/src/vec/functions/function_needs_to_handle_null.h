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
#include <boost/mpl/aux_/na_fwd.hpp>

#include "vec/functions/function.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

// Helper struct to store information about const+nullable columns
struct ColumnWithConstAndNullMap {
    const IColumn* nested_col = nullptr;
    const NullMap* null_map = nullptr;
    bool is_const = false;

    bool is_null_at(size_t row) const { return (null_map && (*null_map)[is_const ? 0 : row]); }
};

// For functions that need to handle const+nullable column combinations
// means that functioin `use_default_implementation_for_nulls()` returns false
template <typename Impl, PrimitiveType ResultPrimitiveType>
class FunctionNeedsToHandleNull : public IFunction {
public:
    using ResultColumnType = PrimitiveTypeTraits<ResultPrimitiveType>::ColumnType;

    static constexpr auto name = Impl::name;
    String get_name() const override { return name; }

    static std::shared_ptr<IFunction> create() {
        return std::make_shared<FunctionNeedsToHandleNull>();
    }

    size_t get_number_of_arguments() const override { return Impl::get_number_of_arguments(); }

    bool is_variadic() const override {
        if constexpr (requires { Impl::is_variadic(); }) {
            return Impl::is_variadic();
        }
        return false;
    }

    bool use_default_implementation_for_nulls() const override { return false; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return Impl::get_return_type_impl(arguments);
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        auto res_col = ResultColumnType::create();
        auto null_map = ColumnUInt8::create();
        auto& null_map_data = null_map->get_data();
        res_col->reserve(input_rows_count);
        null_map_data.resize_fill(input_rows_count, 0);

        const size_t arg_size = arguments.size();

        std::vector<ColumnWithConstAndNullMap> columns_info;
        columns_info.resize(arg_size);
        bool has_nullable = false;
        collect_columns_info(columns_info, block, arguments, has_nullable);

        // Check if there is a const null
        for (size_t i = 0; i < arg_size; ++i) {
            if (columns_info[i].is_const && columns_info[i].null_map &&
                (*columns_info[i].null_map)[0] &&
                execute_const_null(res_col, null_map_data, input_rows_count, i)) {
                block.replace_by_position(
                        result, ColumnNullable::create(std::move(res_col), std::move(null_map)));
                return Status::OK();
            }
        }

        Impl::execute(columns_info, res_col, null_map_data, input_rows_count);

        if (is_return_nullable(has_nullable, columns_info)) {
            block.replace_by_position(
                    result, ColumnNullable::create(std::move(res_col), std::move(null_map)));
        } else {
            block.replace_by_position(result, std::move(res_col));
        }

        return Status::OK();
    }

private:
    // Handle a NULL literal
    // Default behavior is fill result with all NULLs
    // return true when the res_col is ready to be written back to the block without further processing
    bool execute_const_null(typename ResultColumnType::MutablePtr& res_col,
                            PaddedPODArray<UInt8>& res_null_map_data, size_t input_rows_count,
                            size_t null_index) const {
        if constexpr (requires {
                          Impl::execute_const_null(res_col, res_null_map_data, input_rows_count,
                                                   null_index);
                      }) {
            return Impl::execute_const_null(res_col, res_null_map_data, input_rows_count,
                                            null_index);
        }

        res_col->insert_many_defaults(input_rows_count);
        res_null_map_data.assign(input_rows_count, (UInt8)1);

        return true;
    }

    // Collect the required information for each column into columns_info
    // Including whether it is a constant column, nested column and null map(if exists).
    void collect_columns_info(std::vector<ColumnWithConstAndNullMap>& columns_info,
                              const Block& block, const ColumnNumbers& arguments,
                              bool& has_nullable) const {
        for (size_t i = 0; i < arguments.size(); ++i) {
            ColumnPtr col_ptr;
            const auto& col_with_type = block.get_by_position(arguments[i]);
            std::tie(col_ptr, columns_info[i].is_const) = unpack_if_const(col_with_type.column);

            if (is_column_nullable(*col_ptr)) {
                has_nullable = true;
                const auto* nullable = check_and_get_column<ColumnNullable>(col_ptr.get());
                columns_info[i].nested_col = &nullable->get_nested_column();
                columns_info[i].null_map = &nullable->get_null_map_data();
            } else {
                columns_info[i].nested_col = col_ptr.get();
            }
        }
    }

    // Determine if the return type should be wrapped in nullable
    // Default behavior is return nullable if any argument is nullable
    bool is_return_nullable(bool has_nullable,
                            const std::vector<ColumnWithConstAndNullMap>& cols_info) const {
        if constexpr (requires { Impl::is_return_nullable(has_nullable, cols_info); }) {
            return Impl::is_return_nullable(has_nullable, cols_info);
        }
        return has_nullable;
    }
};
#include "common/compile_check_end.h"
} // namespace doris::vectorized