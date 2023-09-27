
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

#include "vec/columns/column_decimal.h"
#include "vec/columns/columns_number.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function.h"
#include "vec/functions/function_helpers.h"

namespace doris::vectorized {

template <typename Impl>
class FunctionMathUnaryToNullType : public IFunction {
public:
    using IFunction::execute;

    static constexpr auto name = Impl::name;
    static FunctionPtr create() { return std::make_shared<FunctionMathUnaryToNullType>(); }

private:
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        const auto& arg = arguments.front();
        if (!is_number(arg)) {
            return nullptr;
        }
        return make_nullable(std::make_shared<typename Impl::Type>());
    }

    template <typename T, typename ReturnType>
    static void execute_in_iterations(const T* src_data, ReturnType* dst_data, NullMap& null_map,
                                      size_t size) {
        for (size_t i = 0; i < size; i++) {
            Impl::execute(&src_data[i], &dst_data[i], null_map[i]);
        }
    }

    template <typename T, typename ReturnType>
    static bool execute(Block& block, const ColumnVector<T>* col, const size_t result) {
        const auto& src_data = col->get_data();
        const size_t size = src_data.size();

        auto dst = ColumnVector<ReturnType>::create();
        auto& dst_data = dst->get_data();
        dst_data.resize(size);

        auto null_column = ColumnVector<UInt8>::create();
        auto& null_map = null_column->get_data();
        null_map.resize(size);

        execute_in_iterations(src_data.data(), dst_data.data(), null_map, size);

        block.replace_by_position(result,
                                  ColumnNullable::create(std::move(dst), std::move(null_column)));
        return true;
    }

    template <typename T, typename ReturnType>
    static bool execute(Block& block, const ColumnDecimal<T>* col, const size_t result) {
        const auto& src_data = col->get_data();
        const size_t size = src_data.size();
        UInt32 scale = src_data.get_scale();

        auto dst = ColumnVector<ReturnType>::create();
        auto& dst_data = dst->get_data();
        dst_data.resize(size);

        auto null_column = ColumnVector<UInt8>::create();
        auto& null_map = null_column->get_data();
        null_map.resize(size);

        for (size_t i = 0; i < size; ++i)
            dst_data[i] = convert_from_decimal<DataTypeDecimal<T>, DataTypeNumber<ReturnType>>(
                    src_data[i], scale);

        execute_in_iterations(dst_data.data(), dst_data.data(), null_map, size);

        block.replace_by_position(result,
                                  ColumnNullable::create(std::move(dst), std::move(null_column)));
        return true;
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        const ColumnWithTypeAndName& col = block.get_by_position(arguments[0]);

        auto call = [&](const auto& types) -> bool {
            using Types = std::decay_t<decltype(types)>;
            using Type = typename Types::RightType;
            using ColVecType = std::conditional_t<IsDecimalNumber<Type>, ColumnDecimal<Type>,
                                                  ColumnVector<Type>>;

            const auto col_vec = check_and_get_column<ColVecType>(col.column.get());
            return execute<Type, typename Impl::RetType>(block, col_vec, result);
        };

        if (!call_on_basic_type<void, true, true, true, false>(col.type->get_type_id(), call)) {
            return Status::InvalidArgument("Illegal column {} of argument of function {}",
                                           col.column->get_name(), get_name());
        }
        return Status::OK();
    }
};

} // namespace doris::vectorized
