
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

#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function.h"

namespace doris::vectorized {

template <typename Impl>
class FunctionMathLog : public IFunction {
public:
    using IFunction::execute;

    static constexpr auto name = Impl::name;
    static FunctionPtr create() { return std::make_shared<FunctionMathLog>(); }

private:
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeFloat64>());
    }

    static void execute_in_iterations(const double* src_data, double* dst_data, NullMap& null_map,
                                      size_t size) {
        for (size_t i = 0; i < size; i++) {
            null_map[i] = src_data[i] <= 0;
            Impl::execute(&src_data[i], &dst_data[i]);
        }
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        const auto* col =
                assert_cast<const ColumnFloat64*>(block.get_by_position(arguments[0]).column.get());

        const auto& src_data = col->get_data();
        const size_t size = src_data.size();

        auto dst = ColumnFloat64::create();
        auto& dst_data = dst->get_data();
        dst_data.resize(size);

        auto null_column = ColumnVector<UInt8>::create();
        auto& null_map = null_column->get_data();
        null_map.resize(size);

        execute_in_iterations(col->get_data().data(), dst_data.data(), null_map, size);

        block.replace_by_position(result,
                                  ColumnNullable::create(std::move(dst), std::move(null_column)));
        return Status::OK();
    }
};

struct ImplLog10 {
    static constexpr auto name = "log10";
    static void execute(const double* src, double* dst) { *dst = std::log10(*src); }
};

struct ImplLog2 {
    static constexpr auto name = "log2";
    static void execute(const double* src, double* dst) { *dst = std::log2(*src); }
};

struct ImplLn {
    static constexpr auto name = "ln";
    static void execute(const double* src, double* dst) { *dst = std::log(*src); }
};

} // namespace doris::vectorized
