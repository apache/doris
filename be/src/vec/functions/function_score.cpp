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

#include <glog/logging.h>
#include <stddef.h>

#include <memory>

#include "common/status.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris::vectorized {
#include "common/compile_check_begin.h"

class Score : public IFunction {
public:
    static constexpr auto name = "score";

    static FunctionPtr create() { return std::make_shared<Score>(); }

    String get_name() const override { return name; }

    bool use_default_implementation_for_constants() const override { return false; }

    size_t get_number_of_arguments() const override { return 0; }

    bool is_variadic() const override { return false; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeFloat32>());
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        auto col_res = ColumnFloat32::create(input_rows_count, 0.0);
        auto null_map = ColumnUInt8::create(input_rows_count, 0);
        auto nullable_col = ColumnNullable::create(std::move(col_res), std::move(null_map));
        block.replace_by_position(result, std::move(nullable_col));
        return Status::OK();
    }
};

void register_function_score(SimpleFunctionFactory& factory) {
    factory.register_function<Score>();
}

#include "common/compile_check_end.h"
} // namespace doris::vectorized
