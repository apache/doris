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

#include "core/column/column_file.h"
#include "core/data_type/data_type_file.h"
#include "core/data_type/data_type_varbinary.h"
#include "exprs/function/function.h"
#include "exprs/function/simple_function_factory.h"

namespace doris {

class FunctionFlGetBinary : public IFunction {
public:
    static constexpr auto name = "fl_get_binary";
    static FunctionPtr create() { return std::make_shared<FunctionFlGetBinary>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 1; }
    bool use_default_implementation_for_constants() const override { return true; }

    DataTypePtr get_return_type_impl(const DataTypes&) const override {
        return make_nullable(std::make_shared<DataTypeVarbinary>());
    }

    Status execute_impl(FunctionContext*, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        const auto& file =
                assert_cast<const ColumnFile&>(*block.get_by_position(arguments[0]).column);
        const auto& inline_column = file.get_column_ptr(DataTypeFile::INLINE_FIELD_INDEX);
        if (!inline_column->has_null()) {
            block.replace_by_position(result, inline_column);
            return Status::OK();
        }

        auto result_column = inline_column->clone_empty();
        result_column->reserve(input_rows_count);
        for (size_t row = 0; row < input_rows_count; ++row) {
            if (!inline_column->is_null_at(row)) {
                result_column->insert_from(*inline_column, row);
            } else {
                result_column->insert_default();
            }
        }
        block.replace_by_position(result, std::move(result_column));
        return Status::OK();
    }
};

void register_function_fl_get_binary(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionFlGetBinary>();
}

} // namespace doris
