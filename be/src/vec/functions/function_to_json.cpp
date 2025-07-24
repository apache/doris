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

#include "vec/data_types/data_type_jsonb.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

class FunctionToJson : public IFunction {
public:
    static constexpr auto name = "to_json";

    static FunctionPtr create() { return std::make_shared<FunctionToJson>(); }
    using NullMapType = PaddedPODArray<UInt8>;

    String get_name() const override { return name; }

    bool is_variadic() const override { return false; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeJsonb>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        auto to_column = ColumnString::create();
        auto from_type_serde = block.get_by_position(arguments[0]).type->get_serde();
        auto from_column = block.get_by_position(arguments[0]).column;
        RETURN_IF_ERROR(
                from_type_serde->serialize_column_to_jsonb_vector(*from_column, *to_column));
        block.get_by_position(result).column = std::move(to_column);
        return Status::OK();
    }
};

void register_function_to_json(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionToJson>();
}

} // namespace doris::vectorized
