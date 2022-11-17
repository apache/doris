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

#include "vec/columns/column_number.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {
class FunctionWidthBucket: public IFunction {
public:
    static constexpr auto name = "width_bucket";
    static FunctionPtr create() { return std::make_shared<FunctionWidthBucket>(); }

    /// Get function name.
    String get_name() const override { return name; }

    bool is_variadic() const override { return false; }

    size_t get_number_of_arguments() const override { return 4; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypesInt64>);
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        
        auto expr = block.get_by_position(arguments[0]).convert_to_full_column_if_const();
        auto min_value = block.get_by_position(arguments[1]).convert_to_full_column_if_const();
        auto max_value = block.get_by_position(arguments[2]).convert_to_full_column_if_const();
        auto num_buckets= block.get_by_position(arguments[3]).convert_to_full_column_if_const();

        auto nested_column_ptr = ColumnInt64::create(input_rows_count, 0);

        for(size_t i=0;i<input_rows_count;++i){
            if(expr[i]<min_value){
                continue;
            }
            else if (expr[i]>max_value){
                nested_column_ptr[i]=num_buckets+1;
            }else{
                nested_column_ptr[i]=(expr[i]-min_value[i])/((max_value[i]-min_value[i])/num_buckets[i]);
            }
        }

        auto dest_column_ptr = ColumnNullable::create(std::move(nested_column_ptr),ColumnUInt8::create(nested_column_ptr->size(), 0));
        block.replace_by_position(result, std::move(dest_column_ptr));
        return Status::OK();
    }

};

void register_function_width_bucket(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionWidthBucket>();
}

} // namespace doris::vectorized