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

#include "vec/columns/column_const.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_struct.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_struct.h"
#include "vec/functions/function.h"
#include "vec/functions/function_helpers.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

class FunctionStructElement : public IFunction {
public:
    static constexpr auto name = "struct_element";
    static FunctionPtr create() { return std::make_shared<FunctionStructElement>(); }

    // Get function name.
    String get_name() const override { return name; }

    bool is_variadic() const override { return true; }

    bool use_default_implementation_for_nulls() const override { return false; }

    size_t get_number_of_arguments() const override { return 2; }
    ColumnNumbers get_arguments_that_are_always_constant() const override { return {1}; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DCHECK(is_struct(remove_nullable(arguments[0])))
                << "First argument for function: " << name
                << " should be DataTypeStruct but it has type " << arguments[0]->get_name() << ".";
        // Due to the inability to get the actual value of the index column
        // in function's build stage, we directly return nothing here.
        // Todo(xy): Is there any good way to return right type?
        return make_nullable(std::make_shared<DataTypeNothing>());
    }
};

} // namespace doris::vectorized

