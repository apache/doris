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

#include "runtime/primitive_type.h"
#include "vec/functions/llm/functions_llm.h"

namespace doris::vectorized {
class FunctionEmbed : public LLMFunction<FunctionEmbed, ColumnArray> {
public:
    static constexpr auto name = "embed";

    static constexpr size_t number_of_arguments = 2;

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeFloat32>()));
    }

    static FunctionPtr create() { return std::make_shared<FunctionEmbed>(); }

    Status build_prompt(const Block& block, const ColumnNumbers& arguments, size_t row_num,
                        std::string& prompt) const;
};

}; // namespace doris::vectorized
