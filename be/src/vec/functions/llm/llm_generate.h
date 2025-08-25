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

#include "vec/functions/llm/functions_llm.h"

namespace doris::vectorized {

class FunctionLLMGenerate : public LLMFunction<FunctionLLMGenerate> {
public:
    static constexpr auto name = "llm_generate";

    static constexpr auto system_prompt =
            "You are a creative text generator. You will generate a concise and highly relevant "
            "response based on the user's input; aim for maximum brevity—cut every non-essential "
            "word.";

    static constexpr size_t number_of_arguments = 2;

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeString>();
    }

    static FunctionPtr create() { return std::make_shared<FunctionLLMGenerate>(); }

    Status build_prompt(const Block& block, const ColumnNumbers& arguments, size_t row_num,
                        std::string& prompt) const;
};

} // namespace doris::vectorized