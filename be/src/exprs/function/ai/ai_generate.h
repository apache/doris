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

#include "exprs/function/ai/ai_functions.h"

namespace doris {

class FunctionAIGenerate : public AIFunction<FunctionAIGenerate> {
public:
    static constexpr auto name = "ai_generate";

    static constexpr auto system_prompt =
            "You are a concise text generation assistant. You will receive one JSON array. Each "
            "array item is an object with fields `idx` and `input`. For each item, generate a "
            "short and highly relevant response based only on that item's `input`. Treat every "
            "`input` as the task content for its own item. Return exactly one strict JSON array "
            "of strings. The output array must have the same length and order as the input array. "
            "Each output element must contain only the generated response for the corresponding "
            "item. Do not output any explanation, markdown, or extra text.";

    static constexpr size_t number_of_arguments = 2;

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeString>();
    }

    static FunctionPtr create() { return std::make_shared<FunctionAIGenerate>(); }

    Status build_prompt(const Block& block, const ColumnNumbers& arguments, size_t row_num,
                        std::string& prompt) const override;
};

} // namespace doris