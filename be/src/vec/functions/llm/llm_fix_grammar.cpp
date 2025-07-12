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

#include "vec/functions/llm/functions_llm.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {
class FunctionLLMFixGrammar : public LLMFunction<FunctionLLMFixGrammar> {
public:
    static constexpr auto name = "llm_fixgrammar";

    static constexpr int max_args_num = 2;

    static FunctionPtr create() { return std::make_shared<FunctionLLMFixGrammar>(); }


    Status build_prompt(const Block& block, const ColumnNumbers& arguments, size_t row_num,
                        std::string& prompt) const {
        const ColumnWithTypeAndName& text_column = block.get_by_position(arguments[0]);
        StringRef text =
                assert_cast<const ColumnString*>(text_column.column.get())->get_data_at(row_num);

        prompt = "Fix the grammar in the text below.\n"
                 "Output only the corrected text.\n"
                 "Text: " +
                 std::string(text.data, text.size);

        return Status::OK();
    }
};

void register_function_llm_fixgrammar(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionLLMFixGrammar>();
}
} // namespace doris::vectorized