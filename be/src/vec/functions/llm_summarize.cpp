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

#include "vec/functions/functions_llm.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

class FunctionLLMSummarize : public LLMFunction<FunctionLLMSummarize> {
public:
    static constexpr auto name = "llm_summarize";

    FunctionLLMSummarize() {
        Status status = init();
        if (!status.ok()) {
            throw Status::InternalError("Failed to initialize FunctionLLMSummarize: " +
                                        status.to_string());
        }
    }

    static FunctionPtr create() { return std::make_shared<FunctionLLMSummarize>(); }

    size_t number_of_arguments() const { return 1; }

    Status build_prompt(const Block& block, const ColumnNumbers& arguments, size_t row_num,
                        std::string& prompt) const {
        const ColumnWithTypeAndName& text_column = block.get_by_position(arguments[0]);
        StringRef text =
                assert_cast<const ColumnString*>(text_column.column.get())->get_data_at(row_num);

        prompt = "Summarize the following text in a concise way.\n"
                 "Return only the summary.\n"
                 "information:" +
                 std::string(text.data, text.size);

        return Status::OK();
    }
};

void register_function_llm_summarize(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionLLMSummarize>();
}
} // namespace doris::vectorized