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

#include "vec/columns/column_array.h"
#include "vec/functions/llm/functions_llm.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {
class FunctionLLMExtract : public LLMFunction<FunctionLLMExtract> {
public:
    static constexpr auto name = "llm_extract";

    static constexpr int max_args_num = 3;

    static FunctionPtr create() { return std::make_shared<FunctionLLMExtract>(); }

    Status build_prompt(const Block& block, const ColumnNumbers& arguments, size_t row_num,
                        std::string& prompt) const {
        // Get the text column (first argument)
        const ColumnWithTypeAndName& text_column = block.get_by_position(arguments[0]);
        std::string text_str;

        if (const auto* col_const_text =
                    check_and_get_column<ColumnConst>(text_column.column.get())) {
            StringRef text_ref = col_const_text->get_data_at(0);
            text_str = std::string(text_ref.data, text_ref.size);
        } else {
            const auto* col_text = assert_cast<const ColumnString*>(text_column.column.get());
            StringRef text = col_text->get_data_at(row_num);
            text_str = std::string(text.data, text.size);
        }

        // Get the labels array column (second argument)
        const ColumnWithTypeAndName& labels_column = block.get_by_position(arguments[1]);

        std::vector<std::string> label_values;

        if (const auto* col_const_labels =
                    check_and_get_column<ColumnConst>(labels_column.column.get())) {
            const auto* const nested_column = &col_const_labels->get_data_column();
            const auto* col_array = assert_cast<const ColumnArray*>(nested_column);

            const auto& data = col_array->get_data();
            const auto& offsets = col_array->get_offsets();

            size_t start = 0;
            size_t end = offsets[0];

            for (size_t i = start; i < end; ++i) {
                Field field;
                data.get(i, field);
                label_values.emplace_back(field.get<String>());
            }
        } else {
            const auto* col_array = assert_cast<const ColumnArray*>(labels_column.column.get());
            const auto& data = col_array->get_data();
            const auto& offsets = col_array->get_offsets();

            size_t start = row_num > 0 ? offsets[row_num - 1] : 0;
            size_t end = offsets[row_num];

            for (size_t i = start; i < end; ++i) {
                Field field;
                data.get(i, field);
                label_values.emplace_back(field.get<String>());
            }
        }

        std::string labels_str = "[";
        for (size_t i = 0; i < label_values.size(); ++i) {
            if (i > 0) {
                labels_str += ", ";
            }
            labels_str += "\"" + label_values[i] + "\"";
        }
        labels_str += "]";

        prompt = "Extract a value for each of the JSON encoded labels from the text below.\n"
                 "For each label, only extract a single value.\n"
                 "Labels: " +
                 labels_str +
                 "\n"
                 "Output the extracted values as a JSON object.\n"
                 "Answer type like `label_1=info1, label2=info2, ...`"
                 "Output only the asnwer.\n"
                 "Text: " +
                 text_str;

        return Status::OK();
    }
};

void register_function_llm_extract(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionLLMExtract>();
}
} // namespace doris::vectorized