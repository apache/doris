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
class FunctionLLMTranslate : public LLMFunction<FunctionLLMTranslate> {
public:
    static constexpr auto name = "llm_translate";

    FunctionLLMTranslate() {
        Status status = init();
        if (!status.ok()) {
            throw Status::InternalError("Failed to initialize FunctionLLMTranslate: " +
                                        status.to_string());
        }
    }

    static FunctionPtr create() { return std::make_shared<FunctionLLMTranslate>(); }

    size_t number_of_arguments() const { return 2; }

    Status build_prompt(const Block& block, const ColumnNumbers& arguments, size_t row_num,
                        std::string& prompt) const {
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

        const ColumnWithTypeAndName& lang_column = block.get_by_position(arguments[1]);
        std::string target_lang;

        if (const auto* col_const_lang =
                    check_and_get_column<ColumnConst>(lang_column.column.get())) {
            StringRef lang_ref = col_const_lang->get_data_at(0);
            target_lang = std::string(lang_ref.data, lang_ref.size);
        } else {
            const auto* col_lang = assert_cast<const ColumnString*>(lang_column.column.get());
            StringRef lang = col_lang->get_data_at(row_num);
            target_lang = std::string(lang.data, lang.size);
        }
        prompt = "Translate the following text to " + target_lang +
                 ". Output only the translated text.\n" + "Text: " + text_str;

        return Status::OK();
    }
};

void register_function_llm_translate(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionLLMTranslate>();
}

} // namespace doris::vectorized
