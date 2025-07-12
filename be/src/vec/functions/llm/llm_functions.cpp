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
#include "vec/functions/llm/llm_classify.h"
#include "vec/functions/llm/llm_extract.h"
#include "vec/functions/llm/llm_fix_grammar.h"
#include "vec/functions/llm/llm_generate.h"
#include "vec/functions/llm/llm_mask.h"
#include "vec/functions/llm/llm_sentiment.h"
#include "vec/functions/llm/llm_summarize.h"
#include "vec/functions/llm/llm_translate.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {
Status FunctionLLMClassify::build_prompt(const Block& block, const ColumnNumbers& arguments,
                                         size_t row_num, std::string& prompt) const {
    // Get the text column
    const ColumnWithTypeAndName& text_column = block.get_by_position(arguments[1]);
    std::string text_str;
    if (const auto* col_const_text = check_and_get_column<ColumnConst>(text_column.column.get())) {
        StringRef text_ref = col_const_text->get_data_at(0);
        text_str = std::string(text_ref.data, text_ref.size);
    } else {
        const auto* col_text = assert_cast<const ColumnString*>(text_column.column.get());
        StringRef text = col_text->get_data_at(row_num);
        text_str = std::string(text.data, text.size);
    }

    // Get the labels array column
    const ColumnWithTypeAndName& labels_column = block.get_by_position(arguments[2]);
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

    prompt =
            "Classify the text below into one of the following JSON encoded labels: " + labels_str +
            "\n"
            "Output only the label without any quotation marks or additional text.\n"
            "Text: " +
            text_str;

    return Status::OK();
}

Status FunctionLLMExtract::build_prompt(const Block& block, const ColumnNumbers& arguments,
                                        size_t row_num, std::string& prompt) const {
    // Get the text column
    const ColumnWithTypeAndName& text_column = block.get_by_position(arguments[1]);
    std::string text_str;
    if (const auto* col_const_text = check_and_get_column<ColumnConst>(text_column.column.get())) {
        StringRef text_ref = col_const_text->get_data_at(0);
        text_str = std::string(text_ref.data, text_ref.size);
    } else {
        const auto* col_text = assert_cast<const ColumnString*>(text_column.column.get());
        StringRef text = col_text->get_data_at(row_num);
        text_str = std::string(text.data, text.size);
    }

    // Get the labels array column
    const ColumnWithTypeAndName& labels_column = block.get_by_position(arguments[2]);
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
             "Output only the answer.\n"
             "Text: " +
             text_str;

    return Status::OK();
}

Status FunctionLLMFixGrammar::build_prompt(const Block& block, const ColumnNumbers& arguments,
                                           size_t row_num, std::string& prompt) const {
    std::string text;
    const ColumnWithTypeAndName& text_column = block.get_by_position(arguments[1]);
    if (const auto* col_const_text = check_and_get_column<ColumnConst>(text_column.column.get())) {
        StringRef text_ref = col_const_text->get_data_at(0);
        text = std::string(text_ref.data, text_ref.size);
    } else {
        StringRef text_ref =
                assert_cast<const ColumnString*>(text_column.column.get())->get_data_at(row_num);
        text = std::string(text_ref.data, text_ref.size);
    }

    prompt = "Fix the grammar in the text below.\n"
             "Output only the corrected text.\n"
             "Text: " +
             text;

    return Status::OK();
}

Status FunctionLLMGenerate::build_prompt(const Block& block, const ColumnNumbers& arguments,
                                         size_t row_num, std::string& prompt) const {
    std::string text;
    const ColumnWithTypeAndName& text_column = block.get_by_position(arguments[1]);
    if (const auto* col_const_text = check_and_get_column<ColumnConst>(text_column.column.get())) {
        StringRef text_ref = col_const_text->get_data_at(0);
        text = std::string(text_ref.data, text_ref.size);
    } else {
        StringRef text_ref =
                assert_cast<const ColumnString*>(text_column.column.get())->get_data_at(row_num);
        text = std::string(text_ref.data, text_ref.size);
    }

    prompt = "Generate a response based on the following input.\n"
             "Output only the generated text.\n"
             "Text: " +
             text;

    return Status::OK();
}

Status FunctionLLMMask::build_prompt(const Block& block, const ColumnNumbers& arguments,
                                     size_t row_num, std::string& prompt) const {
    // Get the text column
    const ColumnWithTypeAndName& text_column = block.get_by_position(arguments[1]);
    std::string text_str;
    if (const auto* col_const_text = check_and_get_column<ColumnConst>(text_column.column.get())) {
        StringRef text_ref = col_const_text->get_data_at(0);
        text_str = std::string(text_ref.data, text_ref.size);
    } else {
        const auto* col_text = assert_cast<const ColumnString*>(text_column.column.get());
        StringRef text = col_text->get_data_at(row_num);
        text_str = std::string(text.data, text.size);
    }

    // Get the labels array column
    const ColumnWithTypeAndName& labels_column = block.get_by_position(arguments[2]);
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

    prompt = "Identify and mask sensitive information in the text below.\n"
             "For each of these categories, replace the entire value with \"[MASKED]\":\n"
             "Labels: " +
             labels_str +
             "\n"
             "Do not include any explanations or introductions in your response.\n"
             "Return only the masked text.\n\n"
             "Text: " +
             text_str;

    return Status::OK();
}

Status FunctionLLMSentiment::build_prompt(const Block& block, const ColumnNumbers& arguments,
                                          size_t row_num, std::string& prompt) const {
    std::string text;
    const ColumnWithTypeAndName& text_column = block.get_by_position(arguments[1]);
    if (const auto* col_const_text = check_and_get_column<ColumnConst>(text_column.column.get())) {
        StringRef text_ref = col_const_text->get_data_at(0);
        text = std::string(text_ref.data, text_ref.size);
    } else {
        StringRef text_ref =
                assert_cast<const ColumnString*>(text_column.column.get())->get_data_at(row_num);
        text = std::string(text_ref.data, text_ref.size);
    }

    prompt = "Classify the text below into one of the following labels: "
             "[positive, negative, neutral, mixed]\n"
             "Output only the label.\n"
             "Text: " +
             text;

    return Status::OK();
}

Status FunctionLLMSummarize::build_prompt(const Block& block, const ColumnNumbers& arguments,
                                          size_t row_num, std::string& prompt) const {
    std::string text;
    const ColumnWithTypeAndName& text_column = block.get_by_position(arguments[1]);
    if (const auto* col_const_text = check_and_get_column<ColumnConst>(text_column.column.get())) {
        StringRef text_ref = col_const_text->get_data_at(0);
        text = std::string(text_ref.data, text_ref.size);
    } else {
        StringRef text_ref =
                assert_cast<const ColumnString*>(text_column.column.get())->get_data_at(row_num);
        text = std::string(text_ref.data, text_ref.size);
    }

    prompt = "Summarize the following text in a concise way.\n"
             "Return only the summary.\n"
             "information:" +
             text;

    return Status::OK();
}

Status FunctionLLMTranslate::build_prompt(const Block& block, const ColumnNumbers& arguments,
                                          size_t row_num, std::string& prompt) const {
    // text
    const ColumnWithTypeAndName& text_column = block.get_by_position(arguments[1]);
    std::string text_str;
    if (const auto* col_const_text = check_and_get_column<ColumnConst>(text_column.column.get())) {
        StringRef text_ref = col_const_text->get_data_at(0);
        text_str = std::string(text_ref.data, text_ref.size);
    } else {
        StringRef text =
                assert_cast<const ColumnString*>(text_column.column.get())->get_data_at(row_num);
        text_str = std::string(text.data, text.size);
    }

    // target language
    const ColumnWithTypeAndName& lang_column = block.get_by_position(arguments[2]);
    std::string target_lang;
    if (const auto* col_const_lang = check_and_get_column<ColumnConst>(lang_column.column.get())) {
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

void register_function_llm_classify(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionLLMClassify>();
}

void register_function_llm_extract(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionLLMExtract>();
}

void register_function_llm_fixgrammar(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionLLMFixGrammar>();
}

void register_function_llm_generate(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionLLMGenerate>();
}

void register_function_llm_mask(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionLLMMask>();
}

void register_function_llm_sentiment(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionLLMSentiment>();
}

void register_function_llm_summarize(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionLLMSummarize>();
}

void register_function_llm_translate(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionLLMTranslate>();
}
} // namespace doris::vectorized
