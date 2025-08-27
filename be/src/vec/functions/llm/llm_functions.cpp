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
#include "vec/functions/llm/llm_filter.h"
#include "vec/functions/llm/llm_fix_grammar.h"
#include "vec/functions/llm/llm_generate.h"
#include "vec/functions/llm/llm_mask.h"
#include "vec/functions/llm/llm_sentiment.h"
#include "vec/functions/llm/llm_similarity.h"
#include "vec/functions/llm/llm_summarize.h"
#include "vec/functions/llm/llm_translate.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {
Status FunctionLLMClassify::build_prompt(const Block& block, const ColumnNumbers& arguments,
                                         size_t row_num, std::string& prompt) const {
    // Get the text column
    const ColumnWithTypeAndName& text_column = block.get_by_position(arguments[1]);
    StringRef text = text_column.column->get_data_at(row_num);
    std::string text_str = std::string(text.data, text.size);

    // Get the labels array column
    const ColumnWithTypeAndName& labels_column = block.get_by_position(arguments[2]);
    const auto& [array_column, array_row_num] =
            check_column_const_set_readability(*labels_column.column, row_num);
    const auto* col_array = check_and_get_column<ColumnArray>(*array_column);
    if (col_array == nullptr) {
        return Status::InternalError(
                "labels argument for {} must be Array(String) or Array(Varchar)", name);
    }

    std::vector<std::string> label_values;
    const auto& data = col_array->get_data();
    const auto& offsets = col_array->get_offsets();
    size_t start = array_row_num > 0 ? offsets[array_row_num - 1] : 0;
    size_t end = offsets[array_row_num];
    for (size_t i = start; i < end; ++i) {
        Field field;
        data.get(i, field);
        label_values.emplace_back(field.get<String>());
    }

    std::string labels_str = "[";
    for (size_t i = 0; i < label_values.size(); ++i) {
        if (i > 0) {
            labels_str += ", ";
        }
        labels_str += "\"" + label_values[i] + "\"";
    }
    labels_str += "]";

    prompt = "Labels: " + labels_str + "\nText: " + text_str;

    return Status::OK();
}

Status FunctionLLMExtract::build_prompt(const Block& block, const ColumnNumbers& arguments,
                                        size_t row_num, std::string& prompt) const {
    // Get the text column
    const ColumnWithTypeAndName& text_column = block.get_by_position(arguments[1]);
    StringRef text = text_column.column->get_data_at(row_num);
    std::string text_str = std::string(text.data, text.size);

    // Get the labels array column
    const ColumnWithTypeAndName& labels_column = block.get_by_position(arguments[2]);
    const auto& [array_column, array_row_num] =
            check_column_const_set_readability(*labels_column.column, row_num);
    const auto* col_array = check_and_get_column<ColumnArray>(*array_column);
    if (col_array == nullptr) {
        return Status::InternalError(
                "labels argument for {} must be Array(String) or Array(Varchar)", name);
    }

    std::vector<std::string> label_values;
    const auto& offsets = col_array->get_offsets();
    const auto& data = col_array->get_data();
    size_t start = array_row_num > 0 ? offsets[array_row_num - 1] : 0;
    size_t end = offsets[array_row_num];
    for (size_t i = start; i < end; ++i) {
        Field field;
        data.get(i, field);
        label_values.emplace_back(field.get<String>());
    }

    std::string labels_str = "[";
    for (size_t i = 0; i < label_values.size(); ++i) {
        if (i > 0) {
            labels_str += ", ";
        }
        labels_str += "\"" + label_values[i] + "\"";
    }
    labels_str += "]";

    prompt = "Labels: " + labels_str + "\nText: " + text_str;

    return Status::OK();
}

Status FunctionLLMFilter::build_prompt(const Block& block, const ColumnNumbers& arguments,
                                       size_t row_num, std::string& prompt) const {
    const ColumnWithTypeAndName& text_column = block.get_by_position(arguments[1]);
    StringRef text_ref = text_column.column->get_data_at(row_num);
    prompt = std::string(text_ref.data, text_ref.size);

    return Status::OK();
}

Status FunctionLLMFixGrammar::build_prompt(const Block& block, const ColumnNumbers& arguments,
                                           size_t row_num, std::string& prompt) const {
    const ColumnWithTypeAndName& text_column = block.get_by_position(arguments[1]);
    StringRef text_ref = text_column.column->get_data_at(row_num);
    prompt = std::string(text_ref.data, text_ref.size);

    return Status::OK();
}

Status FunctionLLMGenerate::build_prompt(const Block& block, const ColumnNumbers& arguments,
                                         size_t row_num, std::string& prompt) const {
    const ColumnWithTypeAndName& text_column = block.get_by_position(arguments[1]);
    StringRef text_ref = text_column.column->get_data_at(row_num);
    prompt = std::string(text_ref.data, text_ref.size);

    return Status::OK();
}

Status FunctionLLMMask::build_prompt(const Block& block, const ColumnNumbers& arguments,
                                     size_t row_num, std::string& prompt) const {
    // Get the text column
    const ColumnWithTypeAndName& text_column = block.get_by_position(arguments[1]);
    StringRef text = text_column.column->get_data_at(row_num);
    std::string text_str = std::string(text.data, text.size);

    // Get the labels array column
    const ColumnWithTypeAndName& labels_column = block.get_by_position(arguments[2]);
    const auto& [array_column, array_row_num] =
            check_column_const_set_readability(*labels_column.column, row_num);
    const auto* col_array = check_and_get_column<ColumnArray>(*array_column);
    if (col_array == nullptr) {
        return Status::InternalError(
                "labels argument for {} must be Array(String) or Array(Varchar)", name);
    }

    std::vector<std::string> label_values;
    const auto& offsets = col_array->get_offsets();
    const auto& data = col_array->get_data();
    size_t start = array_row_num > 0 ? offsets[array_row_num - 1] : 0;
    size_t end = offsets[array_row_num];
    for (size_t i = start; i < end; ++i) {
        Field field;
        data.get(i, field);
        label_values.emplace_back(field.get<String>());
    }

    std::string labels_str = "[";
    for (size_t i = 0; i < label_values.size(); ++i) {
        if (i > 0) {
            labels_str += ", ";
        }
        labels_str += "\"" + label_values[i] + "\"";
    }
    labels_str += "]";

    prompt = "Labels: " + labels_str + "\nText: " + text_str;

    return Status::OK();
}

Status FunctionLLMSentiment::build_prompt(const Block& block, const ColumnNumbers& arguments,
                                          size_t row_num, std::string& prompt) const {
    const ColumnWithTypeAndName& text_column = block.get_by_position(arguments[1]);
    StringRef text_ref = text_column.column->get_data_at(row_num);
    prompt = std::string(text_ref.data, text_ref.size);

    return Status::OK();
}

Status FunctionLLMSimilarity::build_prompt(const Block& block, const ColumnNumbers& arguments,
                                           size_t row_num, std::string& prompt) const {
    // text1
    const ColumnWithTypeAndName& text_column_1 = block.get_by_position(arguments[1]);
    StringRef text_1 = text_column_1.column.get()->get_data_at(row_num);
    std::string text_str_1 = std::string(text_1.data, text_1.size);

    // text2
    const ColumnWithTypeAndName& text_column_2 = block.get_by_position(arguments[2]);
    StringRef text_2 = text_column_2.column.get()->get_data_at(row_num);
    std::string text_str_2 = std::string(text_2.data, text_2.size);

    prompt = "Text 1: " + text_str_1 + "\nText 2: " + text_str_2;

    return Status::OK();
}

Status FunctionLLMSummarize::build_prompt(const Block& block, const ColumnNumbers& arguments,
                                          size_t row_num, std::string& prompt) const {
    const ColumnWithTypeAndName& text_column = block.get_by_position(arguments[1]);
    StringRef text_ref = text_column.column->get_data_at(row_num);
    prompt = std::string(text_ref.data, text_ref.size);

    return Status::OK();
}

Status FunctionLLMTranslate::build_prompt(const Block& block, const ColumnNumbers& arguments,
                                          size_t row_num, std::string& prompt) const {
    // text
    const ColumnWithTypeAndName& text_column = block.get_by_position(arguments[1]);
    StringRef text = text_column.column.get()->get_data_at(row_num);
    std::string text_str = std::string(text.data, text.size);

    // target language
    const ColumnWithTypeAndName& lang_column = block.get_by_position(arguments[2]);
    StringRef lang = lang_column.column.get()->get_data_at(row_num);
    std::string target_lang = std::string(lang.data, lang.size);

    prompt = "Translate the following text to " + target_lang + ".\nText: " + text_str;

    return Status::OK();
}

void register_function_llm_classify(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionLLMClassify>();
}

void register_function_llm_extract(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionLLMExtract>();
}

void register_function_llm_filter(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionLLMFilter>();
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

void register_function_llm_similarity(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionLLMSimilarity>();
}

void register_function_llm_summarize(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionLLMSummarize>();
}

void register_function_llm_translate(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionLLMTranslate>();
}
} // namespace doris::vectorized
