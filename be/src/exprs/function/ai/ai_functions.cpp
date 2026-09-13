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

#include "core/column/column_array_view.h"
#include "exprs/function/ai/ai_classify.h"
#include "exprs/function/ai/ai_extract.h"
#include "exprs/function/ai/ai_filter.h"
#include "exprs/function/ai/ai_fix_grammar.h"
#include "exprs/function/ai/ai_generate.h"
#include "exprs/function/ai/ai_mask.h"
#include "exprs/function/ai/ai_sentiment.h"
#include "exprs/function/ai/ai_similarity.h"
#include "exprs/function/ai/ai_summarize.h"
#include "exprs/function/ai/ai_translate.h"
#include "exprs/function/ai/embed.h"
#include "exprs/function/simple_function_factory.h"

namespace doris {
static Status format_labels(const ColumnPtr& labels_column, size_t row_num,
                            std::string_view function_name, std::string& labels_str) {
    auto readable_column = check_column_const_set_readability(*labels_column, row_num);
    if (!is_column<ColumnArray>(*readable_column.first)) {
        return Status::InternalError(
                "labels argument for {} must be Array(String) or Array(Varchar)", function_name);
    }

    auto labels_view = ColumnArrayView<TYPE_STRING>::create(labels_column);
    auto labels = labels_view[row_num];
    labels_str = "[";
    bool is_first_label = true;
    for (size_t i = 0; i < labels.size(); ++i) {
        if (labels.is_null_at(i)) {
            continue;
        }
        if (!is_first_label) {
            labels_str += ", ";
        }
        StringRef label = labels.value_at(i);
        labels_str += "\"";
        labels_str.append(label.data, label.size);
        labels_str += "\"";
        is_first_label = false;
    }
    labels_str += "]";
    return Status::OK();
}

Status FunctionAIClassify::build_prompt(const Columns& prompt_columns, size_t row_num,
                                        std::string& prompt) const {
    // Get the text column
    StringRef text = prompt_columns[0]->get_data_at(row_num);
    std::string text_str = std::string(text.data, text.size);

    std::string labels_str;
    RETURN_IF_ERROR(format_labels(prompt_columns[1], row_num, name, labels_str));

    prompt = "Labels: " + labels_str + "\nText: " + text_str;

    return Status::OK();
}

Status FunctionAIExtract::build_prompt(const Columns& prompt_columns, size_t row_num,
                                       std::string& prompt) const {
    // Get the text column
    StringRef text = prompt_columns[0]->get_data_at(row_num);
    std::string text_str = std::string(text.data, text.size);

    std::string labels_str;
    RETURN_IF_ERROR(format_labels(prompt_columns[1], row_num, name, labels_str));

    prompt = "Labels: " + labels_str + "\nText: " + text_str;

    return Status::OK();
}

Status FunctionAIGenerate::build_prompt(const Columns& prompt_columns, size_t row_num,
                                        std::string& prompt) const {
    StringRef text_ref = prompt_columns[0]->get_data_at(row_num);
    prompt = std::string(text_ref.data, text_ref.size);

    return Status::OK();
}

Status FunctionAIMask::build_prompt(const Columns& prompt_columns, size_t row_num,
                                    std::string& prompt) const {
    // Get the text column
    StringRef text = prompt_columns[0]->get_data_at(row_num);
    std::string text_str = std::string(text.data, text.size);

    std::string labels_str;
    RETURN_IF_ERROR(format_labels(prompt_columns[1], row_num, name, labels_str));

    prompt = "Labels: " + labels_str + "\nText: " + text_str;

    return Status::OK();
}

Status FunctionAISimilarity::build_prompt(const Columns& prompt_columns, size_t row_num,
                                          std::string& prompt) const {
    // text1
    StringRef text_1 = prompt_columns[0]->get_data_at(row_num);
    std::string text_str_1 = std::string(text_1.data, text_1.size);

    // text2
    StringRef text_2 = prompt_columns[1]->get_data_at(row_num);
    std::string text_str_2 = std::string(text_2.data, text_2.size);

    prompt = "Text 1: " + text_str_1 + "\nText 2: " + text_str_2;

    return Status::OK();
}

Status FunctionAITranslate::build_prompt(const Columns& prompt_columns, size_t row_num,
                                         std::string& prompt) const {
    // text
    StringRef text = prompt_columns[0]->get_data_at(row_num);
    std::string text_str = std::string(text.data, text.size);

    // target language
    StringRef lang = prompt_columns[1]->get_data_at(row_num);
    std::string target_lang = std::string(lang.data, lang.size);

    prompt = "Translate the following text to " + target_lang + ".\nText: " + text_str;

    return Status::OK();
}

void register_function_ai(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionEmbed>();
    factory.register_function<FunctionAIClassify>();
    factory.register_function<FunctionAIExtract>();
    factory.register_function<FunctionAIFilter>();
    factory.register_function<FunctionAIFixGrammar>();
    factory.register_function<FunctionAIGenerate>();
    factory.register_function<FunctionAIMask>();
    factory.register_function<FunctionAISentiment>();
    factory.register_function<FunctionAISimilarity>();
    factory.register_function<FunctionAISummarize>();
    factory.register_function<FunctionAITranslate>();
}

} // namespace doris
