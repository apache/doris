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

#include <gtest/gtest.h>

#include <string>

#include "testutil/column_helper.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_array.h"
#include "vec/functions/llm/llm_classify.h"
#include "vec/functions/llm/llm_extract.h"
#include "vec/functions/llm/llm_fix_grammar.h"
#include "vec/functions/llm/llm_generate.h"
#include "vec/functions/llm/llm_mask.h"
#include "vec/functions/llm/llm_sentiment.h"
#include "vec/functions/llm/llm_summarize.h"
#include "vec/functions/llm/llm_translate.h"

namespace doris::vectorized {

TEST(BuildPromptTest, LLMSummarizeTest) {
    FunctionLLMSummarize function;

    std::vector<std::string> resources = {"resource_name"};
    std::vector<std::string> texts = {"This is a test document that needs to be summarized."};

    auto col_resource = ColumnHelper::create_column<DataTypeString>(resources);
    auto col_text = ColumnHelper::create_column<DataTypeString>(texts);

    Block block;
    block.insert({std::move(col_resource), std::make_shared<DataTypeString>(), "resource"});
    block.insert({std::move(col_text), std::make_shared<DataTypeString>(), "text"});

    ColumnNumbers arguments = {0, 1};
    std::string prompt;
    Status status = function.build_prompt(block, arguments, 0, prompt);

    ASSERT_TRUE(status.ok());
    ASSERT_EQ(prompt,
              "Summarize the following text in a concise way.\n"
              "Return only the summary.\n"
              "information:This is a test document that needs to be summarized.");
}

TEST(BuildPromptTest, LLMSentimentTest) {
    FunctionLLMSentiment function;

    std::vector<std::string> resources = {"resource_name"};
    std::vector<std::string> texts = {"I really enjoyed the doris community!"};

    auto col_resource = ColumnHelper::create_column<DataTypeString>(resources);
    auto col_text = ColumnHelper::create_column<DataTypeString>(texts);

    Block block;
    block.insert({std::move(col_resource), std::make_shared<DataTypeString>(), "resource"});
    block.insert({std::move(col_text), std::make_shared<DataTypeString>(), "text"});

    ColumnNumbers arguments = {0, 1};
    std::string prompt;
    Status status = function.build_prompt(block, arguments, 0, prompt);

    ASSERT_TRUE(status.ok());
    ASSERT_EQ(prompt,
              "Classify the text below into one of the following labels: "
              "[positive, negative, neutral, mixed]\n"
              "Output only the label.\n"
              "Text: I really enjoyed the doris community!");
}

TEST(BuildPromptTest, LLMMaskTest) {
    FunctionLLMMask function;

    std::vector<std::string> resources = {"resource_name"};
    std::vector<std::string> texts = {
            "My email is rarity@example.com and my phone is 123-456-7890"};
    std::vector<std::vector<std::string>> labels = {{"email", "phone"}};

    auto col_resource = ColumnHelper::create_column<DataTypeString>(resources);
    auto col_text = ColumnHelper::create_column<DataTypeString>(texts);

    auto nested_column = ColumnString::create();
    auto offsets_column = ColumnOffset64::create();

    IColumn::Offset offset = 0;
    for (const auto& row : labels) {
        for (const auto& value : row) {
            nested_column->insert_data(value.data(), value.size());
        }
        offset += row.size();
        offsets_column->insert_value(offset);
    }

    auto array_column = ColumnArray::create(std::move(nested_column), std::move(offsets_column));

    Block block;
    block.insert({std::move(col_resource), std::make_shared<DataTypeString>(), "resource"});
    block.insert({std::move(col_text), std::make_shared<DataTypeString>(), "text"});
    block.insert({std::move(array_column),
                  std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "labels"});

    ColumnNumbers arguments = {0, 1, 2};
    std::string prompt;
    Status status = function.build_prompt(block, arguments, 0, prompt);

    ASSERT_TRUE(status.ok());
    ASSERT_EQ(prompt,
              "Identify and mask sensitive information in the text below.\n"
              "For each of these categories, replace the entire value with \"[MASKED]\":\n"
              "Labels: [\"email\", \"phone\"]\n"
              "Do not include any explanations or introductions in your response.\n"
              "Return only the masked text.\n\n"
              "Text: My email is rarity@example.com and my phone is 123-456-7890");
}

TEST(BuildPromptTest, LLMGenerateTest) {
    FunctionLLMGenerate function;

    std::vector<std::string> resources = {"resource_name"};
    std::vector<std::string> texts = {"Write a poem about spring"};

    auto col_resource = ColumnHelper::create_column<DataTypeString>(resources);
    auto col_text = ColumnHelper::create_column<DataTypeString>(texts);

    Block block;
    block.insert({std::move(col_resource), std::make_shared<DataTypeString>(), "resource"});
    block.insert({std::move(col_text), std::make_shared<DataTypeString>(), "text"});

    ColumnNumbers arguments = {0, 1};
    std::string prompt;
    Status status = function.build_prompt(block, arguments, 0, prompt);

    ASSERT_TRUE(status.ok());
    ASSERT_EQ(prompt,
              "Generate a response based on the following input.\n"
              "Output only the generated text.\n"
              "Text: Write a poem about spring");
}

TEST(BuildPromptTest, LLMFixGrammarTest) {
    FunctionLLMFixGrammar function;

    std::vector<std::string> resources = {"resource_name"};
    std::vector<std::string> texts = {"She don't like apples"};

    auto col_resource = ColumnHelper::create_column<DataTypeString>(resources);
    auto col_text = ColumnHelper::create_column<DataTypeString>(texts);

    Block block;
    block.insert({std::move(col_resource), std::make_shared<DataTypeString>(), "resource"});
    block.insert({std::move(col_text), std::make_shared<DataTypeString>(), "text"});

    ColumnNumbers arguments = {0, 1};
    std::string prompt;
    Status status = function.build_prompt(block, arguments, 0, prompt);

    ASSERT_TRUE(status.ok());
    ASSERT_EQ(prompt,
              "Fix the grammar in the text below.\n"
              "Output only the corrected text.\n"
              "Text: She don't like apples");
}

TEST(BuildPromptTest, LLMExtractTest) {
    FunctionLLMExtract function;

    std::vector<std::string> resources = {"resource_name"};
    std::vector<std::string> texts = {"John Smith was born on January 15, 1980"};
    std::vector<std::vector<std::string>> labels = {{"name", "birthdate"}};

    auto col_resource = ColumnHelper::create_column<DataTypeString>(resources);
    auto col_text = ColumnHelper::create_column<DataTypeString>(texts);

    auto nested_column = ColumnString::create();
    auto offsets_column = ColumnOffset64::create();

    IColumn::Offset offset = 0;
    for (const auto& row : labels) {
        for (const auto& value : row) {
            nested_column->insert_data(value.data(), value.size());
        }
        offset += row.size();
        offsets_column->insert_value(offset);
    }

    auto array_column = ColumnArray::create(std::move(nested_column), std::move(offsets_column));

    Block block;
    block.insert({std::move(col_resource), std::make_shared<DataTypeString>(), "resource"});
    block.insert({std::move(col_text), std::make_shared<DataTypeString>(), "text"});
    block.insert({std::move(array_column),
                  std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "labels"});

    ColumnNumbers arguments = {0, 1, 2};
    std::string prompt;
    Status status = function.build_prompt(block, arguments, 0, prompt);

    ASSERT_TRUE(status.ok());
    ASSERT_EQ(prompt,
              "Extract a value for each of the JSON encoded labels from the text below.\n"
              "For each label, only extract a single value.\n"
              "Labels: [\"name\", \"birthdate\"]\n"
              "Output the extracted values as a JSON object.\n"
              "Answer type like `label_1=info1, label2=info2, ...`"
              "Output only the answer.\n"
              "Text: John Smith was born on January 15, 1980");
}

TEST(BuildPromptTest, LLMClassifyTest) {
    FunctionLLMClassify function;

    std::vector<std::string> resources = {"resource_name"};
    std::vector<std::string> texts = {"This product exceeded my expectations"};
    std::vector<std::vector<std::string>> labels = {{"positive", "negative", "neutral"}};

    auto col_resource = ColumnHelper::create_column<DataTypeString>(resources);
    auto col_text = ColumnHelper::create_column<DataTypeString>(texts);

    auto nested_column = ColumnString::create();
    auto offsets_column = ColumnOffset64::create();

    IColumn::Offset offset = 0;
    for (const auto& row : labels) {
        for (const auto& value : row) {
            nested_column->insert_data(value.data(), value.size());
        }
        offset += row.size();
        offsets_column->insert_value(offset);
    }

    auto array_column = ColumnArray::create(std::move(nested_column), std::move(offsets_column));

    Block block;
    block.insert({std::move(col_resource), std::make_shared<DataTypeString>(), "resource"});
    block.insert({std::move(col_text), std::make_shared<DataTypeString>(), "text"});
    block.insert({std::move(array_column),
                  std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "labels"});

    ColumnNumbers arguments = {0, 1, 2};
    std::string prompt;
    Status status = function.build_prompt(block, arguments, 0, prompt);

    ASSERT_TRUE(status.ok());
    ASSERT_EQ(prompt,
              "Classify the text below into one of the following JSON encoded labels: "
              "[\"positive\", \"negative\", \"neutral\"]\n"
              "Output only the label without any quotation marks or additional text.\n"
              "Text: This product exceeded my expectations");
}

TEST(BuildPromptTest, LLMTranslateTest) {
    FunctionLLMTranslate function;

    std::vector<std::string> resources = {"resource_name"};
    std::vector<std::string> texts = {"Hello world"};
    std::vector<std::string> target_langs = {"Spanish"};

    auto col_resource = ColumnHelper::create_column<DataTypeString>(resources);
    auto col_text = ColumnHelper::create_column<DataTypeString>(texts);
    auto col_target_lang = ColumnHelper::create_column<DataTypeString>(target_langs);

    Block block;
    block.insert({std::move(col_resource), std::make_shared<DataTypeString>(), "resource"});
    block.insert({std::move(col_text), std::make_shared<DataTypeString>(), "text"});
    block.insert({std::move(col_target_lang), std::make_shared<DataTypeString>(), "target_lang"});

    ColumnNumbers arguments = {0, 1, 2};
    std::string prompt;
    Status status = function.build_prompt(block, arguments, 0, prompt);

    ASSERT_TRUE(status.ok());
    ASSERT_EQ(prompt,
              "Translate the following text to Spanish. "
              "Output only the translated text.\n"
              "Text: Hello world");
}

} // namespace doris::vectorized