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
#include "testutil/mock/mock_runtime_state.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_array.h"
#include "vec/functions/ai/ai_classify.h"
#include "vec/functions/ai/ai_extract.h"
#include "vec/functions/ai/ai_filter.h"
#include "vec/functions/ai/ai_fix_grammar.h"
#include "vec/functions/ai/ai_generate.h"
#include "vec/functions/ai/ai_mask.h"
#include "vec/functions/ai/ai_sentiment.h"
#include "vec/functions/ai/ai_similarity.h"
#include "vec/functions/ai/ai_summarize.h"
#include "vec/functions/ai/ai_translate.h"
#include "vec/functions/ai/embed.h"

namespace doris::vectorized {

TEST(AIFunctionTest, AISummarizeTest) {
    FunctionAISummarize function;

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
    ASSERT_EQ(prompt, "This is a test document that needs to be summarized.");
}

TEST(AIFunctionTest, AISentimentTest) {
    FunctionAISentiment function;

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
    ASSERT_EQ(prompt, "I really enjoyed the doris community!");
}

TEST(AIFunctionTest, AIMaskTest) {
    FunctionAIMask function;

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
              "Labels: [\"email\", \"phone\"]\n"
              "Text: My email is rarity@example.com and my phone is 123-456-7890");
}

TEST(AIFunctionTest, AIGenerateTest) {
    FunctionAIGenerate function;

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
    ASSERT_EQ(prompt, "Write a poem about spring");
}

TEST(AIFunctionTest, AIFixGrammarTest) {
    FunctionAIFixGrammar function;

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
    ASSERT_EQ(prompt, "She don't like apples");
}

TEST(AIFunctionTest, AIExtractTest) {
    FunctionAIExtract function;

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
              "Labels: [\"name\", \"birthdate\"]\n"
              "Text: John Smith was born on January 15, 1980");
}

TEST(AIFunctionTest, AIClassifyTest) {
    FunctionAIClassify function;

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
              "Labels: [\"positive\", \"negative\", \"neutral\"]\n"
              "Text: This product exceeded my expectations");
}

TEST(AIFunctionTest, AITranslateTest) {
    FunctionAITranslate function;

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
              "Translate the following text to Spanish.\n"
              "Text: Hello world");
}

TEST(AIFunctionTest, AISimilarityTest) {
    FunctionAISimilarity function;

    std::vector<std::string> resources = {"resource_name"};
    std::vector<std::string> text1 = {"I like this dish"};
    std::vector<std::string> text2 = {"This dish is very good"};

    auto col_resource = ColumnHelper::create_column<DataTypeString>(resources);
    auto col_text1 = ColumnHelper::create_column<DataTypeString>(text1);
    auto col_text2 = ColumnHelper::create_column<DataTypeString>(text2);

    Block block;
    block.insert({std::move(col_resource), std::make_shared<DataTypeString>(), "resource"});
    block.insert({std::move(col_text1), std::make_shared<DataTypeString>(), "text1"});
    block.insert({std::move(col_text2), std::make_shared<DataTypeString>(), "text2"});

    ColumnNumbers arguments = {0, 1, 2};
    std::string prompt;
    Status status = function.build_prompt(block, arguments, 0, prompt);

    ASSERT_TRUE(status.ok());
    ASSERT_EQ(prompt, "Text 1: I like this dish\nText 2: This dish is very good");
}

TEST(AIFunctionTest, AIFilterTest) {
    FunctionAIFilter function;

    std::vector<std::string> resources = {"resource_name"};
    std::vector<std::string> texts = {"This is a valid sentence."};

    auto col_resource = ColumnHelper::create_column<DataTypeString>(resources);
    auto col_text = ColumnHelper::create_column<DataTypeString>(texts);

    Block block;
    block.insert({std::move(col_resource), std::make_shared<DataTypeString>(), "resource"});
    block.insert({std::move(col_text), std::make_shared<DataTypeString>(), "text"});

    ColumnNumbers arguments = {0, 1};
    std::string prompt;
    Status status = function.build_prompt(block, arguments, 0, prompt);

    ASSERT_TRUE(status.ok());
    ASSERT_EQ(prompt, "This is a valid sentence.");
}

TEST(AIFunctionTest, ResourceNotFound) {
    auto runtime_state = std::make_unique<MockRuntimeState>();
    auto ctx = FunctionContext::create_context(runtime_state.get(), {}, {});

    std::vector<std::string> resources = {"not_exist_resource"};
    std::vector<std::string> texts = {"test"};
    auto col_resource = ColumnHelper::create_column<DataTypeString>(resources);
    auto col_text = ColumnHelper::create_column<DataTypeString>(texts);

    Block block;
    block.insert({std::move(col_resource), std::make_shared<DataTypeString>(), "resource"});
    block.insert({std::move(col_text), std::make_shared<DataTypeString>(), "text"});
    block.insert({nullptr, std::make_shared<DataTypeString>(), "result"});

    ColumnNumbers arguments = {0, 1};
    size_t result_idx = 2;

    auto sentiment_func = FunctionAISentiment::create();
    Status exec_status =
            sentiment_func->execute_impl(ctx.get(), block, arguments, result_idx, texts.size());

    ASSERT_FALSE(exec_status.ok());
    ASSERT_TRUE(exec_status.to_string().find("AI resource not found") != std::string::npos);
}

TEST(AIFunctionTest, MockResourceSendRequest) {
    auto runtime_state = std::make_unique<MockRuntimeState>();
    auto ctx = FunctionContext::create_context(runtime_state.get(), {}, {});

    std::vector<std::string> resources = {"mock_resource"};
    std::vector<std::string> texts = {"test input"};
    auto col_resource = ColumnHelper::create_column<DataTypeString>(resources);
    auto col_text = ColumnHelper::create_column<DataTypeString>(texts);

    Block block;
    block.insert({std::move(col_resource), std::make_shared<DataTypeString>(), "resource"});
    block.insert({std::move(col_text), std::make_shared<DataTypeString>(), "text"});
    block.insert({nullptr, std::make_shared<DataTypeString>(), "result"});

    ColumnNumbers arguments = {0, 1};
    size_t result_idx = 2;

    auto sentiment_func = FunctionAISentiment::create();
    Status exec_status =
            sentiment_func->execute_impl(ctx.get(), block, arguments, result_idx, texts.size());

    ASSERT_TRUE(exec_status.ok()) << exec_status.to_string();
    const auto& res_col =
            assert_cast<const ColumnString&>(*block.get_by_position(result_idx).column);
    StringRef ref = res_col.get_data_at(0);
    std::string val(ref.data, ref.size);
    ASSERT_EQ(val, "this is a mock response. test input");
}

TEST(AIFunctionTest, ReturnTypeTest) {
    FunctionAIClassify func_classify;
    DataTypes args;
    DataTypePtr ret_type = func_classify.get_return_type_impl(args);
    ASSERT_TRUE(ret_type != nullptr);
    ASSERT_EQ(ret_type->get_family_name(), "String");

    FunctionAIExtract func_extract;
    ret_type = func_extract.get_return_type_impl(args);
    ASSERT_TRUE(ret_type != nullptr);
    ASSERT_EQ(ret_type->get_family_name(), "String");

    FunctionAIFilter func_filter;
    ret_type = func_filter.get_return_type_impl(args);
    ASSERT_TRUE(ret_type != nullptr);
    ASSERT_EQ(ret_type->get_family_name(), "BOOL");

    FunctionAIFixGrammar func_fix_grammar;
    ret_type = func_fix_grammar.get_return_type_impl(args);
    ASSERT_TRUE(ret_type != nullptr);
    ASSERT_EQ(ret_type->get_family_name(), "String");

    FunctionAIGenerate func_generate;
    ret_type = func_generate.get_return_type_impl(args);
    ASSERT_TRUE(ret_type != nullptr);
    ASSERT_EQ(ret_type->get_family_name(), "String");

    FunctionAIMask func_mask;
    ret_type = func_mask.get_return_type_impl(args);
    ASSERT_TRUE(ret_type != nullptr);
    ASSERT_EQ(ret_type->get_family_name(), "String");

    FunctionAISentiment func_sentiment;
    ret_type = func_sentiment.get_return_type_impl(args);
    ASSERT_TRUE(ret_type != nullptr);
    ASSERT_EQ(ret_type->get_family_name(), "String");

    FunctionAISimilarity func_similarity;
    ret_type = func_similarity.get_return_type_impl(args);
    ASSERT_TRUE(ret_type != nullptr);
    ASSERT_EQ(ret_type->get_family_name(), "FLOAT");

    FunctionAISummarize func_summarize;
    ret_type = func_summarize.get_return_type_impl(args);
    ASSERT_TRUE(ret_type != nullptr);
    ASSERT_EQ(ret_type->get_family_name(), "String");

    FunctionAITranslate func_translate;
    ret_type = func_translate.get_return_type_impl(args);
    ASSERT_TRUE(ret_type != nullptr);
    ASSERT_EQ(ret_type->get_family_name(), "String");

    FunctionEmbed func_embed;
    ret_type = func_embed.get_return_type_impl(args);
    ASSERT_TRUE(ret_type != nullptr);
    ASSERT_EQ(ret_type->get_family_name(), "Array");
}

} // namespace doris::vectorized