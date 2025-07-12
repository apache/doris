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
#include "vec/functions/llm/llm_adapter.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_array.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_array.h"

#include <gtest/gtest.h>
#include <string>
#include <memory>

namespace doris::vectorized {

// 创建测试数据的辅助方法
Block create_test_block_with_string(const std::string& text, bool is_const = false) {
    Block block;
    
    if (is_const) {
        // 常量列
        auto col = ColumnConst::create(
            ColumnString::create(1),
            1
        );
        col->get_data_column().insert_data(text.data(), text.size());
        
        block.insert({
            std::move(col),
            std::make_shared<DataTypeString>(),
            "text"
        });
    } else {
        // 变量列
        auto col = ColumnString::create();
        col->insert_data(text.data(), text.size());
        
        block.insert({
            std::move(col),
            std::make_shared<DataTypeString>(),
            "text"
        });
    }
    
    return block;
}

// 创建包含字符串和数组的测试数据
Block create_test_block_with_array(const std::string& text, const std::vector<std::string>& array_values, 
                                  bool is_text_const = false, bool is_array_const = false) {
    Block block = create_test_block_with_string(text, is_text_const);
    
    if (is_array_const) {
        // 常量数组列
        auto array_data = ColumnString::create();
        auto offsets = ColumnVector<UInt64>::create();
        
        size_t offset = 0;
        for (const auto& value : array_values) {
            array_data->insert_data(value.data(), value.size());
            offset += 1;
        }
        offsets->insert_value(offset);
        
        auto array_col = ColumnArray::create(std::move(array_data), std::move(offsets));
        auto const_array_col = ColumnConst::create(std::move(array_col), 1);
        
        block.insert({
            std::move(const_array_col),
            std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()),
            "array"
        });
    } else {
        // 变量数组列
        auto array_data = ColumnString::create();
        auto offsets = ColumnVector<UInt64>::create();
        
        size_t offset = 0;
        for (const auto& value : array_values) {
            array_data->insert_data(value.data(), value.size());
            offset += 1;
        }
        offsets->insert_value(offset);
        
        auto array_col = ColumnArray::create(std::move(array_data), std::move(offsets));
        
        block.insert({
            std::move(array_col),
            std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()),
            "array"
        });
    }
    
    return block;
}

// 测试翻译函数
TEST(LLMPromptTest, TranslateTest) {
    FunctionLLMTranslate translate_func;
    
    // 测试变量情况
    {
        auto block = create_test_block_with_string("Hello world", false);
        block.insert({
            ColumnString::create(1)->insert_data("Chinese", strlen("Chinese")),
            std::make_shared<DataTypeString>(),
            "lang"
        });
        
        std::string prompt;
        Status status = translate_func.build_prompt(block, {0, 1}, 0, prompt);
        
        ASSERT_TRUE(status.ok());
        EXPECT_TRUE(prompt.find("Translate the following text to Chinese") != std::string::npos);
        EXPECT_TRUE(prompt.find("Hello world") != std::string::npos);
    }
    
    // 测试常量情况
    {
        auto block = create_test_block_with_string("Hello world", true);
        auto lang_col = ColumnConst::create(ColumnString::create(1), 1);
        lang_col->get_data_column().insert_data("French", strlen("French"));
        
        block.insert({
            std::move(lang_col),
            std::make_shared<DataTypeString>(),
            "lang"
        });
        
        std::string prompt;
        Status status = translate_func.build_prompt(block, {0, 1}, 0, prompt);
        
        ASSERT_TRUE(status.ok());
        EXPECT_TRUE(prompt.find("Translate the following text to French") != std::string::npos);
        EXPECT_TRUE(prompt.find("Hello world") != std::string::npos);
    }
}

// 测试摘要函数
TEST(LLMPromptTest, SummarizeTest) {
    FunctionLLMSummarize summarize_func;
    
    auto block = create_test_block_with_string(
        "This is a long text that needs to be summarized. It contains multiple sentences and ideas.", 
        false
    );
    
    std::string prompt;
    Status status = summarize_func.build_prompt(block, {0}, 0, prompt);
    
    ASSERT_TRUE(status.ok());
    EXPECT_TRUE(prompt.find("Summarize the following text") != std::string::npos);
    EXPECT_TRUE(prompt.find("Return only the summary") != std::string::npos);
    EXPECT_TRUE(prompt.find("This is a long text") != std::string::npos);
}

// 测试情感分析函数
TEST(LLMPromptTest, SentimentTest) {
    FunctionLLMSentiment sentiment_func;
    
    auto block = create_test_block_with_string("I love this product!", false);
    
    std::string prompt;
    Status status = sentiment_func.build_prompt(block, {0}, 0, prompt);
    
    ASSERT_TRUE(status.ok());
    EXPECT_TRUE(prompt.find("Classify the text below into one of the following labels") != std::string::npos);
    EXPECT_TRUE(prompt.find("[positive, negative, neutral, mixed]") != std::string::npos);
    EXPECT_TRUE(prompt.find("Output only the label") != std::string::npos);
    EXPECT_TRUE(prompt.find("I love this product!") != std::string::npos);
}

// 测试信息掩盖函数
TEST(LLMPromptTest, MaskTest) {
    FunctionLLMMask mask_func;
    
    auto block = create_test_block_with_array(
        "My name is John Doe and my email is john@example.com", 
        {"name", "email"}, 
        false, 
        false
    );
    
    std::string prompt;
    Status status = mask_func.build_prompt(block, {0, 1}, 0, prompt);
    
    ASSERT_TRUE(status.ok());
    EXPECT_TRUE(prompt.find("Identify and mask sensitive information") != std::string::npos);
    EXPECT_TRUE(prompt.find("replace the entire value with \"[MASKED]\"") != std::string::npos);
    EXPECT_TRUE(prompt.find("Labels: [\"name\", \"email\"]") != std::string::npos);
    EXPECT_TRUE(prompt.find("My name is John Doe") != std::string::npos);
}

// 测试文本生成函数
TEST(LLMPromptTest, GenerateTest) {
    FunctionLLMGenerate generate_func;
    
    auto block = create_test_block_with_string("Write a poem about nature", false);
    
    std::string prompt;
    Status status = generate_func.build_prompt(block, {0}, 0, prompt);
    
    ASSERT_TRUE(status.ok());
    EXPECT_TRUE(prompt.find("Generate a response based on the following input") != std::string::npos);
    EXPECT_TRUE(prompt.find("Output only the generated text") != std::string::npos);
    EXPECT_TRUE(prompt.find("Write a poem about nature") != std::string::npos);
}

// 测试语法修复函数
TEST(LLMPromptTest, FixGrammarTest) {
    FunctionLLMFixGrammar fix_grammar_func;
    
    auto block = create_test_block_with_string("I has went to the store yesterday", false);
    
    std::string prompt;
    Status status = fix_grammar_func.build_prompt(block, {0}, 0, prompt);
    
    ASSERT_TRUE(status.ok());
    EXPECT_TRUE(prompt.find("Fix the grammar in the text below") != std::string::npos);
    EXPECT_TRUE(prompt.find("Output only the corrected text") != std::string::npos);
    EXPECT_TRUE(prompt.find("I has went to the store yesterday") != std::string::npos);
}

// 测试信息提取函数
TEST(LLMPromptTest, ExtractTest) {
    FunctionLLMExtract extract_func;
    
    auto block = create_test_block_with_array(
        "John is 25 years old and works at Google. His email is john@example.com", 
        {"name", "age", "company"}, 
        false, 
        false
    );
    
    std::string prompt;
    Status status = extract_func.build_prompt(block, {0, 1}, 0, prompt);
    
    ASSERT_TRUE(status.ok());
    EXPECT_TRUE(prompt.find("Extract a value for each of the JSON encoded labels") != std::string::npos);
    EXPECT_TRUE(prompt.find("Labels: [\"name\", \"age\", \"company\"]") != std::string::npos);
    EXPECT_TRUE(prompt.find("Output the extracted values as a JSON object") != std::string::npos);
    EXPECT_TRUE(prompt.find("John is 25 years old") != std::string::npos);
}

// 测试分类函数
TEST(LLMPromptTest, ClassifyTest) {
    FunctionLLMClassify classify_func;
    
    auto block = create_test_block_with_array(
        "The sky is blue and the weather is sunny", 
        {"weather", "news", "sports", "technology"}, 
        true, 
        true
    );
    
    std::string prompt;
    Status status = classify_func.build_prompt(block, {0, 1}, 0, prompt);
    
    ASSERT_TRUE(status.ok());
    EXPECT_TRUE(prompt.find("Classify the text below into one of the following JSON encoded labels") != std::string::npos);
    EXPECT_TRUE(prompt.find("[\"weather\", \"news\", \"sports\", \"technology\"]") != std::string::npos);
    EXPECT_TRUE(prompt.find("Output only the label without any quotation marks") != std::string::npos);
    EXPECT_TRUE(prompt.find("The sky is blue") != std::string::npos);
}

// 测试混合输入情况
TEST(LLMPromptTest, MixedInputsTest) {
    // 测试常量文本 + 变量数组
    {
        FunctionLLMExtract extract_func;
        
        auto block = create_test_block_with_array(
            "John is 25 years old and works at Google", 
            {"name", "age", "company"}, 
            true,  // 常量文本
            false  // 变量数组
        );
        
        std::string prompt;
        Status status = extract_func.build_prompt(block, {0, 1}, 0, prompt);
        
        ASSERT_TRUE(status.ok());
        EXPECT_TRUE(prompt.find("Extract a value for each of the JSON encoded labels") != std::string::npos);
        EXPECT_TRUE(prompt.find("John is 25 years old") != std::string::npos);
    }
    
    // 测试变量文本 + 常量数组
    {
        FunctionLLMClassify classify_func;
        
        auto block = create_test_block_with_array(
            "This article is about technology trends", 
            {"tech", "politics", "sports"}, 
            false,  // 变量文本
            true    // 常量数组
        );
        
        std::string prompt;
        Status status = classify_func.build_prompt(block, {0, 1}, 0, prompt);
        
        ASSERT_TRUE(status.ok());
        EXPECT_TRUE(prompt.find("Classify the text below") != std::string::npos);
        EXPECT_TRUE(prompt.find("[\"tech\", \"politics\", \"sports\"]") != std::string::npos);
        EXPECT_TRUE(prompt.find("This article is about technology trends") != std::string::npos);
    }
}

// 边界情况测试
TEST(LLMPromptTest, EdgeCasesTest) {
    // 测试空文本
    {
        FunctionLLMSentiment sentiment_func;
        
        auto block = create_test_block_with_string("", false);
        
        std::string prompt;
        Status status = sentiment_func.build_prompt(block, {0}, 0, prompt);
        
        ASSERT_TRUE(status.ok());
        EXPECT_TRUE(prompt.find("Text: ") != std::string::npos);
    }
    
    // 测试空数组
    {
        FunctionLLMClassify classify_func;
        
        auto block = create_test_block_with_array("Some text", {}, false, false);
        
        std::string prompt;
        Status status = classify_func.build_prompt(block, {0, 1}, 0, prompt);
        
        ASSERT_TRUE(status.ok());
        EXPECT_TRUE(prompt.find("[]") != std::string::npos);
    }
}

} // namespace doris::vectorized

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}