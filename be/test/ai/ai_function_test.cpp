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

#include <arpa/inet.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <string>
#include <thread>

#include "core/block/block.h"
#include "core/column/column_array.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "exprs/function/ai/ai_adapter.h"
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
#include "testutil/column_helper.h"
#include "testutil/mock/mock_runtime_state.h"

namespace doris {

class FunctionAITransportTestHelper : public FunctionAISentiment {
public:
    using FunctionAISentiment::do_send_request;
    using FunctionAISentiment::execute_embedding_request;
};

class EmptyEmbeddingResultAdapter : public MockAdapter {
public:
    Status parse_embedding_response(const std::string& /*response_body*/,
                                    std::vector<std::vector<float>>& /*results*/) const override {
        return Status::OK();
    }
};

class OneShotHttpServer {
public:
    OneShotHttpServer(int status_code, std::string response_body)
            : _status_code(status_code), _response_body(std::move(response_body)) {
        _listen_fd = socket(AF_INET, SOCK_STREAM, 0);
        DCHECK_GE(_listen_fd, 0);

        int reuse_addr = 1;
        int ret = setsockopt(_listen_fd, SOL_SOCKET, SO_REUSEADDR, &reuse_addr, sizeof(reuse_addr));
        DCHECK_EQ(ret, 0);

        sockaddr_in addr {};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = 0;
        ret = bind(_listen_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
        DCHECK_EQ(ret, 0);
        ret = listen(_listen_fd, 1);
        DCHECK_EQ(ret, 0);

        socklen_t addr_len = sizeof(addr);
        ret = getsockname(_listen_fd, reinterpret_cast<sockaddr*>(&addr), &addr_len);
        DCHECK_EQ(ret, 0);
        _port = ntohs(addr.sin_port);

        _thread = std::thread([this] { _serve_once(); });
    }

    ~OneShotHttpServer() {
        if (_thread.joinable()) {
            _thread.join();
        }
        if (_listen_fd >= 0) {
            close(_listen_fd);
        }
    }

    std::string endpoint() const { return "http://127.0.0.1:" + std::to_string(_port); }

    std::string join_and_get_request() {
        if (_thread.joinable()) {
            _thread.join();
        }
        return _request;
    }

private:
    void _serve_once() {
        int client_fd = accept(_listen_fd, nullptr, nullptr);
        DCHECK_GE(client_fd, 0);

        char buffer[4096];
        while (true) {
            ssize_t read_bytes = recv(client_fd, buffer, sizeof(buffer), 0);
            if (read_bytes <= 0) {
                break;
            }
            _request.append(buffer, read_bytes);
            if (_request.find("\r\n\r\n") != std::string::npos) {
                auto header_end = _request.find("\r\n\r\n");
                size_t body_length = 0;
                size_t length_pos = _request.find("Content-Length:");
                if (length_pos != std::string::npos) {
                    size_t value_begin = length_pos + sizeof("Content-Length:") - 1;
                    size_t value_end = _request.find("\r\n", value_begin);
                    std::string length_str = _request.substr(value_begin, value_end - value_begin);
                    body_length = std::stoul(length_str);
                }
                if (_request.size() >= header_end + 4 + body_length) {
                    break;
                }
            }
        }

        std::string status_text = _status_code == 200 ? "OK" : "Internal Server Error";
        std::string response = "HTTP/1.1 " + std::to_string(_status_code) + " " + status_text +
                               "\r\nContent-Type: application/json\r\nContent-Length: " +
                               std::to_string(_response_body.size()) +
                               "\r\nConnection: close\r\n\r\n" + _response_body;
        size_t sent_bytes = 0;
        while (sent_bytes < response.size()) {
            ssize_t sent =
                    send(client_fd, response.data() + sent_bytes, response.size() - sent_bytes, 0);
            DCHECK_GT(sent, 0);
            sent_bytes += sent;
        }
        shutdown(client_fd, SHUT_RDWR);
        close(client_fd);
    }

    int _listen_fd = -1;
    uint16_t _port = 0;
    int _status_code;
    std::string _response_body;
    std::string _request;
    std::thread _thread;
};

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

TEST(AIFunctionTest, AISimilarityExecuteTest) {
    auto runtime_state = std::make_unique<MockRuntimeState>();
    auto ctx = FunctionContext::create_context(runtime_state.get(), {}, {});

    std::vector<std::string> resources = {"mock_resource"};
    std::vector<std::string> text1 = {"I like this dish"};
    std::vector<std::string> text2 = {"This dish is very good"};
    auto col_resource = ColumnHelper::create_column<DataTypeString>(resources);
    auto col_text1 = ColumnHelper::create_column<DataTypeString>(text1);
    auto col_text2 = ColumnHelper::create_column<DataTypeString>(text2);

    Block block;
    block.insert({std::move(col_resource), std::make_shared<DataTypeString>(), "resource"});
    block.insert({std::move(col_text1), std::make_shared<DataTypeString>(), "text1"});
    block.insert({std::move(col_text2), std::make_shared<DataTypeString>(), "text2"});
    block.insert({nullptr, std::make_shared<DataTypeFloat32>(), "result"});

    ColumnNumbers arguments = {0, 1, 2};
    size_t result_idx = 3;

    auto similarity_func = FunctionAISimilarity::create();
    Status exec_status =
            similarity_func->execute_impl(ctx.get(), block, arguments, result_idx, text1.size());

    ASSERT_TRUE(exec_status.ok());
}

TEST(AIFunctionTest, AISimilarityTrimWhitespace) {
    auto runtime_state = std::make_unique<MockRuntimeState>();
    auto ctx = FunctionContext::create_context(runtime_state.get(), {}, {});

    std::vector<std::pair<std::string, float>> test_cases = {
            {"0.5", 0.5f},        {"1.0", 1.0f},     {"0.0", 0.0f},           {" 0.5", 0.5f},
            {"0.5 ", 0.5f},       {" 0.5 ", 0.5f},   {"\n0.8", 0.8f},         {"0.3\n", 0.3f},
            {"\n0.7\n", 0.7f},    {"\t0.2\t", 0.2f}, {" \n\t0.9 \n\t", 0.9f}, {"  0.1  ", 0.1f},
            {"\r\n0.6\r\n", 0.6f}};

    for (const auto& test_case : test_cases) {
        setenv("AI_TEST_RESULT", test_case.first.c_str(), 1);

        std::vector<std::string> resources = {"mock_resource"};
        std::vector<std::string> text1 = {"Test text 1"};
        std::vector<std::string> text2 = {"Test text 2"};
        auto col_resource = ColumnHelper::create_column<DataTypeString>(resources);
        auto col_text1 = ColumnHelper::create_column<DataTypeString>(text1);
        auto col_text2 = ColumnHelper::create_column<DataTypeString>(text2);

        Block block;
        block.insert({std::move(col_resource), std::make_shared<DataTypeString>(), "resource"});
        block.insert({std::move(col_text1), std::make_shared<DataTypeString>(), "text1"});
        block.insert({std::move(col_text2), std::make_shared<DataTypeString>(), "text2"});
        block.insert({nullptr, std::make_shared<DataTypeFloat32>(), "result"});

        ColumnNumbers arguments = {0, 1, 2};
        size_t result_idx = 3;

        auto similarity_func = FunctionAISimilarity::create();
        Status exec_status = similarity_func->execute_impl(ctx.get(), block, arguments, result_idx,
                                                           text1.size());

        ASSERT_TRUE(exec_status.ok()) << "Failed for test case: '" << test_case.first << "'";

        const auto& res_col =
                assert_cast<const ColumnFloat32&>(*block.get_by_position(result_idx).column);
        float val = res_col.get_data()[0];
        ASSERT_FLOAT_EQ(val, test_case.second)
                << "Failed for test case: '" << test_case.first
                << "', expected: " << test_case.second << ", got: " << val;
    }

    unsetenv("AI_TEST_RESULT");
}

TEST(AIFunctionTest, AISimilarityInvalidValue) {
    auto runtime_state = std::make_unique<MockRuntimeState>();
    auto ctx = FunctionContext::create_context(runtime_state.get(), {}, {});

    std::vector<std::string> invalid_cases = {"abc", "1.2x", "", " ", "\n\t", "1,2"};

    for (const auto& invalid_value : invalid_cases) {
        setenv("AI_TEST_RESULT", invalid_value.c_str(), 1);

        std::vector<std::string> resources = {"mock_resource"};
        std::vector<std::string> text1 = {"Test text 1"};
        std::vector<std::string> text2 = {"Test text 2"};
        auto col_resource = ColumnHelper::create_column<DataTypeString>(resources);
        auto col_text1 = ColumnHelper::create_column<DataTypeString>(text1);
        auto col_text2 = ColumnHelper::create_column<DataTypeString>(text2);

        Block block;
        block.insert({std::move(col_resource), std::make_shared<DataTypeString>(), "resource"});
        block.insert({std::move(col_text1), std::make_shared<DataTypeString>(), "text1"});
        block.insert({std::move(col_text2), std::make_shared<DataTypeString>(), "text2"});
        block.insert({nullptr, std::make_shared<DataTypeFloat32>(), "result"});

        ColumnNumbers arguments = {0, 1, 2};
        size_t result_idx = 3;

        auto similarity_func = FunctionAISimilarity::create();
        Status exec_status = similarity_func->execute_impl(ctx.get(), block, arguments, result_idx,
                                                           text1.size());

        ASSERT_FALSE(exec_status.ok())
                << "Should have failed for invalid value: '" << invalid_value << "'";
        ASSERT_NE(exec_status.to_string().find("Failed to parse float value"), std::string::npos);
    }

    unsetenv("AI_TEST_RESULT");
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

TEST(AIFunctionTest, AIFilterExecuteTest) {
    auto runtime_state = std::make_unique<MockRuntimeState>();
    auto ctx = FunctionContext::create_context(runtime_state.get(), {}, {});

    std::vector<std::string> resources = {"mock_resource"};
    std::vector<std::string> texts = {"This is a valid sentence."};
    auto col_resource = ColumnHelper::create_column<DataTypeString>(resources);
    auto col_text = ColumnHelper::create_column<DataTypeString>(texts);

    Block block;
    block.insert({std::move(col_resource), std::make_shared<DataTypeString>(), "resource"});
    block.insert({std::move(col_text), std::make_shared<DataTypeString>(), "text"});
    block.insert({nullptr, std::make_shared<DataTypeBool>(), "result"});

    ColumnNumbers arguments = {0, 1};
    size_t result_idx = 2;

    auto filter_func = FunctionAIFilter::create();
    Status exec_status =
            filter_func->execute_impl(ctx.get(), block, arguments, result_idx, texts.size());

    const auto& res_col =
            assert_cast<const ColumnUInt8&>(*block.get_by_position(result_idx).column);
    UInt8 val = res_col.get_data()[0];
    ASSERT_TRUE(val == 0);
}

TEST(AIFunctionTest, AIFilterExecuteMultipleRows) {
    auto runtime_state = std::make_unique<MockRuntimeState>();
    auto ctx = FunctionContext::create_context(runtime_state.get(), {}, {});

    setenv("AI_TEST_RESULT", " 1 ", 1);

    std::vector<std::string> resources = {"mock_resource", "mock_resource"};
    std::vector<std::string> texts = {"This is valid.", "This is also valid."};
    auto col_resource = ColumnHelper::create_column<DataTypeString>(resources);
    auto col_text = ColumnHelper::create_column<DataTypeString>(texts);

    Block block;
    block.insert({std::move(col_resource), std::make_shared<DataTypeString>(), "resource"});
    block.insert({std::move(col_text), std::make_shared<DataTypeString>(), "text"});
    block.insert({nullptr, std::make_shared<DataTypeBool>(), "result"});

    ColumnNumbers arguments = {0, 1};
    size_t result_idx = 2;

    auto filter_func = FunctionAIFilter::create();
    Status exec_status =
            filter_func->execute_impl(ctx.get(), block, arguments, result_idx, texts.size());

    unsetenv("AI_TEST_RESULT");

    ASSERT_TRUE(exec_status.ok()) << exec_status.to_string();
    const auto& res_col =
            assert_cast<const ColumnUInt8&>(*block.get_by_position(result_idx).column);
    ASSERT_EQ(res_col.size(), 2);
    ASSERT_EQ(res_col.get_data()[0], 1);
    ASSERT_EQ(res_col.get_data()[1], 1);
}

TEST(AIFunctionTest, AIFilterTrimWhitespace) {
    auto runtime_state = std::make_unique<MockRuntimeState>();
    auto ctx = FunctionContext::create_context(runtime_state.get(), {}, {});

    std::vector<std::pair<std::string, UInt8>> test_cases = {
            {"0", 0},     {"1", 1},           {" 0", 0},    {"0 ", 0},
            {" 0 ", 0},   {"\n0", 0},         {"0\n", 0},   {"\n0\n", 0},
            {"\t1\t", 1}, {" \n\t1 \n\t", 1}, {"  1  ", 1}, {"\r\n0\r\n", 0}};

    for (const auto& test_case : test_cases) {
        setenv("AI_TEST_RESULT", test_case.first.c_str(), 1);

        std::vector<std::string> resources = {"mock_resource"};
        std::vector<std::string> texts = {"Test input"};
        auto col_resource = ColumnHelper::create_column<DataTypeString>(resources);
        auto col_text = ColumnHelper::create_column<DataTypeString>(texts);

        Block block;
        block.insert({std::move(col_resource), std::make_shared<DataTypeString>(), "resource"});
        block.insert({std::move(col_text), std::make_shared<DataTypeString>(), "text"});
        block.insert({nullptr, std::make_shared<DataTypeBool>(), "result"});

        ColumnNumbers arguments = {0, 1};
        size_t result_idx = 2;

        auto filter_func = FunctionAIFilter::create();
        Status exec_status =
                filter_func->execute_impl(ctx.get(), block, arguments, result_idx, texts.size());

        ASSERT_TRUE(exec_status.ok()) << "Failed for test case: '" << test_case.first << "'";

        const auto& res_col =
                assert_cast<const ColumnUInt8&>(*block.get_by_position(result_idx).column);
        UInt8 val = res_col.get_data()[0];
        ASSERT_EQ(val, test_case.second)
                << "Failed for test case: '" << test_case.first
                << "', expected: " << (int)test_case.second << ", got: " << (int)val;
    }

    unsetenv("AI_TEST_RESULT");
}

TEST(AIFunctionTest, AIFilterInvalidValue) {
    auto runtime_state = std::make_unique<MockRuntimeState>();
    auto ctx = FunctionContext::create_context(runtime_state.get(), {}, {});

    std::vector<std::string> invalid_cases = {
            "2",    "maybe", "ok",   "",      "   ", "01", "0.5",  "sure",  "truee", "falsee",
            "yess", "noo",   "true", "false", "yes", "no", "TRUE", "FALSE", "YES",   "NO"};

    for (const auto& invalid_value : invalid_cases) {
        setenv("AI_TEST_RESULT", invalid_value.c_str(), 1);

        std::vector<std::string> resources = {"mock_resource"};
        std::vector<std::string> texts = {"Test input"};
        auto col_resource = ColumnHelper::create_column<DataTypeString>(resources);
        auto col_text = ColumnHelper::create_column<DataTypeString>(texts);

        Block block;
        block.insert({std::move(col_resource), std::make_shared<DataTypeString>(), "resource"});
        block.insert({std::move(col_text), std::make_shared<DataTypeString>(), "text"});
        block.insert({nullptr, std::make_shared<DataTypeBool>(), "result"});

        ColumnNumbers arguments = {0, 1};
        size_t result_idx = 2;

        auto filter_func = FunctionAIFilter::create();
        Status exec_status =
                filter_func->execute_impl(ctx.get(), block, arguments, result_idx, texts.size());

        ASSERT_FALSE(exec_status.ok())
                << "Should have failed for invalid value: '" << invalid_value << "'";
        ASSERT_TRUE(exec_status.to_string().find("Failed to parse boolean value") !=
                    std::string::npos)
                << "Error message should mention boolean parsing for value: '" << invalid_value
                << "'";
    }

    unsetenv("AI_TEST_RESULT");
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

TEST(AIFunctionTest, MissingAIResourcesMetadataTest) {
    auto query_ctx = MockQueryContext::create();
    TQueryOptions query_options;
    TQueryGlobals query_globals;
    RuntimeState runtime_state(TUniqueId(), 0, query_options, query_globals, nullptr,
                               query_ctx.get());
    auto ctx = FunctionContext::create_context(&runtime_state, {}, {});

    std::vector<std::string> resources = {"resource_name"};
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
    ASSERT_NE(exec_status.to_string().find("AI resources metadata missing"), std::string::npos);
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

class FunctionAISentimentTestHelper : public FunctionAISentiment {
public:
    using FunctionAISentiment::normalize_endpoint;
};

TEST(AIFunctionTest, NormalizeLegacyCompletionsEndpoint) {
    TAIResource resource;
    resource.endpoint = "https://api.openai.com/v1/completions";
    FunctionAISentimentTestHelper::normalize_endpoint(resource);
    ASSERT_EQ(resource.endpoint, "https://api.openai.com/v1/chat/completions");
}

TEST(AIFunctionTest, NormalizeEndpointNoopForOtherPaths) {
    TAIResource resource;
    resource.endpoint = "https://api.openai.com/v1/chat/completions";
    FunctionAISentimentTestHelper::normalize_endpoint(resource);
    ASSERT_EQ(resource.endpoint, "https://api.openai.com/v1/chat/completions");

    resource.endpoint = "https://localhost/v1/responses";
    FunctionAISentimentTestHelper::normalize_endpoint(resource);
    ASSERT_EQ(resource.endpoint, "https://localhost/v1/responses");
}

TEST(AIFunctionTest, DoSendRequestTransportError) {
    TQueryOptions query_options = create_fake_query_options();
    query_options.__set_query_timeout(5);
    auto query_ctx = MockQueryContext::create(TUniqueId(), ExecEnv::GetInstance(), query_options);
    TQueryGlobals query_globals;
    RuntimeState runtime_state(TUniqueId(), 0, query_options, query_globals, nullptr,
                               query_ctx.get());
    auto ctx = FunctionContext::create_context(&runtime_state, {}, {});

    TAIResource config;
    config.endpoint = "http://127.0.0.1:1";
    config.provider_type = "OPENAI";
    config.model_name = "test-model";
    config.api_key = "secret";

    std::shared_ptr<AIAdapter> adapter = std::make_shared<OpenAIAdapter>();
    adapter->init(config);

    HttpClient client;
    std::string response;
    FunctionAITransportTestHelper helper;
    Status st = helper.do_send_request(&client, "{}", response, config, adapter, ctx.get());

    ASSERT_FALSE(st.ok());
    ASSERT_EQ(response, "");
}

TEST(AIFunctionTest, DoSendRequestNon200) {
    TQueryOptions query_options = create_fake_query_options();
    query_options.__set_query_timeout(5);
    auto query_ctx = MockQueryContext::create(TUniqueId(), ExecEnv::GetInstance(), query_options);
    TQueryGlobals query_globals;
    RuntimeState runtime_state(TUniqueId(), 0, query_options, query_globals, nullptr,
                               query_ctx.get());
    auto ctx = FunctionContext::create_context(&runtime_state, {}, {});

    OneShotHttpServer server(500, R"({"error":"bad request"})");

    TAIResource config;
    config.endpoint = server.endpoint();
    config.provider_type = "OPENAI";
    config.model_name = "test-model";
    config.api_key = "secret";

    std::shared_ptr<AIAdapter> adapter = std::make_shared<OpenAIAdapter>();
    adapter->init(config);

    HttpClient client;
    std::string response;
    FunctionAITransportTestHelper helper;
    Status st = helper.do_send_request(&client, R"({"message":"hello"})", response, config, adapter,
                                       ctx.get());

    ASSERT_FALSE(st.ok());
    ASSERT_NE(st.to_string().find("http status code is not 200"), std::string::npos);
    ASSERT_EQ(response, R"({"error":"bad request"})");

    std::string request = server.join_and_get_request();
    ASSERT_NE(request.find("Authorization: Bearer secret"), std::string::npos);
    ASSERT_NE(request.find("Content-Type: application/json"), std::string::npos);
}

TEST(AIFunctionTest, DoSendRequestSuccess) {
    TQueryOptions query_options = create_fake_query_options();
    query_options.__set_query_timeout(5);
    auto query_ctx = MockQueryContext::create(TUniqueId(), ExecEnv::GetInstance(), query_options);
    TQueryGlobals query_globals;
    RuntimeState runtime_state(TUniqueId(), 0, query_options, query_globals, nullptr,
                               query_ctx.get());
    auto ctx = FunctionContext::create_context(&runtime_state, {}, {});

    OneShotHttpServer server(200, R"({"ok":true})");

    TAIResource config;
    config.endpoint = server.endpoint();
    config.provider_type = "OPENAI";
    config.model_name = "test-model";
    config.api_key = "secret";

    std::shared_ptr<AIAdapter> adapter = std::make_shared<OpenAIAdapter>();
    adapter->init(config);

    HttpClient client;
    std::string response;
    FunctionAITransportTestHelper helper;
    Status st = helper.do_send_request(&client, R"({"message":"hello"})", response, config, adapter,
                                       ctx.get());

    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_EQ(response, R"({"ok":true})");

    std::string request = server.join_and_get_request();
    ASSERT_NE(request.find("POST "), std::string::npos);
    ASSERT_NE(request.find(R"({"message":"hello"})"), std::string::npos);
}

TEST(AIFunctionTest, ExecuteEmbeddingRequestMockSuccess) {
    auto runtime_state = std::make_unique<MockRuntimeState>();
    auto ctx = FunctionContext::create_context(runtime_state.get(), {}, {});

    TAIResource config;
    config.provider_type = "MOCK";
    std::shared_ptr<AIAdapter> adapter = std::make_shared<MockAdapter>();
    adapter->init(config);

    FunctionAITransportTestHelper helper;
    std::vector<float> result;
    Status st = helper.execute_embedding_request("{}", result, config, adapter, ctx.get());

    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_EQ(result.size(), 5);
    for (size_t i = 0; i < result.size(); ++i) {
        ASSERT_FLOAT_EQ(result[i], static_cast<float>(i));
    }
}

TEST(AIFunctionTest, ExecuteEmbeddingRequestEmptyResult) {
    auto runtime_state = std::make_unique<MockRuntimeState>();
    auto ctx = FunctionContext::create_context(runtime_state.get(), {}, {});

    TAIResource config;
    config.provider_type = "MOCK";
    std::shared_ptr<AIAdapter> adapter = std::make_shared<EmptyEmbeddingResultAdapter>();
    adapter->init(config);

    FunctionAITransportTestHelper helper;
    std::vector<float> result;
    Status st = helper.execute_embedding_request("{}", result, config, adapter, ctx.get());

    ASSERT_FALSE(st.ok());
    ASSERT_NE(st.to_string().find("AI returned empty result"), std::string::npos);
}

} // namespace doris
