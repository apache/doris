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
};

class FunctionAIFilterBatchTestHelper : public AIFunction<FunctionAIFilterBatchTestHelper> {
public:
    friend class AIFunction<FunctionAIFilterBatchTestHelper>;

    static constexpr auto name = "ai_filter";
    static constexpr auto system_prompt = FunctionAIFilter::system_prompt;
    static constexpr size_t number_of_arguments = FunctionAIFilter::number_of_arguments;

    using AIFunction<FunctionAIFilterBatchTestHelper>::execute_batch_request;

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeBool>();
    }

    MutableColumnPtr create_result_column() const { return ColumnUInt8::create(); }

    Status append_batch_results(const std::vector<std::string>& batch_results,
                                IColumn& col_result) const {
        auto& bool_col = assert_cast<ColumnUInt8&>(col_result);
        for (const auto& batch_result : batch_results) {
            std::string_view trimmed = doris::trim(batch_result);
            if (trimmed != "1" && trimmed != "0") {
                return Status::RuntimeError("Failed to parse boolean value: " +
                                            std::string(trimmed));
            }
            bool_col.insert_value(static_cast<UInt8>(trimmed == "1"));
        }
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
    setenv("AI_TEST_RESULT", R"(["0.5"])", 1);

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
    unsetenv("AI_TEST_RESULT");
}

TEST(AIFunctionTest, AISimilarityTrimWhitespace) {
    auto runtime_state = std::make_unique<MockRuntimeState>();
    auto ctx = FunctionContext::create_context(runtime_state.get(), {}, {});

    std::vector<std::pair<std::string, float>> test_cases = {
            {R"(["0.5"])", 0.5f},
            {R"(["1.0"])", 1.0f},
            {R"(["0.0"])", 0.0f},
            {" " + std::string(R"(["0.5"])"), 0.5f},
            {std::string(R"(["0.5"])") + " ", 0.5f},
            {" " + std::string(R"(["0.5"])") + " ", 0.5f},
            {"\n" + std::string(R"(["0.8"])"), 0.8f},
            {std::string(R"(["0.3"])") + "\n", 0.3f},
            {"\n" + std::string(R"(["0.7"])") + "\n", 0.7f},
            {"\t" + std::string(R"(["0.2"])") + "\t", 0.2f},
            {" \n\t" + std::string(R"(["0.9"])") + " \n\t", 0.9f},
            {"  " + std::string(R"(["0.1"])") + "  ", 0.1f},
            {"\r\n" + std::string(R"(["0.6"])") + "\r\n", 0.6f}};

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

TEST(AIFunctionTest, AISimilarityBatchExecuteTest) {
    auto runtime_state = std::make_unique<MockRuntimeState>();
    auto ctx = FunctionContext::create_context(runtime_state.get(), {}, {});

    setenv("AI_TEST_RESULT", R"(["0.5","1.0","0.0"])", 1);

    std::vector<std::string> resources = {"mock_resource"};
    std::vector<std::string> text1 = {"a", "b", "c"};
    std::vector<std::string> text2 = {"d", "e", "f"};
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

    const auto& res_col =
            assert_cast<const ColumnFloat32&>(*block.get_by_position(result_idx).column);
    ASSERT_EQ(res_col.size(), 3);
    EXPECT_FLOAT_EQ(res_col.get_data()[0], 0.5f);
    EXPECT_FLOAT_EQ(res_col.get_data()[1], 1.0f);
    EXPECT_FLOAT_EQ(res_col.get_data()[2], 0.0f);

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
    setenv("AI_TEST_RESULT", R"(["0"])", 1);

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

    ASSERT_TRUE(exec_status.ok());

    const auto& res_col =
            assert_cast<const ColumnUInt8&>(*block.get_by_position(result_idx).column);
    UInt8 val = res_col.get_data()[0];
    ASSERT_TRUE(val == 0);
    unsetenv("AI_TEST_RESULT");
}

TEST(AIFunctionTest, AIFilterExecuteMultipleRows) {
    auto runtime_state = std::make_unique<MockRuntimeState>();
    auto ctx = FunctionContext::create_context(runtime_state.get(), {}, {});

    setenv("AI_TEST_RESULT", R"(["1","1"])", 1);

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
            {R"(["0"])", 0},
            {R"(["1"])", 1},
            {" " + std::string(R"(["0"])"), 0},
            {std::string(R"(["0"])") + " ", 0},
            {" " + std::string(R"(["0"])") + " ", 0},
            {"\n" + std::string(R"(["0"])"), 0},
            {std::string(R"(["0"])") + "\n", 0},
            {"\n" + std::string(R"(["0"])") + "\n", 0},
            {"\t" + std::string(R"(["1"])") + "\t", 1},
            {" \n\t" + std::string(R"(["1"])") + " \n\t", 1},
            {"  " + std::string(R"(["1"])") + "  ", 1},
            {"\r\n" + std::string(R"(["0"])") + "\r\n", 0}};

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
            R"(["2"])",    R"(["maybe"])", R"(["ok"])",    R"([""])",       R"(["01"])",
            R"(["0.5"])",  R"(["sure"])",  R"(["truee"])", R"(["falsee"])", R"(["yess"])",
            R"(["noo"])",  R"(["true"])",  R"(["false"])", R"(["yes"])",    R"(["no"])",
            R"(["TRUE"])", R"(["FALSE"])", R"(["YES"])",   "[\"NO\"]"};

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

TEST(AIFunctionTest, AIFilterBatchExecuteTest) {
    auto runtime_state = std::make_unique<MockRuntimeState>();
    auto ctx = FunctionContext::create_context(runtime_state.get(), {}, {});

    setenv("AI_TEST_RESULT", R"(["1","0","1"])", 1);

    std::vector<std::string> resources = {"mock_resource"};
    std::vector<std::string> texts = {"valid text", "invalid text", "valid again"};
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

    ASSERT_TRUE(exec_status.ok());

    const auto& res_col =
            assert_cast<const ColumnUInt8&>(*block.get_by_position(result_idx).column);
    ASSERT_EQ(res_col.size(), 3);
    EXPECT_EQ(res_col.get_data()[0], 1);
    EXPECT_EQ(res_col.get_data()[1], 0);
    EXPECT_EQ(res_col.get_data()[2], 1);

    unsetenv("AI_TEST_RESULT");
}

TEST(AIFunctionTest, AIFilterBatchLengthMismatch) {
    auto runtime_state = std::make_unique<MockRuntimeState>();
    auto ctx = FunctionContext::create_context(runtime_state.get(), {}, {});

    setenv("AI_TEST_RESULT", R"(["1","0"])", 1);

    std::vector<std::string> resources = {"mock_resource"};
    std::vector<std::string> texts = {"row1", "row2", "row3"};
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

    ASSERT_FALSE(exec_status.ok());
    ASSERT_TRUE(exec_status.to_string().find("expected 3 items but got 2") != std::string::npos);

    unsetenv("AI_TEST_RESULT");
}

TEST(AIFunctionTest, AIFilterBatchInvalidJson) {
    auto runtime_state = std::make_unique<MockRuntimeState>();
    auto ctx = FunctionContext::create_context(runtime_state.get(), {}, {});

    std::vector<std::string> invalid_cases = {"1,0", "{}", "", "   "};

    for (const auto& invalid_value : invalid_cases) {
        setenv("AI_TEST_RESULT", invalid_value.c_str(), 1);

        std::vector<std::string> resources = {"mock_resource"};
        std::vector<std::string> texts = {"row1", "row2"};
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
                << "Should have failed for invalid batch json: '" << invalid_value << "'";
        ASSERT_TRUE(
                exec_status.to_string().find("Invalid batch result format") != std::string::npos ||
                exec_status.to_string().find("expected 2 items but got 1") != std::string::npos);
    }

    unsetenv("AI_TEST_RESULT");
}

TEST(AIFunctionTest, AIFilterBatchInvalidElement) {
    auto runtime_state = std::make_unique<MockRuntimeState>();
    auto ctx = FunctionContext::create_context(runtime_state.get(), {}, {});

    std::vector<std::string> invalid_cases = {R"(["1","2"])", R"(["1",0])", R"(["yes","no"])"};

    for (const auto& invalid_value : invalid_cases) {
        setenv("AI_TEST_RESULT", invalid_value.c_str(), 1);

        std::vector<std::string> resources = {"mock_resource"};
        std::vector<std::string> texts = {"row1", "row2"};
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
                << "Should have failed for invalid batch element: '" << invalid_value << "'";
        ASSERT_TRUE(exec_status.to_string().find("Failed to parse boolean value") !=
                            std::string::npos ||
                    exec_status.to_string().find("Invalid batch result format") !=
                            std::string::npos);
    }

    unsetenv("AI_TEST_RESULT");
}

TEST(AIFunctionTest, AIFilterBatchSplitByWindow) {
    TQueryOptions query_options = create_fake_query_options();
    query_options.__set_ai_context_window_size(128 * 1024);
    auto query_ctx = MockQueryContext::create(TUniqueId(), ExecEnv::GetInstance(), query_options);
    query_ctx->set_mock_ai_resource();
    TQueryGlobals query_globals;
    auto runtime_state = std::make_unique<MockRuntimeState>(
            TUniqueId(), 0, query_options, query_globals, nullptr, query_ctx.get());
    auto ctx = FunctionContext::create_context(runtime_state.get(), {}, {});

    setenv("AI_TEST_RESULT", R"(["1"])", 1);

    std::vector<std::string> resources = {"mock_resource"};
    std::vector<std::string> texts = {std::string(70 * 1024, 'a'), std::string(70 * 1024, 'b'),
                                      std::string(70 * 1024, 'c')};
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

    ASSERT_TRUE(exec_status.ok());

    const auto& res_col =
            assert_cast<const ColumnUInt8&>(*block.get_by_position(result_idx).column);
    ASSERT_EQ(res_col.size(), 3);
    EXPECT_EQ(res_col.get_data()[0], 1);
    EXPECT_EQ(res_col.get_data()[1], 1);
    EXPECT_EQ(res_col.get_data()[2], 1);

    unsetenv("AI_TEST_RESULT");
}

TEST(AIFunctionTest, AIFilterSingleRowExceedsBatchWindow) {
    TQueryOptions query_options = create_fake_query_options();
    query_options.__set_ai_context_window_size(128 * 1024);
    auto query_ctx = MockQueryContext::create(TUniqueId(), ExecEnv::GetInstance(), query_options);
    query_ctx->set_mock_ai_resource();
    TQueryGlobals query_globals;
    auto runtime_state = std::make_unique<MockRuntimeState>(
            TUniqueId(), 0, query_options, query_globals, nullptr, query_ctx.get());
    auto ctx = FunctionContext::create_context(runtime_state.get(), {}, {});

    std::vector<std::string> resources = {"mock_resource"};
    std::vector<std::string> texts = {std::string(130 * 1024, 'x')};
    auto col_resource = ColumnHelper::create_column<DataTypeString>(resources);
    auto col_text = ColumnHelper::create_column<DataTypeString>(texts);

    Block block;
    block.insert({std::move(col_resource), std::make_shared<DataTypeString>(), "resource"});
    block.insert({std::move(col_text), std::make_shared<DataTypeString>(), "text"});
    block.insert({nullptr, std::make_shared<DataTypeBool>(), "result"});

    ColumnNumbers arguments = {0, 1};
    size_t result_idx = 2;

    auto filter_func = FunctionAIFilter::create();
    // Even if a single row exceeds the batch window, it should be sent as a standalone request.
    setenv("AI_TEST_RESULT", "[\"1\"]", 1);
    Status exec_status =
            filter_func->execute_impl(ctx.get(), block, arguments, result_idx, texts.size());

    ASSERT_TRUE(exec_status.ok());

    const auto& res_col =
            assert_cast<const ColumnUInt8&>(*block.get_by_position(result_idx).column);
    ASSERT_EQ(res_col.size(), 1);
    EXPECT_EQ(res_col.get_data()[0], 1);

    unsetenv("AI_TEST_RESULT");
}

TEST(AIFunctionTest, AIFilterOversizedRowFlushesHistoryBatchFirst) {
    TQueryOptions query_options = create_fake_query_options();
    query_options.__set_ai_context_window_size(128 * 1024);
    auto query_ctx = MockQueryContext::create(TUniqueId(), ExecEnv::GetInstance(), query_options);
    query_ctx->set_mock_ai_resource();
    TQueryGlobals query_globals;
    auto runtime_state = std::make_unique<MockRuntimeState>(
            TUniqueId(), 0, query_options, query_globals, nullptr, query_ctx.get());
    auto ctx = FunctionContext::create_context(runtime_state.get(), {}, {});

    std::vector<std::string> resources = {"mock_resource", "mock_resource"};
    std::vector<std::string> texts = {"small row", std::string(130 * 1024, 'x')};
    auto col_resource = ColumnHelper::create_column<DataTypeString>(resources);
    auto col_text = ColumnHelper::create_column<DataTypeString>(texts);

    Block block;
    block.insert({std::move(col_resource), std::make_shared<DataTypeString>(), "resource"});
    block.insert({std::move(col_text), std::make_shared<DataTypeString>(), "text"});
    block.insert({nullptr, std::make_shared<DataTypeBool>(), "result"});

    ColumnNumbers arguments = {0, 1};
    size_t result_idx = 2;

    auto filter_func = FunctionAIFilter::create();
    setenv("AI_TEST_RESULT", R"(["1"])", 1);
    Status exec_status =
            filter_func->execute_impl(ctx.get(), block, arguments, result_idx, texts.size());

    ASSERT_TRUE(exec_status.ok()) << exec_status.to_string();

    const auto& res_col =
            assert_cast<const ColumnUInt8&>(*block.get_by_position(result_idx).column);
    ASSERT_EQ(res_col.size(), 2);
    EXPECT_EQ(res_col.get_data()[0], 1);
    EXPECT_EQ(res_col.get_data()[1], 1);

    unsetenv("AI_TEST_RESULT");
}

TEST(AIFunctionTest, AIFilterUsesAiContextWindowSizeSessionVariable) {
    TQueryOptions query_options = create_fake_query_options();
    query_options.__set_ai_context_window_size(16);
    auto query_ctx = MockQueryContext::create(TUniqueId(), ExecEnv::GetInstance(), query_options);
    query_ctx->set_mock_ai_resource();
    TQueryGlobals query_globals;
    auto runtime_state = std::make_unique<MockRuntimeState>(
            TUniqueId(), 0, query_options, query_globals, nullptr, query_ctx.get());
    auto ctx = FunctionContext::create_context(runtime_state.get(), {}, {});

    setenv("AI_TEST_RESULT", R"(["1"])", 1);

    std::vector<std::string> resources = {"mock_resource"};
    std::vector<std::string> texts = {"12345678901234567890", "abcdefghijabcdefghij"};
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

    ASSERT_TRUE(exec_status.ok()) << exec_status.to_string();

    const auto& res_col =
            assert_cast<const ColumnUInt8&>(*block.get_by_position(result_idx).column);
    ASSERT_EQ(res_col.size(), 2);
    EXPECT_EQ(res_col.get_data()[0], 1);
    EXPECT_EQ(res_col.get_data()[1], 1);

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

    ASSERT_DEATH(
            {
                auto sentiment_func = FunctionAISentiment::create();
                Status exec_status = sentiment_func->execute_impl(ctx.get(), block, arguments,
                                                                  result_idx, texts.size());
                static_cast<void>(exec_status);
            },
            "it != ai_resources->end");
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

TEST(AIFunctionTest, AIStringFunctionBatchExecuteTest) {
    auto runtime_state = std::make_unique<MockRuntimeState>();
    auto ctx = FunctionContext::create_context(runtime_state.get(), {}, {});

    setenv("AI_TEST_RESULT", R"(["positive","negative","neutral"])", 1);

    std::vector<std::string> resources = {"mock_resource"};
    std::vector<std::string> texts = {"great", "bad", "okay"};
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

    ASSERT_TRUE(exec_status.ok());

    const auto& res_col =
            assert_cast<const ColumnString&>(*block.get_by_position(result_idx).column);
    ASSERT_EQ(res_col.size(), 3);
    EXPECT_EQ(res_col.get_data_at(0).to_string(), "positive");
    EXPECT_EQ(res_col.get_data_at(1).to_string(), "negative");
    EXPECT_EQ(res_col.get_data_at(2).to_string(), "neutral");

    unsetenv("AI_TEST_RESULT");
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

    ASSERT_DEATH(
            {
                auto sentiment_func = FunctionAISentiment::create();
                Status exec_status = sentiment_func->execute_impl(ctx.get(), block, arguments,
                                                                  result_idx, texts.size());
                static_cast<void>(exec_status);
            },
            "ai_resources");
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

class FunctionEmbedTestHelper : public FunctionEmbed {
public:
    using FunctionEmbed::normalize_endpoint;
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

TEST(AIFunctionTest, NormalizeGeminiGenerateEndpointFromBaseVersion) {
    TAIResource resource;
    resource.provider_type = "gemini";
    resource.model_name = "gemini-pro";
    resource.endpoint = "https://generativelanguage.googleapis.com/v1beta";

    FunctionAISentimentTestHelper::normalize_endpoint(resource);
    ASSERT_EQ(resource.endpoint,
              "https://generativelanguage.googleapis.com/v1beta/models/gemini-pro:generateContent");
}

TEST(AIFunctionTest, NormalizeGeminiEmbedEndpointFromBaseVersion) {
    TAIResource resource;
    resource.provider_type = "GEMINI";
    resource.model_name = "gemini-embedding-2-preview";
    resource.endpoint = "https://generativelanguage.googleapis.com/v1beta";

    FunctionEmbedTestHelper::normalize_endpoint(resource);
    ASSERT_EQ(resource.endpoint,
              "https://generativelanguage.googleapis.com/v1beta/models/"
              "gemini-embedding-2-preview:batchEmbedContents");
}

TEST(AIFunctionTest, NormalizeGeminiEndpointNoopForNonBasePath) {
    TAIResource resource;
    resource.provider_type = "gemini";
    resource.model_name = "gemini-pro";
    resource.endpoint =
            "https://generativelanguage.googleapis.com/v1beta/models/gemini-pro:generateContent";

    FunctionAISentimentTestHelper::normalize_endpoint(resource);
    ASSERT_EQ(resource.endpoint,
              "https://generativelanguage.googleapis.com/v1beta/models/gemini-pro:generateContent");
}

TEST(AIFunctionTest, NormalizeGeminiEmbedLegacySingleEndpointToBatchEndpoint) {
    TAIResource resource;
    resource.provider_type = "gemini";
    resource.model_name = "gemini-embedding-2-preview";
    resource.endpoint =
            "https://generativelanguage.googleapis.com/v1beta/models/"
            "gemini-embedding-2-preview:embedContent";

    FunctionEmbedTestHelper::normalize_endpoint(resource);
    ASSERT_EQ(resource.endpoint,
              "https://generativelanguage.googleapis.com/v1beta/models/"
              "gemini-embedding-2-preview:batchEmbedContents");
}

TEST(AIFunctionTest, ExecuteBatchRequestSuccess) {
    TQueryOptions query_options = create_fake_query_options();
    query_options.__set_query_timeout(5);
    auto query_ctx = MockQueryContext::create(TUniqueId(), ExecEnv::GetInstance(), query_options);
    TQueryGlobals query_globals;
    RuntimeState runtime_state(TUniqueId(), 0, query_options, query_globals, nullptr,
                               query_ctx.get());
    auto ctx = FunctionContext::create_context(&runtime_state, {}, {});

    OneShotHttpServer server(200, R"({"choices":[{"message":{"content":"[\"1\",\"0\"]"}}]})");

    TAIResource config;
    config.endpoint = server.endpoint();
    config.provider_type = "OPENAI";
    config.model_name = "test-model";
    config.api_key = "secret";
    config.max_retries = 1;

    std::shared_ptr<AIAdapter> adapter = std::make_shared<OpenAIAdapter>();
    adapter->init(config);

    FunctionAIFilterBatchTestHelper helper;
    std::vector<std::string> results;
    Status st = helper.execute_batch_request({"first row", "second row"}, results, config, adapter,
                                             ctx.get());

    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_EQ(results.size(), 2);
    EXPECT_EQ(results[0], "1");
    EXPECT_EQ(results[1], "0");

    std::string request = server.join_and_get_request();
    ASSERT_NE(request.find("Authorization: Bearer secret"), std::string::npos);
    ASSERT_NE(
            request.find(
                    R"([{\"idx\":0,\"input\":\"first row\"},{\"idx\":1,\"input\":\"second row\"}])"),
            std::string::npos);
}

TEST(AIFunctionTest, ExecuteBatchRequestResultSizeMismatch) {
    TQueryOptions query_options = create_fake_query_options();
    query_options.__set_query_timeout(5);
    auto query_ctx = MockQueryContext::create(TUniqueId(), ExecEnv::GetInstance(), query_options);
    TQueryGlobals query_globals;
    RuntimeState runtime_state(TUniqueId(), 0, query_options, query_globals, nullptr,
                               query_ctx.get());
    auto ctx = FunctionContext::create_context(&runtime_state, {}, {});

    OneShotHttpServer server(200, R"({"choices":[{"message":{"content":"[\"1\"]"}}]})");

    TAIResource config;
    config.endpoint = server.endpoint();
    config.provider_type = "OPENAI";
    config.model_name = "test-model";
    config.api_key = "secret";
    config.max_retries = 1;

    std::shared_ptr<AIAdapter> adapter = std::make_shared<OpenAIAdapter>();
    adapter->init(config);

    FunctionAIFilterBatchTestHelper helper;
    std::vector<std::string> results;
    Status st = helper.execute_batch_request({"first row", "second row"}, results, config, adapter,
                                             ctx.get());

    ASSERT_FALSE(st.ok());
    ASSERT_NE(st.to_string().find(
                      "Failed to parse ai_filter batch result, expected 2 items but got 1"),
              std::string::npos);
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

} // namespace doris
