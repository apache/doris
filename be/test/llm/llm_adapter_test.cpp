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

#include "vec/functions/llm/llm_adapter.h"

#include <curl/curl.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "vec/functions/llm/llm_classify.h"
#include "vec/functions/llm/llm_extract.h"
#include "vec/functions/llm/llm_sentiment.h"
#include "vec/functions/llm/llm_summarize.h"

namespace doris::vectorized {
class MockHttpClient : public HttpClient {
public:
    curl_slist* get() { return this->_header_list; }

private:
    std::unordered_map<std::string, std::string> _headers;
    std::string _content_type;
};

TEST(LLM_ADAPTER_TEST, local_adapter_request) {
    LocalAdapter adapter;
    TLLMResource config;
    config.model_name = "ollama";
    config.temperature = 0.7;
    config.max_tokens = 128;
    adapter.init(config);

    // header test
    MockHttpClient mock_client;
    Status auth_status = adapter.set_authentication(&mock_client);
    ASSERT_TRUE(auth_status.ok());
    EXPECT_STREQ(mock_client.get()->data, "Content-Type: application/json");

    std::vector<std::string> inputs = {"hello world"};
    std::string request_body;
    Status st = adapter.build_request_payload(inputs, FunctionLLMSummarize::system_prompt,
                                              request_body);
    ASSERT_TRUE(st.ok());

    // body test
    rapidjson::Document doc;
    doc.Parse(request_body.c_str());
    ASSERT_FALSE(doc.HasParseError()) << "JSON parse error";
    ASSERT_TRUE(doc.IsObject()) << "JSON is not an object";

    // model name
    ASSERT_TRUE(doc.HasMember("model")) << "Missing model field";
    ASSERT_TRUE(doc["model"].IsString()) << "Model field is not a string";
    ASSERT_STREQ(doc["model"].GetString(), "ollama");

    // temperature
    ASSERT_TRUE(doc.HasMember("temperature")) << "Missing temperature field";
    ASSERT_TRUE(doc["temperature"].IsNumber()) << "Temperature field is not a number";
    ASSERT_DOUBLE_EQ(doc["temperature"].GetDouble(), 0.7);

    // max token
    ASSERT_TRUE(doc.HasMember("max_tokens")) << "Missing max_tokens field";
    ASSERT_TRUE(doc["max_tokens"].IsInt()) << "Max_tokens field is not an integer";
    ASSERT_EQ(doc["max_tokens"].GetInt(), 128);

    // content
    if (doc.HasMember("messages")) {
        ASSERT_TRUE(doc["messages"].IsArray()) << "Messages is not an array";
        ASSERT_GT(doc["messages"].Size(), 0) << "Messages array is empty";
        // system_prompt
        const auto& first_message = doc["messages"][0];
        ASSERT_TRUE(first_message.HasMember("role")) << "Message missing role field";
        ASSERT_TRUE(first_message["role"].IsString()) << "Role field is not a string";
        ASSERT_STREQ(first_message["role"].GetString(), "system");
        ASSERT_STREQ(first_message["content"].GetString(), FunctionLLMSummarize::system_prompt);

        const auto& last_message = doc["messages"][doc["messages"].Size() - 1];
        ASSERT_TRUE(last_message.HasMember("content")) << "Message missing content field";
        ASSERT_TRUE(last_message["content"].IsString()) << "Content field is not a string";
        ASSERT_STREQ(last_message["content"].GetString(), "hello world");
    } else if (doc.HasMember("prompt")) {
        ASSERT_TRUE(doc["prompt"].IsString()) << "Prompt field is not a string";
        ASSERT_STREQ(doc["prompt"].GetString(), "hello world");
    }
}

TEST(LLM_ADAPTER_TEST, local_adapter_parse_response) {
    LocalAdapter adapter;
    // OpenAI type
    std::string resp = R"({"choices":[{"message":{"content":"hi"}}]})";
    std::vector<std::string> results;
    Status st = adapter.parse_response(resp, results);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(results.size(), 1);
    ASSERT_EQ(results[0], "hi");

    // Simple text type
    resp = R"({"text":"simple result"})";
    results.clear();
    st = adapter.parse_response(resp, results);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(results.size(), 1);
    ASSERT_EQ(results[0], "simple result");

    resp = R"({"content":"simple result"})";
    results.clear();
    st = adapter.parse_response(resp, results);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(results.size(), 1);
    ASSERT_EQ(results[0], "simple result");

    // Ollama type
    resp = R"({"response":"ollama result"})";
    results.clear();
    st = adapter.parse_response(resp, results);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(results.size(), 1);
    ASSERT_EQ(results[0], "ollama result");
}

TEST(LLM_ADAPTER_TEST, openai_adapter_request) {
    OpenAIAdapter adapter;
    TLLMResource config;
    config.model_name = "gpt-3.5-turbo";
    config.temperature = 0.5;
    config.max_tokens = 64;
    config.api_key = "test_openai_key";
    adapter.init(config);

    // header
    MockHttpClient mock_client;
    Status auth_status = adapter.set_authentication(&mock_client);
    ASSERT_TRUE(auth_status.ok());

    EXPECT_STREQ(mock_client.get()->data, "Authorization: Bearer test_openai_key");
    EXPECT_STREQ(mock_client.get()->next->data, "Content-Type: application/json");

    std::vector<std::string> inputs = {"hi openai"};
    std::string request_body;
    Status st = adapter.build_request_payload(inputs, FunctionLLMSentiment::system_prompt,
                                              request_body);
    ASSERT_TRUE(st.ok());

    // body
    rapidjson::Document doc;
    doc.Parse(request_body.c_str());
    ASSERT_FALSE(doc.HasParseError()) << "JSON parse error";
    ASSERT_TRUE(doc.IsObject()) << "JSON is not an object";

    // model
    ASSERT_TRUE(doc.HasMember("model")) << "Missing model field";
    ASSERT_TRUE(doc["model"].IsString()) << "Model field is not a string";
    ASSERT_STREQ(doc["model"].GetString(), "gpt-3.5-turbo");

    // temperature
    ASSERT_TRUE(doc.HasMember("temperature")) << "Missing temperature field";
    ASSERT_TRUE(doc["temperature"].IsNumber()) << "Temperature field is not a number";
    ASSERT_DOUBLE_EQ(doc["temperature"].GetDouble(), 0.5);

    // max token
    ASSERT_TRUE(doc.HasMember("max_tokens")) << "Missing max_tokens field";
    ASSERT_TRUE(doc["max_tokens"].IsInt()) << "Max_tokens field is not an integer";
    ASSERT_EQ(doc["max_tokens"].GetInt(), 64);
    // msg
    ASSERT_TRUE(doc.HasMember("messages")) << "Missing messages field";
    ASSERT_TRUE(doc["messages"].IsArray()) << "Messages is not an array";
    ASSERT_GT(doc["messages"].Size(), 0) << "Messages array is empty";

    // system_prompt
    const auto& first_message = doc["messages"][0];
    ASSERT_TRUE(first_message.HasMember("role")) << "Message missing role field";
    ASSERT_TRUE(first_message["role"].IsString()) << "Role field is not a string";
    ASSERT_STREQ(first_message["role"].GetString(), "system");
    ASSERT_STREQ(first_message["content"].GetString(), FunctionLLMSentiment::system_prompt);

    // The content of the last message
    const auto& last_message = doc["messages"][doc["messages"].Size() - 1];
    ASSERT_TRUE(last_message.HasMember("content")) << "Message missing content field";
    ASSERT_TRUE(last_message["content"].IsString()) << "Content field is not a string";
    ASSERT_STREQ(last_message["content"].GetString(), "hi openai");
}

TEST(LLM_ADAPTER_TEST, openai_adapter_parse_response) {
    OpenAIAdapter adapter;
    std::string resp = R"({"choices":[{"message":{"content":"openai result"}}]})";
    std::vector<std::string> results;
    Status st = adapter.parse_response(resp, results);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(results.size(), 1);
    ASSERT_EQ(results[0], "openai result");
}

TEST(LLM_ADAPTER_TEST, gemini_adapter_request) {
    GeminiAdapter adapter;
    TLLMResource config;
    config.temperature = 0.2;
    config.max_tokens = 32;
    config.api_key = "test_gemini_key";
    adapter.init(config);

    // header test
    MockHttpClient mock_client;
    Status auth_status = adapter.set_authentication(&mock_client);
    ASSERT_TRUE(auth_status.ok());

    EXPECT_STREQ(mock_client.get()->data, "x-goog-api-key: test_gemini_key");
    EXPECT_STREQ(mock_client.get()->next->data, "Content-Type: application/json");

    std::vector<std::string> inputs = {"hello gemini"};
    std::string request_body;
    Status st =
            adapter.build_request_payload(inputs, FunctionLLMExtract::system_prompt, request_body);
    ASSERT_TRUE(st.ok());

    // body test
    rapidjson::Document doc;
    doc.Parse(request_body.c_str());
    ASSERT_FALSE(doc.HasParseError()) << "JSON parse error";
    ASSERT_TRUE(doc.IsObject()) << "JSON is not an object";

    ASSERT_TRUE(doc.HasMember("generationConfig"));
    const auto& gen_cfg = doc["generationConfig"];

    //temperature
    ASSERT_TRUE(gen_cfg.HasMember("temperature"));
    ASSERT_TRUE(gen_cfg["temperature"].IsNumber());
    ASSERT_DOUBLE_EQ(gen_cfg["temperature"].GetDouble(), 0.2);

    //max_token
    ASSERT_TRUE(gen_cfg.HasMember("maxOutputTokens")) << "Missing maxOutputTokens field";
    ASSERT_TRUE(gen_cfg["maxOutputTokens"].IsInt());
    ASSERT_EQ(gen_cfg["maxOutputTokens"].GetInt(), 32);

    // system_prompt
    ASSERT_TRUE(doc.HasMember("systemInstruction")) << "Missing system field";
    ASSERT_TRUE(doc["systemInstruction"].IsObject()) << request_body;
    ASSERT_TRUE(doc["systemInstruction"].HasMember("parts")) << request_body;
    ASSERT_TRUE(doc["systemInstruction"]["parts"].IsArray()) << request_body;
    ASSERT_GT(doc["systemInstruction"]["parts"].Size(), 0) << request_body;

    const auto& content_sys = doc["systemInstruction"]["parts"][0];
    ASSERT_TRUE(content_sys.HasMember("text")) << "parts missing text field";
    ASSERT_TRUE(content_sys["text"].IsString()) << "Text field is not a string";
    ASSERT_STREQ(content_sys["text"].GetString(), FunctionLLMExtract::system_prompt);

    // content structure
    ASSERT_TRUE(doc.HasMember("contents")) << "Missing contents field";
    ASSERT_TRUE(doc["contents"].IsArray()) << "Contents is not an array";
    ASSERT_GT(doc["contents"].Size(), 0) << "Contents array is empty";

    // content
    const auto& content = doc["contents"][0];
    ASSERT_TRUE(content.HasMember("parts")) << "Content missing parts field";
    ASSERT_TRUE(content["parts"].IsArray()) << "Parts is not an array";
    ASSERT_GT(content["parts"].Size(), 0) << "Parts array is empty";

    const auto& part = content["parts"][0];
    ASSERT_TRUE(part.HasMember("text")) << "Part missing text field";
    ASSERT_TRUE(part["text"].IsString()) << "Text field is not a string";
    ASSERT_STREQ(part["text"].GetString(), "hello gemini");
}

TEST(LLM_ADAPTER_TEST, gemini_adapter_parse_response) {
    GeminiAdapter adapter;
    std::string resp = R"({"candidates":[{"content":{"parts":[{"text":"gemini result"}]}}]})";
    std::vector<std::string> results;
    Status st = adapter.parse_response(resp, results);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(results.size(), 1);
    ASSERT_EQ(results[0], "gemini result");
}

TEST(LLM_ADAPTER_TEST, anthropic_adapter_request) {
    AnthropicAdapter adapter;
    TLLMResource config;
    config.model_name = "claude-3";
    config.temperature = 1.0;
    config.max_tokens = 256;
    config.api_key = "test_anthropic_key";
    config.anthropic_version = "2023-06-01";
    adapter.init(config);

    // header
    MockHttpClient mock_client;
    Status auth_status = adapter.set_authentication(&mock_client);
    ASSERT_TRUE(auth_status.ok());

    EXPECT_STREQ(mock_client.get()->data, "x-api-key: test_anthropic_key");
    EXPECT_STREQ(mock_client.get()->next->data, "anthropic-version: 2023-06-01");
    EXPECT_STREQ(mock_client.get()->next->next->data, "Content-Type: application/json");

    std::vector<std::string> inputs = {"hi anthropic"};
    std::string request_body;
    Status st =
            adapter.build_request_payload(inputs, FunctionLLMClassify::system_prompt, request_body);
    ASSERT_TRUE(st.ok());

    // body
    rapidjson::Document doc;
    doc.Parse(request_body.c_str());
    ASSERT_FALSE(doc.HasParseError()) << "JSON parse error";
    ASSERT_TRUE(doc.IsObject()) << "JSON is not an object";

    // model
    ASSERT_TRUE(doc.HasMember("model")) << "Missing model field";
    ASSERT_TRUE(doc["model"].IsString()) << "Model field is not a string";
    ASSERT_STREQ(doc["model"].GetString(), "claude-3");

    // temperature
    ASSERT_TRUE(doc.HasMember("temperature")) << "Missing temperature field";
    ASSERT_TRUE(doc["temperature"].IsNumber()) << "Temperature field is not a number";
    ASSERT_DOUBLE_EQ(doc["temperature"].GetDouble(), 1.0);

    // max token
    ASSERT_TRUE(doc.HasMember("max_tokens")) << "Missing max_tokens field";
    ASSERT_TRUE(doc["max_tokens"].IsInt()) << "Max_tokens field is not an integer";
    ASSERT_EQ(doc["max_tokens"].GetInt(), 256);

    // system_prompt
    ASSERT_TRUE(doc.HasMember("system")) << "Missing system field";
    ASSERT_TRUE(doc["system"].IsString()) << "System field is not a string";
    ASSERT_STREQ(doc["system"].GetString(), FunctionLLMClassify::system_prompt);

    // message Format
    ASSERT_TRUE(doc.HasMember("messages")) << "Missing messages field";
    ASSERT_TRUE(doc["messages"].IsArray()) << "Messages is not an array";
    ASSERT_GT(doc["messages"].Size(), 0) << "Messages array is empty";

    const auto& message = doc["messages"][0];
    ASSERT_TRUE(message.HasMember("role")) << "Message missing role field";
    ASSERT_TRUE(message.HasMember("content")) << "Message missing content field";

    // content of the last message
    if (message["content"].IsArray()) {
        ASSERT_GT(message["content"].Size(), 0) << "Content array is empty";
        const auto& content = message["content"][0];
        ASSERT_TRUE(content.HasMember("type")) << "Content missing type field";
        ASSERT_TRUE(content.HasMember("text")) << "Content missing text field";
        ASSERT_STREQ(content["text"].GetString(), "hi anthropic");
    } else if (message["content"].IsString()) {
        ASSERT_STREQ(message["content"].GetString(), "hi anthropic");
    }
}

TEST(LLM_ADAPTER_TEST, anthropic_adapter_parse_response) {
    AnthropicAdapter adapter;
    std::string resp = R"({"content":[{"type":"text","text":"anthropic result"}]})";
    std::vector<std::string> results;
    Status st = adapter.parse_response(resp, results);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(results.size(), 1);
    ASSERT_EQ(results[0], "anthropic result");
}

TEST(LLM_ADAPTER_TEST, unsupported_provider_type) {
    TLLMResource config;
    config.provider_type = "not_exist";
    auto adapter = doris::vectorized::LLMAdapterFactory::create_adapter(config.provider_type);
    ASSERT_EQ(adapter, nullptr);
}

TEST(LLM_ADAPTER_TEST, adapter_factory_all_types) {
    std::vector<std::string> types = {"LOCAL",     "OPENAI", "MOONSHOT", "DEEPSEEK",
                                      "MINIMAX",   "ZHIPU",  "QWEN",     "BAICHUAN",
                                      "ANTHROPIC", "GEMINI", "VOYAGEAI", "MOCK"};
    for (const auto& type : types) {
        auto adapter = doris::vectorized::LLMAdapterFactory::create_adapter(type);
        ASSERT_TRUE(adapter != nullptr) << "Adapter not found for type: " << type;
    }
}

TEST(LLM_ADAPTER_TEST, local_adapter_parse_response_parse_error) {
    LocalAdapter adapter;
    std::string resp = "not a json";
    std::vector<std::string> results;
    Status st = adapter.parse_response(resp, results);
    ASSERT_FALSE(st.ok());
}

TEST(LLM_ADAPTER_TEST, parse_response_wrong_type) {
    LocalAdapter adapter;
    // response field is not a string
    std::string resp = R"({"response":123})";
    std::vector<std::string> results;
    Status st = adapter.parse_response(resp, results);
    ASSERT_FALSE(st.ok());
}

TEST(LLM_ADAPTER_TEST, openai_adapter_parse_response_choice_format_error) {
    OpenAIAdapter adapter;
    // message field missing
    std::string resp = R"({"choices":[{}]})";
    std::vector<std::string> results;
    Status st = adapter.parse_response(resp, results);
    ASSERT_FALSE(st.ok());

    // content field is not a string
    resp = R"({"choices":[{"message":{"content":123}}]})";
    results.clear();
    st = adapter.parse_response(resp, results);
    ASSERT_FALSE(st.ok());
}

TEST(LLM_ADAPTER_TEST, openai_adapter_parse_response_parse_error) {
    OpenAIAdapter adapter;
    std::string resp = "not a json";
    std::vector<std::string> results;
    Status st = adapter.parse_response(resp, results);
    ASSERT_FALSE(st.ok());
}

TEST(LLM_ADAPTER_TEST, openai_adapter_parse_response_choices_not_array) {
    OpenAIAdapter adapter;
    std::string resp = R"({"choices":123})";
    std::vector<std::string> results;
    Status st = adapter.parse_response(resp, results);
    ASSERT_FALSE(st.ok());
}

TEST(LLM_ADAPTER_TEST, gemini_adapter_parse_response_parse_error) {
    GeminiAdapter adapter;
    std::string resp = "not a json";
    std::vector<std::string> results;
    Status st = adapter.parse_response(resp, results);
    ASSERT_FALSE(st.ok());
}

TEST(LLM_ADAPTER_TEST, gemini_parse_response_missing_candidates) {
    GeminiAdapter adapter;
    std::string resp = R"({"foo":"bar"})";
    std::vector<std::string> results;
    Status st = adapter.parse_response(resp, results);
    ASSERT_FALSE(st.ok());
}

TEST(LLM_ADAPTER_TEST, anthropic_adapter_parse_response_parse_error) {
    AnthropicAdapter adapter;
    std::string resp = "not a json";
    std::vector<std::string> results;
    Status st = adapter.parse_response(resp, results);
    ASSERT_FALSE(st.ok());
}

TEST(LLM_ADAPTER_TEST, anthropic_adapter_parse_response_content_not_array) {
    AnthropicAdapter adapter;
    std::string resp = R"({"content":123})";
    std::vector<std::string> results;
    Status st = adapter.parse_response(resp, results);
    ASSERT_FALSE(st.ok());
}

TEST(LLM_ADAPTER_TEST, voyage_adapter_chat_test) {
    VoyageAIAdapter adapter;
    TLLMResource config;
    config.api_key = "test_voyage_key";

    adapter.init(config);
    MockHttpClient mock_client;
    Status auth_status = adapter.set_authentication(&mock_client);
    ASSERT_TRUE(auth_status.ok());
    EXPECT_STREQ(mock_client.get()->data, "Authorization: Bearer test_voyage_key");
    EXPECT_STREQ(mock_client.get()->next->data, "Content-Type: application/json");

    std::vector<std::string> inputs = {"test_inputs"};
    std::string request_body;
    Status st = adapter.build_request_payload(inputs, "test_system_prompt", request_body);
    ASSERT_FALSE(st.ok());
    ASSERT_STREQ(st.to_string().c_str(),
                 "[NOT_IMPLEMENTED_ERROR]VoyageAI only support embedding function");

    std::string response_body = "test_response_body";
    std::vector<std::string> result;
    st = adapter.parse_response(response_body, result);
    ASSERT_FALSE(st.ok());
    ASSERT_STREQ(st.to_string().c_str(),
                 "[NOT_IMPLEMENTED_ERROR]VoyageAI only support embedding function");
}

} // namespace doris::vectorized
