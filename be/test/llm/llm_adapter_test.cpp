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

#include "vec/functiqons/llm/llm_adapter.h"

#include <gtest/gtest.h>

#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include "http/http_client.h"

namespace doris::vectorized {

// Mock HTTP客户端用于测试认证设置
class MockHttpClient : public HttpClient {
public:
    std::unordered_map<std::string, std::string> headers;
    std::string content_type;
    std::string initialized_url;

    Status init(const std::string& url) override {
        initialized_url = url;
        return Status::OK();
    }

    void set_header(const std::string& key, const std::string& value) override {
        headers[key] = value;
    }

    void set_content_type(const std::string& type) override {
        content_type = type;
    }
};

// 辅助函数，将请求体解析为JSON对象
void parse_json(const std::string& json_str, rapidjson::Document* doc) {
    doc->Parse(json_str.c_str());
    ASSERT_FALSE(doc->HasParseError()) << "JSON解析错误: " << json_str;
    ASSERT_TRUE(doc->IsObject()) << "解析结果不是JSON对象";
}

class LLMAdapterTest : public testing::Test {
protected:
    void SetUp() override {
        // 设置通用配置
        _config.api_key = "test_api_key";
        _config.endpoint_url = "https://api.example.com/v1/chat";
        _config.model_name = "test_model";
        _config.max_retries = 3;
        _config.retry_delay_ms = 1000;
        _config.timeout_ms = 30000;
        _config.max_batch_size = 10;

        // 设置测试输入
        _inputs = {"Hello, how are you?", "What is Doris?"};
    }

    // 测试适配器的认证设置
    void test_authentication(LLMAdapter* adapter, 
                            std::function<void(MockHttpClient*)> validator) {
        MockHttpClient client;
        adapter->init(_config);
        
        Status status = adapter->set_authentication(&client);
        ASSERT_TRUE(status.ok()) << "认证设置失败: " << status.to_string();
        
        validator(&client);
    }

    // 测试适配器的请求构建
    void test_request_payload(LLMAdapter* adapter,
                            std::function<void(const rapidjson::Document&)> validator) {
        adapter->init(_config);
        
        std::string request_body;
        Status status = adapter->build_request_payload(_inputs, request_body);
        ASSERT_TRUE(status.ok()) << "请求构建失败: " << status.to_string();
        
        rapidjson::Document doc;
        parse_json(request_body, &doc);
        
        validator(doc);
    }

    // 测试适配器的响应解析
    void test_response_parsing(LLMAdapter* adapter, 
                             const std::string& response_body,
                             const std::vector<std::string>& expected_results) {
        adapter->init(_config);
        
        std::vector<std::string> results;
        Status status = adapter->parse_response(response_body, results);
        ASSERT_TRUE(status.ok()) << "响应解析失败: " << status.to_string();
        
        ASSERT_EQ(results.size(), expected_results.size()) 
            << "结果数量不匹配";
        
        for (size_t i = 0; i < results.size(); i++) {
            EXPECT_EQ(results[i], expected_results[i]) 
                << "结果 " << i << " 不匹配";
        }
    }

    LLMConfig _config;
    std::vector<std::string> _inputs;
};

// 本地适配器测试
TEST_F(LLMAdapterTest, LocalAdapterTest) {
    LocalAdapter adapter;
    
    // 测试认证
    test_authentication(&adapter, [](MockHttpClient* client) {
        EXPECT_EQ(client->content_type, "application/json");
        EXPECT_TRUE(client->headers.empty()) << "本地适配器不应设置认证头";
    });
    
    // 测试请求构建
    test_request_payload(&adapter, [this](const rapidjson::Document& doc) {
        EXPECT_TRUE(doc.HasMember("model"));
        EXPECT_EQ(doc["model"].GetString(), _config.model_name);
        
        ASSERT_TRUE(doc.HasMember("messages"));
        ASSERT_TRUE(doc["messages"].IsArray());
        ASSERT_EQ(doc["messages"].Size(), _inputs.size());
        
        for (size_t i = 0; i < _inputs.size(); i++) {
            EXPECT_EQ(std::string(doc["messages"][i]["role"].GetString()), "user");
            EXPECT_EQ(std::string(doc["messages"][i]["content"].GetString()), _inputs[i]);
        }
    });
    
    // 测试响应解析 - OpenAI格式
    std::string openai_response = R"({
        "choices": [
            {"message": {"content": "I'm doing well, thank you!"}}
        ]
    })";
    test_response_parsing(&adapter, openai_response, {"I'm doing well, thank you!"});
    
    // 测试响应解析 - 简单文本格式
    std::string simple_response = R"({"text": "Apache Doris is a database."})";
    test_response_parsing(&adapter, simple_response, {"Apache Doris is a database."});
    
    // 测试响应解析 - Ollama格式
    std::string ollama_response = R"({"response": "This is an Ollama response."})";
    test_response_parsing(&adapter, ollama_response, {"This is an Ollama response."});
}

// OpenAI适配器测试
TEST_F(LLMAdapterTest, OpenAIAdapterTest) {
    OpenAIAdapter adapter;
    
    // 测试认证
    test_authentication(&adapter, [this](MockHttpClient* client) {
        EXPECT_EQ(client->content_type, "application/json");
        ASSERT_TRUE(client->headers.count(HttpHeaders::AUTHORIZATION));
        EXPECT_EQ(client->headers[HttpHeaders::AUTHORIZATION], "Bearer " + _config.api_key);
    });
    
    // 测试请求构建
    test_request_payload(&adapter, [this](const rapidjson::Document& doc) {
        EXPECT_TRUE(doc.HasMember("model"));
        EXPECT_EQ(doc["model"].GetString(), _config.model_name);
        
        ASSERT_TRUE(doc.HasMember("messages"));
        ASSERT_TRUE(doc["messages"].IsArray());
        ASSERT_EQ(doc["messages"].Size(), _inputs.size());
        
        for (size_t i = 0; i < _inputs.size(); i++) {
            EXPECT_EQ(std::string(doc["messages"][i]["role"].GetString()), "user");
            EXPECT_EQ(std::string(doc["messages"][i]["content"].GetString()), _inputs[i]);
        }
    });
    
    // 测试响应解析
    std::string response = R"({
        "choices": [
            {"message": {"content": "OpenAI response"}}
        ]
    })";
    test_response_parsing(&adapter, response, {"OpenAI response"});
}

// Gemini适配器测试
TEST_F(LLMAdapterTest, GeminiAdapterTest) {
    GeminiAdapter adapter;
    
    // 测试认证
    test_authentication(&adapter, [this](MockHttpClient* client) {
        EXPECT_EQ(client->content_type, "application/json");
        EXPECT_TRUE(client->initialized_url.find("key=" + _config.api_key) != std::string::npos);
    });
    
    // 测试请求构建
    test_request_payload(&adapter, [this](const rapidjson::Document& doc) {
        ASSERT_TRUE(doc.HasMember("contents"));
        ASSERT_TRUE(doc["contents"].IsArray());
        ASSERT_EQ(doc["contents"].Size(), _inputs.size());
        
        for (size_t i = 0; i < _inputs.size(); i++) {
            ASSERT_TRUE(doc["contents"][i].HasMember("parts"));
            ASSERT_TRUE(doc["contents"][i]["parts"].IsArray());
            ASSERT_TRUE(doc["contents"][i]["parts"][0].HasMember("text"));
            EXPECT_EQ(std::string(doc["contents"][i]["parts"][0]["text"].GetString()), _inputs[i]);
        }
        
        ASSERT_TRUE(doc.HasMember("generationConfig"));
    });
    
    // 测试响应解析
    std::string response = R"({
        "candidates": [
            {
                "content": {
                    "parts": [
                        {"text": "Gemini response"}
                    ]
                }
            }
        ]
    })";
    test_response_parsing(&adapter, response, {"Gemini response"});
}

// Anthropic适配器测试
TEST_F(LLMAdapterTest, AnthropicAdapterTest) {
    AnthropicAdapter adapter;
    
    // 测试认证
    test_authentication(&adapter, [this](MockHttpClient* client) {
        ASSERT_TRUE(client->headers.count("x-api-key"));
        EXPECT_EQ(client->headers["x-api-key"], _config.api_key);
        ASSERT_TRUE(client->headers.count("anthropic-version"));
        EXPECT_EQ(client->headers["anthropic-version"], "2023-06-01");
    });
    
    // 测试请求构建
    test_request_payload(&adapter, [this](const rapidjson::Document& doc) {
        EXPECT_TRUE(doc.HasMember("model"));
        EXPECT_EQ(doc["model"].GetString(), _config.model_name);
        
        ASSERT_TRUE(doc.HasMember("messages"));
        ASSERT_TRUE(doc["messages"].IsArray());
        ASSERT_EQ(doc["messages"].Size(), _inputs.size());
        
        for (size_t i = 0; i < _inputs.size(); i++) {
            EXPECT_EQ(std::string(doc["messages"][i]["role"].GetString()), "user");
            ASSERT_TRUE(doc["messages"][i].HasMember("content"));
            ASSERT_TRUE(doc["messages"][i]["content"].IsArray());
            ASSERT_TRUE(doc["messages"][i]["content"][0].HasMember("type"));
            ASSERT_TRUE(doc["messages"][i]["content"][0].HasMember("text"));
            EXPECT_EQ(std::string(doc["messages"][i]["content"][0]["type"].GetString()), "text");
            EXPECT_EQ(std::string(doc["messages"][i]["content"][0]["text"].GetString()), _inputs[i]);
        }
        
        ASSERT_TRUE(doc.HasMember("max_tokens"));
    });
    
    // 测试响应解析
    std::string response = R"({
        "content": [
            {"type": "text", "text": "First part"},
            {"type": "text", "text": "Second part"}
        ]
    })";
    test_response_parsing(&adapter, response, {"First part\nSecond part"});
}

// LLMAdapterFactory测试
TEST_F(LLMAdapterTest, AdapterFactoryTest) {
    // 测试所有支持的适配器类型
    auto local_adapter = LLMAdapterFactory::create_adapter("local");
    ASSERT_TRUE(local_adapter != nullptr);
    EXPECT_EQ(local_adapter->get_type(), "local");
    
    auto openai_adapter = LLMAdapterFactory::create_adapter("openai");
    ASSERT_TRUE(openai_adapter != nullptr);
    EXPECT_EQ(openai_adapter->get_type(), "openai");
    
    auto gemini_adapter = LLMAdapterFactory::create_adapter("gemini");
    ASSERT_TRUE(gemini_adapter != nullptr);
    EXPECT_EQ(gemini_adapter->get_type(), "gemini");
    
    auto deepseek_adapter = LLMAdapterFactory::create_adapter("deepseek");
    ASSERT_TRUE(deepseek_adapter != nullptr);
    EXPECT_EQ(deepseek_adapter->get_type(), "deepseek");
    
    auto anthropic_adapter = LLMAdapterFactory::create_adapter("anthropic");
    ASSERT_TRUE(anthropic_adapter != nullptr);
    EXPECT_EQ(anthropic_adapter->get_type(), "anthropic");
    
    auto moonshot_adapter = LLMAdapterFactory::create_adapter("moonshot");
    ASSERT_TRUE(moonshot_adapter != nullptr);
    EXPECT_EQ(moonshot_adapter->get_type(), "moonshot");
    
    // 测试不支持的适配器类型
    auto invalid_adapter = LLMAdapterFactory::create_adapter("unsupported");
    EXPECT_TRUE(invalid_adapter == nullptr);
}

// 错误情况测试
TEST_F(LLMAdapterTest, ErrorHandlingTest) {
    OpenAIAdapter adapter;
    adapter.init(_config);
    
    // 测试无效JSON响应
    std::string invalid_json = "This is not a valid JSON";
    std::vector<std::string> results;
    Status status = adapter.parse_response(invalid_json, results);
    EXPECT_FALSE(status.ok()) << "应该识别出无效的JSON";
    
    // 测试格式错误的响应
    std::string missing_choices = R"({"not_choices": []})";
    status = adapter.parse_response(missing_choices, results);
    EXPECT_FALSE(status.ok()) << "应该识别出缺少必要字段的响应";
    
    // 测试格式错误的选择
    std::string invalid_choice = R"({
        "choices": [
            {"not_message": {"content": "test"}}
        ]
    })";
    status = adapter.parse_response(invalid_choice, results);
    EXPECT_FALSE(status.ok()) << "应该识别出格式错误的选择";
}

} // namespace doris::vectorized

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}