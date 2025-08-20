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

#include "vec/functions/llm/embed.h"

#include <curl/curl.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "testutil/column_helper.h"
#include "testutil/mock/mock_runtime_state.h"
#include "vec/functions/llm/llm_adapter.h"

namespace doris::vectorized {

class MockHttpClient : public HttpClient {
public:
    curl_slist* get() { return this->_header_list; }

private:
    std::unordered_map<std::string, std::string> _headers;
    std::string _content_type;
};

TEST(EMBED_TEST, embed_function_build_test) {
    FunctionEmbed function;

    std::vector<std::string> resources = {"resource_name"};
    std::vector<std::string> texts = {"this is a test prompt"};

    auto col_resource = ColumnHelper::create_column<DataTypeString>(resources);
    auto col_text = ColumnHelper::create_column<DataTypeString>(texts);

    Block block;
    block.insert({std::move(col_resource), std::make_shared<DataTypeString>(), "resource"});
    block.insert({std::move(col_text), std::make_shared<DataTypeString>(), "text"});

    ColumnNumbers arguments = {0, 1};
    std::string prompt;
    Status status = function.build_prompt(block, arguments, 0, prompt);

    ASSERT_TRUE(status.ok());
    ASSERT_EQ(prompt, "this is a test prompt");
}

TEST(EMBED_TEST, embed_function_test) {
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

    auto sentiment_func = FunctionEmbed::create();
    Status exec_status =
            sentiment_func->execute_impl(ctx.get(), block, arguments, result_idx, texts.size());

    ASSERT_FALSE(exec_status.ok());
}

TEST(EMBED_TEST, local_adapter_embedding_request) {
    LocalAdapter adapter;
    TLLMResource config;
    config.model_name = "local-embedding-model";
    config.dimensions = 1536;
    adapter.init(config);

    // header test
    MockHttpClient mock_client;
    Status auth_status = adapter.set_authentication(&mock_client);
    ASSERT_TRUE(auth_status.ok());

    curl_slist* headers = mock_client.get();
    ASSERT_TRUE(headers != nullptr);
    EXPECT_STREQ(headers->data, "Content-Type: application/json");

    std::vector<std::string> inputs = {"test sentence for embedding"};
    std::string request_body;
    Status st = adapter.build_embedding_request(inputs, request_body);
    ASSERT_TRUE(st.ok());

    // body test
    rapidjson::Document doc;
    doc.Parse(request_body.c_str());
    ASSERT_FALSE(doc.HasParseError()) << "JSON parse error";
    ASSERT_TRUE(doc.IsObject()) << "JSON is not an object";

    // model name
    ASSERT_TRUE(doc.HasMember("model")) << "Missing model field";
    ASSERT_TRUE(doc["model"].IsString()) << "Model field is not a string";
    ASSERT_STREQ(doc["model"].GetString(), "local-embedding-model");

    // input
    ASSERT_TRUE(doc.HasMember("input")) << "Missing input field";
    ASSERT_TRUE(doc["input"].IsString() || doc["input"].IsArray())
            << "Input field is not a string or array";
    if (doc["input"].IsString()) {
        ASSERT_STREQ(doc["input"].GetString(), "test sentence for embedding");
    } else {
        ASSERT_EQ(doc["input"].Size(), 1);
        ASSERT_STREQ(doc["input"][0].GetString(), "test sentence for embedding");
    }
}

TEST(EMBED_TEST, local_adapter_parse_embedding_response) {
    LocalAdapter adapter;

    // Test various formats for compatibility
    // Format 1: Direct embedding array (Ollama format)
    std::string resp1 = R"({
        "embedding": [0.1, 0.2, 0.3, 0.4, 0.5]
    })";
    std::vector<std::vector<float>> results;
    Status st = adapter.parse_embedding_response(resp1, results);
    ASSERT_TRUE(st.ok()) << "Format 1 failed: " << st.to_string();
    ASSERT_EQ(results.size(), 1);
    ASSERT_EQ(results[0].size(), 5);
    ASSERT_FLOAT_EQ(results[0][0], 0.1F);
    ASSERT_FLOAT_EQ(results[0][1], 0.2F);
    ASSERT_FLOAT_EQ(results[0][4], 0.5F);

    // Format 2: Embeddings array (VoyageAI format)
    std::string resp2 = R"({
        "embedding": [0.1, 0.2, 0.3]
    })";
    results.clear();
    st = adapter.parse_embedding_response(resp2, results);
    ASSERT_TRUE(st.ok()) << "Format 2 failed: " << st.to_string();
    ASSERT_EQ(results.size(), 1);
    ASSERT_EQ(results[0].size(), 3);
    ASSERT_FLOAT_EQ(results[0][0], 0.1F);
    ASSERT_FLOAT_EQ(results[0][2], 0.3F);
}

TEST(EMBED_TEST, openai_adapter_embedding_request) {
    OpenAIAdapter adapter;
    TLLMResource config;
    config.model_name = "text-embedding-ada-002";
    config.api_key = "test_openai_key";
    config.dimensions = 1536;
    adapter.init(config);

    // header test
    MockHttpClient mock_client;
    Status auth_status = adapter.set_authentication(&mock_client);
    ASSERT_TRUE(auth_status.ok());

    EXPECT_STREQ(mock_client.get()->data, "Authorization: Bearer test_openai_key");
    EXPECT_STREQ(mock_client.get()->next->data, "Content-Type: application/json");

    std::vector<std::string> inputs = {"embed this text"};
    std::string request_body;
    Status st = adapter.build_embedding_request(inputs, request_body);
    ASSERT_TRUE(st.ok());

    // body test
    rapidjson::Document doc;
    doc.Parse(request_body.c_str());
    ASSERT_FALSE(doc.HasParseError()) << "JSON parse error";
    ASSERT_TRUE(doc.IsObject()) << "JSON is not an object";

    // model name
    ASSERT_TRUE(doc.HasMember("model")) << "Missing model field";
    ASSERT_TRUE(doc["model"].IsString()) << "Model field is not a string";
    ASSERT_STREQ(doc["model"].GetString(), "text-embedding-ada-002");

    // input
    ASSERT_TRUE(doc.HasMember("input")) << "Missing input field";
    ASSERT_TRUE(doc["input"].IsArray()) << "Input field is not an array";
    ASSERT_EQ(doc["input"].Size(), 1);
    ASSERT_STREQ(doc["input"][0].GetString(), "embed this text");

    // should not support dimensions param
    ASSERT_FALSE(doc.HasMember("dimensions"));

    config.model_name = "text-embedding-3";
    adapter.init(config);
    st = adapter.build_embedding_request(inputs, request_body);
    ASSERT_TRUE(st.ok());
    doc.Parse(request_body.c_str());
    ASSERT_FALSE(doc.HasParseError()) << "JSON parse error";
    ASSERT_TRUE(doc.IsObject()) << "JSON is not an object";
    ASSERT_TRUE(doc.HasMember("dimensions")) << request_body;
    ASSERT_TRUE(doc["dimensions"].IsInt()) << "Dimensions is not an integer";
    ASSERT_EQ(doc["dimensions"].GetInt(), 1536);
}

TEST(EMBED_TEST, openai_adapter_parse_embedding_response) {
    OpenAIAdapter adapter;

    std::string resp = R"({
        "object": "list",
        "data": [
            {
                "object": "embedding",
                "embedding": [0.1, 0.2, 0.3],
                "index": 0
            },
            {
                "object": "embedding",
                "embedding": [0.4, 0.5],
                "index": 1
            }
        ],
        "model": "text-embedding-ada-002",
        "usage": {
            "prompt_tokens": 8,
            "total_tokens": 8
        }
    })";

    std::vector<std::vector<float>> results;
    Status st = adapter.parse_embedding_response(resp, results);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(results.size(), 2);
    ASSERT_EQ(results[0].size(), 3);
    ASSERT_EQ(results[1].size(), 2);
    ASSERT_FLOAT_EQ(results[0][0], 0.1F);
    ASSERT_FLOAT_EQ(results[0][1], 0.2F);
    ASSERT_FLOAT_EQ(results[0][2], 0.3F);
    ASSERT_FLOAT_EQ(results[1][0], 0.4F);
    ASSERT_FLOAT_EQ(results[1][1], 0.5F);
}

TEST(EMBED_TEST, gemini_adapter_embedding_request) {
    GeminiAdapter adapter;
    TLLMResource config;
    config.model_name = "embedding-001";
    config.api_key = "test_gemini_key";
    config.dimensions = 768;
    adapter.init(config);

    // header test
    MockHttpClient mock_client;
    Status auth_status = adapter.set_authentication(&mock_client);
    ASSERT_TRUE(auth_status.ok());

    EXPECT_STREQ(mock_client.get()->data, "x-goog-api-key: test_gemini_key");
    EXPECT_STREQ(mock_client.get()->next->data, "Content-Type: application/json");

    std::vector<std::string> inputs = {"embed with gemini"};
    std::string request_body;
    Status st = adapter.build_embedding_request(inputs, request_body);
    ASSERT_TRUE(st.ok());

    // body test
    rapidjson::Document doc;
    doc.Parse(request_body.c_str());
    ASSERT_FALSE(doc.HasParseError()) << "JSON parse error";
    ASSERT_TRUE(doc.IsObject()) << "JSON is not an object";

    ASSERT_TRUE(doc.HasMember("requests")) << "Missing requests field";
    ASSERT_TRUE(doc["requests"].IsArray());

    auto& request = doc["requests"][0];
    ASSERT_TRUE(request.HasMember("model")) << "Missing model field";
    ASSERT_STREQ(request["model"].GetString(), "models/embedding-001");
    ASSERT_TRUE(request.HasMember("content")) << "Missing content field";
    ASSERT_TRUE(request["content"].IsObject());
    ASSERT_TRUE(request["content"].HasMember("parts")) << "Missing parts field";
    ASSERT_TRUE(request["content"]["parts"].IsArray());
    ASSERT_TRUE(request["content"]["parts"][0].HasMember("text")) << "Missing text field";
    ASSERT_STREQ(request["content"]["parts"][0]["text"].GetString(), "embed with gemini");

    // should not have dimension param;
    ASSERT_FALSE(doc.HasMember("outputDimensionality"));

    config.model_name = "gemini-embedding-001";
    adapter.init(config);
    st = adapter.build_embedding_request(inputs, request_body);
    ASSERT_TRUE(st.ok());
    doc.Parse(request_body.c_str());
    ASSERT_FALSE(doc.HasParseError()) << "JSON parse error";
    ASSERT_TRUE(doc.IsObject()) << "JSON is not an object";
    ASSERT_TRUE(doc.HasMember("requests")) << "Missing requests field";
    ASSERT_TRUE(doc["requests"].IsArray());
    request = doc["requests"][0];
    ASSERT_TRUE(request.HasMember("model")) << "Missing model field";
    ASSERT_STREQ(request["model"].GetString(), "models/gemini-embedding-001");
    ASSERT_TRUE(request.HasMember("outputDimensionality")) << request_body;
    ASSERT_EQ(request["outputDimensionality"].GetInt(), 768);
}

TEST(EMBED_TEST, gemini_adapter_parse_embedding_response) {
    GeminiAdapter adapter;

    std::string resp = R"({
        "embeddings":[
        {
            "value":[
                0.1,
                0.2,
                0.3
            ]
        },
        {
            "value":[
                0.4,
                0.5
            ]
        }]
    })";

    std::vector<std::vector<float>> results;
    Status st = adapter.parse_embedding_response(resp, results);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(results.size(), 2);
    ASSERT_EQ(results[0].size(), 3);
    ASSERT_EQ(results[1].size(), 2);
    ASSERT_FLOAT_EQ(results[0][0], 0.1F);
    ASSERT_FLOAT_EQ(results[0][1], 0.2F);
    ASSERT_FLOAT_EQ(results[0][2], 0.3F);
    ASSERT_FLOAT_EQ(results[1][0], 0.4F);
    ASSERT_FLOAT_EQ(results[1][1], 0.5F);
}

TEST(EMBED_TEST, voyageai_adapter_embedding_request) {
    VoyageAIAdapter adapter;
    TLLMResource config;
    config.model_name = "voyage-multimodal-3";
    config.api_key = "test_voyage_key";
    config.dimensions = 1024;
    adapter.init(config);

    // header test
    MockHttpClient mock_client;
    Status auth_status = adapter.set_authentication(&mock_client);
    ASSERT_TRUE(auth_status.ok());

    EXPECT_STREQ(mock_client.get()->data, "Authorization: Bearer test_voyage_key");
    EXPECT_STREQ(mock_client.get()->next->data, "Content-Type: application/json");

    std::vector<std::string> inputs = {"embed with voyage"};
    std::string request_body;
    Status st = adapter.build_embedding_request(inputs, request_body);
    ASSERT_TRUE(st.ok());

    // body test
    rapidjson::Document doc;
    doc.Parse(request_body.c_str());
    ASSERT_FALSE(doc.HasParseError()) << "JSON parse error";
    ASSERT_TRUE(doc.IsObject()) << "JSON is not an object";

    // model name
    ASSERT_TRUE(doc.HasMember("model")) << "Missing model field";
    ASSERT_TRUE(doc["model"].IsString()) << "Model field is not a string";
    ASSERT_STREQ(doc["model"].GetString(), "voyage-multimodal-3");

    // input
    ASSERT_TRUE(doc.HasMember("input")) << "Missing input field";
    ASSERT_TRUE(doc["input"].IsArray()) << "Input field is not an array";
    ASSERT_EQ(doc["input"].Size(), 1);
    ASSERT_STREQ(doc["input"][0].GetString(), "embed with voyage");

    // dimension parameter
    ASSERT_FALSE(doc.HasMember("output_dimension"));

    config.model_name = "voyage-3.5";
    adapter.init(config);
    st = adapter.build_embedding_request(inputs, request_body);
    ASSERT_TRUE(st.ok());
    doc.Parse(request_body.c_str());
    ASSERT_FALSE(doc.HasParseError()) << "JSON parse error";
    ASSERT_TRUE(doc.IsObject()) << "JSON is not an object";
    ASSERT_TRUE(doc.HasMember("model"));
    ASSERT_STREQ(doc["model"].GetString(), "voyage-3.5") << request_body;
    ASSERT_TRUE(doc.HasMember("output_dimension")) << request_body;
}

TEST(EMBED_TEST, voyageai_adapter_parse_embedding_response) {
    VoyageAIAdapter adapter;

    std::string resp = R"({
        "object": "list",
        "data": [
            {
            "embedding": [0.1, 0.2, 0.3],
            "index": 0
            },
            {
            "embedding": [0.4, 0.5],
            "index": 1
            }
        ],
        "model": "voyage-3.5",
        "usage": {
            "total_tokens": 10
        }
    })";

    std::vector<std::vector<float>> results;
    Status st = adapter.parse_embedding_response(resp, results);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(results.size(), 2);
    ASSERT_EQ(results[0].size(), 3);
    ASSERT_EQ(results[1].size(), 2);
    ASSERT_FLOAT_EQ(results[0][0], 0.1F);
    ASSERT_FLOAT_EQ(results[0][1], 0.2F);
    ASSERT_FLOAT_EQ(results[0][2], 0.3F);
    ASSERT_FLOAT_EQ(results[1][0], 0.4F);
    ASSERT_FLOAT_EQ(results[1][1], 0.5F);
}

TEST(EMBED_TEST, deepseek_adapter_embedding_request) {
    DeepSeekAdapter adapter;
    TLLMResource config;
    config.model_name = "deepseek-embedding";
    config.api_key = "test_deepseek_key";
    adapter.init(config);

    // header test
    MockHttpClient mock_client;
    Status auth_status = adapter.set_authentication(&mock_client);
    ASSERT_TRUE(auth_status.ok());

    curl_slist* headers = mock_client.get();
    ASSERT_TRUE(headers != nullptr);
    EXPECT_STREQ(headers->data, "Authorization: Bearer test_deepseek_key");
    EXPECT_STREQ(headers->next->data, "Content-Type: application/json");

    std::vector<std::string> inputs = {"embed with deepseek"};
    std::string request_body;
    Status st = adapter.build_embedding_request(inputs, request_body);
    ASSERT_FALSE(st.ok());
}

TEST(EMBED_TEST, deepseek_adapter_parse_embedding_response) {
    DeepSeekAdapter adapter;

    std::string resp = R"({
        "data": [
            {
                "embedding": [0.1, 0.2, 0.3, 0.4, 0.5],
                "index": 0,
                "object": "embedding"
            }
        ],
        "model": "deepseek-embedding",
        "object": "list",
        "usage": {
            "prompt_tokens": 4,
            "total_tokens": 4
        }
    })";

    std::vector<std::vector<float>> results;
    Status st = adapter.parse_embedding_response(resp, results);
    ASSERT_FALSE(st.ok());
}

TEST(EMBED_TEST, moonshot_adapter_embedding_request) {
    MoonShotAdapter adapter;
    TLLMResource config;
    config.model_name = "moonshot-embedding";
    config.api_key = "test_moonshot_key";
    adapter.init(config);

    // header test
    MockHttpClient mock_client;
    Status auth_status = adapter.set_authentication(&mock_client);
    ASSERT_TRUE(auth_status.ok());

    curl_slist* headers = mock_client.get();
    ASSERT_TRUE(headers != nullptr);
    EXPECT_STREQ(headers->data, "Authorization: Bearer test_moonshot_key");
    EXPECT_STREQ(headers->next->data, "Content-Type: application/json");

    std::vector<std::string> inputs = {"embed with moonshot"};
    std::string request_body;
    Status st = adapter.build_embedding_request(inputs, request_body);
    ASSERT_FALSE(st.ok());
}

TEST(EMBED_TEST, moonshot_adapter_parse_embedding_response) {
    MoonShotAdapter adapter;

    std::string resp = R"({
        "id": "embedding-123",
        "object": "list",
        "data": [
            {
                "embedding": [0.1, 0.2, 0.3, 0.4, 0.5],
                "index": 0,
                "object": "embedding"
            }
        ],
        "model": "moonshot-embedding",
        "usage": {
            "prompt_tokens": 3,
            "total_tokens": 3
        }
    })";

    std::vector<std::vector<float>> results;
    Status st = adapter.parse_embedding_response(resp, results);
    ASSERT_FALSE(st.ok());
}

TEST(EMBED_TEST, minimax_adapter_embedding_request) {
    MinimaxAdapter adapter;
    TLLMResource config;
    config.model_name = "minimax-embedding";
    config.api_key = "test_minimax_key";
    adapter.init(config);

    // header test
    MockHttpClient mock_client;
    Status auth_status = adapter.set_authentication(&mock_client);
    ASSERT_TRUE(auth_status.ok());

    curl_slist* headers = mock_client.get();
    ASSERT_TRUE(headers != nullptr);
    EXPECT_STREQ(headers->data, "Authorization: Bearer test_minimax_key");
    EXPECT_STREQ(headers->next->data, "Content-Type: application/json");

    std::vector<std::string> inputs = {"embed with minimax"};
    std::string request_body;
    Status st = adapter.build_embedding_request(inputs, request_body);
    ASSERT_TRUE(st.ok());

    // body test
    rapidjson::Document doc;
    doc.Parse(request_body.c_str());
    ASSERT_FALSE(doc.HasParseError()) << "JSON parse error";
    ASSERT_TRUE(doc.IsObject()) << "JSON is not an object";

    // model name
    ASSERT_TRUE(doc.HasMember("model")) << "Missing model field";
    ASSERT_TRUE(doc["model"].IsString()) << "Model field is not a string";
    ASSERT_STREQ(doc["model"].GetString(), "minimax-embedding");

    // type
    ASSERT_TRUE(doc.HasMember("type")) << "Missing type field";
    ASSERT_TRUE(doc["type"].IsString());
    ASSERT_STREQ(doc["type"].GetString(), "db");

    // input
    ASSERT_TRUE(doc.HasMember("texts")) << "Missing texts field";
    ASSERT_TRUE(doc["texts"].IsArray()) << "Texts field is not an array";
    ASSERT_EQ(doc["texts"].Size(), 1);
    ASSERT_STREQ(doc["texts"][0].GetString(), "embed with minimax");
}

} // namespace doris::vectorized