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

#pragma once

#include <gen_cpp/PaloInternalService_types.h>
#include <rapidjson/rapidjson.h>

#include <algorithm>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/logging.h"
#include "common/status.h"
#include "http/http_client.h"
#include "http/http_headers.h"
#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include "vec/common/string_buffer.hpp"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

struct AIResource {
    AIResource() = default;
    AIResource(const TAIResource& tai)
            : endpoint(tai.endpoint),
              provider_type(tai.provider_type),
              model_name(tai.model_name),
              api_key(tai.api_key),
              temperature(tai.temperature),
              max_tokens(tai.max_tokens),
              max_retries(tai.max_retries),
              retry_delay_second(tai.retry_delay_second),
              anthropic_version(tai.anthropic_version),
              dimensions(tai.dimensions) {}

    std::string endpoint;
    std::string provider_type;
    std::string model_name;
    std::string api_key;
    double temperature;
    int64_t max_tokens;
    int32_t max_retries;
    int32_t retry_delay_second;
    std::string anthropic_version;
    int32_t dimensions;

    void serialize(BufferWritable& buf) const {
        buf.write_binary(endpoint);
        buf.write_binary(provider_type);
        buf.write_binary(model_name);
        buf.write_binary(api_key);
        buf.write_binary(temperature);
        buf.write_binary(max_tokens);
        buf.write_binary(max_retries);
        buf.write_binary(retry_delay_second);
        buf.write_binary(anthropic_version);
        buf.write_binary(dimensions);
    }

    void deserialize(BufferReadable& buf) {
        buf.read_binary(endpoint);
        buf.read_binary(provider_type);
        buf.read_binary(model_name);
        buf.read_binary(api_key);
        buf.read_binary(temperature);
        buf.read_binary(max_tokens);
        buf.read_binary(max_retries);
        buf.read_binary(retry_delay_second);
        buf.read_binary(anthropic_version);
        buf.read_binary(dimensions);
    }
};

class AIAdapter {
public:
    virtual ~AIAdapter() = default;

    // Set authentication headers for the HTTP client
    virtual Status set_authentication(HttpClient* client) const = 0;

    virtual void init(const TAIResource& config) {
        LOG(INFO) << "[AI_CHECK]: Initializing AI adapter with provider type: "
                  << config.provider_type << ", model: " << config.model_name
                  << ", endpoint: " << config.endpoint;
        _config = config;
    }
    virtual void init(const AIResource& config) {
        LOG(INFO) << "[AI_CHECK]: Initializing AI adapter from AIResource with provider type: "
                  << config.provider_type << ", model: " << config.model_name
                  << ", endpoint: " << config.endpoint;
        _config.endpoint = config.endpoint;
        _config.provider_type = config.provider_type;
        _config.model_name = config.model_name;
        _config.api_key = config.api_key;
        _config.temperature = config.temperature;
        _config.max_tokens = config.max_tokens;
        _config.max_retries = config.max_retries;
        _config.retry_delay_second = config.retry_delay_second;
        _config.anthropic_version = config.anthropic_version;
    }

    // Build request payload based on input text strings
    virtual Status build_request_payload(const std::vector<std::string>& inputs,
                                         const char* const system_prompt,
                                         std::string& request_body) const {
        return Status::NotSupported("{} don't support text generation", _config.provider_type);
    }

    // Parse response from AI service and extract generated text results
    virtual Status parse_response(const std::string& response_body,
                                  std::vector<std::string>& results) const {
        return Status::NotSupported("{} don't support text generation", _config.provider_type);
    }

    virtual Status build_embedding_request(const std::vector<std::string>& inputs,
                                           std::string& request_body) const {
        return Status::NotSupported("{} does not support the Embed feature.",
                                    _config.provider_type);
    }

    virtual Status parse_embedding_response(const std::string& response_body,
                                            std::vector<std::vector<float>>& results) const {
        return Status::NotSupported("{} does not support the Embed feature.",
                                    _config.provider_type);
    }

protected:
    TAIResource _config;

    // return true if the model support dimension parameter
    virtual bool supports_dimension_param(const std::string& model_name) const { return false; }

    // Different providers may have different dimension parameter names.
    virtual std::string get_dimension_param_name() const { return "dimensions"; }

    virtual void add_dimension_params(rapidjson::Value& doc,
                                      rapidjson::Document::AllocatorType& allocator) const {
        if (_config.dimensions != -1 && supports_dimension_param(_config.model_name)) {
            std::string param_name = get_dimension_param_name();
            rapidjson::Value name(param_name.c_str(), allocator);
            doc.AddMember(name, _config.dimensions, allocator);
        }
    }
};

// Most LLM-providers' Embedding formats are based on VoyageAI.
// The following adapters inherit from VoyageAIAdapter to directly reuse its embedding logic.
class VoyageAIAdapter : public AIAdapter {
public:
    Status set_authentication(HttpClient* client) const override {
        LOG(INFO) << "[AI_CHECK]: Setting VoyageAI authentication";
        client->set_header(HttpHeaders::AUTHORIZATION, "Bearer " + _config.api_key);
        client->set_content_type("application/json");
        LOG(INFO) << "[AI_CHECK]: VoyageAI authentication set successfully";

        return Status::OK();
    }

    Status build_embedding_request(const std::vector<std::string>& inputs,
                                   std::string& request_body) const override {
        LOG(INFO) << "[AI_CHECK]: Building VoyageAI embedding request with " << inputs.size()
                  << " inputs";
        rapidjson::Document doc;
        doc.SetObject();
        auto& allocator = doc.GetAllocator();

        /*{
            "model": "xxx",
            "input": [
              "xxx",
              "xxx",
              ...
            ],
            "output_dimensions": 512
        }*/
        doc.AddMember("model", rapidjson::Value(_config.model_name.c_str(), allocator), allocator);
        add_dimension_params(doc, allocator);

        rapidjson::Value input(rapidjson::kArrayType);
        for (const auto& msg : inputs) {
            input.PushBack(rapidjson::Value(msg.c_str(), allocator), allocator);
        }
        doc.AddMember("input", input, allocator);

        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        doc.Accept(writer);
        request_body = buffer.GetString();
        LOG(INFO) << "[AI_CHECK]: VoyageAI embedding request built: " << request_body;

        return Status::OK();
    }

    Status parse_embedding_response(const std::string& response_body,
                                    std::vector<std::vector<float>>& results) const override {
        LOG(INFO) << "[AI_CHECK]: Parsing VoyageAI embedding response: " << response_body;
        rapidjson::Document doc;
        doc.Parse(response_body.c_str());

        if (doc.HasParseError() || !doc.IsObject()) {
            LOG(ERROR) << "[AI_CHECK]: Failed to parse VoyageAI embedding response";
            return Status::InternalError("Failed to parse {} response: {}", _config.provider_type,
                                         response_body);
        }
        if (!doc.HasMember("data") || !doc["data"].IsArray()) {
            LOG(ERROR) << "[AI_CHECK]: Invalid VoyageAI embedding response format";
            return Status::InternalError("Invalid {} response format: {}", _config.provider_type,
                                         response_body);
        }

        /*{
            "data":[
              {
                "object": "embedding",
                "embedding": [...], <- only need this
                "index": 0
              },
              {
                "object": "embedding",
                "embedding": [...],
                "index": 1
              }, ...
            ],
            "model"....
        }*/
        const auto& data = doc["data"];
        results.reserve(data.Size());
        LOG(INFO) << "[AI_CHECK]: Processing " << data.Size() << " embedding entries";
        for (rapidjson::SizeType i = 0; i < data.Size(); i++) {
            if (!data[i].HasMember("embedding") || !data[i]["embedding"].IsArray()) {
                LOG(ERROR) << "[AI_CHECK]: Invalid embedding entry at index " << i;
                return Status::InternalError("Invalid {} response format: {}",
                                             _config.provider_type, response_body);
            }

            std::transform(data[i]["embedding"].Begin(), data[i]["embedding"].End(),
                           std::back_inserter(results.emplace_back()),
                           [](const auto& val) { return val.GetFloat(); });
        }
        LOG(INFO) << "[AI_CHECK]: VoyageAI embedding response parsed successfully, "
                  << results.size() << " embeddings extracted";

        return Status::OK();
    }

protected:
    bool supports_dimension_param(const std::string& model_name) const override {
        static const std::unordered_set<std::string> no_dimension_models = {
                "voyage-law-2", "voyage-2", "voyage-code-2", "voyage-finance-2",
                "voyage-multimodal-3"};
        return !no_dimension_models.contains(model_name);
    }

    std::string get_dimension_param_name() const override { return "output_dimension"; }
};

// Local AI adapter for locally hosted models (Ollama, LLaMA, etc.)
class LocalAdapter : public AIAdapter {
public:
    // Local deployments typically don't need authentication
    Status set_authentication(HttpClient* client) const override {
        LOG(INFO) << "[AI_CHECK]: Setting Local adapter authentication (no auth required)";
        client->set_content_type("application/json");
        return Status::OK();
    }

    Status build_request_payload(const std::vector<std::string>& inputs,
                                 const char* const system_prompt,
                                 std::string& request_body) const override {
        LOG(INFO) << "[AI_CHECK]: Building Local adapter request payload with " << inputs.size()
                  << " inputs";
        rapidjson::Document doc;
        doc.SetObject();
        auto& allocator = doc.GetAllocator();

        if (!_config.model_name.empty()) {
            LOG(INFO) << "[AI_CHECK]: Using model: " << _config.model_name;
            doc.AddMember("model", rapidjson::Value(_config.model_name.c_str(), allocator),
                          allocator);
        }

        // If 'temperature' and 'max_tokens' are set, add them to the request body.
        if (_config.temperature != -1) {
            LOG(INFO) << "[AI_CHECK]: Setting temperature: " << _config.temperature;
            doc.AddMember("temperature", _config.temperature, allocator);
        }
        if (_config.max_tokens != -1) {
            LOG(INFO) << "[AI_CHECK]: Setting max_tokens: " << _config.max_tokens;
            doc.AddMember("max_tokens", _config.max_tokens, allocator);
        }

        rapidjson::Value messages(rapidjson::kArrayType);
        if (system_prompt && *system_prompt) {
            LOG(INFO) << "[AI_CHECK]: Adding system prompt with length " << strlen(system_prompt);
            rapidjson::Value sys_msg(rapidjson::kObjectType);
            sys_msg.AddMember("role", "system", allocator);
            sys_msg.AddMember("content", rapidjson::Value(system_prompt, allocator), allocator);
            messages.PushBack(sys_msg, allocator);
        }
        for (const auto& input : inputs) {
            rapidjson::Value message(rapidjson::kObjectType);
            message.AddMember("role", "user", allocator);
            message.AddMember("content", rapidjson::Value(input.c_str(), allocator), allocator);
            messages.PushBack(message, allocator);
        }
        doc.AddMember("messages", messages, allocator);

        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        doc.Accept(writer);
        request_body = buffer.GetString();
        LOG(INFO) << "[AI_CHECK]: Local adapter request payload built: " << request_body;

        return Status::OK();
    }

    Status parse_response(const std::string& response_body,
                          std::vector<std::string>& results) const override {
        LOG(INFO) << "[AI_CHECK]: Parsing Local adapter response: " << response_body;
        rapidjson::Document doc;
        doc.Parse(response_body.c_str());

        if (doc.HasParseError() || !doc.IsObject()) {
            LOG(ERROR) << "[AI_CHECK]: Failed to parse Local adapter response";
            return Status::InternalError("Failed to parse {} response: {}", _config.provider_type,
                                         response_body);
        }

        // Handle various response formats from local LLMs
        // Format 1: OpenAI-compatible format with choices/message/content
        if (doc.HasMember("choices") && doc["choices"].IsArray()) {
            LOG(INFO) << "[AI_CHECK]: Parsing OpenAI-compatible format";
            const auto& choices = doc["choices"];
            results.reserve(choices.Size());

            for (rapidjson::SizeType i = 0; i < choices.Size(); i++) {
                if (choices[i].HasMember("message") && choices[i]["message"].HasMember("content") &&
                    choices[i]["message"]["content"].IsString()) {
                    results.emplace_back(choices[i]["message"]["content"].GetString());
                } else if (choices[i].HasMember("text") && choices[i]["text"].IsString()) {
                    // Some local LLMs use a simpler format
                    results.emplace_back(choices[i]["text"].GetString());
                }
            }

            if (!results.empty()) {
                LOG(INFO) << "[AI_CHECK]: Successfully parsed " << results.size()
                          << " results from choices format";
                return Status::OK();
            }
        }

        // Format 2: Simple response with just "text" or "content" field
        if (doc.HasMember("text") && doc["text"].IsString()) {
            LOG(INFO) << "[AI_CHECK]: Parsing text format";
            results.emplace_back(doc["text"].GetString());
            return Status::OK();
        }

        if (doc.HasMember("content") && doc["content"].IsString()) {
            LOG(INFO) << "[AI_CHECK]: Parsing content format";
            results.emplace_back(doc["content"].GetString());
            return Status::OK();
        }

        // Format 3: Response field (Ollama format)
        if (doc.HasMember("response") && doc["response"].IsString()) {
            LOG(INFO) << "[AI_CHECK]: Parsing Ollama response format";
            results.emplace_back(doc["response"].GetString());
            return Status::OK();
        }

        LOG(ERROR) << "[AI_CHECK]: Unsupported response format from local AI";
        return Status::NotSupported("Unsupported response format from local AI.");
    }

    Status build_embedding_request(const std::vector<std::string>& inputs,
                                   std::string& request_body) const override {
        LOG(INFO) << "[AI_CHECK]: Building Local embedding request with " << inputs.size()
                  << " inputs";
        rapidjson::Document doc;
        doc.SetObject();
        auto& allocator = doc.GetAllocator();

        if (!_config.model_name.empty()) {
            LOG(INFO) << "[AI_CHECK]: Setting embedding model: " << _config.model_name;
            doc.AddMember("model", rapidjson::Value(_config.model_name.c_str(), allocator),
                          allocator);
        }

        add_dimension_params(doc, allocator);
        LOG(INFO) << "[AI_CHECK]: Added dimension parameters for Local embedding";

        rapidjson::Value input(rapidjson::kArrayType);
        for (const auto& msg : inputs) {
            input.PushBack(rapidjson::Value(msg.c_str(), allocator), allocator);
        }
        doc.AddMember("input", input, allocator);
        LOG(INFO) << "[AI_CHECK]: Added " << inputs.size()
                  << " input texts to Local embedding request";

        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        doc.Accept(writer);
        request_body = buffer.GetString();
        LOG(INFO) << "[AI_CHECK]: Local embedding request built: " << request_body;

        return Status::OK();
    }

    Status parse_embedding_response(const std::string& response_body,
                                    std::vector<std::vector<float>>& results) const override {
        LOG(INFO) << "[AI_CHECK]: Parsing Local embedding response: " << response_body;
        rapidjson::Document doc;
        doc.Parse(response_body.c_str());

        if (doc.HasParseError() || !doc.IsObject()) {
            LOG(ERROR) << "[AI_CHECK]: Failed to parse Local embedding response";
            return Status::InternalError("Failed to parse {} response: {}", _config.provider_type,
                                         response_body);
        }

        // parse different response format
        rapidjson::Value embedding;
        if (doc.HasMember("data") && doc["data"].IsArray()) {
            // "data":["object":"embedding", "embedding":[0.1, 0.2...], "index":0]
            LOG(INFO) << "[AI_CHECK]: Parsing Local embedding response in 'data' format";
            const auto& data = doc["data"];
            results.reserve(data.Size());
            LOG(INFO) << "[AI_CHECK]: Processing " << data.Size() << " Local embedding entries";
            for (rapidjson::SizeType i = 0; i < data.Size(); i++) {
                if (!data[i].HasMember("embedding") || !data[i]["embedding"].IsArray()) {
                    LOG(ERROR) << "[AI_CHECK]: Invalid Local embedding entry at index " << i;
                    return Status::InternalError("Invalid {} response format",
                                                 _config.provider_type);
                }

                std::transform(data[i]["embedding"].Begin(), data[i]["embedding"].End(),
                               std::back_inserter(results.emplace_back()),
                               [](const auto& val) { return val.GetFloat(); });
            }
        } else if (doc.HasMember("embedding") && doc["embedding"].IsArray()) {
            // "embedding":[0.1, 0.2, ...]
            LOG(INFO) << "[AI_CHECK]: Parsing Local embedding response in 'embedding' format";
            results.reserve(1);
            embedding = doc["embedding"];
            std::transform(embedding.Begin(), embedding.End(),
                           std::back_inserter(results.emplace_back()),
                           [](const auto& val) { return val.GetFloat(); });
        } else {
            LOG(ERROR) << "[AI_CHECK]: Invalid Local embedding response format";
            return Status::InternalError("Invalid {} response format: {}", _config.provider_type,
                                         response_body);
        }

        LOG(INFO) << "[AI_CHECK]: Local embedding response parsed successfully, " << results.size()
                  << " embeddings extracted";
        return Status::OK();
    }
};

// The OpenAI API format can be reused with some compatible AIs.
class OpenAIAdapter : public VoyageAIAdapter {
public:
    Status set_authentication(HttpClient* client) const override {
        LOG(INFO) << "[AI_CHECK]: Setting OpenAI authentication";
        client->set_header(HttpHeaders::AUTHORIZATION, "Bearer " + _config.api_key);
        client->set_content_type("application/json");
        LOG(INFO) << "[AI_CHECK]: OpenAI authentication set successfully";

        return Status::OK();
    }

    Status build_request_payload(const std::vector<std::string>& inputs,
                                 const char* const system_prompt,
                                 std::string& request_body) const override {
        LOG(INFO) << "[AI_CHECK]: Building OpenAI request payload with " << inputs.size()
                  << " inputs";
        rapidjson::Document doc;
        doc.SetObject();
        auto& allocator = doc.GetAllocator();

        if (_config.endpoint.ends_with("responses")) {
            LOG(INFO) << "[AI_CHECK]: Using OpenAI responses endpoint format";
            /*{
              "model": "gpt-4.1-mini",
              "input": [
                {"role": "system", "content": "system_prompt here"},
                {"role": "user", "content": "xxx"}
              ],
              "temperature": 0.7,
              "max_output_tokens": 150
            }*/
            doc.AddMember("model", rapidjson::Value(_config.model_name.c_str(), allocator),
                          allocator);
            LOG(INFO) << "[AI_CHECK]: Set OpenAI model: " << _config.model_name;

            // If 'temperature' and 'max_tokens' are set, add them to the request body.
            if (_config.temperature != -1) {
                LOG(INFO) << "[AI_CHECK]: Setting OpenAI temperature: " << _config.temperature;
                doc.AddMember("temperature", _config.temperature, allocator);
            }
            if (_config.max_tokens != -1) {
                LOG(INFO) << "[AI_CHECK]: Setting OpenAI max_output_tokens: " << _config.max_tokens;
                doc.AddMember("max_output_tokens", _config.max_tokens, allocator);
            }

            // input
            rapidjson::Value input(rapidjson::kArrayType);
            if (system_prompt && *system_prompt) {
                LOG(INFO) << "[AI_CHECK]: Adding OpenAI system prompt with length "
                          << strlen(system_prompt);
                rapidjson::Value sys_msg(rapidjson::kObjectType);
                sys_msg.AddMember("role", "system", allocator);
                sys_msg.AddMember("content", rapidjson::Value(system_prompt, allocator), allocator);
                input.PushBack(sys_msg, allocator);
            }
            for (const auto& msg : inputs) {
                rapidjson::Value message(rapidjson::kObjectType);
                message.AddMember("role", "user", allocator);
                message.AddMember("content", rapidjson::Value(msg.c_str(), allocator), allocator);
                input.PushBack(message, allocator);
            }
            doc.AddMember("input", input, allocator);
            LOG(INFO) << "[AI_CHECK]: Added " << inputs.size()
                      << " input messages to OpenAI request";
        } else {
            LOG(INFO) << "[AI_CHECK]: Using OpenAI completions endpoint format";
            /*{
              "model": "gpt-4",
              "messages": [
                {"role": "system", "content": "system_prompt here"},
                {"role": "user", "content": "xxx"}
              ],
              "temperature": x,
              "max_tokens": x,
            }*/
            doc.AddMember("model", rapidjson::Value(_config.model_name.c_str(), allocator),
                          allocator);
            LOG(INFO) << "[AI_CHECK]: Set OpenAI model: " << _config.model_name;

            // If 'temperature' and 'max_tokens' are set, add them to the request body.
            if (_config.temperature != -1) {
                LOG(INFO) << "[AI_CHECK]: Setting OpenAI temperature: " << _config.temperature;
                doc.AddMember("temperature", _config.temperature, allocator);
            }
            if (_config.max_tokens != -1) {
                LOG(INFO) << "[AI_CHECK]: Setting OpenAI max_tokens: " << _config.max_tokens;
                doc.AddMember("max_tokens", _config.max_tokens, allocator);
            }

            rapidjson::Value messages(rapidjson::kArrayType);
            if (system_prompt && *system_prompt) {
                LOG(INFO) << "[AI_CHECK]: Adding OpenAI system prompt with length "
                          << strlen(system_prompt);
                rapidjson::Value sys_msg(rapidjson::kObjectType);
                sys_msg.AddMember("role", "system", allocator);
                sys_msg.AddMember("content", rapidjson::Value(system_prompt, allocator), allocator);
                messages.PushBack(sys_msg, allocator);
            }
            for (const auto& input : inputs) {
                rapidjson::Value message(rapidjson::kObjectType);
                message.AddMember("role", "user", allocator);
                message.AddMember("content", rapidjson::Value(input.c_str(), allocator), allocator);
                messages.PushBack(message, allocator);
            }
            doc.AddMember("messages", messages, allocator);
            LOG(INFO) << "[AI_CHECK]: Added " << inputs.size() << " messages to OpenAI request";
        }

        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        doc.Accept(writer);
        request_body = buffer.GetString();
        LOG(INFO) << "[AI_CHECK]: OpenAI request payload built: " << request_body;

        return Status::OK();
    }

    Status parse_response(const std::string& response_body,
                          std::vector<std::string>& results) const override {
        LOG(INFO) << "[AI_CHECK]: Parsing OpenAI response: " << response_body;
        rapidjson::Document doc;
        doc.Parse(response_body.c_str());

        if (doc.HasParseError() || !doc.IsObject()) {
            LOG(ERROR) << "[AI_CHECK]: Failed to parse OpenAI response";
            return Status::InternalError("Failed to parse {} response: {}", _config.provider_type,
                                         response_body);
        }

        if (doc.HasMember("output") && doc["output"].IsArray()) {
            /// for responses endpoint
            LOG(INFO) << "[AI_CHECK]: Parsing OpenAI responses endpoint format";
            /*{
              "output": [
                {
                  "id": "msg_123",
                  "type": "message",
                  "role": "assistant",
                  "content": [
                    {
                      "type": "text",
                      "text": "result text here"   <- result
                    }
                  ]
                }
              ]
            }*/
            const auto& output = doc["output"];
            results.reserve(output.Size());
            LOG(INFO) << "[AI_CHECK]: Processing " << output.Size() << " OpenAI output entries";

            for (rapidjson::SizeType i = 0; i < output.Size(); i++) {
                if (!output[i].HasMember("content") || !output[i]["content"].IsArray() ||
                    output[i]["content"].Empty() || !output[i]["content"][0].HasMember("text") ||
                    !output[i]["content"][0]["text"].IsString()) {
                    LOG(ERROR) << "[AI_CHECK]: Invalid OpenAI output format at index " << i;
                    return Status::InternalError("Invalid output format in {} response: {}",
                                                 _config.provider_type, response_body);
                }

                results.emplace_back(output[i]["content"][0]["text"].GetString());
            }
        } else if (doc.HasMember("choices") && doc["choices"].IsArray()) {
            /// for completions endpoint
            LOG(INFO) << "[AI_CHECK]: Parsing OpenAI completions endpoint format";
            /*{
              "object": "chat.completion",
              "model": "gpt-4",
              "choices": [
                {
                  ...
                  "message": {
                    "role": "assistant",
                    "content": "xxx"      <- result
                  },
                  ...
                }
              ],
              ...
            }*/
            const auto& choices = doc["choices"];
            results.reserve(choices.Size());
            LOG(INFO) << "[AI_CHECK]: Processing " << choices.Size() << " OpenAI choice entries";

            for (rapidjson::SizeType i = 0; i < choices.Size(); i++) {
                if (!choices[i].HasMember("message") ||
                    !choices[i]["message"].HasMember("content") ||
                    !choices[i]["message"]["content"].IsString()) {
                    LOG(ERROR) << "[AI_CHECK]: Invalid OpenAI choice format at index " << i;
                    return Status::InternalError("Invalid choice format in {} response: {}",
                                                 _config.provider_type, response_body);
                }

                results.emplace_back(choices[i]["message"]["content"].GetString());
            }
        } else {
            LOG(ERROR) << "[AI_CHECK]: Invalid OpenAI response format";
            return Status::InternalError("Invalid {} response format: {}", _config.provider_type,
                                         response_body);
        }

        LOG(INFO) << "[AI_CHECK]: OpenAI response parsed successfully, " << results.size()
                  << " results extracted";
        return Status::OK();
    }

protected:
    bool supports_dimension_param(const std::string& model_name) const override {
        return !(model_name == "text-embedding-ada-002");
    }

    std::string get_dimension_param_name() const override { return "dimensions"; }
};

class DeepSeekAdapter : public OpenAIAdapter {
public:
    Status build_embedding_request(const std::vector<std::string>& inputs,
                                   std::string& request_body) const override {
        return Status::NotSupported("{} does not support the Embed feature.",
                                    _config.provider_type);
    }

    Status parse_embedding_response(const std::string& response_body,
                                    std::vector<std::vector<float>>& results) const override {
        return Status::NotSupported("{} does not support the Embed feature.",
                                    _config.provider_type);
    }
};

class MoonShotAdapter : public OpenAIAdapter {
public:
    Status build_embedding_request(const std::vector<std::string>& inputs,
                                   std::string& request_body) const override {
        return Status::NotSupported("{} does not support the Embed feature.",
                                    _config.provider_type);
    }

    Status parse_embedding_response(const std::string& response_body,
                                    std::vector<std::vector<float>>& results) const override {
        return Status::NotSupported("{} does not support the Embed feature.",
                                    _config.provider_type);
    }
};

class MinimaxAdapter : public OpenAIAdapter {
public:
    Status build_embedding_request(const std::vector<std::string>& inputs,
                                   std::string& request_body) const override {
        rapidjson::Document doc;
        doc.SetObject();
        auto& allocator = doc.GetAllocator();

        /*{
          "text": ["xxx", "xxx", ...],
          "model": "embo-1",
          "type": "db"
        }*/
        rapidjson::Value texts(rapidjson::kArrayType);
        for (const auto& input : inputs) {
            texts.PushBack(rapidjson::Value(input.c_str(), allocator), allocator);
        }
        doc.AddMember("model", rapidjson::Value(_config.model_name.c_str(), allocator), allocator);
        doc.AddMember("texts", texts, allocator);
        doc.AddMember("type", rapidjson::Value("db", allocator), allocator);

        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        doc.Accept(writer);
        request_body = buffer.GetString();

        return Status::OK();
    }
};

class ZhipuAdapter : public OpenAIAdapter {
protected:
    bool supports_dimension_param(const std::string& model_name) const override {
        return !(model_name == "embedding-2");
    }
};

class QwenAdapter : public OpenAIAdapter {
protected:
    bool supports_dimension_param(const std::string& model_name) const override {
        static const std::unordered_set<std::string> no_dimension_models = {
                "text-embedding-v1", "text-embedding-v2", "text2vec", "m3e-base", "m3e-small"};
        return !no_dimension_models.contains(model_name);
    }

    std::string get_dimension_param_name() const override { return "dimension"; }
};

class BaichuanAdapter : public OpenAIAdapter {
protected:
    bool supports_dimension_param(const std::string& model_name) const override { return false; }
};

// Gemini's embedding format is different from VoyageAI, so it requires a separate adapter
class GeminiAdapter : public AIAdapter {
public:
    Status set_authentication(HttpClient* client) const override {
        LOG(INFO) << "[AI_CHECK]: Setting Gemini authentication";
        client->set_header("x-goog-api-key", _config.api_key);
        client->set_content_type("application/json");
        LOG(INFO) << "[AI_CHECK]: Gemini authentication set successfully";
        return Status::OK();
    }

    Status build_request_payload(const std::vector<std::string>& inputs,
                                 const char* const system_prompt,
                                 std::string& request_body) const override {
        LOG(INFO) << "[AI_CHECK]: Building Gemini request payload with " << inputs.size()
                  << " inputs";
        rapidjson::Document doc;
        doc.SetObject();
        auto& allocator = doc.GetAllocator();

        /*{
          "systemInstruction": {
              "parts": [
                {
                  "text": "system_prompt here"
                }
              ]
            }
          ],
          "contents": [
            {
              "parts": [
                {
                  "text": "xxx"
                }
              ]
            }
          ],
          "generationConfig": {
          "temperature": 0.7,
          "maxOutputTokens": 1024
          }

        }*/
        if (system_prompt && *system_prompt) {
            LOG(INFO) << "[AI_CHECK]: Adding Gemini system instruction with length "
                      << strlen(system_prompt);
            rapidjson::Value system_instruction(rapidjson::kObjectType);
            rapidjson::Value parts(rapidjson::kArrayType);

            rapidjson::Value part(rapidjson::kObjectType);
            part.AddMember("text", rapidjson::Value(system_prompt, allocator), allocator);
            parts.PushBack(part, allocator);
            // system_instruction.PushBack(content, allocator);
            system_instruction.AddMember("parts", parts, allocator);
            doc.AddMember("systemInstruction", system_instruction, allocator);
        }

        rapidjson::Value contents(rapidjson::kArrayType);
        for (const auto& input : inputs) {
            rapidjson::Value content(rapidjson::kObjectType);
            rapidjson::Value parts(rapidjson::kArrayType);

            rapidjson::Value part(rapidjson::kObjectType);
            part.AddMember("text", rapidjson::Value(input.c_str(), allocator), allocator);

            parts.PushBack(part, allocator);
            content.AddMember("parts", parts, allocator);
            contents.PushBack(content, allocator);
        }
        doc.AddMember("contents", contents, allocator);
        LOG(INFO) << "[AI_CHECK]: Added " << inputs.size() << " content parts to Gemini request";

        // If 'temperature' and 'max_tokens' are set, add them to the request body.
        rapidjson::Value generationConfig(rapidjson::kObjectType);
        if (_config.temperature != -1) {
            LOG(INFO) << "[AI_CHECK]: Setting Gemini temperature: " << _config.temperature;
            generationConfig.AddMember("temperature", _config.temperature, allocator);
        }
        if (_config.max_tokens != -1) {
            LOG(INFO) << "[AI_CHECK]: Setting Gemini maxOutputTokens: " << _config.max_tokens;
            generationConfig.AddMember("maxOutputTokens", _config.max_tokens, allocator);
        }
        doc.AddMember("generationConfig", generationConfig, allocator);

        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        doc.Accept(writer);
        request_body = buffer.GetString();
        LOG(INFO) << "[AI_CHECK]: Gemini request payload built: " << request_body;

        return Status::OK();
    }

    Status parse_response(const std::string& response_body,
                          std::vector<std::string>& results) const override {
        LOG(INFO) << "[AI_CHECK]: Parsing Gemini response: " << response_body;
        rapidjson::Document doc;
        doc.Parse(response_body.c_str());

        if (doc.HasParseError() || !doc.IsObject()) {
            LOG(ERROR) << "[AI_CHECK]: Failed to parse Gemini response";
            return Status::InternalError("Failed to parse {} response: {}", _config.provider_type,
                                         response_body);
        }
        if (!doc.HasMember("candidates") || !doc["candidates"].IsArray()) {
            LOG(ERROR) << "[AI_CHECK]: Invalid Gemini response format, missing candidates";
            return Status::InternalError("Invalid {} response format: {}", _config.provider_type,
                                         response_body);
        }

        /*{
          "candidates":[
            {
              "content": {
                "parts": [
                  {
                    "text": "xxx"
                  }
                ]
              }
            }
          ]
        }*/
        const auto& candidates = doc["candidates"];
        results.reserve(candidates.Size());
        LOG(INFO) << "[AI_CHECK]: Processing " << candidates.Size() << " Gemini candidates";

        for (rapidjson::SizeType i = 0; i < candidates.Size(); i++) {
            if (!candidates[i].HasMember("content") ||
                !candidates[i]["content"].HasMember("parts") ||
                !candidates[i]["content"]["parts"].IsArray() ||
                candidates[i]["content"]["parts"].Empty() ||
                !candidates[i]["content"]["parts"][0].HasMember("text") ||
                !candidates[i]["content"]["parts"][0]["text"].IsString()) {
                LOG(ERROR) << "[AI_CHECK]: Invalid Gemini candidate format at index " << i;
                return Status::InternalError("Invalid candidate format in {} response",
                                             _config.provider_type);
            }

            results.emplace_back(candidates[i]["content"]["parts"][0]["text"].GetString());
        }

        LOG(INFO) << "[AI_CHECK]: Gemini response parsed successfully, " << results.size()
                  << " results extracted";
        return Status::OK();
    }

    Status build_embedding_request(const std::vector<std::string>& inputs,
                                   std::string& request_body) const override {
        LOG(INFO) << "[AI_CHECK]: Building Gemini embedding request with " << inputs.size()
                  << " inputs";
        rapidjson::Document doc;
        doc.SetObject();
        auto& allocator = doc.GetAllocator();

        /*{
          "model": "models/gemini-embedding-001",
          "content": {
              "parts": [
                {
                  "text": "xxx"
                }
              ]
            }
          "outputDimensionality": 1024
        }*/

        // gemini requires the model format as `models/{model}`
        std::string model_name = _config.model_name;
        if (!model_name.starts_with("models/")) {
            model_name = "models/" + model_name;
        }
        LOG(INFO) << "[AI_CHECK]: Set Gemini embedding model: " << model_name;
        doc.AddMember("model", rapidjson::Value(model_name.c_str(), allocator), allocator);
        add_dimension_params(doc, allocator);
        LOG(INFO) << "[AI_CHECK]: Added dimension parameters for Gemini embedding";

        rapidjson::Value content(rapidjson::kObjectType);
        for (const auto& input : inputs) {
            rapidjson::Value parts(rapidjson::kArrayType);
            rapidjson::Value part(rapidjson::kObjectType);
            part.AddMember("text", rapidjson::Value(input.c_str(), allocator), allocator);
            parts.PushBack(part, allocator);
            content.AddMember("parts", parts, allocator);
        }
        doc.AddMember("content", content, allocator);
        LOG(INFO) << "[AI_CHECK]: Added " << inputs.size()
                  << " content parts to Gemini embedding request";

        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        doc.Accept(writer);
        request_body = buffer.GetString();
        LOG(INFO) << "[AI_CHECK]: Gemini embedding request built: " << request_body;

        return Status::OK();
    }

    Status parse_embedding_response(const std::string& response_body,
                                    std::vector<std::vector<float>>& results) const override {
        LOG(INFO) << "[AI_CHECK]: Parsing Gemini embedding response: " << response_body;
        rapidjson::Document doc;
        doc.Parse(response_body.c_str());

        if (doc.HasParseError() || !doc.IsObject()) {
            LOG(ERROR) << "[AI_CHECK]: Failed to parse Gemini embedding response";
            return Status::InternalError("Failed to parse {} response: {}", _config.provider_type,
                                         response_body);
        }
        if (!doc.HasMember("embedding") || !doc["embedding"].IsObject()) {
            LOG(ERROR) << "[AI_CHECK]: Invalid Gemini embedding response format";
            return Status::InternalError("Invalid {} response format: {}", _config.provider_type,
                                         response_body);
        }

        /*{
          "embedding":{
            "values": [0.1, 0.2, 0.3]
          }
        }*/
        const auto& embedding = doc["embedding"];
        if (!embedding.HasMember("values") || !embedding["values"].IsArray()) {
            LOG(ERROR) << "[AI_CHECK]: Invalid Gemini embedding values format";
            return Status::InternalError("Invalid {} response format: {}", _config.provider_type,
                                         response_body);
        }
        std::transform(embedding["values"].Begin(), embedding["values"].End(),
                       std::back_inserter(results.emplace_back()),
                       [](const auto& val) { return val.GetFloat(); });

        LOG(INFO) << "[AI_CHECK]: Gemini embedding response parsed successfully, extracted "
                  << results[0].size() << " dimensions";
        return Status::OK();
    }

protected:
    bool supports_dimension_param(const std::string& model_name) const override {
        static const std::unordered_set<std::string> no_dimension_models = {"models/embedding-001",
                                                                            "embedding-001"};
        return !no_dimension_models.contains(model_name);
    }

    std::string get_dimension_param_name() const override { return "outputDimensionality"; }
};

class AnthropicAdapter : public VoyageAIAdapter {
public:
    Status set_authentication(HttpClient* client) const override {
        LOG(INFO) << "[AI_CHECK]: Setting Anthropic authentication with version "
                  << _config.anthropic_version;
        client->set_header("x-api-key", _config.api_key);
        client->set_header("anthropic-version", _config.anthropic_version);
        client->set_content_type("application/json");
        LOG(INFO) << "[AI_CHECK]: Anthropic authentication set successfully";

        return Status::OK();
    }

    Status build_request_payload(const std::vector<std::string>& inputs,
                                 const char* const system_prompt,
                                 std::string& request_body) const override {
        LOG(INFO) << "[AI_CHECK]: Building Anthropic request payload with " << inputs.size()
                  << " inputs";
        rapidjson::Document doc;
        doc.SetObject();
        auto& allocator = doc.GetAllocator();

        /*
            "model": "claude-opus-4-1-20250805",
            "max_tokens": 1024,
            "system": "system_prompt here",
            "messages": [
              {"role": "user", "content": "xxx"}
            ],
            "temperature": 0.7
        */

        // If 'temperature' and 'max_tokens' are set, add them to the request body.
        doc.AddMember("model", rapidjson::Value(_config.model_name.c_str(), allocator), allocator);
        LOG(INFO) << "[AI_CHECK]: Set Anthropic model: " << _config.model_name;
        if (_config.temperature != -1) {
            LOG(INFO) << "[AI_CHECK]: Setting Anthropic temperature: " << _config.temperature;
            doc.AddMember("temperature", _config.temperature, allocator);
        }
        if (_config.max_tokens != -1) {
            LOG(INFO) << "[AI_CHECK]: Setting Anthropic max_tokens: " << _config.max_tokens;
            doc.AddMember("max_tokens", _config.max_tokens, allocator);
        } else {
            // Keep the default value, Anthropic requires this parameter
            LOG(INFO) << "[AI_CHECK]: Setting Anthropic default max_tokens: 2048";
            doc.AddMember("max_tokens", 2048, allocator);
        }
        if (system_prompt && *system_prompt) {
            LOG(INFO) << "[AI_CHECK]: Adding Anthropic system prompt with length "
                      << strlen(system_prompt);
            doc.AddMember("system", rapidjson::Value(system_prompt, allocator), allocator);
        }

        rapidjson::Value messages(rapidjson::kArrayType);
        for (const auto& input : inputs) {
            rapidjson::Value message(rapidjson::kObjectType);
            message.AddMember("role", "user", allocator);
            message.AddMember("content", rapidjson::Value(input.c_str(), allocator), allocator);
            messages.PushBack(message, allocator);
        }
        doc.AddMember("messages", messages, allocator);
        LOG(INFO) << "[AI_CHECK]: Added " << inputs.size() << " messages to Anthropic request";

        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        doc.Accept(writer);
        request_body = buffer.GetString();
        LOG(INFO) << "[AI_CHECK]: Anthropic request payload built: " << request_body;

        return Status::OK();
    }

    Status parse_response(const std::string& response_body,
                          std::vector<std::string>& results) const override {
        LOG(INFO) << "[AI_CHECK]: Parsing Anthropic response: " << response_body;
        rapidjson::Document doc;
        doc.Parse(response_body.c_str());
        if (doc.HasParseError() || !doc.IsObject()) {
            LOG(ERROR) << "[AI_CHECK]: Failed to parse Anthropic response";
            return Status::InternalError("Failed to parse {} response: {}", _config.provider_type,
                                         response_body);
        }
        if (!doc.HasMember("content") || !doc["content"].IsArray()) {
            LOG(ERROR) << "[AI_CHECK]: Invalid Anthropic response format, missing content";
            return Status::InternalError("Invalid {} response format: {}", _config.provider_type,
                                         response_body);
        }

        /*{
            "content": [
              {
                "text": "xxx",
                "type": "text"
              }
            ]
        }*/
        const auto& content = doc["content"];
        results.reserve(1);
        LOG(INFO) << "[AI_CHECK]: Processing " << content.Size() << " Anthropic content blocks";

        std::string result;
        for (rapidjson::SizeType i = 0; i < content.Size(); i++) {
            if (!content[i].HasMember("type") || !content[i]["type"].IsString() ||
                !content[i].HasMember("text") || !content[i]["text"].IsString()) {
                LOG(WARNING) << "[AI_CHECK]: Skipping invalid Anthropic content block at index "
                             << i;
                continue;
            }

            if (std::string(content[i]["type"].GetString()) == "text") {
                if (!result.empty()) {
                    result += "\n";
                }
                result += content[i]["text"].GetString();
            }
        }

        results.emplace_back(std::move(result));
        LOG(INFO)
                << "[AI_CHECK]: Anthropic response parsed successfully, extracted text with length "
                << results[0].length();
        return Status::OK();
    }
};

// Mock adapter used only for UT to bypass real HTTP calls and return deterministic data.
class MockAdapter : public AIAdapter {
public:
    Status set_authentication(HttpClient* client) const override {
        LOG(INFO) << "[AI_CHECK]: Setting Mock adapter authentication (no auth required)";
        return Status::OK();
    }

    Status build_request_payload(const std::vector<std::string>& inputs,
                                 const char* const system_prompt,
                                 std::string& request_body) const override {
        LOG(INFO) << "[AI_CHECK]: Building Mock request payload (no-op)";
        return Status::OK();
    }

    Status parse_response(const std::string& response_body,
                          std::vector<std::string>& results) const override {
        LOG(INFO) << "[AI_CHECK]: Parsing Mock response: " << response_body;
        results.emplace_back(response_body);
        LOG(INFO) << "[AI_CHECK]: Mock response parsed successfully";
        return Status::OK();
    }

    Status build_embedding_request(const std::vector<std::string>& inputs,
                                   std::string& request_body) const override {
        LOG(INFO) << "[AI_CHECK]: Building Mock embedding request (no-op)";
        return Status::OK();
    }

    Status parse_embedding_response(const std::string& response_body,
                                    std::vector<std::vector<float>>& results) const override {
        LOG(INFO) << "[AI_CHECK]: Parsing Mock embedding response: " << response_body;
        rapidjson::Document doc;
        doc.SetObject();
        doc.Parse(response_body.c_str());
        if (doc.HasParseError() || !doc.IsObject()) {
            LOG(ERROR) << "[AI_CHECK]: Failed to parse Mock embedding response";
            return Status::InternalError("Failed to parse embedding response");
        }
        if (!doc.HasMember("embedding") || !doc["embedding"].IsArray()) {
            LOG(ERROR) << "[AI_CHECK]: Invalid Mock embedding response format";
            return Status::InternalError("Invalid embedding response format");
        }

        results.reserve(1);
        std::transform(doc["embedding"].Begin(), doc["embedding"].End(),
                       std::back_inserter(results.emplace_back()),
                       [](const auto& val) { return val.GetFloat(); });
        LOG(INFO) << "[AI_CHECK]: Mock embedding response parsed successfully, extracted "
                  << results[0].size() << " dimensions";
        return Status::OK();
    }
};

class AIAdapterFactory {
public:
    static std::shared_ptr<AIAdapter> create_adapter(const std::string& provider_type) {
        LOG(INFO) << "[AI_CHECK]: Creating AI adapter for provider type: " << provider_type;
        static const std::unordered_map<std::string, std::function<std::shared_ptr<AIAdapter>()>>
                adapters = {{"LOCAL", []() { return std::make_shared<LocalAdapter>(); }},
                            {"OPENAI", []() { return std::make_shared<OpenAIAdapter>(); }},
                            {"MOONSHOT", []() { return std::make_shared<MoonShotAdapter>(); }},
                            {"DEEPSEEK", []() { return std::make_shared<DeepSeekAdapter>(); }},
                            {"MINIMAX", []() { return std::make_shared<MinimaxAdapter>(); }},
                            {"ZHIPU", []() { return std::make_shared<ZhipuAdapter>(); }},
                            {"QWEN", []() { return std::make_shared<QwenAdapter>(); }},
                            {"BAICHUAN", []() { return std::make_shared<BaichuanAdapter>(); }},
                            {"ANTHROPIC", []() { return std::make_shared<AnthropicAdapter>(); }},
                            {"GEMINI", []() { return std::make_shared<GeminiAdapter>(); }},
                            {"VOYAGEAI", []() { return std::make_shared<VoyageAIAdapter>(); }},
                            {"MOCK", []() { return std::make_shared<MockAdapter>(); }}};

        auto it = adapters.find(provider_type);
        if (it != adapters.end()) {
            LOG(INFO) << "[AI_CHECK]: Successfully created adapter for " << provider_type;
            return it->second();
        } else {
            LOG(ERROR) << "[AI_CHECK]: Unknown provider type: " << provider_type;
            return nullptr;
        }
    }
};

#include "common/compile_check_end.h"
} // namespace doris::vectorized