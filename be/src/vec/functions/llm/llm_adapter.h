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

#include "common/status.h"
#include "http/http_client.h"
#include "http/http_headers.h"
#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

namespace doris::vectorized {
class LLMAdapter {
public:
    virtual ~LLMAdapter() = default;

    // Set authentication headers for the HTTP client
    virtual Status set_authentication(HttpClient* client) const = 0;

    virtual void init(const TLLMResource& config) { _config = config; }

    // Build request payload based on input text strings
    virtual Status build_request_payload(const std::vector<std::string>& inputs,
                                         const char* const system_prompt,
                                         std::string& request_body) const = 0;

    // Parse response from LLM service and extract generated text results
    virtual Status parse_response(const std::string& response_body,
                                  std::vector<std::string>& results) const = 0;

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
    TLLMResource _config;

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
class VoyageAIAdapter : public LLMAdapter {
public:
    Status set_authentication(HttpClient* client) const override {
        client->set_header(HttpHeaders::AUTHORIZATION, "Bearer " + _config.api_key);
        client->set_content_type("application/json");

        return Status::OK();
    }

    Status build_request_payload(const std::vector<std::string>& inputs,
                                 const char* const system_prompt,
                                 std::string& request_body) const override {
        return Status::NotSupported("VoyageAI only support embedding function");
    }

    Status parse_response(const std::string& response_body,
                          std::vector<std::string>& results) const override {
        return Status::NotSupported("VoyageAI only support embedding function");
    }

    Status build_embedding_request(const std::vector<std::string>& inputs,
                                   std::string& request_body) const override {
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

        return Status::OK();
    }

    Status parse_embedding_response(const std::string& response_body,
                                    std::vector<std::vector<float>>& results) const override {
        rapidjson::Document doc;
        doc.Parse(response_body.c_str());

        if (doc.HasParseError() || !doc.IsObject()) {
            return Status::InternalError("Failed to parse {} response: {}", _config.provider_type,
                                         response_body);
        }
        if (!doc.HasMember("data") || !doc["data"].IsArray()) {
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
        for (rapidjson::SizeType i = 0; i < data.Size(); i++) {
            if (!data[i].HasMember("embedding") || !data[i]["embedding"].IsArray()) {
                return Status::InternalError("Invalid {} response format: {}",
                                             _config.provider_type, response_body);
            }

            std::transform(data[i]["embedding"].Begin(), data[i]["embedding"].End(),
                           std::back_inserter(results.emplace_back()),
                           [](const auto& val) { return val.GetFloat(); });
        }

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

// Local LLM adapter for locally hosted models (Ollama, LLaMA, etc.)
class LocalAdapter : public LLMAdapter {
public:
    // Local deployments typically don't need authentication
    Status set_authentication(HttpClient* client) const override {
        client->set_content_type("application/json");
        return Status::OK();
    }

    Status build_request_payload(const std::vector<std::string>& inputs,
                                 const char* const system_prompt,
                                 std::string& request_body) const override {
        rapidjson::Document doc;
        doc.SetObject();
        auto& allocator = doc.GetAllocator();

        if (!_config.model_name.empty()) {
            doc.AddMember("model", rapidjson::Value(_config.model_name.c_str(), allocator),
                          allocator);
        }

        // If 'temperature' and 'max_tokens' are set, add them to the request body.
        if (_config.temperature != -1) {
            doc.AddMember("temperature", _config.temperature, allocator);
        }
        if (_config.max_tokens != -1) {
            doc.AddMember("max_tokens", _config.max_tokens, allocator);
        }

        rapidjson::Value messages(rapidjson::kArrayType);
        if (system_prompt && *system_prompt) {
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

        return Status::OK();
    }

    Status parse_response(const std::string& response_body,
                          std::vector<std::string>& results) const override {
        rapidjson::Document doc;
        doc.Parse(response_body.c_str());

        if (doc.HasParseError() || !doc.IsObject()) {
            return Status::InternalError("Failed to parse {} response: {}", _config.provider_type,
                                         response_body);
        }

        // Handle various response formats from local LLMs
        // Format 1: OpenAI-compatible format with choices/message/content
        if (doc.HasMember("choices") && doc["choices"].IsArray()) {
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
                return Status::OK();
            }
        }

        // Format 2: Simple response with just "text" or "content" field
        if (doc.HasMember("text") && doc["text"].IsString()) {
            results.emplace_back(doc["text"].GetString());
            return Status::OK();
        }

        if (doc.HasMember("content") && doc["content"].IsString()) {
            results.emplace_back(doc["content"].GetString());
            return Status::OK();
        }

        // Format 3: Response field (Ollama format)
        if (doc.HasMember("response") && doc["response"].IsString()) {
            results.emplace_back(doc["response"].GetString());
            return Status::OK();
        }

        return Status::InternalError("Unsupported response format from local LLM");
    }

    Status build_embedding_request(const std::vector<std::string>& inputs,
                                   std::string& request_body) const override {
        rapidjson::Document doc;
        doc.SetObject();
        auto& allocator = doc.GetAllocator();

        if (!_config.model_name.empty()) {
            doc.AddMember("model", rapidjson::Value(_config.model_name.c_str(), allocator),
                          allocator);
        }

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

        return Status::OK();
    }

    Status parse_embedding_response(const std::string& response_body,
                                    std::vector<std::vector<float>>& results) const override {
        rapidjson::Document doc;
        doc.Parse(response_body.c_str());

        if (doc.HasParseError() || !doc.IsObject()) {
            return Status::InternalError("Failed to parse {} response: {}", _config.provider_type,
                                         response_body);
        }

        // parse different response format
        rapidjson::Value embedding;
        if (doc.HasMember("data") && doc["data"].IsArray()) {
            // "data":["object":"embedding", "embedding":[0.1, 0.2...], "index":0]
            const auto& data = doc["data"];
            results.reserve(data.Size());
            for (rapidjson::SizeType i = 0; i < data.Size(); i++) {
                if (!data[i].HasMember("embedding") || !data[i]["embedding"].IsArray()) {
                    return Status::InternalError("Invalid {} response format",
                                                 _config.provider_type);
                }

                std::transform(data[i]["embedding"].Begin(), data[i]["embedding"].End(),
                               std::back_inserter(results.emplace_back()),
                               [](const auto& val) { return val.GetFloat(); });
            }
        } else if (doc.HasMember("embedding") && doc["embedding"].IsArray()) {
            // "embedding":[0.1, 0.2, ...]
            results.reserve(1);
            embedding = doc["embedding"];
            std::transform(embedding.Begin(), embedding.End(),
                           std::back_inserter(results.emplace_back()),
                           [](const auto& val) { return val.GetFloat(); });
        } else {
            return Status::InternalError("Invalid {} response format: {}", _config.provider_type,
                                         response_body);
        }

        return Status::OK();
    }
};

// The OpenAI API format can be reused with some compatible LLMs.
class OpenAIAdapter : public VoyageAIAdapter {
public:
    Status set_authentication(HttpClient* client) const override {
        client->set_header(HttpHeaders::AUTHORIZATION, "Bearer " + _config.api_key);
        client->set_content_type("application/json");

        return Status::OK();
    }

    // TODO: Only supports GPT-4 and earlier; GPT-5 and newer are not supported yet.
    Status build_request_payload(const std::vector<std::string>& inputs,
                                 const char* const system_prompt,
                                 std::string& request_body) const override {
        rapidjson::Document doc;
        doc.SetObject();
        auto& allocator = doc.GetAllocator();

        /*{
          "model": "gpt-4",
          "messages": [
            {"role": "system", "content": "system_prompt here"},
            {"role": "user", "content": "xxx"}
          ],
          "temperature": x,
          "max_tokens": x,
        }*/
        doc.AddMember("model", rapidjson::Value(_config.model_name.c_str(), allocator), allocator);

        // If 'temperature' and 'max_tokens' are set, add them to the request body.
        if (_config.temperature != -1) {
            doc.AddMember("temperature", _config.temperature, allocator);
        }
        if (_config.max_tokens != -1) {
            doc.AddMember("max_tokens", _config.max_tokens, allocator);
        }

        rapidjson::Value messages(rapidjson::kArrayType);
        if (system_prompt && *system_prompt) {
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

        return Status::OK();
    }

    Status parse_response(const std::string& response_body,
                          std::vector<std::string>& results) const override {
        rapidjson::Document doc;
        doc.Parse(response_body.c_str());

        if (doc.HasParseError() || !doc.IsObject()) {
            return Status::InternalError("Failed to parse {} response: {}", _config.provider_type,
                                         response_body);
        }

        if (!doc.HasMember("choices") || !doc["choices"].IsArray()) {
            return Status::InternalError("Invalid {} response format: {}", _config.provider_type,
                                         response_body);
        }

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

        for (rapidjson::SizeType i = 0; i < choices.Size(); i++) {
            if (!choices[i].HasMember("message") || !choices[i]["message"].HasMember("content") ||
                !choices[i]["message"]["content"].IsString()) {
                return Status::InternalError("Invalid choice format in {} response: {}",
                                             _config.provider_type, response_body);
            }

            results.emplace_back(choices[i]["message"]["content"].GetString());
        }

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
class GeminiAdapter : public LLMAdapter {
public:
    Status set_authentication(HttpClient* client) const override {
        client->set_header("x-goog-api-key", _config.api_key);
        client->set_content_type("application/json");
        return Status::OK();
    }

    Status build_request_payload(const std::vector<std::string>& inputs,
                                 const char* const system_prompt,
                                 std::string& request_body) const override {
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

        // If 'temperature' and 'max_tokens' are set, add them to the request body.
        rapidjson::Value generationConfig(rapidjson::kObjectType);
        if (_config.temperature != -1) {
            generationConfig.AddMember("temperature", _config.temperature, allocator);
        }
        if (_config.max_tokens != -1) {
            generationConfig.AddMember("maxOutputTokens", _config.max_tokens, allocator);
        }
        doc.AddMember("generationConfig", generationConfig, allocator);

        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        doc.Accept(writer);
        request_body = buffer.GetString();

        return Status::OK();
    }

    Status parse_response(const std::string& response_body,
                          std::vector<std::string>& results) const override {
        rapidjson::Document doc;
        doc.Parse(response_body.c_str());

        if (doc.HasParseError() || !doc.IsObject()) {
            return Status::InternalError("Failed to parse {} response: {}", _config.provider_type,
                                         response_body);
        }
        if (!doc.HasMember("candidates") || !doc["candidates"].IsArray()) {
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

        for (rapidjson::SizeType i = 0; i < candidates.Size(); i++) {
            if (!candidates[i].HasMember("content") ||
                !candidates[i]["content"].HasMember("parts") ||
                !candidates[i]["content"]["parts"].IsArray() ||
                candidates[i]["content"]["parts"].Empty() ||
                !candidates[i]["content"]["parts"][0].HasMember("text") ||
                !candidates[i]["content"]["parts"][0]["text"].IsString()) {
                return Status::InternalError("Invalid candidate format in {} response",
                                             _config.provider_type);
            }

            results.emplace_back(candidates[i]["content"]["parts"][0]["text"].GetString());
        }

        return Status::OK();
    }

    Status build_embedding_request(const std::vector<std::string>& inputs,
                                   std::string& request_body) const override {
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
        doc.AddMember("model", rapidjson::Value(model_name.c_str(), allocator), allocator);
        add_dimension_params(doc, allocator);

        rapidjson::Value content(rapidjson::kObjectType);
        for (const auto& input : inputs) {
            rapidjson::Value parts(rapidjson::kArrayType);
            rapidjson::Value part(rapidjson::kObjectType);
            part.AddMember("text", rapidjson::Value(input.c_str(), allocator), allocator);
            parts.PushBack(part, allocator);
            content.AddMember("parts", parts, allocator);
        }
        doc.AddMember("content", content, allocator);

        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        doc.Accept(writer);
        request_body = buffer.GetString();

        return Status::OK();
    }

    Status parse_embedding_response(const std::string& response_body,
                                    std::vector<std::vector<float>>& results) const override {
        rapidjson::Document doc;
        doc.Parse(response_body.c_str());

        if (doc.HasParseError() || !doc.IsObject()) {
            return Status::InternalError("Failed to parse {} response: {}", _config.provider_type,
                                         response_body);
        }
        if (!doc.HasMember("embedding") || !doc["embedding"].IsObject()) {
            return Status::InternalError("Invalid {} response format: {}", _config.provider_type,
                                         response_body);
        }

        /*{
          "embedding": [
            {
              "values": [0.1, 0.2, 0.3]
            }
          ]
        }*/
        const auto& embedding = doc["embedding"];
        if (!embedding.HasMember("values") || !embedding["values"].IsArray()) {
            return Status::InternalError("Invalid {} response format: {}", _config.provider_type,
                                         response_body);
        }
        std::transform(embedding["values"].Begin(), embedding["values"].End(),
                       std::back_inserter(results.emplace_back()),
                       [](const auto& val) { return val.GetFloat(); });

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
        client->set_header("x-api-key", _config.api_key);
        client->set_header("anthropic-version", _config.anthropic_version);
        client->set_content_type("application/json");

        return Status::OK();
    }

    Status build_request_payload(const std::vector<std::string>& inputs,
                                 const char* const system_prompt,
                                 std::string& request_body) const override {
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
        if (_config.temperature != -1) {
            doc.AddMember("temperature", _config.temperature, allocator);
        }
        if (_config.max_tokens != -1) {
            doc.AddMember("max_tokens", _config.max_tokens, allocator);
        } else {
            // Keep the default value, Anthropic requires this parameter
            doc.AddMember("max_tokens", 2048, allocator);
        }
        if (system_prompt && *system_prompt) {
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

        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        doc.Accept(writer);
        request_body = buffer.GetString();

        return Status::OK();
    }

    Status parse_response(const std::string& response_body,
                          std::vector<std::string>& results) const override {
        rapidjson::Document doc;
        doc.Parse(response_body.c_str());
        if (doc.HasParseError() || !doc.IsObject()) {
            return Status::InternalError("Failed to parse {} response: {}", _config.provider_type,
                                         response_body);
        }
        if (!doc.HasMember("content") || !doc["content"].IsArray()) {
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

        std::string result;
        for (rapidjson::SizeType i = 0; i < content.Size(); i++) {
            if (!content[i].HasMember("type") || !content[i]["type"].IsString() ||
                !content[i].HasMember("text") || !content[i]["text"].IsString()) {
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
        return Status::OK();
    }
};

// Mock adapter used only for UT to bypass real HTTP calls and return deterministic data.
class MockAdapter : public LLMAdapter {
public:
    Status set_authentication(HttpClient* client) const override { return Status::OK(); }

    Status build_request_payload(const std::vector<std::string>& inputs,
                                 const char* const system_prompt,
                                 std::string& request_body) const override {
        return Status::OK();
    }

    Status parse_response(const std::string& response_body,
                          std::vector<std::string>& results) const override {
        results.emplace_back(response_body);
        return Status::OK();
    }

    Status build_embedding_request(const std::vector<std::string>& inputs,
                                   std::string& request_body) const override {
        return Status::OK();
    }

    Status parse_embedding_response(const std::string& response_body,
                                    std::vector<std::vector<float>>& results) const override {
        rapidjson::Document doc;
        doc.SetObject();
        doc.Parse(response_body.c_str());
        if (doc.HasParseError() || !doc.IsObject()) {
            return Status::InternalError("Failed to parse embedding response");
        }
        if (!doc.HasMember("embedding") || !doc["embedding"].IsArray()) {
            return Status::InternalError("Invalid embedding response format");
        }

        results.reserve(1);
        std::transform(doc["embedding"].Begin(), doc["embedding"].End(),
                       std::back_inserter(results.emplace_back()),
                       [](const auto& val) { return val.GetFloat(); });
        return Status::OK();
    }
};

class LLMAdapterFactory {
public:
    static std::shared_ptr<LLMAdapter> create_adapter(const std::string& provider_type) {
        static const std::unordered_map<std::string, std::function<std::shared_ptr<LLMAdapter>()>>
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
        return (it != adapters.end()) ? it->second() : nullptr;
    }
};
} // namespace doris::vectorized