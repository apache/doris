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

#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "http/http_client.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_response.h"
#include "rapidjson/document.h"
#include "vec/functions/function.h"
#include "vec/functions/llm_adapter.h"

namespace doris::vectorized {
// Base class for LLM-based functions
template <typename Derived>
class LLMFunction : public IFunction {
public:
    Status init() {
        LLMConfig config;
        config.provider_type = config::llm_provider;
        config.endpoint_url = config::llm_endpoint_url;
        config.model_name = config::llm_model_name;
        config.api_key = config::llm_api_key;
        config.max_retries = config::llm_max_retries;
        config.retry_delay_ms = config::llm_retry_delay_ms;
        config.timeout_ms = config::llm_timeout_ms;

        _config = config;

        _adapter = LLMAdapterFactory::create_adapter(config.provider_type);
        if (!_adapter) {
            return Status::InternalError("Unsupported LLM provider type: " + config.provider_type);
        }

        _adapter->init(config);

        return Status::OK();
    }

    std::string get_name() const override { return assert_cast<const Derived&>(*this).name; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeString>());
    }

    size_t get_number_of_arguments() const override {
        return assert_cast<const Derived&>(*this).number_of_arguments();
    }

    virtual Status config_is_valid() const {
        if (_config.endpoint_url.empty()) {
            return Status::InternalError("LLM endpoint URL is not configured");
        }

        if (_config.model_name.empty()) {
            return Status::InternalError("LLM model name is not configured");
        }

        if (_config.provider_type.empty()) {
            return Status::InternalError("LLM provider type is not configured");
        }

        if (_config.provider_type != "local" && _config.api_key.empty()) {
            return Status::InternalError("LLM API key is not configured");
        }

        if (!_adapter) {
            return Status::InternalError("LLM adapter is not initialized");
        }

        return Status::OK();
    }

    // Executes the actual HTTP request
    Status do_send_request(HttpClient* client, const std::string& request_body,
                           std::string& response) const {
        RETURN_IF_ERROR(client->init(_config.endpoint_url));

        client->set_timeout_ms(_config.timeout_ms);

        if (!_config.api_key.empty()) {
            RETURN_IF_ERROR(_adapter->set_authentication(client));
        }

        return client->execute_post_request(request_body, &response);
    }

    // Sends the request with retry mechanism for handling transient failures
    Status send_request_to_llm(const std::string& request_body, std::string& response) const {
        return HttpClient::execute_with_retry(
                _config.max_retries, _config.retry_delay_ms,
                [this, &request_body, &response](HttpClient* client) -> Status {
                    return this->do_send_request(client, request_body, response);
                });
    }

    // Wrapper for executing a single LLM request
    Status execute_single_request(const std::string& input, std::string& result) const {
        std::vector<std::string> inputs = {input};
        std::vector<std::string> results;

        std::string request_body;
        RETURN_IF_ERROR(_adapter->build_request_payload(inputs, request_body));

        std::string response;
        RETURN_IF_ERROR(send_request_to_llm(request_body, response));

        Status status = _adapter->parse_response(response, results);
        if (!status.ok()) {
            return status;
        }

        if (results.empty()) {
            return Status::InternalError("LLM returned empty result");
        }

        result = results[0];
        return Status::OK();
    }

    // Currently processing rows sequentially one by one.
    // TODO: Implement parallel processing using multi-threading to handle multiple LLM
    // requests concurrently for better performance.
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        RETURN_IF_ERROR(config_is_valid());

        auto col_result = ColumnString::create();
        auto null_map = ColumnUInt8::create(input_rows_count, 0);

        // processing rows sequentially one by one
        for (size_t i = 0; i < input_rows_count; ++i) {
            std::string prompt;
            RETURN_IF_ERROR(
                    assert_cast<const Derived&>(*this).build_prompt(block, arguments, i, prompt));

            std::string result_str;
            RETURN_IF_ERROR(execute_single_request(prompt, result_str));

            col_result->insert_data(result_str.data(), result_str.size());
        }

        block.replace_by_position(
                result, ColumnNullable::create(std::move(col_result), std::move(null_map)));
        return Status::OK();
    }

private:
    LLMConfig _config;
    std::shared_ptr<LLMAdapter> _adapter;
};

} // namespace doris::vectorized