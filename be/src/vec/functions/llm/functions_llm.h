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

#include <gen_cpp/FrontendService.h>
#include <gen_cpp/PaloInternalService_types.h>

#include <memory>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "http/http_client.h"
#include "util/threadpool.h"
#include "vec/functions/function.h"
#include "vec/functions/llm/llm_adapter.h"

namespace doris::vectorized {
// Used to receive LLM Resource in the fragment
class LLMFunctionUtil {
public:
    LLMFunctionUtil(const LLMFunctionUtil&) = delete;
    LLMFunctionUtil& operator=(const LLMFunctionUtil&) = delete;

    static LLMFunctionUtil& instance() {
        static LLMFunctionUtil util;
        return util;
    }

    void prepare(std::map<std::string, TLLMResource> llm_resources) {
        _llm_resources = std::move(llm_resources);
    }

    const TLLMResource& get_llm_resource(const std::string& resource_name) const {
        auto it = _llm_resources.find(resource_name);
        if (it != _llm_resources.end()) {
            return it->second;
        }
        throw Status::InternalError("LLM resource not found: " + resource_name);
    }

private:
    std::map<std::string, TLLMResource> _llm_resources;
    LLMFunctionUtil() = default;
    ~LLMFunctionUtil() = default;
};

// Base class for LLM-based functions
template <typename Derived>
class LLMFunction : public IFunction {
public:
    std::string get_name() const override { return assert_cast<const Derived&>(*this).name; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeString>());
    }

    // If the user doesn't provide the first arg, `resource_name`
    // FE will add the `resource_name` to the arguments list using the Session Variable.
    // So the value here should be the maximum number that the function can accept.
    size_t get_number_of_arguments() const override {
        return assert_cast<const Derived&>(*this).number_of_arguments;
    }

    // The llm resource must be literal
    Status init_from_resource(const Block& block, const ColumnNumbers& arguments, size_t row_num) {
        // 1. Initialize config
        const ColumnWithTypeAndName& resource_column = block.get_by_position(arguments[0]);
        if (const auto* col_const_resource =
                    check_and_get_column<ColumnConst>(resource_column.column.get())) {
            StringRef resource_name_ref = col_const_resource->get_data_at(0);
            const std::string resource_name =
                    std::string(resource_name_ref.data, resource_name_ref.size);
            _config = LLMFunctionUtil::instance().get_llm_resource(resource_name);
        } else {
            return Status::InternalError("LLM Function must accept literal for the resource name.");
        }

        // 2. Create an adapter based on provider_type
        _adapter = LLMAdapterFactory::create_adapter(_config.provider_type);
        if (!_adapter) {
            return Status::InternalError("Unsupported LLM provider type: " + _config.provider_type);
        }
        _adapter->init(_config);

        return Status::OK();
    }

    // Executes the actual HTTP request
    Status do_send_request(HttpClient* client, const std::string& request_body,
                           std::string& response) const {
        RETURN_IF_ERROR(client->init(_config.endpoint));

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

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        auto col_result = ColumnString::create();
        auto null_map = ColumnUInt8::create(input_rows_count, 0);

        std::unique_ptr<ThreadPool> thread_pool;
        Status st = ThreadPoolBuilder("LLMRequestPool")
                            .set_min_threads(1)
                            .set_max_threads(config::llm_max_concurrent_requests > 0
                                                     ? config::llm_max_concurrent_requests
                                                     : 1)
                            .build(&thread_pool);
        if (!st.ok()) {
            return Status::InternalError("Failed to create thread pool: " + st.to_string());
        }

        struct RowResult {
            std::string data;
            Status status;
            bool is_null = false;
        };

        std::vector<RowResult> results(input_rows_count);
        for (size_t i = 0; i < input_rows_count; ++i) {
            Status submit_status =
                    thread_pool->submit_func([this, i, &block, &arguments, &results]() {
                        RowResult& row_result = results[i];

                        try {
                            // 1. Build LLM prompt text
                            std::string prompt;
                            Status status = assert_cast<const Derived&>(*this).build_prompt(
                                    block, arguments, i, prompt);
                            // 2. Init LLM resources and adapters
                            if (status.ok()) {
                                status = const_cast<Derived*>(assert_cast<const Derived*>(this))
                                                 ->init_from_resource(block, arguments, i);
                            }

                            if (!status.ok()) {
                                row_result.status = status;
                                row_result.is_null = true;
                                return;
                            }

                            // 3. Execute a single LLM request and get the result
                            std::string result_str;
                            status = execute_single_request(prompt, result_str);
                            if (!status.ok()) {
                                row_result.status = status;
                                row_result.is_null = true;
                                return;
                            }
                            row_result.data = std::move(result_str);
                            row_result.status = Status::OK();
                        } catch (const std::exception& e) {
                            row_result.status = Status::InternalError("Exception in LLM request: " +
                                                                      std::string(e.what()));
                            row_result.is_null = true;
                        }
                    });

            if (!submit_status.ok()) {
                return Status::InternalError("Failed to submit task to thread pool: " +
                                             submit_status.to_string());
            }
        }

        thread_pool->wait();

        for (size_t i = 0; i < input_rows_count; ++i) {
            const RowResult& row_result = results[i];

            if (!row_result.status.ok()) {
                return row_result.status;
            }

            null_map->get_data()[i] = row_result.is_null ? 1 : 0;
            if (!row_result.is_null) {
                col_result->insert_data(row_result.data.data(), row_result.data.size());
            } else {
                col_result->insert_default();
            }
        }

        block.replace_by_position(
                result, ColumnNullable::create(std::move(col_result), std::move(null_map)));
        return Status::OK();
    }

private:
    TLLMResource _config;
    std::shared_ptr<LLMAdapter> _adapter;
};

} // namespace doris::vectorized