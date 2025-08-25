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
#include "runtime/query_context.h"
#include "runtime/runtime_state.h"
#include "util/threadpool.h"
#include "vec/columns/column_const.h"
#include "vec/common/cow.h"
#include "vec/functions/function.h"
#include "vec/functions/llm/llm_adapter.h"

namespace doris::vectorized {
// Base class for LLM-based functions
template <typename Derived, typename ReturnType = ColumnString>
class LLMFunction : public IFunction {
public:
    std::string get_name() const override { return assert_cast<const Derived&>(*this).name; }

    // If the user doesn't provide the first arg, `resource_name`
    // FE will add the `resource_name` to the arguments list using the Session Variable.
    // So the value here should be the maximum number that the function can accept.
    size_t get_number_of_arguments() const override {
        return assert_cast<const Derived&>(*this).number_of_arguments;
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        MutableColumnPtr col_result = ReturnType::create();

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

        TLLMResource config;
        std::shared_ptr<LLMAdapter> adapter;
        if (Status status =
                    const_cast<Derived*>(assert_cast<const Derived*>(this))
                            ->_init_from_resource(context, block, arguments, config, adapter);
            !status.ok()) {
            return status;
        }

        std::vector<RowResult> results(input_rows_count);
        for (size_t i = 0; i < input_rows_count; ++i) {
            Status submit_status = thread_pool->submit_func([this, i, &block, &arguments, &results,
                                                             &adapter, &config, context]() {
                RowResult& row_result = results[i];

                try {
                    // Build LLM prompt text
                    std::string prompt;
                    Status status = assert_cast<const Derived&>(*this).build_prompt(
                            block, arguments, i, prompt);

                    if (!status.ok()) {
                        row_result.status = status;
                        row_result.is_null = true;
                        return;
                    }

                    // Execute a single LLM request and get the result
                    std::string result_str;
                    status = execute_single_request(prompt, result_str, config, adapter, context);
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

            if (!row_result.is_null) {
                if constexpr (std::is_same_v<ReturnType, ColumnString>) {
                    // string
                    assert_cast<ColumnString&>(*col_result)
                            .insert_data(row_result.data.data(), row_result.data.size());
                } else if constexpr (std::is_same_v<ReturnType, ColumnUInt8>) {
                    // bool
                    if (row_result.data != "1" && row_result.data != "0") {
                        return Status::RuntimeError("Failed to parse boolean value: " +
                                                    row_result.data);
                    }
                    assert_cast<ColumnUInt8&>(*col_result)
                            .insert_value(static_cast<UInt8>(row_result.data == "1"));
                } else if constexpr (std::is_same_v<ReturnType, ColumnFloat32>) {
                    // float
                    assert_cast<ColumnFloat32&>(*col_result)
                            .insert_value(std::stof(row_result.data));
                } else {
                    return Status::InternalError("Unsupported ReturnType for LLMFunction");
                }
            } else {
                col_result->insert_default();
            }
        }

        block.replace_by_position(result, std::move(col_result));
        return Status::OK();
    }

private:
    // The llm resource must be literal
    Status _init_from_resource(FunctionContext* context, const Block& block,
                               const ColumnNumbers& arguments, TLLMResource& config,
                               std::shared_ptr<LLMAdapter>& adapter) {
        // 1. Initialize config
        const ColumnWithTypeAndName& resource_column = block.get_by_position(arguments[0]);
        StringRef resource_name_ref = resource_column.column->get_data_at(0);
        std::string resource_name = std::string(resource_name_ref.data, resource_name_ref.size);

        const std::map<std::string, TLLMResource>& llm_resources =
                context->state()->get_query_ctx()->get_llm_resources();
        auto it = llm_resources.find(resource_name);
        if (it == llm_resources.end()) {
            return Status::InternalError("LLM resource not found: " + resource_name);
        }
        config = it->second;

        // 2. Create an adapter based on provider_type
        adapter = LLMAdapterFactory::create_adapter(config.provider_type);
        if (!adapter) {
            return Status::InternalError("Unsupported LLM provider type: " + config.provider_type);
        }
        adapter->init(config);

        return Status::OK();
    }

    // Executes the actual HTTP request
    Status do_send_request(HttpClient* client, const std::string& request_body,
                           std::string& response, const TLLMResource& config,
                           std::shared_ptr<LLMAdapter>& adapter, FunctionContext* context) const {
        RETURN_IF_ERROR(client->init(config.endpoint));

        QueryContext* query_ctx = context->state()->get_query_ctx();
        int64_t remaining_query_time = query_ctx->get_remaining_query_time_seconds();
        if (remaining_query_time <= 0) {
            return Status::InternalError("Query timeout exceeded before LLM request");
        }

        client->set_timeout_ms(remaining_query_time * 1000);

        if (!config.api_key.empty()) {
            RETURN_IF_ERROR(adapter->set_authentication(client));
        }

        return client->execute_post_request(request_body, &response);
    }

    // Sends the request with retry mechanism for handling transient failures
    Status send_request_to_llm(const std::string& request_body, std::string& response,
                               const TLLMResource& config, std::shared_ptr<LLMAdapter>& adapter,
                               FunctionContext* context) const {
        return HttpClient::execute_with_retry(config.max_retries, config.retry_delay_second,
                                              [this, &request_body, &response, &config, &adapter,
                                               context](HttpClient* client) -> Status {
                                                  return this->do_send_request(client, request_body,
                                                                               response, config,
                                                                               adapter, context);
                                              });
    }

    // Wrapper for executing a single LLM request
    Status execute_single_request(const std::string& input, std::string& result,
                                  const TLLMResource& config, std::shared_ptr<LLMAdapter>& adapter,
                                  FunctionContext* context) const {
        std::vector<std::string> inputs = {input};
        std::vector<std::string> results;

        std::string request_body;
        RETURN_IF_ERROR(adapter->build_request_payload(
                inputs, assert_cast<const Derived&>(*this).system_prompt, request_body));

        std::string response;
        RETURN_IF_ERROR(send_request_to_llm(request_body, response, config, adapter, context));

        Status status = adapter->parse_response(response, results);
        if (!status.ok()) {
            return status;
        }

        if (results.empty()) {
            return Status::InternalError("LLM returned empty result");
        }

        result = results[0];
        return Status::OK();
    }
};

} // namespace doris::vectorized