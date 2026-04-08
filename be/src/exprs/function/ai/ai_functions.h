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
#include <glog/logging.h>

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <memory>
#include <string>
#include <type_traits>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "core/column/column_array.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/cow.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/define_primitive_type.h"
#include "core/data_type/primitive_type.h"
#include "exprs/function/ai/ai_adapter.h"
#include "exprs/function/function.h"
#include "runtime/query_context.h"
#include "runtime/runtime_state.h"
#include "service/http/http_client.h"
#include "util/security.h"
#include "util/threadpool.h"

namespace doris {

// Base class for AI-based functions
template <typename Derived>
class AIFunction : public IFunction {
public:
    std::string get_name() const override { return assert_cast<const Derived&>(*this).name; }

    // If the user doesn't provide the first arg, `resource_name`
    // FE will add the `resource_name` to the arguments list using the Session Variable.
    // So the value here should be the maximum number that the function can accept.
    size_t get_number_of_arguments() const override {
        return assert_cast<const Derived&>(*this).number_of_arguments;
    }

    bool is_blockable() const override { return true; }

    virtual Status build_prompt(const Block& block, const ColumnNumbers& arguments, size_t row_num,
                                std::string& prompt) const {
        const ColumnWithTypeAndName& text_column = block.get_by_position(arguments[1]);
        StringRef text_ref = text_column.column->get_data_at(row_num);
        prompt = std::string(text_ref.data, text_ref.size);

        return Status::OK();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        TAIResource config;
        std::shared_ptr<AIAdapter> adapter;
        if (Status status = assert_cast<const Derived*>(this)->_init_from_resource(
                    context, block, arguments, config, adapter);
            !status.ok()) {
            return status;
        }

        return assert_cast<const Derived&>(*this).execute_with_adapter(
                context, block, arguments, result, input_rows_count, config, adapter);
    }

protected:
    // Derived classes can override this method for non-text/default behavior.
    // The base implementation keeps previous text-oriented processing unchanged.
    Status execute_with_adapter(FunctionContext* context, Block& block,
                                const ColumnNumbers& arguments, uint32_t result,
                                size_t input_rows_count, const TAIResource& config,
                                std::shared_ptr<AIAdapter>& adapter) const {
        DataTypePtr return_type_impl =
                assert_cast<const Derived&>(*this).get_return_type_impl(DataTypes());
        if (return_type_impl->get_primitive_type() != PrimitiveType::TYPE_STRING) {
            return Status::InternalError("{} must override execute for non-string return type",
                                         get_name());
        }
        MutableColumnPtr col_result = ColumnString::create();

        for (size_t i = 0; i < input_rows_count; ++i) {
            // Build AI prompt text
            std::string prompt;
            RETURN_IF_ERROR(
                    assert_cast<const Derived&>(*this).build_prompt(block, arguments, i, prompt));

            std::string string_result;
            RETURN_IF_ERROR(
                    execute_single_request(prompt, string_result, config, adapter, context));
            assert_cast<ColumnString&>(*col_result)
                    .insert_data(string_result.data(), string_result.size());
        }

        block.replace_by_position(result, std::move(col_result));
        return Status::OK();
    }

    // The endpoint `v1/completions` does not support `system_prompt`.
    // To ensure a clear structure and stable AI results.
    // Convert from `v1/completions` to `v1/chat/completions`
    static void normalize_endpoint(TAIResource& config) {
        if (config.endpoint.ends_with("v1/completions")) {
            static constexpr std::string_view legacy_suffix = "v1/completions";
            config.endpoint.replace(config.endpoint.size() - legacy_suffix.size(),
                                    legacy_suffix.size(), "v1/chat/completions");
        }
    }

    // The ai resource must be literal
    Status _init_from_resource(FunctionContext* context, const Block& block,
                               const ColumnNumbers& arguments, TAIResource& config,
                               std::shared_ptr<AIAdapter>& adapter) const {
        // 1. Initialize config
        const ColumnWithTypeAndName& resource_column = block.get_by_position(arguments[0]);
        StringRef resource_name_ref = resource_column.column->get_data_at(0);
        std::string resource_name = std::string(resource_name_ref.data, resource_name_ref.size);

        const std::shared_ptr<std::map<std::string, TAIResource>>& ai_resources =
                context->state()->get_query_ctx()->get_ai_resources();
        if (!ai_resources) {
            return Status::InternalError("AI resources metadata missing in QueryContext");
        }
        auto it = ai_resources->find(resource_name);
        if (it == ai_resources->end()) {
            return Status::InvalidArgument("AI resource not found: " + resource_name);
        }
        config = it->second;

        normalize_endpoint(config);

        // 2. Create an adapter based on provider_type
        adapter = AIAdapterFactory::create_adapter(config.provider_type);
        if (!adapter) {
            return Status::InvalidArgument("Unsupported AI provider type: " + config.provider_type);
        }
        adapter->init(config);

        return Status::OK();
    }

    // Executes the actual HTTP request
    Status do_send_request(HttpClient* client, const std::string& request_body,
                           std::string& response, const TAIResource& config,
                           std::shared_ptr<AIAdapter>& adapter, FunctionContext* context) const {
        RETURN_IF_ERROR(client->init(config.endpoint, false));

        QueryContext* query_ctx = context->state()->get_query_ctx();
        int64_t remaining_query_time = query_ctx->get_remaining_query_time_seconds();
        if (remaining_query_time <= 0) {
            return Status::TimedOut("Query timeout exceeded before AI request");
        }

        client->set_timeout_ms(remaining_query_time * 1000);

        if (!config.api_key.empty()) {
            RETURN_IF_ERROR(adapter->set_authentication(client));
        }

        Status st = client->execute_post_request(request_body, &response);
        long http_status = client->get_http_status();

        if (!st.ok()) {
            LOG(INFO) << "AI HTTP request failed before status validation, provider="
                      << config.provider_type << ", model=" << config.model_name
                      << ", endpoint=" << mask_token(config.endpoint)
                      << ", exec_status=" << st.to_string() << ", response_body=" << response;
            return st;
        }
        if (http_status != 200) {
            return Status::HttpError(
                    "http status code is not 200, code={}, url={}, response_body={}", http_status,
                    mask_token(config.endpoint), response);
        }
        return Status::OK();
    }

    // Sends the request with retry mechanism for handling transient failures
    Status send_request_to_llm(const std::string& request_body, std::string& response,
                               const TAIResource& config, std::shared_ptr<AIAdapter>& adapter,
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
                                  const TAIResource& config, std::shared_ptr<AIAdapter>& adapter,
                                  FunctionContext* context) const {
        std::vector<std::string> inputs = {input};
        std::vector<std::string> results;

        std::string request_body;
        RETURN_IF_ERROR(adapter->build_request_payload(
                inputs, assert_cast<const Derived&>(*this).system_prompt, request_body));

        std::string response;
        if (config.provider_type == "MOCK") {
            // Mock path for UT
            response = "this is a mock response. " + input;
        } else {
            RETURN_IF_ERROR(send_request_to_llm(request_body, response, config, adapter, context));
        }

        RETURN_IF_ERROR(adapter->parse_response(response, results));
        if (results.empty()) {
            return Status::InternalError("AI returned empty result");
        }

        result = std::move(results[0]);
        return Status::OK();
    }

    Status execute_single_request(const std::string& input, std::vector<float>& result,
                                  const TAIResource& config, std::shared_ptr<AIAdapter>& adapter,
                                  FunctionContext* context) const {
        std::vector<std::string> inputs = {input};
        std::vector<std::vector<float>> results;

        std::string request_body;
        RETURN_IF_ERROR(adapter->build_embedding_request(inputs, request_body));

        std::string response;
        if (config.provider_type == "MOCK") {
            // Mock path for UT
            response = "{\"embedding\": [0, 1, 2, 3, 4]}";
        } else {
            RETURN_IF_ERROR(send_request_to_llm(request_body, response, config, adapter, context));
        }

        RETURN_IF_ERROR(adapter->parse_embedding_response(response, results));
        if (results.empty()) {
            return Status::InternalError("AI returned empty result");
        }

        result = std::move(results[0]);
        return Status::OK();
    }

    // Sends a pre-built embedding request body and parses the float vector result.
    // Used when the request body has already been constructed (e.g., multimodal embedding).
    Status execute_embedding_request(const std::string& request_body, std::vector<float>& result,
                                     const TAIResource& config, std::shared_ptr<AIAdapter>& adapter,
                                     FunctionContext* context) const {
        std::vector<std::vector<float>> results;
        std::string response;
        if (config.provider_type == "MOCK") {
            response = "{\"embedding\": [0, 1, 2, 3, 4]}";
        } else {
            RETURN_IF_ERROR(send_request_to_llm(request_body, response, config, adapter, context));
        }
        RETURN_IF_ERROR(adapter->parse_embedding_response(response, results));
        if (results.empty()) {
            return Status::InternalError("AI returned empty result");
        }
        result = std::move(results[0]);
        return Status::OK();
    }
};

} // namespace doris
