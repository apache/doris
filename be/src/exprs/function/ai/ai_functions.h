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
#include <cstdlib>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
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
#include "util/string_util.h"
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
        if (Status status = this->_init_from_resource(context, block, arguments, config, adapter);
            !status.ok()) {
            return status;
        }

        return assert_cast<const Derived&>(*this).execute_with_adapter(
                context, block, arguments, result, input_rows_count, config, adapter);
    }

protected:
    // Reads the shared AI context window size from query options. String AI batch functions and
    // ai_agg both use the same byte-based session variable so batching behavior stays consistent.
    static int64_t get_ai_context_window_size(FunctionContext* context) {
        DORIS_CHECK(context != nullptr);
        QueryContext* query_ctx = context->state()->get_query_ctx();
        DORIS_CHECK(query_ctx != nullptr);

        return query_ctx->query_options().ai_context_window_size;
    }

    // Derived classes can override this method for non-text/default behavior.
    // The base implementation handles all string-input/string-output batchable functions.
    Status execute_with_adapter(FunctionContext* context, Block& block,
                                const ColumnNumbers& arguments, uint32_t result,
                                size_t input_rows_count, const TAIResource& config,
                                std::shared_ptr<AIAdapter>& adapter) const {
        auto col_result = assert_cast<const Derived&>(*this).create_result_column();
        RETURN_IF_ERROR(execute_batched_prompts(context, block, arguments, input_rows_count, config,
                                                adapter, *col_result));

        block.replace_by_position(result, std::move(col_result));
        return Status::OK();
    }

    MutableColumnPtr create_result_column() const { return ColumnString::create(); }

    // Provider-reusable hook for AI functions(string) -> string.
    Status append_batch_results(const std::vector<std::string>& batch_results,
                                IColumn& col_result) const {
        auto& string_col = assert_cast<ColumnString&>(col_result);
        for (const auto& batch_result : batch_results) {
            string_col.insert_data(batch_result.data(), batch_result.size());
        }
        return Status::OK();
    }

    static void normalize_endpoint(TAIResource& config) {
        // 1. If users configure only the version root like `.../v1` or `.../v1beta`, append
        //    `models/<model>:batchEmbedContents` for `embed`, and `models/<model>:generateContent`
        //    for other AI scalar functions.
        // 2. `:embedContent` -> `:batchEmbedContents`
        if (iequal(config.provider_type, "GEMINI")) {
            if (iequal(Derived::name, "embed") && config.endpoint.ends_with(":embedContent")) {
                static constexpr std::string_view legacy_suffix = ":embedContent";
                config.endpoint.replace(config.endpoint.size() - legacy_suffix.size(),
                                        legacy_suffix.size(), ":batchEmbedContents");
                return;
            }

            if (!config.endpoint.ends_with("v1") && !config.endpoint.ends_with("v1beta")) {
                return;
            }

            std::string model_name = config.model_name;
            if (!model_name.starts_with("models/")) {
                model_name = "models/" + model_name;
            }

            config.endpoint += "/";
            config.endpoint += model_name;
            config.endpoint +=
                    iequal(Derived::name, "embed") ? ":batchEmbedContents" : ":generateContent";
            return;
        }

        // The endpoint `v1/completions` does not support `system_prompt`.
        // To ensure a clear structure and stable AI results.
        // Convert from `v1/completions` to `v1/chat/completions`
        if (config.endpoint.ends_with("v1/completions")) {
            static constexpr std::string_view legacy_suffix = "v1/completions";
            config.endpoint.replace(config.endpoint.size() - legacy_suffix.size(),
                                    legacy_suffix.size(), "v1/chat/completions");
        }
    }

    // Executes one HTTP POST request and validates transport-level success.
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

    // Provider-reusable helper for string-returning functions.
    // Estimates one batch entry size using the raw prompt length plus the fixed JSON wrapper cost.
    size_t estimate_batch_entry_size(size_t idx, const std::string& prompt) const {
        static constexpr size_t json_wrapper_size = 20;
        return prompt.size() + std::to_string(idx).size() + json_wrapper_size;
    }

    // Provider-reusable helper for string-returning functions.
    // Executes one batch request and parses the provider result into one string per input row.
    Status execute_batch_request(const std::vector<std::string>& batch_prompts,
                                 std::vector<std::string>& results, const TAIResource& config,
                                 std::shared_ptr<AIAdapter>& adapter,
                                 FunctionContext* context) const {
#ifdef BE_TEST
        const char* test_result = std::getenv("AI_TEST_RESULT");
        if (test_result != nullptr) {
            std::vector<std::string> parsed_test_response;
            RETURN_IF_ERROR(
                    adapter->parse_response(std::string(test_result), parsed_test_response));
            if (parsed_test_response.empty()) {
                return Status::InternalError("AI returned empty result");
            }
            if (parsed_test_response.size() != batch_prompts.size()) {
                return Status::RuntimeError(
                        "Failed to parse {} batch result, expected {} items but got {}", get_name(),
                        batch_prompts.size(), parsed_test_response.size());
            }
            results = std::move(parsed_test_response);
            return Status::OK();
        }
        if (config.provider_type == "MOCK") {
            results.clear();
            results.reserve(batch_prompts.size());
            for (const auto& prompt : batch_prompts) {
                results.emplace_back("this is a mock response. " + prompt);
            }
            return Status::OK();
        }
#endif

        std::string batch_prompt;
        RETURN_IF_ERROR(build_batch_prompt(batch_prompts, batch_prompt));

        std::vector<std::string> inputs = {batch_prompt};
        std::vector<std::string> parsed_response;

        std::string request_body;
        RETURN_IF_ERROR(adapter->build_request_payload(
                inputs, assert_cast<const Derived&>(*this).system_prompt, request_body));

        std::string response;
        RETURN_IF_ERROR(send_request_to_llm(request_body, response, config, adapter, context));
        RETURN_IF_ERROR(adapter->parse_response(response, parsed_response));
        if (parsed_response.empty()) {
            return Status::InternalError("AI returned empty result");
        }
        if (parsed_response.size() != batch_prompts.size()) {
            LOG(WARNING) << "AI batch result size mismatch, function=" << get_name()
                         << ", provider=" << config.provider_type << ", model=" << config.model_name
                         << ", expected_rows=" << batch_prompts.size()
                         << ", actual_rows=" << parsed_response.size()
                         << ", response_body=" << response;
            return Status::RuntimeError(
                    "Failed to parse {} batch result, expected {} items but got {}", get_name(),
                    batch_prompts.size(), parsed_response.size());
        }
        results = std::move(parsed_response);
        return Status::OK();
    }

    // Provider-reusable helper for string-returning functions.
    // Runs the common batch execution flow; derived classes only need to define how one batch of
    // string results is inserted into the final output column.
    Status execute_batched_prompts(FunctionContext* context, Block& block,
                                   const ColumnNumbers& arguments, size_t input_rows_count,
                                   const TAIResource& config, std::shared_ptr<AIAdapter>& adapter,
                                   IColumn& col_result) const {
        std::vector<std::string> batch_prompts;
        size_t current_batch_size = 2; // []
        const size_t max_batch_prompt_size =
                static_cast<size_t>(get_ai_context_window_size(context));

        for (size_t i = 0; i < input_rows_count; ++i) {
            std::string prompt;
            RETURN_IF_ERROR(
                    assert_cast<const Derived&>(*this).build_prompt(block, arguments, i, prompt));

            size_t entry_size = estimate_batch_entry_size(batch_prompts.size(), prompt);
            if (entry_size > max_batch_prompt_size) {
                if (!batch_prompts.empty()) {
                    std::vector<std::string> batch_results;
                    RETURN_IF_ERROR(this->execute_batch_request(batch_prompts, batch_results,
                                                                config, adapter, context));
                    RETURN_IF_ERROR(assert_cast<const Derived&>(*this).append_batch_results(
                            batch_results, col_result));
                    batch_prompts.clear();
                    current_batch_size = 2;
                }

                std::vector<std::string> single_prompts;
                single_prompts.emplace_back(std::move(prompt));
                std::vector<std::string> single_results;
                RETURN_IF_ERROR(this->execute_batch_request(single_prompts, single_results, config,
                                                            adapter, context));
                RETURN_IF_ERROR(assert_cast<const Derived&>(*this).append_batch_results(
                        single_results, col_result));
                continue;
            }

            size_t additional_size = entry_size + (batch_prompts.empty() ? 0 : 1);
            if (!batch_prompts.empty() &&
                current_batch_size + additional_size > max_batch_prompt_size) {
                std::vector<std::string> batch_results;
                RETURN_IF_ERROR(this->execute_batch_request(batch_prompts, batch_results, config,
                                                            adapter, context));
                RETURN_IF_ERROR(assert_cast<const Derived&>(*this).append_batch_results(
                        batch_results, col_result));
                batch_prompts.clear();
                current_batch_size = 2;
                additional_size = entry_size;
            }

            batch_prompts.emplace_back(std::move(prompt));
            current_batch_size += additional_size;
        }

        if (!batch_prompts.empty()) {
            std::vector<std::string> batch_results;
            RETURN_IF_ERROR(this->execute_batch_request(batch_prompts, batch_results, config,
                                                        adapter, context));
            RETURN_IF_ERROR(assert_cast<const Derived&>(*this).append_batch_results(batch_results,
                                                                                    col_result));
        }
        return Status::OK();
    }

private:
    // The ai resource must be literal
    Status _init_from_resource(FunctionContext* context, const Block& block,
                               const ColumnNumbers& arguments, TAIResource& config,
                               std::shared_ptr<AIAdapter>& adapter) const {
        const ColumnWithTypeAndName& resource_column = block.get_by_position(arguments[0]);
        StringRef resource_name_ref = resource_column.column->get_data_at(0);
        std::string resource_name = std::string(resource_name_ref.data, resource_name_ref.size);

        const std::shared_ptr<std::map<std::string, TAIResource>>& ai_resources =
                context->state()->get_query_ctx()->get_ai_resources();
        DORIS_CHECK(ai_resources);
        auto it = ai_resources->find(resource_name);
        DORIS_CHECK(it != ai_resources->end());
        config = it->second;

        normalize_endpoint(config);

        adapter = AIAdapterFactory::create_adapter(config.provider_type);
        DORIS_CHECK(adapter);

        adapter->init(config);
        return Status::OK();
    }

    // Serializes one text batch into the shared JSON-array prompt format consumed by LLM
    // providers for batch string functions.
    Status build_batch_prompt(const std::vector<std::string>& batch_prompts,
                              std::string& prompt) const {
        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);

        writer.StartArray();
        for (size_t i = 0; i < batch_prompts.size(); ++i) {
            writer.StartObject();
            writer.Key("idx");
            writer.Uint64(i);
            writer.Key("input");
            writer.String(batch_prompts[i].data(),
                          static_cast<rapidjson::SizeType>(batch_prompts[i].size()));
            writer.EndObject();
        }
        writer.EndArray();

        prompt = buffer.GetString();
        return Status::OK();
    }
};

} // namespace doris
