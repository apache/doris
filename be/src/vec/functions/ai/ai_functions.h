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
#include <type_traits>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "http/http_client.h"
#include "runtime/define_primitive_type.h"
#include "runtime/primitive_type.h"
#include "runtime/query_context.h"
#include "runtime/runtime_state.h"
#include "util/threadpool.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/common/cow.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/ai/ai_adapter.h"
#include "vec/functions/function.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

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
        DataTypePtr return_type_impl =
                assert_cast<const Derived&>(*this).get_return_type_impl(DataTypes());
        MutableColumnPtr col_result = return_type_impl->create_column();

        TAIResource config;
        std::shared_ptr<AIAdapter> adapter;
        if (Status status =
                    const_cast<Derived*>(assert_cast<const Derived*>(this))
                            ->_init_from_resource(context, block, arguments, config, adapter);
            !status.ok()) {
            return status;
        }

        for (size_t i = 0; i < input_rows_count; ++i) {
            // Build AI prompt text
            std::string prompt;
            RETURN_IF_ERROR(
                    assert_cast<const Derived&>(*this).build_prompt(block, arguments, i, prompt));

            // Execute a single AI request and get the result
            if (return_type_impl->get_primitive_type() == PrimitiveType::TYPE_ARRAY) {
                // Array(Float) for AI_EMBED
                std::vector<float> float_result;
                RETURN_IF_ERROR(
                        execute_single_request(prompt, float_result, config, adapter, context));

                auto& col_array = assert_cast<ColumnArray&>(*col_result);
                auto& offsets = col_array.get_offsets();
                auto& nested_nullable_col = assert_cast<ColumnNullable&>(col_array.get_data());
                auto& nested_col =
                        assert_cast<ColumnFloat32&>(*(nested_nullable_col.get_nested_column_ptr()));
                nested_col.reserve(nested_col.size() + float_result.size());

                size_t current_offset = nested_col.size();
                nested_col.insert_many_raw_data(reinterpret_cast<const char*>(float_result.data()),
                                                float_result.size());
                offsets.push_back(current_offset + float_result.size());
                auto& null_map = nested_nullable_col.get_null_map_column();
                null_map.insert_many_vals(0, float_result.size());
            } else {
                std::string string_result;
                RETURN_IF_ERROR(
                        execute_single_request(prompt, string_result, config, adapter, context));

                switch (return_type_impl->get_primitive_type()) {
                case PrimitiveType::TYPE_STRING: { // string
                    assert_cast<ColumnString&>(*col_result)
                            .insert_data(string_result.data(), string_result.size());
                    break;
                }
                case PrimitiveType::TYPE_BOOLEAN: { // boolean for AI_FILTER
#ifdef BE_TEST
                    string_result = "false";
#endif
                    if (string_result != "true" && string_result != "false") {
                        return Status::RuntimeError("Failed to parse boolean value: " +
                                                    string_result);
                    }
                    assert_cast<ColumnUInt8&>(*col_result)
                            .insert_value(static_cast<UInt8>(string_result == "true"));
                    break;
                }
                case PrimitiveType::TYPE_FLOAT: { // float for AI_SIMILARITY
                    assert_cast<ColumnFloat32&>(*col_result).insert_value(std::stof(string_result));
                    break;
                }
                default:
                    return Status::InternalError("Unsupported ReturnType for AIFunction");
                }
            }
        }

        block.replace_by_position(result, std::move(col_result));
        return Status::OK();
    }

private:
    // The ai resource must be literal
    Status _init_from_resource(FunctionContext* context, const Block& block,
                               const ColumnNumbers& arguments, TAIResource& config,
                               std::shared_ptr<AIAdapter>& adapter) {
        // 1. Initialize config
        const ColumnWithTypeAndName& resource_column = block.get_by_position(arguments[0]);
        StringRef resource_name_ref = resource_column.column->get_data_at(0);
        std::string resource_name = std::string(resource_name_ref.data, resource_name_ref.size);

        const std::map<std::string, TAIResource>& ai_resources =
                context->state()->get_query_ctx()->get_ai_resources();
        auto it = ai_resources.find(resource_name);
        if (it == ai_resources.end()) {
            return Status::InvalidArgument("AI resource not found: " + resource_name);
        }
        config = it->second;

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
        RETURN_IF_ERROR(client->init(config.endpoint));

        QueryContext* query_ctx = context->state()->get_query_ctx();
        int64_t remaining_query_time = query_ctx->get_remaining_query_time_seconds();
        if (remaining_query_time <= 0) {
            return Status::TimedOut("Query timeout exceeded before AI request");
        }

        client->set_timeout_ms(remaining_query_time * 1000);

        if (!config.api_key.empty()) {
            RETURN_IF_ERROR(adapter->set_authentication(client));
        }

        return client->execute_post_request(request_body, &response);
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
};

#include "common/compile_check_end.h"
} // namespace doris::vectorized