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

#include "core/data_type/primitive_type.h"
#include "exprs/function/ai/ai_functions.h"

namespace doris {
class FunctionEmbed : public AIFunction<FunctionEmbed> {
public:
    static constexpr auto name = "embed";

    static constexpr size_t number_of_arguments = 2;

    static constexpr auto system_prompt = "";

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeFloat32>()));
    }

    static FunctionPtr create() { return std::make_shared<FunctionEmbed>(); }

    Status execute_with_adapter(FunctionContext* context, Block& block,
                                const ColumnNumbers& arguments, uint32_t result,
                                size_t input_rows_count, const TAIResource& config,
                                std::shared_ptr<AIAdapter>& adapter) const {
        if (arguments.size() != 2) {
            return Status::InvalidArgument("Function EMBED expects 2 arguments, but got {}",
                                           arguments.size());
        }

        auto col_result = ColumnArray::create(
                ColumnNullable::create(ColumnFloat32::create(), ColumnUInt8::create()));
        std::vector<std::string> batch_prompts;
        size_t current_batch_size = 0;
        const int32_t max_batch_size = get_embed_max_batch_size(context);
        const size_t max_context_window_size =
                static_cast<size_t>(get_ai_context_window_size(context));

        for (size_t i = 0; i < input_rows_count; ++i) {
            std::string prompt;
            RETURN_IF_ERROR(build_prompt(block, arguments, i, prompt));

            const size_t prompt_size = prompt.size();
            if (prompt_size > max_context_window_size) {
                RETURN_IF_ERROR(flush_text_embedding_batch(batch_prompts, *col_result, config,
                                                           adapter, context));
                current_batch_size = 0;

                batch_prompts.emplace_back(std::move(prompt));
                RETURN_IF_ERROR(flush_text_embedding_batch(batch_prompts, *col_result, config,
                                                           adapter, context));
                continue;
            }

            if (!batch_prompts.empty() &&
                (current_batch_size + prompt_size > max_context_window_size ||
                 batch_prompts.size() >= static_cast<size_t>(max_batch_size))) {
                RETURN_IF_ERROR(flush_text_embedding_batch(batch_prompts, *col_result, config,
                                                           adapter, context));
                current_batch_size = 0;
            }

            batch_prompts.emplace_back(std::move(prompt));
            current_batch_size += prompt_size;
        }

        RETURN_IF_ERROR(
                flush_text_embedding_batch(batch_prompts, *col_result, config, adapter, context));

        block.replace_by_position(result, std::move(col_result));
        return Status::OK();
    }

private:
    static int32_t get_embed_max_batch_size(FunctionContext* context) {
        QueryContext* query_ctx = context->state()->get_query_ctx();
        DORIS_CHECK(query_ctx != nullptr);
        return query_ctx->query_options().embed_max_batch_size;
    }

    Status flush_text_embedding_batch(std::vector<std::string>& batch_prompts,
                                      ColumnArray& col_result, const TAIResource& config,
                                      std::shared_ptr<AIAdapter>& adapter,
                                      FunctionContext* context) const {
        if (batch_prompts.empty()) {
            return Status::OK();
        }

        std::string request_body;
        RETURN_IF_ERROR(adapter->build_embedding_request(batch_prompts, request_body));

        std::vector<std::vector<float>> batch_results;
        RETURN_IF_ERROR(execute_embedding_request(request_body, batch_results, batch_prompts.size(),
                                                  config, adapter, context));
        for (const auto& batch_result : batch_results) {
            insert_embedding_result(col_result, batch_result);
        }
        batch_prompts.clear();
        return Status::OK();
    }

    Status execute_embedding_request(const std::string& request_body,
                                     std::vector<std::vector<float>>& results, size_t expected_size,
                                     const TAIResource& config, std::shared_ptr<AIAdapter>& adapter,
                                     FunctionContext* context) const {
#ifdef BE_TEST
        if (config.provider_type == "MOCK") {
            results.clear();
            results.reserve(expected_size);
            for (size_t i = 0; i < expected_size; ++i) {
                results.emplace_back(std::initializer_list<float> {0, 1, 2, 3, 4});
            }
            return Status::OK();
        }
#endif

        std::string response;
        RETURN_IF_ERROR(
                this->send_request_to_llm(request_body, response, config, adapter, context));
        RETURN_IF_ERROR(adapter->parse_embedding_response(response, results));
        if (results.empty()) {
            return Status::InternalError("AI returned empty result");
        }
        if (results.size() != expected_size) [[unlikely]] {
            return Status::InternalError(
                    "AI embedding returned {} results, but {} inputs were sent", results.size(),
                    expected_size);
        }
        return Status::OK();
    }

    static void insert_embedding_result(ColumnArray& col_array,
                                        const std::vector<float>& float_result) {
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
    }
};

}; // namespace doris
