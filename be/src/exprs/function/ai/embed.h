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

#include <glog/logging.h>
#include <rapidjson/document.h>

#include <string_view>

#include "core/data_type/data_type_nullable.h"
#include "core/data_type/primitive_type.h"
#include "exprs/function/ai/ai_functions.h"
#include "util/jsonb_utils.h"
#include "util/s3_uri.h"
#include "util/s3_util.h"

namespace doris {
class FunctionEmbed : public AIFunction<FunctionEmbed> {
public:
    static constexpr auto name = "embed";

    static constexpr size_t number_of_arguments = 2;

    static constexpr auto system_prompt = "";

    DataTypePtr get_nested_return_type_impl(const DataTypes& /*arguments*/) const {
        return std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeFloat32>()));
    }

    using PreparedFunctionImpl::execute;

    Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                   uint32_t result, size_t input_rows_count, const TAIResource& config,
                   std::shared_ptr<AIAdapter>& adapter) const {
        if (arguments.size() != 2) {
            return Status::InvalidArgument("Function EMBED expects 2 arguments, but got {}",
                                           arguments.size());
        }

        const auto& input = block.get_by_position(arguments[1]);
        ColumnUInt8::MutablePtr result_null_map;
        if (input.type->is_nullable()) {
            const auto& [column, is_const] = unpack_if_const(input.column);
            const auto& nullable =
                    assert_cast<const ColumnNullable&, TypeCheckOnRelease::DISABLE>(*column);
            result_null_map = ColumnUInt8::create(input_rows_count, 0);
            VectorizedUtils::update_null_map(result_null_map->get_data(),
                                             nullable.get_null_map_data(), is_const);
        }

        if (result_null_map &&
            !simd::contain_zero(result_null_map->get_data().data(), input_rows_count)) {
            block.get_by_position(result).column =
                    block.get_by_position(result).type->create_column_const(input_rows_count,
                                                                            Field());
            return Status::OK();
        }

        ColumnPtr input_column = input.unnest_nullable().column;
        PrimitiveType input_type = remove_nullable(input.type)->get_primitive_type();
        if (input_type == PrimitiveType::TYPE_JSONB) {
            return _execute_multimodal_embed(context, block, result, input_rows_count, config,
                                             adapter, input_column, std::move(result_null_map));
        }
        if (input_type == PrimitiveType::TYPE_STRING || input_type == PrimitiveType::TYPE_VARCHAR ||
            input_type == PrimitiveType::TYPE_CHAR) {
            return _execute_text_embed(context, block, result, input_rows_count, config, adapter,
                                       input_column, std::move(result_null_map));
        }
        return Status::InvalidArgument(
                "Function EMBED expects the second argument to be STRING or JSON, but got type {}",
                input.type->get_name());
    }

    static FunctionPtr create() { return std::make_shared<FunctionEmbed>(); }

private:
    static int32_t _get_embed_max_batch_size(FunctionContext* context) {
        QueryContext* query_ctx = context->state()->get_query_ctx();
        DORIS_CHECK(query_ctx != nullptr);

        return query_ctx->query_options().embed_max_batch_size;
    }

    Status _execute_text_embed(FunctionContext* context, Block& block, uint32_t result,
                               size_t input_rows_count, const TAIResource& config,
                               std::shared_ptr<AIAdapter>& adapter, const ColumnPtr& input_column,
                               ColumnUInt8::MutablePtr result_null_map) const {
        auto col_result = ColumnArray::create(
                ColumnNullable::create(ColumnFloat32::create(), ColumnUInt8::create()));
        std::vector<std::string> batch_prompts;
        size_t current_batch_size = 0;
        const int32_t max_batch_size = _get_embed_max_batch_size(context);
        const size_t max_context_window_size =
                static_cast<size_t>(get_ai_context_window_size(context));
        const NullMap* null_map = result_null_map ? &result_null_map->get_data() : nullptr;
        const Columns prompt_columns {input_column};

        for (size_t i = 0; i < input_rows_count; ++i) {
            if (null_map && (*null_map)[i]) {
                continue;
            }

            std::string prompt;
            RETURN_IF_ERROR(build_prompt(prompt_columns, i, prompt));

            const size_t prompt_size = prompt.size();

            if (prompt_size > max_context_window_size) {
                // flush history batch
                RETURN_IF_ERROR(_flush_text_embedding_batch(batch_prompts, *col_result, config,
                                                            adapter, context));
                current_batch_size = 0;

                batch_prompts.emplace_back(std::move(prompt));
                RETURN_IF_ERROR(_flush_text_embedding_batch(batch_prompts, *col_result, config,
                                                            adapter, context));
                continue;
            }

            if (!batch_prompts.empty() &&
                (current_batch_size + prompt_size > max_context_window_size ||
                 batch_prompts.size() >= static_cast<size_t>(max_batch_size))) {
                RETURN_IF_ERROR(_flush_text_embedding_batch(batch_prompts, *col_result, config,
                                                            adapter, context));
                current_batch_size = 0;
            }

            batch_prompts.emplace_back(std::move(prompt));
            current_batch_size += prompt_size;
        }

        RETURN_IF_ERROR(
                _flush_text_embedding_batch(batch_prompts, *col_result, config, adapter, context));

        block.replace_by_position(result, _expand_and_wrap_nullable_result(
                                                  std::move(col_result), std::move(result_null_map),
                                                  input_rows_count));
        return Status::OK();
    }

    Status _execute_multimodal_embed(FunctionContext* context, Block& block, uint32_t result,
                                     size_t input_rows_count, const TAIResource& config,
                                     std::shared_ptr<AIAdapter>& adapter,
                                     const ColumnPtr& input_column,
                                     ColumnUInt8::MutablePtr result_null_map) const {
        auto col_result = ColumnArray::create(
                ColumnNullable::create(ColumnFloat32::create(), ColumnUInt8::create()));
        std::vector<MultimodalType> batch_media_types;
        std::vector<std::string> batch_media_content_types;
        std::vector<std::string> batch_media_urls;
        const NullMap* null_map = result_null_map ? &result_null_map->get_data() : nullptr;

        int64_t ttl_seconds = 3600;
        QueryContext* query_ctx = context->state()->get_query_ctx();
        if (query_ctx && query_ctx->query_options().__isset.file_presigned_url_ttl_seconds) {
            ttl_seconds = query_ctx->query_options().file_presigned_url_ttl_seconds;
            if (ttl_seconds <= 0) {
                ttl_seconds = 3600;
            }
        }

        const int32_t max_batch_size = _get_embed_max_batch_size(context);

        for (size_t i = 0; i < input_rows_count; ++i) {
            if (null_map && (*null_map)[i]) {
                continue;
            }

            rapidjson::Document file_input;
            RETURN_IF_ERROR(_parse_file_input(*input_column, i, file_input));

            std::string content_type;
            MultimodalType media_type;
            RETURN_IF_ERROR(_infer_media_type(file_input, content_type, media_type));

            std::string media_url;
            RETURN_IF_ERROR(_resolve_media_url(file_input, ttl_seconds, media_url));

            if (!batch_media_urls.empty() &&
                batch_media_urls.size() >= static_cast<size_t>(max_batch_size)) {
                RETURN_IF_ERROR(_flush_multimodal_embedding_batch(
                        batch_media_types, batch_media_content_types, batch_media_urls, *col_result,
                        config, adapter, context));
            }

            batch_media_types.emplace_back(media_type);
            batch_media_content_types.emplace_back(std::move(content_type));
            batch_media_urls.emplace_back(std::move(media_url));
        }

        RETURN_IF_ERROR(_flush_multimodal_embedding_batch(
                batch_media_types, batch_media_content_types, batch_media_urls, *col_result, config,
                adapter, context));

        block.replace_by_position(result, _expand_and_wrap_nullable_result(
                                                  std::move(col_result), std::move(result_null_map),
                                                  input_rows_count));
        return Status::OK();
    }

    // EMBED-private helper.
    // Sends one embedding request with a prebuilt request body and validates returned row count.
    Status _execute_prebuilt_embedding_request(const std::string& request_body,
                                               std::vector<std::vector<float>>& results,
                                               size_t expected_size, const TAIResource& config,
                                               std::shared_ptr<AIAdapter>& adapter,
                                               FunctionContext* context) const {
        std::string response;
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

    // EMBED-private helper.
    // Flushes one accumulated text embedding batch into the output array column.
    Status _flush_text_embedding_batch(std::vector<std::string>& batch_prompts,
                                       ColumnArray& col_result, const TAIResource& config,
                                       std::shared_ptr<AIAdapter>& adapter,
                                       FunctionContext* context) const {
        if (batch_prompts.empty()) {
            return Status::OK();
        }

        std::string request_body;
        RETURN_IF_ERROR(adapter->build_embedding_request(batch_prompts, request_body));
        std::vector<std::vector<float>> batch_results;
        RETURN_IF_ERROR(_execute_prebuilt_embedding_request(
                request_body, batch_results, batch_prompts.size(), config, adapter, context));
        for (const auto& batch_result : batch_results) {
            _insert_embedding_result(col_result, batch_result);
        }
        batch_prompts.clear();
        return Status::OK();
    }

    // EMBED-private helper.
    // Flushes one accumulated multimodal embedding batch into the output array column.
    Status _flush_multimodal_embedding_batch(std::vector<MultimodalType>& batch_media_types,
                                             std::vector<std::string>& batch_media_content_types,
                                             std::vector<std::string>& batch_media_urls,
                                             ColumnArray& col_result, const TAIResource& config,
                                             std::shared_ptr<AIAdapter>& adapter,
                                             FunctionContext* context) const {
        if (batch_media_urls.empty()) {
            return Status::OK();
        }

        std::string request_body;
        RETURN_IF_ERROR(adapter->build_multimodal_embedding_request(
                batch_media_types, batch_media_urls, batch_media_content_types, request_body));

        std::vector<std::vector<float>> batch_results;
        RETURN_IF_ERROR(_execute_prebuilt_embedding_request(
                request_body, batch_results, batch_media_urls.size(), config, adapter, context));
        for (const auto& batch_result : batch_results) {
            _insert_embedding_result(col_result, batch_result);
        }
        batch_media_types.clear();
        batch_media_content_types.clear();
        batch_media_urls.clear();
        return Status::OK();
    }

    static void _insert_embedding_result(ColumnArray& col_array,
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

    static ColumnPtr _expand_and_wrap_nullable_result(ColumnArray::MutablePtr result,
                                                      ColumnUInt8::MutablePtr result_null_map,
                                                      size_t input_rows_count) {
        if (!result_null_map) {
            return result;
        }

        auto& offsets = result->get_offsets();
        size_t compact_row = offsets.size();
        offsets.resize(input_rows_count);
        // For example, embedding rows 1 and 3 produces compact offsets [5, 10]. Given
        // result_null_map [1, 0, 1, 0, 1], expand them to [0, 5, 5, 10, 10], where NULL rows
        // reuse the previous offset. Fill backwards to avoid overwriting unread compact offsets.
        for (size_t row = input_rows_count; row-- > 0;) {
            if (result_null_map->get_data()[row]) {
                offsets[row] = compact_row == 0 ? 0 : offsets[compact_row - 1];
            } else {
                offsets[row] = offsets[--compact_row];
            }
        }
        return ColumnNullable::create(std::move(result), std::move(result_null_map));
    }

    static bool _starts_with_ignore_case(std::string_view s, std::string_view prefix) {
        if (s.size() < prefix.size()) {
            return false;
        }
        return std::equal(prefix.begin(), prefix.end(), s.begin(), [](char a, char b) {
            return std::tolower(static_cast<unsigned char>(a)) ==
                   std::tolower(static_cast<unsigned char>(b));
        });
    }

    static Status _infer_media_type(const rapidjson::Value& file_input, std::string& content_type,
                                    MultimodalType& media_type) {
        RETURN_IF_ERROR(_get_required_string_field(file_input, "content_type", content_type));

        if (_starts_with_ignore_case(content_type, "image/")) {
            media_type = MultimodalType::IMAGE;
            return Status::OK();
        } else if (_starts_with_ignore_case(content_type, "video/")) {
            media_type = MultimodalType::VIDEO;
            return Status::OK();
        } else if (_starts_with_ignore_case(content_type, "audio/")) {
            media_type = MultimodalType::AUDIO;
            return Status::OK();
        }

        return Status::InvalidArgument("Unsupported content_type for EMBED: {}", content_type);
    }

    // Parse the FILE-like JSONB argument into a JSON object for downstream field reads.
    static Status _parse_file_input(const IColumn& file_column, size_t row_num,
                                    rapidjson::Document& file_input) {
        StringRef file_ref = file_column.get_data_at(row_num);
        std::string file_json = JsonbToJson::jsonb_to_json_string(file_ref.data, file_ref.size);
        file_input.Parse(file_json.c_str());
        DORIS_CHECK(!file_input.HasParseError() && file_input.IsObject());
        return Status::OK();
    }

    // TODO(lzq): After support FILE type, We should use the interface provided by FILE to get the fields
    // replacing this function
    static Status _get_required_string_field(const rapidjson::Value& obj, const char* field_name,
                                             std::string& value) {
        auto iter = obj.FindMember(field_name);
        if (iter == obj.MemberEnd() || !iter->value.IsString()) {
            return Status::InvalidArgument(
                    "EMBED file json field '{}' is required and must be a string", field_name);
        }
        value = iter->value.GetString();
        if (value.empty()) {
            return Status::InvalidArgument("EMBED file json field '{}' can not be empty",
                                           field_name);
        }
        return Status::OK();
    }

    static Status init_s3_client_conf_from_json(const rapidjson::Value& file_input,
                                                S3ClientConf& s3_client_conf) {
        std::string endpoint;
        RETURN_IF_ERROR(_get_required_string_field(file_input, "endpoint", endpoint));
        std::string region;
        RETURN_IF_ERROR(_get_required_string_field(file_input, "region", region));

        auto get_optional_string_field = [&](const char* field_name, std::string& value) {
            auto iter = file_input.FindMember(field_name);
            if (iter == file_input.MemberEnd() || iter->value.IsNull()) {
                return;
            }
            DORIS_CHECK(iter->value.IsString());
            value = iter->value.GetString();
        };

        get_optional_string_field("ak", s3_client_conf.ak);
        get_optional_string_field("sk", s3_client_conf.sk);
        get_optional_string_field("role_arn", s3_client_conf.role_arn);
        get_optional_string_field("external_id", s3_client_conf.external_id);
        s3_client_conf.endpoint = endpoint;
        s3_client_conf.region = region;

        return Status::OK();
    }

    Status _resolve_media_url(const rapidjson::Value& file_input, int64_t ttl_seconds,
                              std::string& media_url) const {
        std::string uri;
        RETURN_IF_ERROR(_get_required_string_field(file_input, "uri", uri));

        // If it's a direct http/https URL, use it as-is
        if (_starts_with_ignore_case(uri, "http://") || _starts_with_ignore_case(uri, "https://")) {
            media_url = uri;
            return Status::OK();
        }

        S3ClientConf s3_client_conf;
        RETURN_IF_ERROR(init_s3_client_conf_from_json(file_input, s3_client_conf));
        auto s3_client = DORIS_TRY(S3ClientFactory::instance().create(s3_client_conf));

        S3URI s3_uri(uri);
        RETURN_IF_ERROR(s3_uri.parse());
        std::string bucket = s3_uri.get_bucket();
        std::string key = s3_uri.get_key();
        DORIS_CHECK(!bucket.empty() && !key.empty());
        media_url = s3_client->generate_presigned_url({.bucket = bucket, .key = key, .prefix = ""},
                                                      ttl_seconds);
        return Status::OK();
    }
};

}; // namespace doris
