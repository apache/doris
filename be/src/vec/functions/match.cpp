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

#include "vec/functions/match.h"

#include "olap/rowset/segment_v2/inverted_index_reader.h"
#include "runtime/query_context.h"
#include "runtime/runtime_state.h"

namespace doris::vectorized {

Status FunctionMatchBase::execute_impl(FunctionContext* context, Block& block,
                                       const ColumnNumbers& arguments, size_t result,
                                       size_t input_rows_count) {
    auto match_query_str = block.get_by_position(arguments[1]).to_string(0);
    std::string column_name = block.get_by_position(arguments[0]).name;
    auto match_pred_column_name =
            BeConsts::BLOCK_TEMP_COLUMN_PREFIX + column_name + "_match_" + match_query_str;
    if (!block.has(match_pred_column_name)) {
        VLOG_DEBUG << "begin to execute match directly, column_name=" << column_name
                   << ", match_query_str=" << match_query_str;
        InvertedIndexCtx* inverted_index_ctx = reinterpret_cast<InvertedIndexCtx*>(
                context->get_function_state(FunctionContext::THREAD_LOCAL));

        const auto values_col =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        const auto* values = check_and_get_column<ColumnString>(values_col.get());
        if (!values) {
            return Status::InternalError("Not supported input arguments types");
        }
        // result column
        auto res = ColumnUInt8::create();
        ColumnUInt8::Container& vec_res = res->get_data();
        // set default value to 0, and match functions only need to set 1/true
        vec_res.resize_fill(input_rows_count);
        RETURN_IF_ERROR(execute_match(column_name, match_query_str, input_rows_count, values,
                                      inverted_index_ctx, vec_res));
        block.replace_by_position(result, std::move(res));
    } else {
        auto match_pred_column =
                block.get_by_name(match_pred_column_name).column->convert_to_full_column_if_const();
        block.replace_by_position(result, std::move(match_pred_column));
    }

    return Status::OK();
}

Status FunctionMatchAny::execute_match(const std::string& column_name,
                                       const std::string& match_query_str, size_t input_rows_count,
                                       const ColumnString* datas,
                                       InvertedIndexCtx* inverted_index_ctx,
                                       ColumnUInt8::Container& result) {
    doris::InvertedIndexParserType parser_type = doris::InvertedIndexParserType::PARSER_UNKNOWN;
    if (inverted_index_ctx) {
        parser_type = inverted_index_ctx->parser_type;
    }
    VLOG_DEBUG << "begin to run FunctionMatchAny::execute_match, parser_type: "
               << inverted_index_parser_type_to_string(parser_type);
    std::vector<std::wstring> query_tokens =
            doris::segment_v2::InvertedIndexReader::get_analyse_result(
                    column_name, match_query_str,
                    doris::segment_v2::InvertedIndexQueryType::MATCH_ANY_QUERY, inverted_index_ctx);
    for (int i = 0; i < input_rows_count; i++) {
        const auto& str_ref = datas->get_data_at(i);
        std::vector<std::wstring> data_tokens =
                doris::segment_v2::InvertedIndexReader::get_analyse_result(
                        column_name, str_ref.to_string(),
                        doris::segment_v2::InvertedIndexQueryType::MATCH_ANY_QUERY,
                        inverted_index_ctx);
        // TODO: more efficient impl
        for (auto& token : query_tokens) {
            auto it = std::find(data_tokens.begin(), data_tokens.end(), token);
            if (it != data_tokens.end()) {
                result[i] = true;
                break;
            }
        }
    }

    return Status::OK();
}

Status FunctionMatchAll::execute_match(const std::string& column_name,
                                       const std::string& match_query_str, size_t input_rows_count,
                                       const ColumnString* datas,
                                       InvertedIndexCtx* inverted_index_ctx,
                                       ColumnUInt8::Container& result) {
    doris::InvertedIndexParserType parser_type = doris::InvertedIndexParserType::PARSER_UNKNOWN;
    if (inverted_index_ctx) {
        parser_type = inverted_index_ctx->parser_type;
    }
    VLOG_DEBUG << "begin to run FunctionMatchAll::execute_match, parser_type: "
               << inverted_index_parser_type_to_string(parser_type);
    std::vector<std::wstring> query_tokens =
            doris::segment_v2::InvertedIndexReader::get_analyse_result(
                    column_name, match_query_str,
                    doris::segment_v2::InvertedIndexQueryType::MATCH_ALL_QUERY, inverted_index_ctx);

    for (int i = 0; i < input_rows_count; i++) {
        const auto& str_ref = datas->get_data_at(i);
        std::vector<std::wstring> data_tokens =
                doris::segment_v2::InvertedIndexReader::get_analyse_result(
                        column_name, str_ref.to_string(),
                        doris::segment_v2::InvertedIndexQueryType::MATCH_ALL_QUERY,
                        inverted_index_ctx);
        // TODO: more efficient impl
        auto find_count = 0;
        for (auto& token : query_tokens) {
            auto it = std::find(data_tokens.begin(), data_tokens.end(), token);
            if (it != data_tokens.end()) {
                ++find_count;
            } else {
                break;
            }
        }

        if (find_count == query_tokens.size()) {
            result[i] = true;
        }
    }

    return Status::OK();
}

Status FunctionMatchPhrase::execute_match(const std::string& column_name,
                                          const std::string& match_query_str,
                                          size_t input_rows_count, const ColumnString* datas,
                                          InvertedIndexCtx* inverted_index_ctx,
                                          ColumnUInt8::Container& result) {
    doris::InvertedIndexParserType parser_type = doris::InvertedIndexParserType::PARSER_UNKNOWN;
    if (inverted_index_ctx) {
        parser_type = inverted_index_ctx->parser_type;
    }
    VLOG_DEBUG << "begin to run FunctionMatchPhrase::execute_match, parser_type: "
               << inverted_index_parser_type_to_string(parser_type);
    std::vector<std::wstring> query_tokens =
            doris::segment_v2::InvertedIndexReader::get_analyse_result(
                    column_name, match_query_str,
                    doris::segment_v2::InvertedIndexQueryType::MATCH_PHRASE_QUERY,
                    inverted_index_ctx);

    for (int i = 0; i < input_rows_count; i++) {
        const auto& str_ref = datas->get_data_at(i);
        std::vector<std::wstring> data_tokens =
                doris::segment_v2::InvertedIndexReader::get_analyse_result(
                        column_name, str_ref.to_string(),
                        doris::segment_v2::InvertedIndexQueryType::MATCH_PHRASE_QUERY,
                        inverted_index_ctx);
        // TODO: more efficient impl
        bool matched = false;
        auto it = data_tokens.begin();
        while (it != data_tokens.end()) {
            // find position of first token
            it = std::find(it, data_tokens.end(), query_tokens[0]);
            if (it != data_tokens.end()) {
                matched = true;
                it++;
                auto it_more = it;
                // compare query_tokens after the first to data_tokens one by one
                for (size_t idx = 1; idx < query_tokens.size(); idx++) {
                    if (it_more == data_tokens.end() || *it_more != query_tokens[idx]) {
                        matched = false;
                    }
                    it_more++;
                }
                if (matched) {
                    break;
                }
            }
        }

        // check matched
        if (matched) {
            result[i] = true;
        }
    }

    return Status::OK();
}

void register_function_match(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionMatchAny>();
    factory.register_function<FunctionMatchAll>();
    factory.register_function<FunctionMatchPhrase>();
    factory.register_function<FunctionMatchElementEQ>();
    factory.register_function<FunctionMatchElementLT>();
    factory.register_function<FunctionMatchElementGT>();
    factory.register_function<FunctionMatchElementLE>();
    factory.register_function<FunctionMatchElementGE>();
}

} // namespace doris::vectorized
