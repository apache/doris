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

#include "runtime/query_context.h"
#include "runtime/runtime_state.h"

namespace doris::vectorized {

Status FunctionMatchBase::execute_impl(FunctionContext* context, Block& block,
                                       const ColumnNumbers& arguments, size_t result,
                                       size_t input_rows_count) const {
    ColumnPtr& column_ptr = block.get_by_position(arguments[1]).column;
    DataTypePtr& type_ptr = block.get_by_position(arguments[1]).type;
    auto match_query_str = type_ptr->to_string(*column_ptr, 0);
    std::string column_name = block.get_by_position(arguments[0]).name;
    auto match_pred_column_name =
            BeConsts::BLOCK_TEMP_COLUMN_PREFIX + column_name + "_match_" + match_query_str;
    if (!block.has(match_pred_column_name)) {
        VLOG_DEBUG << "begin to execute match directly, column_name=" << column_name
                   << ", match_query_str=" << match_query_str;
        InvertedIndexCtx* inverted_index_ctx = reinterpret_cast<InvertedIndexCtx*>(
                context->get_function_state(FunctionContext::THREAD_LOCAL));
        if (inverted_index_ctx == nullptr) {
            inverted_index_ctx = reinterpret_cast<InvertedIndexCtx*>(
                    context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
        }

        const ColumnPtr source_col =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        const auto* values = check_and_get_column<ColumnString>(source_col.get());
        const ColumnArray* array_col = nullptr;
        if (source_col->is_column_array()) {
            array_col = check_and_get_column<ColumnArray>(source_col.get());
            if (array_col && !array_col->get_data().is_column_string()) {
                return Status::NotSupported(
                        fmt::format("unsupported nested array of type {} for function {}",
                                    is_column_nullable(array_col->get_data())
                                            ? array_col->get_data().get_name()
                                            : array_col->get_data().get_family_name(),
                                    get_name()));
            }

            if (is_column_nullable(array_col->get_data())) {
                const auto& array_nested_null_column =
                        reinterpret_cast<const ColumnNullable&>(array_col->get_data());
                values = check_and_get_column<ColumnString>(
                        *(array_nested_null_column.get_nested_column_ptr()));
            } else {
                values = check_and_get_column<ColumnString>(*(array_col->get_data_ptr()));
            }
        } else if (auto* nullable = check_and_get_column<ColumnNullable>(source_col.get())) {
            values = check_and_get_column<ColumnString>(*nullable->get_nested_column_ptr());
        }

        if (!values) {
            LOG(WARNING) << "Illegal column " << source_col->get_name();
            return Status::InternalError("Not supported input column types");
        }
        // result column
        auto res = ColumnUInt8::create();
        ColumnUInt8::Container& vec_res = res->get_data();
        // set default value to 0, and match functions only need to set 1/true
        vec_res.resize_fill(input_rows_count);
        RETURN_IF_ERROR(execute_match(
                column_name, match_query_str, input_rows_count, values, inverted_index_ctx,
                (array_col ? &(array_col->get_offsets()) : nullptr), vec_res));
        block.replace_by_position(result, std::move(res));
    } else {
        auto match_pred_column =
                block.get_by_name(match_pred_column_name).column->convert_to_full_column_if_const();
        block.replace_by_position(result, std::move(match_pred_column));
    }

    return Status::OK();
}

inline doris::segment_v2::InvertedIndexQueryType FunctionMatchBase::get_query_type_from_fn_name()
        const {
    std::string fn_name = get_name();
    if (fn_name == MATCH_ANY_FUNCTION) {
        return doris::segment_v2::InvertedIndexQueryType::MATCH_ANY_QUERY;
    } else if (fn_name == MATCH_ALL_FUNCTION) {
        return doris::segment_v2::InvertedIndexQueryType::MATCH_ALL_QUERY;
    } else if (fn_name == MATCH_PHRASE_FUNCTION) {
        return doris::segment_v2::InvertedIndexQueryType::MATCH_PHRASE_QUERY;
    }
    return doris::segment_v2::InvertedIndexQueryType::UNKNOWN_QUERY;
}

inline std::vector<std::string> FunctionMatchBase::analyse_data_token(
        const std::string& column_name, InvertedIndexCtx* inverted_index_ctx,
        const ColumnString* string_col, int32_t current_block_row_idx,
        const ColumnArray::Offsets64* array_offsets, int32_t& current_src_array_offset) const {
    std::vector<std::string> data_tokens;
    auto query_type = get_query_type_from_fn_name();
    if (array_offsets) {
        for (auto next_src_array_offset = (*array_offsets)[current_block_row_idx];
             current_src_array_offset < next_src_array_offset; ++current_src_array_offset) {
            const auto& str_ref = string_col->get_data_at(current_src_array_offset);
            auto reader = doris::segment_v2::InvertedIndexReader::create_reader(
                    inverted_index_ctx, str_ref.to_string());

            std::vector<std::string> element_tokens =
                    doris::segment_v2::InvertedIndexReader::get_analyse_result(
                            reader.get(), inverted_index_ctx->analyzer, column_name, query_type,
                            false);
            data_tokens.insert(data_tokens.end(), element_tokens.begin(), element_tokens.end());
        }
    } else {
        const auto& str_ref = string_col->get_data_at(current_block_row_idx);
        auto reader = doris::segment_v2::InvertedIndexReader::create_reader(inverted_index_ctx,
                                                                            str_ref.to_string());

        data_tokens = doris::segment_v2::InvertedIndexReader::get_analyse_result(
                reader.get(), inverted_index_ctx->analyzer, column_name, query_type, false);
    }
    return data_tokens;
}

Status FunctionMatchAny::execute_match(const std::string& column_name,
                                       const std::string& match_query_str, size_t input_rows_count,
                                       const ColumnString* string_col,
                                       InvertedIndexCtx* inverted_index_ctx,
                                       const ColumnArray::Offsets64* array_offsets,
                                       ColumnUInt8::Container& result) const {
    doris::InvertedIndexParserType parser_type = doris::InvertedIndexParserType::PARSER_UNKNOWN;
    if (inverted_index_ctx) {
        parser_type = inverted_index_ctx->parser_type;
    }
    VLOG_DEBUG << "begin to run FunctionMatchAny::execute_match, parser_type: "
               << inverted_index_parser_type_to_string(parser_type);
    auto reader = doris::segment_v2::InvertedIndexReader::create_reader(inverted_index_ctx,
                                                                        match_query_str);
    std::vector<std::string> query_tokens =
            doris::segment_v2::InvertedIndexReader::get_analyse_result(
                    reader.get(), inverted_index_ctx->analyzer, column_name,
                    doris::segment_v2::InvertedIndexQueryType::MATCH_ANY_QUERY);
    if (query_tokens.empty()) {
        LOG(WARNING) << fmt::format(
                "token parser result is empty for query, "
                "please check your query: '{}' and index parser: '{}'",
                match_query_str, inverted_index_parser_type_to_string(parser_type));
        return Status::OK();
    }

    auto current_src_array_offset = 0;
    for (int i = 0; i < input_rows_count; i++) {
        std::vector<std::string> data_tokens =
                analyse_data_token(column_name, inverted_index_ctx, string_col, i, array_offsets,
                                   current_src_array_offset);

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
                                       const ColumnString* string_col,
                                       InvertedIndexCtx* inverted_index_ctx,
                                       const ColumnArray::Offsets64* array_offsets,
                                       ColumnUInt8::Container& result) const {
    doris::InvertedIndexParserType parser_type = doris::InvertedIndexParserType::PARSER_UNKNOWN;
    if (inverted_index_ctx) {
        parser_type = inverted_index_ctx->parser_type;
    }
    VLOG_DEBUG << "begin to run FunctionMatchAll::execute_match, parser_type: "
               << inverted_index_parser_type_to_string(parser_type);
    auto reader = doris::segment_v2::InvertedIndexReader::create_reader(inverted_index_ctx,
                                                                        match_query_str);
    std::vector<std::string> query_tokens =
            doris::segment_v2::InvertedIndexReader::get_analyse_result(
                    reader.get(), inverted_index_ctx->analyzer, column_name,
                    doris::segment_v2::InvertedIndexQueryType::MATCH_ALL_QUERY);
    if (query_tokens.empty()) {
        LOG(WARNING) << fmt::format(
                "token parser result is empty for query, "
                "please check your query: '{}' and index parser: '{}'",
                match_query_str, inverted_index_parser_type_to_string(parser_type));
        return Status::OK();
    }

    auto current_src_array_offset = 0;
    for (int i = 0; i < input_rows_count; i++) {
        std::vector<std::string> data_tokens =
                analyse_data_token(column_name, inverted_index_ctx, string_col, i, array_offsets,
                                   current_src_array_offset);

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
                                          size_t input_rows_count, const ColumnString* string_col,
                                          InvertedIndexCtx* inverted_index_ctx,
                                          const ColumnArray::Offsets64* array_offsets,
                                          ColumnUInt8::Container& result) const {
    doris::InvertedIndexParserType parser_type = doris::InvertedIndexParserType::PARSER_UNKNOWN;
    if (inverted_index_ctx) {
        parser_type = inverted_index_ctx->parser_type;
    }
    VLOG_DEBUG << "begin to run FunctionMatchPhrase::execute_match, parser_type: "
               << inverted_index_parser_type_to_string(parser_type);
    auto reader = doris::segment_v2::InvertedIndexReader::create_reader(inverted_index_ctx,
                                                                        match_query_str);
    std::vector<std::string> query_tokens =
            doris::segment_v2::InvertedIndexReader::get_analyse_result(
                    reader.get(), inverted_index_ctx->analyzer, column_name,
                    doris::segment_v2::InvertedIndexQueryType::MATCH_PHRASE_QUERY);
    if (query_tokens.empty()) {
        LOG(WARNING) << fmt::format(
                "token parser result is empty for query, "
                "please check your query: '{}' and index parser: '{}'",
                match_query_str, inverted_index_parser_type_to_string(parser_type));
        return Status::OK();
    }

    auto current_src_array_offset = 0;
    for (int i = 0; i < input_rows_count; i++) {
        std::vector<std::string> data_tokens =
                analyse_data_token(column_name, inverted_index_ctx, string_col, i, array_offsets,
                                   current_src_array_offset);

        // TODO: more efficient impl
        bool matched = false;
        auto data_it = data_tokens.begin();
        while (data_it != data_tokens.end()) {
            // find position of first token
            data_it = std::find(data_it, data_tokens.end(), query_tokens[0]);
            if (data_it != data_tokens.end()) {
                matched = true;
                auto data_it_next = ++data_it;
                auto query_it = query_tokens.begin() + 1;
                // compare query_tokens after the first to data_tokens one by one
                while (query_it != query_tokens.end()) {
                    if (data_it_next == data_tokens.end() || *data_it_next != *query_it) {
                        matched = false;
                        break;
                    }
                    query_it++;
                    data_it_next++;
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
