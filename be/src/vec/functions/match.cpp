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

Status FunctionMatchBase::execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                    size_t result, size_t input_rows_count) {
    auto match_query_str = block.get_by_position(arguments[1]).to_string(0);
    std::string column_name = block.get_by_position(arguments[0]).name;
    auto match_pred_column_name =
            BeConsts::BLOCK_TEMP_COLUMN_PREFIX + column_name + "_match_" + match_query_str;
    if (!block.has(match_pred_column_name)) {
        LOG(INFO) << "begin to execute match directly, column_name=" << column_name
                << ", match_query_str=" << match_query_str;
        doris::InvertedIndexParserType parser_type = InvertedIndexParserType::PARSER_UNKNOWN;
        if (context) {
            RuntimeState* state = context->state();
            DCHECK(nullptr != state);
            parser_type = state->get_query_ctx()->get_inverted_index_parser(column_name);
        }

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
        RETURN_IF_ERROR(execute_match(column_name, match_query_str,
                                input_rows_count, values, parser_type, vec_res));
        block.replace_by_position(result, std::move(res));
    } else {
        auto match_pred_column =
                block.get_by_name(match_pred_column_name).column->convert_to_full_column_if_const();
        block.replace_by_position(result, std::move(match_pred_column));
    }

    return Status::OK();
}

std::vector<std::string> FunctionMatchBase::vectors_intersection(
        std::vector<std::string>& v1, std::vector<std::string>& v2) {
    std::vector<std::string> result;
    std::sort(v1.begin(), v1.end());
    std::sort(v2.begin(), v2.end());
    std::set_intersection(v1.begin(), v1.end(), v2.begin(), v2.end(), std::back_inserter(result));
    return result;
}

bool FunctionMatchBase::is_equal_vectors(std::vector<std::string>& v1, std::vector<std::string>& v2) {
    if (v1.empty() || v2.empty()) {
        return false;
    }

    if (v1.size() != v2.size()) {
        return false;
    }

    return std::is_permutation(v1.begin(), v1.end(), v2.begin());
}

bool FunctionMatchBase::is_subset_vectors(std::vector<std::string>& v1, std::vector<std::string>& v2) {
    auto vec_inter = vectors_intersection(v1, v2);
    return is_equal_vectors(vec_inter, v2);
}

Status FunctionMatchAny::execute_match(const std::string& column_name,
                        const std::string& match_query_str,
                        size_t input_rows_count,
                        const ColumnString* query_values,
                        doris::InvertedIndexParserType parser_type,
                        ColumnUInt8::Container& result) {
    LOG(INFO) << "begin to run FunctionMatchAny::execute_match";
    std::vector<std::string> tokens =
                doris::segment_v2::InvertedIndexReader::get_analyse_result(
                column_name, match_query_str, doris::segment_v2::InvertedIndexQueryType::MATCH_ANY_QUERY, parser_type);
    for (int i = 0; i < input_rows_count; i++) {
        const auto& str_ref = query_values->get_data_at(i);
        std::vector<std::string> values =
                doris::segment_v2::InvertedIndexReader::get_analyse_result(
                column_name, str_ref.to_string(), doris::segment_v2::InvertedIndexQueryType::MATCH_ANY_QUERY, parser_type);
        auto vec_inter = vectors_intersection(values, tokens);
        if (is_subset_vectors(tokens, vec_inter)) {
            result[i] = true;
        }
    }

    return Status::OK();
}

Status FunctionMatchAll::execute_match(const std::string& column_name,
                        const std::string& match_query_str,
                        size_t input_rows_count,
                        const ColumnString* query_values,
                        doris::InvertedIndexParserType parser_type,
                        ColumnUInt8::Container& result) {
    LOG(INFO) << "begin to run FunctionMatchAll::execute_match";
    std::vector<std::string> tokens =
                doris::segment_v2::InvertedIndexReader::get_analyse_result(
                column_name, match_query_str, doris::segment_v2::InvertedIndexQueryType::MATCH_ALL_QUERY, parser_type);

    for (int i = 0; i < input_rows_count; i++) {
        const auto& str_ref = query_values->get_data_at(i);
        std::vector<std::string> values =
                doris::segment_v2::InvertedIndexReader::get_analyse_result(
                column_name, str_ref.to_string(), doris::segment_v2::InvertedIndexQueryType::MATCH_ALL_QUERY, parser_type);
        auto vec_inter = vectors_intersection(values, tokens);
        if (is_equal_vectors(tokens, vec_inter)) {
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
