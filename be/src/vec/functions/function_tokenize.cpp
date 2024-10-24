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

#include "vec/functions/function_tokenize.h"

#include <glog/logging.h>

#include <algorithm>
#include <boost/regex.hpp>
#include <utility>

#include "CLucene/StdHeader.h"
#include "CLucene/config/repl_wchar.h"
#include "olap/inverted_index_parser.h"
#include "olap/rowset/segment_v2/inverted_index/analyzer/analyzer.h"
#include "olap/rowset/segment_v2/inverted_index_reader.h"
#include "vec/columns/column.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"

namespace doris::vectorized {

Status parse(const std::string& str, std::map<std::string, std::string>& result) {
    boost::regex pattern(
            R"delimiter((?:'([^']*)'|"([^"]*)"|([^, ]*))\s*=\s*(?:'([^']*)'|"([^"]*)"|([^, ]*)))delimiter");
    boost::smatch matches;

    std::string::const_iterator searchStart(str.cbegin());
    while (boost::regex_search(searchStart, str.cend(), matches, pattern)) {
        std::string key = matches[1].length()
                                  ? matches[1].str()
                                  : (matches[2].length() ? matches[2].str() : matches[3].str());
        std::string value = matches[4].length()
                                    ? matches[4].str()
                                    : (matches[5].length() ? matches[5].str() : matches[6].str());

        result[key] = value;

        searchStart = matches.suffix().first;
    }

    return Status::OK();
}

void FunctionTokenize::_do_tokenize(const ColumnString& src_column_string,
                                    InvertedIndexCtx& inverted_index_ctx,
                                    IColumn& dest_nested_column,
                                    ColumnArray::Offsets64& dest_offsets,
                                    NullMapType* dest_nested_null_map) const {
    ColumnString& dest_column_string = reinterpret_cast<ColumnString&>(dest_nested_column);
    ColumnString::Chars& column_string_chars = dest_column_string.get_chars();
    ColumnString::Offsets& column_string_offsets = dest_column_string.get_offsets();
    column_string_chars.reserve(0);

    ColumnArray::Offset64 string_pos = 0;
    ColumnArray::Offset64 dest_pos = 0;
    ColumnArray::Offset64 src_offsets_size = src_column_string.get_offsets().size();

    for (size_t i = 0; i < src_offsets_size; i++) {
        const StringRef tokenize_str = src_column_string.get_data_at(i);

        if (tokenize_str.size == 0) {
            dest_offsets.push_back(dest_pos);
            continue;
        }
        auto reader = doris::segment_v2::inverted_index::InvertedIndexAnalyzer::create_reader(
                inverted_index_ctx.char_filter_map);
        reader->init(tokenize_str.data, tokenize_str.size, true);

        std::vector<std::string> query_tokens =
                doris::segment_v2::inverted_index::InvertedIndexAnalyzer::get_analyse_result(
                        reader.get(), inverted_index_ctx.analyzer, "tokenize",
                        doris::segment_v2::InvertedIndexQueryType::MATCH_PHRASE_QUERY);
        for (auto token : query_tokens) {
            const size_t old_size = column_string_chars.size();
            const size_t split_part_size = token.length();
            if (split_part_size > 0) {
                const size_t new_size = old_size + split_part_size;
                column_string_chars.resize(new_size);
                memcpy(column_string_chars.data() + old_size, token.data(), split_part_size);
                // add dist string offset
                string_pos += split_part_size;
            }
            column_string_offsets.push_back(string_pos);
            // not null
            (*dest_nested_null_map).push_back(false);
            // array offset + 1
            dest_pos++;
        }
        dest_offsets.push_back(dest_pos);
    }
}

Status FunctionTokenize::execute_impl(FunctionContext* /*context*/, Block& block,
                                      const ColumnNumbers& arguments, size_t result,
                                      size_t /*input_rows_count*/) const {
    DCHECK_EQ(arguments.size(), 2);
    const auto& [src_column, left_const] =
            unpack_if_const(block.get_by_position(arguments[0]).column);
    const auto& [right_column, right_const] =
            unpack_if_const(block.get_by_position(arguments[1]).column);

    DataTypePtr src_column_type = block.get_by_position(arguments[0]).type;
    auto dest_column_ptr = ColumnArray::create(make_nullable(src_column_type)->create_column(),
                                               ColumnArray::ColumnOffsets::create());

    IColumn* dest_nested_column = &dest_column_ptr->get_data();
    auto& dest_offsets = dest_column_ptr->get_offsets();
    DCHECK(dest_nested_column != nullptr);
    dest_nested_column->reserve(0);
    dest_offsets.reserve(0);

    NullMapType* dest_nested_null_map = nullptr;
    ColumnNullable* dest_nullable_col = reinterpret_cast<ColumnNullable*>(dest_nested_column);
    dest_nested_column = dest_nullable_col->get_nested_column_ptr();
    dest_nested_null_map = &dest_nullable_col->get_null_map_column().get_data();

    if (auto col_left = check_and_get_column<ColumnString>(src_column.get())) {
        if (auto col_right = check_and_get_column<ColumnString>(right_column.get())) {
            InvertedIndexCtx inverted_index_ctx;
            std::map<std::string, std::string> properties;
            auto st = parse(col_right->get_data_at(0).to_string(), properties);
            if (!st.ok()) {
                return st;
            }
            inverted_index_ctx.parser_type = get_inverted_index_parser_type_from_string(
                    get_parser_string_from_properties(properties));
            inverted_index_ctx.parser_mode = get_parser_mode_string_from_properties(properties);
            inverted_index_ctx.char_filter_map =
                    get_parser_char_filter_map_from_properties(properties);
            inverted_index_ctx.lower_case = get_parser_lowercase_from_properties(properties);
            inverted_index_ctx.stop_words = get_parser_stopwords_from_properties(properties);

            std::unique_ptr<lucene::analysis::Analyzer> analyzer;
            try {
                analyzer =
                        doris::segment_v2::inverted_index::InvertedIndexAnalyzer::create_analyzer(
                                &inverted_index_ctx);
            } catch (CLuceneError& e) {
                return Status::Error<doris::ErrorCode::INVERTED_INDEX_ANALYZER_ERROR>(
                        "inverted index create analyzer failed: {}", e.what());
            }

            inverted_index_ctx.analyzer = analyzer.get();
            _do_tokenize(*col_left, inverted_index_ctx, *dest_nested_column, dest_offsets,
                         dest_nested_null_map);

            block.replace_by_position(result, std::move(dest_column_ptr));
            return Status::OK();
        }
    }
    return Status::RuntimeError("unimplemented function {}", get_name());
}
} // namespace doris::vectorized
