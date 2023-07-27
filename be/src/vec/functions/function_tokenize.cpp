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
#include <utility>

#include "CLucene/StdHeader.h"
#include "CLucene/config/repl_wchar.h"
#include "olap/inverted_index_parser.h"
#include "olap/rowset/segment_v2/inverted_index_reader.h"
#include "vec/columns/column.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/field.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

void FunctionTokenize::_execute_constant(const ColumnString& src_column_string,
                                         const StringRef& tokenize_type,
                                         IColumn& dest_nested_column,
                                         ColumnArray::Offsets64& dest_offsets,
                                         NullMapType* dest_nested_null_map) {
    ColumnString& dest_column_string = reinterpret_cast<ColumnString&>(dest_nested_column);
    ColumnString::Chars& column_string_chars = dest_column_string.get_chars();
    ColumnString::Offsets& column_string_offsets = dest_column_string.get_offsets();
    column_string_chars.reserve(0);

    ColumnArray::Offset64 string_pos = 0;
    ColumnArray::Offset64 dest_pos = 0;
    ColumnArray::Offset64 src_offsets_size = src_column_string.get_offsets().size();

    InvertedIndexCtx inverted_index_ctx;
    auto parser_type = get_inverted_index_parser_type_from_string(tokenize_type.to_string());

    switch (parser_type) {
    case InvertedIndexParserType::PARSER_CHINESE: {
        // we don't support parse_mode params now, so make it default.
        inverted_index_ctx.parser_mode = INVERTED_INDEX_PARSER_COARSE_GRANULARITY;
        inverted_index_ctx.parser_type = parser_type;
        break;
    }
    case InvertedIndexParserType::PARSER_UNICODE: {
        inverted_index_ctx.parser_type = parser_type;
        break;
    }
    default:
        // default as english
        inverted_index_ctx.parser_type = InvertedIndexParserType::PARSER_ENGLISH;
    }

    for (size_t i = 0; i < src_offsets_size; i++) {
        const StringRef tokenize_str = src_column_string.get_data_at(i);

        if (tokenize_str.size == 0) {
            dest_offsets.push_back(dest_pos);
            continue;
        }
        std::vector<std::wstring> query_tokens =
                doris::segment_v2::InvertedIndexReader::get_analyse_result(
                        "tokenize", tokenize_str.to_string(),
                        doris::segment_v2::InvertedIndexQueryType::MATCH_PHRASE_QUERY,
                        &inverted_index_ctx);
        for (auto token_ws : query_tokens) {
            std::string token = lucene_wcstoutf8string(token_ws.data(), token_ws.length());
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
                                      size_t /*input_rows_count*/) {
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
            _execute_constant(*col_left, col_right->get_data_at(0), *dest_nested_column,
                              dest_offsets, dest_nested_null_map);

            block.replace_by_position(result, std::move(dest_column_ptr));
            return Status::OK();
        }
    }
    return Status::RuntimeError("unimplements function {}", get_name());
}
} // namespace doris::vectorized
