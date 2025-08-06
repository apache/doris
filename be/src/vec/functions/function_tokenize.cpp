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
#include <rapidjson/prettywriter.h>

#include <algorithm>
#include <boost/regex.hpp>
#include <memory>
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

using namespace doris::segment_v2::inverted_index;

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

void FunctionTokenize::_do_tokenize_none(const ColumnString& src_column_string,
                                         const MutableColumnPtr& dest_column_ptr) const {
    ColumnArray::Offset64 src_offsets_size = src_column_string.get_offsets().size();
    for (size_t i = 0; i < src_offsets_size; i++) {
        const StringRef tokenize_str = src_column_string.get_data_at(i);

        rapidjson::Document doc;
        doc.SetArray();
        rapidjson::Document::AllocatorType& allocator = doc.GetAllocator();

        rapidjson::Value obj(rapidjson::kObjectType);
        obj.AddMember(
                "token",
                rapidjson::Value(tokenize_str.data,
                                 static_cast<rapidjson::SizeType>(tokenize_str.size), allocator)
                        .Move(),
                allocator);
        doc.PushBack(obj, allocator);

        rapidjson::StringBuffer buffer;
        rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(buffer);
        writer.SetFormatOptions(rapidjson::kFormatSingleLineArray);
        doc.Accept(writer);
        const std::string json_array_str = buffer.GetString();

        dest_column_ptr->insert_data(json_array_str.data(), json_array_str.size());
    }
}

void FunctionTokenize::_do_tokenize(const ColumnString& src_column_string,
                                    InvertedIndexCtx& inverted_index_ctx,
                                    const MutableColumnPtr& dest_column_ptr) const {
    ColumnArray::Offset64 src_offsets_size = src_column_string.get_offsets().size();
    for (size_t i = 0; i < src_offsets_size; i++) {
        const StringRef tokenize_str = src_column_string.get_data_at(i);
        if (tokenize_str.size == 0) {
            dest_column_ptr->insert_data("", 0);
            continue;
        }

        auto reader = InvertedIndexAnalyzer::create_reader(inverted_index_ctx.char_filter_map);
        reader->init(tokenize_str.data, tokenize_str.size, true);
        auto analyzer_tokens = InvertedIndexAnalyzer::get_analyse_result(
                reader.get(), inverted_index_ctx.analyzer);

        rapidjson::Document doc;
        doc.SetArray();
        rapidjson::Document::AllocatorType& allocator = doc.GetAllocator();
        for (const auto& analyzer_token : analyzer_tokens) {
            rapidjson::Value obj(rapidjson::kObjectType);
            obj.AddMember(
                    "token",
                    rapidjson::Value(analyzer_token.get_single_term().c_str(), allocator).Move(),
                    allocator);
            if (inverted_index_ctx.support_phrase == INVERTED_INDEX_PARSER_PHRASE_SUPPORT_YES) {
                obj.AddMember("position", analyzer_token.position, allocator);
            }
            doc.PushBack(obj, allocator);
        }
        rapidjson::StringBuffer buffer;
        rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(buffer);
        writer.SetFormatOptions(rapidjson::kFormatSingleLineArray);
        doc.Accept(writer);
        const std::string json_array_str = buffer.GetString();

        dest_column_ptr->insert_data(json_array_str.data(), json_array_str.size());
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

    auto dest_column_type = std::make_shared<vectorized::DataTypeString>();
    auto dest_column_ptr = dest_column_type->create_column();

    if (const auto* col_left = check_and_get_column<ColumnString>(src_column.get())) {
        if (const auto* col_right = check_and_get_column<ColumnString>(right_column.get())) {
            InvertedIndexCtx inverted_index_ctx;
            std::map<std::string, std::string> properties;
            auto st = parse(col_right->get_data_at(0).to_string(), properties);
            if (!st.ok()) {
                return st;
            }
            inverted_index_ctx.custom_analyzer =
                    get_custom_analyzer_string_from_properties(properties);
            inverted_index_ctx.parser_type = get_inverted_index_parser_type_from_string(
                    get_parser_string_from_properties(properties));
            if (inverted_index_ctx.parser_type == InvertedIndexParserType::PARSER_UNKNOWN) {
                return Status::Error<doris::ErrorCode::INVERTED_INDEX_INVALID_PARAMETERS>(
                        "unsupported parser type. currently, only 'english', 'chinese', "
                        "'unicode', 'icu', 'basic' and 'ik' analyzers are supported.");
            }

            // Special handling for PARSER_NONE: return original string as single token
            if (inverted_index_ctx.custom_analyzer.empty() &&
                inverted_index_ctx.parser_type == InvertedIndexParserType::PARSER_NONE) {
                _do_tokenize_none(*col_left, dest_column_ptr);
                block.replace_by_position(result, std::move(dest_column_ptr));
                return Status::OK();
            }

            inverted_index_ctx.parser_mode = get_parser_mode_string_from_properties(properties);
            inverted_index_ctx.support_phrase =
                    get_parser_phrase_support_string_from_properties(properties);
            inverted_index_ctx.char_filter_map =
                    get_parser_char_filter_map_from_properties(properties);
            inverted_index_ctx.lower_case = get_parser_lowercase_from_properties(properties);
            inverted_index_ctx.stop_words = get_parser_stopwords_from_properties(properties);

            std::shared_ptr<lucene::analysis::Analyzer> analyzer;
            try {
                analyzer =
                        doris::segment_v2::inverted_index::InvertedIndexAnalyzer::create_analyzer(
                                &inverted_index_ctx);
            } catch (CLuceneError& e) {
                return Status::Error<doris::ErrorCode::INVERTED_INDEX_ANALYZER_ERROR>(
                        "inverted index create analyzer failed: {}", e.what());
            } catch (Exception& e) {
                return Status::Error<doris::ErrorCode::INVERTED_INDEX_ANALYZER_ERROR>(
                        "inverted index create analyzer failed: {}", e.what());
            }

            inverted_index_ctx.analyzer = analyzer.get();
            _do_tokenize(*col_left, inverted_index_ctx, dest_column_ptr);

            block.replace_by_position(result, std::move(dest_column_ptr));
            return Status::OK();
        }
    }
    return Status::RuntimeError("unimplemented function {}", get_name());
}
} // namespace doris::vectorized
