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

#include "exprs/function/match.h"

#include <hs/hs.h>

#include <algorithm>
#include <cstdint>
#include <span>
#include <unordered_map>
#include <unordered_set>

#include "core/field.h"
#include "runtime/query_context.h"
#include "runtime/runtime_state.h"
#include "storage/index/index_reader_helper.h"
#include "storage/index/inverted/analyzer/analyzer.h"
#include "util/debug_points.h"
#include "util/hyperscan_util.h"

namespace doris {

namespace {

const InvertedIndexAnalyzerCtx* get_match_analyzer_ctx(FunctionContext* context) {
    if (context == nullptr) {
        return nullptr;
    }
    const auto* analyzer_ctx = reinterpret_cast<const InvertedIndexAnalyzerCtx*>(
            context->get_function_state(FunctionContext::THREAD_LOCAL));
    if (analyzer_ctx == nullptr) {
        analyzer_ctx = reinterpret_cast<const InvertedIndexAnalyzerCtx*>(
                context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    }
    return analyzer_ctx;
}

enum class PhraseMode { EXACT, PREFIX, EDGE };

class StreamingPhraseMatcher {
public:
    StreamingPhraseMatcher(const std::vector<segment_v2::TermInfo>& query_tokens, PhraseMode mode)
            : _mode(mode),
              _query_size(query_tokens.size()),
              _last_word((_query_size - 1) / 64),
              _last_bit(uint64_t {1} << ((_query_size - 1) % 64)),
              _state(_last_word + 1, 0),
              _pending_masks(_last_word + 1, 0),
              _active_marks(_last_word + 1, 0),
              _first_term(query_tokens.front().get_single_term()),
              _last_term(query_tokens.back().get_single_term()) {
        for (size_t pos = 0; pos < _query_size; ++pos) {
            if ((_mode == PhraseMode::PREFIX && pos == _query_size - 1) ||
                (_mode == PhraseMode::EDGE && (pos == 0 || pos == _query_size - 1))) {
                continue;
            }
            auto& words = _exact_masks[query_tokens[pos].get_single_term()];
            const size_t word = pos / 64;
            const uint64_t bit = uint64_t {1} << (pos % 64);
            if (!words.empty() && words.back().first == word) {
                words.back().second |= bit;
            } else {
                words.emplace_back(word, bit);
            }
        }
    }

    void reset_row() {
        for (size_t word : _pending_touched) {
            _pending_masks[word] = 0;
        }
        _pending_touched.clear();
        _pending_sorted = true;
        _pending_position = -1;
        _previous_position = -1;
        _active_words.clear();
        ++_generation;
    }

    bool feed(const std::string& term, int64_t position) {
        if (_pending_position >= 0 && position != _pending_position) {
            if (advance()) {
                return true;
            }
        }
        if (_pending_position < 0) {
            start_position(position);
        }
        const auto mask_it = _exact_masks.find(term);
        if (mask_it != _exact_masks.end()) {
            const auto& mask_words = mask_it->second;
            if (mask_words.size() / 4 > _candidate_words.size()) {
                for (size_t word : _candidate_words) {
                    auto it = std::lower_bound(
                            mask_words.begin(), mask_words.end(), word,
                            [](const auto& entry, size_t value) { return entry.first < value; });
                    if (it != mask_words.end() && it->first == word) {
                        add_pending_mask(word, it->second);
                    }
                }
            } else {
                size_t candidate = 0;
                for (const auto& [word, mask] : mask_words) {
                    while (candidate < _candidate_words.size() &&
                           _candidate_words[candidate] < word) {
                        ++candidate;
                    }
                    if (candidate == _candidate_words.size()) {
                        break;
                    }
                    if (_candidate_words[candidate] == word) {
                        add_pending_mask(word, mask);
                    }
                }
            }
        }
        const bool first_matches = _mode == PhraseMode::EDGE &&
                                   (_query_size == 1 ? term.find(_first_term) != std::string::npos
                                                     : term.ends_with(_first_term));
        const bool last_matches =
                (_mode == PhraseMode::PREFIX || (_mode == PhraseMode::EDGE && _query_size > 1)) &&
                term.starts_with(_last_term);

        if (first_matches) {
            add_pending_mask(0, 1);
        }
        if (last_matches &&
            std::binary_search(_candidate_words.begin(), _candidate_words.end(), _last_word)) {
            add_pending_mask(_last_word, _last_bit);
        }
        return false;
    }

    bool finish() { return _pending_position >= 0 && advance(); }

private:
    void start_position(int64_t position) {
        if (_previous_position >= 0 && position != _previous_position + 1) {
            _active_words.clear();
            ++_generation;
        }
        _pending_position = position;
        _candidate_words.clear();
        _candidate_words.push_back(0);
        for (size_t word : _active_words) {
            if (_candidate_words.back() != word) {
                _candidate_words.push_back(word);
            }
            if (word < _last_word && (_state[word] >> 63) != 0) {
                _candidate_words.push_back(word + 1);
            }
        }
    }

    void add_pending_mask(size_t word, uint64_t mask) {
        if (_pending_masks[word] == 0) {
            if (!_pending_touched.empty() && word < _pending_touched.back()) {
                _pending_sorted = false;
            }
            _pending_touched.push_back(word);
        }
        _pending_masks[word] |= mask;
    }

    bool advance() {
        const int64_t position = _pending_position;
        _pending_position = -1;
        _previous_position = position;

        const uint64_t old_generation = _generation++;
        _next_active_words.clear();
        if (!_pending_touched.empty()) {
            if (!_pending_sorted) {
                std::sort(_pending_touched.begin(), _pending_touched.end());
            }
            for (auto it = _pending_touched.rbegin(); it != _pending_touched.rend(); ++it) {
                const size_t word = *it;
                const uint64_t previous = _active_marks[word] == old_generation ? _state[word] : 0;
                const uint64_t carry = word > 0 && _active_marks[word - 1] == old_generation
                                               ? _state[word - 1] >> 63
                                               : 0;
                const uint64_t next =
                        ((previous << 1) | carry | uint64_t {word == 0}) & _pending_masks[word];
                if (next != 0) {
                    _state[word] = next;
                    _active_marks[word] = _generation;
                    _next_active_words.push_back(word);
                }
            }
        }
        for (size_t word : _pending_touched) {
            _pending_masks[word] = 0;
        }
        _pending_touched.clear();
        _pending_sorted = true;
        std::reverse(_next_active_words.begin(), _next_active_words.end());
        _active_words.swap(_next_active_words);
        return !_active_words.empty() && _active_words.back() == _last_word &&
               (_state[_last_word] & _last_bit) != 0;
    }

    PhraseMode _mode;
    size_t _query_size;
    size_t _last_word;
    uint64_t _last_bit;
    std::vector<uint64_t> _state;
    std::vector<uint64_t> _pending_masks;
    std::vector<uint64_t> _active_marks;
    std::vector<size_t> _pending_touched;
    std::vector<size_t> _candidate_words;
    std::vector<size_t> _active_words;
    std::vector<size_t> _next_active_words;
    std::string _first_term;
    std::string _last_term;
    std::unordered_map<std::string, std::vector<std::pair<size_t, uint64_t>>> _exact_masks;
    int64_t _pending_position = -1;
    int64_t _previous_position = -1;
    uint64_t _generation = 1;
    bool _pending_sorted = true;
};

template <typename Callback>
bool for_each_data_element_tokens(const FunctionMatchBase& function, const std::string& column_name,
                                  const InvertedIndexAnalyzerCtx* analyzer_ctx,
                                  const ColumnString* string_col, size_t row,
                                  const ColumnArray::Offsets64* array_offsets,
                                  const ColumnUInt8::Container* array_element_null_map,
                                  Callback&& callback) {
    const size_t begin = array_offsets ? (row == 0 ? 0 : (*array_offsets)[row - 1]) : row;
    const size_t end = array_offsets ? (*array_offsets)[row] : row + 1;
    const bool keyword = analyzer_ctx &&
                         (!analyzer_ctx->requires_analysis() || analyzer_ctx->analyzer == nullptr);
    segment_v2::TermInfo keyword_token;
    int32_t unused_array_offset = 0;
    for (size_t element = begin; element < end; ++element) {
        if (array_element_null_map && (*array_element_null_map)[element]) {
            continue;
        }
        if (keyword) {
            keyword_token.term = string_col->get_data_at(element).to_string();
            if (callback(std::span(&keyword_token, 1))) {
                return true;
            }
            continue;
        }
        auto tokens = function.analyse_data_token(column_name, analyzer_ctx, string_col, element,
                                                  nullptr, unused_array_offset);
        if (tokens.empty()) {
            continue;
        }
        if (callback(std::span<const segment_v2::TermInfo>(tokens))) {
            return true;
        }
    }
    return false;
}

bool match_phrase_data_tokens(const FunctionMatchBase& function, const std::string& column_name,
                              const InvertedIndexAnalyzerCtx* analyzer_ctx,
                              const ColumnString* string_col, size_t row,
                              const ColumnArray::Offsets64* array_offsets,
                              const ColumnUInt8::Container* array_element_null_map,
                              StreamingPhraseMatcher& matcher) {
    matcher.reset_row();
    int64_t position_base = 0;
    const bool matched = for_each_data_element_tokens(
            function, column_name, analyzer_ctx, string_col, row, array_offsets,
            array_element_null_map, [&](std::span<const segment_v2::TermInfo> tokens) {
                const bool analyzed = analyzer_ctx && analyzer_ctx->requires_analysis() &&
                                      analyzer_ctx->analyzer != nullptr;
                int32_t last_position = 0;
                for (const auto& token : tokens) {
                    const int32_t position = analyzed ? token.position : 1;
                    if (matcher.feed(token.get_single_term(), position_base + position)) {
                        return true;
                    }
                    last_position = position;
                }
                position_base += last_position;
                return false;
            });
    return matched || matcher.finish();
}

} // namespace

Status FunctionMatchBase::evaluate_inverted_index(
        const ColumnsWithTypeAndName& arguments,
        const std::vector<IndexFieldNameAndTypePair>& data_type_with_names,
        std::vector<segment_v2::IndexIterator*> iterators, uint32_t num_rows,
        const InvertedIndexAnalyzerCtx* analyzer_ctx,
        segment_v2::InvertedIndexResultBitmap& bitmap_result) const {
    DCHECK(arguments.size() == 1);
    DCHECK(data_type_with_names.size() == 1);
    DCHECK(iterators.size() == 1);
    auto* iter = iterators[0];
    auto data_type_with_name = data_type_with_names[0];
    if (iter == nullptr) {
        return Status::OK();
    }
    const std::string& function_name = get_name();

    if (function_name == MATCH_PHRASE_FUNCTION || function_name == MATCH_PHRASE_PREFIX_FUNCTION ||
        function_name == MATCH_PHRASE_EDGE_FUNCTION) {
        // Judge phrase support on the index the query will run on: read_from_index selects it
        // from the same column type, query type and analyzer key. The first FULLTEXT reader need
        // not be that index -- a docs-only one (a gram index is docs-only by default) can be
        // declared ahead of the positional index USING ANALYZER names.
        if (auto* inverted_iter = dynamic_cast<segment_v2::InvertedIndexIterator*>(iter)) {
            auto reader = inverted_iter->select_best_reader(
                    data_type_with_name.second, get_query_type_from_fn_name(),
                    analyzer_ctx != nullptr ? analyzer_ctx->analyzer_key : std::string());
            if (reader.has_value() &&
                segment_v2::IndexReaderHelper::is_fulltext_index(reader.value()) &&
                !segment_v2::IndexReaderHelper::is_support_phrase(reader.value())) {
                return Status::Error<ErrorCode::INDEX_INVALID_PARAMETERS>(
                        "phrase queries require setting support_phrase = true");
            }
        }
    }
    Field param_value;
    arguments[0].column->get(0, param_value);
    if (param_value.is_null()) {
        // if query value is null, skip evaluate inverted index
        return Status::OK();
    }
    auto param_type = arguments[0].type->get_primitive_type();
    if (!is_string_type(param_type)) {
        return Status::Error<ErrorCode::INDEX_INVALID_PARAMETERS>(
                "arguments for match must be string");
    }
    InvertedIndexParam param;
    param.column_name = data_type_with_name.first;
    param.column_type = data_type_with_name.second;
    param.query_value = param_value;
    param.query_type = get_query_type_from_fn_name();
    param.num_rows = num_rows;
    param.roaring = std::make_shared<roaring::Roaring>();
    segment_v2::InvertedIndexQueryCacheHandle null_bitmap_cache_handle;
    param.null_bitmap_cache_handle = &null_bitmap_cache_handle;
    param.analyzer_ctx = analyzer_ctx;
    if (is_string_type(param_type)) {
        RETURN_IF_ERROR(iter->read_from_index(&param));
    } else {
        return Status::Error<ErrorCode::INDEX_INVALID_PARAMETERS>(
                "invalid params type for FunctionMatchBase::evaluate_inverted_index {}",
                param_type);
    }
    std::shared_ptr<roaring::Roaring> null_bitmap = null_bitmap_cache_handle.get_bitmap();
    if (null_bitmap == nullptr) {
        // query_with_null_bitmap leaves the handle empty only when the selected reader proves that
        // the index has no null rows.
        null_bitmap = std::make_shared<roaring::Roaring>();
    }
    segment_v2::InvertedIndexResultBitmap result(param.roaring, null_bitmap);
    bitmap_result = result;
    bitmap_result.mask_out_null();

    return Status::OK();
}
Status FunctionMatchBase::execute_impl(FunctionContext* context, Block& block,
                                       const ColumnNumbers& arguments, uint32_t result,
                                       size_t input_rows_count) const {
    ColumnPtr& column_ptr = block.get_by_position(arguments[1]).column;
    DataTypePtr& type_ptr = block.get_by_position(arguments[1]).type;

    auto format_options = DataTypeSerDe::get_default_format_options();
    auto time_zone = cctz::utc_time_zone();
    format_options.timezone =
            (context && context->state()) ? &context->state()->timezone_obj() : &time_zone;

    auto match_query_str = type_ptr->to_string(*column_ptr, 0, format_options);
    std::string column_name = block.get_by_position(arguments[0]).name;
    VLOG_DEBUG << "begin to execute match directly, column_name=" << column_name
               << ", match_query_str=" << match_query_str;
    const auto* analyzer_ctx = get_match_analyzer_ctx(context);
    const ColumnPtr source_col =
            block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
    const auto* values = check_and_get_column<ColumnString>(source_col.get());
    const ColumnArray* array_col = nullptr;
    const ColumnUInt8::Container* array_element_null_map = nullptr;
    if (is_column<ColumnArray>(source_col.get())) {
        array_col = check_and_get_column<ColumnArray>(source_col.get());
        if (array_col && !array_col->get_data().is_column_string()) {
            return Status::NotSupported(fmt::format(
                    "unsupported nested array of type {} for function {}",
                    is_column_nullable(array_col->get_data()) ? array_col->get_data().get_name()
                                                              : array_col->get_data().get_name(),
                    get_name()));
        }

        if (is_column_nullable(array_col->get_data())) {
            const auto& array_nested_null_column =
                    reinterpret_cast<const ColumnNullable&>(array_col->get_data());
            array_element_null_map = &array_nested_null_column.get_null_map_column().get_data();
            values = check_and_get_column<ColumnString>(
                    *(array_nested_null_column.get_nested_column_ptr()));
        } else {
            // array column element is always set Nullable for now.
            values = check_and_get_column<ColumnString>(*(array_col->get_data_ptr()));
        }
    } else if (const auto* nullable = check_and_get_column<ColumnNullable>(source_col.get())) {
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
    RETURN_IF_ERROR(execute_match(context, column_name, match_query_str, input_rows_count, values,
                                  analyzer_ctx, (array_col ? &(array_col->get_offsets()) : nullptr),
                                  vec_res, array_element_null_map));
    block.replace_by_position(result, std::move(res));

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
    } else if (fn_name == MATCH_PHRASE_PREFIX_FUNCTION) {
        return doris::segment_v2::InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY;
    } else if (fn_name == MATCH_PHRASE_REGEXP_FUNCTION) {
        return doris::segment_v2::InvertedIndexQueryType::MATCH_REGEXP_QUERY;
    } else if (fn_name == MATCH_PHRASE_EDGE_FUNCTION) {
        return doris::segment_v2::InvertedIndexQueryType::MATCH_PHRASE_EDGE_QUERY;
    }
    return doris::segment_v2::InvertedIndexQueryType::UNKNOWN_QUERY;
}

std::vector<segment_v2::TermInfo> FunctionMatchBase::analyse_query_str_token(
        const InvertedIndexAnalyzerCtx* analyzer_ctx, const std::string& match_query_str,
        const std::string& column_name) const {
    std::vector<segment_v2::TermInfo> query_tokens;
    if (analyzer_ctx == nullptr) {
        return query_tokens;
    }

    VLOG_DEBUG << "begin to run " << get_name() << ", parser_type: "
               << inverted_index_parser_type_to_string(analyzer_ctx->parser_type);

    // Raw execution is valid only when neither a named analyzer nor a builtin parser is active.
    if (!analyzer_ctx->requires_analysis()) {
        // Keyword index: all strings (including empty) are valid tokens for exact match.
        // Empty string is a valid value in keyword index and should be matchable.
        query_tokens.emplace_back(match_query_str);
        return query_tokens;
    }

    // Safety check: if analyzer is nullptr but tokenization is expected, fall back to no tokenization
    if (analyzer_ctx->analyzer == nullptr) {
        VLOG_DEBUG << "Analyzer is nullptr, falling back to no tokenization";
        // For fallback case, also allow empty strings to be matched
        query_tokens.emplace_back(match_query_str);
        return query_tokens;
    }

    // Tokenize using the analyzer
    auto reader = doris::segment_v2::inverted_index::InvertedIndexAnalyzer::create_reader(
            analyzer_ctx->char_filter_map);
    reader->init(match_query_str.data(), (int)match_query_str.size(), true);
    query_tokens = doris::segment_v2::inverted_index::InvertedIndexAnalyzer::get_analyse_result(
            reader, analyzer_ctx->analyzer.get());
    return query_tokens;
}

inline std::vector<segment_v2::TermInfo> FunctionMatchBase::analyse_data_token(
        const std::string& column_name, const InvertedIndexAnalyzerCtx* analyzer_ctx,
        const ColumnString* string_col, size_t current_block_row_idx,
        const ColumnArray::Offsets64* array_offsets, int32_t& current_src_array_offset,
        const ColumnUInt8::Container* array_element_null_map) const {
    std::vector<segment_v2::TermInfo> data_tokens;
    if (analyzer_ctx == nullptr) {
        return data_tokens;
    }

    const bool requires_analysis =
            analyzer_ctx->requires_analysis() && analyzer_ctx->analyzer != nullptr;

    if (array_offsets) {
        for (auto next_src_array_offset = (*array_offsets)[current_block_row_idx];
             current_src_array_offset < next_src_array_offset; ++current_src_array_offset) {
            if (array_element_null_map && (*array_element_null_map)[current_src_array_offset]) {
                continue;
            }
            const auto& str_ref = string_col->get_data_at(current_src_array_offset);
            if (!requires_analysis) {
                data_tokens.emplace_back(str_ref.to_string());
                continue;
            }
            auto reader = doris::segment_v2::inverted_index::InvertedIndexAnalyzer::create_reader(
                    analyzer_ctx->char_filter_map);
            reader->init(str_ref.data, (int)str_ref.size, true);
            auto element_tokens =
                    doris::segment_v2::inverted_index::InvertedIndexAnalyzer::get_analyse_result(
                            reader, analyzer_ctx->analyzer.get());
            for (auto& token : element_tokens) {
                data_tokens.emplace_back(std::move(token));
            }
        }
    } else {
        const auto& str_ref = string_col->get_data_at(current_block_row_idx);
        if (!requires_analysis) {
            data_tokens.emplace_back(str_ref.to_string());
        } else {
            auto reader = doris::segment_v2::inverted_index::InvertedIndexAnalyzer::create_reader(
                    analyzer_ctx->char_filter_map);
            reader->init(str_ref.data, (int)str_ref.size, true);
            data_tokens =
                    doris::segment_v2::inverted_index::InvertedIndexAnalyzer::get_analyse_result(
                            reader, analyzer_ctx->analyzer.get());
        }
    }
    return data_tokens;
}

Status FunctionMatchBase::check(FunctionContext* context, const std::string& function_name) const {
    if (!context->state()->query_options().enable_match_without_inverted_index) {
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                "{} not support execute_match", function_name);
    }

    DBUG_EXECUTE_IF("match.invert_index_not_support_execute_match", {
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                "debug point: {} not support execute_match", function_name);
    });

    return Status::OK();
}

Status FunctionMatchAny::execute_match(FunctionContext* context, const std::string& column_name,
                                       const std::string& match_query_str, size_t input_rows_count,
                                       const ColumnString* string_col,
                                       const InvertedIndexAnalyzerCtx* analyzer_ctx,
                                       const ColumnArray::Offsets64* array_offsets,
                                       ColumnUInt8::Container& result,
                                       const ColumnUInt8::Container* array_element_null_map) const {
    RETURN_IF_ERROR(check(context, name));

    auto query_tokens = analyse_query_str_token(analyzer_ctx, match_query_str, column_name);
    if (query_tokens.empty()) {
        VLOG_DEBUG << fmt::format(
                "token parser result is empty for query, "
                "please check your query: '{}' and index parser: '{}'",
                match_query_str,
                analyzer_ctx ? inverted_index_parser_type_to_string(analyzer_ctx->parser_type)
                             : "unknown");
        return Status::OK();
    }

    std::unordered_set<std::string> query_terms;
    for (const auto& token : query_tokens) {
        query_terms.emplace(token.get_single_term());
    }

    for (int i = 0; i < input_rows_count; i++) {
        if (for_each_data_element_tokens(*this, column_name, analyzer_ctx, string_col, i,
                                         array_offsets, array_element_null_map,
                                         [&](std::span<const segment_v2::TermInfo> data_tokens) {
                                             for (const auto& info : data_tokens) {
                                                 if (query_terms.contains(info.get_single_term())) {
                                                     return true;
                                                 }
                                             }
                                             return false;
                                         })) {
            result[i] = true;
        }
    }

    return Status::OK();
}

Status FunctionMatchAll::execute_match(FunctionContext* context, const std::string& column_name,
                                       const std::string& match_query_str, size_t input_rows_count,
                                       const ColumnString* string_col,
                                       const InvertedIndexAnalyzerCtx* analyzer_ctx,
                                       const ColumnArray::Offsets64* array_offsets,
                                       ColumnUInt8::Container& result,
                                       const ColumnUInt8::Container* array_element_null_map) const {
    RETURN_IF_ERROR(check(context, name));

    auto query_tokens = analyse_query_str_token(analyzer_ctx, match_query_str, column_name);
    if (query_tokens.empty()) {
        VLOG_DEBUG << fmt::format(
                "token parser result is empty for query, "
                "please check your query: '{}' and index parser: '{}'",
                match_query_str,
                analyzer_ctx ? inverted_index_parser_type_to_string(analyzer_ctx->parser_type)
                             : "unknown");
        return Status::OK();
    }

    std::unordered_map<std::string, size_t> query_term_ids;
    for (const auto& token : query_tokens) {
        query_term_ids.emplace(token.get_single_term(), query_term_ids.size());
    }
    std::vector<size_t> last_seen_row;

    for (int i = 0; i < input_rows_count; i++) {
        size_t remaining = query_term_ids.size();
        if (for_each_data_element_tokens(
                    *this, column_name, analyzer_ctx, string_col, i, array_offsets,
                    array_element_null_map, [&](std::span<const segment_v2::TermInfo> data_tokens) {
                        if (last_seen_row.empty()) {
                            last_seen_row.resize(query_term_ids.size(), 0);
                        }
                        for (const auto& info : data_tokens) {
                            auto it = query_term_ids.find(info.get_single_term());
                            if (it != query_term_ids.end() &&
                                last_seen_row[it->second] != static_cast<size_t>(i) + 1) {
                                last_seen_row[it->second] = static_cast<size_t>(i) + 1;
                                if (--remaining == 0) {
                                    return true;
                                }
                            }
                        }
                        return false;
                    })) {
            result[i] = true;
        }
    }

    return Status::OK();
}

Status FunctionMatchPhrase::execute_match(
        FunctionContext* context, const std::string& column_name,
        const std::string& match_query_str, size_t input_rows_count, const ColumnString* string_col,
        const InvertedIndexAnalyzerCtx* analyzer_ctx, const ColumnArray::Offsets64* array_offsets,
        ColumnUInt8::Container& result,
        const ColumnUInt8::Container* array_element_null_map) const {
    RETURN_IF_ERROR(check(context, name));

    auto query_tokens = analyse_query_str_token(analyzer_ctx, match_query_str, column_name);
    if (query_tokens.empty()) {
        VLOG_DEBUG << fmt::format(
                "token parser result is empty for query, "
                "please check your query: '{}' and index parser: '{}'",
                match_query_str,
                analyzer_ctx ? inverted_index_parser_type_to_string(analyzer_ctx->parser_type)
                             : "unknown");
        return Status::OK();
    }

    StreamingPhraseMatcher matcher(query_tokens, PhraseMode::EXACT);
    for (int i = 0; i < input_rows_count; i++) {
        if (match_phrase_data_tokens(*this, column_name, analyzer_ctx, string_col, i, array_offsets,
                                     array_element_null_map, matcher)) {
            result[i] = true;
        }
    }

    return Status::OK();
}

Status FunctionMatchPhrasePrefix::execute_match(
        FunctionContext* context, const std::string& column_name,
        const std::string& match_query_str, size_t input_rows_count, const ColumnString* string_col,
        const InvertedIndexAnalyzerCtx* analyzer_ctx, const ColumnArray::Offsets64* array_offsets,
        ColumnUInt8::Container& result,
        const ColumnUInt8::Container* array_element_null_map) const {
    RETURN_IF_ERROR(check(context, name));

    auto query_tokens = analyse_query_str_token(analyzer_ctx, match_query_str, column_name);
    if (query_tokens.empty()) {
        VLOG_DEBUG << fmt::format(
                "token parser result is empty for query, "
                "please check your query: '{}' and index parser: '{}'",
                match_query_str,
                analyzer_ctx ? inverted_index_parser_type_to_string(analyzer_ctx->parser_type)
                             : "unknown");
        return Status::OK();
    }

    StreamingPhraseMatcher matcher(query_tokens, PhraseMode::PREFIX);
    for (int i = 0; i < input_rows_count; i++) {
        if (match_phrase_data_tokens(*this, column_name, analyzer_ctx, string_col, i, array_offsets,
                                     array_element_null_map, matcher)) {
            result[i] = true;
        }
    }

    return Status::OK();
}

Status FunctionMatchRegexp::execute_match(
        FunctionContext* context, const std::string& column_name,
        const std::string& match_query_str, size_t input_rows_count, const ColumnString* string_col,
        const InvertedIndexAnalyzerCtx* analyzer_ctx, const ColumnArray::Offsets64* array_offsets,
        ColumnUInt8::Container& result,
        const ColumnUInt8::Container* array_element_null_map) const {
    RETURN_IF_ERROR(check(context, name));

    VLOG_DEBUG << "begin to run FunctionMatchRegexp::execute_match, parser_type: "
               << (analyzer_ctx ? inverted_index_parser_type_to_string(analyzer_ctx->parser_type)
                                : "unknown");

    const std::string& pattern = match_query_str;

    hs_database_t* database = nullptr;
    hs_compile_error_t* compile_err = nullptr;
    hs_scratch_t* scratch = nullptr;

    if (is_hyperscan_regexp_expensive(pattern)) {
        return Status::Error<ErrorCode::INDEX_INVALID_PARAMETERS>(HYPERSCAN_BOUNDED_REPEAT_ERROR);
    }

    if (hs_compile(pattern.data(), HS_FLAG_DOTALL | HS_FLAG_ALLOWEMPTY | HS_FLAG_UTF8,
                   HS_MODE_BLOCK, nullptr, &database, &compile_err) != HS_SUCCESS) {
        std::string err_message = "hyperscan compilation failed: ";
        err_message.append(compile_err->message);
        LOG(ERROR) << err_message;
        hs_free_compile_error(compile_err);
        return Status::Error<ErrorCode::INDEX_INVALID_PARAMETERS>(err_message);
    }

    if (hs_alloc_scratch(database, &scratch) != HS_SUCCESS) {
        LOG(ERROR) << "hyperscan could not allocate scratch space.";
        hs_free_database(database);
        return Status::Error<ErrorCode::INDEX_INVALID_PARAMETERS>(
                "hyperscan could not allocate scratch space.");
    }

    auto on_match = [](unsigned int id, unsigned long long from, unsigned long long to,
                       unsigned int flags, void* context) -> int {
        *((bool*)context) = true;
        return 0;
    };

    try {
        for (int i = 0; i < input_rows_count; i++) {
            for_each_data_element_tokens(
                    *this, column_name, analyzer_ctx, string_col, i, array_offsets,
                    array_element_null_map, [&](std::span<const segment_v2::TermInfo> data_tokens) {
                        for (const auto& input : data_tokens) {
                            bool is_match = false;
                            const auto& input_str = input.get_single_term();
                            if (hs_scan(database, input_str.data(), (uint32_t)input_str.size(), 0,
                                        scratch, on_match, (void*)&is_match) != HS_SUCCESS) {
                                LOG(ERROR) << "hyperscan match failed: " << input_str;
                                return true;
                            }
                            if (is_match) {
                                result[i] = true;
                                return true;
                            }
                        }
                        return false;
                    });
        }
    }
    _CLFINALLY({
        hs_free_scratch(scratch);
        hs_free_database(database);
    })

    return Status::OK();
}

Status FunctionMatchPhraseEdge::execute_match(
        FunctionContext* context, const std::string& column_name,
        const std::string& match_query_str, size_t input_rows_count, const ColumnString* string_col,
        const InvertedIndexAnalyzerCtx* analyzer_ctx, const ColumnArray::Offsets64* array_offsets,
        ColumnUInt8::Container& result,
        const ColumnUInt8::Container* array_element_null_map) const {
    RETURN_IF_ERROR(check(context, name));

    auto query_tokens = analyse_query_str_token(analyzer_ctx, match_query_str, column_name);
    if (query_tokens.empty()) {
        VLOG_DEBUG << fmt::format(
                "token parser result is empty for query, "
                "please check your query: '{}' and index parser: '{}'",
                match_query_str,
                analyzer_ctx ? inverted_index_parser_type_to_string(analyzer_ctx->parser_type)
                             : "unknown");
        return Status::OK();
    }

    StreamingPhraseMatcher matcher(query_tokens, PhraseMode::EDGE);
    for (int i = 0; i < input_rows_count; i++) {
        if (match_phrase_data_tokens(*this, column_name, analyzer_ctx, string_col, i, array_offsets,
                                     array_element_null_map, matcher)) {
            result[i] = true;
        }
    }

    return Status::OK();
}

void register_function_match(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionMatchAny>();
    factory.register_function<FunctionMatchAll>();
    factory.register_function<FunctionMatchPhrase>();
    factory.register_function<FunctionMatchPhrasePrefix>();
    factory.register_function<FunctionMatchRegexp>();
    factory.register_function<FunctionMatchPhraseEdge>();
}
} // namespace doris
