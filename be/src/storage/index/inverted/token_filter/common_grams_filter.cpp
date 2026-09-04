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

#include "storage/index/inverted/token_filter/common_grams_filter.h"

#include <string_view>
#include <utility>

#include "common/exception.h"
#include "common/logging.h"
#include "storage/index/inverted/common_grams/common_grams_key_codec.h"

namespace doris::segment_v2::inverted_index {
namespace {

void validate_input_token_shape(const Token& token, std::string_view term) {
    if (token.getPositionIncrement() != 1) {
        throw Exception(ErrorCode::INVERTED_INDEX_ANALYZER_ERROR,
                        "CommonGrams requires position increment 1, got {}",
                        token.getPositionIncrement());
    }
    if (term.empty()) {
        throw Exception(ErrorCode::INVERTED_INDEX_ANALYZER_ERROR,
                        "CommonGrams requires non-empty input tokens");
    }
}

void validate_input_token(const Token& token, std::string_view term) {
    validate_input_token_shape(token, term);
    auto status = validate_common_grams_logical_term(term, "input token");
    if (!status.ok()) {
        throw Exception(status);
    }
}

void set_output_token(Token* token, std::string_view term, int32_t position_increment,
                      int32_t start_offset, int32_t end_offset, const TCHAR* type) {
    token->clear();
    token->setTextNoCopy(term.data(), static_cast<int32_t>(term.size()));
    token->setPositionIncrement(position_increment);
    token->setStartOffset(start_offset);
    token->setEndOffset(end_offset);
    token->setType(type);
}

bool is_common_word(const CommonGramsBufferedToken& token, const CommonWordSet& common_words,
                    std::optional<bool>* cached_membership) {
    if (!cached_membership->has_value()) {
        cached_membership->emplace(common_words.contains(token.term));
    }
    return cached_membership->value();
}

} // namespace

bool common_grams_query_may_use_gram(std::span<const std::string> terms, CommonGramsQueryMode mode,
                                     const CommonWordSet& common_words) {
    if (terms.size() < 2) {
        return false;
    }
    const size_t relevant_term_count =
            mode == CommonGramsQueryMode::kPhrasePrefix ? terms.size() - 1 : terms.size();
    for (size_t i = 0; i < relevant_term_count; ++i) {
        if (common_words.contains(terms[i])) {
            return true;
        }
    }
    return false;
}

CommonGramsFilter::CommonGramsFilter(TokenStreamPtr in,
                                     std::shared_ptr<const CommonWordSet> common_words,
                                     CommonGramsOutputMode output_mode)
        : DorisTokenFilter(std::move(in)),
          _common_words(std::move(common_words)),
          _output_mode(output_mode) {
    DORIS_CHECK(_common_words != nullptr);
}

bool CommonGramsFilter::read_input(CommonGramsBufferedToken* token,
                                   std::optional<bool>* is_common) {
    if (_in->next(&_input_token) == nullptr) {
        return false;
    }
    std::string_view term(_input_token.termBuffer<char>(), _input_token.termLength<char>());
    validate_input_token(_input_token, term);
    token->term.assign(term);
    token->start_offset = _input_token.startOffset();
    token->end_offset = _input_token.endOffset();
    token->type = _input_token.type();
    is_common->reset();
    return true;
}

Token* CommonGramsFilter::emit_unigram(Token* token, const CommonGramsBufferedToken& buffered) {
    const std::string_view output_term = encode_plain_term(buffered.term);
    set_output_token(token, output_term, 1, buffered.start_offset, buffered.end_offset,
                     buffered.type);
    return token;
}

std::string_view CommonGramsFilter::encode_plain_term(std::string_view term) {
    if (_output_mode != CommonGramsOutputMode::kLogical && !term.empty() &&
        (term.front() == PLAIN_ESCAPE_PREFIX || term.front() == '\x1f')) {
        if (!try_encode_escaped_plain_term_prevalidated(term, _encoded_plain_term)) {
            throw Exception(Status::Error<ErrorCode::INVERTED_INDEX_ANALYZER_ERROR>(
                    "CommonGrams escaped plain term would exceed the 16383-byte key limit; "
                    "set enable_common_grams_index_build=false and retry the import in a new "
                    "transaction"));
        }
        return _encoded_plain_term;
    }
    return term;
}

std::string_view CommonGramsFilter::encode_snii_plain_term(std::string_view term) {
    DORIS_CHECK(_output_mode == CommonGramsOutputMode::kEscapedV1SpimiIndex);
    DORIS_CHECK(!term.empty());
    if (term.front() != PLAIN_ESCAPE_PREFIX && term.front() != '\x1f') {
        return term;
    }
    if (term.size() == COMMON_GRAM_MAX_ENCODED_BYTES) {
        throw Exception(Status::Error<ErrorCode::INVERTED_INDEX_ANALYZER_ERROR>(
                "CommonGrams escaped plain term would exceed the 16383-byte key limit; "
                "set enable_common_grams_index_build=false and retry the import in a new "
                "transaction"));
    }
    DORIS_CHECK_LT(term.size(), COMMON_GRAM_MAX_ENCODED_BYTES);
    _encoded_plain_term.clear();
    _encoded_plain_term.reserve(term.size() + 1);
    _encoded_plain_term.push_back(PLAIN_ESCAPE_PREFIX);
    _encoded_plain_term.push_back(term.front() == PLAIN_ESCAPE_PREFIX ? 'E' : 'G');
    _encoded_plain_term.append(term.substr(1));
    return _encoded_plain_term;
}

Token* CommonGramsFilter::emit_gram(Token* token, int32_t start_offset, int32_t end_offset) {
    set_output_token(token, _gram, 0, start_offset, end_offset, COMMON_GRAM_TOKEN_TYPE);
    return token;
}

Token* CommonGramsFilter::next(Token* token) {
    if (_emit_current) {
        _emit_current = false;
        return emit_unigram(token, _current);
    }

    if (!_has_current) {
        if (!read_input(&_current, &_current_is_common)) {
            return nullptr;
        }
        _has_current = true;
        return emit_unigram(token, _current);
    }

    if (!read_input(&_lookahead, &_lookahead_is_common)) {
        _has_current = false;
        return nullptr;
    }

    const bool uses_gram = is_common_word(_current, *_common_words, &_current_is_common) ||
                           is_common_word(_lookahead, *_common_words, &_lookahead_is_common);
    if (uses_gram) {
        const bool encoded =
                try_encode_common_gram_prevalidated(_current.term, _lookahead.term, _gram);
        if (encoded) {
            const int32_t start_offset = _current.start_offset;
            const int32_t end_offset = _lookahead.end_offset;
            std::swap(_current, _lookahead);
            std::swap(_current_is_common, _lookahead_is_common);
            _emit_current = true;
            return emit_gram(token, start_offset, end_offset);
        }
    }

    std::swap(_current, _lookahead);
    std::swap(_current_is_common, _lookahead_is_common);
    return emit_unigram(token, _current);
}

bool CommonGramsFilter::next_snii_index_event(SniiCommonGramsIndexEvent* event) {
    DORIS_CHECK(event != nullptr);
    DORIS_CHECK(_output_mode == CommonGramsOutputMode::kEscapedV1SpimiIndex);
    if (_in->next(&_input_token) == nullptr) {
        return false;
    }

    const std::string_view logical_term(_input_token.termBuffer<char>(),
                                        _input_token.termLength<char>());
    validate_input_token_shape(_input_token, logical_term);
    const bool requires_prevalidation =
            logical_term.size() > COMMON_GRAM_MAX_ENCODED_BYTES ||
            (logical_term.size() == COMMON_GRAM_MAX_ENCODED_BYTES &&
             (logical_term.front() == PLAIN_ESCAPE_PREFIX || logical_term.front() == '\x1f'));
    if (requires_prevalidation) {
        auto status = validate_common_grams_logical_term(logical_term, "input token");
        if (!status.ok()) {
            throw Exception(status);
        }
    }
    event->logical_term = logical_term;
    event->plain_term = encode_snii_plain_term(logical_term);
    return true;
}

void CommonGramsFilter::reset() {
    DorisTokenFilter::reset();
    _current.term.clear();
    _current.start_offset = 0;
    _current.end_offset = 0;
    _current.type = Token::getDefaultType();
    _lookahead.term.clear();
    _lookahead.start_offset = 0;
    _lookahead.end_offset = 0;
    _lookahead.type = Token::getDefaultType();
    _current_is_common.reset();
    _lookahead_is_common.reset();
    _has_current = false;
    _emit_current = false;
    _gram.clear();
    _encoded_plain_term.clear();
}

Token* CommonGramsPositionFilter::next(Token* token) {
    if (_in->next(token) == nullptr) {
        return nullptr;
    }
    const std::string_view term(token->termBuffer<char>(), token->termLength<char>());
    validate_input_token(*token, term);
    return token;
}

CommonGramsQueryFilterBase::CommonGramsQueryFilterBase(
        TokenStreamPtr in, std::shared_ptr<const CommonWordSet> common_words,
        CommonGramsQueryMode mode)
        : DorisTokenFilter(std::move(in)), _common_words(std::move(common_words)), _mode(mode) {
    DORIS_CHECK(_common_words != nullptr);
}

bool CommonGramsQueryFilterBase::pair_uses_gram(
        const std::vector<CommonGramsBufferedToken>& unigrams, std::optional<bool>* left_is_common,
        size_t pair_index) const {
    const bool is_prefix_boundary =
            _mode == CommonGramsQueryMode::kPhrasePrefix && pair_index + 2 == unigrams.size();
    if (is_prefix_boundary) {
        return is_common_word(unigrams[pair_index], *_common_words, left_is_common);
    }

    std::optional<bool> right_is_common;
    const bool uses_gram =
            is_common_word(unigrams[pair_index], *_common_words, left_is_common) ||
            is_common_word(unigrams[pair_index + 1], *_common_words, &right_is_common);
    *left_is_common = right_is_common;
    return uses_gram;
}

void CommonGramsQueryFilterBase::append_plain_output(std::vector<CommonGramsBufferedToken>* output,
                                                     const CommonGramsBufferedToken& token) {
    output->push_back(token);
}

bool CommonGramsQueryFilterBase::append_gram_output(
        std::vector<CommonGramsBufferedToken>* output,
        const std::optional<CommonGramsBufferedToken>& indexed_gram,
        const CommonGramsBufferedToken& left, const CommonGramsBufferedToken& right) {
    if (!indexed_gram.has_value()) {
        DCHECK(!is_common_gram_encodable(left.term, right.term));
        return false;
    }
    CommonGramsBufferedToken query_gram = *indexed_gram;
    query_gram.type = COMMON_GRAM_TOKEN_TYPE;
    output->push_back(std::move(query_gram));
    return true;
}

void CommonGramsQueryFilterBase::prepare_output() {
    std::vector<CommonGramsBufferedToken> unigrams;
    std::vector<std::optional<CommonGramsBufferedToken>> indexed_grams;
    Token token;
    while (_in->next(&token) != nullptr) {
        const std::string_view term(token.termBuffer<char>(), token.termLength<char>());
        if (token.getPositionIncrement() == 0) {
            if (!is_common_gram_token_type(token.type()) || unigrams.empty() ||
                indexed_grams.back().has_value()) {
                throw Exception(ErrorCode::INVERTED_INDEX_ANALYZER_ERROR,
                                "Invalid indexed CommonGrams token sequence");
            }
            auto status = validate_common_grams_logical_term(term, "indexed gram");
            if (!status.ok()) {
                throw Exception(status);
            }
            indexed_grams.back() = CommonGramsBufferedToken {.term = std::string(term),
                                                             .start_offset = token.startOffset(),
                                                             .end_offset = token.endOffset(),
                                                             .type = token.type()};
            continue;
        }
        validate_input_token(token, term);
        unigrams.push_back({.term = std::string(term),
                            .start_offset = token.startOffset(),
                            .end_offset = token.endOffset(),
                            .type = token.type()});
        indexed_grams.emplace_back();
    }

    std::vector<CommonGramsBufferedToken> output;
    if (unigrams.size() < 2) {
        _output = std::move(unigrams);
        _prepared = true;
        return;
    }

    bool last_pair_used_gram = false;
    std::optional<bool> left_is_common;
    for (size_t i = 0; i + 1 < unigrams.size(); ++i) {
        last_pair_used_gram = pair_uses_gram(unigrams, &left_is_common, i);
        if (last_pair_used_gram) {
            if (!append_gram_output(&output, indexed_grams[i], unigrams[i], unigrams[i + 1])) {
                output.clear();
                for (const auto& unigram : unigrams) {
                    append_plain_output(&output, unigram);
                }
                _output = std::move(output);
                _prepared = true;
                return;
            }
        } else {
            append_plain_output(&output, unigrams[i]);
        }
    }
    if (!last_pair_used_gram) {
        append_plain_output(&output, unigrams.back());
    }
    _output = std::move(output);
    _prepared = true;
}

Token* CommonGramsQueryFilterBase::emit(Token* token, const CommonGramsBufferedToken& buffered) {
    set_output_token(token, buffered.term, 1, buffered.start_offset, buffered.end_offset,
                     buffered.type);
    return token;
}

Token* CommonGramsQueryFilterBase::next(Token* token) {
    if (_failure != nullptr) {
        std::rethrow_exception(_failure);
    }
    if (!_prepared) {
        try {
            prepare_output();
        } catch (...) {
            _failure = std::current_exception();
            throw;
        }
    }
    if (_next_output == _output.size()) {
        return nullptr;
    }
    return emit(token, _output[_next_output++]);
}

void CommonGramsQueryFilterBase::reset() {
    DorisTokenFilter::reset();
    _output.clear();
    _next_output = 0;
    _prepared = false;
    _failure = nullptr;
}

} // namespace doris::segment_v2::inverted_index
