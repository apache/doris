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

#include <exception>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "storage/index/inverted/common_grams/common_word_set.h"
#include "storage/index/inverted/token_filter/token_filter.h"

namespace doris::segment_v2::inverted_index {

inline constexpr const TCHAR* COMMON_GRAM_TOKEN_TYPE = L"common_gram";

inline bool is_common_gram_token_type(const TCHAR* type) {
    return std::wstring_view(type) == COMMON_GRAM_TOKEN_TYPE;
}

struct CommonGramsBufferedToken {
    std::string term;
    int32_t start_offset = 0;
    int32_t end_offset = 0;
    const TCHAR* type = Token::getDefaultType();
};

enum class CommonGramsOutputMode {
    kLogical,
    kEscapedV1Index,
    kEscapedV1SpimiIndex,
};

struct SniiCommonGramsIndexEvent {
    // Analyzer output before the EscapedV1 namespace transform. The view has the
    // same lifetime as plain_term and is used for validate-on-first-intern.
    std::string_view logical_term;
    // EscapedV1 physical plain key. The view remains valid until the next event
    // call or reset; the SNII writer interns it synchronously.
    std::string_view plain_term;
};

class CommonGramsFilter final : public DorisTokenFilter {
public:
    CommonGramsFilter(TokenStreamPtr in, std::shared_ptr<const CommonWordSet> common_words,
                      CommonGramsOutputMode output_mode = CommonGramsOutputMode::kLogical);

    Token* next(Token* token) override;
    bool next_snii_index_event(SniiCommonGramsIndexEvent* event);
    void reset() override;
    const CommonWordSet& common_words() const { return *_common_words; }

private:
    bool read_input(CommonGramsBufferedToken* token, std::optional<bool>* is_common);
    std::string_view encode_plain_term(std::string_view term);
    std::string_view encode_snii_plain_term(std::string_view term);
    Token* emit_unigram(Token* token, const CommonGramsBufferedToken& buffered);
    Token* emit_gram(Token* token, int32_t start_offset, int32_t end_offset);

    std::shared_ptr<const CommonWordSet> _common_words;
    CommonGramsOutputMode _output_mode;
    Token _input_token;
    CommonGramsBufferedToken _current;
    CommonGramsBufferedToken _lookahead;
    std::optional<bool> _current_is_common;
    std::optional<bool> _lookahead_is_common;
    bool _has_current = false;
    bool _emit_current = false;
    std::string _gram;
    std::string _encoded_plain_term;
};

class CommonGramsPositionFilter final : public DorisTokenFilter {
public:
    explicit CommonGramsPositionFilter(TokenStreamPtr in) : DorisTokenFilter(std::move(in)) {}

    Token* next(Token* token) override;
};

enum class CommonGramsQueryMode {
    kExact,
    kPhrasePrefix,
};

// A false result proves that the purpose-specific query filter cannot select a
// gram. A true result stays conservative because key encoding may still force
// the filter to replay the complete plain stream.
bool common_grams_query_may_use_gram(std::span<const std::string> terms, CommonGramsQueryMode mode,
                                     const CommonWordSet& common_words);

class CommonGramsQueryFilterBase : public DorisTokenFilter {
public:
    CommonGramsQueryFilterBase(TokenStreamPtr in, std::shared_ptr<const CommonWordSet> common_words,
                               CommonGramsQueryMode mode);

    Token* next(Token* token) override;
    void reset() override;

private:
    void prepare_output();
    bool pair_uses_gram(const std::vector<CommonGramsBufferedToken>& unigrams,
                        std::optional<bool>* left_is_common, size_t pair_index) const;
    static void append_plain_output(std::vector<CommonGramsBufferedToken>* output,
                                    const CommonGramsBufferedToken& token);
    static bool append_gram_output(std::vector<CommonGramsBufferedToken>* output,
                                   const std::optional<CommonGramsBufferedToken>& indexed_gram,
                                   const CommonGramsBufferedToken& left,
                                   const CommonGramsBufferedToken& right);
    static Token* emit(Token* token, const CommonGramsBufferedToken& buffered);

    std::shared_ptr<const CommonWordSet> _common_words;
    CommonGramsQueryMode _mode;
    std::vector<CommonGramsBufferedToken> _output;
    size_t _next_output = 0;
    bool _prepared = false;
    std::exception_ptr _failure;
};

class CommonGramsQueryFilter final : public CommonGramsQueryFilterBase {
public:
    CommonGramsQueryFilter(TokenStreamPtr in, std::shared_ptr<const CommonWordSet> common_words)
            : CommonGramsQueryFilterBase(std::move(in), std::move(common_words),
                                         CommonGramsQueryMode::kExact) {}
};

class CommonGramsPhrasePrefixFilter final : public CommonGramsQueryFilterBase {
public:
    CommonGramsPhrasePrefixFilter(TokenStreamPtr in,
                                  std::shared_ptr<const CommonWordSet> common_words)
            : CommonGramsQueryFilterBase(std::move(in), std::move(common_words),
                                         CommonGramsQueryMode::kPhrasePrefix) {}
};

} // namespace doris::segment_v2::inverted_index
