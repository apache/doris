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

#include <vector>

#include "token_filter.h"
#include "word_delimiter_iterator.h"

namespace doris::segment_v2::inverted_index {
#include "common/compile_check_begin.h"

class WordDelimiterConcatenation;
using WordDelimiterConcatenationPtr = std::unique_ptr<WordDelimiterConcatenation>;

class WordDelimiterFilter : public DorisTokenFilter {
public:
    WordDelimiterFilter(const TokenStreamPtr& in, std::vector<char> char_type_table,
                        int32_t configuration_flags, std::unordered_set<std::string> prot_words);
    ~WordDelimiterFilter() override = default;

    Token* next(Token* t) override;
    void reset() override;

    static bool is_alpha(int32_t type) { return (type & ALPHA) != 0; }
    static bool is_digit(int32_t type) { return (type & DIGIT) != 0; }

    static constexpr int32_t LOWER = 0x01;
    static constexpr int32_t UPPER = 0x02;
    static constexpr int32_t DIGIT = 0x04;
    static constexpr int32_t SUBWORD_DELIM = 0x08;

    static constexpr int32_t ALPHA = 0x03;
    static constexpr int32_t ALPHANUM = 0x07;

    static constexpr int32_t GENERATE_WORD_PARTS = 1;
    static constexpr int32_t GENERATE_NUMBER_PARTS = 2;
    static constexpr int32_t CATENATE_WORDS = 4;
    static constexpr int32_t CATENATE_NUMBERS = 8;
    static constexpr int32_t CATENATE_ALL = 16;
    static constexpr int32_t PRESERVE_ORIGINAL = 32;
    static constexpr int32_t SPLIT_ON_CASE_CHANGE = 64;
    static constexpr int32_t SPLIT_ON_NUMERICS = 128;
    static constexpr int32_t STEM_ENGLISH_POSSESSIVE = 256;
    static constexpr int32_t IGNORE_KEYWORDS = 512;

private:
    bool has(int32_t flag) const { return (_flags & flag) != 0; }
    void save_state(const std::string_view& term);
    bool flush_concatenation(const WordDelimiterConcatenationPtr& concatenation);
    void buffer();
    void generate_part(bool is_single_word);
    bool should_concatenate(int32_t word_type);
    bool should_generate_parts(int32_t word_type);
    int32_t position(bool inject);
    void concatenate(const WordDelimiterConcatenationPtr& concatenation);

    struct Attribute {
        std::string buffered;
        int32_t start_off = 0;
        int32_t pos_inc = 0;
    };
    Attribute _attribute;

    int32_t _flags = 0;
    std::unordered_set<std::string> _prot_words;
    WordDelimiterIteratorPtr _iterator;

    WordDelimiterConcatenationPtr _concat;
    WordDelimiterConcatenationPtr _concat_all;

    int32_t _last_concat_count = 0;
    int32_t _accum_pos_inc = 0;

    std::string_view _saved_buffer;
    bool _has_saved_state = false;
    bool _has_output_token = false;
    bool _has_output_following_original = false;

    std::vector<Attribute> _states;
    int32_t _buffered_len = 0;
    int32_t _buffered_pos = 0;
    bool _first = false;

    friend class WordDelimiterConcatenation;
};

class WordDelimiterConcatenation {
public:
    WordDelimiterConcatenation(WordDelimiterFilter& filter) : _filter(filter) {};
    ~WordDelimiterConcatenation() = default;

    void append(const char* text, int32_t offset, int32_t length) {
        _buffer.append(text, offset, length);
        _subword_count++;
    }

    void write() {
        _filter._attribute.buffered = _buffer;
        _filter._attribute.start_off = _start_offset;
        _filter._attribute.pos_inc = _filter.position(true);
        _filter._accum_pos_inc = 0;
    }

    bool is_empty() { return _buffer.empty(); }

    void clear() {
        _buffer.clear();
        _start_offset = 0;
        _type = 0;
        _subword_count = 0;
    }

    void write_and_clear() {
        write();
        clear();
    }

    int32_t _subword_count = 0;
    int32_t _start_offset = 0;
    int32_t _type = 0;

private:
    WordDelimiterFilter& _filter;

    std::string _buffer;
};

#include "common/compile_check_end.h"
} // namespace doris::segment_v2::inverted_index