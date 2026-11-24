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

#include <parallel_hashmap/phmap.h>

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "common/logging.h"
#include "core/arena.h"
#include "storage/index/inverted/analysis/analyzer/ik/cfg/Configuration.h"
#include "storage/index/inverted/analysis/analyzer/ik/core/CharacterUtil.h"
#include "storage/index/inverted/analysis/analyzer/ik/core/LexemePath.h"
#include "storage/index/inverted/analysis/analyzer/ik/dic/Dictionary.h"
#include "storage/index/inverted/analysis/common/text_cursor.h"

namespace doris::segment_v2 {

class AnalyzeContext {
private:
    static const size_t BUFF_SIZE = 4096;
    static const size_t BUFF_EXHAUST_CRITICAL = 100;

    static constexpr uint8_t CJK_SEGMENTER_FLAG = 0x01;            // 0001
    static constexpr uint8_t CN_QUANTIFIER_FLAG = 0x02;            // 0010
    static constexpr uint8_t LETTER_SEGMENTER_FLAG = 0x04;         // 0100
    static constexpr uint8_t SURROGATE_PAIR_SEGMENTER_FLAG = 0x08; // 1000
    // String buffer
    std::string segment_buff_;
    // An array storing Unicode code points (runes)Character information array
    CharacterUtil::TypedRuneArray typed_runes_;
    // Total length of bytes analyzed
    size_t buffer_offset_;
    // Current character position pointer
    size_t cursor_;
    // Length of available bytes in the last read
    size_t available_;
    // Number of non-CJK characters at the end
    size_t last_useless_char_num_;

    // Sub-tokenizer lock
    uint8_t buffer_locker_ {0};
    //std::set<std::string> buffer_locker_;
    // Original tokenization result set
    QuickSortSet org_lexemes_;
    // LexemePath position index table
    phmap::flat_hash_map<size_t, LexemePath*> path_map_;
    // The final lexemes of the window, which outputToResult() fills and getNextLexeme() reads
    // from results_head_ on; reused from window to window and value to value.
    std::vector<Lexeme> results_;
    size_t results_head_ {0};
    // Tokenizer configuration
    std::shared_ptr<Configuration> config_;
    // The loaded dictionaries, for the stop words.
    const Dictionary& dictionary_;
    void outputSingleCJK(size_t index);
    void compound(Lexeme& lexeme);
    bool hasResult() const { return results_head_ < results_.size(); }

public:
    enum class SegmenterType {
        CJK_SEGMENTER,
        CN_QUANTIFIER,
        LETTER_SEGMENTER,
        SURROGATE_PAIR_SEGMENTER
    };
    const CharacterUtil::TypedRuneArray& getTypedRuneArray() const { return typed_runes_; }
    AnalyzeContext(Arena& arena, std::shared_ptr<Configuration> config,
                   const Dictionary& dictionary);
    virtual ~AnalyzeContext();

    void reset();

    size_t getCursor() const { return cursor_; }
    const char* getSegmentBuff() const { return segment_buff_.c_str(); }
    size_t getBufferOffset() const { return buffer_offset_; }
    size_t getLastUselessCharNum() const { return last_useless_char_num_; }

    size_t fillBuffer(inverted_index::TextCursor& cursor);
    // The read window, the runes decoded from it and the window's final lexemes, all bounded by
    // BUFF_SIZE.
    size_t reserved_bytes() const {
        return segment_buff_.capacity() +
               typed_runes_.capacity() * sizeof(CharacterUtil::TypedRune) +
               results_.capacity() * sizeof(Lexeme);
    }
    // The cursor and lock helpers below run several times per character, from every segmenter,
    // so they are defined here, where the segmenters can inline them.
    bool moveCursor() {
        if (cursor_ < typed_runes_.size() - 1) {
            cursor_++;
            return true;
        }
        return false;
    }
    void initCursor() { cursor_ = 0; }
    bool isBufferConsumed() const { return cursor_ == typed_runes_.size() - 1; }
    bool needRefillBuffer() const {
        return available_ == BUFF_SIZE && !isBufferLocked() && cursor_ < typed_runes_.size() - 1 &&
               cursor_ > typed_runes_.size() - BUFF_EXHAUST_CRITICAL;
    }
    void markBufferOffset();

    void lockBuffer(SegmenterType type) {
        switch (type) {
        case SegmenterType::CJK_SEGMENTER:
            buffer_locker_ |= CJK_SEGMENTER_FLAG;
            break;
        case SegmenterType::CN_QUANTIFIER:
            buffer_locker_ |= CN_QUANTIFIER_FLAG;
            break;
        case SegmenterType::LETTER_SEGMENTER:
            buffer_locker_ |= LETTER_SEGMENTER_FLAG;
            break;
        case SegmenterType::SURROGATE_PAIR_SEGMENTER:
            buffer_locker_ |= SURROGATE_PAIR_SEGMENTER_FLAG;
            break;
        }
    }
    void unlockBuffer(SegmenterType type) {
        switch (type) {
        case SegmenterType::CJK_SEGMENTER:
            buffer_locker_ &= ~CJK_SEGMENTER_FLAG;
            break;
        case SegmenterType::CN_QUANTIFIER:
            buffer_locker_ &= ~CN_QUANTIFIER_FLAG;
            break;
        case SegmenterType::LETTER_SEGMENTER:
            buffer_locker_ &= ~LETTER_SEGMENTER_FLAG;
            break;
        case SegmenterType::SURROGATE_PAIR_SEGMENTER:
            buffer_locker_ &= ~SURROGATE_PAIR_SEGMENTER_FLAG;
            break;
        }
    }
    bool isBufferLocked() const { return buffer_locker_ != 0; }

    void addLexeme(Lexeme& lexeme);
    void addLexemePath(LexemePath* path);
    // The next final lexeme of the window that is no stop word, or false once there is none.
    bool getNextLexeme(Lexeme& lexeme);
    // The bytes of a lexeme of the current window; valid until the window is refilled.
    std::string_view getText(const Lexeme& lexeme) const {
        return {segment_buff_.data() + lexeme.getByteBegin(), lexeme.getByteLength()};
    }
    QuickSortSet* getOrgLexemes() { return &org_lexemes_; }
    void outputToResult();

    size_t getCurrentCharLen() const {
        return cursor_ < typed_runes_.size() ? typed_runes_[cursor_].len : 0;
    }

    size_t getCurrentCharOffset() const {
        return cursor_ < typed_runes_.size() ? typed_runes_[cursor_].offset : 0;
    }

    CharacterUtil::CharType getCurrentCharType() const { return typed_runes_[cursor_].char_type; }

    int32_t getCurrentChar() const {
        return cursor_ < typed_runes_.size() ? typed_runes_[cursor_].getChar() : 0;
    }
};

} // namespace doris::segment_v2
