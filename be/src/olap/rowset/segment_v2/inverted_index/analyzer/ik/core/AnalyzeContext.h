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

#include <list>
#include <map>
#include <memory>
#include <optional>
#include <queue>
#include <set>
#include <string>

#include "CLucene/_ApiHeader.h"
#include "CLucene/util/CLStreams.h"
#include "CharacterUtil.h"
#include "LexemePath.h"
#include "common/logging.h"
#include "olap/rowset/segment_v2/inverted_index/analyzer/ik/cfg/Configuration.h"
#include "olap/rowset/segment_v2/inverted_index/analyzer/ik/dic/Dictionary.h"
#include "vec/common/arena.h"

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
    // Final tokenization result set
    std::queue<Lexeme> results_;
    // Tokenizer configuration
    std::shared_ptr<Configuration> config_;
    void outputSingleCJK(size_t index);
    void compound(Lexeme& lexeme);

public:
    enum class SegmenterType {
        CJK_SEGMENTER,
        CN_QUANTIFIER,
        LETTER_SEGMENTER,
        SURROGATE_PAIR_SEGMENTER
    };
    const CharacterUtil::TypedRuneArray& getTypedRuneArray() const { return typed_runes_; }
    explicit AnalyzeContext(vectorized::Arena& arena, std::shared_ptr<Configuration> config);
    virtual ~AnalyzeContext();

    void reset();

    size_t getCursor() const { return cursor_; }
    const char* getSegmentBuff() const { return segment_buff_.c_str(); }
    size_t getBufferOffset() const { return buffer_offset_; }
    size_t getLastUselessCharNum() const { return last_useless_char_num_; }

    size_t fillBuffer(lucene::util::Reader* reader);
    bool moveCursor();
    void initCursor();
    bool isBufferConsumed() const;
    bool needRefillBuffer() const;
    void markBufferOffset();

    void lockBuffer(SegmenterType type);
    void unlockBuffer(SegmenterType type);
    bool isBufferLocked() const;

    void addLexeme(Lexeme& lexeme);
    void addLexemePath(LexemePath* path);
    bool getNextLexeme(Lexeme& lexeme);
    QuickSortSet* getOrgLexemes() { return &org_lexemes_; }
    void outputToResult();

    size_t getCurrentCharLen() const {
        return cursor_ < typed_runes_.size() ? typed_runes_[cursor_].len : 0;
    }

    size_t getCurrentCharOffset() const {
        return cursor_ < typed_runes_.size() ? typed_runes_[cursor_].offset : 0;
    }

    int32_t getCurrentCharType() const { return typed_runes_[cursor_].char_type; }

    int32_t getCurrentChar() const {
        return cursor_ < typed_runes_.size() ? typed_runes_[cursor_].getChar() : 0;
    }
};

} // namespace doris::segment_v2
