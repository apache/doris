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

#include "LetterSegmenter.h"

namespace doris::segment_v2 {

LetterSegmenter::LetterSegmenter()
        : letter_connectors_ {'#', '&', '+', '-', '.', '@', '_'}, num_connectors_ {',', '.'} {
    std::sort(std::begin(letter_connectors_), std::end(letter_connectors_));
    std::sort(std::begin(num_connectors_), std::end(num_connectors_));
}

void LetterSegmenter::analyze(AnalyzeContext& context) {
    bool bufferLockFlag = false;
    // Process English letters
    bufferLockFlag = processEnglishLetter(context) || bufferLockFlag;
    // Process Arabic letters
    bufferLockFlag = processArabicLetter(context) || bufferLockFlag;
    // Handle mixed letters (this should be processed last, duplicates can be excluded through QuickSortSet)
    bufferLockFlag = processMixLetter(context) || bufferLockFlag;

    if (bufferLockFlag) {
        context.lockBuffer(LetterSegmenter::SEGMENTER_TYPE);
    } else {
        context.unlockBuffer(LetterSegmenter::SEGMENTER_TYPE);
    }
}

void LetterSegmenter::reset() {
    start_ = -1;
    end_ = -1;
    english_start_ = -1;
    english_end_ = -1;
    arabic_start_ = -1;
    arabic_end_ = -1;
}

bool LetterSegmenter::processEnglishLetter(AnalyzeContext& context) {
    bool need_lock = false;

    const auto& typed_runes = context.getTypedRuneArray();
    if (english_start_ == -1) {
        // The current tokenizer has not yet started processing English characters
        if (context.getCurrentCharType() == CharacterUtil::CHAR_ENGLISH) {
            // Record the starting pointer position, indicate that the tokenizer enters the processing state
            english_start_ = context.getCursor();
            english_end_ = english_start_;
        }
    } else {
        // The current tokenizer is processing English characters
        if (context.getCurrentCharType() == CharacterUtil::CHAR_ENGLISH) {
            // Record the current pointer position as the end position
            english_end_ = context.getCursor();
        } else {
            // Encounter non-English characters, output tokens
            Lexeme newLexeme(context.getBufferOffset(), typed_runes[english_start_].offset,
                             english_end_ - english_start_ + 1, Lexeme::Type::English,
                             english_start_, english_end_);
            context.addLexeme(newLexeme);
            english_start_ = -1;
            english_end_ = -1;
        }
    }

    if (context.isBufferConsumed() && (english_start_ != -1 && english_end_ != -1)) {
        Lexeme newLexeme(context.getBufferOffset(), typed_runes[english_start_].offset,
                         english_end_ - english_start_ + 1, Lexeme::Type::English, english_start_,
                         english_end_);
        context.addLexeme(newLexeme);
        english_start_ = -1;
        english_end_ = -1;
    }

    if (english_start_ == -1 && english_end_ == -1) {
        need_lock = false;
    } else {
        need_lock = true;
    }
    return need_lock;
}

bool LetterSegmenter::processArabicLetter(AnalyzeContext& context) {
    bool need_lock = false;
    const auto& typed_runes = context.getTypedRuneArray();

    if (arabic_start_ == -1) {
        // The current tokenizer has not yet started processing numeric characters
        if (context.getCurrentCharType() == CharacterUtil::CHAR_ARABIC) {
            // Record the starting pointer position, indicate that the tokenizer enters the processing state
            arabic_start_ = context.getCursor();
            arabic_end_ = arabic_start_;
        }
    } else {
        // The current tokenizer is processing numeric characters
        if (context.getCurrentCharType() == CharacterUtil::CHAR_ARABIC) {
            // Record the current pointer position as the end position
            arabic_end_ = context.getCursor();
        } else if (context.getCurrentCharType() == CharacterUtil::CHAR_USELESS &&
                   isNumConnector(context.getCurrentChar())) {
            // Do not output numbers, but do not mark the end
        } else {
            // Encounter non-Arabic characters, output tokens
            Lexeme newLexeme(context.getBufferOffset(), typed_runes[arabic_start_].offset,
                             arabic_end_ - arabic_start_ + 1, Lexeme::Type::Arabic, arabic_start_,
                             arabic_end_);
            context.addLexeme(newLexeme);
            arabic_start_ = -1;
            arabic_end_ = -1;
        }
    }

    if (context.isBufferConsumed() && (arabic_start_ != -1 && arabic_end_ != -1)) {
        Lexeme newLexeme(context.getBufferOffset(), typed_runes[arabic_start_].offset,
                         arabic_end_ - arabic_start_ + 1, Lexeme::Type::Arabic, arabic_start_,
                         arabic_end_);
        context.addLexeme(newLexeme);
        arabic_start_ = -1;
        arabic_end_ = -1;
    }

    if (arabic_start_ == -1 && arabic_end_ == -1) {
        need_lock = false;
    } else {
        need_lock = true;
    }
    return need_lock;
}

bool LetterSegmenter::processMixLetter(AnalyzeContext& context) {
    bool need_lock = false;
    const auto& typed_runes = context.getTypedRuneArray();

    if (start_ == -1) {
        // The current tokenizer has not yet started processing characters.
        if (context.getCurrentCharType() == CharacterUtil::CHAR_ARABIC ||
            context.getCurrentCharType() == CharacterUtil::CHAR_ENGLISH) {
            start_ = context.getCursor();
            end_ = start_;
        }
    } else {
        // The current tokenizer is processing characters
        if (context.getCurrentCharType() == CharacterUtil::CHAR_ARABIC ||
            context.getCurrentCharType() == CharacterUtil::CHAR_ENGLISH) {
            // Record the possible end positions
            end_ = context.getCursor();
        } else if (context.getCurrentCharType() == CharacterUtil::CHAR_USELESS &&
                   isLetterConnector(context.getCurrentChar())) {
            // Record the possible end positions
            end_ = context.getCursor();
        } else {
            // Encounter non-letter characters, output a token
            Lexeme newLexeme(context.getBufferOffset(), typed_runes[start_].offset,
                             end_ - start_ + 1, Lexeme::Type::Letter, start_, end_);
            context.addLexeme(newLexeme);
            start_ = -1;
            end_ = -1;
        }
    }

    if (context.isBufferConsumed() && (start_ != -1 && end_ != -1)) {
        Lexeme newLexeme(context.getBufferOffset(), typed_runes[start_].offset, end_ - start_ + 1,
                         Lexeme::Type::Letter, start_, end_);
        context.addLexeme(newLexeme);
        start_ = -1;
        end_ = -1;
    }

    need_lock = (start_ != -1 && end_ != -1);
    return need_lock;
}

bool LetterSegmenter::isLetterConnector(char input) {
    return std::binary_search(std::begin(letter_connectors_), std::end(letter_connectors_), input);
}

bool LetterSegmenter::isNumConnector(char input) {
    return std::binary_search(std::begin(num_connectors_), std::end(num_connectors_), input);
}
} // namespace doris::segment_v2
