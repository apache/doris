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

#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>

#include "CLucene/_ApiHeader.h"
#include "CharacterUtil.h"

namespace doris::segment_v2 {

class Lexeme {
public:
    enum class Type {
        // Unknown
        Unknown = 0,
        // English
        English = 1,
        // Number
        Arabic = 2,
        // Mixed English and numbers
        Letter = 3,
        // Chinese word
        CNWord = 4,
        // Single Chinese character
        CNChar = 64,
        // CJK characters (Japanese and Korean)
        OtherCJK = 8,
        // Chinese numeric word
        CNum = 16,
        // Chinese measure word
        Count = 32,
        // Chinese numeric-measure compound
        CQuan = 48
    };
    Lexeme()
            : offset_(0),
              byte_begin_(0),
              byte_length_(0),
              char_begin_(0),
              char_end_(0),
              type_(Type::Unknown) {}
    explicit Lexeme(size_t offset, size_t begin, size_t length, Type type, size_t charBegin,
                    size_t charEnd);
    void reset() {
        offset_ = 0;
        byte_begin_ = 0;
        byte_length_ = 0;
        char_begin_ = 0;
        char_end_ = 0;
        text_.clear();
        type_ = Type::Unknown;
    }
    bool append(const Lexeme& other, Type newType);

    size_t getOffset() const noexcept { return offset_; }
    size_t getByteBegin() const noexcept { return byte_begin_; }
    size_t getCharBegin() const noexcept { return char_begin_; }
    size_t getByteLength() const noexcept { return byte_length_; }
    size_t getCharLength() const noexcept { return char_end_ - char_begin_ + 1; }
    Type getType() const noexcept { return type_; }
    std::string getText() noexcept { return std::move(text_); }
    size_t getCharEnd() const noexcept { return char_end_; }

    size_t getByteBeginPosition() const noexcept { return offset_ + byte_begin_; }
    size_t getByteEndPosition() const noexcept { return offset_ + byte_begin_ + byte_length_; }

    void setLength(size_t length);
    void setType(Type type) noexcept { this->type_ = type; }
    void setText(std::string&& text);

    std::string toString() const;

    bool operator==(const Lexeme& other) const noexcept;
    bool operator<(const Lexeme& other) const noexcept;

private:
    size_t offset_;      // Byte offset
    size_t byte_begin_;  // Byte start position
    size_t byte_length_; // Lexeme length
    size_t char_begin_;  // Lexeme character start
    size_t char_end_;    // Lexeme character end
    std::string text_;   // UTF-8 text
    Type type_;          // Lexeme type
};

} // namespace doris::segment_v2
