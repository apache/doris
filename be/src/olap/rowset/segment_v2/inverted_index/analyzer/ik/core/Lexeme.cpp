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

#include "Lexeme.h"

namespace doris::segment_v2 {
#include "common/compile_check_begin.h"

Lexeme::Lexeme(size_t offset, size_t byte_begin, size_t byte_length, Type type, size_t char_begin,
               size_t char_end)
        : offset_(offset),
          byte_begin_(byte_begin),
          byte_length_(byte_length),
          char_begin_(char_begin),
          char_end_(char_end),
          type_(type) {}

void Lexeme::setLength(size_t length) {
    if (length == 0) {
        throw std::invalid_argument("Lexeme length cannot be zero");
    }
    this->byte_length_ = length;
}

void Lexeme::setText(std::string&& text) {
    if (text.empty()) {
        this->text_.clear();
        return;
    }
    this->text_ = std::move(text);
}

bool Lexeme::append(const Lexeme& other, Type new_type) {
    if (this->getByteEndPosition() == other.getByteBeginPosition()) {
        byte_length_ += other.getByteLength();
        char_end_ = other.getCharEnd();
        type_ = new_type;
        return true;
    }
    return false;
}

std::string Lexeme::toString() const {
    std::ostringstream oss;
    oss << getByteBeginPosition() << '-' << getByteEndPosition() << " : " << text_;
    return oss.str();
}

bool Lexeme::operator==(const Lexeme& other) const noexcept {
    return offset_ == other.offset_ && byte_begin_ == other.byte_begin_ &&
           byte_length_ == other.byte_length_;
}

bool Lexeme::operator<(const Lexeme& other) const noexcept {
    return byte_begin_ > other.byte_begin_ ||
           (byte_begin_ == other.byte_begin_ && byte_length_ < other.byte_length_);
}

#include "common/compile_check_end.h"
} // namespace doris::segment_v2
