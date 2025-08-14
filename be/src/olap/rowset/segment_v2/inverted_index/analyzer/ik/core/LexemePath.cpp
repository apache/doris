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

#include "LexemePath.h"

namespace doris::segment_v2 {
#include "common/compile_check_begin.h"

LexemePath::LexemePath(vectorized::Arena& arena)
        : QuickSortSet(arena), path_begin_(-1), path_end_(-1), payload_length_(0) {}

LexemePath::LexemePath(LexemePath& other, vectorized::Arena& arena)
        : QuickSortSet(arena),
          path_begin_(other.path_begin_),
          path_end_(other.path_end_),
          payload_length_(other.payload_length_) {
    auto* c = other.getHead();
    while (c != nullptr) {
        addLexeme(c->getLexeme());
        c = c->getNext();
    }
}

LexemePath::LexemePath(LexemePath&& other, vectorized::Arena& arena) noexcept
        : QuickSortSet(arena),
          path_begin_(other.path_begin_),
          path_end_(other.path_end_),
          payload_length_(other.payload_length_) {
    head_ = other.head_;
    tail_ = other.tail_;
    cell_size_ = other.cell_size_;

    other.head_ = nullptr;
    other.tail_ = nullptr;
    other.cell_size_ = 0;
    other.path_begin_ = -1;
    other.path_end_ = -1;
    other.payload_length_ = 0;
}

bool LexemePath::addCrossLexeme(Lexeme& lexeme) {
    if (isEmpty()) {
        addLexeme(lexeme);
        path_begin_ = lexeme.getByteBegin();
        path_end_ = lexeme.getByteBegin() + lexeme.getByteLength();
        payload_length_ += lexeme.getByteLength();
        return true;
    }

    if (checkCross(lexeme)) {
        addLexeme(lexeme);
        path_end_ = std::max(path_end_, lexeme.getByteBegin() + lexeme.getByteLength());
        payload_length_ = path_end_ - path_begin_;
        return true;
    }

    return false;
}

bool LexemePath::addNotCrossLexeme(Lexeme& lexeme) {
    if (isEmpty()) {
        addLexeme(lexeme);
        path_begin_ = lexeme.getByteBegin();
        path_end_ = lexeme.getByteBegin() + lexeme.getByteLength();
        payload_length_ += lexeme.getByteLength();
        return true;
    }

    if (checkCross(lexeme)) {
        return false;
    }

    addLexeme(lexeme);
    payload_length_ += lexeme.getByteLength();
    const auto* head = peekFirst();
    path_begin_ = head->getByteBegin();
    const auto* tail = peekLast();
    path_end_ = tail->getByteBegin() + tail->getByteLength();
    return true;
}
std::optional<Lexeme> LexemePath::removeTail() {
    auto tail = pollLast();
    if (!tail) {
        return nullopt;
    }

    if (isEmpty()) {
        path_begin_ = -1;
        path_end_ = -1;
        payload_length_ = 0;
    } else {
        payload_length_ -= tail->getByteLength();
        const auto* newTail = peekLast();
        if (newTail) {
            path_end_ = newTail->getByteBegin() + newTail->getByteLength();
        }
    }
    return tail;
}

bool LexemePath::checkCross(const Lexeme& lexeme) const {
    return (lexeme.getByteBegin() >= path_begin_ && lexeme.getByteBegin() < path_end_) ||
           (path_begin_ >= lexeme.getByteBegin() &&
            path_begin_ < lexeme.getByteBegin() + lexeme.getByteLength());
}

size_t LexemePath::getXWeight() const {
    size_t product = 1;
    const auto* c = getHead();
    while (c != nullptr) {
        product *= c->getLexeme().getByteLength();
        c = c->getNext();
    }
    return product;
}

size_t LexemePath::getPWeight() const {
    size_t pWeight = 0;
    size_t p = 0;
    const auto* c = getHead();
    while (c != nullptr) {
        p++;
        pWeight += p * c->getLexeme().getByteLength();
        c = c->getNext();
    }
    return pWeight;
}

bool LexemePath::operator<(const LexemePath& o) const {
    if (payload_length_ > o.payload_length_) return true;
    if (payload_length_ < o.payload_length_) return false;

    if (getSize() < o.getSize()) return true;
    if (getSize() > o.getSize()) return false;

    if (getPathLength() > o.getPathLength()) return true;
    if (getPathLength() < o.getPathLength()) return false;

    if (path_end_ > o.path_end_) return true;
    if (path_end_ < o.path_end_) return false;

    if (getXWeight() > o.getXWeight()) return true;
    if (getXWeight() < o.getXWeight()) return false;

    return getPWeight() > o.getPWeight();
}

bool LexemePath::operator==(const LexemePath& o) const {
    return path_begin_ == o.path_begin_ && path_end_ == o.path_end_ &&
           payload_length_ == o.payload_length_;
}

#include "common/compile_check_end.h"
} // namespace doris::segment_v2
