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
#include <optional>

#include "CLucene/_ApiHeader.h"
#include "CLucene/util/Misc.h"
#include "Lexeme.h"
#include "vec/common/arena.h"
namespace doris::segment_v2 {

class Cell {
public:
    Cell() = default;
    explicit Cell(Lexeme&& lexeme) : lexeme_(std::move(lexeme)) {}
    ~Cell() = default;

    bool operator<(const Cell& other) const { return lexeme_ < other.lexeme_; };
    bool operator==(const Cell& other) const { return lexeme_ == other.lexeme_; };

    Cell* getPrev() const { return prev_; }
    Cell* getNext() const { return next_; }
    const Lexeme& getLexeme() const { return lexeme_; }
    Lexeme& getLexeme() { return lexeme_; }

private:
    Cell* next_ = nullptr;
    Cell* prev_ = nullptr;
    Lexeme lexeme_;

    friend class QuickSortSet;
};

// IK Segmenter's Lexeme Quick Sort Set
class QuickSortSet {
    friend class IKArbitrator;

protected:
    Cell* head_ = nullptr;
    Cell* tail_ = nullptr;
    size_t cell_size_ = 0;

public:
    vectorized::Arena& arena_;

    QuickSortSet(vectorized::Arena& arena) : arena_(arena) {}
    virtual ~QuickSortSet();

    QuickSortSet(const QuickSortSet&) = delete;
    QuickSortSet& operator=(const QuickSortSet&) = delete;

    bool addLexeme(Lexeme& lexeme);
    const Lexeme* peekFirst() const;
    std::optional<Lexeme> pollFirst();
    const Lexeme* peekLast() const;
    std::optional<Lexeme> pollLast();
    void clear();

    size_t getPathBegin() const;
    size_t getPathEnd() const;

    size_t getSize() const { return cell_size_; }
    bool isEmpty() const { return cell_size_ == 0; }
    Cell* getHead() { return head_; }
    const Cell* getHead() const { return head_; }

private:
    Cell* allocateCell(Lexeme&& lexeme);
    void deallocateCell(Cell* cell);
};

} // namespace doris::segment_v2
