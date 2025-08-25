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

#include "QuickSortSet.h"

namespace doris::segment_v2 {
#include "common/compile_check_begin.h"

QuickSortSet::~QuickSortSet() {
    clear();
}

void QuickSortSet::clear() {
    if (head_) {
        Cell* current = head_;
        while (current) {
            Cell* next = current->next_;
            current->~Cell();
            current = next;
        }

        head_ = nullptr;
        tail_ = nullptr;
        cell_size_ = 0;
    }
}

// Add a lexeme to the linked list set
bool QuickSortSet::addLexeme(Lexeme& lexeme) {
    Cell* new_cell = nullptr;
    if (cell_size_ == 0) {
        new_cell = allocateCell(std::move(lexeme));
        head_ = tail_ = new_cell;
        cell_size_++;
        return true;
    }
    if (lexeme < tail_->lexeme_) {
        new_cell = allocateCell(std::move(lexeme));
        tail_->next_ = new_cell;
        new_cell->prev_ = tail_;
        tail_ = new_cell;
        cell_size_++;
        return true;
    }
    if (head_->lexeme_ < lexeme) {
        new_cell = allocateCell(std::move(lexeme));
        head_->prev_ = new_cell;
        new_cell->next_ = head_;
        head_ = new_cell;
        cell_size_++;
        return true;
    }
    if (tail_->lexeme_ == lexeme) {
        // new_cell is nullptr here, no need to deallocate
        return false;
    }

    auto index = tail_;
    while (index && index->lexeme_ < lexeme) {
        index = index->prev_;
    }
    if (index && index->lexeme_ == lexeme) {
        return false;
    }
    new_cell = allocateCell(std::move(lexeme));
    new_cell->prev_ = index;
    if (auto next = index->next_) {
        new_cell->next_ = next;
        next->prev_ = new_cell;
    }
    index->next_ = new_cell;
    cell_size_++;
    return true;
}

const Lexeme* QuickSortSet::peekFirst() const {
    return head_ ? &head_->lexeme_ : nullptr;
}

std::optional<Lexeme> QuickSortSet::pollFirst() {
    if (!head_) return std::nullopt;
    Cell* old_head = head_;
    Lexeme result = std::move(old_head->getLexeme());

    head_ = head_->next_;
    if (head_)
        head_->prev_ = nullptr;
    else
        tail_ = nullptr;

    deallocateCell(old_head);
    --cell_size_;
    return result;
}

const Lexeme* QuickSortSet::peekLast() const {
    return tail_ ? &tail_->lexeme_ : nullptr;
}

std::optional<Lexeme> QuickSortSet::pollLast() {
    if (!tail_) return std::nullopt;
    Cell* old_tail = tail_;
    Lexeme result = std::move(old_tail->getLexeme());

    tail_ = tail_->prev_;
    if (tail_)
        tail_->next_ = nullptr;
    else
        head_ = nullptr;

    deallocateCell(old_tail);
    --cell_size_;
    return result;
}

size_t QuickSortSet::getPathBegin() const {
    return head_ ? head_->lexeme_.getByteBegin() : 0;
}

size_t QuickSortSet::getPathEnd() const {
    return tail_ ? tail_->lexeme_.getByteBegin() + tail_->lexeme_.getByteLength() : 0;
}

Cell* QuickSortSet::allocateCell(Lexeme&& lexeme) {
    Cell* cell = arena_.alloc<Cell>();
    return new (cell) Cell(std::move(lexeme));
}

void QuickSortSet::deallocateCell(Cell* cell) {
    if (cell) {
        cell->~Cell();
    }
}

#include "common/compile_check_end.h"
} // namespace doris::segment_v2
