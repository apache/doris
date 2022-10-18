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
#include "vec/common/arena.h"
#include "vec/common/columns_hashing.h"
#include "vec/common/hash_table/hash_map.h"
#include "vec/core/block.h"

namespace doris::vectorized {
/// Reference to the row in block.
struct RowRef {
    using SizeT = uint32_t; /// Do not use size_t cause of memory economy

    SizeT row_num = 0;
    uint8_t block_offset;

    RowRef() {}
    RowRef(size_t row_num_count, uint8_t block_offset_, bool is_visited = false)
            : row_num(row_num_count), block_offset(block_offset_) {}
};

/// Portion of RowRefs, 16 * (MAX_SIZE + 1) bytes sized.
struct Batch {
    static constexpr size_t MAX_SIZE = 7; /// Adequate values are 3, 7, 15, 31.

    RowRef::SizeT size = 0; /// It's smaller than size_t but keeps align in Arena.
    Batch* next;
    RowRef row_refs[MAX_SIZE];

    Batch(Batch* parent) : next(parent) {}

    bool full() const { return size == MAX_SIZE; }

    Batch* insert(RowRef&& row_ref, Arena& pool) {
        if (full()) {
            auto batch = pool.alloc<Batch>();
            *batch = Batch(this);
            batch->insert(std::move(row_ref), pool);
            return batch;
        }

        row_refs[size++] = std::move(row_ref);
        return this;
    }
};

template <typename RowRefListType>
class ForwardIterator {
public:
    ForwardIterator(RowRefListType* begin)
            : root(begin), first(true), batch(root->next), position(0) {}

    RowRef& operator*() {
        if (first) return *root;
        return batch->row_refs[position];
    }
    RowRef* operator->() { return &(**this); }

    bool operator==(const ForwardIterator<RowRefListType>& rhs) const {
        if (ok() != rhs.ok()) {
            return false;
        }
        if (first && rhs.first) {
            return true;
        }
        return batch == rhs.batch && position == rhs.position;
    }
    bool operator!=(const ForwardIterator<RowRefListType>& rhs) const { return !(*this == rhs); }

    void operator++() {
        if (first) {
            first = false;
            return;
        }

        if (batch) {
            ++position;
            if (position >= batch->size) {
                batch = batch->next;
                position = 0;
            }
        }
    }

    bool ok() const { return first || batch; }

    static ForwardIterator<RowRefListType> end() { return ForwardIterator(); }

private:
    RowRefListType* root;
    bool first;
    Batch* batch;
    size_t position;

    ForwardIterator() : root(nullptr), first(false), batch(nullptr), position(0) {}
};

/// Single linked list of references to rows. Used for ALL JOINs (non-unique JOINs)
struct RowRefList : RowRef {
    RowRefList() {}
    RowRefList(size_t row_num_, uint8_t block_offset_) : RowRef(row_num_, block_offset_) {}

    ForwardIterator<RowRefList> begin() { return ForwardIterator<RowRefList>(this); }
    static ForwardIterator<RowRefList> end() { return ForwardIterator<RowRefList>::end(); }

    /// insert element after current one
    void insert(RowRef&& row_ref, Arena& pool) {
        row_count++;

        if (!next) {
            next = pool.alloc<Batch>();
            *next = Batch(nullptr);
        }
        next = next->insert(std::move(row_ref), pool);
    }

    uint32_t get_row_count() { return row_count; }

private:
    friend class ForwardIterator<RowRefList>;

    Batch* next = nullptr;
    uint32_t row_count = 1;
};

/// Single linked list of references to rows. Used for ALL JOINs (non-unique JOINs)
struct RowRefListWithFlag : RowRef {
    RowRefListWithFlag() {}
    RowRefListWithFlag(size_t row_num_, uint8_t block_offset_) : RowRef(row_num_, block_offset_) {}

    ForwardIterator<RowRefListWithFlag> begin() {
        return ForwardIterator<RowRefListWithFlag>(this);
    }

    static ForwardIterator<RowRefListWithFlag> end() {
        return ForwardIterator<RowRefListWithFlag>::end();
    }

    /// insert element after current one
    void insert(RowRef&& row_ref, Arena& pool) {
        row_count++;

        if (!next) {
            next = pool.alloc<Batch>();
            *next = Batch(nullptr);
        }
        next = next->insert(std::move(row_ref), pool);
    }

    uint32_t get_row_count() { return row_count; }

    bool visited = false;

private:
    friend class ForwardIterator<RowRefListWithFlag>;

    Batch* next = nullptr;
    uint32_t row_count = 1;
};

} // namespace doris::vectorized
