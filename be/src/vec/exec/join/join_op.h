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
/**
 * Now we have different kinds of RowRef for join operation. Overall, RowRef is the base class and
 * the class inheritance is below:
 *                                         RowRef
 *                                           |
 *          ---------------------------------------------------------
 *          |                          |                            |
 *   RowRefListWithFlag           RowRefList                 RowRefWithFlag
 *                                                                 |
 *                                                         RowRefListWithFlags
 *
 *  RowRef is a basic representation for a row which contains only row_num and block_offset.
 *
 *  RowRefList is a list of many RowRefs. It used for join operations which doesn't need any flags to represent whether a row has already been visited.
 *
 *  RowRefListWithFlag is a list of many RowRefs and an extra visited flag. It used for join operations which all RowRefs in a list has the same visited flag.
 *
 *  RowRefWithFlag is a basic representation for a row with an extra visited flag.
 *
 *  RowRefListWithFlags is a list of many RowRefWithFlags. This means each row will have different visited flags. It's used for join operation which has `other_conjuncts`.
 */
struct RowRef {
    uint32_t row_num = 0;
    uint8_t block_offset;

    RowRef() = default;
    RowRef(size_t row_num_count, uint8_t block_offset_)
            : row_num(row_num_count), block_offset(block_offset_) {}
};

struct RowRefWithFlag : public RowRef {
    bool visited;

    RowRefWithFlag() = default;
    RowRefWithFlag(size_t row_num_count, uint8_t block_offset_, bool is_visited = false)
            : RowRef(row_num_count, block_offset_), visited(is_visited) {}
};

/// Portion of RowRefs, 16 * (MAX_SIZE + 1) bytes sized.
template <typename RowRefType>
struct Batch {
    static constexpr uint32_t MAX_SIZE = 7; /// Adequate values are 3, 7, 15, 31.

    uint8_t size = 0; /// It's smaller than size_t but keeps align in Arena.
    Batch<RowRefType>* next;
    RowRefType row_refs[MAX_SIZE];

    Batch(Batch<RowRefType>* parent) : next(parent) {}

    bool full() const { return size == MAX_SIZE; }

    Batch<RowRefType>* insert(RowRefType&& row_ref, Arena& pool) {
        if (full()) {
            auto batch = pool.alloc<Batch<RowRefType>>();
            *batch = Batch<RowRefType>(this);
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
    using RowRefType = typename RowRefListType::RowRefType;
    ForwardIterator() : root(nullptr), first(false), batch(nullptr), position(0) {}

    ForwardIterator(RowRefListType* begin)
            : root(begin), first(true), batch(root->next), position(0) {}

    RowRefType& operator*() {
        if (first) {
            return *root;
        }
        return batch->row_refs[position];
    }
    RowRefType* operator->() { return &(**this); }

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

private:
    RowRefListType* root;
    bool first;
    Batch<RowRefType>* batch;
    size_t position;
};

struct RowRefList : RowRef {
    using RowRefType = RowRef;

    RowRefList() = default;
    RowRefList(size_t row_num_, uint8_t block_offset_) : RowRef(row_num_, block_offset_) {}

    ForwardIterator<RowRefList> begin() { return ForwardIterator<RowRefList>(this); }

    /// insert element after current one
    void insert(RowRefType&& row_ref, Arena& pool) {
        if (!next) {
            next = pool.alloc<Batch<RowRefType>>();
            *next = Batch<RowRefType>(nullptr);
        }
        next = next->insert(std::move(row_ref), pool);
    }

private:
    friend class ForwardIterator<RowRefList>;

    Batch<RowRefType>* next = nullptr;
};

struct RowRefListWithFlag : RowRef {
    using RowRefType = RowRef;

    RowRefListWithFlag() = default;
    RowRefListWithFlag(size_t row_num_, uint8_t block_offset_) : RowRef(row_num_, block_offset_) {}

    ForwardIterator<RowRefListWithFlag> const begin() {
        return ForwardIterator<RowRefListWithFlag>(this);
    }

    /// insert element after current one
    void insert(RowRef&& row_ref, Arena& pool) {
        if (!next) {
            next = pool.alloc<Batch<RowRefType>>();
            *next = Batch<RowRefType>(nullptr);
        }
        next = next->insert(std::move(row_ref), pool);
    }

    bool visited = false;

private:
    friend class ForwardIterator<RowRefListWithFlag>;

    Batch<RowRefType>* next = nullptr;
};

struct RowRefListWithFlags : RowRefWithFlag {
    using RowRefType = RowRefWithFlag;

    RowRefListWithFlags() = default;
    RowRefListWithFlags(size_t row_num_, uint8_t block_offset_)
            : RowRefWithFlag(row_num_, block_offset_) {}

    ForwardIterator<RowRefListWithFlags> const begin() {
        return ForwardIterator<RowRefListWithFlags>(this);
    }

    /// insert element after current one
    void insert(RowRefWithFlag&& row_ref, Arena& pool) {
        if (!next) {
            next = pool.alloc<Batch<RowRefType>>();
            *next = Batch<RowRefType>(nullptr);
        }
        next = next->insert(std::move(row_ref), pool);
    }

private:
    friend class ForwardIterator<RowRefListWithFlags>;

    Batch<RowRefType>* next = nullptr;
};

} // namespace doris::vectorized
