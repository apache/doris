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

    // using union to represent two cases
    // 1. when RowRefList containing only one RowRef, visited + blockptr are valid
    // 2. when RowRefList contaning multi RowRef, it's used through next pointer
    union { 
        struct {
            uint64_t visited : 1;
            uint64_t blockptr : 63;
        };

        void* next; // save RowRefList::Batch* actually
    };

    uint32_t row_num = 0;

    Block* block() const {
        return (Block*)((uint64_t)blockptr << 1);
    }

    void block(Block* ptr) {
        DCHECK(((uint64_t)ptr & 0x0000000000000001) == 0); //the lowest bit must be 0, Arena.alloc() promised
        blockptr = ((uint64_t)ptr >> 1);
    }

    RowRef() {
        static_assert(sizeof(void*) == sizeof(uint64_t));
    }

    RowRef(const Block* ptr, size_t row_num, bool visited = false)
        : visited(visited), row_num(row_num) {
        block((Block*)ptr);
    }
};

/// Single linked list of references to rows. Used for ALL JOINs (non-unique JOINs)
struct RowRefList : RowRef {
    /// Portion of RowRefs, 16 * (MAX_SIZE + 1) bytes sized.
    struct Batch {
        static constexpr size_t MAX_SIZE = 7; /// Adequate values are 3, 7, 15, 31.

        Batch* next;
        RowRef row_refs[MAX_SIZE];
        SizeT size = 0; /// It's smaller than size_t but keeps align in Arena.

        Batch(Batch* parent) : next(parent) {}

        bool full() const { return size == MAX_SIZE; }

        Batch* insert(RowRef&& row_ref, Arena& pool) {
            if (full()) {
                auto batch = new (pool.alloc<Batch>()) Batch(this);
                batch->insert(std::move(row_ref), pool);
                return batch;
            }

            row_refs[size++] = std::move(row_ref);
            return this;
        }
    };

    class ForwardIterator {
    public:
        ForwardIterator(RowRefList* begin) : root(begin), batch((Batch*)root->next), row_count(root->row_count) {}

        RowRef& operator*() { // the user is guaranteed to call ok() before calling this function
            if (row_count == 1) return *root;
            return batch->row_refs[position];
        }

        RowRef* operator->() { return &(**this); }

        bool operator==(const ForwardIterator& rhs) const = delete;
        bool operator!=(const ForwardIterator& rhs) const = delete;

        void operator++() {
            if (batch) {
                ++position;
                if (position >= batch->size) {
                    batch = batch->next;
                    position = 0;
                }
            }
            ++iterate_count;
        }

        bool ok() const { return iterate_count < row_count; }

        static ForwardIterator end() { return ForwardIterator(); }

    private:
        RowRefList* root = nullptr;
        Batch* batch = nullptr;
        size_t position = 0;
        size_t row_count = 0;
        size_t iterate_count = 0;

        ForwardIterator() = default;
    };

    RowRefList() {}
    RowRefList(const Block* block_, size_t row_num_) : RowRef(block_, row_num_) {}

    ForwardIterator begin() { return ForwardIterator(this); }
    ForwardIterator end() { return ForwardIterator::end(); }

    /// insert element after current one
    void insert(RowRef&& row_ref, Arena& pool) {
        if (row_count == 1) {
            auto batch = new (pool.alloc<Batch>()) Batch(nullptr);
            // copy the only RowRef to batch as the first element
            batch->row_refs[batch->size++] = std::move(*this);
            this->next = batch;
        }
        next = ((Batch*)next)->insert(std::move(row_ref), pool);
        row_count++;
    }

    uint32_t get_row_count() const { return row_count; }

private:
    uint32_t row_count = 1;
};

// using MapI32 = doris::vectorized::HashMap<UInt32, MappedAll, HashCRC32<UInt32>>;
// using I32KeyType = doris::vectorized::ColumnsHashing::HashMethodOneNumber<MapI32::value_type, MappedAll, UInt32, false>;
} // namespace doris::vectorized
