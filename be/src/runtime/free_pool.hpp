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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/runtime/free-pool.hpp
// and modified by Doris

#ifndef DORIS_BE_SRC_QUERY_BE_RUNTIME_FREE_POOL_H
#define DORIS_BE_SRC_QUERY_BE_RUNTIME_FREE_POOL_H

#include <stdio.h>
#include <string.h>

#include <string>

#include "common/logging.h"
#include "runtime/mem_pool.h"
#include "util/bit_util.h"

namespace doris {

// Implementation of a free pool to recycle allocations. The pool is broken
// up into 64 lists, one for each power of 2. Each allocation is rounded up
// to the next power of 2. When the allocation is freed, it is added to the
// corresponding free list.
// Each allocation has an 8 byte header that immediately precedes the actual
// allocation. If the allocation is owned by the user, the header contains
// the ptr to the list that it should be added to on Free().
// When the allocation is in the pool (i.e. available to be handed out), it
// contains the link to the next allocation.
// This has O(1) Allocate() and Free().
// This is not thread safe.
// TODO(zxy): consider integrating this with MemPool.
// TODO: consider changing to something more granular than doubling.
class FreePool {
public:
    // C'tor, initializes the FreePool to be empty. All allocations come from the
    // 'mem_pool'.
    FreePool(MemPool* mem_pool) : _mem_pool(mem_pool) { memset(&_lists, 0, sizeof(_lists)); }

    virtual ~FreePool() {}

    // Allocates a buffer of size.
    uint8_t* allocate(int64_t size) {
        // This is the typical malloc behavior. NULL is reserved for failures.
        if (size == 0) {
            return reinterpret_cast<uint8_t*>(0x1);
        }

        // Do ceil(log_2(size))
        int free_list_idx = BitUtil::log2(size);
        DCHECK_LT(free_list_idx, NUM_LISTS);

        FreeListNode* allocation = _lists[free_list_idx].next;

        if (allocation == NULL) {
            // There wasn't an existing allocation of the right size, allocate a new one.
            size = 1L << free_list_idx;
            allocation = reinterpret_cast<FreeListNode*>(
                    _mem_pool->allocate(size + sizeof(FreeListNode)));
        } else {
            // Remove this allocation from the list.
            _lists[free_list_idx].next = allocation->next;
        }

        DCHECK(allocation != NULL);
        // Set the back node to point back to the list it came from so know where
        // to add it on free().
        allocation->list = &_lists[free_list_idx];
        return reinterpret_cast<uint8_t*>(allocation) + sizeof(FreeListNode);
    }

    void free(uint8_t* ptr) {
        if (ptr == NULL || reinterpret_cast<int64_t>(ptr) == 0x1) {
            return;
        }

        FreeListNode* node = reinterpret_cast<FreeListNode*>(ptr - sizeof(FreeListNode));
        FreeListNode* list = node->list;
#ifndef NDEBUG
        check_valid_allocation(list);
#endif
        // Add node to front of list.
        node->next = list->next;
        list->next = node;
    }

    // Returns an allocation that is at least 'size'. If the current allocation
    // backing 'ptr' is big enough, 'ptr' is returned. Otherwise a new one is
    // made and the contents of ptr are copied into it.
    uint8_t* reallocate(uint8_t* ptr, int64_t size) {
        if (ptr == NULL || reinterpret_cast<int64_t>(ptr) == 0x1) {
            return allocate(size);
        }

        FreeListNode* node = reinterpret_cast<FreeListNode*>(ptr - sizeof(FreeListNode));
        FreeListNode* list = node->list;
#ifndef NDEBUG
        check_valid_allocation(list);
#endif
        int bucket_idx = (list - &_lists[0]);
        // This is the actual size of ptr.
        int allocation_size = 1 << bucket_idx;

        // If it's already big enough, just return the ptr.
        if (allocation_size >= size) {
            return ptr;
        }

        // Make a new one. Since allocate() already rounds up to powers of 2, this
        // effectively doubles for the caller.
        uint8_t* new_ptr = allocate(size);
        memcpy(new_ptr, ptr, allocation_size);
        free(ptr);
        return new_ptr;
    }

private:
    static const int NUM_LISTS = 64;

    struct FreeListNode {
        // Union for clarity when manipulating the node.
        union {
            FreeListNode* next; // Used when it is in the free list
            FreeListNode* list; // Used when it is being used by the caller.
        };
    };

    void check_valid_allocation(FreeListNode* computed_list_ptr) {
        // On debug, check that list is valid.
        bool found = false;

        for (int i = 0; i < NUM_LISTS && !found; ++i) {
            if (computed_list_ptr == &_lists[i]) {
                found = true;
            }
        }

        DCHECK(found);
    }

    // MemPool to allocate from. Unowned.
    MemPool* _mem_pool;

    // One list head for each allocation size indexed by the LOG_2 of the allocation size.
    // While it doesn't make too much sense to use this for very small (e.g. 8 byte)
    // allocations, it makes the indexing easy.
    FreeListNode _lists[NUM_LISTS];
};

} // namespace doris

#endif
