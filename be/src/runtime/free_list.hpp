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

#ifndef DORIS_BE_SRC_QUERY_BE_RUNTIME_FREE_LIST_H
#define DORIS_BE_SRC_QUERY_BE_RUNTIME_FREE_LIST_H

#include <stdio.h>
#include <string.h>
#include <algorithm>
#include <vector>
#include <string>
#include "common/logging.h"

namespace doris {

// Implementation of a free list:
// The free list is made up of nodes which contain a pointer to the next node
// and the size of the block.  The free list does not allocate any memory and instead
// overlays the node data over the caller-provided memory.
//
// Since the free list does not allocate or acquire any allocations, it needs to have
// the same lifetime as whereever the allocations came from (i.e MemPool).  If, for
// example, the underlying MemPool is deallocated, if there are any blocks in the
// free list from that pool, the free list is corrupt.

class FreeList {
public:
    // Returns the minimum allocation size that is compatible with
    // the free list.  The free list uses the allocations to maintain
    // its own internal linked list structure.
    static int min_size() {
        return sizeof(FreeListNode);
    }

    // C'tor, initializes the free list to be empty
    FreeList() {
        reset();
    }

    // Attempts to allocate a block that is at least the input size
    // from the free list.
    // Returns the size of the entire buffer in *buffer_size.
    // Returns NULL if there is no matching free list size.
    uint8_t* allocate(int size, int* buffer_size) {
        DCHECK(buffer_size != NULL);
        FreeListNode* prev = &_head;
        FreeListNode* node = _head.next;

        while (node != NULL) {
            if (node->size >= size) {
                prev->next = node->next;
                *buffer_size = node->size;
                return reinterpret_cast<uint8_t*>(node);
            }

            prev = node;
            node = node->next;
        }

        *buffer_size = 0;
        return NULL;
    }

    // add a block to the free list.  The caller can no longer touch
    // the memory.  If the size is too small, the free list ignores
    // the memory.
    void add(uint8_t* memory, int size) {
        if (size < FreeList::min_size()) {
            return;
        }

        FreeListNode* node = reinterpret_cast<FreeListNode*>(memory);
        node->next = _head.next;
        node->size = size;
        _head.next = node;
    }

    // Empties the free list
    void reset() {
        bzero(&_head, sizeof(FreeListNode));
    }

private:
    struct FreeListNode {
        FreeListNode* next;
        int size;
    };

    FreeListNode _head;
};

}

#endif
