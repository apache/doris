// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef BDG_PALO_BE_SRC_RPC_STATIC_BUFFER_H
#define BDG_PALO_BE_SRC_RPC_STATIC_BUFFER_H

#include "dynamic_buffer.h"
#include <cstdlib>
#include <memory>
#include <assert.h>

namespace palo {
/** A memory buffer of static size. The actual buffer can be allocated or
 * assigned by the caller. If the StaticBuffer "owns" the pointer then it
 * will be released when going out of scope.
 */
class StaticBuffer {
public:

    /** Constructor.  Creates an empty buffer */
    StaticBuffer()
        : base(0), alignment(0), size(0), own(true) {
        }

    /** Constructor.
     * Allocates a new buffer of size <code>len</code>. If
     * <code>alignment</code> is non-zero, then the posix_memalign() function
     * will be called to obtain memory and #alignment is set to
     * <code>alignment</code>.  Memory will be released when going out of scope.
     *
     * @param len The size of the new buffer, in bytes
     * @param alignment Buffer alignment
     */
    explicit StaticBuffer(size_t len, size_t alignment=0)
        : alignment(alignment), size(len), own(true) {
            if (alignment > 0) {
                void *vptr = 0;
                size_t aligned_len = (len % alignment) == 0 ? len :
                    ((len / alignment)+1)*alignment;
                assert(posix_memalign(&vptr, alignment, aligned_len) == 0);
                base = (uint8_t *)vptr;
            }
            else
                base = new uint8_t[len];
        }

    /** Constructor; assigns an existing buffer and can take ownership of
     * that buffer.
     *
     * @param data Pointer to the existing buffer
     * @param len Size of the existing buffer
     * @param take_ownership If yes, will "own" the existing buffer and
     *      delete[] the memory when going out of scope. Make sure that the
     *      buffer was allocated with new[]!
     */
    StaticBuffer(void *data, uint32_t len, bool take_ownership = true)
        : base((uint8_t *)data), alignment(0), size(len), own(take_ownership) {
        }

    /** Constructor; takes ownership from a DynamicBuffer */
    StaticBuffer(DynamicBuffer &dbuf) {
        base = dbuf.base;
        size = dbuf.fill();
        own = dbuf.own;
        alignment = 0;
        if (own) {
            dbuf.base = dbuf.ptr = 0;
            dbuf.size = 0;
        }
    }

    /** Destructor; if "own" is true then the buffer will be delete[]d */
    ~StaticBuffer() {
        free();
    }

    /**
     * Copy constructor.
     *
     * WARNING: This assignment operator will cause the ownership of the buffer
     * to transfer to the lvalue buffer if the own flag is set to 'true' in the
     * buffer being copied.  The buffer being copied will be modified to have
     * it's 'own' flag set to false and the 'base' pointer will be set to NULL.
     * In other words, the buffer being copied is no longer usable after the
     * assignment.
     *
     * @param other Reference to the original instance
     */
    StaticBuffer(StaticBuffer& other) {
        base = other.base;
        size = other.size;
        own = other.own;
        alignment = other.alignment;
        if (own) {
            other.own = false;
            other.base = 0;
        }
    }

    /**
     * Assignment operator.
     *
     * WARNING: This assignment operator will cause the ownership of the buffer
     * to transfer to the lvalue buffer if the own flag is set to 'true' in the
     * buffer being copied.  The buffer being copied will be modified to have
     * it's 'own' flag set to false and the 'base' pointer will be set to NULL.
     * In other words, the buffer being copied is no longer usable after the
     * assignment.
     *
     * @param other Reference to the original instance
     */
    StaticBuffer &operator=(StaticBuffer &other) {
        base = other.base;
        size = other.size;
        own = other.own;
        alignment = other.alignment;
        if (own) {
            other.own = false;
            other.base = 0;
        }
        return *this;
    }

    /** Assignment operator for DynamicBuffer
     *
     * @param dbuf Reference to the original DynamicBuffer instance
     */
    StaticBuffer &operator=(DynamicBuffer &dbuf) {
        base = dbuf.base;
        size = dbuf.fill();
        own = dbuf.own;
        alignment = 0;
        if (own) {
            dbuf.base = dbuf.ptr = 0;
            dbuf.size = 0;
        }
        return *this;
    }

    /** Sets data pointer; the existing buffer is discarded and deleted
     *
     * @param data Pointer to the existing buffer
     * @param len Size of the existing buffer
     * @param take_ownership If yes, will "own" the existing buffer and
     *      delete[] the memory when going out of scope. Make sure that the
     *      buffer was allocated with new[]!
     */
    void set(uint8_t *data, uint32_t len, bool take_ownership = true) {
        free();
        base = data;
        size = len;
        own = take_ownership;
        alignment = 0;
    }

    /** Clears the data; if this object is owner of the data then the allocated
     * buffer is delete[]d */
    void free() {
        if (own && base) {
            if (alignment > 0)
                ::free(base);
            else
                delete [] base;
        }
        base = 0;
        size = 0;
    }

    size_t aligned_size() {
        if (alignment == 0 || (size % alignment) == 0)
            return size;
        return ((size / alignment)+1) * alignment;
    }

    uint8_t *base;
    size_t alignment;
    uint32_t size;
    bool own;
};

/** "Less than" operator for StaticBuffer; uses @a memcmp */
inline bool operator<(const StaticBuffer& sb1, const StaticBuffer& sb2) {
    size_t len = (sb1.size < sb2.size) ? sb1.size : sb2.size;
    int cmp = memcmp(sb1.base, sb2.base, len);
    return (cmp==0) ? sb1.size < sb2.size : cmp < 0;
}

/** Equality operator for StaticBuffer; uses @a memcmp */
inline bool operator==(const StaticBuffer& sb1, const StaticBuffer& sb2) {
    if (sb1.size != sb2.size)
        return false;
    size_t len = (sb1.size < sb2.size) ? sb1.size : sb2.size;
    int equal = memcmp(sb1.base, sb2.base, len);
    if (equal == 0 )
        return true;
    return false;
}

/** Inequality operator for StaticBuffer */
inline bool operator!=(const StaticBuffer& sb1, const StaticBuffer& sb2) {
    return !(sb1 == sb2);
}

/// Smart pointer to StaticBuffer
typedef std::shared_ptr<StaticBuffer> StaticBufferPtr;

} // namespace palo

#endif //BDG_PALO_BE_SRC_RPC_STATIC_BUFFER_H
