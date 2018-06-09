// Copyright (C) 2007-2016 Hypertable, Inc.
//
// This file is part of Hypertable.
// 
// Hypertable is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 3
// of the License, or any later version.
//
// Hypertable is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
// 02110-1301, USA.

#ifndef BDG_PALO_BE_SRC_RPC_DYNAMIC_BUFFER_H 
#define BDG_PALO_BE_SRC_RPC_DYNAMIC_BUFFER_H 

#include <cstdint>
#include <cstring>
#include <memory>

namespace palo {
/**
 * A dynamic, resizable and reference counted memory buffer
 */
class DynamicBuffer {
public:
    /**
     * Constructor
     *
     * @param initial_size Initial size of the buffer
     * @param own_buffer If true, then this object takes ownership of the
     *      buffer and releases it when going out of scope
     */
    explicit DynamicBuffer(size_t initial_size = 0, bool own_buffer = true)
        : size(initial_size), own(own_buffer) {
            if (size) {
                base = ptr = mark = new uint8_t[size];
            } else {
                base = ptr = mark = 0;
            }
        }

    /** Destructor; releases the buffer if it "owns" it */
    ~DynamicBuffer() {
        if (own) {
            delete [] base;
        }
    }

    /** Returns the size of the unused portion */
    size_t remaining() const { return size - (ptr - base); }

    /** Returns the size of the used portion */
    size_t fill() const { return ptr - base; }

    /** Returns true if the buffer is empty */
    bool empty() const { return ptr == base; }

    /**
     * Ensure space for additional data
     * Will grow the space to 1.5 of the needed space with existing data
     * unchanged.
     *
     * @param len Additional bytes to grow
     */
    void ensure(size_t len) {
        if (len > remaining()) {
            grow((fill() + len) * 3 / 2);
        }
    }

    /**
     * Reserve space for additional data
     * Will grow the space to exactly what's needed. Existing data is NOT
     * preserved by default
     *
     * @param len Size of the reserved space
     * @param nocopy If true then the existing data is not preserved
     */
    void reserve(size_t len, bool nocopy = false) {
        if (len > remaining()) {
            grow(fill() + len, nocopy);
        }
    }

    /** Adds additional data without boundary checks
     *
     * @param data A pointer to the new data
     * @param len The size of the new data
     * @return A pointer to the added data
     */
    uint8_t *add_unchecked(const void *data, size_t len) {
        if (data == 0) {
            return 0;
        }
        uint8_t *rptr = ptr;
        memcpy(ptr, data, len);
        ptr += len;
        return rptr;
    }

    /** Adds more data WITH boundary checks; if required the buffer is resized
     * and existing data is preserved
     *
     * @param data A pointer to the new data
     * @param len The size of the new data
     * @return A pointer to the added data
     */
    uint8_t *add(const void *data, size_t len) {
        ensure(len);
        return add_unchecked(data, len);
    }

    /** Overwrites the existing data
     *
     * @param data A pointer to the new data
     * @param len The size of the new data
     */
    void set(const void *data, size_t len) {
        clear();
        reserve(len);
        add_unchecked(data, len);
    }

    /** Clears the buffer */
    void clear() {
        ptr = base;
    }

    /** Sets the mark; the mark can be used by the caller just like a
     * bookmark */
    void set_mark() {
        mark = ptr;
    }

    /** Frees resources */
    void free() {
        if (own) {
            delete [] base;
        }
        base = ptr = mark = 0;
        size = 0;
    }

    /** Moves ownership of the buffer to the caller
     *
     * @param lenp If not null then the length of the buffer is stored
     * @return A pointer to the data
     */
    uint8_t *release(size_t *lenp = 0) {
        uint8_t *rbuf = base;
        if (lenp) {
            *lenp = fill();
        }
        ptr = base = mark = 0;
        size = 0;
        return rbuf;
    }

    /** Grows the buffer and copies the data unless nocopy is true
     *
     * @param new_size The new buffer size
     * @param nocopy If true then the data will not be preserved
     */
    void grow(size_t new_size, bool nocopy = false) {
        uint8_t *new_buf = new uint8_t[new_size];
        if (!nocopy && base) {
            memcpy(new_buf, base, ptr-base);
        }
        ptr = new_buf + (ptr-base);
        mark = new_buf + (mark-base);
        if (own) {
            delete [] base;
        }
        base = new_buf;
        size = new_size;
    }

    /** Pointer to the allocated memory buffer */
    uint8_t *base;
    /** Pointer to the end of the used part of the buffer */
    uint8_t *ptr;
    /** A "bookmark", can be set by the caller */
    uint8_t *mark;
    /** The size of the allocated memory buffer (@ref base) */
    uint32_t size;
    /** If true then the buffer (@ref base) will be released when going out of
     * scope; if false then the caller has to release it */
    bool own;
};

typedef std::shared_ptr<DynamicBuffer> DynamicBufferPtr;

} //namespace palo

#endif //BDG_PALO_BE_SRC_RPC_DYNAMIC_BUFFER_H 
