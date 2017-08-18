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

#ifndef BDG_PALO_BE_SRC_RPC_BYTE_STRING_H
#define BDG_PALO_BE_SRC_RPC_BYTE_STRING_H

#include <iostream>
#include <boost/shared_array.hpp>
#include <boost/shared_ptr.hpp>
#include "dynamic_buffer.h"
#include "serialization.h"

namespace palo {
/** @addtogroup palo
 *  @{
 */

/**
 * A class managing one or more serializable ByteStrings
 */

class ByteString {
public:
    /** Default constructor: starts with an empty string */
    ByteString() : ptr(0) { }

    /** Overloaded constructor: takes ownership of a pointer
     *
     * @param buf The pointer with data
     */
    ByteString(const uint8_t *buf) : ptr(buf) { }

    /** Retrieves the length of the serialized string
     *
     * @return The length of the serialized string
     */
    size_t length() const {
        if (ptr == 0) {
            return 1;
        }
        const uint8_t *tmp_ptr = ptr;
        uint32_t len = serialization::decode_vi32(&tmp_ptr);
        return (tmp_ptr - ptr) + len;
    }

    /** Retrieves the next serialized String in the buffer */
    uint8_t *next() {
        uint8_t *rptr = (uint8_t *)ptr;
        uint32_t len = serialization::decode_vi32(&ptr);
        ptr += len;
        return rptr;
    }

    /** Retrieves the decoded length and returns a pointer to the string
     *
     * @param dptr A pointer to the string data
     * @return The decoded length of the string data
     */
    size_t decode_length(const uint8_t **dptr) const {
        *dptr = ptr;
        return serialization::decode_vi32(dptr);
    }

    /** Writes the data of this ByteString into a pointer
     *
     * @param dst A pointer which will receive the serialized data
     * @return The size of the data which was copied
     */
    size_t write(uint8_t *dst) const {
        size_t len = length();
        if (ptr == 0) {
            serialization::encode_vi32(&dst, 0);
        } else {
            memcpy(dst, ptr, len);
        }
        return len;
    }

    /** Returns a pointer to the String's deserialized data
     *
     * @return A const char * pointer to the deserialized data
     */
    const char *str() const {
        const uint8_t *rptr = ptr;
        serialization::decode_vi32(&rptr);
        return (const char *)rptr;
    }

    /** Returns true if this ByteArray is not empty
     *
     * @return True if this ByteArray is not empty, otherwise false
     */
    operator bool () const {
        return ptr != 0;
    }

    /** The pointer to the serialized data */
    const uint8_t *ptr;
};

/** Serializes and appends a byte array to a DynamicBuffer object
 *
 * @param dst_buf The DynamicBuffer which will receive the data
 * @param value A pointer to the data which is appended
 * @param value_len The size of the data, in bytes
 */
inline void append_as_byte_string(DynamicBuffer &dst_buf, const void *value,
        uint32_t value_len) {
    dst_buf.ensure(7 + value_len);
    serialization::encode_vi32(&dst_buf.ptr, value_len);
    if (value_len > 0) {
        memcpy(dst_buf.ptr, value, value_len);
        dst_buf.ptr += value_len;
        *dst_buf.ptr = 0;
    }
}

/** Serializes and appends a string to a DynamicBuffer object
 *
 * @param dst_buf The DynamicBuffer which will receive the data
 * @param str A pointer to a zero-terminated buffer with the data
 */
inline void append_as_byte_string(DynamicBuffer &dst_buf, const char *str) {
    append_as_byte_string(dst_buf, str, strlen(str));
}

} // namespace palo

#endif //BDG_PALO_BE_SRC_RPC_BYTE_STRING_H
