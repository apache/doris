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

#ifndef DORIS_BE_SRC_QUERY_EXEC_READ_WRITE_UTIL_H
#define DORIS_BE_SRC_QUERY_EXEC_READ_WRITE_UTIL_H

#include <sstream>

#include "common/logging.h"
#include "common/status.h"

namespace doris {

#define RETURN_IF_FALSE(x) \
    if (UNLIKELY(!(x))) return false

// Class for reading and writing various data types.
class ReadWriteUtil {
public:
    // Maximum length for Writeable VInt
    static const int MAX_VINT_LEN = 9;

    // Maximum lengths for Zigzag encodings.
    const static int MAX_ZINT_LEN = 5;
    const static int MAX_ZLONG_LEN = 10;

    // Put a zigzag encoded integer into a buffer and return its length.
    static int put_zint(int32_t integer, uint8_t* buf);

    // Put a zigzag encoded long integer into a buffer and return its length.
    static int put_zlong(int64_t longint, uint8_t* buf);

    // Get a big endian integer from a buffer.  The buffer does not have to be word aligned.
    static int32_t get_int(const uint8_t* buffer);
    static int16_t get_small_int(const uint8_t* buffer);
    static int64_t get_long_int(const uint8_t* buffer);

    // Get a variable-length Long or int value from a byte buffer.
    // Returns the length of the long/int
    // If the size byte is corrupted then return -1;
    static int get_vlong(uint8_t* buf, int64_t* vlong);
    static int get_vint(uint8_t* buf, int32_t* vint);

    // Read a variable-length Long value from a byte buffer starting at the specified
    // byte offset.
    static int get_vlong(uint8_t* buf, int64_t offset, int64_t* vlong);

    // Put an Integer into a buffer in big endian order .  The buffer must be at least
    // 4 bytes long.
    static void put_int(uint8_t* buf, int32_t integer);

    // Dump the first length bytes of buf to a Hex string.
    static std::string hex_dump(const uint8_t* buf, int64_t length);
    static std::string hex_dump(const char* buf, int64_t length);

    // Determines the sign of a VInt/VLong from the first byte.
    static bool is_negative_vint(int8_t byte);

    // Determines the total length in bytes of a Writable VInt/VLong from the first byte.
    static int decode_vint_size(int8_t byte);

    // The following methods read data from a buffer without assuming the buffer is long
    // enough. If the buffer isn't long enough or another error occurs, they return false
    // and update the status with the error. Otherwise they return true. buffer is advanced
    // past the data read and buf_len is decremented appropriately.

    // Read a zig-zag encoded long. This is the integer encoding defined by google.com
    // protocol-buffers: https://developers.google.com/protocol-buffers/docs/encoding
    static bool read_zlong(uint8_t** buf, int* buf_len, int64_t* val, Status* status);

    // Read a zig-zag encoded int.
    static bool read_zint(uint8_t** buf, int* buf_len, int32_t* val, Status* status);

    // Read a native type T (e.g. bool, float) directly into output (i.e. input is cast
    // directly to T and incremented by sizeof(T)).
    template <class T>
    static bool read(uint8_t** buf, int* buf_len, T* val, Status* status);

    // Skip the next num_bytes bytes.
    static bool skip_bytes(uint8_t** buf, int* buf_len, int num_bytes, Status* status);
};

inline int16_t ReadWriteUtil::get_small_int(const uint8_t* buf) {
    return (buf[0] << 8) | buf[1];
}

inline int32_t ReadWriteUtil::get_int(const uint8_t* buf) {
    return (buf[0] << 24) | (buf[1] << 16) | (buf[2] << 8) | buf[3];
}

inline int64_t ReadWriteUtil::get_long_int(const uint8_t* buf) {
    return (static_cast<int64_t>(buf[0]) << 56) | (static_cast<int64_t>(buf[1]) << 48) |
           (static_cast<int64_t>(buf[2]) << 40) | (static_cast<int64_t>(buf[3]) << 32) |
           (buf[4] << 24) | (buf[5] << 16) | (buf[6] << 8) | buf[7];
}

inline void ReadWriteUtil::put_int(uint8_t* buf, int32_t integer) {
    buf[0] = integer >> 24;
    buf[1] = integer >> 16;
    buf[2] = integer >> 8;
    buf[3] = integer;
}

inline int ReadWriteUtil::get_vint(uint8_t* buf, int32_t* vint) {
    int64_t vlong = 0;
    int len = get_vlong(buf, &vlong);
    *vint = static_cast<int32_t>(vlong);
    return len;
}

inline int ReadWriteUtil::get_vlong(uint8_t* buf, int64_t* vlong) {
    return get_vlong(buf, 0, vlong);
}

inline int ReadWriteUtil::get_vlong(uint8_t* buf, int64_t offset, int64_t* vlong) {
    int8_t firstbyte = (int8_t)buf[0 + offset];

    int len = decode_vint_size(firstbyte);

    if (len > MAX_VINT_LEN) {
        return -1;
    }

    if (len == 1) {
        *vlong = static_cast<int64_t>(firstbyte);
        return len;
    }

    *vlong &= ~*vlong;

    for (int i = 1; i < len; i++) {
        *vlong = (*vlong << 8) | buf[i + offset];
    }

    if (is_negative_vint(firstbyte)) {
        *vlong = *vlong ^ ((int64_t)-1);
    }

    return len;
}

inline bool ReadWriteUtil::read_zint(uint8_t** buf, int* buf_len, int32_t* val, Status* status) {
    int64_t zlong;
    RETURN_IF_FALSE(read_zlong(buf, buf_len, &zlong, status));
    *val = static_cast<int32_t>(zlong);
    return true;
}

inline bool ReadWriteUtil::read_zlong(uint8_t** buf, int* buf_len, int64_t* val, Status* status) {
    uint64_t zlong = 0;
    int shift = 0;
    bool more;

    do {
        DCHECK_LE(shift, 64);

        if (UNLIKELY(*buf_len < 1)) {
            *status = Status::InternalError("Insufficient buffer length");
            return false;
        }

        zlong |= static_cast<uint64_t>(**buf & 0x7f) << shift;
        shift += 7;
        more = (**buf & 0x80) != 0;
        ++(*buf);
        --(*buf_len);
    } while (more);

    *val = (zlong >> 1) ^ -(zlong & 1);
    return true;
}

template <class T>
inline bool ReadWriteUtil::read(uint8_t** buf, int* buf_len, T* val, Status* status) {
    int val_len = sizeof(T);

    if (UNLIKELY(val_len > *buf_len)) {
        std::stringstream ss;
        ss << "Cannot read " << val_len << " bytes, buffer length is " << *buf_len;
        *status = Status::InternalError(ss.str());
        return false;
    }

    *val = *reinterpret_cast<T*>(*buf);
    *buf += val_len;
    *buf_len -= val_len;
    return true;
}

inline bool ReadWriteUtil::skip_bytes(uint8_t** buf, int* buf_len, int num_bytes, Status* status) {
    DCHECK_GE(*buf_len, 0);

    if (UNLIKELY(num_bytes > *buf_len)) {
        std::stringstream ss;
        ss << "Cannot skip " << num_bytes << " bytes, buffer length is " << *buf_len;
        *status = Status::InternalError(ss.str());
        return false;
    }

    *buf += num_bytes;
    *buf_len -= num_bytes;
    return true;
}

inline bool ReadWriteUtil::is_negative_vint(int8_t byte) {
    return byte < -120 || (byte >= -112 && byte < 0);
}

inline int ReadWriteUtil::decode_vint_size(int8_t byte) {
    if (byte >= -112) {
        return 1;
    } else if (byte < -120) {
        return -119 - byte;
    }

    return -111 - byte;
}

} // namespace doris
#endif
