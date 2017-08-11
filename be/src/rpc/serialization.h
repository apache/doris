// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

#ifndef BDG_PALO_BE_SRC_RPC_SERIALIZATION_H
#define BDG_PALO_BE_SRC_RPC_SERIALIZATION_H

#include "inet_addr.h"
#include "serialization_c.h"

namespace palo { namespace serialization {
    /**
     * Encodes a byte into the given buffer.  Assumes there is
     * enough space available.  Increments buffer pointer.
     *
     * @param bufp Address of the destination buffer
     * @param val The byte
     */
    inline void encode_i8(uint8_t **bufp, uint8_t val) {
        HT_ENCODE_I8(*bufp, val);
    }

    /**
     * Decode a 8-bit integer (a byte/character)
     *
     * @param bufp The pointer to the source buffer
     * @param remainp The pointer to the remaining size variable
     * @return The decoded value
     */
    inline uint8_t decode_i8(const uint8_t **bufp, size_t *remainp) {
        HT_DECODE_NEED(*remainp, 1);
        return *(*bufp)++;
    }

    /**
     * Decodes a single byte from the given buffer. Increments buffer pointer
     * and decrements remainp on success.
     *
     * @param bufp Pointer to the source buffer pointer
     * @param remainp Pointer to the number of bytes remaining in buffer
     * @return The byte value
     */
    inline uint8_t decode_byte(const uint8_t **bufp, size_t *remainp) {
        return decode_i8(bufp, remainp);
    }

    /**
     * Encodes a boolean into the given buffer.  Assumes there is
     * enough space available.  Increments buffer pointer.
     *
     * @param bufp Address of the destination buffer
     * @param bval The boolean value
     */
    inline void encode_bool(uint8_t **bufp, bool bval) {
        HT_ENCODE_BOOL(*bufp, bval);
    }

    /**
     * Decodes a boolean value from the given buffer. Increments buffer pointer
     * and decrements remainp on success.
     *
     * @param bufp Pointer to the source buffer pointer
     * @param remainp Pointer to number of bytes remaining in buffer
     * @return The boolean value
     */
    inline bool decode_bool(const uint8_t **bufp, size_t *remainp) {
        return decode_i8(bufp, remainp) != 0;
    }

    /**
     * Encode a 16-bit integer in little-endian order
     *
     * @param bufp Pointer to the destination buffer
     * @param val The value to encode
     */
    inline void encode_i16(uint8_t **bufp, uint16_t val) {
        HT_ENCODE_I16(*bufp, val);
    }

    /**
     * Decode a 16-bit integer in little-endian order
     *
     * @param bufp Pointer to the source buffer pointer
     * @param remainp Pointer to the remaining size variable
     * @return The decoded value
     */
    inline uint16_t decode_i16(const uint8_t **bufp, size_t *remainp) {
        uint16_t val;
        HT_DECODE_I16(*bufp, *remainp, val);
        return val;
    }

    /**
     * Encode a 32-bit integer in little-endian order
     *
     * @param bufp Pointer to the destination buffer pointer
     * @param val The value to encode
     */
    inline void encode_i32(uint8_t **bufp, uint32_t val) {
        HT_ENCODE_I32(*bufp, val);
    }

    /**
     * Decode a 32-bit integer in little-endian order
     *
     * @param bufp Pointer to the source buffer pointer
     * @param remainp Pointer to the remaining size variable
     * @return The decoded value
     */
    inline uint32_t decode_i32(const uint8_t **bufp, size_t *remainp) {
        uint32_t val;
        HT_DECODE_I32(*bufp, *remainp, val);
        return val;
    }

    /**
     * Encode a 64-bit integer in little-endian order
     *
     * @param bufp Pointer to the destination buffer pointer
     * @param val The value to encode
     */
    inline void encode_i64(uint8_t **bufp, uint64_t val) {
        HT_ENCODE_I64(*bufp, val);
    }

    /**
     * Decode a 64-bit integer in little-endian order
     *
     * @param bufp Pointer to the source buffer pointer
     * @param remainp Pointer to the remaining size variable
     * @return The decoded value
     */
    inline uint64_t decode_i64(const uint8_t **bufp, size_t *remainp) {
        uint64_t val;
        HT_DECODE_I64(*bufp, *remainp, val);
        return val;
    }

    /**
     * Length of a variable length encoded 32-bit integer (up to 5 bytes)
     *
     * @param val The 32-bit integer to encode
     * @return The number of bytes required for serializing this number
     */
    inline int encoded_length_vi32(uint32_t val) {
        return HT_ENCODED_LEN_VI32(val);
    }

    /**
     * Length of a variable length encoded 64-bit integer (up to 9 bytes)
     *
     * @param val The 64-bit integer to encode
     * @return The number of bytes required for serializing this number
     */
    inline int encoded_length_vi64(uint64_t val) {
        return HT_ENCODED_LEN_VI64(val);
    }

    /**
     * Encode a integer (up to 32-bit) in variable length encoding
     *
     * @param bufp Pointer to the destination buffer pointer
     * @param val The value to encode
     */
    inline void encode_vi32(uint8_t **bufp, uint32_t val) {
        HT_ENCODE_VI32(*bufp, val, return);
    }

    /**
     * Encode a integer (up to 64-bit) in variable length encoding
     *
     * @param bufp The pointer to the destination buffer pointer
     * @param val The value to encode
     */
    inline void encode_vi64(uint8_t **bufp, uint64_t val) {
        HT_ENCODE_VI64(*bufp, val, return);
    }

    /**
     * Decode a variable length encoded integer up to 32-bit
     *
     * @param bufp Pointer to the source buffer pointer
     * @param remainp Pointer to the remaining size variable
     * @return The decoded value
     */
    inline uint32_t decode_vi32(const uint8_t **bufp, size_t *remainp) {
        uint32_t n;
        HT_DECODE_VI32(*bufp, *remainp, n, return n);
    }

    /**
     * Decode a variable length encoded integer up to 64-bit
     *
     * @param bufp Pointer to the source buffer pointer
     * @param remainp Pointer to the remaining size variable
     * @return The decoded value
     */
    inline uint64_t decode_vi64(const uint8_t **bufp, size_t *remainp) {
        uint64_t n;
        HT_DECODE_VI64(*bufp, *remainp, n, return n);
    }

    /**
     * Decode a variable length encoded integer up to 32-bit
     *
     * @param bufp Pointer to the source buffer pointer
     * @return The decoded value
     */
    inline uint32_t decode_vi32(const uint8_t **bufp) {
        size_t remain = 6;
        return decode_vi32(bufp, &remain);
    }

    /**
     * Decode a variable length encoded integer up to 64-bit
     *
     * @param bufp Pointer to the source buffer pointer
     * @return The decoded value
     */
    inline uint64_t decode_vi64(const uint8_t **bufp) {
        size_t remain = 12;
        return decode_vi64(bufp, &remain);
    }

    /**
     * Computes the encoded length of a 32-bit length byte array (i32, bytes)
     *
     * @param len Length of the byte array to be encoded
     * @return The encoded length of a byte array of length len
     */
    inline size_t encoded_length_bytes32(int32_t len) {
        return len + 4;
    }

    /**
     * Encodes a variable sized byte array into the given buffer.  Encoded as a
     * 4 byte length followed by the data.  Assumes there is enough space
     * available.  Increments buffer pointer.
     *
     * @param bufp Address of the destination buffer
     * @param data Pointer to array of bytes
     * @param len The length of the byte array
     */
    inline void encode_bytes32(uint8_t **bufp, const void *data, int32_t len) {
        HT_ENCODE_BYTES32(*bufp, data, len);
    }

    /**
     * Decodes a variable sized byte array from the given buffer.  Byte array i
     * encoded as a 4 byte length followed by the data.  Increments buffer
     * pointer and decrements remainp on success.
     *
     * @param bufp Address of buffer containing encoded byte array
     * @param remainp Address of variable containing number of bytes remaining
     * @param lenp Address of length of decoded byte array
     */
    inline uint8_t *decode_bytes32(const uint8_t **bufp, size_t *remainp,
            uint32_t *lenp) {
        uint8_t *out;
        HT_DECODE_BYTES32(*bufp, *remainp, out, *lenp);
        return out;
    }

    /**
     * Computes the encoded length of a string16 encoding
     *
     * @param str Pointer to the c-style string
     * @return The encoded length of str
     */
    inline size_t encoded_length_str16(const char *str) {
        return 2 + ((str == 0) ? 0 : strlen(str)) + 1;
    }

    /**
     * Computes the encoded length of a std::string.
     *
     * @param str Reference to string object
     * @return The encoded length of str
     */
    inline size_t encoded_length_str16(const std::string &str) {
        return 2 + str.length() + 1;
    }

    /**
     * Encodes a string buffer into the given buffer.
     * Encoded as a 2 byte length followed by the string data, followed
     * by a '\\0' termination byte.  The length value does not include
     * the '\\0'.  Assumes there is enough space available.  Increments
     * buffer pointer.
     *
     * @param bufp Address of the destination buffer
     * @param str The c-style string to encode
     * @param len Length of the string
     */
    inline void encode_str16(uint8_t **bufp, const void *str, uint16_t len) {
        HT_ENCODE_STR16(*bufp, str, len);
    }

    /**
     * Encodes a c-style null-terminated string into the given buffer.
     * Encoded as a 2 byte length followed by the string data, followed
     * by a '\\0' termination byte.  The length value does not include
     * the '\\0'.  Assumes there is enough space available.  Increments
     * buffer pointer.
     *
     * @param bufp Pointer to pointer of destination buffer
     * @param str The c-style string to encode
     */
    inline void encode_str16(uint8_t **bufp, const char *str) {
        uint16_t len = (str == 0) ? 0 : strlen(str);
        encode_str16(bufp, str, len);
    }

    /**
     * Encodes a string into the given buffer.  Encoded as a
     * 2 byte length followed by the string data, followed by a '\\0'
     * termination byte.  The length value does not include the '\\0'.
     * Assumes there is enough space available.  Increments buffer
     * pointer.
     *
     * @param bufp Pointer to pointer of the destinatin buffer
     * @param str The std::string to encode
     */
    template <class StringT>
        inline void encode_str16(uint8_t **bufp, const StringT &str) {
            encode_str16(bufp, str.c_str(), str.length());
        }

    /**
     * Decodes a c-style string from the given buffer.  The encoding of the
     * string is a 2 byte length followed by the string, followed by a '\\0'
     * termination byte.  The length does not include the '\\0' terminator.
     * The decoded string pointer points back into the encoding buffer.
     * Increments buffer pointer and decrements remainp on success.
     *
     * @param bufp Pointer to pointer of buffer containing encoded string
     * @param remainp Address of variable of number of bytes remaining in buffer
     * @return Pointer to a c-style string
     */
    inline const char *decode_str16(const uint8_t **bufp, size_t *remainp) {
        const char *str;
        uint16_t len;
        HT_DECODE_STR16(*bufp, *remainp, str, len);
        return str;
    }

    /**
     * Decodes a c-style string from the given buffer.  The encoding of the
     * string is a 2 byte length followed by the string, followed by a '\\0'
     * termination byte.  The length does not include the '\\0' terminator.
     * The decoded string pointer points back into the encoding buffer.
     * Increments buffer pointer and decrements remainp on success.
     *
     * @param bufp Pointer to pointer of buffer containing encoded string
     * @param remainp Address of variable of number of bytes remaining in buffer
     * @param lenp Address of varaible to hold the len of the string
     * @return Pointer to a c-style string
     */
    inline char *decode_str16(const uint8_t **bufp, size_t *remainp,
            uint16_t *lenp) {
        char *str;
        HT_DECODE_STR16(*bufp, *remainp, str, *lenp);
        return str;
    }

    /**
     * Decodes a str16 from the given buffer to a std::string.  The encoding of the
     * string is a 2 byte length followed by the string, followed by a '\\0'
     * termination byte.  The length does not include the '\\0' terminator.
     * Increments buffer pointer and decrements remainp on success.
     *
     * @param bufp Pointer to pointer of buffer containing encoded string
     * @param remainp Address of variable of number of bytes remaining in buffer
     * @return True on success, false if buffer has insufficient room
     */
    template <class StringT>
        inline StringT decode_str16(const uint8_t **bufp, size_t *remainp) {
            char *str;
            uint16_t len;
            HT_DECODE_STR16(*bufp, *remainp, str, len);
            return StringT(str, len); // RVO hopeful
        }

    /**
     * Computes the encoded length of vstr (vint64, data, null)
     *
     * @param len The string length
     * @return The encoded length of str
     */
    inline size_t encoded_length_vstr(size_t len) {
        return encoded_length_vi64(len) + len + 1;
    }

    /**
     * Computes the encoded length of vstr. Assumes that the string length
     * can be encoded in 32-bit integer
     *
     * @param s Pointer to the the c-style string
     * @return The encoded length of s
     */
    inline size_t encoded_length_vstr(const char *s) {
        return encoded_length_vstr(s ? strlen(s) : 0);
    }

    /**
     * Computes the encoded length of vstr. Assumes that the string length
     * can be encoded in 32-bit integer
     *
     * @param s The string to encode
     * @return The encoded length of s
     */
    inline size_t encoded_length_vstr(const std::string &s) {
        return encoded_length_vstr(s.length());
    }

    /**
     * Encode a buffer as variable length string (vint64, data, null)
     *
     * @param bufp Pointer to pointer of destination buffer
     * @param buf Pointer to the start of the input buffer
     * @param len Length of the input buffer
     */
    inline void encode_vstr(uint8_t **bufp, const void *buf, size_t len) {
        HT_ENCODE_VSTR(*bufp, buf, len);
    }

    /**
     * Encode a c-style string as vstr
     *
     * @param bufp Pointer to pointer of destination buffer
     * @param s Pointer to the start of the string
     */
    inline void encode_vstr(uint8_t **bufp, const char *s) {
        encode_vstr(bufp, s, s ? strlen(s) : 0);
    }

    /**
     * Encode a std::string as vstr
     *
     * @param bufp Pointer to pointer of destination buffer
     * @param s The string to encode
     */
    template <class StringT>
        inline void encode_vstr(uint8_t **bufp, const StringT &s) {
            encode_vstr(bufp, s.data(), s.length());
        }

    /**
     * Decode a vstr (vint64, data, null).
     * Note: decoding a vstr longer than 4GiB on a 32-bit platform
     * is obviously futile (throws bad vstr exception)
     *
     * @param bufp Pointer to pointer of the source buffer
     * @param remainp Pointer to the remaining size variable
     * @return Pointer to the decoded string data
     */
    inline char *decode_vstr(const uint8_t **bufp, size_t *remainp) {
        char *buf;
        size_t len;
        HT_DECODE_VSTR(*bufp, *remainp, buf, len);
        (void)len; // avoid warnings because len is assigned but never used
        return buf;
    }

    /**
     * Decode a vstr (vint64, data, null)
     *
     * @param bufp Pointer to pointer of the source buffer
     * @param remainp Pointer to the remaining size variable
     * @return The decoded string
     */
    template <class StringT>
        inline std::string decode_vstr(const uint8_t **bufp, size_t *remainp) {
            char *buf;
            size_t len;
            HT_DECODE_VSTR(*bufp, *remainp, buf, len);
            return StringT(buf, len); // RVO
        }

    /**
     * Decode a vstr (vint64, data, null)
     *
     * @param bufp Pointer to pointer of the source buffer
     * @param remainp Pointer to the remaining size variable
     * @param lenp Pointer to the string length
     * @return Pointer to the decoded string
     */
    inline char *decode_vstr(const uint8_t **bufp, size_t *remainp,
            uint32_t *lenp) {
        char *buf;
        HT_DECODE_VSTR(*bufp, *remainp, buf, *lenp);
        return buf;
    }

    /**
     * Encode an InetAddr structure
     *
     * @param bufp Pointer to pointer of the destination buffer
     * @param addr Reference to the InetAddr object to encode
     */
    inline void encode_inet_addr(uint8_t **bufp, const InetAddr &addr) {
        HT_ENCODE_I16(*bufp, addr.sin_family);
        HT_ENCODE_I32(*bufp, addr.sin_addr.s_addr);
        HT_ENCODE_I16(*bufp, addr.sin_port);
    }

    /**
     * Decode an InetAddr structure
     *
     * @param bufp Pointer to pointer of the source buffer
     * @param remainp Pointer to remaining size variable
     * @return The decoded InetAddr object
     */
    inline InetAddr decode_inet_addr(const uint8_t **bufp, size_t *remainp) {
        InetAddr addr;
        HT_DECODE_I16(*bufp, *remainp, addr.sin_family);
        HT_DECODE_I32(*bufp, *remainp, addr.sin_addr.s_addr);
        HT_DECODE_I16(*bufp, *remainp, addr.sin_port);
        return addr;
    }

    /**
     * Encodes a double with 18 decimal digits of precision as 64-bit
     * left-of-decimal, followed by 64-bit right-of-decimal, both in
     * little-endian order
     *
     * @param bufp Pointer to the destination buffer
     * @param val The double to encode
     */
    inline void encode_double(uint8_t **bufp, double val) {
        int64_t lod = (int64_t)val;
        int64_t rod = (int64_t)((val - (double)lod) * (double)1000000000000000000.00);
        HT_ENCODE_I64(*bufp, lod);
        HT_ENCODE_I64(*bufp, rod);
    }

    /**
     * Decodes a double as 64-bit left-of-decimal, followed by
     * 64-bit right-of-decimal, both in little-endian order
     *
     * @param bufp Pointer to pointer of the source buffer
     * @param remainp Pointer to remaining size variable
     * @return The decoded value
     */
    inline double decode_double(const uint8_t **bufp, size_t *remainp) {
        int64_t lod;
        int64_t rod;
        HT_DECODE_I64(*bufp, *remainp, lod);
        HT_DECODE_I64(*bufp, *remainp, rod);
        return (double)lod + ((double)rod / (double)1000000000000000000.00);
    }

    /**
     * Length of an encoded double (16 bytes)
     *
     * @return The number of bytes required to encode a double
     */
    inline int encoded_length_double() {
        return 16;
    }

    /**
     * Compare doubles that may have been serialized and unserialized.
     * The serialization process loses precision, so this function is
     * necessary to compare pre-serialized with post-serialized values
     *
     * @param a First value to compare
     * @param b Second value to compare
     * @return True of values are logically equal, false otherwise
     */
    inline bool equal(double a, double b) {
        int64_t lod_a = (int64_t)a;
        int64_t rod_a = (int64_t)((a - (double)lod_a) * (double)1000000000000000000.00);
        double aprime = (double)lod_a + ((double)rod_a / (double)1000000000000000000.00);
        int64_t lod_b = (int64_t)b;
        int64_t rod_b = (int64_t)((b - (double)lod_b) * (double)1000000000000000000.00);
        double bprime = (double)lod_b + ((double)rod_b / (double)1000000000000000000.00);
        return aprime == bprime;
    }
} // namespace palo::serialization

} // namespace palo

#endif //BDG_PALO_BE_SRC_RPC_SERIALIZATION_H
