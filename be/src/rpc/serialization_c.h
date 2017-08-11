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

#ifndef BDG_PALO_BE_SRC_RPC_SERIALIZATION_C_H
#define BDG_PALO_BE_SRC_RPC_SERIALIZATION_C_H

/* Check if stdint is included correctly */
#ifndef SIZE_MAX
# error Forgot to include "compat.h" _first_ in your *.cc or *.c?
#endif

/* Selection of implementation */
#ifdef HT_SERIALIZATION_GENERIC
#undef HT_LITTLE_ENDIAN
#else
#include "endian_c.h"
#endif

/* Overridable error handling helper macros */
#ifdef __cplusplus
# include "error.h"
# ifndef HT_THROW_INPUT_OVERRUN
#   define HT_THROW_INPUT_OVERRUN(_r_, _l_) \
    HT_THROWF(error::SERIALIZATION_INPUT_OVERRUN, \
            "Need %lu bytes but only %lu remain", (Lu)(_l_), (Lu)(_r_))
# endif
# ifndef HT_THROW_BAD_VSTR
#   define HT_THROW_BAD_VSTR(_s_) \
    HT_THROWF(error::SERIALIZATION_BAD_VSTR, "Error decoding %s", _s_)
# endif
# ifndef HT_THROW_BAD_VINT
#   define HT_THROW_BAD_VINT(_s_) \
    HT_THROWF(error::SERIALIZATION_BAD_VINT, "Error decoding %s", _s_)
# endif
# ifndef HT_THROW_UNPOSSIBLE
#   define HT_THROW_UNPOSSIBLE(_s_) \
    HT_THROWF(error::UNPOSSIBLE, "%s", _s_)
# endif
#else
# include <cstdio>
# ifndef HT_THROW_INPUT_OVERRUN
#   define HT_THROW_INPUT_OVERRUN(_s_) do { \
    std::fprintf(stderr, "Error: %s (%s:%d): input overrun deocding %s", \
            HT_FUNC, __FILE__, __LINE__, _s_); \
    abort(); \
} while (0)
# endif
# ifndef HT_THROW_BAD_VSTR
#   define HT_THROW_BAD_VSTR(_s_) do { \
    std::fprintf(stderr, "Error: %s (%s:%d): malformed input decoding %s", \
            HT_FUNC, __FILE__, __LINE__, _s_); \
    abort(); \
} while (0)
# endif
# ifndef HT_THROW_BAD_VINT
#   define HT_THROW_BAD_VINT(_s_) do { \
    std::fprintf(stderr, "Error: %s (%s:%d): malformed input decoding %s", \
            HT_FUNC, __FILE__, __LINE__, _s_); \
    abort(); \
} while (0)
# endif
# ifndef HT_THROW_UNPOSSIBLE
#   define HT_THROW_UNPOSSIBLE(_s_) do { \
    std::fprintf(stderr, "Error: %s (%s:%d): unpossible!: %s", \
            HT_FUNC, __FILE__, __LINE__, _s_); \
    abort(); \
} while (0)
# endif
#endif
#define HT_DECODE_NEED(_r_, _l_) do { \
    if (_r_ < _l_) \
    HT_THROW_INPUT_OVERRUN(_r_, _l_); \
    _r_ -= _l_; \
} while (0)

/*
 * Encode a boolean value
 */
#define HT_ENCODE_BOOL(_op_, _v_) *(_op_)++ = (_v_) ? 1 : 0

/*
 * Encode a 8-bit integer (byte)
 */
#define HT_ENCODE_I8(_op_, _v_) *(_op_)++ = _v_

/*
 * Decode a 8-bit integer (a byte/character)
 */
#define HT_DECODE_I8(_ip_, _r_, _v_) do { \
    HT_DECODE_NEED(_r_, 1); \
    _v_ = *(_ip_)++; \
} while (0)

/*
 * Encode a 16 bit integer in little endian format
 */
#ifdef HT_LITTLE_ENDIAN
# define HT_ENCODE_I16(_op_, _v_) do { \
    memcpy(_op_, &(_v_), 2); \
    _op_ += 2; \
} while (0)
#else
# define HT_ENCODE_I16(_op_, _v_) do { \
    *(_op_)++ = (uint8_t)(_v_); \
    *(_op_)++ = (uint8_t)((_v_) >> 8); \
} while (0)
#endif

/*
 * Decoded a 16 bit integer encoded in little endian format
 */
#ifdef HT_LITTLE_ENDIAN
# define HT_DECODE_I16(_ip_, _r_, _v_) do { \
    HT_DECODE_NEED(_r_, 2); \
    memcpy(&(_v_), _ip_, 2); \
    _ip_ += 2; \
} while (0)
#else
# define HT_DECODE_I16(_ip_, _r_, _v_) do { \
    HT_DECODE_NEED(_r_, 2); \
    _v_ = *(_ip_)++; \
    _v_ |= (*(_ip_)++ << 8); \
} while (0)
#endif

/*
 * Enocde a 32-bit integer in little endian format
 */
#ifdef HT_LITTLE_ENDIAN
# define HT_ENCODE_I32(_op_, _v_) do { \
    memcpy(_op_, &(_v_), 4); \
    _op_ += 4; \
} while (0)
#else
# define HT_ENCODE_I32(_op_, _v_) do { \
    *(_op_)++ = (uint8_t)(_v_); \
    *(_op_)++ = (uint8_t)((_v_) >> 8); \
    *(_op_)++ = (uint8_t)((_v_) >> 16); \
    *(_op_)++ = (uint8_t)((_v_) >> 24); \
} while (0)
#endif

/*
 * Decode a 32-bit integer encoded in little endian format
 */
#ifdef HT_LITTLE_ENDIAN
# define HT_DECODE_I32(_ip_, _r_, _v_) do { \
    HT_DECODE_NEED(_r_, 4); \
    memcpy(&_v_, _ip_, 4); \
    _ip_ += 4; \
} while (0)
#else
# define HT_DECODE_I32(_ip_, _r_, _v_) do { \
    HT_DECODE_NEED(_r_, 4); \
    _v_ = *(_ip_)++; \
    _v_ |= (*(_ip_)++ << 8); \
    _v_ |= (*(_ip_)++ << 16); \
    _v_ |= (*(_ip_)++ << 24); \
} while (0)
#endif

/*
 * Encode a 64-bit integer in little endian format
 */
#ifdef HT_LITTLE_ENDIAN
# define HT_ENCODE_I64(_op_, _v_) do { \
    memcpy(_op_, &(_v_), 8); \
    _op_ += 8; \
} while (0)
#else
# define HT_ENCODE_I64(_op_, _v_) do { \
    *(_op_)++ = (uint8_t)(_v_); \
    *(_op_)++ = (uint8_t)((_v_) >> 8); \
    *(_op_)++ = (uint8_t)((_v_) >> 16); \
    *(_op_)++ = (uint8_t)((_v_) >> 24); \
    *(_op_)++ = (uint8_t)((_v_) >> 32); \
    *(_op_)++ = (uint8_t)((_v_) >> 40); \
    *(_op_)++ = (uint8_t)((_v_) >> 48); \
    *(_op_)++ = (uint8_t)((_v_) >> 56); \
} while (0)
#endif

/*
 * Decode a 64-bit integer encoded in little endian format
 */
#ifdef HT_LITTLE_ENDIAN
# define HT_DECODE_I64(_ip_, _r_, _v_) do { \
    HT_DECODE_NEED(_r_, 8); \
    memcpy(&(_v_), _ip_, 8); \
    _ip_ += 8; \
} while (0)
#else
# define HT_DECODE_I64(_ip_, _r_, _v_) do { \
    HT_DECODE_NEED(_r_, 8); \
    _v_ = *(_ip_)++; \
    _v_ |= (*(_ip_)++ << 8); \
    _v_ |= (*(_ip_)++ << 16); \
    _v_ |= ((uint64_t)(*(_ip_)++) << 24); \
    _v_ |= ((uint64_t)(*(_ip_)++) << 32); \
    _v_ |= ((uint64_t)(*(_ip_)++) << 40); \
    _v_ |= ((uint64_t)(*(_ip_)++) << 48); \
    _v_ |= ((uint64_t)(*(_ip_)++) << 56); \
} while (0)
#endif

/* vint limits */
#define HT_MAX_V1B 0x7f
#define HT_MAX_V2B 0x3fff
#define HT_MAX_V3B 0x1fffff
#define HT_MAX_V4B 0xfffffff
#define HT_MAX_V5B 0x7ffffffffull
#define HT_MAX_V6B 0x3ffffffffffull
#define HT_MAX_V7B 0x1ffffffffffffull
#define HT_MAX_V8B 0xffffffffffffffull
#define HT_MAX_V9B 0x7fffffffffffffffull
#define HT_MAX_LEN_VINT32 5
#define HT_MAX_LEN_VINT64 10

/*
 * vint encoded length of a 32-bit integer
 */
#define HT_ENCODED_LEN_VI32(_v_) \
    (_v_ <= HT_MAX_V1B ? 1 : \
     (_v_ <= HT_MAX_V2B ? 2 : \
      (_v_ <= HT_MAX_V3B ? 3 : \
       (_v_ <= HT_MAX_V4B ? 4 : 5))))

/*
 * vint encoded length of a 64-bit integer
 */
#define HT_ENCODED_LEN_VI64(_v_) \
    (val <= HT_MAX_V1B ? 1 : \
     (val <= HT_MAX_V2B ? 2 : \
      (val <= HT_MAX_V3B ? 3 : \
       (val <= HT_MAX_V4B ? 4 : \
        (val <= HT_MAX_V5B ? 5 : \
         (val <= HT_MAX_V6B ? 6 : \
          (val <= HT_MAX_V7B ? 7 : \
           (val <= HT_MAX_V8B ? 8 : \
            (val <= HT_MAX_V9B ? 9 : 10)))))))))

/* vint encode helpers */
#define HT_ENCODE_VINT0(_op_, _v_, _done_) \
    if (_v_ <= HT_MAX_V1B) { \
        *(_op_)++ = (uint8_t)((_v_) & 0x7f); \
        _done_; \
    }

#define HT_ENCODE_VINT_(_op_, _v_, _done_) \
    *(_op_)++ = (uint8_t)((_v_) | 0x80); \
_v_ >>= 7; \
HT_ENCODE_VINT0(_op_, _v_, _done_)

#define HT_ENCODE_VINT4(_op_, _v_, _done_) \
    HT_ENCODE_VINT_(_op_, _v_, _done_) \
HT_ENCODE_VINT_(_op_, _v_, _done_) \
HT_ENCODE_VINT_(_op_, _v_, _done_) \
HT_ENCODE_VINT_(_op_, _v_, _done_)
/*
 * Encode a 32-bit integer in vint format
 *
 * @param _op_ - output buffer pointer
 * @param _v_ - value to encode
 * @param _done_ - return or break
 */
#define HT_ENCODE_VI32(_op_, _v_, _done_) do { \
    uint32_t _evtmp32_ = _v_; /* make sure these var names are not the same */ \
    HT_ENCODE_VINT0(_op_, _evtmp32_, _done_) \
    HT_ENCODE_VINT4(_op_, _evtmp32_, _done_) \
    HT_ENCODE_VINT_(_op_, _evtmp32_, _done_) \
    HT_THROW_UNPOSSIBLE("reach here encoding vint32"); \
} while (0)

/*
 * Encode a 64-bit integer in vint format
 *
 * @param _op_ - output buffer pointer
 * @param _v_ - value to encode
 * @param _done_ - return or break
 */
#define HT_ENCODE_VI64(_op_, _v_, _done_) do { \
    uint64_t _evtmp64_ = _v_; /* ditto */ \
    HT_ENCODE_VINT0(_op_, _evtmp64_, _done_) \
    HT_ENCODE_VINT4(_op_, _evtmp64_, _done_) \
    HT_ENCODE_VINT4(_op_, _evtmp64_, _done_) \
    HT_ENCODE_VINT_(_op_, _evtmp64_, _done_) \
    HT_ENCODE_VINT_(_op_, _evtmp64_, _done_) \
    HT_THROW_UNPOSSIBLE("reach here encoding vint64"); \
} while (0)

/* vint decode helpers */
#define HT_DECODE_VINT0(_type_, _v_, _ip_, _r_) \
    uint32_t _shift_ = 0; \
HT_DECODE_NEED(_r_, 1); \
_v_ = (*(_ip_)++ & 0x7f);
#define HT_DECODE_VINT_(_type_, _v_, _ip_, _r_, _done_) \
    _shift_ += 7; \
if ((_ip_)[-1] & 0x80) { \
    HT_DECODE_NEED(_r_, 1); \
    _v_ |= ((_type_)(*(_ip_)++ & 0x7f) << _shift_); \
} else _done_;

#define HT_DECODE_VINT4(_type_, _v_, _ip_, _r_, _done_) \
    HT_DECODE_VINT_(_type_, _v_, _ip_, _r_, _done_) \
HT_DECODE_VINT_(_type_, _v_, _ip_, _r_, _done_) \
HT_DECODE_VINT_(_type_, _v_, _ip_, _r_, _done_) \
HT_DECODE_VINT_(_type_, _v_, _ip_, _r_, _done_)

/*
 * Decode a 32-bit integer encoded in vint format
 *
 * @param _ip_ - input buffer pointer
 * @param _r_ - varable with remaining bytes
 * @param _v_ - variable for result
 * @param _done_ - return _v_ or break
 */

#define HT_DECODE_VI32(_ip_, _r_, _v_, _done_) do { \
    HT_DECODE_VINT0(uint32_t, _v_, _ip_, _r_) \
    HT_DECODE_VINT4(uint32_t, _v_, _ip_, _r_, _done_) \
    HT_DECODE_VINT_(uint32_t, _v_, _ip_, _r_, _done_) \
    HT_THROW_BAD_VINT("vint32"); \
} while (0)

/*
 * Decode a 64-bit integer encoded in vint format
 *
 * @param _ip_ - input buffer pointer
 * @param _r_ - varable with remaining bytes
 * @param _v_ - variable for result
 * @param _done_ - return _v_ or break
 */
#define HT_DECODE_VI64(_ip_, _r_, _v_, _done_) do { \
    HT_DECODE_VINT0(uint64_t, _v_, _ip_, _r_) \
    HT_DECODE_VINT4(uint64_t, _v_, _ip_, _r_, _done_) \
    HT_DECODE_VINT4(uint64_t, _v_, _ip_, _r_, _done_) \
    HT_DECODE_VINT_(uint64_t, _v_, _ip_, _r_, _done_) \
    HT_DECODE_VINT_(uint64_t, _v_, _ip_, _r_, _done_) \
    HT_THROW_BAD_VINT("vint64"); \
} while (0)

/*
 * Encode a buffer in bytes32 format (i32, data)
 *
 * @param _op_ - output buffer pointer
 * @param _ip_ - input buffer pointer
 * @param _len_ - input buffer length
 */
#define HT_ENCODE_BYTES32(_op_, _ip_, _len_) do { \
    HT_ENCODE_I32(_op_, _len_); \
    memcpy(_op_, _ip_, _len_); \
    _op_ += _len_; \
} while (0)

/*
 * Decode bytes32 (i32, data)
 *
 * @param _ip_ - input buffer pointer
 * @param _r_ - varable with remaining bytes
 * @param _out_ - variable for output
 * @param _len_ - variable for result length
 */
#define HT_DECODE_BYTES32(_ip_, _r_, _out_, _len_) do { \
    uint32_t _tmp_; \
    HT_DECODE_I32(_ip_, _r_, _tmp_); \
    HT_DECODE_NEED(_r_, _tmp_); \
    _out_ = (uint8_t *)(_ip_); \
    _ip_ += _tmp_; \
    _len_ = _tmp_; \
} while (0)

/*
 * Encode a string buffer in str16 format (i16, data, null)
 *
 * @param _op_ - output buffer pointer
 * @param _s_ - input buffer pointer
 * @param _len_ - input buffer length
 */
#define HT_ENCODE_STR16(_op_, _s_, _len_) do { \
    uint16_t _s16tmp_ = _len_; /* just to be cautious */ \
    HT_ENCODE_I16(_op_, _s16tmp_); /* length */ \
    if (_s16tmp_ > 0) { \
        memcpy(_op_, _s_, _s16tmp_); /* data */ \
        _op_ += len; \
    } \
    *(*bufp)++ = 0; /* null */ \
} while (0)

/*
 * Decode str16 (i16, data, null)
 *
 * @param _ip_ - input buffer pointer
 * @param _r_ - varable with remaining bytes
 * @param _s_ - variable for result
 * @param _len_ - variable for result length
 */
#define HT_DECODE_STR16(_ip_, _r_, _s_, _len_) do { \
    HT_DECODE_I16(_ip_, _r_, _len_); \
    _s_ = (char *)(_ip_); \
    _r_ -= (size_t)(_len_) + 1; \
    _ip_ += (size_t)(_len_) + 1; \
} while (0)

/*
 * Encode in vstr format (vint, data, null)
 *
 * @param _op_ - output buffer pointer
 * @param _s_ - input buffer pointer
 * @param _len_ - input buffer length
 */
#define HT_ENCODE_VSTR(_op_, _s_, _len_) do { \
    size_t _vs64tmp_ = _len_; /* ditto */ \
    HT_ENCODE_VI64(_op_, _vs64tmp_, break); \
    if (_vs64tmp_) { \
        memcpy((_op_), _s_, _vs64tmp_); \
        (_op_) += _vs64tmp_; \
    } \
    *(_op_)++ = 0; \
} while (0)

/*
 * Decode a vstr (vint, data, null)
 */
#define HT_DECODE_VSTR(_ip_, _r_, _out_, _len_) do { \
    uint64_t _tmp_; \
    HT_DECODE_VI64(_ip_, _r_, _tmp_, break); \
    if (_tmp_ > (uint64_t)(SIZE_MAX)) \
    HT_THROW_BAD_VSTR("long vstr on 32-bit platform"); \
    _out_ = (char *)(_ip_); \
    _len_ = _tmp_++; \
    HT_DECODE_NEED(_r_, _tmp_); \
    _ip_ += _tmp_; \
    if ((_ip_)[-1]) /* should be null */ \
    HT_THROW_BAD_VSTR("vstr"); \
} while (0)

#endif //BDG_PALO_BE_SRC_RPC_SERIALIZATION_C_H
