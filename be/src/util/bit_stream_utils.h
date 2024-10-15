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
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/util/bit-stream-utils.h
// and modified by Doris

#pragma once

#include "gutil/port.h"
#include "util/bit_packing.h"
#include "util/bit_util.h"
#include "util/faststring.h"

using doris::BitUtil;

namespace doris {

// Utility class to write bit/byte streams.  This class can write data to either be
// bit packed or byte aligned (and a single stream that has a mix of both).
class BitWriter {
public:
    // buffer: buffer to write bits to.
    explicit BitWriter(faststring* buffer) : buffer_(buffer) { Clear(); }

    void Clear() {
        buffered_values_ = 0;
        byte_offset_ = 0;
        bit_offset_ = 0;
        buffer_->clear();
    }

    // Returns a pointer to the underlying buffer
    faststring* buffer() const { return buffer_; }

    // The number of current bytes written, including the current byte (i.e. may include a
    // fraction of a byte). Includes buffered values.
    int bytes_written() const { return byte_offset_ + BitUtil::Ceil(bit_offset_, 8); }

    // Writes a value to buffered_values_, flushing to buffer_ if necessary.  This is bit
    // packed.
    void PutValue(uint64_t v, int num_bits);

    // Writes v to the next aligned byte using num_bits. If T is larger than num_bits, the
    // extra high-order bits will be ignored.
    template <typename T>
    void PutAligned(T v, int num_bits);

    // Write a Vlq encoded int to the buffer. The value is written byte aligned.
    // For more details on vlq: en.wikipedia.org/wiki/Variable-length_quantity
    void PutVlqInt(int32_t v);

    // Get the index to the next aligned byte and advance the underlying buffer by num_bytes.
    size_t GetByteIndexAndAdvance(int num_bytes) {
        uint8_t* ptr = GetNextBytePtr(num_bytes);
        return ptr - buffer_->data();
    }

    // Get a pointer to the next aligned byte and advance the underlying buffer by num_bytes.
    uint8_t* GetNextBytePtr(int num_bytes);

    // Flushes all buffered values to the buffer. Call this when done writing to the buffer.
    // If 'align' is true, buffered_values_ is reset and any future writes will be written
    // to the next byte boundary.
    void Flush(bool align = false);

private:
    // Bit-packed values are initially written to this variable before being memcpy'd to
    // buffer_. This is faster than writing values byte by byte directly to buffer_.
    uint64_t buffered_values_;

    faststring* buffer_ = nullptr;
    int byte_offset_; // Offset in buffer_
    int bit_offset_;  // Offset in buffered_values_
};

// Utility class to read bit/byte stream.  This class can read bits or bytes
// that are either byte aligned or not.  It also has utilities to read multiple
// bytes in one read (e.g. encoded int).
class BitReader {
public:
    // 'buffer' is the buffer to read from.  The buffer's length is 'buffer_len'.
    BitReader(const uint8_t* buffer, int buffer_len);

    BitReader() : buffer_(nullptr), max_bytes_(0) {}

    // Gets the next value from the buffer.  Returns true if 'v' could be read or false if
    // there are not enough bytes left. num_bits must be <= 32.
    template <typename T>
    bool GetValue(int num_bits, T* v);

    // Reads a 'num_bytes'-sized value from the buffer and stores it in 'v'. T needs to be a
    // little-endian native type and big enough to store 'num_bytes'. The value is assumed
    // to be byte-aligned so the stream will be advanced to the start of the next byte
    // before 'v' is read. Returns false if there are not enough bytes left.
    template <typename T>
    bool GetAligned(int num_bytes, T* v);

    // Reads a vlq encoded int from the stream.  The encoded int must start at the
    // beginning of a byte. Return false if there were not enough bytes in the buffer.
    bool GetVlqInt(uint32_t* v);
    // Reads a zigzag encoded int `into` v.
    bool GetZigZagVlqInt(int32_t* v);

    // Reads a vlq encoded int from the stream.  The encoded int must start at the
    // beginning of a byte. Return false if there were not enough bytes in the buffer.
    bool GetVlqInt(uint64_t* v);
    // Reads a zigzag encoded int `into` v.
    bool GetZigZagVlqInt(int64_t* v);

    // Returns the number of bytes left in the stream, not including the current byte (i.e.,
    // there may be an additional fraction of a byte).
    int bytes_left() { return max_bytes_ - (byte_offset_ + BitUtil::Ceil(bit_offset_, 8)); }

    // Current position in the stream, by bit.
    int position() const { return byte_offset_ * 8 + bit_offset_; }

    // Rewind the stream by 'num_bits' bits
    void Rewind(int num_bits);

    // Advance the stream by 'num_bits' bits
    bool Advance(int64_t num_bits);

    // Seek to a specific bit in the buffer
    void SeekToBit(unsigned int stream_position);

    // Maximum byte length of a vlq encoded int
    static const int MAX_VLQ_BYTE_LEN = 5;

    // Maximum byte length of a vlq encoded int64
    static const int MAX_VLQ_BYTE_LEN_FOR_INT64 = 10;

    bool is_initialized() const { return buffer_ != nullptr; }

    const uint8_t* buffer() const { return buffer_; }

    int max_bytes() const { return max_bytes_; }

private:
    // Used by SeekToBit() and GetValue() to fetch the
    // the next word into buffer_.
    void BufferValues();

    const uint8_t* buffer_ = nullptr;
    int max_bytes_;

    // Bytes are memcpy'd from buffer_ and values are read from this variable. This is
    // faster than reading values byte by byte directly from buffer_.
    uint64_t buffered_values_;

    int byte_offset_; // Offset in buffer_
    int bit_offset_;  // Offset in buffered_values_
};

/// Utility class to read bit/byte stream. This class can read bits or bytes that are
/// either byte aligned or not. It also has utilities to read multiple bytes in one
/// read (e.g. encoded int). Exposes a batch-oriented interface to allow efficient
/// processing of multiple values at a time.
class BatchedBitReader {
public:
    /// 'buffer' is the buffer to read from.  The buffer's length is 'buffer_len'.
    /// Does not take ownership of the buffer.
    BatchedBitReader(const uint8_t* buffer, int64_t buffer_len) { Reset(buffer, buffer_len); }

    BatchedBitReader() {}

    // The implicit copy constructor is left defined. If a BatchedBitReader is copied, the
    // two copies do not share any state. Invoking functions on either copy continues
    // reading from the current read position without modifying the state of the other
    // copy.

    /// Resets the read to start reading from the start of 'buffer'. The buffer's
    /// length is 'buffer_len'. Does not take ownership of the buffer.
    void Reset(const uint8_t* buffer, int64_t buffer_len) {
        DCHECK(buffer != nullptr);
        DCHECK_GE(buffer_len, 0);
        buffer_pos_ = buffer;
        buffer_end_ = buffer + buffer_len;
    }

    /// Gets up to 'num_values' bit-packed values, starting from the current byte in the
    /// buffer and advance the read position. 'bit_width' must be <= 64.
    /// If 'bit_width' * 'num_values' is not a multiple of 8, the trailing bytes are
    /// skipped and the next UnpackBatch() call will start reading from the next byte.
    ///
    /// If the caller does not want to drop trailing bits, 'num_values' must be exactly the
    /// total number of values the caller wants to read from a run of bit-packed values, or
    /// 'bit_width' * 'num_values' must be a multiple of 8. This condition is always
    /// satisfied if 'num_values' is a multiple of 32.
    ///
    /// The output type 'T' must be an unsigned integer.
    ///
    /// Returns the number of values read.
    template <typename T>
    int UnpackBatch(int bit_width, int num_values, T* v);

    /// Skip 'num_values_to_skip' bit-packed values.
    /// 'num_values_to_skip * bit_width' is either divisible by 8, or
    /// 'num_values_to_skip' equals to the count of the remaining bit-packed values.
    bool SkipBatch(int bit_width, int num_values_to_skip);

    /// Unpack bit-packed values in the same way as UnpackBatch() and decode them using the
    /// dictionary 'dict' with 'dict_len' entries. Return -1 if a decoding error is
    /// encountered, i.e. if the bit-packed values are not valid indices in 'dict'.
    /// Otherwise returns the number of values decoded. The values are written to 'v' with
    /// a stride of 'stride' bytes.
    template <typename T>
    int UnpackAndDecodeBatch(int bit_width, T* dict, int64_t dict_len, int num_values, T* v,
                             int64_t stride);

    /// Reads an unpacked 'num_bytes'-sized value from the buffer and stores it in 'v'. T
    /// needs to be a little-endian native type and big enough to store 'num_bytes'.
    /// Returns false if there are not enough bytes left.
    template <typename T>
    bool GetBytes(int num_bytes, T* v);

    /// Read an unsigned ULEB-128 encoded int from the stream. The encoded int must start
    /// at the beginning of a byte. Return false if there were not enough bytes in the
    /// buffer or the int is invalid. For more details on ULEB-128:
    /// https://en.wikipedia.org/wiki/LEB128
    /// UINT_T must be an unsigned integer type.
    template <typename UINT_T>
    bool GetUleb128(UINT_T* v);

    /// Read a ZigZag encoded int from the stream. The encoded int must start at the
    /// beginning of a byte. Return false if there were not enough bytes in the buffer or
    /// the int is invalid. For more details on ZigZag encoding:
    /// https://developers.google.com/protocol-buffers/docs/encoding#signed-integers
    /// INT_T must be a signed integer type.
    template <typename INT_T>
    bool GetZigZagInteger(INT_T* v);

    /// Returns the number of bytes left in the stream.
    int bytes_left() { return buffer_end_ - buffer_pos_; }

    /// Maximum byte length of a vlq encoded integer of type T.
    template <typename T>
    static constexpr int max_vlq_byte_len() {
        return BitUtil::Ceil(sizeof(T) * 8, 7);
    }

    /// Maximum supported bitwidth for reader.
    static const int MAX_BITWIDTH = BitPacking::MAX_BITWIDTH;

private:
    /// Current read position in the buffer.
    const uint8_t* buffer_pos_ = nullptr;

    /// Pointer to the byte after the end of the buffer.
    const uint8_t* buffer_end_ = nullptr;
};

} // namespace doris
