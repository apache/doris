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

#include <glog/logging.h>

#include <limits> // IWYU pragma: keep

#include "gutil/port.h"
#include "util/bit_stream_utils.inline.h"
#include "util/bit_util.h"

namespace doris {

// Utility classes to do run length encoding (RLE) for fixed bit width values.  If runs
// are sufficiently long, RLE is used, otherwise, the values are just bit-packed
// (literal encoding).
// For both types of runs, there is a byte-aligned indicator which encodes the length
// of the run and the type of the run.
// This encoding has the benefit that when there aren't any long enough runs, values
// are always decoded at fixed (can be precomputed) bit offsets OR both the value and
// the run length are byte aligned. This allows for very efficient decoding
// implementations.
// The encoding is:
//    encoded-block := run*
//    run := literal-run | repeated-run
//    literal-run := literal-indicator < literal bytes >
//    repeated-run := repeated-indicator < repeated value. padded to byte boundary >
//    literal-indicator := varint_encode( number_of_groups << 1 | 1)
//    repeated-indicator := varint_encode( number_of_repetitions << 1 )
//
// Each run is preceded by a varint. The varint's least significant bit is
// used to indicate whether the run is a literal run or a repeated run. The rest
// of the varint is used to determine the length of the run (eg how many times the
// value repeats).
//
// In the case of literal runs, the run length is always a multiple of 8 (i.e. encode
// in groups of 8), so that no matter the bit-width of the value, the sequence will end
// on a byte boundary without padding.
// Given that we know it is a multiple of 8, we store the number of 8-groups rather than
// the actual number of encoded ints. (This means that the total number of encoded values
// can not be determined from the encoded data, since the number of values in the last
// group may not be a multiple of 8).
// There is a break-even point when it is more storage efficient to do run length
// encoding.  For 1 bit-width values, that point is 8 values.  They require 2 bytes
// for both the repeated encoding or the literal encoding.  This value can always
// be computed based on the bit-width.
// TODO: think about how to use this for strings.  The bit packing isn't quite the same.
//
// Examples with bit-width 1 (eg encoding booleans):
// ----------------------------------------
// 100 1s followed by 100 0s:
// <varint(100 << 1)> <1, padded to 1 byte> <varint(100 << 1)> <0, padded to 1 byte>
//  - (total 4 bytes)
//
// alternating 1s and 0s (200 total):
// 200 ints = 25 groups of 8
// <varint((25 << 1) | 1)> <25 bytes of values, bitpacked>
// (total 26 bytes, 1 byte overhead)
//

// Decoder class for RLE encoded data.
//
// NOTE: the encoded format does not have any length prefix or any other way of
// indicating that the encoded sequence ends at a certain point, so the Decoder
// methods may return some extra bits at the end before the read methods start
// to return 0/false.
template <typename T>
class RleDecoder {
public:
    // Create a decoder object. buffer/buffer_len is the decoded data.
    // bit_width is the width of each value (before encoding).
    RleDecoder(const uint8_t* buffer, int buffer_len, int bit_width)
            : bit_reader_(buffer, buffer_len),
              bit_width_(bit_width),
              current_value_(0),
              repeat_count_(0),
              literal_count_(0),
              rewind_state_(CANT_REWIND) {
        DCHECK_GE(bit_width_, 1);
        DCHECK_LE(bit_width_, 64);
    }

    RleDecoder() {}

    // Skip n values, and returns the number of non-zero entries skipped.
    size_t Skip(size_t to_skip);

    // Gets the next value.  Returns false if there are no more.
    bool Get(T* val);

    // Seek to the previous value.
    void RewindOne();

    // Gets the next run of the same 'val'. Returns 0 if there is no
    // more data to be decoded. Will return a run of at most 'max_run'
    // values. If there are more values than this, the next call to
    // GetNextRun will return more from the same run.
    size_t GetNextRun(T* val, size_t max_run);

    size_t get_values(T* values, size_t num_values);

    // Get the count of current repeated value
    size_t repeated_count();

    // Get current repeated value, make sure that count equals repeated_count()
    T get_repeated_value(size_t count);

    const BitReader& bit_reader() const { return bit_reader_; }

private:
    bool ReadHeader();

    enum RewindState { REWIND_LITERAL, REWIND_RUN, CANT_REWIND };

    BitReader bit_reader_;
    int bit_width_;
    uint64_t current_value_;
    uint32_t repeat_count_;
    uint32_t literal_count_;
    RewindState rewind_state_;
};

// Class to incrementally build the rle data.
// The encoding has two modes: encoding repeated runs and literal runs.
// If the run is sufficiently short, it is more efficient to encode as a literal run.
// This class does so by buffering 8 values at a time.  If they are not all the same
// they are added to the literal run.  If they are the same, they are added to the
// repeated run.  When we switch modes, the previous run is flushed out.
template <typename T>
class RleEncoder {
public:
    // buffer: buffer to write bits to.
    // bit_width: max number of bits for value.
    // TODO: consider adding a min_repeated_run_length so the caller can control
    // when values should be encoded as repeated runs.  Currently this is derived
    // based on the bit_width, which can determine a storage optimal choice.
    explicit RleEncoder(faststring* buffer, int bit_width)
            : bit_width_(bit_width), bit_writer_(buffer) {
        DCHECK_GE(bit_width_, 1);
        DCHECK_LE(bit_width_, 64);
        Clear();
    }

    // Reserve 'num_bytes' bytes for a plain encoded header, set each
    // byte with 'val': this is used for the RLE-encoded data blocks in
    // order to be able to able to store the initial ordinal position
    // and number of elements. This is a part of RleEncoder in order to
    // maintain the correct offset in 'buffer'.
    void Reserve(int num_bytes, uint8_t val);

    // Encode value. This value must be representable with bit_width_ bits.
    void Put(T value, size_t run_length = 1);

    // Flushes any pending values to the underlying buffer.
    // Returns the total number of bytes written
    int Flush();

    // Resets all the state in the encoder.
    void Clear();

    int32_t len() const { return bit_writer_.bytes_written(); }

private:
    // Flushes any buffered values.  If this is part of a repeated run, this is largely
    // a no-op.
    // If it is part of a literal run, this will call FlushLiteralRun, which writes
    // out the buffered literal values.
    // If 'done' is true, the current run would be written even if it would normally
    // have been buffered more.  This should only be called at the end, when the
    // encoder has received all values even if it would normally continue to be
    // buffered.
    void FlushBufferedValues(bool done);

    // Flushes literal values to the underlying buffer.  If update_indicator_byte,
    // then the current literal run is complete and the indicator byte is updated.
    void FlushLiteralRun(bool update_indicator_byte);

    // Flushes a repeated run to the underlying buffer.
    void FlushRepeatedRun();

    // Number of bits needed to encode the value.
    const int bit_width_;

    // Underlying buffer.
    BitWriter bit_writer_;

    // We need to buffer at most 8 values for literals.  This happens when the
    // bit_width is 1 (so 8 values fit in one byte).
    // TODO: generalize this to other bit widths
    uint64_t buffered_values_[8];

    // Number of values in buffered_values_
    int num_buffered_values_;

    // The current (also last) value that was written and the count of how
    // many times in a row that value has been seen.  This is maintained even
    // if we are in a literal run.  If the repeat_count_ get high enough, we switch
    // to encoding repeated runs.
    uint64_t current_value_;
    int repeat_count_;

    // Number of literals in the current run.  This does not include the literals
    // that might be in buffered_values_.  Only after we've got a group big enough
    // can we decide if they should part of the literal_count_ or repeat_count_
    int literal_count_;

    // Index of a byte in the underlying buffer that stores the indicator byte.
    // This is reserved as soon as we need a literal run but the value is written
    // when the literal run is complete. We maintain an index rather than a pointer
    // into the underlying buffer because the pointer value may become invalid if
    // the underlying buffer is resized.
    int literal_indicator_byte_idx_;
};

template <typename T>
bool RleDecoder<T>::ReadHeader() {
    DCHECK(bit_reader_.is_initialized());
    if (PREDICT_FALSE(literal_count_ == 0 && repeat_count_ == 0)) {
        // Read the next run's indicator int, it could be a literal or repeated run
        // The int is encoded as a vlq-encoded value.
        uint32_t indicator_value = 0;
        bool result = bit_reader_.GetVlqInt(&indicator_value);
        if (PREDICT_FALSE(!result)) {
            return false;
        }

        // lsb indicates if it is a literal run or repeated run
        bool is_literal = indicator_value & 1;
        if (is_literal) {
            literal_count_ = (indicator_value >> 1) * 8;
            DCHECK_GT(literal_count_, 0);
        } else {
            repeat_count_ = indicator_value >> 1;
            DCHECK_GT(repeat_count_, 0);
            bool result = bit_reader_.GetAligned<T>(BitUtil::Ceil(bit_width_, 8),
                                                    reinterpret_cast<T*>(&current_value_));
            DCHECK(result);
        }
    }
    return true;
}

template <typename T>
bool RleDecoder<T>::Get(T* val) {
    DCHECK(bit_reader_.is_initialized());
    if (PREDICT_FALSE(!ReadHeader())) {
        return false;
    }

    if (PREDICT_TRUE(repeat_count_ > 0)) {
        *val = current_value_;
        --repeat_count_;
        rewind_state_ = REWIND_RUN;
    } else {
        DCHECK(literal_count_ > 0);
        bool result = bit_reader_.GetValue(bit_width_, val);
        DCHECK(result);
        --literal_count_;
        rewind_state_ = REWIND_LITERAL;
    }

    return true;
}

template <typename T>
void RleDecoder<T>::RewindOne() {
    DCHECK(bit_reader_.is_initialized());

    switch (rewind_state_) {
    case CANT_REWIND:
        LOG(FATAL) << "Can't rewind more than once after each read!";
        break;
    case REWIND_RUN:
        ++repeat_count_;
        break;
    case REWIND_LITERAL: {
        bit_reader_.Rewind(bit_width_);
        ++literal_count_;
        break;
    }
    }

    rewind_state_ = CANT_REWIND;
}

template <typename T>
size_t RleDecoder<T>::GetNextRun(T* val, size_t max_run) {
    DCHECK(bit_reader_.is_initialized());
    DCHECK_GT(max_run, 0);
    size_t ret = 0;
    size_t rem = max_run;
    while (ReadHeader()) {
        if (PREDICT_TRUE(repeat_count_ > 0)) {
            if (PREDICT_FALSE(ret > 0 && *val != current_value_)) {
                return ret;
            }
            *val = current_value_;
            if (repeat_count_ >= rem) {
                // The next run is longer than the amount of remaining data
                // that the caller wants to read. Only consume it partially.
                repeat_count_ -= rem;
                ret += rem;
                return ret;
            }
            ret += repeat_count_;
            rem -= repeat_count_;
            repeat_count_ = 0;
        } else {
            DCHECK(literal_count_ > 0);
            if (ret == 0) {
                bool has_more = bit_reader_.GetValue(bit_width_, val);
                DCHECK(has_more);
                literal_count_--;
                ret++;
                rem--;
            }

            while (literal_count_ > 0) {
                bool result = bit_reader_.GetValue(bit_width_, &current_value_);
                DCHECK(result);
                if (current_value_ != *val || rem == 0) {
                    bit_reader_.Rewind(bit_width_);
                    return ret;
                }
                ret++;
                rem--;
                literal_count_--;
            }
        }
    }
    return ret;
}

template <typename T>
size_t RleDecoder<T>::get_values(T* values, size_t num_values) {
    size_t read_num = 0;
    while (read_num < num_values) {
        size_t read_this_time = num_values - read_num;

        if (LIKELY(repeat_count_ > 0)) {
            read_this_time = std::min((size_t)repeat_count_, read_this_time);
            std::fill(values, values + read_this_time, current_value_);
            values += read_this_time;
            repeat_count_ -= read_this_time;
            read_num += read_this_time;
        } else if (literal_count_ > 0) {
            read_this_time = std::min((size_t)literal_count_, read_this_time);
            for (int i = 0; i < read_this_time; ++i) {
                bool result = bit_reader_.GetValue(bit_width_, values);
                DCHECK(result);
                values++;
            }
            literal_count_ -= read_this_time;
            read_num += read_this_time;
        } else {
            if (!ReadHeader()) {
                return read_num;
            }
        }
    }
    return read_num;
}

template <typename T>
size_t RleDecoder<T>::repeated_count() {
    if (repeat_count_ > 0) {
        return repeat_count_;
    }
    if (literal_count_ == 0) {
        ReadHeader();
    }
    return repeat_count_;
}

template <typename T>
T RleDecoder<T>::get_repeated_value(size_t count) {
    DCHECK_GE(repeat_count_, count);
    repeat_count_ -= count;
    return current_value_;
}

template <typename T>
size_t RleDecoder<T>::Skip(size_t to_skip) {
    DCHECK(bit_reader_.is_initialized());

    size_t set_count = 0;
    while (to_skip > 0) {
        bool result = ReadHeader();
        DCHECK(result);

        if (PREDICT_TRUE(repeat_count_ > 0)) {
            size_t nskip = (repeat_count_ < to_skip) ? repeat_count_ : to_skip;
            repeat_count_ -= nskip;
            to_skip -= nskip;
            if (current_value_ != 0) {
                set_count += nskip;
            }
        } else {
            DCHECK(literal_count_ > 0);
            size_t nskip = (literal_count_ < to_skip) ? literal_count_ : to_skip;
            literal_count_ -= nskip;
            to_skip -= nskip;
            for (; nskip > 0; nskip--) {
                T value = 0;
                bool result = bit_reader_.GetValue(bit_width_, &value);
                DCHECK(result);
                if (value != 0) {
                    set_count++;
                }
            }
        }
    }
    return set_count;
}

// This function buffers input values 8 at a time.  After seeing all 8 values,
// it decides whether they should be encoded as a literal or repeated run.
template <typename T>
void RleEncoder<T>::Put(T value, size_t run_length) {
    DCHECK(bit_width_ == 64 || value < (1LL << bit_width_));

    // TODO(perf): remove the loop and use the repeat_count_
    for (; run_length > 0; run_length--) {
        if (PREDICT_TRUE(current_value_ == value)) {
            ++repeat_count_;
            if (repeat_count_ > 8) {
                // This is just a continuation of the current run, no need to buffer the
                // values.
                // Note that this is the fast path for long repeated runs.
                continue;
            }
        } else {
            if (repeat_count_ >= 8) {
                // We had a run that was long enough but it has ended.  Flush the
                // current repeated run.
                DCHECK_EQ(literal_count_, 0);
                FlushRepeatedRun();
            }
            repeat_count_ = 1;
            current_value_ = value;
        }

        buffered_values_[num_buffered_values_] = value;
        if (++num_buffered_values_ == 8) {
            DCHECK_EQ(literal_count_ % 8, 0);
            FlushBufferedValues(false);
        }
    }
}

template <typename T>
void RleEncoder<T>::FlushLiteralRun(bool update_indicator_byte) {
    if (literal_indicator_byte_idx_ < 0) {
        // The literal indicator byte has not been reserved yet, get one now.
        literal_indicator_byte_idx_ = bit_writer_.GetByteIndexAndAdvance(1);
        DCHECK_GE(literal_indicator_byte_idx_, 0);
    }

    // Write all the buffered values as bit packed literals
    for (int i = 0; i < num_buffered_values_; ++i) {
        bit_writer_.PutValue(buffered_values_[i], bit_width_);
    }
    num_buffered_values_ = 0;

    if (update_indicator_byte) {
        // At this point we need to write the indicator byte for the literal run.
        // We only reserve one byte, to allow for streaming writes of literal values.
        // The logic makes sure we flush literal runs often enough to not overrun
        // the 1 byte.
        int num_groups = BitUtil::Ceil(literal_count_, 8);
        int32_t indicator_value = (num_groups << 1) | 1;
        DCHECK_EQ(indicator_value & 0xFFFFFF00, 0);
        bit_writer_.buffer()->data()[literal_indicator_byte_idx_] = indicator_value;
        literal_indicator_byte_idx_ = -1;
        literal_count_ = 0;
    }
}

template <typename T>
void RleEncoder<T>::FlushRepeatedRun() {
    DCHECK_GT(repeat_count_, 0);
    // The lsb of 0 indicates this is a repeated run
    int32_t indicator_value = repeat_count_ << 1 | 0;
    bit_writer_.PutVlqInt(indicator_value);
    bit_writer_.PutAligned(current_value_, BitUtil::Ceil(bit_width_, 8));
    num_buffered_values_ = 0;
    repeat_count_ = 0;
}

// Flush the values that have been buffered.  At this point we decide whether
// we need to switch between the run types or continue the current one.
template <typename T>
void RleEncoder<T>::FlushBufferedValues(bool done) {
    if (repeat_count_ >= 8) {
        // Clear the buffered values.  They are part of the repeated run now and we
        // don't want to flush them out as literals.
        num_buffered_values_ = 0;
        if (literal_count_ != 0) {
            // There was a current literal run.  All the values in it have been flushed
            // but we still need to update the indicator byte.
            DCHECK_EQ(literal_count_ % 8, 0);
            DCHECK_EQ(repeat_count_, 8);
            FlushLiteralRun(true);
        }
        DCHECK_EQ(literal_count_, 0);
        return;
    }

    literal_count_ += num_buffered_values_;
    int num_groups = BitUtil::Ceil(literal_count_, 8);
    if (num_groups + 1 >= (1 << 6)) {
        // We need to start a new literal run because the indicator byte we've reserved
        // cannot store more values.
        DCHECK_GE(literal_indicator_byte_idx_, 0);
        FlushLiteralRun(true);
    } else {
        FlushLiteralRun(done);
    }
    repeat_count_ = 0;
}

template <typename T>
void RleEncoder<T>::Reserve(int num_bytes, uint8_t val) {
    for (int i = 0; i < num_bytes; ++i) {
        bit_writer_.PutValue(val, 8);
    }
}

template <typename T>
int RleEncoder<T>::Flush() {
    if (literal_count_ > 0 || repeat_count_ > 0 || num_buffered_values_ > 0) {
        bool all_repeat = literal_count_ == 0 &&
                          (repeat_count_ == num_buffered_values_ || num_buffered_values_ == 0);
        // There is something pending, figure out if it's a repeated or literal run
        if (repeat_count_ > 0 && all_repeat) {
            FlushRepeatedRun();
        } else {
            literal_count_ += num_buffered_values_;
            FlushLiteralRun(true);
            repeat_count_ = 0;
        }
    }
    bit_writer_.Flush();
    DCHECK_EQ(num_buffered_values_, 0);
    DCHECK_EQ(literal_count_, 0);
    DCHECK_EQ(repeat_count_, 0);
    return bit_writer_.bytes_written();
}

template <typename T>
void RleEncoder<T>::Clear() {
    current_value_ = 0;
    repeat_count_ = 0;
    num_buffered_values_ = 0;
    literal_count_ = 0;
    literal_indicator_byte_idx_ = -1;
    bit_writer_.Clear();
}

// Copy from https://github.com/apache/impala/blob/master/be/src/util/rle-encoding.h
// Utility classes to do run length encoding (RLE) for fixed bit width values.  If runs
// are sufficiently long, RLE is used, otherwise, the values are just bit-packed
// (literal encoding).
//
// For both types of runs, there is a byte-aligned indicator which encodes the length
// of the run and the type of the run.
//
// This encoding has the benefit that when there aren't any long enough runs, values
// are always decoded at fixed (can be precomputed) bit offsets OR both the value and
// the run length are byte aligned. This allows for very efficient decoding
// implementations.
// The encoding is:
//    encoded-block := run*
//    run := literal-run | repeated-run
//    literal-run := literal-indicator < literal bytes >
//    repeated-run := repeated-indicator < repeated value. padded to byte boundary >
//    literal-indicator := varint_encode( number_of_groups << 1 | 1)
//    repeated-indicator := varint_encode( number_of_repetitions << 1 )
//
// Each run is preceded by a varint. The varint's least significant bit is
// used to indicate whether the run is a literal run or a repeated run. The rest
// of the varint is used to determine the length of the run (eg how many times the
// value repeats).
//
// In the case of literal runs, the run length is always a multiple of 8 (i.e. encode
// in groups of 8), so that no matter the bit-width of the value, the sequence will end
// on a byte boundary without padding.
// Given that we know it is a multiple of 8, we store the number of 8-groups rather than
// the actual number of encoded ints. (This means that the total number of encoded values
// can not be determined from the encoded data, since the number of values in the last
// group may not be a multiple of 8). For the last group of literal runs, we pad
// the group to 8 with zeros. This allows for 8 at a time decoding on the read side
// without the need for additional checks.
//
// There is a break-even point when it is more storage efficient to do run length
// encoding.  For 1 bit-width values, that point is 8 values.  They require 2 bytes
// for both the repeated encoding or the literal encoding.  This value can always
// be computed based on the bit-width.
// TODO: For 1 bit-width values it can be optimal to use 16 or 24 values, but more
// investigation is needed to do this efficiently, see the reverted IMPALA-6658.
// TODO: think about how to use this for strings.  The bit packing isn't quite the same.
//
// Examples with bit-width 1 (eg encoding booleans):
// ----------------------------------------
// 100 1s followed by 100 0s:
// <varint(100 << 1)> <1, padded to 1 byte> <varint(100 << 1)> <0, padded to 1 byte>
//  - (total 4 bytes)
//
// alternating 1s and 0s (200 total):
// 200 ints = 25 groups of 8
// <varint((25 << 1) | 1)> <25 bytes of values, bitpacked>
// (total 26 bytes, 1 byte overhead)

// RLE decoder with a batch-oriented interface that enables fast decoding.
// Users of this class must first initialize the class to point to a buffer of
// RLE-encoded data, passed into the constructor or Reset(). The provided
// bit_width must be at most min(sizeof(T) * 8, BatchedBitReader::MAX_BITWIDTH).
// Then they can decode data by checking NextNumRepeats()/NextNumLiterals() to
// see if the next run is a repeated or literal run, then calling
// GetRepeatedValue() or GetLiteralValues() respectively to read the values.
//
// End-of-input is signalled by NextNumRepeats() == NextNumLiterals() == 0.
// Other decoding errors are signalled by functions returning false. If an
// error is encountered then it is not valid to read any more data until
// Reset() is called.

template <typename T>
class RleBatchDecoder {
public:
    RleBatchDecoder(uint8_t* buffer, int buffer_len, int bit_width) {
        Reset(buffer, buffer_len, bit_width);
    }

    RleBatchDecoder() = default;

    // Reset the decoder to read from a new buffer.
    void Reset(uint8_t* buffer, int buffer_len, int bit_width);

    // Return the size of the current repeated run. Returns zero if the current run is
    // a literal run or if no more runs can be read from the input.
    int32_t NextNumRepeats();

    // Get the value of the current repeated run and consume the given number of repeats.
    // Only valid to call when NextNumRepeats() > 0. The given number of repeats cannot
    // be greater than the remaining number of repeats in the run. 'num_repeats_to_consume'
    // can be set to 0 to peek at the value without consuming repeats.
    T GetRepeatedValue(int32_t num_repeats_to_consume);

    // Return the size of the current literal run. Returns zero if the current run is
    // a repeated run or if no more runs can be read from the input.
    int32_t NextNumLiterals();

    // Consume 'num_literals_to_consume' literals from the current literal run,
    // copying the values to 'values'. 'num_literals_to_consume' must be <=
    // NextNumLiterals(). Returns true if the requested number of literals were
    // successfully read or false if an error was encountered, e.g. the input was
    // truncated.
    bool GetLiteralValues(int32_t num_literals_to_consume, T* values) WARN_UNUSED_RESULT;

    // Consume 'num_values_to_consume' values and copy them to 'values'.
    // Returns the number of consumed values or 0 if an error occurred.
    int32_t GetBatch(T* values, int32_t batch_num);

private:
    // Called when both 'literal_count_' and 'repeat_count_' have been exhausted.
    // Sets either 'literal_count_' or 'repeat_count_' to the size of the next literal
    // or repeated run, or leaves both at 0 if no more values can be read (either because
    // the end of the input was reached or an error was encountered decoding).
    void NextCounts();

    /// Fill the literal buffer. Invalid to call if there are already buffered literals.
    /// Return false if the input was truncated. This does not advance 'literal_count_'.
    bool FillLiteralBuffer() WARN_UNUSED_RESULT;

    bool HaveBufferedLiterals() const { return literal_buffer_pos_ < num_buffered_literals_; }

    /// Output buffered literals, advancing 'literal_buffer_pos_' and decrementing
    /// 'literal_count_'. Returns the number of literals outputted.
    int32_t OutputBufferedLiterals(int32_t max_to_output, T* values);

    BatchedBitReader bit_reader_;

    // Number of bits needed to encode the value. Must be between 0 and 64 after
    // the decoder is initialized with a buffer. -1 indicates the decoder was not
    // initialized.
    int bit_width_ = -1;

    // If a repeated run, the number of repeats remaining in the current run to be read.
    // If the current run is a literal run, this is 0.
    int32_t repeat_count_ = 0;

    // If a literal run, the number of literals remaining in the current run to be read.
    // If the current run is a repeated run, this is 0.
    int32_t literal_count_ = 0;

    // If a repeated run, the current repeated value.
    T repeated_value_;

    // Size of buffer for literal values. Large enough to decode a full batch of 32
    // literals. The buffer is needed to allow clients to read in batches that are not
    // multiples of 32.
    static constexpr int LITERAL_BUFFER_LEN = 32;

    // Buffer containing 'num_buffered_literals_' values. 'literal_buffer_pos_' is the
    // position of the next literal to be read from the buffer.
    T literal_buffer_[LITERAL_BUFFER_LEN];
    int num_buffered_literals_ = 0;
    int literal_buffer_pos_ = 0;
};

template <typename T>
int32_t RleBatchDecoder<T>::OutputBufferedLiterals(int32_t max_to_output, T* values) {
    int32_t num_to_output =
            std::min<int32_t>(max_to_output, num_buffered_literals_ - literal_buffer_pos_);
    memcpy(values, &literal_buffer_[literal_buffer_pos_], sizeof(T) * num_to_output);
    literal_buffer_pos_ += num_to_output;
    literal_count_ -= num_to_output;
    return num_to_output;
}

template <typename T>
void RleBatchDecoder<T>::Reset(uint8_t* buffer, int buffer_len, int bit_width) {
    bit_reader_.Reset(buffer, buffer_len);
    bit_width_ = bit_width;
    repeat_count_ = 0;
    literal_count_ = 0;
    num_buffered_literals_ = 0;
    literal_buffer_pos_ = 0;
}

template <typename T>
int32_t RleBatchDecoder<T>::NextNumRepeats() {
    if (repeat_count_ > 0) return repeat_count_;
    if (literal_count_ == 0) NextCounts();
    return repeat_count_;
}

template <typename T>
void RleBatchDecoder<T>::NextCounts() {
    // Read the next run's indicator int, it could be a literal or repeated run.
    // The int is encoded as a ULEB128-encoded value.
    uint32_t indicator_value = 0;
    if (UNLIKELY(!bit_reader_.GetUleb128<uint32_t>(&indicator_value))) {
        return;
    }

    // lsb indicates if it is a literal run or repeated run
    bool is_literal = indicator_value & 1;

    // Don't try to handle run lengths that don't fit in an int32_t - just fail gracefully.
    // The Parquet standard does not allow longer runs - see PARQUET-1290.
    uint32_t run_len = indicator_value >> 1;
    if (is_literal) {
        // Use int64_t to avoid overflowing multiplication.
        int64_t literal_count = static_cast<int64_t>(run_len) * 8;
        if (UNLIKELY(literal_count > std::numeric_limits<int32_t>::max())) return;
        literal_count_ = literal_count;
    } else {
        if (UNLIKELY(run_len == 0)) return;
        bool result = bit_reader_.GetBytes<T>(BitUtil::Ceil(bit_width_, 8), &repeated_value_);
        if (UNLIKELY(!result)) return;
        repeat_count_ = run_len;
    }
}

template <typename T>
T RleBatchDecoder<T>::GetRepeatedValue(int32_t num_repeats_to_consume) {
    repeat_count_ -= num_repeats_to_consume;
    return repeated_value_;
}

template <typename T>
int32_t RleBatchDecoder<T>::NextNumLiterals() {
    if (literal_count_ > 0) return literal_count_;
    if (repeat_count_ == 0) NextCounts();
    return literal_count_;
}

template <typename T>
bool RleBatchDecoder<T>::GetLiteralValues(int32_t num_literals_to_consume, T* values) {
    int32_t num_consumed = 0;
    // Copy any buffered literals left over from previous calls.
    if (HaveBufferedLiterals()) {
        num_consumed = OutputBufferedLiterals(num_literals_to_consume, values);
    }

    int32_t num_remaining = num_literals_to_consume - num_consumed;
    // Copy literals directly to the output, bypassing 'literal_buffer_' when possible.
    // Need to round to a batch of 32 if the caller is consuming only part of the current
    // run avoid ending on a non-byte boundary.
    int32_t num_to_bypass =
            std::min<int32_t>(literal_count_, BitUtil::RoundDownToPowerOf2(num_remaining, 32));
    if (num_to_bypass > 0) {
        int num_read = bit_reader_.UnpackBatch(bit_width_, num_to_bypass, values + num_consumed);
        // If we couldn't read the expected number, that means the input was truncated.
        if (num_read < num_to_bypass) return false;
        literal_count_ -= num_to_bypass;
        num_consumed += num_to_bypass;
        num_remaining = num_literals_to_consume - num_consumed;
    }

    if (num_remaining > 0) {
        // We weren't able to copy all the literals requested directly from the input.
        // Buffer literals and copy over the requested number.
        if (UNLIKELY(!FillLiteralBuffer())) return false;
        OutputBufferedLiterals(num_remaining, values + num_consumed);
    }
    return true;
}

template <typename T>
bool RleBatchDecoder<T>::FillLiteralBuffer() {
    int32_t num_to_buffer = std::min<int32_t>(LITERAL_BUFFER_LEN, literal_count_);
    num_buffered_literals_ = bit_reader_.UnpackBatch(bit_width_, num_to_buffer, literal_buffer_);
    // If we couldn't read the expected number, that means the input was truncated.
    if (UNLIKELY(num_buffered_literals_ < num_to_buffer)) return false;
    literal_buffer_pos_ = 0;
    return true;
}

template <typename T>
int32_t RleBatchDecoder<T>::GetBatch(T* values, int32_t batch_num) {
    int32_t num_consumed = 0;
    while (num_consumed < batch_num) {
        // Add RLE encoded values by repeating the current value this number of times.
        int32_t num_repeats = NextNumRepeats();
        if (num_repeats > 0) {
            int32_t num_repeats_to_set = std::min(num_repeats, batch_num - num_consumed);
            T repeated_value = GetRepeatedValue(num_repeats_to_set);
            for (int i = 0; i < num_repeats_to_set; ++i) {
                values[num_consumed + i] = repeated_value;
            }
            num_consumed += num_repeats_to_set;
            continue;
        }

        // Add remaining literal values, if any.
        int32_t num_literals = NextNumLiterals();
        if (num_literals == 0) {
            break;
        }
        int32_t num_literals_to_set = std::min(num_literals, batch_num - num_consumed);
        if (!GetLiteralValues(num_literals_to_set, values + num_consumed)) {
            return 0;
        }
        num_consumed += num_literals_to_set;
    }
    return num_consumed;
}

} // namespace doris
