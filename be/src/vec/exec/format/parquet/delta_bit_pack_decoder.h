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

#include <gen_cpp/parquet_types.h>
#include <glog/logging.h>
#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <ostream>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/status.h"
#include "util/bit_stream_utils.h"
#include "util/bit_stream_utils.inline.h"
#include "util/slice.h"
#include "vec/data_types/data_type.h"
#include "vec/exec/format/parquet/decoder.h"
#include "vec/exec/format/parquet/fix_length_plain_decoder.h"
#include "vec/exec/format/parquet/parquet_common.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"
class DeltaDecoder : public Decoder {
public:
    DeltaDecoder(Decoder* decoder) { _type_converted_decoder.reset(decoder); }

    ~DeltaDecoder() override = default;

    template <bool has_filter>
    Status decode_byte_array(const std::vector<Slice>& decoded_vals, MutableColumnPtr& doris_column,
                             DataTypePtr& data_type, ColumnSelectVector& select_vector) {
        ColumnSelectVector::DataReadType read_type;
        int value_idx = 0;
        while (size_t run_length = select_vector.get_next_run<has_filter>(&read_type)) {
            switch (read_type) {
            case ColumnSelectVector::CONTENT: {
                std::vector<StringRef> string_values;
                string_values.reserve(run_length);
                for (size_t i = 0; i < run_length; ++i) {
                    size_t length = decoded_vals[value_idx].size;
                    string_values.emplace_back(decoded_vals[value_idx].data, length);
                    value_idx++;
                }
                doris_column->insert_many_strings(&string_values[0], run_length);
                break;
            }
            case ColumnSelectVector::NULL_DATA: {
                doris_column->insert_many_defaults(run_length);
                break;
            }
            case ColumnSelectVector::FILTERED_CONTENT: {
                value_idx += run_length;
                break;
            }
            case ColumnSelectVector::FILTERED_NULL: {
                // do nothing
                break;
            }
            }
        }
        return Status::OK();
    }

    template <bool has_filter>
    Status decode_fixed_byte_array(const std::vector<Slice>& decoded_vals,
                                   MutableColumnPtr& doris_column, DataTypePtr& data_type,
                                   ColumnSelectVector& select_vector) {
        auto& column_data = reinterpret_cast<ColumnInt8&>(*doris_column).get_data();
        size_t data_index = column_data.size();
        column_data.resize(data_index + _type_length * (select_vector.num_values() -
                                                        select_vector.num_filtered()));
        auto* data = column_data.data();
        ColumnSelectVector::DataReadType read_type;
        int value_idx = 0;
        while (size_t run_length = select_vector.get_next_run<has_filter>(&read_type)) {
            switch (read_type) {
            case ColumnSelectVector::CONTENT: {
                for (size_t i = 0; i < run_length; ++i) {
                    memcpy(data + data_index, decoded_vals[value_idx].data, _type_length);
                    data_index += _type_length;
                    value_idx++;
                }
                break;
            }
            case ColumnSelectVector::NULL_DATA: {
                data_index += run_length * _type_length;
                break;
            }
            case ColumnSelectVector::FILTERED_CONTENT: {
                value_idx += run_length;
                break;
            }
            case ColumnSelectVector::FILTERED_NULL: {
                // do nothing
                break;
            }
            }
        }
        return Status::OK();
    }

protected:
    Status init_values_converter() {
        RETURN_IF_ERROR(_type_converted_decoder->set_data(_data));
        _type_converted_decoder->set_type_length(_type_length);
        return Status::OK();
    }
    // Convert decoded value to doris type value.
    std::unique_ptr<Decoder> _type_converted_decoder;
};

/**
 *   Format
 *      [header] [block 1] [block 2] ... [block N]
 *   Header
 *      [block size] [_mini_blocks_per_block] [_total_value_count] [first value]
 *   Block
 *      [min delta] [list of bitwidths of the mini blocks] [miniblocks]
 */
template <typename T>
class DeltaBitPackDecoder final : public DeltaDecoder {
public:
    using UT = std::make_unsigned_t<T>;

    DeltaBitPackDecoder() : DeltaDecoder(new FixLengthPlainDecoder()) {}
    ~DeltaBitPackDecoder() override = default;

    Status skip_values(size_t num_values) override {
        _values.resize(num_values);
        uint32_t num_valid_values;
        return _get_internal(_values.data(), cast_set<int32_t>(num_values), &num_valid_values);
    }

    Status decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                         ColumnSelectVector& select_vector, bool is_dict_filter) override {
        size_t non_null_size = select_vector.num_values() - select_vector.num_nulls();
        // decode values
        _values.resize(non_null_size);
        uint32_t decoded_count = 0;
        RETURN_IF_ERROR(
                _get_internal(_values.data(), cast_set<uint32_t>(non_null_size), &decoded_count));
        _data->data = reinterpret_cast<char*>(_values.data());
        _type_length = sizeof(T);
        _data->size = _values.size() * _type_length;
        // set decoded value with fix plain decoder
        RETURN_IF_ERROR(init_values_converter());
        return _type_converted_decoder->decode_values(doris_column, data_type, select_vector,
                                                      is_dict_filter);
    }

    Status decode(T* buffer, uint32_t num_values, uint32_t* out_num_values) {
        return _get_internal(buffer, num_values, out_num_values);
    }

    uint32_t valid_values_count() {
        // _total_value_count in header ignores of null values
        return _total_values_remaining;
    }

    Status set_data(Slice* slice) override {
        _bit_reader.reset(
                new BitReader((const uint8_t*)slice->data, cast_set<uint32_t>(slice->size)));
        RETURN_IF_ERROR(_init_header());
        _data = slice;
        _offset = 0;
        return Status::OK();
    }

    // Set BitReader which is already initialized by DeltaLengthByteArrayDecoder or
    // DeltaByteArrayDecoder
    Status set_bit_reader(std::shared_ptr<BitReader> bit_reader) {
        _bit_reader = std::move(bit_reader);
        RETURN_IF_ERROR(_init_header());
        return Status::OK();
    }

private:
    static constexpr int kMaxDeltaBitWidth = static_cast<int>(sizeof(T) * 8);
    Status _init_header();
    Status _init_block();
    Status _init_mini_block(int bit_width);
    Status _get_internal(T* buffer, uint32_t max_values, uint32_t* out_num_values);

    std::vector<T> _values;

    std::shared_ptr<BitReader> _bit_reader;
    uint32_t _values_per_block;
    uint32_t _mini_blocks_per_block;
    uint32_t _values_per_mini_block;
    uint32_t _total_value_count;

    T _min_delta;
    T _last_value;

    uint32_t _mini_block_idx;
    std::vector<uint8_t> _delta_bit_widths;
    int _delta_bit_width;
    // If the page doesn't contain any block, `_block_initialized` will
    // always be false. Otherwise, it will be true when first block initialized.
    bool _block_initialized;

    uint32_t _total_values_remaining;
    // Remaining values in current mini block. If the current block is the last mini block,
    // _values_remaining_current_mini_block may greater than _total_values_remaining.
    uint32_t _values_remaining_current_mini_block;
};

class DeltaLengthByteArrayDecoder final : public DeltaDecoder {
public:
    explicit DeltaLengthByteArrayDecoder()
            : DeltaDecoder(nullptr), _len_decoder(), _buffered_length(0), _buffered_data(0) {}

    Status skip_values(size_t num_values) override {
        _values.resize(num_values);
        int num_valid_values;
        return _get_internal(_values.data(), cast_set<int32_t>(num_values), &num_valid_values);
    }

    Status decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                         ColumnSelectVector& select_vector, bool is_dict_filter) override {
        if (select_vector.has_filter()) {
            return _decode_values<true>(doris_column, data_type, select_vector, is_dict_filter);
        } else {
            return _decode_values<false>(doris_column, data_type, select_vector, is_dict_filter);
        }
    }

    template <bool has_filter>
    Status _decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                          ColumnSelectVector& select_vector, bool is_dict_filter) {
        size_t num_values = select_vector.num_values();
        size_t null_count = select_vector.num_nulls();
        // init read buffer
        _values.resize(num_values - null_count);
        int num_valid_values;
        RETURN_IF_ERROR(_get_internal(_values.data(), cast_set<int32_t>(num_values - null_count),
                                      &num_valid_values));

        if (num_values - null_count != num_valid_values) [[unlikely]] {
            return Status::IOError("Expected to decode {} values, but decoded {} values.",
                                   num_values - null_count, num_valid_values);
        }
        return decode_byte_array<has_filter>(_values, doris_column, data_type, select_vector);
    }

    Status decode(Slice* buffer, int num_values, int* out_num_values) {
        return _get_internal(buffer, num_values, out_num_values);
    }

    Status set_data(Slice* slice) override {
        if (slice->size == 0) {
            return Status::OK();
        }
        _bit_reader = std::make_shared<BitReader>((const uint8_t*)slice->data, slice->size);
        _data = slice;
        _offset = 0;
        RETURN_IF_ERROR(_decode_lengths());
        return Status::OK();
    }

    Status set_bit_reader(std::shared_ptr<BitReader> bit_reader) {
        _bit_reader = std::move(bit_reader);
        RETURN_IF_ERROR(_decode_lengths());
        return Status::OK();
    }

private:
    // Decode all the encoded lengths. The decoder_ will be at the start of the encoded data
    // after that.
    Status _decode_lengths();
    Status _get_internal(Slice* buffer, int max_values, int* out_num_values);

    std::vector<Slice> _values;
    std::shared_ptr<BitReader> _bit_reader;
    DeltaBitPackDecoder<int32_t> _len_decoder;

    int _num_valid_values;
    uint32_t _length_idx;
    std::vector<int32_t> _buffered_length;
    std::vector<char> _buffered_data;
};

class DeltaByteArrayDecoder : public DeltaDecoder {
public:
    explicit DeltaByteArrayDecoder()
            : DeltaDecoder(nullptr), _buffered_prefix_length(0), _buffered_data(0) {}

    Status skip_values(size_t num_values) override {
        _values.resize(num_values);
        int num_valid_values;
        return _get_internal(_values.data(), cast_set<int32_t>(num_values), &num_valid_values);
    }

    Status decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                         ColumnSelectVector& select_vector, bool is_dict_filter) override {
        if (select_vector.has_filter()) {
            return _decode_values<true>(doris_column, data_type, select_vector, is_dict_filter);
        } else {
            return _decode_values<false>(doris_column, data_type, select_vector, is_dict_filter);
        }
    }

    template <bool has_filter>
    Status _decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                          ColumnSelectVector& select_vector, bool is_dict_filter) {
        size_t num_values = select_vector.num_values();
        size_t null_count = select_vector.num_nulls();
        _values.resize(num_values - null_count);
        int num_valid_values;
        RETURN_IF_ERROR(_get_internal(_values.data(), cast_set<uint32_t>(num_values - null_count),
                                      &num_valid_values));
        DCHECK_EQ(num_values - null_count, num_valid_values);
        if (doris_column->is_column_string()) {
            return decode_byte_array<has_filter>(_values, doris_column, data_type, select_vector);
        } else {
            return decode_fixed_byte_array<has_filter>(_values, doris_column, data_type,
                                                       select_vector);
        }
    }

    Status set_data(Slice* slice) override {
        _bit_reader = std::make_shared<BitReader>((const uint8_t*)slice->data, slice->size);
        RETURN_IF_ERROR(_prefix_len_decoder.set_bit_reader(_bit_reader));

        // get the number of encoded prefix lengths
        int num_prefix = _prefix_len_decoder.valid_values_count();
        // call _prefix_len_decoder.Decode to decode all the prefix lengths.
        // all the prefix lengths are buffered in _buffered_prefix_length.
        _buffered_prefix_length.resize(num_prefix);
        uint32_t ret;
        RETURN_IF_ERROR(
                _prefix_len_decoder.decode(_buffered_prefix_length.data(), num_prefix, &ret));
        DCHECK_EQ(ret, num_prefix);
        _prefix_len_offset = 0;
        _num_valid_values = num_prefix;

        // at this time, the decoder_ will be at the start of the encoded suffix data.
        RETURN_IF_ERROR(_suffix_decoder.set_bit_reader(_bit_reader));

        // TODO: read corrupted files written with bug(PARQUET-246). _last_value should be set
        // to _last_value_in_previous_page when decoding a new page(except the first page)
        _last_value = "";
        return Status::OK();
    }

    Status decode(Slice* buffer, int num_values, int* out_num_values) {
        return _get_internal(buffer, num_values, out_num_values);
    }

private:
    Status _get_internal(Slice* buffer, int max_values, int* out_num_values);

    std::vector<Slice> _values;
    std::shared_ptr<BitReader> _bit_reader;
    DeltaBitPackDecoder<int32_t> _prefix_len_decoder;
    DeltaLengthByteArrayDecoder _suffix_decoder;
    std::string _last_value;
    // string buffer for last value in previous page
    std::string _last_value_in_previous_page;
    int _num_valid_values;
    uint32_t _prefix_len_offset;
    std::vector<int32_t> _buffered_prefix_length;
    std::vector<char> _buffered_data;
};
} // namespace doris::vectorized

namespace doris::vectorized {

template <typename T>
Status DeltaBitPackDecoder<T>::_init_header() {
    if (!_bit_reader->GetVlqInt(&_values_per_block) ||
        !_bit_reader->GetVlqInt(&_mini_blocks_per_block) ||
        !_bit_reader->GetVlqInt(&_total_value_count) ||
        !_bit_reader->GetZigZagVlqInt(&_last_value)) {
        return Status::IOError("Init header eof");
    }
    if (_values_per_block == 0) {
        return Status::InvalidArgument("Cannot have zero value per block");
    }
    if (_values_per_block % 128 != 0) {
        return Status::InvalidArgument(
                "the number of values in a block must be multiple of 128, but it's " +
                std::to_string(_values_per_block));
    }
    if (_mini_blocks_per_block == 0) {
        return Status::InvalidArgument("Cannot have zero miniblock per block");
    }
    _values_per_mini_block = _values_per_block / _mini_blocks_per_block;
    if (_values_per_mini_block == 0) {
        return Status::InvalidArgument("Cannot have zero value per miniblock");
    }
    if (_values_per_mini_block % 32 != 0) {
        return Status::InvalidArgument(
                "The number of values in a miniblock must be multiple of 32, but it's " +
                std::to_string(_values_per_mini_block));
    }
    _total_values_remaining = _total_value_count;
    _delta_bit_widths.resize(_mini_blocks_per_block);
    // init as empty property
    _block_initialized = false;
    _values_remaining_current_mini_block = 0;
    return Status::OK();
}

template <typename T>
Status DeltaBitPackDecoder<T>::_init_block() {
    DCHECK_GT(_total_values_remaining, 0) << "InitBlock called at EOF";
    if (!_bit_reader->GetZigZagVlqInt(&_min_delta)) {
        return Status::IOError("Init block eof");
    }

    // read the bitwidth of each miniblock
    uint8_t* bit_width_data = _delta_bit_widths.data();
    for (uint32_t i = 0; i < _mini_blocks_per_block; ++i) {
        if (!_bit_reader->GetAligned<uint8_t>(1, bit_width_data + i)) {
            return Status::IOError("Decode bit-width EOF");
        }
        // Note that non-conformant bitwidth entries are allowed by the Parquet spec
        // for extraneous miniblocks in the last block (GH-14923), so we check
        // the bitwidths when actually using them (see InitMiniBlock()).
    }
    _mini_block_idx = 0;
    _block_initialized = true;
    RETURN_IF_ERROR(_init_mini_block(bit_width_data[0]));
    return Status::OK();
}

template <typename T>
Status DeltaBitPackDecoder<T>::_init_mini_block(int bit_width) {
    if (bit_width > kMaxDeltaBitWidth) [[unlikely]] {
        return Status::InvalidArgument("delta bit width larger than integer bit width");
    }
    _delta_bit_width = bit_width;
    _values_remaining_current_mini_block = _values_per_mini_block;
    return Status::OK();
}

template <typename T>
Status DeltaBitPackDecoder<T>::_get_internal(T* buffer, uint32_t num_values,
                                             uint32_t* out_num_values) {
    num_values = std::min<uint32_t>(num_values, _total_values_remaining);
    if (num_values == 0) {
        *out_num_values = 0;
        return Status::OK();
    }
    uint32_t i = 0;
    while (i < num_values) {
        if (_values_remaining_current_mini_block == 0) [[unlikely]] {
            if (!_block_initialized) [[unlikely]] {
                buffer[i++] = _last_value;
                DCHECK_EQ(i, 1); // we're at the beginning of the page
                if (i == num_values) {
                    // When block is uninitialized and i reaches num_values we have two
                    // different possibilities:
                    // 1. _total_value_count == 1, which means that the page may have only
                    // one value (encoded in the header), and we should not initialize
                    // any block.
                    // 2. _total_value_count != 1, which means we should initialize the
                    // incoming block for subsequent reads.
                    if (_total_value_count != 1) {
                        RETURN_IF_ERROR(_init_block());
                    }
                    break;
                }
                RETURN_IF_ERROR(_init_block());
            } else {
                ++_mini_block_idx;
                if (_mini_block_idx < _mini_blocks_per_block) {
                    RETURN_IF_ERROR(_init_mini_block(_delta_bit_widths.data()[_mini_block_idx]));
                } else {
                    RETURN_IF_ERROR(_init_block());
                }
            }
        }

        uint32_t values_decode = std::min(_values_remaining_current_mini_block, num_values - i);
        for (uint32_t j = 0; j < values_decode; ++j) {
            if (!_bit_reader->GetValue(_delta_bit_width, buffer + i + j)) {
                return Status::IOError("Get batch EOF");
            }
        }
        for (int j = 0; j < values_decode; ++j) {
            // Addition between min_delta, packed int and last_value should be treated as
            // unsigned addition. Overflow is as expected.
            buffer[i + j] = static_cast<UT>(_min_delta) + static_cast<UT>(buffer[i + j]) +
                            static_cast<UT>(_last_value);
            _last_value = buffer[i + j];
        }
        _values_remaining_current_mini_block -= values_decode;
        i += values_decode;
    }
    _total_values_remaining -= num_values;

    if (_total_values_remaining == 0) [[unlikely]] {
        if (!_bit_reader->Advance(_delta_bit_width * _values_remaining_current_mini_block)) {
            return Status::IOError("Skip padding EOF");
        }
        _values_remaining_current_mini_block = 0;
    }
    *out_num_values = num_values;
    return Status::OK();
}

#include "common/compile_check_end.h"

} // namespace doris::vectorized
