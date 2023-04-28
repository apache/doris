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
#include "gutil/port.h"
#include "util/bit_stream_utils.h"
#include "util/bit_stream_utils.inline.h"
#include "util/slice.h"
#include "vec/data_types/data_type.h"
#include "vec/exec/format/parquet/decoder.h"
#include "vec/exec/format/parquet/fix_length_plain_decoder.h"
#include "vec/exec/format/parquet/parquet_common.h"

namespace doris::vectorized {

class DeltaDecoder : public Decoder {
public:
    DeltaDecoder(Decoder* decoder) { _type_converted_decoder.reset(decoder); }

    ~DeltaDecoder() override = default;

    Status skip_values(size_t num_values) override {
        return _type_converted_decoder->skip_values(num_values);
    }

    template <bool has_filter>
    Status decode_byte_array(const std::vector<Slice>& decoded_vals, MutableColumnPtr& doris_column,
                             DataTypePtr& data_type, ColumnSelectVector& select_vector) {
        TypeIndex logical_type = remove_nullable(data_type)->get_type_id();
        switch (logical_type) {
        case TypeIndex::String:
            [[fallthrough]];
        case TypeIndex::FixedString: {
            ColumnSelectVector::DataReadType read_type;
            while (size_t run_length = select_vector.get_next_run<has_filter>(&read_type)) {
                switch (read_type) {
                case ColumnSelectVector::CONTENT: {
                    std::vector<StringRef> string_values;
                    string_values.reserve(run_length);
                    for (size_t i = 0; i < run_length; ++i) {
                        size_t length = decoded_vals[_current_value_idx].size;
                        string_values.emplace_back(decoded_vals[_current_value_idx].data, length);
                        _current_value_idx++;
                    }
                    doris_column->insert_many_strings(&string_values[0], run_length);
                    break;
                }
                case ColumnSelectVector::NULL_DATA: {
                    doris_column->insert_many_defaults(run_length);
                    break;
                }
                case ColumnSelectVector::FILTERED_CONTENT: {
                    _current_value_idx += run_length;
                    break;
                }
                case ColumnSelectVector::FILTERED_NULL: {
                    // do nothing
                    break;
                }
                }
            }
            _current_value_idx = 0;
            return Status::OK();
        }
        default:
            break;
        }
        return Status::InvalidArgument(
                "Can't decode parquet physical type BYTE_ARRAY to doris logical type {}",
                getTypeName(logical_type));
    }

protected:
    void init_values_converter() {
        _type_converted_decoder->set_data(_data);
        _type_converted_decoder->set_type_length(_type_length);
        _type_converted_decoder->init(_field_schema, _decode_params->ctz);
    }
    // Convert decoded value to doris type value.
    std::unique_ptr<Decoder> _type_converted_decoder;
    size_t _current_value_idx = 0;
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

    DeltaBitPackDecoder(const tparquet::Type::type& physical_type)
            : DeltaDecoder(new FixLengthPlainDecoder(physical_type)) {}
    ~DeltaBitPackDecoder() override = default;
    Status decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                         ColumnSelectVector& select_vector, bool is_dict_filter) override {
        size_t non_null_size = select_vector.num_values() - select_vector.num_nulls();
        // decode values
        _values.resize(non_null_size);
        int decoded_count = 0;
        RETURN_IF_ERROR(_get_internal(_values.data(), non_null_size, &decoded_count));
        _data->data = reinterpret_cast<char*>(_values.data());
        _type_length = sizeof(T);
        _data->size = _values.size() * _type_length;
        // set decoded value with fix plain decoder
        init_values_converter();
        return _type_converted_decoder->decode_values(doris_column, data_type, select_vector,
                                                      is_dict_filter);
    }

    Status decode(T* buffer, int num_values, int* out_num_values) {
        return _get_internal(buffer, num_values, out_num_values);
    }

    int valid_values_count() {
        // _total_value_count in header ignores of null values
        return static_cast<int>(_total_values_remaining);
    }

    void set_data(Slice* slice) override {
        _bit_reader.reset(new BitReader((const uint8_t*)slice->data, slice->size));
        Status st = _init_header();
        if (st != Status::OK()) {
            LOG(FATAL) << "Fail to init delta encoding header for " << st.to_string();
        }
        _data = slice;
        _offset = 0;
    }

    // Set BitReader which is already initialized by DeltaLengthByteArrayDecoder or
    // DeltaByteArrayDecoder
    void set_bit_reader(std::shared_ptr<BitReader> bit_reader) {
        _bit_reader = std::move(bit_reader);
        Status st = _init_header();
        if (st != Status::OK()) {
            LOG(FATAL) << "Fail to init delta encoding header for " << st.to_string();
        }
    }

private:
    static constexpr int kMaxDeltaBitWidth = static_cast<int>(sizeof(T) * 8);
    Status _init_header();
    Status _init_block();
    Status _init_mini_block(int bit_width);
    Status _get_internal(T* buffer, int max_values, int* out_num_values);

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
template class DeltaBitPackDecoder<int32_t>;
template class DeltaBitPackDecoder<int64_t>;

class DeltaLengthByteArrayDecoder final : public DeltaDecoder {
public:
    explicit DeltaLengthByteArrayDecoder(const tparquet::Type::type& physical_type)
            : DeltaDecoder(nullptr),
              _len_decoder(physical_type),
              _buffered_length(0),
              _buffered_data(0) {}

    Status skip_values(size_t num_values) override {
        _current_value_idx += num_values;
        RETURN_IF_ERROR(_len_decoder.skip_values(num_values));
        return Status::OK();
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
        RETURN_IF_ERROR(_get_internal(_values.data(), num_values - null_count, &num_valid_values));

        if (PREDICT_FALSE(num_values - null_count != num_valid_values)) {
            return Status::IOError("Expected to decode {} values, but decoded {} values.",
                                   num_values - null_count, num_valid_values);
        }
        return decode_byte_array<has_filter>(_values, doris_column, data_type, select_vector);
    }

    Status decode(Slice* buffer, int num_values, int* out_num_values) {
        return _get_internal(buffer, num_values, out_num_values);
    }

    void set_data(Slice* slice) override {
        if (slice->size == 0) {
            return;
        }
        _bit_reader = std::make_shared<BitReader>((const uint8_t*)slice->data, slice->size);
        _data = slice;
        _offset = 0;
        _decode_lengths();
    }

    void set_bit_reader(std::shared_ptr<BitReader> bit_reader) {
        _bit_reader = std::move(bit_reader);
        _decode_lengths();
    }

private:
    // Decode all the encoded lengths. The decoder_ will be at the start of the encoded data
    // after that.
    void _decode_lengths();
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
    explicit DeltaByteArrayDecoder(const tparquet::Type::type& physical_type)
            : DeltaDecoder(nullptr),
              _prefix_len_decoder(physical_type),
              _suffix_decoder(physical_type),
              _buffered_prefix_length(0),
              _buffered_data(0) {}

    Status skip_values(size_t num_values) override {
        _current_value_idx += num_values;
        RETURN_IF_ERROR(_prefix_len_decoder.skip_values(num_values));
        RETURN_IF_ERROR(_suffix_decoder.skip_values(num_values));
        return Status::OK();
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
        RETURN_IF_ERROR(_get_internal(_values.data(), num_values - null_count, &num_valid_values));
        DCHECK_EQ(num_values - null_count, num_valid_values);
        return decode_byte_array<has_filter>(_values, doris_column, data_type, select_vector);
    }

    void set_data(Slice* slice) override {
        _bit_reader = std::make_shared<BitReader>((const uint8_t*)slice->data, slice->size);
        _prefix_len_decoder.set_bit_reader(_bit_reader);

        // get the number of encoded prefix lengths
        int num_prefix = _prefix_len_decoder.valid_values_count();
        // call _prefix_len_decoder.Decode to decode all the prefix lengths.
        // all the prefix lengths are buffered in _buffered_prefix_length.
        _buffered_prefix_length.resize(num_prefix);
        int ret;
        Status st = _prefix_len_decoder.decode(_buffered_prefix_length.data(), num_prefix, &ret);
        if (st != Status::OK()) {
            LOG(FATAL) << "Fail to decode delta prefix, status: " << st;
        }
        DCHECK_EQ(ret, num_prefix);
        _prefix_len_offset = 0;
        _num_valid_values = num_prefix;

        // at this time, the decoder_ will be at the start of the encoded suffix data.
        _suffix_decoder.set_bit_reader(_bit_reader);

        // TODO: read corrupted files written with bug(PARQUET-246). _last_value should be set
        // to _last_value_in_previous_page when decoding a new page(except the first page)
        _last_value = "";
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
