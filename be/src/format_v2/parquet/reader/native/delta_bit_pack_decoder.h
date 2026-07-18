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

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <ostream>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/status.h"
#include "core/column/column_fixed_length_object.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "exec/common/arithmetic_overflow.h"
#include "format_v2/parquet/reader/native/decoder.h"
#include "util/bit_stream_utils.h"
#include "util/bit_stream_utils.inline.h"
#include "util/slice.h"

namespace doris::format::parquet::native {
namespace detail {

inline Status checked_delta_padding_bits(uint32_t bit_width, uint32_t remaining_values,
                                         int64_t* padding_bits) {
    DORIS_CHECK(padding_bits != nullptr);
    const uint64_t widened_bits = static_cast<uint64_t>(bit_width) * remaining_values;
    if (UNLIKELY(widened_bits > static_cast<uint64_t>(std::numeric_limits<int64_t>::max()))) {
        return Status::Corruption("Parquet delta miniblock padding is too large");
    }
    *padding_bits = static_cast<int64_t>(widened_bits);
    return Status::OK();
}

} // namespace detail

class DeltaDecoder : public Decoder {
public:
    DeltaDecoder() = default;
    ~DeltaDecoder() override = default;
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

    DeltaBitPackDecoder() = default;
    ~DeltaBitPackDecoder() override = default;

    Status skip_values(size_t num_values) override {
        constexpr size_t kSkipBatchSize = 4096;
        _values.resize(std::min(num_values, kSkipBatchSize));
        size_t skipped = 0;
        while (skipped < num_values) {
            const size_t batch_size = std::min(num_values - skipped, kSkipBatchSize);
            uint32_t num_valid_values = 0;
            RETURN_IF_ERROR(_get_internal(_values.data(), static_cast<uint32_t>(batch_size),
                                          &num_valid_values));
            // Skips retain exact validation without allocating in proportion to a sparse gap.
            if (UNLIKELY(num_valid_values != batch_size)) {
                return Status::IOError("Expected to skip {} Parquet delta values, skipped {}",
                                       num_values, skipped + num_valid_values);
            }
            skipped += batch_size;
        }
        return Status::OK();
    }

    Status decode_fixed_values(size_t num_values, ParquetFixedValueConsumer& consumer) override {
        _values.resize(num_values);
        uint32_t decoded_count = 0;
        RETURN_IF_ERROR(
                _get_internal(_values.data(), cast_set<uint32_t>(num_values), &decoded_count));
        if (UNLIKELY(decoded_count != num_values)) {
            return Status::IOError("Expected {} Parquet delta values, decoded {}", num_values,
                                   decoded_count);
        }
        return consumer.consume(reinterpret_cast<const uint8_t*>(_values.data()), num_values,
                                sizeof(T));
    }

    Status decode_selected_fixed_values(const ParquetSelection& selection,
                                        ParquetFixedValueConsumer& consumer) override {
        _values.resize(selection.total_values);
        uint32_t decoded_count = 0;
        RETURN_IF_ERROR(_get_internal(_values.data(), cast_set<uint32_t>(selection.total_values),
                                      &decoded_count));
        if (UNLIKELY(decoded_count != selection.total_values)) {
            return Status::IOError("Expected {} Parquet delta values, decoded {}",
                                   selection.total_values, decoded_count);
        }
        size_t output = 0;
        for (const auto& range : selection.ranges) {
            memmove(_values.data() + output, _values.data() + range.first, range.count * sizeof(T));
            output += range.count;
        }
        DORIS_CHECK_EQ(output, selection.selected_values);
        return consumer.consume(reinterpret_cast<const uint8_t*>(_values.data()), output,
                                sizeof(T));
    }

    Status decode(T* buffer, uint32_t num_values, uint32_t* out_num_values) {
        return _get_internal(buffer, num_values, out_num_values);
    }

    uint32_t valid_values_count() {
        // _total_value_count in header ignores of null values
        return _total_values_remaining;
    }

    void release_scratch(size_t max_retained_bytes) override {
        release_vector_if_oversized(&_values, max_retained_bytes);
        release_vector_if_oversized(&_delta_bit_widths, max_retained_bytes);
    }
    size_t retained_scratch_bytes() const override {
        return _values.capacity() * sizeof(T) + _delta_bit_widths.capacity() * sizeof(uint8_t);
    }
    size_t active_scratch_bytes() const override {
        return _values.size() * sizeof(T) + _delta_bit_widths.size() * sizeof(uint8_t);
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
            : _len_decoder(), _buffered_length(0), _buffered_data(0) {}

    void set_expected_values(size_t expected_values) override {
        Decoder::set_expected_values(expected_values);
        _len_decoder.set_expected_values(expected_values);
    }

    Status skip_values(size_t num_values) override {
        constexpr size_t kSkipBatchSize = 4096;
        _values.resize(std::min(num_values, kSkipBatchSize));
        size_t skipped = 0;
        while (skipped < num_values) {
            const size_t batch_size = std::min(num_values - skipped, kSkipBatchSize);
            int num_valid_values = 0;
            RETURN_IF_ERROR(
                    _get_internal(_values.data(), static_cast<int>(batch_size), &num_valid_values));
            if (UNLIKELY(num_valid_values != batch_size)) {
                return Status::IOError(
                        "Expected to skip {} Parquet delta-length values, skipped {}", num_values,
                        skipped + num_valid_values);
            }
            skipped += batch_size;
        }
        return Status::OK();
    }

    Status decode_binary_values(size_t num_values, ParquetBinaryValueConsumer& consumer) override {
        _values.resize(num_values);
        int decoded_count = 0;
        RETURN_IF_ERROR(
                _get_internal(_values.data(), cast_set<int32_t>(num_values), &decoded_count));
        if (UNLIKELY(decoded_count != num_values)) {
            return Status::IOError("Expected {} Parquet delta-length values, decoded {}",
                                   num_values, decoded_count);
        }
        _string_refs.resize(num_values);
        for (size_t row = 0; row < num_values; ++row) {
            _string_refs[row] = StringRef(_values[row].data, _values[row].size);
        }
        return consumer.consume(_string_refs.data(), _string_refs.size());
    }

    Status decode_selected_binary_values(const ParquetSelection& selection,
                                         ParquetBinaryValueConsumer& consumer) override {
        _values.resize(selection.total_values);
        int decoded_count = 0;
        RETURN_IF_ERROR(_get_internal(_values.data(), cast_set<int32_t>(selection.total_values),
                                      &decoded_count));
        if (UNLIKELY(decoded_count != selection.total_values)) {
            return Status::IOError("Expected {} Parquet delta-length values, decoded {}",
                                   selection.total_values, decoded_count);
        }
        _string_refs.resize(selection.selected_values);
        size_t output = 0;
        for (const auto& range : selection.ranges) {
            for (size_t row = 0; row < range.count; ++row) {
                const auto& value = _values[range.first + row];
                _string_refs[output++] = StringRef(value.data, value.size);
            }
        }
        DORIS_CHECK_EQ(output, selection.selected_values);
        return consumer.consume(_string_refs.data(), _string_refs.size());
    }

    Status decode(Slice* buffer, int num_values, int* out_num_values) {
        return _get_internal(buffer, num_values, out_num_values);
    }

    int valid_values_count() const { return _num_valid_values; }

    void release_scratch(size_t max_retained_bytes) override {
        release_vector_if_oversized(&_values, max_retained_bytes);
        release_vector_if_oversized(&_string_refs, max_retained_bytes);
        release_vector_if_oversized(&_buffered_length, max_retained_bytes);
        release_vector_if_oversized(&_buffered_data, max_retained_bytes);
        _len_decoder.release_scratch(max_retained_bytes);
    }
    size_t retained_scratch_bytes() const override {
        return _values.capacity() * sizeof(Slice) + _string_refs.capacity() * sizeof(StringRef) +
               _buffered_length.capacity() * sizeof(int32_t) +
               _buffered_data.capacity() * sizeof(char) + _len_decoder.retained_scratch_bytes();
    }
    size_t active_scratch_bytes() const override {
        return _values.size() * sizeof(Slice) + _string_refs.size() * sizeof(StringRef) +
               _buffered_length.size() * sizeof(int32_t) + _buffered_data.size() * sizeof(char) +
               _len_decoder.active_scratch_bytes();
    }

    Status set_data(Slice* slice) override {
        if (slice == nullptr || slice->size == 0) {
            // Reused decoders must never retain lengths or payload pointers from the prior page.
            _bit_reader.reset();
            _data = nullptr;
            _offset = 0;
            _num_valid_values = 0;
            _buffered_length.clear();
            _buffered_data.clear();
            return Status::Corruption("Parquet delta-length page is empty");
        }
        _bit_reader = std::make_shared<BitReader>((const uint8_t*)slice->data, slice->size);
        _data = slice;
        _offset = 0;
        RETURN_IF_ERROR(_init_lengths());
        return Status::OK();
    }

    Status set_bit_reader(std::shared_ptr<BitReader> bit_reader) {
        _bit_reader = std::move(bit_reader);
        RETURN_IF_ERROR(_init_lengths());
        return Status::OK();
    }

private:
    Status _init_lengths();
    Status _get_internal(Slice* buffer, int max_values, int* out_num_values);

    std::vector<Slice> _values;
    std::vector<StringRef> _string_refs;
    std::shared_ptr<BitReader> _bit_reader;
    DeltaBitPackDecoder<int32_t> _len_decoder;

    int _num_valid_values;
    std::vector<int32_t> _buffered_length;
    std::vector<char> _buffered_data;
};

class DeltaByteArrayDecoder : public DeltaDecoder {
public:
    explicit DeltaByteArrayDecoder() : _buffered_prefix_length(0), _buffered_data(0) {}

    void set_expected_values(size_t expected_values) override {
        Decoder::set_expected_values(expected_values);
        _prefix_len_decoder.set_expected_values(expected_values);
        _suffix_decoder.set_expected_values(expected_values);
    }

    Status skip_values(size_t num_values) override {
        constexpr size_t kSkipBatchSize = 4096;
        size_t skipped = 0;
        while (skipped < num_values) {
            const size_t batch_size = std::min(num_values - skipped, kSkipBatchSize);
            RETURN_IF_ERROR(_decode_slices(batch_size));
            RETURN_IF_ERROR(_validate_fixed_width_values());
            skipped += batch_size;
        }
        return Status::OK();
    }

    Status decode_binary_values(size_t num_values, ParquetBinaryValueConsumer& consumer) override {
        RETURN_IF_ERROR(_decode_slices(num_values));
        _string_refs.resize(num_values);
        for (size_t row = 0; row < num_values; ++row) {
            _string_refs[row] = StringRef(_values[row].data, _values[row].size);
        }
        return consumer.consume(_string_refs.data(), _string_refs.size());
    }

    Status decode_selected_binary_values(const ParquetSelection& selection,
                                         ParquetBinaryValueConsumer& consumer) override {
        RETURN_IF_ERROR(_decode_slices(selection.total_values));
        _string_refs.resize(selection.selected_values);
        size_t output = 0;
        for (const auto& range : selection.ranges) {
            for (size_t row = 0; row < range.count; ++row) {
                const auto& value = _values[range.first + row];
                _string_refs[output++] = StringRef(value.data, value.size);
            }
        }
        DORIS_CHECK_EQ(output, selection.selected_values);
        return consumer.consume(_string_refs.data(), _string_refs.size());
    }

    Status decode_fixed_values(size_t num_values, ParquetFixedValueConsumer& consumer) override {
        RETURN_IF_ERROR(_decode_slices(num_values));
        RETURN_IF_ERROR(_validate_fixed_width_values());
        const size_t byte_size = num_values * static_cast<size_t>(_type_length);
        _fixed_values.resize(byte_size);
        for (size_t row = 0; row < num_values; ++row) {
            memcpy(_fixed_values.data() + row * _type_length, _values[row].data, _type_length);
        }
        return consumer.consume(_fixed_values.data(), num_values,
                                static_cast<size_t>(_type_length));
    }

    Status decode_selected_fixed_values(const ParquetSelection& selection,
                                        ParquetFixedValueConsumer& consumer) override {
        RETURN_IF_ERROR(_decode_slices(selection.total_values));
        // Validate every consumed value, including filtered ranges, so selection cannot hide a
        // malformed FIXED_LEN_BYTE_ARRAY width that a later cursor advance has already committed.
        RETURN_IF_ERROR(_validate_fixed_width_values());
        DORIS_CHECK(_type_length > 0);
        const size_t value_width = static_cast<size_t>(_type_length);
        if (UNLIKELY(selection.selected_values >
                     std::numeric_limits<size_t>::max() / value_width)) {
            return Status::IOError("Parquet delta-byte-array selection byte size overflows");
        }
        _fixed_values.resize(selection.selected_values * value_width);
        size_t output = 0;
        for (const auto& range : selection.ranges) {
            for (size_t row = 0; row < range.count; ++row) {
                const auto& value = _values[range.first + row];
                memcpy(_fixed_values.data() + output * value_width, value.data, value_width);
                ++output;
            }
        }
        DORIS_CHECK_EQ(output, selection.selected_values);
        return consumer.consume(_fixed_values.data(), output, value_width);
    }

    Status set_data(Slice* slice) override {
        _bit_reader = std::make_shared<BitReader>((const uint8_t*)slice->data, slice->size);
        auto prefix_reader = std::make_shared<BitReader>(*_bit_reader);
        RETURN_IF_ERROR(_prefix_len_decoder.set_bit_reader(prefix_reader));

        // get the number of encoded prefix lengths
        int num_prefix = _prefix_len_decoder.valid_values_count();
        DeltaBitPackDecoder<int32_t> prefix_locator;
        prefix_locator.set_expected_values(_expected_values);
        RETURN_IF_ERROR(prefix_locator.set_bit_reader(_bit_reader));
        constexpr size_t kLocateBatchSize = 4096;
        std::vector<int32_t> ignored_prefixes(
                std::min<size_t>(static_cast<size_t>(num_prefix), kLocateBatchSize));
        size_t located = 0;
        while (located < static_cast<size_t>(num_prefix)) {
            const size_t batch_size =
                    std::min(static_cast<size_t>(num_prefix) - located, kLocateBatchSize);
            uint32_t decoded = 0;
            RETURN_IF_ERROR(prefix_locator.decode(ignored_prefixes.data(),
                                                  static_cast<uint32_t>(batch_size), &decoded));
            if (UNLIKELY(decoded != batch_size)) {
                return Status::Corruption("Parquet delta prefix stream ended early");
            }
            located += batch_size;
        }
        _num_valid_values = num_prefix;

        // at this time, the decoder_ will be at the start of the encoded suffix data.
        RETURN_IF_ERROR(_suffix_decoder.set_bit_reader(_bit_reader));
        if (UNLIKELY(_suffix_decoder.valid_values_count() != num_prefix)) {
            return Status::Corruption("Parquet delta prefix and suffix counts differ");
        }

        // TODO: read corrupted files written with bug(PARQUET-246). _last_value should be set
        // to _last_value_in_previous_page when decoding a new page(except the first page)
        _last_value = "";
        return Status::OK();
    }

    Status decode(Slice* buffer, int num_values, int* out_num_values) {
        return _get_internal(buffer, num_values, out_num_values);
    }

    void release_scratch(size_t max_retained_bytes) override {
        release_vector_if_oversized(&_values, max_retained_bytes);
        release_vector_if_oversized(&_string_refs, max_retained_bytes);
        release_vector_if_oversized(&_fixed_values, max_retained_bytes);
        release_vector_if_oversized(&_buffered_prefix_length, max_retained_bytes);
        release_vector_if_oversized(&_buffered_data, max_retained_bytes);
        _prefix_len_decoder.release_scratch(max_retained_bytes);
        _suffix_decoder.release_scratch(max_retained_bytes);
        if (_last_value.capacity() > max_retained_bytes) std::string().swap(_last_value);
        if (_last_value_in_previous_page.capacity() > max_retained_bytes) {
            std::string().swap(_last_value_in_previous_page);
        }
    }
    size_t retained_scratch_bytes() const override {
        return _values.capacity() * sizeof(Slice) + _string_refs.capacity() * sizeof(StringRef) +
               _fixed_values.capacity() * sizeof(uint8_t) +
               _buffered_prefix_length.capacity() * sizeof(int32_t) +
               _buffered_data.capacity() * sizeof(char) + _last_value.capacity() +
               _last_value_in_previous_page.capacity() +
               _prefix_len_decoder.retained_scratch_bytes() +
               _suffix_decoder.retained_scratch_bytes();
    }
    size_t active_scratch_bytes() const override {
        return _values.size() * sizeof(Slice) + _string_refs.size() * sizeof(StringRef) +
               _fixed_values.size() * sizeof(uint8_t) +
               _buffered_prefix_length.size() * sizeof(int32_t) +
               _buffered_data.size() * sizeof(char) + _last_value.size() +
               _last_value_in_previous_page.size() + _prefix_len_decoder.active_scratch_bytes() +
               _suffix_decoder.active_scratch_bytes();
    }

private:
    Status _validate_fixed_width_values() const {
        if (_type_length <= 0) {
            return Status::OK();
        }
        const size_t value_width = static_cast<size_t>(_type_length);
        for (const auto& value : _values) {
            if (UNLIKELY(value.size != value_width)) {
                return Status::Corruption("Parquet fixed value has length {}, expected {}",
                                          value.size, value_width);
            }
        }
        return Status::OK();
    }

    Status _decode_slices(size_t num_values) {
        _values.resize(num_values);
        int decoded_count = 0;
        RETURN_IF_ERROR(
                _get_internal(_values.data(), cast_set<int32_t>(num_values), &decoded_count));
        if (UNLIKELY(decoded_count != num_values)) {
            return Status::IOError("Expected {} Parquet delta-byte-array values, decoded {}",
                                   num_values, decoded_count);
        }
        return Status::OK();
    }

    Status _get_internal(Slice* buffer, int max_values, int* out_num_values);

    std::vector<Slice> _values;
    std::vector<StringRef> _string_refs;
    std::vector<uint8_t> _fixed_values;
    std::shared_ptr<BitReader> _bit_reader;
    DeltaBitPackDecoder<int32_t> _prefix_len_decoder;
    DeltaLengthByteArrayDecoder _suffix_decoder;
    std::string _last_value;
    // string buffer for last value in previous page
    std::string _last_value_in_previous_page;
    int _num_valid_values;
    std::vector<int32_t> _buffered_prefix_length;
    std::vector<char> _buffered_data;
};
} // namespace doris::format::parquet::native

namespace doris::format::parquet::native {

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
    // Encoded counts are external ULEB32 values. Bound them by the page's advertised logical
    // values before any vector uses the count; optional levels may only reduce this upper bound.
    if (UNLIKELY(_total_value_count > _expected_values)) {
        return Status::Corruption("Parquet delta header advertises {} values, page allows {}",
                                  _total_value_count, _expected_values);
    }
    _total_values_remaining = _total_value_count;
    _delta_bit_widths.clear();
    // init as empty property
    _block_initialized = false;
    _delta_bit_width = 0;
    _values_remaining_current_mini_block = 0;
    return Status::OK();
}

template <typename T>
Status DeltaBitPackDecoder<T>::_init_block() {
    DCHECK_GT(_total_values_remaining, 0) << "InitBlock called at EOF";
    if (!_bit_reader->GetZigZagVlqInt(&_min_delta)) {
        return Status::IOError("Init block eof");
    }

    // One byte follows for each miniblock. Defer allocation until a block is actually consumed so
    // a one-value page cannot turn an irrelevant malicious miniblock count into a large resize.
    if (UNLIKELY(_mini_blocks_per_block > static_cast<uint32_t>(_bit_reader->bytes_left()))) {
        return Status::Corruption("Parquet delta miniblock count {} exceeds remaining {} bytes",
                                  _mini_blocks_per_block, _bit_reader->bytes_left());
    }
    _delta_bit_widths.resize(_mini_blocks_per_block);

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
        int64_t padding_bits = 0;
        // Footer-controlled miniblock counts can exceed 32-bit products. Widen before multiplying;
        // BitReader::Advance then verifies that the full declared padding exists in the stream.
        RETURN_IF_ERROR(detail::checked_delta_padding_bits(cast_set<uint32_t>(_delta_bit_width),
                                                           _values_remaining_current_mini_block,
                                                           &padding_bits));
        if (!_bit_reader->Advance(padding_bits)) {
            return Status::IOError("Skip padding EOF");
        }
        _values_remaining_current_mini_block = 0;
    }
    *out_num_values = num_values;
    return Status::OK();
}

} // namespace doris::format::parquet::native
