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

#include "format_v2/parquet/reader/native/delta_bit_pack_decoder.h"

namespace doris::format::parquet::native {
Status DeltaLengthByteArrayDecoder::_init_lengths() {
    auto length_reader = std::make_shared<BitReader>(*_bit_reader);
    RETURN_IF_ERROR(_len_decoder.set_bit_reader(length_reader));
    const uint32_t num_lengths = _len_decoder.valid_values_count();

    DeltaBitPackDecoder<int32_t> length_locator;
    length_locator.set_expected_values(_expected_values);
    RETURN_IF_ERROR(length_locator.set_bit_reader(_bit_reader));
    constexpr size_t kLocateBatchSize = 4096;
    std::vector<int32_t> lengths(std::min<size_t>(num_lengths, kLocateBatchSize));
    size_t located = 0;
    int64_t payload_size = 0;
    while (located < num_lengths) {
        const size_t batch_size = std::min<size_t>(num_lengths - located, kLocateBatchSize);
        uint32_t decoded = 0;
        RETURN_IF_ERROR(
                length_locator.decode(lengths.data(), static_cast<uint32_t>(batch_size), &decoded));
        if (UNLIKELY(decoded != batch_size)) {
            return Status::Corruption("Parquet delta length stream ended early");
        }
        for (size_t i = 0; i < batch_size; ++i) {
            if (UNLIKELY(lengths[i] < 0) ||
                common::add_overflow(payload_size, static_cast<int64_t>(lengths[i]),
                                     payload_size)) {
                return Status::Corruption("Invalid Parquet delta byte-array length");
            }
        }
        located += batch_size;
    }
    // The locator leaves the shared reader at the first payload byte without retaining every
    // length; the independent decoder supplies bounded batches during materialization or skip.
    if (UNLIKELY(payload_size > _bit_reader->bytes_left())) {
        return Status::Corruption("Parquet delta lengths require {} bytes, only {} remain",
                                  payload_size, _bit_reader->bytes_left());
    }
    _num_valid_values = num_lengths;
    return Status::OK();
}

Status DeltaLengthByteArrayDecoder::_get_internal(Slice* buffer, int max_values,
                                                  int* out_num_values) {
    // Decode up to `max_values` strings into an internal buffer
    // and reference them into `buffer`.
    max_values = std::min(max_values, _num_valid_values);
    if (max_values == 0) {
        *out_num_values = 0;
        return Status::OK();
    }

    int64_t data_size = 0;
    _buffered_length.resize(max_values);
    uint32_t lengths_decoded = 0;
    RETURN_IF_ERROR(_len_decoder.decode(_buffered_length.data(), static_cast<uint32_t>(max_values),
                                        &lengths_decoded));
    if (UNLIKELY(lengths_decoded != max_values)) {
        return Status::Corruption("Parquet delta length stream ended early");
    }
    const int32_t* length_ptr = _buffered_length.data();
    for (int i = 0; i < max_values; ++i) {
        int32_t len = length_ptr[i];
        if (len < 0) [[unlikely]] {
            return Status::InvalidArgument("Negative string delta length");
        }
        buffer[i].size = len;
        if (common::add_overflow(data_size, static_cast<int64_t>(len), data_size)) {
            return Status::InvalidArgument("Excess expansion in DELTA_(LENGTH_)BYTE_ARRAY");
        }
    }
    // Every declared byte must exist in this page. Check before resize so a tiny malformed stream
    // cannot reserve memory based only on attacker-controlled decoded lengths.
    if (UNLIKELY(data_size > _bit_reader->bytes_left())) {
        return Status::Corruption("Parquet delta lengths require {} bytes, only {} remain",
                                  data_size, _bit_reader->bytes_left());
    }
    _buffered_data.resize(data_size);
    char* data_ptr = _buffered_data.data();
    for (int64_t j = 0; j < data_size; j++) {
        if (!_bit_reader->GetValue(8, data_ptr + j)) {
            return Status::IOError("Get length bytes EOF");
        }
    }

    for (int i = 0; i < max_values; ++i) {
        buffer[i].data = data_ptr;
        data_ptr += buffer[i].size;
    }
    // this->num_values_ -= max_values;
    _num_valid_values -= max_values;
    *out_num_values = max_values;
    return Status::OK();
}

Status DeltaByteArrayDecoder::_get_internal(Slice* buffer, int max_values, int* out_num_values) {
    // Decode up to `max_values` strings into an internal buffer
    // and reference them into `buffer`.
    max_values = std::min(max_values, _num_valid_values);
    if (max_values == 0) {
        *out_num_values = max_values;
        return Status::OK();
    }

    int suffix_read;
    RETURN_IF_ERROR(_suffix_decoder.decode(buffer, max_values, &suffix_read));
    if (suffix_read != max_values) [[unlikely]] {
        return Status::IOError("Read {}, expecting {} from suffix decoder",
                               std::to_string(suffix_read), std::to_string(max_values));
    }

    int64_t data_size = 0;
    _buffered_prefix_length.resize(max_values);
    uint32_t prefixes_decoded = 0;
    RETURN_IF_ERROR(_prefix_len_decoder.decode(
            _buffered_prefix_length.data(), static_cast<uint32_t>(max_values), &prefixes_decoded));
    if (UNLIKELY(prefixes_decoded != max_values)) {
        return Status::Corruption("Parquet delta prefix stream ended early");
    }
    const int32_t* prefix_len_ptr = _buffered_prefix_length.data();
    size_t preceding_value_size = _last_value.size();
    for (int i = 0; i < max_values; ++i) {
        if (prefix_len_ptr[i] < 0) [[unlikely]] {
            return Status::InvalidArgument("negative prefix length in DELTA_BYTE_ARRAY");
        }
        const size_t prefix_size = static_cast<size_t>(prefix_len_ptr[i]);
        if (prefix_size > preceding_value_size) [[unlikely]] {
            // Prefixes form a dependency chain, so validate each one before aggregate allocation.
            return Status::InvalidArgument("prefix length too large in DELTA_BYTE_ARRAY");
        }
        size_t reconstructed_size = 0;
        if (common::add_overflow(prefix_size, buffer[i].size, reconstructed_size) ||
            reconstructed_size > static_cast<size_t>(std::numeric_limits<int64_t>::max()) ||
            common::add_overflow(data_size, static_cast<int64_t>(reconstructed_size), data_size))
                [[unlikely]] {
            return Status::InvalidArgument("excess expansion in DELTA_BYTE_ARRAY");
        }
        preceding_value_size = reconstructed_size;
    }
    _buffered_data.resize(data_size);

    std::string_view prefix {_last_value};

    char* data_ptr = _buffered_data.data();
    for (int i = 0; i < max_values; ++i) {
        memcpy(data_ptr, prefix.data(), prefix_len_ptr[i]);
        // buffer[i] currently points to the string suffix
        memcpy(data_ptr + prefix_len_ptr[i], buffer[i].data, buffer[i].size);
        buffer[i].data = data_ptr;
        buffer[i].size += prefix_len_ptr[i];
        data_ptr += buffer[i].size;
        prefix = std::string_view {buffer[i].data, buffer[i].size};
    }
    _num_valid_values -= max_values;
    _last_value = std::string {prefix};

    if (_num_valid_values == 0) {
        _last_value_in_previous_page = _last_value;
    }
    *out_num_values = max_values;
    return Status::OK();
}
} // namespace doris::format::parquet::native
