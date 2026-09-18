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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <ostream>
#include <vector>

#ifdef __AVX2__
#include <immintrin.h>
#endif

#include "common/status.h"
#include "core/custom_allocator.h"
#include "core/data_type_serde/parquet_decode_source.h"
#include "core/types.h"
#include "util/slice.h"

namespace doris {
template <typename T>
class RleBatchDecoder;
} // namespace doris

namespace doris::format::parquet::native {

inline bool dictionary_indices_in_bounds(const uint32_t* indices, size_t count,
                                         size_t dictionary_size) {
    if (count == 0) {
        return true;
    }
    if (dictionary_size == 0) {
        return false;
    }
    uint32_t max_index = 0;
    size_t row = 0;
#ifdef __AVX2__
    __m256i vector_max = _mm256_setzero_si256();
    for (; row + 8 <= count; row += 8) {
        const auto values = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(indices + row));
        vector_max = _mm256_max_epu32(vector_max, values);
    }
    alignas(32) uint32_t lanes[8];
    _mm256_store_si256(reinterpret_cast<__m256i*>(lanes), vector_max);
    for (const auto lane : lanes) {
        max_index = std::max(max_index, lane);
    }
#endif
    for (; row < count; ++row) {
        max_index = std::max(max_index, indices[row]);
    }
    return static_cast<size_t>(max_index) < dictionary_size;
}

class Decoder : public ParquetDecodeSource {
public:
    Decoder() = default;
    virtual ~Decoder() = default;

    static Status get_decoder(tparquet::Type::type type, tparquet::Encoding::type encoding,
                              std::unique_ptr<Decoder>& decoder);

    // The type with fix length
    void set_type_length(int32_t type_length) { _type_length = type_length; }

    // Set the data to be decoded
    virtual Status set_data(Slice* data) {
        _data = data;
        _offset = 0;
        return Status::OK();
    }

    // Page headers provide an upper bound before encoding-specific headers are trusted.
    virtual void set_expected_values(size_t expected_values) { _expected_values = expected_values; }

    Status decode_fixed_values(size_t num_values, ParquetFixedValueConsumer& consumer) override {
        return Status::NotSupported("Fixed values are not supported by this Parquet decoder");
    }

    Status decode_binary_values(size_t num_values, ParquetBinaryValueConsumer& consumer) override {
        return Status::NotSupported("Binary values are not supported by this Parquet decoder");
    }

    Status skip_values(size_t num_values) override = 0;

    virtual void release_scratch(size_t max_retained_bytes) {}
    virtual size_t retained_scratch_bytes() const { return 0; }
    virtual size_t active_scratch_bytes() const { return 0; }

    virtual Status set_dict(DorisUniqueBufferPtr<uint8_t>& dict, int32_t length,
                            size_t num_values) {
        return Status::NotSupported("set_dict is not supported");
    }

protected:
    template <typename T>
    static void release_vector_if_oversized(std::vector<T>* values, size_t max_retained_bytes) {
        if (values->capacity() * sizeof(T) > max_retained_bytes) {
            std::vector<T>().swap(*values);
        }
    }

    int32_t _type_length = -1;
    Slice* _data = nullptr;
    // Page offsets are host-sized so checked advances cannot wrap at the 4 GiB boundary.
    size_t _offset = 0;
    size_t _expected_values = std::numeric_limits<size_t>::max();
};

class BaseDictDecoder : public Decoder {
public:
    // Out-of-line together with every member that dereferences _index_batch_decoder:
    // keeping the RleBatchDecoder<uint32_t> machinery out of this header stops every
    // includer from instantiating the RLE decode chain; the complete type lives only
    // in decoder.cpp. All of these are per-page/per-batch virtual calls. The ctor is
    // out of line for the same reason: a defaulted-in-class one would be defined in
    // every TU that constructs a derived decoder, and it odr-uses the member's dtor.
    BaseDictDecoder();
    ~BaseDictDecoder() override;

    // Set the data to be decoded
    Status set_data(Slice* data) override;

    bool has_dictionary() const override { return true; }
    uint64_t dictionary_generation() const override { return _dictionary_generation; }

    Status decode_dictionary_indices(size_t num_values, std::vector<uint32_t>* indices) override;

    Status decode_selected_dictionary_indices(const ParquetSelection& selection,
                                              std::vector<uint32_t>* indices) override;

    Status decode_dictionary_values(size_t num_values,
                                    ParquetDictionaryValueConsumer& consumer) override {
        return _decode_dictionary_values(num_values, 0, dictionary_size(), consumer);
    }

    Status decode_selected_dictionary_values(const ParquetSelection& selection,
                                             ParquetDictionaryValueConsumer& consumer) override {
        const size_t num_dictionary_values = dictionary_size();
        if (_is_fragmented_selection(selection)) {
            RETURN_IF_ERROR(_decode_fragmented_selection(selection, num_dictionary_values));
            return consumer.consume_indices(_skip_indices.data(), selection.selected_values);
        }
        size_t cursor = 0;
        for (const auto& range : selection.ranges) {
            DORIS_CHECK(range.first >= cursor);
            RETURN_IF_ERROR(_decode_and_validate_skipped(range.first - cursor, cursor,
                                                         num_dictionary_values));
            RETURN_IF_ERROR(_decode_dictionary_values(range.count, range.first,
                                                      num_dictionary_values, consumer));
            cursor = range.first + range.count;
        }
        DORIS_CHECK(cursor <= selection.total_values);
        return _decode_and_validate_skipped(selection.total_values - cursor, cursor,
                                            num_dictionary_values);
    }

    void release_scratch(size_t max_retained_bytes) override {
        release_vector_if_oversized(&_skip_indices, max_retained_bytes);
    }
    size_t retained_scratch_bytes() const override {
        return _skip_indices.capacity() * sizeof(uint32_t);
    }
    size_t active_scratch_bytes() const override { return _skip_indices.size() * sizeof(uint32_t); }

protected:
    static bool _is_fragmented_selection(const ParquetSelection& selection) {
        constexpr size_t MIN_FRAGMENTED_RANGES = 8;
        constexpr size_t MAX_AVERAGE_RANGE_VALUES = 4;
        constexpr size_t MAX_DECODE_EXPANSION = 8;
        return selection.ranges.size() >= MIN_FRAGMENTED_RANGES && selection.selected_values != 0 &&
               selection.total_values / selection.selected_values <= MAX_DECODE_EXPANSION &&
               selection.selected_values <= selection.ranges.size() * MAX_AVERAGE_RANGE_VALUES;
    }

    Status _decode_fragmented_selection(const ParquetSelection& selection,
                                        size_t num_dictionary_values);

    Status skip_values(size_t num_values) override {
        return _decode_and_validate_skipped(num_values, 0, dictionary_size());
    }

    Status _decode_and_validate_skipped(size_t num_values, size_t row_offset,
                                        size_t num_dictionary_values);

    Status _decode_dictionary_values(size_t num_values, size_t row_offset,
                                     size_t num_dictionary_values,
                                     ParquetDictionaryValueConsumer& consumer);

    // For dictionary encoding
    DorisUniqueBufferPtr<uint8_t> _dict;
    std::unique_ptr<RleBatchDecoder<uint32_t>> _index_batch_decoder;
    std::vector<uint32_t> _skip_indices;
    uint64_t _dictionary_generation = 0;
};

} // namespace doris::format::parquet::native
