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

// Binary plain page encoding V2 with Frame-of-Reference encoded lengths.
//
// The page consists of:
// Region 1 (String Data):
//   |binary1|binary2|binary3|... (contiguous string data)
// Region 2 (Lengths):
//   FOR-encoded lengths using ForEncoder
// Trailer:
//   lengths_offset (32-bit): offset to Region 2
//   num_elems (32-bit): number of elements
//

#pragma once

#include "common/logging.h"
#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/options.h"
#include "olap/rowset/segment_v2/page_builder.h"
#include "olap/rowset/segment_v2/page_decoder.h"
#include "olap/types.h"
#include "util/coding.h"
#include "util/faststring.h"
#include "util/frame_of_reference_coding.h"
#include "vec/columns/column_complex.h"
#include "vec/columns/column_nullable.h"

namespace doris {
namespace segment_v2 {
#include "common/compile_check_begin.h"

template <FieldType Type>
class BinaryPlainPageV2Builder : public PageBuilderHelper<BinaryPlainPageV2Builder<Type>> {
public:
    using Self = BinaryPlainPageV2Builder<Type>;
    friend class PageBuilderHelper<Self>;

    Status init() override { return reset(); }

    bool is_page_full() override {
        bool ret = false;
        if (_options.is_dict_page) {
            ret = _options.dict_page_size != 0 && _size_estimate > _options.dict_page_size;
        } else {
            ret = _options.data_page_size != 0 && _size_estimate > _options.data_page_size;
        }
        return ret;
    }

    Status add(const uint8_t* vals, size_t* count) override {
        DCHECK(!_finished);
        DCHECK_GT(*count, 0);
        size_t i = 0;

        while (!is_page_full() && i < *count) {
            const auto* src = reinterpret_cast<const Slice*>(vals);
            if constexpr (Type == FieldType::OLAP_FIELD_TYPE_BITMAP) {
                if (_options.need_check_bitmap) {
                    RETURN_IF_ERROR(BitmapTypeCode::validate(*(src->data)));
                }
            }

            // Store position for later retrieval
            _positions.push_back(cast_set<uint32_t>(_buffer.size()));

            // Store the length for later FOR encoding
            _lengths.push_back(cast_set<uint32_t>(src->size));

            // Write only the string data (no length prefix)
            RETURN_IF_CATCH_EXCEPTION(_buffer.append(src->data, src->size));

            _last_value_size = cast_set<uint32_t>(src->size);
            _size_estimate += src->size;
            _raw_data_size += src->size;

            i++;
            vals += sizeof(Slice);
        }

        *count = i;
        return Status::OK();
    }

    Status finish(OwnedSlice* slice) override {
        DCHECK(!_finished);
        _finished = true;
        RETURN_IF_CATCH_EXCEPTION({
            // Store first and last values for statistics
            if (_positions.size() > 0) {
                _copy_value_at(0, &_first_value);
                _copy_value_at(_positions.size() - 1, &_last_value);
            }

            // Record the offset to Region 2 (lengths)
            uint32_t lengths_offset = cast_set<uint32_t>(_buffer.size());

            // Encode lengths using ForEncoder
            if (!_lengths.empty()) {
                faststring lengths_buffer;
                ForEncoder<uint32_t> encoder(&lengths_buffer);
                encoder.put_batch(_lengths.data(), _lengths.size());
                encoder.flush();

                // Append encoded lengths to buffer
                _buffer.append(lengths_buffer.data(), lengths_buffer.size());
            }

            // Write trailer: lengths_offset and number of elements
            put_fixed32_le(&_buffer, lengths_offset);
            put_fixed32_le(&_buffer, cast_set<uint32_t>(_positions.size()));

            *slice = _buffer.build();
        });
        return Status::OK();
    }

    Status reset() override {
        RETURN_IF_CATCH_EXCEPTION({
            _positions.clear();
            _lengths.clear();
            _buffer.clear();
            _buffer.reserve(_options.data_page_size == 0
                                    ? 1024
                                    : std::min(_options.data_page_size, _options.dict_page_size));
            _size_estimate = 2 * sizeof(uint32_t); // For the trailer (lengths_offset + num_elems)
            _finished = false;
            _last_value_size = 0;
            _raw_data_size = 0;
        });
        return Status::OK();
    }

    size_t count() const override { return _positions.size(); }

    uint64_t size() const override { return _size_estimate; }

    uint64_t get_raw_data_size() const override { return _raw_data_size; }

    Status get_first_value(void* value) const override {
        DCHECK(_finished);
        if (_positions.size() == 0) {
            return Status::Error<ErrorCode::ENTRY_NOT_FOUND>("page is empty");
        }
        *reinterpret_cast<Slice*>(value) = Slice(_first_value);
        return Status::OK();
    }

    Status get_last_value(void* value) const override {
        DCHECK(_finished);
        if (_positions.size() == 0) {
            return Status::Error<ErrorCode::ENTRY_NOT_FOUND>("page is empty");
        }
        *reinterpret_cast<Slice*>(value) = Slice(_last_value);
        return Status::OK();
    }

    Status get_dict_word(uint32_t value_code, Slice* word) override {
        if (value_code >= _positions.size()) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT>(
                    "value_code {} is out of range [0, {})", value_code, _positions.size());
        }

        const char* data_ptr = reinterpret_cast<const char*>(&_buffer[_positions[value_code]]);
        uint32_t length = _lengths[value_code];

        word->data = const_cast<char*>(data_ptr);
        word->size = length;

        return Status::OK();
    }

private:
    BinaryPlainPageV2Builder(const PageBuilderOptions& options)
            : _size_estimate(0), _options(options) {}

    void _copy_value_at(size_t idx, faststring* value) const {
        const uint8_t* ptr = &_buffer[_positions[idx]];
        uint32_t length = _lengths[idx];
        value->assign_copy(ptr, length);
    }

    faststring _buffer;
    size_t _size_estimate = 0;
    // Positions of each entry in the buffer (pointing to the string data)
    std::vector<uint32_t> _positions;
    // Lengths of each string
    std::vector<uint32_t> _lengths;
    bool _finished = false;
    PageBuilderOptions _options;
    uint32_t _last_value_size = 0;
    uint64_t _raw_data_size = 0;
    faststring _first_value;
    faststring _last_value;
};

template <FieldType Type>
class BinaryPlainPageV2Decoder : public PageDecoder {
public:
    BinaryPlainPageV2Decoder(Slice data) : BinaryPlainPageV2Decoder(data, PageDecoderOptions()) {}

    BinaryPlainPageV2Decoder(Slice data, const PageDecoderOptions& options)
            : _data(data), _options(options), _parsed(false), _num_elems(0), _cur_idx(0) {}

    Status init() override {
        CHECK(!_parsed);

        if (_data.size < 2 * sizeof(uint32_t)) {
            return Status::Corruption(
                    "file corruption: not enough bytes for trailer in BinaryPlainPageV2Decoder. "
                    "invalid data size:{}, trailer size:{}",
                    _data.size, 2 * sizeof(uint32_t));
        }

        // Decode trailer (last 8 bytes: lengths_offset and num_elems)
        uint32_t lengths_offset = decode_fixed32_le(
                (const uint8_t*)&_data[_data.get_size() - 2 * sizeof(uint32_t)]);
        _num_elems = decode_fixed32_le((const uint8_t*)&_data[_data.get_size() - sizeof(uint32_t)]);

        // Decode lengths using ForDecoder
        _positions.reserve(_num_elems);
        _lengths.reserve(_num_elems);

        if (_num_elems > 0) {
            const uint8_t* lengths_data = reinterpret_cast<const uint8_t*>(_data.data) + lengths_offset;
            size_t lengths_size = _data.get_size() - lengths_offset - 2 * sizeof(uint32_t);

            ForDecoder<uint32_t> decoder(lengths_data, lengths_size);
            if (!decoder.init()) {
                return Status::Corruption(
                        "file corruption: failed to init ForDecoder in BinaryPlainPageV2Decoder");
            }

            // Decode all lengths
            std::vector<uint32_t> decoded_lengths(_num_elems);
            if (!decoder.get_batch(decoded_lengths.data(), _num_elems)) {
                return Status::Corruption(
                        "file corruption: failed to decode lengths in BinaryPlainPageV2Decoder");
            }

            // Calculate positions based on lengths
            uint32_t current_pos = 0;
            for (uint32_t i = 0; i < _num_elems; i++) {
                _positions.push_back(current_pos);
                _lengths.push_back(decoded_lengths[i]);
                current_pos += decoded_lengths[i];
            }

            // Verify that the last position matches lengths_offset
            if (current_pos != lengths_offset) {
                return Status::Corruption(
                        "file corruption: calculated string data size {} does not match "
                        "lengths_offset {} in BinaryPlainPageV2Decoder",
                        current_pos, lengths_offset);
            }
        }

        _parsed = true;
        return Status::OK();
    }

    Status seek_to_position_in_page(size_t pos) override {
        if (_num_elems == 0) [[unlikely]] {
            if (pos != 0) {
                return Status::Error<ErrorCode::INTERNAL_ERROR, false>(
                        "seek pos {} is larger than total elements {}", pos, _num_elems);
            }
        }
        DCHECK_LE(pos, _num_elems);
        _cur_idx = pos;
        return Status::OK();
    }

    Status next_batch(size_t* n, vectorized::MutableColumnPtr& dst) override {
        DCHECK(_parsed);
        if (*n == 0 || _cur_idx >= _num_elems) [[unlikely]] {
            *n = 0;
            return Status::OK();
        }

        const size_t max_fetch = std::min(*n, static_cast<size_t>(_num_elems - _cur_idx));

        _binary_data.resize(max_fetch);

        for (size_t i = 0; i < max_fetch; i++) {
            _binary_data[i].data = _data.get_data() + _positions[_cur_idx + i];
            _binary_data[i].size = _lengths[_cur_idx + i];

            if constexpr (Type == FieldType::OLAP_FIELD_TYPE_BITMAP) {
                if (_options.need_check_bitmap) {
                    RETURN_IF_ERROR(BitmapTypeCode::validate(*(_binary_data[i].data)));
                }
            }
        }

        dst->insert_many_strings(_binary_data.data(), max_fetch);
        _cur_idx += max_fetch;

        *n = max_fetch;
        return Status::OK();
    }

    Status read_by_rowids(const rowid_t* rowids, ordinal_t page_first_ordinal, size_t* n,
                          vectorized::MutableColumnPtr& dst) override {
        DCHECK(_parsed);
        if (*n == 0) [[unlikely]] {
            *n = 0;
            return Status::OK();
        }

        auto total = *n;
        size_t read_count = 0;
        _binary_data.resize(total);

        for (size_t i = 0; i < total; ++i) {
            ordinal_t ord = rowids[i] - page_first_ordinal;
            if (UNLIKELY(ord >= _num_elems)) {
                break;
            }

            _binary_data[read_count].data = _data.get_data() + _positions[ord];
            _binary_data[read_count].size = _lengths[ord];
            read_count++;
        }

        if (LIKELY(read_count > 0)) {
            dst->insert_many_strings(_binary_data.data(), read_count);
        }

        *n = read_count;
        return Status::OK();
    }

    size_t count() const override {
        DCHECK(_parsed);
        return _num_elems;
    }

    size_t current_index() const override {
        DCHECK(_parsed);
        return _cur_idx;
    }

    Status get_dict_word_info(StringRef* dict_word_info) override {
        if (UNLIKELY(_num_elems <= 0)) {
            return Status::OK();
        }

        for (uint32_t i = 0; i < _num_elems; ++i) {
            dict_word_info[i].data = _data.get_data() + _positions[i];
            dict_word_info[i].size = _lengths[i];
        }

        return Status::OK();
    }

private:
    Slice _data;
    PageDecoderOptions _options;
    bool _parsed;

    uint32_t _num_elems;

    // Positions of each string data in Region 1
    std::vector<uint32_t> _positions;
    // Length of each binary data entry (decoded from Region 2 using ForDecoder)
    std::vector<uint32_t> _lengths;

    std::vector<StringRef> _binary_data;

    // Index of the currently seeked element in the page
    size_t _cur_idx;

    friend class BinaryDictPageDecoder;
    friend class FileColumnIterator;
};

#include "common/compile_check_end.h"
} // namespace segment_v2
} // namespace doris