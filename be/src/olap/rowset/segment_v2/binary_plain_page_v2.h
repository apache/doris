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

// Binary plain page encoding V2 with varuint length prefix.
//
// The page consists of:
// Data:
//   |length1(varuint)|binary1|length2(varuint)|binary2|...
// Trailer:
//   num_elems (32-bit fixed)
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

            // Write varuint length prefix
            uint8_t length_buffer[10];  // Max varuint64 size
            uint8_t* ptr = length_buffer;
            ptr = encode_varint64(ptr, src->size);
            size_t length_size = ptr - length_buffer;
            RETURN_IF_CATCH_EXCEPTION(_buffer.append(length_buffer, length_size));

            // Write the actual data
            RETURN_IF_CATCH_EXCEPTION(_buffer.append(src->data, src->size));

            _last_value_size = cast_set<uint32_t>(src->size);
            _size_estimate += length_size + src->size;

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

            // Write trailer: number of elements
            put_fixed32_le(&_buffer, cast_set<uint32_t>(_positions.size()));

            *slice = _buffer.build();
        });
        return Status::OK();
    }

    Status reset() override {
        RETURN_IF_CATCH_EXCEPTION({
            _positions.clear();
            _buffer.clear();
            _buffer.reserve(_options.data_page_size == 0
                                    ? 1024
                                    : std::min(_options.data_page_size, _options.dict_page_size));
            _size_estimate = sizeof(uint32_t);  // For the trailer
            _finished = false;
            _last_value_size = 0;
        });
        return Status::OK();
    }

    size_t count() const override { return _positions.size(); }

    uint64_t size() const override { return _size_estimate; }

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

    inline Slice operator[](size_t idx) const {
        DCHECK(!_finished);
        DCHECK_LT(idx, _positions.size());

        const uint8_t* ptr = reinterpret_cast<const uint8_t*>(&_buffer[_positions[idx]]);
        uint64_t length;
        const uint8_t* data_ptr = decode_varint64_ptr(ptr, ptr + 10, &length);

        return Slice(data_ptr, length);
    }

    inline Slice get(std::size_t idx) const { return (*this)[idx]; }

private:
    BinaryPlainPageV2Builder(const PageBuilderOptions& options)
            : _size_estimate(0), _options(options) {}

    void _copy_value_at(size_t idx, faststring* value) const {
        const uint8_t* ptr = reinterpret_cast<const uint8_t*>(&_buffer[_positions[idx]]);
        uint64_t length;
        const uint8_t* data_ptr = decode_varint64_ptr(ptr, ptr + 10, &length);
        value->assign_copy(data_ptr, length);
    }

    faststring _buffer;
    size_t _size_estimate;
    // Positions of each entry in the buffer (pointing to the varuint length)
    std::vector<uint32_t> _positions;
    bool _finished;
    PageBuilderOptions _options;
    uint32_t _last_value_size = 0;
    faststring _first_value;
    faststring _last_value;
};

template <FieldType Type>
class BinaryPlainPageV2Decoder : public PageDecoder {
public:
    BinaryPlainPageV2Decoder(Slice data) : BinaryPlainPageV2Decoder(data, PageDecoderOptions()) {}

    BinaryPlainPageV2Decoder(Slice data, const PageDecoderOptions& options)
            : _data(data),
              _options(options),
              _parsed(false),
              _num_elems(0),
              _cur_idx(0),
              _cur_pos(0) {}

    Status init() override {
        CHECK(!_parsed);

        if (_data.size < sizeof(uint32_t)) {
            return Status::Corruption(
                    "file corruption: not enough bytes for trailer in BinaryPlainPageV2Decoder. "
                    "invalid data size:{}, trailer size:{}",
                    _data.size, sizeof(uint32_t));
        }

        // Decode trailer (last 4 bytes contain num_elems)
        _num_elems = decode_fixed32_le((const uint8_t*)&_data[_data.get_size() - sizeof(uint32_t)]);

        // Build position index by scanning through the data
        const uint8_t* ptr = reinterpret_cast<const uint8_t*>(_data.data);
        const uint8_t* limit = ptr + _data.get_size() - sizeof(uint32_t);

        _positions.reserve(_num_elems + 1);  // +1 for end boundary
        _lengths.reserve(_num_elems);

        for (uint32_t i = 0; i < _num_elems; i++) {
            if (ptr >= limit) {
                return Status::Corruption(
                        "file corruption: unexpected end of data while parsing BinaryPlainPageV2Decoder");
            }

            // Decode varuint length
            uint64_t length;
            const uint8_t* data_start = decode_varint64_ptr(ptr, limit, &length);
            if (data_start == nullptr) {
                return Status::Corruption(
                        "file corruption: failed to decode varuint in BinaryPlainPageV2Decoder");
            }

            // Store the position of actual data (after varuint)
            _positions.push_back(data_start - reinterpret_cast<const uint8_t*>(_data.data));
            _lengths.push_back(length);

            // Move to next entry
            ptr = data_start + length;

            if (ptr > limit) {
                return Status::Corruption(
                        "file corruption: data extends beyond page in BinaryPlainPageV2Decoder");
            }
        }

        // Add end position for boundary checking
        _positions.push_back(ptr - reinterpret_cast<const uint8_t*>(_data.data));

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
        if (pos < _positions.size()) {
            _cur_pos = _positions[pos];
        }
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
            const uint8_t* data_ptr = reinterpret_cast<const uint8_t*>(_data.data) + _positions[_cur_idx + i];

            _binary_data[i].data = const_cast<char*>(reinterpret_cast<const char*>(data_ptr));
            _binary_data[i].size = _lengths[_cur_idx + i];

            if constexpr (Type == FieldType::OLAP_FIELD_TYPE_BITMAP) {
                if (_options.need_check_bitmap) {
                    RETURN_IF_ERROR(BitmapTypeCode::validate(*data_ptr));
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

            const uint8_t* data_ptr = reinterpret_cast<const uint8_t*>(_data.data) + _positions[ord];

            _binary_data[read_count].data = const_cast<char*>(reinterpret_cast<const char*>(data_ptr));
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

    Slice string_at_index(size_t idx) const {
        const uint8_t* data_ptr = reinterpret_cast<const uint8_t*>(_data.data) + _positions[idx];
        return Slice(data_ptr, _lengths[idx]);
    }

    Status get_dict_word_info(StringRef* dict_word_info) {
        if (UNLIKELY(_num_elems <= 0)) {
            return Status::OK();
        }

        for (uint32_t i = 0; i < _num_elems; ++i) {
            const uint8_t* data_ptr = reinterpret_cast<const uint8_t*>(_data.data) + _positions[i];
            dict_word_info[i].data = const_cast<char*>(reinterpret_cast<const char*>(data_ptr));
            dict_word_info[i].size = _lengths[i];
        }

        return Status::OK();
    }

private:
    Slice _data;
    PageDecoderOptions _options;
    bool _parsed;

    uint32_t _num_elems;

    // Positions of each entry's actual data (after the varuint length)
    std::vector<uint32_t> _positions;
    // Length of each binary data entry
    std::vector<uint32_t> _lengths;

    std::vector<StringRef> _binary_data;

    // Index of the currently seeked element in the page
    size_t _cur_idx;
    // Current position in the data buffer
    size_t _cur_pos;

    friend class BinaryDictPageDecoder;
    friend class FileColumnIterator;
};

#include "common/compile_check_end.h"
} // namespace segment_v2
} // namespace doris