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

// Simplistic page encoding for strings.
//
// The page consists of:
// Strings:
//   raw strings that were written
// Trailer
//  Offsets:
//    offsets pointing to the beginning of each string
//  num_elems (32-bit fixed)
//

#pragma once

#include "common/logging.h"
#include "core/column/column_complex.h"
#include "storage/olap_common.h"
#include "storage/segment/options.h"
#include "storage/segment/page_builder.h"
#include "storage/segment/page_decoder.h"
#include "storage/types.h"
#include "util/coding.h"
#include "util/faststring.h"

namespace doris {
namespace segment_v2 {

template <FieldType Type>
class BinaryPlainPageBuilder : public PageBuilderHelper<BinaryPlainPageBuilder<Type>> {
public:
    using Self = BinaryPlainPageBuilder<Type>;
    friend class PageBuilderHelper<Self>;

    Status init() override { return reset(); }

    bool is_page_full() override {
        bool ret = false;
        if (_options.is_dict_page) {
            // dict_page_size is 0, do not limit the page size
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

        // If the page is full, should stop adding more items.
        while (!is_page_full() && i < *count) {
            const auto* src = reinterpret_cast<const Slice*>(vals);
            if constexpr (Type == FieldType::OLAP_FIELD_TYPE_BITMAP) {
                if (_options.need_check_bitmap) {
                    RETURN_IF_ERROR(BitmapTypeCode::validate(*(src->data)));
                }
            }
            size_t offset = _buffer.size();
            _offsets.push_back(cast_set<uint32_t>(offset));
            // This may need a large memory, should return error if could not allocated
            // successfully, to avoid BE OOM.
            RETURN_IF_CATCH_EXCEPTION(_buffer.append(src->data, src->size));

            _last_value_size = cast_set<uint32_t>(src->size);
            _size_estimate += src->size;
            _size_estimate += sizeof(uint32_t);
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
            // Set up trailer
            for (uint32_t _offset : _offsets) {
                put_fixed32_le(&_buffer, _offset);
            }
            put_fixed32_le(&_buffer, cast_set<uint32_t>(_offsets.size()));
            *slice = _buffer.build();
        });
        return Status::OK();
    }

    Status reset() override {
        RETURN_IF_CATCH_EXCEPTION({
            _offsets.clear();
            _buffer.clear();
            _buffer.reserve(_options.data_page_size == 0
                                    ? 1024
                                    : std::min(_options.data_page_size, _options.dict_page_size));
            _size_estimate = sizeof(uint32_t);
            _finished = false;
            _last_value_size = 0;
            _raw_data_size = 0;
        });
        return Status::OK();
    }

    size_t count() const override { return _offsets.size(); }

    uint64_t size() const override { return _size_estimate; }

    uint64_t get_raw_data_size() const override { return _raw_data_size; }

private:
    BinaryPlainPageBuilder(const PageBuilderOptions& options)
            : _size_estimate(0), _options(options) {}

    faststring _buffer;
    size_t _size_estimate;
    // Offsets of each entry, relative to the start of the page
    std::vector<uint32_t> _offsets;
    bool _finished;
    PageBuilderOptions _options;
    // size of last added value
    uint32_t _last_value_size = 0;
    uint64_t _raw_data_size = 0;
};

template <FieldType Type>
class BinaryPlainPageDecoder : public PageDecoder {
public:
    BinaryPlainPageDecoder(Slice data) : BinaryPlainPageDecoder(data, PageDecoderOptions()) {}

    BinaryPlainPageDecoder(Slice data, const PageDecoderOptions& options)
            : _data(data),
              _options(options),
              _parsed(false),
              _num_elems(0),
              _offsets_pos(0),
              _cur_idx(0) {}

    Status init() override {
        CHECK(!_parsed);

        if (_data.size < sizeof(uint32_t)) {
            return Status::Corruption(
                    "file corruption: not enough bytes for trailer in BinaryPlainPageDecoder ."
                    "invalid data size:{}, trailer size:{}",
                    _data.size, sizeof(uint32_t));
        }

        // Decode trailer
        _num_elems = decode_fixed32_le((const uint8_t*)&_data[_data.get_size() - sizeof(uint32_t)]);
        _offsets_pos = cast_set<uint32_t>(_data.get_size() - ((_num_elems + 1) * sizeof(uint32_t)));

        if (_offsets_pos > _data.get_size() - sizeof(uint32_t)) {
            return Status::Corruption(
                    "file corruption: offsets pos beyonds data_size: {}, num_element: {}"
                    ", offset_pos: {}",
                    _data.size, _num_elems, _offsets_pos);
        }
        _parsed = true;

        return Status::OK();
    }

    Status seek_to_position_in_page(size_t pos) override {
        if (_num_elems == 0) [[unlikely]] {
            if (pos != 0) {
                return Status::Error<ErrorCode::INTERNAL_ERROR, false>(
                        "seek pos {} is larger than total elements  {}", pos, _num_elems);
            }
        }
        DCHECK_LE(pos, _num_elems);
        _cur_idx = pos;
        return Status::OK();
    }

    Status next_batch(size_t* n, MutableColumnPtr& dst) override {
        DCHECK(_parsed);
        if (*n == 0 || _cur_idx >= _num_elems) [[unlikely]] {
            *n = 0;
            return Status::OK();
        }
        const size_t max_fetch = std::min(*n, static_cast<size_t>(_num_elems - _cur_idx));

        if (_options.only_read_offsets) {
            // OFFSET_ONLY mode: read string lengths from page offset trailer
            // without copying actual char data. This allows length() to work.
            _offsets.resize(max_fetch);
            for (size_t i = 0; i < max_fetch; ++i) {
                uint32_t str_start = offset(_cur_idx + i);
                uint32_t str_end = offset(_cur_idx + i + 1);
                _offsets[i] = str_end - str_start;
            }
            dst->insert_offsets_from_lengths(_offsets.data(), max_fetch);
            _cur_idx += max_fetch;
            *n = max_fetch;
            return Status::OK();
        }

        uint32_t last_offset = guarded_offset(_cur_idx);
        _offsets.resize(max_fetch + 1);
        _offsets[0] = last_offset;
        for (int i = 0; i < max_fetch - 1; i++, _cur_idx++) {
            const uint32_t start_offset = last_offset;
            last_offset = guarded_offset(_cur_idx + 1);
            _offsets[i + 1] = last_offset;
            if constexpr (Type == FieldType::OLAP_FIELD_TYPE_BITMAP) {
                if (_options.need_check_bitmap) {
                    RETURN_IF_ERROR(BitmapTypeCode::validate(*(_data.data + start_offset)));
                }
            }
        }
        _cur_idx++;
        _offsets[max_fetch] = offset(_cur_idx);
        if constexpr (Type == FieldType::OLAP_FIELD_TYPE_BITMAP) {
            if (_options.need_check_bitmap) {
                RETURN_IF_ERROR(BitmapTypeCode::validate(*(_data.data + last_offset)));
            }
        }
        dst->insert_many_continuous_binary_data(_data.data, _offsets.data(), max_fetch);

        *n = max_fetch;
        return Status::OK();
    }

    Status read_by_rowids(const rowid_t* rowids, ordinal_t page_first_ordinal, size_t* n,
                          MutableColumnPtr& dst) override {
        DCHECK(_parsed);
        if (*n == 0) [[unlikely]] {
            *n = 0;
            return Status::OK();
        }

        auto total = *n;

        if (_options.only_read_offsets) {
            // OFFSET_ONLY mode: read string lengths from page offset trailer
            // without copying actual char data. This allows length() to work.
            size_t read_count = 0;
            _offsets.resize(total);
            for (size_t i = 0; i < total; ++i) {
                ordinal_t ord = rowids[i] - page_first_ordinal;
                if (UNLIKELY(ord >= _num_elems)) {
                    break;
                }
                uint32_t str_start = offset(ord);
                uint32_t str_end = offset(ord + 1);
                _offsets[read_count] = str_end - str_start;
                read_count++;
            }
            if (read_count > 0) {
                dst->insert_offsets_from_lengths(_offsets.data(), read_count);
            }
            *n = read_count;
            return Status::OK();
        }

        size_t read_count = 0;
        _binary_data.resize(total);
        for (size_t i = 0; i < total; ++i) {
            ordinal_t ord = rowids[i] - page_first_ordinal;
            if (UNLIKELY(ord >= _num_elems)) {
                break;
            }

            const uint32_t start_offset = offset(ord);
            _binary_data[read_count].data = _data.mutable_data() + start_offset;
            _binary_data[read_count].size = offset(ord + 1) - start_offset;
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

        char* data_begin = (char*)&_data[0];
        char* offset_ptr = (char*)&_data[_offsets_pos];

        for (uint32_t i = 0; i < _num_elems; ++i) {
            uint32_t offset = decode_fixed32_le((uint8_t*)offset_ptr);
            if (offset > _offsets_pos) {
                return Status::Corruption(
                        "file corruption: offsets pos beyonds data_size: {}, num_element: {}"
                        ", offset_pos: {}, offset: {}",
                        _data.size, _num_elems, _offsets_pos, offset);
            }
            dict_word_info[i].data = data_begin + offset;
            offset_ptr += sizeof(uint32_t);
        }

        for (int i = 0; i < (int)_num_elems - 1; ++i) {
            dict_word_info[i].size =
                    (char*)dict_word_info[i + 1].data - (char*)dict_word_info[i].data;
        }

        dict_word_info[_num_elems - 1].size =
                (data_begin + _offsets_pos) - (char*)dict_word_info[_num_elems - 1].data;
        return Status::OK();
    }

private:
    static constexpr size_t SIZE_OF_INT32 = sizeof(uint32_t);
    // Return the offset within '_data' where the string value with index 'idx' can be found.
    uint32_t offset(size_t idx) const {
        if (idx >= _num_elems) {
            return _offsets_pos;
        }
        return guarded_offset(idx);
    }

    uint32_t guarded_offset(size_t idx) const {
        const auto* p =
                reinterpret_cast<const uint8_t*>(&_data[_offsets_pos + idx * SIZE_OF_INT32]);
        return decode_fixed32_le(p);
    }

    Slice _data;
    PageDecoderOptions _options;
    bool _parsed;

    uint32_t _num_elems;
    uint32_t _offsets_pos;

    std::vector<uint32_t> _offsets;
    std::vector<StringRef> _binary_data;

    // Index of the currently seeked element in the page.
    size_t _cur_idx;
    friend class BinaryDictPageDecoder;
    friend class FileColumnIterator;
};

} // namespace segment_v2
} // namespace doris
