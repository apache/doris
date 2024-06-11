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
#include "gutil/strings/substitute.h"
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
            if constexpr (Type == FieldType::OLAP_FIELD_TYPE_OBJECT) {
                if (_options.need_check_bitmap) {
                    RETURN_IF_ERROR(BitmapTypeCode::validate(*(src->data)));
                }
            }
            size_t offset = _buffer.size();
            _offsets.push_back(offset);
            // This may need a large memory, should return error if could not allocated
            // successfully, to avoid BE OOM.
            RETURN_IF_CATCH_EXCEPTION(_buffer.append(src->data, src->size));

            _last_value_size = src->size;
            _size_estimate += src->size;
            _size_estimate += sizeof(uint32_t);

            i++;
            vals += sizeof(Slice);
        }

        *count = i;
        return Status::OK();
    }

    OwnedSlice finish() override {
        DCHECK(!_finished);
        _finished = true;
        // Set up trailer
        for (uint32_t _offset : _offsets) {
            put_fixed32_le(&_buffer, _offset);
        }
        put_fixed32_le(&_buffer, _offsets.size());
        if (_offsets.size() > 0) {
            _copy_value_at(0, &_first_value);
            _copy_value_at(_offsets.size() - 1, &_last_value);
        }
        return _buffer.build();
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
        });
        return Status::OK();
    }

    size_t count() const override { return _offsets.size(); }

    uint64_t size() const override { return _size_estimate; }

    Status get_first_value(void* value) const override {
        DCHECK(_finished);
        if (_offsets.size() == 0) {
            return Status::Error<ErrorCode::ENTRY_NOT_FOUND>("page is empty");
        }
        *reinterpret_cast<Slice*>(value) = Slice(_first_value);
        return Status::OK();
    }
    Status get_last_value(void* value) const override {
        DCHECK(_finished);
        if (_offsets.size() == 0) {
            return Status::Error<ErrorCode::ENTRY_NOT_FOUND>("page is empty");
        }
        *reinterpret_cast<Slice*>(value) = Slice(_last_value);
        return Status::OK();
    }

    inline Slice operator[](size_t idx) const {
        DCHECK(!_finished);
        DCHECK_LT(idx, _offsets.size());
        size_t value_size =
                (idx < _offsets.size() - 1) ? _offsets[idx + 1] - _offsets[idx] : _last_value_size;
        return Slice(&_buffer[_offsets[idx]], value_size);
    }

    inline Slice get(std::size_t idx) const { return (*this)[idx]; }

private:
    BinaryPlainPageBuilder(const PageBuilderOptions& options)
            : _size_estimate(0), _options(options) {}

    void _copy_value_at(size_t idx, faststring* value) const {
        size_t value_size =
                (idx < _offsets.size() - 1) ? _offsets[idx + 1] - _offsets[idx] : _last_value_size;
        value->assign_copy(&_buffer[_offsets[idx]], value_size);
    }

    faststring _buffer;
    size_t _size_estimate;
    // Offsets of each entry, relative to the start of the page
    std::vector<uint32_t> _offsets;
    bool _finished;
    PageBuilderOptions _options;
    // size of last added value
    uint32_t _last_value_size = 0;
    faststring _first_value;
    faststring _last_value;
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
        _offsets_pos = _data.get_size() - (_num_elems + 1) * sizeof(uint32_t);

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
        DCHECK_LE(pos, _num_elems);
        _cur_idx = pos;
        return Status::OK();
    }

    Status next_batch(size_t* n, vectorized::MutableColumnPtr& dst) override {
        DCHECK(_parsed);
        if (PREDICT_FALSE(*n == 0 || _cur_idx >= _num_elems)) {
            *n = 0;
            return Status::OK();
        }
        const size_t max_fetch = std::min(*n, static_cast<size_t>(_num_elems - _cur_idx));

        uint32_t last_offset = guarded_offset(_cur_idx);
        _offsets.resize(max_fetch + 1);
        _offsets[0] = last_offset;
        for (int i = 0; i < max_fetch - 1; i++, _cur_idx++) {
            const uint32_t start_offset = last_offset;
            last_offset = guarded_offset(_cur_idx + 1);
            _offsets[i + 1] = last_offset;
            if constexpr (Type == FieldType::OLAP_FIELD_TYPE_OBJECT) {
                if (_options.need_check_bitmap) {
                    RETURN_IF_ERROR(BitmapTypeCode::validate(*(_data.data + start_offset)));
                }
            }
        }
        _cur_idx++;
        _offsets[max_fetch] = offset(_cur_idx);
        if constexpr (Type == FieldType::OLAP_FIELD_TYPE_OBJECT) {
            if (_options.need_check_bitmap) {
                RETURN_IF_ERROR(BitmapTypeCode::validate(*(_data.data + last_offset)));
            }
        }
        dst->insert_many_continuous_binary_data(_data.data, _offsets.data(), max_fetch);

        *n = max_fetch;
        return Status::OK();
    }

    Status read_by_rowids(const rowid_t* rowids, ordinal_t page_first_ordinal, size_t* n,
                          vectorized::MutableColumnPtr& dst) override {
        DCHECK(_parsed);
        if (PREDICT_FALSE(*n == 0)) {
            *n = 0;
            return Status::OK();
        }

        auto total = *n;
        size_t read_count = 0;
        _len_array.resize(total);
        _start_offset_array.resize(total);
        for (size_t i = 0; i < total; ++i) {
            ordinal_t ord = rowids[i] - page_first_ordinal;
            if (UNLIKELY(ord >= _num_elems)) {
                break;
            }

            const uint32_t start_offset = offset(ord);
            _start_offset_array[read_count] = start_offset;
            _len_array[read_count] = offset(ord + 1) - start_offset;
            read_count++;
        }

        if (LIKELY(read_count > 0)) {
            dst->insert_many_binary_data(_data.mutable_data(), _len_array.data(),
                                         _start_offset_array.data(), read_count);
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
        const uint32_t start_offset = offset(idx);
        uint32_t len = offset(idx + 1) - start_offset;
        return Slice(&_data[start_offset], len);
    }

    Status get_dict_word_info(StringRef* dict_word_info) {
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
        const uint8_t* p =
                reinterpret_cast<const uint8_t*>(&_data[_offsets_pos + idx * SIZE_OF_INT32]);
        return decode_fixed32_le(p);
    }

    uint32_t guarded_offset(size_t idx) const {
        const uint8_t* p =
                reinterpret_cast<const uint8_t*>(&_data[_offsets_pos + idx * SIZE_OF_INT32]);
        return decode_fixed32_le(p);
    }

    Slice _data;
    PageDecoderOptions _options;
    bool _parsed;

    uint32_t _num_elems;
    uint32_t _offsets_pos;

    std::vector<uint32_t> _offsets;
    std::vector<uint32_t> _len_array;
    std::vector<uint32_t> _start_offset_array;

    // Index of the currently seeked element in the page.
    uint32_t _cur_idx;
    friend class BinaryDictPageDecoder;
    friend class FileColumnIterator;
};

} // namespace segment_v2
} // namespace doris
