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
#include "runtime/mem_pool.h"
#include "util/coding.h"
#include "util/faststring.h"
#include "vec/columns/column_complex.h"
#include "vec/columns/column_nullable.h"

namespace doris {
namespace segment_v2 {

class BinaryPlainPageBuilder : public PageBuilder {
public:
    BinaryPlainPageBuilder(const PageBuilderOptions& options)
            : _size_estimate(0), _prepared_size(0), _options(options) {
        reset();
    }

    bool is_page_full() override {
        // data_page_size is 0, do not limit the page size
        return _options.data_page_size != 0 && (_size_estimate > _options.data_page_size ||
                                                _prepared_size > _options.data_page_size);
    }

    Status add(const uint8_t* vals, size_t* count) override {
        DCHECK(!_finished);
        DCHECK_GT(*count, 0);
        size_t i = 0;

        // If the page is full, should stop adding more items.
        while (!is_page_full() && i < *count) {
            auto src = reinterpret_cast<const Slice*>(vals);
            size_t offset = _buffer.size();
            _offsets.push_back(offset);
            _buffer.append(src->data, src->size);

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

    void reset() override {
        _offsets.clear();
        _buffer.clear();
        _buffer.reserve(_options.data_page_size == 0 ? 1024 : _options.data_page_size);
        _size_estimate = sizeof(uint32_t);
        _prepared_size = sizeof(uint32_t);
        _finished = false;
        _last_value_size = 0;
    }

    size_t count() const override { return _offsets.size(); }

    uint64_t size() const override { return _size_estimate; }

    Status get_first_value(void* value) const override {
        DCHECK(_finished);
        if (_offsets.size() == 0) {
            return Status::NotFound("page is empty");
        }
        *reinterpret_cast<Slice*>(value) = Slice(_first_value);
        return Status::OK();
    }
    Status get_last_value(void* value) const override {
        DCHECK(_finished);
        if (_offsets.size() == 0) {
            return Status::NotFound("page is empty");
        }
        *reinterpret_cast<Slice*>(value) = Slice(_last_value);
        return Status::OK();
    }

    void update_prepared_size(size_t added_size) {
        _prepared_size += added_size;
        _prepared_size += sizeof(uint32_t);
    }

private:
    void _copy_value_at(size_t idx, faststring* value) const {
        size_t value_size =
                (idx < _offsets.size() - 1) ? _offsets[idx + 1] - _offsets[idx] : _last_value_size;
        value->assign_copy(&_buffer[_offsets[idx]], value_size);
    }

    faststring _buffer;
    size_t _size_estimate;
    size_t _prepared_size;
    // Offsets of each entry, relative to the start of the page
    std::vector<uint32_t> _offsets;
    bool _finished;
    PageBuilderOptions _options;
    // size of last added value
    uint32_t _last_value_size = 0;
    faststring _first_value;
    faststring _last_value;
};

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
            std::stringstream ss;
            ss << "file corruption: not enough bytes for trailer in BinaryPlainPageDecoder ."
                  "invalid data size:"
               << _data.size << ", trailer size:" << sizeof(uint32_t);
            return Status::Corruption(ss.str());
        }

        // Decode trailer
        _num_elems = decode_fixed32_le((const uint8_t*)&_data[_data.get_size() - sizeof(uint32_t)]);
        _offsets_pos = _data.get_size() - (_num_elems + 1) * sizeof(uint32_t);

        _parsed = true;

        return Status::OK();
    }

    Status seek_to_position_in_page(size_t pos) override {
        DCHECK_LE(pos, _num_elems);
        _cur_idx = pos;
        return Status::OK();
    }

    Status next_batch(size_t* n, ColumnBlockView* dst) override {
        DCHECK(_parsed);
        if (PREDICT_FALSE(*n == 0 || _cur_idx >= _num_elems)) {
            *n = 0;
            return Status::OK();
        }
        const size_t max_fetch = std::min(*n, static_cast<size_t>(_num_elems - _cur_idx));

        Slice* out = reinterpret_cast<Slice*>(dst->data());
        size_t mem_len[max_fetch];
        for (size_t i = 0; i < max_fetch; i++, out++, _cur_idx++) {
            *out = string_at_index(_cur_idx);
            mem_len[i] = out->size;
        }

        // use SIMD instruction to speed up call function `RoundUpToPowerOfTwo`
        auto mem_size = 0;
        for (int i = 0; i < max_fetch; ++i) {
            mem_len[i] = BitUtil::RoundUpToPowerOf2Int32(mem_len[i], MemPool::DEFAULT_ALIGNMENT);
            mem_size += mem_len[i];
        }

        // allocate a batch of memory and do memcpy
        out = reinterpret_cast<Slice*>(dst->data());
        char* destination = (char*)dst->column_block()->pool()->allocate(mem_size);
        if (destination == nullptr) {
            return Status::MemoryAllocFailed(
                    strings::Substitute("memory allocate failed, size:$0", mem_size));
        }
        for (int i = 0; i < max_fetch; ++i) {
            out->relocate(destination);
            destination += mem_len[i];
            ++out;
        }

        *n = max_fetch;
        return Status::OK();
    }

    Status next_batch(size_t* n, vectorized::MutableColumnPtr& dst) override {
        DCHECK(_parsed);
        if (PREDICT_FALSE(*n == 0 || _cur_idx >= _num_elems)) {
            *n = 0;
            return Status::OK();
        }
        const size_t max_fetch = std::min(*n, static_cast<size_t>(_num_elems - _cur_idx));

        uint32_t len_array[max_fetch];
        uint32_t start_offset_array[max_fetch];
        for (int i = 0; i < max_fetch; i++, _cur_idx++) {
            const uint32_t start_offset = offset(_cur_idx);
            uint32_t len = offset(_cur_idx + 1) - start_offset;
            len_array[i] = len;
            start_offset_array[i] = start_offset;
        }
        dst->insert_many_binary_data(_data.mutable_data(), len_array, start_offset_array,
                                     max_fetch);

        *n = max_fetch;
        return Status::OK();
    };

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

    void get_dict_word_info(StringRef* dict_word_info) {
        if (_num_elems <= 0) [[unlikely]] return;

        char* data_begin = (char*)&_data[0];
        char* offset_ptr = (char*)&_data[_offsets_pos];

        for (uint32_t i = 0; i < _num_elems; ++i) {
            dict_word_info[i].data = data_begin + decode_fixed32_le((uint8_t*)offset_ptr);
            offset_ptr += sizeof(uint32_t);
        }

        for (int i = 0; i < (int)_num_elems - 1; ++i) {
            dict_word_info[i].size =
                    (char*)dict_word_info[i + 1].data - (char*)dict_word_info[i].data;
        }

        dict_word_info[_num_elems - 1].size =
                (data_begin + _offsets_pos) - (char*)dict_word_info[_num_elems - 1].data;
    }

private:
    // Return the offset within '_data' where the string value with index 'idx' can be found.
    uint32_t offset(size_t idx) const {
        if (idx >= _num_elems) {
            return _offsets_pos;
        }
        const uint8_t* p =
                reinterpret_cast<const uint8_t*>(&_data[_offsets_pos + idx * sizeof(uint32_t)]);
        return decode_fixed32_le(p);
    }

    Slice _data;
    PageDecoderOptions _options;
    bool _parsed;

    uint32_t _num_elems;
    uint32_t _offsets_pos;

    // Index of the currently seeked element in the page.
    uint32_t _cur_idx;
    friend class BinaryDictPageDecoder;
    friend class FileColumnIterator;
};

} // namespace segment_v2
} // namespace doris
