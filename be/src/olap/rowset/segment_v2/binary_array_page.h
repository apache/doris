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

// page encoding fixed length strings
//
// The page consists of:
// Strings:
//   raw strings that were written
// Trailer:
//  num_elems (32-bit fixed)
//  length (32-bit fixed)

#pragma once

#include <gen_cpp/PaloBrokerService_types.h>

#include <cstddef>
#include <cstdint>

#include "olap/rowset/segment_v2/options.h"
#include "olap/rowset/segment_v2/page_builder.h"
#include "olap/rowset/segment_v2/page_decoder.h"
#include "util/coding.h"
#include "util/faststring.h"
namespace doris {
namespace segment_v2 {
class BinaryArrayPageBuilder : public PageBuilder {
public:
    BinaryArrayPageBuilder(const PageBuilderOptions& options) : _size_estimate(0) {
        _page_size = options.is_dict_page ? options.dict_page_size : options.data_page_size;
        reset();
    }

    void reset() override {
        _buffer.clear();
        _buffer.reserve(_page_size);
        _size_estimate = sizeof(uint32_t) * 2;
        _finished = false;
        _elem_length = 0;
    }

    Status add(const uint8_t* vals, size_t* count) override {
        DCHECK(!_finished);
        DCHECK_GT(*count, 0);
        auto src = reinterpret_cast<const Slice*>(vals);
        size_t num_added = 0;
        if (_elem_length == 0) {
            _elem_length = src->size;
        }
        DCHECK_EQ(_elem_length, src->size);
        for (size_t i = 0; i < *count; i++) {
            if (is_page_full()) {
                break;
            }
            const Slice& s = src[i];
            _buffer.append(s.data, s.size);
            _size_estimate += s.size;
            num_added++;
        }
        *count = num_added;
        _num_elem += num_added;

        return Status::OK();
    }

    OwnedSlice finish() override {
        DCHECK(!_finished);
        _finished = true;
        put_fixed32_le(&_buffer, _num_elem);
        put_fixed32_le(&_buffer, _elem_length);
        return _buffer.build();
    }

    bool is_page_full() override { return _size_estimate > _page_size; }

    size_t count() const override { return _num_elem; }

    uint64_t size() const override { return _size_estimate; }

    inline Slice get(std::size_t idx) const override {
        DCHECK(!_finished);
        DCHECK_LT(idx, _num_elem);
        return Slice(&_buffer[idx * _elem_length], _elem_length);
    }

private:
    faststring _buffer;
    size_t _size_estimate;
    bool _finished;
    size_t _page_size;

    uint32_t _num_elem = 0;
    uint32_t _elem_length;
};

class BinaryArrayPageDecoder : public PageDecoder {
public:
    BinaryArrayPageDecoder(Slice data)
            : _data(data), _parsed(false), _num_elems(0), _elem_length(0), _cur_idx(0) {}

    Status init() override {
        CHECK(!_parsed);

        if (_data.size < sizeof(uint32_t) * 2) {
            return Status::Corruption(
                    "file corruption: not enough bytes for trailer in BinaryArrayPageDecoder ."
                    "invalid data size:{}, trailer size:{}",
                    _data.size, sizeof(uint32_t) * 2);
        }

        // Decode trailer
        _num_elems =
                decode_fixed32_le((const uint8_t*)&_data[_data.get_size() - sizeof(uint32_t) * 2]);
        _elem_length =
                decode_fixed32_le((const uint8_t*)&_data[_data.get_size() - sizeof(uint32_t)]);
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

        uint32_t offsets[max_fetch + 1];
        for (size_t i = 0; i < max_fetch + 1; ++i) {
            offsets[i] = (i + _cur_idx) * _elem_length;
        }
        dst->insert_many_continuous_binary_data(_data.data, offsets, max_fetch);

        *n = max_fetch;
        _cur_idx += max_fetch;
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

    Status read_by_rowids(const rowid_t* rowids, ordinal_t page_first_ordinal, size_t* n,
                          vectorized::MutableColumnPtr& dst) override {
        DCHECK(_parsed);
        if (PREDICT_FALSE(*n == 0)) {
            *n = 0;
            return Status::OK();
        }

        auto total = *n;
        size_t read_count = 0;
        uint32_t len_array[total];
        uint32_t start_offset_array[total];
        for (size_t i = 0; i < total; ++i) {
            ordinal_t ord = rowids[i] - page_first_ordinal;
            if (UNLIKELY(ord >= _num_elems)) {
                break;
            }

            const uint32_t start_offset = ord * _elem_length;
            start_offset_array[read_count] = start_offset;
            len_array[read_count] = _elem_length;
            read_count++;
        }

        if (LIKELY(read_count > 0)) {
            dst->insert_many_binary_data(_data.mutable_data(), len_array, start_offset_array,
                                         read_count);
        }

        *n = read_count;
        return Status::OK();
    }

private:
    Slice _data;
    bool _parsed;
    uint32_t _num_elems;
    uint32_t _elem_length;
    // Index of the currently seeked element in the page.
    uint32_t _cur_idx;
};
} // namespace segment_v2
} // namespace doris