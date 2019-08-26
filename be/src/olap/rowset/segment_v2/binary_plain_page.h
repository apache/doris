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

#include "util/arena.h"
#include "util/coding.h"
#include "util/faststring.h"
#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/page_builder.h"
#include "olap/rowset/segment_v2/page_decoder.h"
#include "olap/rowset/segment_v2/options.h"
#include "olap/types.h"

namespace doris {
namespace segment_v2 {

class BinaryPlainPageBuilder : public PageBuilder {
public:
    BinaryPlainPageBuilder(const PageBuilderOptions& options) :
            _size_estimate(0),
            _prepared_size(0),
            _options(options) {
        reset();
    }

    bool is_page_full() override {
        // data_page_size is 0, do not limit the page size
        return _options.data_page_size != 0 && (_size_estimate > _options.data_page_size
            || _prepared_size > _options.data_page_size);
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

            _size_estimate += src->size;
            _size_estimate += sizeof(uint32_t);

            i++;
            vals += sizeof(Slice);
        }

        *count = i;
        return Status::OK();
    }

    Slice finish() override {
        _finished = true;

        // Set up trailer
        for (int i = 0; i < _offsets.size(); i++) {
            put_fixed32_le(&_buffer, _offsets[i]);
        }
        put_fixed32_le(&_buffer, _offsets.size());

        return Slice(_buffer);
    }

    void reset() override {
        _offsets.clear();
        _buffer.clear();
        _buffer.reserve(_options.data_page_size == 0 ? 1024 : _options.data_page_size);
        _size_estimate = sizeof(uint32_t);
        _prepared_size = sizeof(uint32_t);
        _finished = false;
    }

    size_t count() const {
        return _offsets.size();
    }

    // this api will release the memory ownership of encoded data
    // Note:
    //     release() should be called after finish
    //     reset() should be called after this function before reuse the builder
    void release() override {
        uint8_t* ret = _buffer.release();
        _buffer.reserve(_options.data_page_size);
        (void) ret;
    }

    void update_prepared_size(size_t added_size) {
        _prepared_size += added_size;
        _prepared_size += sizeof(uint32_t);
    }

private:
    faststring _buffer;
    size_t _size_estimate;
    size_t _prepared_size;
    // Offsets of each entry, relative to the start of the page
    std::vector<uint32_t> _offsets;
    bool _finished;
    PageBuilderOptions _options;
};


class BinaryPlainPageDecoder : public PageDecoder {
public:
    BinaryPlainPageDecoder(Slice data) : BinaryPlainPageDecoder(data, PageDecoderOptions()) { }

    BinaryPlainPageDecoder(Slice data, const PageDecoderOptions& options) : _data(data),
            _options(options),
            _parsed(false),
            _num_elems(0),
            _cur_idx(0) { }

    Status init() override {
        CHECK(!_parsed);

        if (_data.size < sizeof(uint32_t)) {
            std::stringstream ss;
            ss << "file corrupton: not enough bytes for trailer in BinaryPlainPageDecoder ."
                  "invalid data size:" << _data.size << ", trailer size:" << sizeof(uint32_t);
            return Status::Corruption(ss.str());
        }

        // Decode trailer
        _num_elems = decode_fixed32_le((const uint8_t *)&_data[_data.get_size() - sizeof(uint32_t)]);
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
        size_t max_fetch = std::min(*n, static_cast<size_t>(_num_elems - _cur_idx));

        Slice *out = reinterpret_cast<Slice*>(dst->data());
        
        for (size_t i = 0; i < max_fetch; i++, out++, _cur_idx++) {
            Slice elem(string_at_index(_cur_idx));
            out->data = reinterpret_cast<char*>(dst->arena()->Allocate(elem.size * sizeof(uint8_t)));
            out->size = elem.size;
            memcpy(out->data, elem.data, elem.size);
        }

        *n = max_fetch;
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


private:

    // Return the offset within '_data' where the string value with index 'idx' can be found.
    uint32_t offset(int idx) const {
        if (idx >= _num_elems) {
            return _offsets_pos;
        }
        const uint8_t *p = reinterpret_cast<const uint8_t *>(&_data[_offsets_pos + idx * sizeof(uint32_t)]);
        return decode_fixed32_le(p);
    }

    Slice _data;
    PageDecoderOptions _options;
    bool _parsed;

    uint32_t _num_elems;
    uint32_t _offsets_pos;

    // Index of the currently seeked element in the page.
    uint32_t _cur_idx;
};

} // namespace segment_v2
} // namespace doris
