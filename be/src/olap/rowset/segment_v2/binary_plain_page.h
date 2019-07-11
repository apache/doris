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
// Header:
//   num_elems (32-bit fixed)
//   offsets_pos (32-bit fixed): position of the first offset, relative to page start
// Strings:
//   raw strings that were written
// Offsets:  [pointed to by offsets_pos]
//   offsets pointing to the beginning of each string

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
            _options(options) {
        _buffer.reserve(_options.data_page_size);
        reset();
    }

    bool is_page_full() override {
        return _size_estimate > _options.data_page_size;
    }

    Status add(const uint8_t *vals, size_t *count) override {
        DCHECK(!_finished);
        DCHECK_GT(*count, 0);
        size_t i = 0;

        // If the page is full, should stop adding more items.
        while (!is_page_full() && i < *count) {
            const Slice *src = reinterpret_cast<const Slice *>(vals);
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

        size_t offsets_pos = _buffer.size();

        // Set up the header
        encode_fixed32_le(&_buffer[0], _offsets.size());
        encode_fixed32_le(&_buffer[4], offsets_pos);

        _buffer.append(&_offsets[0], _offsets.size() * sizeof(uint32_t));

        return Slice(_buffer.data(), _buffer.size());
    }

    void reset() override {
        _offsets.clear();
        _buffer.clear();
        _buffer.resize(MAX_HEADER_SIZE);
        _size_estimate = MAX_HEADER_SIZE;
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
        uint8_t *ret = _buffer.release();
        _buffer.reserve(_options.data_page_size);
        (void) ret;
    }

    // Length of a header.
    static const size_t MAX_HEADER_SIZE = sizeof(uint32_t) * 2;
private:
    faststring _buffer;
    size_t _size_estimate;
    // Offsets of each entry, relative to the start of the page
    std::vector<uint32_t> _offsets;
    bool _finished;
    PageBuilderOptions _options;
};


class BinaryPlainPageDecoder : public PageDecoder {
public:
    BinaryPlainPageDecoder(Slice data, const PageDecoderOptions& options) : _data(data),
            _options(options),
            _parsed(false),
            _num_elems(0),
            _cur_idx(0) { }

    Status init() override {
        CHECK(!_parsed);

        if (_data.size < MIN_HEADER_SIZE) {
            std::stringstream ss;
            ss << "file corrupton: not enough bytes for header in BinaryPlainPageDecoder ."
                  "invalid data size:" << _data.size << ", header size:" << MIN_HEADER_SIZE;
            return Status::InternalError(ss.str());
        }

        // Decode header.
        _num_elems         = decode_fixed32_le((const uint8_t *)&_data[0]);
        size_t offsets_pos = decode_fixed32_le((const uint8_t *)&_data[4]);

        // Sanity check.
        if (offsets_pos > _data.size) {
            std::stringstream ss;
            ss << "offsets_pos "<< offsets_pos << " > page size " << _data.size << " in plain string page";
            return Status::InternalError(ss.str());
        }

        // Decode the string offsets themselves
        const uint8_t *p = (const uint8_t*)(_data.data + offsets_pos);

        // Reserve one extra element, which we'll fill in at the end
        // with an offset past the last element.
        _offsets_buf.resize(sizeof(uint32_t) * (_num_elems + 1));
        uint32_t* dst_ptr = reinterpret_cast<uint32_t*>(_offsets_buf.data());
        for (int i = 0 ;i < _num_elems ;i++) {
            dst_ptr[i] = *((uint32_t*)p);
            p += 4;
        }
        // Add one extra entry pointing after the last item to make the indexing easier.
        dst_ptr[_num_elems] = offsets_pos;

        _parsed = true;

        return Status::OK();
    }

    Status seek_to_position_in_page(size_t pos) override {
        DCHECK_LE(pos, _num_elems);
        _cur_idx = pos;
        return Status::OK();
    }

    Status next_batch(size_t *n, ColumnBlockView *dst) override {
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
        const uint32_t str_offset = offset(idx);
        uint32_t len = offset(idx + 1) - str_offset;
        return Slice(&_data[str_offset], len);
    }

    // Minimum length of a header.
    static const size_t MIN_HEADER_SIZE = sizeof(uint32_t) * 2;

private:

    // Return the offset within '_data' where the string value with index 'idx' can be found.
    uint32_t offset(int idx) const {
        const uint8_t *p = &_offsets_buf[idx * sizeof(uint32_t)];
        uint32_t ret;
        memcpy(&ret, p, sizeof(uint32_t));
        return ret;
    }

    Slice _data;
    PageDecoderOptions _options;
    bool _parsed;

    // A buffer for an array of 32-bit integers for the offsets of the underlying strings in '_data'.
    //
    // This array also contains one extra offset at the end, pointing
    // _after_ the last entry. This makes the code much simpler.
    //
    // The array is stored inside a 'faststring' instead of a vector<uint32_t> to
    // avoid the overhead of calling vector::push_back -- one would think it would
    // be fully inlined away, but it's actually a perf win to do this.
    faststring _offsets_buf;

    uint32_t _num_elems;

    // Index of the currently seeked element in the page.
    uint32_t _cur_idx;
};

} // namespace segment_v2
} // namespace doris
