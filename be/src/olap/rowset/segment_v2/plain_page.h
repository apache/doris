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

#include "util/coding.h"
#include "util/faststring.h"
#include "olap/olap_common.h"
#include "olap/types.h"
#include "olap/rowset/segment_v2/page_builder.h"
#include "olap/rowset/segment_v2/page_decoder.h"
#include "olap/rowset/segment_v2/options.h"

namespace doris {
namespace segment_v2 {

static const size_t PLAIN_PAGE_HEADER_SIZE = sizeof(uint32_t);

template<FieldType Type>
class PlainPageBuilder : public PageBuilder {
public:
    PlainPageBuilder(const PageBuilderOptions& options) :
            _options(options) {
        // Reserve enough space for the page, plus a bit of slop since
        // we often overrun the page by a few values.
        _buffer.reserve(_options.data_page_size + 1024);
        reset();
    }

    bool is_page_full() override {
        return _buffer.size() > _options.data_page_size;
    }

    Status add(const uint8_t *vals, size_t *count) override {
        if (is_page_full()) {
            *count = 0;
            return Status::OK();
        }
        size_t old_size = _buffer.size();
        _buffer.resize(old_size + *count * SIZE_OF_TYPE);
        memcpy(&_buffer[old_size], vals, *count * SIZE_OF_TYPE);
        _count += *count;
        return Status::OK();
    }

    Slice finish() override {
        encode_fixed32_le((uint8_t *) &_buffer[0], _count);
        return Slice(_buffer.data(),  PLAIN_PAGE_HEADER_SIZE + _count * SIZE_OF_TYPE);
    }

    void reset() override {
        _count = 0;
        _buffer.clear();
        _buffer.resize(PLAIN_PAGE_HEADER_SIZE);
    }

    size_t count() const {
        return _count;
    }

    // this api will release the memory ownership of encoded data
    // Note:
    //     release() should be called after finish
    //     reset() should be called after this function before reuse the builder
    void release() override {
        uint8_t *ret = _buffer.release();
        _buffer.reserve(_options.data_page_size + 1024);
        (void) ret;
    }

private:
    faststring _buffer;
    PageBuilderOptions _options;
    size_t _count;
    typedef typename TypeTraits<Type>::CppType CppType;
    enum {
        SIZE_OF_TYPE = TypeTraits<Type>::size
    };
};


template<FieldType Type>
class PlainPageDecoder : public PageDecoder {
public:
    PlainPageDecoder(Slice data, const PageDecoderOptions& options) : _data(data),
            _options(options),
            _parsed(false),
            _num_elems(0),
            _cur_idx(0) { }

    Status init() override {
        CHECK(!_parsed);

        if (_data.size < PLAIN_PAGE_HEADER_SIZE) {
            std::stringstream ss;
            ss << "file corrupton: not enough bytes for header in PlainPageDecoder ."
                  "invalid data size:" << _data.size << ", header size:" << PLAIN_PAGE_HEADER_SIZE;
            return Status::InternalError(ss.str());
        }

        _num_elems = decode_fixed32_le((const uint8_t *) &_data[0]);

        if (_data.size != PLAIN_PAGE_HEADER_SIZE + _num_elems * SIZE_OF_TYPE) {
            std::stringstream ss;
            ss << "file corrupton: unexpected data size.";
            return Status::InternalError(ss.str());
        }

        _parsed = true;

        seek_to_position_in_page(0);
        return Status::OK();
    }

    Status seek_to_position_in_page(size_t pos) override {
        CHECK(_parsed) << "Must call init()";

        if (PREDICT_FALSE(_num_elems == 0)) {
            DCHECK_EQ(0, pos);
            return Status::InternalError("invalid pos");
        }

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
        memcpy(dst->data(),
               &_data[PLAIN_PAGE_HEADER_SIZE + _cur_idx * SIZE_OF_TYPE],
               max_fetch * SIZE_OF_TYPE);
        _cur_idx += max_fetch;
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

private:
    Slice _data;
    PageDecoderOptions _options;
    bool _parsed;
    uint32_t _num_elems;
    uint32_t _cur_idx;
    typedef typename TypeTraits<Type>::CppType CppType;
    enum {
        SIZE_OF_TYPE = TypeTraits<Type>::size
    };
};

} // namespace segment_v2
} // namespace doris
