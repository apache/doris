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

#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/options.h"
#include "olap/rowset/segment_v2/page_builder.h"
#include "olap/rowset/segment_v2/page_decoder.h"
#include "olap/types.h"
#include "util/coding.h"
#include "util/faststring.h"

namespace doris {
namespace segment_v2 {

static const size_t PLAIN_PAGE_HEADER_SIZE = sizeof(uint32_t);

template <FieldType Type>
class PlainPageBuilder : public PageBuilder {
public:
    PlainPageBuilder(const PageBuilderOptions& options) : _options(options) {
        // Reserve enough space for the page, plus a bit of slop since
        // we often overrun the page by a few values.
        _buffer.reserve(_options.data_page_size + 1024);
        reset();
    }

    bool is_page_full() override { return _buffer.size() > _options.data_page_size; }

    Status add(const uint8_t* vals, size_t* count) override {
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

    OwnedSlice finish() override {
        encode_fixed32_le((uint8_t*)&_buffer[0], _count);
        if (_count > 0) {
            _first_value.assign_copy(&_buffer[PLAIN_PAGE_HEADER_SIZE], SIZE_OF_TYPE);
            _last_value.assign_copy(&_buffer[PLAIN_PAGE_HEADER_SIZE + (_count - 1) * SIZE_OF_TYPE],
                                    SIZE_OF_TYPE);
        }
        return _buffer.build();
    }

    void reset() override {
        _buffer.reserve(_options.data_page_size + 1024);
        _count = 0;
        _buffer.clear();
        _buffer.resize(PLAIN_PAGE_HEADER_SIZE);
    }

    size_t count() const override { return _count; }

    uint64_t size() const override { return _buffer.size(); }

    Status get_first_value(void* value) const override {
        if (_count == 0) {
            return Status::Error<ErrorCode::ENTRY_NOT_FOUND>("page is empty");
        }
        memcpy(value, _first_value.data(), SIZE_OF_TYPE);
        return Status::OK();
    }

    Status get_last_value(void* value) const override {
        if (_count == 0) {
            return Status::Error<ErrorCode::ENTRY_NOT_FOUND>("page is empty");
        }
        memcpy(value, _last_value.data(), SIZE_OF_TYPE);
        return Status::OK();
    }

private:
    faststring _buffer;
    PageBuilderOptions _options;
    size_t _count;
    typedef typename TypeTraits<Type>::CppType CppType;
    enum { SIZE_OF_TYPE = TypeTraits<Type>::size };
    faststring _first_value;
    faststring _last_value;
};

template <FieldType Type>
class PlainPageDecoder : public PageDecoder {
public:
    PlainPageDecoder(Slice data, const PageDecoderOptions& options)
            : _data(data), _options(options), _parsed(false), _num_elems(0), _cur_idx(0) {}

    Status init() override {
        CHECK(!_parsed);

        if (_data.size < PLAIN_PAGE_HEADER_SIZE) {
            return Status::InternalError(
                    "file corruption: not enough bytes for header in PlainPageDecoder ."
                    "invalid data size:{}, header size:{}",
                    _data.size, PLAIN_PAGE_HEADER_SIZE);
        }

        _num_elems = decode_fixed32_le((const uint8_t*)&_data[0]);

        if (_data.size != PLAIN_PAGE_HEADER_SIZE + _num_elems * SIZE_OF_TYPE) {
            return Status::InternalError("file corruption: unexpected data size.");
        }

        _parsed = true;

        static_cast<void>(seek_to_position_in_page(0));
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

    Status seek_at_or_after_value(const void* value, bool* exact_match) override {
        DCHECK(_parsed) << "Must call init() firstly";

        if (_num_elems == 0) {
            return Status::Error<ErrorCode::ENTRY_NOT_FOUND>("page is empty");
        }

        size_t left = 0;
        size_t right = _num_elems;

        const void* mid_value = nullptr;

        // find the first value >= target. after loop,
        // - left == index of first value >= target when found
        // - left == _num_elems when not found (all values < target)
        while (left < right) {
            size_t mid = left + (right - left) / 2;
            mid_value = &_data[PLAIN_PAGE_HEADER_SIZE + mid * SIZE_OF_TYPE];
            if (TypeTraits<Type>::cmp(mid_value, value) < 0) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        if (left >= _num_elems) {
            return Status::Error<ErrorCode::ENTRY_NOT_FOUND>("all value small than the value");
        }
        const void* find_value = &_data[PLAIN_PAGE_HEADER_SIZE + left * SIZE_OF_TYPE];
        if (TypeTraits<Type>::cmp(find_value, value) == 0) {
            *exact_match = true;
        } else {
            *exact_match = false;
        }

        _cur_idx = left;
        return Status::OK();
    }

    Status next_batch(size_t* n, vectorized::MutableColumnPtr& dst) override {
        return Status::NotSupported("plain page not implement vec op now");
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
    enum { SIZE_OF_TYPE = TypeTraits<Type>::size };
};

} // namespace segment_v2
} // namespace doris
