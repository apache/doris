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

#include "olap/rowset/segment_v2/options.h"      // for PageBuilderOptions/PageDecoderOptions
#include "olap/rowset/segment_v2/page_builder.h" // for PageBuilder
#include "olap/rowset/segment_v2/page_decoder.h" // for PageDecoder
#include "util/frame_of_reference_coding.h"

namespace doris {
namespace segment_v2 {

// Encode page use frame-of-reference coding
template <FieldType Type>
class FrameOfReferencePageBuilder : public PageBuilderHelper<FrameOfReferencePageBuilder<Type>> {
public:
    using Self = FrameOfReferencePageBuilder<Type>;
    friend class PageBuilderHelper<Self>;

    Status init() override {
        _encoder.reset(new ForEncoder<CppType>(&_buf));
        return Status::OK();
    }

    bool is_page_full() override { return _encoder->len() >= _options.data_page_size; }

    Status add(const uint8_t* vals, size_t* count) override {
        DCHECK(!_finished);
        if (*count == 0) {
            return Status::OK();
        }
        auto new_vals = reinterpret_cast<const CppType*>(vals);
        if (_count == 0) {
            _first_val = *new_vals;
        }
        _encoder->put_batch(new_vals, *count);
        _count += *count;
        _last_val = new_vals[*count - 1];
        return Status::OK();
    }

    Status finish(OwnedSlice* slice) override {
        DCHECK(!_finished);
        _finished = true;
        _encoder->flush();
        RETURN_IF_CATCH_EXCEPTION({ *slice = _buf.build(); });
        return Status::OK();
    }

    Status reset() override {
        _count = 0;
        _finished = false;
        _encoder->clear();
        return Status::OK();
    }

    size_t count() const override { return _count; }

    uint64_t size() const override { return _buf.size(); }

    Status get_first_value(void* value) const override {
        if (_count == 0) {
            return Status::Error<ErrorCode::ENTRY_NOT_FOUND>("page is empty");
        }
        memcpy(value, &_first_val, sizeof(CppType));
        return Status::OK();
    }

    Status get_last_value(void* value) const override {
        if (_count == 0) {
            return Status::Error<ErrorCode::ENTRY_NOT_FOUND>("page is empty");
        }
        memcpy(value, &_last_val, sizeof(CppType));
        return Status::OK();
    }

private:
    explicit FrameOfReferencePageBuilder(const PageBuilderOptions& options)
            : _options(options), _count(0), _finished(false) {}

    typedef typename TypeTraits<Type>::CppType CppType;
    PageBuilderOptions _options;
    size_t _count;
    bool _finished;
    std::unique_ptr<ForEncoder<CppType>> _encoder;
    faststring _buf;
    CppType _first_val;
    CppType _last_val;
};

template <FieldType Type>
class FrameOfReferencePageDecoder : public PageDecoder {
public:
    FrameOfReferencePageDecoder(Slice slice, const PageDecoderOptions& options)
            : _parsed(false), _data(slice), _num_elements(0), _cur_index(0) {
        _decoder.reset(new ForDecoder<CppType>((uint8_t*)_data.data, _data.size));
    }

    Status init() override {
        CHECK(!_parsed);
        bool result = _decoder->init();
        if (result) {
            _num_elements = _decoder->count();
            _parsed = true;
            return Status::OK();
        } else {
            return Status::Corruption("The frame of reference page metadata maybe broken");
        }
    }

    Status seek_to_position_in_page(size_t pos) override {
        DCHECK(_parsed) << "Must call init() firstly";
        DCHECK_LE(pos, _num_elements)
                << "Tried to seek to " << pos << " which is > number of elements (" << _num_elements
                << ") in the block!";
        // If the block is empty (e.g. the column is filled with nulls), there is no data to seek.
        if (PREDICT_FALSE(_num_elements == 0)) {
            return Status::OK();
        }

        int32_t skip_num = pos - _cur_index;
        _decoder->skip(skip_num);
        _cur_index = pos;
        return Status::OK();
    }

    Status seek_at_or_after_value(const void* value, bool* exact_match) override {
        DCHECK(_parsed) << "Must call init() firstly";
        bool found = _decoder->seek_at_or_after_value(value, exact_match);
        if (!found) {
            return Status::Error<ErrorCode::ENTRY_NOT_FOUND>("not found");
        }
        _cur_index = _decoder->current_index();
        return Status::OK();
    }

    Status next_batch(size_t* n, vectorized::MutableColumnPtr& dst) override {
        return Status::NotSupported("frame page not implement vec op now");
    }

    Status peek_next_batch(size_t* n, vectorized::MutableColumnPtr& dst) override {
        return Status::NotSupported("frame page not implement vec op now");
    }

    size_t count() const override { return _num_elements; }

    size_t current_index() const override { return _cur_index; }

private:
    typedef typename TypeTraits<Type>::CppType CppType;

    bool _parsed;
    Slice _data;
    uint32_t _num_elements;
    size_t _cur_index;
    std::unique_ptr<ForDecoder<CppType>> _decoder;
};

} // namespace segment_v2
} // namespace doris