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

#include "olap/rowset/segment_v2/page_builder.h" // for PageBuilder
#include "olap/rowset/segment_v2/page_decoder.h" // for PageDecoder
#include "olap/rowset/segment_v2/options.h" // for PageBuilderOptions/PageDecoderOptions
#include "olap/rowset/segment_v2/common.h" // for rowid_t
#include "util/frame_of_reference_coding.h"

namespace doris {
namespace segment_v2 {

// Encode page use frame-of-reference coding
template<FieldType Type>
class FrameOfReferencePageBuilder : public PageBuilder {
public:
    explicit FrameOfReferencePageBuilder(const PageBuilderOptions& options) :
        _options(options),
        _count(0),
        _finished(false) {
        _encoder.reset(new ForEncoder<CppType>(&_buf));
    }

    bool is_page_full() override {
        return _encoder->len() >= _options.data_page_size;
    }

    Status add(const uint8_t* vals, size_t* count) override {
        DCHECK(!_finished);
        auto new_vals = reinterpret_cast<const CppType*>(vals);
        _encoder->put_batch(new_vals, *count);
        _count += *count;
        return Status::OK();
    }

    OwnedSlice finish() override {
        DCHECK(!_finished);
        _finished = true;
        _encoder->flush();
        return _buf.build();
    }

    void reset() override {
        _count = 0;
        _finished = false;
        _encoder->clear();
    }

    size_t count() const override {
        return _count;
    }

    uint64_t size() const override {
        return _buf.size();
    }

private:
    typedef typename TypeTraits<Type>::CppType CppType;
    PageBuilderOptions _options;
    size_t _count;
    bool _finished;
    std::unique_ptr<ForEncoder<CppType>> _encoder;
    faststring _buf;
};

template<FieldType Type>
class FrameOfReferencePageDecoder : public PageDecoder {
public:
    FrameOfReferencePageDecoder(Slice slice, const PageDecoderOptions& options) :
        _parsed(false),
        _data(slice),
        _num_elements(0),
        _cur_index(0){
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
        DCHECK_LE(pos, _num_elements) << "Tried to seek to " << pos << " which is > number of elements ("
                                      << _num_elements << ") in the block!";
        // If the block is empty (e.g. the column is filled with nulls), there is no data to seek.
        if (PREDICT_FALSE(_num_elements == 0)) {
            return Status::OK();
        }

        int32_t skip_num = pos - _cur_index;
        _decoder->skip(skip_num);
        _cur_index = pos;
        return Status::OK();
    }

    Status next_batch(size_t* n, ColumnBlockView* dst) override {
        DCHECK(_parsed) << "Must call init() firstly";
        if (PREDICT_FALSE(*n == 0 || _cur_index >= _num_elements)) {
            *n = 0;
            return Status::OK();
        }

        size_t to_fetch = std::min(*n, static_cast<size_t>(_num_elements - _cur_index));
        uint8_t* data_ptr = dst->data();
        _decoder->get_batch(reinterpret_cast<CppType*>(data_ptr), to_fetch);
        _cur_index += to_fetch;
        *n = to_fetch;
        return Status::OK();
    }

    size_t count() const override {
        return _num_elements;
    }

    size_t current_index() const override {
        return _cur_index;
    }

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