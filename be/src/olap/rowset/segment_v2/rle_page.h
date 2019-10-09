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
#include "util/rle_encoding.h" // for RleEncoder/RleDecoder
#include "util/coding.h" // for encode_fixed32_le/decode_fixed32_le

namespace doris {
namespace segment_v2 {

enum {
    RLE_PAGE_HEADER_SIZE = 4
};

// RLE builder for generic integer and bool types. What is missing is some way
// to enforce that this can only be instantiated for INT and BOOL types.
//
// The page format is as follows:
//
// 1. Header: (4 bytes total)
//
//    <num_elements> [32-bit]
//      The number of elements encoded in the page.
//
//    NOTE: all on-disk ints are encoded little-endian
//
// 2. Element data
//
//    The header is followed by the rle-encoded element data.
//
// This Rle encoding algorithm is only effective for repeated INT type and bool type,
// It is not good for sequence number or random number. BitshufflePage is recommended
// for these case.
//
// TODO(hkp): optimize rle algorithm 
template<FieldType Type>
class RlePageBuilder : public PageBuilder {
public:
    RlePageBuilder(const PageBuilderOptions& options) :
        _options(options),
        _count(0),
        _finished(false),
        _bit_width(0),
        _rle_encoder(nullptr) {
        switch(Type) {
            case OLAP_FIELD_TYPE_BOOL: {
                _bit_width = 1;
                break;
            }
            default: {
                _bit_width = SIZE_OF_TYPE * 8;
                break;
            }
        }
        _rle_encoder = new RleEncoder<CppType>(&_buf, _bit_width);
        reset();
    }

    ~RlePageBuilder() {
        delete _rle_encoder;
    }

    bool is_page_full() override {
        return _rle_encoder->len() >= _options.data_page_size;
    }

    Status add(const uint8_t* vals, size_t* count) override {
        DCHECK(!_finished);
        DCHECK_EQ(reinterpret_cast<uintptr_t>(vals) & (alignof(CppType) - 1), 0)
                << "Pointer passed to Add() must be naturally-aligned";

        const CppType* new_vals = reinterpret_cast<const CppType*>(vals);
        for (int i = 0; i < *count; ++i) {
            _rle_encoder->Put(new_vals[i]);
        }
        
        _count += *count;
        return Status::OK();
    }

    Slice finish() override {
        _finished = true;
        // here should Flush first and then encode the count header
        // or it will lead to a bug if the header is less than 8 byte and the data is small
        _rle_encoder->Flush();
        encode_fixed32_le(&_buf[0], _count);
        return Slice(_buf.data(), _buf.size());
    }

    void reset() override {
        _count = 0;
        _rle_encoder->Clear();
        _rle_encoder->Reserve(RLE_PAGE_HEADER_SIZE, 0);
    }

    size_t count() const override {
        return _count;
    }

    uint64_t size() const override {
        return _rle_encoder->len();
    }

    // this api will release the memory ownership of encoded data
    // Note:
    //     release() should be called after finish
    //     reset() should be called after this function before reuse the builder
    void release() override {
        uint8_t* ret = _buf.release();
        (void)ret;
    }

private:
    typedef typename TypeTraits<Type>::CppType CppType;
    enum {
        SIZE_OF_TYPE = TypeTraits<Type>::size
    };

    PageBuilderOptions _options;
    size_t _count;
    bool _finished;
    int _bit_width;
    RleEncoder<CppType>* _rle_encoder;
    faststring _buf;
};

template<FieldType Type>
class RlePageDecoder : public PageDecoder {
public:
    RlePageDecoder(Slice slice, const PageDecoderOptions& options) :
        _data(slice),
        _options(options),
        _parsed(false),
        _num_elements(0),
        _cur_index(0),
        _bit_width(0) { }

    Status init() override {
        CHECK(!_parsed);

        if (_data.size < RLE_PAGE_HEADER_SIZE) {
            return Status::Corruption(
                "not enough bytes for header in RleBitMapBlockDecoder");
        }
        _num_elements = decode_fixed32_le((const uint8_t*)&_data[0]);

        _parsed = true;

        switch(Type) {
            case OLAP_FIELD_TYPE_BOOL: {
                _bit_width = 1;
                break;
            }
            default: {
                _bit_width = SIZE_OF_TYPE * 8;
                break;
            }
        }

        _rle_decoder = RleDecoder<CppType>((uint8_t*)_data.data + RLE_PAGE_HEADER_SIZE,
                 _data.size - RLE_PAGE_HEADER_SIZE, _bit_width);

        seek_to_position_in_page(0);
        return Status::OK();
    }

    Status seek_to_position_in_page(size_t pos) override {
        DCHECK(_parsed) << "Must call init()";
        DCHECK_LE(pos, _num_elements) << "Tried to seek to " << pos << " which is > number of elements ("
                << _num_elements << ") in the block!";
        // If the block is empty (e.g. the column is filled with nulls), there is no data to seek.
        if (PREDICT_FALSE(_num_elements == 0)) {
            return Status::OK();
        }
        if (_cur_index == pos) {
            // No need to seek.
            return Status::OK();
        } else if (_cur_index < pos) {
            uint nskip = pos - _cur_index;
            _rle_decoder.Skip(nskip);
        } else {
            _rle_decoder = RleDecoder<CppType>((uint8_t*)_data.data + RLE_PAGE_HEADER_SIZE,
                    _data.size - RLE_PAGE_HEADER_SIZE, _bit_width);
            _rle_decoder.Skip(pos);
        }
        _cur_index = pos;
        return Status::OK();
    }

    Status next_batch(size_t* n, ColumnBlockView* dst) override {
        DCHECK(_parsed);
        if (PREDICT_FALSE(*n == 0 || _cur_index >= _num_elements)) {
            *n = 0;
            return Status::OK();
        }

        size_t to_fetch = std::min(*n, static_cast<size_t>(_num_elements - _cur_index));
        size_t remaining = to_fetch;
        uint8_t* data_ptr = dst->data();
        bool result = false;
        while (remaining > 0) {
            result = _rle_decoder.Get(reinterpret_cast<CppType*>(data_ptr));
            DCHECK(result);
            remaining--;
            data_ptr += SIZE_OF_TYPE;
        }

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
    enum {
        SIZE_OF_TYPE = TypeTraits<Type>::size
    };

    Slice _data;
    PageDecoderOptions _options;
    bool _parsed;
    uint32_t _num_elements;
    size_t _cur_index;
    int _bit_width;
    RleDecoder<CppType> _rle_decoder;
};

} // namespace segment_v2
} // namespace doris
