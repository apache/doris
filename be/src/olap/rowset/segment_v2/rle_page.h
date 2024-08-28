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
#include "util/coding.h"                         // for encode_fixed32_le/decode_fixed32_le
#include "util/rle_encoding.h"                   // for RleEncoder/RleDecoder
#include "util/slice.h"                          // for OwnedSlice

namespace doris {
namespace segment_v2 {

enum { RLE_PAGE_HEADER_SIZE = 4 };

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
template <FieldType Type>
class RlePageBuilder : public PageBuilderHelper<RlePageBuilder<Type> > {
public:
    using Self = RlePageBuilder<Type>;
    friend class PageBuilderHelper<Self>;

    Status init() override {
        switch (Type) {
        case FieldType::OLAP_FIELD_TYPE_BOOL: {
            _bit_width = 1;
            break;
        }
        default: {
            _bit_width = SIZE_OF_TYPE * 8;
            break;
        }
        }
        _rle_encoder = new RleEncoder<CppType>(&_buf, _bit_width);
        return reset();
    }

    ~RlePageBuilder() { delete _rle_encoder; }

    bool is_page_full() override { return _rle_encoder->len() >= _options.data_page_size; }

    Status add(const uint8_t* vals, size_t* count) override {
        DCHECK(!_finished);
        auto new_vals = reinterpret_cast<const CppType*>(vals);
        for (int i = 0; i < *count; ++i) {
            // note: vals is not guaranteed to be aligned for now, thus memcpy here
            CppType value;
            memcpy(&value, &new_vals[i], SIZE_OF_TYPE);
            _rle_encoder->Put(value);
        }

        if (_count == 0) {
            memcpy(&_first_value, new_vals, SIZE_OF_TYPE);
        }
        memcpy(&_last_value, &new_vals[*count - 1], SIZE_OF_TYPE);

        _count += *count;
        return Status::OK();
    }

    Status finish(OwnedSlice* slice) override {
        DCHECK(!_finished);
        _finished = true;
        // here should Flush first and then encode the count header
        // or it will lead to a bug if the header is less than 8 byte and the data is small
        _rle_encoder->Flush();
        encode_fixed32_le(&_buf[0], _count);
        *slice = _buf.build();
        return Status::OK();
    }

    Status reset() override {
        RETURN_IF_CATCH_EXCEPTION({
            _count = 0;
            _finished = false;
            _rle_encoder->Clear();
            _rle_encoder->Reserve(RLE_PAGE_HEADER_SIZE, 0);
        });
        return Status::OK();
    }

    size_t count() const override { return _count; }

    uint64_t size() const override { return _rle_encoder->len(); }

    Status get_first_value(void* value) const override {
        DCHECK(_finished);
        if (_count == 0) {
            return Status::Error<ErrorCode::ENTRY_NOT_FOUND>("page is empty");
        }
        memcpy(value, &_first_value, SIZE_OF_TYPE);
        return Status::OK();
    }

    Status get_last_value(void* value) const override {
        DCHECK(_finished);
        if (_count == 0) {
            return Status::Error<ErrorCode::ENTRY_NOT_FOUND>("page is empty");
        }
        memcpy(value, &_last_value, SIZE_OF_TYPE);
        return Status::OK();
    }

private:
    RlePageBuilder(const PageBuilderOptions& options)
            : _options(options),
              _count(0),
              _finished(false),
              _bit_width(0),
              _rle_encoder(nullptr) {}

    typedef typename TypeTraits<Type>::CppType CppType;
    enum { SIZE_OF_TYPE = TypeTraits<Type>::size };

    PageBuilderOptions _options;
    size_t _count;
    bool _finished;
    int _bit_width;
    RleEncoder<CppType>* _rle_encoder = nullptr;
    faststring _buf;
    CppType _first_value;
    CppType _last_value;
};

template <FieldType Type>
class RlePageDecoder : public PageDecoder {
public:
    RlePageDecoder(Slice slice, const PageDecoderOptions& options)
            : _data(slice),
              _options(options),
              _parsed(false),
              _num_elements(0),
              _cur_index(0),
              _bit_width(0) {}

    Status init() override {
        CHECK(!_parsed);

        if (_data.size < RLE_PAGE_HEADER_SIZE) {
            return Status::Corruption("not enough bytes for header in RleBitMapBlockDecoder");
        }
        _num_elements = decode_fixed32_le((const uint8_t*)&_data[0]);

        _parsed = true;

        switch (Type) {
        case FieldType::OLAP_FIELD_TYPE_BOOL: {
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

        RETURN_IF_ERROR(seek_to_position_in_page(0));
        return Status::OK();
    }

    Status seek_to_position_in_page(size_t pos) override {
        DCHECK(_parsed) << "Must call init()";
        DCHECK_LE(pos, _num_elements)
                << "Tried to seek to " << pos << " which is > number of elements (" << _num_elements
                << ") in the block!";
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

    Status next_batch(size_t* n, vectorized::MutableColumnPtr& dst) override {
        DCHECK(_parsed);
        if (PREDICT_FALSE(*n == 0 || _cur_index >= _num_elements)) {
            *n = 0;
            return Status::OK();
        }

        size_t to_fetch = std::min(*n, static_cast<size_t>(_num_elements - _cur_index));
        size_t remaining = to_fetch;
        bool result = false;
        CppType value;
        while (remaining > 0) {
            result = _rle_decoder.Get(&value);
            DCHECK(result);
            dst->insert_data((char*)(&value), SIZE_OF_TYPE);
            remaining--;
        }

        _cur_index += to_fetch;
        *n = to_fetch;
        return Status::OK();
    }

    Status read_by_rowids(const rowid_t* rowids, ordinal_t page_first_ordinal, size_t* n,
                          vectorized::MutableColumnPtr& dst) override {
        DCHECK(_parsed);
        if (PREDICT_FALSE(*n == 0 || _cur_index >= _num_elements)) {
            *n = 0;
            return Status::OK();
        }

        auto total = *n;
        bool result = false;
        size_t read_count = 0;
        CppType value;
        for (size_t i = 0; i < total; ++i) {
            ordinal_t ord = rowids[i] - page_first_ordinal;
            if (UNLIKELY(ord >= _num_elements)) {
                *n = read_count;
                return Status::OK();
            }

            _rle_decoder.Skip(ord - _cur_index);
            _cur_index = ord;

            result = _rle_decoder.Get(&value);
            _cur_index++;
            DCHECK(result);
            dst->insert_data((char*)(&value), SIZE_OF_TYPE);
            read_count++;
        }
        *n = read_count;
        return Status::OK();
    }

    size_t count() const override { return _num_elements; }

    size_t current_index() const override { return _cur_index; }

private:
    typedef typename TypeTraits<Type>::CppType CppType;
    enum { SIZE_OF_TYPE = TypeTraits<Type>::size };

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
