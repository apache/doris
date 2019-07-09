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

#include <sys/types.h>
#include <algorithm>
#include <cstring>
#include <cstdint>
#include <ostream>
#include <glog/logging.h>

#include "util/coding.h"
#include "util/faststring.h"
#include "gutil/port.h"
#include "olap/olap_common.h"
#include "olap/types.h"
#include "olap/rowset/segment_v2/page_builder.h"
#include "olap/rowset/segment_v2/page_decoder.h"
#include "olap/rowset/segment_v2/options.h"
#include "olap/rowset/segment_v2/common.h"
#include "olap/rowset/segment_v2/bitshuffle_wrapper.h"

namespace doris {
namespace segment_v2 {

enum {
    BITSHUFFLE_BLOCK_HEADER_SIZE = 16
};

void warn_with_bitshuffle_error(int64_t val) {
    switch (val) {
    case -1:
      LOG(WARNING) << "Failed to allocate memory";
      break;
    case -11:
      LOG(WARNING) << "Missing SSE";
      break;
    case -12:
      LOG(WARNING) << "Missing AVX";
      break;
    case -80:
      LOG(WARNING) << "Input size not a multiple of 8";
      break;
    case -81:
      LOG(WARNING) << "block_size not multiple of 8";
      break;
    case -91:
      LOG(WARNING) << "Decompression error, wrong number of bytes processed";
      break;
    default:
      LOG(WARNING) << "Error internal to compression routine";
    }
}

// BitshufflePageBuilder bitshuffles and compresses the bits of fixed
// size type blocks with lz4.
//
// The page format is as follows:
//
// 1. Header: (16 bytes total)
//
//    <num_elements> [32-bit]
//      The number of elements encoded in the page.
//
//    <compressed_size> [32-bit]
//      The post-compression size of the page, including this header.
//
//    <padded_num_elements> [32-bit]
//      Padding is needed to meet the requirements of the bitshuffle
//      library such that the input/output is a multiple of 8. Some
//      ignored elements are appended to the end of the page if necessary
//      to meet this requirement.
//
//      This header field is the post-padding element count.
//
//    <elem_size_bytes> [32-bit]
//      The size of the elements, in bytes, as actually encoded. In the
//      case that all of the data in a page can fit into a smaller
//      integer type, then we may choose to encode that smaller type
//      to save CPU costs.
//
//      This is currently only implemented in the UINT32 page type.
//
//   NOTE: all on-disk ints are encoded little-endian
//
// 2. Element data
//
//    The header is followed by the bitshuffle-compressed element data.
//
template<FieldType Type>
class BitshufflePageBuilder : public PageBuilder {
public:
    BitshufflePageBuilder(PageBuilderOptions options) :
            _options(std::move(options)),
            _count(0),
            _remain_element_capacity(0),
            _finished(false) {
        reset();
    }

    bool is_page_full() override {
        return _remain_element_capacity == 0;
    }

    Status add(const uint8_t* vals, size_t* count) override {
        DCHECK(!_finished);
        int to_add = std::min<int>(_remain_element_capacity, *count);
        _data.append(vals, to_add * SIZE_OF_TYPE);
        _count += to_add;
        _remain_element_capacity -= to_add;
        // return added number through count
        *count = to_add;
        return Status::OK();
    }

    Slice finish() override {
        return _finish(SIZE_OF_TYPE);
    }

    void reset() override {
        auto block_size = _options.data_page_size;
        _count = 0;
        _data.clear();
        _data.reserve(block_size);
        DCHECK_EQ(reinterpret_cast<uintptr_t>(_data.data()) & (alignof(CppType) - 1), 0)
            << "buffer must be naturally-aligned";
        _buffer.clear();
        _buffer.resize(BITSHUFFLE_BLOCK_HEADER_SIZE);
        _finished = false;
        _remain_element_capacity = block_size / SIZE_OF_TYPE;
    }

    size_t count() const {
        return _count;
    }

    // this api will release the memory ownership of encoded data
    // Note:
    //     release() should be called after finish
    //     reset() should be called after this function before reuse the builder
    void release() override {
        uint8_t* ret = _buffer.release();
        (void)ret;
    }

private:
    Slice _finish(int final_size_of_type) {
        _data.resize(BITSHUFFLE_BLOCK_HEADER_SIZE + final_size_of_type * _count);

        // Do padding so that the input num of element is multiple of 8.
        int num_elems_after_padding = ALIGN_UP(_count, 8);
        int padding_elems = num_elems_after_padding - _count;
        int padding_bytes = padding_elems * final_size_of_type;
        for (int i = 0; i < padding_bytes; i++) {
            _data.push_back(0);
        }

        _buffer.resize(BITSHUFFLE_BLOCK_HEADER_SIZE +
                bitshuffle::compress_lz4_bound(num_elems_after_padding, final_size_of_type, 0));

        encode_fixed32_le(&_buffer[0], _count);
        int64_t bytes = bitshuffle::compress_lz4(_data.data(), &_buffer[BITSHUFFLE_BLOCK_HEADER_SIZE],
                num_elems_after_padding, final_size_of_type, 0);
        if (PREDICT_FALSE(bytes < 0)) {
            // This means the bitshuffle function fails.
            // Ideally, this should not happen.
            warn_with_bitshuffle_error(bytes);
            // It does not matter what will be returned here,
            // since we have logged fatal in warn_with_bitshuffle_error().
            return Slice();
        }
        encode_fixed32_le(&_buffer[4], BITSHUFFLE_BLOCK_HEADER_SIZE + bytes);
        encode_fixed32_le(&_buffer[8], num_elems_after_padding);
        encode_fixed32_le(&_buffer[12], final_size_of_type);
        _finished = true;
        return Slice(_buffer.data(), BITSHUFFLE_BLOCK_HEADER_SIZE + bytes);
    }

    typedef typename TypeTraits<Type>::CppType CppType;

    CppType cell(int idx) const {
        DCHECK_GE(idx, 0);
        CppType ret;
        memcpy(&ret, &_data[idx * SIZE_OF_TYPE], sizeof(CppType));
        return ret;
    }

    enum {
        SIZE_OF_TYPE = TypeTraits<Type>::size
    };
    PageBuilderOptions _options;
    uint32_t _count;
    int _remain_element_capacity;
    bool _finished;
    faststring _data;
    faststring _buffer;
};

template<FieldType Type>
class BitShufflePageDecoder : public PageDecoder {
public:
    BitShufflePageDecoder(Slice data, const PageDecoderOptions& options) : _data(data),
    _options(options),
    _parsed(false),
    _num_elements(0),
    _compressed_size(0),
    _num_element_after_padding(0),
    _size_of_element(0),
    _cur_index(0) { }

    Status init() override {
        CHECK(!_parsed);
        if (_data.size < BITSHUFFLE_BLOCK_HEADER_SIZE) {
            std::stringstream ss;
            ss << "file corrupton: invalid data size:" << _data.size << ", header size:" << BITSHUFFLE_BLOCK_HEADER_SIZE;
            return Status::InternalError(ss.str());
        }
        _num_elements = decode_fixed32_le((const uint8_t*)&_data[0]);
        _compressed_size   = decode_fixed32_le((const uint8_t*)&_data[4]);
        if (_compressed_size != _data.size) {
            std::stringstream ss;
            ss << "Size information unmatched, _compressed_size:" << _compressed_size
                << ", data size:" << _data.size;
            return Status::InternalError(ss.str());
        }
        _num_element_after_padding = decode_fixed32_le((const uint8_t*)&_data[8]);
        if (_num_element_after_padding != ALIGN_UP(_num_elements, 8)) {
            std::stringstream ss;
            ss << "num of element information corrupted,"
                << " _num_element_after_padding:" << _num_element_after_padding
                << ", _num_elements:" << _num_elements;
            return Status::InternalError(ss.str());
        }
        _size_of_element = decode_fixed32_le((const uint8_t*)&_data[12]);
        switch (_size_of_element) {
            case 1:
            case 2:
            case 4:
            case 8:
            case 16:
                break;
            default:
                std::stringstream ss;
                ss << "invalid size_of_elem:" << _size_of_element;
                return Status::InternalError(ss.str());
        }

        // Currently, only the UINT32 block encoder supports expanding size:
        if (UNLIKELY(Type != OLAP_FIELD_TYPE_UNSIGNED_INT && _size_of_element != SIZE_OF_TYPE)) {
            std::stringstream ss;
            ss << "invalid size info. size of element:" << _size_of_element
                << ", SIZE_OF_TYPE:" << SIZE_OF_TYPE
                << ", type:" << Type;
            return Status::InternalError(ss.str());
        }
        if (UNLIKELY(_size_of_element > SIZE_OF_TYPE)) {
            std::stringstream ss;
            ss << "invalid size info. size of element:" << _size_of_element
                << ", SIZE_OF_TYPE:" << SIZE_OF_TYPE;
            return Status::InternalError(ss.str());
        }

        RETURN_IF_ERROR(_decode());
        _parsed = true;
        return Status::OK();
    }

    Status seek_to_position_in_page(size_t pos) override {
        DCHECK(_parsed) << "Must call init()";
        if (PREDICT_FALSE(_num_elements == 0)) {
            DCHECK_EQ(0, pos);
            return Status::InvalidArgument("invalid pos");
        }

        DCHECK_LE(pos, _num_elements);
        _cur_index = pos;
        return Status::OK();
    }

    Status next_batch(size_t* n, ColumnBlockView* dst) override {
        DCHECK(_parsed);
        if (PREDICT_FALSE(*n == 0 || _cur_index >= _num_elements)) {
            *n = 0;
            return Status::OK();
        }

        size_t max_fetch = std::min(*n, static_cast<size_t>(_num_elements - _cur_index));
        _copy_next_values(max_fetch, dst->data());
        *n = max_fetch;
        _cur_index += max_fetch;

        return Status::OK();
    }

    size_t count() const override {
        return _num_elements;
    }

    size_t current_index() const override {
        return _cur_index;
    }

private:
    void _copy_next_values(size_t n, void* data) {
        memcpy(data, &_decoded[_cur_index * SIZE_OF_TYPE], n * SIZE_OF_TYPE);
    }

    Status _decode() {
        if (_num_elements > 0) {
            int64_t bytes;
            _decoded.resize(_num_element_after_padding * _size_of_element);
            char* in = const_cast<char*>(&_data[BITSHUFFLE_BLOCK_HEADER_SIZE]);
            bytes = bitshuffle::decompress_lz4(in, _decoded.data(), _num_element_after_padding,
                    _size_of_element, 0);
            if (PREDICT_FALSE(bytes < 0)) {
                // Ideally, this should not happen.
                warn_with_bitshuffle_error(bytes);
                return Status::RuntimeError("Unshuffle Process failed");
            }
        }
        return Status::OK();
    }

    typedef typename TypeTraits<Type>::CppType CppType;

    enum {
        SIZE_OF_TYPE = TypeTraits<Type>::size
    };

    Slice _data;
    PageDecoderOptions _options;
    bool _parsed;
    size_t _num_elements;
    size_t _compressed_size;
    size_t _num_element_after_padding;

    int _size_of_element;
    size_t _cur_index;
    faststring _decoded;
};

} // namespace segment_v2
} // namespace doris
