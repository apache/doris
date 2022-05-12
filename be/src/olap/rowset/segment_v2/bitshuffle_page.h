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

#include <glog/logging.h>
#include <sys/types.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <ostream>

#include "gutil/port.h"
#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/bitshuffle_wrapper.h"
#include "olap/rowset/segment_v2/common.h"
#include "olap/rowset/segment_v2/options.h"
#include "olap/rowset/segment_v2/page_builder.h"
#include "olap/rowset/segment_v2/page_decoder.h"
#include "olap/types.h"
#include "runtime/memory/chunk_allocator.h"
#include "util/coding.h"
#include "util/faststring.h"
#include "util/slice.h"
#include "vec/columns/column_nullable.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris {
namespace segment_v2 {

enum { BITSHUFFLE_PAGE_HEADER_SIZE = 16 };

void warn_with_bitshuffle_error(int64_t val);

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
template <FieldType Type>
class BitshufflePageBuilder : public PageBuilder {
public:
    BitshufflePageBuilder(const PageBuilderOptions& options)
            : _options(options), _count(0), _remain_element_capacity(0), _finished(false) {
        reset();
    }

    bool is_page_full() override { return _remain_element_capacity == 0; }

    Status add(const uint8_t* vals, size_t* count) override {
        return add_internal<false>(vals, count);
    }

    Status single_add(const uint8_t* vals, size_t* count) {
        return add_internal<true>(vals, count);
    }

    template <bool single>
    inline Status add_internal(const uint8_t* vals, size_t* count) {
        DCHECK(!_finished);
        if (_remain_element_capacity <= 0) {
            *count = 0;
            return Status::OK();
        }
        int to_add = std::min<int>(_remain_element_capacity, *count);
        int to_add_size = to_add * SIZE_OF_TYPE;
        size_t orig_size = _data.size();
        _data.resize(orig_size + to_add_size);
        _count += to_add;
        _remain_element_capacity -= to_add;
        // return added number through count
        *count = to_add;
        if constexpr (single) {
            if constexpr (SIZE_OF_TYPE == 1) {
                *reinterpret_cast<uint8_t*>(&_data[orig_size]) = *vals;
                return Status::OK();
            } else if constexpr (SIZE_OF_TYPE == 2) {
                *reinterpret_cast<uint16_t*>(&_data[orig_size]) =
                        *reinterpret_cast<const uint16_t*>(vals);
                return Status::OK();
            } else if constexpr (SIZE_OF_TYPE == 4) {
                *reinterpret_cast<uint32_t*>(&_data[orig_size]) =
                        *reinterpret_cast<const uint32_t*>(vals);
                return Status::OK();
            } else if constexpr (SIZE_OF_TYPE == 8) {
                *reinterpret_cast<uint64_t*>(&_data[orig_size]) =
                        *reinterpret_cast<const uint64_t*>(vals);
                return Status::OK();
            }
        }
        // when single is true and SIZE_OF_TYPE > 8 or single is false
        memcpy(&_data[orig_size], vals, to_add_size);
        return Status::OK();
    }

    OwnedSlice finish() override {
        if (_count > 0) {
            _first_value = cell(0);
            _last_value = cell(_count - 1);
        }
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
        _buffer.resize(BITSHUFFLE_PAGE_HEADER_SIZE);
        _finished = false;
        _remain_element_capacity = block_size / SIZE_OF_TYPE;
    }

    size_t count() const override { return _count; }

    uint64_t size() const override { return _buffer.size(); }

    Status get_first_value(void* value) const override {
        DCHECK(_finished);
        if (_count == 0) {
            return Status::NotFound("page is empty");
        }
        memcpy(value, &_first_value, SIZE_OF_TYPE);
        return Status::OK();
    }
    Status get_last_value(void* value) const override {
        DCHECK(_finished);
        if (_count == 0) {
            return Status::NotFound("page is empty");
        }
        memcpy(value, &_last_value, SIZE_OF_TYPE);
        return Status::OK();
    }

private:
    OwnedSlice _finish(int final_size_of_type) {
        _data.resize(final_size_of_type * _count);

        // Do padding so that the input num of element is multiple of 8.
        int num_elems_after_padding = ALIGN_UP(_count, 8);
        int padding_elems = num_elems_after_padding - _count;
        int padding_bytes = padding_elems * final_size_of_type;
        for (int i = 0; i < padding_bytes; i++) {
            _data.push_back(0);
        }

        // reserve enough place for compression
        _buffer.resize(
                BITSHUFFLE_PAGE_HEADER_SIZE +
                bitshuffle::compress_lz4_bound(num_elems_after_padding, final_size_of_type, 0));

        int64_t bytes =
                bitshuffle::compress_lz4(_data.data(), &_buffer[BITSHUFFLE_PAGE_HEADER_SIZE],
                                         num_elems_after_padding, final_size_of_type, 0);
        if (PREDICT_FALSE(bytes < 0)) {
            // This means the bitshuffle function fails.
            // Ideally, this should not happen.
            warn_with_bitshuffle_error(bytes);
            // It does not matter what will be returned here,
            // since we have logged fatal in warn_with_bitshuffle_error().
            return OwnedSlice();
        }
        // update header
        encode_fixed32_le(&_buffer[0], _count);
        encode_fixed32_le(&_buffer[4], BITSHUFFLE_PAGE_HEADER_SIZE + bytes);
        encode_fixed32_le(&_buffer[8], num_elems_after_padding);
        encode_fixed32_le(&_buffer[12], final_size_of_type);
        _finished = true;
        // before build(), update buffer length to the actual compressed size
        _buffer.resize(BITSHUFFLE_PAGE_HEADER_SIZE + bytes);
        return _buffer.build();
    }

    typedef typename TypeTraits<Type>::CppType CppType;

    CppType cell(int idx) const {
        DCHECK_GE(idx, 0);
        CppType ret;
        memcpy(&ret, &_data[idx * SIZE_OF_TYPE], SIZE_OF_TYPE);
        return ret;
    }

    enum { SIZE_OF_TYPE = TypeTraits<Type>::size };
    PageBuilderOptions _options;
    uint32_t _count;
    int _remain_element_capacity;
    bool _finished;
    faststring _data;
    faststring _buffer;
    CppType _first_value;
    CppType _last_value;
};

template <FieldType Type>
class BitShufflePageDecoder : public PageDecoder {
public:
    BitShufflePageDecoder(Slice data, const PageDecoderOptions& options)
            : _data(data),
              _options(options),
              _parsed(false),
              _num_elements(0),
              _compressed_size(0),
              _num_element_after_padding(0),
              _size_of_element(0),
              _cur_index(0) {}

    ~BitShufflePageDecoder() { ChunkAllocator::instance()->free(_chunk); }

    Status init() override {
        CHECK(!_parsed);
        if (_data.size < BITSHUFFLE_PAGE_HEADER_SIZE) {
            std::stringstream ss;
            ss << "file corruption: invalid data size:" << _data.size
               << ", header size:" << BITSHUFFLE_PAGE_HEADER_SIZE;
            return Status::InternalError(ss.str());
        }
        _num_elements = decode_fixed32_le((const uint8_t*)&_data[0]);
        _compressed_size = decode_fixed32_le((const uint8_t*)&_data[4]);
        if (_compressed_size != _data.size) {
            std::stringstream ss;
            ss << "Size information unmatched, _compressed_size:" << _compressed_size
               << ", _num_elements:" << _num_elements << ", data size:" << _data.size;
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
        case 3:
        case 4:
        case 8:
        case 12:
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
               << ", SIZE_OF_TYPE:" << SIZE_OF_TYPE << ", type:" << Type;
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

    Status seek_at_or_after_value(const void* value, bool* exact_match) override {
        DCHECK(_parsed) << "Must call init() firstly";

        if (_num_elements == 0) {
            return Status::NotFound("page is empty");
        }

        size_t left = 0;
        size_t right = _num_elements;

        void* mid_value = nullptr;

        // find the first value >= target. after loop,
        // - left == index of first value >= target when found
        // - left == _num_elements when not found (all values < target)
        while (left < right) {
            size_t mid = left + (right - left) / 2;
            mid_value = &_chunk.data[mid * SIZE_OF_TYPE];
            if (TypeTraits<Type>::cmp(mid_value, value) < 0) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        if (left >= _num_elements) {
            return Status::NotFound("all value small than the value");
        }
        void* find_value = &_chunk.data[left * SIZE_OF_TYPE];
        if (TypeTraits<Type>::cmp(find_value, value) == 0) {
            *exact_match = true;
        } else {
            *exact_match = false;
        }

        _cur_index = left;
        return Status::OK();
    }

    Status next_batch(size_t* n, ColumnBlockView* dst) override { return next_batch<true>(n, dst); }

    template <bool forward_index>
    inline Status next_batch(size_t* n, ColumnBlockView* dst) {
        DCHECK(_parsed);
        if (PREDICT_FALSE(*n == 0 || _cur_index >= _num_elements)) {
            *n = 0;
            return Status::OK();
        }

        size_t max_fetch = std::min(*n, static_cast<size_t>(_num_elements - _cur_index));
        _copy_next_values(max_fetch, dst->data());
        *n = max_fetch;
        if (forward_index) {
            _cur_index += max_fetch;
        }

        return Status::OK();
    }

    Status next_batch(size_t* n, vectorized::MutableColumnPtr& dst) override {
        DCHECK(_parsed);
        if (PREDICT_FALSE(*n == 0 || _cur_index >= _num_elements)) {
            *n = 0;
            return Status::OK();
        }

        size_t max_fetch = std::min(*n, static_cast<size_t>(_num_elements - _cur_index));

        dst->insert_many_fix_len_data((char*)&_chunk.data[_cur_index * SIZE_OF_TYPE], max_fetch);

        *n = max_fetch;
        _cur_index += max_fetch;

        return Status::OK();
    };

    Status peek_next_batch(size_t* n, ColumnBlockView* dst) override {
        return next_batch<false>(n, dst);
    }

    size_t count() const override { return _num_elements; }

    size_t current_index() const override { return _cur_index; }

private:
    void _copy_next_values(size_t n, void* data) {
        memcpy(data, &_chunk.data[_cur_index * SIZE_OF_TYPE], n * SIZE_OF_TYPE);
    }

    Status _decode() {
        if (_num_elements > 0) {
            int64_t bytes;
            if (!ChunkAllocator::instance()->allocate_align(
                        _num_element_after_padding * _size_of_element, &_chunk)) {
                return Status::RuntimeError("Decoded Memory Alloc failed");
            }
            char* in = const_cast<char*>(&_data[BITSHUFFLE_PAGE_HEADER_SIZE]);
            bytes = bitshuffle::decompress_lz4(in, _chunk.data, _num_element_after_padding,
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

    enum { SIZE_OF_TYPE = TypeTraits<Type>::size };

    Slice _data;
    PageDecoderOptions _options;
    bool _parsed;
    size_t _num_elements;
    size_t _compressed_size;
    size_t _num_element_after_padding;

    int _size_of_element;
    size_t _cur_index;
    Chunk _chunk;
    friend class BinaryDictPageDecoder;
};

} // namespace segment_v2
} // namespace doris
