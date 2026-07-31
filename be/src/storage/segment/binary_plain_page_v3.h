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

// Binary plain page encoding V3.
//
// The page consists of:
// Data:
//   |binary1|binary2|...|binaryN|
// Lengths (contiguous varuint block):
//   |varuint_len1|varuint_len2|...|varuint_lenN|
// Trailer:
//   |data_block_size(32-bit fixed)|num_elems(32-bit fixed)|
//
// vs V2 (which interleaves length and data per-entry), V3 lets the pre-decoder
// memcpy the entire data block in a single shot when converting to V1 layout,
// and walk the varint lengths once to fill the offsets array.
//
// V3 stores exactly the same bytes as V1/V2 — only the on-disk layout differs. In
// particular, CHAR values keep their trailing '\0' padding on disk (as written by
// OlapColumnDataConvertorChar); that padding is stripped on read by
// BinaryPlainPageV3PreDecoder<true>, selected for (CHAR, PLAIN_ENCODING_V3), exactly
// mirroring PLAIN_ENCODING_V2.

#pragma once

#include "common/logging.h"
#include "core/column/column_complex.h"
#include "core/column/column_nullable.h"
#include "storage/olap_common.h"
#include "storage/segment/binary_plain_page.h"
#include "storage/segment/options.h"
#include "storage/segment/page_builder.h"
#include "storage/segment/page_decoder.h"
#include "storage/types.h"
#include "util/coding.h"
#include "util/faststring.h"

namespace doris {
namespace segment_v2 {

template <FieldType Type>
class BinaryPlainPageV3Builder : public PageBuilderHelper<BinaryPlainPageV3Builder<Type>> {
public:
    using Self = BinaryPlainPageV3Builder<Type>;
    friend class PageBuilderHelper<Self>;

    Status init() override { return reset(); }

    bool is_page_full() override {
        bool ret = false;
        if (_options.is_dict_page) {
            ret = _options.dict_page_size != 0 && _size_estimate > _options.dict_page_size;
        } else {
            ret = _options.data_page_size != 0 && _size_estimate > _options.data_page_size;
        }
        return ret;
    }

    Status add(const uint8_t* vals, size_t* count) override {
        DCHECK(!_finished);
        DCHECK_GT(*count, 0);
        size_t i = 0;

        while (!is_page_full() && i < *count) {
            const auto* src = reinterpret_cast<const Slice*>(vals);
            if constexpr (Type == FieldType::OLAP_FIELD_TYPE_BITMAP) {
                if (_options.need_check_bitmap) {
                    RETURN_IF_ERROR(BitmapTypeCode::validate(*(src->data)));
                }
            }

            // Append the data straight into the contiguous data buffer. V3 stores the same
            // bytes as V1/V2 (CHAR keeps its '\0' padding, VARCHAR does not); only the layout
            // differs. CHAR padding is stripped on read by BinaryPlainPageV3PreDecoder<true>.
            RETURN_IF_CATCH_EXCEPTION(_data_buffer.append(src->data, src->size));

            // Encode varuint length into a scratch buffer, then append.
            uint8_t length_buffer[5]; // max varint32 size
            uint8_t* ptr = encode_varint32(length_buffer, cast_set<uint32_t>(src->size));
            size_t length_size = ptr - length_buffer;
            RETURN_IF_CATCH_EXCEPTION(_lengths_buffer.append(length_buffer, length_size));

            _num_elems++;
            _size_estimate += src->size + length_size;
            _raw_data_size += src->size;

            i++;
            vals += sizeof(Slice);
        }

        *count = i;
        return Status::OK();
    }

    Status finish(OwnedSlice* slice) override {
        DCHECK(!_finished);
        _finished = true;
        RETURN_IF_CATCH_EXCEPTION({
            // Layout: |data...|lengths...|data_block_size(u32)|num_elems(u32)|
            const uint32_t data_block_size = cast_set<uint32_t>(_data_buffer.size());
            // Append lengths after data.
            _data_buffer.append(_lengths_buffer.data(), _lengths_buffer.size());
            // Trailer: data_block_size, num_elems.
            put_fixed32_le(&_data_buffer, data_block_size);
            put_fixed32_le(&_data_buffer, _num_elems);
            *slice = _data_buffer.build();
        });
        return Status::OK();
    }

    Status reset() override {
        RETURN_IF_CATCH_EXCEPTION({
            _data_buffer.clear();
            _lengths_buffer.clear();
            _data_buffer.reserve(_options.data_page_size == 0 ? 1024
                                                              : std::min(_options.data_page_size,
                                                                         _options.dict_page_size));
            _lengths_buffer.reserve(256);
            _num_elems = 0;
            // Reserve the trailer (data_block_size + num_elems).
            _size_estimate = 2 * sizeof(uint32_t);
            _finished = false;
            _raw_data_size = 0;
        });
        return Status::OK();
    }

    size_t count() const override { return _num_elems; }

    uint64_t size() const override { return _size_estimate; }

    uint64_t get_raw_data_size() const override { return _raw_data_size; }

private:
    BinaryPlainPageV3Builder(const PageBuilderOptions& options)
            : _size_estimate(0), _options(options) {}

    faststring _data_buffer;
    faststring _lengths_buffer;
    uint32_t _num_elems = 0;
    size_t _size_estimate = 0;
    bool _finished = false;
    PageBuilderOptions _options;
    uint64_t _raw_data_size = 0;
};

// V3 decoder behaves identically to the V1 decoder because the V3 pre-decoder
// converts the on-disk V3 layout to the V1 (offsets-array) layout before the
// page is put into the page cache. The decoder operating on the cached page
// therefore only needs to know how to read the V1 layout.
template <FieldType Type>
class BinaryPlainPageV3Decoder : public BinaryPlainPageDecoder<Type> {
public:
    BinaryPlainPageV3Decoder(Slice data) : BinaryPlainPageDecoder<Type>(data) {}

    BinaryPlainPageV3Decoder(Slice data, const PageDecoderOptions& options)
            : BinaryPlainPageDecoder<Type>(data, options) {}
};

} // namespace segment_v2
} // namespace doris
