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

// Binary plain page encoding V2 with varuint length prefix.
//
// The page consists of:
// Data:
//   |length1(varuint)|binary1|length2(varuint)|binary2|...
// Trailer:
//   num_elems (32-bit fixed)
//

#pragma once

#include "common/logging.h"
#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/binary_plain_page.h"
#include "olap/rowset/segment_v2/options.h"
#include "olap/rowset/segment_v2/page_builder.h"
#include "olap/rowset/segment_v2/page_decoder.h"
#include "olap/types.h"
#include "util/coding.h"
#include "util/faststring.h"
#include "vec/columns/column_complex.h"
#include "vec/columns/column_nullable.h"

namespace doris {
namespace segment_v2 {
#include "common/compile_check_begin.h"

template <FieldType Type>
class BinaryPlainPageV2Builder : public PageBuilderHelper<BinaryPlainPageV2Builder<Type>> {
public:
    using Self = BinaryPlainPageV2Builder<Type>;
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

            // Store position for later retrieval
            _positions.push_back(cast_set<uint32_t>(_buffer.size()));

            // Write varuint length prefix
            uint8_t length_buffer[5]; // Max varuint32 size
            uint8_t* ptr = length_buffer;
            ptr = encode_varint32(ptr, cast_set<uint32_t>(src->size));
            size_t length_size = ptr - length_buffer;
            RETURN_IF_CATCH_EXCEPTION(_buffer.append(length_buffer, length_size));

            // Write the actual data
            RETURN_IF_CATCH_EXCEPTION(_buffer.append(src->data, src->size));

            _last_value_size = cast_set<uint32_t>(src->size);
            _size_estimate += length_size + src->size;
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
            // Store first and last values for statistics
            if (_positions.size() > 0) {
                _copy_value_at(0, &_first_value);
                _copy_value_at(_positions.size() - 1, &_last_value);
            }

            // Write trailer: number of elements
            put_fixed32_le(&_buffer, cast_set<uint32_t>(_positions.size()));

            *slice = _buffer.build();
        });
        return Status::OK();
    }

    Status reset() override {
        RETURN_IF_CATCH_EXCEPTION({
            _positions.clear();
            _buffer.clear();
            _buffer.reserve(_options.data_page_size == 0
                                    ? 1024
                                    : std::min(_options.data_page_size, _options.dict_page_size));
            _size_estimate = sizeof(uint32_t); // For the trailer
            _finished = false;
            _last_value_size = 0;
            _raw_data_size = 0;
        });
        return Status::OK();
    }

    size_t count() const override { return _positions.size(); }

    uint64_t size() const override { return _size_estimate; }

    uint64_t get_raw_data_size() const override { return _raw_data_size; }

    Status get_first_value(void* value) const override {
        DCHECK(_finished);
        if (_positions.size() == 0) {
            return Status::Error<ErrorCode::ENTRY_NOT_FOUND>("page is empty");
        }
        *reinterpret_cast<Slice*>(value) = Slice(_first_value);
        return Status::OK();
    }

    Status get_last_value(void* value) const override {
        DCHECK(_finished);
        if (_positions.size() == 0) {
            return Status::Error<ErrorCode::ENTRY_NOT_FOUND>("page is empty");
        }
        *reinterpret_cast<Slice*>(value) = Slice(_last_value);
        return Status::OK();
    }

    Status get_dict_word(uint32_t value_code, Slice* word) override {
        if (value_code >= _positions.size()) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT>(
                    "value_code {} is out of range [0, {})", value_code, _positions.size());
        }

        const uint8_t* ptr = reinterpret_cast<const uint8_t*>(&_buffer[_positions[value_code]]);
        uint32_t length;
        const uint8_t* data_ptr = decode_varint32_ptr(ptr, ptr + 5, &length);

        word->data = const_cast<char*>(reinterpret_cast<const char*>(data_ptr));
        word->size = length;

        return Status::OK();
    }

private:
    BinaryPlainPageV2Builder(const PageBuilderOptions& options)
            : _size_estimate(0), _options(options) {}

    void _copy_value_at(size_t idx, faststring* value) const {
        const uint8_t* ptr = &_buffer[_positions[idx]];
        uint32_t length;
        const uint8_t* data_ptr = decode_varint32_ptr(ptr, ptr + 5, &length);
        value->assign_copy(data_ptr, length);
    }

    faststring _buffer;
    size_t _size_estimate = 0;
    // Positions of each entry in the buffer (pointing to the varuint length)
    std::vector<uint32_t> _positions;
    bool _finished = false;
    PageBuilderOptions _options;
    uint32_t _last_value_size = 0;
    uint64_t _raw_data_size = 0;
    faststring _first_value;
    faststring _last_value;
};

// BinaryPlainPageV2Decoder now inherits from BinaryPlainPageDecoder.
// When used with BinaryPlainPageV2PreDecoder, the V2 format (varint-encoded lengths)
// is converted to V1 format (offset array) before being put into the page cache.
// This allows the decoder to use the base class implementation with O(1) seeking.
template <FieldType Type>
class BinaryPlainPageV2Decoder : public BinaryPlainPageDecoder<Type> {
public:
    BinaryPlainPageV2Decoder(Slice data) : BinaryPlainPageDecoder<Type>(data) {}

    BinaryPlainPageV2Decoder(Slice data, const PageDecoderOptions& options)
            : BinaryPlainPageDecoder<Type>(data, options) {}
};

#include "common/compile_check_end.h"
} // namespace segment_v2
} // namespace doris