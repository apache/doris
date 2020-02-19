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

#include "olap/out_stream.h" // for OutStream

#include "olap/byte_buffer_stream.h" // for ByteBufferStream
#include "olap/rowset/run_length_integer_reader.h" // for RunLengthIntegerReader
#include "olap/rowset/run_length_integer_writer.h" // for RunLengthIntegerWriter
#include "olap/rowset/segment_v2/options.h" // for PageBuilderOptions/PageDecoderOptions
#include "olap/rowset/segment_v2/page_builder.h" // for PageBuilder
#include "olap/rowset/segment_v2/page_decoder.h" // for PageDecoder
#include "util/slice.h" // for OwnedSlice
#include "util/faststring.h"
#include "util/debug_util.h"

namespace doris {
namespace segment_v2 {

enum {
    RLE_V2_PAGE_HEADER_SIZE = 4
};

// RLEV2 page builder for generic integer and bool.
//
// The page format is as follows:
//
// 1. Header: (4 bytes total)
//
//    <num_elements> [32-bit]
//      The number of elements encoded in the page.
//
// 2. Element data
//
//    The header is followed by the rle-encoded element data.
//
// Refer to the Rle encoding algorithm, please go to class RunLengthIntegerWriter.
//
template<FieldType Type>
class RleV2PageBuilder : public PageBuilder {
public:
    RleV2PageBuilder(const PageBuilderOptions& options) :
        _options(options),
        _count(0),
        _finished(false) {
        // set the buffer size to data_page_size
        // 
        reset();
    }

    ~RleV2PageBuilder() {
    }

    bool is_page_full() override {
        return _output_stream->current_buffer_byte() >= _options.data_page_size;
    }

    Status add(const uint8_t* vals, size_t* count) override {
        DCHECK(!_finished);
        auto new_vals = reinterpret_cast<const CppType*>(vals);
        int i = 0;
        for (; i < *count; ++i) {
            if (is_page_full()) {
                break;
            }
            auto st = _rle_writer->write(new_vals[i]);
            if (st != OLAP_SUCCESS) {
                return Status::InternalError(strings::Substitute("rle writer write error:$0", st));
            }
        }
        if (i == 0) {
            // add no data
            return Status::OK();
        }

        if (_count == 0) {
            memcpy(&_first_value, new_vals, SIZE_OF_TYPE);
        }
        memcpy(&_last_value, &new_vals[i - 1], SIZE_OF_TYPE);

        _count += i;
        *count = i;
        return Status::OK();
    }

    OwnedSlice finish() override {
        DCHECK(!_finished);
        _finished = true;
        encode_fixed32_le(&_buf[0], _count);
        _buf.resize(RLE_V2_PAGE_HEADER_SIZE);
        _rle_writer->flush();
        size_t buffer_num = 0;
        size_t buffer_size = 0;
        for (auto& buffer : _output_stream->output_buffers()) {
            _buf.append(buffer->array(), buffer->limit());
            ++buffer_num;
            buffer_size += buffer->limit();
        }
        return _buf.build();
    }

    void reset() override {
        _count = 0;
        _buf.clear();
        _buf.reserve(_options.data_page_size);
        _output_stream.reset(new OutStream(_options.data_page_size, nullptr));
        bool is_signed = true;
        switch (Type) {
        case OLAP_FIELD_TYPE_UNSIGNED_TINYINT:
        case OLAP_FIELD_TYPE_UNSIGNED_SMALLINT:
        case OLAP_FIELD_TYPE_UNSIGNED_INT:
        case OLAP_FIELD_TYPE_UNSIGNED_BIGINT:
        case OLAP_FIELD_TYPE_DATE:
        case OLAP_FIELD_TYPE_DATETIME:
            is_signed = false;
            break;
        default:
            is_signed = true;
            break;
        }
        is_signed = false;
        _rle_writer.reset(new RunLengthIntegerWriter(_output_stream.get(), is_signed));
    }

    size_t count() const override {
        return _count;
    }

    uint64_t size() const override {
        return _output_stream->get_total_buffer_size();
    }

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
    typedef typename TypeTraits<Type>::CppType CppType;
    enum {
        SIZE_OF_TYPE = TypeTraits<Type>::size
    };

    PageBuilderOptions _options;
    size_t _count;
    bool _finished;
    faststring _buf;
    CppType _first_value;
    CppType _last_value;
    std::unique_ptr<OutStream> _output_stream;
    std::unique_ptr<RunLengthIntegerWriter> _rle_writer;
};

template<FieldType Type>
class RleV2PageDecoder : public PageDecoder {
public:
    RleV2PageDecoder(Slice slice, const PageDecoderOptions& options) :
        _data(slice),
        _options(options),
        _parsed(false),
        _num_elements(0),
        _cur_index(0),
        _is_signed(true) { }

    Status init() override {
        CHECK(!_parsed);

        if (_data.size < RLE_V2_PAGE_HEADER_SIZE) {
            return Status::Corruption(
                "not enough bytes for header in RleBitMapBlockDecoder");
        }
        _num_elements = decode_fixed32_le((const uint8_t*)&_data.data[0]);

        Slice buffer_data = Slice(&_data.data[RLE_V2_PAGE_HEADER_SIZE], _data.size - RLE_V2_PAGE_HEADER_SIZE);
        _byte_buffer_stream.reset(new ByteBufferStream(buffer_data));
        switch (Type) {
        case OLAP_FIELD_TYPE_UNSIGNED_TINYINT:
        case OLAP_FIELD_TYPE_UNSIGNED_SMALLINT:
        case OLAP_FIELD_TYPE_UNSIGNED_INT:
        case OLAP_FIELD_TYPE_UNSIGNED_BIGINT:
        case OLAP_FIELD_TYPE_DATE:
        case OLAP_FIELD_TYPE_DATETIME:
            _is_signed = false;
            break;
        default:
            _is_signed = true;
            break;
        }
        _is_signed = false;
        _rle_reader.reset(new RunLengthIntegerReader(_byte_buffer_stream.get(), _is_signed));

        _parsed = true;

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
            _rle_reader->skip(nskip);
        } else {
            _byte_buffer_stream->reset(0, 0);
            _rle_reader.reset(new RunLengthIntegerReader(_byte_buffer_stream.get(), _is_signed));
            _rle_reader->skip(pos);
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
        OLAPStatus result = OLAP_SUCCESS;
        int64_t value = 0;
        while (remaining > 0) {
            result = _rle_reader->next(&value);
            DCHECK(result == OLAP_SUCCESS);
            CppType* cur_value = reinterpret_cast<CppType*>(data_ptr);
            *cur_value = static_cast<CppType>(value);
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
    bool _is_signed;
    std::unique_ptr<ByteBufferStream> _byte_buffer_stream;
    std::unique_ptr<RunLengthIntegerReader> _rle_reader;
};

} // namespace segment_v2
} // namespace doris
