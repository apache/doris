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

#include <map>
#include <vector>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <string>

#include "util/coding.h"
#include "util/slice.h"
#include "util/faststring.h"
#include "runtime/mem_pool.h"

#include "olap/rowset/segment_v2/options.h"
#include "olap/rowset/segment_v2/page_builder.h"
#include "olap/rowset/segment_v2/page_decoder.h"

namespace doris {
namespace segment_v2 {

// prefix encoding for string dictionary
//
// BinaryPrefixPage := Entry^EntryNum, Trailer
// Entry := SharedPrefixLength(vint), UnsharedLength(vint), Byte^UnsharedLength
// Trailer := NumEntry(uint32_t), RESTART_POINT_INTERVAL(uint8_t)
//            RestartPointStartOffset(uint32_t)^NumRestartPoints,NumRestartPoints(uint32_t)
class BinaryPrefixPageBuilder : public PageBuilder {
public:
    BinaryPrefixPageBuilder(const PageBuilderOptions& options) :
        _options(options) {
        reset();
    }

    bool is_page_full() override {
        return size() >= _options.data_page_size;
    }

    Status add(const uint8_t* vals, size_t* add_count) override;

    OwnedSlice finish() override;

    void reset() override {
        _restart_points_offset.clear();
        _last_entry.clear();
        _count = 0;
        _buffer.clear();
        _finished = false;
    }

    uint64_t size() const override {
        if (_finished) {
            return _buffer.size();
        } else {
            return _buffer.size() + (_restart_points_offset.size() + 2) * sizeof(uint32_t);
        }
    }

    size_t count() const override {
        return _count;
    }

    Status get_first_value(void* value) const override {
        DCHECK(_finished);
        if (_count == 0) {
            return Status::NotFound("page is empty");
        }
        *reinterpret_cast<Slice*>(value) = Slice(_first_entry);
        return Status::OK();
    }

    Status get_last_value(void* value) const override {
        DCHECK(_finished);
        if (_count == 0) {
            return Status::NotFound("page is empty");
        }
        *reinterpret_cast<Slice*>(value) = Slice(_last_entry);
        return Status::OK();
    }

private:
    PageBuilderOptions _options;
    std::vector<uint32_t> _restart_points_offset;
    faststring _first_entry;
    faststring _last_entry;
    size_t _count = 0;
    bool _finished = false;
    faststring _buffer;
    // This is a empirical value, Kudu and LevelDB use this default value
    static const uint8_t RESTART_POINT_INTERVAL = 16;
};

class BinaryPrefixPageDecoder : public PageDecoder {
public:
    BinaryPrefixPageDecoder(Slice data, const PageDecoderOptions& options) :
        _data(data), _parsed(false) {
    }

    Status init() override;

    Status seek_to_position_in_page(size_t pos) override;

    Status seek_at_or_after_value(const void* value, bool* exact_match) override;

    Status next_batch(size_t* n, ColumnBlockView* dst) override;

    size_t count() const override {
        DCHECK(_parsed);
        return _num_values;
    }

    size_t current_index() const override {
        DCHECK(_parsed);
        return _cur_pos;
    }

private:
    // decode shared and non-shared entry length from `ptr`.
    // return ptr past the parsed value when success.
    // return nullptr on failure
    const uint8_t* _decode_value_lengths(const uint8_t* ptr, uint32_t* shared, uint32_t* non_shared);


    // return start pointer of the restart point at index `restart_point_index`
    const uint8_t* _get_restart_point(size_t restart_point_index) const {
        return reinterpret_cast<const uint8_t*>(_data.get_data()) +
               decode_fixed32_le(_restarts_ptr + restart_point_index * sizeof(uint32_t));
    }

    // read next value at `_cur_pos` and `_next_ptr` into `_current_value`.
    // return OK and advance `_next_ptr` on success. `_cur_pos` is not modified.
    // return NotFound when no more entry can be read.
    // return other error status otherwise.
    Status _read_next_value();

    // seek to the first value at the given restart point
    Status _seek_to_restart_point(size_t restart_point_index);

    // like _read_next_value, but derictly copy next value to output, not _current_value
    Status _read_next_value_to_output(Slice prev, MemPool* mem_pool, Slice* output);

    // copy `_current_value` into `output`.
    Status _copy_current_to_output(MemPool* mem_pool, Slice* output);

    Slice _data;
    bool _parsed = false;
    size_t _num_values = 0;
    uint8_t _restart_point_internal = 0;
    uint32_t _num_restarts = 0;
    // pointer to _footer start
    const uint8_t* _footer_start = nullptr;
    // pointer to restart offsets array
    const uint8_t* _restarts_ptr = nullptr;
    // ordinal of the first value to return in next_batch()
    uint32_t _cur_pos = 0;
    // first value to return in next_batch()
    faststring _current_value;
    // pointer to the start of next value to read, advanced by `_read_next_value`
    const uint8_t* _next_ptr = nullptr;
};

} // namespace segment_v2
} // namespace doris