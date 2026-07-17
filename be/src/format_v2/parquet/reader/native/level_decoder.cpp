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

#include "format_v2/parquet/reader/native/level_decoder.h"

#include <gen_cpp/parquet_types.h>

#include <algorithm>
#include <limits>

#include "common/cast_set.h"
#include "format/parquet/parquet_common.h"
#include "util/bit_stream_utils.inline.h"
#include "util/bit_util.h"
#include "util/coding.h"

namespace doris::format::parquet::native {

static constexpr size_t V1_LEVEL_SIZE = 4;

Status LevelDecoder::init(Slice* slice, tparquet::Encoding::type encoding, level_t max_level,
                          uint32_t num_levels) {
    _encoding = encoding;
    _bit_width = cast_set<level_t>(BitUtil::log2(max_level + 1));
    _max_level = max_level;
    _num_levels = num_levels;
    _has_buffered_level = false;
    _can_rewind = false;
    switch (encoding) {
    case tparquet::Encoding::RLE: {
        if (slice->size < V1_LEVEL_SIZE) {
            return Status::Corruption("Wrong parquet level format");
        }

        uint8_t* data = (uint8_t*)slice->data;
        uint32_t num_bytes = decode_fixed32_le(data);
        if (num_bytes > slice->size - V1_LEVEL_SIZE) {
            return Status::Corruption("Wrong parquet level format");
        }
        _rle_decoder = RleBatchDecoder<uint16_t>(data + V1_LEVEL_SIZE, num_bytes, _bit_width);

        slice->data += V1_LEVEL_SIZE + num_bytes;
        slice->size -= V1_LEVEL_SIZE + num_bytes;
        break;
    }
    case tparquet::Encoding::BIT_PACKED: {
        // Header counts are uint32_t and can overflow before the byte bound check. Widen first so
        // a forged level count cannot wrap to a small slice and desynchronize following values.
        const uint64_t num_bits = static_cast<uint64_t>(num_levels) * _bit_width;
        const size_t num_bytes = static_cast<size_t>((num_bits + 7) / 8);
        if (num_bytes > slice->size) {
            return Status::Corruption("Wrong parquet level format");
        }
        if (num_bytes > static_cast<size_t>(std::numeric_limits<int>::max())) {
            return Status::Corruption("Parquet BIT_PACKED level stream is too large");
        }
        _bit_packed_decoder = BitReader((uint8_t*)slice->data, cast_set<int>(num_bytes));

        slice->data += num_bytes;
        slice->size -= num_bytes;
        break;
    }
    default:
        return Status::IOError("Unsupported encoding for parquet level");
    }
    return Status::OK();
}

Status LevelDecoder::init_v2(const Slice& levels, level_t max_level, uint32_t num_levels) {
    _encoding = tparquet::Encoding::RLE;
    _bit_width = cast_set<level_t>(BitUtil::log2(max_level + 1));
    _max_level = max_level;
    _num_levels = num_levels;
    _has_buffered_level = false;
    _can_rewind = false;
    size_t byte_length = levels.size;
    _rle_decoder = RleBatchDecoder<uint16_t>((uint8_t*)levels.data, cast_set<int>(byte_length),
                                             _bit_width);
    return Status::OK();
}

size_t LevelDecoder::get_levels(level_t* levels, size_t n) {
    _can_rewind = false;
    // toto template.
    if (_encoding == tparquet::Encoding::RLE) {
        n = std::min((size_t)_num_levels, n);
        size_t num_decoded = 0;
        if (_has_buffered_level && n > 0) {
            if (!accept_level(_buffered_level)) {
                return 0;
            }
            levels[num_decoded++] = _buffered_level;
            _has_buffered_level = false;
        }
        if (num_decoded < n) {
            const size_t remaining = n - num_decoded;
            _rle_scratch.resize(remaining);
            const size_t batch_decoded =
                    _rle_decoder.GetBatch(_rle_scratch.data(), cast_set<uint32_t>(remaining));
            for (size_t i = 0; i < batch_decoded; ++i) {
                const level_t level = cast_set<level_t>(_rle_scratch[i]);
                if (!accept_level(level)) {
                    return 0;
                }
                levels[num_decoded + i] = level;
            }
            num_decoded += batch_decoded;
        }
        _num_levels -= num_decoded;
        _can_rewind = false;
        return num_decoded;
    } else if (_encoding == tparquet::Encoding::BIT_PACKED) {
        n = std::min((size_t)_num_levels, n);
        size_t decoded = 0;
        for (; decoded < n; ++decoded) {
            level_t level = -1;
            if (!_bit_packed_decoder.GetValue(_bit_width, &level)) {
                break;
            }
            if (!accept_level(level)) {
                return 0;
            }
            levels[decoded] = level;
        }
        _num_levels -= decoded;
        return decoded;
    }
    return 0;
}

size_t LevelDecoder::get_next_run(level_t* val, size_t max_run) {
    DORIS_CHECK(val != nullptr);
    _can_rewind = false;
    max_run = std::min<size_t>(max_run, _num_levels);
    if (max_run == 0) {
        return 0;
    }
    if (_encoding == tparquet::Encoding::RLE) {
        size_t decoded = 0;
        if (_has_buffered_level) {
            *val = _buffered_level;
            _has_buffered_level = false;
            if (!accept_level(*val)) {
                return 0;
            }
            decoded = 1;
        } else {
            uint16_t first = 0;
            if (_rle_decoder.GetBatch(&first, 1) != 1) {
                return 0;
            }
            *val = cast_set<level_t>(first);
            if (!accept_level(*val)) {
                return 0;
            }
            decoded = 1;
        }
        while (decoded < max_run) {
            const int32_t repeats = _rle_decoder.NextNumRepeats();
            if (repeats > 0) {
                const level_t repeated = cast_set<level_t>(_rle_decoder.GetRepeatedValue(0));
                if (!accept_level(repeated)) return 0;
                if (repeated != *val) break;
                const int32_t consume = std::min(repeats, cast_set<int32_t>(max_run - decoded));
                _rle_decoder.GetRepeatedValue(consume);
                decoded += consume;
                continue;
            }
            if (_rle_decoder.NextNumLiterals() == 0) break;
            uint16_t literal = 0;
            if (!_rle_decoder.GetLiteralValues(1, &literal)) break;
            const level_t next = cast_set<level_t>(literal);
            if (!accept_level(next)) return 0;
            if (next != *val) {
                // Batch RLE has no physical rewind; retain the one-value lookahead logically.
                _buffered_level = next;
                _has_buffered_level = true;
                break;
            }
            ++decoded;
        }
        _num_levels -= decoded;
        _can_rewind = false;
        return decoded;
    }
    if (_encoding != tparquet::Encoding::BIT_PACKED ||
        !_bit_packed_decoder.GetValue(_bit_width, val)) {
        return 0;
    }
    if (!accept_level(*val)) {
        return 0;
    }
    size_t decoded = 1;
    while (decoded < max_run) {
        level_t next = -1;
        if (!_bit_packed_decoder.GetValue(_bit_width, &next)) {
            break;
        }
        if (!accept_level(next)) return 0;
        if (next != *val) {
            // The lookahead belongs to the following run, so cursor APIs must leave it unread.
            _bit_packed_decoder.Rewind(_bit_width);
            break;
        }
        ++decoded;
    }
    _num_levels -= decoded;
    return decoded;
}

level_t LevelDecoder::get_next() {
    if (_num_levels == 0) {
        return -1;
    }
    level_t next = -1;
    bool decoded = false;
    if (_encoding == tparquet::Encoding::RLE) {
        if (_has_buffered_level) {
            next = _buffered_level;
            _has_buffered_level = false;
            decoded = true;
        } else {
            uint16_t value = 0;
            decoded = _rle_decoder.GetBatch(&value, 1) == 1;
            next = cast_set<level_t>(value);
        }
    } else if (_encoding == tparquet::Encoding::BIT_PACKED) {
        decoded = _bit_packed_decoder.GetValue(_bit_width, &next);
    }
    if (!decoded) {
        return -1;
    }
    if (!accept_level(next)) {
        return -1;
    }
    --_num_levels;
    _last_level = next;
    _can_rewind = true;
    return next;
}

void LevelDecoder::rewind_one() {
    if (_encoding == tparquet::Encoding::RLE) {
        DORIS_CHECK(_can_rewind && !_has_buffered_level);
        _buffered_level = _last_level;
        _has_buffered_level = true;
    } else if (_encoding == tparquet::Encoding::BIT_PACKED) {
        DORIS_CHECK(_can_rewind);
        _bit_packed_decoder.Rewind(_bit_width);
    } else {
        return;
    }
    // Rewinding restores one advertised level as well as its encoded bits.
    ++_num_levels;
    _can_rewind = false;
}

} // namespace doris::format::parquet::native
