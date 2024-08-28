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

#include <gen_cpp/parquet_types.h>
#include <stddef.h>

#include <cstdint>

#include "common/status.h"
#include "parquet_common.h"
#include "util/bit_stream_utils.h"
#include "util/rle_encoding.h"
#include "util/slice.h"

namespace doris::vectorized {

class LevelDecoder {
public:
    LevelDecoder() = default;
    ~LevelDecoder() = default;

    Status init(Slice* slice, tparquet::Encoding::type encoding, level_t max_level,
                uint32_t num_levels);

    Status init_v2(const Slice& levels, level_t max_level, uint32_t num_levels);

    inline bool has_levels() const { return _num_levels > 0; }

    size_t get_levels(level_t* levels, size_t n);

    inline size_t get_next_run(level_t* val, size_t max_run) {
        return _rle_decoder.GetNextRun(val, max_run);
    }

    inline level_t get_next() {
        level_t next = -1;
        _rle_decoder.Get(&next);
        return next;
    }

    inline void rewind_one() { _rle_decoder.RewindOne(); }

    const RleDecoder<level_t>& rle_decoder() const { return _rle_decoder; }

private:
    tparquet::Encoding::type _encoding;
    level_t _bit_width = 0;
    level_t _max_level = 0;
    uint32_t _num_levels = 0;
    RleDecoder<level_t> _rle_decoder;
    BitReader _bit_packed_decoder;
};

} // namespace doris::vectorized
