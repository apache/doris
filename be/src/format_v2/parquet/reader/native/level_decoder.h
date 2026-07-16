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
#include <vector>

#include "common/status.h"
#include "format/parquet/parquet_common.h"
#include "util/bit_stream_utils.h"
#include "util/rle_encoding.h"
#include "util/slice.h"

namespace doris::format::parquet::native {
class LevelDecoder {
public:
    LevelDecoder() = default;
    ~LevelDecoder() = default;

    Status init(Slice* slice, tparquet::Encoding::type encoding, level_t max_level,
                uint32_t num_levels);

    Status init_v2(const Slice& levels, level_t max_level, uint32_t num_levels);

    inline bool has_levels() const { return _num_levels > 0; }

    size_t get_levels(level_t* levels, size_t n);

    size_t get_next_run(level_t* val, size_t max_run);

    level_t get_next();

    void rewind_one();

    void release_scratch(size_t max_retained_bytes) {
        if (_rle_scratch.capacity() * sizeof(uint16_t) > max_retained_bytes) {
            std::vector<uint16_t>().swap(_rle_scratch);
        }
    }

private:
    tparquet::Encoding::type _encoding;
    level_t _bit_width = 0;
    level_t _max_level = 0;
    uint32_t _num_levels = 0;
    RleBatchDecoder<uint16_t> _rle_decoder;
    std::vector<uint16_t> _rle_scratch;
    BitReader _bit_packed_decoder;
    bool _has_buffered_level = false;
    bool _can_rewind = false;
    level_t _buffered_level = -1;
    level_t _last_level = -1;
};

} // namespace doris::format::parquet::native
