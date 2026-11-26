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

#include <algorithm>
#include <cstddef>

#include "common/exception.h"
#include "core/block/block.h"

namespace doris {

// Bounds how many rows a re-batching iterator emits into one output Block: at most `max_rows`,
// or `preferred_bytes` divided by the observed average row size (clamped to `[1, max_rows]`)
// when `preferred_bytes > 0`.
class BlockBatchLimit {
public:
    BlockBatchLimit() = default;

    explicit BlockBatchLimit(size_t max_rows, size_t preferred_bytes = 0)
            : _max_rows(max_rows), _preferred_bytes(preferred_bytes) {
        DORIS_CHECK_GT(_max_rows, 0);
    }

    size_t max_rows() const { return _max_rows; }
    size_t preferred_bytes() const { return _preferred_bytes; }

    size_t row_limit(size_t sample_bytes, size_t sample_rows) const {
        if (_preferred_bytes == 0 || sample_rows == 0) {
            return _max_rows;
        }
        const size_t row_bytes = std::max<size_t>(1, sample_bytes / sample_rows);
        return std::clamp<size_t>(_preferred_bytes / row_bytes, 1, _max_rows);
    }

    size_t row_limit(const Block& sample_block) const {
        return row_limit(sample_block.bytes(), sample_block.rows());
    }

private:
    size_t _max_rows = 4096 - 32;
    size_t _preferred_bytes = 0;
};

} // namespace doris
