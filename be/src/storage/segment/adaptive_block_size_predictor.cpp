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

#include "storage/segment/adaptive_block_size_predictor.h"

#include <algorithm>
#include <cstddef>

#include "core/block/block.h"

namespace doris {
AdaptiveBlockSizePredictor::AdaptiveBlockSizePredictor(size_t preferred_block_size_bytes,
                                                       double metadata_hint_bytes_per_row,
                                                       size_t probe_rows, size_t block_size_rows)
        : _block_size_bytes(preferred_block_size_bytes),
          _block_size_rows(block_size_rows),
          _initial_probe_rows(probe_rows),
          _metadata_hint_bytes_per_row(metadata_hint_bytes_per_row) {}

void AdaptiveBlockSizePredictor::update(const Block& block) {
    size_t rows = block.rows();
    if (rows == 0) {
        return;
    }
    double cur = static_cast<double>(block.bytes()) / static_cast<double>(rows);

    if (!_has_history) {
        _bytes_per_row = cur;
        _has_history = true;
    } else {
        _bytes_per_row = kAlpha * _bytes_per_row + kBeta * cur;
    }
}

size_t AdaptiveBlockSizePredictor::predict_next_rows() {
    if (_block_size_bytes == 0) {
        return _block_size_rows;
    }

    auto clamp_predicted_rows = [&](size_t predicted_rows) {
        size_t clamped_rows = std::min(predicted_rows, _block_size_rows);
        if (!_has_history) {
            clamped_rows = std::min(clamped_rows, _initial_probe_rows);
        }
        return std::max(size_t(1), clamped_rows);
    };

    double estimated_bytes_per_row = 0.0;

    if (!_has_history) {
        if (_metadata_hint_bytes_per_row > 0.0) {
            estimated_bytes_per_row = _metadata_hint_bytes_per_row;
        } else {
            return clamp_predicted_rows(_block_size_rows);
        }
    } else {
        estimated_bytes_per_row = _bytes_per_row;
    }

    if (estimated_bytes_per_row <= 0.0) {
        return clamp_predicted_rows(_block_size_rows);
    }

    auto predicted =
            static_cast<size_t>(static_cast<double>(_block_size_bytes) / estimated_bytes_per_row);

    return clamp_predicted_rows(predicted);
}

} // namespace doris
