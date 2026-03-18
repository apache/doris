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
#include "storage/segment/segment.h"
#include "storage/tablet/tablet_schema.h"

namespace doris::segment_v2 {

AdaptiveBlockSizePredictor::AdaptiveBlockSizePredictor(size_t preferred_block_size_bytes,
                                                       size_t preferred_max_col_bytes,
                                                       const Segment& segment,
                                                       const std::vector<ColumnId>& output_columns)
        : _block_size_bytes(preferred_block_size_bytes), _max_col_bytes(preferred_max_col_bytes) {
    // Compute metadata hint from cached per-column raw_data_bytes in Segment.
    // Segment::column_raw_data_bytes() is a read-only map lookup, thread-safe.
    uint32_t seg_rows = segment.num_rows();
    if (seg_rows > 0) {
        const auto& ts = segment.tablet_schema();
        if (ts) {
            double total_bytes = 0.0;
            uint64_t matched_cols = 0;
            for (ColumnId cid : output_columns) {
                if (static_cast<size_t>(cid) < ts->num_columns()) {
                    int32_t uid = ts->column(cid).unique_id();
                    uint64_t raw_bytes = segment.column_raw_data_bytes(uid);
                    if (uid >= 0 && raw_bytes > 0) {
                        double col_bpr =
                                static_cast<double>(raw_bytes) / static_cast<double>(seg_rows);
                        _col_bytes_per_row[cid] = col_bpr;
                        total_bytes += static_cast<double>(raw_bytes);
                        matched_cols++;
                    }
                }
            }
            if (matched_cols > 0 && total_bytes > 0.0) {
                _metadata_hint_bytes_per_row = (total_bytes / static_cast<double>(seg_rows)) * 1.2;
            }
        }
    }
}

void AdaptiveBlockSizePredictor::update(const Block& block,
                                        const std::vector<ColumnId>& output_columns) {
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

    // Per-column EWMA sampling.
    // output_columns[i] corresponds to block column position i (caller's guarantee).
    size_t ncols = std::min(output_columns.size(), size_t(block.columns()));
    for (size_t i = 0; i < ncols; ++i) {
        ColumnId cid = output_columns[i];
        double col_cur = static_cast<double>(block.get_by_position(i).column->byte_size()) /
                         static_cast<double>(rows);
        auto it = _col_bytes_per_row.find(cid);
        if (it == _col_bytes_per_row.end()) {
            _col_bytes_per_row[cid] = col_cur;
        } else {
            it->second = kAlpha * it->second + kBeta * col_cur;
        }
    }
}

size_t AdaptiveBlockSizePredictor::predict_next_rows(size_t max_rows) {
    if (_block_size_bytes == 0) {
        return max_rows;
    }

    auto clamp_predicted_rows = [&](size_t predicted_rows) {
        size_t clamped_rows = std::min(predicted_rows, max_rows);
        if (!_has_history) {
            clamped_rows = std::min(clamped_rows, kSafetyBatchRowThreshold);
        }
        return std::max(size_t(1), clamped_rows);
    };

    double estimated_bytes_per_row = 0.0;

    if (!_has_history) {
        if (_metadata_hint_bytes_per_row > 0.0) {
            estimated_bytes_per_row = _metadata_hint_bytes_per_row;
        } else {
            return clamp_predicted_rows(max_rows);
        }
    } else {
        estimated_bytes_per_row = _bytes_per_row;
    }

    if (estimated_bytes_per_row <= 0.0) {
        return clamp_predicted_rows(max_rows);
    }

    auto predicted =
            static_cast<size_t>(static_cast<double>(_block_size_bytes) / estimated_bytes_per_row);

    // Apply per-column constraints when available (from metadata hints or EWMA history).
    if (!_col_bytes_per_row.empty() && _max_col_bytes > 0) {
        for (const auto& [cid, col_bpr] : _col_bytes_per_row) {
            if (col_bpr > 0.0) {
                auto col_limit = static_cast<size_t>(static_cast<double>(_max_col_bytes) / col_bpr);
                predicted = std::min(col_limit, predicted);
            }
        }
    }

    return clamp_predicted_rows(predicted);
}

} // namespace doris::segment_v2
