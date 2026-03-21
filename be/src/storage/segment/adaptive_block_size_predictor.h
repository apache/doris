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

#include <stddef.h>
#include <stdint.h>

#include <unordered_map>
#include <vector>

#include "storage/olap_common.h"

namespace doris {

class Block;

namespace segment_v2 {

class Segment;

// Predicts the number of rows to read in the next SegmentIterator::next_batch() call so that
// the resulting Block stays close to |preferred_block_size_bytes|.
//
// The predictor maintains an EWMA estimate of bytes-per-row for the whole block and for each
// individual output column.  After each successful batch the caller must invoke update(); before
// each batch the caller invokes predict_next_rows() to obtain the recommended row count.
//
// Not thread-safe; must be used by a single thread per instance.
class AdaptiveBlockSizePredictor {
public:
    // |preferred_block_size_bytes|: target total bytes of each output block chunk.
    // |preferred_max_col_bytes|: per-column byte budget (0 = unlimited).
    // |segment|: used only during construction to compute a metadata hint (not stored).
    // |output_columns|: column ids to consider for the metadata hint.
    AdaptiveBlockSizePredictor(size_t preferred_block_size_bytes, size_t preferred_max_col_bytes,
                               const Segment& segment, const std::vector<ColumnId>& output_columns);

    // Update EWMA estimates from a completed batch.  Must be called only when block.rows() > 0
    // and the batch returned Status::OK().
    // |output_columns[i]| must correspond to block column position i (caller guarantees order).
    void update(const Block& block, const std::vector<ColumnId>& output_columns);

    // Predict how many rows the next batch should read.
    // Never exceeds |max_rows|; never returns less than 1.
    // Uses pre-computed metadata hint for first-call estimate when no history exists.
    // Does NOT modify internal state (_has_history is only flipped by update()).
    size_t predict_next_rows(size_t max_rows);

private:
    // EWMA weight for historical estimate (0.9) and current sample (0.1).
    static constexpr double kAlpha = 0.9;
    static constexpr double kBeta = 0.1;
    // Safety guard for the first prediction before any real batch history is available.
    static constexpr size_t kSafetyBatchRowThreshold = 4096;

    const size_t _block_size_bytes;
    const size_t _max_col_bytes;

    // EWMA estimate of total bytes per row across all output columns.
    double _bytes_per_row = 0.0;
    // EWMA estimate per output column id.
    std::unordered_map<ColumnId, double> _col_bytes_per_row;

    // Whether at least one update() has been called (i.e. we have real measured history).
    bool _has_history = false;

    // Cached conservative metadata estimate computed on the first predict_next_rows() call.
    // Reused on subsequent first-round predictions (before _has_history is set) to avoid
    // re-traversing the segment footer on every call.
    double _metadata_hint_bytes_per_row = 0.0;

#ifdef BE_TEST
public:
    // Test-only constructor that directly sets the metadata hint.
    AdaptiveBlockSizePredictor(size_t preferred_block_size_bytes, size_t preferred_max_col_bytes,
                               double metadata_hint_bytes_per_row)
            : _block_size_bytes(preferred_block_size_bytes),
              _max_col_bytes(preferred_max_col_bytes),
              _metadata_hint_bytes_per_row(metadata_hint_bytes_per_row) {}

    double bytes_per_row_for_test() const { return _bytes_per_row; }
    bool has_history_for_test() const { return _has_history; }
    static constexpr size_t safety_batch_row_threshold_for_test() {
        return kSafetyBatchRowThreshold;
    }
    double col_bytes_per_row_for_test(ColumnId cid) const {
        auto it = _col_bytes_per_row.find(cid);
        return it == _col_bytes_per_row.end() ? 0.0 : it->second;
    }
    void set_metadata_hint_for_test(double v) { _metadata_hint_bytes_per_row = v; }
    void set_has_history_for_test(bool h, double bpr) {
        _has_history = h;
        _bytes_per_row = bpr;
    }
    void set_col_bytes_per_row_for_test(ColumnId cid, double v) { _col_bytes_per_row[cid] = v; }
#endif
};

} // namespace segment_v2
} // namespace doris
