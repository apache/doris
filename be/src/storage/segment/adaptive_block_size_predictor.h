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

#include <vector>

#include "storage/olap_common.h"

namespace doris {

class Block;

// Predicts the number of rows to read in the next batch so that the resulting Block stays close
// to |preferred_block_size_bytes|.
//
// The predictor maintains an EWMA estimate of bytes-per-row for the whole block.  After each
// successful batch the caller must invoke update(); before each batch the caller invokes
// predict_next_rows() to obtain the recommended row count.
//
// Not thread-safe; must be used by a single thread per instance.
class AdaptiveBlockSizePredictor {
public:
    static constexpr size_t kDefaultProbeRows = 4096;
    static constexpr size_t kDefaultBlockSizeRows = 65535;

    // Per-column metadata for computing segment-level hints.
    struct ColumnMetadata {
        ColumnId column_id;
        uint64_t raw_bytes; // total raw data bytes for this column in the segment
    };

    // |preferred_block_size_bytes|: target total bytes of each output block chunk.
    // |metadata_hint_bytes_per_row|: pre-computed conservative estimate from metadata (e.g.
    //     segment footer or file statistics). 0.0 means no hint available.
    // |probe_rows|: first-batch row cap before any real history is available.
    // |block_size_rows|: hard maximum rows of each output block chunk.
    AdaptiveBlockSizePredictor(size_t preferred_block_size_bytes,
                               double metadata_hint_bytes_per_row,
                               size_t probe_rows = kDefaultProbeRows,
                               size_t block_size_rows = kDefaultBlockSizeRows);

    // Update EWMA estimates from a completed batch.  Must be called only when block.rows() > 0
    // and the batch returned Status::OK().
    void update(const Block& block);

    // Predict how many rows the next batch should read.
    // Never exceeds |block_size_rows|; never returns less than 1.
    // Uses pre-computed metadata hint for first-call estimate when no history exists.
    // Does NOT modify internal state (_has_history is only flipped by update()).
    size_t predict_next_rows();

    bool has_history() const { return _has_history; }

private:
    // EWMA weight for historical estimate (0.9) and current sample (0.1).
    static constexpr double kAlpha = 0.9;
    static constexpr double kBeta = 0.1;

    const size_t _block_size_bytes;
    const size_t _block_size_rows;
    const size_t _initial_probe_rows;

    // EWMA estimate of total bytes per row across all output columns.
    double _bytes_per_row = 0.0;

    // Whether at least one update() has been called (i.e. we have real measured history).
    bool _has_history = false;

    // Cached conservative metadata estimate computed on the first predict_next_rows() call.
    // Reused on subsequent first-round predictions (before _has_history is set) to avoid
    // re-traversing the segment footer on every call.
    double _metadata_hint_bytes_per_row = 0.0;

#ifdef BE_TEST
public:
    double bytes_per_row_for_test() const { return _bytes_per_row; }
    bool has_history_for_test() const { return _has_history; }
    size_t probe_rows_for_test() const { return _initial_probe_rows; }
    size_t block_size_rows_for_test() const { return _block_size_rows; }
    static constexpr size_t default_probe_rows_for_test() { return kDefaultProbeRows; }
    static constexpr size_t default_block_size_rows_for_test() { return kDefaultBlockSizeRows; }
    void set_metadata_hint_for_test(double v) { _metadata_hint_bytes_per_row = v; }
    void set_has_history_for_test(bool h, double bpr) {
        _has_history = h;
        _bytes_per_row = bpr;
    }
#endif
};

} // namespace doris
