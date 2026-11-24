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

#include <cstddef>
#include <cstdint>
#include <vector>

#include "common/status.h"
#include "core/block/block.h"
#include "storage/iterator/agg_key_block_aggregator.h"
#include "storage/iterators.h"
#include "storage/schema.h"

namespace doris {
class RuntimeProfile;

// Folds the rows of each key into one row for the AGG_KEYS reads over rowsets that may hold a key
// more than once: queries that did not push their aggregation down, compactions and the checksum
// task. It feeds the merged Blocks to an AggKeyBlockAggregator, each one whole, and once the batch
// holds more keys than its row limit, it returns every key but the last, which may go on in the
// next merged Block. An output Block thus holds at least its row limit of keys, but the last one,
// and at most the keys of one merged Block more; the merge bounds its Blocks by the same row limit
// and byte budget.
class AggKeyBlockIterator {
public:
    // `input` hands out the rows of the read in key order, the rows of a key newest first, as a
    // VHeapMergeIterator does; the key columns lead `read_schema`. The row limit is
    // `block_row_max`, or, with a byte budget (`preferred_block_size_bytes`, 0 for none), the budget
    // divided by the average row size of the latest merged Block. `merged_rows` counts the rows
    // folded into an earlier row of their key.
    AggKeyBlockIterator(RowwiseIteratorUPtr input, size_t block_row_max,
                        size_t preferred_block_size_bytes, ReadSchemaSPtr read_schema,
                        int be_exec_version, uint64_t* merged_rows);

    // Set up the aggregation; nothing is read yet.
    Status init();

    // Fill `block`, which must be empty, with the next batch of aggregated rows; `*eof` once
    // there are none.
    Status next_block(Block* block, bool* eof);

    void update_profile(RuntimeProfile* profile) { _input->update_profile(profile); }

private:
    // The row limit for the merged Block in `_input_block`, see the constructor.
    size_t _compute_batch_row_limit() const;

    const RowwiseIteratorUPtr _input;
    const size_t _block_row_max;
    const size_t _preferred_block_size_bytes;
    const ReadSchemaSPtr _read_schema;
    const int _be_exec_version;
    AggKeyBlockAggregator _aggregator;
    // The merged Block the aggregator works on.
    Block _input_block;
};

} // namespace doris
