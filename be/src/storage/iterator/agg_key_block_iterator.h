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

#include <cstdint>

#include "common/status.h"
#include "core/block/block.h"
#include "storage/iterator/agg_key_block_aggregator.h"
#include "storage/iterator/block_batch_limit.h"
#include "storage/iterator/block_iterator.h"
#include "storage/iterators.h"
#include "storage/schema.h"

namespace doris {

// Folds the rows of each key into one row for the AGG_KEYS reads over rowsets that may hold a key
// more than once: queries that did not push their aggregation down, compactions and the checksum
// task. It feeds the merged Blocks to an AggKeyBlockAggregator, each one whole, and once the batch
// holds more keys than its row limit, it returns every key but the last, which may go on in the
// next merged Block. An output Block thus holds at least its row limit of keys, but the last one,
// and at most the keys of one merged Block more; the merge bounds its Blocks by the same row limit
// and byte budget.
class AggKeyBlockIterator final : public BlockIterator {
public:
    // `input` hands out the rows of the read in key order, the rows of a key newest first, as a
    // VHeapMergeIterator does; the key columns lead `read_schema`. `batch_limit` bounds the output
    // Block by row count and optional byte budget. `merged_rows` counts the rows folded into an
    // earlier row of their key.
    AggKeyBlockIterator(RowwiseIteratorUPtr input, BlockBatchLimit batch_limit,
                        ReadSchemaSPtr read_schema, int be_exec_version, uint64_t* merged_rows);

    // Set up the aggregation; nothing is read yet.
    Status init();

    // Fill `block`, which must be empty, with the next batch of aggregated rows; `*eof` once
    // there are none.
    Status next_block(Block* block, bool* eof) override;

    void update_profile(RuntimeProfile* profile) override { _input->update_profile(profile); }

private:
    const RowwiseIteratorUPtr _input;
    const BlockBatchLimit _batch_limit;
    const ReadSchemaSPtr _read_schema;
    const int _be_exec_version;
    AggKeyBlockAggregator _aggregator;
    // The merged Block the aggregator works on.
    Block _input_block;
};

} // namespace doris
