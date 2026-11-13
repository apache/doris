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

// The reads of AGG_KEYS tables that fold the rows of a key into one row: queries that did not push
// their aggregation down, compactions and the checksum task, over rowsets that may hold a key more
// than once. Like StarRocks' AggregateIterator, it reads the rows in key order from a
// VHeapMergeIterator, the rows of a key newest first, hands every input Block to a
// AggKeyBlockAggregator, and returns the batches of aggregated rows it fills.
class AggKeyBlockIterator {
public:
    // `input` hands out the rows of the read in key order, the rows of a key newest first, as a
    // VHeapMergeIterator does. `key_ordinals` are the read-schema ordinals of the key columns, the
    // ones the merge orders by. An output Block holds `max_rows` rows at most, and about
    // `preferred_block_size_bytes` bytes unless that is 0. `merged_rows` counts the rows folded
    // into an earlier row of their key.
    AggKeyBlockIterator(RowwiseIteratorUPtr input, std::vector<uint32_t> key_ordinals,
                        size_t max_rows, size_t preferred_block_size_bytes,
                        ReadSchemaSPtr read_schema, int be_exec_version, uint64_t* merged_rows);

    // Set up the aggregation; nothing is read yet.
    Status init();

    // Fill `block`, which must be empty, with the next batch of aggregated rows; `*eof` once
    // there are none.
    Status next_block(Block* block, bool* eof);

    void update_profile(RuntimeProfile* profile) { _input->update_profile(profile); }

private:
    const RowwiseIteratorUPtr _input;
    const ReadSchemaSPtr _read_schema;
    const int _be_exec_version;
    AggKeyBlockAggregator _aggregator;
    // The input Block the aggregator works on.
    Block _input_block;
    // The input has returned EndOfFile.
    bool _input_done = false;
};

} // namespace doris
