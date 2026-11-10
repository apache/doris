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
#include <vector>

#include "common/status.h"
#include "core/column/column.h"
#include "storage/iterator/same_key_flags.h"
#include "storage/iterators.h"

namespace doris {

class Block;
class RuntimeProfile;

// The rows of a VHeapMergeIterator with only the first row of every key, which the merge orders
// first as the newest version: the merge-on-read UNIQUE_KEYS reads
// (MergeDecision::drop_older_versions). The rows that repeat the key before them are dropped and
// counted in `*merged_rows`. Example: the merged rows k1(v3) k1(v2) k2(v1) come out as
// k1(v3) k2(v1), with one merged row.
class UniqueKeyIterator final : public RowwiseIterator {
public:
    UniqueKeyIterator(RowwiseIteratorUPtr rows, const MergeDecision& decision,
                      uint64_t* merged_rows);

    Status init(const StorageReadOptions& opts) override { return _rows->init(opts); }

    // Return OK with at least one row added to `block`, or EndOfFile once every row has been
    // read.
    Status next_batch(Block* block) override;

    const ReadSchema& schema() const override { return _rows->schema(); }

    void update_profile(RuntimeProfile* profile) override { _rows->update_profile(profile); }

private:
    const RowwiseIteratorUPtr _rows;
    SameKeyFlags _flags;
    uint64_t* const _merged_rows;
    // Reused by every next_batch().
    std::vector<uint8_t> _same_key;
    IColumn::Filter _filter;
};

RowwiseIteratorUPtr new_unique_key_iterator(RowwiseIteratorUPtr rows, const MergeDecision& decision,
                                            uint64_t* merged_rows);

} // namespace doris
