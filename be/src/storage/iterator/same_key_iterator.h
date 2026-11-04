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
#include <utility>
#include <vector>

#include "common/status.h"
#include "core/column/column.h"
#include "storage/iterators.h"

namespace doris {

class Block;
class RuntimeProfile;

// The same-key flags of a key-ordered stream of Blocks, worked out from the rows themselves:
// a row's flag is 1 when its key columns equal those of the row before it, the last row of the
// previous Block for the first row of a Block. Example: Blocks k1 k1 k2 | k2 k3 give the flags
// 0 1 0 | 1 0.
class SameKeyFlags {
public:
    explicit SameKeyFlags(std::vector<uint32_t> key_ordinals)
            : _key_ordinals(std::move(key_ordinals)) {}

    // Set `flags` to one flag per row of `block` from `first_row` on, at least one row, and keep
    // the key of its last row for the rows that come next. The rows before `first_row` are not
    // part of the stream.
    void compute(const Block& block, size_t first_row, std::vector<uint8_t>* flags);

private:
    const std::vector<uint32_t> _key_ordinals;
    // One row: the key columns of the last row seen. Empty before the first Block.
    MutableColumns _last_key;
};

// The rows of a key-ordered iterator, usually a VHeapMergeIterator, with the rows of one key
// told apart by comparing them, so that the merge below only sorts.
//
// With the read's `drop_older_versions` (merge-on-read UNIQUE_KEYS), next_batch(Block*) keeps
// the first row of every key, which the merge orders first as the newest version, and drops the
// others; merged_rows() counts them. Otherwise next_batch(Block*) passes the rows through, and
// next_batch(Block*, same_key) flags them for the folds that join the rows of a key (AGG_KEYS,
// a sequence map, changelog scans).
class SameKeyIterator final : public RowwiseIterator {
public:
    SameKeyIterator(RowwiseIteratorUPtr rows, const MergeDecision& decision);

    Status init(const StorageReadOptions& opts) override { return _rows->init(opts); }

    Status next_batch(Block* block) override;

    // Not with drop_older_versions: the folds that need the flags keep every row.
    Status next_batch(Block* block, std::vector<uint8_t>* same_key) override;

    const ReadSchema& schema() const override { return _rows->schema(); }

    void update_profile(RuntimeProfile* profile) override { _rows->update_profile(profile); }

    uint64_t merged_rows() const override { return _merged_rows + _rows->merged_rows(); }

private:
    const RowwiseIteratorUPtr _rows;
    const bool _drop_older_versions;
    SameKeyFlags _flags;
    // Reused by next_batch(Block*) with drop_older_versions.
    std::vector<uint8_t> _same_key;
    IColumn::Filter _filter;
    uint64_t _merged_rows = 0;
};

RowwiseIteratorUPtr new_same_key_iterator(RowwiseIteratorUPtr rows, const MergeDecision& decision);

} // namespace doris
