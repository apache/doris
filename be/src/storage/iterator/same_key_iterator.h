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
// told apart by comparing them, so that the merge below only sorts: next_batch(Block*, same_key)
// flags them for the folds that join the rows of a key (AGG_KEYS, a sequence map, changelog
// scans), and next_batch(Block*) passes them through. The merge-on-read UNIQUE_KEYS reads, which
// keep only the newest row of a key, read through a UniqueKeyIterator instead.
class SameKeyIterator final : public RowwiseIterator {
public:
    // `key_ordinals` are the read-schema ordinals the rows are merged by.
    SameKeyIterator(RowwiseIteratorUPtr rows, std::vector<uint32_t> key_ordinals)
            : _rows(std::move(rows)), _flags(std::move(key_ordinals)) {}

    Status init(const StorageReadOptions& opts) override { return _rows->init(opts); }

    Status next_batch(Block* block) override { return _rows->next_batch(block); }

    Status next_batch(Block* block, std::vector<uint8_t>* same_key) override;

    const ReadSchema& schema() const override { return _rows->schema(); }

    void update_profile(RuntimeProfile* profile) override { _rows->update_profile(profile); }

    uint64_t merged_rows() const override { return _rows->merged_rows(); }

private:
    const RowwiseIteratorUPtr _rows;
    SameKeyFlags _flags;
};

RowwiseIteratorUPtr new_same_key_iterator(RowwiseIteratorUPtr rows,
                                          std::vector<uint32_t> key_ordinals);

} // namespace doris
