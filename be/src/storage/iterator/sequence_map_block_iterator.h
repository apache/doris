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

#include "common/status.h"
#include "core/block/block.h"
#include "core/column/column.h"
#include "storage/iterator/block_iterator.h"
#include "storage/iterators.h"
#include "storage/schema.h"

namespace doris {
class RuntimeProfile;

// Folds same-key rows for merge-on-read UNIQUE_KEYS tables with sequence mapping (multiple
// sequence columns, each governing a group of value columns). Input Blocks arrive in key order
// with newer rowsets/segments first; for each key, the newest row initializes all columns and
// older rows replace any sequence group whose sequence value is strictly larger. The last key in
// `_batch_columns` stays open across input Blocks until a new key or EOF arrives.
class SequenceMapBlockIterator final : public BlockIterator {
public:
    SequenceMapBlockIterator(RowwiseIteratorUPtr input, size_t block_row_max,
                             size_t preferred_block_size_bytes, ReadSchemaSPtr read_schema,
                             uint64_t* merged_rows);

    Status next_block(Block* block, bool* eof) override;

    void update_profile(RuntimeProfile* profile) override { _input->update_profile(profile); }

private:
    size_t _compute_batch_row_limit() const;
    void _mark_same_keys(const Block& source);
    void _fold_block(const Block& source);
    void _replace_by_sequence(const Block& source, size_t src_pos);
    void _output_finished_keys(Block* block);
    void _output_all_keys(Block* block);

    const RowwiseIteratorUPtr _input;
    const size_t _block_row_max;
    const size_t _preferred_block_size_bytes;
    const ReadSchemaSPtr _read_schema;
    const size_t _num_key_columns;
    uint64_t* const _merged_rows;

    Block _input_block;
    MutableColumns _batch_columns;
    size_t _batch_rows = 0;
    EqualFlags _same_key;
};

} // namespace doris
