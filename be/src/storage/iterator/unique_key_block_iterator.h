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
#include "core/column/column.h"
#include "storage/iterator/block_iterator.h"
#include "storage/iterators.h"
#include "storage/schema.h"

namespace doris {

// Keeps only the first row of each key from a key-ordered stream where the winning row of a key
// (highest sequence value, then newest rowset/segment) sorts first: merge-on-read UNIQUE_KEYS
// queries, horizontal compactions, sorting schema-change final merges, and checksums. When an
// input Block contains no duplicate keys, its columns are returned as-is without copying.
class UniqueKeyBlockIterator final : public BlockIterator {
public:
    UniqueKeyBlockIterator(RowwiseIteratorUPtr input, ReadSchemaSPtr read_schema,
                           uint64_t* merged_rows);

    Status next_block(Block* block, bool* eof) override;

    void update_profile(RuntimeProfile* profile) override { _input->update_profile(profile); }

private:
    void _mark_same_keys(const Block& block);
    void _save_last_key(const Block& block);

    const RowwiseIteratorUPtr _input;
    const ReadSchemaSPtr _read_schema;
    const size_t _num_key_columns;
    uint64_t* const _merged_rows;

    // Single-row key columns of the previous Block's last row, used to detect keys spanning
    // across Block boundaries. Empty before the first output Block.
    MutableColumns _last_key_columns;

    EqualFlags _same_key;
    IColumn::Filter _filter;
};

} // namespace doris
