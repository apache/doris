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
#include <memory>
#include <vector>

#include "common/status.h"
#include "core/block/block.h"
#include "core/column/column.h"
#include "core/custom_allocator.h"

namespace doris {

// Flush-only attachment. It never crosses the segment file boundary: a writer materializes one
// requested range into a normal nullable column and releases it immediately after append.
// Common storage skeleton used by both MemTable's input-side sidecar and the final output-side
// attachment. Row ids are meaningful only in the owning object's coordinate space: input row ids
// in SparseNullableSidecar, sorted output row ids in SparseFlushOutput.
struct SparseColumnData {
    size_t row_count = 0;
    DorisVector<uint32_t> rowids;
    MutableColumnPtr values;
};

class SparseFlushOutput {
public:
    using Column = SparseColumnData;
    using Columns = std::vector<std::unique_ptr<Column>>;

    // Validate the complete attachment before publishing an immutable shared pointer.
    static Status create(Columns columns, std::shared_ptr<const SparseFlushOutput>* result);

    bool has_column(size_t cid) const { return cid < _columns.size() && _columns[cid] != nullptr; }

    Status materialize(size_t cid, size_t row_pos, size_t num_rows, const IColumn& prototype,
                       MutableColumnPtr* result) const;

private:
    explicit SparseFlushOutput(Columns columns) : _columns(std::move(columns)) {}
    static Status _validate(const Columns& columns);

    const Columns _columns;
};

// Atomic flush result: the block and its optional immutable Vertical-writer attachment always
// travel together, so dense fallback and failures cannot expose stale attachment state.
struct MemTableFlushOutput {
    std::unique_ptr<Block> block;
    std::shared_ptr<const SparseFlushOutput> sparse_attachment;
};

} // namespace doris
