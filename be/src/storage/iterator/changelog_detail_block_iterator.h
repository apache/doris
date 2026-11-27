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

#include "common/status.h"
#include "core/block/block.h"
#include "core/column/column.h"
#include "storage/changelog_types.h"
#include "storage/iterator/block_batch_limit.h"
#include "storage/iterator/block_iterator.h"
#include "storage/iterator/changelog_iterator_utils.h"
#include "storage/schema.h"

namespace doris {

// Emits every recorded changelog event in (key, TSO) order for DETAIL scans: APPEND -> INSERT,
// DELETE -> DELETE (with before image), UPDATE -> UPDATE_BEFORE + UPDATE_AFTER.
class ChangelogDetailBlockIterator final : public BlockIterator {
public:
    ChangelogDetailBlockIterator(std::unique_ptr<BlockIterator> input, BlockBatchLimit batch_limit,
                                 ReadSchemaSPtr read_schema);

    Status next_block(Block* block, bool* eof) override;

    void update_profile(RuntimeProfile* profile) override { _cursor.update_profile(profile); }

private:
    Status _emit_change(ChangelogOp op, MutableColumns* target_columns, size_t row_limit,
                        size_t* output_rows);
    Status _append_change_row(MutableColumns* target_columns, changelog::StreamChangeOp output_op,
                              bool use_before) const;

    changelog::ChangelogInputCursor _cursor;
    const BlockBatchLimit _batch_limit;
    const ReadSchemaSPtr _read_schema;
    const int32_t _op_col_idx;
    changelog::ChangelogPendingRow _pending_row;
};

} // namespace doris
