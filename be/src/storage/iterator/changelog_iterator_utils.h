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
#include "storage/changelog.h"
#include "storage/changelog_types.h"
#include "storage/iterator/block_iterator.h"
#include "storage/schema.h"

namespace doris {
class RuntimeProfile;

ChangelogOp read_changelog_op(const IColumn& col, size_t row);
Status write_changelog_op(IColumn& col, changelog::StreamChangeOp op);

// Parks the UPDATE_AFTER half of an UPDATE pair when UPDATE_BEFORE fills an output Block.
class ChangelogPendingRow {
public:
    bool has_row() const { return _has_row; }

    MutableColumns& park(const MutableColumns& target_columns);
    bool emit(MutableColumns& target_columns);

private:
    MutableColumns _columns;
    bool _has_row = false;
};

// Steps over a BlockIterator, reading its first Block lazily on first use.
class ChangelogInputCursor {
public:
    ChangelogInputCursor(std::unique_ptr<BlockIterator> input, ReadSchemaSPtr read_schema);

    Status ensure_started();
    Status advance();
    Status advance_to(size_t pos);

    bool eof() const { return _eof; }
    const Block& block() const { return _block; }
    size_t row_pos() const { return _pos; }
    uint64_t block_id() const { return _block_id; }

    void update_profile(RuntimeProfile* profile) { _input->update_profile(profile); }

private:
    Status _refill();

    const std::unique_ptr<BlockIterator> _input;
    const ReadSchemaSPtr _read_schema;
    Block _block;
    size_t _pos = 0;
    uint64_t _block_id = 0;
    bool _started = false;
    bool _eof = false;
};

} // namespace doris
