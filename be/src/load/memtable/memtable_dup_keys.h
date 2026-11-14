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

#include <memory>

#include "load/memtable/memtable.h"

namespace doris {

// DUP_KEYS never merges rows, so the only per-row state it needs is the order of the source row
// positions. A uint32_t permutation avoids one shared_ptr and one RowInBlock per input row, plus
// one allocation per batch; the rows themselves stay in the mutable block.
class DupKeysMemTable final : public MemTable {
public:
    DupKeysMemTable(int64_t tablet_id, std::shared_ptr<TabletSchema> tablet_schema,
                    const std::vector<SlotDescriptor*>* slot_descs, TupleDescriptor* tuple_desc,
                    PartialUpdateInfo* partial_update_info,
                    const std::shared_ptr<ResourceContext>& resource_ctx, bool need_lsn);
    ~DupKeysMemTable() override;

protected:
    void _reserve_rows(size_t num_rows, size_t cursor) override;
    void _append_row_metadata(size_t num_rows, size_t cursor,
                              const DorisVector<int64_t>& allocated_lsns) override;
    size_t _sort() override;
    Status _put_into_output(Block& in_block) override;
    Status _to_block(std::unique_ptr<Block>* res) override;

private:
    // Permutation of source row positions. Sorting mutates only this compact vector.
    DorisVector<uint32_t> _row_positions;
    // Optional row-binlog LSNs stay indexed by the original row position. Output applies the
    // sorted permutation to the rows and to their LSNs identically. Empty unless need_lsn, and an
    // empty DorisVector holds no buffer, so it costs no allocation for the common case.
    DorisVector<int64_t> _allocated_lsns;
};

} // namespace doris
