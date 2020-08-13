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

#include "olap/memory/common.h"
#include "olap/memory/delta_index.h"

namespace doris {
namespace memory {

// ColumnDelta store a column's updates of a commit(version)
class ColumnDelta : public RefCountedThreadSafe<ColumnDelta> {
public:
    ColumnDelta() = default;

    size_t memory() const;

    size_t size() const { return _size; }

    Buffer& nulls() { return _nulls; }

    Buffer& data() { return _data; }

    DeltaIndex* index() { return _index.get(); }

    bool contains_block(uint32_t bid) const { return _index->contains_block(bid); }

    uint32_t find_idx(uint32_t rid) { return _index->find_idx(rid); }

    Status alloc(size_t nblock, size_t size, size_t esize, bool has_null);

private:
    size_t _size = 0;
    scoped_refptr<DeltaIndex> _index;
    Buffer _nulls;
    Buffer _data;
    DISALLOW_COPY_AND_ASSIGN(ColumnDelta);
};

} // namespace memory
} // namespace doris
