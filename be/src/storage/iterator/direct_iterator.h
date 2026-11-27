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

#include <vector>

#include "common/status.h"
#include "storage/iterators.h"
#include "storage/rowset/rowset_fwd.h"
#include "storage/schema.h"

namespace doris {
class Block;
class RuntimeProfile;
struct OlapReaderStatistics;

// Reads the rows of a DUP_KEYS query that does not ask for key order: the rowsets one after
// another in version order, each through its rowset reader, with nothing to merge or fold. A
// rowset reader, and with it its rowset, is released once it is read.
class DirectIterator {
public:
    // `rs_readers` are inited and in version order; their Blocks have `read_schema`. `stats`
    // outlives the iterator.
    DirectIterator(std::vector<RowsetReaderSharedPtr> rs_readers, ReadSchemaSPtr read_schema,
                   OlapReaderStatistics* stats);

    // Set up the read; nothing is read yet: a rowset reader opens its segments on its first read.
    Status init();

    // Fill `block`, which must be empty, with the next rows of the read; `*eof` once there are
    // none.
    Status next_block(Block* block, bool* eof);

    void update_profile(RuntimeProfile* profile) { _rowsets->update_profile(profile); }

private:
    // The rowset readers, each behind a RowwiseIterator, in one VUnionIterator.
    RowwiseIteratorUPtr _rowsets;
    OlapReaderStatistics* const _stats;
};

} // namespace doris
