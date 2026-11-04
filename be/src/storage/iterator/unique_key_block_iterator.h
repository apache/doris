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

#include "common/status.h"
#include "storage/iterator/tablet_block_iterator.h"

namespace doris {

class Block;
class RowwiseIterator;

// Merge-on-read UNIQUE_KEYS tables without a sequence map: queries, compactions and the
// checksum task.
//
// The rows of the read arrive in key order with only the newest version of every key: the
// SameKeyIterator above the merge drops the older ones (drop_older_versions), and rowsets read
// one after another hold every key once. So the rows are copied as they come. The reads that may
// apply the delete sign drop the deleted rows in BlockReader::next_block_with_aggregation, after
// the merge.
class UniqueKeyBlockIterator final : public TabletBlockIterator {
public:
    explicit UniqueKeyBlockIterator(RowwiseIterator* rows) : _rows(rows) {}

    Status init() override { return Status::OK(); }

    Status next_block(Block* block, bool* eof) override;

private:
    RowwiseIterator* _rows;
};

} // namespace doris
