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

// Reads that need no row-level fold in BlockReader:
//   * DUP_KEYS tables,
//   * queries on merge-on-write UNIQUE_KEYS tables (the delete bitmap already resolved the
//     duplicate keys),
//   * every direct-mode read, i.e. the scanner asked the storage layer not to merge, for
//     example a single-version tablet or a query whose aggregation is pushed down.
// The rows of the read already come as whole Blocks, so this iterator only passes them through.
class DirectBlockIterator final : public TabletBlockIterator {
public:
    explicit DirectBlockIterator(RowwiseIterator* rows) : _rows(rows) {}

    Status init() override { return Status::OK(); }

    Status next_block(Block* block, bool* eof) override;

private:
    RowwiseIterator* _rows;
};

} // namespace doris
