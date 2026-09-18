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
#include <vector>

#include "common/status.h"
#include "storage/index/snii/compaction/indexed_winner_tree.h"
#include "storage/index/snii/compaction/term_cursor.h"

namespace doris::snii::compaction {

// K-way term frontier. Each source cursor is a dense winner-tree leaf.
// The caller consumes front()->entry() before advance_front(); advancing a
// source updates one leaf-to-root path and never materializes an intermediate
// merged-term group.
class TermMergeFrontier {
    struct Before {
        const std::vector<SniiSegmentTermCursor*>* cursors = nullptr;

        bool operator()(size_t lhs, size_t rhs) const;
    };

public:
    TermMergeFrontier() : frontier_(Before {.cursors = &cursors_}) {}

    Status init(std::vector<SniiSegmentTermCursor*> cursors);

    bool empty() const;
    SniiSegmentTermCursor* front() const;
    Status advance_front();

private:
    std::vector<SniiSegmentTermCursor*> cursors_;
    IndexedWinnerTree<Before> frontier_;
    bool initialized_ = false;
    Status failed_ = Status::OK();
};

} // namespace doris::snii::compaction
