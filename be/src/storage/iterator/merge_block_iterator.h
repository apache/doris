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

#include <utility>

#include "common/status.h"
#include "storage/iterator/block_iterator.h"
#include "storage/iterators.h"

namespace doris {

// Key-ordered BlockReader read that merges overlapping streams without folding same-key rows
// (e.g. DUP_KEYS horizontal compaction, sorting schema-change final merge, checksum, and
// changelog tablet horizontal compaction).
class MergeBlockIterator final : public BlockIterator {
public:
    explicit MergeBlockIterator(RowwiseIteratorUPtr input) : _input(std::move(input)) {}

    Status next_block(Block* block, bool* eof) override {
        Status st = _input->next_batch(block);
        if (st.is<ErrorCode::END_OF_FILE>()) {
            *eof = true;
            return Status::OK();
        }
        *eof = false;
        return st;
    }

    void update_profile(RuntimeProfile* profile) override { _input->update_profile(profile); }

private:
    const RowwiseIteratorUPtr _input;
};

} // namespace doris
