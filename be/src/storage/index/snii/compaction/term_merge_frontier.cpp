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

#include "storage/index/snii/compaction/term_merge_frontier.h"

#include <utility>

#include "common/check.h"

namespace doris::snii::compaction {

bool TermMergeFrontier::Before::operator()(size_t lhs, size_t rhs) const {
    const uint64_t lhs_prefix = (*cursors)[lhs]->term_prefix();
    const uint64_t rhs_prefix = (*cursors)[rhs]->term_prefix();
    if (lhs_prefix != rhs_prefix) {
        return lhs_prefix < rhs_prefix;
    }
    const std::string& lhs_term = (*cursors)[lhs]->term();
    const std::string& rhs_term = (*cursors)[rhs]->term();
    if (lhs_term != rhs_term) {
        return lhs_term < rhs_term;
    }
    return (*cursors)[lhs]->source_ordinal() < (*cursors)[rhs]->source_ordinal();
}

Status TermMergeFrontier::init(std::vector<SniiSegmentTermCursor*> cursors) {
    if (initialized_) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "term_merge_frontier: init called twice");
    }
    initialized_ = true;
    cursors_ = std::move(cursors);
    std::vector<uint8_t> live(cursors_.size(), 0);
    for (size_t source = 0; source < cursors_.size(); ++source) {
        SniiSegmentTermCursor* cursor = cursors_[source];
        if (cursor == nullptr) {
            failed_ = Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "term_merge_frontier: null cursor");
            return failed_;
        }
        bool has_term = false;
        const Status status = cursor->next(&has_term);
        if (!status.ok()) {
            failed_ = status;
            return failed_;
        }
        live[source] = has_term;
    }
    frontier_.build(cursors_.size(), [&live](size_t source) { return live[source] != 0; });
    return Status::OK();
}

bool TermMergeFrontier::empty() const {
    DCHECK(initialized_);
    DCHECK(failed_.ok());
    return frontier_.empty();
}

SniiSegmentTermCursor* TermMergeFrontier::front() const {
    DCHECK(initialized_);
    DCHECK(failed_.ok());
    return cursors_[frontier_.winner()];
}

Status TermMergeFrontier::advance_front() {
    if (!initialized_) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "term_merge_frontier: advance before init");
    }
    if (!failed_.ok()) {
        return failed_;
    }
    if (frontier_.empty()) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "term_merge_frontier: advance on empty frontier");
    }

    const size_t source = frontier_.winner();
    bool has_term = false;
    const Status status = cursors_[source]->next(&has_term);
    if (!status.ok()) {
        failed_ = status;
        return failed_;
    }
    frontier_.update(source, has_term);
    return Status::OK();
}

} // namespace doris::snii::compaction
