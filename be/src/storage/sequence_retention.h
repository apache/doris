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

#include <algorithm>
#include <cstdint>

#include "common/status.h"

namespace doris {
class Block;
struct RowVisibilityFilter;

// Refine an initialized post-merge mask; only base/full readers receive a non-negative cutoff.
Status apply_sequence_retention_delete_filter(const Block& block, int64_t prune_version,
                                              RowVisibilityFilter* filter);

// Use a complete bitmap-ready prefix and a conservative 2N lag.
int64_t sequence_retention_prune_version(int64_t published_version, int64_t gap);

// The gap comes from TabletMeta during publish. Only plan_version is stored with pending input.
struct SequenceRetentionContext {
    int64_t gap = 0;
    int64_t plan_version = 0;

    Status validate() const {
        if (gap <= 0 || plan_version <= 0) {
            return Status::InvalidArgument("invalid sequence retention input: gap={}, plan={}", gap,
                                           plan_version);
        }
        return Status::OK();
    }

    int64_t expiry_version() const { return std::max<int64_t>(0, plan_version - gap); }

    Status check_commit(int64_t commit_version) const {
        RETURN_IF_ERROR(validate());
        if (commit_version <= plan_version || commit_version - plan_version > gap) {
            return Status::InvalidArgument(
                    "sequence retention transaction exceeded version window: plan={}, commit={}, "
                    "gap={}",
                    plan_version, commit_version, gap);
        }
        return Status::OK();
    }

    Result<bool> expired_delete(int64_t row_version) const {
        if (row_version <= 0) {
            return ResultError(Status::InternalError(
                    "published sequence delete sign has unknown original version {}", row_version));
        }
        return row_version <= expiry_version();
    }
};

} // namespace doris
