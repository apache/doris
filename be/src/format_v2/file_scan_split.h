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

#include "gen_cpp/PlanNodes_types.h"

namespace doris {

// Format readers may derive from this type, while the shared scheduler deliberately treats the
// payload as opaque and only preserves its lifetime across generated child tasks.
struct FileScanSplitContext {
    virtual ~FileScanSplitContext() = default;
};

struct FileScanSplitTask {
    TFileRangeDesc range;
    std::shared_ptr<const TFileRangeDesc> parent_range;
    std::shared_ptr<const FileScanSplitContext> context;

    TFileRangeDesc materialize_range() const {
        if (parent_range == nullptr) {
            return range;
        }
        // Queued children share bulky table/delete descriptors and override only physical bounds;
        // materializing on claim keeps descriptor copies bounded by active scanners, not row groups.
        TFileRangeDesc result = *parent_range;
        if (range.__isset.start_offset) {
            result.__set_start_offset(range.start_offset);
        }
        if (range.__isset.size) {
            result.__set_size(range.size);
        }
        result.__set_is_file_parent(false);
        return result;
    }
};

} // namespace doris
