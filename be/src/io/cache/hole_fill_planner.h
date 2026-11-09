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
#include "io/fs/file_range_coalescer.h"

namespace doris::io {

/// Computes the remote reads needed to complete one File Cache block. Covered intervals are first
/// unioned, their complement inside `block` becomes the hole list, and the supplied coalescing
/// policy may combine nearby holes without ever crossing the block boundary.
class HoleFillPlanner {
public:
    /// `covered_intervals` may overlap and arrive in any order, but each interval must be non-empty
    /// and contained by `block`. The output is sorted, disjoint, and contained by `block`.
    static Status plan(const FileRange& block, const std::vector<FileRange>& covered_intervals,
                       const FileRangeCoalesceOptions& options,
                       std::vector<FileRange>* output_ranges);
};

} // namespace doris::io
