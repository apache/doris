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

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "exprs/vexpr_fwd.h"

namespace doris {

using LateRuntimeFilterExprGroup = VExprContextSPtrs;

struct LateRuntimeFilterEntry {
    // The publisher writes expr first and then stores true with release semantics. Readers must
    // load this flag with acquire semantics before reading expr.
    std::atomic<bool> valid {false};
    int32_t filter_id = -1;
    std::shared_ptr<const LateRuntimeFilterExprGroup> expr;
};

struct LateRuntimeFilterContainer {
    explicit LateRuntimeFilterContainer(const std::vector<int32_t>& filter_ids)
            : filters(filter_ids.size()) {
        for (size_t i = 0; i < filter_ids.size(); ++i) {
            filters[i].filter_id = filter_ids[i];
        }
    }

    // Number of entries whose expression groups are visible to storage readers. The publisher
    // increments it with release semantics; readers use an acquire load as the change notification.
    std::atomic<uint32_t> arrived_cnt {0};

    // Constructed once after the open-phase acquire and never resized afterwards.
    std::vector<LateRuntimeFilterEntry> filters;
};

} // namespace doris
