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
#include <cstddef>
#include <cstdint>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/format/prx_decode_stats.h"
#include "storage/index/snii/query/internal/docid_conjunction.h"
#include "storage/index/snii/reader/logical_index_reader.h"

namespace doris::snii::query {
struct PhraseMatch;
}

namespace doris::snii::query::internal {

struct ResolvedPhrasePlan {
    std::vector<ResolvedQueryTerm> unique_terms;
    std::vector<size_t> phrase_plan_index;
    std::vector<uint32_t> position_offsets;

    [[nodiscard]] bool is_valid() const {
        if (phrase_plan_index.size() != position_offsets.size() ||
            unique_terms.empty() != phrase_plan_index.empty()) {
            return false;
        }
        if (phrase_plan_index.empty()) {
            return true;
        }

        std::vector<uint8_t> referenced(unique_terms.size(), 0);
        for (size_t i = 0; i < phrase_plan_index.size(); ++i) {
            if ((i == 0 && position_offsets[i] != 0) ||
                (i != 0 && position_offsets[i - 1] >= position_offsets[i])) {
                return false;
            }
            const size_t plan_index = phrase_plan_index[i];
            if (plan_index >= unique_terms.size()) {
                return false;
            }
            referenced[plan_index] = 1;
        }
        return std::ranges::all_of(referenced, [](uint8_t used) { return used != 0; });
    }
};

// Executes an already-resolved exact phrase plan. Planning policy and term-key
// semantics stay above this boundary; the executor treats terms as opaque and
// consumes the resolved entries so inline posting payloads can move into the
// execution plans without another allocation/copy.
Status execute_resolved_phrase_plan(const reader::LogicalIndexReader& idx,
                                    ResolvedPhrasePlan&& plan, std::vector<uint32_t>* docids,
                                    format::PrxDecodeContext* observer_context = nullptr,
                                    std::vector<PhraseMatch>* matches = nullptr,
                                    const std::vector<uint32_t>* candidate_prefilter = nullptr);

} // namespace doris::snii::query::internal
