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

#include "storage/index/query/exec/chained_conjunction.h"

#include <algorithm>
#include <numeric>

#include "common/check.h"
#include "storage/index/query/spi/io_batch.h"

namespace doris::index_query {
namespace {

// Lists one term wave by wave; each wave's buffers are released before the next.
Status list_term(ChainedPostings& term, const std::vector<uint32_t>* candidates, IoBatch& batch,
                 std::vector<uint32_t>* out) {
    RETURN_IF_ERROR(term.start(candidates));
    bool done = false;
    while (!done) {
        batch.clear();
        RETURN_IF_ERROR(term.prepare_wave(batch, &done));
        if (batch.pending() > 0) {
            RETURN_IF_ERROR(batch.fetch());
        } else if (!done) {
            return Status::Error<ErrorCode::MEM_LIMIT_EXCEEDED, false>(
                    "chained conjunction: a wave admitted no reads");
        }
        RETURN_IF_ERROR(term.collect_wave(batch, out));
    }
    batch.clear();
    return Status::OK();
}

} // namespace

Status chained_conjunction(std::span<ChainedPostings* const> terms,
                           const std::vector<uint32_t>* initial_candidates, IoBatch& batch,
                           std::vector<uint32_t>* result, std::vector<size_t>* visited) {
    DORIS_CHECK(result != nullptr);
    DORIS_CHECK_EQ(batch.pending(), 0);
    result->clear();
    if (visited != nullptr) {
        visited->clear();
    }
    if (terms.empty()) {
        if (initial_candidates != nullptr) {
            *result = *initial_candidates;
        }
        return Status::OK();
    }
    if (initial_candidates != nullptr && initial_candidates->empty()) {
        return Status::OK();
    }
    std::vector<size_t> order(terms.size());
    std::iota(order.begin(), order.end(), 0);
    std::ranges::sort(order, [&](size_t left, size_t right) {
        return terms[left]->doc_freq() < terms[right]->doc_freq();
    });
    std::vector<uint32_t> next;
    for (size_t k = 0; k < order.size(); ++k) {
        // The first term reads the caller's candidates in place; later terms narrow
        // the previous result.
        const std::vector<uint32_t>* candidates = k == 0 ? initial_candidates : result;
        next.clear();
        RETURN_IF_ERROR(list_term(*terms[order[k]], candidates, batch, &next));
        if (visited != nullptr) {
            visited->push_back(order[k]);
        }
        result->swap(next);
        if (result->empty()) {
            break;
        }
    }
    return Status::OK();
}

} // namespace doris::index_query
