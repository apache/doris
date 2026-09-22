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
#include <cstdint>
#include <span>
#include <vector>

#include "common/status.h"

namespace doris::index_query {

class IoBatch;

// One term of a chained conjunction. A term lists its documents among the
// surviving candidates in one or more waves: a wave registers its reads into the
// shared batch, the chain fetches the batch, and the term decodes the wave.
class ChainedPostings {
public:
    virtual ~ChainedPostings() = default;

    // Documents holding the term; the chain lists cheaper terms first.
    virtual uint64_t doc_freq() const = 0;

    // Begins listing the term's documents that are in `candidates`, or all of its
    // documents when `candidates` is null. The candidates stay unchanged until the
    // listing ends.
    virtual Status start(const std::vector<uint32_t>* candidates) = 0;

    // Registers the reads of the next wave without reading. Sets `*done` when this
    // wave is the last one. Only a last wave may register nothing.
    virtual Status prepare_wave(IoBatch& batch, bool* done) = 0;

    // Appends the wave's documents that are in the candidates, ascending, to `out`.
    virtual Status collect_wave(const IoBatch& batch, std::vector<uint32_t>* out) = 0;
};

// Intersects `terms` over all documents, or over `initial_candidates` when given,
// listing terms in ascending document frequency. Each term reads only what the
// surviving candidates need, and an empty intermediate result ends the chain
// before a later term reads anything. `batch` must be empty; it is left empty.
// `visited` receives the indexes of the listed terms in listing order.
Status chained_conjunction(std::span<ChainedPostings* const> terms,
                           const std::vector<uint32_t>* initial_candidates, IoBatch& batch,
                           std::vector<uint32_t>* result, std::vector<size_t>* visited = nullptr);

} // namespace doris::index_query
