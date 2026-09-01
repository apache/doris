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

#include <cstdint>

#include "common/status.h"
#include "storage/index/snii/common/slice.h"

// Where the builder's fully ordered point stream comes from (design 6.2).
//
// Leaf cutting is the SAME code in both build modes: consume the ordered stream,
// slice off points_per_leaf points at a time, encode a leaf (design 6.4). Only the
// upstream differs -- Phase 1 has one resident, freshly sorted run; Phase 2 adds a
// k-way merge over spilled runs. Naming that seam now is what lets Phase 2 be a NEW
// implementation of this interface rather than an edit to BkdBuilder::finish.
namespace doris::snii::bkd {

// A forward-only cursor over build-time point records, ordered by the memcmp of the
// whole record, i.e. by (value, doc_id) (see kPointDocIdBytes).
//
// Everything a source produces was produced by the builder in this same run, so its
// preconditions are internal invariants (DORIS_CHECK). The Status return exists for
// the Phase 2 merge, whose refills read spilled runs back from disk and can fail on
// IO -- corruption of an index FILE is not in scope here, that contract belongs to
// the decode side (design 8).
class PointSource {
public:
    virtual ~PointSource() = default;

    PointSource(const PointSource&) = delete;
    PointSource& operator=(const PointSource&) = delete;

    // Hands back the next run of at most `max_points` CONSECUTIVE records as one
    // contiguous view, which is exactly the shape encode_leaf_block consumes -- no
    // per-leaf PointRef array is ever materialized.
    //
    // The view is owned by the source and stays valid only until the next call.
    // Fewer than `max_points` records come back only when the stream runs out; an
    // EMPTY slice means exhausted, and every later call returns empty again.
    virtual Status next_block(uint32_t max_points, Slice* records) = 0;

protected:
    PointSource() = default;
};

} // namespace doris::snii::bkd
