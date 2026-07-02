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

#include "storage/compaction/collection_similarity.h"
#include "storage/compaction/collection_statistics.h"

namespace doris::segment_v2 {

struct IndexQueryContext {
    io::IOContext* io_ctx = nullptr;
    OlapReaderStatistics* stats = nullptr;
    RuntimeState* runtime_state = nullptr;

    CollectionStatisticsPtr collection_statistics;
    CollectionSimilarityPtr collection_similarity;

    size_t query_limit = 0;
    bool is_asc = false;

    // G02 count-only fast-path handshake. Set by SegmentIterator ONLY while it
    // evaluates the single pushed-down MATCH predicate of a COUNT_ON_INDEX scan
    // whose row space is provably unfiltered (no deletes, no other conjuncts,
    // full row bitmap, no row-id consumers -- see count_on_index_fastpath.h),
    // and reset immediately after. When set, an index reader MAY answer the
    // query with a bitmap whose CARDINALITY equals the match count without the
    // row ids being real (SNII returns [0, df) straight from dict-entry df,
    // skipping the posting decode). Readers must never cache such a bitmap
    // under a key a row-accurate query could hit.
    bool count_on_index_fastpath = false;
    // G03 reply direction of the same handshake. Set by a reader iff it DID
    // answer with such a fabricated count bitmap (never on a query-cache hit,
    // a single-flight shared result, or any row-accurate decode). Read and
    // reset by SegmentIterator right after the index apply; a true value is
    // the precondition for the count-emission shortcut that materializes the
    // remaining count as default rows without iterating the row bitmap.
    bool count_on_index_fastpath_hit = false;
};
using IndexQueryContextPtr = std::shared_ptr<IndexQueryContext>;

} // namespace doris::segment_v2