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
#include <roaring/roaring.hh>

#include "storage/compaction/collection_similarity.h"
#include "storage/index/inverted/similarity/collection_statistics.h"

namespace doris::segment_v2 {

struct IndexQueryContext {
    io::IOContext* io_ctx = nullptr;
    OlapReaderStatistics* stats = nullptr;
    RuntimeState* runtime_state = nullptr;

    CollectionStatisticsPtr collection_statistics;
    CollectionSimilarityPtr collection_similarity;
    std::shared_ptr<const roaring::Roaring> delete_bitmap;

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

    // ---- Reply direction: fields a READER writes and the CALLER reads back ----
    //
    // A caller that hands a reader a COPY of this context rather than the context itself must
    // fold the copy back with merge_reader_outputs(), or the reader's reply is dropped in
    // silence: nothing fails to compile, no test goes red, the query simply takes the wrong plan.
    // FunctionSearch's SNII leaf builder is such a caller -- it copies the context so the reader
    // publishes its BM25 into a throwaway CollectionSimilarity instead of the query's own.
    //
    // Every field added below this line must also be merged in merge_reader_outputs().

    // G03 reply direction of the same handshake. Set by a reader iff it DID
    // answer with such a fabricated count bitmap (never on a query-cache hit,
    // a single-flight shared result, or any row-accurate decode). Read and
    // reset by SegmentIterator right after the index apply; a true value is
    // the precondition for the count-emission shortcut that materializes the
    // remaining count as default rows without iterating the row bitmap.
    bool count_on_index_fastpath_hit = false;

    // Folds the reply-direction fields a reader wrote on a copy of this context back into it.
    // Latching (never clearing) is what makes this safe to call for each of several readers.
    void merge_reader_outputs(const IndexQueryContext& reader_context) {
        count_on_index_fastpath_hit |= reader_context.count_on_index_fastpath_hit;
    }
};
using IndexQueryContextPtr = std::shared_ptr<IndexQueryContext>;

} // namespace doris::segment_v2
