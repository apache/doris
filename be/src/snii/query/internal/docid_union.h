#pragma once

#include <vector>

#include "common/status.h"
#include "snii/query/docid_sink.h"
#include "snii/query/internal/docid_posting_reader.h"
#include "snii/reader/logical_index_reader.h"

namespace snii::query::internal {

// Reads already-resolved docid postings in planned batches, merges them as a
// sorted deduplicated union, then emits one bulk span to the sink.
doris::Status build_docid_union(const snii::reader::LogicalIndexReader& idx,
                         const std::vector<ResolvedDocidPosting>& postings,
                         std::vector<uint32_t>* out);

doris::Status emit_docid_union(const snii::reader::LogicalIndexReader& idx,
                        const std::vector<ResolvedDocidPosting>& postings, DocIdSink* sink);

} // namespace snii::query::internal
