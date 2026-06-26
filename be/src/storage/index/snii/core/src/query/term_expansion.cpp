#include "snii/query/internal/term_expansion.h"

#include <utility>
#include <vector>

#include "snii/query/internal/docid_posting_reader.h"
#include "snii/query/internal/docid_union.h"

namespace snii::query::internal {

Status emit_expanded_docid_union(const snii::reader::LogicalIndexReader& idx,
                                 std::string_view enum_prefix, const TermMatcher& matches,
                                 DocIdSink* sink) {
    if (sink == nullptr) return Status::InvalidArgument("term_expansion: null sink");

    std::vector<snii::reader::LogicalIndexReader::PrefixHit> hits;
    SNII_RETURN_IF_ERROR(idx.prefix_terms(enum_prefix, &hits));

    std::vector<ResolvedDocidPosting> postings;
    postings.reserve(hits.size());
    for (snii::reader::LogicalIndexReader::PrefixHit& hit : hits) {
        if (!matches(hit.term)) continue;
        postings.push_back({std::move(hit.entry), hit.frq_base, hit.prx_base});
    }
    return emit_docid_union(idx, postings, sink);
}

} // namespace snii::query::internal
