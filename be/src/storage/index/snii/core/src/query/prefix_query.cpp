#include "snii/query/prefix_query.h"

#include <utility>
#include <vector>

#include "snii/query/internal/docid_posting_reader.h"
#include "snii/query/internal/docid_union.h"

namespace snii::query {

using snii::reader::LogicalIndexReader;

Status prefix_query(const LogicalIndexReader& idx, std::string_view prefix,
                    std::vector<uint32_t>* docids) {
    if (docids == nullptr) return Status::InvalidArgument("prefix_query: null out");
    docids->clear();
    VectorDocIdSink sink(*docids);
    return prefix_query(idx, prefix, &sink);
}

Status prefix_query(const LogicalIndexReader& idx, std::string_view prefix,
                    std::vector<uint32_t>* docids, QueryProfile* profile) {
    QueryProfileScope profile_scope(idx.reader(), profile);
    return prefix_query(idx, prefix, docids);
}

Status prefix_query(const LogicalIndexReader& idx, std::string_view prefix, DocIdSink* sink) {
    if (sink == nullptr) return Status::InvalidArgument("prefix_query: null sink");

    std::vector<LogicalIndexReader::PrefixHit> hits;
    SNII_RETURN_IF_ERROR(idx.prefix_terms(prefix, &hits));

    std::vector<internal::ResolvedDocidPosting> postings;
    postings.reserve(hits.size());
    for (LogicalIndexReader::PrefixHit& hit : hits) {
        postings.push_back({std::move(hit.entry), hit.frq_base, hit.prx_base});
    }
    return internal::emit_docid_union(idx, postings, sink);
}

} // namespace snii::query
