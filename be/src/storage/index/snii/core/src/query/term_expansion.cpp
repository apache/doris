#include "snii/query/internal/term_expansion.h"

#include <utility>
#include <vector>

#include "snii/format/phrase_bigram.h"
#include "snii/query/internal/docid_posting_reader.h"
#include "snii/query/internal/docid_union.h"

namespace snii::query::internal {
using doris::Status; // RETURN_IF_ERROR expands to bare Status

doris::Status emit_expanded_docid_union(const snii::reader::LogicalIndexReader& idx,
                                 std::string_view enum_prefix, const TermMatcher& matches,
                                 DocIdSink* const sink, int32_t max_expansions) {
    if (sink == nullptr) {
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>("term_expansion: null sink");
    }

    std::vector<ResolvedDocidPosting> postings;
    int32_t count = 0;
    RETURN_IF_ERROR(idx.visit_prefix_terms(
            enum_prefix, [&](snii::reader::LogicalIndexReader::PrefixHit&& hit, bool* stop) {
                if (snii::format::is_phrase_bigram_term(hit.term)) {
                    return doris::Status::OK();
                }
                if (!matches(hit.term)) {
                    return doris::Status::OK();
                }
                postings.push_back({std::move(hit.entry), hit.frq_base, hit.prx_base});
                ++count;
                *stop = max_expansions > 0 && count >= max_expansions;
                return doris::Status::OK();
            }));
    return emit_docid_union(idx, postings, sink);
}

} // namespace snii::query::internal
