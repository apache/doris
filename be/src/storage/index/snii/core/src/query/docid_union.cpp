#include "snii/query/internal/docid_union.h"

#include <vector>

#include "snii/query/internal/docid_set_ops.h"

namespace snii::query::internal {
using doris::Status; // RETURN_IF_ERROR expands to bare Status

doris::Status build_docid_union(const snii::reader::LogicalIndexReader& idx,
                         const std::vector<ResolvedDocidPosting>& postings,
                         std::vector<uint32_t>* out) {
    if (out == nullptr) return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>("docid_union: null out");
    out->clear();
    if (postings.empty()) return doris::Status::OK();

    std::vector<std::vector<uint32_t>> docs_by_posting;
    RETURN_IF_ERROR(read_docid_postings_batched(idx, postings, &docs_by_posting));
    *out = union_sorted_many(docs_by_posting);
    return doris::Status::OK();
}

doris::Status emit_docid_union(const snii::reader::LogicalIndexReader& idx,
                        const std::vector<ResolvedDocidPosting>& postings, DocIdSink* sink) {
    if (sink == nullptr) return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>("docid_union: null sink");
    std::vector<uint32_t> acc;
    RETURN_IF_ERROR(build_docid_union(idx, postings, &acc));
    if (acc.empty()) return doris::Status::OK();
    return sink->append_sorted(acc);
}

} // namespace snii::query::internal
