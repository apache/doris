#include "snii/query/boolean_query.h"

#include <algorithm>
#include <string_view>
#include <utility>
#include <vector>

#include "snii/format/dict_entry.h"
#include "snii/query/docid_sink.h"
#include "snii/query/internal/docid_conjunction.h"
#include "snii/query/internal/docid_posting_reader.h"
#include "snii/query/internal/docid_union.h"

namespace snii::query {

namespace {

std::vector<std::string_view> unique_terms(const std::vector<std::string>& terms) {
    std::vector<std::string_view> out;
    out.reserve(terms.size());
    for (const std::string& term : terms) out.emplace_back(term);
    std::sort(out.begin(), out.end());
    out.erase(std::unique(out.begin(), out.end()), out.end());
    return out;
}

Status resolve_or_postings(const snii::reader::LogicalIndexReader& idx,
                           const std::vector<std::string>& terms,
                           std::vector<internal::ResolvedDocidPosting>* postings) {
    postings->clear();
    for (std::string_view term : unique_terms(terms)) {
        bool found = false;
        snii::format::DictEntry entry;
        uint64_t frq_base = 0;
        uint64_t prx_base = 0;
        SNII_RETURN_IF_ERROR(idx.lookup(term, &found, &entry, &frq_base, &prx_base));
        if (!found) continue;

        postings->push_back({std::move(entry), frq_base, prx_base});
    }
    return Status::OK();
}

} // namespace

Status boolean_or(const snii::reader::LogicalIndexReader& idx,
                  const std::vector<std::string>& terms, std::vector<uint32_t>* docids) {
    if (docids == nullptr) return Status::InvalidArgument("boolean_or: null out");
    docids->clear();
    if (terms.empty()) return Status::OK();

    std::vector<internal::ResolvedDocidPosting> postings;
    SNII_RETURN_IF_ERROR(resolve_or_postings(idx, terms, &postings));
    return internal::build_docid_union(idx, postings, docids);
}

Status boolean_or(const snii::reader::LogicalIndexReader& idx,
                  const std::vector<std::string>& terms, std::vector<uint32_t>* docids,
                  QueryProfile* profile) {
    QueryProfileScope profile_scope(idx.reader(), profile);
    return boolean_or(idx, terms, docids);
}

Status boolean_or(const snii::reader::LogicalIndexReader& idx,
                  const std::vector<std::string>& terms, DocIdSink* sink) {
    if (sink == nullptr) return Status::InvalidArgument("boolean_or: null sink");
    if (terms.empty()) return Status::OK();

    std::vector<internal::ResolvedDocidPosting> postings;
    SNII_RETURN_IF_ERROR(resolve_or_postings(idx, terms, &postings));
    return internal::emit_docid_union(idx, postings, sink);
}

Status boolean_and(const snii::reader::LogicalIndexReader& idx,
                   const std::vector<std::string>& terms, std::vector<uint32_t>* docids) {
    if (docids == nullptr) return Status::InvalidArgument("boolean_and: null out");
    docids->clear();
    if (terms.empty()) return Status::OK();

    snii::io::BatchRangeFetcher round1(idx.reader());
    std::vector<internal::TermPlan> plans;
    bool all_present = false;
    SNII_RETURN_IF_ERROR(internal::plan_terms(idx, terms, &round1, &plans, &all_present,
                                              /*need_positions=*/false));
    if (!all_present) return Status::OK();
    if (round1.pending() > 0) SNII_RETURN_IF_ERROR(round1.fetch());
    SNII_RETURN_IF_ERROR(internal::open_preludes(round1, &plans,
                                                 /*need_positions=*/false));
    return internal::build_docid_only_conjunction(idx, round1, plans, docids);
}

Status boolean_and(const snii::reader::LogicalIndexReader& idx,
                   const std::vector<std::string>& terms, std::vector<uint32_t>* docids,
                   QueryProfile* profile) {
    QueryProfileScope profile_scope(idx.reader(), profile);
    return boolean_and(idx, terms, docids);
}

} // namespace snii::query
