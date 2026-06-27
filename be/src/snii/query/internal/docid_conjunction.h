#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "snii/common/slice.h"
#include "snii/common/status.h"
#include "snii/format/dict_entry.h"
#include "snii/format/frq_prelude.h"
#include "snii/io/batch_range_fetcher.h"
#include "snii/reader/logical_index_reader.h"

namespace snii::query::internal {

struct ResolvedQueryTerm {
    snii::format::DictEntry entry;
    uint64_t frq_base = 0;
    uint64_t prx_base = 0;
};

struct TermPlan {
    snii::format::DictEntry entry;
    uint64_t frq_base = 0;
    uint64_t prx_base = 0;
    uint32_t df = 0;
    size_t order = 0;
    size_t frq_handle = 0;
    size_t prx_handle = 0;
    size_t prelude_handle = 0;
    bool pod_ref = false;
    bool windowed = false;
    snii::format::FrqPreludeReader prelude;
};

struct DocidChunk {
    std::vector<uint32_t> docids;
    std::vector<uint32_t> prx_doc_ordinals;
    uint32_t prx_doc_count = 0;
    bool windowed = false;
    uint32_t window = 0;
};

struct DocidSource {
    std::vector<DocidChunk> chunks;
    bool docids_are_final_candidates = false;
};

Status resolve_query_term(const snii::reader::LogicalIndexReader& idx, const std::string& term,
                          ResolvedQueryTerm* resolved, bool* found);

Status plan_terms(const snii::reader::LogicalIndexReader& idx,
                  const std::vector<std::string>& terms, snii::io::BatchRangeFetcher* fetcher,
                  std::vector<TermPlan>* plans, bool* all_present, bool need_positions);

Status plan_resolved_terms(const snii::reader::LogicalIndexReader& idx,
                           const std::vector<ResolvedQueryTerm>& terms,
                           snii::io::BatchRangeFetcher* fetcher, std::vector<TermPlan>* plans,
                           bool need_positions);

Status open_preludes(const snii::io::BatchRangeFetcher& fetcher, std::vector<TermPlan>* plans,
                     bool need_positions);

Status inline_dd_region(const snii::format::DictEntry& entry, Slice* out);

Status build_docid_only_conjunction(const snii::reader::LogicalIndexReader& idx,
                                    const snii::io::BatchRangeFetcher& round1,
                                    const std::vector<TermPlan>& plans,
                                    std::vector<uint32_t>* candidates);

Status build_docid_only_conjunction(const snii::reader::LogicalIndexReader& idx,
                                    const snii::io::BatchRangeFetcher& round1,
                                    const std::vector<TermPlan>& plans,
                                    std::vector<uint32_t>* candidates,
                                    std::vector<DocidSource>* sources);

} // namespace snii::query::internal
