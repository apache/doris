#pragma once

#include <cstdint>
#include <vector>

#include "snii/common/status.h"
#include "snii/format/dict_entry.h"
#include "snii/query/docid_sink.h"
#include "snii/reader/logical_index_reader.h"

namespace snii::query::internal {

struct ResolvedDocidPosting {
    snii::format::DictEntry entry;
    uint64_t frq_base = 0;
    uint64_t prx_base = 0;
};

// Decodes the docid-only posting for a resolved term. The caller owns term
// lookup and can batch/plan lookups independently; this module owns only the
// three posting encodings (inline, slim pod_ref, windowed pod_ref).
Status read_docid_posting(const snii::reader::LogicalIndexReader& idx,
                          const snii::format::DictEntry& entry, uint64_t frq_base,
                          uint64_t prx_base, std::vector<uint32_t>* docids);

Status read_docid_posting(const snii::reader::LogicalIndexReader& idx,
                          const snii::format::DictEntry& entry, uint64_t frq_base,
                          uint64_t prx_base, snii::query::DocIdSink* sink);

// Batch counterpart for multi-term docid-only operators. Windowed terms share one
// prelude fetch round and one docid fetch round, so OR-style operators pay by
// stage rather than by term.
Status read_docid_postings_batched(const snii::reader::LogicalIndexReader& idx,
                                   const std::vector<ResolvedDocidPosting>& postings,
                                   std::vector<std::vector<uint32_t>>* docids);

} // namespace snii::query::internal
