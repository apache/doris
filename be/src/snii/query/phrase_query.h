#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "snii/common/status.h"
#include "snii/query/query_profile.h"
#include "snii/reader/logical_index_reader.h"

// phrase_query -- MATCH_PHRASE: return the sorted docid set in which the terms
// occur consecutively (for some i, every term k appears at position pos+k in
// the same doc). It first builds the docid conjunction with docs-only posting
// reads, then fetches PRX only for chunks that can contain final candidates:
//   1. read preludes / docs-only posting ranges and intersect per-term docids;
//   2. fetch retained PRX chunks and stream positions for survivors;
//   3. for each surviving doc, check that some position p exists with
//      term[0]@p, term[1]@p+1, ... term[n-1]@p+(n-1).
// An empty term list -> empty result. Any term absent -> empty result.
namespace snii::query {

Status phrase_query(const snii::reader::LogicalIndexReader& idx,
                    const std::vector<std::string>& terms, std::vector<uint32_t>* docids);
Status phrase_query(const snii::reader::LogicalIndexReader& idx,
                    const std::vector<std::string>& terms, std::vector<uint32_t>* docids,
                    QueryProfile* profile);

// phrase_prefix_query -- MATCH_PHRASE_PREFIX: the last item in `terms` is a
// term prefix and preceding items are exact terms. For example {"quick", "bro"}
// matches "quick brown" and "quick bronze". Empty terms -> empty result.
Status phrase_prefix_query(const snii::reader::LogicalIndexReader& idx,
                           const std::vector<std::string>& terms, std::vector<uint32_t>* docids);
Status phrase_prefix_query(const snii::reader::LogicalIndexReader& idx,
                           const std::vector<std::string>& terms, std::vector<uint32_t>* docids,
                           QueryProfile* profile);

} // namespace snii::query
