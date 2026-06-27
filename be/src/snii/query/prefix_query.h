#pragma once

#include <cstdint>
#include <string_view>
#include <vector>

#include "snii/common/status.h"
#include "snii/query/docid_sink.h"
#include "snii/query/query_profile.h"
#include "snii/reader/logical_index_reader.h"

// prefix_query -- MATCH_PREFIX semantics: enumerate dictionary terms with the
// requested prefix, then return the sorted docid set containing any enumerated
// term. Empty prefix enumerates all terms. No matching terms -> empty result.
namespace snii::query {

Status prefix_query(const snii::reader::LogicalIndexReader& idx, std::string_view prefix,
                    std::vector<uint32_t>* const docids, int32_t max_expansions = 0);
Status prefix_query(const snii::reader::LogicalIndexReader& idx, std::string_view prefix,
                    std::vector<uint32_t>* const docids, QueryProfile* profile,
                    int32_t max_expansions = 0);
Status prefix_query(const snii::reader::LogicalIndexReader& idx, std::string_view prefix,
                    DocIdSink* const sink, int32_t max_expansions = 0);

} // namespace snii::query
