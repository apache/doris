#pragma once

#include <cstdint>
#include <string_view>
#include <vector>

#include "snii/common/status.h"
#include "snii/query/docid_sink.h"
#include "snii/query/query_profile.h"
#include "snii/reader/logical_index_reader.h"

// regexp_query -- MATCH_REGEXP semantics over dictionary terms. The pattern is
// evaluated with std::regex_match, so it must match the whole term. Matching
// terms are executed as a sorted deduplicated docid union.
namespace snii::query {

Status regexp_query(const snii::reader::LogicalIndexReader& idx, std::string_view pattern,
                    std::vector<uint32_t>* const docids, int32_t max_expansions = 0);
Status regexp_query(const snii::reader::LogicalIndexReader& idx, std::string_view pattern,
                    std::vector<uint32_t>* const docids, QueryProfile* profile,
                    int32_t max_expansions = 0);
Status regexp_query(const snii::reader::LogicalIndexReader& idx, std::string_view pattern,
                    DocIdSink* const sink, int32_t max_expansions = 0);

} // namespace snii::query
