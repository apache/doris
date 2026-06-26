#pragma once

#include <cstdint>
#include <string_view>
#include <vector>

#include "snii/common/status.h"
#include "snii/query/docid_sink.h"
#include "snii/query/query_profile.h"
#include "snii/reader/logical_index_reader.h"

// wildcard_query -- MATCH_WILDCARD semantics over dictionary terms. `*` matches
// any byte sequence, `?` matches one byte, and all other bytes match literally.
// Matching terms are executed as a sorted deduplicated docid union.
namespace snii::query {

Status wildcard_query(const snii::reader::LogicalIndexReader& idx, std::string_view pattern,
                      std::vector<uint32_t>* docids);
Status wildcard_query(const snii::reader::LogicalIndexReader& idx, std::string_view pattern,
                      std::vector<uint32_t>* docids, QueryProfile* profile);
Status wildcard_query(const snii::reader::LogicalIndexReader& idx, std::string_view pattern,
                      DocIdSink* sink);

} // namespace snii::query
