#pragma once

#include <cstdint>
#include <string_view>
#include <vector>

#include "common/status.h"
#include "snii/query/docid_sink.h"
#include "snii/query/query_profile.h"
#include "snii/reader/logical_index_reader.h"

// term_query -- the simplest SNII query: return the sorted docid set that
// contains term. It runs the term lookup on the logical index, then issues a
// single batched .frq range read (one serial round) to decode the postings.
// Absent term -> empty result (OK status).
namespace snii::query {

doris::Status term_query(const snii::reader::LogicalIndexReader& idx, std::string_view term,
                  std::vector<uint32_t>* docids);
doris::Status term_query(const snii::reader::LogicalIndexReader& idx, std::string_view term,
                  DocIdSink* sink);
doris::Status term_query(const snii::reader::LogicalIndexReader& idx, std::string_view term,
                  std::vector<uint32_t>* docids, QueryProfile* profile);

} // namespace snii::query
