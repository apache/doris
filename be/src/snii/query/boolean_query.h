#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "snii/common/status.h"
#include "snii/query/docid_sink.h"
#include "snii/query/query_profile.h"
#include "snii/reader/logical_index_reader.h"

// boolean_or -- MATCH_ANY semantics: return the sorted docid set containing at
// least one query term. Empty terms or all-absent terms produce an empty
// result. Duplicate input terms are ignored semantically and do not duplicate
// output docids.
namespace snii::query {

Status boolean_or(const snii::reader::LogicalIndexReader& idx,
                  const std::vector<std::string>& terms, std::vector<uint32_t>* docids);
Status boolean_or(const snii::reader::LogicalIndexReader& idx,
                  const std::vector<std::string>& terms, std::vector<uint32_t>* docids,
                  QueryProfile* profile);
Status boolean_or(const snii::reader::LogicalIndexReader& idx,
                  const std::vector<std::string>& terms, DocIdSink* sink);

// boolean_and (MATCH all-terms): sorted docid set of docs containing EVERY
// term, no positional constraint. Valid on docs-only indexes. Empty terms or
// any absent term -> empty result.
Status boolean_and(const snii::reader::LogicalIndexReader& idx,
                   const std::vector<std::string>& terms, std::vector<uint32_t>* docids);
Status boolean_and(const snii::reader::LogicalIndexReader& idx,
                   const std::vector<std::string>& terms, std::vector<uint32_t>* docids,
                   QueryProfile* profile);

} // namespace snii::query
