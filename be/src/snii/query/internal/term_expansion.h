#pragma once

#include <functional>
#include <string_view>

#include "snii/common/status.h"
#include "snii/query/docid_sink.h"
#include "snii/reader/logical_index_reader.h"

namespace snii::query::internal {

using TermMatcher = std::function<bool(std::string_view)>;

// Enumerates dictionary terms from `enum_prefix`, filters them with `matches`,
// and emits the sorted docid union for matching entries. PrefixHit carries the
// DictEntry and block bases, so callers avoid a second lookup per expanded term.
Status emit_expanded_docid_union(const snii::reader::LogicalIndexReader& idx,
                                 std::string_view enum_prefix, const TermMatcher& matches,
                                 DocIdSink* sink);

} // namespace snii::query::internal
