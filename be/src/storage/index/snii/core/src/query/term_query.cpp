#include "snii/query/term_query.h"

#include <vector>

#include "snii/format/dict_entry.h"
#include "snii/query/internal/docid_posting_reader.h"

namespace snii::query {
using doris::Status; // RETURN_IF_ERROR expands to bare Status

using snii::format::DictEntry;
using snii::reader::LogicalIndexReader;

doris::Status term_query(const LogicalIndexReader& idx, std::string_view term,
                  std::vector<uint32_t>* docids) {
    if (docids == nullptr) return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>("term_query: null out");
    docids->clear();
    VectorDocIdSink sink(*docids);
    return term_query(idx, term, &sink);
}

doris::Status term_query(const LogicalIndexReader& idx, std::string_view term, DocIdSink* sink) {
    if (sink == nullptr) return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>("term_query: null sink");

    bool found = false;
    DictEntry entry;
    uint64_t frq_base = 0;
    uint64_t prx_base = 0;
    RETURN_IF_ERROR(idx.lookup(term, &found, &entry, &frq_base, &prx_base));
    if (!found) return doris::Status::OK();
    return internal::read_docid_posting(idx, entry, frq_base, prx_base, sink);
}

doris::Status term_query(const LogicalIndexReader& idx, std::string_view term,
                  std::vector<uint32_t>* docids, QueryProfile* profile) {
    QueryProfileScope profile_scope(idx.reader(), profile);
    return term_query(idx, term, docids);
}

} // namespace snii::query
