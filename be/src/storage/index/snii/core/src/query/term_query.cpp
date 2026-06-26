#include "snii/query/term_query.h"

#include <vector>

#include "snii/format/dict_entry.h"
#include "snii/query/internal/docid_posting_reader.h"

namespace snii::query {

using snii::format::DictEntry;
using snii::reader::LogicalIndexReader;

Status term_query(const LogicalIndexReader& idx, std::string_view term,
                  std::vector<uint32_t>* docids) {
    if (docids == nullptr) return Status::InvalidArgument("term_query: null out");
    docids->clear();

    bool found = false;
    DictEntry entry;
    uint64_t frq_base = 0;
    uint64_t prx_base = 0;
    SNII_RETURN_IF_ERROR(idx.lookup(term, &found, &entry, &frq_base, &prx_base));
    if (!found) return Status::OK();
    return internal::read_docid_posting(idx, entry, frq_base, prx_base, docids);
}

Status term_query(const LogicalIndexReader& idx, std::string_view term,
                  std::vector<uint32_t>* docids, QueryProfile* profile) {
    QueryProfileScope profile_scope(idx.reader(), profile);
    return term_query(idx, term, docids);
}

} // namespace snii::query
