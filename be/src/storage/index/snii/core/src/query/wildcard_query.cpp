#include "snii/query/wildcard_query.h"

#include <algorithm>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "snii/query/internal/term_expansion.h"

namespace snii::query {

namespace {

std::string literal_prefix_for_wildcard(std::string_view pattern) {
    std::string out;
    for (char c : pattern) {
        if (c == '*' || c == '?') break;
        out.push_back(c);
    }
    return out;
}

bool wildcard_match(std::string_view pattern, std::string_view text) {
    std::vector<uint8_t> prev(text.size() + 1, 0);
    std::vector<uint8_t> curr(text.size() + 1, 0);
    prev[0] = 1;

    for (char p : pattern) {
        std::fill(curr.begin(), curr.end(), 0);
        if (p == '*') {
            curr[0] = prev[0];
            for (size_t i = 1; i <= text.size(); ++i) {
                curr[i] = prev[i] || curr[i - 1];
            }
        } else {
            for (size_t i = 1; i <= text.size(); ++i) {
                curr[i] = prev[i - 1] && (p == '?' || p == text[i - 1]);
            }
        }
        prev.swap(curr);
    }
    return prev[text.size()] != 0;
}

} // namespace

Status wildcard_query(const snii::reader::LogicalIndexReader& idx, std::string_view pattern,
                      std::vector<uint32_t>* docids) {
    if (docids == nullptr) return Status::InvalidArgument("wildcard_query: null out");
    docids->clear();
    VectorDocIdSink sink(*docids);
    return wildcard_query(idx, pattern, &sink);
}

Status wildcard_query(const snii::reader::LogicalIndexReader& idx, std::string_view pattern,
                      std::vector<uint32_t>* docids, QueryProfile* profile) {
    QueryProfileScope profile_scope(idx.reader(), profile);
    return wildcard_query(idx, pattern, docids);
}

Status wildcard_query(const snii::reader::LogicalIndexReader& idx, std::string_view pattern,
                      DocIdSink* sink) {
    if (sink == nullptr) return Status::InvalidArgument("wildcard_query: null sink");
    const std::string enum_prefix = literal_prefix_for_wildcard(pattern);
    return internal::emit_expanded_docid_union(
            idx, enum_prefix,
            [pattern](std::string_view term) { return wildcard_match(pattern, term); }, sink);
}

} // namespace snii::query
