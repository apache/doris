#include "snii/query/regexp_query.h"

#include <regex>
#include <string>
#include <string_view>
#include <vector>

#include "snii/query/internal/term_expansion.h"

namespace snii::query {

namespace {

bool is_regex_metachar(char c) {
    switch (c) {
    case '.':
    case '^':
    case '$':
    case '|':
    case '(':
    case ')':
    case '[':
    case ']':
    case '*':
    case '+':
    case '?':
    case '{':
    case '}':
    case '\\':
        return true;
    default:
        return false;
    }
}

std::string literal_prefix_for_regex(std::string_view pattern) {
    std::string out;
    size_t i = 0;
    if (!pattern.empty() && pattern.front() == '^') i = 1;
    for (; i < pattern.size(); ++i) {
        const char c = pattern[i];
        if (is_regex_metachar(c)) break;
        out.push_back(c);
    }
    return out;
}

} // namespace

Status regexp_query(const snii::reader::LogicalIndexReader& idx, std::string_view pattern,
                    std::vector<uint32_t>* docids) {
    if (docids == nullptr) return Status::InvalidArgument("regexp_query: null out");
    docids->clear();
    VectorDocIdSink sink(*docids);
    return regexp_query(idx, pattern, &sink);
}

Status regexp_query(const snii::reader::LogicalIndexReader& idx, std::string_view pattern,
                    std::vector<uint32_t>* docids, QueryProfile* profile) {
    QueryProfileScope profile_scope(idx.reader(), profile);
    return regexp_query(idx, pattern, docids);
}

Status regexp_query(const snii::reader::LogicalIndexReader& idx, std::string_view pattern,
                    DocIdSink* sink) {
    if (sink == nullptr) return Status::InvalidArgument("regexp_query: null sink");

    std::regex re;
    try {
        re = std::regex(std::string(pattern));
    } catch (const std::regex_error& e) {
        return Status::InvalidArgument(std::string("regexp_query: invalid regex: ") + e.what());
    }

    const std::string enum_prefix = literal_prefix_for_regex(pattern);
    return internal::emit_expanded_docid_union(
            idx, enum_prefix,
            [&re](std::string_view term) { return std::regex_match(term.begin(), term.end(), re); },
            sink);
}

} // namespace snii::query
