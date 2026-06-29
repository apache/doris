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
    if (!pattern.empty() && pattern.front() == '^') {
        i = 1;
    }
    for (; i < pattern.size(); ++i) {
        const char c = pattern[i];
        if (is_regex_metachar(c)) {
            break;
        }
        out.push_back(c);
    }
    return out;
}

} // namespace

doris::Status regexp_query(const snii::reader::LogicalIndexReader& idx, std::string_view pattern,
                    std::vector<uint32_t>* const docids, int32_t max_expansions) {
    if (docids == nullptr) {
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>("regexp_query: null out");
    }
    docids->clear();
    VectorDocIdSink sink(*docids);
    return regexp_query(idx, pattern, &sink, max_expansions);
}

doris::Status regexp_query(const snii::reader::LogicalIndexReader& idx, std::string_view pattern,
                    std::vector<uint32_t>* const docids, QueryProfile* profile,
                    int32_t max_expansions) {
    QueryProfileScope profile_scope(idx.reader(), profile);
    return regexp_query(idx, pattern, docids, max_expansions);
}

doris::Status regexp_query(const snii::reader::LogicalIndexReader& idx, std::string_view pattern,
                    DocIdSink* const sink, int32_t max_expansions) {
    if (sink == nullptr) {
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>("regexp_query: null sink");
    }

    std::regex re;
    try {
        re = std::regex(std::string(pattern));
    } catch (const std::regex_error& e) {
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>(std::string("regexp_query: invalid regex: ") + e.what());
    }

    const std::string enum_prefix = literal_prefix_for_regex(pattern);
    return internal::emit_expanded_docid_union(
            idx, enum_prefix,
            [&re](std::string_view term) { return std::regex_match(term.begin(), term.end(), re); },
            sink, max_expansions);
}

} // namespace snii::query
