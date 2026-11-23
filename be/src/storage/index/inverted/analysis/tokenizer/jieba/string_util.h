// Derived from limonp's StringUtil.hpp (https://github.com/yanyiwu/limonp), MIT License,
// Copyright (c) 2013. See dist/licenses/LICENSE-limonp.txt.
//
// Only Split and Trim, which jieba's dictionary and model loading use.

#pragma once

#include <algorithm>
#include <cctype>
#include <cstddef>
#include <string>
#include <vector>

namespace doris::segment_v2::inverted_index::jieba {

// Splits `src` at every byte that is one of `pattern`. Two separators in a row give the empty piece
// between them and a separator at the start an empty first piece, but a separator at the end gives
// no piece after it: "a  b " split at " " is {"a", "", "b"}, and "" is no piece at all. Upstream's
// maxsplit parameter is gone: no caller passed one, and with its default of npos it never stopped
// a split.
inline void split(const std::string& src, std::vector<std::string>& res,
                  const std::string& pattern) {
    res.clear();
    size_t start = 0;
    size_t end = 0;
    while (start < src.size()) {
        end = src.find_first_of(pattern, start);
        if (std::string::npos == end) {
            res.push_back(src.substr(start));
            return;
        }
        res.push_back(src.substr(start, end - start));
        start = end + 1;
    }
}

inline std::vector<std::string> split(const std::string& src, const std::string& pattern) {
    std::vector<std::string> res;
    split(src, res, pattern);
    return res;
}

// Whether `c` is a space by std::isspace, so by the C locale, which the BE never changes. A char
// is sign-extended on the way in, so where char is signed a byte of 0x80 and above is above 0xFF
// here, and no space.
inline bool is_space(unsigned c) {
    // when passing large int as the argument of isspace, it core dump, so here need a type cast.
    return c > 0xff ? false : std::isspace(static_cast<int>(c & 0xff)) != 0;
}

inline std::string& ltrim(std::string& s) {
    s.erase(s.begin(), std::ranges::find_if(s, [](const char& c) {
                return !is_space(static_cast<unsigned>(c));
            }));
    return s;
}

inline std::string& rtrim(std::string& s) {
    s.erase(std::ranges::find_if(s.rbegin(), s.rend(),
                                 [](const char& c) { return !is_space(static_cast<unsigned>(c)); })
                    .base(),
            s.end());
    return s;
}

inline std::string& trim(std::string& s) {
    return ltrim(rtrim(s));
}

} // namespace doris::segment_v2::inverted_index::jieba
