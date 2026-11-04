// Derived from cppjieba's Unicode.hpp (https://github.com/yanyiwu/cppjieba), MIT License,
// Copyright (c) 2013. See dist/licenses/LICENSE-cppjieba.txt.
//
// Only what the dictionary loading and the two cut paths use. RuneStr, RuneStrArray and the
// decoder come from analysis/common/rune.h, which carries jieba's decoding rules; RuneStrArray is
// a std::vector there instead of upstream's LocalVector.

#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "storage/index/inverted/analysis/common/rune.h"

namespace doris::segment_v2::inverted_index::jieba {

using Rune = uint32_t;
// The code points of a dictionary word: the key of the trie.
using Unicode = std::vector<Rune>;

// [left, right]: the runes of a word in a decoded sentence, both ends included.
struct WordRange {
    RuneStrArray::const_iterator left;
    RuneStrArray::const_iterator right;
    WordRange(RuneStrArray::const_iterator l, RuneStrArray::const_iterator r) : left(l), right(r) {}
};

// The code points of `s`, decoded with decode_runes(). Returns false with no code point when one of
// them does not decode. Upstream's DecodeRunesInString(const string&, Unicode&).
inline bool decode_unicode(const std::string& s, Unicode& unicode) {
    unicode.clear();
    RuneStrArray runes;
    if (!decode_runes(s.data(), s.size(), runes)) {
        return false;
    }
    unicode.reserve(runes.size());
    for (const RuneStr& rune : runes) {
        unicode.push_back(rune.rune);
    }
    return true;
}

} // namespace doris::segment_v2::inverted_index::jieba
