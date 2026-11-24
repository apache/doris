// Derived from cppjieba's Unicode.hpp (https://github.com/yanyiwu/cppjieba), MIT License,
// Copyright (c) 2013. See dist/licenses/LICENSE-cppjieba.txt.
//
// Only what the dictionary and model loading use. The decoder comes from analysis/common/rune.h,
// which carries jieba's decoding rules; the cuts decode with it too (segmenter.h).

#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "storage/index/inverted/analysis/common/rune.h"

namespace doris::segment_v2::inverted_index::jieba {

using Rune = uint32_t;
// The code points of a dictionary word: the key of the trie.
using Unicode = std::vector<Rune>;

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
