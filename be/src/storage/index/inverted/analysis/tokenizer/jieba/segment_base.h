// Derived from cppjieba's SegmentBase.hpp (https://github.com/yanyiwu/cppjieba), MIT License,
// Copyright (c) 2013. See dist/licenses/LICENSE-cppjieba.txt.

#pragma once

#include <algorithm>
#include <string>
#include <unordered_set>

#include "common/check.h"
#include "storage/index/inverted/analysis/tokenizer/jieba/unicode.h"

namespace doris::segment_v2::inverted_index::jieba {

// Space, tab, newline, the fullwidth comma U+FF0C and the ideographic full stop U+3002.
inline constexpr const char* SPECIAL_SEPARATORS = " \t\n\xEF\xBC\x8C\xE3\x80\x82";

// The separators the PreFilter of a segment splits a sentence at. Upstream's pure virtual Cut of
// vector<string> and the virtual destructor are gone with that overload; nothing deletes a segment
// through this base.
class SegmentBase {
public:
    // Upstream failed the construction when this failed, which it cannot for the constant
    // separators.
    SegmentBase() { DORIS_CHECK(reset_separators(SPECIAL_SEPARATORS)); }

protected:
    std::unordered_set<Rune> _symbols;

private:
    // Fails when `s` does not decode or names a separator twice; all_of stops at the first
    // separator that is there already, where upstream's loop returned false.
    bool reset_separators(const std::string& s) {
        _symbols.clear();
        RuneStrArray runes;
        if (!decode_runes(s.data(), s.size(), runes)) {
            return false;
        }
        return std::ranges::all_of(
                runes, [this](const RuneStr& rune) { return _symbols.insert(rune.rune).second; });
    }
};

} // namespace doris::segment_v2::inverted_index::jieba
