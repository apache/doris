// Derived from cppjieba's PreFilter.hpp (https://github.com/yanyiwu/cppjieba), MIT License,
// Copyright (c) 2013. See dist/licenses/LICENSE-cppjieba.txt.

#pragma once

#include <cstdint>
#include <string>
#include <unordered_set>

#include "storage/index/inverted/analysis/tokenizer/jieba/unicode.h"

namespace doris::segment_v2::inverted_index::jieba {

// Decodes a sentence and hands out its ranges one after the other: a separator is a range of its
// own, and the runes between two separators are one range. With `alnum`, a run of ASCII letters and
// digits is a range of its own too, typed EN_NUMBER. For "ab，c d" (with the separators of
// SegmentBase) the ranges are "ab" "，" "c" " " "d", and with `alnum` the same, but "ab", "c" and
// "d" are EN_NUMBER; "中ab" is one range without `alnum` and "中" "ab" with it.
class PreFilter {
public:
    // Upstream's INVALID, which nothing set, is gone.
    enum class Language {
        CHINESE,
        EN_NUMBER,
    };

    //TODO use WordRange instead of Range
    struct Range {
        Language type = Language::CHINESE;
        RuneStrArray::const_iterator begin;
        RuneStrArray::const_iterator end;
    };

    // Upstream's two constructors, the second of which set isAlnum.
    PreFilter(const std::unordered_set<Rune>& symbols, const std::string& sentence,
              bool alnum = false)
            : _is_alnum(alnum), _symbols(symbols) {
        // A sentence that does not decode leaves no rune, so it has no range and no word.
        decode_runes(sentence.data(), sentence.size(), _sentence);
        _cursor = _sentence.begin();
    }

    bool has_next() const { return _cursor != _sentence.end(); }

    Range next() {
        Range range;
        range.begin = _cursor;
        while (_cursor != _sentence.end()) {
            if (_symbols.contains(_cursor->rune)) {
                if (range.begin == _cursor) {
                    _cursor++;
                }
                range.end = _cursor;
                return range;
            }
            if (_is_alnum && is_alnum(_cursor->rune)) {
                return find_range(is_alnum, range);
            }
            _cursor++;
        }
        range.end = _sentence.end();
        return range;
    }

    static bool is_alnum(uint32_t codepoint) {
        return is_number(codepoint) || is_letter(codepoint);
    }

    static bool is_number(uint32_t codepoint) { return codepoint >= 0x0030 && codepoint <= 0x0039; }

    static bool is_letter(uint32_t codepoint) {
        return (codepoint >= 0x0041 && codepoint <= 0x005A) ||
               (codepoint >= 0x0061 && codepoint <= 0x007A);
    }

    // When the range does not start with a rune of `pred`, it ends at the cursor, which stopped at
    // the first rune of `pred`; otherwise it is the run of `pred` from its start.
    template <typename Predicate>
    Range find_range(Predicate pred, Range range) {
        if (!pred(range.begin->rune)) {
            range.end = _cursor;
            return range;
        }
        while (_cursor != _sentence.end() && pred(_cursor->rune)) {
            _cursor++;
        }
        range.end = _cursor;
        range.type = Language::EN_NUMBER;
        return range;
    }

private:
    bool _is_alnum = false;
    RuneStrArray::const_iterator _cursor;
    RuneStrArray _sentence;
    const std::unordered_set<Rune>& _symbols;
};

} // namespace doris::segment_v2::inverted_index::jieba
