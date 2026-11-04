// Derived from cppjieba's FullSegment.hpp (https://github.com/yanyiwu/cppjieba), MIT License,
// Copyright (c) 2013. See dist/licenses/LICENSE-cppjieba.txt.

#pragma once

#include <cstddef>
#include <cstdint>
#include <fstream>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

#include "common/check.h"
#include "common/logging.h"
#include "storage/index/inverted/analysis/tokenizer/jieba/dict_trie.h"
#include "storage/index/inverted/analysis/tokenizer/jieba/pre_filter.h"
#include "storage/index/inverted/analysis/tokenizer/jieba/segment_base.h"
#include "storage/index/inverted/analysis/tokenizer/jieba/trie.h"
#include "storage/index/inverted/analysis/tokenizer/jieba/unicode.h"

namespace doris::segment_v2::inverted_index::jieba {

// Jieba's full cut: every dictionary word of two runes or more in a range between separators,
// overlapping, and the runes no word covers; a run of ASCII letters and digits is a word, and so
// is each run of two or more letters or of two or more digits inside it. The separators
// themselves are words. Words that are stop words are dropped, as they are, before anything
// lowercases them.
class FullSegment : public SegmentBase {
public:
    explicit FullSegment(const DictTrie* dict_trie, const std::string& stop_word_path = "")
            : _dict_trie(dict_trie) {
        DORIS_CHECK(_dict_trie != nullptr);
        load_stop_word_dict(stop_word_path);
    }
    // _stop_words views the strings of _stop_word_list.
    FullSegment(const FullSegment&) = delete;
    FullSegment& operator=(const FullSegment&) = delete;

    // The words of `sentence` are views of it.
    void cut(const std::string& sentence, std::vector<std::string_view>& words) const {
        PreFilter pre_filter(_symbols, sentence, true);
        std::vector<WordRange> wrs;
        wrs.reserve(sentence.size() / 2);
        while (pre_filter.has_next()) {
            const PreFilter::Range range = pre_filter.next();
            if (range.type == PreFilter::Language::CHINESE) {
                cut(range.begin, range.end, wrs);
            } else {
                cut_alnum(range.begin, range.end, wrs);
            }
        }
        words.clear();
        words.reserve(wrs.size());
        for (const WordRange& wr : wrs) {
            uint32_t len = wr.right->offset - wr.left->offset + wr.right->len;
            std::string_view word(sentence.data() + wr.left->offset, len);
            if (_stop_words.contains(word)) {
                continue;
            }
            words.emplace_back(word);
        }
    }

private:
    // Every word of two runes or more is handed out. A rune whose Dag has no next but the rune
    // itself is handed out, word or not, when max_idx, the end of the words before it as far as
    // word_len tells, does not reach past it.
    //
    // word_len keeps its value from one next to the other and from one rune to the next: a next
    // with no unit does not set it, yet still raises max_idx to u_idx + the length of the last unit
    // seen. This looks like a bug, but it decides which single runes are handed out, so it is kept
    // as upstream has it. Say A, B, C and D are CJK runes, and AB is the only word of the
    // dictionary that starts with any of them. The runes ABCD give only AB: B is skipped because
    // AB covers it (max_idx 2 > u_idx 1), and the stale 2 then raises max_idx to 3; C meets 3 > 2,
    // is skipped and raises it to 4, and D meets 4 > 3. With a word_len of 1 for a next with no
    // unit, max_idx would stay 2 after B, and C and D would both be handed out.
    void cut(RuneStrArray::const_iterator begin, RuneStrArray::const_iterator end,
             std::vector<WordRange>& res) const {
        // max index of res's words
        size_t max_idx = 0;

        // always equals to (uItr - begin)
        size_t u_idx = 0;

        // tmp variables
        size_t word_len = 0;
        DCHECK(_dict_trie != nullptr);
        std::vector<Dag> dags;
        _dict_trie->find(begin, end, dags);
        for (size_t i = 0; i < dags.size(); i++) {
            for (const auto& [nextoffset, du] : dags[i].nexts) {
                DCHECK(nextoffset < dags.size());
                if (du == nullptr) {
                    if (dags[i].nexts.size() == 1 && max_idx <= u_idx) {
                        res.emplace_back(begin + i, begin + nextoffset);
                    }
                } else {
                    word_len = du->word.size();
                    if (word_len >= 2 || (dags[i].nexts.size() == 1 && max_idx <= u_idx)) {
                        res.emplace_back(begin + i, begin + nextoffset);
                    }
                }
                max_idx = u_idx + word_len > max_idx ? u_idx + word_len : max_idx;
            }
            u_idx++;
        }
    }

    // The whole range, then the runs of two or more digits or letters in it, unless one is the
    // whole range: "abc123" gives "abc123", "abc" and "123", and "abc" only "abc".
    void cut_alnum(RuneStrArray::const_iterator begin, RuneStrArray::const_iterator end,
                   std::vector<WordRange>& res) const {
        res.emplace_back(begin, end - 1);

        auto cursor = begin;
        while (cursor != end) {
            if (PreFilter::is_number(cursor->rune)) {
                find_range(PreFilter::is_number, cursor, begin, end, res);
                continue;
            }
            if (PreFilter::is_letter(cursor->rune)) {
                find_range(PreFilter::is_letter, cursor, begin, end, res);
                continue;
            }
            cursor++;
        }
    }

    template <typename Predicate>
    void find_range(Predicate pred, RuneStrArray::const_iterator& cursor,
                    RuneStrArray::const_iterator begin, RuneStrArray::const_iterator end,
                    std::vector<WordRange>& res) const {
        auto wr_begin = cursor;
        while (cursor != end && pred(cursor->rune)) {
            cursor++;
        }
        auto wr_end = cursor;
        if (wr_begin == begin && wr_end == end) {
            return;
        }
        if (wr_end - wr_begin <= 1) {
            return;
        }
        res.emplace_back(wr_begin, wr_end - 1);
    }

    // A stop word file that cannot be opened leaves no stop word. Every line is a stop word as it
    // is, untrimmed; an empty line is one too, but no word is empty.
    void load_stop_word_dict(const std::string& file_path) {
        std::ifstream ifs(file_path);
        if (ifs.is_open()) {
            std::string line;
            while (std::getline(ifs, line)) {
                _stop_word_list.push_back(line);
            }
            for (const std::string& word : _stop_word_list) {
                _stop_words.insert(std::string_view(word.data(), word.size()));
            }
        }
    }

    const DictTrie* _dict_trie;

    std::vector<std::string> _stop_word_list;
    std::unordered_set<std::string_view> _stop_words;
};

} // namespace doris::segment_v2::inverted_index::jieba
