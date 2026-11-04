// Derived from cppjieba's MPSegment.hpp, HMMSegment.hpp, MixSegment.hpp, FullSegment.hpp and
// PreFilter.hpp (https://github.com/yanyiwu/cppjieba), MIT License, Copyright (c) 2013. See
// dist/licenses/LICENSE-cppjieba.txt.

#pragma once

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string_view>
#include <vector>

#include "common/logging.h"
#include "storage/index/inverted/analysis/common/rune.h"
#include "storage/index/inverted/analysis/tokenizer/jieba/dict_trie.h"
#include "storage/index/inverted/analysis/tokenizer/jieba/flat_dict.h"
#include "storage/index/inverted/analysis/tokenizer/jieba/hmm_model.h"
#include "storage/index/inverted/analysis/tokenizer/jieba/jieba_scratch.h"

namespace doris::segment_v2::inverted_index::jieba {

// The longest dictionary word a cut looks for, in runes.
inline constexpr uint32_t MAX_WORD_LENGTH = 512;

// One cut of a sentence into words, which are views of it, with the stop words dropped as they
// are, before anything lowercases them. A sentence that does not decode by decode_rune() has no
// word at all.
//
// Both cuts split the sentence into ranges first, as upstream's PreFilter: every separator (space,
// tab, newline, the fullwidth comma U+FF0C and the ideographic full stop U+3002, upstream's
// SPECIAL_SEPARATORS) is a range of its own, and the runes between two separators are one range;
// the full cut also makes each run of ASCII letters and digits a range of its own. "ab，c d" has
// the ranges "ab" "，" "c" " " "d" in both cuts; "中ab" is one range in the mix cut and "中" "ab"
// in the full cut. A separator is always one word, so it is handed out without a cut: the
// maximum-probability cut of one rune is that rune, which the HMM keeps whole too, and the full
// cut hands out the only rune of a range, since no word starts there but the rune alone and none
// before it covers it.
//
// Upstream decoded into runes of 20 bytes, built a Dag of a few hundred bytes for every rune, and
// allocated each step's vectors anew. Here the runes and the steps' arrays are in the scratch, and
// the dictionary walk from a rune feeds the step that reads it directly, in the order the Dag held
// the words: the rune alone, a word or not, then the longer words in increasing end.
class Segmenter {
public:
    Segmenter(const FlatDict& dict, std::string_view sentence, JiebaScratch& scratch,
              std::vector<std::string_view>& words)
            : _dict(dict), _sentence(sentence), _scratch(scratch), _words(words) {}

    // Jieba's default cut, upstream's MixSegment::Cut with the HMM: the maximum-probability cut of
    // each range, with every run of its single runes that are no user word of one rune cut again
    // by the HMM.
    void mix_cut() {
        _words.clear();
        if (!decode()) {
            return;
        }
        const uint32_t* const runes = _scratch.runes.data();
        const auto count = static_cast<uint32_t>(_scratch.runes.size());
        uint32_t begin = 0;
        for (uint32_t cursor = 0; cursor < count; ++cursor) {
            if (is_separator(runes[cursor])) {
                if (begin < cursor) {
                    mix_range(begin, cursor);
                }
                emit(cursor, cursor);
                begin = cursor + 1;
            }
        }
        if (begin < count) {
            mix_range(begin, count);
        }
    }

    // Jieba's full cut, upstream's FullSegment::Cut: every dictionary word of two runes or more in
    // a range, overlapping, and the runes no word covers; a run of ASCII letters and digits is a
    // word, and so is each run of two or more letters or of two or more digits inside it.
    void full_cut() {
        _words.clear();
        if (!decode()) {
            return;
        }
        const uint32_t* const runes = _scratch.runes.data();
        const auto count = static_cast<uint32_t>(_scratch.runes.size());
        uint32_t cursor = 0;
        while (cursor < count) {
            const uint32_t begin = cursor;
            if (is_separator(runes[cursor])) {
                emit(cursor, cursor);
                ++cursor;
            } else if (is_alnum(runes[cursor])) {
                do {
                    ++cursor;
                } while (cursor < count && is_alnum(runes[cursor]));
                alnum_range(begin, cursor);
            } else {
                do {
                    ++cursor;
                } while (cursor < count && !is_separator(runes[cursor]) &&
                         !is_alnum(runes[cursor]));
                full_range(begin, cursor);
            }
        }
    }

private:
    // Decodes the sentence by decode_rune(), the rules of decode_runes(): the lead byte alone
    // sets a rune's length, so a stray continuation byte reads as the lead of a two-byte rune, and
    // "\x80" "7" is U+0037. False when a rune does not decode.
    bool decode() {
        DCHECK(_sentence.size() < std::numeric_limits<uint32_t>::max());
        _scratch.runes.resize(_sentence.size());
        _scratch.offsets.resize(_sentence.size() + 1);
        uint32_t* const runes = _scratch.runes.data();
        uint32_t* const offsets = _scratch.offsets.data();
        uint32_t count = 0;
        for (size_t offset = 0; offset < _sentence.size();) {
            const RuneStrLite decoded =
                    decode_rune(_sentence.data() + offset, _sentence.size() - offset);
            if (decoded.len == 0) {
                return false;
            }
            runes[count] = decoded.rune;
            offsets[count] = static_cast<uint32_t>(offset);
            ++count;
            offset += decoded.len;
        }
        offsets[count] = static_cast<uint32_t>(_sentence.size());
        _scratch.runes.resize(count);
        _scratch.offsets.resize(count + 1);
        return true;
    }

    // MixSegment::Cut over the runes [lo, hi): the maximum-probability cut, whose words of two
    // runes or more, and single runes that are user words of one rune, are words, and whose other
    // single runes are cut again by the HMM, run by run.
    void mix_range(uint32_t lo, uint32_t hi) {
        const uint32_t size = hi - lo;
        max_probability_cut(lo, size);
        const uint32_t* const runes = _scratch.runes.data() + lo;
        const uint32_t* const best_len = _scratch.best_len.data();
        uint32_t i = 0;
        while (i < size) {
            const uint32_t len = best_len[i];
            if (len > 1 || _dict.is_user_single_word(runes[i])) {
                emit(lo + i, lo + i + len - 1);
                i += len;
                continue;
            }
            uint32_t j = i + 1;
            while (j < size && best_len[j] == 1 && !_dict.is_user_single_word(runes[j])) {
                ++j;
            }
            hmm_range(lo + i, lo + j);
            i = j;
        }
    }

    // MPSegment's CalcDP over the `size` runes from `lo`, from the last rune to the first: the best
    // weight of the rest of the range from each rune, and the runes of the word that starts it. A
    // rune that is no word weighs the smallest weight of the dictionary. The weight of a word is
    // added to the best weight after it, in that order, and the strict > keeps the first of equal
    // sums: a tie goes to the shorter word. dp[size] is 0.0, where upstream added nothing: 0.0 +
    // 0.0 + w is 0.0 + w bit for bit, whatever w is.
    void max_probability_cut(uint32_t lo, uint32_t size) {
        _scratch.dp.resize(size + 1);
        _scratch.best_len.resize(size);
        double* const dp = _scratch.dp.data();
        uint32_t* const best_len = _scratch.best_len.data();
        const uint32_t* const runes = _scratch.runes.data() + lo;
        dp[size] = 0.0;
        for (uint32_t i = size; i-- > 0;) {
            uint32_t node = _dict.root_child(runes[i]);
            double val = 0.0;
            val += dp[i + 1];
            val += node != FlatDict::kNoNode && _dict.has_value(node) ? _dict.weight(node)
                                                                      : _dict.min_weight();
            // Upstream started at MIN_DOUBLE and took the rune alone when it was above: a NaN, or
            // a sum of MIN_DOUBLE or less, leaves MIN_DOUBLE and still a cut of the rune alone.
            double best = std::max(MIN_DOUBLE, val);
            uint32_t len = 1;
            const uint32_t end = walk_end(i, size);
            for (uint32_t k = i + 1; node != FlatDict::kNoNode && k < end; ++k) {
                node = _dict.child(node, runes[k]);
                if (node != FlatDict::kNoNode && _dict.has_value(node)) {
                    val = 0.0;
                    val += dp[k + 1];
                    val += _dict.weight(node);
                    if (val > best) {
                        best = val;
                        len = k - i + 1;
                    }
                }
            }
            dp[i] = best;
            best_len[i] = len;
        }
    }

    // HMMSegment::Cut over the runes [lo, hi): an ASCII rune ends the run of other runes before
    // it, which Viterbi cuts, and starts a word of its own: a letter followed by letters and
    // digits, a digit followed by digits and '.', or else the rune alone, none reaching past hi.
    void hmm_range(uint32_t lo, uint32_t hi) {
        const uint32_t* const runes = _scratch.runes.data();
        uint32_t left = lo;
        uint32_t right = lo;
        while (right != hi) {
            if (runes[right] >= 0x80) {
                ++right;
                continue;
            }
            if (left != right) {
                viterbi(left, right);
            }
            left = right;
            right = ascii_word_end(runes, left, hi);
            emit(left, right - 1);
            left = right;
        }
        if (left != right) {
            viterbi(left, right);
        }
    }

    // The end of the word that starts at the ASCII rune `begin`: upstream's sequential letter
    // rule, its numbers rule, or the rune alone.
    static uint32_t ascii_word_end(const uint32_t* runes, uint32_t begin, uint32_t end) {
        const uint32_t first = runes[begin];
        uint32_t cursor = begin + 1;
        if (is_letter(first)) {
            while (cursor != end && (is_letter(runes[cursor]) || is_digit(runes[cursor]))) {
                ++cursor;
            }
        } else if (is_digit(first)) {
            while (cursor != end && (is_digit(runes[cursor]) || runes[cursor] == '.')) {
                ++cursor;
            }
        }
        return cursor;
    }

    // HMMSegment's Viterbi over the runes [lo, hi), which is not empty, and the words it makes: a
    // word ends at every rune tagged E or S, and the last rune is always one of them. The sums run
    // weight + transition + emission in that order; the strict > keeps the first of equal previous
    // statuses, and E where none beats MIN_DOUBLE; the last rune is E when E and S are equal. Only
    // the path is kept for every rune: the next rune needs the weights of this one alone.
    void viterbi(uint32_t lo, uint32_t hi) {
        constexpr size_t kStatuses = HMMModel::STATUS_SUM;
        const uint32_t size = hi - lo;
        _scratch.path.resize(size_t {size} * kStatuses);
        _scratch.status.resize(size);
        int8_t* const path = _scratch.path.data();
        uint8_t* const status = _scratch.status.data();
        const uint32_t* const runes = _scratch.runes.data() + lo;
        const auto& start_prob = _dict.start_prob();
        const auto& trans_prob = _dict.trans_prob();

        std::array<double, kStatuses> weight;
        const double* emit_prob = _dict.emit_prob(runes[0]);
        for (size_t y = 0; y < kStatuses; ++y) {
            weight[y] = start_prob[y] + emit_prob[y];
            path[y] = -1;
        }
        for (uint32_t x = 1; x < size; ++x) {
            emit_prob = _dict.emit_prob(runes[x]);
            std::array<double, kStatuses> next;
            for (size_t y = 0; y < kStatuses; ++y) {
                double best = MIN_DOUBLE;
                auto from = static_cast<int8_t>(HMMModel::E);
                for (size_t pre_y = 0; pre_y < kStatuses; ++pre_y) {
                    const double tmp = weight[pre_y] + trans_prob[pre_y][y] + emit_prob[y];
                    if (tmp > best) {
                        best = tmp;
                        from = static_cast<int8_t>(pre_y);
                    }
                }
                next[y] = best;
                path[size_t {x} * kStatuses + y] = from;
            }
            weight = next;
        }

        int stat = weight[HMMModel::E] >= weight[HMMModel::S] ? HMMModel::E : HMMModel::S;
        // The path of the first rune is -1, which `stat` takes after its last use.
        for (uint32_t x = size; x-- > 0;) {
            status[x] = static_cast<uint8_t>(stat);
            stat = path[size_t {x} * kStatuses + static_cast<size_t>(stat)];
        }
        uint32_t left = lo;
        for (uint32_t x = 0; x < size; ++x) {
            // E and S are odd.
            if (status[x] % 2 != 0) {
                emit(left, lo + x);
                left = lo + x + 1;
            }
        }
    }

    // FullSegment::Cut over the runes [lo, hi). Every word of two runes or more is handed out. A
    // rune that starts no longer word is handed out, a word or not, when max_idx, the end of the
    // words before it as far as word_len tells, does not reach past it.
    //
    // word_len keeps its value from one word to the next and from one rune to the next: a rune
    // that is no word does not set it, yet still raises max_idx to u_idx + the length of the last
    // word seen. This looks like a bug, but it decides which single runes are handed out, so it is
    // kept as upstream has it. Say A, B, C and D are CJK runes, and AB is the only word of the
    // dictionary that starts with any of them. The runes ABCD give only AB: B is skipped because AB
    // covers it (max_idx 2 > u_idx 1), and the stale 2 then raises max_idx to 3; C meets 3 > 2, is
    // skipped and raises it to 4, and D meets 4 > 3. With a word_len of 1 for a rune that is no
    // word, max_idx would stay 2 after B, and C and D would both be handed out.
    //
    // Upstream had the words of a rune in its Dag before it looked at the rune alone, and handed
    // the rune out only when no longer word started there. The walk here takes the rune alone at
    // the first longer word, where it hands nothing out, or after the walk when there is none;
    // word_len and max_idx change in upstream's order either way.
    void full_range(uint32_t lo, uint32_t hi) {
        const uint32_t size = hi - lo;
        const uint32_t* const runes = _scratch.runes.data() + lo;
        size_t max_idx = 0;
        size_t word_len = 0;
        for (uint32_t u_idx = 0; u_idx < size; ++u_idx) {
            uint32_t node = _dict.root_child(runes[u_idx]);
            const bool alone_is_word = node != FlatDict::kNoNode && _dict.has_value(node);
            bool has_longer = false;
            const uint32_t end = walk_end(u_idx, size);
            for (uint32_t k = u_idx + 1; node != FlatDict::kNoNode && k < end; ++k) {
                node = _dict.child(node, runes[k]);
                if (node == FlatDict::kNoNode || !_dict.has_value(node)) {
                    continue;
                }
                if (!has_longer) {
                    has_longer = true;
                    if (alone_is_word) {
                        word_len = 1;
                    }
                    max_idx = std::max(max_idx, u_idx + word_len);
                }
                word_len = size_t {k} - u_idx + 1;
                emit(lo + u_idx, lo + k);
                max_idx = std::max(max_idx, u_idx + word_len);
            }
            if (!has_longer) {
                if (max_idx <= u_idx) {
                    emit(lo + u_idx, lo + u_idx);
                }
                if (alone_is_word) {
                    word_len = 1;
                }
                max_idx = std::max(max_idx, u_idx + word_len);
            }
        }
    }

    // FullSegment::CutAlnum over a run of ASCII letters and digits [lo, hi): the whole run, then
    // the runs of two or more digits or of two or more letters in it, unless one is the whole run:
    // "abc123" gives "abc123", "abc" and "123", and "abc" only "abc".
    void alnum_range(uint32_t lo, uint32_t hi) {
        emit(lo, hi - 1);
        const uint32_t* const runes = _scratch.runes.data();
        uint32_t cursor = lo;
        while (cursor != hi) {
            const bool digits = is_digit(runes[cursor]);
            DCHECK(digits || is_letter(runes[cursor]));
            const uint32_t start = cursor;
            do {
                ++cursor;
            } while (cursor != hi && (digits ? is_digit(runes[cursor]) : is_letter(runes[cursor])));
            if (cursor - start > 1 && (start != lo || cursor != hi)) {
                emit(start, cursor - 1);
            }
        }
    }

    // Where the dictionary walk from the rune `begin` of a range of `size` runes stops: a word of
    // more than MAX_WORD_LENGTH runes is never found, as upstream.
    static uint32_t walk_end(uint32_t begin, uint32_t size) {
        return static_cast<uint32_t>(std::min<size_t>(size, size_t {begin} + MAX_WORD_LENGTH));
    }

    // Hands out the runes [left, right] as a word, unless it is a stop word.
    void emit(uint32_t left, uint32_t right) {
        const uint32_t begin = _scratch.offsets[left];
        const std::string_view word(_sentence.data() + begin, _scratch.offsets[right + 1] - begin);
        if (!_dict.is_stop_word(word)) {
            _words.push_back(word);
        }
    }

    static bool is_separator(uint32_t rune) {
        return rune == ' ' || rune == '\t' || rune == '\n' || rune == 0xFF0C || rune == 0x3002;
    }

    static bool is_digit(uint32_t rune) { return rune >= '0' && rune <= '9'; }

    static bool is_letter(uint32_t rune) {
        return (rune >= 'A' && rune <= 'Z') || (rune >= 'a' && rune <= 'z');
    }

    static bool is_alnum(uint32_t rune) { return is_digit(rune) || is_letter(rune); }

    const FlatDict& _dict;
    const std::string_view _sentence;
    JiebaScratch& _scratch;
    std::vector<std::string_view>& _words;
};

} // namespace doris::segment_v2::inverted_index::jieba
