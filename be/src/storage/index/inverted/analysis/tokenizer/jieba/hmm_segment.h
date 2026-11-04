// Derived from cppjieba's HMMSegment.hpp (https://github.com/yanyiwu/cppjieba), MIT License,
// Copyright (c) 2013. See dist/licenses/LICENSE-cppjieba.txt.

#pragma once

#include <cstddef>
#include <vector>

#include "storage/index/inverted/analysis/tokenizer/jieba/dict_trie.h"
#include "storage/index/inverted/analysis/tokenizer/jieba/hmm_model.h"
#include "storage/index/inverted/analysis/tokenizer/jieba/unicode.h"

namespace doris::segment_v2::inverted_index::jieba {

// Segmentation of a range by the HMM. An ASCII rune ends the run of non-ASCII runes before it,
// which Viterbi cuts, and starts a word of its own: a letter followed by letters and digits, a
// digit followed by digits and '.', or else the rune alone. Only the range cut MixSegment calls is
// kept; it does not use the separators, so this is no SegmentBase any more. MIN_DOUBLE comes from
// dict_trie.h, as upstream's did.
class HMMSegment {
public:
    explicit HMMSegment(const HMMModel* model) : _model(model) {}

    void cut(RuneStrArray::const_iterator begin, RuneStrArray::const_iterator end,
             std::vector<WordRange>& res) const {
        auto left = begin;
        auto right = begin;
        while (right != end) {
            if (right->rune < 0x80) {
                if (left != right) {
                    internal_cut(left, right, res);
                }
                left = right;
                do {
                    right = sequential_letter_rule(left, end);
                    if (right != left) {
                        break;
                    }
                    right = numbers_rule(left, end);
                    if (right != left) {
                        break;
                    }
                    right++;
                } while (false);
                res.emplace_back(left, right - 1);
                left = right;
            } else {
                right++;
            }
        }
        if (left != right) {
            internal_cut(left, right, res);
        }
    }

private:
    // sequential letters rule
    RuneStrArray::const_iterator sequential_letter_rule(RuneStrArray::const_iterator begin,
                                                        RuneStrArray::const_iterator end) const {
        Rune x = begin->rune;
        if (('a' <= x && x <= 'z') || ('A' <= x && x <= 'Z')) {
            begin++;
        } else {
            return begin;
        }
        while (begin != end) {
            x = begin->rune;
            if (('a' <= x && x <= 'z') || ('A' <= x && x <= 'Z') || ('0' <= x && x <= '9')) {
                begin++;
            } else {
                break;
            }
        }
        return begin;
    }

    RuneStrArray::const_iterator numbers_rule(RuneStrArray::const_iterator begin,
                                              RuneStrArray::const_iterator end) const {
        Rune x = begin->rune;
        if ('0' <= x && x <= '9') {
            begin++;
        } else {
            return begin;
        }
        while (begin != end) {
            x = begin->rune;
            if (('0' <= x && x <= '9') || x == '.') {
                begin++;
            } else {
                break;
            }
        }
        return begin;
    }

    // A word ends at every rune Viterbi tags E or S. The last rune is always one of them, so no
    // rune is left over.
    void internal_cut(RuneStrArray::const_iterator begin, RuneStrArray::const_iterator end,
                      std::vector<WordRange>& res) const {
        std::vector<size_t> status;
        viterbi(begin, end, status);

        auto left = begin;
        for (size_t i = 0; i < status.size(); i++) {
            if (status[i] % 2) { //if (HMMModel::E == status[i] || HMMModel::S == status[i])
                const auto right = begin + i + 1;
                res.emplace_back(left, right - 1);
                left = right;
            }
        }
    }

    // The most likely status of each rune of [begin, end), which is not empty. The sums run
    // weight + transition + emission in that order, the strict > keeps the first of equal
    // previous statuses, and the last rune is E when E and S are equal.
    void viterbi(RuneStrArray::const_iterator begin, RuneStrArray::const_iterator end,
                 std::vector<size_t>& status) const {
        const size_t Y = HMMModel::STATUS_SUM;
        const auto X = static_cast<size_t>(end - begin);

        const size_t XYSize = X * Y;

        std::vector<int> path(XYSize);
        std::vector<double> weight(XYSize);

        //start
        for (size_t y = 0; y < Y; y++) {
            weight[0 + y * X] =
                    _model->start_prob[y] +
                    _model->get_emit_prob(_model->emit_prob_vec[y], begin->rune, MIN_DOUBLE);
            path[0 + y * X] = -1;
        }

        for (size_t x = 1; x < X; x++) {
            for (size_t y = 0; y < Y; y++) {
                const size_t now = x + y * X;
                weight[now] = MIN_DOUBLE;
                path[now] = HMMModel::E; // warning
                const double emit_prob = _model->get_emit_prob(_model->emit_prob_vec[y],
                                                               (begin + x)->rune, MIN_DOUBLE);
                for (size_t pre_y = 0; pre_y < Y; pre_y++) {
                    const size_t old = x - 1 + pre_y * X;
                    const double tmp = weight[old] + _model->trans_prob[pre_y][y] + emit_prob;
                    if (tmp > weight[now]) {
                        weight[now] = tmp;
                        path[now] = static_cast<int>(pre_y);
                    }
                }
            }
        }

        const double end_e = weight[X - 1 + HMMModel::E * X];
        const double end_s = weight[X - 1 + HMMModel::S * X];
        size_t stat = 0;
        if (end_e >= end_s) {
            stat = HMMModel::E;
        } else {
            stat = HMMModel::S;
        }

        status.resize(X);
        // The path of the first rune is -1, which turns `stat` into SIZE_MAX after its last use.
        for (int x = static_cast<int>(X - 1); x >= 0; x--) {
            status[x] = stat;
            stat = path[x + stat * X];
        }
    }

    const HMMModel* _model;
};

} // namespace doris::segment_v2::inverted_index::jieba
