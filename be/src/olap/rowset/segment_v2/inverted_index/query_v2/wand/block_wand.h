// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <algorithm>
#include <cassert>
#include <ranges>
#include <vector>

#include "olap/rowset/segment_v2/inverted_index/query_v2/term_query/term_scorer.h"

namespace doris::segment_v2::inverted_index::query_v2 {

class BlockWand {
public:
    template <typename Callback>
    static void execute(TermScorerPtr scorer, float threshold, Callback&& callback) {
        uint32_t doc = scorer->doc();
        while (doc != TERMINATED) {
            while (scorer->block_max_score() < threshold) {
                uint32_t last_doc_in_block = scorer->last_doc_in_block();
                if (last_doc_in_block == TERMINATED) {
                    return;
                }
                doc = last_doc_in_block + 1;
                scorer->seek_block(doc);
            }

            doc = scorer->seek(doc);
            if (doc == TERMINATED) {
                break;
            }

            while (true) {
                float score = scorer->score();
                if (score > threshold) {
                    threshold = callback(doc, score);
                }
                if (doc == scorer->last_doc_in_block()) {
                    break;
                }
                doc = scorer->advance();
                if (doc == TERMINATED) {
                    return;
                }
            }
            doc++;
            scorer->seek_block(doc);
        }
    }

    template <typename Callback>
    static void execute(std::vector<TermScorerPtr> scorers, float threshold, Callback&& callback) {
        if (scorers.empty()) {
            return;
        }

        if (scorers.size() == 1) {
            execute(std::move(scorers[0]), threshold, std::forward<Callback>(callback));
            return;
        }

        std::vector<ScorerWrapper> wrappers;
        wrappers.reserve(scorers.size());
        for (auto& s : scorers) {
            if (s->doc() != TERMINATED) {
                wrappers.emplace_back(std::move(s));
            }
        }

        std::sort(wrappers.begin(), wrappers.end(),
                  [](const ScorerWrapper& a, const ScorerWrapper& b) { return a.doc() < b.doc(); });

        while (true) {
            auto result = find_pivot_doc(wrappers, threshold);
            if (result.pivot_doc == TERMINATED) {
                break;
            }
            auto [before_pivot_len, pivot_len, pivot_doc] = result;

            assert(std::ranges::is_sorted(wrappers,
                                          [](const ScorerWrapper& a, const ScorerWrapper& b) {
                                              return a.doc() < b.doc();
                                          }));
            assert(pivot_doc != TERMINATED);
            assert(before_pivot_len < pivot_len);

            float block_max_score_upperbound = 0.0F;
            for (size_t i = 0; i < pivot_len; ++i) {
                wrappers[i].seek_block(pivot_doc);
                block_max_score_upperbound += wrappers[i].block_max_score();
            }

            if (block_max_score_upperbound <= threshold) {
                block_max_was_too_low_advance_one_scorer(wrappers, pivot_len);
                continue;
            }

            if (!align_scorers(wrappers, pivot_doc, before_pivot_len)) {
                continue;
            }

            float score = 0.0F;
            for (size_t i = 0; i < pivot_len; ++i) {
                score += wrappers[i].score();
            }

            if (score > threshold) {
                threshold = callback(pivot_doc, score);
            }

            advance_all_scorers_on_pivot(wrappers, pivot_len);
        }
    }

private:
    class ScorerWrapper {
    public:
        explicit ScorerWrapper(TermScorerPtr scorer)
                : _scorer(std::move(scorer)), _max_score(_scorer->max_score()) {}

        uint32_t doc() const { return _scorer->doc(); }
        uint32_t advance() { return _scorer->advance(); }
        uint32_t seek(uint32_t target) { return _scorer->seek(target); }
        float score() { return _scorer->score(); }

        void seek_block(uint32_t target) { _scorer->seek_block(target); }
        uint32_t last_doc_in_block() const { return _scorer->last_doc_in_block(); }
        float block_max_score() const { return _scorer->block_max_score(); }
        float max_score() const { return _max_score; }

    private:
        TermScorerPtr _scorer;
        float _max_score;
    };

    struct PivotResult {
        size_t before_pivot_len;
        size_t pivot_len;
        uint32_t pivot_doc;
    };

    static PivotResult find_pivot_doc(std::vector<ScorerWrapper>& scorers, float threshold) {
        float max_score = 0.0F;
        size_t before_pivot_len = 0;
        uint32_t pivot_doc = TERMINATED;

        while (before_pivot_len < scorers.size()) {
            max_score += scorers[before_pivot_len].max_score();
            if (max_score > threshold) {
                pivot_doc = scorers[before_pivot_len].doc();
                break;
            }
            before_pivot_len++;
        }

        if (pivot_doc == TERMINATED) {
            return PivotResult {.before_pivot_len = 0, .pivot_len = 0, .pivot_doc = TERMINATED};
        }

        size_t pivot_len = before_pivot_len + 1;
        while (pivot_len < scorers.size() && scorers[pivot_len].doc() == pivot_doc) {
            pivot_len++;
        }

        return PivotResult {.before_pivot_len = before_pivot_len,
                            .pivot_len = pivot_len,
                            .pivot_doc = pivot_doc};
    }

    static void restore_ordering(std::vector<ScorerWrapper>& scorers, size_t ord) {
        uint32_t doc = scorers[ord].doc();
        while (ord + 1 < scorers.size() && doc > scorers[ord + 1].doc()) {
            std::swap(scorers[ord], scorers[ord + 1]);
            ord++;
        }
        assert(std::ranges::is_sorted(scorers, [](const ScorerWrapper& a, const ScorerWrapper& b) {
            return a.doc() < b.doc();
        }));
    }

    static void block_max_was_too_low_advance_one_scorer(std::vector<ScorerWrapper>& scorers,
                                                         size_t pivot_len) {
        assert(std::ranges::is_sorted(scorers, [](const ScorerWrapper& a, const ScorerWrapper& b) {
            return a.doc() < b.doc();
        }));

        size_t scorer_to_seek = pivot_len - 1;
        float global_max_score = scorers[scorer_to_seek].max_score();
        uint32_t doc_to_seek_after = scorers[scorer_to_seek].last_doc_in_block();
        for (size_t i = pivot_len - 1; i > 0; --i) {
            size_t scorer_ord = i - 1;
            const auto& scorer = scorers[scorer_ord];
            doc_to_seek_after = std::min(doc_to_seek_after, scorer.last_doc_in_block());
            if (scorer.max_score() > global_max_score) {
                global_max_score = scorer.max_score();
                scorer_to_seek = scorer_ord;
            }
        }
        if (doc_to_seek_after != TERMINATED) {
            doc_to_seek_after++;
        }
        for (size_t i = pivot_len; i < scorers.size(); ++i) {
            const auto& scorer = scorers[i];
            doc_to_seek_after = std::min(doc_to_seek_after, scorer.doc());
        }
        scorers[scorer_to_seek].seek(doc_to_seek_after);
        restore_ordering(scorers, scorer_to_seek);

        assert(std::ranges::is_sorted(scorers, [](const ScorerWrapper& a, const ScorerWrapper& b) {
            return a.doc() < b.doc();
        }));
    }

    static bool align_scorers(std::vector<ScorerWrapper>& scorers, uint32_t pivot_doc,
                              size_t before_pivot_len) {
        for (size_t i = before_pivot_len; i > 0; --i) {
            size_t idx = i - 1;
            uint32_t new_doc = scorers[idx].seek(pivot_doc);
            if (new_doc != pivot_doc) {
                if (new_doc == TERMINATED) {
                    std::swap(scorers[idx], scorers.back());
                    scorers.pop_back();
                }
                restore_ordering(scorers, idx);
                return false;
            }
        }
        return true;
    }

    static void advance_all_scorers_on_pivot(std::vector<ScorerWrapper>& scorers,
                                             size_t pivot_len) {
        for (size_t i = 0; i < pivot_len; ++i) {
            scorers[i].advance();
        }

        size_t i = 0;
        while (i < scorers.size()) {
            if (scorers[i].doc() == TERMINATED) {
                std::swap(scorers[i], scorers.back());
                scorers.pop_back();
            } else {
                i++;
            }
        }

        std::ranges::sort(scorers, [](const ScorerWrapper& a, const ScorerWrapper& b) {
            return a.doc() < b.doc();
        });
    }
};

template <typename Callback>
inline void block_wand_single_scorer(TermScorerPtr scorer, float threshold, Callback&& callback) {
    BlockWand::execute(std::move(scorer), threshold, std::forward<Callback>(callback));
}

template <typename Callback>
inline void block_wand(std::vector<TermScorerPtr> scorers, float threshold, Callback&& callback) {
    BlockWand::execute(std::move(scorers), threshold, std::forward<Callback>(callback));
}

} // namespace doris::segment_v2::inverted_index::query_v2
