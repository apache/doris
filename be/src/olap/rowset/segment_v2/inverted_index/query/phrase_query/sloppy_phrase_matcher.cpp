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

#include "olap/rowset/segment_v2/inverted_index/query/phrase_query/sloppy_phrase_matcher.h"

namespace doris::segment_v2::inverted_index {
#include "common/compile_check_begin.h"

SloppyPhraseMatcher::SloppyPhraseMatcher(const std::vector<PostingsAndFreq>& postings, int32_t slop)
        : _slop(slop), _num_postings(static_cast<int32_t>(postings.size())) {
    _pq = std::make_unique<PhraseQueue>(postings.size());
    _phrase_positions.resize(postings.size());
    for (size_t i = 0; i < postings.size(); i++) {
        _phrase_positions[i] = std::make_unique<PhrasePositions>(
                postings[i]._postings, postings[i]._position, i, postings[i]._terms);
    }
}

void SloppyPhraseMatcher::reset(int32_t doc) {
    for (const auto& pp : _phrase_positions) {
        int32_t cur_doc = visit_node(pp->_postings, DocID {});
        if (cur_doc != doc) {
            std::string error_message = "docID mismatch: expected " + std::to_string(doc) +
                                        ", but got " + std::to_string(cur_doc);
            throw Exception(ErrorCode::INTERNAL_ERROR, error_message);
        }
    }

    _positioned = init_phrase_positions();
    _match_length = std::numeric_limits<int32_t>::max();
}

bool SloppyPhraseMatcher::next_match() {
    if (!_positioned) {
        return false;
    }
    if (_pq->size() < 2) {
        return false;
    }
    auto* pp = _pq->pop();
    assert(pp != nullptr);
    _match_length = _end - pp->_position;
    int32_t next = _pq->top()->_position;
    while (advance_pp(pp)) {
        if (_has_rpts && !advance_rpts(pp)) {
            break;
        }
        if (pp->_position > next) {
            _pq->add(pp);
            if (_match_length <= _slop) {
                return true;
            }
            pp = _pq->pop();
            next = _pq->top()->_position;
            assert(pp != nullptr);
            _match_length = _end - pp->_position;
        } else {
            int32_t match_length2 = _end - pp->_position;
            if (match_length2 < _match_length) {
                _match_length = match_length2;
            }
        }
    }
    _positioned = false;
    return _match_length <= _slop;
}

bool SloppyPhraseMatcher::advance_rpts(PhrasePositions* pp) {
    if (pp->_rpt_group < 0) {
        return true;
    }
    const auto& rg = _rpt_groups[pp->_rpt_group];
    FixedBitSet bits(static_cast<int32_t>(rg.size()));
    int32_t k0 = pp->_rpt_ind;
    int32_t k = 0;
    while ((k = collide(pp)) >= 0) {
        pp = lesser(pp, rg[k]);
        if (!advance_pp(pp)) {
            return false;
        }
        if (k != k0) {
            bits.ensure_capacity(k);
            bits.set(k);
        }
    }
    int32_t n = 0;
    int32_t num_bits = bits.length();
    while (bits.cardinality() > 0) {
        auto* pp2 = _pq->pop();
        _rpt_stack[n++] = pp2;
        if (pp2->_rpt_group >= 0 && pp2->_rpt_ind < num_bits && bits.get(pp2->_rpt_ind)) {
            bits.clear(pp2->_rpt_ind);
        }
    }
    for (int32_t i = n - 1; i >= 0; i--) {
        _pq->add(_rpt_stack[i]);
    }
    return true;
}

bool SloppyPhraseMatcher::advance_pp(PhrasePositions* pp) {
    if (!pp->next_position()) {
        return false;
    }
    if (pp->_position > _end) {
        _end = pp->_position;
    }
    return true;
}

PhrasePositions* SloppyPhraseMatcher::lesser(PhrasePositions* pp, PhrasePositions* pp2) {
    if (pp->_position < pp2->_position ||
        (pp->_position == pp2->_position && pp->_offset < pp2->_offset)) {
        return pp;
    }
    return pp2;
}

int32_t SloppyPhraseMatcher::collide(PhrasePositions* pp) {
    if (pp->_rpt_group + 1 > _rpt_groups.size()) {
        return -1;
    }

    int32_t cur_tp_pos = tp_pos(pp);
    const auto& rg = _rpt_groups[pp->_rpt_group];
    for (auto* pp2 : rg) {
        if (pp2 != pp && tp_pos(pp2) == cur_tp_pos) {
            return pp2->_rpt_ind;
        }
    }
    return -1;
}

int32_t SloppyPhraseMatcher::tp_pos(PhrasePositions* pp) {
    return pp->_position + pp->_offset;
}

bool SloppyPhraseMatcher::init_phrase_positions() {
    _end = std::numeric_limits<int32_t>::min();
    if (!_checked_rpts) {
        return init_first_time();
    }
    if (!_has_rpts) {
        init_simple();
        return true;
    }
    return init_complex();
}

bool SloppyPhraseMatcher::init_first_time() {
    _checked_rpts = true;
    place_first_positions();

    auto rpt_terms = repeating_terms();
    _has_rpts = !rpt_terms.empty();

    if (_has_rpts) {
        _rpt_stack.resize(_num_postings);
        auto rgs = gather_rpt_groups(rpt_terms);
        sort_rpt_groups(rgs);
        if (!advance_repeat_groups()) {
            return false;
        }
    }

    fill_queue();
    return true;
}

LinkedHashMap<std::string, int32_t> SloppyPhraseMatcher::repeating_terms() {
    LinkedHashMap<std::string, int32_t> tord;
    std::unordered_map<std::string, int32_t> tcnt;
    for (const auto& pp : _phrase_positions) {
        for (const auto& t : pp->_terms) {
            tcnt[t]++;
            if (tcnt[t] == 2) {
                tord.insert(t, static_cast<int32_t>(tord.size()));
            }
        }
    }
    return tord;
}

void SloppyPhraseMatcher::place_first_positions() {
    for (const auto& pp : _phrase_positions) {
        pp->first_position();
    }
}

std::vector<std::vector<PhrasePositions*>> SloppyPhraseMatcher::gather_rpt_groups(
        const LinkedHashMap<std::string, int32_t>& rpt_terms) {
    auto rpp = repeating_pps(rpt_terms);
    std::vector<std::vector<PhrasePositions*>> res;
    if (!_has_multi_term_rpts) {
        // simpler - no multi-terms - can base on positions in first doc
        for (size_t i = 0; i < rpp.size(); i++) {
            auto* pp = rpp[i];
            if (pp->_rpt_group >= 0) {
                continue;
            }
            int32_t cur_tp_pos = tp_pos(pp);
            for (size_t j = i + 1; j < rpp.size(); j++) {
                auto* pp2 = rpp[j];
                if (pp2->_rpt_group >= 0 || pp2->_offset == pp->_offset ||
                    tp_pos(pp2) != cur_tp_pos) {
                    continue;
                }
                int32_t g = pp->_rpt_group;
                if (g < 0) {
                    g = static_cast<int32_t>(res.size());
                    pp->_rpt_group = g;
                    std::vector<PhrasePositions*> rl;
                    rl.reserve(2);
                    rl.emplace_back(pp);
                    res.emplace_back(rl);
                }
                pp2->_rpt_group = g;
                res[g].emplace_back(pp2);
            }
        }
    } else {
        // more involved - has multi-terms
        throw Exception(ErrorCode::INTERNAL_ERROR, "Not supported yet");
    }
    return res;
}

std::vector<PhrasePositions*> SloppyPhraseMatcher::repeating_pps(
        const LinkedHashMap<std::string, int32_t>& rpt_terms) {
    std::vector<PhrasePositions*> rp;
    for (const auto& pp : _phrase_positions) {
        for (const auto& t : pp->_terms) {
            if (rpt_terms.contains(t)) {
                rp.emplace_back(pp.get());
                _has_multi_term_rpts |= (pp->_terms.size() > 1);
                break;
            }
        }
    }
    return rp;
}

void SloppyPhraseMatcher::sort_rpt_groups(std::vector<std::vector<PhrasePositions*>>& rgs) {
    _rpt_groups.resize(rgs.size());
    for (size_t i = 0; i < rgs.size(); ++i) {
        auto& rg = rgs[i];
        std::sort(rg.begin(), rg.end(), [](PhrasePositions* pp1, PhrasePositions* pp2) {
            return pp1->_offset < pp2->_offset;
        });
        _rpt_groups[i] = rg;
        for (size_t j = 0; j < _rpt_groups[i].size(); ++j) {
            _rpt_groups[i][j]->_rpt_ind = static_cast<int32_t>(j);
        }
    }
}

bool SloppyPhraseMatcher::advance_repeat_groups() {
    for (const auto& rg : _rpt_groups) {
        if (_has_multi_term_rpts) {
            throw Exception(ErrorCode::INTERNAL_ERROR, "Not supported yet");
        } else {
            for (size_t j = 1; j < rg.size(); j++) {
                for (size_t k = 0; k < j; k++) {
                    if (!rg[j]->next_position()) {
                        return false;
                    }
                }
            }
        }
    }
    return true;
}

void SloppyPhraseMatcher::fill_queue() {
    _pq->clear();
    for (const auto& pp : _phrase_positions) {
        if (pp->_position > _end) {
            _end = pp->_position;
        }
        _pq->add(pp.get());
    }
}

void SloppyPhraseMatcher::init_simple() {
    _pq->clear();
    for (const auto& pp : _phrase_positions) {
        pp->first_position();
        if (pp->_position > _end) {
            _end = pp->_position;
        }
        _pq->add(pp.get());
    }
}

bool SloppyPhraseMatcher::init_complex() {
    place_first_positions();
    if (!advance_repeat_groups()) {
        return false;
    }
    fill_queue();
    return true;
}

#include "common/compile_check_end.h"
} // namespace doris::segment_v2::inverted_index