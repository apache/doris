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

#include "olap/rowset/segment_v2/inverted_index/query/phrase_query/phrase_matcher.h"
#include "olap/rowset/segment_v2/inverted_index/query/phrase_query/phrase_positions.h"
#include "olap/rowset/segment_v2/inverted_index/query/phrase_query/phrase_queue.h"
#include "olap/rowset/segment_v2/inverted_index/util/fixed_bit_set.h"
#include "olap/rowset/segment_v2/inverted_index/util/linked_hash_map.h"

namespace doris::segment_v2::inverted_index {
#include "common/compile_check_begin.h"

class SloppyPhraseMatcher : public PhraseMatcherBase<SloppyPhraseMatcher> {
public:
    SloppyPhraseMatcher(const std::vector<PostingsAndFreq>& postings, int32_t slop);

    void reset(int32_t doc);
    bool next_match();
    bool advance_rpts(PhrasePositions* pp);

private:
    bool advance_pp(PhrasePositions* pp);
    PhrasePositions* lesser(PhrasePositions* pp, PhrasePositions* pp2);
    int32_t collide(PhrasePositions* pp);
    int32_t tp_pos(PhrasePositions* pp);
    bool init_phrase_positions();
    bool init_first_time();
    LinkedHashMap<std::string, int32_t> repeating_terms();
    void place_first_positions();
    std::vector<std::vector<PhrasePositions*>> gather_rpt_groups(
            const LinkedHashMap<std::string, int32_t>& rptTerms);
    std::vector<PhrasePositions*> repeating_pps(
            const LinkedHashMap<std::string, int32_t>& rpt_terms);
    void sort_rpt_groups(std::vector<std::vector<PhrasePositions*>>& rgs);
    bool advance_repeat_groups();
    void fill_queue();
    void init_simple();
    bool init_complex();

    std::vector<std::unique_ptr<PhrasePositions>> _phrase_positions;

    int32_t _slop = 0;
    int32_t _num_postings = 0;
    std::unique_ptr<PhraseQueue> _pq;

    int32_t _end = 0;

    bool _has_rpts = false;
    bool _checked_rpts = false;
    bool _has_multi_term_rpts = false;
    std::vector<std::vector<PhrasePositions*>> _rpt_groups;
    std::vector<PhrasePositions*> _rpt_stack;

    bool _positioned = false;
    int32_t _match_length = 0;
};

#include "common/compile_check_end.h"
} // namespace doris::segment_v2::inverted_index