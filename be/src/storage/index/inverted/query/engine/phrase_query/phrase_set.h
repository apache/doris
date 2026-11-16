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

#include <cstdint>
#include <memory>
#include <optional>
#include <roaring/roaring.hh>
#include <string>
#include <vector>

#include "storage/index/inverted/common/query_info.h"
#include "storage/index/inverted/query/engine/query.h"
#include "storage/index/inverted/query/engine/scorer.h"
#include "storage/index/inverted/query/engine/segment_postings.h"
#include "storage/index/inverted/query/engine/weight.h"
#include "storage/index/inverted/query/phrase/phrase_verifier.h"
#include "storage/index/inverted/query/spi/index_source.h"

// Several phrases of single terms on one field, OR'ed and unscored: `f:"a b" OR f:"b c" OR ...`.
//
// Run as separate phrase queries, each phrase opens its own postings of every word, intersects
// them and reads positions on the rows they share, so a word that several phrases hold is looked
// up, decoded and position-read once per phrase. Here every distinct word is opened once for the
// whole set. A row is a candidate when it holds every word of at least one phrase, which the
// words' document lists answer without positions; positions are read, once per word and row, only
// on candidates, and only for the phrases whose words the row holds.
//
// The set is one two-valued scorer: a row matches when any phrase does, so the first phrase that
// matches settles it. It also lists its rows for a conjunction's candidates, which leaves
// positions unread on rows the other clauses ruled out. Scored queries keep their phrase queries.
namespace doris::segment_v2::inverted_index::query_v2 {

// One phrase of a set: its single-term clauses at their positions.
struct PhraseSetPhrase {
    std::vector<TermInfo> clauses;
    uint32_t slop = 0;
    bool ordered = false;
};

// The words of a set opened once on one segment, and the rows that hold every word of a phrase.
class PhraseSetEngine {
public:
    // Opens the distinct words of `phrases` on `source`, reading only the rows of `within` when
    // it is given.
    static std::shared_ptr<PhraseSetEngine> open(index_query::IndexSource& source,
                                                 const std::vector<PhraseSetPhrase>& phrases,
                                                 const roaring::Roaring* within = nullptr);

    PhraseSetEngine(const PhraseSetEngine&) = delete;
    PhraseSetEngine& operator=(const PhraseSetEngine&) = delete;
    ~PhraseSetEngine();

    // The rows holding every word of some phrase.
    const roaring::Roaring& candidates() const { return _candidates; }

    // The first row at or after `target` some phrase matches, or TERMINATED. Rows are asked
    // for in ascending order.
    uint32_t next_match(uint32_t target);
    // The rows some phrase matches among `rows` (every candidate when null).
    void match_rows(const roaring::Roaring* rows, roaring::Roaring* out);

private:
    struct Word {
        SegmentPostingsPtr postings;
        std::vector<uint32_t> positions;
        uint32_t loaded_row = TERMINATED;
    };
    struct Phrase {
        // The engine word of each distinct word of the phrase, in the verifier's term order.
        std::vector<size_t> words;
        std::optional<index_query::PhraseVerifier> verifier;
        roaring::Roaring rows;
        // The first of `rows` at or after the last row asked about, read in step with the rows
        // the engine visits.
        std::optional<roaring::Roaring::const_iterator> next;
        uint32_t next_row = 0;
    };

    PhraseSetEngine() = default;

    // Whether `phrase` holds all its words at `row`; rows are asked about in ascending order.
    static bool holds(Phrase& phrase, uint32_t row);
    // The positions of `word` at `row`, read once per row.
    Status load(size_t word, uint32_t row, index_query::PhrasePositionSpan* span);
    // Whether `phrase` matches at `row`, which holds all its words.
    bool verify(Phrase& phrase, uint32_t row);

    std::vector<Word> _words;
    std::vector<Phrase> _phrases;
    roaring::Roaring _candidates;
};

using PhraseSetEnginePtr = std::shared_ptr<PhraseSetEngine>;

// The phrases of one set on one field.
class PhraseSetGroup {
public:
    PhraseSetGroup(std::wstring field, std::vector<PhraseSetPhrase> phrases)
            : _field(std::move(field)), _phrases(std::move(phrases)) {}

    const std::wstring& field() const { return _field; }
    const std::vector<PhraseSetPhrase>& phrases() const { return _phrases; }

private:
    std::wstring _field;
    std::vector<PhraseSetPhrase> _phrases;
};

using PhraseSetGroupPtr = std::shared_ptr<PhraseSetGroup>;

// The OR of a set's phrases, unscored.
class PhraseSetQuery final : public Query {
public:
    explicit PhraseSetQuery(PhraseSetGroupPtr group) : _group(std::move(group)) {}
    WeightPtr weight(bool enable_scoring) override;

private:
    PhraseSetGroupPtr _group;
};

} // namespace doris::segment_v2::inverted_index::query_v2
