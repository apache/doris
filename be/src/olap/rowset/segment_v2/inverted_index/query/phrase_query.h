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

// clang-format off
#include <CLucene.h> // IWYU pragma: keep
#include <CLucene/index/Term.h>
#include <CLucene/search/query/TermIterator.h>
#include <CLucene/search/query/TermPositionIterator.h>
#include "CLucene/search/PhraseQuery.h"
// clang-format on

#include <gen_cpp/PaloInternalService_types.h>

#include <memory>
#include <variant>

#include "roaring/roaring.hh"

CL_NS_USE(index)
CL_NS_USE(search)
CL_NS_USE(util)

namespace doris::segment_v2 {

class PostingsAndPosition {
public:
    PostingsAndPosition(const TermPositionIterator& postings, int32_t offset)
            : _postings(postings), _offset(offset) {}

    TermPositionIterator _postings;
    int32_t _offset = 0;
    int32_t _freq = 0;
    int32_t _upTo = 0;
    int32_t _pos = 0;
};

template <typename Derived>
class PhraseMatcherBase {
public:
    bool matches(int32_t doc);

private:
    void reset(int32_t doc);

protected:
    bool advance_position(PostingsAndPosition& posting, int32_t target);

public:
    std::vector<PostingsAndPosition> _postings;
};

class ExactPhraseMatcher : public PhraseMatcherBase<ExactPhraseMatcher> {
public:
    bool next_match();
};

class OrderedSloppyPhraseMatcher : public PhraseMatcherBase<OrderedSloppyPhraseMatcher> {
public:
    bool next_match();

private:
    bool stretchToOrder(PostingsAndPosition* prev_posting);

public:
    int32_t _allowed_slop = 0;

private:
    int32_t _match_width = -1;
};

using PhraseQueryPtr = std::unique_ptr<CL_NS(search)::PhraseQuery>;
using Matcher = std::variant<ExactPhraseMatcher, OrderedSloppyPhraseMatcher, PhraseQueryPtr>;

class PhraseQuery {
public:
    PhraseQuery(const std::shared_ptr<lucene::search::IndexSearcher>& searcher,
                const TQueryOptions& query_options);
    ~PhraseQuery();

    void add(const std::wstring& field_name, const std::vector<std::string>& terms, int32_t slop,
             bool ordered);
    void add(const std::wstring& field_name, const std::vector<std::string>& terms, int32_t slop);
    void search(roaring::Roaring& roaring);

private:
    void search_by_bitmap(roaring::Roaring& roaring);
    void search_by_skiplist(roaring::Roaring& roaring);

    int32_t do_next(int32_t doc);
    bool matches(int32_t doc);

public:
    static void parser_slop(std::string& query, int32_t& slop, bool& ordered_phrase);

private:
    std::shared_ptr<lucene::search::IndexSearcher> _searcher;

    TermIterator _lead1;
    TermIterator _lead2;
    std::vector<TermIterator> _others;

    std::vector<PostingsAndPosition> _postings;

    std::vector<Term*> _terms;
    std::vector<TermDocs*> _term_docs;

    Matcher _matcher;
};

} // namespace doris::segment_v2