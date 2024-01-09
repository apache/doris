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

#include <CLucene.h>
#include <CLucene/index/IndexReader.h>
#include <CLucene/index/Term.h>
#include <CLucene/search/query/TermIterator.h>
#include <CLucene/search/query/TermPositionIterator.h>

#include <memory>

#include "roaring/roaring.hh"

CL_NS_USE(index)

namespace doris::segment_v2 {

class PhraseQuery {
public:
    PhraseQuery(const std::shared_ptr<lucene::search::IndexSearcher>& searcher)
            : _searcher(searcher) {}
    ~PhraseQuery();

    void add(const std::wstring& field_name, const std::vector<std::string>& terms);
    void search(roaring::Roaring& roaring);

private:
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

    void search_by_bitmap(roaring::Roaring& roaring);
    void search_by_skiplist(roaring::Roaring& roaring);

    int32_t do_next(int32_t doc);
    bool next_match();
    bool advance_position(PostingsAndPosition& posting, int32_t target);
    void reset();

private:
    std::shared_ptr<lucene::search::IndexSearcher> _searcher;

    TermIterator _lead1;
    TermIterator _lead2;
    std::vector<TermIterator> _others;

    std::vector<PostingsAndPosition> _postings;

    std::vector<Term*> _terms;
    std::vector<TermDocs*> _term_docs;
};

} // namespace doris::segment_v2