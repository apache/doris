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
#include <CLucene/index/IndexVersion.h>
#include <CLucene/index/Term.h>
#include <CLucene/search/query/TermIterator.h>

#include "roaring/roaring.hh"

CL_NS_USE(index)

namespace doris {

class ConjunctionQuery {
public:
    ConjunctionQuery(IndexReader* reader);
    ~ConjunctionQuery();

    void set_conjunction_ratio(int32_t conjunction_ratio) {
        _conjunction_ratio = conjunction_ratio;
    }

    void add(const std::wstring& field_name, const std::vector<std::string>& terms);
    void search(roaring::Roaring& roaring);

private:
    void search_by_bitmap(roaring::Roaring& roaring);
    void search_by_skiplist(roaring::Roaring& roaring);

    int32_t do_next(int32_t doc);

    IndexReader* _reader = nullptr;
    IndexVersion _index_version = IndexVersion::kV0;
    int32_t _conjunction_ratio = 1000;
    bool _use_skip = false;

    TermIterator _lead1;
    TermIterator _lead2;
    std::vector<TermIterator> _others;

    std::vector<Term*> _terms;
    std::vector<TermDocs*> _term_docs;
};

} // namespace doris