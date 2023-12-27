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

#include "regexp_query.h"

#include <CLucene/config/repl_wchar.h>
#include <hs/hs.h>

#include "common/logging.h"

namespace doris::segment_v2 {

RegexpQuery::RegexpQuery(const std::shared_ptr<lucene::search::IndexSearcher>& searcher)
        : _searcher(searcher), query(searcher->getReader()) {}

void RegexpQuery::add(const std::wstring& field_name, const std::string& pattern) {
    hs_database_t* database = nullptr;
    hs_compile_error_t* compile_err = nullptr;
    hs_scratch_t* scratch = nullptr;

    if (hs_compile(pattern.data(), HS_FLAG_DOTALL | HS_FLAG_ALLOWEMPTY | HS_FLAG_UTF8,
                   HS_MODE_BLOCK, nullptr, &database, &compile_err) != HS_SUCCESS) {
        LOG(ERROR) << "hyperscan compilation failed: " << compile_err->message;
        hs_free_compile_error(compile_err);
        return;
    }

    if (hs_alloc_scratch(database, &scratch) != HS_SUCCESS) {
        LOG(ERROR) << "hyperscan could not allocate scratch space.";
        hs_free_database(database);
        return;
    }

    auto on_match = [](unsigned int id, unsigned long long from, unsigned long long to,
                       unsigned int flags, void* context) -> int {
        *((bool*)context) = true;
        return 0;
    };

    Term* term = nullptr;
    TermEnum* enumerator = nullptr;
    std::vector<std::string> terms;
    int32_t count = 0;

    try {
        enumerator = _searcher->getReader()->terms();
        while (enumerator->next()) {
            term = enumerator->term();
            std::string input = lucene_wcstoutf8string(term->text(), term->textLength());

            bool is_match = false;
            if (hs_scan(database, input.data(), input.size(), 0, scratch, on_match,
                        (void*)&is_match) != HS_SUCCESS) {
                LOG(ERROR) << "hyperscan match failed: " << input;
                break;
            }

            if (is_match) {
                terms.emplace_back(std::move(input));
                if (++count >= _max_expansions) {
                    break;
                }
            }

            _CLDECDELETE(term);
        }
    }
    _CLFINALLY({
        _CLDECDELETE(term);
        enumerator->close();
        _CLDELETE(enumerator);

        hs_free_scratch(scratch);
        hs_free_database(database);
    })

    query.add(field_name, terms);
}

void RegexpQuery::search(roaring::Roaring& roaring) {
    query.search(roaring);
}

} // namespace doris::segment_v2
