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

#include <optional>

#include "common/logging.h"
#include "util/debug_points.h"

namespace doris::segment_v2 {
#include "common/compile_check_begin.h"

RegexpQuery::RegexpQuery(const std::shared_ptr<lucene::search::IndexSearcher>& searcher,
                         const TQueryOptions& query_options, const io::IOContext* io_ctx)
        : _searcher(searcher),
          _io_ctx(io_ctx),
          _max_expansions(query_options.inverted_index_max_expansions),
          _query(searcher, query_options, io_ctx) {}

void RegexpQuery::add(const InvertedIndexQueryInfo& query_info) {
    if (query_info.term_infos.empty()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "term_infos cannot be empty");
    }

    const std::string& pattern = query_info.term_infos[0].get_single_term();
    auto prefix = get_regex_prefix(pattern);

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

    std::vector<std::string> terms;
    try {
        collect_matching_terms(query_info.field_name, terms, database, scratch, prefix);
    }
    _CLFINALLY({
        hs_free_scratch(scratch);
        hs_free_database(database);
    })

    if (terms.empty()) {
        return;
    }

    InvertedIndexQueryInfo new_query_info;
    new_query_info.field_name = query_info.field_name;
    new_query_info.term_infos.emplace_back(std::move(terms), query_info.term_infos[0].position);
    _query.add(new_query_info);
}

void RegexpQuery::search(roaring::Roaring& roaring) {
    _query.search(roaring);
}

std::optional<std::string> RegexpQuery::get_regex_prefix(const std::string& pattern) {
    DBUG_EXECUTE_IF("RegexpQuery.get_regex_prefix", { return std::nullopt; });

    if (pattern.empty() || pattern[0] != '^') {
        return std::nullopt;
    }

    re2::RE2 re(pattern);
    if (!re.ok()) {
        return std::nullopt;
    }

    std::string min_prefix, max_prefix;
    if (!re.PossibleMatchRange(&min_prefix, &max_prefix, 256)) {
        return std::nullopt;
    }

    if (min_prefix.empty() || max_prefix.empty() || min_prefix[0] != max_prefix[0]) {
        return std::nullopt;
    }

    auto [it1, it2] = std::mismatch(min_prefix.begin(), min_prefix.end(), max_prefix.begin(),
                                    max_prefix.end());

    const size_t common_len = std::distance(min_prefix.begin(), it1);
    if (common_len == 0) {
        return std::nullopt;
    }

    return min_prefix.substr(0, common_len);
}

void RegexpQuery::collect_matching_terms(const std::wstring& field_name,
                                         std::vector<std::string>& terms, hs_database_t* database,
                                         hs_scratch_t* scratch,
                                         const std::optional<std::string>& prefix) {
    auto on_match = [](unsigned int id, unsigned long long from, unsigned long long to,
                       unsigned int flags, void* context) -> int {
        *((bool*)context) = true;
        return 0;
    };

    int32_t count = 0;
    Term* term = nullptr;
    TermEnum* enumerator = nullptr;
    try {
        if (prefix) {
            std::wstring ws_prefix = StringUtil::string_to_wstring(*prefix);
            Term prefix_term(field_name.c_str(), ws_prefix.c_str());
            enumerator = _searcher->getReader()->terms(&prefix_term, _io_ctx);
        } else {
            enumerator = _searcher->getReader()->terms(nullptr, _io_ctx);
            enumerator->next();
        }
        do {
            term = enumerator->term();
            if (term != nullptr) {
                std::string input = lucene_wcstoutf8string(term->text(), term->textLength());

                if (prefix) {
                    if (!input.starts_with(*prefix)) {
                        break;
                    }
                }

                bool is_match = false;
                if (hs_scan(database, input.data(), static_cast<uint32_t>(input.size()), 0, scratch,
                            on_match, (void*)&is_match) != HS_SUCCESS) {
                    LOG(ERROR) << "hyperscan match failed: " << input;
                    break;
                }

                if (is_match) {
                    if (_max_expansions > 0 && count >= _max_expansions) {
                        break;
                    }

                    terms.emplace_back(std::move(input));
                    count++;
                }
            } else {
                break;
            }
            _CLDECDELETE(term);
        } while (enumerator->next());
    }
    _CLFINALLY({
        _CLDECDELETE(term);
        enumerator->close();
        _CLDELETE(enumerator);
    })
}

#include "common/compile_check_end.h"
} // namespace doris::segment_v2
