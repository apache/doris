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

#include "regexp_weight.h"

#include <CLucene/index/IndexReader.h>
#include <CLucene/index/Term.h>
#include <gen_cpp/PaloBrokerService_types.h>

#include <algorithm>
#include <string>

#include "olap/rowset/segment_v2/inverted_index/query_v2/bit_set_query/bit_set_scorer.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/const_score_query/const_score_scorer.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/nullable_scorer.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/segment_postings.h"
#include "olap/rowset/segment_v2/inverted_index/util/string_helper.h"

CL_NS_USE(index)

namespace doris::segment_v2::inverted_index::query_v2 {

RegexpWeight::RegexpWeight(IndexQueryContextPtr context, std::wstring field, std::string pattern,
                           bool enable_scoring, bool nullable)
        : _context(std::move(context)),
          _field(std::move(field)),
          _pattern(std::move(pattern)),
          _enable_scoring(enable_scoring),
          _nullable(nullable) {
    // _max_expansions = _context->runtime_state->query_options().inverted_index_max_expansions;
}

ScorerPtr RegexpWeight::scorer(const QueryExecutionContext& context,
                               const std::string& binding_key) {
    auto scorer = regexp_scorer(context, binding_key);
    if (_nullable) {
        auto logical_field = logical_field_or_fallback(context, binding_key, _field);
        return make_nullable_scorer(scorer, logical_field, context.null_resolver);
    }
    return scorer;
}

ScorerPtr RegexpWeight::regexp_scorer(const QueryExecutionContext& context,
                                      const std::string& binding_key) {
    auto prefix = get_regex_prefix(_pattern);

    hs_database_t* database = nullptr;
    hs_compile_error_t* compile_err = nullptr;
    hs_scratch_t* scratch = nullptr;

    if (hs_compile(_pattern.data(), HS_FLAG_DOTALL | HS_FLAG_ALLOWEMPTY | HS_FLAG_UTF8,
                   HS_MODE_BLOCK, nullptr, &database, &compile_err) != HS_SUCCESS) {
        LOG(ERROR) << "hyperscan compilation failed: " << compile_err->message;
        hs_free_compile_error(compile_err);
        return std::make_shared<EmptyScorer>();
    }

    if (hs_alloc_scratch(database, &scratch) != HS_SUCCESS) {
        LOG(ERROR) << "hyperscan could not allocate scratch space.";
        hs_free_database(database);
        return std::make_shared<EmptyScorer>();
    }

    std::vector<std::wstring> matching_terms;
    try {
        collect_matching_terms(context, binding_key, matching_terms, database, scratch, prefix);
    } catch (...) {
        hs_free_scratch(scratch);
        hs_free_database(database);
        throw;
    }

    hs_free_scratch(scratch);
    hs_free_database(database);

    if (matching_terms.empty()) {
        return std::make_shared<EmptyScorer>();
    }

    auto doc_bitset = std::make_shared<roaring::Roaring>();
    for (const auto& term : matching_terms) {
        auto t = make_term_ptr(_field.c_str(), term.c_str());
        auto reader = lookup_reader(_field, context, binding_key);
        auto iter = make_term_doc_ptr(reader.get(), t.get(), _enable_scoring, _context->io_ctx);
        auto segment_postings = make_segment_postings(std::move(iter), _enable_scoring);

        uint32_t doc = segment_postings->doc();
        while (doc != TERMINATED) {
            doc_bitset->add(doc);
            doc = segment_postings->advance();
        }
    }

    auto bit_set = std::make_shared<BitSetScorer>(doc_bitset);
    auto const_score = std::make_shared<ConstScoreScorer<BitSetScorerPtr>>(std::move(bit_set));
    return const_score;
}

std::optional<std::string> RegexpWeight::get_regex_prefix(const std::string& pattern) {
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

    auto [it1, it2] = std::ranges::mismatch(min_prefix, max_prefix);

    const size_t common_len = std::distance(min_prefix.begin(), it1);
    if (common_len == 0) {
        return std::nullopt;
    }

    return min_prefix.substr(0, common_len);
}

void RegexpWeight::collect_matching_terms(const QueryExecutionContext& context,
                                          const std::string& binding_key,
                                          std::vector<std::wstring>& terms, hs_database_t* database,
                                          hs_scratch_t* scratch,
                                          const std::optional<std::string>& prefix) {
    auto on_match = [](unsigned int id, unsigned long long from, unsigned long long to,
                       unsigned int flags, void* context) -> int {
        *((bool*)context) = true;
        return 0;
    };

    auto reader = lookup_reader(_field, context, binding_key);
    if (reader == nullptr) {
        return;
    }

    int32_t count = 0;
    Term* term = nullptr;
    TermEnum* enumerator = nullptr;

    try {
        if (prefix) {
            std::wstring ws_prefix = StringUtil::string_to_wstring(*prefix);
            Term prefix_term(_field.c_str(), ws_prefix.c_str());
            enumerator = reader->terms(&prefix_term, _context->io_ctx);
        } else {
            enumerator = reader->terms(nullptr, _context->io_ctx);
            if (enumerator) {
                enumerator->next();
            }
        }

        if (!enumerator) {
            return;
        }

        do {
            term = enumerator->term();
            if (term == nullptr) {
                break;
            }

            if (_field != term->field()) {
                _CLDECDELETE(term);
                break;
            }

            auto term_text =
                    StringHelper::to_string(std::wstring(term->text(), term->textLength()));

            if (prefix && !term_text.starts_with(*prefix)) {
                _CLDECDELETE(term);
                break;
            }

            bool is_match = false;
            if (hs_scan(database, term_text.data(), static_cast<uint32_t>(term_text.size()), 0,
                        scratch, on_match, (void*)&is_match) != HS_SUCCESS) {
                LOG(ERROR) << "hyperscan match failed: " << term_text;
                _CLDECDELETE(term);
                break;
            }

            if (is_match) {
                if (_max_expansions > 0 && count >= _max_expansions) {
                    _CLDECDELETE(term);
                    break;
                }

                terms.emplace_back(term->text(), term->textLength());
                count++;
            }

            _CLDECDELETE(term);
        } while (enumerator->next());

        _CLDECDELETE(term);
        if (enumerator) {
            enumerator->close();
            _CLDELETE(enumerator);
        }
    } catch (...) {
        _CLDECDELETE(term);
        if (enumerator) {
            enumerator->close();
            _CLDELETE(enumerator);
        }
        throw;
    }
}

} // namespace doris::segment_v2::inverted_index::query_v2
