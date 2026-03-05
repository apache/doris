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

#include <CLucene/config/repl_wchar.h>
#include <CLucene/index/IndexReader.h>
#include <CLucene/index/Term.h>

#include "olap/rowset/segment_v2/index_query_context.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/bit_set_query/bit_set_scorer.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/const_score_query/const_score_scorer.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/nullable_scorer.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/segment_postings.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/weight.h"
#include "olap/rowset/segment_v2/inverted_index/util/string_helper.h"

CL_NS_USE(index)

namespace doris::segment_v2::inverted_index::query_v2 {

class PrefixWeight : public Weight {
public:
    PrefixWeight(IndexQueryContextPtr context, std::wstring field, std::string prefix,
                 bool enable_scoring, int32_t max_expansions, bool nullable)
            : _context(std::move(context)),
              _field(std::move(field)),
              _prefix(std::move(prefix)),
              _enable_scoring(enable_scoring),
              _max_expansions(max_expansions),
              _nullable(nullable) {}

    ~PrefixWeight() override = default;

    ScorerPtr scorer(const QueryExecutionContext& ctx, const std::string& binding_key) override {
        auto scorer = prefix_scorer(ctx, binding_key);
        if (_nullable) {
            auto logical_field = logical_field_or_fallback(ctx, binding_key, _field);
            return make_nullable_scorer(scorer, logical_field, ctx.null_resolver);
        }
        return scorer;
    }

    static std::vector<std::string> expand_prefix(lucene::index::IndexReader* reader,
                                                  const std::wstring& field,
                                                  const std::string& prefix, int32_t max_expansions,
                                                  const io::IOContext* io_ctx) {
        std::vector<std::string> terms;
        std::wstring ws_prefix = StringHelper::to_wstring(prefix);

        Term* prefix_term = _CLNEW Term(field.c_str(), ws_prefix.c_str());
        TermEnum* enumerator = reader->terms(prefix_term, io_ctx);

        int32_t count = 0;
        Term* lastTerm = nullptr;

        try {
            const TCHAR* prefixText = prefix_term->text();
            const TCHAR* prefixField = prefix_term->field();
            size_t prefixLen = prefix_term->textLength();

            do {
                lastTerm = enumerator->term();
                if (lastTerm != nullptr && lastTerm->field() == prefixField) {
                    size_t termLen = lastTerm->textLength();
                    if (prefixLen > termLen) {
                        break;
                    }

                    const TCHAR* tmp = lastTerm->text();

                    for (size_t i = prefixLen - 1; i != static_cast<size_t>(-1); --i) {
                        if (tmp[i] != prefixText[i]) {
                            tmp = nullptr;
                            break;
                        }
                    }
                    if (tmp == nullptr) {
                        break;
                    }

                    if (max_expansions > 0 && count >= max_expansions) {
                        break;
                    }

                    std::string term = lucene_wcstoutf8string(tmp, termLen);
                    terms.emplace_back(std::move(term));
                    count++;
                } else {
                    break;
                }
                _CLDECDELETE(lastTerm);
            } while (enumerator->next());
        }
        _CLFINALLY({
            enumerator->close();
            _CLDELETE(enumerator);
            _CLDECDELETE(lastTerm);
            _CLDECDELETE(prefix_term);
        });

        return terms;
    }

private:
    ScorerPtr prefix_scorer(const QueryExecutionContext& ctx, const std::string& binding_key) {
        auto reader = lookup_reader(_field, ctx, binding_key);
        if (!reader) {
            return std::make_shared<EmptyScorer>();
        }

        auto matching_terms =
                expand_prefix(reader.get(), _field, _prefix, _max_expansions, _context->io_ctx);

        if (matching_terms.empty()) {
            return std::make_shared<EmptyScorer>();
        }

        auto doc_bitset = std::make_shared<roaring::Roaring>();
        for (const auto& term : matching_terms) {
            auto term_wstr = StringHelper::to_wstring(term);
            auto t = make_term_ptr(_field.c_str(), term_wstr.c_str());
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

    IndexQueryContextPtr _context;
    std::wstring _field;
    std::string _prefix;
    bool _enable_scoring = false;
    int32_t _max_expansions = 50;
    bool _nullable = true;
};

} // namespace doris::segment_v2::inverted_index::query_v2
