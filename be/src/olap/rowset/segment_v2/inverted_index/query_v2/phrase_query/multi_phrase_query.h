// be/src/olap/rowset/segment_v2/inverted_index/query_v2/phrase_query/multi_phrase_query.h

#pragma once

#include "olap/rowset/segment_v2/index_query_context.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/phrase_query/multi_phrase_weight.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/query.h"
#include "olap/rowset/segment_v2/inverted_index/similarity/bm25_similarity.h"

namespace doris::segment_v2::inverted_index::query_v2 {

class MultiPhraseQuery : public Query {
public:
    MultiPhraseQuery(IndexQueryContextPtr context, std::wstring field,
                     std::vector<TermInfo> term_infos)
            : _context(std::move(context)),
              _field(std::move(field)),
              _term_infos(std::move(term_infos)) {}
    ~MultiPhraseQuery() override = default;

    WeightPtr weight(bool enable_scoring) override {
        if (_term_infos.size() < 2) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Multi-phrase query requires at least 2 terms, got {}",
                            _term_infos.size());
        }

        SimilarityPtr bm25_similarity;
        if (enable_scoring) {
            bm25_similarity = std::make_shared<BM25Similarity>();
            std::vector<std::wstring> all_terms;
            for (const auto& term_info : _term_infos) {
                if (term_info.is_single_term()) {
                    all_terms.push_back(StringHelper::to_wstring(term_info.get_single_term()));
                } else {
                    for (const auto& term : term_info.get_multi_terms()) {
                        all_terms.push_back(StringHelper::to_wstring(term));
                    }
                }
            }
            bm25_similarity->for_terms(_context, _field, all_terms);
        }

        return std::make_shared<MultiPhraseWeight>(_context, _field, _term_infos, bm25_similarity,
                                                   enable_scoring);
    }

private:
    IndexQueryContextPtr _context;
    std::wstring _field;
    std::vector<TermInfo> _term_infos;
};

} // namespace doris::segment_v2::inverted_index::query_v2