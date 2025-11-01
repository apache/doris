// be/src/olap/rowset/segment_v2/inverted_index/query_v2/phrase_query/multi_phrase_weight.h

#pragma once

#include "olap/rowset/segment_v2/index_query_context.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/bit_set_query/bit_set_scorer.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/const_score_query/const_score_scorer.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/null_bitmap_fetcher.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/phrase_query/phrase_scorer.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/postings/loaded_postings.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/union/simple_union.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/weight.h"
#include "olap/rowset/segment_v2/inverted_index/util/string_helper.h"

namespace doris::segment_v2::inverted_index::query_v2 {

class MultiPhraseWeight : public Weight {
public:
    MultiPhraseWeight(IndexQueryContextPtr context, std::wstring field,
                      std::vector<TermInfo> term_infos, SimilarityPtr similarity,
                      bool enable_scoring)
            : _context(std::move(context)),
              _field(std::move(field)),
              _term_infos(std::move(term_infos)),
              _similarity(std::move(similarity)),
              _enable_scoring(enable_scoring) {}
    ~MultiPhraseWeight() override = default;

    ScorerPtr scorer(const QueryExecutionContext& ctx, const std::string& binding_key) override {
        auto phrase = phrase_scorer(ctx, binding_key);
        auto logical_field = logical_field_or_fallback(ctx, binding_key, _field);
        auto null_bitmap = FieldNullBitmapFetcher::fetch(ctx, logical_field);

        auto doc_bitset = std::make_shared<roaring::Roaring>();
        if (phrase) {
            uint32_t doc = phrase->doc();
            if (doc == TERMINATED) {
                doc = phrase->advance();
            }
            while (doc != TERMINATED) {
                doc_bitset->add(doc);
                doc = phrase->advance();
            }
        }

        auto bit_set =
                std::make_shared<BitSetScorer>(std::move(doc_bitset), std::move(null_bitmap));
        if (!phrase) {
            return bit_set;
        }
        return std::make_shared<ConstScoreScorer<BitSetScorerPtr>>(std::move(bit_set));
    }

private:
    ScorerPtr phrase_scorer(const QueryExecutionContext& ctx, const std::string& binding_key) {
        auto reader = lookup_reader(_field, ctx, binding_key);
        if (!reader) {
            throw Exception(ErrorCode::NOT_FOUND, "Reader not found for field '{}'",
                            StringHelper::to_string(_field));
        }

        std::vector<std::pair<size_t, PostingsPtr>> term_postings_list;
        for (const auto& term_info : _term_infos) {
            size_t offset = term_info.position;
            if (term_info.is_single_term()) {
                auto posting =
                        create_position_posting(reader.get(), _field, term_info.get_single_term(),
                                                _enable_scoring, _context->io_ctx);
                if (posting) {
                    term_postings_list.emplace_back(offset, std::move(posting));
                } else {
                    return nullptr;
                }
            } else {
                const auto& terms = term_info.get_multi_terms();
                std::vector<PositionPostingsPtr> postings;
                for (const auto& term : terms) {
                    auto posting = create_position_posting(reader.get(), _field, term,
                                                           _enable_scoring, _context->io_ctx);
                    if (posting) {
                        postings.push_back(posting);
                    }
                }
                if (postings.empty()) {
                    return nullptr;
                }
                auto union_posting = SimpleUnion<PositionPostingsPtr>::create(std::move(postings));
                term_postings_list.emplace_back(offset, std::move(union_posting));
            }
        }
        return PhraseScorer<PostingsPtr>::create(term_postings_list, _similarity, 0);
    }

    IndexQueryContextPtr _context;
    std::wstring _field;
    std::vector<TermInfo> _term_infos;
    SimilarityPtr _similarity;
    bool _enable_scoring = false;
};

} // namespace doris::segment_v2::inverted_index::query_v2