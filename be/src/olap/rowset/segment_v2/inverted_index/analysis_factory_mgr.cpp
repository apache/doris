
#include "analysis_factory_mgr.h"

#include "olap/rowset/segment_v2/inverted_index/token_filter/ascii_folding_filter_factory.h"
#include "olap/rowset/segment_v2/inverted_index/token_filter/loser_case_filter_factory.h"
#include "olap/rowset/segment_v2/inverted_index/token_filter/word_delimiter_filter_factory.h"
#include "olap/rowset/segment_v2/inverted_index/tokenizer/keyword/keyword_tokenizer_factory.h"
#include "olap/rowset/segment_v2/inverted_index/tokenizer/ngram/edge_ngram_tokenizer_factory.h"
#include "olap/rowset/segment_v2/inverted_index/tokenizer/standard/standard_tokenizer_factory.h"

namespace doris::segment_v2::inverted_index {

void AnalysisFactoryMgr::initialise() {
    // tokenizer
    registerFactory("standard", []() { return std::make_shared<StandardTokenizerFactory>(); });
    registerFactory("keyword", []() { return std::make_shared<KeywordTokenizerFactory>(); });
    registerFactory("edge_ngram", []() { return std::make_shared<EdgeNGramTokenizerFactory>(); });
    registerFactory("ngram", []() { return std::make_shared<NGramTokenizerFactory>(); });

    // token_filter
    registerFactory("lowercase", []() { return std::make_shared<LowerCaseFilterFactory>(); });
    registerFactory("asciifolding", []() { return std::make_shared<ASCIIFoldingFilterFactory>(); });
    registerFactory("word_delimiter",
                    []() { return std::make_shared<WordDelimiterFilterFactory>(); });
}

void AnalysisFactoryMgr::registerFactory(const std::string& name, FactoryCreator creator) {
    registry_[name] = std::move(creator);
}

template <typename FactoryType>
std::shared_ptr<FactoryType> AnalysisFactoryMgr::create(const std::string& name,
                                                        const Settings& params) {
    auto it = registry_.find(name);
    if (it == registry_.end()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Unknown factory name: {}", name);
    }

    auto factory = std::static_pointer_cast<FactoryType>(it->second());
    factory->initialize(params);
    return factory;
}

template std::shared_ptr<TokenizerFactory> AnalysisFactoryMgr::create<TokenizerFactory>(
        const std::string&, const Settings&);

template std::shared_ptr<TokenFilterFactory> AnalysisFactoryMgr::create<TokenFilterFactory>(
        const std::string&, const Settings&);

} // namespace doris::segment_v2::inverted_index