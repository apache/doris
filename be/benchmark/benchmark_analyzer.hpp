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

#include <benchmark/benchmark.h>

#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "storage/index/inverted/analysis/analyzer/analyzer.h"
#include "storage/index/inverted/analysis/analyzer/custom_analyzer.h"
#include "storage/index/inverted/analysis/token_stream.h"
#include "util/hash_util.hpp"

// Tokenizer throughput along the path the SNII writer takes for one row. The corpus is a directory
// of `*.rows` files (little-endian u32 length + raw bytes per row) named by
// ANALYZER_BENCH_CORPUS_DIR; without it nothing is registered. ANALYZER_BENCH_CORPUS_MB caps the
// bytes used from each file (default 8) and ANALYZER_BENCH_DICT_DIR is the dictionary directory
// for the dictionary-based parsers.
//
// With ANALYZER_BENCH_DIGEST=<file>, only the benchmark "digest" is registered. It writes one line
// per analyzer, corpus and row, "<analyzer> <corpus> <row> <tokens> <digest>", over the terms a row
// gives on the production path, as the writers see them: empty terms dropped, positions summed.
// Running the baseline binary and a new one over the same corpora and diffing the two files names
// every row whose tokens changed; see be/benchmark/analyzer_benchmark.md.
namespace doris::segment_v2::inverted_index::analyzer_benchmark {

struct Corpus {
    std::string bytes;
    std::vector<std::string_view> rows;
};

inline std::shared_ptr<const Corpus> load_corpus(const std::filesystem::path& path,
                                                 size_t max_bytes) {
    auto corpus = std::make_shared<Corpus>();
    std::ifstream in(path, std::ios::binary);
    corpus->bytes.reserve(max_bytes + (1U << 20));
    std::vector<std::pair<size_t, size_t>> extents;
    uint32_t length = 0;
    while (corpus->bytes.size() < max_bytes &&
           in.read(reinterpret_cast<char*>(&length), sizeof(length))) {
        const size_t start = corpus->bytes.size();
        corpus->bytes.resize(start + length);
        in.read(corpus->bytes.data() + start, length);
        extents.emplace_back(start, length);
    }
    corpus->rows.reserve(extents.size());
    for (const auto& [start, size] : extents) {
        corpus->rows.emplace_back(corpus->bytes.data() + start, size);
    }
    return corpus;
}

using AnalyzerFactory = std::function<AnalyzerPtr()>;

inline AnalyzerPtr builtin(InvertedIndexParserType type, const std::string& mode = "") {
    return InvertedIndexAnalyzer::create_builtin_analyzer(type, mode, "true", "");
}

inline AnalyzerPtr custom(const std::string& tokenizer, const std::vector<std::string>& filters) {
    CustomAnalyzer::Builder builder;
    builder.with_tokenizer(tokenizer, Settings());
    for (const auto& filter : filters) {
        builder.add_token_filter(filter, Settings());
    }
    return builder.build();
}

inline uint64_t consume(lucene::analysis::TokenStream* stream, lucene::analysis::Token* token) {
    uint64_t tokens = 0;
    int32_t position = 0;
    while (stream->next(token)) {
        const size_t length = token->termLength<char>();
        if (length != 0) {
            position += token->getPositionIncrement();
            benchmark::DoNotOptimize(token->termBuffer<char>()[0]);
            benchmark::DoNotOptimize(position);
            ++tokens;
        }
    }
    return tokens;
}

// The production row: tokenStream() builds a stream per row, as SniiIndexColumnWriter does.
inline void per_row_stream(benchmark::State& state, const AnalyzerFactory& factory,
                           const std::shared_ptr<const Corpus>& corpus) {
    AnalyzerPtr analyzer = factory();
    ReaderPtr reader = InvertedIndexAnalyzer::create_reader({});
    uint64_t tokens = 0;
    for (auto _ : state) {
        for (const std::string_view row : corpus->rows) {
            reader->init(row.data(), static_cast<int32_t>(row.size()), false);
            std::unique_ptr<lucene::analysis::TokenStream> stream(
                    analyzer->tokenStream(L"", reader));
            lucene::analysis::Token token;
            tokens += consume(stream.get(), &token);
            stream->close();
        }
    }
    state.SetBytesProcessed(state.iterations() * static_cast<int64_t>(corpus->bytes.size()));
    state.SetItemsProcessed(static_cast<int64_t>(tokens));
}

// One stream across rows, the shape the gram tokenizer already uses: the distance to
// per_row_stream is what the per-row object graph costs.
inline void reused_stream(benchmark::State& state, const AnalyzerFactory& factory,
                          const std::shared_ptr<const Corpus>& corpus) {
    AnalyzerPtr analyzer = factory();
    ReaderPtr reader = InvertedIndexAnalyzer::create_reader({});
    uint64_t tokens = 0;
    lucene::analysis::Token token;
    for (auto _ : state) {
        for (const std::string_view row : corpus->rows) {
            reader->init(row.data(), static_cast<int32_t>(row.size()), false);
            auto* stream = analyzer->reusableTokenStream(L"", reader);
            stream->reset();
            tokens += consume(stream, &token);
        }
    }
    state.SetBytesProcessed(state.iterations() * static_cast<int64_t>(corpus->bytes.size()));
    state.SetItemsProcessed(static_cast<int64_t>(tokens));
}

// One line per row: the number of non-empty terms and a digest of their bytes and absolute
// positions, or "threw" for a row whose analysis threw.
inline void write_digests(benchmark::State& state,
                          const std::map<std::string, AnalyzerFactory>& analyzers,
                          const std::map<std::string, std::shared_ptr<const Corpus>>& corpora,
                          const std::string& path) {
    for (auto _ : state) {
        std::ofstream out(path);
        for (const auto& [name, factory] : analyzers) {
            AnalyzerPtr analyzer = factory();
            ReaderPtr reader = InvertedIndexAnalyzer::create_reader({});
            for (const auto& [corpus_name, corpus] : corpora) {
                for (size_t row = 0; row < corpus->rows.size(); ++row) {
                    const std::string_view value = corpus->rows[row];
                    uint64_t tokens = 0;
                    uint64_t digest = 0;
                    bool threw = false;
                    try {
                        reader->init(value.data(), static_cast<int32_t>(value.size()), false);
                        std::unique_ptr<lucene::analysis::TokenStream> stream(
                                analyzer->tokenStream(L"", reader));
                        lucene::analysis::Token token;
                        int32_t position = 0;
                        while (stream->next(&token)) {
                            const size_t length = token.termLength<char>();
                            if (length == 0) {
                                continue;
                            }
                            position += token.getPositionIncrement();
                            digest = HashUtil::xxHash64WithSeed(token.termBuffer<char>(), length,
                                                                digest);
                            digest = HashUtil::xxHash64WithSeed(
                                    reinterpret_cast<const char*>(&position), sizeof(position),
                                    digest);
                            ++tokens;
                        }
                        stream->close();
                    } catch (const CLuceneError&) {
                        threw = true;
                    } catch (const Exception&) {
                        threw = true;
                    }
                    out << name << ' ' << corpus_name << ' ' << row << ' ';
                    if (threw) {
                        out << "threw\n";
                    } else {
                        out << tokens << ' ' << std::hex << digest << std::dec << '\n';
                    }
                }
            }
        }
    }
}

inline const bool registered = [] {
    const char* directory = std::getenv("ANALYZER_BENCH_CORPUS_DIR");
    if (directory == nullptr) {
        return false;
    }
    const char* megabytes = std::getenv("ANALYZER_BENCH_CORPUS_MB");
    const size_t max_bytes = static_cast<size_t>(megabytes == nullptr ? 8 : std::atoi(megabytes))
                             << 20;
    const char* dict = std::getenv("ANALYZER_BENCH_DICT_DIR");

    // The dictionary directory has to reach config before the first factory call.
    const std::string dict_path = dict == nullptr ? "" : dict;
    const auto with_dict = [dict_path](AnalyzerFactory factory) -> AnalyzerFactory {
        return [dict_path, factory = std::move(factory)]() {
            config::inverted_index_dict_path = dict_path;
            return factory();
        };
    };

    const std::map<std::string, AnalyzerFactory> analyzers {
            {"english", [] { return builtin(InvertedIndexParserType::PARSER_ENGLISH); }},
            {"standard", [] { return builtin(InvertedIndexParserType::PARSER_STANDARD); }},
            {"basic", [] { return builtin(InvertedIndexParserType::PARSER_BASIC); }},
            {"chinese_coarse", with_dict([] {
                 return builtin(InvertedIndexParserType::PARSER_CHINESE,
                                INVERTED_INDEX_PARSER_COARSE_GRANULARITY);
             })},
            {"chinese_fine", with_dict([] {
                 return builtin(InvertedIndexParserType::PARSER_CHINESE,
                                INVERTED_INDEX_PARSER_FINE_GRANULARITY);
             })},
            {"icu", with_dict([] { return builtin(InvertedIndexParserType::PARSER_ICU); })},
            {"ik_smart", with_dict([] {
                 return builtin(InvertedIndexParserType::PARSER_IK, INVERTED_INDEX_PARSER_SMART);
             })},
            {"ik_max_word", with_dict([] {
                 return builtin(InvertedIndexParserType::PARSER_IK, INVERTED_INDEX_PARSER_MAX_WORD);
             })},
            {"kuromoji", with_dict([] {
                 config::enable_kuromoji_analyzer = true;
                 return builtin(InvertedIndexParserType::PARSER_KUROMOJI);
             })},
            {"custom_standard_lower", [] { return custom("standard", {"lowercase"}); }},
            {"custom_basic_lower", [] { return custom("basic", {"lowercase"}); }},
            {"custom_keyword_lower", [] { return custom("keyword", {"lowercase"}); }},
            {"custom_standard_folding",
             [] {
                 return custom("standard", {"asciifolding", "lowercase"});
             }},
            {"custom_standard_word_delimiter",
             [] {
                 return custom("standard", {"word_delimiter", "lowercase"});
             }},
    };

    std::map<std::string, std::shared_ptr<const Corpus>> corpora;
    for (const auto& entry : std::filesystem::directory_iterator(directory)) {
        if (entry.path().extension() == ".rows") {
            corpora.emplace(entry.path().stem().string(), load_corpus(entry.path(), max_bytes));
        }
    }
    if (const char* digest_path = std::getenv("ANALYZER_BENCH_DIGEST"); digest_path != nullptr) {
        benchmark::RegisterBenchmark(
                "digest",
                [analyzers, corpora, path = std::string(digest_path)](benchmark::State& state) {
                    write_digests(state, analyzers, corpora, path);
                })
                ->Iterations(1)
                ->Unit(benchmark::kSecond);
        return true;
    }
    for (const auto& [corpus_name, corpus] : corpora) {
        for (const auto& [name, factory] : analyzers) {
            benchmark::RegisterBenchmark(("row/" + name + "/" + corpus_name).c_str(),
                                         [factory, corpus](benchmark::State& state) {
                                             per_row_stream(state, factory, corpus);
                                         })
                    ->Unit(benchmark::kMillisecond)
                    ->MinTime(1.0);
            benchmark::RegisterBenchmark(("reuse/" + name + "/" + corpus_name).c_str(),
                                         [factory, corpus](benchmark::State& state) {
                                             reused_stream(state, factory, corpus);
                                         })
                    ->Unit(benchmark::kMillisecond)
                    ->MinTime(1.0);
        }
    }
    return true;
}();

} // namespace doris::segment_v2::inverted_index::analyzer_benchmark
