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
#include "storage/index/inverted/analysis/analyzer/custom_analyzer_config.h"
#include "storage/index/inverted/analysis/text_analyzer.h"
#include "util/hash_util.hpp"

// Tokenizer throughput along the paths the consumers take: "row/" makes a stream per row, as a
// query does, and "reuse/" keeps one stream for all rows, as the SNII writer does. The analyzers
// are built through the production entry points. The corpus is a directory
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

using AnalyzerFactory = std::function<TextAnalyzerPtr()>;

// A builtin parser with lower_case=true, as the writers configure it.
inline TextAnalyzerPtr builtin(InvertedIndexParserType type, const std::string& mode = "") {
    InvertedIndexAnalyzerConfig config;
    config.parser_type = type;
    config.parser_mode = mode;
    config.lower_case = INVERTED_INDEX_PARSER_TRUE;
    return InvertedIndexAnalyzer::create_analyzer(&config);
}

inline TextAnalyzerPtr custom(const std::string& tokenizer,
                              const std::vector<std::string>& filters) {
    CustomAnalyzerConfig::Builder builder;
    builder.with_tokenizer_config(tokenizer, Settings());
    for (const auto& filter : filters) {
        builder.add_token_filter_config(filter, Settings());
    }
    return CustomAnalyzerProvider(builder.build()).get_analyzer();
}

// The non-empty terms of `row`, read as the writers read them.
inline uint64_t consume(TextTokenStream& stream, TokenBatch& batch, std::string_view row) {
    uint64_t tokens = 0;
    int32_t position = 0;
    stream.reset(row);
    while (stream.next_batch(batch)) {
        for (const TokenRef& token : batch.tokens()) {
            if (token.size != 0) {
                position += token.position_increment;
                char first = token.data[0];
                benchmark::DoNotOptimize(first);
                benchmark::DoNotOptimize(position);
                ++tokens;
            }
        }
    }
    batch.release_oversized();
    return tokens;
}

// A new stream for every row, as a query reads a value.
inline void per_row_stream(benchmark::State& state, const AnalyzerFactory& factory,
                           const std::shared_ptr<const Corpus>& corpus) {
    const TextAnalyzerPtr analyzer = factory();
    TokenBatch batch;
    uint64_t tokens = 0;
    for (auto _ : state) {
        for (const std::string_view row : corpus->rows) {
            const std::unique_ptr<TextTokenStream> stream = analyzer->create_stream();
            tokens += consume(*stream, batch, row);
        }
    }
    state.SetBytesProcessed(state.iterations() * static_cast<int64_t>(corpus->bytes.size()));
    state.SetItemsProcessed(static_cast<int64_t>(tokens));
}

// One stream for all rows, as the SNII writer reads values: the distance to per_row_stream is
// what making a stream costs.
inline void reused_stream(benchmark::State& state, const AnalyzerFactory& factory,
                          const std::shared_ptr<const Corpus>& corpus) {
    const TextAnalyzerPtr analyzer = factory();
    const std::unique_ptr<TextTokenStream> stream = analyzer->create_stream();
    TokenBatch batch;
    uint64_t tokens = 0;
    for (auto _ : state) {
        for (const std::string_view row : corpus->rows) {
            tokens += consume(*stream, batch, row);
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
            const TextAnalyzerPtr analyzer = factory();
            std::unique_ptr<TextTokenStream> stream = analyzer->create_stream();
            TokenBatch batch;
            for (const auto& [corpus_name, corpus] : corpora) {
                for (size_t row = 0; row < corpus->rows.size(); ++row) {
                    const std::string_view value = corpus->rows[row];
                    uint64_t tokens = 0;
                    uint64_t digest = 0;
                    bool threw = false;
                    try {
                        int32_t position = 0;
                        stream->reset(value);
                        while (stream->next_batch(batch)) {
                            for (const TokenRef& token : batch.tokens()) {
                                if (token.size == 0) {
                                    continue;
                                }
                                position += token.position_increment;
                                digest = HashUtil::xxHash64WithSeed(token.data, token.size, digest);
                                digest = HashUtil::xxHash64WithSeed(
                                        reinterpret_cast<const char*>(&position), sizeof(position),
                                        digest);
                                ++tokens;
                            }
                        }
                        batch.release_oversized();
                    } catch (const Exception&) {
                        threw = true;
                        // A stream is not used again after it threw.
                        stream = analyzer->create_stream();
                        batch.clear();
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
