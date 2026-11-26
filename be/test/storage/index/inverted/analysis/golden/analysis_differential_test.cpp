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

// Live differential. The old implementations of the english, standard, custom standard and chinese
// analyzers stay linked into the unit tests as oracles, and each production configuration is
// compared with its oracle token by token: on the golden families, and on random input with a new
// seed every run (ANALYSIS_DIFFERENTIAL_SEED repeats one). Until an analyzer is replaced, the
// production path runs the oracle's own code, so the two must agree; ReportsAnInjectedDifference
// shows that a disagreement is caught.

#include <CLucene.h>
#include <fmt/format.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <functional>
#include <iterator>
#include <memory>
#include <optional>
#include <random>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "CLucene/analysis/LanguageBasedAnalyzer.h"
#include "common/cast_set.h"
#include "common/check.h"
#include "common/config.h"
#include "common/exception.h"
#include "storage/index/inverted/analysis/analyzer/analyzer.h"
#include "storage/index/inverted/analysis/golden/golden_configs.h"
#include "storage/index/inverted/analysis/golden/golden_corpus.h"
#include "storage/index/inverted/analysis/golden/golden_driver.h"
#include "storage/index/inverted/analysis/golden/golden_environment.h"
#include "storage/index/inverted/analysis/tokenizer/standard/standard_tokenizer_impl.h"

// clang-format off
#include "common/compile_check_avoid_begin.h"
#include "CLucene/analysis/standard95/StandardAnalyzer.h"
#include "common/compile_check_avoid_end.h"
// clang-format on

namespace doris::segment_v2::inverted_index::golden {

namespace {

// The default max_token_length of the custom standard tokenizer.
constexpr int32_t kDefaultMaxTokenLength = 255;

struct Disagreement {
    std::string family;
    size_t line_index = 0;
    std::string line;
    GoldenLine candidate;
    GoldenLine oracle;
};

std::string describe(const Disagreement& disagreement) {
    return fmt::format("{} line {}: \"{}\"\n  candidate: {}\n  oracle:    {}", disagreement.family,
                       disagreement.line_index, escape_bytes(disagreement.line),
                       format_line(disagreement.candidate), format_line(disagreement.oracle));
}

// The first line of `family` on which `candidate` and `oracle` disagree.
std::optional<Disagreement> first_disagreement(GoldenDriver& candidate, GoldenDriver& oracle,
                                               DriveMode mode, const CorpusFamily& family) {
    for (size_t i = 0; i < family.lines.size(); ++i) {
        GoldenLine candidate_line = candidate.drive(mode, family.lines[i]);
        GoldenLine oracle_line = oracle.drive(mode, family.lines[i]);
        if (candidate_line != oracle_line) {
            return Disagreement {.family = family.name,
                                 .line_index = i,
                                 .line = family.lines[i],
                                 .candidate = std::move(candidate_line),
                                 .oracle = std::move(oracle_line)};
        }
    }
    return std::nullopt;
}

// Mostly valid UTF-8 from the scripts and classes the tokenizers tell apart, with an arbitrary
// byte now and then.
std::string random_line(GoldenRandom& random) {
    static constexpr uint32_t kRanges[][2] = {
            {0x20, 0x7E},       {0x09, 0x0D},      {0xA0, 0x24F},    {0x300, 0x36F},
            {0x370, 0x52F},     {0x590, 0x6FF},    {0x900, 0x97F},   {0xE00, 0xEFF},
            {0x1000, 0x109F},   {0x1100, 0x11FF},  {0x2000, 0x206F}, {0x3000, 0x30FF},
            {0x3400, 0x4DBF},   {0x4E00, 0x9FFF},  {0xAC00, 0xD7A3}, {0xFF00, 0xFFEF},
            {0x1F300, 0x1FAFF}, {0x20000, 0x2A6DF}};
    std::string line;
    const uint32_t count = random.below(40);
    for (uint32_t i = 0; i < count; ++i) {
        const uint32_t kind = random.below(20);
        if (kind == 0) {
            line.push_back(static_cast<char>(random.below(256)));
        } else if (kind < 8) {
            line.push_back(
                    static_cast<char>((random.below(2) == 0 ? 'a' : 'A') + random.below(26)));
        } else {
            const auto& range = kRanges[random.below(std::size(kRanges))];
            append_utf8(&line, range[0] + random.below(range[1] - range[0] + 1));
        }
    }
    return line;
}

struct DriverPair {
    std::string name;
    std::unique_ptr<GoldenDriver> candidate;
    std::unique_ptr<GoldenDriver> oracle;
};

// Random lines through every pair until `budget` runs out. Returns the first disagreement, with
// the seed that repeats it.
std::optional<std::string> random_disagreement(std::vector<DriverPair>& pairs, uint64_t seed,
                                               std::chrono::milliseconds budget) {
    GoldenRandom random(seed);
    const auto deadline = std::chrono::steady_clock::now() + budget;
    for (size_t index = 0; std::chrono::steady_clock::now() < deadline; ++index) {
        const std::string line = random_line(random);
        for (DriverPair& pair : pairs) {
            GoldenLine candidate = pair.candidate->fresh(line);
            GoldenLine oracle = pair.oracle->fresh(line);
            if (candidate != oracle) {
                return fmt::format(
                        "{} disagrees with its oracle on random line {} of seed {} "
                        "(ANALYSIS_DIFFERENTIAL_SEED={}): {}",
                        pair.name, index, seed, seed,
                        describe(Disagreement {.family = "random",
                                               .line_index = index,
                                               .line = line,
                                               .candidate = std::move(candidate),
                                               .oracle = std::move(oracle)}));
            }
        }
    }
    return std::nullopt;
}

uint64_t random_seed() {
    if (const char* seed = std::getenv("ANALYSIS_DIFFERENTIAL_SEED"); seed != nullptr) {
        return std::stoull(seed);
    }
    return (static_cast<uint64_t>(std::random_device {}()) << 32) ^
           static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count());
}

std::string property(const GoldenConfig& config, const std::string& key) {
    const auto it = config.properties.find(key);
    return it == config.properties.end() ? std::string() : it->second;
}

// lower_case and stopwords as create_builtin_analyzer applies them.
void configure(lucene::analysis::Analyzer* analyzer, const GoldenConfig& config) {
    const std::string lower_case = property(config, "lower_case");
    if (lower_case == "true") {
        analyzer->set_lowercase(true);
    } else if (lower_case == "false") {
        analyzer->set_lowercase(false);
    }
    if (property(config, "stopwords") == "none") {
        analyzer->set_stopwords(nullptr);
    } else {
        analyzer->set_stopwords(&lucene::analysis::standard95::stop_words);
    }
}

// The custom standard tokenizer's rule over StandardTokenizerImpl: a token longer than
// max_token_length is dropped and leaves its position behind.
class StandardScannerDriver final : public GoldenDriver {
public:
    explicit StandardScannerDriver(int32_t max_token_length) : _max_token_length(max_token_length) {
        size_buffer(&_scanner);
    }

    GoldenLine fresh(std::string_view line) override {
        StandardTokenizerImpl scanner;
        size_buffer(&scanner);
        return scan(&scanner, line);
    }

    GoldenLine reused(std::string_view line) override { return scan(&_scanner, line); }

private:
    void size_buffer(StandardTokenizerImpl* scanner) const {
        if (_max_token_length != kDefaultMaxTokenLength) {
            scanner->set_buffer_size(_max_token_length);
        }
    }

    GoldenLine scan(StandardTokenizerImpl* scanner, std::string_view line) const {
        GoldenLine out;
        try {
            auto reader = InvertedIndexAnalyzer::create_reader({});
            reader->init(line.data(), cast_set<int32_t>(line.size()), false);
            scanner->yyreset(reader);
            int32_t skipped = 0;
            for (auto type = scanner->get_next_token();
                 type != StandardTokenizerImpl::TokenType::EOF_TOKEN;
                 type = scanner->get_next_token()) {
                if (scanner->yylength() <= _max_token_length) {
                    out.tokens.push_back({std::string(scanner->get_text()), skipped + 1});
                    skipped = 0;
                } else {
                    ++skipped;
                }
            }
        } catch (const CLuceneError&) {
            out = threw_line();
        } catch (const Exception&) {
            out = threw_line();
        }
        return out;
    }

    const int32_t _max_token_length;
    StandardTokenizerImpl _scanner;
};

struct OraclePair {
    const GoldenConfig* candidate;
    std::function<std::unique_ptr<GoldenDriver>()> make_oracle;
};

// Every configuration that has an oracle: the builtin english, standard, unicode and chinese
// parsers without a character filter, and the custom standard tokenizer without filters.
std::vector<OraclePair> oracle_pairs(const std::vector<GoldenConfig>& configs) {
    std::vector<OraclePair> pairs;
    for (const GoldenConfig& config : configs) {
        if (config.kind != GoldenKind::ANALYZE || config.properties.contains("char_filter_type")) {
            continue;
        }
        const std::string parser = property(config, "parser");
        const GoldenConfig* candidate = &config;
        if (parser == "english") {
            pairs.push_back(
                    {candidate, [candidate] {
                         return make_lucene_driver([candidate] {
                             auto analyzer =
                                     std::make_shared<lucene::analysis::SimpleAnalyzer<char>>();
                             configure(analyzer.get(), *candidate);
                             return analyzer;
                         });
                     }});
        } else if (parser == "standard" || parser == "unicode") {
            pairs.push_back({candidate, [candidate] {
                                 return make_lucene_driver([candidate] {
                                     auto analyzer = std::make_shared<
                                             lucene::analysis::standard95::StandardAnalyzer>();
                                     configure(analyzer.get(), *candidate);
                                     return analyzer;
                                 });
                             }});
        } else if (parser == "chinese") {
            const bool coarse = property(config, "parser_mode") == "coarse_grained";
            pairs.push_back(
                    {candidate, [candidate, coarse] {
                         return make_lucene_driver([candidate, coarse] {
                             auto analyzer =
                                     std::make_shared<lucene::analysis::LanguageBasedAnalyzer>(
                                             L"chinese", false);
                             analyzer->initDict(config::inverted_index_dict_path);
                             analyzer->setMode(coarse ? lucene::analysis::AnalyzerMode::Default
                                                      : lucene::analysis::AnalyzerMode::All);
                             configure(analyzer.get(), *candidate);
                             return analyzer;
                         });
                     }});
        } else if (config.name == "custom/standard" ||
                   config.name.starts_with("custom/standard/max_token_length=")) {
            const int32_t length =
                    config.name == "custom/standard"
                            ? kDefaultMaxTokenLength
                            : std::stoi(config.name.substr(config.name.find('=') + 1));
            pairs.push_back({candidate,
                             [length] { return std::make_unique<StandardScannerDriver>(length); }});
        }
    }
    return pairs;
}

std::unique_ptr<GoldenDriver> candidate_driver(const GoldenConfig& config) {
    std::unique_ptr<GoldenDriver> driver;
    const Status status = make_golden_driver(config, &driver);
    EXPECT_TRUE(status.ok()) << status.to_string();
    return driver;
}

// Changes the output of the lines `affected` selects, by one byte at the end of the last token.
class InjectedDifference final : public GoldenDriver {
public:
    InjectedDifference(std::unique_ptr<GoldenDriver> driver,
                       std::function<bool(std::string_view)> affected)
            : _driver(std::move(driver)), _affected(std::move(affected)) {}

    GoldenLine fresh(std::string_view line) override { return change(line, _driver->fresh(line)); }
    GoldenLine reused(std::string_view line) override {
        return change(line, _driver->reused(line));
    }

private:
    GoldenLine change(std::string_view line, GoldenLine out) const {
        if (_affected(line) && !out.tokens.empty()) {
            out.tokens.back().bytes += 'x';
        }
        return out;
    }

    std::unique_ptr<GoldenDriver> _driver;
    std::function<bool(std::string_view)> _affected;
};

} // namespace

class AnalysisDifferential : public testing::Test {
protected:
    void SetUp() override {
        const Status status = _environment.init(_configs);
        ASSERT_TRUE(status.ok()) << status.to_string();
    }

    const std::vector<GoldenConfig> _configs = golden_configs();
    GoldenEnvironment _environment;
};

TEST_F(AnalysisDifferential, CandidatesAgreeWithTheirOracles) {
    const std::vector<OraclePair> pairs = oracle_pairs(_configs);
    // english, standard and unicode with three configurations each, chinese with six, and eleven
    // custom standard tokenizers.
    ASSERT_EQ(pairs.size(), 26);
    const std::vector<CorpusFamily> families = default_families(_environment.corpus_sources());
    struct Job {
        const OraclePair* pair = nullptr;
        const CorpusFamily* family = nullptr;
        DriveMode mode = DriveMode::FRESH;
        std::optional<Disagreement> disagreement;
        std::string error;
    };
    std::vector<Job> jobs;
    for (const OraclePair& pair : pairs) {
        // Built once here, so the dictionaries are loaded before the jobs run in parallel.
        ASSERT_NE(candidate_driver(*pair.candidate), nullptr);
        pair.make_oracle();
        for (const CorpusFamily& family : families) {
            for (DriveMode mode : {DriveMode::FRESH, DriveMode::REUSED}) {
                Job job;
                job.pair = &pair;
                job.family = &family;
                job.mode = mode;
                jobs.push_back(std::move(job));
            }
        }
    }
    parallel_for(jobs.size(), [&](size_t i) {
        Job& job = jobs[i];
        std::unique_ptr<GoldenDriver> candidate;
        DORIS_CHECK(make_golden_driver(*job.pair->candidate, &candidate).ok());
        try {
            const std::unique_ptr<GoldenDriver> oracle = job.pair->make_oracle();
            job.disagreement = first_disagreement(*candidate, *oracle, job.mode, *job.family);
        } catch (const std::exception& e) {
            job.error = e.what();
        }
    });
    for (const Job& job : jobs) {
        EXPECT_TRUE(job.error.empty())
                << job.pair->candidate->name << " (" << drive_mode_name(job.mode) << ") on "
                << job.family->name << ": " << job.error;
        if (job.disagreement) {
            ADD_FAILURE() << job.pair->candidate->name << " (" << drive_mode_name(job.mode)
                          << ") disagrees with its oracle on " << describe(*job.disagreement);
        }
    }
    std::vector<DriverPair> drivers;
    for (const OraclePair& pair : pairs) {
        drivers.push_back(
                {pair.candidate->name, candidate_driver(*pair.candidate), pair.make_oracle()});
    }
    if (const auto failure =
                random_disagreement(drivers, random_seed(), std::chrono::milliseconds(1000))) {
        ADD_FAILURE() << *failure;
    }
}

TEST_F(AnalysisDifferential, ReportsAnInjectedDifference) {
    const std::vector<OraclePair> pairs = oracle_pairs(_configs);
    const auto english = std::ranges::find_if(pairs, [](const OraclePair& pair) {
        return pair.candidate->name == "builtin/english/lower_case=unset/stopwords=unset";
    });
    ASSERT_NE(english, pairs.end());
    const CorpusFamily family = default_families(_environment.corpus_sources()).front();

    // The first line with a token, after a few lines that agree.
    const std::unique_ptr<GoldenDriver> oracle = english->make_oracle();
    size_t target = 5;
    while (oracle->fresh(family.lines.at(target)).tokens.empty()) {
        ++target;
    }
    const std::string target_line = family.lines[target];
    InjectedDifference candidate(candidate_driver(*english->candidate),
                                 [&](std::string_view line) { return line == target_line; });
    const std::unique_ptr<GoldenDriver> fresh_oracle = english->make_oracle();
    const auto disagreement =
            first_disagreement(candidate, *fresh_oracle, DriveMode::FRESH, family);
    ASSERT_TRUE(disagreement.has_value());
    EXPECT_EQ(disagreement->line_index, target);
    EXPECT_NE(describe(*disagreement).find("candidate"), std::string::npos);

    // Random input catches a difference too, and names the seed that repeats it.
    std::vector<DriverPair> drivers;
    drivers.push_back({english->candidate->name,
                       std::make_unique<InjectedDifference>(
                               candidate_driver(*english->candidate),
                               [](std::string_view line) {
                                   return line.find_first_of("aeiou") != std::string_view::npos;
                               }),
                       english->make_oracle()});
    const auto failure = random_disagreement(drivers, 42, std::chrono::milliseconds(1000));
    ASSERT_TRUE(failure.has_value());
    EXPECT_NE(failure->find("ANALYSIS_DIFFERENTIAL_SEED=42"), std::string::npos) << *failure;
}

} // namespace doris::segment_v2::inverted_index::golden
