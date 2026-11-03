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

// Frozen analyzer output. A change in what an analyzer emits raises no error anywhere else: queries
// just stop finding rows that were indexed with the old tokens. So every configuration in
// golden_configs.cpp is run over every family in golden_corpus.cpp, and the digest of the tokens
// must equal the one in analysis_golden.txt, which was generated on the analysis layer before it
// moved off CLucene. Lines in that file are added, never changed or removed; the lines named
// "write/..." belong to AnalysisWriteGolden.
//
// Environment variables:
//   ANALYSIS_GOLDEN_REGEN=<commit>  append the digests the file does not have yet under a comment
//                                   naming the commit of the analysis code; never rewrites one
//   ANALYSIS_GOLDEN_WRITE=path  append every computed digest to path, to compare two builds
//   ANALYSIS_GOLDEN_DUMP=dir    write the tokens of every line to dir, to diff with another binary
//   ANALYSIS_GOLDEN_ONLY=text   check only the configurations whose name contains text
//   ANALYSIS_GOLDEN_FULL=1      also scan every code point against analysis_golden_full.txt
//   ANALYSIS_GOLDEN_TIMING=1    print the slowest configurations

#include <fmt/format.h>
#include <gtest/gtest.h>
#include <unicode/locid.h>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <iterator>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "storage/index/inverted/analysis/golden/golden_configs.h"
#include "storage/index/inverted/analysis/golden/golden_corpus.h"
#include "storage/index/inverted/analysis/golden/golden_driver.h"
#include "storage/index/inverted/analysis/golden/golden_environment.h"
#include "testutil/golden_file.h"
#include "util/defer_op.h"

namespace doris::segment_v2::inverted_index::golden {

namespace {

constexpr const char* kGoldenFile =
        "./be/test/storage/index/inverted/analysis/golden/analysis_golden.txt";
constexpr const char* kFullGoldenFile =
        "./be/test/storage/index/inverted/analysis/golden/analysis_golden_full.txt";

// Configurations whose fresh and reused streams disagree on the old code, with the mode whose
// output is frozen. The new streams cannot disagree, so an entry goes away once its analyzer is
// replaced.
const std::map<std::string, DriveMode> kModesDisagree = {};

// The lines of analysis_golden.txt that AnalysisWriteGolden owns.
constexpr const char* kWritePrefix = "write/";

std::string env(const char* name) {
    const char* value = std::getenv(name);
    return value == nullptr ? std::string() : std::string(value);
}

std::string golden_line(const std::string& key, const FamilyDigest& digest) {
    return fmt::format("{} {} {:016x}", key, digest.tokens, digest.digest);
}

bool owns_analysis_line(const std::string& name) {
    return !name.starts_with(kWritePrefix);
}

void write_dump(const std::string& dir, const std::string& key, DriveMode mode,
                const std::string& dump) {
    std::string name;
    for (char c : key) {
        name += std::isalnum(static_cast<unsigned char>(c)) || c == '=' || c == '.' ? c : '_';
    }
    std::filesystem::create_directories(dir);
    std::ofstream out(std::filesystem::path(dir) / (name + "." + drive_mode_name(mode) + ".txt"));
    out << dump;
}

// Makes the driver of a configuration: make_golden_driver(), or make_golden_text_driver().
using DriverFactory = std::function<Status(const GoldenConfig&, std::unique_ptr<GoldenDriver>*)>;

// One configuration over one family in one mode.
struct GoldenJob {
    const GoldenConfig* config = nullptr;
    const CorpusFamily* family = nullptr;
    DriveMode mode = DriveMode::FRESH;
    // Filled by the job.
    FamilyDigest digest;
    std::string error;
    std::string dump;
    double seconds = 0;
};

void run_job(GoldenJob* job, bool dump, const DriverFactory& make_driver) {
    const auto started = std::chrono::steady_clock::now();
    // A driver per job, so no family sees the state another one left.
    std::unique_ptr<GoldenDriver> driver;
    if (const Status status = make_driver(*job->config, &driver); !status.ok()) {
        job->error = status.to_string();
    } else {
        try {
            job->digest =
                    digest_family(*driver, job->mode, *job->family, dump ? &job->dump : nullptr);
        } catch (const std::exception& e) {
            job->error = std::string("an error production does not catch, on ") + e.what();
        }
    }
    job->seconds =
            std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();
}

DriveMode frozen_mode(const GoldenConfig& config) {
    const auto disagreement = kModesDisagree.find(config.name);
    return disagreement == kModesDisagree.end() ? DriveMode::FRESH : disagreement->second;
}

// One job per configuration, family and mode, for the requested modes and the frozen one. The jobs
// of one configuration and family are adjacent.
std::vector<GoldenJob> make_jobs(const std::vector<const GoldenConfig*>& configs,
                                 const std::vector<CorpusFamily>& families,
                                 const std::vector<DriveMode>& requested_modes) {
    std::vector<GoldenJob> jobs;
    for (const GoldenConfig* config : configs) {
        std::vector<DriveMode> modes = requested_modes;
        if (std::ranges::find(modes, frozen_mode(*config)) == modes.end()) {
            modes.push_back(frozen_mode(*config));
        }
        for (const CorpusFamily& family : families) {
            for (DriveMode mode : modes) {
                GoldenJob job;
                job.config = config;
                job.family = &family;
                job.mode = mode;
                jobs.push_back(std::move(job));
            }
        }
    }
    return jobs;
}

// Checks the jobs of the configuration and family that start at `begin`, and returns where the
// next ones start.
size_t check_jobs(const std::vector<GoldenJob>& jobs, size_t begin, const std::string& dump_dir,
                  test::GoldenFile* file) {
    const GoldenConfig& config = *jobs[begin].config;
    const CorpusFamily& family = *jobs[begin].family;
    const std::string key = config.name + " " + family.name;
    std::map<DriveMode, FamilyDigest> digests;
    bool failed = false;
    size_t end = begin;
    for (; end < jobs.size() && jobs[end].config == &config && jobs[end].family == &family; ++end) {
        const GoldenJob& job = jobs[end];
        if (!dump_dir.empty()) {
            write_dump(dump_dir, key, job.mode, job.dump);
        }
        EXPECT_TRUE(job.error.empty())
                << key << " (" << drive_mode_name(job.mode) << ") failed: " << job.error << "\n"
                << describe(config);
        failed |= !job.error.empty();
        digests[job.mode] = job.digest;
    }
    if (failed) {
        return end;
    }
    if (digests.size() == 2 && !kModesDisagree.contains(config.name)) {
        EXPECT_EQ(digests[DriveMode::FRESH], digests[DriveMode::REUSED])
                << "fresh and reused streams disagree on " << key << ": fresh "
                << golden_line(key, digests[DriveMode::FRESH]) << ", reused "
                << golden_line(key, digests[DriveMode::REUSED]) << "\n"
                << describe(config);
    }
    const FamilyDigest& digest = digests.at(frozen_mode(config));
    file->check(config.name, family.name, digest.tokens, digest.digest,
                describe(config) +
                        "\nRun the old and the new binary with ANALYSIS_GOLDEN_DUMP and "
                        "ANALYSIS_GOLDEN_ONLY and diff the dumps.");
    return end;
}

void print_timing(const std::vector<GoldenJob>& jobs, double wall) {
    std::map<std::string, double> config_seconds;
    std::map<std::string, double> family_seconds;
    double total = 0;
    for (const GoldenJob& job : jobs) {
        config_seconds[job.config->name] += job.seconds;
        family_seconds[job.family->name] += job.seconds;
        total += job.seconds;
    }
    std::vector<std::pair<double, std::string>> slowest;
    for (const auto& [name, seconds] : config_seconds) {
        slowest.emplace_back(seconds, name);
    }
    std::ranges::sort(slowest, std::greater<>());
    std::cout << fmt::format(
            "analysis golden: {} jobs, {:.1f} s of work in {:.1f} s on {} threads\n", jobs.size(),
            total, wall, golden_threads());
    for (size_t i = 0; i < std::min<size_t>(slowest.size(), 15); ++i) {
        std::cout << fmt::format("  {:7.2f} s  {}\n", slowest[i].first, slowest[i].second);
    }
    for (const auto& [name, seconds] : family_seconds) {
        std::cout << fmt::format("  {:7.2f} s  family {}\n", seconds, name);
    }
}

// The configurations ANALYSIS_GOLDEN_ONLY selects: those whose name contains it, or all.
std::vector<const GoldenConfig*> selected_configs(const std::vector<GoldenConfig>& configs) {
    const std::string only = env("ANALYSIS_GOLDEN_ONLY");
    std::vector<const GoldenConfig*> selected;
    for (const GoldenConfig& config : configs) {
        if (only.empty() || config.name.find(only) != std::string::npos) {
            selected.push_back(&config);
        }
    }
    return selected;
}

// The families of `families` that `names` names, in their order.
std::vector<CorpusFamily> families_named(std::vector<CorpusFamily> families,
                                         const std::set<std::string>& names) {
    std::erase_if(families,
                  [&](const CorpusFamily& family) { return !names.contains(family.name); });
    return families;
}

struct ThreadDigest {
    FamilyDigest digest;
    std::string error;
};

// The digests of `family` from `threads` threads that share `analyzer`, the even ones with a new
// stream per line and the odd ones with one stream. A thread that failed has its error instead.
std::vector<ThreadDigest> digests_on_threads(const TextAnalyzerPtr& analyzer,
                                             const CorpusFamily& family, size_t threads) {
    std::vector<ThreadDigest> results(threads);
    std::vector<std::thread> workers;
    for (size_t t = 0; t < threads; ++t) {
        workers.emplace_back([&, t] {
            try {
                const auto driver = make_text_driver(analyzer);
                results[t].digest = digest_family(
                        *driver, t % 2 == 0 ? DriveMode::FRESH : DriveMode::REUSED, family);
            } catch (const std::exception& e) {
                results[t].error = e.what();
            }
        });
    }
    for (std::thread& worker : workers) {
        worker.join();
    }
    return results;
}

// Runs `configs` over `families` in `requested_modes`, and in the mode whose output is frozen, and
// checks every digest against `file`. The jobs run on several threads; the checks run here, in
// the order of the matrix.
void check_golden(const std::vector<GoldenConfig>& configs,
                  const std::vector<CorpusFamily>& families,
                  const std::vector<DriveMode>& requested_modes, test::GoldenFile* file,
                  const DriverFactory& make_driver = make_golden_driver) {
    const std::string dump_dir = env("ANALYSIS_GOLDEN_DUMP");
    const std::vector<const GoldenConfig*> selected = selected_configs(configs);
    // Build each configuration and analyze one line before going parallel, so a process-wide
    // dictionary is loaded by one thread, as in a BE that has served a first query.
    for (const GoldenConfig* config : selected) {
        std::unique_ptr<GoldenDriver> driver;
        const Status status = make_driver(*config, &driver);
        ASSERT_TRUE(status.ok()) << status.to_string() << "\n" << describe(*config);
        driver->fresh("warm up 预热");
    }
    std::vector<GoldenJob> jobs = make_jobs(selected, families, requested_modes);
    const auto started = std::chrono::steady_clock::now();
    parallel_for(jobs.size(), [&](size_t i) { run_job(&jobs[i], !dump_dir.empty(), make_driver); });
    const double wall =
            std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();
    for (size_t i = 0; i < jobs.size();) {
        i = check_jobs(jobs, i, dump_dir, file);
    }
    if (env("ANALYSIS_GOLDEN_TIMING") == "1") {
        print_timing(jobs, wall);
    }
}

} // namespace

class AnalysisGolden : public testing::Test {
protected:
    void SetUp() override {
        const Status status = _environment.init(_configs);
        ASSERT_TRUE(status.ok()) << status.to_string();
    }

    // The configurations that build an analyzer.
    std::vector<GoldenConfig> analyzer_configs() const {
        std::vector<GoldenConfig> configs;
        std::ranges::copy_if(_configs, std::back_inserter(configs), [](const GoldenConfig& config) {
            return config.kind == GoldenKind::ANALYZE;
        });
        return configs;
    }

    const std::vector<GoldenConfig> _configs = golden_configs();
    GoldenEnvironment _environment;
};

TEST_F(AnalysisGolden, MatchesTheFrozenDigests) {
    test::GoldenFile file(kGoldenFile);
    check_golden(_configs, default_families(_environment.corpus_sources()),
                 {DriveMode::FRESH, DriveMode::REUSED}, &file);
    file.finish(owns_analysis_line, env("ANALYSIS_GOLDEN_ONLY").empty());
}

// The consumers move to the TextAnalyzer interface before the analyzers do, through
// LuceneBackedTextAnalyzer. Through it every analyzer gives the frozen output, with a new stream
// per line and with one stream for all lines.
TEST_F(AnalysisGolden, LuceneBackedTextAnalyzerMatchesTheFrozenDigests) {
    test::GoldenFile file(kGoldenFile);
    check_golden(analyzer_configs(), default_families(_environment.corpus_sources()),
                 {DriveMode::FRESH, DriveMode::REUSED}, &file, make_golden_text_driver);
    file.finish(owns_analysis_line, false);
}

// An analyzer serves many threads at once. Eight threads share the LuceneBackedTextAnalyzer of
// each configuration, half of them with a new stream per line and half with one stream, and each
// thread gives the frozen output.
TEST_F(AnalysisGolden, EightThreadsShareOneTextAnalyzer) {
    const std::vector<CorpusFamily> families = families_named(
            default_families(_environment.corpus_sources()), {"mixed_zh_en", "invalid_utf8"});
    ASSERT_EQ(families.size(), 2);
    const std::vector<GoldenConfig> configs = analyzer_configs();
    test::GoldenFile file(kGoldenFile);
    for (const GoldenConfig* config : selected_configs(configs)) {
        TextAnalyzerPtr analyzer;
        const Status status = make_golden_text_analyzer(*config, &analyzer);
        ASSERT_TRUE(status.ok()) << status.to_string() << "\n" << describe(*config);
        for (const CorpusFamily& family : families) {
            const std::vector<ThreadDigest> results = digests_on_threads(analyzer, family, 8);
            for (size_t t = 0; t < results.size(); ++t) {
                ASSERT_TRUE(results[t].error.empty())
                        << config->name << " thread " << t << ": " << results[t].error;
                EXPECT_EQ(results[t].digest, results[0].digest)
                        << config->name << " " << family.name << ": thread " << t
                        << " disagrees with thread 0";
            }
            file.check(config->name, family.name, results[0].digest.tokens,
                       results[0].digest.digest, describe(*config));
        }
    }
    file.finish(owns_analysis_line, false);
}

// Every component the analysis factory registers is in some configuration, so a new component
// cannot ship without frozen output.
TEST_F(AnalysisGolden, EveryRegisteredComponentHasAConfiguration) {
    std::set<std::pair<std::string, std::string>> used;
    for (const GoldenConfig& config : _configs) {
        std::map<std::string, std::string> component_types;
        for (const GoldenPolicy& policy : config.policies) {
            if (const auto it = policy.properties.find("type"); it != policy.properties.end()) {
                component_types[policy.name] = it->second;
            }
        }
        // A component is a policy of the configuration or a builtin component named directly.
        const auto type_of = [&](std::string name) {
            name.erase(0, name.find_first_not_of(' '));
            name.erase(name.find_last_not_of(' ') + 1);
            const auto it = component_types.find(name);
            return it == component_types.end() ? name : it->second;
        };
        for (const GoldenPolicy& policy : config.policies) {
            for (const auto& [kind, names] : policy.properties) {
                if (kind != "tokenizer" && kind != "token_filter" && kind != "char_filter") {
                    continue;
                }
                std::istringstream list(names);
                for (std::string name; std::getline(list, name, ',');) {
                    used.emplace(kind, type_of(name));
                }
            }
        }
    }
    const auto components = registered_components();
    EXPECT_FALSE(components.empty());
    for (const auto& component : components) {
        EXPECT_TRUE(used.contains(component))
                << "the " << component.first << " " << component.second
                << " is registered, but no golden configuration uses it";
    }
}

// The ICU analyzers lowercase with the root locale: with Turkish as the default locale, I still
// lowercases to i, and the output stays the frozen one.
TEST_F(AnalysisGolden, IcuIgnoresTheDefaultLocale) {
    const icu::Locale saved = icu::Locale::getDefault();
    UErrorCode status = U_ZERO_ERROR;
    icu::Locale::setDefault(icu::Locale("tr"), status);
    ASSERT_TRUE(U_SUCCESS(status));
    Defer restore {[&] {
        UErrorCode restore_status = U_ZERO_ERROR;
        icu::Locale::setDefault(saved, restore_status);
    }};
    std::vector<GoldenConfig> icu_configs;
    std::ranges::copy_if(_configs, std::back_inserter(icu_configs), [](const GoldenConfig& config) {
        return config.name.find("icu") != std::string::npos;
    });
    ASSERT_FALSE(icu_configs.empty());
    const std::vector<CorpusFamily> families = families_named(
            default_families(_environment.corpus_sources()), {"latin_words", "bmp_code_points"});
    ASSERT_EQ(families.size(), 2);
    test::GoldenFile file(kGoldenFile);
    check_golden(icu_configs, families, {DriveMode::FRESH}, &file);
    file.finish(owns_analysis_line, false);
}

// Every code point from U+0000 to U+10FFFF, against its own file. It runs long, so only on
// request: when the analysis layer is frozen and when its rewrite is finished.
TEST_F(AnalysisGolden, EveryCodePoint) {
    if (env("ANALYSIS_GOLDEN_FULL") != "1") {
        GTEST_SKIP() << "set ANALYSIS_GOLDEN_FULL=1 to scan every code point";
    }
    test::GoldenFile file(kFullGoldenFile);
    check_golden(_configs, {full_code_point_family()}, {DriveMode::FRESH}, &file);
    file.finish([](const std::string&) { return true; }, env("ANALYSIS_GOLDEN_ONLY").empty());
}

TEST(AnalysisGoldenMatrix, NamesAreUniqueKeys) {
    std::set<std::string> names;
    for (const GoldenConfig& config : golden_configs()) {
        EXPECT_TRUE(names.insert(config.name).second) << "duplicate configuration " << config.name;
        EXPECT_EQ(config.name.find_first_of(" \t\n"), std::string::npos) << config.name;
    }
    std::set<std::string> family_names {full_code_point_family().name};
    for (const CorpusFamily& family : default_families(GoldenEnvironment().corpus_sources())) {
        EXPECT_TRUE(family_names.insert(family.name).second) << "duplicate family " << family.name;
        EXPECT_EQ(family.name.find_first_of(" \t\n"), std::string::npos) << family.name;
        EXPECT_FALSE(family.lines.empty()) << family.name;
    }
}

TEST(AnalysisGoldenMatrix, PairwiseRowsCoverEveryPair) {
    for (size_t factors : {1, 2, 3, 9, 16, 35}) {
        const std::vector<std::vector<bool>> rows = pairwise_rows(factors);
        ASSERT_FALSE(rows.empty());
        EXPECT_EQ(std::ranges::count(rows[0], true), 0);
        for (size_t i = 0; i < factors; ++i) {
            for (size_t j = i + 1; j < factors; ++j) {
                for (bool a : {false, true}) {
                    for (bool b : {false, true}) {
                        EXPECT_TRUE(std::ranges::any_of(
                                rows, [&](const auto& row) { return row[i] == a && row[j] == b; }))
                                << factors << " factors: (" << i << ", " << j << ") = (" << a
                                << ", " << b << ") is in no row";
                    }
                }
            }
        }
    }
}

} // namespace doris::segment_v2::inverted_index::golden
