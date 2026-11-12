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

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/status.h"
#include "storage/index/inverted/analysis/golden/golden_configs.h"
#include "storage/index/inverted/analysis/golden/golden_corpus.h"
#include "storage/index/inverted/analysis/text_analyzer.h"

namespace lucene::analysis {
class Analyzer;
} // namespace lucene::analysis

namespace doris::segment_v2::inverted_index::golden {

struct GoldenToken {
    std::string bytes;
    int32_t position_increment = 0;

    bool operator==(const GoldenToken&) const = default;
};

struct GoldenLine {
    bool threw = false; // the tokens of a line that threw are discarded
    std::vector<GoldenToken> tokens;

    bool operator==(const GoldenLine&) const = default;
};

// The output of a line that threw.
inline GoldenLine threw_line() {
    GoldenLine line;
    line.threw = true;
    return line;
}

enum class DriveMode {
    FRESH,  // a new stream for every line, as queries and the SNII writer read values
    REUSED, // one stream across the lines, as the V1 and V2 writers read values
};

const char* drive_mode_name(DriveMode mode);

// Analyzes lines with one analyzer configuration. The implementations are the only code in the
// golden tests that names the analyzer API, so a task that changes the API changes only them.
class GoldenDriver {
public:
    virtual ~GoldenDriver() = default;

    // Analyzes `line` with a stream of its own.
    virtual GoldenLine fresh(std::string_view line) = 0;
    // Analyzes `line` with the stream the previous calls used.
    virtual GoldenLine reused(std::string_view line) = 0;

    GoldenLine drive(DriveMode mode, std::string_view line) {
        return mode == DriveMode::FRESH ? fresh(line) : reused(line);
    }
};

struct FamilyDigest {
    uint64_t digest = 0;
    uint64_t tokens = 0;

    bool operator==(const FamilyDigest&) const = default;
};

// Digest of (threw, size, bytes, position_increment) over every line of a family, in order. With
// `dump`, every line and its tokens are appended to it as text, for ANALYSIS_GOLDEN_DUMP.
FamilyDigest digest_family(GoldenDriver& driver, DriveMode mode, const CorpusFamily& family,
                           std::string* dump = nullptr);

// Printable forms of an input line and of its output, for dumps and failure messages.
std::string escape_bytes(std::string_view bytes);
std::string format_line(const GoldenLine& line);

// The analyzer, or for GoldenKind::NORMALIZE the normalization, that production builds from the
// index properties of `config`. The policies of `config` must be registered. Fails when the
// analyzer cannot be built.
Status make_golden_driver(const GoldenConfig& config, std::unique_ptr<GoldenDriver>* driver);

// The analyzer of a GoldenKind::ANALYZE `config` behind the TextAnalyzer interface: the one
// make_golden_driver() drives, in a LuceneBackedTextAnalyzer.
Status make_golden_text_analyzer(const GoldenConfig& config, TextAnalyzerPtr* analyzer);

// Drives `analyzer` as the consumers of the TextAnalyzer interface do: FRESH makes a stream for
// every line, as a query does, and REUSED keeps one stream for all lines, as a writer does.
std::unique_ptr<GoldenDriver> make_text_driver(TextAnalyzerPtr analyzer);

// make_golden_text_analyzer() in make_text_driver(), with the signature of make_golden_driver().
Status make_golden_text_driver(const GoldenConfig& config, std::unique_ptr<GoldenDriver>* driver);

// The kind ("tokenizer", "token_filter" or "char_filter") and name of every component the
// analysis factory registers.
std::vector<std::pair<std::string, std::string>> registered_components();

// An analyzer built directly, for the oracles of the differential tests. `make_analyzer` runs once
// up front and again after a line throws in REUSED mode, because a writer never reuses a stream
// that threw.
std::unique_ptr<GoldenDriver> make_lucene_driver(
        std::function<std::shared_ptr<lucene::analysis::Analyzer>()> make_analyzer);

// Drivers share nothing but the process-wide dictionaries, as the writers' analyzers do, so the
// golden tests run them on several threads: body(0) to body(count - 1), each once, on up to
// golden_threads() threads. `body` must not throw.
void parallel_for(size_t count, const std::function<void(size_t)>& body);

// ANALYSIS_GOLDEN_THREADS, or the hardware's threads but at most 16.
size_t golden_threads();

} // namespace doris::segment_v2::inverted_index::golden
