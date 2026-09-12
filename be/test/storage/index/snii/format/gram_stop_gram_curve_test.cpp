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
//
// What does each df cut-off actually buy?
//
// The stop-gram threshold has twice been chosen as a ratio of the segment -- 0.15%, then a
// tenth -- and both times the number came from an argument rather than from the corpus. The
// two arguments reach thresholds 66x apart, which is a sign that neither is derived from
// anything the data says.
//
// This sweeps the cut-off across the whole range on a real corpus and reports what each one
// costs and buys: how many terms it drops, what share of the posting entries those terms
// held, and what the index actually weighs afterwards. A rule that adapts to the segment has
// to be read off this curve, and the shape differs by script -- English log text puts almost
// all of its posting mass in a few hundred terms, while a CJK vocabulary spreads it out --
// so the same rule must produce different cut-offs on the two, which is the whole point.
//
// Corpus-driven and skipped without one:
//   GRAM_CURVE_CORPUS=/path/to/lines.txt  GRAM_CURVE_ROWS=200000
//   (optional) GRAM_CURVE_JSON_FIELD=body to pull one field out of JSON lines.

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "storage/index/inverted/gram/gram_extractor.h"
#include "storage/index/inverted/gram/gram_scheme.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii/writer/logical_index_writer.h"
#include "storage/index/snii/writer/snii_compound_writer.h"
#include "storage/index/snii_query_test_util.h"

namespace doris::snii {
namespace {

using snii_test::assert_ok;
using snii_test::make_term;
using snii_test::MemoryFile;
using snii_test::PostingDoc;

std::vector<std::string> read_corpus(const char* path, size_t rows) {
    std::vector<std::string> out;
    std::ifstream in(path);
    std::string line;
    while (out.size() < rows && std::getline(in, line)) {
        if (!line.empty()) {
            out.push_back(line);
        }
    }
    return out;
}

// term -> sorted docids, built by running the real extractor over the corpus.
struct Corpus {
    std::vector<writer::TermPostings> terms;
    uint32_t doc_count = 0;
    uint64_t raw_bytes = 0;
    uint64_t posting_entries = 0;
};

// How the non-ASCII runs are turned into terms. The extractor emits one term per code
// point today (kUni); the other two are what a word-shaped scheme would produce, and the
// point of measuring them is that a Chinese word is rare where its characters are not.
// kNeededBi: keep a character term when it is rare enough to survive the cut, and emit a
// pair term only where BOTH characters are above it -- those are the positions where a query
// would otherwise have no rare handle at all. Purely distributional: the cut-off decides what
// "common" means, so the scheme adapts to the corpus rather than to a script.
enum class CjkMode { kUni, kBi, kUniBi, kNeededBi };

size_t codepoint_len_at(const std::string& s, size_t i) {
    const auto c = static_cast<unsigned char>(s[i]);
    size_t len = 1;
    if ((c >> 5) == 0x6) {
        len = 2;
    } else if ((c >> 4) == 0xE) {
        len = 3;
    } else if ((c >> 3) == 0x1E) {
        len = 4;
    }
    return i + len > s.size() ? 1 : len;
}

// Terms of one row: ASCII segments through the real extractor, non-ASCII runs through the
// mode under test. Deduplicated per row, exactly as the writer sees them.
void row_terms(const std::string& line, segment_v2::gram::GramExtractor& extractor, CjkMode mode,
               const std::set<std::string>* common, std::vector<std::string>* out) {
    out->clear();
    std::vector<std::string_view> grams;
    size_t i = 0;
    const size_t L = line.size();
    while (i < L) {
        if (static_cast<unsigned char>(line[i]) < 0x80) {
            size_t j = i;
            while (j < L && static_cast<unsigned char>(line[j]) < 0x80) {
                j++;
            }
            // The extractor hands back views INTO the text it was given, so the segment has
            // to outlive them; a temporary from substr() would leave every view dangling.
            const std::string segment = line.substr(i, j - i);
            extractor.extract(segment, &grams);
            for (const std::string_view g : grams) {
                out->emplace_back(g);
            }
            i = j;
        } else {
            std::vector<std::string> run;
            while (i < L && static_cast<unsigned char>(line[i]) >= 0x80) {
                const size_t len = codepoint_len_at(line, i);
                run.push_back(line.substr(i, len));
                i += len;
            }
            if (mode == CjkMode::kUni || mode == CjkMode::kUniBi) {
                for (const std::string& cp : run) {
                    out->push_back(cp);
                }
            }
            if (mode == CjkMode::kBi || mode == CjkMode::kUniBi) {
                for (size_t k = 0; k + 1 < run.size(); ++k) {
                    out->push_back(run[k] + run[k + 1]);
                }
                if (run.size() == 1 && mode == CjkMode::kBi) {
                    out->push_back(run[0]); // a lone character has no pair to form
                }
            }
            if (mode == CjkMode::kNeededBi) {
                for (const std::string& cp : run) {
                    if (common == nullptr || !common->contains(cp)) {
                        out->push_back(cp);
                    }
                }
                for (size_t k = 0; k + 1 < run.size(); ++k) {
                    if (common != nullptr && common->contains(run[k]) &&
                        common->contains(run[k + 1])) {
                        out->push_back(run[k] + run[k + 1]);
                    }
                }
            }
        }
    }
    std::ranges::sort(*out);
    out->erase(std::unique(out->begin(), out->end()), out->end());
}

Corpus build_corpus(const std::vector<std::string>& lines,
                    const segment_v2::gram::GramScheme& scheme, CjkMode mode,
                    const std::set<std::string>* common = nullptr) {
    segment_v2::gram::GramExtractor extractor(scheme);
    std::map<std::string, std::vector<PostingDoc>> by_term;
    Corpus corpus;
    corpus.doc_count = static_cast<uint32_t>(lines.size());
    std::vector<std::string> terms;
    for (uint32_t docid = 0; docid < lines.size(); ++docid) {
        corpus.raw_bytes += lines[docid].size();
        row_terms(lines[docid], extractor, mode, common, &terms);
        for (const std::string& term : terms) {
            by_term[term].push_back(PostingDoc {.docid = docid, .positions = {}});
        }
    }
    corpus.terms.reserve(by_term.size());
    for (auto& [term, docs] : by_term) {
        corpus.posting_entries += docs.size();
        corpus.terms.push_back(make_term(term, std::move(docs)));
    }
    return corpus;
}

struct Built {
    uint64_t index_bytes = 0;
    uint64_t dict_bytes = 0;
    uint64_t posting_bytes = 0;
    uint64_t terms_dropped = 0;
    uint64_t entries_dropped = 0;
};

Built build_at(const Corpus& corpus, const segment_v2::gram::GramScheme& scheme,
               uint32_t threshold) {
    writer::SniiIndexInput input;
    input.index_id = 77;
    input.index_suffix = "body";
    input.config = format::IndexConfig::kDocsOnly;
    input.doc_count = corpus.doc_count;
    input.terms = corpus.terms;
    input.gram_scheme = scheme;
    input.stop_gram_df_threshold = threshold;

    Built built;
    for (const writer::TermPostings& t : corpus.terms) {
        if (threshold > 0 && t.docids.size() > threshold) {
            built.terms_dropped++;
            built.entries_dropped += t.docids.size();
        }
    }

    MemoryFile file;
    writer::SniiCompoundWriter compound(&file);
    assert_ok(compound.add_logical_index(input));
    assert_ok(compound.finish());

    reader::SniiSegmentReader segment;
    assert_ok(reader::SniiSegmentReader::open(&file, &segment));
    reader::LogicalIndexReader index;
    assert_ok(segment.open_index(77, "body", &index));
    const auto& refs = index.section_refs();
    built.dict_bytes = refs.dict_region.length;
    built.posting_bytes = refs.posting_region.length;
    built.index_bytes = file.size();
    return built;
}

} // namespace

TEST(GramStopGramCurveTest, ReportsWhatEachCutOffBuys) {
    const char* path = std::getenv("GRAM_CURVE_CORPUS");
    if (path == nullptr) {
        GTEST_SKIP() << "set GRAM_CURVE_CORPUS to a line-per-record corpus";
    }
    const char* rows_env = std::getenv("GRAM_CURVE_ROWS");
    const size_t rows = rows_env != nullptr ? std::strtoul(rows_env, nullptr, 10) : 200000;
    const std::vector<std::string> lines = read_corpus(path, rows);
    ASSERT_FALSE(lines.empty()) << "corpus " << path << " is empty or unreadable";

    segment_v2::gram::GramScheme scheme;
    scheme.mode = segment_v2::gram::GramMode::SPARSE;
    scheme.min_len = 3;
    scheme.max_len = 4;
    scheme.density_permille = 250;

    const char* mode_env = std::getenv("GRAM_CURVE_CJK");
    const std::string mode_name = mode_env != nullptr ? mode_env : "uni";
    CjkMode mode = CjkMode::kUni;
    if (mode_name == "bi") {
        mode = CjkMode::kBi;
    } else if (mode_name == "uni+bi") {
        mode = CjkMode::kUniBi;
    } else if (mode_name == "needed-bi") {
        mode = CjkMode::kNeededBi;
    }
    const Corpus corpus = build_corpus(lines, scheme, mode);
    printf("cjk mode: %s\n", mode_name.c_str());
    if (mode == CjkMode::kNeededBi) {
        // The scheme depends on which terms the cut-off calls common, so each cut-off gets
        // its own corpus: one pass to learn the character frequencies, one to emit the
        // terms the rule keeps.
        printf("%9s %10s %11s %10s %12s %12s %11s %9s\n", "cut-off", "terms", "entries", "per row",
               "dict B", "posting B", "index B", "idx/raw");
        for (const double share : {0.02, 0.01, 0.005, 0.0015}) {
            const auto threshold = static_cast<uint32_t>(corpus.doc_count * share);
            std::set<std::string> common;
            for (const writer::TermPostings& t : corpus.terms) {
                if (t.docids.size() > threshold) {
                    common.insert(t.term);
                }
            }
            const Corpus shaped = build_corpus(lines, scheme, mode, &common);
            const Built b = build_at(shaped, scheme, threshold);
            printf("%8.2f%% %10zu %11llu %10.2f %12llu %12llu %11llu %9.3f\n", share * 100.0,
                   shaped.terms.size(), static_cast<unsigned long long>(shaped.posting_entries),
                   static_cast<double>(shaped.posting_entries) / shaped.doc_count,
                   static_cast<unsigned long long>(b.dict_bytes),
                   static_cast<unsigned long long>(b.posting_bytes),
                   static_cast<unsigned long long>(b.index_bytes),
                   static_cast<double>(b.index_bytes) / static_cast<double>(shaped.raw_bytes));
        }
        return;
    }
    printf("\ncorpus: %s, %u rows, %.2f MB raw, %zu terms, %llu posting entries "
           "(%.2f per row)\n",
           path, corpus.doc_count, corpus.raw_bytes / 1e6, corpus.terms.size(),
           static_cast<unsigned long long>(corpus.posting_entries),
           static_cast<double>(corpus.posting_entries) / corpus.doc_count);

    // Cut-offs as a share of the segment, from "keep everything" down past the line the
    // writer currently draws (0.15%).
    const double shares[] = {0.0, 0.50, 0.20, 0.10, 0.05, 0.02, 0.01, 0.005, 0.0015};
    printf("%9s %10s %11s %10s %12s %12s %11s %9s\n", "cut-off", "terms cut", "entries cut",
           "entries%", "dict B", "posting B", "index B", "idx/raw");
    for (const double share : shares) {
        const uint32_t threshold =
                share == 0.0 ? 0 : static_cast<uint32_t>(corpus.doc_count * share);
        const Built b = build_at(corpus, scheme, threshold);
        printf("%8.2f%% %10llu %11llu %9.1f%% %12llu %12llu %11llu %9.3f\n", share * 100.0,
               static_cast<unsigned long long>(b.terms_dropped),
               static_cast<unsigned long long>(b.entries_dropped),
               100.0 * static_cast<double>(b.entries_dropped) /
                       static_cast<double>(corpus.posting_entries),
               static_cast<unsigned long long>(b.dict_bytes),
               static_cast<unsigned long long>(b.posting_bytes),
               static_cast<unsigned long long>(b.index_bytes),
               static_cast<double>(b.index_bytes) / static_cast<double>(corpus.raw_bytes));
        EXPECT_GT(b.index_bytes, 0U);
    }
}

} // namespace doris::snii
