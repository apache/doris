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

// How much of the postings region pays for grams a query can never use?
//
// Two facts meet here. The section split shows the postings region holds most of the
// index -- 65% at the default scheme, 82% once max_gram is lowered. And the candidate
// ratio gate makes a gram with a high document frequency inert at query time: once its
// df crosses gram_index_max_candidate_ratio_bp of the segment, the node it appears in
// returns the full range rather than intersect, because reading a posting list that
// matches most of the segment costs more than the scan it saves.
//
// A gram that is inert at query time still costs its full posting list on disk and in
// S3. This measures the size of that overlap: the share of posting entries held by grams
// whose df is above a series of thresholds, and what dropping them would leave behind.
//
// Dropping is semantically safe in this direction. The index is a superset filter, so
// removing a conjunct from an AND widens the candidate set and can only add false
// positives, never false negatives -- and a query whose grams are all dropped degrades
// to a full scan, which is what the gate already does for it today.

#include <gtest/gtest.h>

#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "storage/index/inverted/gram/gram_extractor.h"

namespace doris::segment_v2::gram {
namespace {

GramScheme Sparse(uint32_t min_len, uint32_t max_len, uint32_t density_permille) {
    GramScheme s;
    s.mode = GramMode::SPARSE;
    s.min_len = min_len;
    s.max_len = max_len;
    s.density_permille = density_permille;
    return s;
}

std::vector<std::string> LoadCorpus(const char* path, size_t max_rows) {
    std::vector<std::string> out;
    std::ifstream in(path);
    std::string line;
    while (out.size() < max_rows && std::getline(in, line)) {
        if (!line.empty()) {
            out.push_back(line);
        }
    }
    return out;
}

struct Term {
    uint64_t df = 0;      // rows the gram appears in
    uint64_t entries = 0; // posting entries it owns (one per row it appears in)
};

} // namespace

// Reports, per scheme, how the postings region divides between grams a query can use and
// grams the gate already refuses to read. Assertions stay structural: the shape of the
// distribution is the finding, and pinning its values would only make the case brittle.
TEST(GramDfDistributionTest, HighDfGramsHoldMostOfThePostings) {
    const char* corpus_path = std::getenv("GRAM_SECTION_CORPUS");
    if (corpus_path == nullptr) {
        GTEST_SKIP() << "set GRAM_SECTION_CORPUS to a corpus file (one row per line) to run";
    }
    const char* rows_env = std::getenv("GRAM_SECTION_ROWS");
    const size_t max_rows = rows_env ? static_cast<size_t>(std::atoll(rows_env)) : 200000;
    const std::vector<std::string> corpus = LoadCorpus(corpus_path, max_rows);
    ASSERT_FALSE(corpus.empty()) << "no rows read from " << corpus_path;

    const std::vector<std::pair<const char*, GramScheme>> schemes = {
            {"d=0.25 max=16 (default)", Sparse(3, 16, 250)},
            {"d=0.25 max=4", Sparse(3, 4, 250)},
            {"d=0.10 max=16", Sparse(3, 16, 100)},
            {"d=0.05 max=16", Sparse(3, 16, 50)},
            {"d=0.05 max=4", Sparse(3, 4, 50)},
    };
    // The first is the shipped gate threshold (15 bp); the rest bracket it so the curve
    // is visible rather than a single point.
    const std::vector<double> cutoff_bp = {15.0, 50.0, 100.0, 500.0, 1000.0};

    printf("\ncorpus: %s, %zu rows\n", corpus_path, corpus.size());

    for (const auto& [name, scheme] : schemes) {
        GramExtractor ex(scheme);
        std::unordered_map<std::string, Term> terms;
        std::vector<std::string_view> grams;
        uint64_t total_entries = 0;
        for (const std::string& row : corpus) {
            // extract() is the index-side call and already deduplicates within the row,
            // so each gram it returns is exactly one posting entry.
            ex.extract(row, &grams);
            for (std::string_view g : grams) {
                Term& t = terms[std::string(g)];
                t.df++;
                t.entries++;
                total_entries++;
            }
        }
        ASSERT_GT(total_entries, 0U) << name;

        printf("\n%s: %zu terms, %llu posting entries, %.1f entries/row\n", name, terms.size(),
               static_cast<unsigned long long>(total_entries),
               static_cast<double>(total_entries) / static_cast<double>(corpus.size()));
        printf("  %-10s %10s %10s %10s %10s\n", "df cutoff", "terms>cut", "entries>cut", "entries%",
               "terms%");
        for (double bp : cutoff_bp) {
            const uint64_t cut =
                    static_cast<uint64_t>(static_cast<double>(corpus.size()) * bp / 10000.0);
            uint64_t over_terms = 0, over_entries = 0;
            for (const auto& [term, t] : terms) {
                if (t.df > cut) {
                    over_terms++;
                    over_entries += t.entries;
                }
            }
            printf("  df>%-7llu %10llu %10llu %9.1f%% %9.1f%%\n",
                   static_cast<unsigned long long>(cut),
                   static_cast<unsigned long long>(over_terms),
                   static_cast<unsigned long long>(over_entries),
                   100.0 * static_cast<double>(over_entries) / static_cast<double>(total_entries),
                   100.0 * static_cast<double>(over_terms) / static_cast<double>(terms.size()));
        }
    }
}

} // namespace doris::segment_v2::gram
