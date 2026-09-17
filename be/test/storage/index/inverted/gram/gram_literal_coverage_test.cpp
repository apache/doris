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

// How much of a query pattern's literal can actually be turned into grams?
//
// A row is tokenized as a whole, so its ASCII runs are long and content-defined
// boundaries are plentiful. A query only ever sees an isolated literal, and a gram it
// derives is usable only if the gram's whole extent lies inside that literal. Under
// SPARSE that needs either two boundaries inside the literal, or a full max_len window
// inside it -- so a literal shorter than max_len is queryable only by luck, and one
// that produces nothing degrades to ALL and the index goes unused.
//
// This measures that directly on the production extractor: for a grid of schemes, the
// share of literals of each length that yield at least one gram ("coverage"), plus the
// mean gram length, which is the other half of the trade -- shorter grams are easier to
// produce from a short literal and less selective once produced.

#include <gtest/gtest.h>

#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <numeric>
#include <string>
#include <vector>

#include "storage/index/inverted/gram/gram_extractor.h"

namespace doris::segment_v2::gram {
namespace {

// Coverage is a property of the boundary distribution over adjacent byte pairs, so it
// depends on the corpus. GRAM_LITERAL_COVERAGE_CORPUS names a file of one row per line
// to measure against real text; without it the built-in rows below stand in. They are
// deterministic and in the shape of the benchmarked data -- request lines, service log
// lines, structured agent observations, all ASCII -- but they are templated, so read the
// built-in run as the shape of the curve and a real corpus for its values. The assertions
// hold either way: they test monotonicity and DENSE's guarantee, not particular numbers.
std::vector<std::string> BuiltinCorpus() {
    std::vector<std::string> rows;
    const char* verbs[] = {"GET", "POST", "HEAD"};
    const char* dirs[] = {"images", "english", "french", "history", "venues"};
    const char* exts[] = {"gif", "jpg", "html", "htm", "txt"};
    for (int i = 0; i < 400; i++) {
        rows.push_back(std::string(verbs[i % 3]) + " /" + dirs[i % 5] + "/nav_" +
                       std::to_string(i * 7919 % 100000) + "." + exts[i % 5] + " HTTP/1.0");
        rows.push_back(
                "rpc error: code = Unavailable desc = error reading from server: "
                "context deadline exceeded, trace.rb:" +
                std::to_string(i % 500) + ":in block");
        rows.push_back("{\"cwd\":\"/workspace/tenant_" + std::to_string(i % 32) +
                       "/app_013\",\"target\":\"/workspace/demo/app_013/trace_" +
                       std::to_string(i * 31 % 999983) + "/mcp__matrix__bash_exec_" +
                       std::to_string(i % 8) + ".md\"}");
    }
    return rows;
}

const std::vector<std::string>& Corpus() {
    static const std::vector<std::string> rows = [] {
        const char* path = std::getenv("GRAM_LITERAL_COVERAGE_CORPUS");
        if (path == nullptr) {
            return BuiltinCorpus();
        }
        const char* rows_env = std::getenv("GRAM_LITERAL_COVERAGE_ROWS");
        const size_t max_rows = rows_env ? static_cast<size_t>(std::atoll(rows_env)) : 20000;
        std::vector<std::string> out;
        std::ifstream in(path);
        std::string line;
        while (out.size() < max_rows && std::getline(in, line)) {
            if (!line.empty()) {
                out.push_back(line);
            }
        }
        return out.empty() ? BuiltinCorpus() : out;
    }();
    return rows;
}

struct Coverage {
    double covered = 0.0;  // share of literals yielding >= 1 gram
    double mean_len = 0.0; // mean length of the grams produced
};

// Every substring of the given length, taken from the corpus, run through the real
// extractor's literal path -- the same call the compiler makes.
Coverage Measure(const GramScheme& s, size_t lit_len) {
    GramExtractor ex(s);
    size_t total = 0, hit = 0, gram_bytes = 0, gram_count = 0;
    std::vector<std::string> grams;
    for (const std::string& row : Corpus()) {
        if (row.size() < lit_len) {
            continue;
        }
        for (size_t off = 0; off + lit_len <= row.size(); off += 3) {
            ex.grams_of_literal(std::string_view(row).substr(off, lit_len), &grams);
            total++;
            if (!grams.empty()) {
                hit++;
                for (const std::string& g : grams) {
                    gram_bytes += g.size();
                }
                gram_count += grams.size();
            }
        }
    }
    Coverage c;
    c.covered = total ? static_cast<double>(hit) / static_cast<double>(total) : 0.0;
    c.mean_len =
            gram_count ? static_cast<double>(gram_bytes) / static_cast<double>(gram_count) : 0.0;
    return c;
}

GramScheme Sparse(uint32_t min_len, uint32_t max_len, uint32_t density_permille) {
    GramScheme s;
    s.mode = GramMode::SPARSE;
    s.min_len = min_len;
    s.max_len = max_len;
    s.density_permille = density_permille;
    return s;
}

} // namespace

// Reports the coverage grid. It asserts only the two properties the design rests on,
// so it cannot fail for a change in corpus or hash: coverage must not fall as the
// literal grows, and DENSE must cover every literal at or above min_len.
TEST(GramLiteralCoverageTest, ShortLiteralsAreOftenUnqueryableUnderSparse) {
    struct Cfg {
        const char* name;
        GramScheme scheme;
    };
    // The two levers are crossed rather than swept one at a time. Lowering density is
    // what shrinks the index, and it costs coverage. Lowering max_gram shrinks the
    // dictionary at almost no cost to the postings, and it should *raise* coverage: a
    // literal too short to hold two boundaries can still hold one fixed window once the
    // window is short enough. Whether the second offsets the first is exactly the
    // question a one-lever sweep cannot answer.
    const std::vector<Cfg> cfgs = {
            {"sparse d=050 max=16", Sparse(3, 16, 50)},
            {"sparse d=050 max=8", Sparse(3, 8, 50)},
            {"sparse d=050 max=6", Sparse(3, 6, 50)},
            {"sparse d=050 max=4", Sparse(3, 4, 50)},
            {"sparse d=100 max=16", Sparse(3, 16, 100)},
            {"sparse d=100 max=6", Sparse(3, 6, 100)},
            {"sparse d=100 max=4", Sparse(3, 4, 100)},
            {"sparse d=150 max=16", Sparse(3, 16, 150)},
            {"sparse d=250 max=16 (default)", Sparse(3, 16, 250)},
            {"sparse d=250 max=8", Sparse(3, 8, 250)},
            {"sparse d=250 max=6", Sparse(3, 6, 250)},
            {"sparse d=250 max=4", Sparse(3, 4, 250)},
            {"sparse d=500 max=16", Sparse(3, 16, 500)},
            {"sparse d=500 max=6", Sparse(3, 6, 500)},
    };
    const std::vector<size_t> lens = {3, 4, 6, 8, 10, 12, 16, 20, 24};

    for (const Cfg& cfg : cfgs) {
        std::string line = std::string(cfg.name) + "  |";
        std::string mean = std::string(cfg.name) + "  | mean gram len:";
        double prev = -1.0;
        for (size_t l : lens) {
            const Coverage c = Measure(cfg.scheme, l);
            char buf[64];
            snprintf(buf, sizeof(buf), " L%zu=%.0f%%", l, c.covered * 100.0);
            line += buf;
            snprintf(buf, sizeof(buf), " L%zu=%.1f", l, c.mean_len);
            mean += buf;
            // Coverage is monotone in literal length: a longer literal contains every
            // window the shorter one did.
            EXPECT_GE(c.covered + 1e-9, prev) << cfg.name << " length " << l;
            prev = c.covered;
        }
        std::cout << line << "\n" << mean << "\n";
    }

    GramScheme dense;
    dense.mode = GramMode::DENSE;
    dense.min_len = 3;
    std::string dline = "dense min=3                    |";
    for (size_t l : lens) {
        const Coverage c = Measure(dense, l);
        char buf[64];
        snprintf(buf, sizeof(buf), " L%zu=%.0f%%", l, c.covered * 100.0);
        dline += buf;
        EXPECT_DOUBLE_EQ(c.covered, 1.0) << "dense must cover every literal >= min_len";
    }
    std::cout << dline << "\n";
}

} // namespace doris::segment_v2::gram
