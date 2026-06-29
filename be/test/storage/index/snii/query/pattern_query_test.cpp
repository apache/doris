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

#include <gtest/gtest.h>
#include <unistd.h>

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <regex>
#include <set>
#include <string>
#include <string_view>
#include <vector>

#include "common/status.h"
#include "snii/io/local_file.h"
#include "snii/io/metered_file_reader.h"
#include "snii/query/regexp_query.h"
#include "snii/query/wildcard_query.h"
#include "snii/reader/logical_index_reader.h"
#include "snii/reader/snii_segment_reader.h"
#include "snii/writer/snii_compound_writer.h"
#include "snii/writer/spimi_term_buffer.h"

using namespace snii;
using namespace snii::reader;
using namespace snii::writer;

namespace {

std::string TempPath() {
    static int counter = 0;
    return "/tmp/snii_pattern_query_" + std::to_string(getpid()) + "_" + std::to_string(counter++) +
           ".idx";
}

bool WildcardMatch(std::string_view pattern, std::string_view text) {
    std::vector<uint8_t> prev(text.size() + 1, 0);
    std::vector<uint8_t> curr(text.size() + 1, 0);
    prev[0] = 1;
    for (char p : pattern) {
        std::ranges::fill(curr, 0);
        if (p == '*') {
            curr[0] = prev[0];
            for (size_t i = 1; i <= text.size(); ++i) {
                curr[i] = prev[i] || curr[i - 1];
            }
        } else {
            for (size_t i = 1; i <= text.size(); ++i) {
                curr[i] = prev[i - 1] && (p == '?' || p == text[i - 1]);
            }
        }
        prev.swap(curr);
    }
    return prev[text.size()] != 0;
}

struct Corpus {
    uint32_t doc_count = 0;
    std::vector<std::vector<std::string>> docs;

    std::vector<uint32_t> matching_docs(
            const std::function<bool(std::string_view)>& matches) const {
        std::set<uint32_t> ids;
        for (uint32_t d = 0; d < docs.size(); ++d) {
            for (const std::string& term : docs[d]) {
                if (matches(term)) {
                    ids.insert(d);
                }
            }
        }
        return {ids.begin(), ids.end()};
    }
};

Corpus BuildMixedCorpus() {
    Corpus c;
    c.doc_count = 120;
    c.docs.resize(c.doc_count);
    for (uint32_t d = 0; d < c.doc_count; ++d) {
        std::vector<std::string>& terms = c.docs[d];
        if (d < 80) {
            char term[16];
            std::snprintf(term, sizeof(term), "aa_%03u", d);
            terms.emplace_back(term);
        }
        if (d < 80 && d % 2 == 0) {
            terms.emplace_back("aa_even");
        }
        if (d < 50) {
            char term[16];
            std::snprintf(term, sizeof(term), "ab_%03u", d);
            terms.emplace_back(term);
        }
        char filler[16];
        std::snprintf(filler, sizeof(filler), "zz_%03u", d);
        terms.emplace_back(filler);
    }
    return c;
}

Corpus BuildLowDfPrefixCorpus() {
    Corpus c;
    c.doc_count = 96;
    c.docs.resize(c.doc_count);
    for (uint32_t d = 0; d < c.doc_count; ++d) {
        char term[16];
        std::snprintf(term, sizeof(term), "aa_%03u", d);
        c.docs[d].emplace_back(term);
    }
    return c;
}

void WriteCorpus(const Corpus& c, const std::string& path) {
    SpimiTermBuffer buf(/*has_positions=*/false);
    for (uint32_t d = 0; d < c.docs.size(); ++d) {
        const std::vector<std::string>& terms = c.docs[d];
        for (uint32_t pos = 0; pos < terms.size(); ++pos) {
            buf.add_token(terms[pos], d, pos);
        }
    }

    SniiIndexInput in;
    in.index_id = 1;
    in.index_suffix = "body";
    in.config = snii::format::IndexConfig::kDocsOnly;
    in.doc_count = c.doc_count;
    in.terms = buf.finalize_sorted();
    in.target_dict_block_bytes = 2048;

    io::LocalFileWriter writer;
    ASSERT_TRUE(writer.open(path).ok());
    SniiCompoundWriter compound(&writer);
    ASSERT_TRUE(compound.add_logical_index(in).ok());
    ASSERT_TRUE(compound.finish().ok());
}

LogicalIndexReader OpenIndex(io::LocalFileReader* file, SniiSegmentReader* segment,
                             const std::string& path) {
    EXPECT_TRUE(file->open(path).ok());
    EXPECT_TRUE(SniiSegmentReader::open(file, segment).ok());
    LogicalIndexReader idx;
    EXPECT_TRUE(segment->open_index(1, "body", &idx).ok());
    return idx;
}

LogicalIndexReader OpenMeteredIndex(io::MeteredFileReader* file, SniiSegmentReader* segment) {
    EXPECT_TRUE(SniiSegmentReader::open(file, segment).ok());
    LogicalIndexReader idx;
    EXPECT_TRUE(segment->open_index(1, "body", &idx).ok());
    return idx;
}

} // namespace

TEST(SniiPatternQuery, WildcardMatchesOracle) {
    const Corpus corpus = BuildMixedCorpus();
    const std::string path = TempPath();
    WriteCorpus(corpus, path);

    io::LocalFileReader file;
    SniiSegmentReader segment;
    LogicalIndexReader idx = OpenIndex(&file, &segment, path);

    for (const char* pattern : {"aa_*", "aa_00?", "aa_even", "zz_0??", "missing*"}) {
        std::vector<uint32_t> got;
        ASSERT_TRUE(query::wildcard_query(idx, pattern, &got).ok()) << pattern;
        EXPECT_TRUE(std::ranges::is_sorted(got)) << pattern;
        EXPECT_EQ(got, corpus.matching_docs([&](std::string_view term) {
            return WildcardMatch(pattern, term);
        })) << pattern;
    }

    std::remove(path.c_str());
}

TEST(SniiPatternQuery, RegexpMatchesOracle) {
    const Corpus corpus = BuildMixedCorpus();
    const std::string path = TempPath();
    WriteCorpus(corpus, path);

    io::LocalFileReader file;
    SniiSegmentReader segment;
    LogicalIndexReader idx = OpenIndex(&file, &segment, path);

    for (const char* pattern :
         {"aa_00[0-9]", "aa_even", "ab_0[0-9][0-9]", "zz_1..", "no_match.*"}) {
        const std::regex re(pattern);
        std::vector<uint32_t> got;
        ASSERT_TRUE(query::regexp_query(idx, pattern, &got).ok()) << pattern;
        EXPECT_TRUE(std::ranges::is_sorted(got)) << pattern;
        EXPECT_EQ(got, corpus.matching_docs([&](std::string_view term) {
            return std::regex_match(term.begin(), term.end(), re);
        })) << pattern;
    }

    std::vector<uint32_t> ignored;
    EXPECT_FALSE(query::regexp_query(idx, "[", &ignored).ok());

    std::remove(path.c_str());
}

TEST(SniiPatternQuery, WideWildcardUsesPrefixEnumerationWithoutPerTermLookup) {
    const Corpus corpus = BuildLowDfPrefixCorpus();
    const std::string path = TempPath();
    WriteCorpus(corpus, path);

    io::LocalFileReader local;
    ASSERT_TRUE(local.open(path).ok());
    io::MeteredFileReader metered(&local, /*block_size=*/4096);
    SniiSegmentReader segment;
    LogicalIndexReader idx = OpenMeteredIndex(&metered, &segment);

    metered.reset_metrics();
    std::vector<uint32_t> got;
    ASSERT_TRUE(query::wildcard_query(idx, "aa_*", &got).ok());

    EXPECT_EQ(got, corpus.matching_docs(
                           [](std::string_view term) { return WildcardMatch("aa_*", term); }));
    EXPECT_LT(metered.metrics().read_at_calls, corpus.doc_count / 3)
            << "wildcard_query must reuse enumerated entries, not lookup every term again";

    std::remove(path.c_str());
}
