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
#include <cstdio>
#include <set>
#include <string>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/io/local_file.h"
#include "storage/index/snii/io/metered_file_reader.h"
#include "storage/index/snii/query/phrase_query.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii/writer/snii_compound_writer.h"
#include "storage/index/snii/writer/spimi_term_buffer.h"

using namespace doris::snii;
using namespace doris::snii::reader;
using namespace doris::snii::writer;
using doris::Status;

namespace {

std::string TempPath() {
    static int counter = 0;
    return "/tmp/snii_phrase_prefix_query_" + std::to_string(getpid()) + "_" +
           std::to_string(counter++) + ".idx";
}

bool HasPrefix(const std::string& term, const std::string& prefix) {
    return term.size() >= prefix.size() && term.starts_with(prefix);
}

struct Corpus {
    std::vector<std::vector<std::string>> docs;

    std::vector<uint32_t> phrase_prefix_docs(const std::vector<std::string>& terms) const {
        std::vector<uint32_t> out;
        if (terms.empty()) {
            return out;
        }
        for (uint32_t d = 0; d < docs.size(); ++d) {
            const std::vector<std::string>& doc = docs[d];
            bool match = false;
            if (terms.size() == 1) {
                for (const std::string& term : doc) {
                    if (HasPrefix(term, terms.front())) {
                        match = true;
                        break;
                    }
                }
            } else if (doc.size() >= terms.size()) {
                for (size_t start = 0; start + terms.size() <= doc.size(); ++start) {
                    bool exact = true;
                    for (size_t i = 0; i + 1 < terms.size(); ++i) {
                        if (doc[start + i] != terms[i]) {
                            exact = false;
                            break;
                        }
                    }
                    if (exact && HasPrefix(doc[start + terms.size() - 1], terms.back())) {
                        match = true;
                        break;
                    }
                }
            }
            if (match) {
                out.push_back(d);
            }
        }
        return out;
    }
};

Corpus BuildPhraseCorpus() {
    Corpus c;
    c.docs = {{"quick", "brown", "fox"}, {"quick", "blue", "fox"}, {"quick", "bronze", "fox"},
              {"slow", "brown", "fox"},  {"quick", "brownish"},    {"quick", "brown", "fossil"},
              {"quick", "brown", "fog"}, {"quick", "brown"},       {"brown", "fox", "quick"}};
    return c;
}

Corpus BuildWideTailCorpus() {
    Corpus c;
    c.docs.resize(96);
    for (uint32_t d = 0; d < c.docs.size(); ++d) {
        c.docs[d].emplace_back(d == 0 ? "lead" : "other");
        char term[16];
        std::snprintf(term, sizeof(term), "aa_%03u", d);
        c.docs[d].emplace_back(term);
    }
    return c;
}

Corpus BuildRepeatedExactCorpus() {
    Corpus c;
    c.docs = {{"x", "x", "brown"},
              {"x", "y", "brown"},
              {"x", "brown", "x"},
              {"x", "x", "bronze"},
              {"x", "x", "blue"}};
    return c;
}

Corpus BuildSharedExactWideTailCorpus() {
    Corpus c;
    c.docs.resize(768);
    for (uint32_t d = 0; d < c.docs.size(); ++d) {
        c.docs[d].emplace_back("lead");
        char term[16];
        std::snprintf(term, sizeof(term), "aa_%03u", d);
        c.docs[d].emplace_back(term);
    }
    return c;
}

void WriteCorpus(const Corpus& c, const std::string& path) {
    SpimiTermBuffer buf(/*has_positions=*/true);
    for (uint32_t d = 0; d < c.docs.size(); ++d) {
        const std::vector<std::string>& terms = c.docs[d];
        for (uint32_t pos = 0; pos < terms.size(); ++pos) {
            buf.add_token(terms[pos], d, pos);
        }
    }

    SniiIndexInput in;
    in.index_id = 1;
    in.index_suffix = "body";
    in.config = doris::snii::format::IndexConfig::kDocsPositionsScoring;
    in.doc_count = static_cast<uint32_t>(c.docs.size());
    in.encoded_norms.assign(c.docs.size(), 1);
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

TEST(SniiPhrasePrefixQuery, MatchesPositionOracle) {
    const Corpus corpus = BuildPhraseCorpus();
    const std::string path = TempPath();
    WriteCorpus(corpus, path);

    io::LocalFileReader file;
    SniiSegmentReader segment;
    LogicalIndexReader idx = OpenIndex(&file, &segment, path);

    const std::vector<std::vector<std::string>> cases = {
            {"quick", "bro"},  {"quick", "brown", "fo"}, {"slow", "bro"},
            {"absent", "bro"}, {"quick", "missing"},     {"bro"}};
    for (const std::vector<std::string>& terms : cases) {
        std::vector<uint32_t> got;
        const Status st = query::phrase_prefix_query(idx, terms, &got);
        ASSERT_TRUE(st.ok()) << st.to_string();
        EXPECT_TRUE(std::ranges::is_sorted(got));
        EXPECT_EQ(got, corpus.phrase_prefix_docs(terms));
    }

    std::remove(path.c_str());
}

TEST(SniiPhrasePrefixQuery, WideTailPrefixAvoidsPerExpansionLookup) {
    const Corpus corpus = BuildWideTailCorpus();
    const std::string path = TempPath();
    WriteCorpus(corpus, path);

    io::LocalFileReader local;
    ASSERT_TRUE(local.open(path).ok());
    io::MeteredFileReader metered(&local, /*block_size=*/4096);
    SniiSegmentReader segment;
    LogicalIndexReader idx = OpenMeteredIndex(&metered, &segment);

    metered.reset_metrics();
    const std::vector<std::string> terms = {"lead", "aa_"};
    std::vector<uint32_t> got;
    const Status st = query::phrase_prefix_query(idx, terms, &got);
    ASSERT_TRUE(st.ok()) << st.to_string();

    EXPECT_EQ(got, corpus.phrase_prefix_docs(terms));
    EXPECT_LT(metered.metrics().read_at_calls, corpus.docs.size() / 3)
            << "phrase_prefix_query must reuse PrefixHit entries, not lookup every tail term";

    std::remove(path.c_str());
}

TEST(SniiPhrasePrefixQuery, RepeatedExactTermsMatchPositionOracle) {
    const Corpus corpus = BuildRepeatedExactCorpus();
    const std::string path = TempPath();
    WriteCorpus(corpus, path);

    io::LocalFileReader file;
    SniiSegmentReader segment;
    LogicalIndexReader idx = OpenIndex(&file, &segment, path);

    const std::vector<std::vector<std::string>> cases = {
            {"x", "x", "br"}, {"x", "brown", "x"}, {"x", "x", "missing"}};
    for (const std::vector<std::string>& terms : cases) {
        std::vector<uint32_t> got;
        const Status st = query::phrase_prefix_query(idx, terms, &got);
        ASSERT_TRUE(st.ok()) << st.to_string();
        EXPECT_EQ(got, corpus.phrase_prefix_docs(terms));
    }

    std::remove(path.c_str());
}

TEST(SniiPhrasePrefixQuery, WideTailPrefixReusesExactTermPostingReads) {
    const Corpus corpus = BuildSharedExactWideTailCorpus();
    const std::string path = TempPath();
    WriteCorpus(corpus, path);

    io::LocalFileReader local;
    ASSERT_TRUE(local.open(path).ok());
    io::MeteredFileReader metered(&local, /*block_size=*/4096);
    SniiSegmentReader segment;
    LogicalIndexReader idx = OpenMeteredIndex(&metered, &segment);

    metered.reset_metrics();
    const std::vector<std::string> terms = {"lead", "aa_"};
    std::vector<uint32_t> got;
    const Status st = query::phrase_prefix_query(idx, terms, &got);
    ASSERT_TRUE(st.ok()) << st.to_string();

    EXPECT_EQ(got, corpus.phrase_prefix_docs(terms));
    EXPECT_LT(metered.metrics().read_at_calls, corpus.docs.size() / 4)
            << "wide phrase-prefix must not re-read exact term postings for every tail hit";

    std::remove(path.c_str());
}
