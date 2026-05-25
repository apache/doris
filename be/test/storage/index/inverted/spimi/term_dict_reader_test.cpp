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

#include "storage/index/inverted/spimi/term_dict_reader.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <random>
#include <string>
#include <vector>

#include "storage/index/inverted/spimi/lucene_output.h"
#include "storage/index/inverted/spimi/term_dict_writer.h"

namespace doris::segment_v2::inverted_index::spimi {

namespace {

// Convenience fixture: builds a `.tis` / `.tii` pair in memory by
// driving the real `TermDictWriter`. Avoids any byte-level mocks so
// any writer/reader byte-format drift is caught.
struct InMemoryDictionary {
    MemoryLuceneOutput tis;
    MemoryLuceneOutput tii;
    std::unique_ptr<TermDictWriter> writer;
    int32_t index_interval;
    int32_t skip_interval;

    explicit InMemoryDictionary(int32_t idx_iv = TermDictWriter::kDefaultIndexInterval,
                                int32_t skip_iv = TermDictWriter::kDefaultSkipInterval)
            : index_interval(idx_iv), skip_interval(skip_iv) {
        writer = std::make_unique<TermDictWriter>(&tis, &tii, idx_iv, skip_iv);
    }

    void Add(int32_t field, std::string_view term, int32_t doc_freq, int64_t freq_pointer,
             int64_t prox_pointer, int32_t skip_offset = 0) {
        TermInfo info {doc_freq, freq_pointer, prox_pointer, skip_offset};
        writer->Add(field, term, info);
    }

    std::unique_ptr<TermDictReader> Finalize() {
        writer->Close();
        writer.reset();
        return std::make_unique<TermDictReader>(tis.bytes(), tii.bytes());
    }
};

} // namespace

TEST(TermDictReaderTest, LooksUpSingleTerm) {
    InMemoryDictionary dict;
    // skip_offset must be 0 since doc_freq (3) < skip_interval (512).
    dict.Add(/*field=*/0, "alpha", /*doc_freq=*/3, /*freq_pointer=*/1, /*prox_pointer=*/2);
    auto reader = dict.Finalize();

    const auto found = reader->LookupTerm(0, "alpha");
    ASSERT_TRUE(found.has_value());
    EXPECT_EQ(found->doc_freq, 3);
    EXPECT_EQ(found->freq_pointer, 1);
    EXPECT_EQ(found->prox_pointer, 2);
    EXPECT_EQ(found->skip_offset, 0);
}

TEST(TermDictReaderTest, ReturnsNulloptForMissingTerm) {
    InMemoryDictionary dict;
    dict.Add(0, "alpha", 1, 0, 0);
    dict.Add(0, "beta", 2, 5, 5);
    dict.Add(0, "gamma", 3, 9, 9);
    auto reader = dict.Finalize();

    EXPECT_FALSE(reader->LookupTerm(0, "absent").has_value());
    EXPECT_FALSE(reader->LookupTerm(0, "between").has_value()); // between beta/gamma
    EXPECT_FALSE(reader->LookupTerm(0, "zzz").has_value());     // past last term
    EXPECT_FALSE(reader->LookupTerm(0, "a").has_value());       // before first
    EXPECT_FALSE(reader->LookupTerm(1, "alpha").has_value());   // wrong field
}

TEST(TermDictReaderTest, RecoversAllTermsInSequence) {
    InMemoryDictionary dict;
    std::vector<std::string> terms {"apple", "banana", "cherry",   "date", "elder",
                                    "fig",   "grape",  "honeydew", "iced", "jackfruit"};
    for (size_t i = 0; i < terms.size(); ++i) {
        dict.Add(0, terms[i], static_cast<int32_t>(i + 1), static_cast<int64_t>(i * 10),
                 static_cast<int64_t>(i * 20));
    }
    auto reader = dict.Finalize();

    for (size_t i = 0; i < terms.size(); ++i) {
        const auto info = reader->LookupTerm(0, terms[i]);
        ASSERT_TRUE(info.has_value()) << "term=" << terms[i];
        EXPECT_EQ(info->doc_freq, static_cast<int32_t>(i + 1));
        EXPECT_EQ(info->freq_pointer, static_cast<int64_t>(i * 10));
        EXPECT_EQ(info->prox_pointer, static_cast<int64_t>(i * 20));
    }
}

TEST(TermDictReaderTest, CrossesTiiIndexBoundaries) {
    // index_interval=4 ⇒ .tii has sentinel + entries at .tis positions
    // 3, 7, 11, 15, 19. Lookup must work across every boundary.
    InMemoryDictionary dict(/*idx_iv=*/4);
    constexpr int kTotal = 30;
    std::vector<std::string> terms;
    terms.reserve(kTotal);
    for (int i = 0; i < kTotal; ++i) {
        // Zero-pad so lexicographic order matches numeric order.
        char buf[8];
        std::snprintf(buf, sizeof(buf), "t%05d", i);
        terms.emplace_back(buf);
        dict.Add(0, terms.back(), i + 1, i * 100, i * 200);
    }
    auto reader = dict.Finalize();

    for (int i = 0; i < kTotal; ++i) {
        const auto info = reader->LookupTerm(0, terms[i]);
        ASSERT_TRUE(info.has_value()) << " i=" << i;
        EXPECT_EQ(info->doc_freq, i + 1);
        EXPECT_EQ(info->freq_pointer, i * 100);
        EXPECT_EQ(info->prox_pointer, i * 200);
    }
    // Boundary case: missing terms at every .tii block.
    EXPECT_FALSE(reader->LookupTerm(0, "t00000xxx").has_value());
    EXPECT_FALSE(reader->LookupTerm(0, "t99999").has_value());
}

TEST(TermDictReaderTest, RoundTripsSkipOffsetForHighDfTerms) {
    // skip_interval=4 ⇒ doc_freq >= 4 forces the writer to emit
    // skip_offset. Reader must recover it.
    InMemoryDictionary dict(/*idx_iv=*/4, /*skip_iv=*/4);
    dict.Add(0, "low_freq", /*doc_freq=*/2, /*freq=*/0, /*prox=*/0, /*skip=*/0);
    dict.Add(0, "med_freq", /*doc_freq=*/4, /*freq=*/5, /*prox=*/5, /*skip=*/13);
    dict.Add(0, "top_freq", /*doc_freq=*/100, /*freq=*/30, /*prox=*/30, /*skip=*/420);
    auto reader = dict.Finalize();

    EXPECT_EQ(reader->LookupTerm(0, "low_freq")->skip_offset, 0);
    EXPECT_EQ(reader->LookupTerm(0, "med_freq")->skip_offset, 13);
    EXPECT_EQ(reader->LookupTerm(0, "top_freq")->skip_offset, 420);
}

TEST(TermDictReaderTest, HandlesUtf8MultiByteTerms) {
    InMemoryDictionary dict;
    // UTF-8 sequences for a few non-ASCII strings sorted in wide-char
    // codepoint order:
    //   "café"   = c a f \xC3\xA9        (é = U+00E9)
    //   "naïve"  = n a \xC3\xAF v e      (ï = U+00EF)
    //   "résumé" = r \xC3\xA9 s u m \xC3\xA9
    //   "Σ"      = U+03A3
    //   "東"     = U+6771 (3-byte UTF-8 \xE6\x9D\xB1)
    dict.Add(0, "café", 1, 0, 0);
    dict.Add(0, "naïve", 2, 5, 5);
    dict.Add(0, "résumé", 3, 10, 10);
    dict.Add(0, "Σ", 4, 15, 15);
    dict.Add(0, "東", 5, 20, 20);
    auto reader = dict.Finalize();

    EXPECT_EQ(reader->LookupTerm(0, "café")->doc_freq, 1);
    EXPECT_EQ(reader->LookupTerm(0, "naïve")->doc_freq, 2);
    EXPECT_EQ(reader->LookupTerm(0, "résumé")->doc_freq, 3);
    EXPECT_EQ(reader->LookupTerm(0, "Σ")->doc_freq, 4);
    EXPECT_EQ(reader->LookupTerm(0, "東")->doc_freq, 5);
}

TEST(TermDictReaderTest, RandomizedRoundTrip) {
    InMemoryDictionary dict(/*idx_iv=*/16);
    std::mt19937 rng(0x71D1C7U);
    // Generate ~500 random sorted unique terms.
    std::vector<std::string> terms;
    while (terms.size() < 500) {
        int len = 3 + static_cast<int>(rng() % 6);
        std::string s;
        for (int j = 0; j < len; ++j) {
            s.push_back(static_cast<char>('a' + rng() % 26));
        }
        terms.push_back(std::move(s));
    }
    std::sort(terms.begin(), terms.end());
    terms.erase(std::unique(terms.begin(), terms.end()), terms.end());

    int64_t fp = 0;
    int64_t pp = 0;
    for (size_t i = 0; i < terms.size(); ++i) {
        const int32_t df = 1 + static_cast<int32_t>(i % 8);
        dict.Add(0, terms[i], df, fp, pp);
        fp += df;
        pp += df * 2;
    }
    auto reader = dict.Finalize();

    int64_t exp_fp = 0;
    for (size_t i = 0; i < terms.size(); ++i) {
        const int32_t df = 1 + static_cast<int32_t>(i % 8);
        const auto info = reader->LookupTerm(0, terms[i]);
        ASSERT_TRUE(info.has_value()) << "i=" << i << " term=" << terms[i];
        EXPECT_EQ(info->doc_freq, df);
        EXPECT_EQ(info->freq_pointer, exp_fp);
        exp_fp += df;
    }
    // A handful of negative probes — terms NOT in the dictionary.
    for (int t = 0; t < 50; ++t) {
        std::string probe;
        for (int j = 0; j < 12; ++j) {
            probe.push_back(static_cast<char>('a' + rng() % 26));
        }
        if (std::binary_search(terms.begin(), terms.end(), probe)) {
            continue; // collision — skip
        }
        EXPECT_FALSE(reader->LookupTerm(0, probe).has_value()) << probe;
    }
}

} // namespace doris::segment_v2::inverted_index::spimi
