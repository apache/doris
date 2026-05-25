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

#include "storage/index/inverted/spimi/clucene_term_enum.h"

#include <CLucene/index/Term.h>
#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "storage/index/inverted/spimi/lucene_output.h"
#include "storage/index/inverted/spimi/posting_buffer.h"
#include "storage/index/inverted/spimi/segment_writer.h"
#include "storage/index/inverted/spimi/term_dict_writer.h"

namespace doris::segment_v2::inverted_index::spimi {

namespace {

// Builds a real `.tis` byte buffer via `SegmentWriter` and constructs
// `SpimiCLuceneTermEnum` over it. Any byte-format drift between
// writer and enum would surface here because we never hand-craft
// bytes.
struct EnumFixture {
    MemoryLuceneOutput tis;
    MemoryLuceneOutput tii;
    MemoryLuceneOutput frq;
    MemoryLuceneOutput prx;
    int32_t skip_interval;

    explicit EnumFixture(int32_t skip_iv = TermDictWriter::kDefaultSkipInterval)
            : skip_interval(skip_iv) {}

    void Write(const std::vector<std::tuple<std::string, uint32_t, uint32_t>>& posts) {
        SpimiPostingBuffer buffer;
        for (const auto& [term, doc, pos] : posts) {
            buffer.Append(term, doc, pos);
        }
        buffer.Sort();
        SegmentWriter w(&tis, &tii, &frq, &prx, TermDictWriter::kDefaultIndexInterval,
                        skip_interval, TermDictWriter::kMaxSkipLevels);
        w.Emit(buffer, /*field_number=*/0);
        w.Close();
    }

    std::unique_ptr<SpimiCLuceneTermEnum> MakeEnum() {
        return std::make_unique<SpimiCLuceneTermEnum>(tis.bytes().data(), tis.bytes().size(),
                                                      skip_interval,
                                                      std::vector<std::wstring> {L"body"});
    }
};

// Compares a CLucene `Term*` wide-char text against an expected
// `std::wstring`. `Term::text()` returns a wchar_t* with the term
// text only (no field component).
::testing::AssertionResult TermTextEquals(lucene::index::Term* term, const wchar_t* expected) {
    if (term == nullptr) {
        return ::testing::AssertionFailure() << "term is null";
    }
    const wchar_t* got = term->text();
    if (std::wstring(got) != expected) {
        return ::testing::AssertionFailure() << "term text mismatch";
    }
    return ::testing::AssertionSuccess();
}

} // namespace

TEST(SpimiCLuceneTermEnumTest, IteratesSingleTerm) {
    EnumFixture fx;
    fx.Write({{"alpha", 0, 0}});
    auto enumerator = fx.MakeEnum();

    ASSERT_TRUE(enumerator->next());
    auto* t = enumerator->term(/*pointer=*/false);
    ASSERT_NE(t, nullptr);
    EXPECT_TRUE(TermTextEquals(t, L"alpha"));
    EXPECT_EQ(std::wstring(t->field()), L"body");
    EXPECT_EQ(enumerator->docFreq(), 1);

    EXPECT_FALSE(enumerator->next());
    EXPECT_FALSE(enumerator->next()); // idempotent past-end
}

TEST(SpimiCLuceneTermEnumTest, IteratesMultipleTermsInOrder) {
    EnumFixture fx;
    fx.Write({{"alpha", 0, 0}, {"beta", 1, 0}, {"gamma", 2, 0}, {"gamma", 3, 0}});
    auto enumerator = fx.MakeEnum();

    ASSERT_TRUE(enumerator->next());
    EXPECT_TRUE(TermTextEquals(enumerator->term(false), L"alpha"));
    EXPECT_EQ(enumerator->docFreq(), 1);

    ASSERT_TRUE(enumerator->next());
    EXPECT_TRUE(TermTextEquals(enumerator->term(false), L"beta"));
    EXPECT_EQ(enumerator->docFreq(), 1);

    ASSERT_TRUE(enumerator->next());
    EXPECT_TRUE(TermTextEquals(enumerator->term(false), L"gamma"));
    EXPECT_EQ(enumerator->docFreq(), 2);

    EXPECT_FALSE(enumerator->next());
}

TEST(SpimiCLuceneTermEnumTest, ExposesTermInfoForDownstreamReaders) {
    // `term_info()` is the bridge for `SpimiCLuceneTermDocs::seek` to
    // get freq_pointer/prox_pointer without re-binary-searching .tis.
    EnumFixture fx;
    fx.Write({{"alpha", 0, 0}, {"beta", 1, 0}});
    auto enumerator = fx.MakeEnum();

    ASSERT_TRUE(enumerator->next());
    EXPECT_EQ(enumerator->term_info().doc_freq, 1);
    EXPECT_EQ(enumerator->term_info().freq_pointer, 0);
    EXPECT_EQ(enumerator->term_info().prox_pointer, 0);

    ASSERT_TRUE(enumerator->next());
    // The second term's freq_pointer must be strictly greater than
    // the first's (postings of alpha come first in .frq).
    EXPECT_GT(enumerator->term_info().freq_pointer, 0);
}

TEST(SpimiCLuceneTermEnumTest, TermPointerBumpsRefcount) {
    EnumFixture fx;
    fx.Write({{"alpha", 0, 0}});
    auto enumerator = fx.MakeEnum();

    ASSERT_TRUE(enumerator->next());
    auto* held = enumerator->term(/*pointer=*/true);
    ASSERT_NE(held, nullptr);
    // Holder owns one reference. The enum still owns its own. Both
    // must release independently; ASAN would flag a double-free if
    // the contract is wrong.
    _CLDECDELETE(held);
    enumerator->close();
}

TEST(SpimiCLuceneTermEnumTest, HandlesUtf8MultiByteTerms) {
    EnumFixture fx;
    // Sort is by wide-char codepoint, ASCII-first:
    //   "café"   = c(0x63) ...
    //   "naïve"  = n(0x6E) ...
    //   "résumé" = r(0x72) ...
    //   "Σ"      = U+03A3
    //   "東"     = U+6771
    fx.Write({{"café", 0, 0}, {"naïve", 0, 0}, {"résumé", 0, 0}, {"Σ", 0, 0}, {"東", 0, 0}});
    auto enumerator = fx.MakeEnum();

    ASSERT_TRUE(enumerator->next());
    EXPECT_EQ(std::wstring(enumerator->term(false)->text()), std::wstring(L"café"));
    ASSERT_TRUE(enumerator->next());
    EXPECT_EQ(std::wstring(enumerator->term(false)->text()), std::wstring(L"naïve"));
    ASSERT_TRUE(enumerator->next());
    EXPECT_EQ(std::wstring(enumerator->term(false)->text()), std::wstring(L"résumé"));
    ASSERT_TRUE(enumerator->next());
    EXPECT_EQ(std::wstring(enumerator->term(false)->text()), std::wstring(L"Σ"));
    ASSERT_TRUE(enumerator->next());
    EXPECT_EQ(std::wstring(enumerator->term(false)->text()), std::wstring(L"東"));
    EXPECT_FALSE(enumerator->next());
}

} // namespace doris::segment_v2::inverted_index::spimi
