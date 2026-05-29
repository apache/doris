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

#include "storage/index/inverted/tokenizer/basic/basic_tokenizer_simd.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

#include "storage/index/inverted/token_filter/lower_case_filter.h"
#include "storage/index/inverted/tokenizer/basic/basic_tokenizer.h"

namespace doris::segment_v2::inverted_index {

namespace {

// Drives BasicTokenizer end-to-end (the path BasicAnalyzer wires via
// set_lowercase) and returns the emitted token strings.
std::vector<std::string> tokenize(const std::string& text, bool lowercase) {
    ReaderPtr reader = std::make_shared<lucene::util::SStringReader<char>>();
    reader->init(text.data(), static_cast<int32_t>(text.size()), false);
    auto tk = std::make_shared<BasicTokenizer>();
    tk->initialize();
    tk->set_lowercase(lowercase);
    tk->set_reader(reader);
    tk->reset();
    Token t;
    std::vector<std::string> out;
    while (tk->next(&t) != nullptr) {
        out.emplace_back(t.termBuffer<char>(), static_cast<size_t>(t.termLength<char>()));
    }
    return out;
}

// Reference scalar lowercase of the ASCII subset, for equivalence checks.
std::string ref_ascii_lower(const std::string& in) {
    std::string out = in;
    for (char& c : out) {
        if (c >= 'A' && c <= 'Z') {
            c = static_cast<char>(c + 0x20);
        }
    }
    return out;
}

std::vector<uint8_t> bytes_of(const std::string& s) {
    return {s.begin(), s.end()};
}

std::string str_of(const std::vector<uint8_t>& v, size_t begin, size_t end) {
    return {v.begin() + static_cast<long>(begin), v.begin() + static_cast<long>(end)};
}

} // namespace

// ---- scan_ascii_alnum_run -------------------------------------------------

TEST(ScanAsciiAlnumRunTest, DelimitsRunAndLowercasesScalarTail) {
    // Short input (< 32 bytes) exercises only the scalar tail.
    auto s = bytes_of("HeLLo World");
    const int32_t end = scan_ascii_alnum_run<true>(s.data(), 0, static_cast<int32_t>(s.size()));
    EXPECT_EQ(end, 5) << "run is 'HeLLo', space terminates";
    EXPECT_EQ(str_of(s, 0, 5), "hello");
    EXPECT_EQ(str_of(s, 5, s.size()), " World") << "bytes past the run must be untouched";
}

TEST(ScanAsciiAlnumRunTest, LowercaseFalseLeavesRunUnchanged) {
    auto s = bytes_of("HeLLo World");
    const int32_t end = scan_ascii_alnum_run<false>(s.data(), 0, static_cast<int32_t>(s.size()));
    EXPECT_EQ(end, 5);
    EXPECT_EQ(str_of(s, 0, 5), "HeLLo") << "Lowercase=false must not mutate";
}

// P0-1 regression: when a 32-byte AVX2 window ends a run partway through, the
// in-place lowercase must touch ONLY the run's bytes, never the separator or the
// next token sharing that window.
TEST(ScanAsciiAlnumRunTest, LowercaseDoesNotMutateBytesPastRunInAvx2Window) {
    // "HELLO" (run) + "!" (separator) + uppercase tail, total > 32 bytes so the
    // first AVX2 window covers the run end and part of the next token.
    std::string in = "HELLO!ABCDEFGHIJKLMNOPQRSTUVWXYZABCDEFGH"; // 40 bytes
    auto s = bytes_of(in);
    const std::string expected_tail = in.substr(5); // everything from '!' on
    const int32_t end = scan_ascii_alnum_run<true>(s.data(), 0, static_cast<int32_t>(s.size()));
    EXPECT_EQ(end, 5) << "'!' at index 5 ends the run";
    EXPECT_EQ(str_of(s, 0, 5), "hello");
    EXPECT_EQ(str_of(s, 5, s.size()), expected_tail)
            << "separator + next token (still uppercase) must be byte-identical";
}

TEST(ScanAsciiAlnumRunTest, RunSpanningMultipleAvx2Windows) {
    // 70 alnum chars then a terminator: exercises two full AVX2 iterations plus
    // the partial-window terminator path, all lowercased.
    std::string in(70, 'A');
    in += "!tail";
    auto s = bytes_of(in);
    const int32_t end = scan_ascii_alnum_run<true>(s.data(), 0, static_cast<int32_t>(s.size()));
    EXPECT_EQ(end, 70);
    EXPECT_EQ(str_of(s, 0, 70), std::string(70, 'a'));
    EXPECT_EQ(str_of(s, 70, s.size()), "!tail");
}

TEST(ScanAsciiAlnumRunTest, BoundaryLengths) {
    for (int n : {0, 1, 31, 32, 33, 63, 64, 65}) {
        std::string in(static_cast<size_t>(n), 'X');
        auto s = bytes_of(in);
        const int32_t end = scan_ascii_alnum_run<true>(s.data(), 0, static_cast<int32_t>(s.size()));
        EXPECT_EQ(end, n) << "all-alnum length " << n << " must consume the whole buffer";
        EXPECT_EQ(str_of(s, 0, static_cast<size_t>(n)), std::string(static_cast<size_t>(n), 'x'))
                << "length " << n;
    }
}

TEST(ScanAsciiAlnumRunTest, NonAsciiByteEndsRun) {
    // A high-bit byte at index 5 (inside the first AVX2 window) ends the run;
    // the caller's UTF-8 path handles the rest.
    std::string in = "abcde\xC3\xA9rest_padding_to_force_avx2_window_xx"; // é = C3 A9
    auto s = bytes_of(in);
    const int32_t end = scan_ascii_alnum_run<true>(s.data(), 0, static_cast<int32_t>(s.size()));
    EXPECT_EQ(end, 5) << "scan stops at the first non-ASCII byte";
}

// ---- ascii_lower_inplace --------------------------------------------------

using lower_case_filter_detail::ascii_lower_inplace;

TEST(AsciiLowerInplaceTest, AllAsciiEquivalentToReferenceAcrossLengths) {
    // Build a deterministic mixed-case ASCII string and compare to the scalar
    // reference at every length around the 32-byte AVX2 boundary.
    std::string base;
    for (int i = 0; i < 80; ++i) {
        base += static_cast<char>('A' + (i % 26)); // A..Z repeating (all upper)
    }
    for (size_t n : {size_t(0), size_t(1), size_t(31), size_t(32), size_t(33), size_t(63),
                     size_t(64), size_t(65), size_t(80)}) {
        const std::string in = base.substr(0, n);
        std::string dst(n, '\0');
        const bool ok = ascii_lower_inplace(in.data(), dst.data(), n);
        EXPECT_TRUE(ok) << "pure ASCII length " << n;
        EXPECT_EQ(dst, ref_ascii_lower(in)) << "length " << n;
    }
}

TEST(AsciiLowerInplaceTest, AlreadyLowerIsNoOp) {
    const std::string in = "already lower 123 with spaces and digits long enough for avx2 path!!";
    std::string dst(in.size(), '\0');
    EXPECT_TRUE(ascii_lower_inplace(in.data(), dst.data(), in.size()));
    EXPECT_EQ(dst, in);
}

TEST(AsciiLowerInplaceTest, NonAsciiReturnsFalse) {
    // High-bit byte anywhere (first chunk, second chunk, tail) returns false.
    for (size_t pos : {size_t(0), size_t(5), size_t(31), size_t(32), size_t(40)}) {
        std::string in(48, 'A');
        in[pos] = static_cast<char>(0x80);
        std::string dst(in.size(), '\0');
        EXPECT_FALSE(ascii_lower_inplace(in.data(), dst.data(), in.size()))
                << "non-ASCII byte at " << pos << " must bail to ICU";
    }
}

// ---- BasicTokenizer end-to-end lowercase (cut() inline path) --------------

TEST(BasicTokenizerLowercaseTest, LowercasesAsciiTokensEndToEnd) {
    EXPECT_EQ(tokenize("Hello WORLD MixedCase123 end", /*lowercase=*/true),
              (std::vector<std::string> {"hello", "world", "mixedcase123", "end"}));
}

// A single token longer than 32 bytes exercises the AVX2 in-place lowercase
// inside cut(); the trailing token confirms the run boundary is respected.
TEST(BasicTokenizerLowercaseTest, LowercaseAcrossAvx2Window) {
    EXPECT_EQ(tokenize(std::string(40, 'A') + " Tail", /*lowercase=*/true),
              (std::vector<std::string> {std::string(40, 'a'), "tail"}));
}

TEST(BasicTokenizerLowercaseTest, LowercaseFalsePreservesCase) {
    EXPECT_EQ(tokenize("Hello WORLD", /*lowercase=*/false),
              (std::vector<std::string> {"Hello", "WORLD"}));
}

} // namespace doris::segment_v2::inverted_index
