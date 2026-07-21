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
#include <unicode/utf8.h>

#include <cstdint>
#include <random>
#include <string>
#include <vector>

#include "CLucene/util/stringUtil.h"
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

// ---- Differential / property test (output-identity contract) --------------
//
// The hard contract for the SIMD tokenizer is byte-identity with the scalar
// BasicTokenizer for ALL inputs: identical token boundaries, identical in-place
// lowercasing, identical emission order. The fixed cases above lock specific
// boundaries; this test additionally feeds randomized + adversarial inputs
// through the real tokenizer and asserts equality with an independent,
// pure-scalar reference that mirrors `BasicTokenizer::cut` byte-for-byte (same
// `is_alnum`/`to_lower` LUTs from CLucene, same CJK single-char emission, no
// extra-chars). Under the default USE_AVX2=ON build (CI default) `tokenize()`
// runs the AVX2 arm, so any divergence between the AVX2 scanner and the scalar
// definition of correctness fails here; under USE_AVX2=0 it locks the scalar
// `#else` arm against the same reference.
namespace {

#define REF_IN_RANGE(c, lo, hi) ((uint32_t)((c) - (lo)) <= ((hi) - (lo)))
#define REF_IS_CHINESE(c)                                                      \
    (REF_IN_RANGE(c, 0x4E00, 0x9FFF) || REF_IN_RANGE(c, 0x3400, 0x4DBF) ||     \
     REF_IN_RANGE(c, 0x20000, 0x2A6DF) || REF_IN_RANGE(c, 0x2A700, 0x2EBEF) || \
     REF_IN_RANGE(c, 0x30000, 0x3134A))

// Pure-scalar reference implementation of BasicTokenizer::cut (no extra chars).
std::vector<std::string> ref_tokenize(const std::string& text, bool lowercase) {
    const auto* s = reinterpret_cast<const uint8_t*>(text.data());
    const auto length = static_cast<int32_t>(text.size());
    std::vector<std::string> out;
    for (int32_t i = 0; i < length;) {
        const uint8_t first = s[i];
        if (is_alnum(first)) {
            std::string tok;
            while (i < length && is_alnum(s[i])) {
                const uint8_t b = s[i];
                tok.push_back(lowercase ? to_lower(b) : static_cast<char>(b));
                ++i;
            }
            out.emplace_back(std::move(tok));
        } else {
            UChar32 c = U_UNASSIGNED;
            const int32_t prev_i = i;
            U8_NEXT(s, i, length, c);
            if (c < 0) {
                continue;
            }
            if (REF_IS_CHINESE(c)) {
                out.emplace_back(reinterpret_cast<const char*>(s + prev_i),
                                 static_cast<size_t>(i - prev_i));
            }
        }
    }
    return out;
}

#undef REF_IS_CHINESE
#undef REF_IN_RANGE

} // namespace

TEST(BasicTokenizerDifferentialTest, AdversarialFixtures) {
    const std::string cjk = "\xE4\xBD\xA0\xE5\xA5\xBD"; // 你好 (two CJK chars)
    const std::string acc = "\xC3\xA9";                 // é (2-byte UTF-8, non-CJK)
    std::vector<std::string> inputs = {
            "",
            " ",
            "        ",
            "!!!...,,,",
            "a",
            "A",
            "Z9",
            "Hello World",
            "HeLLo WORLD MixedCase123 end",
            std::string(31, 'A'),
            std::string(32, 'A'),
            std::string(33, 'A'),
            std::string(63, 'B') + " " + std::string(64, 'c'),
            std::string(65, 'D') + "!" + std::string(70, 'E'),
            "token" + cjk + "Mixed",
            cjk + cjk + "ABC" + cjk,
            "ascii" + acc + "Tail", // accented byte ends the ASCII run, é is not CJK
            "a1B2c3D4e5F6g7H8i9J0kLmNoPqRsTuVwXyZ",
    };
    // Multibyte UTF-8 (CJK) injected at every offset around the 32-byte window.
    for (int pad : {0, 1, 30, 31, 32, 33, 63, 64, 65}) {
        inputs.push_back(std::string(static_cast<size_t>(pad), 'A') + cjk + "ZZ");
    }
    for (bool lower : {true, false}) {
        for (const auto& in : inputs) {
            EXPECT_EQ(tokenize(in, lower), ref_tokenize(in, lower))
                    << "lower=" << lower << " input(len=" << in.size() << ")";
        }
    }
}

TEST(BasicTokenizerDifferentialTest, RandomizedAsciiAndDelimiters) {
    std::mt19937 rng(0xC0FFEEU);
    // Bias toward ASCII letters/digits/spaces/punct so we get long alnum runs
    // that cross AVX2 windows, interleaved with delimiters.
    const std::string alphabet =
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 "
            "                       .,!?-_/\t\n";
    std::uniform_int_distribution<size_t> pick(0, alphabet.size() - 1);
    std::uniform_int_distribution<size_t> len(0, 200);
    for (int iter = 0; iter < 2000; ++iter) {
        std::string in;
        const size_t n = len(rng);
        in.reserve(n);
        for (size_t k = 0; k < n; ++k) {
            in.push_back(alphabet[pick(rng)]);
        }
        const bool lower = (iter & 1) != 0;
        EXPECT_EQ(tokenize(in, lower), ref_tokenize(in, lower))
                << "iter=" << iter << " lower=" << lower << " len=" << in.size();
    }
}

// Mix in arbitrary high-bit / multibyte bytes (well-formed CJK, accented
// 2-byte, and raw garbage bytes) so the differential exercises U8_NEXT's
// malformed-sequence handling (c < 0 -> skip) in addition to clean UTF-8.
TEST(BasicTokenizerDifferentialTest, RandomizedWithRawHighBitBytes) {
    std::mt19937 rng(0xBADF00DU);
    const std::string cjk = "\xE4\xBD\xA0"; // 你 (3-byte CJK)
    const std::string acc = "\xC3\xA9";     // é (2-byte non-CJK)
    std::uniform_int_distribution<int> kind(0, 6);
    std::uniform_int_distribution<int> raw(0x80, 0xFF); // high-bit-only bytes
    std::uniform_int_distribution<int> ascii('!', '~');
    std::uniform_int_distribution<size_t> len(0, 120);
    for (int iter = 0; iter < 2000; ++iter) {
        std::string in;
        const size_t n = len(rng);
        for (size_t k = 0; k < n; ++k) {
            switch (kind(rng)) {
            case 0:
            case 1:
            case 2:
                in.push_back(static_cast<char>(ascii(rng)));
                break;
            case 3:
                in.push_back(' ');
                break;
            case 4:
                in += cjk;
                break;
            case 5:
                in += acc;
                break;
            default:
                in.push_back(static_cast<char>(raw(rng))); // possibly malformed
                break;
            }
        }
        const bool lower = (iter & 1) != 0;
        EXPECT_EQ(tokenize(in, lower), ref_tokenize(in, lower))
                << "iter=" << iter << " lower=" << lower << " len=" << in.size();
    }
}

// Offset identity: emission order alone is verified above, but the brief also
// requires token *positions/offsets* to match. Positions are implicit (token
// index == position). For offsets, every emitted token slice must point inside
// the tokenizer's working buffer at the exact byte range the scalar reference
// would assign. We recover the scalar reference's (offset, length) spans and
// assert the real tokenizer emits tokens whose byte spans are identical, so a
// SIMD off-by-one in run delimiting would surface as an offset mismatch even if
// the lowercased text happened to collide.
namespace {

struct RefSpan {
    int32_t begin;
    int32_t end;
};

#define REF_IN_RANGE(c, lo, hi) ((uint32_t)((c) - (lo)) <= ((hi) - (lo)))
#define REF_IS_CHINESE(c)                                                      \
    (REF_IN_RANGE(c, 0x4E00, 0x9FFF) || REF_IN_RANGE(c, 0x3400, 0x4DBF) ||     \
     REF_IN_RANGE(c, 0x20000, 0x2A6DF) || REF_IN_RANGE(c, 0x2A700, 0x2EBEF) || \
     REF_IN_RANGE(c, 0x30000, 0x3134A))

// Same control flow as ref_tokenize but records source byte spans instead of
// (lowercased) text — spans are independent of the lowercase flag.
std::vector<RefSpan> ref_spans(const std::string& text) {
    const auto* s = reinterpret_cast<const uint8_t*>(text.data());
    const auto length = static_cast<int32_t>(text.size());
    std::vector<RefSpan> out;
    for (int32_t i = 0; i < length;) {
        if (is_alnum(s[i])) {
            const int32_t begin = i;
            while (i < length && is_alnum(s[i])) {
                ++i;
            }
            out.push_back({begin, i});
        } else {
            UChar32 c = U_UNASSIGNED;
            const int32_t prev_i = i;
            U8_NEXT(s, i, length, c);
            if (c < 0) {
                continue;
            }
            if (REF_IS_CHINESE(c)) {
                out.push_back({prev_i, i});
            }
        }
    }
    return out;
}

#undef REF_IS_CHINESE
#undef REF_IN_RANGE

// Drives the real tokenizer but returns each token's byte span within the
// internal buffer (recovered via the token's data pointer minus the buffer
// base, exposed through tokens_text()).
std::vector<RefSpan> tokenize_spans(const std::string& text, bool lowercase) {
    auto tk = std::make_shared<BasicTokenizer>();
    tk->initialize();
    tk->set_lowercase(lowercase);
    ReaderPtr reader = std::make_shared<lucene::util::SStringReader<char>>();
    reader->init(text.data(), static_cast<int32_t>(text.size()), false);
    tk->set_reader(reader);
    tk->reset();
    const auto& views = tk->tokens_text();
    if (views.empty()) {
        return {};
    }
    // tokens_text() slices alias one contiguous working buffer; use the first
    // slice's base as the origin so spans are comparable to the source offsets
    // (the tokenizer copies the input 1:1 into its buffer, length-preserving).
    const char* base = nullptr;
    int32_t min_off = 0;
    // Recover the buffer origin: the smallest data() across slices corresponds
    // to buffer offset == the source offset of the earliest token. Because the
    // buffer mirrors the source byte-for-byte, (slice.data() - base) + min_off
    // equals the source offset. We anchor base/min_off on the first token by
    // matching it against the scalar reference's first span.
    const auto ref = ref_spans(text);
    if (ref.empty()) {
        // Real tokenizer must also emit nothing.
        EXPECT_TRUE(views.empty());
        return {};
    }
    base = views.front().data();
    min_off = ref.front().begin;
    std::vector<RefSpan> out;
    out.reserve(views.size());
    for (const auto& v : views) {
        const auto rel = static_cast<int32_t>(v.data() - base) + min_off;
        out.push_back({rel, rel + static_cast<int32_t>(v.size())});
    }
    return out;
}

} // namespace

TEST(BasicTokenizerDifferentialTest, TokenByteSpansMatchReference) {
    const std::string cjk = "\xE4\xBD\xA0\xE5\xA5\xBD"; // 你好
    const std::string acc = "\xC3\xA9";                 // é
    std::vector<std::string> inputs = {
            "",
            "   ",
            "Hello World",
            "HeLLo!WORLD",
            std::string(40, 'A') + " Tail",
            std::string(33, 'Z') + "!" + std::string(40, 'q'),
            "token" + cjk + "Mixed",
            cjk + "ABC" + cjk + "123",
            "ascii" + acc + "Tail",
            "a1B2c3D4e5F6g7H8i9J0kLmNoPqRsTuVwXyZ extra",
    };
    for (int pad : {0, 1, 31, 32, 33, 63, 64, 65}) {
        inputs.push_back(std::string(static_cast<size_t>(pad), 'A') + cjk + "ZZ tail");
    }
    for (bool lower : {true, false}) {
        for (const auto& in : inputs) {
            const auto got = tokenize_spans(in, lower);
            const auto want = ref_spans(in);
            ASSERT_EQ(got.size(), want.size())
                    << "token count mismatch lower=" << lower << " len=" << in.size();
            for (size_t k = 0; k < want.size(); ++k) {
                EXPECT_EQ(got[k].begin, want[k].begin)
                        << "token[" << k << "] begin mismatch lower=" << lower;
                EXPECT_EQ(got[k].end, want[k].end)
                        << "token[" << k << "] end mismatch lower=" << lower;
            }
        }
    }
}

// ---- Direct SIMD-vs-scalar identity (build-config independent) -------------
//
// The differential tests above compare the *active* build's tokenizer against a
// hand-written scalar reference. Under the CI default (USE_AVX2=ON) that proves
// the AVX2 arm is correct but never exercises the scalar `#else` arm of
// `scan_ascii_alnum_run` / `ascii_lower_inplace`, and vice-versa under
// USE_AVX2=0. These tests instead pit the real (build-selected) implementation
// against an INDEPENDENT, always-scalar reimplementation of the exact same
// contract, compiled unconditionally here regardless of -mavx2. So whichever
// arm the build selected is locked byte-for-byte against scalar truth.
namespace {

// Portable scalar twin of scan_ascii_alnum_run — identical branchless predicate
// and partial-run lowercase semantics, but no SIMD and no #ifdef. Operates on a
// private copy so we can compare both the returned end index and the mutated
// bytes.
template <bool Lowercase>
int32_t ref_scan_ascii_alnum_run(uint8_t* s, int32_t start, int32_t length) {
    for (int32_t i = start; i < length; ++i) {
        const uint8_t c = s[i];
        if ((c & 0x80U) != 0U) {
            return i;
        }
        const bool is_a = (static_cast<uint8_t>(c - '0') <= 9U) ||
                          (static_cast<uint8_t>(c - 'A') <= 25U) ||
                          (static_cast<uint8_t>(c - 'a') <= 25U);
        if (!is_a) {
            return i;
        }
        if constexpr (Lowercase) {
            if (static_cast<uint8_t>(c - 'A') <= 25U) {
                s[i] = static_cast<uint8_t>(c | 0x20U);
            }
        }
    }
    return length;
}

template <bool Lowercase>
void assert_scan_identity(const std::string& in, int32_t start) {
    std::vector<uint8_t> a(in.begin(), in.end());
    std::vector<uint8_t> b = a;
    const auto len = static_cast<int32_t>(a.size());
    const int32_t got = scan_ascii_alnum_run<Lowercase>(a.data(), start, len);
    const int32_t want = ref_scan_ascii_alnum_run<Lowercase>(b.data(), start, len);
    ASSERT_EQ(got, want) << "end index diverged start=" << start << " len=" << len
                         << " lower=" << Lowercase;
    ASSERT_EQ(a, b) << "mutated bytes diverged start=" << start << " len=" << len
                    << " lower=" << Lowercase;
}

bool ref_ascii_lower_inplace(const char* src, char* dst, size_t len) {
    for (size_t i = 0; i < len; ++i) {
        const auto c = static_cast<unsigned char>(src[i]);
        if (c & 0x80U) {
            return false;
        }
        dst[i] = (static_cast<unsigned char>(c - 'A') <= 25U) ? static_cast<char>(c | 0x20)
                                                              : static_cast<char>(c);
    }
    return true;
}

} // namespace

TEST(SimdScalarTokenizerIdentityTest, ScanMatchesScalarOnRandomInputs) {
    std::mt19937 rng(0x5EEDU);
    // Heavy alnum bias to produce multi-window runs that terminate at every
    // possible position within and across 32-byte windows.
    const std::string alphabet =
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
            "    !._-\x80\xC3\xA9"; // include high-bit + a valid 2-byte lead/cont
    std::uniform_int_distribution<size_t> pick(0, alphabet.size() - 1);
    std::uniform_int_distribution<size_t> len(0, 160);
    for (int iter = 0; iter < 4000; ++iter) {
        std::string in;
        const size_t n = len(rng);
        in.reserve(n);
        for (size_t k = 0; k < n; ++k) {
            in.push_back(alphabet[pick(rng)]);
        }
        // start at 0 and (when non-empty) a random interior offset.
        assert_scan_identity<true>(in, 0);
        assert_scan_identity<false>(in, 0);
        if (!in.empty()) {
            std::uniform_int_distribution<int32_t> off(0, static_cast<int32_t>(in.size()) - 1);
            const int32_t s = off(rng);
            assert_scan_identity<true>(in, s);
            assert_scan_identity<false>(in, s);
        }
    }
}

TEST(SimdScalarTokenizerIdentityTest, ScanMatchesScalarAtEveryBoundaryLength) {
    // Exhaustively walk lengths around the 32/64-byte windows with the run
    // terminator placed at every offset, so a SIMD off-by-one anywhere in the
    // window-boundary math diverges from scalar.
    for (int len = 0; len <= 80; ++len) {
        for (int term = 0; term <= len; ++term) {
            std::string in(static_cast<size_t>(len), 'A'); // all-uppercase alnum
            if (term < len) {
                in[static_cast<size_t>(term)] = '!'; // first terminator at `term`
            }
            assert_scan_identity<true>(in, 0);
            assert_scan_identity<false>(in, 0);
        }
    }
}

TEST(SimdScalarTokenizerIdentityTest, AsciiLowerMatchesScalarOnRandomInputs) {
    std::mt19937 rng(0xABCDU);
    std::uniform_int_distribution<int> byte(0, 0xFF);
    std::uniform_int_distribution<size_t> len(0, 200);
    for (int iter = 0; iter < 4000; ++iter) {
        std::string in;
        const size_t n = len(rng);
        in.reserve(n);
        for (size_t k = 0; k < n; ++k) {
            in.push_back(static_cast<char>(byte(rng)));
        }
        std::string a(n, '\0');
        std::string b(n, '\0');
        const bool ra = ascii_lower_inplace(in.data(), a.data(), n);
        const bool rb = ref_ascii_lower_inplace(in.data(), b.data(), n);
        ASSERT_EQ(ra, rb) << "return diverged iter=" << iter << " len=" << n;
        if (ra) {
            // Only the success path writes the full buffer; on bail (false) the
            // partial contents are unspecified and ICU re-reads the original.
            ASSERT_EQ(a, b) << "lowercased bytes diverged iter=" << iter << " len=" << n;
        }
    }
}

} // namespace doris::segment_v2::inverted_index
