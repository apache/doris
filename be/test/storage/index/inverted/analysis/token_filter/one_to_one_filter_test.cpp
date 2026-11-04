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

// The one-to-one filters, lowercase, icu_normalizer and asciifolding without preserve_original,
// rewrite the batches of their input where they stand. Each is checked against a per-token filter
// running the code it replaced, on random token streams: the same terms, increments and
// exceptions, however the input cuts its batches and wherever its terms live.

#include <gtest/gtest.h>
#include <unicode/normalizer2.h>
#include <unicode/ucasemap.h>
#include <unicode/unistr.h>
#include <unicode/utf8.h>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <random>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/check.h"
#include "common/exception.h"
#include "storage/index/inverted/analysis/per_token_stream.h"
#include "storage/index/inverted/analysis/text_stream_contract.h"
#include "storage/index/inverted/analysis/token_batch.h"
#include "storage/index/inverted/analysis/token_filter/ascii_folding_filter_factory.h"
#include "storage/index/inverted/analysis/token_filter/icu_normalizer_filter_factory.h"
#include "storage/index/inverted/analysis/token_filter/lower_case_filter_factory.h"
#include "util/utf8_check.h"

namespace doris::segment_v2::inverted_index {
namespace {

struct ScriptedToken {
    std::string term;
    int32_t position_increment = 1;
    // Pushed as a copy into the term storage of the batch, which the filter then grows; otherwise
    // as a view of the script.
    bool in_batch = false;
};

// The batches of one value.
using Script = std::vector<std::vector<ScriptedToken>>;

// Hands out the batches of its script, whatever the text.
class ScriptedBatchInput final : public TextTokenizer {
public:
    explicit ScriptedBatchInput(const Script& script) : _script(script) {}

    void reset(std::string_view /*text*/) override { _next = 0; }

    bool next_batch(TokenBatch& batch) override {
        batch.clear();
        if (_next == _script.size()) {
            return false;
        }
        for (const ScriptedToken& token : _script[_next++]) {
            if (token.in_batch) {
                batch.push_copy(token.term, token.position_increment);
            } else {
                batch.push(token.term, token.position_increment);
            }
        }
        return true;
    }

    size_t reserved_bytes() const override { return 0; }

private:
    const Script& _script;
    size_t _next = 0;
};

// What a filter did to one token before it rewrote batches: the new term, or nothing to keep it.
using Rewrite = std::function<std::optional<std::string>(std::string_view)>;

// The per-token form of a one-to-one filter, as the three filters were.
class PerTokenRewrite final : public PerTokenFilter {
public:
    PerTokenRewrite(std::unique_ptr<TextTokenStream> in, Rewrite rewrite)
            : PerTokenFilter(std::move(in)), _rewrite(std::move(rewrite)) {}

protected:
    bool next_token(TokenRef& token) override {
        if (!input_token(token)) {
            return false;
        }
        if (std::optional<std::string> term = _rewrite(token.term())) {
            _term = std::move(*term);
            token.data = _term.data();
            token.size = static_cast<uint32_t>(_term.size());
        }
        return true;
    }

private:
    Rewrite _rewrite;
    std::string _term;
};

// LowerCaseFilter before it rewrote batches: ASCII byte by byte, anything else validated and then
// lowercased with the root-locale case map.
std::optional<std::string> lower_case_per_token(std::string_view term) {
    bool ascii = true;
    bool upper = false;
    for (const char c : term) {
        const auto byte = static_cast<uint8_t>(c);
        ascii &= byte < 0x80;
        upper |= byte >= 'A' && byte <= 'Z';
    }
    if (ascii) {
        if (!upper) {
            return std::nullopt;
        }
        std::string lower(term);
        for (char& c : lower) {
            c = c >= 'A' && c <= 'Z' ? static_cast<char>(c - 'A' + 'a') : c;
        }
        return lower;
    }
    if (!validate_utf8(term.data(), term.size())) {
        throw Exception(ErrorCode::INVERTED_INDEX_ANALYZER_ERROR,
                        "Failed to lowercase token: invalid UTF-8");
    }
    static const UCaseMap* const case_map = [] {
        UErrorCode status = U_ZERO_ERROR;
        const UCaseMap* map = ucasemap_open("", 0, &status);
        DORIS_CHECK(U_SUCCESS(status));
        return map;
    }();
    // Lowercasing at most doubles a term.
    std::string lower(2 * term.size(), '\0');
    UErrorCode status = U_ZERO_ERROR;
    const int32_t length =
            ucasemap_utf8ToLower(case_map, lower.data(), static_cast<int32_t>(lower.size()),
                                 term.data(), static_cast<int32_t>(term.size()), &status);
    DORIS_CHECK(U_SUCCESS(status));
    lower.resize(length);
    return lower;
}

// ICUNormalizerFilter before it rewrote batches.
std::optional<std::string> normalize_per_token(const icu::Normalizer2& normalizer,
                                               std::string_view term) {
    UErrorCode status = U_ZERO_ERROR;
    const icu::UnicodeString src16 = icu::UnicodeString::fromUTF8(
            icu::StringPiece(term.data(), static_cast<int32_t>(term.size())));
    if (normalizer.quickCheck(src16, status) == UNORM_YES && U_SUCCESS(status)) {
        return std::nullopt;
    }
    icu::UnicodeString result16;
    status = U_ZERO_ERROR;
    normalizer.normalize(src16, result16, status);
    DORIS_CHECK(U_SUCCESS(status));
    std::string result;
    result16.toUTF8String(result);
    return result;
}

// ASCIIFoldingFilter before it rewrote batches: a term with a valid character at or above U+0080
// is folded, any other is kept.
std::optional<std::string> fold_per_token(std::string_view term) {
    const char* text = term.data();
    const auto length = static_cast<int32_t>(term.size());
    for (int32_t i = 0; i < length;) {
        UChar32 c = U_UNASSIGNED;
        U8_NEXT(text, i, length, c);
        if (c >= 0x80) {
            std::string folded(2 * term.size(), '\0');
            folded.resize(ASCIIFoldingFilter::fold_to_ascii(text, 0, folded.data(), 0, length));
            return folded;
        }
    }
    return std::nullopt;
}

std::string utf8_of(UChar32 c) {
    char buffer[U8_MAX_LENGTH];
    int32_t length = 0;
    U8_APPEND_UNSAFE(buffer, length, c);
    return {buffer, static_cast<size_t>(length)};
}

// Characters that take different paths through the three filters: cased, case-expanding, folded,
// normalized, combining, caseless CJK just inside and outside U+3000..U+9FFF, supplementary.
const std::vector<std::string>& unicode_pieces() {
    static const std::vector<std::string> pieces = {
            "é", "É", "Ü", "ß", "ẞ", "İ", "ı", "Σ", "ς", "Ω", "Ⱥ", "Ⱦ", "Ǆ", "ǅ", "Я", "я", "Ⓐ",
            "Ａ", "ｆ", "ﬁ", "ﬃ", "⒇", "⑽", "½", "Ⅻ", "㎏", "ꜳ", "Œ", "Þ", "中", "文", "あ", "カ",
            "한",
            // KELVIN SIGN and ANGSTROM SIGN, which lowercase and normalize to ASCII k and å.
            "\xE2\x84\xAA", "\xE2\x84\xAB",
            // A combining acute alone and after e, and Hangul jamo that compose to 가.
            "\xCC\x81", "e\xCC\x81", "\xE1\x84\x80\xE1\x85\xA1",
            // SOFT HYPHEN and ZERO WIDTH SPACE, which NFKC_CF removes, and U+FFFD.
            "\xC2\xAD", "\xE2\x80\x8B", "\xEF\xBF\xBD",
            // U+3000 and U+9FFF, the ends of the caseless range, and U+A000 and U+2FFF next to it.
            "\xE3\x80\x80", "\xE9\xBF\xBF", "\xEA\x80\x80", "\xE2\xBF\xBF",
            // Supplementary: an emoji, DESERET CAPITAL LONG I, MATHEMATICAL BOLD CAPITAL A.
            "\U0001F600", "\U00010400", "\U0001D400", std::string("\0", 1)};
    return pieces;
}

// Malformed UTF-8: stray, truncated, overlong, surrogate and out-of-range sequences, and leads of
// E3..E9 without their continuation bytes.
const std::vector<std::string>& invalid_pieces() {
    static const std::vector<std::string> pieces = {"\xFF",
                                                    "\x80",
                                                    "\xBF",
                                                    "\xC3",
                                                    "\xC0\x80",
                                                    "\xE4\xB8",
                                                    "\xE4"
                                                    "AA",
                                                    "\xE4\xB8"
                                                    "A",
                                                    "\xE9\xBF\xFF",
                                                    "\xED\xA0\x80",
                                                    "\xF0\x9F\x98",
                                                    "\xF4\x90\x80\x80",
                                                    "\xE0\x80\x80",
                                                    "\xFF\xC3\x86",
                                                    "\xC3\x86\xFF"};
    return pieces;
}

std::string random_ascii(std::mt19937_64& random, bool capital) {
    static constexpr std::string_view kLower = "abcdefghijklmnopqrstuvwxyz0123456789-_.@";
    static constexpr std::string_view kUpper = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    std::string term;
    const size_t size = 1 + random() % 12;
    for (size_t i = 0; i < size; ++i) {
        term.push_back(kLower[random() % kLower.size()]);
    }
    if (capital) {
        term[random() % size] = kUpper[random() % kUpper.size()];
    }
    return term;
}

// A term of one of the classes the filters tell apart, or a malformed one.
std::string random_term(std::mt19937_64& random, bool malformed) {
    if (malformed) {
        std::string term = random() % 2 == 0 ? random_ascii(random, random() % 2 == 0) : "";
        term += invalid_pieces()[random() % invalid_pieces().size()];
        if (random() % 2 == 0) {
            term += unicode_pieces()[random() % unicode_pieces().size()];
        }
        return term;
    }
    switch (random() % 12) {
    case 0:
    case 1:
        return random_ascii(random, false);
    case 2:
    case 3:
        return random_ascii(random, true);
    case 4:
    case 5:
        // One CJK character of three bytes.
        return utf8_of(static_cast<UChar32>(0x3000 + random() % 0x7000));
    case 6:
        return "";
    case 7:
    case 8:
    case 9:
    case 10: {
        std::string term;
        const size_t pieces = 1 + random() % 4;
        for (size_t i = 0; i < pieces; ++i) {
            term += random() % 3 == 0 ? random_ascii(random, random() % 2 == 0)
                                      : unicode_pieces()[random() % unicode_pieces().size()];
        }
        return term;
    }
    default: {
        // A long term, longer than the inline buffer of an icu::UnicodeString.
        std::string term;
        const size_t pieces = 20 + random() % 60;
        for (size_t i = 0; i < pieces; ++i) {
            term += unicode_pieces()[random() % unicode_pieces().size()];
        }
        return term;
    }
    }
}

// One value in 1 to 4 batches of 1 to 300 tokens; a token is malformed with the given odds.
Script random_script(std::mt19937_64& random, size_t malformed_per_mille) {
    Script script(1 + random() % 4);
    for (std::vector<ScriptedToken>& batch : script) {
        batch.resize(1 + random() % 300);
        for (ScriptedToken& token : batch) {
            token.term = random_term(random, random() % 1000 < malformed_per_mille);
            token.position_increment = static_cast<int32_t>(random() % 4);
            token.in_batch = random() % 2 == 0;
        }
    }
    return script;
}

using MakeFilter =
        std::function<std::unique_ptr<TextTokenStream>(std::unique_ptr<TextTokenStream>)>;

// The tokens of the value `script` scripts, or the exception the filter threw.
struct Outcome {
    std::vector<CollectedToken> tokens;
    bool threw = false;
    int code = 0;
    std::string message;

    bool operator==(const Outcome&) const = default;
};

Outcome read(const Script& script, const MakeFilter& make_filter) {
    auto stream = make_filter(std::make_unique<ScriptedBatchInput>(script));
    Outcome outcome;
    try {
        outcome.tokens = collect_checked(*stream, "");
    } catch (const Exception& error) {
        outcome.threw = true;
        outcome.code = error.code();
        outcome.message = error.message();
    }
    return outcome;
}

size_t tokens_of(const Script& script) {
    size_t tokens = 0;
    for (const auto& batch : script) {
        tokens += batch.size();
    }
    return tokens;
}

constexpr size_t kValues = 100;
// The values of every third index may have malformed tokens.
constexpr size_t kValuesWithMalformedTokens = (kValues + 2) / 3;

// `make_filter` against the per-token `rewrite` on random values; those of every third index have
// malformed tokens with the given odds. Returns how many values threw, so a caller can tell that
// both kinds of value were met.
size_t expect_matches_per_token(const MakeFilter& make_filter, const Rewrite& rewrite,
                                size_t malformed_per_mille, uint64_t seed) {
    std::mt19937_64 random(seed);
    const MakeFilter per_token = [&](std::unique_ptr<TextTokenStream> in) {
        return std::make_unique<PerTokenRewrite>(std::move(in), rewrite);
    };
    size_t threw = 0;
    for (size_t value = 0; value < kValues; ++value) {
        const Script script = random_script(random, value % 3 == 0 ? malformed_per_mille : 0);
        const Outcome expected = read(script, per_token);
        const Outcome actual = read(script, make_filter);
        EXPECT_EQ(actual, expected) << "value " << value << " of " << tokens_of(script)
                                    << " tokens in " << script.size() << " batches";
        threw += expected.threw ? 1 : 0;
    }
    return threw;
}

TEST(OneToOneFilterTest, LowerCaseMatchesThePerTokenFilter) {
    LowerCaseFilterFactory factory;
    factory.initialize({});
    // About one malformed token in each value that may have one: most of those values throw, at
    // any token of any batch, and the others do not.
    const size_t threw = expect_matches_per_token(
            [&](std::unique_ptr<TextTokenStream> in) { return factory.create(std::move(in)); },
            lower_case_per_token, 3, 20261104);
    EXPECT_GT(threw, 0);
    EXPECT_LT(threw, kValuesWithMalformedTokens);
}

TEST(OneToOneFilterTest, ASCIIFoldingMatchesThePerTokenFilter) {
    ASCIIFoldingFilterFactory factory;
    factory.initialize({});
    // Malformed tokens never throw here: folding drops their stray bytes or keeps them.
    const size_t threw = expect_matches_per_token(
            [&](std::unique_ptr<TextTokenStream> in) { return factory.create(std::move(in)); },
            fold_per_token, 100, 20261105);
    EXPECT_EQ(threw, 0);
}

Settings setting_of(const std::string& key, const std::string& value) {
    Settings settings;
    settings.set(key, value);
    return settings;
}

TEST(OneToOneFilterTest, ICUNormalizerMatchesThePerTokenFilter) {
    // NFKC_CF, two other forms, and Elasticsearch's example of a set filter, which leaves the
    // Swedish letters alone.
    const std::vector<Settings> settings = {Settings(), setting_of("name", "nfd"),
                                            setting_of("name", "nfkc"),
                                            setting_of("unicode_set_filter", "[^åäöÅÄÖ]")};
    uint64_t seed = 20261106;
    for (const Settings& setting : settings) {
        ICUNormalizerFilterFactory factory;
        factory.initialize(setting);
        const icu::Normalizer2& normalizer = *factory._normalizer;
        const size_t threw = expect_matches_per_token(
                [&](std::unique_ptr<TextTokenStream> in) { return factory.create(std::move(in)); },
                [&](std::string_view term) { return normalize_per_token(normalizer, term); }, 100,
                seed++);
        EXPECT_EQ(threw, 0);
    }
}

// A term a filter leaves as it is keeps its view of the input; a changed one points into the
// term storage of the batch.
void expect_views(const MakeFilter& make_filter,
                  const std::vector<std::pair<std::string, std::optional<std::string>>>& cases) {
    Script script(1);
    for (const auto& [term, changed] : cases) {
        script[0].push_back({.term = term, .position_increment = 1, .in_batch = false});
    }
    auto stream = make_filter(std::make_unique<ScriptedBatchInput>(script));
    stream->reset("");
    TokenBatch batch;
    ASSERT_TRUE(stream->next_batch(batch));
    ASSERT_EQ(batch.size(), cases.size());
    for (size_t i = 0; i < cases.size(); ++i) {
        SCOPED_TRACE(cases[i].first);
        const TokenRef& token = batch.tokens()[i];
        if (cases[i].second.has_value()) {
            EXPECT_EQ(token.term(), *cases[i].second);
            EXPECT_NE(token.data, script[0][i].term.data());
        } else {
            EXPECT_EQ(token.term(), cases[i].first);
            EXPECT_EQ(token.data, script[0][i].term.data());
        }
    }
    EXPECT_FALSE(stream->next_batch(batch));
}

TEST(OneToOneFilterTest, ASCIIFoldingKeepsTheViewsOfTermsItLeaves) {
    ASCIIFoldingFilterFactory factory;
    factory.initialize({});
    // "中" folds to itself; a stray byte without a character to fold is kept, and dropped along
    // with folding one.
    expect_views([&](std::unique_ptr<TextTokenStream> in) { return factory.create(std::move(in)); },
                 {{"abc", std::nullopt},
                  {"", std::nullopt},
                  {"中", std::nullopt},
                  {"a\xFF", std::nullopt},
                  {"café", "cafe"},
                  {"\xFF\xC3\x86", "AE"},
                  {"⒇", "(20)"}});
}

TEST(OneToOneFilterTest, ICUNormalizerKeepsTheViewsOfTermsItLeaves) {
    ICUNormalizerFilterFactory factory;
    factory.initialize({});
    expect_views([&](std::unique_ptr<TextTokenStream> in) { return factory.create(std::move(in)); },
                 {{"abc", std::nullopt},
                  {"", std::nullopt},
                  {"中", std::nullopt},
                  {"ABC", "abc"},
                  {"ﬁ", "fi"},
                  {"Ｆｕｌｌ", "full"}});
}

} // namespace
} // namespace doris::segment_v2::inverted_index
