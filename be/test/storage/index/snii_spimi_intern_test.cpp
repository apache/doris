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

// T05: SPIMI vocab transparent-hash interning + single-string storage.
//
// These tests pin the writer-side SpimiTermBuffer owned-mode interning to its new
// shape: each distinct vocab string is materialized into owned_vocab_ EXACTLY ONCE
// (no double-store, no per-token temporary probe std::string), and the term-id
// assignment / finalize output is byte-identical to the prior behavior. Writer-only,
// no reader fixture (build_reader) needed -- the buffer is driven directly via
// add_token(string_view) and drained via finalize_sorted().

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "common/exception.h"
#include "common/status.h"
#include "storage/index/inverted/common_grams/common_grams_key_codec.h"
#include "storage/index/inverted/common_grams/common_word_set.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/writer/spimi_term_buffer.h"
#include "util/md5.h"

using doris::snii::writer::ClassifiedPlainTerm;
using doris::snii::writer::PlainTermId;
using doris::snii::writer::SpimiTermBuffer;
using doris::snii::writer::StreamedTermPostings;
using doris::snii::writer::TermPostingBuffer;
using doris::snii::writer::TermPostings;
using doris::Status;
// Alias the writer's TEST-ONLY counter namespace so it never collides with gtest's
// own ::testing namespace.
namespace stb_testing = doris::snii::writer::testing;

namespace {

// An ordinary >SSO ASCII term with a prefix shared by several cases below.
std::string MixedLongAscii() {
    return "ordinary-prefix-sharing-quick-brown-term";
}

// A >SSO multibyte (CJK) token (a direct UTF-8 string literal): 18 bytes (6 chars x
// 3 bytes), well past libstdc++'s 15-byte SSO, exercising a long non-ASCII vocab key.
std::string MixedCjk() {
    return "中文长词条目";
}

Status MaterializeInIrregularWindows(StreamedTermPostings&& source, TermPostings* output) {
    constexpr std::array<size_t, 7> kChunkSizes = {1, 3, 17, 64, 127, 251, 509};
    if (source.source == nullptr || output == nullptr) {
        return Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>(
                "test posting materializer: invalid arguments");
    }
    *output = TermPostings();
    output->term = std::move(source.term);
    output->retain_positions = source.retain_positions;
    TermPostingBuffer buffer(nullptr);
    size_t chunk_index = 0;
    bool exhausted = false;
    while (!exhausted) {
        buffer.clear_reuse();
        const uint32_t target_docs =
                static_cast<uint32_t>(kChunkSizes[chunk_index % kChunkSizes.size()]);
        RETURN_IF_ERROR(source.source->fill(target_docs, &buffer, &exhausted));
        if (!exhausted && buffer.document_count() != target_docs) {
            return Status::Error<doris::ErrorCode::INTERNAL_ERROR, false>(
                    "posting source returned a short non-terminal fill");
        }
        output->docids.insert(output->docids.end(), buffer.docids().begin(), buffer.docids().end());
        output->freqs.insert(output->freqs.end(), buffer.freqs().begin(), buffer.freqs().end());
        output->positions_flat.insert(output->positions_flat.end(), buffer.positions_flat().begin(),
                                      buffer.positions_flat().end());
        ++chunk_index;
    }
    return Status::OK();
}

std::vector<uint32_t> AddSortedStatlessCommonGram(SpimiTermBuffer* common_grams,
                                                  uint32_t document_count) {
    common_grams->enable_common_gram_pair_keys();
    const PlainTermId left = common_grams->intern_plain_term("of");
    const PlainTermId right = common_grams->intern_plain_term("world");
    std::vector<uint32_t> expected;
    expected.reserve(document_count);
    for (uint32_t ordinal = 0; ordinal < document_count; ++ordinal) {
        const uint32_t docid = 3 * ordinal + 1;
        expected.push_back(docid);
        common_grams->add_common_gram(left, right, docid, /*pos=*/0,
                                      /*retain_positions=*/false);
    }
    return expected;
}

// Feeds a fixed, implementation-independent token script mixing a long ASCII
// term, a short ASCII term, and a >SSO CJK token across several docids (some
// re-touched). Two buffers fed this script must finalize identically.
void FeedMixedScript(SpimiTermBuffer& b) {
    const std::string long_ascii = MixedLongAscii();
    const std::string cjk = MixedCjk();
    b.add_token(std::string_view("alpha"), 0, 0);
    b.add_token(std::string_view(long_ascii), 0, 1);
    b.add_token(std::string_view(cjk), 0, 2);
    b.add_token(std::string_view("alpha"), 1, 0);
    b.add_token(std::string_view(long_ascii), 1, 1);
    b.add_token(std::string_view("alpha"), 5, 3);
    b.add_token(std::string_view(cjk), 5, 4);
    b.add_token(std::string_view("alpha"), 9, 0);
}

void ExpectPostingsEqual(const std::vector<TermPostings>& a, const std::vector<TermPostings>& b) {
    ASSERT_EQ(a.size(), b.size());
    for (size_t i = 0; i < a.size(); ++i) {
        EXPECT_EQ(a[i].term, b[i].term);
        EXPECT_EQ(a[i].docids, b[i].docids);
        EXPECT_EQ(a[i].freqs, b[i].freqs);
        EXPECT_EQ(a[i].positions_flat, b[i].positions_flat);
        EXPECT_EQ(a[i].retain_positions, b[i].retain_positions);
    }
}

namespace inverted_index = doris::segment_v2::inverted_index;

enum class CommonGramsPostingEventKind { kPlain, kGram };

struct CommonGramsPostingEvent {
    CommonGramsPostingEventKind kind;
    std::string left;
    std::string right;
    uint32_t docid;
    uint32_t position;
    bool retain_positions;
};

std::string PhysicalPlainTerm(std::string_view logical_term) {
    if (logical_term.front() != inverted_index::PLAIN_ESCAPE_PREFIX &&
        logical_term.front() != '\x1f') {
        return std::string(logical_term);
    }
    std::string physical_term;
    EXPECT_TRUE(inverted_index::try_encode_escaped_plain_term_prevalidated(logical_term,
                                                                           physical_term));
    return physical_term;
}

size_t PlainHotCacheSet(std::string_view term) {
    constexpr size_t kPlainHotCacheSetCount = 1024;
    const size_t raw_hash = SpimiTermBuffer::hash_term_bytes_for_test(term);
    const size_t mixed_hash = phmap::phmap_mix<sizeof(size_t)>()(raw_hash);
    return mixed_hash & (kPlainHotCacheSetCount - 1);
}

std::array<std::string, 2> FindPlainHotCacheColliders(std::string_view target) {
    std::array<std::string, 2> result;
    size_t found = 0;
    for (uint64_t candidate = 0; found < result.size(); ++candidate) {
        std::string term = "plain-cache-collider-" + std::to_string(candidate);
        if (PlainHotCacheSet(term) == PlainHotCacheSet(target)) {
            result[found++] = std::move(term);
        }
    }
    return result;
}

size_t CommonGramPairL0Index(uint32_t left, uint32_t right) {
    constexpr uint64_t kHashMultiplier = 11400714819323198485ULL;
    const uint64_t pair = (static_cast<uint64_t>(left) << 32) | right;
    return static_cast<size_t>((pair * kHashMultiplier) >> 54);
}

std::optional<uint32_t> FindCommonGramPairL0RightCollider(uint32_t left, uint32_t target_right) {
    const size_t target_index = CommonGramPairL0Index(left, target_right);
    for (uint32_t candidate = target_right + 1; candidate < 4096; ++candidate) {
        if (CommonGramPairL0Index(left, candidate) == target_index) {
            return candidate;
        }
    }
    return std::nullopt;
}

std::string NativePairPlainTerm(uint32_t id) {
    return "native-pair-plain-" + std::to_string(id);
}

const std::vector<std::vector<std::string>>& CommonGramsLogicalDocuments() {
    static const std::string internal = std::string(1, '\x1f') + "literal";
    static const std::string escaped =
            std::string(1, inverted_index::PLAIN_ESCAPE_PREFIX) + "literal";
    static const std::vector<std::vector<std::string>> documents = {
            {internal, "of", "the", escaped, internal, "of"},
            {internal, "of", "the"},
    };
    return documents;
}

void FeedSeparateCommonGramAndPlainAdds(SpimiTermBuffer* buffer) {
    buffer->enable_common_gram_pair_keys();
    const auto& common_words = inverted_index::CommonWordSet::builtin_english_stop_words_v1();
    const auto& documents = CommonGramsLogicalDocuments();
    for (uint32_t docid = 0; docid < documents.size(); ++docid) {
        std::optional<PlainTermId> previous;
        bool previous_is_common = false;
        for (uint32_t position = 0; position < documents[docid].size(); ++position) {
            const std::string& logical = documents[docid][position];
            const PlainTermId current =
                    buffer->intern_plain_term(PhysicalPlainTerm(logical), logical);
            const bool current_is_common = common_words.contains(logical);
            if (previous.has_value() && (previous_is_common || current_is_common)) {
                buffer->add_common_gram(*previous, current, docid, position,
                                        previous_is_common && current_is_common);
            }
            buffer->add_plain_token(current, docid, position + 1);
            previous = current;
            previous_is_common = current_is_common;
        }
    }
}

void FeedFusedCommonGramAndPlainAdds(SpimiTermBuffer* buffer) {
    buffer->enable_common_gram_pair_keys();
    const auto& common_words = inverted_index::CommonWordSet::builtin_english_stop_words_v1();
    const auto& documents = CommonGramsLogicalDocuments();
    for (uint32_t docid = 0; docid < documents.size(); ++docid) {
        std::optional<ClassifiedPlainTerm> previous;
        for (uint32_t position = 0; position < documents[docid].size(); ++position) {
            const std::string& logical = documents[docid][position];
            const ClassifiedPlainTerm current = buffer->intern_classified_plain_term(
                    PhysicalPlainTerm(logical), logical, common_words);
            if (previous.has_value() && (previous->is_common || current.is_common)) {
                buffer->add_common_gram_and_plain(previous->id, current.id, docid, position,
                                                  position + 1,
                                                  previous->is_common && current.is_common);
            } else {
                buffer->add_plain_token(current.id, docid, position + 1);
            }
            previous = current;
        }
    }
}

std::vector<CommonGramsPostingEvent> CommonGramsPostingScript() {
    const std::string internal_plain = std::string(1, '\x1f') + "literal";
    const std::string escaped_plain =
            std::string(1, inverted_index::PLAIN_ESCAPE_PREFIX) + "literal";
    const std::string utf8 = "中文长词条目";
    return {
            {CommonGramsPostingEventKind::kPlain, internal_plain, {}, 0, 0, true},
            {CommonGramsPostingEventKind::kGram, internal_plain, "of", 0, 0, false},
            {CommonGramsPostingEventKind::kPlain, "of", {}, 0, 1, true},
            {CommonGramsPostingEventKind::kGram, "of", "the", 0, 1, true},
            {CommonGramsPostingEventKind::kPlain, "the", {}, 0, 2, true},
            {CommonGramsPostingEventKind::kGram, "the", escaped_plain, 0, 2, false},
            {CommonGramsPostingEventKind::kPlain, escaped_plain, {}, 0, 3, true},
            {CommonGramsPostingEventKind::kGram, internal_plain, "of", 0, 8, false},
            {CommonGramsPostingEventKind::kGram, "of", "the", 0, 9, true},
            {CommonGramsPostingEventKind::kPlain, internal_plain, {}, 1, 10, true},
            {CommonGramsPostingEventKind::kGram, internal_plain, "of", 1, 10, false},
            {CommonGramsPostingEventKind::kPlain, "of", {}, 1, 11, true},
            {CommonGramsPostingEventKind::kGram, "of", "the", 1, 11, true},
            {CommonGramsPostingEventKind::kPlain, "the", {}, 1, 12, true},
            {CommonGramsPostingEventKind::kGram, "the", utf8, 2, 0, false},
            {CommonGramsPostingEventKind::kPlain, utf8, {}, 2, 1, true},
    };
}

std::vector<CommonGramsPostingEvent> AdversarialPairSortScript() {
    const std::string control_before_escape = std::string(1, '\x1d') + "control";
    const std::string escape_prefix =
            std::string(1, inverted_index::PLAIN_ESCAPE_PREFIX) + "escaped";
    const std::string internal_prefix = std::string(1, '\x1f') + "internal";
    constexpr size_t kCommonGramLengthAndSeparatorBytes = 9;
    const std::string max_left(64, 'm');
    const size_t max_right_size = inverted_index::COMMON_GRAM_MAX_ENCODED_BYTES -
                                  inverted_index::CG_V1_MARKER.size() -
                                  kCommonGramLengthAndSeparatorBytes - max_left.size();
    const std::string max_right(max_right_size, 'r');

    return {
            // A control-byte component pins physical/logical rank equivalence
            // immediately below the EscapedV1 namespace.
            {CommonGramsPostingEventKind::kGram, control_before_escape, "tail", 0, 1, true},
            // The physical common-gram order starts with encoded left length, not
            // left lexical order: one-byte "z" must precede two-byte "aa".
            {CommonGramsPostingEventKind::kGram, "aa", "right", 0, 2, true},
            {CommonGramsPostingEventKind::kGram, "z", "right", 0, 3, true},
            // Equal left components fall through to logical right-byte order.
            {CommonGramsPostingEventKind::kGram, "same", "z", 0, 4, true},
            {CommonGramsPostingEventKind::kGram, "same", "aa", 0, 5, true},
            // EscapedV1 physical bytes must be decoded before comparing logical
            // components; 0x1e and 0x1f are both reserved physical prefixes.
            {CommonGramsPostingEventKind::kGram, escape_prefix, "tail", 0, 6, true},
            {CommonGramsPostingEventKind::kGram, internal_prefix, "tail", 0, 7, true},
            {CommonGramsPostingEventKind::kGram, "same", escape_prefix, 0, 8, true},
            {CommonGramsPostingEventKind::kGram, "same", internal_prefix, 0, 9, true},
            // Exact maximum encodable physical key size exercises the trusted
            // materializer without weakening the writer's analyzer-side limit.
            {CommonGramsPostingEventKind::kGram, max_left, max_right, 0, 10, true},
    };
}

void MaybeRequestForcedSpill(SpimiTermBuffer* buffer, bool force_spill, size_t event_index) {
    if (force_spill && (event_index == 3 || event_index == 7 || event_index == 12)) {
        buffer->request_global_spill_for_test();
    }
}

void FeedPhysicalCommonGrams(SpimiTermBuffer* buffer,
                             const std::vector<CommonGramsPostingEvent>& events, bool force_spill) {
    for (size_t i = 0; i < events.size(); ++i) {
        const auto& event = events[i];
        if (event.kind == CommonGramsPostingEventKind::kPlain) {
            buffer->add_token(PhysicalPlainTerm(event.left), event.docid, event.position,
                              event.retain_positions);
        } else {
            std::string physical_gram;
            EXPECT_TRUE(inverted_index::try_encode_common_gram_prevalidated(event.left, event.right,
                                                                            physical_gram));
            buffer->add_token(physical_gram, event.docid, event.position, event.retain_positions);
        }
        MaybeRequestForcedSpill(buffer, force_spill, i);
    }
}

void FeedPairKeyCommonGrams(SpimiTermBuffer* buffer,
                            const std::vector<CommonGramsPostingEvent>& events, bool force_spill) {
    buffer->enable_common_gram_pair_keys();
    for (size_t i = 0; i < events.size(); ++i) {
        const auto& event = events[i];
        const PlainTermId left = buffer->intern_plain_term(PhysicalPlainTerm(event.left));
        if (event.kind == CommonGramsPostingEventKind::kPlain) {
            buffer->add_plain_token(left, event.docid, event.position);
        } else {
            const PlainTermId right = buffer->intern_plain_term(PhysicalPlainTerm(event.right));
            buffer->add_common_gram(left, right, event.docid, event.position,
                                    event.retain_positions);
        }
        MaybeRequestForcedSpill(buffer, force_spill, i);
    }
}

void ExpectPairKeysMatchPhysicalTerms(const std::vector<CommonGramsPostingEvent>& events,
                                      bool force_spill) {
    SpimiTermBuffer physical(/*has_positions=*/true);
    SpimiTermBuffer pair_keys(/*has_positions=*/true);
    if (force_spill) {
        for (SpimiTermBuffer* buffer : {&physical, &pair_keys}) {
            buffer->set_forced_spill_min_arena_bytes(0);
            buffer->set_max_run_files(1);
        }
    }

    FeedPhysicalCommonGrams(&physical, events, force_spill);
    FeedPairKeyCommonGrams(&pair_keys, events, force_spill);
    ASSERT_TRUE(physical.status().ok()) << physical.status();
    ASSERT_TRUE(pair_keys.status().ok()) << pair_keys.status();
    if (force_spill) {
        EXPECT_GE(physical.run_count_for_test(), 1U);
        EXPECT_GE(pair_keys.run_count_for_test(), 1U);
    }

    const std::vector<TermPostings> expected = physical.finalize_sorted();
    const std::vector<TermPostings> actual = pair_keys.finalize_sorted();
    ASSERT_TRUE(physical.status().ok()) << physical.status();
    ASSERT_TRUE(pair_keys.status().ok()) << pair_keys.status();
    ASSERT_EQ(actual.size(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(actual[i].term, expected[i].term);
        EXPECT_EQ(actual[i].docids, expected[i].docids);
        EXPECT_EQ(actual[i].retain_positions, expected[i].retain_positions);
        EXPECT_EQ(actual[i].positions_flat, expected[i].positions_flat);
        if (expected[i].retain_positions) {
            EXPECT_EQ(actual[i].freqs, expected[i].freqs);
        } else {
            EXPECT_TRUE(actual[i].freqs.empty());
        }
    }
}

} // namespace

TEST(SniiSpimiTermBufferTest, InternHashUsesFastStringViewHash) {
    const std::vector<std::string> terms = {
            "",
            "short",
            MixedLongAscii(),
            MixedCjk(),
            std::string("\x1F\x01\0\x03gram-key", 12),
            std::string("embedded\0nul", 12),
    };

    for (const std::string& term : terms) {
        const std::string_view view(term);
        EXPECT_EQ(SpimiTermBuffer::hash_term_bytes_for_test(view),
                  std::hash<std::string_view> {}(view));
    }
}

TEST(SniiSpimiTermBufferTest, PhysicalCommonGramsRemainPhysicalOnFinalDrain) {
    namespace inverted_index = doris::segment_v2::inverted_index;
    const std::string first = inverted_index::encode_common_gram("of", "the").value();
    const std::string second = inverted_index::encode_common_gram("the", "world").value();

    SpimiTermBuffer buf(/*has_positions=*/true);
    buf.add_token(second, 0, 1);
    buf.add_token(std::string_view("plain"), 0, 0);
    buf.add_token(first, 0, 0);
    buf.add_token(first, 1, 2);

    std::vector<TermPostings> terms = buf.finalize_sorted();
    ASSERT_EQ(terms.size(), 3U);
    EXPECT_EQ(terms[0].term, first);
    EXPECT_EQ(terms[1].term, second);
    EXPECT_EQ(terms[2].term, "plain");
    EXPECT_EQ(terms[0].docids, (std::vector<uint32_t> {0U, 1U}));
    EXPECT_EQ(terms[0].positions_flat, (std::vector<uint32_t> {0U, 2U}));
}

TEST(SniiSpimiTermBufferTest, PairKeyModeRejectsGenericStringTokenEntryPoint) {
    EXPECT_DEATH(
            {
                SpimiTermBuffer buffer(/*has_positions=*/true);
                buffer.enable_common_gram_pair_keys();
                buffer.add_token(std::string_view("plain"), /*docid=*/0, /*pos=*/0);
            },
            ".*");
}

TEST(SniiSpimiTermBufferTest, PairKeysMatchPhysicalTermsForMixedRepeatedEscapedAndUtf8Grams) {
    ExpectPairKeysMatchPhysicalTerms(CommonGramsPostingScript(), /*force_spill=*/false);
}

TEST(SniiSpimiTermBufferTest, FusedCommonGramAndPlainAddsMatchSeparatePostingAdds) {
    SpimiTermBuffer separate(/*has_positions=*/true);
    SpimiTermBuffer fused(/*has_positions=*/true);
    FeedSeparateCommonGramAndPlainAdds(&separate);
    FeedFusedCommonGramAndPlainAdds(&fused);

    const std::vector<TermPostings> expected = separate.finalize_sorted();
    const std::vector<TermPostings> actual = fused.finalize_sorted();
    ASSERT_TRUE(separate.status().ok()) << separate.status();
    ASSERT_TRUE(fused.status().ok()) << fused.status();
    ExpectPostingsEqual(actual, expected);

    const std::string internal = std::string(1, '\x1f') + "literal";
    const std::string escaped = std::string(1, inverted_index::PLAIN_ESCAPE_PREFIX) + "literal";
    const std::string repeated_mixed_gram =
            inverted_index::encode_common_gram(internal, "of").value();
    const std::string positioned_gram = inverted_index::encode_common_gram("of", "the").value();
    const auto mixed = std::ranges::find(actual, repeated_mixed_gram, &TermPostings::term);
    const auto positioned = std::ranges::find(actual, positioned_gram, &TermPostings::term);
    const auto escaped_plain =
            std::ranges::find(actual, PhysicalPlainTerm(escaped), &TermPostings::term);
    ASSERT_NE(mixed, actual.end());
    ASSERT_NE(positioned, actual.end());
    ASSERT_NE(escaped_plain, actual.end());
    EXPECT_FALSE(mixed->retain_positions);
    EXPECT_EQ(mixed->docids, (std::vector<uint32_t> {0U, 1U}));
    EXPECT_TRUE(mixed->freqs.empty());
    EXPECT_TRUE(positioned->retain_positions);
    EXPECT_EQ(positioned->docids, (std::vector<uint32_t> {0U, 1U}));
}

TEST(SniiSpimiTermBufferTest, FusedAllCommonTokensCheckSpillGateOncePerInputToken) {
    const auto& common_words = inverted_index::CommonWordSet::builtin_english_stop_words_v1();
    SpimiTermBuffer buffer(/*has_positions=*/true);
    buffer.enable_common_gram_pair_keys();
    std::vector<ClassifiedPlainTerm> terms;
    for (std::string_view logical : {"of", "the", "and", "to"}) {
        terms.push_back(buffer.intern_classified_plain_term(logical, logical, common_words));
        ASSERT_TRUE(terms.back().is_common);
    }

    stb_testing::reset_spill_gate_check_count();
    buffer.add_plain_token(terms.front().id, /*docid=*/0, /*pos=*/1);
    for (uint32_t i = 1; i < terms.size(); ++i) {
        buffer.add_common_gram_and_plain(terms[i - 1].id, terms[i].id, /*docid=*/0,
                                         /*gram_pos=*/i, /*plain_pos=*/i + 1,
                                         /*retain_positions=*/true);
    }

    EXPECT_EQ(buffer.total_tokens(), 7U);
    EXPECT_EQ(stb_testing::spill_gate_check_count(), 4U);
}

TEST(SniiSpimiTermBufferTest, PairKeysMatchPhysicalTermsAcrossForcedSpills) {
    ExpectPairKeysMatchPhysicalTerms(CommonGramsPostingScript(), /*force_spill=*/true);
}

TEST(SniiSpimiTermBufferTest, PairRankSortPreservesOrderWithLinearPairDecodes) {
    stb_testing::reset_common_gram_pair_fast_path_counts();

    SpimiTermBuffer physical(/*has_positions=*/true);
    SpimiTermBuffer pair_keys(/*has_positions=*/true);
    const auto events = AdversarialPairSortScript();
    FeedPhysicalCommonGrams(&physical, events, /*force_spill=*/false);
    FeedPairKeyCommonGrams(&pair_keys, events, /*force_spill=*/false);

    const std::vector<TermPostings> expected = physical.finalize_sorted();
    const std::vector<TermPostings> actual = pair_keys.finalize_sorted();
    ASSERT_TRUE(physical.status().ok()) << physical.status();
    ASSERT_TRUE(pair_keys.status().ok()) << pair_keys.status();
    ExpectPostingsEqual(actual, expected);
    ASSERT_EQ(actual.size(), events.size());
    EXPECT_EQ(actual.back().term.size(), inverted_index::COMMON_GRAM_MAX_ENCODED_BYTES);
    // Each pair is decoded once while building integer component ranks and once
    // while materializing the final physical term. Sorting must not decode pairs.
    EXPECT_EQ(stb_testing::common_gram_pair_unchecked_decode_count(), 2U * events.size());
    EXPECT_EQ(stb_testing::common_gram_trusted_plain_decode_count(), 2U * events.size());
}

TEST(SniiSpimiTermBufferTest, PairRankAdversarialOrderSurvivesForcedSpills) {
    stb_testing::reset_run_compactions();
    ExpectPairKeysMatchPhysicalTerms(AdversarialPairSortScript(), /*force_spill=*/true);
    EXPECT_GT(stb_testing::run_compactions(), 0U);
}

TEST(SniiSpimiTermBufferTest, StatlessCommonGramOmitsFrequenciesAcrossSpillAndCompaction) {
    stb_testing::reset_run_compactions();
    SpimiTermBuffer common_grams(/*has_positions=*/true);
    common_grams.enable_common_gram_pair_keys();
    common_grams.set_forced_spill_min_arena_bytes(0);
    common_grams.set_max_run_files(1);
    const PlainTermId left = common_grams.intern_plain_term("of");
    const PlainTermId right = common_grams.intern_plain_term("world");

    common_grams.add_common_gram(left, right, /*docid=*/5, /*pos=*/0,
                                 /*retain_positions=*/false);
    common_grams.request_global_spill_for_test();
    common_grams.add_common_gram(left, right, /*docid=*/5, /*pos=*/1,
                                 /*retain_positions=*/false);
    common_grams.add_common_gram(left, right, /*docid=*/5, /*pos=*/2,
                                 /*retain_positions=*/false);
    common_grams.add_common_gram(left, right, /*docid=*/9, /*pos=*/0,
                                 /*retain_positions=*/false);
    common_grams.add_common_gram(left, right, /*docid=*/9, /*pos=*/1,
                                 /*retain_positions=*/false);
    common_grams.add_common_gram(left, right, /*docid=*/13, /*pos=*/0,
                                 /*retain_positions=*/false);
    common_grams.request_global_spill_for_test();
    common_grams.add_common_gram(left, right, /*docid=*/13, /*pos=*/1,
                                 /*retain_positions=*/false);
    common_grams.add_common_gram(left, right, /*docid=*/17, /*pos=*/0,
                                 /*retain_positions=*/false);
    common_grams.add_common_gram(left, right, /*docid=*/21, /*pos=*/0,
                                 /*retain_positions=*/false);
    common_grams.request_global_spill_for_test();
    common_grams.add_common_gram(left, right, /*docid=*/25, /*pos=*/0,
                                 /*retain_positions=*/false);

    EXPECT_EQ(common_grams.total_tokens(), 10U);
    EXPECT_GE(common_grams.run_count_for_test(), 1U);
    EXPECT_GT(stb_testing::run_compactions(), 0U);
    const std::vector<TermPostings> gram_terms = common_grams.finalize_sorted();
    ASSERT_TRUE(common_grams.status().ok()) << common_grams.status();
    ASSERT_EQ(gram_terms.size(), 1U);
    EXPECT_FALSE(gram_terms[0].retain_positions);
    EXPECT_EQ(gram_terms[0].docids, (std::vector<uint32_t> {5U, 9U, 13U, 17U, 21U, 25U}));
    EXPECT_TRUE(gram_terms[0].freqs.empty());
    EXPECT_TRUE(gram_terms[0].positions_flat.empty());

    SpimiTermBuffer ordinary_docs(/*has_positions=*/false);
    ordinary_docs.add_token("ordinary", /*docid=*/5, /*pos=*/0);
    ordinary_docs.add_token("ordinary", /*docid=*/5, /*pos=*/0);
    ordinary_docs.add_token("ordinary", /*docid=*/9, /*pos=*/0);
    const std::vector<TermPostings> ordinary_terms = ordinary_docs.finalize_sorted();
    ASSERT_EQ(ordinary_terms.size(), 1U);
    EXPECT_EQ(ordinary_terms[0].docids, (std::vector<uint32_t> {5U, 9U}));
    EXPECT_EQ(ordinary_terms[0].freqs, (std::vector<uint32_t> {1U, 1U}));
}

TEST(SniiSpimiTermBufferTest, PlainTermHotCacheSurvivesForcedSpill) {
    stb_testing::reset_vocab_string_materialization_count();
    stb_testing::reset_owned_term_full_byte_comparison_count();
    stb_testing::reset_common_gram_plain_cache_counts();

    SpimiTermBuffer common_grams(/*has_positions=*/true);
    common_grams.enable_common_gram_pair_keys();
    common_grams.set_forced_spill_min_arena_bytes(0);
    common_grams.set_max_run_files(1);

    constexpr std::array<std::string_view, 8> terms = {"the", "database", "the", "database",
                                                       "the", "database", "the", "database"};
    for (size_t i = 0; i < terms.size(); ++i) {
        if (i == 2) {
            common_grams.request_global_spill_for_test();
            stb_testing::reset_owned_term_full_byte_comparison_count();
        }
        const PlainTermId term = common_grams.intern_plain_term(terms[i], terms[i]);
        common_grams.add_plain_token(term, static_cast<uint32_t>(i / 2),
                                     static_cast<uint32_t>(i % 2));
    }

    EXPECT_GE(common_grams.run_count_for_test(), 1U);
    const std::vector<TermPostings> postings = common_grams.finalize_sorted();
    ASSERT_TRUE(common_grams.status().ok()) << common_grams.status();
    ASSERT_EQ(postings.size(), 2U);
    EXPECT_EQ(postings[0].term, "database");
    EXPECT_EQ(postings[0].docids, (std::vector<uint32_t> {0U, 1U, 2U, 3U}));
    EXPECT_EQ(postings[0].freqs, (std::vector<uint32_t> {1U, 1U, 1U, 1U}));
    EXPECT_EQ(postings[0].positions_flat, (std::vector<uint32_t> {1U, 1U, 1U, 1U}));
    EXPECT_EQ(postings[1].term, "the");
    EXPECT_EQ(postings[1].docids, (std::vector<uint32_t> {0U, 1U, 2U, 3U}));
    EXPECT_EQ(postings[1].freqs, (std::vector<uint32_t> {1U, 1U, 1U, 1U}));
    EXPECT_EQ(postings[1].positions_flat, (std::vector<uint32_t> {0U, 0U, 0U, 0U}));
    EXPECT_EQ(stb_testing::vocab_string_materialization_count(), 2U);
    EXPECT_EQ(stb_testing::common_gram_plain_cache_probes(), terms.size());
    EXPECT_EQ(stb_testing::common_gram_plain_cache_hits(), terms.size() - 2);
    EXPECT_EQ(stb_testing::common_gram_plain_intern_table_probes(), 2U);
    EXPECT_EQ(stb_testing::owned_term_full_byte_comparison_count(), 0U);
}

TEST(SniiSpimiTermBufferTest, PlainTermHotCacheUsesFullBytesForHashCollisions) {
    SpimiTermBuffer buffer(/*has_positions=*/true);
    buffer.enable_common_gram_pair_keys();
    buffer.set_owned_term_hash_mask_for_test(0);

    const PlainTermId alpha = buffer.intern_plain_term("alpha", "alpha");
    const PlainTermId beta = buffer.intern_plain_term("beta", "beta");
    const PlainTermId gamma = buffer.intern_plain_term("gamma", "gamma");
    EXPECT_NE(alpha.value, beta.value);
    EXPECT_NE(alpha.value, gamma.value);
    EXPECT_NE(beta.value, gamma.value);

    EXPECT_EQ(buffer.intern_plain_term("beta", "beta").value, beta.value);
    EXPECT_EQ(buffer.intern_plain_term("alpha", "alpha").value, alpha.value);
    EXPECT_EQ(buffer.intern_plain_term("gamma", "gamma").value, gamma.value);
}

TEST(SniiSpimiTermBufferTest, PlainTermHotCacheDoesNotPublishFailedInsertion) {
    stb_testing::reset_vocab_string_materialization_count();
    stb_testing::reset_common_gram_plain_cache_counts();

    SpimiTermBuffer buffer(/*has_positions=*/true);
    buffer.enable_common_gram_pair_keys();
    stb_testing::fail_next_owned_term_reserve();
    EXPECT_THROW(buffer.intern_plain_term("reserve-recoverable", "reserve-recoverable"),
                 std::bad_alloc);
    stb_testing::fail_next_owned_term_emplace();
    EXPECT_THROW(buffer.intern_plain_term("emplace-recoverable", "emplace-recoverable"),
                 std::bad_alloc);
    EXPECT_EQ(stb_testing::vocab_string_materialization_count(), 0U);
    EXPECT_EQ(stb_testing::common_gram_plain_cache_hits(), 0U);

    stb_testing::reset_common_gram_plain_cache_counts();
    const PlainTermId reserve_retry =
            buffer.intern_plain_term("reserve-recoverable", "reserve-recoverable");
    const PlainTermId emplace_retry =
            buffer.intern_plain_term("emplace-recoverable", "emplace-recoverable");
    EXPECT_EQ(reserve_retry.value, 0U);
    EXPECT_EQ(emplace_retry.value, 1U);
    EXPECT_EQ(stb_testing::common_gram_plain_cache_probes(), 2U);
    EXPECT_EQ(stb_testing::common_gram_plain_cache_hits(), 0U);
    EXPECT_EQ(stb_testing::common_gram_plain_intern_table_probes(), 2U);

    EXPECT_EQ(buffer.intern_plain_term("reserve-recoverable", "reserve-recoverable").value,
              reserve_retry.value);
    EXPECT_EQ(buffer.intern_plain_term("emplace-recoverable", "emplace-recoverable").value,
              emplace_retry.value);
    EXPECT_EQ(stb_testing::common_gram_plain_cache_probes(), 4U);
    EXPECT_EQ(stb_testing::common_gram_plain_cache_hits(), 2U);
    EXPECT_EQ(stb_testing::common_gram_plain_intern_table_probes(), 2U);
}

TEST(SniiSpimiTermBufferTest, ClassifiedPlainTermMembershipSurvivesHotCacheEviction) {
    const auto& common_words = inverted_index::CommonWordSet::builtin_english_stop_words_v1();
    SpimiTermBuffer buffer(/*has_positions=*/true);
    buffer.enable_common_gram_pair_keys();

    inverted_index::common_grams_testing::reset_common_word_membership_lookup_count();
    const ClassifiedPlainTerm first =
            buffer.intern_classified_plain_term("the", "the", common_words);
    ASSERT_TRUE(first.is_common);
    for (const std::string& term : FindPlainHotCacheColliders("the")) {
        const ClassifiedPlainTerm classified =
                buffer.intern_classified_plain_term(term, term, common_words);
        EXPECT_FALSE(classified.is_common);
    }
    EXPECT_EQ(inverted_index::common_grams_testing::common_word_membership_lookup_count(), 3U);

    stb_testing::reset_common_gram_plain_cache_counts();
    const ClassifiedPlainTerm after_eviction =
            buffer.intern_classified_plain_term("the", "the", common_words);

    EXPECT_EQ(after_eviction.id.value, first.id.value);
    EXPECT_TRUE(after_eviction.is_common);
    EXPECT_EQ(stb_testing::common_gram_plain_cache_hits(), 0U);
    EXPECT_EQ(stb_testing::common_gram_plain_intern_table_probes(), 1U);
    EXPECT_EQ(inverted_index::common_grams_testing::common_word_membership_lookup_count(), 3U);
}

TEST(SniiSpimiTermBufferTest, ClassifiedPlainTermUsesLogicalBytesForMembership) {
    const std::string logical = std::string(1, '\x1f') + "common";
    const std::string physical = PhysicalPlainTerm(logical);
    ASSERT_NE(physical, logical);
    const std::string wordset_content = logical + "\n";
    auto parsed = inverted_index::CommonWordSet::parse_words(wordset_content);
    ASSERT_TRUE(parsed.has_value()) << parsed.error();

    SpimiTermBuffer buffer(/*has_positions=*/true);
    buffer.enable_common_gram_pair_keys();
    inverted_index::common_grams_testing::reset_common_word_membership_lookup_count();
    const ClassifiedPlainTerm classified =
            buffer.intern_classified_plain_term(physical, logical, *parsed);

    EXPECT_TRUE(classified.is_common);
    EXPECT_EQ(inverted_index::common_grams_testing::common_word_membership_lookup_count(), 1U);
}

TEST(SniiSpimiTermBufferTest, DocsOnlyPairCacheSkipsHashAndSurvivesSpill) {
    stb_testing::reset_common_gram_pair_cache_counts();

    SpimiTermBuffer common_grams(/*has_positions=*/true);
    const uint64_t bytes_before_cache = common_grams.resident_bytes_for_test();
    common_grams.enable_common_gram_pair_keys();
    EXPECT_EQ(common_grams.resident_bytes_for_test() - bytes_before_cache, 32U * 1024U);
    common_grams.set_forced_spill_min_arena_bytes(0);
    common_grams.set_max_run_files(1);
    const PlainTermId left = common_grams.intern_plain_term("of");
    const PlainTermId right = common_grams.intern_plain_term("world");

    common_grams.add_common_gram(left, right, /*docid=*/5, /*pos=*/0,
                                 /*retain_positions=*/false);
    common_grams.request_global_spill_for_test();
    common_grams.add_common_gram(left, right, /*docid=*/5, /*pos=*/1,
                                 /*retain_positions=*/false);
    common_grams.add_common_gram(left, right, /*docid=*/5, /*pos=*/2,
                                 /*retain_positions=*/false);
    common_grams.add_common_gram(left, right, /*docid=*/9, /*pos=*/0,
                                 /*retain_positions=*/false);

    EXPECT_EQ(common_grams.total_tokens(), 4U);
    EXPECT_GE(common_grams.run_count_for_test(), 1U);
    const std::vector<TermPostings> gram_terms = common_grams.finalize_sorted();
    ASSERT_TRUE(common_grams.status().ok()) << common_grams.status();
    ASSERT_EQ(gram_terms.size(), 1U);
    EXPECT_EQ(gram_terms[0].docids, (std::vector<uint32_t> {5U, 9U}));
    EXPECT_TRUE(gram_terms[0].freqs.empty());
    EXPECT_EQ(stb_testing::common_gram_pair_cache_probes(), 4U);
    EXPECT_EQ(stb_testing::common_gram_pair_cache_pair_hits(), 3U);
    EXPECT_EQ(stb_testing::common_gram_pair_cache_same_doc_hits(), 2U);
}

TEST(SniiSpimiTermBufferTest, NativePairInternerSurvivesL0CollisionAndForcedSpill) {
    constexpr uint32_t kLeft = 0;
    constexpr uint32_t kTargetRight = 1;
    const std::optional<uint32_t> collider_right =
            FindCommonGramPairL0RightCollider(kLeft, kTargetRight);
    ASSERT_TRUE(collider_right.has_value());
    std::optional<uint32_t> second_collider_right;
    for (uint32_t candidate = *collider_right + 1; candidate < 4096; ++candidate) {
        if (CommonGramPairL0Index(kLeft, candidate) == CommonGramPairL0Index(kLeft, kTargetRight)) {
            second_collider_right = candidate;
            break;
        }
    }
    ASSERT_TRUE(second_collider_right.has_value());
    ASSERT_NE(*collider_right, kTargetRight);
    ASSERT_EQ(CommonGramPairL0Index(kLeft, *collider_right),
              CommonGramPairL0Index(kLeft, kTargetRight));
    ASSERT_EQ(CommonGramPairL0Index(kLeft, *second_collider_right),
              CommonGramPairL0Index(kLeft, kTargetRight));

    stb_testing::reset_common_gram_pair_cache_counts();
    stb_testing::reset_common_gram_native_pair_intern_counts();
    SpimiTermBuffer common_grams(/*has_positions=*/true);
    common_grams.enable_common_gram_pair_keys();
    common_grams.set_forced_spill_min_arena_bytes(0);
    common_grams.set_max_run_files(1);

    std::vector<PlainTermId> ids;
    ids.reserve(static_cast<size_t>(*second_collider_right) + 1);
    for (uint32_t id = 0; id <= *second_collider_right; ++id) {
        const PlainTermId interned = common_grams.intern_plain_term(NativePairPlainTerm(id));
        ASSERT_EQ(interned.value, id);
        ids.push_back(interned);
    }

    common_grams.add_common_gram(ids[kLeft], ids[kTargetRight], /*docid=*/1, /*pos=*/0,
                                 /*retain_positions=*/false);
    common_grams.add_common_gram(ids[kLeft], ids[kTargetRight], /*docid=*/2, /*pos=*/0,
                                 /*retain_positions=*/false);
    common_grams.add_common_gram(ids[kLeft], ids[*collider_right], /*docid=*/3, /*pos=*/0,
                                 /*retain_positions=*/false);
    common_grams.request_global_spill_for_test();
    common_grams.add_common_gram(ids[kLeft], ids[*second_collider_right], /*docid=*/4, /*pos=*/0,
                                 /*retain_positions=*/false);
    ASSERT_GE(common_grams.run_count_for_test(), 1U);
    common_grams.add_common_gram(ids[kLeft], ids[kTargetRight], /*docid=*/5, /*pos=*/0,
                                 /*retain_positions=*/false);

    EXPECT_EQ(stb_testing::common_gram_pair_cache_probes(), 5U);
    EXPECT_EQ(stb_testing::common_gram_pair_cache_pair_hits(), 1U);
    EXPECT_EQ(stb_testing::common_gram_native_pair_probes(), 4U);
    EXPECT_EQ(stb_testing::common_gram_native_pair_hits(), 1U);
    EXPECT_EQ(stb_testing::common_gram_native_pair_inserts(), 3U);
    const std::vector<TermPostings> terms = common_grams.finalize_sorted();
    ASSERT_TRUE(common_grams.status().ok()) << common_grams.status();
    ASSERT_EQ(terms.size(), 3U);

    auto target_key = inverted_index::encode_common_gram(NativePairPlainTerm(kLeft),
                                                         NativePairPlainTerm(kTargetRight));
    ASSERT_TRUE(target_key.has_value());
    const auto target = std::ranges::find(terms, target_key.value(), &TermPostings::term);
    ASSERT_NE(target, terms.end());
    EXPECT_EQ(target->docids, (std::vector<uint32_t> {1U, 2U, 5U}));
    EXPECT_EQ(common_grams.resident_bytes_for_test(), 0U);
}

TEST(SniiSpimiTermBufferTest, PositionedPairCacheSurvivesForcedSpill) {
    stb_testing::reset_common_gram_pair_cache_counts();
    SpimiTermBuffer positioned(/*has_positions=*/true);
    positioned.enable_common_gram_pair_keys();
    positioned.set_forced_spill_min_arena_bytes(0);
    positioned.set_max_run_files(1);
    const PlainTermId positioned_left = positioned.intern_plain_term("of");
    const PlainTermId positioned_right = positioned.intern_plain_term("the");
    positioned.request_global_spill_for_test();
    positioned.add_common_gram(positioned_left, positioned_right, /*docid=*/7, /*pos=*/3,
                               /*retain_positions=*/true);
    EXPECT_GE(positioned.run_count_for_test(), 1U);
    positioned.add_common_gram(positioned_left, positioned_right, /*docid=*/7, /*pos=*/4,
                               /*retain_positions=*/true);
    const std::vector<TermPostings> positioned_terms = positioned.finalize_sorted();
    ASSERT_TRUE(positioned.status().ok()) << positioned.status();
    ASSERT_EQ(positioned_terms.size(), 1U);
    EXPECT_EQ(positioned_terms[0].freqs, (std::vector<uint32_t> {2U}));
    EXPECT_EQ(positioned_terms[0].positions_flat, (std::vector<uint32_t> {3U, 4U}));
    EXPECT_EQ(stb_testing::common_gram_pair_cache_probes(), 2U);
    EXPECT_EQ(stb_testing::common_gram_pair_cache_pair_hits(), 1U);
    EXPECT_EQ(stb_testing::common_gram_pair_cache_same_doc_hits(), 0U);
}

TEST(SniiSpimiTermBufferTest, CommonGramSpillGateObservesResidentGrowthImmediately) {
    doris::snii::writer::MemoryReporter reporter(
            nullptr, /*cap_bytes=*/1,
            doris::snii::writer::MemoryReporter::CapPolicy::kSpillThreshold);
    SpimiTermBuffer common_grams(/*has_positions=*/true, /*spill_threshold_bytes=*/0, &reporter);
    common_grams.enable_common_gram_pair_keys();
    const PlainTermId term = common_grams.intern_plain_term("common");

    common_grams.add_plain_token(term, /*docid=*/0, /*pos=*/0);
    EXPECT_EQ(common_grams.run_count_for_test(), 1U);
    const std::vector<TermPostings> terms = common_grams.finalize_sorted();
    ASSERT_TRUE(common_grams.status().ok()) << common_grams.status();
    ASSERT_EQ(terms.size(), 1U);
    EXPECT_EQ(terms[0].freqs, (std::vector<uint32_t> {1U}));
}

TEST(SniiSpimiTermBufferTest, StatlessCommonGramStoresOnlyDocumentDeltas) {
    SpimiTermBuffer common_grams(/*has_positions=*/true);
    common_grams.enable_common_gram_pair_keys();
    const PlainTermId left = common_grams.intern_plain_term("of");
    const PlainTermId right = common_grams.intern_plain_term("world");
    const uint64_t resident_before_postings = common_grams.resident_bytes_for_test();

    constexpr uint32_t kDocumentCount = 20000;
    for (uint32_t docid = 1; docid <= kDocumentCount; ++docid) {
        common_grams.add_common_gram(left, right, docid, /*pos=*/0,
                                     /*retain_positions=*/false);
    }

    // One one-byte zigzag delta per document fits in a single 32 KiB arena block.
    // The old tagged representation wrote a second, constant byte and needed two.
    EXPECT_LT(common_grams.resident_bytes_for_test() - resident_before_postings, 60U * 1024U);
    const std::vector<TermPostings> terms = common_grams.finalize_sorted();
    ASSERT_TRUE(common_grams.status().ok()) << common_grams.status();
    ASSERT_EQ(terms.size(), 1U);
    EXPECT_FALSE(terms[0].retain_positions);
    ASSERT_EQ(terms[0].docids.size(), kDocumentCount);
    EXPECT_EQ(terms[0].docids.front(), 1U);
    EXPECT_EQ(terms[0].docids.back(), kDocumentCount);
    EXPECT_TRUE(terms[0].freqs.empty());
}

TEST(SniiSpimiTermBufferTest, SortedWideStatlessCommonGramStreamsSourceWindows) {
    for (const uint32_t document_count : {doris::snii::format::kSlimDfThreshold, uint32_t {777}}) {
        SCOPED_TRACE(document_count);
        SpimiTermBuffer common_grams(/*has_positions=*/true);
        const std::vector<uint32_t> expected =
                AddSortedStatlessCommonGram(&common_grams, document_count);

        size_t callback_count = 0;
        const doris::Status status = common_grams.for_each_term_sorted(
                [&](StreamedTermPostings&& source) -> doris::Status {
                    ++callback_count;
                    TermPostings postings;
                    RETURN_IF_ERROR(MaterializeInIrregularWindows(std::move(source), &postings));
                    EXPECT_FALSE(postings.retain_positions);
                    EXPECT_TRUE(postings.freqs.empty());
                    EXPECT_TRUE(postings.positions_flat.empty());
                    EXPECT_EQ(postings.document_count(), document_count);
                    EXPECT_EQ(postings.docids, expected);
                    return doris::Status::OK();
                });

        ASSERT_TRUE(status.ok()) << status;
        EXPECT_EQ(callback_count, 1U);
    }
}

TEST(SniiSpimiTermBufferTest, StreamedStatlessCommonGramRequiresSynchronousConsumption) {
    SpimiTermBuffer common_grams(/*has_positions=*/true);
    constexpr uint32_t kDocumentCount = 777;
    AddSortedStatlessCommonGram(&common_grams, kDocumentCount);

    const doris::Status status = common_grams.for_each_term_sorted(
            [&](StreamedTermPostings&&) { return doris::Status::OK(); });

    EXPECT_TRUE(status.is<doris::ErrorCode::INVALID_ARGUMENT>()) << status;
}

TEST(SniiSpimiTermBufferTest, ForcedSpillStatlessCommonGramSourceCoalescesSeamDuplicates) {
    SpimiTermBuffer common_grams(/*has_positions=*/true);
    common_grams.enable_common_gram_pair_keys();
    common_grams.set_forced_spill_min_arena_bytes(0);
    const PlainTermId left = common_grams.intern_plain_term("of");
    const PlainTermId right = common_grams.intern_plain_term("world");

    std::vector<uint32_t> expected;
    expected.reserve(768);
    for (uint32_t docid = 0; docid < 256; ++docid) {
        expected.push_back(docid);
        common_grams.add_common_gram(left, right, docid, /*pos=*/0,
                                     /*retain_positions=*/false);
    }
    common_grams.request_global_spill_for_test();
    expected.push_back(256);
    common_grams.add_common_gram(left, right, /*docid=*/256, /*pos=*/0,
                                 /*retain_positions=*/false);
    common_grams.add_common_gram(left, right, /*docid=*/256, /*pos=*/0,
                                 /*retain_positions=*/false);
    for (uint32_t docid = 257; docid < 512; ++docid) {
        expected.push_back(docid);
        common_grams.add_common_gram(left, right, docid, /*pos=*/0,
                                     /*retain_positions=*/false);
    }
    common_grams.request_global_spill_for_test();
    expected.push_back(512);
    common_grams.add_common_gram(left, right, /*docid=*/512, /*pos=*/0,
                                 /*retain_positions=*/false);
    common_grams.add_common_gram(left, right, /*docid=*/512, /*pos=*/0,
                                 /*retain_positions=*/false);
    for (uint32_t docid = 513; docid < 768; ++docid) {
        expected.push_back(docid);
        common_grams.add_common_gram(left, right, docid, /*pos=*/0,
                                     /*retain_positions=*/false);
    }

    ASSERT_EQ(common_grams.run_count_for_test(), 2U);
    size_t callback_count = 0;
    const doris::Status status =
            common_grams.for_each_term_sorted([&](StreamedTermPostings&& source) {
                ++callback_count;
                TermPostings postings;
                RETURN_IF_ERROR(MaterializeInIrregularWindows(std::move(source), &postings));
                EXPECT_EQ(postings.document_count(), expected.size());
                EXPECT_EQ(postings.docids, expected);
                EXPECT_TRUE(std::ranges::is_sorted(postings.docids));
                EXPECT_EQ(std::ranges::adjacent_find(postings.docids), postings.docids.end());
                return doris::Status::OK();
            });

    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(callback_count, 1U);
}

TEST(SniiSpimiTermBufferTest, OutOfOrderWideStatlessCommonGramStaysMaterialized) {
    SpimiTermBuffer common_grams(/*has_positions=*/true);
    common_grams.enable_common_gram_pair_keys();
    const PlainTermId left = common_grams.intern_plain_term("of");
    const PlainTermId right = common_grams.intern_plain_term("world");
    constexpr uint32_t kDocumentCount = 600;
    for (uint32_t docid = 0; docid < kDocumentCount; ++docid) {
        common_grams.add_common_gram(left, right, docid, /*pos=*/0,
                                     /*retain_positions=*/false);
    }
    common_grams.add_common_gram(left, right, /*docid=*/100, /*pos=*/1,
                                 /*retain_positions=*/false);
    common_grams.add_common_gram(left, right, /*docid=*/50, /*pos=*/2,
                                 /*retain_positions=*/false);

    size_t callback_count = 0;
    const doris::Status status =
            common_grams.for_each_term_sorted([&](StreamedTermPostings&& source) {
                ++callback_count;
                TermPostings postings;
                RETURN_IF_ERROR(MaterializeInIrregularWindows(std::move(source), &postings));
                EXPECT_EQ(postings.document_count(), kDocumentCount);
                if (postings.docids.size() != kDocumentCount) {
                    return doris::Status::Error<doris::ErrorCode::INTERNAL_ERROR, false>(
                            "unexpected materialized document count");
                }
                for (uint32_t docid = 0; docid < kDocumentCount; ++docid) {
                    EXPECT_EQ(postings.docids[docid], docid);
                }
                EXPECT_TRUE(postings.freqs.empty());
                EXPECT_TRUE(postings.positions_flat.empty());
                return doris::Status::OK();
            });

    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(callback_count, 1U);
}

TEST(SniiSpimiTermBufferTest, OrdinaryDocsOnlyMarkerTermRetainsFrequency) {
    const std::string literal_marker_term = std::string(inverted_index::CG_V1_MARKER) + "literal";
    SpimiTermBuffer ordinary(/*has_positions=*/false);
    ordinary.add_token(literal_marker_term, /*docid=*/3, /*pos=*/0,
                       /*retain_positions=*/false);

    const std::vector<TermPostings> terms = ordinary.finalize_sorted();
    ASSERT_TRUE(ordinary.status().ok()) << ordinary.status();
    ASSERT_EQ(terms.size(), 1U);
    EXPECT_EQ(terms[0].term, literal_marker_term);
    EXPECT_EQ(terms[0].docids, (std::vector<uint32_t> {3U}));
    EXPECT_EQ(terms[0].freqs, (std::vector<uint32_t> {1U}));
}

TEST(SniiSpimiTermBufferTest, CommonGramLogicalValidationRunsOncePerDistinctPlainTerm) {
    stb_testing::reset_common_gram_logical_validation_count();
    stb_testing::reset_vocab_string_materialization_count();

    SpimiTermBuffer common_grams(/*has_positions=*/true);
    common_grams.enable_common_gram_pair_keys();
    const std::string internal = std::string(1, '\x1f') + "literal";
    const std::string physical_internal = PhysicalPlainTerm(internal);

    for (int repeat = 0; repeat < 8; ++repeat) {
        common_grams.intern_plain_term("ordinary", "ordinary");
        common_grams.intern_plain_term("中文长词条目", "中文长词条目");
        common_grams.intern_plain_term(physical_internal, internal);
    }

    EXPECT_EQ(stb_testing::common_gram_logical_validation_count(), 3U);
    EXPECT_EQ(stb_testing::vocab_string_materialization_count(), 3U);
}

TEST(SniiSpimiTermBufferTest, CommonGramLogicalValidationFailureDoesNotMutateVocabulary) {
    stb_testing::reset_common_gram_logical_validation_count();
    stb_testing::reset_vocab_string_materialization_count();

    SpimiTermBuffer common_grams(/*has_positions=*/true);
    common_grams.enable_common_gram_pair_keys();
    for (const std::string& invalid : {std::string("bad\0term", 8), std::string("\xc3", 1)}) {
        try {
            common_grams.intern_plain_term(invalid, invalid);
            FAIL() << "expected analyzer error";
        } catch (const doris::Exception& error) {
            EXPECT_EQ(error.code(), doris::ErrorCode::INVERTED_INDEX_ANALYZER_ERROR);
        }
    }

    EXPECT_EQ(stb_testing::common_gram_logical_validation_count(), 2U);
    EXPECT_EQ(stb_testing::vocab_string_materialization_count(), 0U);
    EXPECT_EQ(common_grams.intern_plain_term("valid", "valid").value, 0U);
}

TEST(SniiSpimiTermBufferTest, CompactPostingPoolVarintFastPathCrossesSliceBoundary) {
    doris::snii::writer::CompactPostingPool pool;
    doris::snii::writer::CompactPostingPool::SliceWriter writer;
    uint8_t level = 0;
    const uint32_t head = pool.start_chain(&writer, &level);
    for (uint32_t i = 1; i < doris::snii::writer::CompactPostingPool::kSliceSizes_level0(); ++i) {
        pool.append_byte(&writer, &level, 0);
    }

    const std::array<uint64_t, 5> values = {
            300, 0, 127, 128, static_cast<uint64_t>(std::numeric_limits<uint32_t>::max())};
    for (const uint64_t value : values) {
        pool.append_varint(&writer, &level, value);
    }

    auto cursor = pool.cursor(head, writer.cur);
    for (uint32_t i = 1; i < doris::snii::writer::CompactPostingPool::kSliceSizes_level0(); ++i) {
        EXPECT_EQ(cursor.next(), 0U);
    }
    for (const uint64_t expected : values) {
        EXPECT_EQ(cursor.read_varint(), expected);
    }
}

TEST(SniiSpimiTermBufferTest, PerTermDocsOnlyDropsSameDocOccurrencesBeforeDecode) {
    SpimiTermBuffer buf(/*has_positions=*/true);
    buf.add_token("docs", /*docid=*/5, /*pos=*/10, /*retain_positions=*/false);
    buf.add_token("docs", /*docid=*/5, /*pos=*/11, /*retain_positions=*/false);
    buf.add_token("docs", /*docid=*/1, /*pos=*/3, /*retain_positions=*/false);
    buf.add_token("docs", /*docid=*/5, /*pos=*/12, /*retain_positions=*/false);
    buf.add_token("positioned", /*docid=*/5, /*pos=*/20, /*retain_positions=*/true);
    buf.add_token("positioned", /*docid=*/5, /*pos=*/21, /*retain_positions=*/true);

    EXPECT_EQ(buf.total_tokens(), 6U);
    const std::vector<TermPostings> terms = buf.finalize_sorted();
    ASSERT_EQ(terms.size(), 2U);
    EXPECT_EQ(terms[0].term, "docs");
    EXPECT_FALSE(terms[0].retain_positions);
    EXPECT_EQ(terms[0].docids, (std::vector<uint32_t> {1U, 5U}));
    EXPECT_EQ(terms[0].freqs, (std::vector<uint32_t> {1U, 1U}));
    EXPECT_TRUE(terms[0].positions_flat.empty());
    EXPECT_EQ(terms[1].term, "positioned");
    EXPECT_TRUE(terms[1].retain_positions);
    EXPECT_EQ(terms[1].docids, (std::vector<uint32_t> {5U}));
    EXPECT_EQ(terms[1].freqs, (std::vector<uint32_t> {2U}));
    EXPECT_EQ(terms[1].positions_flat, (std::vector<uint32_t> {20U, 21U}));
}

// ---------------------------------------------------------------------------------
// Functional verification (FV1-FV9)
// ---------------------------------------------------------------------------------

// FV1: ids are assigned in first-seen order (b=0,a=1,c=2) but the emitted order is
// lexicographic (a,b,c); docids/freqs are recovered correctly.
TEST(SniiSpimiTermBufferTest, VocabAssignsIdsInFirstSeenOrder) {
    SpimiTermBuffer buf(/*has_positions=*/false);
    buf.add_token(std::string_view("b"), 0, 0);
    buf.add_token(std::string_view("a"), 1, 0);
    buf.add_token(std::string_view("b"), 2, 0);
    buf.add_token(std::string_view("c"), 3, 0);
    buf.add_token(std::string_view("a"), 4, 0);

    EXPECT_EQ(buf.unique_terms(), 3U);
    EXPECT_EQ(buf.total_tokens(), 5U);
    EXPECT_TRUE(buf.status().ok());

    std::vector<TermPostings> terms = buf.finalize_sorted();
    ASSERT_EQ(terms.size(), 3U);
    EXPECT_EQ(terms[0].term, "a");
    EXPECT_EQ(terms[1].term, "b");
    EXPECT_EQ(terms[2].term, "c");
    EXPECT_EQ(terms[0].docids, (std::vector<uint32_t> {1U, 4U}));
    EXPECT_EQ(terms[1].docids, (std::vector<uint32_t> {0U, 2U}));
    EXPECT_EQ(terms[2].docids, (std::vector<uint32_t> {3U}));
    EXPECT_EQ(terms[0].freqs, (std::vector<uint32_t> {1U, 1U}));
    EXPECT_EQ(terms[1].freqs, (std::vector<uint32_t> {1U, 1U}));
    EXPECT_EQ(terms[2].freqs, (std::vector<uint32_t> {1U}));
}

// FV2: the same >SSO ordinary term fed 1000 times reuses ONE id (heterogeneous hit
// path), yielding a single term with 1000 ascending docids and freq 1 each.
TEST(SniiSpimiTermBufferTest, RepeatedTermReusesSingleId) {
    SpimiTermBuffer buf(/*has_positions=*/false);
    const std::string term = MixedLongAscii();
    ASSERT_GT(term.size(), 15U); // exceeds libstdc++ SSO: the OLD probe heap-allocated

    constexpr uint32_t kRepeats = 1000;
    for (uint32_t d = 0; d < kRepeats; ++d) {
        buf.add_token(std::string_view(term), d, 0);
    }
    EXPECT_EQ(buf.unique_terms(), 1U);
    EXPECT_EQ(buf.total_tokens(), kRepeats);

    std::vector<TermPostings> terms = buf.finalize_sorted();
    ASSERT_EQ(terms.size(), 1U);
    EXPECT_EQ(terms[0].term, term);
    ASSERT_EQ(terms[0].docids.size(), kRepeats);
    for (uint32_t d = 0; d < kRepeats; ++d) {
        EXPECT_EQ(terms[0].docids[d], d);
        EXPECT_EQ(terms[0].freqs[d], 1U);
    }
}

// FV3 (also the byte-identity perf gate): two independent buffers fed the same mixed
// script (long ASCII + plain + CJK token) finalize to element-identical postings.
TEST(SniiSpimiTermBufferTest, FinalizeIsByteIdenticalAcrossRuns) {
    SpimiTermBuffer a(/*has_positions=*/true);
    SpimiTermBuffer b(/*has_positions=*/true);
    FeedMixedScript(a);
    FeedMixedScript(b);

    std::vector<TermPostings> ra = a.finalize_sorted();
    std::vector<TermPostings> rb = b.finalize_sorted();
    ExpectPostingsEqual(ra, rb);
    EXPECT_TRUE(a.status().ok());
    EXPECT_TRUE(b.status().ok());

    bool saw_long_ascii = false;
    bool saw_cjk = false;
    for (const auto& tp : ra) {
        if (tp.term == MixedLongAscii()) {
            saw_long_ascii = true;
        }
        if (tp.term == MixedCjk()) {
            saw_cjk = true;
        }
    }
    EXPECT_TRUE(saw_long_ascii);
    EXPECT_TRUE(saw_cjk);
}

// FV4: no tokens -> empty result, status stays OK.
TEST(SniiSpimiTermBufferTest, EmptyVocabProducesNoTerms) {
    SpimiTermBuffer buf(/*has_positions=*/false);
    EXPECT_EQ(buf.unique_terms(), 0U);
    std::vector<TermPostings> terms = buf.finalize_sorted();
    EXPECT_TRUE(terms.empty());
    EXPECT_TRUE(buf.status().ok());
}

// FV5: a single token yields a single term, single docid, freq 1.
TEST(SniiSpimiTermBufferTest, SingleTokenProducesSingleTerm) {
    SpimiTermBuffer buf(/*has_positions=*/true);
    buf.add_token(std::string_view("solo"), 7, 3);
    EXPECT_EQ(buf.unique_terms(), 1U);

    std::vector<TermPostings> terms = buf.finalize_sorted();
    ASSERT_EQ(terms.size(), 1U);
    EXPECT_EQ(terms[0].term, "solo");
    EXPECT_EQ(terms[0].docids, (std::vector<uint32_t> {7U}));
    EXPECT_EQ(terms[0].freqs, (std::vector<uint32_t> {1U}));
    EXPECT_EQ(terms[0].positions_flat, (std::vector<uint32_t> {3U}));
}

// FV6: the empty string is a valid distinct term; the heterogeneous equality functor
// matches "" against a stored "" so a repeat empty token reuses the same id.
TEST(SniiSpimiTermBufferTest, EmptyStringIsAValidDistinctTerm) {
    SpimiTermBuffer buf(/*has_positions=*/false);
    buf.add_token(std::string_view(""), 0, 0); // empty term, first occurrence
    buf.add_token(std::string_view("x"), 1, 0);
    buf.add_token(std::string_view(""), 2, 0); // empty term reused via transparent eq

    EXPECT_EQ(buf.unique_terms(), 2U);
    std::vector<TermPostings> terms = buf.finalize_sorted();
    ASSERT_EQ(terms.size(), 2U);
    EXPECT_EQ(terms[0].term, ""); // "" sorts before "x"
    EXPECT_EQ(terms[0].docids, (std::vector<uint32_t> {0U, 2U}));
    EXPECT_EQ(terms[1].term, "x");
    EXPECT_EQ(terms[1].docids, (std::vector<uint32_t> {1U}));
    EXPECT_TRUE(buf.status().ok());
}

// FV7: add_token(string_view) on a BORROWED-vocab buffer is rejected (latches
// InvalidArgument, token ignored). The interning functors hold &owned_vocab_ but are
// never dereferenced on this path (reject happens before the find), so empty
// owned_vocab_ is never indexed out of bounds.
TEST(SniiSpimiTermBufferTest, BorrowedModeRejectsStringView) {
    const std::vector<std::string> vocab = {"a", "b"};
    SpimiTermBuffer buf(&vocab, /*has_positions=*/false);
    buf.add_token(0U, 0, 0);                    // valid id-path token
    buf.add_token(std::string_view("a"), 1, 0); // illegal on a borrowed-vocab buffer

    EXPECT_FALSE(buf.status().ok());
    EXPECT_EQ(buf.total_tokens(), 1U); // the string-view token was ignored
    EXPECT_EQ(buf.unique_terms(), 1U);
}

// FV8: long ordinary terms sharing a prefix remain distinct and round-trip with
// exact bytes.
TEST(SniiSpimiTermBufferTest, PrefixSharingOrdinaryTermsRoundTrip) {
    SpimiTermBuffer buf(/*has_positions=*/true);
    const std::string first = "ordinary-prefix-sharing-alpha-suffix";
    const std::string second = "ordinary-prefix-sharing-beta-suffix";
    const std::string third = "ordinary-prefix-sharing-gamma-suffix";
    buf.add_token(std::string_view(first), 0, 0);
    buf.add_token(std::string_view(second), 0, 1);
    buf.add_token(std::string_view(third), 0, 2);

    EXPECT_EQ(buf.unique_terms(), 3U);
    std::vector<TermPostings> terms = buf.finalize_sorted();
    ASSERT_EQ(terms.size(), 3U);

    bool saw_first = false;
    bool saw_second = false;
    bool saw_third = false;
    for (const auto& tp : terms) {
        if (tp.term == first) {
            saw_first = true;
        } else if (tp.term == second) {
            saw_second = true;
        } else if (tp.term == third) {
            saw_third = true;
        }
    }
    EXPECT_TRUE(saw_first);
    EXPECT_TRUE(saw_second);
    EXPECT_TRUE(saw_third);
}

// FV9: out-of-order / revisited docids for one term coalesce into one strictly
// ascending entry per docid (orthogonal to the intern change; guards no regression).
TEST(SniiSpimiTermBufferTest, OutOfOrderDocidCoalesces) {
    SpimiTermBuffer buf(/*has_positions=*/true);
    buf.add_token(std::string_view("t"), 5, 50);
    buf.add_token(std::string_view("t"), 1, 10);
    buf.add_token(std::string_view("t"), 5, 52); // revisit doc 5

    std::vector<TermPostings> terms = buf.finalize_sorted();
    ASSERT_EQ(terms.size(), 1U);
    EXPECT_EQ(terms[0].docids, (std::vector<uint32_t> {1U, 5U}));
    EXPECT_EQ(terms[0].freqs, (std::vector<uint32_t> {1U, 2U}));
    EXPECT_EQ(terms[0].positions_flat, (std::vector<uint32_t> {10U, 50U, 52U}));
    EXPECT_TRUE(buf.status().ok());
}

// ---------------------------------------------------------------------------------
// Deterministic performance verification (allocation seam counts)
// ---------------------------------------------------------------------------------

// The same >SSO term fed M times materializes its string EXACTLY ONCE: no per-token
// temporary probe std::string (F21) and no second owned-string map key (F03). The
// OLD instrumented baseline would have been M temporaries + 1 emplace = M+1.
TEST(SniiSpimiTermBufferTest, VocabInterningMaterializesEachStringOnce) {
    stb_testing::reset_vocab_string_materialization_count();
    SpimiTermBuffer buf(/*has_positions=*/false);
    const std::string term = MixedLongAscii();
    ASSERT_GT(term.size(), 15U);

    constexpr uint32_t kRepeats = 1000;
    for (uint32_t d = 0; d < kRepeats; ++d) {
        buf.add_token(std::string_view(term), d, 0);
    }
    EXPECT_EQ(stb_testing::vocab_string_materialization_count(), 1U);
    EXPECT_EQ(buf.unique_terms(), 1U);
}

// N distinct >SSO terms, each fed twice, materialize exactly N strings: the count
// tracks DISTINCT terms (one owned_vocab_.emplace_back each), not total tokens -- the
// repeat of an already-seen term allocates nothing on the heterogeneous hit path.
TEST(SniiSpimiTermBufferTest, VocabMaterializesOncePerDistinctTerm) {
    stb_testing::reset_vocab_string_materialization_count();
    SpimiTermBuffer buf(/*has_positions=*/false);

    constexpr uint32_t kDistinct = 500;
    for (uint32_t i = 0; i < kDistinct; ++i) {
        const std::string term =
                "ordinary-prefix-sharing-distinct-term-number-" + std::to_string(i);
        buf.add_token(std::string_view(term), i, 0);
        buf.add_token(std::string_view(term), i + kDistinct, 0); // repeat: zero materialization
    }
    EXPECT_EQ(stb_testing::vocab_string_materialization_count(), static_cast<uint64_t>(kDistinct));
    EXPECT_EQ(buf.unique_terms(), static_cast<size_t>(kDistinct));
}

// The seam resets cleanly between measurements (guards the reset_/count_ contract the
// two perf tests above rely on for determinism in a shared process).
TEST(SniiSpimiTermBufferTest, MaterializationCounterResets) {
    stb_testing::reset_vocab_string_materialization_count();
    EXPECT_EQ(stb_testing::vocab_string_materialization_count(), 0U);
    SpimiTermBuffer buf(/*has_positions=*/false);
    buf.add_token(std::string_view("one"), 0, 0);
    buf.add_token(std::string_view("two"), 0, 0);
    EXPECT_EQ(stb_testing::vocab_string_materialization_count(), 2U);
    stb_testing::reset_vocab_string_materialization_count();
    EXPECT_EQ(stb_testing::vocab_string_materialization_count(), 0U);
}
