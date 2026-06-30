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

#include "storage/index/snii/writer/spimi_term_buffer.h"

#include <fcntl.h>
#include <gtest/gtest.h>
#include <sys/resource.h>
#include <unistd.h>

#include <algorithm>
#include <cstdint>
#include <string>
#include <vector>

#include "common/status.h"

using doris::snii::writer::MemoryReporter;
using doris::snii::writer::SpimiTermBuffer;
using doris::snii::writer::TermPostings;
using doris::Status;

// Tokens accumulate into sorted terms with ascending docids and per-doc positions.
TEST(SniiSpimiTermBuffer, AccumulateAndSort) {
    SpimiTermBuffer buf(/*has_positions=*/true);
    // doc 0: "banana apple apple"
    buf.add_token("banana", 0, 0);
    buf.add_token("apple", 0, 1);
    buf.add_token("apple", 0, 2);
    // doc 1: "apple cherry"
    buf.add_token("apple", 1, 0);
    buf.add_token("cherry", 1, 1);

    EXPECT_EQ(buf.unique_terms(), 3U);
    EXPECT_EQ(buf.total_tokens(), 5U);

    std::vector<TermPostings> terms = buf.finalize_sorted();
    ASSERT_EQ(terms.size(), 3U);
    // Sorted lexicographically: apple, banana, cherry.
    EXPECT_EQ(terms[0].term, "apple");
    EXPECT_EQ(terms[1].term, "banana");
    EXPECT_EQ(terms[2].term, "cherry");

    // apple: docs 0 (freq 2, pos {1,2}) and 1 (freq 1, pos {0}).
    const TermPostings& apple = terms[0];
    ASSERT_EQ(apple.docids.size(), 2U);
    EXPECT_EQ(apple.docids[0], 0U);
    EXPECT_EQ(apple.freqs[0], 2U);
    ASSERT_EQ(apple.doc_positions(0).size(), 2U);
    EXPECT_EQ(apple.doc_positions(0)[0], 1U);
    EXPECT_EQ(apple.doc_positions(0)[1], 2U);
    EXPECT_EQ(apple.docids[1], 1U);
    EXPECT_EQ(apple.freqs[1], 1U);
}

// Without positions, freq is still counted but positions vectors stay empty.
TEST(SniiSpimiTermBuffer, DocsOnlyNoPositions) {
    SpimiTermBuffer buf(/*has_positions=*/false);
    buf.add_token("x", 0, 0);
    buf.add_token("x", 0, 1);
    buf.add_token("x", 2, 0);

    std::vector<TermPostings> terms = buf.finalize_sorted();
    ASSERT_EQ(terms.size(), 1U);
    EXPECT_EQ(terms[0].term, "x");
    ASSERT_EQ(terms[0].docids.size(), 2U);
    EXPECT_EQ(terms[0].docids[0], 0U);
    EXPECT_EQ(terms[0].freqs[0], 2U);
    EXPECT_EQ(terms[0].docids[1], 2U);
    EXPECT_EQ(terms[0].freqs[1], 1U);
    EXPECT_TRUE(terms[0].positions_flat.empty());
}

TEST(SniiSpimiTermBuffer, Empty) {
    SpimiTermBuffer buf(true);
    EXPECT_EQ(buf.unique_terms(), 0U);
    EXPECT_TRUE(buf.finalize_sorted().empty());
}

// Feeds the same token stream into two buffers and asserts the streaming
// for_each_term_sorted produces the byte-identical postings finalize_sorted
// returns (same flat-array refactor must not change observable output).
TEST(SniiSpimiTermBuffer, StreamingMatchesMaterialized) {
    auto feed = [](SpimiTermBuffer& b) {
        b.add_token("banana", 0, 0);
        b.add_token("apple", 0, 1);
        b.add_token("apple", 0, 2);
        b.add_token("apple", 1, 0);
        b.add_token("cherry", 1, 1);
        b.add_token("apple", 5, 3);
        b.add_token("apple", 5, 7);
        b.add_token("banana", 9, 0);
    };
    SpimiTermBuffer mat(/*has_positions=*/true);
    SpimiTermBuffer strm(/*has_positions=*/true);
    feed(mat);
    feed(strm);

    std::vector<TermPostings> material = mat.finalize_sorted();
    std::vector<TermPostings> streamed;
    Status st = strm.for_each_term_sorted(
            [&](TermPostings&& tp) { streamed.push_back(std::move(tp)); });
    EXPECT_TRUE(st.ok());

    ASSERT_EQ(material.size(), streamed.size());
    for (size_t i = 0; i < material.size(); ++i) {
        EXPECT_EQ(material[i].term, streamed[i].term);
        EXPECT_EQ(material[i].docids, streamed[i].docids);
        EXPECT_EQ(material[i].freqs, streamed[i].freqs);
        EXPECT_EQ(material[i].positions_flat, streamed[i].positions_flat);
    }
    // apple: docs {0(pos 1,2), 1(pos 0), 5(pos 3,7)} -> positions re-sliced by freq.
    ASSERT_EQ(streamed[0].term, "apple");
    ASSERT_EQ(streamed[0].docids.size(), 3U);
    EXPECT_EQ(streamed[0].freqs, (std::vector<uint32_t> {2U, 1U, 2U}));
    EXPECT_EQ(std::vector<uint32_t>(streamed[0].doc_positions(2).begin(),
                                    streamed[0].doc_positions(2).end()),
              (std::vector<uint32_t> {3U, 7U}));
}

// Out-of-order docid GROUPS (each doc's tokens stay contiguous, but the docids
// are not non-decreasing) are tolerated and reordered once at finalize, with
// each doc carrying its own positions (defensive fallback path, e.g. a merge of
// pre-sorted runs). Tokens for a single docid are always contiguous.
TEST(SniiSpimiTermBuffer, OutOfOrderDocidsSortedAtFinalize) {
    SpimiTermBuffer buf(/*has_positions=*/true);
    buf.add_token("t", 5, 50); // doc 5 group (contiguous)
    buf.add_token("t", 5, 51);
    buf.add_token("t", 1, 10); // doc 1 group, arrives after doc 5
    buf.add_token("t", 1, 11);
    buf.add_token("t", 3, 30); // doc 3 group

    std::vector<TermPostings> terms = buf.finalize_sorted();
    ASSERT_EQ(terms.size(), 1U);
    const TermPostings& t = terms[0];
    EXPECT_EQ(t.docids, (std::vector<uint32_t> {1U, 3U, 5U}));
    EXPECT_EQ(t.freqs, (std::vector<uint32_t> {2U, 1U, 2U}));
    EXPECT_EQ(t.positions_flat, (std::vector<uint32_t> {10U, 11U, 30U, 50U, 51U}));
}

// BORROWED-vocab id path: feeding raw term-ids (no per-token string work)
// produces the SAME lexicographically sorted postings as the string path. The
// vocab order (apple < banana < cherry) drives the emitted order, NOT the id
// order (banana=0, apple=1, cherry=2).
TEST(SniiSpimiTermBuffer, TermIdPathMatchesStringPath) {
    const std::vector<std::string> vocab = {"banana", "apple", "cherry"};
    SpimiTermBuffer buf(&vocab, /*has_positions=*/true);
    // doc 0: "banana apple apple", doc 1: "apple cherry" -- by id.
    buf.add_token(0, 0, 0); // banana
    buf.add_token(1, 0, 1); // apple
    buf.add_token(1, 0, 2); // apple
    buf.add_token(1, 1, 0); // apple
    buf.add_token(2, 1, 1); // cherry

    EXPECT_EQ(buf.unique_terms(), 3U);
    EXPECT_EQ(buf.total_tokens(), 5U);
    EXPECT_TRUE(buf.status().ok());

    std::vector<TermPostings> terms = buf.finalize_sorted();
    ASSERT_EQ(terms.size(), 3U);
    EXPECT_EQ(terms[0].term, "apple");
    EXPECT_EQ(terms[1].term, "banana");
    EXPECT_EQ(terms[2].term, "cherry");
    const TermPostings& apple = terms[0];
    ASSERT_EQ(apple.docids.size(), 2U);
    EXPECT_EQ(apple.freqs[0], 2U);
    EXPECT_EQ(std::vector<uint32_t>(apple.doc_positions(0).begin(), apple.doc_positions(0).end()),
              (std::vector<uint32_t> {1U, 2U}));
    EXPECT_EQ(apple.docids[1], 1U);
    EXPECT_EQ(apple.freqs[1], 1U);
}

// A term-id never touched is simply skipped (no empty term emitted); an empty
// vocab yields no terms and stays valid.
TEST(SniiSpimiTermBuffer, UntouchedIdSkippedAndEmptyVocab) {
    const std::vector<std::string> vocab = {"a", "b", "c", "d"};
    SpimiTermBuffer buf(&vocab, /*has_positions=*/false);
    buf.add_token(0, 0, 0); // a
    buf.add_token(2, 1, 0); // c -- ids 1 (b) and 3 (d) never touched
    std::vector<TermPostings> terms = buf.finalize_sorted();
    ASSERT_EQ(terms.size(), 2U);
    EXPECT_EQ(terms[0].term, "a");
    EXPECT_EQ(terms[1].term, "c");

    const std::vector<std::string> empty;
    SpimiTermBuffer empty_buf(&empty, /*has_positions=*/false);
    EXPECT_EQ(empty_buf.unique_terms(), 0U);
    EXPECT_TRUE(empty_buf.finalize_sorted().empty());
    EXPECT_TRUE(empty_buf.status().ok());
}

// An out-of-range term-id is rejected: the token is ignored and an
// InvalidArgument is latched into status().
TEST(SniiSpimiTermBuffer, OutOfRangeTermIdRejected) {
    const std::vector<std::string> vocab = {"x", "y"};
    SpimiTermBuffer buf(&vocab, /*has_positions=*/true);
    buf.add_token(0, 0, 0); // valid
    buf.add_token(5, 0, 1); // out of range -> ignored + latched
    EXPECT_FALSE(buf.status().ok());
    EXPECT_EQ(buf.unique_terms(), 1U);
    EXPECT_EQ(buf.total_tokens(), 1U); // the rejected token was not counted
}

// The borrowed-vocab id path is byte-identical across a spill: a tiny threshold
// (many spills + k-way merge over term-id runs) must match the unlimited build.
TEST(SniiSpimiTermBuffer, TermIdSpillMatchesUnlimited) {
    const std::vector<std::string> vocab = {"alpha", "beta", "gamma", "delta"};
    auto feed = [&](SpimiTermBuffer& b) {
        for (uint32_t d = 0; d < 300; ++d) {
            b.add_token(0, d, 0); // alpha: every doc
            b.add_token(0, d, 9); // freq 2
            if (d % 2 == 0) {
                b.add_token(1, d, 1); // beta
            }
            if (d % 3 == 0) {
                b.add_token(2, d, 2); // gamma
            }
            if (d % 5 == 0) {
                b.add_token(3, d, 3); // delta
            }
        }
    };
    SpimiTermBuffer un(&vocab, /*has_positions=*/true, /*spill=*/0);
    SpimiTermBuffer sp(&vocab, /*has_positions=*/true, /*spill=*/256);
    feed(un);
    feed(sp);
    const std::vector<TermPostings> a = un.finalize_sorted();
    const std::vector<TermPostings> b = sp.finalize_sorted();
    ASSERT_EQ(a.size(), b.size());
    for (size_t i = 0; i < a.size(); ++i) {
        EXPECT_EQ(a[i].term, b[i].term);
        EXPECT_EQ(a[i].docids, b[i].docids);
        EXPECT_EQ(a[i].freqs, b[i].freqs);
        EXPECT_EQ(a[i].positions_flat, b[i].positions_flat);
    }
    EXPECT_TRUE(un.status().ok());
    EXPECT_TRUE(sp.status().ok());
}

// for_each_term_sorted drains the buffer term-by-term: after each callback the
// consumed term is gone, so at most one term's arrays remain materialized.
TEST(SniiSpimiTermBuffer, StreamingDrainsAndShrinks) {
    SpimiTermBuffer buf(/*has_positions=*/false);
    for (uint32_t d = 0; d < 100; ++d) {
        buf.add_token("a", d, 0);
        buf.add_token("b", d, 0);
        buf.add_token("c", d, 0);
    }
    EXPECT_EQ(buf.unique_terms(), 3U);
    std::vector<size_t> remaining_after_each;
    size_t seen = 0;
    EXPECT_TRUE(buf.for_each_term_sorted([&](TermPostings&& tp) {
                       ++seen;
                       EXPECT_EQ(tp.docids.size(), 100U);
                       remaining_after_each.push_back(buf.unique_terms());
                   }).ok());
    EXPECT_EQ(seen, 3U);
    // After consuming each of the 3 terms, the live count drops 2,1,0.
    EXPECT_EQ(remaining_after_each, (std::vector<size_t> {2U, 1U, 0U}));
    EXPECT_EQ(buf.unique_terms(), 0U);
}

// A REVISITED docid (the out-of-order defensive path actually re-touches a doc:
// feed 5,1,5) MUST coalesce into ONE entry per docid -- summed freq, positions
// concatenated in document order -- matching the k-way merge path and the writer's
// strictly-ascending precondition. Without coalescing this yielded docids
// {1,5,5} (duplicate, unsorted) that the writer later rejects.
TEST(SniiSpimiTermBuffer, RevisitedDocidCoalescesWithPositions) {
    SpimiTermBuffer buf(/*has_positions=*/true);
    buf.add_token("t", 5, 50); // doc 5, first visit
    buf.add_token("t", 5, 51);
    buf.add_token("t", 1, 10); // doc 1
    buf.add_token("t", 5, 52); // doc 5 REVISITED (a fresh doc-group, same docid)

    std::vector<TermPostings> terms = buf.finalize_sorted();
    ASSERT_EQ(terms.size(), 1U);
    const TermPostings& t = terms[0];
    // Exactly one entry per docid, strictly ascending.
    EXPECT_EQ(t.docids, (std::vector<uint32_t> {1U, 5U}));
    // doc 5 freq = 2 (first visit) + 1 (revisit) = 3; doc 1 freq = 1.
    EXPECT_EQ(t.freqs, (std::vector<uint32_t> {1U, 3U}));
    // Positions in document order: doc 1 {10}, then doc 5's two visits in arrival
    // order {50,51} then {52}.
    EXPECT_EQ(t.positions_flat, (std::vector<uint32_t> {10U, 50U, 51U, 52U}));
    // doc_positions slices stay consistent with the merged freqs.
    EXPECT_EQ(std::vector<uint32_t>(t.doc_positions(1).begin(), t.doc_positions(1).end()),
              (std::vector<uint32_t> {50U, 51U, 52U}));
    EXPECT_TRUE(buf.status().ok());
}

// Same revisit, positions disabled: freqs still sum and docids stay unique.
TEST(SniiSpimiTermBuffer, RevisitedDocidCoalescesNoPositions) {
    SpimiTermBuffer buf(/*has_positions=*/false);
    buf.add_token("t", 5, 0);
    buf.add_token("t", 1, 0);
    buf.add_token("t", 5, 0); // revisit doc 5

    std::vector<TermPostings> terms = buf.finalize_sorted();
    ASSERT_EQ(terms.size(), 1U);
    EXPECT_EQ(terms[0].docids, (std::vector<uint32_t> {1U, 5U}));
    EXPECT_EQ(terms[0].freqs, (std::vector<uint32_t> {1U, 2U}));
    EXPECT_TRUE(terms[0].positions_flat.empty());
}

// The coalesced out-of-order output satisfies the writer's strictly-ascending
// docid precondition: docids are unique AND strictly increasing (the exact check
// LogicalIndexWriter::validate_term enforces). This is the contract the fix
// restores -- previously a revisited docid produced a non-ascending list.
TEST(SniiSpimiTermBuffer, RevisitedDocidProducesStrictlyAscending) {
    SpimiTermBuffer buf(/*has_positions=*/true);
    // A messy revisit pattern: 9,3,9,3,1.
    buf.add_token("w", 9, 0);
    buf.add_token("w", 3, 0);
    buf.add_token("w", 9, 1);
    buf.add_token("w", 3, 1);
    buf.add_token("w", 1, 0);

    std::vector<TermPostings> terms = buf.finalize_sorted();
    ASSERT_EQ(terms.size(), 1U);
    const TermPostings& t = terms[0];
    ASSERT_FALSE(t.docids.empty());
    for (size_t i = 1; i < t.docids.size(); ++i) {
        EXPECT_LT(t.docids[i - 1], t.docids[i]) << "docids must be strictly ascending";
    }
    EXPECT_EQ(t.docids, (std::vector<uint32_t> {1U, 3U, 9U}));
    EXPECT_EQ(t.freqs, (std::vector<uint32_t> {1U, 2U, 2U}));
    // Total positions equals total tokens (sum of freqs) -- nothing dropped.
    uint64_t total_freq = 0;
    for (uint32_t f : t.freqs) {
        total_freq += f;
    }
    EXPECT_EQ(t.positions_flat.size(), total_freq);
}

// Hardening: add_token(string_view) on a BORROWED-vocab buffer is rejected (it
// would otherwise grow the owned vocab out of step with the borrowed one and
// corrupt the build). The token is ignored and an error is latched.
TEST(SniiSpimiTermBuffer, AddTokenStringViewRejectedInBorrowedMode) {
    const std::vector<std::string> vocab = {"a", "b"};
    SpimiTermBuffer buf(&vocab, /*has_positions=*/false);
    buf.add_token(0, 0, 0);                     // valid id-path token
    buf.add_token(std::string_view("a"), 1, 0); // illegal on a borrowed-vocab buffer
    EXPECT_FALSE(buf.status().ok());
    // The string-view token was ignored: only the one id-path token counts.
    EXPECT_EQ(buf.total_tokens(), 1U);
    EXPECT_EQ(buf.unique_terms(), 1U);
}

// A spill's open() I/O failure surfaces as a LATCHED error in status() (the
// streaming for_each_term_sorted swallows the failure, so callers must check
// status()). We force open() to fail deterministically by EXHAUSTING every free
// file descriptor below the soft limit, so the spill's ::open returns EMFILE.
TEST(SniiSpimiTermBuffer, SpillOpenIoFailureLatched) {
    // Tiny threshold so the very first token triggers a spill_to_run().
    SpimiTermBuffer buf(/*has_positions=*/false, /*spill_threshold_bytes=*/1);

    // Cap the soft limit low so we can exhaust the fd table cheaply, then hold it.
    struct rlimit saved {};
    ASSERT_EQ(getrlimit(RLIMIT_NOFILE, &saved), 0);
    struct rlimit tight = saved;
    tight.rlim_cur = 64; // small, but >= the few gtest/std fds already open
    tight.rlim_cur = std::min(tight.rlim_cur, saved.rlim_max);
    ASSERT_EQ(setrlimit(RLIMIT_NOFILE, &tight), 0);

    // Open /dev/null until the table is full: every free fd below the limit is now
    // taken, so the next ::open (the spill's) cannot get one -> EMFILE.
    std::vector<int> hogs;
    for (;;) {
        int fd = ::open("/dev/null", O_RDONLY);
        if (fd < 0) {
            break; // table exhausted
        }
        hogs.push_back(fd);
    }
    ASSERT_FALSE(hogs.empty());

    buf.add_token("z", 0, 0); // triggers a spill whose RunWriter::open must fail

    // Release the hog fds and restore the limit before asserting (so gtest I/O works).
    for (int fd : hogs) {
        ::close(fd);
    }
    ASSERT_EQ(setrlimit(RLIMIT_NOFILE, &saved), 0);

    EXPECT_FALSE(buf.status().ok()) << "spill open() failure must latch an error";
}

// Double-drain safety: a SECOND drain (finalize_sorted after for_each_term_sorted,
// or a second finalize_sorted) must NOT silently re-emit or emit a wrong stream;
// it returns empty / no callbacks AND latches an error.
TEST(SniiSpimiTermBuffer, DoubleDrainIsRejected) {
    SpimiTermBuffer buf(/*has_positions=*/true);
    buf.add_token("a", 0, 0);
    buf.add_token("b", 0, 0);

    std::vector<TermPostings> first = buf.finalize_sorted();
    ASSERT_EQ(first.size(), 2U);
    EXPECT_TRUE(buf.status().ok());

    // Second finalize_sorted: empty result + latched error.
    std::vector<TermPostings> second = buf.finalize_sorted();
    EXPECT_TRUE(second.empty());
    EXPECT_FALSE(buf.status().ok());

    // for_each_term_sorted after a drain also emits nothing; now returns an error.
    size_t seen = 0;
    EXPECT_FALSE(buf.for_each_term_sorted([&](TermPostings&&) { ++seen; }).ok());
    EXPECT_EQ(seen, 0U);
}

// Double-drain via for_each_term_sorted first, then finalize_sorted: same guard.
TEST(SniiSpimiTermBuffer, DoubleDrainStreamingThenMaterialized) {
    SpimiTermBuffer buf(/*has_positions=*/false);
    buf.add_token("a", 0, 0);

    size_t seen = 0;
    EXPECT_TRUE(buf.for_each_term_sorted([&](TermPostings&&) { ++seen; }).ok());
    EXPECT_EQ(seen, 1U);
    EXPECT_TRUE(buf.status().ok());

    std::vector<TermPostings> again = buf.finalize_sorted();
    EXPECT_TRUE(again.empty());
    EXPECT_FALSE(buf.status().ok());
}

// BYTE-IDENTICAL guard for normal ascending input: the streaming and materialized
// drains over an ASCENDING feed (the common, valid path -- NOT the out-of-order
// path the coalescing fix touches) must produce identical docids/freqs/positions.
// This asserts the fix did not perturb the normal path's produced postings.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST(SniiSpimiTermBuffer, AscendingInputByteIdenticalAcrossDrains) {
    auto feed = [](SpimiTermBuffer& b) {
        for (uint32_t d = 0; d < 50; ++d) {
            b.add_token("apple", d, d * 2);
            b.add_token("apple", d, d * 2 + 1); // freq 2 per doc
            if (d % 2 == 0) {
                b.add_token("banana", d, d);
            }
            if (d % 3 == 0) {
                b.add_token("cherry", d, d + 100);
            }
        }
    };
    SpimiTermBuffer mat(/*has_positions=*/true);
    SpimiTermBuffer strm(/*has_positions=*/true);
    feed(mat);
    feed(strm);

    std::vector<TermPostings> material = mat.finalize_sorted();
    std::vector<TermPostings> streamed;
    Status st = strm.for_each_term_sorted(
            [&](TermPostings&& tp) { streamed.push_back(std::move(tp)); });
    EXPECT_TRUE(st.ok());

    ASSERT_EQ(material.size(), streamed.size());
    for (size_t i = 0; i < material.size(); ++i) {
        EXPECT_EQ(material[i].term, streamed[i].term);
        EXPECT_EQ(material[i].docids, streamed[i].docids);
        EXPECT_EQ(material[i].freqs, streamed[i].freqs);
        EXPECT_EQ(material[i].positions_flat, streamed[i].positions_flat);
    }
    // Spot-check apple stayed exactly one entry per ascending docid (no coalescing
    // path was taken for this valid feed).
    ASSERT_EQ(material[0].term, "apple");
    EXPECT_EQ(material[0].docids.size(), 50U);
    for (uint32_t f : material[0].freqs) {
        EXPECT_EQ(f, 2U);
    }
    EXPECT_TRUE(mat.status().ok());
    EXPECT_TRUE(strm.status().ok());
}

// ---------------------------------------------------------------------------
// T17: MemoryReporter per-token zero-delta debounce.
//
// accumulate() reports its REAL resident-byte delta (posting arena + the
// vocab-sized slot index) to the writer-level MemoryReporter once per token. The
// arena grows only ~every 32 KiB block and the borrowed-vocab slot index is
// fixed-capacity, so the vast majority of tokens see delta==0. report_arena_delta()
// now SKIPS the locked fetch_add for those (debounce). These tests pin the
// deterministic op-count (report() calls == arena-growth events, never per token),
// the byte-level equivalence (current_bytes() unchanged), and the REDLINE: over_cap()
// is still evaluated UNCONDITIONALLY every token (never gated on the local delta), so
// a dict-side push over the unified cap still triggers a spill when this buffer's own
// delta is 0. A MemoryReporter built with a counting consume_release lambda exposes
// the exact per-token report() count / delta values as a deterministic seam.
// ---------------------------------------------------------------------------

// FV-1 (deterministic op-count + functional): feeding 100 same-doc tokens issues
// exactly TWO report() calls -- one ctor delta (the resident slot index) and one for
// the first token's 32 KiB arena block -- and NEVER a zero-delta report. Before the
// debounce, tokens 2..100 each issued report(0): 101 calls, 99 of them zero.
TEST(SniiSpimiTermBufferTest, AccumulateIssuesNoZeroDeltaReport) {
    std::vector<int64_t> deltas;
    MemoryReporter rep([&deltas](int64_t d) { deltas.push_back(d); }, /*cap_bytes=*/0);
    const std::vector<std::string> vocab = {"a"};
    SpimiTermBuffer buf(&vocab, /*has_positions=*/false, /*spill=*/0, &rep);

    for (int i = 0; i < 100; ++i) {
        buf.add_token(0, /*docid=*/1, /*pos=*/0); // same term, same doc
    }

    // 1 ctor report (slot index) + 1 first-token report (first 32 KiB arena block).
    // The other 99 same-doc tokens leave resident unchanged -> debounced away.
    ASSERT_EQ(deltas.size(), 2U);
    EXPECT_GT(deltas[0], 0);     // slot index resident bytes (vocab-sized)
    EXPECT_EQ(deltas[1], 32768); // exactly one CompactPostingPool block (1 << 15)
    for (int64_t d : deltas) {
        EXPECT_NE(d, 0) << "no zero-delta report() may be issued on the hot path";
    }
    EXPECT_TRUE(buf.status().ok());
}

// FV-2 (equivalence + count stability): the report() COUNT is independent of the
// token count (100 vs 500 same-doc tokens both issue exactly 2 reports), and the
// resulting unified total is byte-identical (resident = first arena block + slot
// index, not a function of token count). The sum of issued deltas equals
// current_bytes() -- the MemoryReporter self-balancing invariant the debounce
// preserves. Snapshots are taken WHILE each buffer is live (before its dtor reports
// the final balancing negative).
TEST(SniiSpimiTermBufferTest, ReportedTotalMatchesResidentRegardlessOfTokenCount) {
    const std::vector<std::string> vocab = {"a"};

    std::vector<int64_t> d100;
    std::vector<int64_t> d500;
    int64_t cur100 = 0;
    int64_t cur500 = 0;
    size_t count100 = 0;
    size_t count500 = 0;

    {
        MemoryReporter rep([&d100](int64_t d) { d100.push_back(d); }, /*cap_bytes=*/0);
        SpimiTermBuffer buf(&vocab, /*has_positions=*/false, /*spill=*/0, &rep);
        for (int i = 0; i < 100; ++i) {
            buf.add_token(0, /*docid=*/1, /*pos=*/0);
        }
        count100 = d100.size(); // snapshot before the dtor's balancing report
        cur100 = rep.current_bytes();
    }
    {
        MemoryReporter rep([&d500](int64_t d) { d500.push_back(d); }, /*cap_bytes=*/0);
        SpimiTermBuffer buf(&vocab, /*has_positions=*/false, /*spill=*/0, &rep);
        for (int i = 0; i < 500; ++i) {
            buf.add_token(0, /*docid=*/1, /*pos=*/0);
        }
        count500 = d500.size();
        cur500 = rep.current_bytes();
    }

    // Count stability: neither buffer's report count scales with token count.
    EXPECT_EQ(count100, 2U);
    EXPECT_EQ(count500, 2U);
    // Byte-level equivalence: identical unified total despite 5x the tokens.
    EXPECT_EQ(cur100, cur500);
    ASSERT_GE(d100.size(), 2U);
    ASSERT_GE(d500.size(), 2U);
    // Both: identical first 32 KiB arena block (the slot index cancels in this delta).
    EXPECT_EQ(d100[1], 32768);
    EXPECT_EQ(d500[1], 32768);
    // Self-balancing: live current_bytes() == sum of every delta issued so far.
    int64_t sum100 = 0;
    for (size_t i = 0; i < count100; ++i) {
        sum100 += d100[i];
    }
    EXPECT_EQ(cur100, sum100);
    EXPECT_EQ(cur100, 32768 + d100[0]); // arena block + slot index resident
}

// FV-3 (REDLINE guard): the debounce skips report() ONLY -- it must NOT gate
// over_cap(). over_cap() reads the writer-level UNIFIED total (shared with the dict
// buffer), so a dict-side allocation can push the total over the cap while THIS
// buffer's local arena delta is 0. accumulate() must still evaluate over_cap() every
// token and spill. The dict-side growth is simulated with an external rep.report().
TEST(SniiSpimiTermBufferTest, OverCapStillFiresWhenLocalArenaDeltaIsZero) {
    // Cap just above one arena block + slot index, so token1 alone does NOT spill.
    MemoryReporter rep(/*consume_release=*/nullptr, /*cap_bytes=*/32768 + 1000);
    const std::vector<std::string> vocab = {"a"};
    SpimiTermBuffer buf(&vocab, /*has_positions=*/false, /*spill=*/0, &rep);

    // token1: arena grows to one 32 KiB block; unified total < cap -> no spill.
    buf.add_token(0, /*docid=*/0, /*pos=*/0);
    ASSERT_EQ(buf.run_count_for_test(), 0U);

    // Simulate dict-side growth pushing the UNIFIED total over the cap. The buffer's
    // own reported_resident_ is unchanged by this external report.
    rep.report(2000);
    ASSERT_TRUE(rep.over_cap());

    // token2: same doc -> this buffer's local arena delta is 0 (report() debounced),
    // but over_cap() is still evaluated unconditionally and is now true -> spill.
    buf.add_token(0, /*docid=*/0, /*pos=*/0);
    EXPECT_EQ(buf.run_count_for_test(), 1U)
            << "over_cap() must NOT be gated on the local arena delta";
    EXPECT_TRUE(buf.status().ok());
}

// FV-4 (boundary): an EMPTY borrowed vocab has a zero-capacity slot index, so the
// ctor's resident delta is 0 and is debounced away -- no report() is issued and
// construction does not crash. Before the debounce the ctor issued report(0).
TEST(SniiSpimiTermBufferTest, EmptyVocabReportsNoDelta) {
    std::vector<int64_t> deltas;
    MemoryReporter rep([&deltas](int64_t d) { deltas.push_back(d); }, /*cap_bytes=*/0);
    const std::vector<std::string> empty_vocab;
    SpimiTermBuffer buf(&empty_vocab, /*has_positions=*/false, /*spill=*/0, &rep);

    // Zero-capacity slot index + empty arena -> resident 0 -> ctor delta 0 -> skipped.
    EXPECT_TRUE(deltas.empty());
    EXPECT_EQ(buf.unique_terms(), 0U);
    EXPECT_TRUE(buf.finalize_sorted().empty());
    EXPECT_TRUE(buf.status().ok());
}

// FV-5 (no-reporter path unaffected): with a null reporter, report_arena_delta() is a
// no-op and finalize_sorted() still produces the correct postings. Confirms the
// debounce change did not perturb the off-Doris path.
TEST(SniiSpimiTermBufferTest, NullReporterFinalizeIsCorrect) {
    const std::vector<std::string> vocab = {"a"};
    SpimiTermBuffer buf(&vocab, /*has_positions=*/false, /*spill=*/0, /*reporter=*/nullptr);

    for (int i = 0; i < 100; ++i) {
        buf.add_token(0, /*docid=*/1, /*pos=*/0); // same term, same doc
    }

    std::vector<TermPostings> terms = buf.finalize_sorted();
    ASSERT_EQ(terms.size(), 1U);
    EXPECT_EQ(terms[0].term, "a");
    EXPECT_EQ(terms[0].docids, (std::vector<uint32_t> {1U}));
    EXPECT_EQ(terms[0].freqs, (std::vector<uint32_t> {100U}));
    EXPECT_TRUE(terms[0].positions_flat.empty());
    EXPECT_TRUE(buf.status().ok());
}

// FV-6 (spill/drain negative path + self-balance): a forced spill emits NONZERO
// negative deltas (arena reset, then slot-index free), never a zero. After a full
// drain the reporter's unified total returns to 0 -- the debounce preserves the
// self-balancing invariant (no leaked positive). The merged postings are correct.
TEST(SniiSpimiTermBufferTest, SpillNegativeDeltasHaveNoZeroAndBalance) {
    std::vector<int64_t> deltas;
    // Cap below one arena block, so the first token's block forces a spill.
    MemoryReporter rep([&deltas](int64_t d) { deltas.push_back(d); }, /*cap_bytes=*/1000);
    const std::vector<std::string> vocab = {"a"};
    SpimiTermBuffer buf(&vocab, /*has_positions=*/false, /*spill=*/0, &rep);

    buf.add_token(0, /*docid=*/0, /*pos=*/0); // grows one block, then spills
    ASSERT_EQ(buf.run_count_for_test(), 1U) << "the over-cap token must spill";

    std::vector<TermPostings> terms = buf.finalize_sorted();
    ASSERT_EQ(terms.size(), 1U);
    EXPECT_EQ(terms[0].term, "a");
    EXPECT_EQ(terms[0].docids, (std::vector<uint32_t> {0U}));
    EXPECT_EQ(terms[0].freqs, (std::vector<uint32_t> {1U}));
    EXPECT_TRUE(buf.status().ok());

    // No zero-delta report on any path (grow, spill-negative, or merge-free).
    for (int64_t d : deltas) {
        EXPECT_NE(d, 0) << "spill/drain deltas must all be nonzero";
    }
    // Self-balancing: after a full drain every reported byte has been returned.
    EXPECT_EQ(rep.current_bytes(), 0);
}
