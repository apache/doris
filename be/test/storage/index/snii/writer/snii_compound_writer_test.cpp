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

#include "storage/index/snii/writer/snii_compound_writer.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_source.h"
#include "storage/index/snii/format/bootstrap_header.h"
#include "storage/index/snii/format/bsbf.h"
#include "storage/index/snii/format/dict_block.h"
#include "storage/index/snii/format/dict_block_directory.h"
#include "storage/index/snii/format/dict_entry.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/format/frq_pod.h"
#include "storage/index/snii/format/frq_prelude.h"
#include "storage/index/snii/format/per_index_meta.h"
#include "storage/index/snii/format/prx_pod.h"
#include "storage/index/snii/format/sampled_term_index.h"
#include "storage/index/snii/format/tail_meta_region.h"
#include "storage/index/snii/format/tail_pointer.h"
#include "storage/index/snii/io/local_file.h"
#include "storage/index/snii/io/metered_file_reader.h"
#include "storage/index/snii/query/phrase_query.h"
#include "storage/index/snii/query/term_query.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii/writer/logical_index_writer.h"

using namespace doris::snii;
using namespace doris::snii::format;
using namespace doris::snii::writer;
using doris::Status;

namespace {

// Temp file path helper (process-unique).
std::string TempPath() {
    static int counter = 0;
    return "/tmp/snii_cw_test_" + std::to_string(getpid()) + "_" + std::to_string(counter++) +
           ".idx";
}

// Reads the whole file into a buffer.
std::vector<uint8_t> ReadAll(const std::string& path) {
    io::LocalFileReader r;
    EXPECT_TRUE(r.open(path).ok());
    std::vector<uint8_t> out;
    EXPECT_TRUE(r.read_at(0, r.size(), &out).ok());
    return out;
}

// Writes bytes to a fresh temp file and returns its path.
std::string WriteTemp(const std::vector<uint8_t>& bytes) {
    const std::string p = TempPath();
    io::LocalFileWriter w;
    EXPECT_TRUE(w.open(p).ok());
    EXPECT_TRUE(w.append(Slice(bytes)).ok());
    EXPECT_TRUE(w.finalize().ok());
    return p;
}

// A FileReader decorator that counts how many physical reads (single or batched)
// touch a given byte window. Used to assert that the BSBF section is NOT read at
// all on the non-resident (L1) path: with the P1 cold-read fix, open must not
// read the 28B bloom header and lookup must not issue a 32B bloom probe.
class WindowTouchCountingReader : public io::FileReader {
public:
    WindowTouchCountingReader(io::FileReader* inner, uint64_t win_off, uint64_t win_len)
            : inner_(inner), win_off_(win_off), win_len_(win_len) {}

    Status read_at(uint64_t offset, size_t len, std::vector<uint8_t>* out) override {
        account(offset, len);
        return inner_->read_at(offset, len, out);
    }
    Status read_batch(const std::vector<io::Range>& ranges,
                      std::vector<std::vector<uint8_t>>* outs) override {
        for (const auto& r : ranges) {
            account(r.offset, r.len);
        }
        return inner_->read_batch(ranges, outs);
    }
    uint64_t size() const override { return inner_->size(); }

    uint64_t window_touches() const { return window_touches_; }

private:
    void account(uint64_t offset, size_t len) {
        if (win_len_ == 0 || len == 0) {
            return;
        }
        const uint64_t end = offset + len;
        const uint64_t win_end = win_off_ + win_len_;
        if (offset < win_end && win_off_ < end) {
            ++window_touches_;
        }
    }
    io::FileReader* inner_;
    uint64_t win_off_;
    uint64_t win_len_;
    uint64_t window_touches_ = 0;
};

// Builds a TermPostings with constant freq per doc and (optionally) positions.
TermPostings MakeTerm(const std::string& term, const std::vector<uint32_t>& docids,
                      bool with_positions) {
    TermPostings tp;
    tp.term = term;
    tp.docids = docids;
    tp.freqs.assign(docids.size(), 0);
    for (size_t i = 0; i < docids.size(); ++i) {
        tp.freqs[i] = static_cast<uint32_t>((i % 3) + 1);
    }
    if (with_positions) {
        for (size_t i = 0; i < docids.size(); ++i) {
            for (uint32_t k = 0; k < tp.freqs[i]; ++k) {
                tp.positions_flat.push_back(k * 2); // ascending positions (flat, by freq)
            }
        }
    }
    return tp;
}

// Builds a FrqRegionMeta for a window's dd region from its WindowMeta.
FrqRegionMeta DdMetaOf(const WindowMeta& m) {
    FrqRegionMeta r;
    r.zstd = m.dd_zstd;
    r.uncomp_len = m.dd_uncomp_len;
    r.disk_len = m.dd_disk_len;
    r.crc = m.crc_dd;
    return r;
}

// Index 0: ~30 docs. Vocab includes a HIGH-df term (df=600 > 512 -> windowed)
// and several LOW-df terms (-> slim/inline). Terms must be lexicographically
// sorted.
SniiIndexInput MakeIndex(uint64_t index_id, const std::string& suffix, uint32_t doc_count) {
    SniiIndexInput in;
    in.index_id = index_id;
    in.index_suffix = suffix;
    in.config = IndexConfig::kDocsPositions;
    in.doc_count = doc_count;
    // Force one DICT block per term so the sampled-term index covers every term
    // (its max_term == the lexicographically largest term).
    in.target_dict_block_bytes = 1;

    // low-df "apple" in a handful of docs.
    in.terms.push_back(MakeTerm("apple", {0, 5, 12, 20}, true));
    // mid-low "banana".
    in.terms.push_back(MakeTerm("banana", {1, 2, 3, 4, 9, 15}, true));
    // HIGH-df "common": df=600 (>=512) -> windowed pod_ref.
    std::vector<uint32_t> common_docs;
    for (uint32_t d = 0; d < 600; ++d) {
        common_docs.push_back(d);
    }
    in.terms.push_back(MakeTerm("common", common_docs, true));
    // low-df "zebra".
    in.terms.push_back(MakeTerm("zebra", {7, 21, 29}, true));
    return in;
}

// Locate a term through the full reader walk and return its DictEntry.
Status LocateEntry(const std::vector<uint8_t>& file, const PerIndexMetaReader& meta,
                   const std::string& term, bool* found, DictEntry* out) {
    std::vector<uint8_t> sti_scratch;
    Slice sti_frame;
    RETURN_IF_ERROR(meta.sampled_term_index_frame(&sti_scratch, &sti_frame));
    SampledTermIndexReader sti;
    RETURN_IF_ERROR(SampledTermIndexReader::open(sti_frame, &sti));
    std::vector<uint8_t> dbd_scratch;
    Slice dbd_frame;
    RETURN_IF_ERROR(meta.dict_block_directory_frame(&dbd_scratch, &dbd_frame));
    DictBlockDirectoryReader dbd;
    RETURN_IF_ERROR(DictBlockDirectoryReader::open(dbd_frame, &dbd));

    bool maybe = false;
    uint32_t ordinal = 0;
    RETURN_IF_ERROR(sti.locate(term, &maybe, &ordinal));
    if (!maybe) {
        *found = false;
        return Status::OK();
    }
    BlockRef ref {};
    RETURN_IF_ERROR(dbd.get(ordinal, &ref));
    Slice block(file.data() + ref.offset, ref.length);
    DictBlockReader br;
    RETURN_IF_ERROR(DictBlockReader::open(block, IndexTier::kT2, true, &br));
    return br.find_term(term, found, out);
}

} // namespace

// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST(SniiCompoundWriter, ReadBackSelfValidation) {
    const std::string path = TempPath();
    {
        io::LocalFileWriter w;
        ASSERT_TRUE(w.open(path).ok());
        SniiCompoundWriter cw(&w);
        ASSERT_TRUE(cw.add_logical_index(MakeIndex(10, "title", 30)).ok());
        ASSERT_TRUE(cw.add_logical_index(MakeIndex(11, "body", 30)).ok());
        ASSERT_TRUE(cw.finish().ok());
    }

    std::vector<uint8_t> file = ReadAll(path);
    ASSERT_GT(file.size(), kBootstrapHeaderSize + tail_pointer_size());

    // --- bootstrap header ---
    BootstrapHeader bh;
    ASSERT_TRUE(decode_bootstrap_header(Slice(file.data(), kBootstrapHeaderSize), &bh).ok());
    EXPECT_EQ(bh.magic, kContainerMagic);
    EXPECT_EQ(bh.format_version, kFormatVersion);
    EXPECT_EQ(bh.tail_pointer_size, static_cast<uint8_t>(tail_pointer_size()));

    // --- tail pointer (last tail_pointer_size() bytes) ---
    Slice tail_bytes(file.data() + file.size() - tail_pointer_size(), tail_pointer_size());
    TailPointer tp;
    ASSERT_TRUE(decode_tail_pointer(tail_bytes, &tp).ok());
    EXPECT_EQ(tp.hot_off, 0U);
    ASSERT_GT(tp.meta_region_length, 0U);
    ASSERT_LE(tp.meta_region_offset + tp.meta_region_length, file.size() - tail_pointer_size());

    // --- tail meta region ---
    Slice region(file.data() + tp.meta_region_offset, tp.meta_region_length);
    TailMetaRegionReader tmr;
    ASSERT_TRUE(TailMetaRegionReader::open(region, &tmr).ok());
    EXPECT_EQ(tmr.n_logical_indexes(), 2U);

    struct Expect {
        uint64_t id;
        std::string suffix;
    };
    std::vector<Expect> expects = {{.id = 10, .suffix = "title"}, {.id = 11, .suffix = "body"}};
    for (const auto& e : expects) {
        bool found = false;
        Slice meta_bytes;
        ASSERT_TRUE(tmr.find(e.id, e.suffix, &found, &meta_bytes).ok());
        ASSERT_TRUE(found) << "index " << e.id;

        PerIndexMetaReader meta;
        ASSERT_TRUE(PerIndexMetaReader::open(meta_bytes, &meta).ok());
        EXPECT_EQ(meta.index_id(), e.id);
        EXPECT_EQ(meta.index_suffix(), e.suffix);
        EXPECT_EQ(meta.stats().doc_count, 30U);
        EXPECT_EQ(meta.stats().term_count, 4U);

        const SectionRefs& refs = meta.section_refs();
        // posting_region / dict_region must be within file bounds. With the order flip
        // (posting region first, then DICT trailer), the posting region precedes the
        // DICT region: posting_off < dict_off and posting_off + posting_len == dict_off.
        ASSERT_GT(refs.posting_region.length, 0U);
        ASSERT_LE(refs.posting_region.offset + refs.posting_region.length, file.size());
        ASSERT_GT(refs.dict_region.length, 0U);
        ASSERT_LE(refs.dict_region.offset + refs.dict_region.length, file.size());
        EXPECT_LT(refs.posting_region.offset, refs.dict_region.offset);
        EXPECT_EQ(refs.posting_region.offset + refs.posting_region.length, refs.dict_region.offset);
        // norms absent for docs-positions (no scoring).
        EXPECT_EQ(refs.norms.offset, 0U);
        EXPECT_EQ(refs.norms.length, 0U);
        EXPECT_EQ(refs.null_bitmap.offset, 0U);
        EXPECT_EQ(refs.null_bitmap.length, 0U);

        // --- XFilter (block-split bloom, physical section): present true, absent false ---
        // Probe the on-disk filter directly: one 32-byte block at a self-computed offset.
        EXPECT_TRUE(meta.has_bsbf());
        ASSERT_GT(refs.bsbf.length, doris::snii::format::kBsbfHeaderSize);
        ASSERT_LE(refs.bsbf.offset + refs.bsbf.length, file.size());
        const uint64_t bsbf_bitset = refs.bsbf.offset + doris::snii::format::kBsbfHeaderSize;
        const auto bsbf_nblocks =
                static_cast<uint32_t>((refs.bsbf.length - doris::snii::format::kBsbfHeaderSize) /
                                      doris::snii::format::kBsbfBytesPerBlock);
        auto bsbf_present = [&](std::string_view term) {
            const uint64_t h = doris::snii::format::bsbf_hash(term);
            const uint64_t off =
                    bsbf_bitset +
                    static_cast<uint64_t>(doris::snii::format::bsbf_block_index(h, bsbf_nblocks)) *
                            doris::snii::format::kBsbfBytesPerBlock;
            return doris::snii::format::bsbf_block_contains(h, file.data() + off);
        };
        EXPECT_TRUE(bsbf_present("apple"));
        EXPECT_TRUE(bsbf_present("common"));
        EXPECT_FALSE(bsbf_present("nonexistent-term-xyzzy-12345"));

        // --- windowed high-df term "common": read its .frq window ---
        bool found_common = false;
        DictEntry common_entry;
        ASSERT_TRUE(LocateEntry(file, meta, "common", &found_common, &common_entry).ok());
        ASSERT_TRUE(found_common);
        EXPECT_EQ(common_entry.df, 600U);
        EXPECT_EQ(common_entry.kind, DictEntryKind::kPodRef);
        EXPECT_EQ(common_entry.enc, DictEntryEnc::kWindowed);

        // The DICT block carrying "common" supplies frq_base via DictBlockReader.
        // Recompute frq_base by re-locating the block.
        std::vector<uint8_t> sti_scratch;
        Slice sti_frame;
        ASSERT_TRUE(meta.sampled_term_index_frame(&sti_scratch, &sti_frame).ok());
        SampledTermIndexReader sti;
        ASSERT_TRUE(SampledTermIndexReader::open(sti_frame, &sti).ok());
        std::vector<uint8_t> dbd_scratch;
        Slice dbd_frame;
        ASSERT_TRUE(meta.dict_block_directory_frame(&dbd_scratch, &dbd_frame).ok());
        DictBlockDirectoryReader dbd;
        ASSERT_TRUE(DictBlockDirectoryReader::open(dbd_frame, &dbd).ok());
        bool maybe = false;
        uint32_t ord = 0;
        ASSERT_TRUE(sti.locate("common", &maybe, &ord).ok());
        ASSERT_TRUE(maybe);
        BlockRef bref {};
        ASSERT_TRUE(dbd.get(ord, &bref).ok());
        Slice block(file.data() + bref.offset, bref.length);
        DictBlockReader br;
        ASSERT_TRUE(DictBlockReader::open(block, IndexTier::kT2, true, &br).ok());

        // Absolute .frq offset = posting_region.offset + frq_base + frq_off_delta. The
        // windowed payload is [prelude][dd-block][freq-block]; parse the two-level
        // prelude, then decode every window's dd region from the dd-block (each with
        // its prelude win_base) to reconstruct the full posting (docs-only path).
        uint64_t frq_abs = refs.posting_region.offset + br.frq_base() + common_entry.frq_off_delta;
        Slice prelude_bytes(file.data() + frq_abs, common_entry.prelude_len);
        FrqPreludeReader prelude;
        ASSERT_TRUE(FrqPreludeReader::open(prelude_bytes, &prelude).ok());
        const uint64_t dd_block_start = frq_abs + common_entry.prelude_len;
        std::vector<uint32_t> got_docs;
        uint32_t summed = 0;
        for (uint32_t w = 0; w < prelude.n_windows(); ++w) {
            WindowMeta m;
            ASSERT_TRUE(prelude.window(w, &m).ok());
            Slice dd(file.data() + dd_block_start + m.dd_off, m.dd_disk_len);
            std::vector<uint32_t> wdocs;
            ASSERT_TRUE(decode_dd_region(dd, DdMetaOf(m), m.win_base, &wdocs).ok());
            ASSERT_EQ(wdocs.size(), m.doc_count);
            summed += m.doc_count;
            got_docs.insert(got_docs.end(), wdocs.begin(), wdocs.end());
        }
        EXPECT_EQ(summed, 600U); // window doc_counts sum to df.
        ASSERT_EQ(got_docs.size(), 600U);
        EXPECT_EQ(got_docs.front(), 0U);
        EXPECT_EQ(got_docs.back(), 599U);

        // --- inline low-df term "apple": decode its inline .frq bytes ---
        bool found_apple = false;
        DictEntry apple_entry;
        ASSERT_TRUE(LocateEntry(file, meta, "apple", &found_apple, &apple_entry).ok());
        ASSERT_TRUE(found_apple);
        EXPECT_EQ(apple_entry.df, 4U);
        EXPECT_EQ(apple_entry.kind, DictEntryKind::kInline);
        Slice apple_dd(apple_entry.frq_bytes.data(),
                       static_cast<size_t>(apple_entry.dd_meta.disk_len));
        std::vector<uint32_t> apple_docs;
        ASSERT_TRUE(decode_dd_region(apple_dd, apple_entry.dd_meta, 0, &apple_docs).ok());
        std::vector<uint32_t> expected_apple = {0, 5, 12, 20};
        EXPECT_EQ(apple_docs, expected_apple);

        // inline .prx bytes decode to per-doc positions.
        ByteSource psrc(Slice(apple_entry.prx_bytes));
        std::vector<std::vector<uint32_t>> apple_pos;
        ASSERT_TRUE(read_prx_window(&psrc, &apple_pos).ok());
        EXPECT_EQ(apple_pos.size(), 4U);

        // absent term -> not found through the dict walk.
        bool found_absent = true;
        DictEntry absent_entry;
        ASSERT_TRUE(LocateEntry(file, meta, "zzz-not-here", &found_absent, &absent_entry).ok());
        EXPECT_FALSE(found_absent);
    }

    std::remove(path.c_str());
}

namespace {

// Oracle for the multi-super-block read-back test. "hot" is a very high-df term;
// at df=70000 it uses adaptive 1024-doc windows (df >= kAdaptiveWindowDfThreshold)
// -> ~69 windows -> >1 super-block at group_size=64. "spark" appears at position
// 1 immediately after "hot" in docs where d % 9 == 0, so the phrase "hot spark"
// holds exactly in those docs. "rare" is a tiny low-df term.
struct BigCorpus {
    uint32_t doc_count = 70000;
    std::vector<uint32_t> hot_docs;   // every doc
    std::vector<uint32_t> spark_docs; // d % 9 == 0
    std::vector<uint32_t> rare_docs = {3, 17, 99, 1000, 19999};
    std::vector<uint32_t> phrase_oracle; // docs where "hot spark" is consecutive
};

BigCorpus MakeBigCorpus() {
    BigCorpus c;
    for (uint32_t d = 0; d < c.doc_count; ++d) {
        c.hot_docs.push_back(d);
        if (d % 9 == 0) {
            c.spark_docs.push_back(d);
            c.phrase_oracle.push_back(d); // "hot"@0 then "spark"@1 -> phrase hit
        }
    }
    return c;
}

// Builds a TermPostings with all freq=1 and a single position per doc.
TermPostings MakePosTerm(const std::string& term, const std::vector<uint32_t>& docs, uint32_t pos) {
    TermPostings tp;
    tp.term = term;
    tp.docids = docs;
    tp.freqs.assign(docs.size(), 1);
    tp.positions_flat.assign(docs.size(), pos); // one position per doc, flat
    return tp;
}

SniiIndexInput MakeBigIndex(const BigCorpus& c) {
    SniiIndexInput in;
    in.index_id = 1;
    in.index_suffix = "body";
    in.config = IndexConfig::kDocsPositions;
    in.doc_count = c.doc_count;
    // One DICT block per term so every term is a block first-term (covered by the
    // sampled-term index regardless of order), mirroring the other read tests.
    in.target_dict_block_bytes = 1;
    // Terms must be lexicographically sorted.
    in.terms.push_back(MakePosTerm("hot", c.hot_docs, /*pos=*/0));     // huge -> windowed
    in.terms.push_back(MakePosTerm("rare", c.rare_docs, /*pos=*/0));   // tiny -> inline
    in.terms.push_back(MakePosTerm("spark", c.spark_docs, /*pos=*/1)); // -> windowed
    return in;
}

} // namespace

// PHASE A read-back self-validation: a high-df term spanning MANY windows across
// MULTIPLE super-blocks. Asserts (a) sum of window doc_counts == df, (b) the
// windows tile the posting in order, (c) locate_window resolves the covering
// window, and (d) term_query / phrase_query agree with the oracle.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST(SniiCompoundWriter, MultiSuperBlockReadBack) {
    const BigCorpus c = MakeBigCorpus();
    const std::string path = TempPath();
    {
        io::LocalFileWriter w;
        ASSERT_TRUE(w.open(path).ok());
        SniiCompoundWriter cw(&w);
        ASSERT_TRUE(cw.add_logical_index(MakeBigIndex(c)).ok());
        ASSERT_TRUE(cw.finish().ok());
    }

    io::LocalFileReader local;
    ASSERT_TRUE(local.open(path).ok());
    io::MeteredFileReader metered(&local);
    reader::SniiSegmentReader seg;
    ASSERT_TRUE(reader::SniiSegmentReader::open(&metered, &seg).ok());
    reader::LogicalIndexReader idx;
    ASSERT_TRUE(seg.open_index(1, "body", &idx).ok());

    // Resolve "hot" to its DictEntry + the block's frq_base.
    bool found = false;
    DictEntry hot;
    uint64_t frq_base = 0, prx_base = 0;
    ASSERT_TRUE(idx.lookup("hot", &found, &hot, &frq_base, &prx_base).ok());
    ASSERT_TRUE(found);
    EXPECT_EQ(hot.df, c.doc_count);
    EXPECT_EQ(hot.enc, DictEntryEnc::kWindowed);
    EXPECT_TRUE(hot.has_sb);

    // L0 tiering: this index's bsbf filter is tiny (<= kBsbfResidentMaxBytes), so it is
    // loaded resident at open and an absent-term lookup is rejected IN MEMORY with zero
    // reads (no per-lookup round). Loop a few absents to skip the rare false positive.
    bool saw_resident_reject = false;
    for (int i = 0; i < 8 && !saw_resident_reject; ++i) {
        metered.reset_metrics();
        bool af = true;
        DictEntry ad;
        uint64_t afb = 0, apb = 0;
        ASSERT_TRUE(idx.lookup("absent-zzz-" + std::to_string(i), &af, &ad, &afb, &apb).ok());
        if (!af) {
            EXPECT_EQ(metered.metrics().read_at_calls, 0U);
            EXPECT_EQ(metered.metrics().serial_rounds, 0U);
            saw_resident_reject = true;
        }
    }
    EXPECT_TRUE(saw_resident_reject);
    metered.reset_metrics();

    // L1 tiering (P1 cold-read fix): force the bloom NON-resident by lowering the
    // resident threshold to 0, then re-open the SAME index through a reader that
    // counts every physical read touching the bsbf section. With the fix the bloom
    // is skipped ENTIRELY -- open does NOT read the 28B header and lookup does NOT
    // issue a 32B probe -- so ZERO reads touch the bsbf window, yet a present term
    // is still found and absent terms are still not-found, all via sti -> dict.
    const RegionRef bsbf_ref = idx.section_refs().bsbf;
    ASSERT_GT(bsbf_ref.length, kBsbfHeaderSize); // a real (small) filter exists on disk
    ::setenv("SNII_BSBF_RESIDENT_MAX", "0", /*overwrite=*/1);
    {
        io::LocalFileReader l1_local;
        ASSERT_TRUE(l1_local.open(path).ok());
        WindowTouchCountingReader counting(&l1_local, bsbf_ref.offset, bsbf_ref.length);
        reader::SniiSegmentReader seg_l1;
        ASSERT_TRUE(reader::SniiSegmentReader::open(&counting, &seg_l1).ok());
        reader::LogicalIndexReader idx_l1;
        ASSERT_TRUE(seg_l1.open_index(1, "body", &idx_l1).ok());
        // open() must not have read the bsbf header (28B) on the non-resident path.
        EXPECT_EQ(counting.window_touches(), 0U) << "non-resident open must skip the bsbf header";

        // Present term: still found via sti -> dict, with no bloom involved.
        bool pf = false;
        DictEntry pe;
        uint64_t pfb = 0, ppb = 0;
        ASSERT_TRUE(idx_l1.lookup("hot", &pf, &pe, &pfb, &ppb).ok());
        EXPECT_TRUE(pf);

        // Absent terms: not found via dict. "absent-zzz-*" sorts before every
        // sample (out-of-range sti reject); "rzz" sorts inside the term range so
        // sti routes it to a real dict block that then misses -- both return absent
        // and NEITHER probes the bloom.
        for (int i = 0; i < 8; ++i) {
            bool af = true;
            DictEntry ad;
            uint64_t afb = 0, apb = 0;
            ASSERT_TRUE(
                    idx_l1.lookup("absent-zzz-" + std::to_string(i), &af, &ad, &afb, &apb).ok());
            EXPECT_FALSE(af) << "out-of-range absent i=" << i;
        }
        bool rf = true;
        DictEntry rd;
        uint64_t rfb = 0, rpb = 0;
        ASSERT_TRUE(idx_l1.lookup("rzz", &rf, &rd, &rfb, &rpb).ok());
        EXPECT_FALSE(rf) << "in-range absent term must miss in the dict";

        // No lookup may have probed the bsbf section: the bloom is skipped, not
        // read on demand. This is the core of the P1 cold-read fix.
        EXPECT_EQ(counting.window_touches(), 0U)
                << "non-resident lookups must not probe the bsbf section";
    }
    ::unsetenv("SNII_BSBF_RESIDENT_MAX");
    metered.reset_metrics();

    // Fetch + parse the two-level prelude.
    const uint64_t prelude_abs =
            idx.section_refs().posting_region.offset + frq_base + hot.frq_off_delta;
    std::vector<uint8_t> prelude_bytes;
    ASSERT_TRUE(local.read_at(prelude_abs, hot.prelude_len, &prelude_bytes).ok());
    FrqPreludeReader prelude;
    ASSERT_TRUE(FrqPreludeReader::open(Slice(prelude_bytes), &prelude).ok());
    EXPECT_GT(prelude.n_super_blocks(), 1U) << "expected >1 super-block";

    // (a)+(b): decode every window's dd region from the dd-block; doc_counts sum to
    // df and the concatenated docids equal the full ascending posting [0, doc_count).
    const uint64_t dd_block_start = prelude_abs + hot.prelude_len;
    std::vector<uint32_t> tiled;
    uint64_t summed = 0;
    uint64_t expect_win_base = 0;
    for (uint32_t w = 0; w < prelude.n_windows(); ++w) {
        WindowMeta m;
        ASSERT_TRUE(prelude.window(w, &m).ok());
        EXPECT_EQ(m.win_base, expect_win_base) << "win_base w=" << w;
        std::vector<uint8_t> ddbytes;
        ASSERT_TRUE(local.read_at(dd_block_start + m.dd_off, m.dd_disk_len, &ddbytes).ok());
        std::vector<uint32_t> wdocs;
        ASSERT_TRUE(decode_dd_region(Slice(ddbytes), DdMetaOf(m), m.win_base, &wdocs).ok());
        ASSERT_EQ(wdocs.size(), m.doc_count) << "w=" << w;
        summed += m.doc_count;
        expect_win_base = m.last_docid;
        tiled.insert(tiled.end(), wdocs.begin(), wdocs.end());
    }
    EXPECT_EQ(summed, c.doc_count);
    ASSERT_EQ(tiled.size(), c.doc_count);
    EXPECT_EQ(tiled, c.hot_docs);

    // (c): locate_window returns the window actually containing the docid.
    const uint32_t probes[] = {0, 255, 256, 257, 5000, 16383, 16384, 19999};
    for (uint32_t docid : probes) {
        bool lfound = false;
        uint32_t w = 0;
        ASSERT_TRUE(prelude.locate_window(docid, &lfound, &w).ok());
        ASSERT_TRUE(lfound) << "docid=" << docid;
        WindowMeta m;
        ASSERT_TRUE(prelude.window(w, &m).ok());
        EXPECT_GE(static_cast<uint64_t>(docid), m.win_base + (w == 0 ? 0 : 1)) << "docid=" << docid;
        EXPECT_LE(docid, m.last_docid) << "docid=" << docid;
    }
    // past-end -> not found.
    {
        bool lfound = true;
        uint32_t w = 0;
        ASSERT_TRUE(prelude.locate_window(c.doc_count, &lfound, &w).ok());
        EXPECT_FALSE(lfound);
    }

    // (d): term_query / phrase_query agree with the oracle (full-read path).
    std::vector<uint32_t> hot_docs;
    ASSERT_TRUE(query::term_query(idx, "hot", &hot_docs).ok());
    EXPECT_EQ(hot_docs, c.hot_docs);

    std::vector<uint32_t> spark_docs;
    ASSERT_TRUE(query::term_query(idx, "spark", &spark_docs).ok());
    EXPECT_EQ(spark_docs, c.spark_docs);

    std::vector<uint32_t> rare_docs;
    ASSERT_TRUE(query::term_query(idx, "rare", &rare_docs).ok());
    EXPECT_EQ(rare_docs, c.rare_docs);

    std::vector<uint32_t> phrase_docs;
    ASSERT_TRUE(query::phrase_query(idx, {"hot", "spark"}, &phrase_docs).ok());
    EXPECT_EQ(phrase_docs, c.phrase_oracle);

    // reversed phrase must be absent ("spark"@1 never precedes "hot"@0).
    std::vector<uint32_t> reversed;
    ASSERT_TRUE(query::phrase_query(idx, {"spark", "hot"}, &reversed).ok());
    EXPECT_TRUE(reversed.empty());

    std::remove(path.c_str());
}

// BYTE-IDENTICAL OUTPUT GUARANTEE: the section bytes (DICT region, .frq POD,
// .prx POD) now stream through scratch temp files instead of in-RAM vectors, and
// the xfilter is built from per-term hashes instead of retained strings. The
// produced container must be deterministic and byte-for-byte stable: building the
// same input twice yields identical container bytes (proves the temp-file path
// reproduces the exact same layout/offsets/content as itself, with no in-RAM
// residue affecting the bytes). The big multi-super-block index exercises every
// section type (windowed pod_ref + inline + multi-block dict + prx).
TEST(SniiCompoundWriter, StreamedOutputIsDeterministicByteForByte) {
    const BigCorpus c = MakeBigCorpus();

    auto build_one = [&](const std::string& path) {
        io::LocalFileWriter w;
        ASSERT_TRUE(w.open(path).ok());
        SniiCompoundWriter cw(&w);
        ASSERT_TRUE(cw.add_logical_index(MakeBigIndex(c)).ok());
        ASSERT_TRUE(cw.finish().ok());
    };

    const std::string path_a = TempPath();
    const std::string path_b = TempPath();
    build_one(path_a);
    build_one(path_b);

    const std::vector<uint8_t> file_a = ReadAll(path_a);
    const std::vector<uint8_t> file_b = ReadAll(path_b);
    ASSERT_FALSE(file_a.empty());
    EXPECT_EQ(file_a.size(), file_b.size());
    EXPECT_EQ(file_a, file_b) << "streamed container must be byte-identical run-to-run";

    std::remove(path_a.c_str());
    std::remove(path_b.c_str());
}

// Determinism for a multi-index docs-positions container with multiple DICT
// blocks per index (target_dict_block_bytes forces real block cuts, exercising
// the dict scratch-file concat + the BlockRecord rel_offset directory math).
TEST(SniiCompoundWriter, StreamedMultiIndexDeterministic) {
    auto build_one = [&](const std::string& path) {
        io::LocalFileWriter w;
        ASSERT_TRUE(w.open(path).ok());
        SniiCompoundWriter cw(&w);
        SniiIndexInput a = MakeIndex(10, "title", 30);
        SniiIndexInput b = MakeIndex(11, "body", 30);
        a.target_dict_block_bytes = 64; // force multiple dict blocks
        b.target_dict_block_bytes = 64;
        ASSERT_TRUE(cw.add_logical_index(a).ok());
        ASSERT_TRUE(cw.add_logical_index(b).ok());
        ASSERT_TRUE(cw.finish().ok());
    };
    const std::string p1 = TempPath();
    const std::string p2 = TempPath();
    build_one(p1);
    build_one(p2);
    EXPECT_EQ(ReadAll(p1), ReadAll(p2));
    std::remove(p1.c_str());
    std::remove(p2.c_str());
}

// A flipped on-disk byte in the bsbf filter (header or bitset) or in the meta region
// must be detected on the read path, not silently propagated as a wrong probe result.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST(SniiCompoundWriter, ChecksumCorruptionDetectedOnRead) {
    const std::string good = TempPath();
    {
        io::LocalFileWriter w;
        ASSERT_TRUE(w.open(good).ok());
        SniiCompoundWriter cw(&w);
        ASSERT_TRUE(cw.add_logical_index(MakeIndex(7, "body", 30)).ok());
        ASSERT_TRUE(cw.finish().ok());
    }
    const std::vector<uint8_t> file = ReadAll(good);
    std::remove(good.c_str());

    // Locate the bsbf section + meta region from the tail.
    TailPointer tp;
    ASSERT_TRUE(decode_tail_pointer(
                        Slice(file.data() + file.size() - tail_pointer_size(), tail_pointer_size()),
                        &tp)
                        .ok());
    TailMetaRegionReader tmr;
    ASSERT_TRUE(TailMetaRegionReader::open(
                        Slice(file.data() + tp.meta_region_offset, tp.meta_region_length), &tmr)
                        .ok());
    bool found = false;
    Slice meta_bytes;
    ASSERT_TRUE(tmr.find(7, "body", &found, &meta_bytes).ok());
    ASSERT_TRUE(found);
    PerIndexMetaReader meta;
    ASSERT_TRUE(PerIndexMetaReader::open(meta_bytes, &meta).ok());
    const auto bsbf = meta.section_refs().bsbf;
    ASSERT_GT(bsbf.length, doris::snii::format::kBsbfHeaderSize); // small filter -> L0

    // The clean file opens fine.
    auto opens_ok = [](const std::vector<uint8_t>& bytes) {
        const std::string p = WriteTemp(bytes);
        io::LocalFileReader r;
        EXPECT_TRUE(r.open(p).ok());
        reader::SniiSegmentReader seg;
        Status sopen = reader::SniiSegmentReader::open(&r, &seg);
        bool ok = sopen.ok();
        if (ok) {
            reader::LogicalIndexReader idx;
            ok = seg.open_index(7, "body", &idx).ok();
        }
        std::remove(p.c_str());
        return ok;
    };
    EXPECT_TRUE(opens_ok(file));

    // (1) bsbf BITSET byte flip -> L0 open verifies the bitset crc -> rejected.
    {
        std::vector<uint8_t> bad = file;
        bad[bsbf.offset + doris::snii::format::kBsbfHeaderSize] ^= 0xFF;
        EXPECT_FALSE(opens_ok(bad));
    }
    // (2) bsbf HEADER byte flip (num_bytes field, covered by header crc) -> rejected.
    {
        std::vector<uint8_t> bad = file;
        bad[bsbf.offset + 8] ^= 0xFF;
        EXPECT_FALSE(opens_ok(bad));
    }
    // (3) META REGION byte flip -> segment open verifies meta_region_checksum -> rejected.
    {
        std::vector<uint8_t> bad = file;
        bad[tp.meta_region_offset] ^= 0xFF;
        EXPECT_FALSE(opens_ok(bad));
    }
}
