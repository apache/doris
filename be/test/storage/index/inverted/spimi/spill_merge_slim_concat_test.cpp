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

// 批 4（k>1 slim verbatim 字节拼接快路径）测试网：
//   1) CrossPathByteIdentityAndDispatch —— 拼接(默认) vs 强制平面化重编 vs
//      直写单 flush，三方四流逐字节相同；并断言 dispatch 计数：fast run 的
//      slim_concat_terms 精确等于输出中 Σdf<512 的 term 数（快路径落地前恒
//      0 → RED），强制重编 run 恒 0。顺带打印 verbatim 命中率（交付物）。
//   2) SlimBoundaryStraddleUpgradeDispatch —— Σdf=511/512/513 跨 spill：
//      511 走拼接（仍 slim），512/513 走重编（升级 windowed），.tis 标志与
//      外置块首字节复核（R2 的批 4 计数版）。
//   3) PrxZstdBoundaryEnvelope —— slim .prx ZSTD 边界两个方向：输入 raw、
//      合并 payload 跨 512B 门（合并块对拼接 payload 重跑 ZSTD 决策）；输入
//      已 ZSTD（先 inflate 再拼接）。信封字节显式复核 + 字节金标准。
//   4) StreamingCursorSmallBufferByteIdentity —— 4KB 滑窗文件游标下拼接扫描
//      跨 Refill 边界，输出仍与直写逐字节相同（生产 EmitMerged 形态）。
//
// 构造遵循生产合约（spill_segment_merger_test.cpp:1404-1421 注释）：绝对
// doc_id、k 份连续 doc 区间、cumulative doc_count、SpillManager::FlushBuffer
// 真落盘。

#include <gtest/gtest.h>

#include <cstdint>
#include <cstdio>
#include <iostream>
#include <string>
#include <vector>

#include "common/config.h"
#include "storage/index/inverted/spimi/byte_output.h"
#include "storage/index/inverted/spimi/field_infos_writer.h"
#include "storage/index/inverted/spimi/freq_prox_encoder.h"
#include "storage/index/inverted/spimi/fulltext_writer.h"
#include "storage/index/inverted/spimi/posting_buffer.h"
#include "storage/index/inverted/spimi/segment_merger.h"
#include "storage/index/inverted/spimi/spill_manager.h"
#include "storage/index/inverted/spimi/term_dict_writer.h"
#include "storage/index/inverted/spimi/term_enum.h"

namespace doris::segment_v2::inverted_index::spimi {

namespace {

// 钉死所有影响输出字节的 config（RAII 还原）：生产默认 frq_zstd OFF /
// prx_zstd ON / zstd_min=512 / prx_win=1024（与金标准/RED 测试同口径）。
struct ScopedSpimiConfigPin {
    int64_t saved_zstd_min = config::inverted_index_spimi_zstd_min_window_bytes;
    bool saved_frq = config::inverted_index_spimi_frq_zstd_enable;
    bool saved_prx = config::inverted_index_spimi_prx_zstd_enable;
    int64_t saved_prx_win = config::inverted_index_spimi_prx_window_docs;

    ScopedSpimiConfigPin() {
        config::inverted_index_spimi_zstd_min_window_bytes = 512;
        config::inverted_index_spimi_frq_zstd_enable = false;
        config::inverted_index_spimi_prx_zstd_enable = true;
        config::inverted_index_spimi_prx_window_docs = 1024;
    }
    ~ScopedSpimiConfigPin() {
        config::inverted_index_spimi_zstd_min_window_bytes = saved_zstd_min;
        config::inverted_index_spimi_frq_zstd_enable = saved_frq;
        config::inverted_index_spimi_prx_zstd_enable = saved_prx;
        config::inverted_index_spimi_prx_window_docs = saved_prx_win;
    }
};

// 测试钩子 RAII：强制 slim term 走平面化重编，析构必还原（断言失败也不泄漏）。
struct ScopedForceSlimReencode {
    ScopedForceSlimReencode() { SegmentMerger::SetForceSlimReencodeForTest(true); }
    ~ScopedForceSlimReencode() { SegmentMerger::SetForceSlimReencodeForTest(false); }
};

struct FourStreams {
    std::vector<uint8_t> tis;
    std::vector<uint8_t> tii;
    std::vector<uint8_t> frq;
    std::vector<uint8_t> prx;
};

void ExpectStreamsEqual(const FourStreams& a, const FourStreams& b, const std::string& label) {
    EXPECT_EQ(a.tis, b.tis) << label << " .tis bytes diverged";
    EXPECT_EQ(a.tii, b.tii) << label << " .tii bytes diverged";
    EXPECT_EQ(a.frq, b.frq) << label << " .frq bytes diverged";
    EXPECT_EQ(a.prx, b.prx) << label << " .prx bytes diverged";
}

// 乘法散列抖动（杜绝周期收敛，保留真实熵）。
inline uint32_t Jitter(int64_t a, int64_t b, uint32_t mod) {
    return static_cast<uint32_t>((static_cast<uint64_t>(a) * 2654435761ULL + b * 97) % mod);
}

// 批 4 测试矩阵（绝对 doc_id 的确定性词表；gen(lo,hi,buf) 只灌 [lo,hi) 区间）：
//   tiny_000..119  df∈{1,2,3}     —— inline 小 term 海（k'∈{1,2} 个 run）
//   s511           Σdf=511        —— 跨 spill 仍 slim（拼接路径上界）
//   u512 / u513    Σdf=512/513    —— 跨 spill 升级 windowed（重编路径）
//   f1             df=64 全 freq=1 —— docCode 低位恒 1（无后随 freq VInt）
//   zmerge         df=128 payload 4B/doc —— 每输入 raw < 512B，合并 = 512B
//                  跨 ZSTD 门（合并块重跑决策）
//   zin            df=320 payload 8B/doc —— k=2 时每输入 ≥512B 已 ZSTD
//                  （inflate 后拼接再统一压缩）
constexpr int32_t kTotalDocs = 38'900;

void GenVocab(int32_t lo, int32_t hi, SpimiPostingBuffer& buf) {
    auto in_range = [&](int32_t doc) { return doc >= lo && doc < hi; };
    // tiny 小 term 海。
    char name[24];
    for (int n = 0; n < 120; ++n) {
        std::snprintf(name, sizeof(name), "tiny_%03d", n);
        const int df = 1 + (n % 3);
        const int32_t base = static_cast<int32_t>((static_cast<uint32_t>(n) * 331U) % 38'000U);
        for (int d = 0; d < df; ++d) {
            const int32_t doc = base + d * 97;
            if (!in_range(doc)) {
                continue;
            }
            const int freq = 1 + ((n + d) % 2);
            uint32_t p = static_cast<uint32_t>(d % 3);
            for (int j = 0; j < freq; ++j) {
                buf.Append(name, static_cast<uint32_t>(doc), p);
                p += 1 + Jitter(n, d * 7 + j, 5);
            }
        }
    }
    // 跨界三连。
    struct Straddle {
        const char* term;
        int32_t df;
        int32_t stride;
        int32_t phase;
    };
    static constexpr Straddle kStraddle[] = {
            {"s511", 511, 76, 0},
            {"u512", 512, 76, 1},
            {"u513", 513, 75, 2},
    };
    for (const auto& s : kStraddle) {
        for (int32_t i = 0; i < s.df; ++i) {
            const int32_t doc = i * s.stride + s.phase;
            if (!in_range(doc)) {
                continue;
            }
            const int freq = 1 + (i % 3);
            uint32_t p = static_cast<uint32_t>(i % 4);
            for (int j = 0; j < freq; ++j) {
                buf.Append(s.term, static_cast<uint32_t>(doc), p);
                p += 1 + Jitter(i, j, 7);
            }
        }
    }
    // f1：全 freq=1（docCode 低位恒 1）。
    for (int32_t i = 0; i < 64; ++i) {
        const int32_t doc = i * 600 + 3;
        if (in_range(doc)) {
            buf.Append("f1", static_cast<uint32_t>(doc), static_cast<uint32_t>(i % 5));
        }
    }
    // zmerge：position delta 恒 200（VInt 2B）×2 occurrence = 4B/doc。
    for (int32_t i = 0; i < 128; ++i) {
        const int32_t doc = i * 300 + 4;
        if (in_range(doc)) {
            buf.Append("zmerge", static_cast<uint32_t>(doc), 200);
            buf.Append("zmerge", static_cast<uint32_t>(doc), 400);
        }
    }
    // zin：4 occurrence × 2B = 8B/doc。
    for (int32_t i = 0; i < 320; ++i) {
        const int32_t doc = i * 120 + 5;
        if (in_range(doc)) {
            for (int j = 1; j <= 4; ++j) {
                buf.Append("zin", static_cast<uint32_t>(doc), static_cast<uint32_t>(j) * 200U);
            }
        }
    }
}

// 直写单 flush（无 spill）：与 SpimiIndexWriter::EmitDirect 同参（V4、
// omit_norms=true、inline_small_terms=true）。
FourStreams EmitDirect(bool omit) {
    SpimiPostingBuffer buf(omit);
    GenVocab(0, kTotalDocs, buf);
    MemoryByteOutput tis, tii, frq, prx, fnm, nrm, seg_n, seg_gen;
    SpimiSegmentSink sink {.tis = &tis,
                           .tii = &tii,
                           .frq = &frq,
                           .prx = &prx,
                           .fnm = &fnm,
                           .nrm = &nrm,
                           .segments_n = &seg_n,
                           .segments_gen = &seg_gen};
    SpimiFulltextWriter::EmitSegment(buf, sink, "_0", "content", kTotalDocs,
                                     FieldInfosWriter::kIndexVersionV4, omit,
                                     /*omit_norms=*/true, /*out_byte_counts=*/nullptr,
                                     /*inline_small_terms=*/true);
    return {tis.bytes(), tii.bytes(), frq.bytes(), prx.bytes()};
}

std::vector<int32_t> EvenCuts(int k) {
    std::vector<int32_t> cuts;
    for (int j = 0; j <= k; ++j) {
        cuts.push_back(static_cast<int32_t>(static_cast<int64_t>(kTotalDocs) * j / k));
    }
    return cuts;
}

// k 份连续 doc 区间 → k 个 spill（FlushBuffer 真落盘；SpillManager 不可
// 拷贝/移动，调用方构造后引用填充）。
void FillSpills(SpillManager& mgr, const std::vector<int32_t>& cuts, bool omit) {
    SpimiPostingBuffer buf(omit);
    for (size_t j = 0; j + 1 < cuts.size(); ++j) {
        GenVocab(cuts[j], cuts[j + 1], buf);
        EXPECT_GT(buf.RecordCount(), 0) << "spill " << j << " must not be empty";
        mgr.FlushBuffer(buf, /*doc_count=*/cuts[j + 1]); // Reset()s the buffer
    }
    EXPECT_EQ(mgr.SpillCount(), cuts.size() - 1);
}

// LoadSpill（内存输入）路径归并。
FourStreams MergeLoaded(SpillManager& mgr, bool omit) {
    std::vector<SegmentMerger::Input> inputs;
    inputs.reserve(mgr.SpillCount());
    for (size_t i = 0; i < mgr.SpillCount(); ++i) {
        SegmentMerger::Input in;
        EXPECT_TRUE(mgr.LoadSpill(i, in).ok());
        inputs.push_back(std::move(in));
    }
    MemoryByteOutput tis, tii, frq, prx, fnm, nrm, seg_n, seg_gen;
    SpimiSegmentSink sink {.tis = &tis,
                           .tii = &tii,
                           .frq = &frq,
                           .prx = &prx,
                           .fnm = &fnm,
                           .nrm = &nrm,
                           .segments_n = &seg_n,
                           .segments_gen = &seg_gen};
    SegmentMerger::Merge(inputs, sink, "_0", "content", kTotalDocs,
                         FieldInfosWriter::kIndexVersionV4, omit, /*omit_norms=*/true);
    return {tis.bytes(), tii.bytes(), frq.bytes(), prx.bytes()};
}

// OpenSpillCursor（文件滑窗游标）路径归并 —— 生产 EmitMerged 形态。
FourStreams MergeStreaming(SpillManager& mgr, bool omit, size_t cursor_buffer_bytes) {
    std::vector<SegmentMerger::StreamInput> inputs;
    inputs.reserve(mgr.SpillCount());
    for (size_t i = 0; i < mgr.SpillCount(); ++i) {
        SegmentMerger::StreamInput in;
        EXPECT_TRUE(mgr.OpenSpillCursor(i, in, cursor_buffer_bytes).ok());
        inputs.push_back(std::move(in));
    }
    MemoryByteOutput tis, tii, frq, prx, fnm, nrm, seg_n, seg_gen;
    SpimiSegmentSink sink {.tis = &tis,
                           .tii = &tii,
                           .frq = &frq,
                           .prx = &prx,
                           .fnm = &fnm,
                           .nrm = &nrm,
                           .segments_n = &seg_n,
                           .segments_gen = &seg_gen};
    SegmentMerger::Merge(inputs, sink, "_0", "content", kTotalDocs,
                         FieldInfosWriter::kIndexVersionV4, omit, /*omit_norms=*/true);
    return {tis.bytes(), tii.bytes(), frq.bytes(), prx.bytes()};
}

std::vector<TermEntry> ReadAllTerms(const std::vector<uint8_t>& tis_bytes) {
    std::vector<TermEntry> entries;
    TermEnum en(tis_bytes);
    while (en.Next()) {
        entries.push_back(en.Current());
    }
    return entries;
}

// 输出 .tis 中 Σdf < skip_interval 的 term 数（= 拼接快路径应命中的 term 数；
// dispatch 纯由 Σdf 决定）。
int64_t CountSlimTerms(const std::vector<TermEntry>& terms) {
    int64_t n = 0;
    for (const TermEntry& e : terms) {
        if (e.info.doc_freq < TermDictWriter::kDefaultSkipInterval) {
            ++n;
        }
    }
    return n;
}

} // namespace

// 1) 交叉字节断言 + dispatch 计数 + 命中率统计。
TEST(SpimiSlimConcat, CrossPathByteIdentityAndDispatch) {
    ScopedSpimiConfigPin pin;
    for (const bool omit : {false, true}) {
        const FourStreams direct = EmitDirect(omit);
        for (const int k : {2, 3, 7}) {
            const std::string label = "omit=" + std::to_string(omit) + " k=" + std::to_string(k);
            // 默认（拼接快路径）。
            SegmentMerger::ResetStats();
            SpillManager mgr_fast("content", /*is_v4=*/true, /*tmp_dir=*/"",
                                  /*omit_term_freq_and_positions=*/omit);
            FillSpills(mgr_fast, EvenCuts(k), omit);
            const FourStreams fast = MergeLoaded(mgr_fast, omit);
            const SegmentMerger::MergeStats fast_stats = SegmentMerger::Stats();
            // 强制平面化重编（慢路径），同一数据再归并一次。
            FourStreams slow;
            SegmentMerger::MergeStats slow_stats;
            {
                ScopedForceSlimReencode force;
                SegmentMerger::ResetStats();
                SpillManager mgr_slow("content", /*is_v4=*/true, /*tmp_dir=*/"",
                                      /*omit_term_freq_and_positions=*/omit);
                FillSpills(mgr_slow, EvenCuts(k), omit);
                slow = MergeLoaded(mgr_slow, omit);
                slow_stats = SegmentMerger::Stats();
            }

            // 三方逐字节：拼接 == 重编 == 直写。
            ExpectStreamsEqual(direct, fast, label + " concat-vs-direct");
            ExpectStreamsEqual(slow, fast, label + " concat-vs-reencode");

            // dispatch 计数：拼接 run 必须把全部 slim term（Σdf<512）走快
            // 路径，其余走重编；强制 run 全部走重编。快路径落地前
            // slim_concat_terms 恒 0 —— 本断言即批 4 的 RED 线。
            const auto terms = ReadAllTerms(fast.tis);
            const auto total_terms = static_cast<int64_t>(terms.size());
            const int64_t slim_terms = CountSlimTerms(terms);
            EXPECT_GE(slim_terms, 100) << label << " test matrix lost its slim majority";
            EXPECT_EQ(fast_stats.slim_concat_terms, slim_terms)
                    << label << " slim terms must take the verbatim concat fast path";
            EXPECT_EQ(fast_stats.reencode_terms, total_terms - slim_terms)
                    << label << " only Σdf>=512 terms may take the re-encode path";
            EXPECT_EQ(slow_stats.slim_concat_terms, 0)
                    << label << " forced re-encode must bypass the concat path";
            EXPECT_EQ(slow_stats.reencode_terms, total_terms);

            // verbatim 命中率（交付统计）。
            std::cout << "[HITRATE] " << label << " slim_concat=" << fast_stats.slim_concat_terms
                      << "/" << total_terms << " ("
                      << (100.0 * static_cast<double>(fast_stats.slim_concat_terms) /
                          static_cast<double>(total_terms))
                      << "%)" << std::endl;
        }
    }
}

// 2) Σdf 跨 spill 升级判定（R2 的批 4 计数版）：511 拼接保 slim，512/513
//    重编升级 windowed。
TEST(SpimiSlimConcat, SlimBoundaryStraddleUpgradeDispatch) {
    ScopedSpimiConfigPin pin;
    SegmentMerger::ResetStats();
    SpillManager mgr("content", /*is_v4=*/true, /*tmp_dir=*/"",
                     /*omit_term_freq_and_positions=*/false);
    FillSpills(mgr, EvenCuts(2), /*omit=*/false);
    const FourStreams merged = MergeLoaded(mgr, /*omit=*/false);
    const SegmentMerger::MergeStats stats = SegmentMerger::Stats();

    const auto terms = ReadAllTerms(merged.tis);
    int checked = 0;
    for (const TermEntry& e : terms) {
        if (e.term_utf8 == "s511") {
            EXPECT_EQ(e.info.doc_freq, 511);
            EXPECT_TRUE(e.info.is_slim) << "Σdf=511 must stay slim after the concat merge";
            ++checked;
        } else if (e.term_utf8 == "u512" || e.term_utf8 == "u513") {
            EXPECT_EQ(e.info.doc_freq, e.term_utf8 == "u512" ? 512 : 513);
            EXPECT_FALSE(e.info.is_slim)
                    << e.term_utf8 << " Σdf>=512 must upgrade to windowed after the merge";
            if (!e.info.inlined) {
                ASSERT_LT(static_cast<size_t>(e.info.freq_pointer), merged.frq.size());
                EXPECT_EQ(merged.frq[static_cast<size_t>(e.info.freq_pointer)],
                          FreqProxEncoder::kCodeModeSpimiWindowed);
            }
            ++checked;
        }
    }
    EXPECT_EQ(checked, 3);
    // 升级 term 必须走重编（slim 拼接不得越过 Σdf 门）。
    EXPECT_EQ(stats.reencode_terms, 2) << "exactly u512/u513 take the re-encode path";
    EXPECT_EQ(stats.slim_concat_terms, static_cast<int64_t>(terms.size()) - 2);
}

// 3) slim .prx ZSTD 边界：输入信封与合并信封显式复核（字节金标准由用例 1
//    背书，这里证明矩阵确实跨过了 512B 门，而不是恒 raw / 恒 ZSTD）。
TEST(SpimiSlimConcat, PrxZstdBoundaryEnvelope) {
    ScopedSpimiConfigPin pin;
    SpillManager mgr("content", /*is_v4=*/true, /*tmp_dir=*/"",
                     /*omit_term_freq_and_positions=*/false);
    FillSpills(mgr, EvenCuts(2), /*omit=*/false);

    // 输入侧：zmerge 每输入 raw payload 256B < 512 → kProxRaw；zin 每输入
    // payload 1280B ≥ 512（高度可压）→ kProxZstd。
    int checked_inputs = 0;
    for (size_t i = 0; i < mgr.SpillCount(); ++i) {
        SegmentMerger::Input in;
        ASSERT_TRUE(mgr.LoadSpill(i, in).ok());
        for (const TermEntry& e : ReadAllTerms(in.tis_bytes)) {
            if (e.term_utf8 != "zmerge" && e.term_utf8 != "zin") {
                continue;
            }
            const uint8_t want = e.term_utf8 == "zmerge" ? FreqProxEncoder::kProxRaw
                                                         : FreqProxEncoder::kProxZstd;
            uint8_t got = 0;
            if (e.info.inlined) {
                ASSERT_GT(e.info.inline_prx_len, 0U);
                got = e.info.inline_prx[0];
            } else {
                ASSERT_LT(static_cast<size_t>(e.info.prox_pointer), in.prx_bytes.size());
                got = in.prx_bytes[static_cast<size_t>(e.info.prox_pointer)];
            }
            EXPECT_EQ(got, want) << "spill " << i << " " << e.term_utf8
                                 << " input .prx envelope drifted — the boundary test matrix "
                                    "no longer crosses the ZSTD gate as designed";
            ++checked_inputs;
        }
    }
    EXPECT_EQ(checked_inputs, 4) << "zmerge/zin must appear in both spills";

    // 合并侧：两个 term 的合并 payload（512B / 2560B，均高度可压）都必须对
    // 拼接后的整块重跑 ZSTD 决策 → kProxZstd。
    SegmentMerger::ResetStats();
    const FourStreams merged = MergeLoaded(mgr, /*omit=*/false);
    const SegmentMerger::MergeStats stats = SegmentMerger::Stats();
    int checked_merged = 0;
    for (const TermEntry& e : ReadAllTerms(merged.tis)) {
        if (e.term_utf8 != "zmerge" && e.term_utf8 != "zin") {
            continue;
        }
        EXPECT_TRUE(e.info.is_slim);
        uint8_t got = 0;
        if (e.info.inlined) {
            ASSERT_GT(e.info.inline_prx_len, 0U);
            got = e.info.inline_prx[0];
        } else {
            ASSERT_LT(static_cast<size_t>(e.info.prox_pointer), merged.prx.size());
            got = merged.prx[static_cast<size_t>(e.info.prox_pointer)];
        }
        EXPECT_EQ(got, FreqProxEncoder::kProxZstd)
                << e.term_utf8
                << " merged slim payload crossed the 512B gate and must be "
                   "ZSTD-compressed by the re-run policy";
        ++checked_merged;
    }
    EXPECT_EQ(checked_merged, 2);
    EXPECT_GT(stats.slim_concat_terms, 0) << "boundary terms must ride the concat fast path";
}

// 4) 4KB 滑窗文件游标（生产形态）：拼接扫描跨 Refill 边界仍逐字节正确。
TEST(SpimiSlimConcat, StreamingCursorSmallBufferByteIdentity) {
    ScopedSpimiConfigPin pin;
    for (const bool omit : {false, true}) {
        const FourStreams direct = EmitDirect(omit);
        for (const int k : {2, 7}) {
            const std::string label =
                    "cursor omit=" + std::to_string(omit) + " k=" + std::to_string(k);
            SegmentMerger::ResetStats();
            SpillManager mgr("content", /*is_v4=*/true, /*tmp_dir=*/"",
                             /*omit_term_freq_and_positions=*/omit);
            FillSpills(mgr, EvenCuts(k), omit);
            const FourStreams merged = MergeStreaming(mgr, omit, /*cursor_buffer_bytes=*/4096);
            ExpectStreamsEqual(direct, merged, label);
            const SegmentMerger::MergeStats stats = SegmentMerger::Stats();
            const auto terms = ReadAllTerms(merged.tis);
            EXPECT_EQ(stats.slim_concat_terms, CountSlimTerms(terms))
                    << label << " fast path must also engage over streaming file cursors";
        }
    }
}

} // namespace doris::segment_v2::inverted_index::spimi
