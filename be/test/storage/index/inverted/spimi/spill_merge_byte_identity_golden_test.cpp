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

// G1_MergeByteIdentityGolden — spill 归并输出的字节金标准。
//
// 对一个固定 term 矩阵（slim 511/512/513 跨界、windowed 多窗、PFOR 常数块、
// DOCS_ONLY omit、phrase 重 positions、inline 小 term 混合）按生产合约
// （绝对 doc_id、k 份连续 doc 区间、SpillManager::FlushBuffer 真落盘）构造
// k∈{1,2,7} 个 spill，跑 SegmentMerger::Merge，对输出 .tis/.tii/.frq/.prx
// 四流做 FNV-1a 链式 digest 并以字面量 pin（机制同 window_frame_encoder_test
// 的 ByteIdentityGolden）。digest 在归并实现改造前的 HEAD 上采集；后续
// 流式拼接/平面化重编批次改归并实现时这些值必须保持不变——任何 1 字节
// 漂移即红。同时以 MergedEqualsDirectWrite 直接断言「归并输出 == 同数据
// 直写单 flush」的强金标准（两路径同时漂移也会被 digest 字面量抓住）。

#include <gtest/gtest.h>

#include <cstdint>
#include <cstdio>
#include <iostream>
#include <map>
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
#include "storage/index/inverted/spimi/term_enum.h"

namespace doris::segment_v2::inverted_index::spimi {

namespace {

constexpr uint64_t kFnvOffset = 1469598103934665603ULL;
constexpr uint64_t kFnvPrime = 1099511628211ULL;

uint64_t Fnv1a(const std::vector<uint8_t>& data) {
    uint64_t h = kFnvOffset;
    for (const uint8_t b : data) {
        h ^= b;
        h *= kFnvPrime;
    }
    return h;
}

struct EmitResult {
    std::vector<uint8_t> tis;
    std::vector<uint8_t> tii;
    std::vector<uint8_t> frq;
    std::vector<uint8_t> prx;
};

// .tis/.tii/.frq/.prx 四流链式 digest（一个字面量 pin 全部四流）。
uint64_t ChainDigest(const EmitResult& r) {
    uint64_t h = Fnv1a(r.tis);
    h = h * kFnvPrime + Fnv1a(r.tii);
    h = h * kFnvPrime + Fnv1a(r.frq);
    h = h * kFnvPrime + Fnv1a(r.prx);
    return h;
}

struct Posting {
    int32_t doc;
    std::vector<int32_t> positions; // 严格递增（buffer 会去重相同三元组）
};

// term -> 升序 doc 的 posting 列表。完全确定性（无 rng），改一个字节即红。
using Vocab = std::map<std::string, std::vector<Posting>>;

Vocab BuildVocab() {
    Vocab v;
    // ① inline 混合：40 个 df∈{1,2,3} 小 term（V4 下应内联进 .tis）。
    for (int n = 0; n < 40; ++n) {
        char name[32];
        std::snprintf(name, sizeof(name), "tiny_%02d", n);
        const int df = 1 + (n % 3);
        std::vector<Posting> ps;
        for (int i = 0; i < df; ++i) {
            const int32_t doc = n * 160 + i * 53 + (n % 7);
            const int freq = 1 + ((n + i) % 2);
            std::vector<int32_t> pos;
            int32_t p = i % 2;
            for (int j = 0; j < freq; ++j) {
                pos.push_back(p);
                p += 2 + (n % 3);
            }
            ps.push_back({doc, std::move(pos)});
        }
        v[name] = std::move(ps);
    }
    // ② slim/windowed 跨界三连：Σdf=511 保持 slim；512/513 升级 windowed
    //（kDefaultSkipInterval=512 即 slim/coded 判定线）。
    auto add_slim = [&](const char* name, int df, int phase) {
        std::vector<Posting> ps;
        for (int i = 0; i < df; ++i) {
            const int32_t doc = i * 13 + phase;
            const int freq = 1 + (i % 3);
            std::vector<int32_t> pos;
            int32_t p = i % 4;
            for (int j = 0; j < freq; ++j) {
                pos.push_back(p);
                p += 1 + (j % 3);
            }
            ps.push_back({doc, std::move(pos)});
        }
        v[name] = std::move(ps);
    };
    add_slim("slim_511", 511, 0);
    add_slim("slim_512", 512, 5);
    add_slim("slim_513", 513, 7);
    // ③ windowed 多窗：df=5000（>2048 候选 W 上限 → 必然多窗），
    //    变 delta(1/2)、变 freq(1..4)、变 positions，杜绝常数块收敛。
    {
        std::vector<Posting> ps;
        int32_t doc = 1;
        for (int i = 0; i < 5000; ++i) {
            const int freq = 1 + (i % 4);
            std::vector<int32_t> pos;
            int32_t p = i % 3;
            for (int j = 0; j < freq; ++j) {
                pos.push_back(p);
                // 乘法散列抖动：破坏周期性，让 .prx payload 保留真实熵
                //（纯周期数据 ZSTD 后过小，弱化对拼接错位的字节判别力）。
                p += 1 + static_cast<int32_t>((i * 2654435761ULL + j * 97) % 7);
            }
            ps.push_back({doc, std::move(pos)});
            doc += (i % 3 == 0) ? 2 : 1;
        }
        v["win_5000"] = std::move(ps);
    }
    // ④ PFOR 常数块：stride≡1、freq≡2、positions 恒定 → b=0 子块路径。
    {
        std::vector<Posting> ps;
        for (int i = 0; i < 2048; ++i) {
            ps.push_back({100 + i, {3, 7}});
        }
        v["const_2048"] = std::move(ps);
    }
    // ⑤ phrase 重 positions：df=600 × freq=8，prox payload 大 → .prx ZSTD 窗。
    {
        std::vector<Posting> ps;
        for (int i = 0; i < 600; ++i) {
            std::vector<int32_t> pos;
            int32_t p = i % 6;
            for (int j = 0; j < 8; ++j) {
                pos.push_back(p);
                p += 1 + static_cast<int32_t>(((i * 2654435761ULL >> 3) + j * 13) % 11);
            }
            ps.push_back({500 + i * 9, std::move(pos)});
        }
        v["heavy_600"] = std::move(ps);
    }
    return v;
}

int32_t TotalDocCount(const Vocab& v) {
    int32_t max_doc = 0;
    for (const auto& [term, postings] : v) {
        for (const Posting& p : postings) {
            max_doc = std::max(max_doc, p.doc);
        }
    }
    return max_doc + 1;
}

// 把 doc∈[lo,hi) 的全部 posting 灌入 buffer（生产合约：绝对 doc_id）。
void AppendRange(SpimiPostingBuffer& buf, const Vocab& v, int32_t lo, int32_t hi) {
    for (const auto& [term, postings] : v) {
        for (const Posting& p : postings) {
            if (p.doc < lo || p.doc >= hi) {
                continue;
            }
            for (const int32_t pos : p.positions) {
                buf.Append(term, static_cast<uint32_t>(p.doc), static_cast<uint32_t>(pos));
            }
        }
    }
}

// 直写单 flush（无 spill）：与 SpimiIndexWriter::EmitDirect 同参（V4、
// omit_norms=true、inline_small_terms=true）。is_v4=false 镜像生产非 V4
//（shadow/debug）路径取 kIndexVersionV0（EmitSegment 内部以 use_windowed
// 门控，V0 段保持 -4 外置、整 term PFOR/ZSTD 信封）。
EmitResult EmitDirectSegment(const Vocab& v, int32_t total_docs, bool omit, bool is_v4 = true) {
    SpimiPostingBuffer buf(omit);
    AppendRange(buf, v, 0, total_docs);
    MemoryByteOutput tis, tii, frq, prx, fnm, nrm, seg_n, seg_gen;
    SpimiSegmentSink sink {.tis = &tis,
                           .tii = &tii,
                           .frq = &frq,
                           .prx = &prx,
                           .fnm = &fnm,
                           .nrm = &nrm,
                           .segments_n = &seg_n,
                           .segments_gen = &seg_gen};
    const int32_t index_version =
            is_v4 ? FieldInfosWriter::kIndexVersionV4 : FieldInfosWriter::kIndexVersionV0;
    SpimiFulltextWriter::EmitSegment(buf, sink, "_0", "content", total_docs, index_version, omit,
                                     /*omit_norms=*/true, /*out_byte_counts=*/nullptr,
                                     /*inline_small_terms=*/true);
    return {tis.bytes(), tii.bytes(), frq.bytes(), prx.bytes()};
}

// k 份连续 doc 区间 → k 个 spill（SpillManager 真落盘，V4+omit lockstep）
// → LoadSpill → SegmentMerger::Merge。与 SpimiIndexWriter::EmitMerged 同参。
// is_v4=false 时 spill 以 kIndexVersionV1 写出、归并输出 kIndexVersionV0
//（镜像生产 SpillManager / EmitMerged 的非 V4 取值）。
// saw_zstd_envelope 非空时回报「至少一个输入 spill 含整 term kCodeModeZstd
// 信封」（外置非 slim term 的 .frq 首字节 == 0x80）—— 供 V1 信封金标准断言
// 该分支确实被喂到。
EmitResult EmitMergedSegment(const Vocab& v, int32_t total_docs, bool omit, int k,
                             bool is_v4 = true, bool* saw_zstd_envelope = nullptr) {
    SpillManager mgr("content", is_v4, /*tmp_dir=*/"",
                     /*omit_term_freq_and_positions=*/omit);
    SpimiPostingBuffer buf(omit);
    for (int j = 0; j < k; ++j) {
        const auto lo = static_cast<int32_t>(static_cast<int64_t>(total_docs) * j / k);
        const auto hi = static_cast<int32_t>(static_cast<int64_t>(total_docs) * (j + 1) / k);
        AppendRange(buf, v, lo, hi);
        EXPECT_GT(buf.RecordCount(), 0) << "spill " << j << " must not be empty";
        // 生产合约：cumulative doc_count = 已消费行数上界（max _rid + 1）。
        mgr.FlushBuffer(buf, /*doc_count=*/hi); // Reset()s the buffer
    }
    EXPECT_EQ(mgr.SpillCount(), static_cast<size_t>(k));
    std::vector<SegmentMerger::Input> inputs;
    inputs.reserve(mgr.SpillCount());
    for (size_t i = 0; i < mgr.SpillCount(); ++i) {
        SegmentMerger::Input in;
        EXPECT_TRUE(mgr.LoadSpill(i, in).ok());
        inputs.push_back(std::move(in));
    }
    if (saw_zstd_envelope != nullptr) {
        *saw_zstd_envelope = false;
        for (const auto& in : inputs) {
            TermEnum e(in.tis_bytes);
            while (e.Next()) {
                const TermInfo& info = e.Current().info;
                if (!info.inlined && !info.is_slim &&
                    static_cast<size_t>(info.freq_pointer) < in.frq_bytes.size() &&
                    in.frq_bytes[static_cast<size_t>(info.freq_pointer)] ==
                            FreqProxEncoder::kCodeModeZstd) {
                    *saw_zstd_envelope = true;
                }
            }
        }
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
    const int32_t index_version =
            is_v4 ? FieldInfosWriter::kIndexVersionV4 : FieldInfosWriter::kIndexVersionV0;
    SegmentMerger::Merge(inputs, sink, "_0", "content", total_docs, index_version, omit,
                         /*omit_norms=*/true);
    return {tis.bytes(), tii.bytes(), frq.bytes(), prx.bytes()};
}

// 钉死所有影响输出字节的 config（RAII 还原），digest 不随生产默认漂移。
// 基线 = 当前生产默认：frq_zstd OFF / prx_zstd ON / zstd_min=512 / prx_win=1024。
struct ScopedSpimiConfigPin {
    int64_t saved_zstd_min = config::inverted_index_spimi_zstd_min_window_bytes;
    bool saved_frq = config::inverted_index_spimi_frq_zstd_enable;
    bool saved_prx = config::inverted_index_spimi_prx_zstd_enable;
    int64_t saved_prx_win = config::inverted_index_spimi_prx_window_docs;

    explicit ScopedSpimiConfigPin(bool frq_zstd) {
        config::inverted_index_spimi_zstd_min_window_bytes = 512;
        config::inverted_index_spimi_frq_zstd_enable = frq_zstd;
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

} // namespace

TEST(SpimiMergeByteIdentityGolden, MergeByteIdentityGolden) {
    struct Case {
        const char* name;
        bool omit;     // DOCS_ONLY（omit_term_freq_and_positions）
        bool frq_zstd; // .frq 整 term ZSTD 开关（基线 = 生产默认 OFF）
        int k;         // 0 = 直写单 flush；>=1 = k 个 spill 归并
        uint64_t digest;
    };
    // NOTE: digest 在归并改造前的 HEAD（a56d72e20f2）上采集。直写与 k∈{1,2,7}
    // 归并的 digest 逐组相同 —— 这正是「归并输出 == 直写」字节金标准成立的
    // 直接证据，后续批次必须原样保持。
    std::vector<Case> cases = {
            {"phrase_direct", false, false, 0, 1665124282164366072ULL},
            {"phrase_k1", false, false, 1, 1665124282164366072ULL},
            {"phrase_k2", false, false, 2, 1665124282164366072ULL},
            {"phrase_k7", false, false, 7, 1665124282164366072ULL},
            {"docsonly_direct", true, false, 0, 697336412412448313ULL},
            {"docsonly_k1", true, false, 1, 697336412412448313ULL},
            {"docsonly_k2", true, false, 2, 697336412412448313ULL},
            {"docsonly_k7", true, false, 7, 697336412412448313ULL},
            {"frqzstd_direct", false, true, 0, 2422315138705886935ULL},
            {"frqzstd_k2", false, true, 2, 2422315138705886935ULL},
            // 收尾补强（审查 G1-a）：frq_zstd=ON 补 k=7（窗级 kWinZstd ×
            // 高路数重切）与 DOCS_ONLY（omit 下 .frq 窗仍受 frq_zstd 影响）。
            {"frqzstd_k7", false, true, 7, 2422315138705886935ULL},
            {"frqzstd_docsonly_direct", true, true, 0, 15110323713027440716ULL},
            {"frqzstd_docsonly_k2", true, true, 2, 15110323713027440716ULL},
    };
    const Vocab vocab = BuildVocab();
    const int32_t total_docs = TotalDocCount(vocab);
    for (const auto& c : cases) {
        ScopedSpimiConfigPin pin(c.frq_zstd);
        const EmitResult r = (c.k == 0) ? EmitDirectSegment(vocab, total_docs, c.omit)
                                        : EmitMergedSegment(vocab, total_docs, c.omit, c.k);
        const uint64_t got = ChainDigest(r);
        // 每次运行都打印，格式变更（有意时）从日志回填新值。
        std::cout << "[GOLDEN] " << c.name << " = " << got << "ULL"
                  << "  (tis=" << r.tis.size() << " tii=" << r.tii.size() << " frq=" << r.frq.size()
                  << " prx=" << r.prx.size() << ")" << std::endl;
        EXPECT_EQ(got, c.digest)
                << "byte-identity broken for " << c.name
                << " — merged/direct .tis/.tii/.frq/.prx bytes changed. If this change is "
                   "INTENTIONAL (format moved), update the golden to the printed value.";
    }
}

// 收尾补强（审查 G1-b）：V1 整 term kCodeModeZstd 信封作为归并输入的字节
// 金标准。V4 生产不可达该信封（生产唯一构造点恒 is_v4=true；V4 spill 以
// kIndexVersionV4 写出，df>=512 走 windowed 路径不经 FlushFrqBlock 的信封
// 分支，df<512 slim 永不 ZSTD；且信封还需非默认 frq_zstd_enable=true），但
// merge 的 DecodeFlat 格式上接受它（shadow/debug is_v4=false 理论可达），
// 故以金标准钉死：V1 spill k=2 时 win_5000 的 df_i=2500 >= 512，必然写出
// 整 term PFOR+kCodeModeZstd 信封（下方对输入 spill 的 .frq 首字节显式断言，
// 杜绝「金标准存在但分支未真正进入」），归并输出（version V0，镜像生产
// EmitMerged 非 V4 取值）必须与同数据 V0 直写逐字节相同。
TEST(SpimiMergeByteIdentityGolden, LegacyV1WholeTermZstdEnvelopeGolden) {
    ScopedSpimiConfigPin pin(/*frq_zstd=*/true);
    const Vocab vocab = BuildVocab();
    const int32_t total_docs = TotalDocCount(vocab);
    const EmitResult direct = EmitDirectSegment(vocab, total_docs, /*omit=*/false, /*is_v4=*/false);
    bool saw_zstd_envelope = false;
    const EmitResult merged = EmitMergedSegment(vocab, total_docs, /*omit=*/false, /*k=*/2,
                                                /*is_v4=*/false, &saw_zstd_envelope);
    // 前提断言：归并输入确实含 V1 整 term kCodeModeZstd 信封。
    EXPECT_TRUE(saw_zstd_envelope)
            << "V1 spill 输入未产生整 term kCodeModeZstd 信封 —— 本金标准未覆盖目标分支";
    // 强金标准：四流逐字节相同 + digest 字面量 pin（防双路径同步漂移）。
    EXPECT_EQ(direct.tis, merged.tis) << "V1 envelope merge .tis";
    EXPECT_EQ(direct.tii, merged.tii) << "V1 envelope merge .tii";
    EXPECT_EQ(direct.frq, merged.frq) << "V1 envelope merge .frq";
    EXPECT_EQ(direct.prx, merged.prx) << "V1 envelope merge .prx";
    constexpr uint64_t kLegacyGolden = 14661402543875904047ULL;
    for (const EmitResult* r : {&direct, &merged}) {
        const uint64_t got = ChainDigest(*r);
        std::cout << "[GOLDEN] legacy_v1_frqzstd = " << got << "ULL"
                  << "  (tis=" << r->tis.size() << " tii=" << r->tii.size()
                  << " frq=" << r->frq.size() << " prx=" << r->prx.size() << ")" << std::endl;
        EXPECT_EQ(got, kLegacyGolden)
                << "byte-identity broken for legacy_v1_frqzstd. If this change is INTENTIONAL "
                   "(format moved), update the golden to the printed value.";
    }
}

// 强金标准的活体断言：归并输出与「同数据直写单 flush」逐字节相同（四流
// 各自 vector 全等）。digest 字面量防双路径同步漂移，本测试防两者偏离。
TEST(SpimiMergeByteIdentityGolden, MergedEqualsDirectWrite) {
    const Vocab vocab = BuildVocab();
    const int32_t total_docs = TotalDocCount(vocab);
    for (const bool omit : {false, true}) {
        ScopedSpimiConfigPin pin(/*frq_zstd=*/false);
        const EmitResult direct = EmitDirectSegment(vocab, total_docs, omit);
        for (const int k : {1, 2, 7}) {
            const EmitResult merged = EmitMergedSegment(vocab, total_docs, omit, k);
            EXPECT_EQ(direct.tis, merged.tis) << "omit=" << omit << " k=" << k << " .tis";
            EXPECT_EQ(direct.tii, merged.tii) << "omit=" << omit << " k=" << k << " .tii";
            EXPECT_EQ(direct.frq, merged.frq) << "omit=" << omit << " k=" << k << " .frq";
            EXPECT_EQ(direct.prx, merged.prx) << "omit=" << omit << " k=" << k << " .prx";
        }
    }
}

} // namespace doris::segment_v2::inverted_index::spimi
