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

// 批2 RED 网（R1-R7）：merger 平面化重编 + Σdf 升级判定的峰值/语义双断言。
// 批3 RED 网（R8）：spill 流式游标 —— 消 LoadSpill 全量载回的峰值断言。
//
// 每例两段断言：
//   (a) 峰值 —— ScopedHeapHighWater 包住「输入构造+Merge」全程。批2 杀掉了
//       vector<DecodedDoc> 物化（R1/R3/R4/R5 由此转绿）；批3 进一步要求
//       merge 阶段总驻留 < k×游标缓冲 + 平面化批 + 常数 —— R8 的 term 海
//       形态下 Σ输入字节主导峰值，LoadSpill 全量载回必超线（RED），
//       流式游标（OpenSpillCursor）后转绿。
//   (b) 语义 —— 归并输出与「同数据直写单 flush」四流逐字节相同（强金标准，
//       与 spill_merge_byte_identity_golden_test 同口径），改前改后都必须绿。
//
// 构造遵循生产合约（spill_segment_merger_test.cpp:1404-1421 的注释）：绝对
// doc_id、k 份连续 doc 区间、cumulative doc_count、SpillManager::FlushBuffer
// 真落盘。R2/R6/R7 是绿守护（HEAD 上也绿），分别盯 Σdf 跨 spill 升级判定
// （511/512/513）、k=3 绝对 doc_id 合约、staged-inline 判定不被流式重构破坏。

#include <gtest/gtest.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "common/config.h"
#include "heap_high_water.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "storage/index/inverted/spimi/byte_output.h"
#include "storage/index/inverted/spimi/field_infos_writer.h"
#include "storage/index/inverted/spimi/file_byte_output.h"
#include "storage/index/inverted/spimi/freq_prox_encoder.h"
#include "storage/index/inverted/spimi/fulltext_writer.h"
#include "storage/index/inverted/spimi/posting_buffer.h"
#include "storage/index/inverted/spimi/posting_decoder.h"
#include "storage/index/inverted/spimi/segment_merger.h"
#include "storage/index/inverted/spimi/spill_manager.h"
#include "storage/index/inverted/spimi/term_enum.h"
#include "util/slice.h"

namespace doris::segment_v2::inverted_index::spimi {

namespace {

// 钉死所有影响输出字节的 config（RAII 还原）：生产默认 frq_zstd OFF /
// prx_zstd ON / zstd_min=512 / prx_win=1024（与金标准测试同口径）。
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

struct FourStreams {
    std::vector<uint8_t> tis;
    std::vector<uint8_t> tii;
    std::vector<uint8_t> frq;
    std::vector<uint8_t> prx;
};

// 直写单 flush（无 spill）：与 SpimiIndexWriter::EmitDirect 同参（V4、
// omit_norms=true、inline_small_terms=true）。`gen(lo, hi, buf)` 把
// doc∈[lo,hi) 的全部 posting 灌入 buffer（绝对 doc_id）。
template <typename Gen>
FourStreams EmitDirect(const Gen& gen, int32_t total_docs, bool omit) {
    SpimiPostingBuffer buf(omit);
    gen(0, total_docs, buf);
    MemoryByteOutput tis, tii, frq, prx, fnm, nrm, seg_n, seg_gen;
    SpimiSegmentSink sink {.tis = &tis,
                           .tii = &tii,
                           .frq = &frq,
                           .prx = &prx,
                           .fnm = &fnm,
                           .nrm = &nrm,
                           .segments_n = &seg_n,
                           .segments_gen = &seg_gen};
    SpimiFulltextWriter::EmitSegment(buf, sink, "_0", "content", total_docs,
                                     FieldInfosWriter::kIndexVersionV4, omit,
                                     /*omit_norms=*/true, /*out_byte_counts=*/nullptr,
                                     /*inline_small_terms=*/true);
    return {tis.bytes(), tii.bytes(), frq.bytes(), prx.bytes()};
}

struct MergedRun {
    FourStreams streams;
    size_t peak_bytes = 0;  // LoadSpill+Merge 全程堆峰值（净分配高水位）
    size_t input_bytes = 0; // Σ 输入四流字节
    int64_t term_count = 0;
};

// k 份连续 doc 区间（cuts = k+1 个边界）→ k 个 spill（FlushBuffer 真落盘）
// → ScopedHeapHighWater 包住「OpenSpillCursor + Merge」（批3 后的生产形态：
// 流式游标，无全量载回）。`cursor_buffer_bytes` 控制每流滑窗容量（默认与
// 生产相同；R9 用 4KB 压力暴露生命周期 bug）。
template <typename Gen>
MergedRun EmitMergedWithPeak(const Gen& gen, const std::vector<int32_t>& cuts, bool omit,
                             size_t cursor_buffer_bytes = SpillManager::kDefaultCursorBufferBytes) {
    const int32_t total_docs = cuts.back();
    SpillManager mgr("content", /*is_v4=*/true, /*tmp_dir=*/"",
                     /*omit_term_freq_and_positions=*/omit);
    {
        SpimiPostingBuffer buf(omit);
        for (size_t j = 0; j + 1 < cuts.size(); ++j) {
            gen(cuts[j], cuts[j + 1], buf);
            EXPECT_GT(buf.RecordCount(), 0) << "spill " << j << " must not be empty";
            EXPECT_FALSE(buf.Saturated());
            // 生产合约：cumulative doc_count = 已消费行数上界（max _rid + 1）。
            mgr.FlushBuffer(buf, /*doc_count=*/cuts[j + 1]); // Reset()s the buffer
        }
    }
    EXPECT_EQ(mgr.SpillCount(), cuts.size() - 1);

    MergedRun out;
    for (const SpillSegment& seg : mgr.Spills()) {
        out.input_bytes += static_cast<size_t>(seg.tis.length + seg.tii.length + seg.frq.length +
                                               seg.prx.length);
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
    {
        ScopedHeapHighWater hw;
        std::vector<SegmentMerger::StreamInput> inputs;
        inputs.reserve(mgr.SpillCount());
        for (size_t i = 0; i < mgr.SpillCount(); ++i) {
            SegmentMerger::StreamInput in;
            EXPECT_TRUE(mgr.OpenSpillCursor(i, in, cursor_buffer_bytes).ok());
            inputs.push_back(std::move(in));
        }
        out.term_count = SegmentMerger::Merge(inputs, sink, "_0", "content", total_docs,
                                              FieldInfosWriter::kIndexVersionV4, omit,
                                              /*omit_norms=*/true);
        out.peak_bytes = hw.PeakDeltaBytes();
    }
    out.streams = {tis.bytes(), tii.bytes(), frq.bytes(), prx.bytes()};
    return out;
}

std::vector<uint8_t> ReadFileBytes(const std::string& path) {
    io::FileReaderSPtr reader;
    EXPECT_TRUE(io::global_local_filesystem()->open_file(path, &reader).ok()) << path;
    std::vector<uint8_t> bytes(reader->size());
    if (!bytes.empty()) {
        size_t got = 0;
        EXPECT_TRUE(reader->read_at(0, Slice(bytes.data(), bytes.size()), &got).ok()) << path;
        EXPECT_EQ(got, bytes.size()) << path;
    }
    static_cast<void>(reader->close());
    return bytes;
}

// 批3 专用：与 EmitMergedWithPeak 同合约，但归并的四个输出流走 FileByteOutput
// （对齐生产 EmitMerged 的文件背书 IndexOutput —— 输出字节不驻留内存），
// ScopedHeapHighWater 因此只量「输入构造 + Merge 工作集」。Σ输入字节从
// SpillSegment 元数据取（不依赖载回方式）。term 海形态下旧实现的
// LoadSpill 全量载回 ≈ Σ输入 全程驻留 → 超线 RED（b3_red_run.log 留证）；
// 流式游标后只剩 k×4×min(缓冲, 流长) + 平面化批 → GREEN。
template <typename Gen>
MergedRun EmitMergedWithPeakFileSink(const Gen& gen, const std::vector<int32_t>& cuts, bool omit) {
    const int32_t total_docs = cuts.back();
    SpillManager mgr("content", /*is_v4=*/true, /*tmp_dir=*/"",
                     /*omit_term_freq_and_positions=*/omit);
    {
        SpimiPostingBuffer buf(omit);
        for (size_t j = 0; j + 1 < cuts.size(); ++j) {
            gen(cuts[j], cuts[j + 1], buf);
            EXPECT_GT(buf.RecordCount(), 0) << "spill " << j << " must not be empty";
            EXPECT_FALSE(buf.Saturated());
            mgr.FlushBuffer(buf, /*doc_count=*/cuts[j + 1]);
        }
    }
    EXPECT_EQ(mgr.SpillCount(), cuts.size() - 1);

    MergedRun out;
    for (const SpillSegment& seg : mgr.Spills()) {
        out.input_bytes += static_cast<size_t>(seg.tis.length + seg.tii.length + seg.frq.length +
                                               seg.prx.length);
    }

    // 输出文件目录（用例级唯一，结束即删）。
    static std::atomic<uint64_t> g_dir_counter {0};
    const std::string dir = "/tmp/spimi_peak_red_" + std::to_string(::getpid()) + "_" +
                            std::to_string(g_dir_counter.fetch_add(1));
    EXPECT_TRUE(
            io::global_local_filesystem()->create_directory(dir, /*failed_if_exists=*/false).ok());

    struct OutStream {
        io::FileWriterPtr writer;
        std::unique_ptr<FileByteOutput> out;
    };
    auto open_out = [&](const char* ext) -> OutStream {
        OutStream os;
        EXPECT_TRUE(io::global_local_filesystem()
                            ->create_file(dir + "/merged." + ext, &os.writer)
                            .ok());
        os.out = std::make_unique<FileByteOutput>(os.writer.get());
        return os;
    };
    OutStream o_tis = open_out("tis");
    OutStream o_tii = open_out("tii");
    OutStream o_frq = open_out("frq");
    OutStream o_prx = open_out("prx");
    MemoryByteOutput fnm, nrm, seg_n, seg_gen; // 元数据流极小，不影响峰值口径
    SpimiSegmentSink sink {.tis = o_tis.out.get(),
                           .tii = o_tii.out.get(),
                           .frq = o_frq.out.get(),
                           .prx = o_prx.out.get(),
                           .fnm = &fnm,
                           .nrm = &nrm,
                           .segments_n = &seg_n,
                           .segments_gen = &seg_gen};
    {
        ScopedHeapHighWater hw;
        std::vector<SegmentMerger::StreamInput> inputs;
        inputs.reserve(mgr.SpillCount());
        for (size_t i = 0; i < mgr.SpillCount(); ++i) {
            SegmentMerger::StreamInput in;
            EXPECT_TRUE(mgr.OpenSpillCursor(i, in).ok());
            inputs.push_back(std::move(in));
        }
        out.term_count = SegmentMerger::Merge(inputs, sink, "_0", "content", total_docs,
                                              FieldInfosWriter::kIndexVersionV4, omit,
                                              /*omit_norms=*/true);
        out.peak_bytes = hw.PeakDeltaBytes();
    }
    auto close_out = [](OutStream& os) {
        EXPECT_TRUE(os.out->Finish().ok());
        EXPECT_TRUE(os.writer->close().ok());
    };
    close_out(o_tis);
    close_out(o_tii);
    close_out(o_frq);
    close_out(o_prx);

    out.streams = {ReadFileBytes(dir + "/merged.tis"), ReadFileBytes(dir + "/merged.tii"),
                   ReadFileBytes(dir + "/merged.frq"), ReadFileBytes(dir + "/merged.prx")};
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(dir).ok());
    return out;
}

// (b) 语义：归并输出 == 直写单 flush（四流逐字节）。
void ExpectStreamsEqual(const FourStreams& direct, const FourStreams& merged, const char* label) {
    EXPECT_EQ(direct.tis, merged.tis) << label << " .tis diverged from direct write";
    EXPECT_EQ(direct.tii, merged.tii) << label << " .tii diverged from direct write";
    EXPECT_EQ(direct.frq, merged.frq) << label << " .frq diverged from direct write";
    EXPECT_EQ(direct.prx, merged.prx) << label << " .prx diverged from direct write";
}

// (a) 峰值：peak ≤ max(floor_mb MiB, ratio × Σ输入字节)。比值式为主、绝对值
// 为辅（heap_high_water.h 的断言建议），每次都打印实测以便回填阈值。
void ExpectPeakBounded(const char* label, const MergedRun& run, size_t floor_mb, double ratio) {
    const auto line = std::max<size_t>(
            floor_mb << 20U, static_cast<size_t>(ratio * static_cast<double>(run.input_bytes)));
    std::cout << "[PEAK] " << label << " peak=" << run.peak_bytes << "B ("
              << (run.peak_bytes >> 20U) << "MB) input=" << run.input_bytes << "B line=" << line
              << "B" << std::endl;
    if (!ScopedHeapHighWater::Available()) {
        std::cout << "[PEAK] " << label << " high-water probe unavailable; peak not asserted"
                  << std::endl;
        return;
    }
    EXPECT_LE(run.peak_bytes, line)
            << label << " merge peak exceeds the flattened budget — per-term materialization "
            << "(vector<DecodedDoc> + per-doc positions heap blocks) is back";
}

std::vector<TermEntry> ReadAllTerms(const std::vector<uint8_t>& tis_bytes) {
    std::vector<TermEntry> entries;
    TermEnum en(tis_bytes);
    while (en.Next()) {
        entries.push_back(en.Current());
    }
    return entries;
}

// 乘法散列抖动（杜绝周期/常数收敛，保留真实熵）。
inline uint32_t Jitter(int64_t a, int64_t b, uint32_t mod) {
    return static_cast<uint32_t>((static_cast<uint64_t>(a) * 2654435761ULL + b * 97) % mod);
}

} // namespace

// R1 —— df≈80 万 windowed 多窗 term，k=2（各 50 万 doc 区间），freq=3、
// positions 抖动防常数收敛。旧实现 ≈64B/doc 物化 + merged_docs 扩容瞬态
// 必超线（RED）；平面化后 flat 数组 ≈12B/doc + pos 字节。
TEST(SpimiMergePeakRed, R1_HighDfWindowedTwoSpillPeakBounded) {
    ScopedSpimiConfigPin pin;
    constexpr int32_t kTotal = 1'000'000;
    auto gen = [](int32_t lo, int32_t hi, SpimiPostingBuffer& buf) {
        for (int32_t doc = lo; doc < hi; ++doc) {
            if (Jitter(doc, 0, 5) == 0) {
                continue; // 不规则空洞：doc-delta 非常数
            }
            uint32_t p = static_cast<uint32_t>(doc % 5);
            for (int j = 0; j < 3; ++j) {
                buf.Append("hot", static_cast<uint32_t>(doc), p);
                p += 1 + Jitter(doc, j, 7);
            }
        }
    };
    const MergedRun merged = EmitMergedWithPeak(gen, {0, 500'000, kTotal}, /*omit=*/false);
    EXPECT_EQ(merged.term_count, 1);
    const FourStreams direct = EmitDirect(gen, kTotal, /*omit=*/false);
    ExpectStreamsEqual(direct, merged.streams, "R1");
    // 批3 收紧：输入字节不再是合法驻留（流式游标），ratio 归零、floor 压到
    // 平面化批 + 有界游标缓冲 + 余量（基线 26.7MB，输入 0.28MB）。
    ExpectPeakBounded("R1", merged, /*floor_mb=*/30, /*ratio=*/0.0);
}

// R3 —— PFOR 常数块（b=0）高 df：stride≡1、freq≡2、positions 恒定 {3,7}，
// df=100 万，k=2。输入字节极小（常数块 ≈2B/128 doc）、解码后膨胀比最大，
// 物化路径在此最难看（RED 线同 R1）。
TEST(SpimiMergePeakRed, R3_ConstBlockHighDfPeakBounded) {
    ScopedSpimiConfigPin pin;
    constexpr int32_t kTotal = 1'000'000;
    auto gen = [](int32_t lo, int32_t hi, SpimiPostingBuffer& buf) {
        for (int32_t doc = lo; doc < hi; ++doc) {
            buf.Append("cst", static_cast<uint32_t>(doc), 3);
            buf.Append("cst", static_cast<uint32_t>(doc), 7);
        }
    };
    const MergedRun merged = EmitMergedWithPeak(gen, {0, 500'000, kTotal}, /*omit=*/false);
    EXPECT_EQ(merged.term_count, 1);
    const FourStreams direct = EmitDirect(gen, kTotal, /*omit=*/false);
    ExpectStreamsEqual(direct, merged.streams, "R3");
    // 批3 收紧：ratio 归零（基线 28.2MB，输入 0.06MB）。
    ExpectPeakBounded("R3", merged, /*floor_mb=*/32, /*ratio=*/0.0);
}

// R4 —— DOCS_ONLY（omit=true、无 positions）df≈80 万，k=2，FlushBuffer/
// Merge 全程 omit lockstep。物化仍 32B/doc → RED；平面化后仅 flat dd。
TEST(SpimiMergePeakRed, R4_OmitTfapHighDfPeakBounded) {
    ScopedSpimiConfigPin pin;
    constexpr int32_t kTotal = 1'000'000;
    auto gen = [](int32_t lo, int32_t hi, SpimiPostingBuffer& buf) {
        for (int32_t doc = lo; doc < hi; ++doc) {
            if (Jitter(doc, 1, 5) == 0) {
                continue; // 不规则空洞
            }
            buf.Append("omt", static_cast<uint32_t>(doc), 0);
        }
    };
    const MergedRun merged = EmitMergedWithPeak(gen, {0, 500'000, kTotal}, /*omit=*/true);
    EXPECT_EQ(merged.term_count, 1);
    const FourStreams direct = EmitDirect(gen, kTotal, /*omit=*/true);
    ExpectStreamsEqual(direct, merged.streams, "R4");
    // 批3 收紧：16→9MB（基线 7.7MB，输入 0.21MB）。
    ExpectPeakBounded("R4", merged, /*floor_mb=*/9, /*ratio=*/0.0);
}

// R5 —— phrase 重 positions：df≈60 万 × freq=8（positions 独立堆块在旧实现
// 占大头），k=3。floor=48MB：平面化后的合法工作集以 pos 字节为主
//（flat.pos_vint + WindowFrameEncoder pos_parts + 输出 .prx ≈ 3×7MB，加
// 3×12B/doc flat 数组，实测 34MB）；旧物化实现实测 81MB，仍远超此线。
TEST(SpimiMergePeakRed, R5_HeavyPositionsPhraseK3PeakBounded) {
    ScopedSpimiConfigPin pin;
    constexpr int32_t kTotal = 700'000;
    auto gen = [](int32_t lo, int32_t hi, SpimiPostingBuffer& buf) {
        for (int32_t doc = lo; doc < hi; ++doc) {
            if (doc % 7 == 3) {
                continue;
            }
            uint32_t p = static_cast<uint32_t>(doc % 6);
            for (int j = 0; j < 8; ++j) {
                buf.Append("hvy", static_cast<uint32_t>(doc), p);
                p += 1 + Jitter(doc >> 3, j * 13, 11);
            }
        }
    };
    const MergedRun merged = EmitMergedWithPeak(gen, {0, 233'334, 466'667, kTotal}, /*omit=*/false);
    EXPECT_EQ(merged.term_count, 1);
    const FourStreams direct = EmitDirect(gen, kTotal, /*omit=*/false);
    ExpectStreamsEqual(direct, merged.streams, "R5");
    // 批3 收紧：48→40MB（基线 34.3MB，输入 0.43MB）。
    ExpectPeakBounded("R5", merged, /*floor_mb=*/40, /*ratio=*/0.0);
}

// R2 —— Σdf 跨 spill 升级判定专项（绿守护）：cross_511 = 256+255（归并后仍
// slim）、cross_512 = 256+256（升级 windowed）、cross_513 = 256+257；外加
// 20 万个 df∈[1,3] 小 term 压 per-term 开销（小 term 海也不许整段物化）。
// 判定必须以 Σdf 而非各输入的 is_slim 为准 —— 输出 is_slim/windowed 与直写
// 完全一致由四流逐字节断言兜底，.tis 标志再显式复核。
TEST(SpimiMergePeakRed, R2_SlimBoundaryStraddleMergeCorrect) {
    ScopedSpimiConfigPin pin;
    constexpr int32_t kTotal = 500'000;
    constexpr int32_t kHalf = 250'000;
    constexpr int32_t kTinyTerms = 200'000;
    auto gen = [](int32_t lo, int32_t hi, SpimiPostingBuffer& buf) {
        // 跨界三连：A 半区 doc=i*900+phase（i<256），B 半区 doc=250000+i*900+phase。
        struct Cross {
            const char* name;
            int32_t df_a;
            int32_t df_b;
            int32_t phase;
        };
        static constexpr Cross kCross[] = {
                {"cross_511", 256, 255, 13},
                {"cross_512", 256, 256, 17},
                {"cross_513", 256, 257, 29},
        };
        for (const auto& c : kCross) {
            auto emit_half = [&](int32_t base, int32_t df) {
                for (int32_t i = 0; i < df; ++i) {
                    const int32_t doc = base + i * 900 + c.phase;
                    if (doc < lo || doc >= hi) {
                        continue;
                    }
                    const int freq = 1 + (i % 3);
                    uint32_t p = static_cast<uint32_t>(i % 4);
                    for (int j = 0; j < freq; ++j) {
                        buf.Append(c.name, static_cast<uint32_t>(doc), p);
                        p += 1 + Jitter(i, j, 5);
                    }
                }
            };
            emit_half(/*base=*/0, c.df_a);
            emit_half(/*base=*/kHalf, c.df_b);
        }
        // 小 term 海：df=1+(i%3)，doc 按散列铺开（严格递增、< kTotal）。
        char name[24];
        for (int32_t i = 0; i < kTinyTerms; ++i) {
            std::snprintf(name, sizeof(name), "tiny_%06d", i);
            const int32_t base =
                    static_cast<int32_t>((static_cast<uint64_t>(i) * 2654435761ULL) % 490'000ULL);
            const int df = 1 + (i % 3);
            for (int d = 0; d < df; ++d) {
                const int32_t doc = base + d * 1009;
                if (doc < lo || doc >= hi) {
                    continue;
                }
                buf.Append(name, static_cast<uint32_t>(doc), static_cast<uint32_t>(i % 7));
            }
        }
    };
    const MergedRun merged = EmitMergedWithPeak(gen, {0, kHalf, kTotal}, /*omit=*/false);
    const FourStreams direct = EmitDirect(gen, kTotal, /*omit=*/false);
    ExpectStreamsEqual(direct, merged.streams, "R2");

    // 显式复核 Σdf 判定：511 → slim；512/513 → windowed（is_slim=false，外置
    // 块首字节 = kCodeModeSpimiWindowed）。
    const auto terms = ReadAllTerms(merged.streams.tis);
    int checked = 0;
    for (const TermEntry& e : terms) {
        if (e.term_utf8 == "cross_511") {
            EXPECT_EQ(e.info.doc_freq, 511);
            EXPECT_TRUE(e.info.is_slim) << "Σdf=511 must stay slim after the merge";
            ++checked;
        } else if (e.term_utf8 == "cross_512" || e.term_utf8 == "cross_513") {
            EXPECT_EQ(e.info.doc_freq, e.term_utf8 == "cross_512" ? 512 : 513);
            EXPECT_FALSE(e.info.is_slim)
                    << e.term_utf8 << " Σdf>=512 must upgrade to windowed after the merge";
            if (!e.info.inlined) {
                ASSERT_LT(static_cast<size_t>(e.info.freq_pointer), merged.streams.frq.size());
                EXPECT_EQ(merged.streams.frq[static_cast<size_t>(e.info.freq_pointer)],
                          FreqProxEncoder::kCodeModeSpimiWindowed);
            }
            ++checked;
        }
    }
    EXPECT_EQ(checked, 3);
    ExpectPeakBounded("R2", merged, /*floor_mb=*/16, /*ratio=*/0.75);
}

// R6 —— k=3 绝对 doc_id 合约（绿守护）：df=51 万（刚过 512 windowed 线的
// 高 df 版），三段连续绝对 id；归并后逐 doc 校验绝对 id 原样保留。
TEST(SpimiMergePeakRed, R6_K3HighDfAbsoluteDocIds) {
    ScopedSpimiConfigPin pin;
    constexpr int32_t kTotal = 510'000;
    auto gen = [](int32_t lo, int32_t hi, SpimiPostingBuffer& buf) {
        for (int32_t doc = lo; doc < hi; ++doc) {
            buf.Append("r6hot", static_cast<uint32_t>(doc), static_cast<uint32_t>(doc % 3));
        }
    };
    const MergedRun merged = EmitMergedWithPeak(gen, {0, 170'000, 340'000, kTotal}, /*omit=*/false);
    EXPECT_EQ(merged.term_count, 1);
    const FourStreams direct = EmitDirect(gen, kTotal, /*omit=*/false);
    ExpectStreamsEqual(direct, merged.streams, "R6");

    const auto terms = ReadAllTerms(merged.streams.tis);
    ASSERT_EQ(terms.size(), 1U);
    const TermEntry& e = terms[0];
    ASSERT_FALSE(e.info.inlined);
    ASSERT_EQ(e.info.doc_freq, kTotal);
    const auto fp = static_cast<size_t>(e.info.freq_pointer);
    const auto pp = static_cast<size_t>(e.info.prox_pointer);
    const auto docs = PostingDecoder::Decode(
            merged.streams.frq.data() + fp, merged.streams.frq.size() - fp,
            merged.streams.prx.data() + pp, merged.streams.prx.size() - pp, e.info.doc_freq,
            /*has_prox=*/true, e.info.is_slim);
    ASSERT_EQ(docs.size(), static_cast<size_t>(kTotal));
    size_t mismatches = 0;
    for (int32_t i = 0; i < kTotal; ++i) {
        if (docs[static_cast<size_t>(i)].doc_id != i) {
            ++mismatches;
        }
    }
    EXPECT_EQ(mismatches, 0U) << "absolute doc_ids must survive the k=3 merge verbatim";
    EXPECT_LT(docs.back().doc_id, kTotal) << "doc_id past total_doc_count (offset double-shift)";
    ExpectPeakBounded("R6", merged, /*floor_mb=*/96, /*ratio=*/1.0);
}

// R7 —— inline 混合（绿守护）：1 个 df=50 万大 term + 5 万个 df≤3 小 term，
// k=2。断言小 term 仍内联进 .tis、大 term 外置 —— 防流式重构破坏
// staged-inline 判定。
TEST(SpimiMergePeakRed, R7_MixedInlineAndHighDfLockstep) {
    ScopedSpimiConfigPin pin;
    constexpr int32_t kTotal = 500'000;
    constexpr int32_t kTinyTerms = 50'000;
    auto gen = [](int32_t lo, int32_t hi, SpimiPostingBuffer& buf) {
        for (int32_t doc = lo; doc < hi; ++doc) {
            uint32_t p = static_cast<uint32_t>(doc % 4);
            for (int j = 0; j < 2; ++j) {
                buf.Append("hot", static_cast<uint32_t>(doc), p);
                p += 1 + Jitter(doc, j + 7, 6);
            }
        }
        char name[24];
        for (int32_t i = 0; i < kTinyTerms; ++i) {
            std::snprintf(name, sizeof(name), "tiny_%05d", i);
            const int32_t base =
                    static_cast<int32_t>((static_cast<uint64_t>(i) * 7919ULL) % 490'000ULL);
            const int df = 1 + (i % 3);
            for (int d = 0; d < df; ++d) {
                const int32_t doc = base + d * 1500;
                if (doc < lo || doc >= hi) {
                    continue;
                }
                buf.Append(name, static_cast<uint32_t>(doc), static_cast<uint32_t>(i % 11));
            }
        }
    };
    const MergedRun merged = EmitMergedWithPeak(gen, {0, 250'000, kTotal}, /*omit=*/false);
    const FourStreams direct = EmitDirect(gen, kTotal, /*omit=*/false);
    ExpectStreamsEqual(direct, merged.streams, "R7");

    const auto terms = ReadAllTerms(merged.streams.tis);
    size_t tiny_inlined = 0;
    size_t tiny_total = 0;
    bool hot_seen = false;
    for (const TermEntry& e : terms) {
        if (e.term_utf8 == "hot") {
            hot_seen = true;
            EXPECT_FALSE(e.info.inlined) << "df=500K term must stay external";
            EXPECT_FALSE(e.info.is_slim);
        } else {
            ++tiny_total;
            tiny_inlined += e.info.inlined ? 1U : 0U;
        }
    }
    EXPECT_TRUE(hot_seen);
    EXPECT_EQ(tiny_total, static_cast<size_t>(kTinyTerms));
    EXPECT_EQ(tiny_inlined, tiny_total) << "every df<=3 term must stay inlined in .tis";
    ExpectPeakBounded("R7", merged, /*floor_mb=*/96, /*ratio=*/1.0);
}

// R8 —— 批3 核心 RED：httplogs 形态 50 万 doc 合成 term 海，k=7。每 doc：
// 2 个 hot term（get/http，df=50 万）+ 1 个状态 term（5 词表）+ 2 个唯一长名
// term（url_/ref_，64-hex 后缀 → .tis 体量主导，Σ输入 ≈ 数十 MB）+ 1 个
// ip term（df≈3）+ 1 个 ua term（千词表）。输出走 FileByteOutput（输出字节
// 不驻留），故峰值 ≈ 输入驻留 + 平面化批：
//   旧实现 LoadSpill 把 k=7 个 spill 全量载回 → 峰值 ≥ Σ输入 > 64MB（RED）；
//   流式游标后 ≈ k×4×min(1MB,流长) + 平面化批 + 常数 < 64MB（GREEN）。
TEST(SpimiMergePeakRed, R8_HttpLogsShapedTermSeaPeakBounded) {
    ScopedSpimiConfigPin pin;
    constexpr int32_t kTotal = 500'000;
    auto gen = [](int32_t lo, int32_t hi, SpimiPostingBuffer& buf) {
        char name[96];
        for (int32_t doc = lo; doc < hi; ++doc) {
            const auto d = static_cast<uint32_t>(doc);
            buf.Append("get", d, 0);
            buf.Append("http", d, 1);
            std::snprintf(name, sizeof(name), "st_%u", Jitter(doc, 3, 5));
            buf.Append(name, d, 2);
            // 唯一 URL / referrer：64-hex 后缀（乘法散列展开，逐 doc 唯一）。
            const auto h = static_cast<uint64_t>(d);
            std::snprintf(name, sizeof(name), "url_%016llx%016llx%016llx%016llx",
                          static_cast<unsigned long long>(h * 0x9E3779B97F4A7C15ULL),
                          static_cast<unsigned long long>((h + 1) * 0xC2B2AE3D27D4EB4FULL),
                          static_cast<unsigned long long>((h + 2) * 0x165667B19E3779F9ULL),
                          static_cast<unsigned long long>(h ^ 0xD6E8FEB86659FD93ULL));
            buf.Append(name, d, 3);
            std::snprintf(name, sizeof(name), "ref_%016llx%016llx%016llx%016llx",
                          static_cast<unsigned long long>(h * 0xFF51AFD7ED558CCDULL),
                          static_cast<unsigned long long>((h + 3) * 0x9E3779B97F4A7C15ULL),
                          static_cast<unsigned long long>((h + 4) * 0xC2B2AE3D27D4EB4FULL),
                          static_cast<unsigned long long>(h ^ 0xA24BAED4963EE407ULL));
            buf.Append(name, d, 4);
            std::snprintf(name, sizeof(name), "ip_%08x",
                          Jitter(doc / 3, 7, 0xFFFFFFFFU)); // df≈3 的客户端 IP
            buf.Append(name, d, 5);
            std::snprintf(name, sizeof(name), "ua_%03u", Jitter(doc, 11, 1000));
            buf.Append(name, d, 6);
        }
    };
    const MergedRun merged = EmitMergedWithPeakFileSink(
            gen, {0, 71'429, 142'858, 214'287, 285'716, 357'145, 428'574, kTotal},
            /*omit=*/false);
    EXPECT_GT(merged.term_count, 1'000'000); // url+ref 唯一 term 海
    const FourStreams direct = EmitDirect(gen, kTotal, /*omit=*/false);
    ExpectStreamsEqual(direct, merged.streams, "R8");
    // 设计文档批3 指定线：httplogs 形态 50 万 doc 合成 term 下 <64MB。
    ExpectPeakBounded("R8", merged, /*floor_mb=*/64, /*ratio=*/0.0);
}

// R9 —— 4KB 小读缓冲压力（绿守护，批3 流式游标专属）：R2 同款 Σdf 跨界
// 三连（511/512/513）+ 小 term 海（inline）+ 高 df windowed term（prx ZSTD
// 窗口），k=3，游标缓冲压到 4KB —— 高频 Refill / SkipForwardTo / inline
// span 拷贝在 ASAN 下暴露借用生命周期与短读 bug；输出仍须与直写逐字节相同。
TEST(SpimiMergePeakRed, R9_TinyCursorBufferStressByteIdentical) {
    ScopedSpimiConfigPin pin;
    constexpr int32_t kTotal = 120'000;
    constexpr int32_t kHalf = 60'000;
    constexpr int32_t kTinyTerms = 20'000;
    auto gen = [](int32_t lo, int32_t hi, SpimiPostingBuffer& buf) {
        // 高 df windowed term（跨全部 spill，positions 抖动防常数收敛）。
        for (int32_t doc = lo; doc < hi; ++doc) {
            if (Jitter(doc, 5, 4) == 0) {
                continue;
            }
            uint32_t p = static_cast<uint32_t>(doc % 5);
            for (int j = 0; j < 2; ++j) {
                buf.Append("r9hot", static_cast<uint32_t>(doc), p);
                p += 1 + Jitter(doc, j + 17, 9);
            }
        }
        // Σdf 跨界三连（与 R2 同构，跨前两段切分点 kHalf）。
        struct Cross {
            const char* name;
            int32_t df_a;
            int32_t df_b;
            int32_t phase;
        };
        static constexpr Cross kCross[] = {
                {"r9cross_511", 256, 255, 7},
                {"r9cross_512", 256, 256, 11},
                {"r9cross_513", 256, 257, 19},
        };
        for (const auto& c : kCross) {
            auto emit_half = [&](int32_t base, int32_t df) {
                for (int32_t i = 0; i < df; ++i) {
                    const int32_t doc = base + i * 200 + c.phase;
                    if (doc < lo || doc >= hi) {
                        continue;
                    }
                    const int freq = 1 + (i % 3);
                    uint32_t p = static_cast<uint32_t>(i % 4);
                    for (int j = 0; j < freq; ++j) {
                        buf.Append(c.name, static_cast<uint32_t>(doc), p);
                        p += 1 + Jitter(i, j, 5);
                    }
                }
            };
            emit_half(/*base=*/0, c.df_a);
            emit_half(/*base=*/kHalf, c.df_b);
        }
        // 小 term 海（inline 路径 + .tis 滑窗高频换页）。
        char name[24];
        for (int32_t i = 0; i < kTinyTerms; ++i) {
            std::snprintf(name, sizeof(name), "r9tiny_%05d", i);
            const int32_t base =
                    static_cast<int32_t>((static_cast<uint64_t>(i) * 2654435761ULL) % 110'000ULL);
            const int df = 1 + (i % 3);
            for (int d = 0; d < df; ++d) {
                const int32_t doc = base + d * 433;
                if (doc < lo || doc >= hi) {
                    continue;
                }
                buf.Append(name, static_cast<uint32_t>(doc), static_cast<uint32_t>(i % 7));
            }
        }
    };
    const MergedRun merged = EmitMergedWithPeak(gen, {0, kHalf, 90'000, kTotal}, /*omit=*/false,
                                                /*cursor_buffer_bytes=*/4096);
    const FourStreams direct = EmitDirect(gen, kTotal, /*omit=*/false);
    ExpectStreamsEqual(direct, merged.streams, "R9");

    // DOCS_ONLY（omit）同口径再压一遍：纯 .frq 流（.prx 长度 0 的游标）。
    auto gen_omit = [&gen](int32_t lo, int32_t hi, SpimiPostingBuffer& buf) { gen(lo, hi, buf); };
    const MergedRun merged_omit =
            EmitMergedWithPeak(gen_omit, {0, kHalf, 90'000, kTotal}, /*omit=*/true,
                               /*cursor_buffer_bytes=*/4096);
    const FourStreams direct_omit = EmitDirect(gen_omit, kTotal, /*omit=*/true);
    ExpectStreamsEqual(direct_omit, merged_omit.streams, "R9-omit");
}

} // namespace doris::segment_v2::inverted_index::spimi
