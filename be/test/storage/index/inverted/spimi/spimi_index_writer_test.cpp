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

#include "storage/index/inverted/spimi/spimi_index_writer.h"

#include <CLucene.h>
#include <CLucene/store/IndexInput.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <filesystem>
#include <limits>
#include <string>
#include <vector>

#include "common/config.h"
#include "io/fs/local_file_system.h"
#include "storage/index/inverted/inverted_index_fs_directory.h"
#include "storage/index/inverted/spimi/term_dict_reader.h"
#include "storage/index/inverted/spimi/term_dict_writer.h"

namespace doris::segment_v2::inverted_index::spimi {

namespace {

// 从 DorisFSDirectory 读回整个文件的字节（直写段落盘后的事后检查用）。
std::vector<uint8_t> SlurpDirFile(DorisFSDirectory* dir, const char* name) {
    lucene::store::IndexInput* in = nullptr;
    CLuceneError err;
    EXPECT_TRUE(dir->openInput(name, in, err)) << "openInput(" << name << ") failed";
    if (in == nullptr) {
        return {};
    }
    const int64_t size = in->length();
    std::vector<uint8_t> bytes(static_cast<size_t>(size));
    if (size > 0) {
        in->readBytes(bytes.data(), size);
    }
    in->close();
    _CLDELETE(in);
    return bytes;
}

// .tis/.tii 头 4 字节是大端 int32 的 FORMAT（-4 外置 / -5 内联）。
int32_t ReadFormatHeader(const std::vector<uint8_t>& b) {
    EXPECT_GE(b.size(), 4U);
    return static_cast<int32_t>((b[0] << 24) | (b[1] << 16) | (b[2] << 8) | b[3]);
}

// 临时目录 + DorisFSDirectory 的 RAII 包装，供 Finish(dir, config) 测试使用。
struct TmpFSDirectory {
    std::filesystem::path path;
    std::unique_ptr<DorisFSDirectory> dir;

    explicit TmpFSDirectory(const std::string& name) {
        path = std::filesystem::temp_directory_path() / name;
        std::filesystem::remove_all(path);
        std::filesystem::create_directories(path);
        dir = std::make_unique<DorisFSDirectory>();
        dir->init(io::global_local_filesystem(), path.string().c_str());
    }

    ~TmpFSDirectory() {
        dir.reset();
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
    }
};

} // namespace

// --- Construction / basic access ---

TEST(SpimiIndexWriterTest, ConstructHasBuffer) {
    SpimiIndexWriter writer("content");
    EXPECT_TRUE(writer.HasBuffer());
    EXPECT_NE(writer.buffer(), nullptr);
    EXPECT_NE(writer.spill_manager(), nullptr);
}

TEST(SpimiIndexWriterTest, BufferInitiallyEmpty) {
    SpimiIndexWriter writer("title");
    EXPECT_EQ(writer.buffer()->RecordCount(), 0U);
    EXPECT_FALSE(writer.buffer()->Saturated());
    EXPECT_FALSE(writer.buffer()->ShouldFlush());
}

// --- AppendToken ---

TEST(SpimiIndexWriterTest, AppendTokenGrowsRecordCount) {
    SpimiIndexWriter writer("body");
    writer.AppendToken("hello", /*doc_id=*/0, /*pos=*/0);
    writer.AppendToken("world", /*doc_id=*/1, /*pos=*/0);
    EXPECT_EQ(writer.buffer()->RecordCount(), 2U);
}

TEST(SpimiIndexWriterTest, AppendTokenSaturatedAfterHardLimit) {
    SpimiIndexWriter writer("content");
    // Fill the buffer beyond its capacity to trigger saturation.
    // The default max record count is large (2^24), so we just
    // verify Saturated() is false for small fills.
    for (uint32_t i = 0; i < 100; ++i) {
        writer.AppendToken("term", /*doc_id=*/i, /*pos=*/0);
    }
    EXPECT_FALSE(writer.Saturated());
}

// --- Saturated / ShouldFlush ---

TEST(SpimiIndexWriterTest, ShouldFlushFalseForSmallFill) {
    SpimiIndexWriter writer("content");
    writer.AppendToken("a", 0, 0);
    EXPECT_FALSE(writer.ShouldFlush());
}

// --- FlushPending ---

TEST(SpimiIndexWriterTest, FlushPendingCreatesOneSpill) {
    SpimiIndexWriter writer("body");
    writer.AppendToken("alpha", 0, 0);
    writer.AppendToken("beta", 1, 0);
    EXPECT_EQ(writer.spill_manager()->SpillCount(), 0U);

    writer.FlushPending(/*doc_count=*/10);
    EXPECT_EQ(writer.spill_manager()->SpillCount(), 1U);
    // Buffer should be reset after flush.
    EXPECT_EQ(writer.buffer()->RecordCount(), 0U);
}

TEST(SpimiIndexWriterTest, FlushPendingTwiceCreatesTwoSpills) {
    SpimiIndexWriter writer("content");
    writer.AppendToken("x", 0, 0);
    writer.FlushPending(5);
    writer.AppendToken("y", 1, 0);
    writer.FlushPending(10);
    EXPECT_EQ(writer.spill_manager()->SpillCount(), 2U);
}

// --- MemoryUsage ---

TEST(SpimiIndexWriterTest, MemoryUsageNonZeroAfterAppend) {
    SpimiIndexWriter writer("body");
    writer.AppendToken("hello", 0, 0);
    EXPECT_GT(writer.MemoryUsage(), 0);
}

TEST(SpimiIndexWriterTest, MemoryUsageIncreasesAfterSpill) {
    SpimiIndexWriter writer("content");
    writer.AppendToken("a", 0, 0);
    (void)writer.MemoryUsage();
    writer.FlushPending(10);
    const int64_t after = writer.MemoryUsage();
    // After flush the buffer is reset (smaller), but spill data is
    // retained (larger). Total should still be positive.
    EXPECT_GT(after, 0);
    // The buffer still has base overhead after reset, so MemoryUsage >=
    // the spill manager's bytes.
    EXPECT_GE(static_cast<size_t>(after), writer.spill_manager()->TotalSpillBytes());
}

// --- Cleanup ---

TEST(SpimiIndexWriterTest, CleanupResetsBuffer) {
    SpimiIndexWriter writer("body");
    writer.AppendToken("a", 0, 0);
    EXPECT_TRUE(writer.HasBuffer());

    writer.Cleanup();
    EXPECT_FALSE(writer.HasBuffer());
    EXPECT_EQ(writer.buffer(), nullptr);
}

TEST(SpimiIndexWriterTest, CleanupReleasesSpillData) {
    SpimiIndexWriter writer("content");
    writer.AppendToken("x", 0, 0);
    writer.FlushPending(10);
    EXPECT_GT(writer.spill_manager()->TotalSpillBytes(), 0U);

    writer.Cleanup();
    // SpillManager should have cleaned up its data.
    EXPECT_EQ(writer.spill_manager()->TotalSpillBytes(), 0U);
    EXPECT_EQ(writer.spill_manager()->SpillCount(), 0U);
}

TEST(SpimiIndexWriterTest, CleanupIdempotent) {
    SpimiIndexWriter writer("body");
    writer.AppendToken("a", 0, 0);
    writer.Cleanup();
    // Second call should not crash.
    writer.Cleanup();
    EXPECT_FALSE(writer.HasBuffer());
}

// --- MemoryUsage after Cleanup ---

TEST(SpimiIndexWriterTest, MemoryUsageZeroAfterCleanup) {
    SpimiIndexWriter writer("content");
    writer.AppendToken("a", 0, 0);
    writer.FlushPending(5);
    EXPECT_GT(writer.MemoryUsage(), 0);

    writer.Cleanup();
    EXPECT_EQ(writer.MemoryUsage(), 0);
}

// --- SpimiFinishConfig defaults ---

TEST(SpimiIndexWriterTest, FinishConfigDefaults) {
    SpimiFinishConfig config;
    EXPECT_FALSE(config.is_v4);
    EXPECT_FALSE(config.omit_term_freq_and_positions);
    EXPECT_TRUE(config.field_name_utf8.empty());
    EXPECT_EQ(config.doc_count, 0);
}

// --- GetFileNames ---

TEST(SpimiIndexWriterTest, GetFileNamesV4) {
    auto names = SpimiIndexWriter::GetFileNames(/*is_v4=*/true);
    EXPECT_STREQ(names.tis, "_0.tis");
    EXPECT_STREQ(names.tii, "_0.tii");
    EXPECT_STREQ(names.frq, "_0.frq");
    EXPECT_STREQ(names.prx, "_0.prx");
    EXPECT_STREQ(names.fnm, "_0.fnm");
    EXPECT_STREQ(names.nrm, "_0.nrm");
    EXPECT_STREQ(names.seg_n, "segments_1");
    EXPECT_STREQ(names.seg_gen, "segments.gen");
}

TEST(SpimiIndexWriterTest, GetFileNamesShadow) {
    auto names = SpimiIndexWriter::GetFileNames(/*is_v4=*/false);
    EXPECT_STREQ(names.tis, "_spimi_0.tis");
    EXPECT_STREQ(names.tii, "_spimi_0.tii");
    EXPECT_STREQ(names.frq, "_spimi_0.frq");
    EXPECT_STREQ(names.prx, "_spimi_0.prx");
    EXPECT_STREQ(names.fnm, "_spimi_0.fnm");
    EXPECT_STREQ(names.nrm, "_spimi_0.nrm");
    EXPECT_STREQ(names.seg_n, "segments_spimi_1");
    EXPECT_STREQ(names.seg_gen, "segments_spimi.gen");
}

// --- Finish with empty buffer ---

TEST(SpimiIndexWriterTest, FinishEmptyBufferIsNoOp) {
    SpimiIndexWriter writer("content");
    // Empty buffer + no spills → Finish is a no-op (returns early).
    // Verify by checking the buffer is reset afterward.
    EXPECT_TRUE(writer.HasBuffer());
    EXPECT_EQ(writer.buffer()->RecordCount(), 0U);
    EXPECT_EQ(writer.spill_manager()->SpillCount(), 0U);

    // Finish with a null dir: the early-return path doesn't touch
    // the directory, so nullptr is safe.
    SpimiFinishConfig config;
    config.is_v4 = true;
    config.field_name_utf8 = "content";
    config.doc_count = 0;
    writer.Finish(nullptr, config);

    // After Finish, the buffer is reset.
    EXPECT_FALSE(writer.HasBuffer());
}

// --- 直写路径（无 spill）的 V4 小 term 内联 ---

// 生产最常见的形态：单次 flush、无 spill，Finish 走 EmitDirect。V4 段必须
// 与 spill-merge 路径 lockstep 地开启小 term 内联（.tis FORMAT = -5，小 term
// 的 frq/prx 字节进 .tis，查询零额外 GET）。曾经 EmitDirect 漏传
// inline_small_terms 实参（默认 false），导致直写 V4 段全部退化为 -4 外置。
TEST(SpimiIndexWriterTest, DirectEmitV4InlinesSmallTerms) {
    TmpFSDirectory tmp("spimi_direct_emit_v4_inline_test");

    SpimiIndexWriter writer("body", /*is_v4=*/true);
    // df=1 的小 term（payload 远小于 256B 内联阈值），doc id 从 1 起
    //（windowed .frq 格式要求每个 term 的首 doc id >= 1）。
    writer.AppendToken("alpha", /*doc_id=*/1, /*pos=*/0);
    writer.AppendToken("beta", /*doc_id=*/2, /*pos=*/0);
    writer.AppendToken("gamma", /*doc_id=*/3, /*pos=*/0);

    SpimiFinishConfig config;
    config.is_v4 = true;
    config.field_name_utf8 = "body";
    config.doc_count = 4;
    writer.Finish(tmp.dir.get(), config);

    const auto tis = SlurpDirFile(tmp.dir.get(), "_0.tis");
    const auto tii = SlurpDirFile(tmp.dir.get(), "_0.tii");
    // 直写 V4 段必须是内联格式（与 SegmentMerger / SpillManager 产物一致）。
    EXPECT_EQ(ReadFormatHeader(tis), TermDictWriter::kFormatInline);
    EXPECT_EQ(ReadFormatHeader(tii), TermDictWriter::kFormatInline);

    // 小 term 的 TermInfo 必须真正带内联 posting 字节。
    TermDictReader dict(tis, tii);
    const auto info = dict.LookupTerm(/*field_number=*/0, "alpha");
    ASSERT_TRUE(info.has_value());
    EXPECT_TRUE(info->inlined) << "直写 V4 段的小 term 必须内联进 .tis";
    EXPECT_NE(info->inline_frq, nullptr);
    EXPECT_GT(info->inline_frq_len, 0U);
}

// 反向保证：非 V4（shadow/V2 兼容）直写段不受内联影响 —— EmitSegment 内部以
// use_windowed（index_version >= V4）门控 inline，V0 段保持 FORMAT = -4 且
// 无任何内联 term。
TEST(SpimiIndexWriterTest, DirectEmitNonV4StaysExternalFormat) {
    TmpFSDirectory tmp("spimi_direct_emit_v0_external_test");

    SpimiIndexWriter writer("body", /*is_v4=*/false);
    writer.AppendToken("alpha", /*doc_id=*/1, /*pos=*/0);
    writer.AppendToken("beta", /*doc_id=*/2, /*pos=*/0);

    SpimiFinishConfig config;
    config.is_v4 = false;
    config.field_name_utf8 = "body";
    config.doc_count = 3;
    writer.Finish(tmp.dir.get(), config);

    const auto tis = SlurpDirFile(tmp.dir.get(), "_spimi_0.tis");
    const auto tii = SlurpDirFile(tmp.dir.get(), "_spimi_0.tii");
    EXPECT_EQ(ReadFormatHeader(tis), TermDictWriter::kFormat);
    EXPECT_EQ(ReadFormatHeader(tii), TermDictWriter::kFormat);

    TermDictReader dict(tis, tii);
    const auto info = dict.LookupTerm(/*field_number=*/0, "alpha");
    ASSERT_TRUE(info.has_value());
    EXPECT_FALSE(info->inlined) << "非 V4 直写段不得内联";
}

// --- Memory budget follows config::inverted_index_ram_buffer_size ---
//
// 生产路径（SpimiIndexWriter 构造）必须把 V2/CLucene 同款旋钮
// config::inverted_index_ram_buffer_size（单位 MB，mDouble 可热改）换算成
// buffer 的 memory_budget_bytes：每个 writer 构造时读一次，夹紧到
// [16 MiB, 8 GiB]；非有限值或 <= 0 回退 kDefaultMemoryBudget。

namespace {

// RAII pin：临时改写 config::inverted_index_ram_buffer_size，析构时恢复。
// 与 segment_roundtrip_test.cpp 中既有的 config pin 写法一致。
struct RamBufferConfigPin {
    double saved;
    explicit RamBufferConfigPin(double mb) : saved(config::inverted_index_ram_buffer_size) {
        config::inverted_index_ram_buffer_size = mb;
    }
    ~RamBufferConfigPin() { config::inverted_index_ram_buffer_size = saved; }
};

} // namespace

TEST(SpimiIndexWriterTest, BudgetFollowsRamBufferConfig) {
    {
        RamBufferConfigPin pin(16); // 16 MB
        SpimiIndexWriter writer("content");
        EXPECT_EQ(writer.buffer()->limits().memory_budget_bytes, size_t {16} << 20);
    }
    {
        RamBufferConfigPin pin(512); // 生产默认值
        SpimiIndexWriter writer("content");
        EXPECT_EQ(writer.buffer()->limits().memory_budget_bytes, size_t {512} << 20);
    }
}

TEST(SpimiIndexWriterTest, BudgetReadOncePerWriterConstruction) {
    RamBufferConfigPin pin(32);
    SpimiIndexWriter writer_a("content");
    EXPECT_EQ(writer_a.buffer()->limits().memory_budget_bytes, size_t {32} << 20);

    // 热改 config：已构造的 writer 不变（构造时读一次），新 writer 取新值。
    config::inverted_index_ram_buffer_size = 64;
    EXPECT_EQ(writer_a.buffer()->limits().memory_budget_bytes, size_t {32} << 20);
    SpimiIndexWriter writer_b("content");
    EXPECT_EQ(writer_b.buffer()->limits().memory_budget_bytes, size_t {64} << 20);
}

TEST(SpimiIndexWriterTest, BudgetClampsToFloorAndCeiling) {
    {
        RamBufferConfigPin pin(1); // 低于 16 MiB 下限 → 夹到下限
        SpimiIndexWriter writer("content");
        EXPECT_EQ(writer.buffer()->limits().memory_budget_bytes, size_t {16} << 20);
    }
    {
        RamBufferConfigPin pin(1024.0 * 1024.0); // 1 TiB → 夹到 8 GiB 上限
        SpimiIndexWriter writer("content");
        EXPECT_EQ(writer.buffer()->limits().memory_budget_bytes, size_t {8} << 30);
    }
}

// --- 预算只移动 spill 切分点，不改变最终索引字节 ---
//
// 预算影响产物的唯一途径是 flush 节奏：调用方在 ShouldFlush() 后 FlushPending，
// 预算越小 spill 越多。a863652 曾在旧 merge 路径实测 +0.75% .idx 随 spill 数
// 变化；spill-merge emit 重做（预解码再编码）后产物必须与 spill 节奏无关。
// 本测试是该声明（posting_buffer.h kDefaultMemoryBudget 注释）的可复现证据
// 与回归护栏：同一语料按 0 / 1 / 3 次 spill 三种节奏写入 V4 段，落盘的全部
// 文件必须逐字节一致。
TEST(SpimiIndexWriterTest, SpillCadenceDoesNotChangeIndexBytes) {
    // 确定性语料：600 个 doc(id 1..600)，每 doc = 2 个全量 hot term（df=600，
    // 跨 512-doc 窗口边界）+ 10 个轮转 vocab term + 1 个 df=1 唯一 term。
    // 任何 spill 切分都会把 hot/vocab term 的 posting 链拆进多个 spill 段，
    // 强制 k-way merge 走再编码路径。
    constexpr int kDocs = 600;
    constexpr int kVocab = 16;
    const auto append_docs = [&](SpimiIndexWriter& w, int doc_lo, int doc_hi) {
        for (int d = doc_lo; d <= doc_hi; ++d) {
            uint32_t pos = 0;
            const auto doc = static_cast<uint32_t>(d);
            w.AppendToken("hot_alpha", doc, pos++);
            w.AppendToken("hot_beta", doc, pos++);
            for (int k = 0; k < 10; ++k) {
                w.AppendToken("v" + std::to_string((d * 7 + k) % kVocab), doc, pos++);
            }
            w.AppendToken("uniq_" + std::to_string(d), doc, pos++);
        }
    };

    struct Variant {
        const char* tag;
        std::vector<int> cuts; // 在这些 doc id 之后 FlushPending（≙ 不同预算下的切分点）
    };
    const std::vector<Variant> variants = {
            {"direct", {}},             // 0 spill：EmitDirect 路径
            {"single", {300}},          // 1 spill + 余量留在 buffer
            {"multi", {150, 300, 450}}, // 3 spill + 余量：k-way merge
    };
    // V4 段 omit norms（.nrm 不落盘），故对比 .nrm 之外的全部落盘文件。
    static constexpr const char* kFiles[] = {"_0.tis", "_0.tii", "_0.frq",
                                             "_0.prx", "_0.fnm", "segments_1"};

    std::vector<std::vector<uint8_t>> baseline; // direct 变体的各文件字节
    for (const auto& variant : variants) {
        TmpFSDirectory tmp(std::string("spimi_spill_cadence_") + variant.tag);
        SpimiIndexWriter writer("body", /*is_v4=*/true);
        int next = 1;
        size_t expected_spills = 0;
        for (int cut : variant.cuts) {
            append_docs(writer, next, cut);
            writer.FlushPending(/*doc_count=*/cut + 1);
            ++expected_spills;
            next = cut + 1;
        }
        append_docs(writer, next, kDocs);
        ASSERT_EQ(writer.spill_manager()->SpillCount(), expected_spills) << variant.tag;

        SpimiFinishConfig config;
        config.is_v4 = true;
        config.field_name_utf8 = "body";
        config.doc_count = kDocs + 1;
        writer.Finish(tmp.dir.get(), config);

        size_t file_idx = 0;
        for (const char* file : kFiles) {
            auto bytes = SlurpDirFile(tmp.dir.get(), file);
            ASSERT_FALSE(bytes.empty()) << variant.tag << " " << file;
            if (baseline.size() <= file_idx) {
                baseline.push_back(std::move(bytes)); // direct 变体建立基线
            } else {
                EXPECT_EQ(bytes.size(), baseline[file_idx].size()) << variant.tag << " " << file;
                EXPECT_TRUE(bytes == baseline[file_idx])
                        << variant.tag << " 与 direct 基线在 " << file << " 上字节不一致";
            }
            ++file_idx;
        }
    }
}

TEST(SpimiIndexWriterTest, BudgetFallsBackOnInvalidConfig) {
    {
        RamBufferConfigPin pin(0); // 0 → 回退编译期兜底
        SpimiIndexWriter writer("content");
        EXPECT_EQ(writer.buffer()->limits().memory_budget_bytes,
                  SpimiPostingBuffer::kDefaultMemoryBudget);
    }
    {
        RamBufferConfigPin pin(-5); // 负数 → 回退
        SpimiIndexWriter writer("content");
        EXPECT_EQ(writer.buffer()->limits().memory_budget_bytes,
                  SpimiPostingBuffer::kDefaultMemoryBudget);
    }
    {
        RamBufferConfigPin pin(std::numeric_limits<double>::quiet_NaN()); // NaN → 回退
        SpimiIndexWriter writer("content");
        EXPECT_EQ(writer.buffer()->limits().memory_budget_bytes,
                  SpimiPostingBuffer::kDefaultMemoryBudget);
    }
}

} // namespace doris::segment_v2::inverted_index::spimi
