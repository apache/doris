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

#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "common/status.h"
#include "storage/index/inverted/spimi/posting_buffer.h"
#include "storage/index/inverted/spimi/segment_merger.h"

namespace doris::segment_v2::inverted_index::spimi {

// One spill segment flushed from the posting buffer.
//
// To keep RAM bounded under the 256 MiB budget, each spill's encoded Lucene 2.x
// streams (.tis/.tii/.frq/.prx/.fnm + segments_N/segments.gen) are STREAMED
// directly to PER-STREAM node-local temp files as the encoder emits them — no
// stream is ever fully buffered in RAM. The streams are interleaved during
// EmitSegment (the encoder writes a little .tis, then .frq, then .prx, ...), so
// they cannot share one sequentially-written file; each stream gets its own
// file `<spill_path>.<ext>` (ext in {tis,tii,frq,prx,fnm,segments_n,
// segments_gen}). `spill_path` is the absolute path stem; `<spill_path>.tis`
// etc. are the actual files.
//
// The encoded bytes are byte-for-byte what an in-memory (MemoryByteOutput)
// spill would have held — FileByteOutput::FilePointer() returns the running
// total bytes written (flushed + staged), identical to MemoryByteOutput's
// _bytes.size(), so every cross-stream offset the encoder records is unchanged.
// Hence the final .idx stays byte-identical; only the storage path differs.
struct SpillSegment {
    std::string segment_name; // "_spill_0", "_spill_1", ...
    int32_t doc_count = 0;    // max doc_id + 1 in this spill
    int64_t term_count = 0;   // distinct terms emitted

    // Absolute path stem of this spill's per-stream tmp files. The actual files
    // are `<spill_path>.tis`, `<spill_path>.frq`, etc.
    std::string spill_path;

    // Per-stream length. `length` is the full byte length of the stream's tmp
    // file (== FileByteOutput::FilePointer() at Flush time). `offset` is always
    // 0 now that each stream has its own file; it is retained so existing tests
    // that reference `.length` keep compiling.
    struct Range {
        int64_t offset = 0;
        int64_t length = 0;
    };

    Range tis;
    Range tii;
    Range frq;
    Range prx;
    Range fnm;
    Range segments_n;
    Range segments_gen;

    // Path of one stream's tmp file: `<spill_path>.<ext>`.
    std::string StreamPath(const char* ext) const { return spill_path + "." + ext; }
};

// Manages the spill-to-disk lifecycle of the SPIMI posting buffer. When the
// buffer's `ShouldFlush()` fires (memory budget exceeded), the integration
// layer calls `FlushBuffer()`, which sorts the buffer and emits a complete
// Lucene segment, STREAMING each encoded stream directly to its own node-local
// tmp file via FileByteOutput (no stream is ever fully buffered in RAM), then
// resets the buffer. Only a small per-stream staging buffer is resident during
// the flush, so the ~150-200 MB output-buffer transient of the old
// MemoryByteOutput approach is gone.
//
// At finish() time the integration layer streams each spill back from its tmp
// file (LoadSpill) and hands the resulting SegmentMerger::Input(s) to the
// SegmentMerger for the final k-way merge. Only the live merge working set is
// resident; spilled bytes do NOT count against the 256 MiB budget.
class SpillManager {
public:
    // `is_v4` makes spill segments use kIndexVersionV4 (windowed+inline-capable)
    // in lockstep with the final segment's .fnm. This MUST match the final
    // EmitDirect/EmitMerged index_version so SegmentMerger::MergeSingleInput's
    // byte-copy of a spill's .frq/.prx stays format-consistent with the .fnm it
    // rewrites. Adaptive per-term windowing keeps the df=1 tail legacy either way.
    //
    // `tmp_dir` is the directory under which per-spill tmp files are created. If
    // empty, the manager resolves one itself ($DORIS_SPILL_TMP, else /tmp). A
    // process-unique subdir is appended so concurrent writers never collide and
    // gtest contexts work without a full ExecEnv. tmp files are BE-local only.
    //
    // `omit_term_freq_and_positions` MUST equal the flag the final segment is
    // emitted with (derived from the field's support_phrase property). Spill
    // segments are written in lockstep: when phrase support is off the spill
    // drops freq+positions too, so the .frq/.prx format the k-way merge decodes
    // (has_prox = !omit) matches what the spill actually wrote. Hardcoding false
    // here while the final merge omits positions corrupts a spilled phrase-off
    // field; lockstepping it also skips the wasted position encode + spill IO.
    explicit SpillManager(std::string field_name, bool is_v4 = false, std::string tmp_dir = "",
                          bool omit_term_freq_and_positions = false);

    // Defensively cleans up any remaining spill tmp files and the per-instance
    // spill subdir, so an abandoned manager (no explicit CleanupSpillFiles call)
    // never leaks in /tmp.
    ~SpillManager();

    SpillManager(const SpillManager&) = delete;
    SpillManager& operator=(const SpillManager&) = delete;

    // Sorts `buffer`, emits its contents as a complete Lucene segment, STREAMING
    // each encoded stream directly to its own node-local tmp file
    // (`<spill_path>.tis`, `.frq`, ...) via FileByteOutput, and calls
    // `buffer.Reset()`. `doc_count` is the number of documents the segment
    // covers.
    //
    // Returns the number of distinct terms emitted. Throws doris::Exception on
    // IO failure (create_file/append/close) so it flows through
    // SpimiIndexWriter::Finish's try/catch + FINALLY_CLOSE.
    // After this call, `buffer` is empty and ready for new Appends.
    int64_t FlushBuffer(SpimiPostingBuffer& buffer, int32_t doc_count);

    // 把 `buffer` 在内存中 EmitSegment 成与 LoadSpill 同形的归并输入（四流
    // .tis/.tii/.frq/.prx + doc_count），不落任何 spill tmp 文件。供
    // SpimiIndexWriter::EmitMerged 在 Finish 时处理残余 buffer：直接作为
    // 最后一路归并输入，省去「FlushBuffer 落盘 + LoadSpill 读回」一次磁盘
    // 往返。编码参数与 FlushBuffer 逐项相同（index_version、omit、
    // omit_norms=true、inline_small_terms=true），且 MemoryByteOutput 与
    // FileByteOutput 的 FilePointer() 字节流恒等（见 SpillSegment 注释），
    // 因此产出的四流与「先落盘再读回」逐字节相同，最终归并产物不变。
    //
    // 与 FlushBuffer 一致地 Reset() buffer 并复位 flush 锁存。空 buffer
    //（RecordCount()==0）只复位并返回 false，`out` 不被触碰、不得参与归并
    //（镜像 FlushBuffer 的空跳过）。不触 _spills/_spill_counter。
    bool EmitBufferToInput(SpimiPostingBuffer& buffer, int32_t doc_count,
                           SegmentMerger::Input& out) const;

    // Streams spill `i`'s .tis/.tii/.frq/.prx back from their per-stream tmp
    // files into `out` (the only streams the merge consumes). Resident cost is
    // exactly one spill's four streams. Returns an error Status on IO failure.
    //
    // 生产归并已改走 OpenSpillCursor（流式游标）；本接口保留给测试/诊断。
    Status LoadSpill(size_t i, SegmentMerger::Input& out) const;

    // 游标读缓冲默认容量（每流一个，按 min(容量, 流长) 一次性分配）。
    static constexpr size_t kDefaultCursorBufferBytes = 1U << 20; // 1 MiB

    // 以顺序读滑窗游标打开 spill `i` 的四个归并消费流，替代 LoadSpill 的
    // 全量载回：k 路归并期间的输入驻留从 Σspill 字节降到
    // k×4×min(buffer_bytes, 流长)。spill 布局保证 k×4 顺序游标可行 ——
    // .tis 按 term 序 emit、.frq/.prx posting 指针随 term 序单调不回退
    // （TermDictWriter::Add 的 DCHECK），TermEnum / PostingDecoder 只前进。
    // IO 打开失败返回错误 Status；打开后的读错误由游标抛 doris::Exception
    // （与 LoadSpill 的错误传播一致，经 Finish 的 try/catch 收口）。
    Status OpenSpillCursor(size_t i, SegmentMerger::StreamInput& out,
                           size_t buffer_bytes = kDefaultCursorBufferBytes) const;

    size_t SpillCount() const { return _spills.size(); }
    const std::vector<SpillSegment>& Spills() const { return _spills; }

    // 本 manager 生命周期内创建过的 spill 总数（单调递增：CleanupSpillFiles
    // 清空 _spills 但不回退计数）。用于「Finish 阶段不得新建 spill」一类的
    // 事后断言——Finish 末尾会清理 spill 文件，SpillCount() 观测不到过程量。
    int32_t TotalSpillsCreated() const { return _spill_counter; }

    // Best-effort deletes every spill tmp file (idempotent; NOT_FOUND ignored)
    // then clears the metadata. Called after the merge produces the final
    // segment AND on the exception path, so tmp files never leak.
    void CleanupSpillFiles();

    // Bytes of spilled data RESIDENT in RAM. Now that spills live on disk this
    // is only the small per-spill metadata (~0), so SpimiIndexWriter::MemoryUsage
    // reflects true resident RAM against the 256 MiB budget.
    size_t TotalSpillBytes() const;

private:
    // Resolves + lazily creates the tmp directory for spill files.
    Status EnsureTmpDir();

    std::string _field_name;
    bool _is_v4 = false;
    // Lockstep with the final segment's omit flag (see ctor doc). Spill segments
    // omit freq+positions iff the field's index will.
    bool _omit_tfap = false;
    std::string _tmp_dir;        // resolved spill directory (created lazily)
    bool _tmp_dir_ready = false; // whether _tmp_dir has been created
    std::vector<SpillSegment> _spills;
    int32_t _spill_counter = 0;
};

} // namespace doris::segment_v2::inverted_index::spimi
