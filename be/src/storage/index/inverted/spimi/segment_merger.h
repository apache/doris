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
#include <memory>
#include <string>
#include <vector>

#include "storage/index/inverted/spimi/byte_output.h"
#include "storage/index/inverted/spimi/byte_source.h"
#include "storage/index/inverted/spimi/fulltext_writer.h"

namespace doris::segment_v2::inverted_index::spimi {

// K-way merge of multiple Lucene 2.x segments produced by the spill
// path.  All input segments are successive slices of the SAME
// monotonically increasing _rid (doc_id) stream: the write path appends
// every token with the absolute row id and spills the buffer at row
// boundaries, so each segment already carries GLOBAL, absolute doc_ids
// whose ranges never overlap across inputs.  The merge therefore
// concatenates the already-ordered posting runs verbatim and must NOT
// apply any per-segment doc_id offset — doing so double-shifts every doc
// after the first spill and pushes doc_ids past total_doc_count (see
// SegmentMergerTest.MultiSpillAbsoluteDocIdsArePreserved).
//
// Inputs:
//   - N spill segments from SpillManager (each flushed when the
//     posting buffer exceeded the memory budget)
//   - Optionally, one "final" segment produced from the remaining
//     buffer at finish() time (its doc_ids are likewise global)
//
// Output: a single merged segment written to the caller-provided
// SpimiSegmentSink.
class SegmentMerger {
public:
    // One input segment.  All byte vectors must contain a complete
    // .tis/.tii/.frq/.prx as produced by SpimiFulltextWriter::
    // EmitSegment (or equivalently, SegmentWriter + TermDictWriter).
    // `doc_count` is the cumulative doc_count recorded when this segment was
    // spilled (max absolute doc_id + 1). It is retained as metadata only:
    // because inputs carry global absolute doc_ids, the merge does NOT use it to
    // offset doc_ids. The merged segment's doc_count is passed separately as
    // `total_doc_count`.
    struct Input {
        std::vector<uint8_t> tis_bytes;
        std::vector<uint8_t> tii_bytes;
        std::vector<uint8_t> frq_bytes;
        std::vector<uint8_t> prx_bytes;
        int32_t doc_count = 0;
    };

    // 流式输入：同一段的四条前向游标（spill 文件滑窗游标或内存游标）。
    // 这是核心 Merge 的入参形态 —— 归并期每输入只驻留 4×min(缓冲, 流长)
    // 的读窗口，Σspill 字节不再同时进内存。`doc_count` 语义同 Input（仅
    // 元数据）。游标由 StreamInput 持有，须覆盖完整流且定位在流首。
    struct StreamInput {
        std::unique_ptr<ForwardByteSource> tis;
        std::unique_ptr<ForwardByteSource> tii;
        std::unique_ptr<ForwardByteSource> frq;
        std::unique_ptr<ForwardByteSource> prx;
        int32_t doc_count = 0;
    };

    // 把内存 Input（如残余 buffer 的 EmitBufferToInput 产物）包成持有型
    // 游标输入（字节 move 进游标，无拷贝）。
    static StreamInput WrapOwnedInput(Input&& in);

    // Merges `inputs` into a single output segment.
    //
    // `sink`: the ByteOutput streams for the merged segment.
    // `segment_name`: written into the segments_N manifest.
    // `field_name`: written into the .fnm.
    // `total_doc_count`: the merged segment's doc_count (typically
    //   `_spimi_doc_count` from the integration layer).
    // `index_version`: field-info version tag.
    // `omit_term_freq_and_positions`: field-level flag propagated
    //   to the encoder and .fnm.
    // `omit_norms`: field-level flag propagated to .fnm.
    //
    // Returns the number of distinct terms in the merged segment.
    //
    // 内存 Input 重载是核心（游标）重载的薄包装：对每路输入构造借用型
    // MemoryByteSource —— 两条入口产出逐字节相同的归并段。
    static int64_t Merge(const std::vector<Input>& inputs, const SpimiSegmentSink& sink,
                         const std::string& segment_name, const std::string& field_name,
                         int32_t total_doc_count, int32_t index_version,
                         bool omit_term_freq_and_positions, bool omit_norms);

    static int64_t Merge(std::vector<StreamInput>& inputs, const SpimiSegmentSink& sink,
                         const std::string& segment_name, const std::string& field_name,
                         int32_t total_doc_count, int32_t index_version,
                         bool omit_term_freq_and_positions, bool omit_norms);

    // 观测计数器（thread_local —— 一次 Merge 在单线程内完成）：按 dispatch
    // 优先级统计三条路径的命中（k=1 整段字节拷贝 > k>1 slim 字节拼接 > 平面
    // 化重编）。测试 / 基准用它核对 verbatim 命中率；生产逻辑从不读取。
    struct MergeStats {
        int64_t single_input_segments = 0; // k=1：整段 posting 字节拷贝
        int64_t slim_concat_terms = 0;     // k>1：Σdf<skip_interval 的字节级拼接
        int64_t reencode_terms = 0;        // k>1：平面化重编（windowed/PFOR/强制 slim）
    };
    static const MergeStats& Stats();
    static void ResetStats();

    // 测试钩子：强制 k>1 的 slim term 走平面化重编慢路径 —— 用于「拼接输出
    // == 重编输出」的交叉字节断言（spill_merge_slim_concat_test）。生产恒
    // false。
    static void SetForceSlimReencodeForTest(bool v);
};

} // namespace doris::segment_v2::inverted_index::spimi
