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

#include "storage/index/inverted/spimi/byte_source.h"
#include "storage/index/inverted/spimi/term_dict_writer.h"

namespace doris::segment_v2::inverted_index::spimi {

// One term entry recovered from a sequential scan of a `.tis` file.
// `term_utf8` is the fully-reconstructed term text (after prefix
// decompression), converted back from the on-disk wide-char modified
// UTF-8 to standard UTF-8.  `info` carries absolute (non-delta)
// pointers into the companion `.frq` / `.prx` streams.
struct TermEntry {
    int32_t field_number = 0;
    std::string term_utf8;
    TermInfo info {};
};

// Forward-only sequential iterator over the `.tis` entries produced
// by `TermDictWriter`.  Parses the header, then each call to `Next()`
// decodes one entry until the footer is reached or `Done()` returns
// true.
//
// This is the sequential-scan counterpart to `TermDictReader` (which
// does point queries via the `.tii` sparse index).  TermEnum is used
// by the segment merger to walk every term in sorted order.
//
// 两种背书：
//   - 整段内存（旧 ctor）：`.tis` 字节按引用借用、须比 enum 长寿；inline
//     span 直接指进该缓冲（生命周期 = 缓冲生命周期，既有契约不变）。
//   - 前向滑窗源（流式 ctor）：spill 文件游标顺序解析，整段 .tis 不再驻留；
//     inline span 拷入 enum 私有 scratch，仅在下一次 `Next()` 之前有效
//     （归并的 take() 即时拷走，≤ inline 上限的有界拷贝）。
class TermEnum {
public:
    // `tis_bytes` must include the 8-byte footer emitted by
    // `TermDictWriter::Close()`.
    explicit TermEnum(const std::vector<uint8_t>& tis_bytes);

    // 流式构造：`src` 是定位在流首、覆盖完整 .tis（含 8 字节 footer）的
    // 前向源，由调用方持有且须比 enum 长寿。footer 经 ReadAt 一次性随机读，
    // 主扫描严格顺序前进。
    explicit TermEnum(ForwardByteSource* src);

    TermEnum(const TermEnum&) = delete;
    TermEnum& operator=(const TermEnum&) = delete;

    // Advances to the next term.  Returns true if an entry was
    // decoded; returns false when all entries have been consumed
    // (the footer's declared count is reached).
    bool Next();

    // The most recently decoded entry.  Only valid after `Next()`
    // returned true.
    const TermEntry& Current() const { return _current; }

    // True once all entries have been consumed.
    bool Done() const { return _done; }

    // Number of entries declared in the `.tis` footer.
    int64_t TotalEntries() const { return _total_entries; }

    int32_t IndexInterval() const { return _index_interval; }
    int32_t SkipInterval() const { return _skip_interval; }

private:
    // 头/尾解析 + 状态初始化（两个 ctor 共用）。
    void Init();

    std::unique_ptr<MemoryByteSource> _owned_src; // 旧 ctor 的内存借用源
    ForwardByteSource* _src = nullptr;            // 实际解析源（永不为空）
    int64_t _data_end = 0;                        // footer 起始偏移（== Length() - 8）
    int32_t _index_interval = 0;
    int32_t _skip_interval = 0;
    // True when the .tis was written in kFormatInline (-5).
    bool _inline_format = false;
    int64_t _total_entries = 0; // from footer
    int64_t _consumed = 0;      // entries decoded so far
    bool _done = false;

    // Running prefix-decode state (inherited across entries).
    std::wstring _last_term;
    int64_t _last_freq_pointer = 0;
    int64_t _last_prox_pointer = 0;

    // 流式源下当前 entry 的 inline span 拷贝（frq 在前、prx 紧随；内存源
    // 走借用路径，此缓冲恒空）。
    std::vector<uint8_t> _inline_scratch;

    TermEntry _current;
};

} // namespace doris::segment_v2::inverted_index::spimi
