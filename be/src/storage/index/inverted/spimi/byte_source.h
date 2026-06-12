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

#include <cstddef>
#include <cstdint>
#include <vector>

#include "io/fs/file_reader.h"

namespace doris::segment_v2::inverted_index::spimi {

// 归并输入侧的「前向滑窗字节源」：TermEnum / PostingDecoder 经它顺序消费一条
// 输入流（.tis/.tii/.frq/.prx）。热路径（ReadByte/VInt）是窗口内的内联指针
// 算术，只有窗口耗尽时才落到虚函数 Refill() —— 内存源的 Refill 直接按
// corrupt 抛错（窗口即整个缓冲），文件源按固定容量 pread 滑动窗口，使 k 路
// 归并的输入驻留从 Σspill 字节降到 k×4×min(容量, 流长)。
//
// 约定：
//   - 严格前向：SkipForwardTo 只允许向前（posting 指针随 term 序单调，
//     spill 布局保证，见 spill_manager.h / term_dict_writer.cpp 的 DCHECK）。
//   - 越过流尾的任何读取按 SPIMI_THROW_CORRUPT 抛出（与各解码器既有的
//     bounds 语义一致）；文件 IO 失败抛 doris::Exception（与 LoadSpill 的
//     错误传播路径一致，经 SpimiIndexWriter::Finish 的 try/catch 收口）。
//   - ReadAt 是不动游标的一次性随机小读（仅 .tis footer 这类场景）。
class ForwardByteSource {
public:
    virtual ~ForwardByteSource() = default;

    ForwardByteSource(const ForwardByteSource&) = delete;
    ForwardByteSource& operator=(const ForwardByteSource&) = delete;

    int64_t Length() const { return _len; }
    int64_t Position() const { return _window_base + static_cast<int64_t>(_wpos); }
    int64_t Remaining() const { return _len - Position(); }

    uint8_t ReadByte() {
        if (_wpos >= _wlen) [[unlikely]] {
            RefillForRead();
        }
        return _window[_wpos++];
    }

    // LEB128 VInt / VLong（shift 越界按 corrupt 抛出，与 posting_decoder /
    // term_enum 既有 Cursor 的加固语义一致）。
    int32_t ReadVInt();
    int64_t ReadVLong();

    // 大端定长整数（.tis 头/尾使用）。
    int32_t ReadInt32BE();
    int64_t ReadInt64BE();

    // 追加读取 n 字节到 out（先做 Remaining 边界检查，杜绝坏长度触发的
    // 超量分配）。
    void ReadInto(std::vector<uint8_t>* out, size_t n);

    // 前向绝对定位（target ∈ [Position(), Length()]，否则 corrupt）。
    void SkipForwardTo(int64_t target);

    // 借用语义探测：内存源返回指向接下来 n 字节的稳定指针并前进游标（指针
    // 生命周期 = 底层缓冲，与 TermEnum 旧的「.tis 借用」契约一致）；文件源
    // 返回 nullptr，调用方改走拷贝路径。
    virtual const uint8_t* BorrowStable(size_t n) = 0;

    // 不动游标的一次性随机读（越界按 corrupt 抛出）。
    virtual void ReadAt(int64_t offset, uint8_t* dst, size_t n) const = 0;

protected:
    ForwardByteSource() = default;

    // 把窗口推进到当前 Position() 并装入至少 1 字节；流尾时按 corrupt 抛出。
    virtual void Refill() = 0;

    const uint8_t* _window = nullptr;
    size_t _wpos = 0;
    size_t _wlen = 0;
    int64_t _window_base = 0; // _window[0] 的流内绝对偏移
    int64_t _len = 0;

private:
    void RefillForRead();
};

// 整段内存上的源：窗口即整个缓冲，Refill 必为越界（corrupt）。两种构造：
//   - 借用 (data,len)：调用方保证缓冲存活（SegmentMerger::Input 路径）；
//   - 持有 vector：残余 buffer 的 EmitBufferToInput 产物直接 move 进来。
class MemoryByteSource final : public ForwardByteSource {
public:
    MemoryByteSource(const uint8_t* data, size_t len);
    explicit MemoryByteSource(std::vector<uint8_t> bytes);

    const uint8_t* BorrowStable(size_t n) override;
    void ReadAt(int64_t offset, uint8_t* dst, size_t n) const override;

protected:
    void Refill() override;

private:
    std::vector<uint8_t> _owned; // 持有型构造的存储；借用型恒空
};

// spill tmp 文件上的源：固定容量（默认 1MiB，按 min(容量, 流长) 一次性分配）
// 的顺序读滑动窗口。spill 文件永远是 BE 本地盘（见 spill_manager.cpp 的
// ResolveBaseTmpDir 注释），顺序小窗 pread 没有对象存储请求模型问题。
class SpillFileByteSource final : public ForwardByteSource {
public:
    SpillFileByteSource(io::FileReaderSPtr reader, int64_t length, size_t buffer_capacity);
    ~SpillFileByteSource() override;

    const uint8_t* BorrowStable(size_t n) override { return nullptr; }
    void ReadAt(int64_t offset, uint8_t* dst, size_t n) const override;

protected:
    void Refill() override;

private:
    io::FileReaderSPtr _reader;
    std::vector<uint8_t> _buf;
    size_t _cap = 0;
};

} // namespace doris::segment_v2::inverted_index::spimi
