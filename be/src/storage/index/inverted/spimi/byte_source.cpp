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

#include "storage/index/inverted/spimi/byte_source.h"

#include <algorithm>
#include <cstring>
#include <utility>

#include "common/exception.h"
#include "storage/index/inverted/spimi/byte_parser_error.h"
#include "util/slice.h"

namespace doris::segment_v2::inverted_index::spimi {

void ForwardByteSource::RefillForRead() {
    Refill();
    if (_wpos >= _wlen) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI ForwardByteSource: read past end of stream");
    }
}

int32_t ForwardByteSource::ReadVInt() {
    uint32_t v = 0;
    uint32_t shift = 0;
    while (true) {
        const uint8_t b = ReadByte();
        v |= static_cast<uint32_t>(b & 0x7FU) << shift;
        if ((b & 0x80U) == 0) {
            break;
        }
        shift += 7;
        if (shift >= 32U) [[unlikely]] {
            SPIMI_THROW_CORRUPT("SPIMI ForwardByteSource VInt: shift overflow");
        }
    }
    return static_cast<int32_t>(v);
}

int64_t ForwardByteSource::ReadVLong() {
    uint64_t v = 0;
    uint32_t shift = 0;
    while (true) {
        const uint8_t b = ReadByte();
        v |= static_cast<uint64_t>(b & 0x7FU) << shift;
        if ((b & 0x80U) == 0) {
            break;
        }
        shift += 7;
        if (shift >= 64U) [[unlikely]] {
            SPIMI_THROW_CORRUPT("SPIMI ForwardByteSource VLong: shift overflow");
        }
    }
    return static_cast<int64_t>(v);
}

int32_t ForwardByteSource::ReadInt32BE() {
    const uint32_t b0 = ReadByte();
    const uint32_t b1 = ReadByte();
    const uint32_t b2 = ReadByte();
    const uint32_t b3 = ReadByte();
    return static_cast<int32_t>((b0 << 24) | (b1 << 16) | (b2 << 8) | b3);
}

int64_t ForwardByteSource::ReadInt64BE() {
    const uint64_t hi = static_cast<uint32_t>(ReadInt32BE());
    const uint64_t lo = static_cast<uint32_t>(ReadInt32BE());
    return static_cast<int64_t>((hi << 32) | lo);
}

void ForwardByteSource::ReadInto(std::vector<uint8_t>* out, size_t n) {
    // 先验边界：坏长度直接 corrupt，不让 out 先被超量增长。
    if (static_cast<int64_t>(n) > Remaining()) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI ForwardByteSource ReadInto: bounds violation");
    }
    while (n > 0) {
        if (_wpos >= _wlen) [[unlikely]] {
            RefillForRead();
        }
        const size_t take = std::min(n, _wlen - _wpos);
        out->insert(out->end(), _window + _wpos, _window + _wpos + take);
        _wpos += take;
        n -= take;
    }
}

void ForwardByteSource::SkipForwardTo(int64_t target) {
    const int64_t pos = Position();
    if (target < pos || target > _len) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI ForwardByteSource: bad forward seek target");
    }
    if (target <= _window_base + static_cast<int64_t>(_wlen)) {
        _wpos = static_cast<size_t>(target - _window_base);
    } else {
        // 跳出当前窗口：窗口作废，下次读取按新位置 Refill。
        _window_base = target;
        _wpos = 0;
        _wlen = 0;
    }
}

MemoryByteSource::MemoryByteSource(const uint8_t* data, size_t len) {
    _window = data;
    _wlen = len;
    _len = static_cast<int64_t>(len);
}

MemoryByteSource::MemoryByteSource(std::vector<uint8_t> bytes) : _owned(std::move(bytes)) {
    _window = _owned.data();
    _wlen = _owned.size();
    _len = static_cast<int64_t>(_owned.size());
}

void MemoryByteSource::Refill() {
    // 内存源的窗口即整个缓冲：要求补充就是越界。
    SPIMI_THROW_CORRUPT("SPIMI MemoryByteSource: read past end of buffer");
}

const uint8_t* MemoryByteSource::BorrowStable(size_t n) {
    if (static_cast<int64_t>(n) > Remaining()) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI MemoryByteSource: borrow past end of buffer");
    }
    const uint8_t* p = _window + _wpos;
    _wpos += n;
    return p;
}

void MemoryByteSource::ReadAt(int64_t offset, uint8_t* dst, size_t n) const {
    if (offset < 0 || offset + static_cast<int64_t>(n) > _len) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI MemoryByteSource ReadAt: bounds violation");
    }
    if (n > 0) {
        std::memcpy(dst, _window + offset, n);
    }
}

SpillFileByteSource::SpillFileByteSource(io::FileReaderSPtr reader, int64_t length,
                                         size_t buffer_capacity)
        : _reader(std::move(reader)), _cap(std::max<size_t>(1, buffer_capacity)) {
    _len = length;
}

SpillFileByteSource::~SpillFileByteSource() {
    if (_reader != nullptr) {
        static_cast<void>(_reader->close());
    }
}

void SpillFileByteSource::Refill() {
    const int64_t pos = Position();
    const int64_t avail = _len - pos;
    if (avail <= 0) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI SpillFileByteSource: read past end of stream");
    }
    // 缓冲一次性按 min(容量, 流长) 分配：小流不付整容量的驻留。
    const size_t n = static_cast<size_t>(std::min<int64_t>(static_cast<int64_t>(_cap), avail));
    if (_buf.size() < n) {
        _buf.resize(static_cast<size_t>(std::min<int64_t>(static_cast<int64_t>(_cap), _len)));
    }
    size_t total = 0;
    while (total < n) {
        size_t got = 0;
        Status st = _reader->read_at(pos + static_cast<int64_t>(total),
                                     Slice(_buf.data() + total, n - total), &got);
        if (!st.ok()) {
            throw doris::Exception(st);
        }
        if (got == 0) [[unlikely]] {
            SPIMI_THROW_CORRUPT("SPIMI SpillFileByteSource: short read on spill tmp file");
        }
        total += got;
    }
    _window = _buf.data();
    _window_base = pos;
    _wpos = 0;
    _wlen = n;
}

void SpillFileByteSource::ReadAt(int64_t offset, uint8_t* dst, size_t n) const {
    if (offset < 0 || offset + static_cast<int64_t>(n) > _len) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI SpillFileByteSource ReadAt: bounds violation");
    }
    size_t total = 0;
    while (total < n) {
        size_t got = 0;
        Status st = _reader->read_at(offset + static_cast<int64_t>(total),
                                     Slice(dst + total, n - total), &got);
        if (!st.ok()) {
            throw doris::Exception(st);
        }
        if (got == 0) [[unlikely]] {
            SPIMI_THROW_CORRUPT("SPIMI SpillFileByteSource: short read on spill tmp file");
        }
        total += got;
    }
}

} // namespace doris::segment_v2::inverted_index::spimi
