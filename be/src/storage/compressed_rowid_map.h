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
#include <memory>
#include <span>
#include <utility>

#include "common/status.h"

namespace doris {

class RowIdConversion;

// Task-local, immutable after finish(). Each source has fixed physical-row blocks,
// so even a source with billions of rows only needs one builder/decoder block.
class CompressedRowIdMap {
public:
    static constexpr uint32_t BLOCK_ROWS = 4096;
    static constexpr size_t PAGE_BYTES = 1 << 20;
    static constexpr uint64_t MISSING = UINT64_MAX;

    class Cursor {
    public:
        Cursor();
        ~Cursor();
        Cursor(Cursor&&) noexcept;
        Cursor& operator=(Cursor&&) noexcept;
        Status reserve();
        size_t memory_usage() const;
        uint64_t decoded_blocks() const;

    private:
        friend class CompressedRowIdMap;
        struct Impl;
        std::unique_ptr<Impl> _impl;
    };

    // Share between cursors executing on the same thread; never between threads.
    // In particular, an SNII term can keep A cursor blocks but only one Zstd context.
    class DecodeWorkspace {
    public:
        DecodeWorkspace();
        ~DecodeWorkspace();
        size_t memory_usage() const;

    private:
        friend class CompressedRowIdMap;
        struct Impl;
        std::unique_ptr<Impl> _impl;
    };

    struct MemoryUsage {
        size_t directory_bytes = 0;
        size_t payload_bytes = 0;
        size_t arena_capacity = 0;
        size_t metadata_bytes = 0;
        size_t builder_bytes = 0;
        size_t encoder_bytes = 0;
        size_t peak_bytes = 0;
        size_t allocated_bytes() const {
            return directory_bytes + arena_capacity + metadata_bytes + builder_bytes +
                   encoder_bytes;
        }
    };

    CompressedRowIdMap();
    ~CompressedRowIdMap();
    Status init_source(uint32_t rows);
    Status append(uint32_t source, uint32_t row, uint64_t target);
    Status finish();
    Status get(uint32_t source, uint32_t row, Cursor& cursor, DecodeWorkspace& workspace,
               uint64_t* target) const;
    MemoryUsage memory_usage() const;

private:
    enum class Kind : uint8_t { EMPTY, LINEAR, DELTA, RAW64 };
    enum class Flag : uint8_t { BITMAP = 1, ZSTD = 2 };
    static constexpr uint8_t flag_bits(Flag flag) { return static_cast<uint8_t>(flag); }

    // Process-local layout, not a persistent/wire format. Each source has a directory:
    //
    // source row r --> blocks[r / BLOCK_ROWS] --> arena payload at meta.offset
    //                  (one 32-byte entry)       (never crosses a 1 MiB page)
    //
    // BlockMeta field widths in bytes, in declaration order:
    // +----------+---------+-----------+---------+--------+--------+--------+---------+------------+
    // | offset:8 | first:8 | payload:4 | plain:4 | rows:2 | live:2 | kind:1 | flags:1 | reserved:2 |
    // +----------+---------+-----------+---------+--------+--------+--------+---------+------------+
    // offset is page_index * PAGE_BYTES + in_page_offset; first is the first live global
    // target ordinal. rows counts physical source rows; live counts mapped rows. payload
    // and plain denote payload_bytes (stored) and plain_bytes (before optional Zstd).
    //
    // Uncompressed payload by kind (N = rows, L = live, G = live target sequence):
    //   EMPTY   []                                  all N source rows are missing
    //   LINEAR  [validity bitmap, if BITMAP]         G[i] = first + i
    //   DELTA   [validity bitmap, if BITMAP][varint(g1)]...[varint(gL-1)]
    //                                              gi = G[i] - G[i-1] - 1
    //   RAW64   [native uint64_t v0]...[vN-1]        one value per physical source row
    //
    // BITMAP uses ceil(N / 8) bytes, LSB first within each byte; 1 means mapped.
    // It is omitted when L == N; padding bits are zero. RAW64 has no flags and uses
    // MISSING for holes, with native byte order because the map never leaves this process.
    // EMPTY and full LINEAR need no payload. Otherwise LINEAR/DELTA may set ZSTD:
    // the entire [bitmap | varints] is replaced by one Zstd frame only if smaller.
    // first stays in BlockMeta, never in the varint stream or the Zstd frame.

    struct BlockMeta {
        uint64_t offset = 0;
        uint64_t first = 0;
        uint32_t payload_bytes = 0;
        uint32_t plain_bytes = 0;
        uint16_t rows = 0;
        uint16_t live = 0;
        Kind kind = Kind::EMPTY;
        uint8_t flags = 0;
        uint16_t reserved = 0;
    };
    static_assert(sizeof(BlockMeta) == 32);
    struct Impl;
    std::unique_ptr<Impl> _impl;

    const BlockMeta& block_meta(uint32_t source, uint32_t block) const;
    Status block_payload(const BlockMeta& meta, std::span<const uint8_t>* payload) const;
    static Status decode(const BlockMeta& meta, std::span<const uint8_t> payload, Cursor& cursor,
                         DecodeWorkspace& workspace);
};

// A borrowed source, not a materialized dense segment. Dense spans remain useful
// for CLucene and SNII's existing callers/tests. Missing is the existing pair sentinel.
class RowIdConversionSource {
public:
    using Pair = std::pair<uint32_t, uint32_t>;
    RowIdConversionSource() = default;
    explicit RowIdConversionSource(std::span<const Pair> dense)
            : _dense(dense), _rows(dense.size()) {}
    size_t size() const { return _rows; }
    bool is_dense() const { return _owner == nullptr; }
    std::span<const Pair> dense_values() const { return _dense; }
    Status get(uint32_t row, CompressedRowIdMap::Cursor& cursor,
               CompressedRowIdMap::DecodeWorkspace& workspace, Pair* destination) const;

private:
    friend class RowIdConversion;
    RowIdConversionSource(const RowIdConversion* owner, uint32_t source, uint32_t rows)
            : _owner(owner), _source(source), _rows(rows) {}
    std::span<const Pair> _dense;
    const RowIdConversion* _owner = nullptr;
    uint32_t _source = 0;
    size_t _rows = 0;
};

} // namespace doris
