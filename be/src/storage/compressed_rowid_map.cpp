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
#include "storage/compressed_rowid_map.h"

#define ZSTD_STATIC_LINKING_ONLY
#include <zstd.h>

#include <algorithm>
#include <cstring>
#include <limits>
#include <vector>

#include "common/check.h"
#include "common/exception.h"
#include "core/custom_allocator.h"
#include "util/coding.h"

namespace doris {
namespace {
// Account small ownership objects as well as their buffers. Class-specific
// allocation keeps the public header independent of allocator implementation.
struct TrackedRowIdObject {
    static void* operator new(size_t size) { return Allocator<false>().alloc(size); }
    static void operator delete(void* pointer, size_t size) {
        Allocator<false>().free(pointer, size);
    }
};

constexpr size_t MAX_PLAIN_BYTES =
        CompressedRowIdMap::BLOCK_ROWS / 8 + 10 * (CompressedRowIdMap::BLOCK_ROWS - 1);

size_t encoder_context_bytes() {
    // The input is at most one bitmap and 4095 varints, not an unbounded stream.
    return ZSTD_estimateCCtxSize_usingCParams(ZSTD_getCParams(1, MAX_PLAIN_BYTES, 0));
}

Status invalid_block(const char* reason) {
    return Status::InternalError("Invalid compressed rowid block: {}", reason);
}

Status zstd_status(size_t result) {
    if (ZSTD_isError(result)) {
        return Status::InternalError("Rowid Zstd: {}", ZSTD_getErrorName(result));
    }
    return Status::OK();
}
} // namespace

struct CompressedRowIdMap::Cursor::Impl : TrackedRowIdObject {
    DorisUniqueBufferPtr<uint64_t> values {BLOCK_ROWS};
    const CompressedRowIdMap* owner = nullptr;
    uint32_t source = 0;
    uint32_t block = 0;
    uint64_t decoded = 0;
};

CompressedRowIdMap::Cursor::Cursor() = default;
CompressedRowIdMap::Cursor::~Cursor() = default;
CompressedRowIdMap::Cursor::Cursor(Cursor&&) noexcept = default;
CompressedRowIdMap::Cursor& CompressedRowIdMap::Cursor::operator=(Cursor&&) noexcept = default;

Status CompressedRowIdMap::Cursor::reserve() {
    RETURN_IF_CATCH_EXCEPTION({
        if (_impl == nullptr) {
            _impl = std::make_unique<Impl>();
        }
    });
    return Status::OK();
}

size_t CompressedRowIdMap::Cursor::memory_usage() const {
    return _impl == nullptr ? 0 : sizeof(Impl) + BLOCK_ROWS * sizeof(uint64_t);
}
uint64_t CompressedRowIdMap::Cursor::decoded_blocks() const {
    return _impl == nullptr ? 0 : _impl->decoded;
}

struct CompressedRowIdMap::DecodeWorkspace::Impl : TrackedRowIdObject {
    DorisUniqueBufferPtr<uint8_t> context {ZSTD_estimateDCtxSize()};
    DorisUniqueBufferPtr<uint8_t> plain {MAX_PLAIN_BYTES};
    ZSTD_DCtx* dctx = ZSTD_initStaticDCtx(context.get(), ZSTD_estimateDCtxSize());
};
CompressedRowIdMap::DecodeWorkspace::DecodeWorkspace() = default;
CompressedRowIdMap::DecodeWorkspace::~DecodeWorkspace() = default;
size_t CompressedRowIdMap::DecodeWorkspace::memory_usage() const {
    return _impl == nullptr ? 0 : sizeof(Impl) + ZSTD_estimateDCtxSize() + MAX_PLAIN_BYTES;
}

struct CompressedRowIdMap::Impl : TrackedRowIdObject {
    struct Source {
        DorisVector<BlockMeta> blocks;
        DorisUniqueBufferPtr<uint64_t> builder;
        uint32_t rows = 0;
        uint32_t current_block = 0;
        uint32_t last_row = 0;
        uint64_t last_target = 0;
    };
    struct Encoder : TrackedRowIdObject {
        DorisUniqueBufferPtr<uint8_t> context {encoder_context_bytes()};
        DorisUniqueBufferPtr<uint8_t> plain {MAX_PLAIN_BYTES};
        DorisUniqueBufferPtr<uint8_t> compressed {ZSTD_compressBound(MAX_PLAIN_BYTES)};
        ZSTD_CCtx* cctx = ZSTD_initStaticCCtx(context.get(), encoder_context_bytes());
        static size_t memory_usage() {
            return sizeof(Encoder) + encoder_context_bytes() + MAX_PLAIN_BYTES +
                   ZSTD_compressBound(MAX_PLAIN_BYTES);
        }
    };
    DorisVector<Source> sources;
    DorisVector<DorisUniqueBufferPtr<uint8_t>> pages;
    size_t page_used = PAGE_BYTES;
    std::unique_ptr<Encoder> encoder;
    MemoryUsage usage;
    bool finished = false;

    void update_usage() {
        usage.metadata_bytes = sizeof(Impl) + sources.capacity() * sizeof(Source) +
                               pages.capacity() * sizeof(DorisUniqueBufferPtr<uint8_t>);
        usage.encoder_bytes = encoder == nullptr ? 0 : Encoder::memory_usage();
        usage.arena_capacity = pages.size() * PAGE_BYTES;
        usage.peak_bytes = std::max(usage.peak_bytes, usage.allocated_bytes());
    }

    void append_payload(BlockMeta& meta, std::span<const uint8_t> payload) {
        if (payload.empty()) {
            return;
        }
        DORIS_CHECK_LE(payload.size(), BLOCK_ROWS * sizeof(uint64_t));
        if (PAGE_BYTES - page_used < payload.size()) {
            pages.push_back(make_unique_buffer<uint8_t>(PAGE_BYTES));
            page_used = 0;
        }
        meta.offset = (pages.size() - 1) * PAGE_BYTES + page_used;
        meta.payload_bytes = static_cast<uint32_t>(payload.size());
        memcpy(pages.back().get() + page_used, payload.data(), payload.size());
        page_used += payload.size();
        usage.payload_bytes += payload.size();
    }

    Status seal(Source& source) {
        if (source.builder == nullptr) {
            return Status::OK();
        }
        BlockMeta meta;
        meta.rows = source.blocks[source.current_block].rows;
        uint64_t previous = MISSING;
        bool linear = true;
        for (size_t i = 0; i < meta.rows; ++i) {
            uint64_t value = source.builder[i];
            if (value == MISSING) {
                continue;
            }
            if (meta.live++ == 0) {
                meta.first = value;
            } else {
                linear &= value == previous + 1;
            }
            previous = value;
        }
        DORIS_CHECK_GT(meta.live, 0);
        meta.kind = linear ? Kind::LINEAR : Kind::DELTA;
        meta.flags = meta.live == meta.rows ? 0 : flag_bits(Flag::BITMAP);
        if (linear && meta.flags == 0) {
            source.blocks[source.current_block] = meta;
            return Status::OK();
        }
        if (encoder == nullptr) {
            encoder = std::make_unique<Encoder>();
            update_usage();
        }
        if (encoder->cctx == nullptr) {
            return Status::InternalError("Cannot initialize rowid Zstd encoder workspace");
        }
        // Encode only live targets; the bitmap keeps their physical source positions.
        // Example before optional Zstd (all shown integers fit in one varint byte):
        //   source offsets:    0       1       2       3
        //   target ordinals:  100   MISSING   103     104
        //   meta: first=100, rows=4, live=3, kind=DELTA, flags=BITMAP
        //   plain: [0x0d bitmap][0x02 gap][0x00 gap]
        // The bitmap has bits 0,2,3 set; gaps are 103-100-1 and 104-103-1.
        uint8_t* plain = encoder->plain.get();
        size_t bitmap_bytes = (meta.flags & flag_bits(Flag::BITMAP)) != 0 ? (meta.rows + 7) / 8 : 0;
        memset(plain, 0, bitmap_bytes);
        uint8_t* end = plain + bitmap_bytes;
        previous = MISSING;
        for (size_t i = 0; i < meta.rows; ++i) {
            uint64_t value = source.builder[i];
            if (value == MISSING) {
                continue;
            }
            if (bitmap_bytes != 0) {
                plain[i / 8] |= static_cast<uint8_t>(1U << (i % 8));
            }
            if (!linear && previous != MISSING) {
                end = encode_varint64(end, value - previous - 1);
            }
            previous = value;
        }
        meta.plain_bytes = static_cast<uint32_t>(end - plain);
        std::span<const uint8_t> payload(plain, meta.plain_bytes);
        size_t compressed_size =
                ZSTD_compressCCtx(encoder->cctx, encoder->compressed.get(),
                                  ZSTD_compressBound(MAX_PLAIN_BYTES), plain, meta.plain_bytes, 1);
        RETURN_IF_ERROR(zstd_status(compressed_size));
        if (compressed_size < payload.size()) {
            meta.flags |= flag_bits(Flag::ZSTD);
            payload = {encoder->compressed.get(), compressed_size};
        }
        // A sparse/outlier-heavy tail can have more varint bytes than raw rows.
        // Never expand the final payload beyond 8 bytes per physical source row.
        if (payload.size() > meta.rows * sizeof(uint64_t)) {
            meta.kind = Kind::RAW64;
            meta.flags = 0;
            meta.plain_bytes = meta.rows * sizeof(uint64_t);
            payload = {reinterpret_cast<const uint8_t*>(source.builder.get()), meta.plain_bytes};
        }
        append_payload(meta, payload);
        source.blocks[source.current_block] = meta;
        update_usage();
        return Status::OK();
    }
};

CompressedRowIdMap::CompressedRowIdMap() : _impl(std::make_unique<Impl>()) {
    _impl->update_usage();
}
CompressedRowIdMap::~CompressedRowIdMap() = default;

Status CompressedRowIdMap::init_source(uint32_t rows) {
    if (_impl->finished) {
        return Status::InvalidArgument("Cannot register a finished rowid map");
    }
    RETURN_IF_CATCH_EXCEPTION({
        Impl::Source source;
        source.rows = rows;
        source.blocks.resize((static_cast<size_t>(rows) + BLOCK_ROWS - 1) / BLOCK_ROWS);
        for (size_t block = 0; block < source.blocks.size(); ++block) {
            source.blocks[block].rows =
                    static_cast<uint16_t>(std::min<size_t>(BLOCK_ROWS, rows - block * BLOCK_ROWS));
        }
        size_t directory_bytes = source.blocks.capacity() * sizeof(BlockMeta);
        _impl->sources.push_back(std::move(source));
        _impl->usage.directory_bytes += directory_bytes;
        _impl->update_usage();
    });
    return Status::OK();
}

Status CompressedRowIdMap::append(uint32_t source_id, uint32_t row, uint64_t target) {
    if (_impl->finished || source_id >= _impl->sources.size() || target == MISSING) {
        return Status::InvalidArgument("Invalid rowid map append state/source/target");
    }
    auto& source = _impl->sources[source_id];
    if (row >= source.rows ||
        (source.builder != nullptr && (row <= source.last_row || target <= source.last_target))) {
        return Status::InvalidArgument("Rowid source {} must advance: row {}, target {}", source_id,
                                       row, target);
    }
    RETURN_IF_CATCH_EXCEPTION({
        uint32_t block = row / BLOCK_ROWS;
        if (source.builder == nullptr) {
            source.builder = make_unique_buffer<uint64_t>(BLOCK_ROWS);
            _impl->usage.builder_bytes += BLOCK_ROWS * sizeof(uint64_t);
            std::fill_n(source.builder.get(), BLOCK_ROWS, MISSING);
            source.current_block = block;
            _impl->update_usage();
        } else if (block != source.current_block) {
            RETURN_IF_ERROR(_impl->seal(source));
            std::fill_n(source.builder.get(), BLOCK_ROWS, MISSING);
            source.current_block = block;
        }
        source.builder[row % BLOCK_ROWS] = target;
        source.last_row = row;
        source.last_target = target;
    });
    return Status::OK();
}

Status CompressedRowIdMap::finish() {
    if (_impl->finished) {
        return Status::OK();
    }
    RETURN_IF_CATCH_EXCEPTION({
        for (auto& source : _impl->sources) {
            RETURN_IF_ERROR(_impl->seal(source));
        }
        for (auto& source : _impl->sources) {
            source.builder.reset();
        }
        _impl->usage.builder_bytes = 0;
        _impl->encoder.reset();
        _impl->finished = true;
        _impl->update_usage();
    });
    return Status::OK();
}

CompressedRowIdMap::MemoryUsage CompressedRowIdMap::memory_usage() const {
    return _impl->usage;
}

const CompressedRowIdMap::BlockMeta& CompressedRowIdMap::block_meta(uint32_t source,
                                                                    uint32_t block) const {
    return _impl->sources[source].blocks[block];
}

Status CompressedRowIdMap::block_payload(const BlockMeta& meta,
                                         std::span<const uint8_t>* payload) const {
    if (meta.payload_bytes == 0) {
        *payload = {};
        return Status::OK();
    }
    uint64_t page = meta.offset / PAGE_BYTES;
    size_t offset = meta.offset % PAGE_BYTES;
    if (page >= _impl->pages.size() || meta.payload_bytes > PAGE_BYTES - offset ||
        (page == _impl->pages.size() - 1 && offset + meta.payload_bytes > _impl->page_used)) {
        return invalid_block("payload outside arena");
    }
    *payload = {_impl->pages[page].get() + offset, meta.payload_bytes};
    return Status::OK();
}

Status CompressedRowIdMap::decode(const BlockMeta& meta, std::span<const uint8_t> payload,
                                  Cursor& cursor, DecodeWorkspace& workspace) {
    if (meta.rows == 0 || meta.rows > BLOCK_ROWS || meta.live > meta.rows || meta.reserved != 0 ||
        (meta.flags & ~(flag_bits(Flag::BITMAP) | flag_bits(Flag::ZSTD))) != 0 ||
        meta.kind > Kind::RAW64 || meta.payload_bytes != payload.size() ||
        meta.payload_bytes > meta.rows * sizeof(uint64_t) || meta.plain_bytes > MAX_PLAIN_BYTES) {
        return invalid_block("directory fields");
    }
    RETURN_IF_ERROR(cursor.reserve());
    // A failed decode must invalidate the previous cache before changing its values.
    cursor._impl->owner = nullptr;
    auto* values = cursor._impl->values.get();
    std::fill_n(values, BLOCK_ROWS, MISSING);
    if (meta.kind == Kind::EMPTY) {
        return meta.live == 0 && meta.flags == 0 && payload.empty() && meta.plain_bytes == 0
                       ? Status::OK()
                       : invalid_block("nonempty EMPTY");
    }
    if (meta.live == 0 || meta.first == MISSING) {
        return invalid_block("empty live sequence");
    }
    if (meta.kind == Kind::RAW64) {
        if (meta.flags != 0 || payload.size() != meta.rows * sizeof(uint64_t) ||
            meta.plain_bytes != payload.size()) {
            return invalid_block("RAW64 size/flags");
        }
        memcpy(values, payload.data(), payload.size());
        uint16_t live = 0;
        uint64_t previous = 0;
        for (size_t i = 0; i < meta.rows; ++i) {
            if (values[i] == MISSING) {
                continue;
            }
            if ((live == 0 && values[i] != meta.first) || (live != 0 && values[i] <= previous)) {
                return invalid_block("RAW64 order/first");
            }
            ++live;
            previous = values[i];
        }
        return live == meta.live ? Status::OK() : invalid_block("RAW64 live count");
    }
    if (((meta.flags & flag_bits(Flag::BITMAP)) != 0) != (meta.live != meta.rows)) {
        return invalid_block("bitmap flag/live count");
    }
    if ((meta.flags & flag_bits(Flag::ZSTD)) != 0) {
        RETURN_IF_CATCH_EXCEPTION({
            if (workspace._impl == nullptr) {
                workspace._impl = std::make_unique<DecodeWorkspace::Impl>();
            }
        });
        if (workspace._impl->dctx == nullptr) {
            return Status::InternalError("Cannot initialize rowid Zstd decoder workspace");
        }
        size_t size = ZSTD_decompressDCtx(workspace._impl->dctx, workspace._impl->plain.get(),
                                          meta.plain_bytes, payload.data(), payload.size());
        RETURN_IF_ERROR(zstd_status(size));
        if (size != meta.plain_bytes) {
            return invalid_block("Zstd plain length");
        }
        payload = {workspace._impl->plain.get(), size};
    } else if (meta.plain_bytes != payload.size()) {
        return invalid_block("plain length");
    }
    size_t bitmap_bytes = (meta.flags & flag_bits(Flag::BITMAP)) != 0 ? (meta.rows + 7) / 8 : 0;
    if (payload.size() < bitmap_bytes || (bitmap_bytes != 0 && meta.rows % 8 != 0 &&
                                          (payload[bitmap_bytes - 1] >> (meta.rows % 8)) != 0)) {
        return invalid_block("bitmap length/padding");
    }
    // Zstd, when present, has already been removed. Walk the bitmap in physical
    // row order while consuming the live sequence: first, then previous + gap + 1.
    // LINEAR consumes no varints (gap is implicitly zero); holes consume neither
    // a target nor a gap. E.g. [0x0d | 0x02 0x00], first=100 reconstructs
    // [100, MISSING, 103, 104], rather than advancing the live sequence at the hole.
    size_t position = bitmap_bytes;
    uint16_t live = 0;
    uint64_t value = meta.first;
    for (size_t row = 0; row < meta.rows; ++row) {
        if (bitmap_bytes != 0 && (payload[row / 8] & (1U << (row % 8))) == 0) {
            continue;
        }
        if (live != 0) {
            uint64_t gap = 0;
            if (meta.kind == Kind::DELTA) {
                // The general coding helper accepts an overflowing tenth byte.
                // Check all ten bytes here before reconstructing a global ordinal.
                bool terminated = false;
                for (unsigned shift = 0; shift <= 63; shift += 7) {
                    if (position == payload.size()) {
                        return invalid_block("truncated varint");
                    }
                    uint8_t byte = payload[position++];
                    if (shift == 63 && byte > 1) {
                        return invalid_block("varint overflow");
                    }
                    gap |= static_cast<uint64_t>(byte & 127) << shift;
                    if ((byte & 128) == 0) {
                        terminated = true;
                        break;
                    }
                }
                if (!terminated) {
                    return invalid_block("unterminated varint");
                }
            }
            if (gap >= MISSING - value - 1) {
                return invalid_block("target overflow");
            }
            value += gap + 1;
        }
        values[row] = value;
        ++live;
    }
    if (live != meta.live || position != payload.size()) {
        return invalid_block("live count/trailing bytes");
    }
    return Status::OK();
}

Status CompressedRowIdMap::get(uint32_t source_id, uint32_t row, Cursor& cursor,
                               DecodeWorkspace& workspace, uint64_t* target) const {
    *target = MISSING;
    if (!_impl->finished) {
        return Status::InvalidArgument("Rowid map must be finished before reading");
    }
    if (source_id >= _impl->sources.size() || row >= _impl->sources[source_id].rows) {
        return Status::OK();
    }
    uint32_t block = row / BLOCK_ROWS;
    const auto& meta = block_meta(source_id, block);
    uint16_t expected_rows = static_cast<uint16_t>(std::min<uint64_t>(
            BLOCK_ROWS,
            _impl->sources[source_id].rows - static_cast<uint64_t>(block) * BLOCK_ROWS));
    if (meta.rows != expected_rows) {
        return invalid_block("source block length");
    }
    if (meta.kind == Kind::EMPTY && meta.live == 0 && meta.flags == 0 && meta.payload_bytes == 0 &&
        meta.plain_bytes == 0 && meta.reserved == 0) {
        return Status::OK();
    }
    if (meta.kind == Kind::LINEAR && meta.live == meta.rows && meta.flags == 0 &&
        meta.payload_bytes == 0 && meta.plain_bytes == 0 && meta.reserved == 0) {
        if (meta.first >= MISSING - (meta.rows - 1)) {
            return invalid_block("LINEAR target overflow");
        }
        *target = meta.first + row % BLOCK_ROWS;
        return Status::OK();
    }
    if (cursor._impl == nullptr || cursor._impl->owner != this ||
        cursor._impl->source != source_id || cursor._impl->block != block) {
        std::span<const uint8_t> payload;
        RETURN_IF_ERROR(block_payload(meta, &payload));
        RETURN_IF_ERROR(decode(meta, payload, cursor, workspace));
        cursor._impl->owner = this;
        cursor._impl->source = source_id;
        cursor._impl->block = block;
        ++cursor._impl->decoded;
    }
    *target = cursor._impl->values[row % BLOCK_ROWS];
    return Status::OK();
}

} // namespace doris
