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

#include "format_v2/blob/blob_file_reader.h"

#include <zlib.h>

#include <array>
#include <cstring>
#include <limits>
#include <utility>

#include "common/logging.h"
#include "core/pod_array.h"
#include "io/io_common.h"
#include "util/coding.h"
#include "util/slice.h"

namespace doris::format {
namespace {

// Paimon managed .blob format, version 1.
//
// File layout (all fixed-width integers are little-endian):
//   offset 0                                                           EOF
//      |                                                                |
//      v                                                                v
//      +---------------------------+--------------------+---------------+
//      | Non-NULL records          | Row-length index   | Footer        |
//      | R0 | R1 | ...             | variable size      | 5 bytes       |
//      +---------------------------+--------------------+---------------+
//                                  ^                    |
//                                  +-- index_size bytes-+
//
// Each non-NULL record has 16 framing bytes around its payload:
//   relative offset: 0         4                  4 + N          12 + N    16 + N
//                    |         |                    |               |         |
//                    v         v                    v               v         v
//                    +---------+--------------------+---------------+---------+
//                    | Magic   | Payload            | Record length | CRC32   |
//                    | 4 bytes | N bytes            | 8 bytes       | 4 bytes |
//                    +---------+--------------------+---------------+---------+
//                    |<----------- CRC32 input -------------------->|
//                    |<-------------- record_length = N + 16 ---------------->|
//
// record_length is N + 16, including the magic, length and CRC fields. CRC32 covers
// the magic, payload and record_length, excluding the CRC field itself.
//
// Footer:
//   file_size - 5                         file_size - 1     file_size
//         |                                     |               |
//         v                                     v               v
//         +-------------------------------------+---------------+
//         | index_size: uint32 (4 bytes)        | version: 1    |
//         |                                     | (1 byte)      |
//         +-------------------------------------+---------------+
//   index_start = file_size - 5 - index_size
//
// The index has one entry per file row, including NULLs. Each entry is a zigzag varint
// encoding the delta from the previous row's record length (initial previous length: 0).
// A decoded length of -1 means NULL and consumes no record bytes. An empty non-NULL
// payload still has a 16-byte record, so it remains distinct from NULL.
//
// open() reads the last 5 bytes to locate the index, then reconstructs payload ranges
// by accumulating non-NULL record lengths from file offset 0. A payload starts 4 bytes
// after its record start and has record_length - 16 bytes. The expected row count comes
// from the caller's manifest metadata; it is not stored in the footer. These ranges
// allow descriptor output without reading payloads; read_payload() reads and validates
// an individual record only when inline bytes are requested.

constexpr uint32_t BLOB_MAGIC = 1481511375;
constexpr int64_t BLOB_HEADER_SIZE = 4;
constexpr int64_t BLOB_TRAILER_SIZE = 12; // Record length (8) followed by CRC32 (4).
constexpr int64_t BLOB_RECORD_OVERHEAD = BLOB_HEADER_SIZE + BLOB_TRAILER_SIZE;
constexpr int64_t BLOB_FOOTER_SIZE = 5; // Index length (4) followed by format version (1).

} // namespace

Status BlobFileReader::_read_exact(int64_t offset, char* data, size_t size,
                                   const io::IOContext* io_ctx) const {
    if (offset < 0 || uint64_t(offset) > _reader->size() ||
        size > _reader->size() - uint64_t(offset)) {
        return Status::Corruption("BLOB read exceeds file bounds");
    }
    size_t read = 0;
    RETURN_IF_ERROR(_reader->read_at(offset, Slice(data, size), &read, io_ctx));
    if (read != size) {
        return Status::Corruption("Truncated BLOB file: expected {} bytes, got {}", size, read);
    }
    return Status::OK();
}

Status BlobFileReader::open(io::FileReaderSPtr reader, int64_t expected_rows,
                            const io::IOContext* io_ctx) {
    _reader = std::move(reader);
    _ranges.clear();
    if (_reader == nullptr || expected_rows < 0 || _reader->size() < BLOB_FOOTER_SIZE ||
        _reader->size() > uint64_t(std::numeric_limits<int64_t>::max())) {
        return Status::Corruption("Invalid BLOB file size or row count");
    }
    std::array<char, BLOB_FOOTER_SIZE> footer;
    // Read the 5-byte footer: index byte length (4 bytes) and format version (1 byte).
    RETURN_IF_ERROR(
            _read_exact(_reader->size() - footer.size(), footer.data(), footer.size(), io_ctx));
    if (footer[4] != 1) {
        return Status::NotSupported("Unsupported Paimon BLOB version {}",
                                    static_cast<unsigned char>(footer[4]));
    }
    const uint64_t index_size = decode_fixed32_le(reinterpret_cast<const uint8_t*>(footer.data()));
    // Each row contributes one varint of 1..10 bytes. Check the upper bound by division
    // so an invalid expected_rows cannot overflow a multiplication.
    if (index_size > _reader->size() - footer.size() || index_size < uint64_t(expected_rows) ||
        (index_size + 9) / 10 > uint64_t(expected_rows)) {
        return Status::Corruption("Invalid BLOB index length {} for {} rows", index_size,
                                  expected_rows);
    }
    const int64_t index_offset = _reader->size() - footer.size() - index_size;
    PODArray<char> index;
    index.resize(index_size);
    // Read the delta/zigzag-varint record-length index to reconstruct each row's payload range.
    RETURN_IF_ERROR(_read_exact(index_offset, index.data(), index.size(), io_ctx));
    _ranges.reserve(expected_rows);

    int64_t previous_length = 0;
    int64_t offset = 0;
    size_t cursor = 0;
    // Each zigzag varint encodes the delta from the previous record length, including its
    // 16 framing bytes. A length of -1 denotes NULL and occupies an index entry but no data.
    while (cursor < index.size()) {
        if (io_ctx != nullptr && io_ctx->should_stop) {
            return Status::Cancelled("Paimon BLOB index read cancelled");
        }
        if (_ranges.size() >= uint64_t(expected_rows)) {
            return Status::Corruption("BLOB index has more rows than its manifest");
        }
        uint64_t encoded = 0;
        bool complete = false;
        for (unsigned shift = 0; shift < 64; shift += 7) {
            if (cursor == index.size()) {
                return Status::Corruption("Truncated BLOB index varint");
            }
            const auto byte = static_cast<unsigned char>(index[cursor++]);
            if (shift == 63 && byte > 1) {
                return Status::Corruption("BLOB index varint overflow");
            }
            encoded |= uint64_t(byte & 0x7f) << shift;
            if ((byte & 0x80) == 0) {
                complete = true;
                break;
            }
        }
        if (!complete) {
            return Status::Corruption("BLOB index varint overflow");
        }
        const auto delta = static_cast<int64_t>((encoded >> 1) ^ (0 - (encoded & 1)));
        const __int128 length = __int128(previous_length) + delta;
        if (length < -1 || length > std::numeric_limits<int64_t>::max() ||
            (length != -1 && (length < BLOB_RECORD_OVERHEAD || length > index_offset - offset))) {
            return Status::Corruption("Invalid BLOB record length");
        }
        previous_length = static_cast<int64_t>(length);
        _ranges.push_back(length == -1
                                  ? ByteRange {.offset = -1, .size = 0}
                                  : ByteRange {.offset = offset + BLOB_HEADER_SIZE,
                                               .size = previous_length - BLOB_RECORD_OVERHEAD});
        if (length != -1) {
            offset += previous_length;
        }
    }
    // Every manifest row must have an index entry, and data records must end at the index.
    if (_ranges.size() != uint64_t(expected_rows) || offset != index_offset) {
        return Status::Corruption("BLOB index does not match manifest rows or data size");
    }
    return Status::OK();
}

Status BlobFileReader::get_range(int64_t row, std::optional<ByteRange>* range) const {
    if (row < 0 || uint64_t(row) >= _ranges.size()) {
        return Status::Corruption("BLOB row {} is outside file row count {}", row, _ranges.size());
    }
    if (_ranges[row].offset < 0) {
        range->reset();
    } else {
        *range = _ranges[row];
    }
    return Status::OK();
}

Status BlobFileReader::read_payload(const ByteRange& range, PODArray<char>* bytes,
                                    const io::IOContext* io_ctx) const {
    // Non-NULL ranges from get_range() were already validated by open().
    DCHECK(_reader != nullptr);
    DCHECK_GE(range.offset, BLOB_HEADER_SIZE);
    DCHECK_GE(range.size, 0);
    DCHECK_LE(uint64_t(range.offset), _reader->size());
    DCHECK_LE(uint64_t(range.size), _reader->size() - uint64_t(range.offset));
    DCHECK_GE(_reader->size() - uint64_t(range.offset) - uint64_t(range.size),
              uint64_t(BLOB_TRAILER_SIZE));
    // Large values can still be described, so open() does not enforce the inline capacity.
    if (uint64_t(range.size) > std::numeric_limits<int32_t>::max()) {
        return Status::NotSupported("BLOB payload exceeds FILE inline VARBINARY capacity");
    }
    // Framing and payload are contiguous. Read the whole record once so object storage does
    // not need separate requests for the magic, payload and trailer.
    const size_t record_size = static_cast<size_t>(range.size) + BLOB_RECORD_OVERHEAD;
    bytes->resize(record_size);
    // Read this row's complete record: magic, payload, stored record length and CRC32.
    RETURN_IF_ERROR(
            _read_exact(range.offset - BLOB_HEADER_SIZE, bytes->data(), record_size, io_ctx));
    // If profiling shows record validation is costly, consider skipping CRC verification or
    // reading only the indexed payload range. The latter also avoids reading framing bytes and
    // the memmove below, but gives up both framing and CRC validation. Keep file/index bounds,
    // exact-read checks and the caller's payload size limit even if these checks are relaxed.
    const auto* record = reinterpret_cast<const uint8_t*>(bytes->data());
    const auto* trailer = record + BLOB_HEADER_SIZE + range.size;
    if (decode_fixed32_le(record) != BLOB_MAGIC || decode_fixed64_le(trailer) != record_size) {
        return Status::Corruption("Invalid BLOB record framing");
    }
    // CRC covers magic, payload and the stored record length, excluding the CRC itself.
    const auto crc = crc32(0, record, static_cast<uInt>(record_size - sizeof(uint32_t)));
    if (uint32_t(crc) != decode_fixed32_le(trailer + sizeof(uint64_t))) {
        return Status::Corruption("BLOB record checksum mismatch");
    }
    // Strip framing in place and keep the buffer capacity for the next payload.
    std::memmove(bytes->data(), bytes->data() + BLOB_HEADER_SIZE, static_cast<size_t>(range.size));
    bytes->resize(static_cast<size_t>(range.size));
    return Status::OK();
}

} // namespace doris::format
