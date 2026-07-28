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

#include "storage/index/snii/format/tail_pointer.h"

#include "storage/index/snii/encoding/byte_source.h"
#include "storage/index/snii/encoding/crc32c.h"
#include "storage/index/snii/format/format_constants.h"

namespace doris::snii::format {

namespace {

// Byte widths of every fixed field, used to derive the constant on-disk size:
// u32 magic + u16 version + 2*u64 + u32 directory crc + u8 size + u32 tail crc.
constexpr size_t kMagicBytes = 4;
constexpr size_t kVersionBytes = 2;
constexpr size_t kU64Bytes = 8;
constexpr size_t kU32Bytes = 4;
constexpr size_t kSizeByteBytes = 1;

constexpr size_t kFixedSize =
        kMagicBytes + kVersionBytes + 2 * kU64Bytes + kU32Bytes + kSizeByteBytes + kU32Bytes;
// tail_checksum is the trailing u32 and covers every byte before it.
constexpr size_t kChecksumCoverage = kFixedSize - kU32Bytes;

// Serializes the checksum-covered region in fixed field order into covered.
void serialize_covered(const TailPointer& tp, ByteSink* covered) {
    covered->put_fixed32(kTailMagic);
    covered->put_fixed16(kFormatVersion);
    covered->put_fixed64(tp.directory_offset);
    covered->put_fixed64(tp.directory_length);
    covered->put_fixed32(tp.directory_crc32c);
    covered->put_u8(static_cast<uint8_t>(kFixedSize));
}

} // namespace

size_t tail_pointer_size() {
    return kFixedSize;
}

Status encode_tail_pointer(const TailPointer& tp, ByteSink* sink) {
    if (sink == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("tail_pointer: null sink");
    }
    ByteSink covered;
    serialize_covered(tp, &covered);
    DORIS_CHECK_EQ(covered.size(), kChecksumCoverage);
    const uint32_t tail_checksum = crc32c(covered.view());
    sink->put_bytes(covered.view());
    sink->put_fixed32(tail_checksum);
    return Status::OK();
}

Status decode_tail_pointer(Slice last_bytes, TailPointer* out) {
    if (out == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("tail_pointer: null output");
    }
    // Anti-DoS / framing: the tail pointer is a fixed-size footer, so reject any
    // input that is not exactly the fixed size before touching its contents.
    if (last_bytes.size() != kFixedSize) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "tail_pointer: input is not the fixed size");
    }
    const Slice covered = last_bytes.subslice(0, kChecksumCoverage);
    DORIS_CHECK_EQ(covered.size(), kChecksumCoverage);
    ByteSource checksum_source(last_bytes.subslice(kChecksumCoverage, kU32Bytes));
    uint32_t tail_checksum = 0;
    RETURN_IF_ERROR(checksum_source.get_fixed32(&tail_checksum));
    DORIS_CHECK(checksum_source.eof());
    if (tail_checksum != crc32c(covered)) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "tail_pointer: tail_checksum mismatch");
    }

    // Only interpret fields after authenticating the complete covered region.
    ByteSource src(covered);

    uint32_t magic = 0;
    RETURN_IF_ERROR(src.get_fixed32(&magic));
    if (magic != kTailMagic) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "tail_pointer: bad magic");
    }

    uint16_t tail_format_version = 0;
    RETURN_IF_ERROR(src.get_fixed16(&tail_format_version));
    if (tail_format_version != kFormatVersion) {
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED, false>(
                "tail_pointer: unsupported container format_version");
    }
    RETURN_IF_ERROR(src.get_fixed64(&out->directory_offset));
    RETURN_IF_ERROR(src.get_fixed64(&out->directory_length));
    RETURN_IF_ERROR(src.get_fixed32(&out->directory_crc32c));

    uint8_t on_disk_size = 0;
    RETURN_IF_ERROR(src.get_u8(&on_disk_size));
    if (on_disk_size != kFixedSize) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "tail_pointer: embedded size mismatch");
    }

    DORIS_CHECK(src.eof());
    return Status::OK();
}

} // namespace doris::snii::format
