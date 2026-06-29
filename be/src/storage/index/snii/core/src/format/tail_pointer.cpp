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

#include "snii/format/tail_pointer.h"

#include "snii/encoding/byte_source.h"
#include "snii/encoding/crc32c.h"
#include "snii/format/format_constants.h"

namespace snii::format {
using doris::Status; // RETURN_IF_ERROR expands to bare Status

namespace {

// Byte widths of every fixed field, used to derive the constant on-disk size:
// u32 magic + u16 version + 3*u64 + 2*u32 + u8 size + u32 tail_checksum.
constexpr size_t kMagicBytes = 4;
constexpr size_t kVersionBytes = 2;
constexpr size_t kU64Bytes = 8;
constexpr size_t kU32Bytes = 4;
constexpr size_t kSizeByteBytes = 1;

constexpr size_t kFixedSize =
        kMagicBytes + kVersionBytes + 3 * kU64Bytes + 2 * kU32Bytes + kSizeByteBytes + kU32Bytes;
// tail_checksum is the trailing u32 and covers every byte before it.
constexpr size_t kChecksumCoverage = kFixedSize - kU32Bytes;

// Serializes the checksum-covered region in fixed field order into covered.
void serialize_covered(const TailPointer& tp, ByteSink* covered) {
    covered->put_fixed32(kTailMagic);
    covered->put_fixed16(kFormatVersion);
    covered->put_fixed64(tp.meta_region_offset);
    covered->put_fixed64(tp.meta_region_length);
    covered->put_fixed64(tp.hot_off);
    covered->put_fixed32(tp.meta_region_checksum);
    covered->put_fixed32(tp.bootstrap_header_checksum);
    covered->put_u8(static_cast<uint8_t>(kFixedSize));
}

} // namespace

size_t tail_pointer_size() {
    return kFixedSize;
}

doris::Status encode_tail_pointer(const TailPointer& tp, ByteSink* sink) {
    ByteSink covered;
    serialize_covered(tp, &covered);
    if (covered.size() != kChecksumCoverage) {
        return doris::Status::Error<doris::ErrorCode::INTERNAL_ERROR, false>(
                "tail_pointer: covered size mismatch");
    }
    const uint32_t tail_checksum = crc32c(covered.view());
    sink->put_bytes(covered.view());
    sink->put_fixed32(tail_checksum);
    return doris::Status::OK();
}

doris::Status decode_tail_pointer(Slice last_bytes, TailPointer* out) {
    // Anti-DoS / framing: the tail pointer is a fixed-size footer, so reject any
    // input that is not exactly the fixed size before touching its contents.
    if (last_bytes.size() != kFixedSize) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "tail_pointer: input is not the fixed size");
    }
    // Verify the trailing tail_checksum over the covered region first; a mismatch
    // means any parsed field would be untrustworthy.
    const Slice covered = last_bytes.subslice(0, kChecksumCoverage);
    ByteSource src(last_bytes);

    uint32_t magic = 0;
    RETURN_IF_ERROR(src.get_fixed32(&magic));
    if (magic != kTailMagic) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "tail_pointer: bad magic");
    }

    uint16_t tail_format_version = 0;
    RETURN_IF_ERROR(src.get_fixed16(&tail_format_version));
    if (tail_format_version != kFormatVersion) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_NOT_SUPPORTED, false>(
                "tail_pointer: unsupported container format_version");
    }
    RETURN_IF_ERROR(src.get_fixed64(&out->meta_region_offset));
    RETURN_IF_ERROR(src.get_fixed64(&out->meta_region_length));
    RETURN_IF_ERROR(src.get_fixed64(&out->hot_off));
    RETURN_IF_ERROR(src.get_fixed32(&out->meta_region_checksum));
    RETURN_IF_ERROR(src.get_fixed32(&out->bootstrap_header_checksum));

    uint8_t on_disk_size = 0;
    RETURN_IF_ERROR(src.get_u8(&on_disk_size));
    if (on_disk_size != kFixedSize) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "tail_pointer: embedded size mismatch");
    }

    uint32_t tail_checksum = 0;
    RETURN_IF_ERROR(src.get_fixed32(&tail_checksum));
    if (tail_checksum != crc32c(covered)) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "tail_pointer: tail_checksum mismatch");
    }
    return doris::Status::OK();
}

} // namespace snii::format
