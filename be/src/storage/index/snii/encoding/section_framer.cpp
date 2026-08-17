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

#include "storage/index/snii/encoding/section_framer.h"

#include "storage/index/snii/encoding/crc32c.h"

namespace doris::snii {

void SectionFramer::write(ByteSink& sink, uint8_t section_type, Slice payload) {
    // Single-copy framing: write [type][varint64 len][payload] straight into the
    // target sink, then crc exactly those bytes. view() is taken AFTER the payload
    // and BEFORE the crc, so subslice([start, framed_len)) is over a settled,
    // contiguous buffer with no pending realloc/aliasing. Byte-identical to the
    // former temp-ByteSink assembly, minus one heap alloc + one payload copy.
    const size_t start = sink.size();
    sink.put_u8(section_type);
    sink.put_varint64(payload.size());
    sink.put_bytes(payload);
    const size_t framed_len = sink.size() - start;
    const uint32_t crc = crc32c(sink.view().subslice(start, framed_len));
    sink.put_fixed32(crc);
}

Status SectionFramer::read(ByteSource& src, FramedSection* out) {
    size_t start = src.position();
    uint8_t type;
    RETURN_IF_ERROR(src.get_u8(&type));
    uint64_t len;
    RETURN_IF_ERROR(src.get_varint64(&len));
    Slice payload;
    RETURN_IF_ERROR(src.get_bytes(static_cast<size_t>(len), &payload));
    size_t framed_len = src.position() - start;
    uint32_t stored;
    RETURN_IF_ERROR(src.get_fixed32(&stored));
    if (crc32c(src.slice_from(start, framed_len)) != stored) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "section crc mismatch");
    }
    out->type = type;
    out->payload = payload;
    return Status::OK();
}

} // namespace doris::snii
