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

#include "storage/index/snii/format/norms_pod.h"

#include <limits>

#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_source.h"
#include "storage/index/snii/encoding/section_framer.h"
#include "storage/index/snii/encoding/varint.h"
#include "storage/index/snii/format/format_constants.h"

namespace doris::snii::format {

void NormsPodWriter::finish(ByteSink* sink) const {
    finish(norms_, sink);
}

void NormsPodWriter::finish(std::span<const uint8_t> norms, ByteSink* sink) {
    // Build inner payload: [varint64 doc_count][raw norm bytes].
    ByteSink payload;
    const size_t payload_size = varint_len(norms.size()) + norms.size();
    payload.reserve(payload_size);
    payload.put_varint64(norms.size());
    payload.put_bytes(Slice(norms.data(), norms.size()));
    // Delegate outer framing to SectionFramer to append type+len+crc32c, avoiding manual checksum assembly.
    sink->reserve(1 + varint_len(payload_size) + payload_size + sizeof(uint32_t));
    SectionFramer::write(*sink, static_cast<uint8_t>(SectionType::kNormsPod), payload.view());
}

Status NormsPodReader::open(Slice framed, NormsPodReader* out) {
    // framer handles CRC verify, truncation detection, and payload slicing.
    ByteSource src(framed);
    FramedSection sec;
    RETURN_IF_ERROR(SectionFramer::read(src, &sec));
    if (sec.type != static_cast<uint8_t>(SectionType::kNormsPod)) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "norms POD section type mismatch");
    }
    if (!src.eof()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "norms POD trailing framed bytes");
    }

    // Parse inner payload: [varint64 doc_count][bytes].
    ByteSource payload(sec.payload);
    uint64_t doc_count = 0;
    RETURN_IF_ERROR(payload.get_varint64(&doc_count));
    if (payload.position() != varint_len(doc_count)) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "norms POD non-canonical doc_count");
    }
    if (doc_count > std::numeric_limits<uint32_t>::max()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "norms POD doc_count overflows uint32");
    }
    // doc_count must exactly equal the remaining byte count (1 byte per doc).
    if (payload.remaining() != doc_count) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "norms POD length mismatch");
    }

    Slice bytes;
    RETURN_IF_ERROR(payload.get_bytes(static_cast<size_t>(doc_count), &bytes));
    out->doc_count_ = static_cast<uint32_t>(doc_count);
    out->norms_ = bytes.data();
    return Status::OK();
}

} // namespace doris::snii::format
