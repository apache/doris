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

#include "snii/format/null_bitmap.h"

#include <limits>
#include <vector>

#include "roaring/roaring.h"
#include "roaring/roaring.hh"
#include "snii/common/slice.h"
#include "snii/encoding/byte_source.h"
#include "snii/encoding/section_framer.h"

namespace snii::format {
using doris::Status; // RETURN_IF_ERROR expands to bare Status

NullBitmapWriter::
        NullBitmapWriter() // NOLINT(modernize-use-equals-default): roaring type is incomplete in the header.
        : bitmap_(std::make_unique<roaring::Roaring>()) {}

NullBitmapWriter::~NullBitmapWriter() = default;

void NullBitmapWriter::add_null(uint32_t docid) {
    bitmap_->add(docid);
}

uint32_t NullBitmapWriter::null_count() const {
    return static_cast<uint32_t>(bitmap_->cardinality());
}

void NullBitmapWriter::finish(uint32_t doc_count, ByteSink* sink) const {
    // Serialize the Roaring bitmap to its portable on-disk form.
    const size_t roaring_size = bitmap_->getSizeInBytes();
    std::vector<char> roaring_buf(roaring_size);
    bitmap_->write(roaring_buf.data());

    // Build inner payload: [varint64 doc_count][varint64 roaring_size][bytes].
    ByteSink payload;
    payload.put_varint64(doc_count);
    payload.put_varint64(roaring_size);
    payload.put_bytes(Slice(reinterpret_cast<const uint8_t*>(roaring_buf.data()), roaring_size));

    // Delegate the type + len + crc32c envelope to SectionFramer.
    SectionFramer::write(*sink, kNullBitmapSectionType, payload.view());
}

NullBitmapReader::
        NullBitmapReader() // NOLINT(modernize-use-equals-default): roaring type is incomplete in the header.
        : bitmap_(std::make_unique<roaring::Roaring>()) {}

NullBitmapReader::~NullBitmapReader() = default;

NullBitmapReader::NullBitmapReader(NullBitmapReader&&) noexcept = default;
NullBitmapReader& NullBitmapReader::operator=(NullBitmapReader&&) noexcept = default;

doris::Status NullBitmapReader::open(Slice framed, NullBitmapReader* out) {
    // SectionFramer handles CRC verification, truncation detection, and payload
    // slicing.
    ByteSource src(framed);
    FramedSection sec;
    RETURN_IF_ERROR(SectionFramer::read(src, &sec));

    // Parse inner payload: [varint64 doc_count][varint64 roaring_size][bytes].
    ByteSource payload(sec.payload);
    uint64_t doc_count = 0;
    RETURN_IF_ERROR(payload.get_varint64(&doc_count));
    if (doc_count > std::numeric_limits<uint32_t>::max()) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("null bitmap doc_count overflows uint32");
    }

    uint64_t roaring_size = 0;
    RETURN_IF_ERROR(payload.get_varint64(&roaring_size));
    // Anti-DoS: the declared roaring_size must not exceed the bytes actually
    // present, otherwise readSafe could be told to walk past the payload.
    if (roaring_size > payload.remaining()) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("null bitmap roaring_size exceeds payload");
    }

    Slice roaring_bytes;
    RETURN_IF_ERROR(payload.get_bytes(static_cast<size_t>(roaring_size), &roaring_bytes));

    // Validate the Roaring container BEFORE deserializing. A CRC-valid frame can
    // still carry malformed roaring bytes; Roaring::readSafe / read would then hit
    // CRoaring's terminate-or-throw path (NULL -> ROARING_TERMINATE). The safe,
    // non-throwing C probe returns the exact byte count a valid container would
    // consume, or 0 on malformed/insufficient input.
    const char* rb = reinterpret_cast<const char*>(roaring_bytes.data());
    const size_t probed =
            roaring_bitmap_portable_deserialize_size(rb, static_cast<size_t>(roaring_size));
    if (probed == 0 || probed != static_cast<size_t>(roaring_size)) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("null bitmap: malformed roaring container");
    }
    *out->bitmap_ = roaring::Roaring::readSafe(rb, static_cast<size_t>(roaring_size));
    out->doc_count_ = static_cast<uint32_t>(doc_count);
    return doris::Status::OK();
}

bool NullBitmapReader::is_null(uint32_t docid) const {
    return bitmap_->contains(docid);
}

uint32_t NullBitmapReader::null_count() const {
    return static_cast<uint32_t>(bitmap_->cardinality());
}

void NullBitmapReader::copy_to(roaring::Roaring* out) const {
    *out = *bitmap_;
}

} // namespace snii::format
