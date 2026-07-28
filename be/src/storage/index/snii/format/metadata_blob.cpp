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

#include "storage/index/snii/format/metadata_blob.h"

#include <cstdlib>

#include "storage/index/snii/encoding/byte_source.h"
#include "storage/index/snii/encoding/section_framer.h"
#include "storage/index/snii/encoding/zstd_codec.h"

namespace doris::snii::format {

namespace {

constexpr int kMetaSectionZstdLevel = 3;
constexpr uint64_t kMaxMetaSectionUncompBytes = 256ULL * 1024 * 1024;

size_t meta_compress_min_bytes() {
    const char* s = std::getenv("SNII_META_COMPRESS_MIN");
    if (s != nullptr) {
        char* end = nullptr;
        const unsigned long long v = std::strtoull(s, &end, 10);
        if (end != s) {
            return v;
        }
    }
    return kMetaSectionCompressMinBytes;
}

} // namespace

Status encode_metadata_blob(Slice raw_frame, SectionType raw_type, SectionType compressed_type,
                            ByteSink* out) {
    if (out == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("metadata_blob: null sink");
    }

    ByteSource source(raw_frame);
    FramedSection raw_section;
    RETURN_IF_ERROR(SectionFramer::read(source, &raw_section));
    if (!source.eof() || raw_section.type != static_cast<uint8_t>(raw_type)) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "metadata_blob: raw input is not exactly one frame of the expected type");
    }

    if (raw_frame.size() >= meta_compress_min_bytes()) {
        std::vector<uint8_t> compressed;
        if (zstd_compress(raw_frame, kMetaSectionZstdLevel, &compressed).ok()) {
            ByteSink payload;
            payload.put_varint64(raw_frame.size());
            payload.put_bytes(Slice(compressed));
            if (payload.size() + 16 < raw_frame.size()) {
                SectionFramer::write(*out, static_cast<uint8_t>(compressed_type), payload.view());
                return Status::OK();
            }
        }
    }
    out->put_bytes(raw_frame);
    return Status::OK();
}

Status materialize_metadata_blob(Slice stored_frame, SectionType raw_type,
                                 SectionType compressed_type, std::vector<uint8_t>* scratch,
                                 Slice* raw_frame) {
    if (scratch == nullptr || raw_frame == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("metadata_blob: null frame out");
    }

    ByteSource source(stored_frame);
    FramedSection stored_section;
    RETURN_IF_ERROR(SectionFramer::read(source, &stored_section));
    if (!source.eof()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "metadata_blob: trailing stored frame bytes");
    }
    if (stored_section.type == static_cast<uint8_t>(raw_type)) {
        *raw_frame = stored_frame;
        return Status::OK();
    }
    if (stored_section.type != static_cast<uint8_t>(compressed_type)) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "metadata_blob: unexpected stored frame type");
    }

    ByteSource payload(stored_section.payload);
    uint64_t uncomp_len = 0;
    RETURN_IF_ERROR(payload.get_varint64(&uncomp_len));
    if (uncomp_len == 0 || uncomp_len > kMaxMetaSectionUncompBytes) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "metadata_blob: zstd uncomp_len out of range");
    }
    Slice compressed;
    RETURN_IF_ERROR(payload.get_bytes(payload.remaining(), &compressed));
    RETURN_IF_ERROR(zstd_decompress(compressed, static_cast<size_t>(uncomp_len), scratch));
    ByteSource raw_source {Slice(*scratch)};
    FramedSection raw_section;
    RETURN_IF_ERROR(SectionFramer::read(raw_source, &raw_section));
    if (!raw_source.eof() || raw_section.type != static_cast<uint8_t>(raw_type)) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "metadata_blob: decompressed bytes are not exactly one expected raw frame");
    }
    *raw_frame = Slice(*scratch);
    return Status::OK();
}

} // namespace doris::snii::format
