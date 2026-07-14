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

#include "storage/index/snii/format/dict_block_directory.h"

#include "storage/index/snii/encoding/byte_source.h"
#include "storage/index/snii/encoding/section_framer.h"
#include "storage/index/snii/format/format_constants.h"

namespace doris::snii::format {

namespace {

// Each block_ref has a fixed field order; reuse ByteSink varint/fixed primitives — do not hand-craft bytes manually.
// uncomp_len trails only when the kZstd flag is set, so uncompressed-block
// directories keep their compact (v1-identical) per-ref byte layout.
void encode_ref(const BlockRef& ref, ByteSink* payload) {
    payload->put_varint64(ref.offset);
    payload->put_varint64(ref.length);
    payload->put_varint32(ref.n_entries);
    payload->put_u8(ref.flags);
    payload->put_fixed32(ref.checksum);
    if (ref.flags & block_ref_flags::kZstd) payload->put_varint64(ref.uncomp_len);
}

Status decode_ref(ByteSource* ps, BlockRef* ref) {
    RETURN_IF_ERROR(ps->get_varint64(&ref->offset));
    RETURN_IF_ERROR(ps->get_varint64(&ref->length));
    RETURN_IF_ERROR(ps->get_varint32(&ref->n_entries));
    RETURN_IF_ERROR(ps->get_u8(&ref->flags));
    RETURN_IF_ERROR(ps->get_fixed32(&ref->checksum));
    if (ref->flags & block_ref_flags::kZstd) {
        RETURN_IF_ERROR(ps->get_varint64(&ref->uncomp_len));
    }
    return Status::OK();
}

Status decode_payload(Slice payload, std::vector<BlockRef>* refs) {
    ByteSource ps(payload);
    uint32_t n_blocks = 0;
    RETURN_IF_ERROR(ps.get_varint32(&n_blocks));
    // Guard against a corrupted, inflated count from untrusted bytes: each BlockRef
    // needs >= 8 bytes (flags u8 + checksum u32 + >= 1 byte for each of 3 varints),
    // so cap before reserve to avoid a huge allocation.
    constexpr size_t kMinRefBytes = 8;
    if (n_blocks > ps.remaining() / kMinRefBytes) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "dict_block_directory: n_blocks exceeds payload capacity");
    }
    refs->clear();
    refs->reserve(n_blocks);
    for (uint32_t i = 0; i < n_blocks; ++i) {
        BlockRef ref {};
        RETURN_IF_ERROR(decode_ref(&ps, &ref));
        refs->push_back(ref);
    }
    if (!ps.eof()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "dict_block_directory: trailing bytes in payload");
    }
    return Status::OK();
}

} // namespace

void DictBlockDirectoryBuilder::finish(ByteSink* sink) const {
    ByteSink payload;
    payload.put_varint32(static_cast<uint32_t>(refs_.size()));
    for (const auto& ref : refs_) {
        encode_ref(ref, &payload);
    }
    SectionFramer::write(*sink, static_cast<uint8_t>(SectionType::kDictBlockDirectory),
                         payload.view());
}

Status DictBlockDirectoryReader::open(Slice section, DictBlockDirectoryReader* out) {
    ByteSource src(section);
    FramedSection sec;
    RETURN_IF_ERROR(SectionFramer::read(src, &sec));
    if (sec.type != static_cast<uint8_t>(SectionType::kDictBlockDirectory)) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "dict_block_directory: unexpected section type");
    }
    return decode_payload(sec.payload, &out->refs_);
}

Status DictBlockDirectoryReader::get(uint32_t ordinal, BlockRef* out) const {
    if (ordinal >= refs_.size()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_SNII_NOT_FOUND, false>(
                "dict_block_directory: ordinal out of range");
    }
    *out = refs_[ordinal];
    return Status::OK();
}

} // namespace doris::snii::format
