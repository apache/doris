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

#include "snii/format/logical_index_directory.h"

#include "snii/encoding/byte_source.h"
#include "snii/encoding/section_framer.h"
#include "snii/format/format_constants.h"

namespace snii::format {
using doris::Status; // RETURN_IF_ERROR expands to bare Status

namespace {

// Minimum payload bytes any entry can occupy: index_id (>=1) + suffix_len (>=1, value 0) +
// meta_off (>=1) + meta_len (>=1). Used as an anti-DoS lower bound before reserving.
constexpr size_t kMinEntryBytes = 4;

// Encode one directory entry. Fixed field order; reuse ByteSink varint/bytes primitives.
void encode_entry(const LogicalIndexRef& ref, ByteSink* payload) {
    payload->put_varint64(ref.index_id);
    payload->put_varint32(static_cast<uint32_t>(ref.index_suffix.size()));
    payload->put_bytes(Slice(std::string_view(ref.index_suffix)));
    payload->put_varint64(ref.meta_off);
    payload->put_varint64(ref.meta_len);
}

// Decode one directory entry, validating suffix_len against the remaining payload before copying.
doris::Status decode_entry(ByteSource* ps, LogicalIndexRef* ref) {
    RETURN_IF_ERROR(ps->get_varint64(&ref->index_id));
    uint32_t suffix_len = 0;
    RETURN_IF_ERROR(ps->get_varint32(&suffix_len));
    // Anti-DoS: reject a suffix_len that cannot fit in the remaining bytes before allocating.
    if (suffix_len > ps->remaining()) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "logical_index_directory: suffix_len exceeds payload");
    }
    Slice suffix;
    RETURN_IF_ERROR(ps->get_bytes(suffix_len, &suffix));
    ref->index_suffix.assign(reinterpret_cast<const char*>(suffix.data()), suffix.size());
    RETURN_IF_ERROR(ps->get_varint64(&ref->meta_off));
    RETURN_IF_ERROR(ps->get_varint64(&ref->meta_len));
    return doris::Status::OK();
}

doris::Status decode_payload(Slice payload, std::vector<LogicalIndexRef>* refs) {
    ByteSource ps(payload);
    uint32_t n_entries = 0;
    RETURN_IF_ERROR(ps.get_varint32(&n_entries));
    // Anti-DoS: cap n_entries against the remaining payload before reserving, so a corrupted
    // inflated count cannot trigger a huge allocation.
    if (n_entries > ps.remaining() / kMinEntryBytes) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "logical_index_directory: n_entries exceeds payload capacity");
    }
    refs->clear();
    refs->reserve(n_entries);
    for (uint32_t i = 0; i < n_entries; ++i) {
        LogicalIndexRef ref {};
        RETURN_IF_ERROR(decode_entry(&ps, &ref));
        refs->push_back(std::move(ref));
    }
    if (!ps.eof()) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "logical_index_directory: trailing bytes in payload");
    }
    return doris::Status::OK();
}

} // namespace

void LogicalIndexDirectoryBuilder::finish(ByteSink* sink) const {
    ByteSink payload;
    payload.put_varint32(static_cast<uint32_t>(refs_.size()));
    for (const auto& ref : refs_) {
        encode_entry(ref, &payload);
    }
    SectionFramer::write(*sink, static_cast<uint8_t>(SectionType::kLogicalIndexDirectory),
                         payload.view());
}

doris::Status LogicalIndexDirectoryReader::open(Slice framed, LogicalIndexDirectoryReader* out) {
    if (out == nullptr) {
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>(
                "logical_index_directory: out is null");
    }
    ByteSource src(framed);
    FramedSection sec;
    RETURN_IF_ERROR(SectionFramer::read(src, &sec));
    if (sec.type != static_cast<uint8_t>(SectionType::kLogicalIndexDirectory)) {
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>(
                "logical_index_directory: unexpected section type");
    }
    return decode_payload(sec.payload, &out->refs_);
}

doris::Status LogicalIndexDirectoryReader::get(uint32_t i, LogicalIndexRef* out) const {
    if (out == nullptr) {
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>(
                "logical_index_directory: out is null");
    }
    if (i >= refs_.size()) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_SNII_NOT_FOUND, false>(
                "logical_index_directory: index out of range");
    }
    *out = refs_[i];
    return doris::Status::OK();
}

doris::Status LogicalIndexDirectoryReader::find(uint64_t index_id, std::string_view suffix,
                                                bool* found, LogicalIndexRef* out) const {
    if (found == nullptr || out == nullptr) {
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>(
                "logical_index_directory: output pointer is null");
    }
    *found = false;
    for (const auto& ref : refs_) {
        if (ref.index_id != index_id || std::string_view(ref.index_suffix) != suffix) {
            continue;
        }
        *out = ref;
        *found = true;
        return doris::Status::OK();
    }
    return doris::Status::OK();
}

} // namespace snii::format
