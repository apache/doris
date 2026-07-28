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

#include "storage/index/snii/reader/snii_segment_reader.h"

#include <cstdint>
#include <limits>
#include <string_view>
#include <vector>

#include "storage/index/snii/encoding/crc32c.h"
#include "storage/index/snii/format/core_metadata.h"
#include "storage/index/snii/format/tail_pointer.h"

namespace doris::snii::reader {
namespace {

Status corrupted(std::string_view message) {
    return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(message);
}

Status read_tail_pointer(io::FileReader* reader, format::TailPointer* tail,
                         uint64_t* footer_offset) {
    const size_t footer_size = format::tail_pointer_size();
    const uint64_t total = reader->size();
    if (total < footer_size) {
        return corrupted("segment: file smaller than tail pointer");
    }
    *footer_offset = total - footer_size;
    std::vector<uint8_t> bytes;
    RETURN_IF_ERROR(reader->read_at(*footer_offset, footer_size, &bytes));
    return format::decode_tail_pointer(Slice(bytes), tail);
}

// Proves Core -> STI -> DBD are adjacent and end at or before directory_offset, so open_index() can
// cover the whole group with one range read. It also bounds core.length + sti.length + dbd.length by
// directory_offset (itself bounded by the file size), which is why open_index() may sum and narrow
// those on-disk 64-bit lengths to size_t without a further overflow check.
Status validate_metadata_group(const format::LogicalIndexMetadataRef& entry,
                               uint64_t directory_offset) {
    const auto& core = entry.core_metadata;
    const auto& sti = entry.sampled_term_index;
    const auto& dbd = entry.dict_block_directory;
    if (core.offset > directory_offset || core.length > directory_offset - core.offset) {
        return corrupted("segment: Core metadata reference is outside metadata area");
    }
    const uint64_t sti_offset = core.offset + core.length;
    if (sti.offset != sti_offset) {
        return corrupted("segment: STI metadata is not adjacent to Core metadata");
    }
    if (sti.length > directory_offset - sti.offset) {
        return corrupted("segment: STI metadata reference is outside metadata area");
    }
    const uint64_t dbd_offset = sti.offset + sti.length;
    if (dbd.offset != dbd_offset) {
        return corrupted("segment: DBD metadata is not adjacent to STI metadata");
    }
    if (dbd.length > directory_offset - dbd.offset) {
        return corrupted("segment: DBD metadata reference is outside metadata area");
    }
    return Status::OK();
}

Status find_metadata_ref(const format::MetadataDirectory& directory, uint64_t index_id,
                         std::string_view suffix, const format::LogicalIndexMetadataRef** out) {
    *out = directory.find(index_id, suffix);
    if (*out == nullptr) {
        return Status::Error<ErrorCode::INVERTED_INDEX_SNII_NOT_FOUND, false>(
                "segment: logical index not found");
    }
    return Status::OK();
}

} // namespace

Status SniiSegmentReader::open(io::FileReader* const reader, SniiSegmentReader* const out) {
    if (reader == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("segment: null reader");
    }
    if (out == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("segment: null out");
    }
    *out = {};

    // The per-segment bootstrap header at offset zero remains an inspect-tool record and is
    // intentionally not read here. The footer validates the exact format version and its own CRC,
    // and it locates the metadata directory. Reading only the file tail avoids an otherwise
    // redundant offset-zero cache block or remote round trip on cold queries. Future incompatible
    // evolution must bump the footer format version rather than rely on a min-reader-version change
    // under a stable format version.
    format::TailPointer tail;
    uint64_t footer_offset = 0;
    RETURN_IF_ERROR(read_tail_pointer(reader, &tail, &footer_offset));
    if (tail.directory_offset > footer_offset ||
        tail.directory_length > footer_offset - tail.directory_offset) {
        return corrupted("segment: metadata directory reference overlaps footer or EOF");
    }
    if (tail.directory_length > static_cast<uint64_t>(std::numeric_limits<int>::max())) {
        return corrupted("segment: metadata directory exceeds protobuf parse limit");
    }
    // The protobuf parse limit above already bounds the length, so narrowing it is safe.
    const auto directory_length = static_cast<size_t>(tail.directory_length);

    std::vector<uint8_t> directory_bytes;
    RETURN_IF_ERROR(reader->read_at(tail.directory_offset, directory_length, &directory_bytes));
    if (crc32c(Slice(directory_bytes)) != tail.directory_crc32c) {
        return corrupted("segment: metadata directory crc32c mismatch");
    }

    format::MetadataDirectory directory;
    RETURN_IF_ERROR(format::MetadataDirectory::decode(Slice(directory_bytes), &directory));
    for (const auto& entry : directory.entries()) {
        RETURN_IF_ERROR(validate_metadata_group(entry, tail.directory_offset));
    }

    out->reader_ = reader;
    out->directory_ = std::move(directory);
    return Status::OK();
}

Status SniiSegmentReader::index_exists(uint64_t index_id, std::string_view suffix,
                                       bool* const exists) const {
    if (exists == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("segment: null exists out");
    }
    if (reader_ == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("segment: not opened");
    }
    *exists = directory_.find(index_id, suffix) != nullptr;
    return Status::OK();
}

Status SniiSegmentReader::open_index(uint64_t index_id, std::string_view suffix,
                                     LogicalIndexReader* const out,
                                     LogicalIndexOpenMode open_mode) const {
    if (out == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("segment: null index out");
    }
    *out = {};
    if (reader_ == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("segment: not opened");
    }
    const format::LogicalIndexMetadataRef* entry = nullptr;
    RETURN_IF_ERROR(find_metadata_ref(directory_, index_id, suffix, &entry));

    // Safe to sum and narrow: open() ran validate_metadata_group on every directory entry.
    const auto core_length = static_cast<size_t>(entry->core_metadata.length);
    const auto sti_length = static_cast<size_t>(entry->sampled_term_index.length);
    const auto dbd_length = static_cast<size_t>(entry->dict_block_directory.length);
    const size_t group_length = core_length + sti_length + dbd_length;
    std::vector<uint8_t> group;
    RETURN_IF_ERROR(reader_->read_at(entry->core_metadata.offset, group_length, &group));
    DORIS_CHECK_EQ(group.size(), group_length);
    const Slice bytes(group);
    return LogicalIndexReader::open(
            reader_, bytes.subslice(0, core_length), bytes.subslice(core_length, sti_length),
            bytes.subslice(core_length + sti_length, dbd_length), out, open_mode);
}

Status SniiSegmentReader::section_refs_for_index(uint64_t index_id, std::string_view suffix,
                                                 format::SectionRefs* const out) const {
    if (out == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("segment: null section refs out");
    }
    if (reader_ == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("segment: not opened");
    }
    const format::LogicalIndexMetadataRef* entry = nullptr;
    RETURN_IF_ERROR(find_metadata_ref(directory_, index_id, suffix, &entry));
    std::vector<uint8_t> core_bytes;
    RETURN_IF_ERROR(reader_->read_at(entry->core_metadata.offset, entry->core_metadata.length,
                                     &core_bytes));
    format::CoreMetadata core;
    RETURN_IF_ERROR(format::decode_core_metadata(Slice(core_bytes), &core));
    *out = core.section_refs;
    return Status::OK();
}

} // namespace doris::snii::reader
