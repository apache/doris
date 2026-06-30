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
#include <vector>

#include "storage/index/snii/encoding/crc32c.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/format/per_index_meta.h"
#include "storage/index/snii/format/stats_block.h"
#include "storage/index/snii/format/tail_pointer.h"

namespace doris::snii::reader {

using format::IndexTier;
using format::PerIndexMetaReader;
using format::StatsBlock;
using format::TailMetaRegionReader;
using format::TailPointer;

namespace {

// Reads the fixed tail pointer (last tail_pointer_size() bytes) of the file.
Status ReadTailPointer(io::FileReader* reader, TailPointer* tp) {
    const size_t tp_size = format::tail_pointer_size();
    const uint64_t total = reader->size();
    if (total < tp_size) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "segment: file smaller than tail pointer");
    }
    std::vector<uint8_t> buf;
    RETURN_IF_ERROR(reader->read_at(total - tp_size, tp_size, &buf));
    return format::decode_tail_pointer(Slice(buf), tp);
}

Status ReadTailMetaHeader(io::FileReader* reader, const TailPointer& tp,
                          format::TailMetaRegionHeader* header) {
    const size_t header_size = format::tail_meta_header_size();
    if (tp.meta_region_length < header_size) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "segment: tail meta region smaller than header");
    }
    std::vector<uint8_t> buf;
    RETURN_IF_ERROR(reader->read_at(tp.meta_region_offset, header_size, &buf));
    return format::TailMetaRegionReader::parse_header(Slice(buf), header);
}

} // namespace

Status SniiSegmentReader::open(io::FileReader* const reader, SniiSegmentReader* const out) {
    if (reader == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("segment: null reader");
    }
    if (out == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("segment: null out");
    }

    // NOTE: the per-segment bootstrap header (offset 0) is intentionally NOT read
    // here. It is still WRITTEN on disk for inspect tooling, but its only runtime
    // role -- gating the container format_version -- is already covered (more
    // strictly, exact-match) by ReadTailPointer below, which validates the 'TAIL'
    // magic, the embedded format_version == kFormatVersion, and the tail crc. The
    // open() path reads only the file tail, avoiding a redundant offset-0 cache
    // block / remote round-trip per segment on cold queries.
    //
    // The bootstrap header's min_reader_version forward-compat gate is intentionally
    // retired together with this read: the tail pointer carries format_version but not
    // min_reader_version, so future format evolution must bump format_version (which the
    // tail rejects on exact-match) rather than rely on a min_reader_version bump under a
    // stable format_version.
    TailPointer tp;
    RETURN_IF_ERROR(ReadTailPointer(reader, &tp));
    if (tp.meta_region_length == 0) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "segment: empty tail meta region");
    }

    format::TailMetaRegionHeader meta_header;
    RETURN_IF_ERROR(ReadTailMetaHeader(reader, tp, &meta_header));
    if (meta_header.meta_region_len != tp.meta_region_length) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "segment: tail meta length mismatch");
    }

    std::vector<uint8_t> directory;
    if (meta_header.directory_offset > UINT64_MAX - tp.meta_region_offset) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "segment: tail meta directory file offset overflow");
    }
    RETURN_IF_ERROR(reader->read_at(tp.meta_region_offset + meta_header.directory_offset,
                                    meta_header.directory_length, &directory));

    out->reader_ = reader;
    out->meta_region_offset_ = tp.meta_region_offset;
    out->meta_region_length_ = tp.meta_region_length;
    return TailMetaRegionReader::open_directory(meta_header, Slice(directory),
                                                &out->region_reader_);
}

Status SniiSegmentReader::read_index_meta(uint64_t index_id, std::string_view suffix,
                                          std::vector<uint8_t>* const out) const {
    if (out == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("segment: null meta out");
    }
    if (reader_ == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("segment: not opened");
    }

    bool found = false;
    format::LogicalIndexRef ref;
    RETURN_IF_ERROR(region_reader_.find_ref(index_id, suffix, &found, &ref));
    if (!found) {
        return Status::Error<ErrorCode::INVERTED_INDEX_SNII_NOT_FOUND, false>(
                "segment: logical index not found");
    }
    if (ref.meta_off > meta_region_length_ || ref.meta_len > meta_region_length_ - ref.meta_off) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "segment: logical index meta out of tail region");
    }
    if (ref.meta_off > UINT64_MAX - meta_region_offset_) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "segment: logical index meta file offset overflow");
    }
    RETURN_IF_ERROR(reader_->read_at(meta_region_offset_ + ref.meta_off, ref.meta_len, out));
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

    format::LogicalIndexRef ref;
    return region_reader_.find_ref(index_id, suffix, exists, &ref);
}

Status SniiSegmentReader::open_index_from_meta(Slice meta_bytes,
                                               LogicalIndexReader* const out) const {
    if (out == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("segment: null index out");
    }
    if (reader_ == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("segment: not opened");
    }
    // Determine tier / positions capability from the per-index meta. Positions
    // capability is read from the PERSISTED header flag (kHasPositions), NOT from
    // any region length: after the frq/prx merge, posting_region.length is non-zero
    // for ANY index with a pod_ref term -- docs-only included -- so a region-length
    // heuristic would mis-classify a docs-only index as positional and make
    // DictBlockReader::check_flags hard-fail. The "|| has_norms" is kept only as a
    // defensive belt-and-suspenders (a scoring index always has positions).
    PerIndexMetaReader meta;
    RETURN_IF_ERROR(PerIndexMetaReader::open(meta_bytes, &meta));
    const bool has_norms = meta.section_refs().norms.length > 0;
    const bool has_positions = meta.has_positions() || has_norms;
    IndexTier tier = IndexTier::kT1;
    if (has_norms) {
        tier = IndexTier::kT3;
    } else if (has_positions) {
        tier = IndexTier::kT2;
    }

    return LogicalIndexReader::open(reader_, tier, has_positions, meta_bytes, out);
}

Status SniiSegmentReader::open_index(uint64_t index_id, std::string_view suffix,
                                     LogicalIndexReader* const out) const {
    std::vector<uint8_t> meta_bytes;
    RETURN_IF_ERROR(read_index_meta(index_id, suffix, &meta_bytes));
    return open_index_from_meta(Slice(meta_bytes), out);
}

Status SniiSegmentReader::section_refs_for_index(uint64_t index_id, std::string_view suffix,
                                                 format::SectionRefs* const out) const {
    if (out == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("segment: null section refs out");
    }
    if (reader_ == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("segment: not opened");
    }

    std::vector<uint8_t> meta_bytes;
    RETURN_IF_ERROR(read_index_meta(index_id, suffix, &meta_bytes));
    PerIndexMetaReader meta;
    RETURN_IF_ERROR(PerIndexMetaReader::open(Slice(meta_bytes), &meta));
    *out = meta.section_refs();
    return Status::OK();
}

} // namespace doris::snii::reader
