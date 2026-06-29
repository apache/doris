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

#include "snii/format/per_index_meta.h"

#include "snii/encoding/byte_source.h"
#include "snii/encoding/crc32c.h"
#include "snii/encoding/section_framer.h"

namespace snii::format {
using doris::Status; // RETURN_IF_ERROR expands to bare Status

namespace {

// Upper bound on index_suffix length read from untrusted bytes, capped before
// allocation to avoid a DoS-inflated reserve. A logical index suffix is a short
// column/field name; 64 KiB is far beyond any legitimate value.
constexpr uint32_t kMaxSuffixLen = 64u * 1024u;

void encode_region(const RegionRef& r, ByteSink* payload) {
    payload->put_varint64(r.offset);
    payload->put_varint64(r.length);
}

doris::Status decode_region(ByteSource* ps, RegionRef* r) {
    RETURN_IF_ERROR(ps->get_varint64(&r->offset));
    RETURN_IF_ERROR(ps->get_varint64(&r->length));
    return doris::Status::OK();
}

// SectionRefs payload: five RegionRefs in fixed order, each as varint64 pair.
// Order: dict_region, posting_region, norms, null_bitmap, bsbf.
void encode_section_refs(const SectionRefs& refs, ByteSink* sink) {
    ByteSink payload;
    encode_region(refs.dict_region, &payload);
    encode_region(refs.posting_region, &payload);
    encode_region(refs.norms, &payload);
    encode_region(refs.null_bitmap, &payload);
    encode_region(refs.bsbf, &payload);
    SectionFramer::write(*sink, static_cast<uint8_t>(SectionType::kSectionRefs), payload.view());
}

doris::Status decode_section_refs(Slice payload, SectionRefs* out) {
    ByteSource ps(payload);
    RETURN_IF_ERROR(decode_region(&ps, &out->dict_region));
    RETURN_IF_ERROR(decode_region(&ps, &out->posting_region));
    RETURN_IF_ERROR(decode_region(&ps, &out->norms));
    RETURN_IF_ERROR(decode_region(&ps, &out->null_bitmap));
    RETURN_IF_ERROR(decode_region(&ps, &out->bsbf));
    if (!ps.eof()) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("per_index_meta: trailing bytes in section_refs");
    }
    return doris::Status::OK();
}

// Writes the self-checksummed header prefix. Layout matches the class comment.
void encode_header(uint64_t index_id, const std::string& suffix, uint32_t flags, ByteSink* sink) {
    ByteSink head;
    head.put_fixed16(kMetaFormatVersion);
    head.put_varint64(index_id);
    head.put_varint32(static_cast<uint32_t>(suffix.size()));
    head.put_bytes(Slice(suffix));
    head.put_fixed32(flags);
    uint32_t crc = crc32c(head.view());
    sink->put_bytes(head.view());
    sink->put_fixed32(crc);
}

// Parses and crc-verifies the header prefix, advancing src past the crc field.
doris::Status decode_header(Slice block, ByteSource* src, uint64_t* index_id, std::string* suffix,
                     uint32_t* flags) {
    size_t start = src->position();
    uint16_t version = 0;
    RETURN_IF_ERROR(src->get_fixed16(&version));
    if (version != kMetaFormatVersion) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("per_index_meta: unsupported meta_format_version");
    }
    RETURN_IF_ERROR(src->get_varint64(index_id));
    uint32_t suffix_len = 0;
    RETURN_IF_ERROR(src->get_varint32(&suffix_len));
    if (suffix_len > kMaxSuffixLen || suffix_len > src->remaining()) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("per_index_meta: suffix_len exceeds bounds");
    }
    Slice suffix_view;
    RETURN_IF_ERROR(src->get_bytes(suffix_len, &suffix_view));
    RETURN_IF_ERROR(src->get_fixed32(flags));
    size_t covered = src->position() - start;
    uint32_t stored = 0;
    RETURN_IF_ERROR(src->get_fixed32(&stored));
    if (crc32c(block.subslice(start, covered)) != stored) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("per_index_meta: header crc mismatch");
    }
    suffix->assign(reinterpret_cast<const char*>(suffix_view.data()), suffix_view.size());
    return doris::Status::OK();
}

// Reads one framed section, returning both its type and the FULL frame Slice
// (type+len+payload+crc) so it can be re-opened by a sub-module reader. The
// framer itself crc-verifies the frame.
doris::Status read_frame(Slice block, ByteSource* src, uint8_t* type, Slice* frame) {
    size_t start = src->position();
    FramedSection sec;
    RETURN_IF_ERROR(SectionFramer::read(*src, &sec));
    *type = sec.type;
    *frame = block.subslice(start, src->position() - start);
    return doris::Status::OK();
}

// Captures one frame into the matching reader field by section type. Returns
// false (via *handled) for unrecognized types so the caller skips them.
// Routes an optional sub-section frame to its slot. Unknown section types are
// intentionally ignored (forward compatibility: skip unknown optional sections).
void dispatch_frame(uint8_t type, Slice frame, Slice* sampled, Slice* dict) {
    if (type == static_cast<uint8_t>(SectionType::kSampledTermIndex)) {
        *sampled = frame;
    } else if (type == static_cast<uint8_t>(SectionType::kDictBlockDirectory)) {
        *dict = frame;
    }
}

} // namespace

PerIndexMetaBuilder::PerIndexMetaBuilder(uint64_t index_id, std::string index_suffix,
                                         uint32_t flags)
        : index_id_(index_id), index_suffix_(std::move(index_suffix)), flags_(flags) {}

void PerIndexMetaBuilder::set_stats(const StatsBlock& stats) {
    stats_ = stats;
}

void PerIndexMetaBuilder::set_sampled_term_index(Slice framed_bytes) {
    sampled_term_index_.assign(framed_bytes.data(), framed_bytes.data() + framed_bytes.size());
}

void PerIndexMetaBuilder::set_dict_block_directory(Slice framed_bytes) {
    dict_block_directory_.assign(framed_bytes.data(), framed_bytes.data() + framed_bytes.size());
}

void PerIndexMetaBuilder::set_section_refs(const SectionRefs& refs) {
    section_refs_ = refs;
}

void PerIndexMetaBuilder::add_raw_section(Slice framed_bytes) {
    extra_sections_.emplace_back(framed_bytes.data(), framed_bytes.data() + framed_bytes.size());
}

doris::Status PerIndexMetaBuilder::finish(ByteSink* sink) const {
    if (sink == nullptr) {
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>("per_index_meta: null sink");
    }
    encode_header(index_id_, index_suffix_, flags_, sink);
    encode_stats_block(stats_, sink);
    sink->put_bytes(Slice(sampled_term_index_));
    sink->put_bytes(Slice(dict_block_directory_));
    encode_section_refs(section_refs_, sink);
    for (const auto& extra : extra_sections_) {
        sink->put_bytes(Slice(extra));
    }
    return doris::Status::OK();
}

doris::Status PerIndexMetaReader::open(Slice block, PerIndexMetaReader* out) {
    if (out == nullptr) {
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>("per_index_meta: null reader");
    }
    ByteSource src(block);
    RETURN_IF_ERROR(
            decode_header(block, &src, &out->index_id_, &out->index_suffix_, &out->flags_));
    bool have_stats = false;
    bool have_refs = false;
    while (!src.eof()) {
        uint8_t type = 0;
        Slice frame;
        RETURN_IF_ERROR(read_frame(block, &src, &type, &frame));
        if (type == static_cast<uint8_t>(SectionType::kStatsBlock)) {
            ByteSource fs(frame);
            RETURN_IF_ERROR(decode_stats_block(&fs, &out->stats_));
            have_stats = true;
        } else if (type == static_cast<uint8_t>(SectionType::kSectionRefs)) {
            FramedSection sec;
            ByteSource fs(frame);
            RETURN_IF_ERROR(SectionFramer::read(fs, &sec));
            RETURN_IF_ERROR(decode_section_refs(sec.payload, &out->section_refs_));
            have_refs = true;
        } else {
            dispatch_frame(type, frame, &out->sampled_term_index_, &out->dict_block_directory_);
        }
    }
    if (!have_stats || !have_refs) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("per_index_meta: missing required sub-section");
    }
    return doris::Status::OK();
}

} // namespace snii::format
