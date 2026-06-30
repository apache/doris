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

#include "storage/index/snii/format/dict_entry.h"

#include <algorithm>

#include "storage/index/snii/common/slice.h"

namespace doris::snii::format {

namespace {

// Pure-function assembly / parsing of flags bits; avoids a long inline if-else
// chain.
uint8_t pack_flags(const DictEntry& e) {
    uint8_t f = 0;
    if (e.kind == DictEntryKind::kInline) f |= dict_flags::kKind;
    if (e.enc == DictEntryEnc::kWindowed) f |= dict_flags::kEnc;
    if (e.has_sb) f |= dict_flags::kHasSb;
    // bit3 has_champion / bit4 offsets_ref are always 0 in v1.
    return f;
}

void apply_flags(uint8_t f, DictEntry* e) {
    e->kind = (f & dict_flags::kKind) ? DictEntryKind::kInline : DictEntryKind::kPodRef;
    e->enc = (f & dict_flags::kEnc) ? DictEntryEnc::kWindowed : DictEntryEnc::kSlim;
    e->has_sb = (f & dict_flags::kHasSb) != 0;
}

// Length of the longest common prefix between term and prev_term.
uint32_t common_prefix_len(std::string_view term, std::string_view prev) {
    uint32_t n = 0;
    const uint32_t lim = static_cast<uint32_t>(std::min(term.size(), prev.size()));
    while (n < lim && term[n] == prev[n]) ++n;
    return n;
}

bool tier_has_stats(IndexTier tier) {
    return tier >= IndexTier::kT2;
}

// ---- Encode entry body (excluding entry_len and trailing crc) ----

void write_term_key(const DictEntry& e, std::string_view prev, ByteSink* sink) {
    const uint32_t prefix = common_prefix_len(e.term, prev);
    const std::string_view suffix = std::string_view(e.term).substr(prefix);
    sink->put_varint32(prefix);
    sink->put_varint32(static_cast<uint32_t>(suffix.size()));
    sink->put_bytes(Slice(suffix));
}

void write_stats(const DictEntry& e, IndexTier tier, ByteSink* sink) {
    sink->put_varint32(e.df);
    if (!tier_has_stats(tier)) return;
    sink->put_varint64(e.ttf_delta);
    sink->put_varint64(e.max_freq);
}

// Per-window codec mode byte shared by slim/inline single-window regions.
uint8_t pack_win_mode(const DictEntry& e) {
    uint8_t mode = 0;
    if (e.dd_meta.zstd) mode |= 1u << 0;   // dd_zstd
    if (e.freq_meta.zstd) mode |= 1u << 1; // freq_zstd
    return mode;
}

// Writes the slim/inline region codec metadata (dd always; freq when tier>=T2).
// store_crc=false (INLINE entries, format v2) omits the redundant per-region
// crc32c: the inline bytes already sit inside the dict block, whose own
// block-level crc32c covers them. POD-ref entries pass store_crc=true (their
// regions live in the separately-fetched .frq POD, uncovered by the block crc).
void write_region_meta(const DictEntry& e, IndexTier tier, bool store_crc, ByteSink* sink) {
    sink->put_u8(pack_win_mode(e));
    sink->put_varint64(e.dd_meta.uncomp_len);
    if (store_crc) sink->put_fixed32(e.dd_meta.crc);
    if (!tier_has_stats(tier)) return;
    sink->put_varint64(e.freq_meta.uncomp_len);
    if (store_crc) sink->put_fixed32(e.freq_meta.crc);
}

void write_pod_ref(const DictEntry& e, IndexTier tier, ByteSink* sink) {
    sink->put_varint64(e.frq_off_delta);
    sink->put_varint64(e.frq_len);
    if (e.enc == DictEntryEnc::kWindowed) {
        sink->put_varint64(e.prelude_len);
        sink->put_varint64(e.frq_docs_len);
    } else {
        sink->put_varint64(e.frq_docs_len); // slim pod_ref: dd region on-disk length
        // POD-ref regions live in the .frq POD (not covered by the block crc): keep
        // crc.
        write_region_meta(e, tier, /*store_crc=*/true, sink);
    }
    if (!tier_has_stats(tier)) return;
    sink->put_varint64(e.prx_off_delta);
    sink->put_varint64(e.prx_len);
}

void write_inline(const DictEntry& e, IndexTier tier, ByteSink* sink) {
    sink->put_varint64(static_cast<uint64_t>(e.frq_bytes.size()));
    sink->put_bytes(Slice(e.frq_bytes));
    sink->put_varint64(e.inline_dd_disk_len);
    // INLINE bytes are covered by the dict block crc32c: omit the redundant
    // per-region crc.
    write_region_meta(e, tier, /*store_crc=*/false, sink);
    if (!tier_has_stats(tier)) return;
    sink->put_varint64(static_cast<uint64_t>(e.prx_bytes.size()));
    sink->put_bytes(Slice(e.prx_bytes));
}

void write_body(const DictEntry& e, std::string_view prev, IndexTier tier, ByteSink* sink) {
    write_term_key(e, prev, sink);
    sink->put_u8(pack_flags(e));
    write_stats(e, tier, sink);
    if (e.kind == DictEntryKind::kInline) {
        write_inline(e, tier, sink);
    } else {
        write_pod_ref(e, tier, sink);
    }
}

// ---- Decode entry body ----

Status read_term_key(ByteSource* src, std::string_view prev, DictEntry* out) {
    uint32_t prefix = 0;
    uint32_t suffix_len = 0;
    RETURN_IF_ERROR(src->get_varint32(&prefix));
    RETURN_IF_ERROR(src->get_varint32(&suffix_len));
    if (prefix > prev.size()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "dict_entry: prefix_len exceeds prev_term length");
    }
    Slice suffix;
    RETURN_IF_ERROR(src->get_bytes(suffix_len, &suffix));
    out->term.assign(prev.substr(0, prefix));
    out->term.append(reinterpret_cast<const char*>(suffix.data()), suffix.size());
    return Status::OK();
}

Status read_stats(ByteSource* src, IndexTier tier, DictEntry* out) {
    RETURN_IF_ERROR(src->get_varint32(&out->df));
    if (!tier_has_stats(tier)) return Status::OK();
    RETURN_IF_ERROR(src->get_varint64(&out->ttf_delta));
    RETURN_IF_ERROR(src->get_varint64(&out->max_freq));
    return Status::OK();
}

// Reads the slim/inline region codec metadata (mode/uncomp/[crc]) and fills the
// dd/freq region disk_len from the supplied total/split lengths. has_crc=false
// (INLINE entries, format v2) means no per-region crc was stored: the on-disk
// crc field is absent and region decode must skip crc verification (verify_crc=
// false) since the dict block's own crc32c already covers the inline bytes.
Status read_region_meta(ByteSource* src, IndexTier tier, bool has_crc, uint64_t dd_disk_len,
                        uint64_t freq_disk_len, DictEntry* out) {
    uint8_t mode = 0;
    RETURN_IF_ERROR(src->get_u8(&mode));
    if ((mode & ~0x3u) != 0) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "dict_entry: unknown win_mode bits");
    }
    out->dd_meta.zstd = (mode & (1u << 0)) != 0;
    out->dd_meta.disk_len = dd_disk_len;
    out->dd_meta.verify_crc = has_crc;
    RETURN_IF_ERROR(src->get_varint64(&out->dd_meta.uncomp_len));
    if (has_crc) RETURN_IF_ERROR(src->get_fixed32(&out->dd_meta.crc));
    if (!tier_has_stats(tier)) {
        if (mode & (1u << 1)) {
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "dict_entry: freq mode set without freq tier");
        }
        return Status::OK();
    }
    out->freq_meta.zstd = (mode & (1u << 1)) != 0;
    out->freq_meta.disk_len = freq_disk_len;
    out->freq_meta.verify_crc = has_crc;
    RETURN_IF_ERROR(src->get_varint64(&out->freq_meta.uncomp_len));
    if (has_crc) RETURN_IF_ERROR(src->get_fixed32(&out->freq_meta.crc));
    return Status::OK();
}

Status read_pod_ref(ByteSource* src, IndexTier tier, DictEntry* out) {
    RETURN_IF_ERROR(src->get_varint64(&out->frq_off_delta));
    RETURN_IF_ERROR(src->get_varint64(&out->frq_len));
    if (out->enc == DictEntryEnc::kWindowed) {
        RETURN_IF_ERROR(src->get_varint64(&out->prelude_len));
        RETURN_IF_ERROR(src->get_varint64(&out->frq_docs_len));
        if (out->prelude_len == 0 || out->prelude_len > out->frq_docs_len ||
            out->frq_docs_len > out->frq_len) {
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "dict_entry: invalid windowed docs prefix");
        }
    } else {
        RETURN_IF_ERROR(src->get_varint64(&out->frq_docs_len));
        if (out->frq_docs_len > out->frq_len) {
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "dict_entry: frq_docs_len exceeds frq_len");
        }
        RETURN_IF_ERROR(read_region_meta(src, tier, /*has_crc=*/true, out->frq_docs_len,
                                         out->frq_len - out->frq_docs_len, out));
    }
    if (!tier_has_stats(tier)) return Status::OK();
    RETURN_IF_ERROR(src->get_varint64(&out->prx_off_delta));
    RETURN_IF_ERROR(src->get_varint64(&out->prx_len));
    return Status::OK();
}

Status read_byte_blob(ByteSource* src, std::vector<uint8_t>* out) {
    uint64_t len = 0;
    RETURN_IF_ERROR(src->get_varint64(&len));
    Slice bytes;
    RETURN_IF_ERROR(src->get_bytes(static_cast<size_t>(len), &bytes));
    out->assign(bytes.data(), bytes.data() + bytes.size());
    return Status::OK();
}

Status read_inline(ByteSource* src, IndexTier tier, DictEntry* out) {
    RETURN_IF_ERROR(read_byte_blob(src, &out->frq_bytes));
    RETURN_IF_ERROR(src->get_varint64(&out->inline_dd_disk_len));
    if (out->inline_dd_disk_len > out->frq_bytes.size()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "dict_entry: inline_dd_disk_len exceeds frq_bytes");
    }
    const uint64_t freq_disk_len =
            static_cast<uint64_t>(out->frq_bytes.size()) - out->inline_dd_disk_len;
    // INLINE entries store no per-region crc (covered by the block crc):
    // has_crc=false.
    RETURN_IF_ERROR(read_region_meta(src, tier, /*has_crc=*/false, out->inline_dd_disk_len,
                                     freq_disk_len, out));
    if (!tier_has_stats(tier)) return Status::OK();
    RETURN_IF_ERROR(read_byte_blob(src, &out->prx_bytes));
    return Status::OK();
}

Status read_locator(ByteSource* src, IndexTier tier, DictEntry* out) {
    if (out->kind == DictEntryKind::kInline) return read_inline(src, tier, out);
    return read_pod_ref(src, tier, out);
}

// Read entry_len (= body length) and verify that src has enough remaining
// bytes.
Status read_entry_len(ByteSource* src, uint64_t* total) {
    RETURN_IF_ERROR(src->get_varint64(total));
    if (*total > src->remaining()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "dict_entry: entry_len out of range");
    }
    return Status::OK();
}

} // namespace

Status encode_dict_entry(const DictEntry& entry, std::string_view prev_term, IndexTier tier,
                         ByteSink* sink) {
    if (sink == nullptr)
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("dict_entry: sink is null");

    // Serialize the body into a temporary buffer first to obtain the exact
    // length, then write entry_len + body. CRC verification is done uniformly at
    // the DICT block level (covering block header + all entries + anchor table);
    // CRC is not repeated at the entry level, to keep slim/inline low-frequency
    // terms maximally compact (spec §DICT block/§dict entry).
    ByteSink body;
    write_body(entry, prev_term, tier, &body);
    sink->put_varint64(static_cast<uint64_t>(body.size()));
    sink->put_bytes(body.view());
    return Status::OK();
}

Status decode_dict_entry(ByteSource* src, std::string_view prev_term, IndexTier tier,
                         DictEntry* out) {
    if (src == nullptr || out == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("dict_entry: src / out is null");
    }
    *out = DictEntry {};

    uint64_t total = 0;
    RETURN_IF_ERROR(read_entry_len(src, &total));
    const size_t body_start = src->position();

    RETURN_IF_ERROR(read_term_key(src, prev_term, out));
    uint8_t flags = 0;
    RETURN_IF_ERROR(src->get_u8(&flags));
    apply_flags(flags, out);
    RETURN_IF_ERROR(read_stats(src, tier, out));
    RETURN_IF_ERROR(read_locator(src, tier, out));

    // The body must consume exactly entry_len bytes; otherwise the structure is
    // inconsistent with the tier.
    const size_t consumed = src->position() - body_start;
    if (consumed != static_cast<size_t>(total)) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "dict_entry: body length does not match entry_len");
    }
    return Status::OK();
}

Status skip_dict_entry(ByteSource* src) {
    if (src == nullptr)
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("dict_entry: src is null");
    uint64_t total = 0;
    RETURN_IF_ERROR(read_entry_len(src, &total));
    Slice unused;
    return src->get_bytes(static_cast<size_t>(total), &unused);
}

} // namespace doris::snii::format
