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

#include "storage/index/snii/format/per_index_meta.h"

#include <cstdlib>

#include "storage/index/snii/encoding/byte_source.h"
#include "storage/index/snii/encoding/crc32c.h"
#include "storage/index/snii/encoding/section_framer.h"
#include "storage/index/snii/encoding/zstd_codec.h"

namespace doris::snii::format {

namespace {

// Upper bound on index_suffix length read from untrusted bytes, capped before
// allocation to avoid a DoS-inflated reserve. A logical index suffix is a short
// column/field name; 64 KiB is far beyond any legitimate value.
constexpr uint32_t kMaxSuffixLen = 64u * 1024u;

// G13 zstd-compressed embedded sub-sections. Level matches the other SNII zstd
// producers (dict blocks / .prx windows); these sorted tables compress several
// fold at level 3 already.
constexpr int kMetaSectionZstdLevel = 3;
// Upper bound on a declared uncomp_len read from untrusted bytes, capped before
// the decompress allocation (same bound as dict blocks). A meta sub-section is a
// per-segment sampled-term/offset table -- far below this in practice.
constexpr uint64_t kMaxMetaSectionUncompBytes = 256ULL * 1024 * 1024;

// Raw-frame size at/above which finish() emits the zstd carrier. The env
// SNII_META_COMPRESS_MIN overrides the default for tuning and for building
// uncompressed control segments in tests without a huge corpus. Read per finish.
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

// Emits one embedded sub-section: the raw already-framed bytes verbatim (legacy
// layout), or -- when the raw frame reaches the compress threshold AND zstd
// actually shrinks it -- a `zstd_type` carrier frame whose payload is
// varint64 uncomp_len + zstd(raw frame). An empty `framed` (absent section)
// emits nothing, exactly as before. zstd failure falls back to the raw layout:
// compression is an encoding optimization, never a correctness dependency.
void emit_embedded_section(const std::vector<uint8_t>& framed, SectionType zstd_type,
                           ByteSink* sink) {
    if (framed.empty()) {
        return;
    }
    if (framed.size() >= meta_compress_min_bytes()) {
        std::vector<uint8_t> compressed;
        if (zstd_compress(Slice(framed), kMetaSectionZstdLevel, &compressed).ok()) {
            ByteSink payload;
            payload.put_varint64(framed.size());
            payload.put_bytes(Slice(compressed));
            // ~9B frame overhead (type+len+crc); require a real win over raw.
            if (payload.size() + 16 < framed.size()) {
                SectionFramer::write(*sink, static_cast<uint8_t>(zstd_type), payload.view());
                return;
            }
        }
    }
    sink->put_bytes(Slice(framed));
}

// Materializes an embedded sub-section frame captured by open(): a raw frame is
// returned as-is (view into the block; *scratch untouched); a zstd carrier frame
// is unwrapped and decompressed into *scratch. The decompressed bytes are the
// byte-exact original frame, whose own crc32c the sub-module reader re-verifies.
Status materialize_embedded_frame(Slice frame, bool is_zstd, const char* what,
                                  std::vector<uint8_t>* scratch, Slice* out) {
    if (scratch == nullptr || out == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("per_index_meta: null frame out");
    }
    if (!is_zstd) {
        *out = frame;
        return Status::OK();
    }
    // Re-read the carrier frame (crc already verified during open's walk).
    ByteSource src(frame);
    FramedSection sec;
    RETURN_IF_ERROR(SectionFramer::read(src, &sec));
    ByteSource ps(sec.payload);
    uint64_t uncomp_len = 0;
    RETURN_IF_ERROR(ps.get_varint64(&uncomp_len));
    if (uncomp_len == 0 || uncomp_len > kMaxMetaSectionUncompBytes) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "per_index_meta: {} zstd uncomp_len out of range", what);
    }
    Slice comp;
    RETURN_IF_ERROR(ps.get_bytes(ps.remaining(), &comp));
    RETURN_IF_ERROR(zstd_decompress(comp, static_cast<size_t>(uncomp_len), scratch));
    *out = Slice(*scratch);
    return Status::OK();
}

void encode_region(const RegionRef& r, ByteSink* payload) {
    payload->put_varint64(r.offset);
    payload->put_varint64(r.length);
}

Status decode_region(ByteSource* ps, RegionRef* r) {
    RETURN_IF_ERROR(ps->get_varint64(&r->offset));
    RETURN_IF_ERROR(ps->get_varint64(&r->length));
    return Status::OK();
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

Status decode_section_refs(Slice payload, SectionRefs* out) {
    ByteSource ps(payload);
    RETURN_IF_ERROR(decode_region(&ps, &out->dict_region));
    RETURN_IF_ERROR(decode_region(&ps, &out->posting_region));
    RETURN_IF_ERROR(decode_region(&ps, &out->norms));
    RETURN_IF_ERROR(decode_region(&ps, &out->null_bitmap));
    RETURN_IF_ERROR(decode_region(&ps, &out->bsbf));
    if (!ps.eof()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "per_index_meta: trailing bytes in section_refs");
    }
    return Status::OK();
}

// kBigramPruneInfo payload: varint64 effective bigram_prune_min_df, then
// (G15) varint64 effective bigram_prune_max_df -- both written whenever the
// section is emitted (either gate active), 0 marking the inactive gate. The
// section is OPTIONAL (emitted only when the writer pruned) and
// forward-extensible: pre-G15 writers emitted only the min varint (decode
// defaults max to 0), and trailing payload bytes past the known fields are
// IGNORED so a future writer can append more without a new section type.
void encode_bigram_prune_info(uint64_t min_df, uint64_t max_df, ByteSink* sink) {
    ByteSink payload;
    payload.put_varint64(min_df);
    payload.put_varint64(max_df);
    SectionFramer::write(*sink, static_cast<uint8_t>(SectionType::kBigramPruneInfo),
                         payload.view());
}

Status decode_bigram_prune_info(Slice payload, uint64_t* min_df, uint64_t* max_df) {
    ByteSource ps(payload);
    RETURN_IF_ERROR(ps.get_varint64(min_df));
    *max_df = 0;
    if (!ps.eof()) {
        RETURN_IF_ERROR(ps.get_varint64(max_df));
    }
    return Status::OK();
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
Status decode_header(Slice block, ByteSource* src, uint64_t* index_id, std::string* suffix,
                     uint32_t* flags) {
    size_t start = src->position();
    uint16_t version = 0;
    RETURN_IF_ERROR(src->get_fixed16(&version));
    if (version != kMetaFormatVersion) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "per_index_meta: unsupported meta_format_version");
    }
    RETURN_IF_ERROR(src->get_varint64(index_id));
    uint32_t suffix_len = 0;
    RETURN_IF_ERROR(src->get_varint32(&suffix_len));
    if (suffix_len > kMaxSuffixLen || suffix_len > src->remaining()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "per_index_meta: suffix_len exceeds bounds");
    }
    Slice suffix_view;
    RETURN_IF_ERROR(src->get_bytes(suffix_len, &suffix_view));
    RETURN_IF_ERROR(src->get_fixed32(flags));
    size_t covered = src->position() - start;
    uint32_t stored = 0;
    RETURN_IF_ERROR(src->get_fixed32(&stored));
    if (crc32c(block.subslice(start, covered)) != stored) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "per_index_meta: header crc mismatch");
    }
    suffix->assign(reinterpret_cast<const char*>(suffix_view.data()), suffix_view.size());
    return Status::OK();
}

// Reads one framed section, returning both its type and the FULL frame Slice
// (type+len+payload+crc) so it can be re-opened by a sub-module reader. The
// framer itself crc-verifies the frame.
Status read_frame(Slice block, ByteSource* src, uint8_t* type, Slice* frame) {
    size_t start = src->position();
    FramedSection sec;
    RETURN_IF_ERROR(SectionFramer::read(*src, &sec));
    *type = sec.type;
    *frame = block.subslice(start, src->position() - start);
    return Status::OK();
}

// Routes an embedded sub-section frame (raw or its G13 zstd carrier) to its
// slot, remembering which layout was captured. Unknown section types are
// intentionally ignored (forward compatibility: skip unknown optional sections).
void dispatch_frame(uint8_t type, Slice frame, Slice* sampled, bool* sampled_zstd, Slice* dict,
                    bool* dict_zstd) {
    if (type == static_cast<uint8_t>(SectionType::kSampledTermIndex)) {
        *sampled = frame;
        *sampled_zstd = false;
    } else if (type == static_cast<uint8_t>(SectionType::kSampledTermIndexZstd)) {
        *sampled = frame;
        *sampled_zstd = true;
    } else if (type == static_cast<uint8_t>(SectionType::kDictBlockDirectory)) {
        *dict = frame;
        *dict_zstd = false;
    } else if (type == static_cast<uint8_t>(SectionType::kDictBlockDirectoryZstd)) {
        *dict = frame;
        *dict_zstd = true;
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

Status PerIndexMetaBuilder::finish(ByteSink* sink) const {
    if (sink == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("per_index_meta: null sink");
    }
    encode_header(index_id_, index_suffix_, flags_, sink);
    encode_stats_block(stats_, sink);
    emit_embedded_section(sampled_term_index_, SectionType::kSampledTermIndexZstd, sink);
    emit_embedded_section(dict_block_directory_, SectionType::kDictBlockDirectoryZstd, sink);
    encode_section_refs(section_refs_, sink);
    if (bigram_prune_min_df_ != 0 || bigram_prune_max_df_ != 0) {
        encode_bigram_prune_info(bigram_prune_min_df_, bigram_prune_max_df_, sink);
    }
    for (const auto& extra : extra_sections_) {
        sink->put_bytes(Slice(extra));
    }
    return Status::OK();
}

Status PerIndexMetaReader::open(Slice block, PerIndexMetaReader* out) {
    if (out == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("per_index_meta: null reader");
    }
    ByteSource src(block);
    RETURN_IF_ERROR(decode_header(block, &src, &out->index_id_, &out->index_suffix_, &out->flags_));
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
        } else if (type == static_cast<uint8_t>(SectionType::kBigramPruneInfo)) {
            FramedSection sec;
            ByteSource fs(frame);
            RETURN_IF_ERROR(SectionFramer::read(fs, &sec));
            RETURN_IF_ERROR(decode_bigram_prune_info(sec.payload, &out->bigram_prune_min_df_,
                                                     &out->bigram_prune_max_df_));
        } else {
            dispatch_frame(type, frame, &out->sampled_term_index_, &out->sampled_term_index_zstd_,
                           &out->dict_block_directory_, &out->dict_block_directory_zstd_);
        }
    }
    if (!have_stats || !have_refs) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "per_index_meta: missing required sub-section");
    }
    return Status::OK();
}

Status PerIndexMetaReader::sampled_term_index_frame(std::vector<uint8_t>* scratch,
                                                    Slice* frame) const {
    return materialize_embedded_frame(sampled_term_index_, sampled_term_index_zstd_,
                                      "sampled_term_index", scratch, frame);
}

Status PerIndexMetaReader::dict_block_directory_frame(std::vector<uint8_t>* scratch,
                                                      Slice* frame) const {
    return materialize_embedded_frame(dict_block_directory_, dict_block_directory_zstd_,
                                      "dict_block_directory", scratch, frame);
}

} // namespace doris::snii::format
