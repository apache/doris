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

#pragma once

#include <cstdint>

// SNII container and per-section on-disk contract constants.
// Once published, these values are format semantics; changes require bumping
// format_version and maintaining a compatibility policy. All multi-byte
// fixed-width fields are little-endian; variable-length integers use LEB128
// (see snii/encoding/varint.h).
namespace doris::snii::format {

// ---- Container-level magic / version ----
// "SNII" reads as 0x49494E53 in little-endian.
inline constexpr uint32_t kContainerMagic = 0x49494E53u; // 'S''N''I''I'
inline constexpr uint32_t kTailMagic = 0x4C494154u;      // 'T''A''I''L'
inline constexpr uint16_t kFormatVersion = 2;
inline constexpr uint16_t kMinReaderVersion = 2;
// Self-describing version of the meta layout (the per-index meta header AND the
// tail meta region share this single constant; a reader fails fast with
// Corruption on any mismatch). This is a from-scratch, pre-launch format: there
// is exactly ONE meta layout, so the value is 1. Bump it only AFTER launch,
// when a real on-disk change must coexist with already-written indexes --
// pre-launch changes just fold into v1.
inline constexpr uint16_t kMetaFormatVersion = 1;

// ---- SectionFramer section type ids (within per-index meta / tail region)
// ----
enum class SectionType : uint8_t {
    kStatsBlock = 1,
    kSampledTermIndex = 2,
    kDictBlockDirectory = 3,
    kXFilter = 4, // reserved: legacy embedded XFilter; meta no longer emits/reads it
    kSectionRefs = 5,
    kPerIndexMetaHeader = 6,
    kLogicalIndexDirectory = 7,
    kTailMetaHeader = 8,
    kFeatureBits = 9,
    // OPTIONAL per-index meta section (G01): payload = varint64
    // bigram_prune_min_df, the effective phrase-bigram df-prune threshold the
    // writer applied to THIS index. Emitted only when pruning was active (> 0);
    // absent == 0 == legacy semantics (every adjacent pair materialized, so a
    // bigram dict miss means "no adjacency"). Readers that predate this section
    // skip it (unknown optional type), keeping old binaries readable on new
    // segments and new binaries on old segments.
    kBigramPruneInfo = 10,
    // G13: zstd-compressed carriers for the two large embedded meta sub-sections
    // (they are highly compressible sorted string/offset tables and dominate the
    // per-segment meta blob fetched serially at open). Payload = varint64
    // uncomp_len followed by zstd(original full frame), where "original full
    // frame" is the byte-exact kSampledTermIndex / kDictBlockDirectory frame
    // (type+len+payload+crc32c) the raw layout would have embedded. Decompression
    // therefore reproduces the legacy frame verbatim and the sub-module readers
    // (which re-verify the inner crc) stay unchanged. The writer emits these ONLY
    // when the raw frame reaches kMetaSectionCompressMinBytes AND compression
    // shrinks it; otherwise the raw frame is emitted as before, so small/legacy
    // segments remain byte-identical and always readable.
    kSampledTermIndexZstd = 11,
    kDictBlockDirectoryZstd = 12,
};

// ---- Logical index postings storage content configuration (fixed per logical
// index, not per-term) ---- Determines whether to write freq / positions /
// norms+stats.
enum class IndexConfig : uint8_t {
    kDocsOnly = 0,             // docid only: term/match filtering
    kDocsPositions = 1,        // docid+freq+positions: MATCH_PHRASE
    kDocsPositionsScoring = 2, // + norms + stats: phrase + BM25
    kPositionsOffsets = 3,     // reserved (highlight/RAG), not implemented in this release
};

// term stats / postings capability tiers: only tier>=kT2 writes
// ttf_delta/max_freq and .prx.
enum class IndexTier : uint8_t {
    kT1 = 1, // docs-only
    kT2 = 2, // docs-positions
    kT3 = 3, // docs-positions-scoring
};

inline constexpr IndexTier tier_of(IndexConfig cfg) {
    return cfg == IndexConfig::kDocsOnly        ? IndexTier::kT1
           : cfg == IndexConfig::kDocsPositions ? IndexTier::kT2
                                                : IndexTier::kT3; // scoring / offsets
}
inline constexpr bool has_positions(IndexConfig cfg) {
    return cfg != IndexConfig::kDocsOnly;
}
inline constexpr bool has_scoring(IndexConfig cfg) {
    return cfg == IndexConfig::kDocsPositionsScoring;
}

// ---- DictEntry flags bit definitions ----
namespace dict_flags {
inline constexpr uint8_t kKind = 1u << 0;        // 0=pod_ref / 1=inline
inline constexpr uint8_t kEnc = 1u << 1;         // 0=slim / 1=windowed
inline constexpr uint8_t kHasSb = 1u << 2;       // posting prelude includes sub-block directory
inline constexpr uint8_t kHasChampion = 1u << 3; // v1 always 0
inline constexpr uint8_t kOffsetsRef = 1u << 4;  // v1 always 0
// bit5-7 reserved
} // namespace dict_flags

enum class DictEntryKind : uint8_t { kPodRef = 0, kInline = 1 };
enum class DictEntryEnc : uint8_t { kSlim = 0, kWindowed = 1 };

// ---- .prx window codec (codec byte bit0-5) ----
// kRaw  : plaintext varint payload (doc_count, per-doc pos_count + position
// deltas). kZstd : zstd-compressed plaintext payload (legacy reader still
// supported). kPfor : doc_count + per-doc pos_count (varint), then position
// deltas bit-packed
//         as PFOR runs (kFrqBaseUnit each). No entropy coding -> far cheaper
//         build CPU than zstd while staying competitive on size for ascending
//         deltas.
enum class PrxCodec : uint8_t {
    kRaw = 0,
    kZstd = 1,
    kPfor = 2 /* bit7 cont-reserved */
};

// ---- Build-time parameters (not format semantics; may be tuned against real
// metrics) ----
inline constexpr uint32_t kFrqBaseUnit = 256;            // window base unit
inline constexpr uint32_t kSlimDfThreshold = 512;        // df < this → slim
inline constexpr uint32_t kDefaultInlineThreshold = 256; // slim encoded bytes ≤ this → inline
// Adaptive window sizing (design #4): high-df windowed terms use larger windows
// to cut prelude rows + per-window header/crc overhead. Windows remain a whole
// multiple of kFrqBaseUnit so .prx alignment and win_base/last_docid semantics
// are preserved. A term whose df >= kAdaptiveWindowDfThreshold splits into
// kAdaptiveWindowDocs-sized windows instead of kFrqBaseUnit-sized ones.
inline constexpr uint32_t kAdaptiveWindowDfThreshold = 8192; // df >= this -> larger windows
inline constexpr uint32_t kAdaptiveWindowDocs = 1024;        // larger window size (4 * base unit)
inline constexpr uint32_t kDefaultTargetDictBlockBytes = 64 * 1024;
// G13: embedded meta sub-sections (SampledTermIndex / DictBlockDirectory frames)
// at or above this raw size are emitted zstd-compressed (kSampledTermIndexZstd /
// kDictBlockDirectoryZstd); smaller ones stay raw -- compression overhead is not
// worth it below a few KB and the raw layout keeps small segments byte-identical
// to the pre-G13 format. A build-time parameter, not format semantics: readers
// accept both layouts regardless of the value.
inline constexpr size_t kMetaSectionCompressMinBytes = 4 * 1024;

} // namespace doris::snii::format
