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
#include <string>
#include <vector>

#include "snii/common/slice.h"
#include "common/status.h"
#include "snii/encoding/byte_sink.h"
#include "snii/format/format_constants.h"
#include "snii/format/stats_block.h"

// PerIndexMeta -- the per-logical-index metadata block that enters the searcher
// cache. It COMPOSES already-built sub-sections (StatsBlock, SampledTermIndex,
// DICT block directory, optional XFilter) plus the physical SectionRefs into a
// single contiguous block. See design spec "Per-index meta block".
//
// On-disk layout:
//   PerIndexMetaHeader (fixed prefix, self-checksummed):
//     u16      meta_format_version (== kMetaFormatVersion), little-endian
//     varint64 index_id
//     varint32 suffix_len
//     u8[]     suffix_bytes
//     u32      flags (fixed32, little-endian)   # feature bits, e.g. kHasBsbf
//     u32      crc32c (fixed32) over all preceding header bytes
//   then framed sub-sections (each via SectionFramer, type+len+payload+crc32c):
//     StatsBlock            (kStatsBlock,        built here)
//     SampledTermIndex      (kSampledTermIndex,  embedded already-framed bytes)
//     DICT block directory  (kDictBlockDirectory,embedded already-framed bytes)
//     SectionRefs           (kSectionRefs,       built here; carries the bsbf ref)
//     (+ any extra raw framed sections appended by add_raw_section)
//
// Design choice: the SampledTermIndex / DICT block directory / XFilter
// sub-sections are EMBEDDED as their producers' already-framed output (the raw
// SectionFramer frame), not re-framed. This lets the reader hand the exact frame
// Slice straight back to each sub-module's open() (which expects a full frame),
// and reuses the framer instead of re-implementing sub-section parsing.
namespace snii::format {

// Physical reference to a contiguous region within the container. (0, 0) means
// the region is absent (e.g. no norms POD for a non-scoring index). A present-
// but-empty region (e.g. an all-INLINE index's posting_region) is (off, 0).
struct RegionRef {
    uint64_t offset = 0;
    uint64_t length = 0;
};

// Physical references to the data sections / side PODs of one logical index.
// Each RegionRef is encoded as varint64 offset followed by varint64 length, in
// the field order below.
//
// posting_region is the single interleaved [prx][frq] posting region (it replaced
// the former two separate frq_pod + prx_pod refs). Each pod_ref term writes its
// prx span first then its frq span, contiguously, in term order; both
// frq_off_delta and prx_off_delta now index into this one region. NO positions
// capability is inferred from posting_region.length -- it is non-zero for any
// docs-only index with a pod_ref term, and zero for an all-INLINE positional
// index; capability lives in the header kHasPositions flag instead.
struct SectionRefs {
    RegionRef dict_region;
    RegionRef posting_region; // interleaved [prx][frq] per term; was frq_pod + prx_pod
    RegionRef norms;
    RegionRef null_bitmap;
    // Block-split bloom XFilter section ([28B header][bitset]); {0,0} when absent.
    // A PHYSICAL section (not embedded in the resident meta) so a single 32-byte block
    // can be probed on demand without loading the whole filter at open.
    RegionRef bsbf;
};

// Builds a per-index meta block by composing already-built sub-sections.
class PerIndexMetaBuilder {
public:
    // Header flags / feature bits.
    static constexpr uint32_t kHasPositions = 1u << 0; // index is positions-capable (tier>=T2)
    static constexpr uint32_t kHasBsbf = 1u << 1;      // block-split bloom XFilter (section ref)

    PerIndexMetaBuilder(uint64_t index_id, std::string index_suffix, uint32_t flags);

    void set_stats(const StatsBlock& stats);

    // Raw output of SampledTermIndexBuilder::finish (a full kSampledTermIndex frame).
    void set_sampled_term_index(Slice framed_bytes);

    // Raw output of DictBlockDirectoryBuilder::finish (a full kDictBlockDirectory frame).
    void set_dict_block_directory(Slice framed_bytes);

    void set_section_refs(const SectionRefs& refs);

    // Appends an arbitrary already-framed section verbatim. Used for forward-compat
    // optional sections; the reader skips unrecognized types.
    void add_raw_section(Slice framed_bytes);

    // Serializes the header and all sub-sections into sink.
    // sink == nullptr -> kInvalidArgument.
    doris::Status finish(ByteSink* sink) const;

private:
    uint64_t index_id_;
    std::string index_suffix_;
    uint32_t flags_;
    StatsBlock stats_;
    std::vector<uint8_t> sampled_term_index_;
    std::vector<uint8_t> dict_block_directory_;
    SectionRefs section_refs_;
    std::vector<std::vector<uint8_t>> extra_sections_;
};

// Parses a per-index meta block: verifies the header crc, then walks the framed
// sub-sections (each crc-verified by the framer), capturing the full frame Slice
// of each known sub-section so callers can re-open it with the sub-module reader.
// Unrecognized optional section types are skipped.
class PerIndexMetaReader {
public:
    PerIndexMetaReader() = default;

    // block == the full per-index meta block bytes; out must be non-null.
    // Header crc mismatch / truncation / a sub-section crc mismatch -> kCorruption;
    // missing a required sub-section -> kCorruption; out == nullptr -> kInvalidArgument.
    static doris::Status open(Slice block, PerIndexMetaReader* out);

    uint64_t index_id() const { return index_id_; }
    const std::string& index_suffix() const { return index_suffix_; }
    uint32_t flags() const { return flags_; }

    const StatsBlock& stats() const { return stats_; }
    const SectionRefs& section_refs() const { return section_refs_; }

    // Full kSampledTermIndex frame Slice, ready for SampledTermIndexReader::open.
    Slice sampled_term_index_bytes() const { return sampled_term_index_; }
    // Full kDictBlockDirectory frame Slice, ready for DictBlockDirectoryReader::open.
    Slice dict_block_directory_bytes() const { return dict_block_directory_; }

    // Block-split bloom XFilter: present iff a non-empty bsbf section ref exists.
    bool has_bsbf() const { return section_refs_.bsbf.length > 0; }

    // Positions capability, read from the persisted header flag (NOT from any region
    // length). True iff the index was built as docs-positions(+scoring) (tier>=T2).
    bool has_positions() const { return (flags_ & PerIndexMetaBuilder::kHasPositions) != 0; }

private:
    uint64_t index_id_ = 0;
    std::string index_suffix_;
    uint32_t flags_ = 0;
    StatsBlock stats_;
    SectionRefs section_refs_;
    Slice sampled_term_index_;
    Slice dict_block_directory_;
};

} // namespace snii::format
