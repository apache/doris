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

#include "storage/index/snii/format/core_metadata.h"

#include <limits>
#include <string>
#include <string_view>
#include <utility>

#include "gen_cpp/snii.pb.h"
#include "storage/index/snii/encoding/byte_source.h"
#include "storage/index/snii/encoding/section_framer.h"

namespace doris::snii::format {

// Storage is a unity build (several .cpp files merged into one unity_N_cxx.cxx TU): other .cpp
// files in this directory may define file-level helpers with the same names, so the gram_scheme
// codec helpers go into this file-private namespace (with an inner anonymous namespace to keep
// internal linkage) instead of sharing the anonymous namespace further below, which keeps
// same-named symbols from different files from colliding in the unity TU (Ruling R8).
namespace core_metadata_detail {
namespace {

void encode_gram_scheme(const segment_v2::gram::GramScheme& scheme,
                        doris::snii::SniiGramSchemePB* out) {
    out->set_mode(static_cast<uint32_t>(scheme.mode));
    out->set_min_len(scheme.min_len);
    out->set_max_len(scheme.max_len);
    out->set_density_permille(scheme.density_permille);
    out->set_lower_case(scheme.lower_case);
    out->set_hash_version(scheme.hash_version);
}

Status decode_gram_scheme(const doris::snii::SniiGramSchemePB& input,
                          segment_v2::gram::GramScheme* out) {
    if (input.mode() != 1 && input.mode() != 2) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "core metadata: unsupported gram scheme mode {}", input.mode());
    }
    const segment_v2::gram::GramScheme scheme {
            .mode = static_cast<segment_v2::gram::GramMode>(input.mode()),
            .min_len = input.min_len(),
            .max_len = input.max_len(),
            .density_permille = input.density_permille(),
            .lower_case = input.lower_case(),
            .hash_version = input.hash_version()};
    // The valid range of each field is written down in exactly one place,
    // GramScheme::from_properties (the single source of truth), so it is reused here through a
    // "property round trip": a persisted scheme must round-trip back to the very same scheme, or
    // the file counts as corrupted. Without this step a truncated (or tampered) PB would carry
    // values such as min_len=0 all the way into GramExtractor -- every unset field of a partial
    // message is 0, and 0 is not part of any valid scheme.
    segment_v2::gram::GramScheme round_tripped;
    const Status validated =
            segment_v2::gram::GramScheme::from_properties(scheme.to_properties(), &round_tripped);
    if (!validated.ok() || !(round_tripped == scheme)) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "core metadata: invalid gram scheme (mode={}, min_len={}, max_len={}, "
                "density_permille={}, hash_version={}): {}",
                input.mode(), scheme.min_len, scheme.max_len, scheme.density_permille,
                scheme.hash_version, validated.to_string());
    }
    *out = scheme;
    return Status::OK();
}

} // namespace
} // namespace core_metadata_detail

namespace {

Status corrupted(std::string_view message) {
    return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(message);
}

Status unsupported(std::string_view message) {
    return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED, false>(message);
}

Status validate_index_config(uint32_t value, IndexConfig* out) {
    switch (value) {
    case static_cast<uint32_t>(IndexConfig::kDocsOnly):
    case static_cast<uint32_t>(IndexConfig::kDocsPositions):
        *out = static_cast<IndexConfig>(value);
        return Status::OK();
    default:
        return unsupported("core metadata: unsupported index config");
    }
}

void encode_region_ref(const RegionRef& ref, doris::snii::SniiRegionRefPB* out) {
    out->set_offset(ref.offset);
    out->set_length(ref.length);
}

Status decode_region_ref(const doris::snii::SniiRegionRefPB& input, RegionRef* out) {
    if (!input.has_offset() || !input.has_length()) {
        return corrupted("core metadata: missing region reference field");
    }
    *out = {.offset = input.offset(), .length = input.length()};
    return Status::OK();
}

Status decode_core_pb(const doris::snii::SniiCoreMetadataPB& input, CoreMetadata* out) {
    if (!input.has_index_config() || !input.has_stats() || !input.has_section_refs()) {
        return corrupted("core metadata: missing required field");
    }
    RETURN_IF_ERROR(validate_index_config(input.index_config(), &out->index_config));

    const auto& stats = input.stats();
    if (!stats.has_doc_count() || !stats.has_indexed_doc_count() || !stats.has_term_count() ||
        !stats.has_null_count()) {
        return corrupted("core metadata: missing statistics field");
    }
    // sum_total_term_freq (stats field 5) and norms (section_refs field 5) are optional additions
    // absent from the deployed 3.1-series writer. Missing fields mean no scoring statistics or
    // norms, affecting BM25 availability but not filtering queries.
    out->stats = {.doc_count = stats.doc_count(),
                  .indexed_doc_count = stats.indexed_doc_count(),
                  .term_count = stats.term_count(),
                  .sum_total_term_freq =
                          stats.has_sum_total_term_freq() ? stats.sum_total_term_freq() : 0,
                  .null_count = stats.null_count()};

    const auto& refs = input.section_refs();
    if (!refs.has_dict_region() || !refs.has_posting_region() || !refs.has_null_bitmap() ||
        !refs.has_bsbf()) {
        return corrupted("core metadata: missing section reference");
    }
    RETURN_IF_ERROR(decode_region_ref(refs.dict_region(), &out->section_refs.dict_region));
    RETURN_IF_ERROR(decode_region_ref(refs.posting_region(), &out->section_refs.posting_region));
    if (refs.has_norms()) {
        RETURN_IF_ERROR(decode_region_ref(refs.norms(), &out->section_refs.norms));
    } else {
        out->section_refs.norms = {};
    }
    RETURN_IF_ERROR(decode_region_ref(refs.null_bitmap(), &out->section_refs.null_bitmap));
    RETURN_IF_ERROR(decode_region_ref(refs.bsbf(), &out->section_refs.bsbf));

    // Tombstones for the removed CommonGrams feature. Fields 4/5 identify segments written with
    // a CommonGrams analyzer (gram terms, escaped keys, or mixed posting policies). Their term
    // keys and query semantics are no longer supported, so these indexes must be rebuilt.
    // Production writers never emitted these fields, so upgrades are unaffected.
    if (input.has_legacy_common_grams() || input.has_legacy_common_grams_posting_policy()) {
        return unsupported(
                "core metadata: segment was written with CommonGrams, which is no longer "
                "supported; rebuild the index");
    }

    if (input.has_gram_scheme()) {
        segment_v2::gram::GramScheme gram_scheme;
        RETURN_IF_ERROR(
                core_metadata_detail::decode_gram_scheme(input.gram_scheme(), &gram_scheme));
        out->gram_scheme = gram_scheme;
    }

    if (input.has_high_df_terms()) {
        const auto& digest = input.high_df_terms();
        if (digest.term_hash_size() != digest.df_size()) {
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "core metadata: high-df digest has {} hashes and {} frequencies",
                    digest.term_hash_size(), digest.df_size());
        }
        out->high_df_terms.term_hash.assign(digest.term_hash().begin(), digest.term_hash().end());
        out->high_df_terms.df.assign(digest.df().begin(), digest.df().end());
        out->high_df_terms.df_ceiling = digest.df_ceiling();
        // The lookup is a binary search, so a digest that is not ascending would silently
        // return wrong bounds rather than fail. Reject it instead: a wrong upper bound can
        // make the gate give up on a query the index would have answered quickly.
        if (!std::ranges::is_sorted(out->high_df_terms.term_hash)) {
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "core metadata: high-df digest hashes are not ascending");
        }
    }

    // Norms encode BM25 document lengths in one byte and require positions for term frequencies.
    if (out->section_refs.norms.length != 0 && !has_positions(out->index_config)) {
        return corrupted("core metadata: norms require positions");
    }
    return Status::OK();
}

} // namespace

Status encode_core_metadata(const CoreMetadata& metadata, ByteSink* out) {
    if (out == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("core metadata: null output");
    }

    doris::snii::SniiCoreMetadataPB core;
    core.set_index_config(static_cast<uint32_t>(metadata.index_config));
    auto* stats = core.mutable_stats();
    stats->set_doc_count(metadata.stats.doc_count);
    stats->set_indexed_doc_count(metadata.stats.indexed_doc_count);
    stats->set_term_count(metadata.stats.term_count);
    stats->set_sum_total_term_freq(metadata.stats.sum_total_term_freq);
    stats->set_null_count(metadata.stats.null_count);
    auto* refs = core.mutable_section_refs();
    encode_region_ref(metadata.section_refs.dict_region, refs->mutable_dict_region());
    encode_region_ref(metadata.section_refs.posting_region, refs->mutable_posting_region());
    // Omit field 5 when norms are absent, matching production bytes without affecting old readers.
    if (metadata.section_refs.norms.length != 0) {
        encode_region_ref(metadata.section_refs.norms, refs->mutable_norms());
    }
    encode_region_ref(metadata.section_refs.null_bitmap, refs->mutable_null_bitmap());
    encode_region_ref(metadata.section_refs.bsbf, refs->mutable_bsbf());
    if (metadata.gram_scheme.has_value()) {
        core_metadata_detail::encode_gram_scheme(*metadata.gram_scheme, core.mutable_gram_scheme());
    }
    // A ceiling with no entries is still worth writing: it says every term in this index is
    // below it, which is the strongest bound the digest can offer. Absent BOTH means no
    // digest was built at all (not a gram index, or a segment too small to have one), and
    // the field stays off the wire so those segments are byte-identical to before.
    if (!metadata.high_df_terms.empty() || metadata.high_df_terms.df_ceiling > 0) {
        auto* digest = core.mutable_high_df_terms();
        digest->mutable_term_hash()->Assign(metadata.high_df_terms.term_hash.begin(),
                                            metadata.high_df_terms.term_hash.end());
        digest->mutable_df()->Assign(metadata.high_df_terms.df.begin(),
                                     metadata.high_df_terms.df.end());
        digest->set_df_ceiling(metadata.high_df_terms.df_ceiling);
    }

    CoreMetadata validated;
    RETURN_IF_ERROR(decode_core_pb(core, &validated));
    const size_t size = core.ByteSizeLong();
    if (size > static_cast<size_t>(std::numeric_limits<int>::max())) {
        return corrupted("core metadata: protobuf payload exceeds INT_MAX");
    }
    std::string payload(size, '\0');
    if (!core.SerializeToArray(payload.data(), static_cast<int>(size))) {
        return corrupted("core metadata: protobuf serialization failed");
    }
    SectionFramer::write(*out, static_cast<uint8_t>(SectionType::kCoreMetadataPB), Slice(payload));
    return Status::OK();
}

Status decode_core_metadata(Slice framed_bytes, CoreMetadata* out) {
    if (out == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("core metadata: null output");
    }
    *out = {};
    ByteSource source(framed_bytes);
    FramedSection section;
    RETURN_IF_ERROR(SectionFramer::read(source, &section));
    if (!source.eof() || section.type != static_cast<uint8_t>(SectionType::kCoreMetadataPB)) {
        return corrupted("core metadata: invalid frame");
    }
    if (section.payload.size() > static_cast<size_t>(std::numeric_limits<int>::max())) {
        return corrupted("core metadata: protobuf payload exceeds INT_MAX");
    }
    doris::snii::SniiCoreMetadataPB core;
    if (!core.ParseFromArray(section.payload.data(), static_cast<int>(section.payload.size()))) {
        return corrupted("core metadata: protobuf parsing failed");
    }
    return decode_core_pb(core, out);
}

} // namespace doris::snii::format
