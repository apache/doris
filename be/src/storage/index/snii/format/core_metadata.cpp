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
    out->set_stop_df_permille(scheme.stop_df_permille);
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
            .stop_df_permille = input.stop_df_permille(),
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
                "density_permille={}, stop_df_permille={}, hash_version={}): {}",
                input.mode(), scheme.min_len, scheme.max_len, scheme.density_permille,
                scheme.stop_df_permille, scheme.hash_version, validated.to_string());
    }
    *out = scheme;
    return Status::OK();
}

} // namespace
} // namespace core_metadata_detail

namespace {

using segment_v2::inverted_index::CommonGramsCoverage;
using segment_v2::inverted_index::CommonGramsSegmentMetadata;
using segment_v2::inverted_index::PlainTermKeyVersion;
using segment_v2::inverted_index::ScoringCoverage;
using segment_v2::inverted_index::validate_common_grams_segment_metadata;
using segment_v2::inverted_index::validate_snii_scoring_metadata;

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
    case static_cast<uint32_t>(IndexConfig::kDocsPositionsScoring):
        *out = static_cast<IndexConfig>(value);
        return Status::OK();
    default:
        return unsupported("core metadata: unsupported index config");
    }
}

Status validate_posting_policy(uint32_t value, CommonGramsPostingPolicy* out) {
    switch (value) {
    case 0:
        *out = CommonGramsPostingPolicy::kNone;
        return Status::OK();
    case 1:
        *out = CommonGramsPostingPolicy::kHybridV1;
        return Status::OK();
    default:
        return unsupported("core metadata: unsupported CommonGrams posting policy");
    }
}

Status validate_plain_term_key_version(uint32_t value) {
    switch (value) {
    case static_cast<uint32_t>(PlainTermKeyVersion::kLegacyRaw):
    case static_cast<uint32_t>(PlainTermKeyVersion::kEscapedV1):
    case static_cast<uint32_t>(PlainTermKeyVersion::kRawNoInternal):
        return Status::OK();
    default:
        return unsupported("core metadata: unsupported plain-term key version");
    }
}

Status validate_common_grams_coverage(uint32_t value) {
    switch (value) {
    case static_cast<uint32_t>(CommonGramsCoverage::kNone):
    case static_cast<uint32_t>(CommonGramsCoverage::kComplete):
    case static_cast<uint32_t>(CommonGramsCoverage::kMixed):
        return Status::OK();
    default:
        return unsupported("core metadata: unsupported CommonGrams coverage");
    }
}

Status validate_scoring_coverage(uint32_t value) {
    switch (value) {
    case static_cast<uint32_t>(ScoringCoverage::kNone):
    case static_cast<uint32_t>(ScoringCoverage::kComplete):
        return Status::OK();
    default:
        return unsupported("core metadata: unsupported scoring coverage");
    }
}

void encode_region_ref(const RegionRef& ref, doris::snii::SniiRegionRefPB* out) {
    out->set_offset(ref.offset);
    out->set_length(ref.length);
}

void encode_common_grams(const CommonGramsSegmentMetadata& metadata,
                         doris::snii::SniiCommonGramsMetadataPB* out) {
    out->set_plain_term_key_version(static_cast<uint32_t>(metadata.plain_term_key_version));
    out->set_common_grams_coverage(static_cast<uint32_t>(metadata.common_grams_coverage));
    out->set_common_grams_semantics_version(metadata.common_grams_semantics_version);
    out->set_common_grams_key_version(metadata.common_grams_key_version);
    out->set_common_grams_dictionary_identity(metadata.common_grams_dictionary_identity);
    out->set_base_analyzer_fingerprint(metadata.base_analyzer_fingerprint);
    out->set_common_grams_fingerprint(metadata.common_grams_fingerprint);
    out->set_scoring_coverage(static_cast<uint32_t>(metadata.scoring_coverage));
    out->set_scoring_stats_version(metadata.scoring_stats_version);
    out->set_norm_semantics_version(metadata.norm_semantics_version);
    out->set_scoring_doc_count(metadata.scoring_doc_count);
    out->set_scoring_token_count(metadata.scoring_token_count);
}

Status decode_region_ref(const doris::snii::SniiRegionRefPB& input, RegionRef* out) {
    if (!input.has_offset() || !input.has_length()) {
        return corrupted("core metadata: missing region reference field");
    }
    *out = {.offset = input.offset(), .length = input.length()};
    return Status::OK();
}

Status decode_common_grams(const doris::snii::SniiCommonGramsMetadataPB& input,
                           CommonGramsSegmentMetadata* out) {
    if (!input.has_plain_term_key_version() || !input.has_common_grams_coverage() ||
        !input.has_common_grams_semantics_version() || !input.has_common_grams_key_version() ||
        !input.has_common_grams_dictionary_identity() || !input.has_base_analyzer_fingerprint() ||
        !input.has_common_grams_fingerprint() || !input.has_scoring_coverage() ||
        !input.has_scoring_stats_version() || !input.has_norm_semantics_version() ||
        !input.has_scoring_doc_count() || !input.has_scoring_token_count()) {
        return corrupted("core metadata: missing CommonGrams metadata field");
    }
    RETURN_IF_ERROR(validate_plain_term_key_version(input.plain_term_key_version()));
    RETURN_IF_ERROR(validate_common_grams_coverage(input.common_grams_coverage()));
    RETURN_IF_ERROR(validate_scoring_coverage(input.scoring_coverage()));
    *out = {.plain_term_key_version =
                    static_cast<PlainTermKeyVersion>(input.plain_term_key_version()),
            .common_grams_coverage =
                    static_cast<CommonGramsCoverage>(input.common_grams_coverage()),
            .common_grams_semantics_version = input.common_grams_semantics_version(),
            .common_grams_key_version = input.common_grams_key_version(),
            .common_grams_dictionary_identity = input.common_grams_dictionary_identity(),
            .base_analyzer_fingerprint = input.base_analyzer_fingerprint(),
            .common_grams_fingerprint = input.common_grams_fingerprint(),
            .scoring_coverage = static_cast<ScoringCoverage>(input.scoring_coverage()),
            .scoring_stats_version = input.scoring_stats_version(),
            .norm_semantics_version = input.norm_semantics_version(),
            .scoring_doc_count = input.scoring_doc_count(),
            .scoring_token_count = input.scoring_token_count()};
    return validate_common_grams_segment_metadata(*out);
}

Status decode_core_pb(const doris::snii::SniiCoreMetadataPB& input, CoreMetadata* out) {
    if (!input.has_index_config() || !input.has_stats() || !input.has_section_refs()) {
        return corrupted("core metadata: missing required field");
    }
    RETURN_IF_ERROR(validate_index_config(input.index_config(), &out->index_config));

    const auto& stats = input.stats();
    if (!stats.has_doc_count() || !stats.has_indexed_doc_count() || !stats.has_term_count() ||
        !stats.has_sum_total_term_freq() || !stats.has_null_count()) {
        return corrupted("core metadata: missing statistics field");
    }
    out->stats = {.doc_count = stats.doc_count(),
                  .indexed_doc_count = stats.indexed_doc_count(),
                  .term_count = stats.term_count(),
                  .sum_total_term_freq = stats.sum_total_term_freq(),
                  .null_count = stats.null_count()};

    const auto& refs = input.section_refs();
    if (!refs.has_dict_region() || !refs.has_posting_region() || !refs.has_norms() ||
        !refs.has_null_bitmap() || !refs.has_bsbf()) {
        return corrupted("core metadata: missing section reference");
    }
    RETURN_IF_ERROR(decode_region_ref(refs.dict_region(), &out->section_refs.dict_region));
    RETURN_IF_ERROR(decode_region_ref(refs.posting_region(), &out->section_refs.posting_region));
    RETURN_IF_ERROR(decode_region_ref(refs.norms(), &out->section_refs.norms));
    RETURN_IF_ERROR(decode_region_ref(refs.null_bitmap(), &out->section_refs.null_bitmap));
    RETURN_IF_ERROR(decode_region_ref(refs.bsbf(), &out->section_refs.bsbf));

    if (input.has_common_grams()) {
        CommonGramsSegmentMetadata common_grams;
        RETURN_IF_ERROR(decode_common_grams(input.common_grams(), &common_grams));
        out->common_grams_metadata = std::move(common_grams);
    }

    if (input.has_gram_scheme()) {
        segment_v2::gram::GramScheme gram_scheme;
        RETURN_IF_ERROR(
                core_metadata_detail::decode_gram_scheme(input.gram_scheme(), &gram_scheme));
        out->gram_scheme = gram_scheme;
    }

    RETURN_IF_ERROR(validate_posting_policy(input.common_grams_posting_policy(),
                                            &out->common_grams_posting_policy));
    if (out->common_grams_posting_policy == CommonGramsPostingPolicy::kHybridV1 &&
        (!out->common_grams_metadata.has_value() ||
         out->common_grams_metadata->common_grams_coverage != CommonGramsCoverage::kMixed)) {
        return corrupted("core metadata: hybrid policy requires mixed CommonGrams metadata");
    }
    const bool has_scoring_tier = out->index_config == IndexConfig::kDocsPositionsScoring;
    if (has_scoring_tier) {
        if (out->section_refs.norms.length == 0) {
            return corrupted("core metadata: scoring index requires a norms region");
        }
    }
    if (has_scoring_tier ||
        (out->common_grams_metadata.has_value() &&
         out->common_grams_metadata->scoring_coverage == ScoringCoverage::kComplete)) {
        RETURN_IF_ERROR(validate_snii_scoring_metadata(
                out->common_grams_metadata ? &*out->common_grams_metadata : nullptr,
                out->stats.doc_count, out->stats.sum_total_term_freq, has_scoring_tier,
                has_positions(out->index_config), out->section_refs.norms.length != 0));
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
    encode_region_ref(metadata.section_refs.norms, refs->mutable_norms());
    encode_region_ref(metadata.section_refs.null_bitmap, refs->mutable_null_bitmap());
    encode_region_ref(metadata.section_refs.bsbf, refs->mutable_bsbf());
    if (metadata.common_grams_metadata.has_value()) {
        encode_common_grams(*metadata.common_grams_metadata, core.mutable_common_grams());
    }
    if (metadata.gram_scheme.has_value()) {
        core_metadata_detail::encode_gram_scheme(*metadata.gram_scheme, core.mutable_gram_scheme());
    }
    if (metadata.common_grams_posting_policy != CommonGramsPostingPolicy::kNone) {
        core.set_common_grams_posting_policy(
                static_cast<uint32_t>(metadata.common_grams_posting_policy));
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
