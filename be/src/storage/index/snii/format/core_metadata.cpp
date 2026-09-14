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
