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

#include <cstddef>
#include <functional>
#include <optional>
#include <span>

#include "common/status.h"
#include "storage/index/inverted/analysis/analyzer/analyzer_provider.h"
#include "storage/index/inverted/analysis/inverted_index_parser.h"
#include "storage/index/inverted/snii/format/core_metadata.h"

namespace doris {

class TabletIndex;

namespace snii::reader {
class LogicalIndexReader;
}

namespace snii::compaction {

// One opened SNII source and its index metadata. Both objects must outlive validation.
struct PlainT2CompactionSource {
    std::reference_wrapper<const reader::LogicalIndexReader> reader;
    std::reference_wrapper<const TabletIndex> index_meta;
};

// Injectable only at analyzer-provider construction. Production callers omit
// it and use InvertedIndexAnalyzer::create_analyzer_provider; focused tests can
// supply an immutable provider without mutating the process IndexPolicyMgr.
using AnalyzerProviderFactory = std::function<segment_v2::inverted_index::AnalyzerProviderPtr(
        const InvertedIndexAnalyzerConfig&)>;

struct SniiCompactionEligibility {
    // When the phrase property is absent, preserve the source posting shape.
    bool destination_has_positions = true;
    // A2: An analyzed destination index with positions must include norms. The posting merge
    // rebuilds them from source term frequencies (sum per document, clamped to 1..255), so older
    // T2 segments without norms can gain norms without reanalysis.
    bool destination_writes_norms = false;
};

// O(1) physical/semantic validation for one source. The merge planner may call
// this while preparing opened sources; the aggregate validator below reuses the
// exact same predicate.
Status validate_plain_t2_source(const reader::LogicalIndexReader& source, size_t source_ordinal);

// Gate-only validation. In addition to the O(1) physical shape checks above,
// performs one bounded DICT prefix seek for the legacy hidden-bigram namespace
// so the owner can select raw rebuild before creating streamed output.
Status validate_plain_t2_source_eligibility(const reader::LogicalIndexReader& source,
                                            size_t source_ordinal);

// Revalidates one source against the selected destination posting shape.
Status validate_snii_source_eligibility(const reader::LogicalIndexReader& source,
                                        size_t source_ordinal,
                                        const SniiCompactionEligibility& eligibility);

// O(source-count), O(1)-metadata validation for the SNII postings-merge fast
// path. OK means every source is a plain positions-only T2 index, all source
// properties are byte-for-byte equal to the current destination properties,
// source/destination index ids and escaped suffixes match, and the current
// destination analyzer policy will not build CommonGrams.
// Rejections use INVERTED_INDEX_NOT_SUPPORTED with a concrete reason so the
// caller can select raw-column rebuild before creating output.
//
// Each source also receives one bounded seek for the legacy hidden-bigram
// marker namespace. This avoids discovering an unsupported legacy segment only
// after the raw-column compaction path has already been skipped.
Status validate_plain_t2_compaction_eligibility(
        std::span<const PlainT2CompactionSource> sources, const TabletIndex& destination_index,
        const AnalyzerProviderFactory& analyzer_provider_factory = {});

// Accepts homogeneous T1 or T2 sources when their posting shape matches the destination.
// Mixed shapes fall back to rebuilding from raw columns.
Status validate_snii_compaction_eligibility(
        std::span<const PlainT2CompactionSource> sources, const TabletIndex& destination_index,
        SniiCompactionEligibility* out,
        const AnalyzerProviderFactory& analyzer_provider_factory = {});

} // namespace snii::compaction
} // namespace doris
