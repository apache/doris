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
#include <cstdint>
#include <memory>
#include <span>
#include <utility>
#include <vector>

#include "common/status.h"

namespace doris::snii::compaction {

using RowIdConversionMap = std::vector<std::vector<std::pair<uint32_t, uint32_t>>>;

// Capability proving that a complete row-id conversion has passed the global
// shape, bounds, monotonicity and destination-coverage validation. Construction
// is restricted to create(), so merge plans cannot accidentally accept an
// unvalidated conversion. The conversion map is borrowed and must outlive this
// token and every merge plan prepared from it.
class ValidatedRowIdConversion {
public:
    ValidatedRowIdConversion(const ValidatedRowIdConversion&) = delete;
    ValidatedRowIdConversion& operator=(const ValidatedRowIdConversion&) = delete;

    static Status create(const RowIdConversionMap* conversion,
                         std::span<const uint32_t> source_segment_doc_counts,
                         std::span<const uint32_t> destination_segment_doc_counts,
                         std::unique_ptr<ValidatedRowIdConversion>* out);

    size_t source_segment_count() const { return source_segment_doc_counts_.size(); }
    const std::vector<uint32_t>& source_segment_doc_counts() const {
        return source_segment_doc_counts_;
    }
    const std::vector<uint32_t>& destination_segment_doc_counts() const {
        return destination_segment_doc_counts_;
    }
    bool source_has_deletions(size_t source_ordinal) const;
    std::span<const std::pair<uint32_t, uint32_t>> source_mapping(size_t source_ordinal) const;

private:
    ValidatedRowIdConversion(const RowIdConversionMap* conversion,
                             std::vector<uint32_t> source_segment_doc_counts,
                             std::vector<uint32_t> destination_segment_doc_counts);

    const RowIdConversionMap* conversion_ = nullptr;
    std::vector<uint32_t> source_segment_doc_counts_;
    std::vector<uint32_t> destination_segment_doc_counts_;
    std::vector<uint8_t> source_has_deletions_;
};

// Validates the complete row-id conversion before an SNII index fast merge
// writes any destination bytes. Each inner conversion vector belongs to one
// source segment and is indexed by source docid. A deleted source doc must be
// represented by exactly (UINT32_MAX, UINT32_MAX); every other pair is a live
// (destination segment, destination docid).
//
// In addition to shape and bounds, the function proves that:
//   * each source stream is strictly increasing in global destination order;
//   * all source streams together contain every destination doc exactly once.
//
// Completeness is checked by a k-way merge of the monotonic source streams.
// Memory is O(source segments + destination segments), never O(output rows).
Status validate_rowid_conversion(const RowIdConversionMap& conversion,
                                 const std::vector<uint32_t>& source_segment_doc_counts,
                                 const std::vector<uint32_t>& destination_segment_doc_counts);

} // namespace doris::snii::compaction
