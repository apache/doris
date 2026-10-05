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

#include "storage/index/snii/compaction/rowid_conversion.h"

#include <fmt/format.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <queue>
#include <utility>
#include <vector>

#include "common/check.h"

namespace doris::snii::compaction {
namespace {

constexpr uint32_t kDeleted = std::numeric_limits<uint32_t>::max();

struct HeapEntry {
    uint64_t destination_ordinal = 0;
    size_t source_ordinal = 0;
    size_t source_docid = 0;
};

struct HeapEntryGreater {
    bool operator()(const HeapEntry& lhs, const HeapEntry& rhs) const {
        if (lhs.destination_ordinal != rhs.destination_ordinal) {
            return lhs.destination_ordinal > rhs.destination_ordinal;
        }
        return lhs.source_ordinal > rhs.source_ordinal;
    }
};

uint64_t destination_ordinal(const std::pair<uint32_t, uint32_t>& destination,
                             const std::vector<uint64_t>& destination_segment_prefixes) {
    return destination_segment_prefixes[destination.first] + destination.second;
}

} // namespace

Status validate_rowid_conversion(const RowIdConversionMap& conversion,
                                 const std::vector<uint32_t>& source_segment_doc_counts,
                                 const std::vector<uint32_t>& destination_segment_doc_counts) {
    if (conversion.size() != source_segment_doc_counts.size()) {
        return Status::InvalidArgument(
                fmt::format("SNII rowid conversion source segment count mismatch: conversion={}, "
                            "doc_counts={}",
                            conversion.size(), source_segment_doc_counts.size()));
    }
    if (destination_segment_doc_counts.size() >
        static_cast<size_t>(std::numeric_limits<uint32_t>::max())) {
        return Status::InvalidArgument(fmt::format(
                "SNII rowid conversion destination segment count {} exceeds uint32 encoding",
                destination_segment_doc_counts.size()));
    }

    std::vector<uint64_t> destination_segment_prefixes;
    destination_segment_prefixes.reserve(destination_segment_doc_counts.size() + 1);
    destination_segment_prefixes.push_back(0);
    uint64_t destination_doc_count = 0;
    for (size_t destination_segment = 0;
         destination_segment < destination_segment_doc_counts.size(); ++destination_segment) {
        const uint64_t segment_doc_count = destination_segment_doc_counts[destination_segment];
        if (segment_doc_count > std::numeric_limits<uint64_t>::max() - destination_doc_count) {
            return Status::InvalidArgument(fmt::format(
                    "SNII rowid conversion destination prefix sum overflows uint64 at segment {}",
                    destination_segment));
        }
        destination_doc_count += segment_doc_count;
        destination_segment_prefixes.push_back(destination_doc_count);
    }

    for (size_t source = 0; source < conversion.size(); ++source) {
        const auto& source_conversion = conversion[source];
        if (source_conversion.size() != source_segment_doc_counts[source]) {
            return Status::InvalidArgument(fmt::format(
                    "SNII rowid conversion source doc count mismatch at source {}: "
                    "conversion={}, doc_count={}",
                    source, source_conversion.size(), source_segment_doc_counts[source]));
        }

        bool has_previous = false;
        uint64_t previous_ordinal = 0;
        for (size_t source_docid = 0; source_docid < source_conversion.size(); ++source_docid) {
            const auto& destination = source_conversion[source_docid];
            const bool segment_deleted = destination.first == kDeleted;
            const bool row_deleted = destination.second == kDeleted;
            if (segment_deleted != row_deleted) {
                return Status::InvalidArgument(fmt::format(
                        "SNII rowid conversion entry is partially deleted at source {} doc {}: "
                        "destination=({}, {})",
                        source, source_docid, destination.first, destination.second));
            }
            if (segment_deleted) {
                continue;
            }
            if (destination.first >= destination_segment_doc_counts.size()) {
                return Status::InvalidArgument(fmt::format(
                        "SNII rowid conversion destination segment {} is out of range at "
                        "source {} doc {} (segment_count={})",
                        destination.first, source, source_docid,
                        destination_segment_doc_counts.size()));
            }
            if (destination.second >= destination_segment_doc_counts[destination.first]) {
                return Status::InvalidArgument(fmt::format(
                        "SNII rowid conversion destination row {} is out of range at source {} "
                        "doc {} (destination segment {} has {} docs)",
                        destination.second, source, source_docid, destination.first,
                        destination_segment_doc_counts[destination.first]));
            }

            const uint64_t ordinal = destination_ordinal(destination, destination_segment_prefixes);
            if (has_previous && ordinal <= previous_ordinal) {
                return Status::InvalidArgument(fmt::format(
                        "SNII rowid conversion source {} is not strictly increasing at doc {}: "
                        "destination ordinal {} follows {}",
                        source, source_docid, ordinal, previous_ordinal));
            }
            previous_ordinal = ordinal;
            has_previous = true;
        }
    }

    std::priority_queue<HeapEntry, std::vector<HeapEntry>, HeapEntryGreater> heap;
    auto push_next_live = [&](size_t source, size_t source_docid) {
        const auto& source_conversion = conversion[source];
        while (source_docid < source_conversion.size() &&
               source_conversion[source_docid].first == kDeleted) {
            ++source_docid;
        }
        if (source_docid < source_conversion.size()) {
            heap.push({destination_ordinal(source_conversion[source_docid],
                                           destination_segment_prefixes),
                       source, source_docid});
        }
    };

    for (size_t source = 0; source < conversion.size(); ++source) {
        push_next_live(source, 0);
    }

    uint64_t expected_ordinal = 0;
    while (!heap.empty()) {
        const HeapEntry entry = heap.top();
        heap.pop();
        if (entry.destination_ordinal < expected_ordinal) {
            return Status::InvalidArgument(fmt::format(
                    "SNII rowid conversion has duplicate destination ordinal {} at source {} "
                    "doc {}",
                    entry.destination_ordinal, entry.source_ordinal, entry.source_docid));
        }
        if (entry.destination_ordinal > expected_ordinal) {
            return Status::InvalidArgument(fmt::format(
                    "SNII rowid conversion is missing destination ordinal {} before ordinal {} "
                    "at source {} doc {}",
                    expected_ordinal, entry.destination_ordinal, entry.source_ordinal,
                    entry.source_docid));
        }
        ++expected_ordinal;
        push_next_live(entry.source_ordinal, entry.source_docid + 1);
    }

    if (expected_ordinal != destination_doc_count) {
        return Status::InvalidArgument(fmt::format(
                "SNII rowid conversion is missing destination ordinal {}: covered {} of {} "
                "destination docs",
                expected_ordinal, expected_ordinal, destination_doc_count));
    }
    return Status::OK();
}

ValidatedRowIdConversion::ValidatedRowIdConversion(
        const RowIdConversionMap* conversion, std::vector<uint32_t> source_segment_doc_counts,
        std::vector<uint32_t> destination_segment_doc_counts)
        : conversion_(conversion),
          source_segment_doc_counts_(std::move(source_segment_doc_counts)),
          destination_segment_doc_counts_(std::move(destination_segment_doc_counts)) {
    DORIS_CHECK(conversion_ != nullptr);
    source_has_deletions_.reserve(conversion_->size());
    for (const auto& source : *conversion_) {
        const bool has_deletions =
                std::ranges::any_of(source, [](const std::pair<uint32_t, uint32_t>& mapping) {
                    return mapping.first == kDeleted;
                });
        source_has_deletions_.push_back(static_cast<uint8_t>(has_deletions));
    }
}

Status ValidatedRowIdConversion::create(const RowIdConversionMap* conversion,
                                        std::span<const uint32_t> source_segment_doc_counts,
                                        std::span<const uint32_t> destination_segment_doc_counts,
                                        std::unique_ptr<ValidatedRowIdConversion>* out) {
    if (out == nullptr) {
        return Status::InvalidArgument("SNII validated rowid conversion has null out parameter");
    }
    out->reset();
    if (conversion == nullptr) {
        return Status::InvalidArgument("SNII validated rowid conversion has null conversion");
    }

    std::vector<uint32_t> source_counts(source_segment_doc_counts.begin(),
                                        source_segment_doc_counts.end());
    std::vector<uint32_t> destination_counts(destination_segment_doc_counts.begin(),
                                             destination_segment_doc_counts.end());
    RETURN_IF_ERROR(validate_rowid_conversion(*conversion, source_counts, destination_counts));
    out->reset(new ValidatedRowIdConversion(conversion, std::move(source_counts),
                                            std::move(destination_counts)));
    return Status::OK();
}

std::span<const std::pair<uint32_t, uint32_t>> ValidatedRowIdConversion::source_mapping(
        size_t source_ordinal) const {
    DCHECK_LT(source_ordinal, conversion_->size());
    return std::span<const std::pair<uint32_t, uint32_t>>((*conversion_)[source_ordinal]);
}

bool ValidatedRowIdConversion::source_has_deletions(size_t source_ordinal) const {
    DCHECK_LT(source_ordinal, source_has_deletions_.size());
    return source_has_deletions_[source_ordinal] != 0;
}

} // namespace doris::snii::compaction
