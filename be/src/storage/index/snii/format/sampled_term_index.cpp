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

#include "storage/index/snii/format/sampled_term_index.h"

#include <algorithm>

#include "storage/index/snii/encoding/byte_source.h"
#include "storage/index/snii/encoding/section_framer.h"

namespace doris::snii::format {

namespace {

// Longest common prefix length of term and prev (front coding primitive, consistent with dict_entry).
uint32_t common_prefix_len(std::string_view term, std::string_view prev) {
    uint32_t n = 0;
    const uint32_t lim = static_cast<uint32_t>(std::min(term.size(), prev.size()));
    while (n < lim && term[n] == prev[n]) ++n;
    return n;
}

// Write a front-coded term key (prefix_len + suffix_len + suffix).
void write_term_key(std::string_view term, std::string_view prev, ByteSink* sink) {
    const uint32_t prefix = common_prefix_len(term, prev);
    const std::string_view suffix = term.substr(prefix);
    sink->put_varint32(prefix);
    sink->put_varint32(static_cast<uint32_t>(suffix.size()));
    sink->put_bytes(Slice(suffix));
}

// Read a front-coded term key and reconstruct it into out from prev + suffix.
Status read_term_key(ByteSource* src, std::string_view prev, std::string* out) {
    uint32_t prefix = 0;
    uint32_t suffix_len = 0;
    RETURN_IF_ERROR(src->get_varint32(&prefix));
    RETURN_IF_ERROR(src->get_varint32(&suffix_len));
    if (prefix > prev.size()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "sampled_term_index: prefix_len exceeds prev_term length");
    }
    Slice suffix;
    RETURN_IF_ERROR(src->get_bytes(suffix_len, &suffix));
    out->assign(prev.substr(0, prefix));
    out->append(reinterpret_cast<const char*>(suffix.data()), suffix.size());
    return Status::OK();
}

} // namespace

void SampledTermIndexBuilder::add_block_first_term(std::string_view first_term) {
    first_terms_.emplace_back(first_term);
}

void SampledTermIndexBuilder::finish(ByteSink* sink) {
    ByteSink payload;
    payload.put_varint32(static_cast<uint32_t>(first_terms_.size()));
    // min_term / max_term are written only when non-empty (== first/last sample_term).
    if (!first_terms_.empty()) {
        write_term_key(first_terms_.front(), std::string_view {}, &payload);
        write_term_key(first_terms_.back(), std::string_view {}, &payload);
        std::string_view prev {};
        for (const auto& t : first_terms_) {
            write_term_key(t, prev, &payload);
            prev = t;
        }
    }
    SectionFramer::write(*sink, static_cast<uint8_t>(SectionType::kSampledTermIndex),
                         payload.view());
}

namespace {

// Parse n_blocks, min/max (not used directly; consumed for checksum alignment), and all sample_terms from payload.
Status parse_payload(Slice payload, std::vector<std::string>* terms) {
    ByteSource src(payload);
    uint32_t n_blocks = 0;
    RETURN_IF_ERROR(src.get_varint32(&n_blocks));
    if (n_blocks == 0) {
        if (!src.eof()) {
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "sampled_term_index: empty index contains trailing bytes");
        }
        terms->clear();
        return Status::OK();
    }

    // min_term / max_term (do not drive binary search directly; must be consumed to verify structural alignment).
    std::string min_term;
    std::string max_term;
    RETURN_IF_ERROR(read_term_key(&src, std::string_view {}, &min_term));
    RETURN_IF_ERROR(read_term_key(&src, std::string_view {}, &max_term));

    std::vector<std::string> out;
    out.reserve(n_blocks);
    std::string prev;
    for (uint32_t i = 0; i < n_blocks; ++i) {
        std::string term;
        RETURN_IF_ERROR(read_term_key(&src, prev, &term));
        prev = term;
        out.push_back(std::move(term));
    }
    if (!src.eof()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "sampled_term_index: payload contains trailing bytes");
    }
    if (out.front() != min_term || out.back() != max_term) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "sampled_term_index: min/max inconsistent with sample_terms");
    }
    *terms = std::move(out);
    return Status::OK();
}

} // namespace

Status SampledTermIndexReader::open(Slice section, SampledTermIndexReader* out) {
    if (out == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("sampled_term_index: out is null");
    }
    ByteSource src(section);
    FramedSection sec;
    RETURN_IF_ERROR(SectionFramer::read(src, &sec));
    if (sec.type != static_cast<uint8_t>(SectionType::kSampledTermIndex)) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "sampled_term_index: not a kSampledTermIndex section");
    }
    *out = SampledTermIndexReader {};
    return parse_payload(sec.payload, &out->sample_terms_);
}

size_t SampledTermIndexReader::heap_bytes() const {
    size_t bytes = sample_terms_.capacity() * sizeof(std::string);
    for (const auto& term : sample_terms_) {
        bytes += std_string_heap_bytes(term);
    }
    return bytes;
}

Status SampledTermIndexReader::locate(std::string_view target, bool* maybe_present,
                                      uint32_t* block_ordinal) const {
    if (maybe_present == nullptr || block_ordinal == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "sampled_term_index: output pointer is null");
    }
    *maybe_present = false;
    *block_ordinal = 0;
    if (sample_terms_.empty()) {
        return Status::OK(); // empty index: always out of range.
    }
    // target < min_term (first block's first term) -> before the first block, so it
    // cannot exist in any block. NOTE: a target GREATER than the last sample term is
    // NOT out of range -- sample_terms_ holds each block's FIRST term, so the LAST
    // block can contain terms greater than its first term. Such a target routes to
    // the last block (upper_bound -> end()), where find_term confirms presence.
    if (target < std::string_view(sample_terms_.front())) {
        return Status::OK();
    }
    // Last sample_term <= target: step back one position after upper_bound. For a
    // target past every sample term, upper_bound returns end() and idx = n-1 (the
    // last block), which is correct.
    auto it = std::upper_bound(
            sample_terms_.begin(), sample_terms_.end(), target,
            [](std::string_view t, const std::string& s) { return t < std::string_view(s); });
    const auto idx = (it - sample_terms_.begin()) - 1; // it > begin (< min excluded).
    *maybe_present = true;
    *block_ordinal = static_cast<uint32_t>(idx);
    return Status::OK();
}

} // namespace doris::snii::format
