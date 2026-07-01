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
#include <string>
#include <string_view>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_sink.h"
#include "storage/index/snii/format/format_constants.h"

// SampledTermIndex -- resident metadata for locating a query term to a candidate DICT block.
//
// Sampling granularity is per DICT block (not a fixed term count): each time the writer produces a DICT block,
// it writes the block's first_term into this index. Size grows proportionally to block count. At read time it is
// loaded into the searcher cache together with SniiLogicalIndexReader. See design spec "Sampled Term Index".
//
// On-disk layout (framed by SectionFramer, uniform type+len+crc32c):
//   [u8 type=kSampledTermIndex][varint64 payload_len][payload][fixed32 crc32c]
//   payload =
//     n_blocks       varint32
//     min_term        len(varint32) + bytes        # == sample_terms[0], omitted when n_blocks=0
//     max_term        len(varint32) + bytes        # == sample_terms[n-1], omitted when n_blocks=0
//     sample_terms[n_blocks]:                       # first_term of each block, in ascending order
//       prefix_len   varint32                       # shared prefix length with the previous sample_term
//       suffix_len   varint32
//       suffix       u8[suffix_len]
//
// Term bytes are compared as unsigned byte order (UTF-8 friendly, binary-safe). Front coding reuses
// the same prefix/suffix primitives as DictEntry; do not reimplement.
namespace doris::snii::format {

// SSO-aware heap-byte accounting for a std::string. libstdc++ keeps up to 15
// chars inline (SSO), so only capacity() > 15 implies a separate heap buffer of
// capacity()+1 bytes (the +1 is the NUL terminator); an SSO string owns no heap
// and contributes 0. Shared by the resident format readers' heap_bytes() charge
// helpers, which back LogicalIndexReader::memory_usage() (the searcher-cache
// charge). NOTE: the threshold 15 is libstdc++-specific; a different standard
// library needs a different SSO bound here.
inline size_t std_string_heap_bytes(const std::string& s) {
    return s.capacity() > 15 ? s.capacity() + 1 : 0;
}

// Builder: appends the first_term of each DICT block in block ordinal order (must be strictly ascending),
// and serializes the entire set into a single kSampledTermIndex framed section on finish.
class SampledTermIndexBuilder {
public:
    // Appends the first_term of the next DICT block. Call order determines block ordinal order.
    void add_block_first_term(std::string_view first_term);

    // Serializes and appends to sink. An empty collection (no blocks) is valid; n_blocks=0.
    void finish(ByteSink* sink);

private:
    std::vector<std::string> first_terms_;
};

// Reader: verifies the checksum and materializes all sample_terms on open; subsequent locate calls are pure in-memory binary search.
class SampledTermIndexReader {
public:
    SampledTermIndexReader() = default;

    // Parses a kSampledTermIndex framed section.
    // CRC mismatch / truncation / field overrun → kCorruption; type != kSampledTermIndex → kInvalidArgument.
    static Status open(Slice section, SampledTermIndexReader* out);

    // Binary-search locate: returns the block ordinal of the last sample_term <= target.
    //   target < min_term or target > max_term (including empty index) → *maybe_present=false (out of range, term is definitely absent).
    //   Otherwise *maybe_present=true and *block_ordinal is the ordinal of the matching block.
    Status locate(std::string_view target, bool* maybe_present, uint32_t* block_ordinal) const;

    uint32_t n_blocks() const { return static_cast<uint32_t>(sample_terms_.size()); }

    // Resident heap held beyond sizeof(*this): the sample_terms_ vector buffer
    // plus each non-SSO term's heap allocation. Summed into
    // LogicalIndexReader::memory_usage() so the searcher-cache charge reflects the
    // decoded sampled index (previously omitted -> under-charge -> over-commit).
    size_t heap_bytes() const;

private:
    std::vector<std::string> sample_terms_;
};

} // namespace doris::snii::format
