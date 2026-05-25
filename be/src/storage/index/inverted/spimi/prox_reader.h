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
#include <vector>

namespace doris::segment_v2::inverted_index::spimi {

// Reader for the `.prx` positions stream produced by
// `FreqProxEncoder::AddPosition`. The wire layout is the simplest
// of all SPIMI streams:
//
//   for each (term, doc) in (term, doc) order:
//     for each position p_0 < p_1 < ... < p_{freq-1} in doc:
//       VInt(p_i - p_{i-1})       // delta-encoded, reset per doc
//
// There is no header, no per-doc separator, and no per-term
// separator: the reader must know which range of bytes belongs to
// the term (from `.tis` `prox_pointer`) and exactly how many
// positions to decode per doc (from `.frq` `freq` value of that
// doc).
//
// API mirrors the layout: ReadPositions takes the byte slice and
// the per-doc frequency list, decodes the positions for each doc
// in order, and returns them grouped by doc.
class SpimiProxReader {
public:
    // Decodes positions for one term starting at `prx_data[0]`.
    // Caller is responsible for slicing `prx_data` to begin at the
    // term's `prox_pointer` from `.tis`. `freqs_per_doc[i]` is the
    // freq of the i-th doc that contains the term; the reader
    // consumes exactly `sum(freqs_per_doc)` VInts.
    //
    // Returns a vector of position arrays, one per doc, in the same
    // order as `freqs_per_doc`. `result[i].size() == freqs_per_doc[i]`.
    static std::vector<std::vector<int32_t>> ReadPositions(
            const uint8_t* prx_data, size_t prx_length, const std::vector<int32_t>& freqs_per_doc);

    // Convenience overload for tests / callers holding the whole .prx
    // as a vector. The buffer must already start at the term's
    // `prox_pointer` (typically the only term in a unit-test fixture
    // where prox_pointer = 0).
    static std::vector<std::vector<int32_t>> ReadPositions(
            const std::vector<uint8_t>& prx_bytes, const std::vector<int32_t>& freqs_per_doc) {
        return ReadPositions(prx_bytes.data(), prx_bytes.size(), freqs_per_doc);
    }
};

} // namespace doris::segment_v2::inverted_index::spimi
