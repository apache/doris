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
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "storage/index/query/spi/index_source.h"
#include "storage/index/snii/format/frq_prelude.h"
#include "storage/index/snii/format/norms_pod.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_postings_cursor.h"

namespace doris::snii::reader {

// A logical SNII index as the engine's source: terms resolved through the dictionary, a batch
// ahead when prepared or opened together, and postings read by SniiPostingsCursor. Cursors
// opened together share one read wave: the preludes and slim postings they prepare arrive in
// one round when one of them first needs its bytes, so a caller giving up on a missing term
// reads none, and the reads they register afterwards make one round per fetch_pending. A term
// or an expansion that reaches the dictionary's internal phrase-bigram namespace bypasses the
// index, as the format requires. Given `prx_stats`, its cursors add the work of the PRX frames
// they decode there.
class SniiIndexSource final : public index_query::IndexSource {
public:
    explicit SniiIndexSource(const LogicalIndexReader& idx,
                             format::PrxDecodeStats* prx_stats = nullptr);

    uint32_t doc_count() const override;
    bool batches_reads() const override { return true; }
    Status prepare_terms(std::span<const std::string> terms) override;
    Status open_term(std::string_view term, bool positions, bool scoring,
                     std::unique_ptr<index_query::PostingsCursor>* out) override;
    Status open_terms(std::span<const std::string> terms, bool positions, bool scoring,
                      std::vector<std::unique_ptr<index_query::PostingsCursor>>* out) override;
    Status fetch_pending() override { return _wave.fetch(); }
    Status may_hold(std::string_view term, bool* held) override;
    Status doc_freq(std::string_view term, uint64_t* out) override;
    Status encoded_norms(std::span<const uint32_t> docs, std::vector<uint32_t>* out) override;
    Status expand_terms(index_query::TermPattern& pattern, int32_t max_expansions,
                        std::vector<std::string>* out) override;

    // The dictionary's answer for `term`, kept with the terms resolved before it.
    Status lookup(std::string_view term, const LogicalIndexReader::BatchLookupResult** out);

    const LogicalIndexReader& index() const { return _idx; }
    // The rounds the shared wave fetched so far.
    size_t wave_rounds() const { return _wave.rounds(); }
    // The bytes the shared wave's rounds still hold, and the most they held at once.
    uint64_t held_bytes() const { return _wave.held_bytes(); }
    uint64_t peak_held_bytes() const { return _wave.peak_held_bytes(); }

private:
    // A term the dictionary answered, and its prelude while a cursor holds it.
    struct Term {
        LogicalIndexReader::BatchLookupResult hit;
        std::weak_ptr<const format::FrqPreludeReader> prelude;
    };

    Status _resolve(std::string_view term, Term** out);
    // Bypasses an expansion whose enumeration can reach an internal term.
    Status _check_enumeration(std::string_view prefix);
    Status _open_norms(const format::NormsPodReader** out);
    Status _cursor(Term& term, bool positions, bool scoring, SniiReadWave* wave,
                   std::unique_ptr<SniiPostingsCursor>* out);

    const LogicalIndexReader& _idx;
    format::PrxDecodeStats* _prx_stats;
    SniiReadWave _wave;
    std::unordered_map<std::string, Term> _terms;
    format::NormsPodReader _norms;
    bool _norms_opened = false;
    // Whether the dictionary holds internal terms, probed once by the first expansion that
    // enumerates from its beginning.
    std::optional<bool> _has_internal_terms;
};

} // namespace doris::snii::reader
