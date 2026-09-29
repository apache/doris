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
#include <string>
#include <string_view>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/format/dict_entry.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/writer/memory_reporter.h"

// Scans one source dictionary in lexical order, keeping one DICT block resident at a time. A hidden phrase-bigram term returns INVERTED_INDEX_NOT_SUPPORTED so compaction can rebuild that column.
namespace doris::snii::compaction {

uint64_t big_endian_term_prefix(std::string_view term);

class SniiSegmentTermCursor {
public:
    // `index` is borrowed and must outlive the cursor. `source_ordinal` is the
    // caller's stable id for this source segment (frontier tie-break + docid
    // remapping key downstream).
    SniiSegmentTermCursor(const reader::LogicalIndexReader* index, uint32_t source_ordinal,
                          writer::MemoryReporter* memory_reporter = nullptr)
            : index_(index),
              source_ordinal_(source_ordinal),
              memory_reporter_(memory_reporter),
              previous_entries_reservation_(memory_reporter == nullptr
                                                    ? writer::MemoryReporter::Reservation()
                                                    : memory_reporter->make_reservation()),
              entries_reservation_(memory_reporter == nullptr
                                           ? writer::MemoryReporter::Reservation()
                                           : memory_reporter->make_reservation()) {}

    // Advances to the next dictionary term. *has_term=false once the
    // dictionary is exhausted (an empty index yields it on the first call).
    // Errors:
    //   INVERTED_INDEX_NOT_SUPPORTED -- hidden bigram/sentinel term (see above);
    //   Corruption -- the dictionary violated strict lexicographic order.
    // After an error the cursor is poisoned and keeps returning the error.
    Status next(bool* has_term);

    // Accessors for the CURRENT term; valid only after next() returned
    // *has_term=true and, for term()/entry(), before take_entry().
    const std::string& term() const { return entries_[pos_].term; }
    uint64_t term_prefix() const { return term_prefix_; }
    const format::DictEntry& entry() const { return entries_[pos_]; }
    // Moves the current entry out (term + locator + any inline posting bytes).
    // The cursor stays positioned, but term()/entry() must not be used again
    // until the next next() call.
    format::DictEntry take_entry() { return std::move(entries_[pos_]); }

    // frq/prx bases of the DICT block owning the current entry -- required to
    // resolve pod_ref locators against the source's posting region.
    uint64_t frq_base() const { return frq_base_; }
    uint64_t prx_base() const { return prx_base_; }
    uint32_t source_ordinal() const { return source_ordinal_; }

private:
    const reader::LogicalIndexReader* index_ = nullptr;
    uint32_t source_ordinal_ = 0;
    writer::MemoryReporter* memory_reporter_ = nullptr;
    // Keep the preceding block's charge for one additional block transition.
    // The frontier advances a source before the current term's posting cursor
    // has released the DictEntry moved out of that block.
    writer::MemoryReporter::Reservation previous_entries_reservation_;
    writer::MemoryReporter::Reservation entries_reservation_;

    uint32_t next_block_ = 0;                // next DICT block ordinal to decode
    std::vector<format::DictEntry> entries_; // current block, materialized
    size_t pos_ = 0;                         // current entry within entries_
    uint64_t frq_base_ = 0;
    uint64_t prx_base_ = 0;
    uint64_t term_prefix_ = 0;

    bool started_ = false; // first next() must not pre-increment pos_
    bool exhausted_ = false;
    Status failed_ = Status::OK(); // sticky error (poisoned cursor)
    // Strict-order guard across block boundaries: DICT blocks and the merge
    // frontier both assume a strictly increasing term sequence; a violation
    // means a corrupt dictionary and must fail the merge, not scramble output.
    std::string prev_term_;
    bool has_prev_ = false;
};

} // namespace doris::snii::compaction
