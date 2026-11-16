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
#include <string_view>
#include <unordered_map>
#include <vector>

#include "common/status.h"

namespace doris::snii {
class ByteSink;
} // namespace doris::snii

namespace doris::snii::format {

// DICT gram summary: for every 3-byte window (gram) of a term key, the DICT blocks whose keys
// hold it. A wildcard or regexp expansion that must find a literal of three bytes or more in
// every term it keeps visits only the blocks that hold all of the literal's grams; a tokenized
// dictionary of log text keeps about one block in a hundred for a word-sized literal, so the
// walk neither fetches, decompresses nor scans the rest.
//
// The section is gram-major so a query reads the rows of its own grams and nothing else:
//   [u8 version = kDictGramSummaryVersion]
//   [varint32 n_blocks][varint32 n_grams]
//   [n_grams x (fixed32 gram, fixed32 row_offset)]   # ascending gram; offset within the rows
//   [fixed32 rows_length]
//   [rows]                                           # one row per gram, in table order
//   [fixed32 crc32c]                                 # over every byte before it
// A row lists the block ordinals holding the gram as runs of consecutive ordinals:
//   varint32 n_runs, then per run varint32 gap (first ordinal minus the end of the previous
//   run, which starts at 0) and varint32 length - 1.
// A gram is coded as its three bytes, first byte highest, so grams of any byte string sort
// and compare without an alphabet. A reader that does not know the version walks the whole
// dictionary, as without a section.

inline constexpr size_t kDictGramBytes = 3;
inline constexpr uint8_t kDictGramSummaryVersion = 1;
// A summary larger than this share of the DICT region is not written: keys without literal
// structure (random ids, ideographic pairs) spread over about as many grams as keys, so their
// summary would cost as much as the dictionary it lets a walk skip. A tokenized text
// dictionary stays around a twentieth; a small one with few keys per gram may reach a third.
inline constexpr uint64_t kDictGramSummaryMaxDictPercent = 50;
// More distinct grams than this means keys without literal structure: collection stops and no
// summary is written, so the dictionary build does not pay for a section it would drop.
inline constexpr size_t kDictGramSummaryMaxGrams = size_t {1} << 17;

inline uint32_t dict_gram_code(const uint8_t* bytes) {
    return (static_cast<uint32_t>(bytes[0]) << 16) | (static_cast<uint32_t>(bytes[1]) << 8) |
           static_cast<uint32_t>(bytes[2]);
}

// Appends the code of every gram of `term`, in order and with repeats, to *out.
void append_dict_gram_codes(std::string_view term, std::vector<uint32_t>* out);

// The distinct grams of the block being built: an open-addressing set of gram codes that grows
// with the block and is cleared for the next one, so the grams of a block's keys are collected
// once each without a sort of every occurrence (a block of ids has tens of thousands).
class DictGramSet {
public:
    DictGramSet();

    // True when `code` was not in the set; it is then added.
    bool insert(uint32_t code);
    void clear();
    size_t size() const { return count_; }
    size_t resident_bytes() const { return slots_.size() * sizeof(uint32_t); }

private:
    static constexpr uint32_t kEmpty = 0xFFFFFFFFU;
    void grow();

    std::vector<uint32_t> slots_;
    size_t count_ = 0;
};

// Appends the codes of the grams of `term` that `set` did not hold yet, adding them to it.
void collect_dict_gram_codes(std::string_view term, DictGramSet* set, std::vector<uint32_t>* out);

// Collects the grams of DICT blocks in ascending ordinal order and serializes the section.
class DictGramSummaryBuilder {
public:
    // Adds block `ordinal` (one more than the previous call's, the first is 0) with the gram
    // codes of its keys, in any order and with repeats. The vector is sorted in place.
    void add_block(uint32_t ordinal, std::vector<uint32_t>* gram_codes);

    uint32_t n_blocks() const { return n_blocks_; }
    size_t n_grams() const { return rows_.size(); }

    // True once the distinct grams exceed kDictGramSummaryMaxGrams; the caller stops collecting.
    bool over_gram_limit() const { return rows_.size() > kDictGramSummaryMaxGrams; }

    // False when the section could not skip blocks: at most one block, or keys so uniform
    // (hexadecimal ids) that most grams sit in most blocks. Such a section is not written.
    bool prunes() const;

    // Heap bytes held by the builder, for the writer's memory accounting.
    size_t resident_bytes() const;

    // Appends the section to `sink`.
    void finish(ByteSink* sink) const;

private:
    struct Row {
        uint32_t run_start = 0;    // first ordinal of the open run
        uint32_t run_length = 0;   // 0 when no run is open
        uint32_t prev_run_end = 0; // ordinal after the last closed run
        uint32_t n_runs = 0;       // closed runs
        uint32_t n_blocks = 0;     // blocks holding the gram
        std::vector<uint8_t> runs; // closed runs, encoded
    };
    void add_ordinal(Row* row, uint32_t ordinal);
    static void close_run(Row* row);
    static void encode_row(const Row& row, ByteSink* sink);

    std::unordered_map<uint32_t, Row> rows_;
    uint32_t n_blocks_ = 0;
    uint64_t incidences_ = 0;
    size_t runs_bytes_ = 0;
};

// Reads a section and answers which blocks can hold every gram of a text.
class DictGramSummaryReader {
public:
    // Takes the section bytes; verifies the crc and the table. Corruption -> kCorruption.
    static Status open(std::vector<uint8_t> section, DictGramSummaryReader* out);

    uint32_t n_blocks() const { return n_blocks_; }

    // Sets the bit of every block that holds all grams of `text` (at least kDictGramBytes long)
    // in *blocks, sized to n_blocks() bits in 64-bit words. *any is false when some gram is in
    // no block at all, so no term of the dictionary can hold the text.
    Status candidate_blocks(std::string_view text, std::vector<uint64_t>* blocks, bool* any) const;

private:
    // Decodes the row of the gram at table `index` into *blocks, sized to n_blocks() bits and
    // otherwise cleared.
    Status decode_row(size_t index, std::vector<uint64_t>* blocks) const;
    bool find_gram(uint32_t gram, size_t* index) const;

    std::vector<uint8_t> section_;
    uint32_t n_blocks_ = 0;
    uint32_t n_grams_ = 0;
    size_t table_offset_ = 0;
    size_t rows_offset_ = 0;
    uint32_t rows_length_ = 0;
};

} // namespace doris::snii::format
