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

#include "storage/index/inverted/snii/format/dict_gram_summary.h"

#include <algorithm>
#include <utility>

#include "common/check.h"
#include "storage/index/inverted/snii/encoding/byte_sink.h"
#include "storage/index/inverted/snii/encoding/byte_source.h"
#include "storage/index/inverted/snii/encoding/crc32c.h"

namespace doris::snii::format {

namespace {

constexpr size_t kTableEntryBytes = 2 * sizeof(uint32_t);
constexpr size_t kCrcBytes = sizeof(uint32_t);
constexpr size_t kDictGramSetInitialSlots = size_t {1} << 14;

Status corrupted(const char* what) {
    return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
            std::string("dict gram summary: ") + what);
}

void put_varint32(std::vector<uint8_t>* out, uint32_t value) {
    while (value >= 0x80) {
        out->push_back(static_cast<uint8_t>(value | 0x80));
        value >>= 7;
    }
    out->push_back(static_cast<uint8_t>(value));
}

// Little-endian, as ByteSink::put_fixed32 writes it; the bounds were checked at open.
uint32_t read_fixed32(const uint8_t* bytes) {
    return static_cast<uint32_t>(bytes[0]) | (static_cast<uint32_t>(bytes[1]) << 8) |
           (static_cast<uint32_t>(bytes[2]) << 16) | (static_cast<uint32_t>(bytes[3]) << 24);
}

} // namespace

void append_dict_gram_codes(std::string_view term, std::vector<uint32_t>* out) {
    if (term.size() < kDictGramBytes) {
        return;
    }
    const auto* bytes = reinterpret_cast<const uint8_t*>(term.data());
    for (size_t i = 0; i + kDictGramBytes <= term.size(); ++i) {
        out->push_back(dict_gram_code(bytes + i));
    }
}

DictGramSet::DictGramSet() : slots_(kDictGramSetInitialSlots, kEmpty) {}

bool DictGramSet::insert(uint32_t code) {
    DORIS_CHECK_LT(code, kEmpty);
    // Fibonacci hashing: the slot count is a power of two, so the top bits of the product spread
    // the codes, which differ mostly in their low bytes, over the table.
    const size_t mask = slots_.size() - 1;
    size_t slot = (static_cast<uint64_t>(code) * 0x9E3779B97F4A7C15ULL) >> 40;
    for (slot &= mask;; slot = (slot + 1) & mask) {
        if (slots_[slot] == code) {
            return false;
        }
        if (slots_[slot] == kEmpty) {
            slots_[slot] = code;
            ++count_;
            if (count_ * 2 > slots_.size()) {
                grow();
            }
            return true;
        }
    }
}

void DictGramSet::grow() {
    std::vector<uint32_t> old;
    old.swap(slots_);
    slots_.assign(old.size() * 2, kEmpty);
    count_ = 0;
    for (const uint32_t code : old) {
        if (code != kEmpty) {
            insert(code);
        }
    }
}

void DictGramSet::clear() {
    std::ranges::fill(slots_, kEmpty);
    count_ = 0;
}

void collect_dict_gram_codes(std::string_view term, DictGramSet* set, std::vector<uint32_t>* out) {
    if (term.size() < kDictGramBytes) {
        return;
    }
    const auto* bytes = reinterpret_cast<const uint8_t*>(term.data());
    for (size_t i = 0; i + kDictGramBytes <= term.size(); ++i) {
        const uint32_t code = dict_gram_code(bytes + i);
        if (set->insert(code)) {
            out->push_back(code);
        }
    }
}

void DictGramSummaryBuilder::close_run(Row* row) {
    DORIS_CHECK_GT(row->run_length, 0U);
    put_varint32(&row->runs, row->run_start - row->prev_run_end);
    put_varint32(&row->runs, row->run_length - 1);
    row->prev_run_end = row->run_start + row->run_length;
    ++row->n_runs;
    row->run_length = 0;
}

void DictGramSummaryBuilder::add_ordinal(Row* row, uint32_t ordinal) {
    ++row->n_blocks;
    if (row->run_length != 0 && row->run_start + row->run_length == ordinal) {
        ++row->run_length;
        return;
    }
    if (row->run_length != 0) {
        const size_t before = row->runs.size();
        close_run(row);
        runs_bytes_ += row->runs.size() - before;
    }
    row->run_start = ordinal;
    row->run_length = 1;
}

void DictGramSummaryBuilder::add_block(uint32_t ordinal, std::vector<uint32_t>* gram_codes) {
    DORIS_CHECK_EQ(ordinal, n_blocks_);
    n_blocks_ = ordinal + 1;
    std::ranges::sort(*gram_codes);
    uint32_t previous = 0;
    bool has_previous = false;
    for (const uint32_t code : *gram_codes) {
        if (has_previous && code == previous) {
            continue;
        }
        previous = code;
        has_previous = true;
        ++incidences_;
        add_ordinal(&rows_[code], ordinal);
    }
}

bool DictGramSummaryBuilder::prunes() const {
    if (n_blocks_ < 2 || rows_.empty()) {
        return false;
    }
    // The mean gram sits in this share of the blocks; above a half the walk would keep most
    // blocks for any literal, and the section would only add bytes.
    return incidences_ * 2 < static_cast<uint64_t>(n_blocks_) * rows_.size();
}

size_t DictGramSummaryBuilder::resident_bytes() const {
    return rows_.size() * (sizeof(uint32_t) + sizeof(Row) + 2 * sizeof(void*)) + runs_bytes_;
}

void DictGramSummaryBuilder::encode_row(const Row& row, ByteSink* sink) {
    std::vector<uint8_t> scratch;
    // The open run, if any, closes at the end of the dictionary.
    put_varint32(&scratch, row.n_runs + (row.run_length != 0 ? 1U : 0U));
    sink->put_bytes(Slice(scratch));
    sink->put_bytes(Slice(row.runs));
    if (row.run_length != 0) {
        scratch.clear();
        put_varint32(&scratch, row.run_start - row.prev_run_end);
        put_varint32(&scratch, row.run_length - 1);
        sink->put_bytes(Slice(scratch));
    }
}

void DictGramSummaryBuilder::finish(ByteSink* sink) const {
    std::vector<std::pair<uint32_t, const Row*>> ordered;
    ordered.reserve(rows_.size());
    for (const auto& [code, row] : rows_) {
        ordered.emplace_back(code, &row);
    }
    std::ranges::sort(ordered,
                      [](const auto& lhs, const auto& rhs) { return lhs.first < rhs.first; });

    ByteSink rows;
    std::vector<uint32_t> offsets;
    offsets.reserve(ordered.size());
    for (const auto& [code, row] : ordered) {
        offsets.push_back(static_cast<uint32_t>(rows.size()));
        encode_row(*row, &rows);
    }

    ByteSink section;
    section.reserve(16 + ordered.size() * kTableEntryBytes + rows.size() + kCrcBytes);
    section.put_u8(kDictGramSummaryVersion);
    section.put_varint32(n_blocks_);
    section.put_varint32(static_cast<uint32_t>(ordered.size()));
    for (size_t i = 0; i < ordered.size(); ++i) {
        section.put_fixed32(ordered[i].first);
        section.put_fixed32(offsets[i]);
    }
    section.put_fixed32(static_cast<uint32_t>(rows.size()));
    section.put_bytes(rows.view());
    section.put_fixed32(crc32c(section.view()));
    sink->put_bytes(section.view());
}

Status DictGramSummaryReader::open(std::vector<uint8_t> section, DictGramSummaryReader* out) {
    if (section.size() < kCrcBytes + 3) {
        return corrupted("section too small");
    }
    const Slice covered(section.data(), section.size() - kCrcBytes);
    ByteSource trailer(Slice(section.data() + covered.size(), kCrcBytes));
    uint32_t stored_crc = 0;
    RETURN_IF_ERROR(trailer.get_fixed32(&stored_crc));
    if (crc32c(covered) != stored_crc) {
        return corrupted("crc mismatch");
    }
    ByteSource src(covered);
    uint8_t version = 0;
    RETURN_IF_ERROR(src.get_u8(&version));
    if (version != kDictGramSummaryVersion) {
        return corrupted("unknown version");
    }
    DictGramSummaryReader reader;
    RETURN_IF_ERROR(src.get_varint32(&reader.n_blocks_));
    RETURN_IF_ERROR(src.get_varint32(&reader.n_grams_));
    reader.table_offset_ = src.position();
    if (reader.n_grams_ > src.remaining() / kTableEntryBytes) {
        return corrupted("table overruns the section");
    }
    uint32_t previous_gram = 0;
    uint32_t previous_offset = 0;
    for (uint32_t i = 0; i < reader.n_grams_; ++i) {
        uint32_t gram = 0;
        uint32_t offset = 0;
        RETURN_IF_ERROR(src.get_fixed32(&gram));
        RETURN_IF_ERROR(src.get_fixed32(&offset));
        if (gram > 0xFFFFFFU || (i != 0 && (gram <= previous_gram || offset < previous_offset))) {
            return corrupted("table out of order");
        }
        previous_gram = gram;
        previous_offset = offset;
    }
    RETURN_IF_ERROR(src.get_fixed32(&reader.rows_length_));
    reader.rows_offset_ = src.position();
    if (reader.rows_length_ != src.remaining()) {
        return corrupted("rows length mismatch");
    }
    if (reader.n_grams_ != 0 && previous_offset >= reader.rows_length_) {
        return corrupted("row offset past the rows");
    }
    reader.section_ = std::move(section);
    *out = std::move(reader);
    return Status::OK();
}

bool DictGramSummaryReader::find_gram(uint32_t gram, size_t* index) const {
    const uint8_t* table = section_.data() + table_offset_;
    size_t lo = 0;
    size_t hi = n_grams_;
    while (lo < hi) {
        const size_t mid = lo + (hi - lo) / 2;
        const uint32_t code = read_fixed32(table + mid * kTableEntryBytes);
        if (code < gram) {
            lo = mid + 1;
        } else if (code > gram) {
            hi = mid;
        } else {
            *index = mid;
            return true;
        }
    }
    return false;
}

Status DictGramSummaryReader::decode_row(size_t index, std::vector<uint64_t>* blocks) const {
    const uint8_t* table = section_.data() + table_offset_;
    const uint32_t begin = read_fixed32(table + index * kTableEntryBytes + sizeof(uint32_t));
    const uint32_t end =
            index + 1 < n_grams_
                    ? read_fixed32(table + (index + 1) * kTableEntryBytes + sizeof(uint32_t))
                    : rows_length_;
    if (begin > end) {
        return corrupted("row bounds out of order");
    }
    blocks->assign((n_blocks_ + 63) / 64, 0);
    ByteSource row(Slice(section_.data() + rows_offset_ + begin, end - begin));
    uint32_t n_runs = 0;
    RETURN_IF_ERROR(row.get_varint32(&n_runs));
    uint64_t cursor = 0;
    for (uint32_t i = 0; i < n_runs; ++i) {
        uint32_t gap = 0;
        uint32_t length_minus_one = 0;
        RETURN_IF_ERROR(row.get_varint32(&gap));
        RETURN_IF_ERROR(row.get_varint32(&length_minus_one));
        const uint64_t start = cursor + gap;
        const uint64_t stop = start + length_minus_one + 1;
        if (stop > n_blocks_) {
            return corrupted("run past the last block");
        }
        for (uint64_t ordinal = start; ordinal < stop; ++ordinal) {
            (*blocks)[ordinal / 64] |= uint64_t {1} << (ordinal % 64);
        }
        cursor = stop;
    }
    if (row.remaining() != 0) {
        return corrupted("row has trailing bytes");
    }
    return Status::OK();
}

Status DictGramSummaryReader::candidate_blocks(std::string_view text, std::vector<uint64_t>* blocks,
                                               bool* any) const {
    if (text.size() < kDictGramBytes) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "dict gram summary: text shorter than a gram");
    }
    blocks->assign((n_blocks_ + 63) / 64, 0);
    *any = false;
    std::vector<uint32_t> codes;
    append_dict_gram_codes(text, &codes);
    std::ranges::sort(codes);
    const auto [duplicates_begin, duplicates_end] = std::ranges::unique(codes);
    codes.erase(duplicates_begin, duplicates_end);
    std::vector<uint64_t> row_blocks;
    bool first = true;
    for (const uint32_t code : codes) {
        size_t index = 0;
        if (!find_gram(code, &index)) {
            blocks->assign(blocks->size(), 0);
            return Status::OK();
        }
        RETURN_IF_ERROR(decode_row(index, &row_blocks));
        if (first) {
            *blocks = row_blocks;
            first = false;
        } else {
            for (size_t i = 0; i < blocks->size(); ++i) {
                (*blocks)[i] &= row_blocks[i];
            }
        }
    }
    *any = std::ranges::any_of(*blocks, [](uint64_t word) { return word != 0; });
    return Status::OK();
}

} // namespace doris::snii::format
