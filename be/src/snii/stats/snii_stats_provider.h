#pragma once

#include <cstdint>
#include <string_view>

#include "common/status.h"
#include "snii/format/norms_pod.h"
#include "snii/reader/logical_index_reader.h"

// SniiStatsProvider -- exposes the native SNII scoring statistics required by
// BM25, sourced directly from the on-disk structures of one logical index:
//   - segment-level counts (doc_count, indexed_doc_count, sum_total_term_freq)
//     from the StatsBlock embedded in the per-index meta block.
//   - per-term df / ttf from the term's DictEntry (resolved through the reader's
//     lookup flow). The LogicalIndexWriter stores ttf directly in ttf_delta for
//     tier>=T2 entries, so total_term_freq returns entry.ttf_delta.
//   - per-doc length normalization byte (encoded_norm) from the norms POD,
//     range-read once at open via section_refs().norms and parsed with
//     NormsPodReader.
//
// avgdl() = sum_total_term_freq / max(1, indexed_doc_count): the average document
// length used by BM25 length normalization. The provider performs no scoring; it
// only surfaces the statistics so snii::query::Bm25Scorer can combine them.
namespace snii::stats {

class SniiStatsProvider {
public:
    SniiStatsProvider() = default;

    // Binds to idx and materializes the norms POD (one range read) when the index
    // carries scoring norms. idx must outlive this provider. A scoring index
    // without a norms section, or a corrupt norms POD, returns a non-OK doris::Status.
    static doris::Status open(const snii::reader::LogicalIndexReader* idx, SniiStatsProvider* out);

    // Segment-level counts (direct StatsBlock fields).
    uint64_t doc_count() const { return doc_count_; }
    uint64_t indexed_doc_count() const { return indexed_doc_count_; }
    uint64_t sum_total_term_freq() const { return sum_total_term_freq_; }

    // Average document length: sum_total_term_freq / max(1, indexed_doc_count).
    double avgdl() const;

    // Per-term document frequency. Absent term -> *df = 0 (OK status).
    doris::Status doc_freq(std::string_view term, uint64_t* df) const;

    // Per-term total term frequency (ttf = df + ttf_delta at tier>=T2). Absent
    // term -> *ttf = 0 (OK status).
    doris::Status total_term_freq(std::string_view term, uint64_t* ttf) const;

    // 1-byte encoded doc-length norm for docid (raw byte from the norms POD).
    // Out-of-range docid -> InvalidArgument; index without norms -> InvalidArgument.
    doris::Status encoded_norm(uint32_t docid, uint8_t* out) const;

    bool has_norms() const { return has_norms_; }

private:
    const snii::reader::LogicalIndexReader* idx_ = nullptr;
    uint64_t doc_count_ = 0;
    uint64_t indexed_doc_count_ = 0;
    uint64_t sum_total_term_freq_ = 0;
    bool has_norms_ = false;
    // Owned copy of the framed norms section bytes; norms_reader_ borrows from it.
    std::vector<uint8_t> norms_bytes_;
    snii::format::NormsPodReader norms_reader_;
};

} // namespace snii::stats
