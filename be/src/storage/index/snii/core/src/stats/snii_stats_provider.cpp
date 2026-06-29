#include "snii/stats/snii_stats_provider.h"

#include <algorithm>
#include <utility>

#include "snii/common/slice.h"
#include "snii/format/dict_entry.h"
#include "snii/format/format_constants.h"
#include "snii/format/stats_block.h"
#include "snii/io/batch_range_fetcher.h"

namespace snii::stats {
using doris::Status; // RETURN_IF_ERROR expands to bare Status

using snii::format::DictEntry;
using snii::format::NormsPodReader;
using snii::format::RegionRef;

namespace {

// Resolves a term's DictEntry. *found=false for an absent term (OK status).
doris::Status LookupEntry(const snii::reader::LogicalIndexReader& idx, std::string_view term, bool* found,
                   DictEntry* entry) {
    uint64_t frq_base = 0;
    uint64_t prx_base = 0;
    return idx.lookup(term, found, entry, &frq_base, &prx_base);
}

} // namespace

doris::Status SniiStatsProvider::open(const snii::reader::LogicalIndexReader* idx,
                               SniiStatsProvider* out) {
    if (idx == nullptr || out == nullptr) {
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>("stats_provider: null argument");
    }
    out->idx_ = idx;
    const auto& sb = idx->stats();
    out->doc_count_ = sb.doc_count;
    out->indexed_doc_count_ = sb.indexed_doc_count;
    out->sum_total_term_freq_ = sb.sum_total_term_freq;

    const RegionRef& norms = idx->section_refs().norms;
    if (norms.length == 0) {
        out->has_norms_ = false;
        return doris::Status::OK();
    }

    snii::io::BatchRangeFetcher fetcher(idx->reader());
    const size_t h = fetcher.add(norms.offset, norms.length);
    RETURN_IF_ERROR(fetcher.fetch());
    Slice framed = fetcher.get(h);
    out->norms_bytes_.assign(framed.data(), framed.data() + framed.size());
    RETURN_IF_ERROR(NormsPodReader::open(Slice(out->norms_bytes_), &out->norms_reader_));
    out->has_norms_ = true;
    return doris::Status::OK();
}

double SniiStatsProvider::avgdl() const {
    const uint64_t denom = std::max<uint64_t>(1, indexed_doc_count_);
    return static_cast<double>(sum_total_term_freq_) / static_cast<double>(denom);
}

doris::Status SniiStatsProvider::doc_freq(std::string_view term, uint64_t* df) const {
    if (df == nullptr) return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>("stats_provider: null df");
    *df = 0;
    bool found = false;
    DictEntry entry;
    RETURN_IF_ERROR(LookupEntry(*idx_, term, &found, &entry));
    if (found) *df = entry.df;
    return doris::Status::OK();
}

doris::Status SniiStatsProvider::total_term_freq(std::string_view term, uint64_t* ttf) const {
    if (ttf == nullptr) return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>("stats_provider: null ttf");
    *ttf = 0;
    bool found = false;
    DictEntry entry;
    RETURN_IF_ERROR(LookupEntry(*idx_, term, &found, &entry));
    if (!found) return doris::Status::OK();
    // tier>=T2 entries carry the total term frequency directly in ttf_delta (the
    // LogicalIndexWriter stores ttf there, not a delta from df).
    *ttf = entry.ttf_delta;
    return doris::Status::OK();
}

doris::Status SniiStatsProvider::encoded_norm(uint32_t docid, uint8_t* out) const {
    if (out == nullptr) return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>("stats_provider: null out");
    if (!has_norms_) {
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>("stats_provider: index has no norms");
    }
    return norms_reader_.try_encoded_norm(docid, out);
}

} // namespace snii::stats
