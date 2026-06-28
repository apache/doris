#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string_view>
#include <vector>

#include "snii/common/slice.h"
#include "snii/common/status.h"
#include "snii/format/bsbf.h"
#include "snii/format/dict_block.h"
#include "snii/format/dict_block_directory.h"
#include "snii/format/dict_entry.h"
#include "snii/format/format_constants.h"
#include "snii/format/per_index_meta.h"
#include "snii/format/sampled_term_index.h"
#include "snii/format/stats_block.h"
#include "snii/io/file_reader.h"

// LogicalIndexReader -- read-side counterpart of LogicalIndexWriter for one
// logical index. It owns the resident per-index meta sub-readers (XFilter,
// SampledTermIndex, DICT block directory, StatsBlock, SectionRefs) parsed from
// the per-index meta block, and resolves a query term to its DictEntry through
// the documented lookup flow:
//   XFilter (reject absent) -> SampledTermIndex (candidate block ordinal) ->
//   DICT block directory (block range) -> resident small-DICT block or one
//   range read of the DICT block -> DictBlockReader::find_term.
//
// lookup() also returns the block's frq_base/prx_base (captured by the
// DictBlockReader) so callers can resolve a pod_ref entry's absolute .frq/.prx
// offsets via the writer's contract. Both deltas index into the SAME
// interleaved posting region (prx_base == frq_base; the prx span precedes the
// frq span):
//   abs_frq = posting_region.offset + frq_base + entry.frq_off_delta
//   abs_prx = posting_region.offset + prx_base + entry.prx_off_delta
//
// The meta block bytes must outlive this reader (they are owned by the parent
// SniiSegmentReader's resident meta region).
namespace snii::reader {

class LogicalIndexReader {
public:
    LogicalIndexReader() = default;

    // Parses the per-index meta block and binds the reader to file_reader.
    // file_reader / meta_block must outlive this reader.
    static Status open(snii::io::FileReader* file_reader, snii::format::IndexTier tier,
                       bool has_positions, Slice meta_block, LogicalIndexReader* out);

    // Resolves term to a DictEntry. *found=false when the term is absent (XFilter
    // rejection, out-of-range sample, or DICT-block miss). On a hit, *entry is
    // filled and *frq_base / *prx_base carry the candidate block's bases.
    Status lookup(std::string_view term, bool* found, snii::format::DictEntry* entry,
                  uint64_t* frq_base, uint64_t* prx_base) const;

    // One enumerated term whose key has the requested prefix, with its DictEntry
    // and the owning DICT block's frq/prx bases (for posting resolution).
    struct PrefixHit {
        std::string term;
        snii::format::DictEntry entry;
        uint64_t frq_base = 0;
        uint64_t prx_base = 0;
    };

    using PrefixHitVisitor = std::function<Status(PrefixHit&& hit, bool* stop)>;

    // Ordered term enumeration: every term with `prefix`, in lexicographic order,
    // by seeking the start DICT block via the SampledTermIndex and scanning
    // forward across contiguous blocks until the terms pass the prefix range.
    // Empty prefix enumerates all terms. This is the contiguous-DICT-block design
    // the term-anchor layout was built for (MATCH_PHRASE_PREFIX / prefix / range
    // queries). The visitor form avoids materializing all hits when callers only
    // need a bounded expansion.
    Status visit_prefix_terms(std::string_view prefix, const PrefixHitVisitor& visitor) const;
    Status prefix_terms(std::string_view prefix, std::vector<PrefixHit>* const out,
                        int32_t max_terms = 0) const;

    // Resolves a pod_ref entry's absolute .frq / .prx window byte range,
    // validating the locator against the posting_region length (defends against
    // corrupt entries: prelude_len > frq_len underflow, or off_delta+len past the
    // region). Both windows resolve against the single posting_region. *abs_off
    // is the absolute file offset of the window (after prelude); *len its byte
    // length.
    Status resolve_frq_window(const snii::format::DictEntry& entry, uint64_t frq_base,
                              uint64_t* abs_off, uint64_t* len) const;
    Status resolve_prx_window(const snii::format::DictEntry& entry, uint64_t prx_base,
                              uint64_t* abs_off, uint64_t* len) const;

    const snii::format::SectionRefs& section_refs() const { return meta_.section_refs(); }
    const snii::format::StatsBlock& stats() const { return meta_.stats(); }
    snii::format::IndexTier tier() const { return tier_; }
    bool has_positions() const { return has_positions_; }
    snii::io::FileReader* reader() const { return reader_; }
    size_t memory_usage() const;

private:
    snii::io::FileReader* reader_ = nullptr;
    snii::format::IndexTier tier_ = snii::format::IndexTier::kT1;
    bool has_positions_ = false;
    snii::format::PerIndexMetaReader meta_;
    snii::format::SampledTermIndexReader sti_;
    snii::format::DictBlockDirectoryReader dbd_;
    snii::format::BsbfHeader bsbf_header_; // resident header (from section ref)
    bool has_bsbf_ = false;
    // L0 tiering: when the bsbf section is small (<= kBsbfResidentMaxBytes) its
    // whole bitset is loaded here at open -> in-memory probe, no per-lookup
    // round. Empty => L1 (on-demand single-block probe via bsbf_probe).
    bool bsbf_resident_ = false;
    std::vector<uint8_t> bsbf_resident_bitset_;

    // Small DICT blocks are opened once with the index so exact lookups avoid an
    // otherwise serial S3 round for the term dictionary. Empty means the
    // dictionary exceeded the resident threshold and lookup/prefix enumeration
    // read blocks on demand. Each DictBlockReader holds a Slice into the owning
    // bytes.
    struct ResidentDictBlock {
        std::vector<uint8_t> bytes;
        snii::format::DictBlockReader reader;
    };
    struct OnDemandDictBlock {
        std::vector<uint8_t> bytes;
        snii::format::DictBlockReader reader;
    };
    Status load_resident_dict_blocks();
    Status dict_block_reader_for_ordinal(uint32_t ordinal, OnDemandDictBlock* on_demand,
                                         const snii::format::DictBlockReader** out) const;
    std::vector<ResidentDictBlock> resident_dict_blocks_;
};

} // namespace snii::reader
