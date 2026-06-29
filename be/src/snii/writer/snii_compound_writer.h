#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include "common/status.h"
#include "snii/io/file_writer.h"
#include "snii/writer/logical_index_writer.h"

// SniiCompoundWriter -- orchestrates a single-segment SNII container for one or
// more logical indexes, written front-to-back through an append-only
// io::FileWriter (no seek-back). It resolves all back-references by writing the
// tail meta region and the fixed tail pointer LAST.
//
// CONTAINER LAYOUT PRODUCED (this is the on-disk contract the reader matches):
//   [bootstrap_header]                          (kBootstrapHeaderSize bytes)
//   for each logical index, in add order:
//     [posting region]       interleaved [prx][frq] per pod_ref term, term order
//                            (prx span empty when !has_prx)
//     [DICT blocks region]   concatenated DICT blocks, split by
//                            target_dict_block_bytes
//   for each logical index, in add order:
//     [norms POD]            NormsPodWriter::finish (scoring only; else absent)
//     [null bitmap POD]      NullBitmapWriter::finish (when nulls exist)
//   [tail_meta_region]       one per_index_meta block per index + directory
//   [tail_pointer]           encode_tail_pointer at EOF
//
// (The posting region is streamed BEFORE the DICT region per index: postings are
// the large append-only term-ordered stream; the DICT region is the compact
// compressed trailer.)
//
// OFFSET CONVENTIONS (ABSOLUTE file offsets unless stated otherwise):
//   - SectionRefs in each per_index_meta record ABSOLUTE file offset+length of
//     that index's posting_region, dict_region, norms. Absent regions are (0,0)
//     (e.g. norms for a docs-positions index; null_bitmap is always (0,0) in v1).
//     A present-but-empty posting_region (all-INLINE index) is (off, 0).
//   - DictBlockDirectory entries record each DICT block's ABSOLUTE file offset +
//     length.
//   - A windowed/slim pod_ref entry's absolute .frq offset =
//       section_refs.posting_region.offset + frq_base + frq_off_delta
//     where frq_base is the posting-region-relative running offset captured at the
//     block's open (see logical_index_writer.h). prx follows the identical rule
//     against the SAME region (prx_base == frq_base).
//   - tail_pointer.meta_region_offset/length point at the tail_meta_region;
//     hot_off = 0 (no hot region in v1).
namespace snii::writer {

class SniiCompoundWriter {
public:
    explicit SniiCompoundWriter(snii::io::FileWriter* out);

    // Buffers one logical index: builds its section bytes and meta sub-sections.
    // The actual file writing happens in finish() (single front-to-back pass).
    doris::Status add_logical_index(const SniiIndexInput& in);

    // Writes bootstrap header + all index sections + norms + tail meta region +
    // tail pointer, then finalizes the underlying writer. May be called once.
    doris::Status finish();

private:
    // Absolute placement of one index's sections, resolved during finish().
    struct Placement {
        uint64_t dict_off = 0;
        uint64_t dict_len = 0;
        uint64_t post_off = 0; // interleaved [prx][frq] posting region (was frq + prx)
        uint64_t post_len = 0;
        uint64_t norms_off = 0;
        uint64_t norms_len = 0;
        uint64_t null_off = 0;
        uint64_t null_len = 0;
        uint64_t bsbf_off = 0;
        uint64_t bsbf_len = 0;
    };

    doris::Status ensure_bootstrap();
    doris::Status write_bootstrap();
    doris::Status write_norms();
    doris::Status write_tail();
    doris::Status append(const std::vector<uint8_t>& bytes);

    snii::io::FileWriter* out_;
    std::vector<std::unique_ptr<LogicalIndexWriter>> indexes_;
    // Per-index placement; post_off/post_len are filled as each index's posting region
    // streams in during add_logical_index, the rest during finish(). The absolute write
    // offset is out_->bytes_written() (the single source of truth -- no separate cursor).
    std::vector<Placement> placements_;
    bool bootstrap_written_ = false;
    bool finished_ = false;
};

} // namespace snii::writer
