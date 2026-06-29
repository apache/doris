#pragma once

#include <cstdint>
#include <vector>

#include "snii/common/slice.h"
#include "common/status.h"
#include "snii/format/dict_entry.h"
#include "snii/format/frq_prelude.h"
#include "snii/reader/logical_index_reader.h"

// WindowedPostingReader -- shared read-side decode of a windowed term's posting
// from its two-level frq_prelude + GROUPED dd-block / freq-block (design 1.6).
//
// A windowed pod_ref entry's .frq payload is laid out
//   [prelude][dd-block][freq-block]
// where the dd-block concatenates every window's dd_region and the freq-block
// every window's freq_region. The docs-only prefix [prelude][dd-block] is ONE
// contiguous run. This helper:
//   1. range-fetches the prelude (prelude_len bytes) and parses the directory,
//   2. range-fetches the WHOLE dd-block in ONE contiguous range (and, for
//   scoring,
//      the whole freq-block in one more range),
//   3. decodes each window's dd region (and freq region) from the in-memory
//   blocks
//      via the prelude metadata (dd_off/dd_disk_len, freq_off/freq_disk_len),
//      and concatenates the per-window docids / freqs / positions.
//
// The slim/inline single-window path is handled by the term/phrase/scoring
// callers directly; this helper is for enc=windowed entries only.
namespace snii::reader {

// Coalesce gap (bytes) used when batch-fetching MULTIPLE dd sub-ranges of the
// SAME term (the phrase window-skip path): dd regions of one term are
// contiguous in the dd-block, so merging reads separated by <= this gap into
// one physical Range GET trades a little over-read for fewer remote GETs (the
// design's higher-priority metric). Only applied to same-term multi-window
// batches, never to cross-term.
inline constexpr uint64_t kSameTermCoalesceGap = 16 * 1024;

// Full decoded posting for one windowed term (docids ascending across windows).
struct DecodedPosting {
    std::vector<uint32_t> docids;
    std::vector<uint32_t> freqs;                  // aligned with docids
    std::vector<std::vector<uint32_t>> positions; // aligned; empty when no prx
};

// Decodes the entire windowed posting. want_positions requires the index to
// have positions (and the entry to carry prx). want_freq selects whether the
// freq-block is fetched + decoded: when false ONLY the contiguous
// [prelude][dd-block] prefix is fetched (docid-only / phrase callers) and
// DecodedPosting.freqs stays empty; when true the freq-block is additionally
// fetched (scoring). Returns Corruption on any prelude/block inconsistency
// (doc-count mismatch, out-of-range offsets).
doris::Status read_windowed_posting(const LogicalIndexReader& idx, const snii::format::DictEntry& entry,
                             uint64_t frq_base, uint64_t prx_base, bool want_positions,
                             bool want_freq, DecodedPosting* out);

// --- Sub-block (window) skipping helpers (shared with phrase / selective WAND)
// --
//
// These expose the per-window dd/freq/prx addressing within the grouped blocks
// so the skip path can fetch ONLY the windows covering candidate docids (their
// dd sub-ranges within the dd-block, near-contiguous and coalesce-friendly)
// instead of the whole posting, without duplicating the offset arithmetic.

// Absolute file byte ranges of one window's regions. dd is always valid; freq
// is valid only when want_freq; prx is valid only when want_positions (and
// has_prx).
struct WindowAbsRange {
    uint64_t dd_off = 0;
    uint64_t dd_len = 0;
    uint64_t freq_off = 0;
    uint64_t freq_len = 0;
    uint64_t prx_off = 0;
    uint64_t prx_len = 0;
};

// Fetches + parses the two-level prelude of a windowed entry (one batched
// read).
doris::Status fetch_windowed_prelude(const LogicalIndexReader& idx, const snii::format::DictEntry& entry,
                              uint64_t frq_base, snii::format::FrqPreludeReader* prelude);

// Computes the absolute file ranges of window w's dd region (and freq region
// when want_freq, and .prx window when want_positions), fully validated against
// the POD sections (anti-DoS: rejects out-of-range offsets and overflowing
// locators).
doris::Status windowed_window_range(const LogicalIndexReader& idx, const snii::format::DictEntry& entry,
                             uint64_t frq_base, uint64_t prx_base,
                             const snii::format::FrqPreludeReader& prelude, uint32_t w,
                             bool want_positions, bool want_freq, WindowAbsRange* out);

// Decodes one window's docids (and per-doc positions when want_positions, and
// per-doc freqs when want_freq) from already-fetched byte slices: dd_region is
// the window's dd sub-slice; freq_region its freq sub-slice (ignored when
// !want_freq); prx_window its .prx bytes. The decoded docids are absolute
// (win_base applied). Returns Corruption on any doc-count mismatch between the
// prelude, dd/freq and prx.
doris::Status decode_window_slices(const snii::format::WindowMeta& meta, Slice dd_region,
                            Slice freq_region, Slice prx_window, bool want_positions,
                            bool want_freq, std::vector<uint32_t>* docids,
                            std::vector<uint32_t>* freqs,
                            std::vector<std::vector<uint32_t>>* positions);

} // namespace snii::reader
