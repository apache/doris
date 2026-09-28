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
#include <vector>

#include "common/status.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_sink.h"

// FrqPrelude: a TWO-LEVEL (super-block -> window) skippable directory that
// precedes a windowed .frq posting whose payload is laid out as:
//   windowed .frq payload = [prelude][dd-block]
//     dd-block   = dd_region_0 ++ dd_region_1 ++ ... ++ dd_region_{N-1}
// Windows are NOT self-describing: each window's full codec metadata (region
// offsets, on-disk/uncompressed lengths, modes, crcs) lives in the prelude rows.
// The complete posting is therefore one contiguous range.
//
// On-disk layout (strict; all multi-byte fixed fields little-endian, VInt =
// LEB128 via snii/encoding):
//   header:
//     u8   flags        # bit0 has_prx
//     VInt N            # number of .frq windows
//     VInt G            # windows per super-block (group_size; >=1)
//     VInt n_super      # = ceil(N / G); 0 when N==0
//     VInt sbdir_len    # byte length of the super_block_dir region
//     u32  crc32c       # covers header + super_block_dir (NOT the window blocks)
//   super_block_dir[n_super]:  # small, resident: one row per super-block
//     VInt sb_last_docid_delta # cumulative across super-blocks => absolute last
//                              #   docid of the super-block's last window
//     VInt sb_block_off        # byte offset of this super-block's window block,
//                              #   measured from the start of the window_dir region
//     VInt sb_block_len        # byte length of this super-block's window block
//   window_dir: n_super self-contained blocks, each holding <=G window rows.
//     per window row (dd_off/prx_off are NOT stored;
//     the reader derives them as running prefix sums of the disk/prx lengths):
//       VInt last_docid_delta  # cumulative WITHIN the block => absolute last docid
//                              #   (previous window's absolute last docid = win_base;
//                              #    first window of first block: win_base = 0)
//       VInt doc_count         # number of docs in the window (frq_pod needs it)
//       u8   win_mode          # bit0 dd_zstd
//       VInt dd_disk_len       # dd_region on-disk byte length
//      [VInt dd_uncomp_len]    # dd_region plaintext length; present ONLY when
//                              #   win_mode & kDdZstd. A raw region's uncomp_len
//                              #   == dd_disk_len (derived, not stored).
//       u32  crc_dd            # crc32c of the dd_region on-disk bytes
//       VInt prx_len           # .prx payload byte length (present iff has_prx)
//
// The reader reconstructs each window's dd_off and prx_off as the running prefix
// sums of dd_disk_len / prx_len over all windows,
// chained across super-blocks; WindowMeta still exposes those offsets, now derived.
//
// Reconstructing win_base / absolute last_docid (READER CONTRACT) is unchanged:
// the writer chains absolute last docids across windows; each row stores the delta
// of its absolute last docid from the previous window, and sb_last_docid seeds
// each block, so super-block binary search then in-block window binary search
// locate the window covering any docid without decoding the .frq blocks.
//
// The trailing crc32c covers only header + super_block_dir; every region carries
// its own crc_dd in the row.
namespace doris::snii::format {

namespace frq_prelude_flags {
inline constexpr uint8_t kHasPrx = 1u << 0;
// Reserved extension point (T18): kSlimRows = 1u << 2 would gate the trimmed
// window-row layout (no stored dd_off/prx_off, conditional uncomp_len)
// as a distinct on-disk path. It is NOT emitted today: the trim folds into the
// single pre-launch v1 encoding (writer/reader symmetric, no dual decode path).
// If a `lifecycle: launched` index appears before this lands, set this bit on the
// slim writer and branch the reader on it instead of unconditionally decoding slim.
} // namespace frq_prelude_flags

// Per-window codec mode bits (win_mode byte).
namespace frq_win_mode {
inline constexpr uint8_t kDdZstd = 1u << 0;
inline constexpr uint8_t kKnownBits = kDdZstd;
} // namespace frq_win_mode

// Absolute, decoded metadata for one window (as the reader exposes it). The dd /
// dd region locators are offsets within the dd-block. dd_off/prx_off are DERIVED by the
// reader as running prefix sums of the disk/prx lengths (they are no longer stored
// per row; see the header layout note) -- these public members are unchanged and
// still populated, just by derivation. The reader derives the dd-block length from
// the last window's dd_off + dd_disk_len.
struct WindowMeta {
    uint32_t last_docid = 0; // absolute last docid in the window
    uint64_t win_base = 0;   // absolute last docid of the previous window (0 for w==0)
    uint32_t doc_count = 0;

    // dd_region locator (within the dd-block).
    bool dd_zstd = false;
    uint64_t dd_off = 0; // DERIVED: running sum of prior windows' dd_disk_len
    uint64_t dd_disk_len = 0;
    uint64_t dd_uncomp_len = 0; // DERIVED == dd_disk_len for raw; stored only when dd_zstd
    uint32_t crc_dd = 0;

    uint64_t prx_off = 0; // valid only when has_prx; DERIVED: running sum of prior prx_len
    uint64_t prx_len = 0; // valid only when has_prx
    // In-memory only (NOT serialized in the prelude row). When false, the dd
    // region decode skips crc verification -- used when these region bytes are
    // covered by an enclosing crc (e.g. an INLINE entry inside its dict block).
    // Windowed/slim POD-ref rows leave this true (their regions carry a crc).
    bool verify_crc = true;
};

// Builder input: one fully-computed WindowMeta per window, in term order, plus the
// super-block grouping factor. The writer fills last_docid (absolute), doc_count,
// the region locators/crcs and prx locator; win_base is derived
// during build (so callers may leave it 0). group_size must be >= 1.
struct FrqPreludeColumns {
    bool has_prx = false;
    uint32_t group_size = 64; // windows per super-block (G)
    std::vector<WindowMeta> windows;
};

// Builds the prelude bytes and appends them to out.
// Returns InvalidArgument when out is null, group_size is 0, or the windows are
// not in non-decreasing last_docid order (a window's absolute last docid must be
// >= the previous window's).
Status build_frq_prelude(const FrqPreludeColumns& cols, ByteSink* out);

// Reads and verifies a prelude buffer, exposing two-level skip access. The reader
// parses the header + super_block_dir on open (verifying the trailing crc) and
// eagerly decodes every window block into owned WindowMeta rows (the prelude is
// small relative to the postings). It does not retain the input.
class FrqPreludeReader {
public:
    // Parses + verifies the prelude. crc mismatch / truncation / inconsistent
    // offsets-or-lengths / oversized counts => kCorruption.
    static Status open(Slice prelude, FrqPreludeReader* out);

    uint32_t n_windows() const { return static_cast<uint32_t>(windows_.size()); }
    uint32_t n_super_blocks() const { return n_super_; }
    bool has_prx() const { return has_prx_; }

    // Total on-disk byte length of the dd-block (== sum of dd_disk_len; the docs-only
    // prefix after the prelude). 0 when there are no windows.
    uint64_t dd_block_len() const { return dd_block_len_; }
    // Returns the absolute WindowMeta for window w. Out-of-range => InvalidArgument.
    Status window(uint32_t w, WindowMeta* out) const;

    // Locates the window covering docid via super-block binary search then window
    // binary search. *found=false (with OK) when docid is past the term's last
    // docid; otherwise *w is the index of the covering window (the first window
    // whose absolute last_docid >= docid).
    Status locate_window(uint32_t docid, bool* found, uint32_t* w) const;

    // Selects, as a monotonic two-pointer cursor, the ascending de-duplicated set of
    // windows covering the ascending `candidates` (each window covering its
    // (win_base, last_docid] span). Writes them to *windows (cleared first). The
    // result is element-for-element identical to calling locate_window per candidate
    // and collapsing equal runs, but uses O(C + N) window last_docid comparisons
    // (C = candidates, N = windows) instead of O(C * group_size). Pure in-memory over
    // the decoded directory; never fails.
    void select_covering_windows(const std::vector<uint32_t>& candidates,
                                 std::vector<uint32_t>* windows) const;

    // Packed absolute last_docid of window w (byte-identical to window(w).last_docid),
    // exposed for the covering-window cursor's contiguous scan and equivalence tests.
    uint32_t window_last_docid(uint32_t w) const {
        DCHECK_LT(w, win_last_docid_.size());
        return win_last_docid_[w];
    }

private:
    bool has_prx_ = false;
    uint32_t group_size_ = 1;
    uint32_t n_super_ = 0;
    uint64_t dd_block_len_ = 0;
    // Absolute last docid at each super-block boundary (size n_super_).
    std::vector<uint64_t> sb_last_docid_;
    // All windows decoded with absolute fields, in term order (size N).
    std::vector<WindowMeta> windows_;
    // Packed copy of each window's absolute last_docid (size N; win_last_docid_[w] ==
    // windows_[w].last_docid). Built in open() so the covering-window cursor scans a
    // contiguous 4B/window array rather than the ~104B WindowMeta rows. In-memory only:
    // never serialized; immutable after open() (same lifetime as windows_).
    std::vector<uint32_t> win_last_docid_;
};

// Pure cursor core (no FrqPreludeReader / IO): selects into *windows the ascending,
// de-duplicated indices of the windows covering the ascending `candidates`, given the
// packed window last_docid array (size n_windows), the super-block last_docid directory
// (size n_super) and group_size. A super-block cursor does boundary jumps while a window
// cursor advances forward only => O(C + N) window comparisons, element-for-element equal
// to per-candidate locate_window + run collapse. *windows is cleared first; n_windows == 0
// yields an empty result. Exposed for isolated equivalence / complexity tests.
void select_covering_windows_cursor(const uint32_t* win_last_docid, uint32_t n_windows,
                                    const uint64_t* sb_last_docid, uint32_t n_super,
                                    uint32_t group_size, const std::vector<uint32_t>& candidates,
                                    std::vector<uint32_t>* windows);

// TEST-ONLY observability seam (mirrors the format dict-block decode counter). Counts the
// window last_docid comparisons performed by select_covering_windows_cursor and by
// locate_window's level-2 scan, so tests can assert the cursor stays O(C + N) and bounded
// by C + N regardless of group_size, while the legacy per-candidate scan grows with G. The
// counter is thread-local: race-free under the shared const reader and free of atomic cost
// in the production cursor loop; reset and read on the thread that ran the cursor.
namespace testing {
uint64_t window_probe_count();
void reset_window_probe_count();
} // namespace testing

} // namespace doris::snii::format
