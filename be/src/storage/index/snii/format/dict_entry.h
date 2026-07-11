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
#include <string>
#include <string_view>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/encoding/byte_sink.h"
#include "storage/index/snii/encoding/byte_source.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/format/frq_pod.h"

// DictEntry —— on-disk encoding/decoding of a dict entry.
//
// Byte layout (see docs/design/SNII-design-spec.source.md "dict entry"
// section):
//   entry_len   varint   # byte length of entry body, allowing reader to skip
//   unknown extensions or fast-skip entries
//   --- entry body begins here, covered by entry_len ---
//   prefix_len  varint   # length of shared prefix with prev_term
//   suffix_len  varint   # number of suffix bytes
//   suffix      u8[]     # suffix bytes that differ from prev_term
//   flags       u8       # bit0 kind / bit1 enc / bit2 has_sb / bit3
//   has_champion(=0) / bit4 offsets_ref(=0) df          varint ttf_delta varint
//   # only when tier>=T2 max_freq    varint   # only when tier>=T2 locator:
//     pod_ref: frq_off_delta varint, frq_len varint,
//              [prelude_len varint, frq_docs_len varint when enc=windowed]
//                  # docs-only prefix [prelude][dd-block]; windowed entries
//                  carry # per-window region metadata in the prelude.
//              [frq_docs_len varint, slim region meta when enc=slim]:
//                  # frq_docs_len == dd region on-disk length; the docs-only
//                  prefix # [frq_off, frq_off+frq_docs_len) a docid-only reader
//                  fetches # without the freq region. win_mode u8 (bit0
//                  dd_zstd, bit1 freq_zstd) dd_uncomp_len varint, crc_dd u32
//                  [freq_uncomp_len varint, crc_freq u32 when tier>=T2]
//                  # The single slim window is [dd_region][freq_region];
//                  dd_disk_len # = frq_docs_len, freq_disk_len = frq_len -
//                  frq_docs_len.
//              [prx_off_delta varint, prx_len varint when tier>=T2]
//     inline:  frq_len varint, frq_bytes u8[],   # frq_bytes =
//     [dd_region][freq_region]
//              slim region meta (as above, sans frq_docs_len which == dd disk
//              len
//                  carried as inline_dd_disk_len varint),
//              [prx_len varint, prx_bytes u8[] when tier>=T2]
//   --- entry body ends ---
//
// CRC verification is performed at the DICT block level (covering block header
// + all entries + anchor offset table), no per-entry CRC to keep slim/inline
// low-frequency terms compact (spec §DICT block line 330/348). tier and
// positions capability are provided by per-index meta (not stored redundantly
// inside entries): when tier>=T2, ttf_delta / max_freq and .prx locator/bytes
// are written.
namespace doris::snii::format {

// Dict entry: inline or pod-ref (two states), self-described length, supports
// intra-block front coding.
struct DictEntry {
    // term key (front coding relative to prev_term is applied during
    // encode/decode; full term stored here).
    std::string term;

    // flags.
    DictEntryKind kind = DictEntryKind::kPodRef;
    DictEntryEnc enc = DictEntryEnc::kSlim;
    bool has_sb = false;

    // term stats.
    uint32_t df = 0;
    uint64_t ttf_delta = 0; // only when tier>=T2 AND the block carries term stats
    // G16-f: false when decoded from a kNoTermStats block (freq-dropped index):
    // ttf_delta/max_freq above are then meaningless defaults, NOT real zeros.
    // Consumers that need them (stats provider / BM25) must check this flag.
    bool term_stats_present = true;
    uint64_t max_freq = 0; // only when tier>=T2

    // pod_ref locator.
    uint64_t frq_off_delta = 0;
    uint64_t frq_len = 0;
    uint64_t prelude_len = 0;   // only when enc=windowed
    uint64_t frq_docs_len = 0;  // pod_ref docs-only prefix length
    uint64_t prx_off_delta = 0; // only when tier>=T2
    uint64_t prx_len = 0;       // only when tier>=T2

    // slim/inline single-window region codecs. The window is
    // [dd_region][freq_region] (no self-describing header). dd_meta drives the
    // docs-only decode; freq_meta the scoring decode (only when tier>=T2). For
    // slim pod_ref dd_meta.disk_len == frq_docs_len; for inline it is stored as
    // inline_dd_disk_len.
    FrqRegionMeta dd_meta;
    FrqRegionMeta freq_meta;         // only when tier>=T2
    uint64_t inline_dd_disk_len = 0; // only for inline: dd region on-disk length

    // inline payload.
    std::vector<uint8_t> frq_bytes; // = [dd_region][freq_region]
    std::vector<uint8_t> prx_bytes; // only when tier>=T2
};

// Encodes an entry into sink (appending) using the layout above, with front
// coding relative to prev_term. tier determines whether optional fields are
// written. term_stats == false (G16-f: freq-dropped indexes, declared by the
// block header's kNoTermStats flag) omits the ttf_delta/max_freq varints --
// they serve only BM25 scoring, dead on an index that dropped freq; df stays.
// Region metadata (freq/prx locators) remains tier-conditioned.
Status encode_dict_entry(const DictEntry& entry, std::string_view prev_term, IndexTier tier,
                         ByteSink* sink, bool term_stats = true);

// Decodes one entry from the current position of src; term is reconstructed
// from prev_term + suffix. Verifies the trailing CRC; out-of-range / CRC
// mismatch / invalid prefix_len all return Corruption. term_stats must match
// the writer's choice (the dict block header flag carries it).
Status decode_dict_entry(ByteSource* src, std::string_view prev_term, IndexTier tier,
                         DictEntry* out, bool term_stats = true);

// Skips one entry using only entry_len (does not parse internal fields or
// verify CRC).
Status skip_dict_entry(ByteSource* src);

// ---- Key-first decode primitives (T07) ----
//
// decode_dict_entry is split into a "key" stage and a "rest" (body) stage so a
// caller scanning many entries can decide on the (front-coded) term key alone
// whether the entry's body is worth materializing. decode_dict_entry below is
// re-expressed as key + rest and stays byte-for-byte identical in output.

// Reads only entry_len + the front-coded term key. On return src is positioned
// at the start of the entry body (the flags byte). out is reset to defaults and
// out->term is reconstructed from prev_term + suffix; no other field is touched.
// *body_start receives the absolute src position of the body (right after the
// entry_len varint) and *entry_total the body byte length (already bounds-checked
// against src->remaining()). Used to drive key-first scans (find_term, prefix
// streaming) that skip non-matching bodies.
Status decode_dict_entry_key(ByteSource* src, std::string_view prev_term, DictEntry* out,
                             size_t* body_start, uint64_t* entry_total);

// Continues from the position decode_dict_entry_key left at: reads
// flags/stats/locator into *out and verifies the body consumed exactly
// entry_total bytes (body_start anchors the consumed count). Increments the
// body-decode counter seam (see dict_entry_body_decode_count) at its top so
// tests can assert how many entry bodies a scan actually materialized.
Status decode_dict_entry_rest(ByteSource* src, IndexTier tier, size_t body_start,
                              uint64_t entry_total, DictEntry* out, bool term_stats = true);

// Skips the remaining body bytes after decode_dict_entry_key, advancing src to
// the next entry without parsing flags/stats/locator. advance = entry_total -
// (src.position() - body_start); the term key already consumed by the key stage
// is accounted for.
Status skip_dict_entry_body(ByteSource* src, size_t body_start, uint64_t entry_total);

// Test-only instrumentation seam: dict_entry_body_decode_count() returns a
// process-global count of decode_dict_entry_rest calls -- i.e. how many entry
// bodies (flags/stats/locator + any inline byte copies) a scan materialized.
// Key-first find_term / prefix streaming drive this toward the number of
// produced hits instead of the number of entries walked. Counters use relaxed
// atomics; reset between tests.
uint64_t dict_entry_body_decode_count();
void reset_dict_entry_counters();

} // namespace doris::snii::format
