#include "snii/format/prx_pod.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <span>
#include <vector>

#include "snii/common/slice.h"
#include "snii/encoding/byte_source.h"
#include "snii/encoding/crc32c.h"
#include "snii/encoding/pfor.h"
#include "snii/encoding/zstd_codec.h"
#include "snii/format/format_constants.h"

namespace snii::format {
namespace {

// Auto-compression threshold: use raw when payload is smaller than this (zstd
// gain is negligible and metadata overhead is relatively large).
inline constexpr size_t kAutoZstdMinBytes = 512;
// Default zstd level in auto mode.
inline constexpr int kDefaultZstdLevel = 3;
// Maximum decompressed byte size for a single .prx window. Guards against a
// corrupted uncomp_len read from S3 inflated to a huge value: sanity-check
// before allocating/decompressing to avoid GB-scale allocations. Windows are
// 256-doc aligned and normally far below this limit.
inline constexpr uint32_t kMaxWindowUncompBytes = 256u * 1024 * 1024;
// Anti-DoS cap on position count decoded from a single window before
// allocation.
inline constexpr uint32_t kMaxWindowPositions = 1u << 26; // 64M positions/window
// Anti-DoS cap on doc count decoded from a single window before allocation. A
// corrupt doc_count is otherwise fed straight to assign()/reserve() ->
// bad_alloc.
inline constexpr uint32_t kMaxWindowDocs = 1u << 24; // 16M docs/window

// Writer-side precondition for the FLAT builders: the per-doc partition `freqs`
// must address exactly the positions present in `flat`. If sum(freqs) overruns
// flat.size() a (positions_flat, freqs) mismatch would index flat[off+i] past
// the span end -- an out-of-bounds read on caller-supplied data. Reject it as
// InvalidArgument BEFORE any indexing so the bug surfaces as a clean Status,
// never UB. (sum < size leaves trailing positions unused, which is also a
// writer bug, so we require exact equality.) Uint64 accumulation cannot
// overflow for uint32 freqs.
Status check_flat_partition(std::span<const uint32_t> flat, std::span<const uint32_t> freqs) {
    uint64_t sum = 0;
    for (uint32_t fc : freqs) sum += fc;
    if (sum != flat.size()) {
        return Status::InvalidArgument("prx: sum(freqs) does not match positions_flat size");
    }
    return Status::OK();
}

// Encode per-doc position lists into a self-describing plain payload (doc_count
// + per-doc delta stream).
Status encode_payload(std::span<const std::vector<uint32_t>> per_doc, ByteSink* out) {
    out->put_varint32(static_cast<uint32_t>(per_doc.size()));
    for (const auto& doc : per_doc) {
        out->put_varint32(static_cast<uint32_t>(doc.size()));
        uint32_t prev = 0;
        for (size_t i = 0; i < doc.size(); ++i) {
            uint32_t pos = doc[i];
            if (i > 0 && pos < prev) {
                return Status::InvalidArgument("prx: positions within a doc must be ascending");
            }
            out->put_varint32(i == 0 ? pos : pos - prev);
            prev = pos;
        }
    }
    return Status::OK();
}

// FLAT-positions encoder: identical wire output to encode_payload above, but
// reads positions from a single flat span partitioned per-doc by `freqs` (doc d
// owns the next freqs[d] entries). This avoids materializing a
// vector-of-vectors for the window; freqs.size() is the doc count and
// sum(freqs) == flat.size().
Status encode_payload_flat(std::span<const uint32_t> flat, std::span<const uint32_t> freqs,
                           ByteSink* out) {
    SNII_RETURN_IF_ERROR(check_flat_partition(flat, freqs));
    out->put_varint32(static_cast<uint32_t>(freqs.size()));
    size_t off = 0;
    for (uint32_t fc : freqs) {
        out->put_varint32(fc);
        uint32_t prev = 0;
        for (uint32_t i = 0; i < fc; ++i) {
            const uint32_t pos = flat[off + i];
            if (i > 0 && pos < prev) {
                return Status::InvalidArgument("prx: positions within a doc must be ascending");
            }
            out->put_varint32(i == 0 ? pos : pos - prev);
            prev = pos;
        }
        off += fc;
    }
    return Status::OK();
}

// Encode a uint32 array into PFOR runs of kFrqBaseUnit (256) elements each. The
// run count is derived by the decoder from the total length, so it is not
// stored.
void encode_pfor_runs(std::span<const uint32_t> values, ByteSink* out) {
    const size_t n = values.size();
    for (size_t off = 0; off < n; off += kFrqBaseUnit) {
        const size_t run = (n - off < kFrqBaseUnit) ? (n - off) : kFrqBaseUnit;
        pfor_encode(values.data() + off, run, out);
    }
}

// Decode n uint32 values (multiple PFOR runs of kFrqBaseUnit each) into out.
Status decode_pfor_runs(ByteSource* src, size_t n, std::vector<uint32_t>* out) {
    out->assign(n, 0);
    for (size_t off = 0; off < n; off += kFrqBaseUnit) {
        const size_t run = (n - off < kFrqBaseUnit) ? (n - off) : kFrqBaseUnit;
        SNII_RETURN_IF_ERROR(pfor_decode(src, run, out->data() + off));
    }
    return Status::OK();
}

// PFOR window payload (self-describing; no entropy coding):
//   VInt doc_count
//   VInt total_pos             # sum of all pos_counts
//   PFOR_runs(pos_counts)      # doc_count values (bit-packed; mostly 1 -> ~1
//   bit) PFOR_runs(position_deltas) # total_pos deltas, flat across docs (first
//   per
//                              #   doc absolute, rest delta-within-doc)
// Bit-packing the per-doc pos_counts (vs one varint each) is the size win: in a
// uniform corpus most docs have freq 1, so the count column packs to ~1
// bit/doc. Builds the payload from a flat positions span partitioned per-doc by
// `freqs`.
Status encode_pfor_payload_flat(std::span<const uint32_t> flat, std::span<const uint32_t> freqs,
                                ByteSink* out) {
    SNII_RETURN_IF_ERROR(check_flat_partition(flat, freqs));
    out->put_varint32(static_cast<uint32_t>(freqs.size()));
    out->put_varint32(static_cast<uint32_t>(flat.size()));
    encode_pfor_runs(freqs, out);
    std::vector<uint32_t> deltas;
    deltas.reserve(flat.size());
    size_t off = 0;
    for (uint32_t fc : freqs) {
        uint32_t prev = 0;
        for (uint32_t i = 0; i < fc; ++i) {
            const uint32_t pos = flat[off + i];
            if (i > 0 && pos < prev) {
                return Status::InvalidArgument("prx: positions within a doc must be ascending");
            }
            deltas.push_back(i == 0 ? pos : pos - prev);
            prev = pos;
        }
        off += fc;
    }
    encode_pfor_runs(deltas, out);
    return Status::OK();
}

// Builds the PFOR payload from per-doc lists (delegates through a flat view).
Status encode_pfor_payload(std::span<const std::vector<uint32_t>> per_doc, ByteSink* out) {
    std::vector<uint32_t> flat, freqs;
    freqs.reserve(per_doc.size());
    for (const auto& doc : per_doc) {
        freqs.push_back(static_cast<uint32_t>(doc.size()));
        flat.insert(flat.end(), doc.begin(), doc.end());
    }
    return encode_pfor_payload_flat(flat, freqs, out);
}

// Decode per-doc position lists from a PFOR payload.
Status decode_pfor_payload(Slice plain, std::vector<std::vector<uint32_t>>* out) {
    ByteSource src(plain);
    uint32_t doc_count = 0, total_pos = 0;
    SNII_RETURN_IF_ERROR(src.get_varint32(&doc_count));
    SNII_RETURN_IF_ERROR(src.get_varint32(&total_pos));
    if (total_pos > kMaxWindowPositions) {
        return Status::Corruption("prx: position count exceeds sane cap");
    }
    if (doc_count > kMaxWindowDocs) {
        return Status::Corruption("prx: doc count exceeds sane cap");
    }
    std::vector<uint32_t> pos_counts;
    SNII_RETURN_IF_ERROR(decode_pfor_runs(&src, doc_count, &pos_counts));
    uint64_t sum = 0;
    for (uint32_t d = 0; d < doc_count; ++d) sum += pos_counts[d];
    if (sum != total_pos) {
        return Status::Corruption("prx: pos_count sum mismatch");
    }
    std::vector<uint32_t> deltas;
    SNII_RETURN_IF_ERROR(decode_pfor_runs(&src, total_pos, &deltas));
    out->clear();
    out->reserve(doc_count);
    size_t off = 0;
    for (uint32_t d = 0; d < doc_count; ++d) {
        std::vector<uint32_t> doc;
        doc.reserve(pos_counts[d]);
        uint32_t prev = 0;
        for (uint32_t i = 0; i < pos_counts[d]; ++i) {
            prev = (i == 0) ? deltas[off + i] : prev + deltas[off + i];
            doc.push_back(prev);
        }
        off += pos_counts[d];
        out->push_back(std::move(doc));
    }
    if (!src.eof()) return Status::Corruption("prx: trailing bytes after pfor payload");
    return Status::OK();
}

// Writes a PFOR window: codec=pfor, payload, crc(header+payload).
void write_pfor(Slice payload, ByteSink* sink) {
    ByteSink framed;
    framed.put_u8(static_cast<uint8_t>(PrxCodec::kPfor));
    framed.put_varint32(static_cast<uint32_t>(payload.size()));
    framed.put_bytes(payload);
    sink->put_bytes(framed.view());
    sink->put_fixed32(crc32c(framed.view()));
}

// Decode per-doc position lists from a plain payload.
Status decode_payload(Slice plain, std::vector<std::vector<uint32_t>>* out) {
    ByteSource src(plain);
    uint32_t doc_count = 0;
    SNII_RETURN_IF_ERROR(src.get_varint32(&doc_count));
    if (doc_count > kMaxWindowDocs) {
        return Status::Corruption("prx: doc count exceeds sane cap");
    }
    out->clear();
    out->reserve(doc_count);
    for (uint32_t d = 0; d < doc_count; ++d) {
        uint32_t pos_count = 0;
        SNII_RETURN_IF_ERROR(src.get_varint32(&pos_count));
        std::vector<uint32_t> doc;
        doc.reserve(pos_count);
        uint32_t prev = 0;
        for (uint32_t i = 0; i < pos_count; ++i) {
            uint32_t delta = 0;
            SNII_RETURN_IF_ERROR(src.get_varint32(&delta));
            prev = (i == 0) ? delta : prev + delta;
            doc.push_back(prev);
        }
        out->push_back(std::move(doc));
    }
    if (!src.eof()) return Status::Corruption("prx: trailing bytes after payload");
    return Status::OK();
}

// CSR decode of a PFOR payload: all docs' positions into one flat buffer +
// per-doc offsets, with NO per-doc std::vector allocation. `pos_off` has
// doc_count+1 entries (pos_off[0]==0); doc d's positions are
// pos_flat[pos_off[d] .. pos_off[d+1]).
Status decode_pfor_payload_csr(Slice plain, std::vector<uint32_t>* pos_flat,
                               std::vector<uint32_t>* pos_off) {
    ByteSource src(plain);
    uint32_t doc_count = 0, total_pos = 0;
    SNII_RETURN_IF_ERROR(src.get_varint32(&doc_count));
    SNII_RETURN_IF_ERROR(src.get_varint32(&total_pos));
    if (total_pos > kMaxWindowPositions) {
        return Status::Corruption("prx: position count exceeds sane cap");
    }
    if (doc_count > kMaxWindowDocs) {
        return Status::Corruption("prx: doc count exceeds sane cap");
    }
    pos_off->clear();
    pos_off->reserve(static_cast<size_t>(doc_count) + 1);
    SNII_RETURN_IF_ERROR(decode_pfor_runs(&src, doc_count, pos_off));
    uint64_t sum = 0;
    for (uint32_t d = 0; d < doc_count; ++d) sum += (*pos_off)[d];
    if (sum != total_pos) return Status::Corruption("prx: pos_count sum mismatch");
    pos_flat->reserve(total_pos);
    SNII_RETURN_IF_ERROR(decode_pfor_runs(&src, total_pos, pos_flat));
    size_t off = 0;
    uint32_t next_off = 0;
    for (uint32_t d = 0; d < doc_count; ++d) {
        const uint32_t pos_count = (*pos_off)[d];
        (*pos_off)[d] = next_off;
        uint32_t prev = 0;
        for (uint32_t i = 0; i < pos_count; ++i) {
            uint32_t& value = (*pos_flat)[off + i];
            prev = (i == 0) ? value : prev + value;
            value = prev;
        }
        off += pos_count;
        next_off += pos_count;
    }
    pos_off->push_back(next_off);
    if (!src.eof()) return Status::Corruption("prx: trailing bytes after pfor payload");
    return Status::OK();
}

Status validate_doc_ordinals(std::span<const uint32_t> doc_ordinals, uint32_t doc_count) {
    uint32_t prev = 0;
    for (size_t i = 0; i < doc_ordinals.size(); ++i) {
        const uint32_t doc = doc_ordinals[i];
        if (doc >= doc_count) {
            return Status::Corruption("prx: selected doc ordinal out of range");
        }
        if (i != 0 && doc <= prev) {
            return Status::InvalidArgument("prx: selected doc ordinals must be strictly ascending");
        }
        prev = doc;
    }
    return Status::OK();
}

struct SelectedRange {
    SelectedRange(uint32_t begin_, uint32_t end_, uint32_t out_begin_)
            : begin(begin_), end(end_), out_begin(out_begin_) {}

    uint32_t begin;
    uint32_t end;
    uint32_t out_begin;
};

uint32_t count_covered_pfor_runs(std::span<const SelectedRange> selected, uint32_t total_pos) {
    if (selected.empty() || total_pos == 0) {
        return 0;
    }
    uint32_t runs = 0;
    uint32_t next_run = 0;
    for (const SelectedRange& range : selected) {
        if (range.begin == range.end) {
            continue;
        }
        const uint32_t first_run = range.begin / kFrqBaseUnit;
        const uint32_t last_run = (range.end - 1) / kFrqBaseUnit;
        const uint32_t counted_first = std::max(first_run, next_run);
        if (counted_first <= last_run) {
            runs += last_run - counted_first + 1;
            next_run = last_run + 1;
        }
    }
    return runs;
}

bool should_decode_full_prx_positions(std::span<const SelectedRange> selected,
                                      uint32_t selected_pos_count, uint32_t total_pos) {
    if (selected.empty() || total_pos == 0) {
        return false;
    }
    if (selected_pos_count * 2 >= total_pos) {
        return true;
    }
    const uint32_t total_runs = (total_pos + kFrqBaseUnit - 1) / kFrqBaseUnit;
    const uint32_t covered_runs = count_covered_pfor_runs(selected, total_pos);
    return covered_runs * 4 >= total_runs * 3;
}

void compact_selected_pfor_positions(std::span<const SelectedRange> selected,
                                     std::vector<uint32_t>& pos_flat,
                                     std::vector<uint32_t>& pos_off) {
    size_t write_off = 0;
    pos_off.clear();
    pos_off.reserve(selected.size() + 1);
    pos_off.push_back(0);
    for (const SelectedRange& range : selected) {
        const uint32_t count = range.end - range.begin;
        if (count == 1) {
            pos_flat[write_off++] = pos_flat[range.begin];
            pos_off.push_back(static_cast<uint32_t>(write_off));
            continue;
        }
        uint32_t prev = 0;
        for (uint32_t i = 0; i < count; ++i) {
            const uint32_t delta = pos_flat[range.begin + i];
            prev = (i == 0) ? delta : prev + delta;
            pos_flat[write_off++] = prev;
        }
        pos_off.push_back(static_cast<uint32_t>(write_off));
    }
    pos_flat.resize(write_off);
}

Status decode_selected_pfor_count_ranges(ByteSource* src, uint32_t doc_count,
                                         std::span<const uint32_t> doc_ordinals,
                                         std::vector<SelectedRange>& selected,
                                         std::vector<uint32_t>& pos_off, uint64_t* total_pos_count,
                                         uint32_t* selected_pos_count) {
    selected.clear();
    selected.reserve(doc_ordinals.size());
    pos_off.clear();
    pos_off.reserve(doc_ordinals.size() + 1);
    pos_off.push_back(0);

    *selected_pos_count = 0;
    uint32_t delta_begin = 0;
    size_t next_doc = 0;
    *total_pos_count = 0;
    std::array<uint32_t, kFrqBaseUnit> run_buf {};
    for (uint32_t run_begin = 0; run_begin < doc_count; run_begin += kFrqBaseUnit) {
        const uint32_t run_len = std::min<uint32_t>(kFrqBaseUnit, doc_count - run_begin);
        SNII_RETURN_IF_ERROR(pfor_decode(src, run_len, run_buf.data()));
        for (uint32_t i = 0; i < run_len; ++i) {
            const uint32_t d = run_begin + i;
            const uint32_t count = run_buf[i];
            *total_pos_count += count;
            if (next_doc < doc_ordinals.size() && doc_ordinals[next_doc] == d) {
                selected.emplace_back(delta_begin, delta_begin + count, *selected_pos_count);
                *selected_pos_count += count;
                pos_off.push_back(*selected_pos_count);
                ++next_doc;
            }
            delta_begin += count;
        }
    }
    if (next_doc != doc_ordinals.size()) {
        return Status::Corruption("prx: selected doc ordinal was not decoded");
    }
    return Status::OK();
}

Status decode_sparse_selected_pfor_positions(ByteSource* src, uint32_t total_pos,
                                             std::span<const SelectedRange> selected,
                                             std::span<uint32_t> pos_flat) {
    std::array<uint32_t, kFrqBaseUnit> run_buf {};
    size_t range_idx = 0;
    for (uint32_t run_begin = 0; run_begin < total_pos; run_begin += kFrqBaseUnit) {
        const uint32_t run_len = std::min<uint32_t>(kFrqBaseUnit, total_pos - run_begin);
        const uint32_t run_end = run_begin + run_len;
        while (range_idx < selected.size() && selected[range_idx].end <= run_begin) {
            ++range_idx;
        }
        if (range_idx == selected.size() || selected[range_idx].begin >= run_end) {
            SNII_RETURN_IF_ERROR(pfor_skip(src, run_len));
            continue;
        }

        SNII_RETURN_IF_ERROR(pfor_decode(src, run_len, run_buf.data()));
        for (size_t ri = range_idx; ri < selected.size() && selected[ri].begin < run_end; ++ri) {
            const SelectedRange& range = selected[ri];
            const uint32_t copy_begin = std::max(range.begin, run_begin);
            const uint32_t copy_end = std::min(range.end, run_end);
            const uint32_t dst_begin = range.out_begin + copy_begin - range.begin;
            std::copy_n(run_buf.data() + copy_begin - run_begin, copy_end - copy_begin,
                        pos_flat.data() + dst_begin);
        }
    }
    return Status::OK();
}

void restore_selected_position_deltas(const std::vector<uint32_t>& pos_off,
                                      std::span<uint32_t> pos_flat) {
    for (size_t i = 0; i + 1 < pos_off.size(); ++i) {
        uint32_t prev = 0;
        for (uint32_t off = pos_off[i]; off < pos_off[i + 1]; ++off) {
            uint32_t& value = pos_flat[off];
            prev = (off == pos_off[i]) ? value : prev + value;
            value = prev;
        }
    }
}

Status decode_pfor_payload_csr_selective(Slice plain, std::span<const uint32_t> doc_ordinals,
                                         std::vector<uint32_t>* pos_flat,
                                         std::vector<uint32_t>* pos_off) {
    ByteSource src(plain);
    uint32_t doc_count = 0, total_pos = 0;
    SNII_RETURN_IF_ERROR(src.get_varint32(&doc_count));
    SNII_RETURN_IF_ERROR(src.get_varint32(&total_pos));
    if (total_pos > kMaxWindowPositions) {
        return Status::Corruption("prx: position count exceeds sane cap");
    }
    if (doc_count > kMaxWindowDocs) {
        return Status::Corruption("prx: doc count exceeds sane cap");
    }
    SNII_RETURN_IF_ERROR(validate_doc_ordinals(doc_ordinals, doc_count));

    pos_flat->clear();

    std::vector<SelectedRange> selected;
    uint64_t sum = 0;
    uint32_t selected_pos_count = 0;
    SNII_RETURN_IF_ERROR(decode_selected_pfor_count_ranges(&src, doc_count, doc_ordinals, selected,
                                                           *pos_off, &sum, &selected_pos_count));
    if (sum != total_pos) {
        return Status::Corruption("prx: pos_count sum mismatch");
    }

    if (should_decode_full_prx_positions(selected, selected_pos_count, total_pos)) {
        SNII_RETURN_IF_ERROR(decode_pfor_runs(&src, total_pos, pos_flat));
        compact_selected_pfor_positions(selected, *pos_flat, *pos_off);
        if (!src.eof()) {
            return Status::Corruption("prx: trailing bytes after pfor payload");
        }
        return Status::OK();
    }

    pos_flat->resize(selected_pos_count);
    SNII_RETURN_IF_ERROR(decode_sparse_selected_pfor_positions(
            &src, total_pos, selected, std::span<uint32_t>(pos_flat->data(), pos_flat->size())));

    restore_selected_position_deltas(*pos_off,
                                     std::span<uint32_t>(pos_flat->data(), pos_flat->size()));
    if (!src.eof()) {
        return Status::Corruption("prx: trailing bytes after pfor payload");
    }
    return Status::OK();
}

// CSR decode of a plain (raw) payload. See decode_pfor_payload_csr.
Status decode_payload_csr(Slice plain, std::vector<uint32_t>* pos_flat,
                          std::vector<uint32_t>* pos_off) {
    ByteSource src(plain);
    uint32_t doc_count = 0;
    SNII_RETURN_IF_ERROR(src.get_varint32(&doc_count));
    if (doc_count > kMaxWindowDocs) {
        return Status::Corruption("prx: doc count exceeds sane cap");
    }
    pos_flat->clear();
    pos_off->clear();
    pos_off->reserve(static_cast<size_t>(doc_count) + 1);
    pos_off->push_back(0);
    uint64_t total_pos = 0;
    for (uint32_t d = 0; d < doc_count; ++d) {
        uint32_t pos_count = 0;
        SNII_RETURN_IF_ERROR(src.get_varint32(&pos_count));
        total_pos += pos_count;
        if (total_pos > kMaxWindowPositions) {
            return Status::Corruption("prx: position count exceeds sane cap");
        }
        uint32_t prev = 0;
        for (uint32_t i = 0; i < pos_count; ++i) {
            uint32_t delta = 0;
            SNII_RETURN_IF_ERROR(src.get_varint32(&delta));
            prev = (i == 0) ? delta : prev + delta;
            pos_flat->push_back(prev);
        }
        pos_off->push_back(static_cast<uint32_t>(pos_flat->size()));
    }
    if (!src.eof()) return Status::Corruption("prx: trailing bytes after payload");
    return Status::OK();
}

Status decode_payload_csr_selective(Slice plain, std::span<const uint32_t> doc_ordinals,
                                    std::vector<uint32_t>* pos_flat,
                                    std::vector<uint32_t>* pos_off) {
    ByteSource src(plain);
    uint32_t doc_count = 0;
    SNII_RETURN_IF_ERROR(src.get_varint32(&doc_count));
    if (doc_count > kMaxWindowDocs) {
        return Status::Corruption("prx: doc count exceeds sane cap");
    }
    SNII_RETURN_IF_ERROR(validate_doc_ordinals(doc_ordinals, doc_count));
    pos_flat->clear();
    pos_off->clear();
    pos_off->reserve(doc_ordinals.size() + 1);
    pos_off->push_back(0);
    size_t next_doc = 0;
    uint64_t total_pos = 0;
    for (uint32_t d = 0; d < doc_count; ++d) {
        uint32_t pos_count = 0;
        SNII_RETURN_IF_ERROR(src.get_varint32(&pos_count));
        total_pos += pos_count;
        if (total_pos > kMaxWindowPositions) {
            return Status::Corruption("prx: position count exceeds sane cap");
        }
        const bool selected = next_doc < doc_ordinals.size() && doc_ordinals[next_doc] == d;
        uint32_t prev = 0;
        for (uint32_t i = 0; i < pos_count; ++i) {
            uint32_t delta = 0;
            SNII_RETURN_IF_ERROR(src.get_varint32(&delta));
            if (!selected) continue;
            prev = (i == 0) ? delta : prev + delta;
            pos_flat->push_back(prev);
        }
        if (selected) {
            pos_off->push_back(static_cast<uint32_t>(pos_flat->size()));
            ++next_doc;
        }
    }
    if (!src.eof()) return Status::Corruption("prx: trailing bytes after payload");
    return Status::OK();
}

// Decision: given level and plain length, determine whether to compress.
bool should_compress(int level, size_t plain_len) {
    if (level == 0) return false;          // force raw
    if (level > 0) return true;            // force zstd
    return plain_len >= kAutoZstdMinBytes; // auto
}

// Write a raw window: codec=raw, uncomp_len, crc(header+payload), payload.
void write_raw(Slice plain, ByteSink* sink) {
    ByteSink framed;
    framed.put_u8(static_cast<uint8_t>(PrxCodec::kRaw));
    framed.put_varint32(static_cast<uint32_t>(plain.size()));
    framed.put_bytes(plain);
    sink->put_bytes(framed.view());
    sink->put_fixed32(crc32c(framed.view()));
}

// Write a zstd window: codec=zstd, uncomp_len, comp_len, crc(header+payload),
// payload.
Status write_zstd(Slice plain, int level, ByteSink* sink) {
    std::vector<uint8_t> comp;
    SNII_RETURN_IF_ERROR(zstd_compress(plain, level > 0 ? level : kDefaultZstdLevel, &comp));
    ByteSink framed;
    framed.put_u8(static_cast<uint8_t>(PrxCodec::kZstd));
    framed.put_varint32(static_cast<uint32_t>(plain.size()));
    framed.put_varint32(static_cast<uint32_t>(comp.size()));
    framed.put_bytes(Slice(comp));
    sink->put_bytes(framed.view());
    sink->put_fixed32(crc32c(framed.view()));
    return Status::OK();
}

// Read header + payload, verify crc in retrospect, and return the payload view
// and uncomp_len to the caller.
Status read_framed(ByteSource* src, uint8_t* codec, uint32_t* uncomp_len, Slice* payload) {
    size_t start = src->position();
    SNII_RETURN_IF_ERROR(src->get_u8(codec));
    if (*codec != static_cast<uint8_t>(PrxCodec::kRaw) &&
        *codec != static_cast<uint8_t>(PrxCodec::kZstd) &&
        *codec != static_cast<uint8_t>(PrxCodec::kPfor)) {
        return Status::Corruption("prx: unknown codec");
    }
    SNII_RETURN_IF_ERROR(src->get_varint32(uncomp_len));
    if (*uncomp_len > kMaxWindowUncompBytes) {
        return Status::Corruption("prx: uncomp_len exceeds sane window cap");
    }
    size_t payload_len = *uncomp_len;
    if (*codec == static_cast<uint8_t>(PrxCodec::kZstd)) {
        uint32_t comp_len = 0;
        SNII_RETURN_IF_ERROR(src->get_varint32(&comp_len));
        payload_len = comp_len;
    }
    SNII_RETURN_IF_ERROR(src->get_bytes(payload_len, payload));
    size_t framed_len = src->position() - start;
    uint32_t stored = 0;
    SNII_RETURN_IF_ERROR(src->get_fixed32(&stored));
    if (crc32c(src->slice_from(start, framed_len)) != stored) {
        return Status::Corruption("prx: window crc mismatch");
    }
    return Status::OK();
}

} // namespace

Status build_prx_window(std::span<const std::vector<uint32_t>> per_doc_positions,
                        int zstd_level_or_negative_for_auto, ByteSink* sink) {
    if (sink == nullptr) return Status::InvalidArgument("prx: null sink");
    // Forced legacy codecs (level 0 = raw varint, level > 0 = zstd) are kept so
    // the test/legacy paths still exercise them; the auto path (< 0) now emits
    // PFOR bit-packed deltas -- no entropy coding, far cheaper build CPU than
    // zstd-3.
    if (zstd_level_or_negative_for_auto >= 0) {
        ByteSink plain;
        SNII_RETURN_IF_ERROR(encode_payload(per_doc_positions, &plain));
        Slice plain_view = plain.view();
        if (!should_compress(zstd_level_or_negative_for_auto, plain_view.size())) {
            write_raw(plain_view, sink);
            return Status::OK();
        }
        return write_zstd(plain_view, zstd_level_or_negative_for_auto, sink);
    }
    ByteSink payload;
    SNII_RETURN_IF_ERROR(encode_pfor_payload(per_doc_positions, &payload));
    write_pfor(payload.view(), sink);
    return Status::OK();
}

Status build_prx_window_flat(std::span<const uint32_t> positions_flat,
                             std::span<const uint32_t> freqs, int zstd_level_or_negative_for_auto,
                             ByteSink* sink) {
    if (sink == nullptr) return Status::InvalidArgument("prx: null sink");
    if (zstd_level_or_negative_for_auto >= 0) {
        ByteSink plain;
        SNII_RETURN_IF_ERROR(encode_payload_flat(positions_flat, freqs, &plain));
        Slice plain_view = plain.view();
        if (!should_compress(zstd_level_or_negative_for_auto, plain_view.size())) {
            write_raw(plain_view, sink);
            return Status::OK();
        }
        return write_zstd(plain_view, zstd_level_or_negative_for_auto, sink);
    }
    ByteSink payload;
    SNII_RETURN_IF_ERROR(encode_pfor_payload_flat(positions_flat, freqs, &payload));
    write_pfor(payload.view(), sink);
    return Status::OK();
}

Status read_prx_window(ByteSource* source, std::vector<std::vector<uint32_t>>* per_doc_positions) {
    if (source == nullptr || per_doc_positions == nullptr) {
        return Status::InvalidArgument("prx: null arg");
    }
    uint8_t codec = 0;
    uint32_t uncomp_len = 0;
    Slice payload;
    SNII_RETURN_IF_ERROR(read_framed(source, &codec, &uncomp_len, &payload));
    if (codec == static_cast<uint8_t>(PrxCodec::kPfor)) {
        return decode_pfor_payload(payload, per_doc_positions);
    }
    if (codec == static_cast<uint8_t>(PrxCodec::kRaw)) {
        return decode_payload(payload, per_doc_positions);
    }
    std::vector<uint8_t> plain;
    SNII_RETURN_IF_ERROR(zstd_decompress(payload, uncomp_len, &plain));
    return decode_payload(Slice(plain), per_doc_positions);
}

Status read_prx_window_csr(ByteSource* source, std::vector<uint32_t>* pos_flat,
                           std::vector<uint32_t>* pos_off) {
    if (source == nullptr || pos_flat == nullptr || pos_off == nullptr) {
        return Status::InvalidArgument("prx: null arg");
    }
    uint8_t codec = 0;
    uint32_t uncomp_len = 0;
    Slice payload;
    SNII_RETURN_IF_ERROR(read_framed(source, &codec, &uncomp_len, &payload));
    if (codec == static_cast<uint8_t>(PrxCodec::kPfor)) {
        return decode_pfor_payload_csr(payload, pos_flat, pos_off);
    }
    if (codec == static_cast<uint8_t>(PrxCodec::kRaw)) {
        return decode_payload_csr(payload, pos_flat, pos_off);
    }
    std::vector<uint8_t> plain;
    SNII_RETURN_IF_ERROR(zstd_decompress(payload, uncomp_len, &plain));
    return decode_payload_csr(Slice(plain), pos_flat, pos_off);
}

Status read_prx_window_csr_selective(ByteSource* source, std::span<const uint32_t> doc_ordinals,
                                     std::vector<uint32_t>* pos_flat,
                                     std::vector<uint32_t>* pos_off) {
    if (source == nullptr || pos_flat == nullptr || pos_off == nullptr) {
        return Status::InvalidArgument("prx: null arg");
    }
    uint8_t codec = 0;
    uint32_t uncomp_len = 0;
    Slice payload;
    SNII_RETURN_IF_ERROR(read_framed(source, &codec, &uncomp_len, &payload));
    if (codec == static_cast<uint8_t>(PrxCodec::kPfor)) {
        return decode_pfor_payload_csr_selective(payload, doc_ordinals, pos_flat, pos_off);
    }
    if (codec == static_cast<uint8_t>(PrxCodec::kRaw)) {
        return decode_payload_csr_selective(payload, doc_ordinals, pos_flat, pos_off);
    }
    std::vector<uint8_t> plain;
    SNII_RETURN_IF_ERROR(zstd_decompress(payload, uncomp_len, &plain));
    return decode_payload_csr_selective(Slice(plain), doc_ordinals, pos_flat, pos_off);
}

} // namespace snii::format
