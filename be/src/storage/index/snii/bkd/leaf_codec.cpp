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

#include "storage/index/snii/bkd/leaf_codec.h"

#include <cstring>
#include <limits>
#include <span>
#include <string_view>
#include <vector>

#include "common/check.h"
#include "storage/index/snii/bkd/bkd_format.h"
#include "storage/index/snii/encoding/byte_source.h"
#include "storage/index/snii/encoding/pfor.h"
#include "storage/index/snii/encoding/varint.h"

// The encode half and the decode half below share NOTHING but the constants in
// bkd_format.h (design 4): no helper, no struct, no constant is reused across the
// divider. That is the whole point -- the old docids_writer declared both
// directions on one type, which is how the writer TU came to include the entire
// read side.
namespace doris::snii::bkd {

// ===========================================================================
// Encode
// ===========================================================================
namespace {

static_assert(kPointDocIdBytes == 4, "the build-time record tail is a 4-byte big-endian doc id");

// The builder's own point buffer (design 6.2), addressed by point index: fixed
// width [value: bytes_per_dim][doc_id: 4 big-endian] records, sorted by the memcmp
// of the whole record, which IS (value, doc_id) order.
struct PointArray {
    const uint8_t* records = nullptr;
    size_t record_size = 0;
    uint32_t bytes_per_dim = 0;
    size_t point_count = 0;

    const uint8_t* value(size_t point) const { return records + point * record_size; }

    // The big-endian doc id tail. Big-endian is what makes the whole-record memcmp
    // equal (value, doc_id) order, so it is assembled by hand here rather than read
    // as a native integer.
    uint32_t doc_id(size_t point) const {
        const uint8_t* tail = records + point * record_size + bytes_per_dim;
        return (static_cast<uint32_t>(tail[0]) << 24) | (static_cast<uint32_t>(tail[1]) << 16) |
               (static_cast<uint32_t>(tail[2]) << 8) | static_cast<uint32_t>(tail[3]);
    }
};

// Index of the first point of every maximal run of equal values. The input is
// sorted, so one memcmp pass finds them all.
std::vector<uint32_t> scan_runs(const PointArray& points) {
    std::vector<uint32_t> run_starts;
    run_starts.push_back(0);
    for (size_t point = 1; point < points.point_count; ++point) {
        const int order =
                std::memcmp(points.value(point - 1), points.value(point), points.bytes_per_dim);
        // Per-point, hence DCHECK. Equal adjacent records are legal (an array
        // column repeating one value inside one row); descending ones mean the
        // caller handed over an unsorted buffer.
        DCHECK_LE(order, 0);
        if (order != 0) {
            run_starts.push_back(static_cast<uint32_t>(point));
        }
    }
    return run_starts;
}

uint32_t run_length_at(const std::vector<uint32_t>& run_starts, size_t point_count, size_t run) {
    const size_t end = (run + 1 < run_starts.size()) ? run_starts[run + 1] : point_count;
    return static_cast<uint32_t>(end - run_starts[run]);
}

// Bytes the first and last value share. The points are sorted, so a prefix shared
// by those two is shared by every value in between.
uint32_t common_prefix_length(const PointArray& points) {
    const uint8_t* first = points.value(0);
    const uint8_t* last = points.value(points.point_count - 1);
    uint32_t shared = 0;
    while (shared < points.bytes_per_dim && first[shared] == last[shared]) {
        ++shared;
    }
    return shared;
}

LeafValueMode choose_value_mode(const PointArray& points, const std::vector<uint32_t>& run_starts,
                                uint32_t suffix_width) {
    if (run_starts.size() == 1) {
        // One run is one value, so the shared prefix is the whole value and there
        // is nothing left to store per point.
        DORIS_CHECK_EQ(suffix_width, 0U);
        return LeafValueMode::kAllEqual;
    }
    // Both candidate value areas are sized EXACTLY rather than estimated -- the
    // decision only has to be right for this one leaf, and both sizes are already
    // known here.
    uint64_t rle_bytes = varint_len(run_starts.size());
    for (size_t run = 0; run < run_starts.size(); ++run) {
        rle_bytes += suffix_width + varint_len(run_length_at(run_starts, points.point_count, run));
    }
    const uint64_t raw_bytes = static_cast<uint64_t>(points.point_count) * suffix_width;
    // A tie goes to kRaw: same bytes, and its value area decodes as one
    // bounds-checked get_bytes instead of a varint walk.
    return (rle_bytes < raw_bytes) ? LeafValueMode::kRle : LeafValueMode::kRaw;
}

void write_value_area(const PointArray& points, const std::vector<uint32_t>& run_starts,
                      LeafValueMode mode, uint32_t common_prefix_len, ByteSink* sink) {
    const uint32_t suffix_width = points.bytes_per_dim - common_prefix_len;
    if (mode == LeafValueMode::kRle) {
        sink->put_varint32(static_cast<uint32_t>(run_starts.size()));
        for (size_t run = 0; run < run_starts.size(); ++run) {
            sink->put_bytes(Slice(points.value(run_starts[run]) + common_prefix_len, suffix_width));
            sink->put_varint32(run_length_at(run_starts, points.point_count, run));
        }
        return;
    }
    if (mode == LeafValueMode::kRaw) {
        for (size_t point = 0; point < points.point_count; ++point) {
            sink->put_bytes(Slice(points.value(point) + common_prefix_len, suffix_width));
        }
        return;
    }
    // kAllEqual carries no value area at all: the head's prefix is the value.
    DCHECK(mode == LeafValueMode::kAllEqual);
}

// Fills the PFOR codes. Doc ids ride on the (value, doc_id) sort key: inside a run
// they are non-decreasing, so a run's points collapse to small deltas. kRaw skips
// the delta -- its runs are length 1, so the deltas would be the doc ids.
void build_doc_id_codes(const PointArray& points, const std::vector<uint32_t>& run_starts,
                        LeafValueMode mode, std::vector<uint32_t>* codes) {
    codes->resize(points.point_count);
    if (mode == LeafValueMode::kRaw) {
        for (size_t point = 0; point < points.point_count; ++point) {
            (*codes)[point] = points.doc_id(point);
        }
        return;
    }
    for (size_t run = 0; run < run_starts.size(); ++run) {
        const uint32_t first = run_starts[run];
        const uint32_t length = run_length_at(run_starts, points.point_count, run);
        uint32_t previous = points.doc_id(first);
        (*codes)[first] = previous;
        for (uint32_t i = 1; i < length; ++i) {
            const uint32_t doc_id = points.doc_id(first + i);
            DCHECK_GE(doc_id, previous);
            (*codes)[first + i] = doc_id - previous;
            previous = doc_id;
        }
    }
}

} // namespace

void encode_leaf_block(Slice records, uint32_t bytes_per_dim, ByteSink* sink) {
    DORIS_CHECK(sink != nullptr);
    DORIS_CHECK_GT(bytes_per_dim, 0U);
    const size_t record_size = static_cast<size_t>(bytes_per_dim) + kPointDocIdBytes;
    DORIS_CHECK_EQ(records.size() % record_size, 0UL);
    const PointArray points {records.data(), record_size, bytes_per_dim,
                             records.size() / record_size};
    // The builder slices leaves off a non-empty sorted stream, so an empty leaf is
    // a bug in the caller, not a shape this format has to express.
    DORIS_CHECK_GT(points.point_count, 0UL);
    DORIS_CHECK_LE(points.point_count, static_cast<size_t>(std::numeric_limits<uint32_t>::max()));

    const std::vector<uint32_t> run_starts = scan_runs(points);
    const uint32_t common_prefix_len = common_prefix_length(points);
    const uint32_t suffix_width = bytes_per_dim - common_prefix_len;
    const LeafValueMode mode = choose_value_mode(points, run_starts, suffix_width);

    const size_t block_start = sink->size();
    // Capacity only; a loose upper bound over head + value area + PFOR + tail.
    sink->reserve(12 + bytes_per_dim +
                  points.point_count * (static_cast<size_t>(bytes_per_dim) + 11));

    sink->put_varint32(static_cast<uint32_t>(points.point_count));
    sink->put_u8(static_cast<uint8_t>(mode));
    sink->put_varint32(common_prefix_len);
    sink->put_bytes(Slice(points.value(0), common_prefix_len));
    write_value_area(points, run_starts, mode, common_prefix_len, sink);

    const size_t docid_block_offset = sink->size() - block_start;
    DORIS_CHECK_LE(docid_block_offset, static_cast<size_t>(std::numeric_limits<uint32_t>::max()));

    std::vector<uint32_t> codes;
    build_doc_id_codes(points, run_starts, mode, &codes);
    pfor_encode(codes.data(), points.point_count, sink);

    // The trailing length byte is what makes the varint reachable from the end of
    // the block; see the header for why a bare LEB128 varint is not.
    const size_t offset_start = sink->size();
    sink->put_varint32(static_cast<uint32_t>(docid_block_offset));
    sink->put_u8(static_cast<uint8_t>(sink->size() - offset_start));
}

// ===========================================================================
// Decode
// ===========================================================================
namespace {

// Longest LEB128 encoding of a uint32.
constexpr size_t kMaxVarint32Bytes = 5;

// Every rejection of leaf bytes funnels through here. A leaf is read lazily and is
// therefore NOT covered by the open-time validation of bkd_index, so these are the
// checks that stand between a damaged file and the query. Disk data is not an
// invariant: none of them may be a DORIS_CHECK, or a recoverable downgrade would
// become a node crash (design 8).
Status corrupted(std::string_view what) {
    return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("bkd leaf: {}", what);
}

// The fixed part of a leaf block, validated against the index header.
struct LeafHead {
    uint32_t point_count = 0;
    LeafValueMode value_mode = LeafValueMode::kAllEqual;
    Slice common_prefix;
    uint32_t suffix_width = 0;
};

Status decode_leaf_head(ByteSource* src, uint32_t bytes_per_dim, uint32_t expected_point_count,
                        LeafHead* head) {
    uint32_t point_count = 0;
    RETURN_IF_ERROR(src->get_varint32(&point_count));
    // Design 5.2: the leaf directory already said how many points are here, and it
    // was validated at open. Pinning the two together both catches damage and
    // bounds every allocation below by trusted metadata -- necessary because an
    // all-equal leaf spends zero bytes per point, so block length alone bounds
    // nothing.
    if (point_count != expected_point_count) {
        return corrupted("point_count disagrees with the leaf directory");
    }
    if (point_count == 0) {
        return corrupted("leaf carries no points");
    }

    uint8_t raw_mode = 0;
    RETURN_IF_ERROR(src->get_u8(&raw_mode));
    if (raw_mode > static_cast<uint8_t>(kMaxLeafValueMode)) {
        return corrupted("unknown value_mode");
    }
    const auto value_mode = static_cast<LeafValueMode>(raw_mode);

    uint32_t common_prefix_len = 0;
    RETURN_IF_ERROR(src->get_varint32(&common_prefix_len));
    if (common_prefix_len > bytes_per_dim) {
        return corrupted("common_prefix_len exceeds bytes_per_dim");
    }
    Slice common_prefix;
    RETURN_IF_ERROR(src->get_bytes(common_prefix_len, &common_prefix));
    const uint32_t suffix_width = bytes_per_dim - common_prefix_len;

    // kAllEqual means the prefix IS the value, so it covers the whole width; the
    // other two modes need at least one suffix byte to tell their values apart.
    // Either mismatch would make the decoded values silently wrong rather than
    // merely unparseable.
    if ((value_mode == LeafValueMode::kAllEqual) != (suffix_width == 0)) {
        return corrupted("value_mode disagrees with common_prefix_len");
    }

    head->point_count = point_count;
    head->value_mode = value_mode;
    head->common_prefix = common_prefix;
    head->suffix_width = suffix_width;
    return Status::OK();
}

// Reads the trailing { docid_block_offset varint32, offset_length u8 }.
// `docid_block_end` comes back as the first tail byte, i.e. the doc id block is
// exactly [*docid_block_offset, *docid_block_end).
Status decode_leaf_tail(Slice block, uint32_t* docid_block_offset, size_t* docid_block_end) {
    if (block.empty()) {
        return corrupted("leaf block is empty");
    }
    ByteSource length_src(block.subslice(block.size() - 1, 1));
    uint8_t length = 0;
    RETURN_IF_ERROR(length_src.get_u8(&length));
    if (length == 0 || length > kMaxVarint32Bytes ||
        static_cast<size_t>(length) + 1 > block.size()) {
        return corrupted("docid_block_offset length byte is out of range");
    }

    const size_t offset_start = block.size() - 1 - length;
    ByteSource offset_src(block.subslice(offset_start, length));
    RETURN_IF_ERROR(offset_src.get_varint32(docid_block_offset));
    if (!offset_src.eof()) {
        // The length byte and the varint must describe the same bytes, or the two
        // ends of the block disagree about where the doc ids start.
        return corrupted("docid_block_offset is not exactly its declared length");
    }
    if (*docid_block_offset > offset_start) {
        return corrupted("docid_block_offset points past the leaf tail");
    }
    *docid_block_end = offset_start;
    return Status::OK();
}

// Decodes the value area into ascending runs of equal values. `runs` is appended
// to (the caller cleared it), so a reused DecodedLeafBlock keeps its capacity.
//
// `runs` is written through, by reserve() and push_back() -- a pointer-to-const
// would not compile. readability-non-const-parameter still claims otherwise
// because clang-tidy cannot locate stddef.h in this toolchain (see the
// clang-diagnostic-error it reports first), so <vector>'s members parse into
// recovery nodes and the modification becomes invisible to it.
// NOLINTNEXTLINE(readability-non-const-parameter)
Status read_value_area(ByteSource* src, const LeafHead& head, std::vector<LeafValueRun>* runs) {
    if (head.value_mode == LeafValueMode::kAllEqual) {
        // No value area at all: the head's prefix is the value, and the whole leaf
        // is one run.
        runs->push_back(LeafValueRun {Slice(), 0, head.point_count});
        return Status::OK();
    }

    if (head.value_mode == LeafValueMode::kRle) {
        uint32_t run_count = 0;
        RETURN_IF_ERROR(src->get_varint32(&run_count));
        // Bounded by the directory-pinned point_count before it sizes anything.
        if (run_count == 0 || run_count > head.point_count) {
            return corrupted("run_count is out of range");
        }
        // Bounded by what is actually left to read, not by point_count alone: a
        // ~10-byte kRle leaf can otherwise declare a million runs and reserve 25 MB
        // (24 bytes each) before the first suffix read proves the bytes are not
        // there. Every run costs at least a suffix plus one doc id byte.
        if (run_count > src->remaining() / (static_cast<size_t>(head.suffix_width) + 1)) {
            return corrupted("run count exceeds the bytes remaining in the leaf");
        }
        runs->reserve(run_count);
        uint32_t covered = 0;
        Slice previous;
        for (uint32_t run = 0; run < run_count; ++run) {
            Slice suffix;
            RETURN_IF_ERROR(src->get_bytes(head.suffix_width, &suffix));
            uint32_t run_length = 0;
            RETURN_IF_ERROR(src->get_varint32(&run_length));
            if (run_length == 0) {
                return corrupted("run_len is zero");
            }
            // Runs are maximal and ascending. An unordered leaf would not fail to
            // parse -- it would silently break the boundary-leaf early exit, which
            // stops at the first value past the range -- so the order is checked
            // here rather than trusted.
            if (run != 0 && std::memcmp(previous.data(), suffix.data(), head.suffix_width) >= 0) {
                return corrupted("run suffixes are not strictly ascending");
            }
            if (run_length > head.point_count - covered) {
                return corrupted("run lengths overrun point_count");
            }
            runs->push_back(LeafValueRun {suffix, covered, run_length});
            covered += run_length;
            previous = suffix;
        }
        if (covered != head.point_count) {
            return corrupted("run lengths do not sum to point_count");
        }
        return Status::OK();
    }

    // kRaw -- the only mode left, decode_leaf_head rejected everything else.
    DCHECK(head.value_mode == LeafValueMode::kRaw);
    Slice suffixes;
    RETURN_IF_ERROR(
            src->get_bytes(static_cast<size_t>(head.point_count) * head.suffix_width, &suffixes));
    runs->reserve(head.point_count);
    Slice previous;
    for (uint32_t point = 0; point < head.point_count; ++point) {
        const Slice suffix = suffixes.subslice(static_cast<size_t>(point) * head.suffix_width,
                                               head.suffix_width);
        // Non-decreasing, not strictly: kRaw is picked on size, so a leaf with a
        // few short runs can still land here.
        if (point != 0 && std::memcmp(previous.data(), suffix.data(), head.suffix_width) > 0) {
            return corrupted("suffixes are not ascending");
        }
        runs->push_back(LeafValueRun {suffix, point, 1});
        previous = suffix;
    }
    return Status::OK();
}

// The whole-leaf-hit variant: steps over the value area instead of decoding it.
// Only kRaw can be skipped outright -- kRle carries the run lengths its doc id
// deltas restart on, and kAllEqual has no value area to skip.
Status skip_value_area(ByteSource* src, const LeafHead& head, std::vector<LeafValueRun>* runs) {
    if (head.value_mode == LeafValueMode::kRaw) {
        Slice unused;
        return src->get_bytes(static_cast<size_t>(head.point_count) * head.suffix_width, &unused);
    }
    return read_value_area(src, head, runs);
}

// Decodes the PFOR block into absolute doc ids. `region` must be exactly the doc
// id bytes; `runs` is unused for kRaw, whose codes already are the doc ids.
Status decode_doc_id_block(Slice region, const LeafHead& head, std::span<const LeafValueRun> runs,
                           std::vector<uint32_t>* doc_ids) {
    ByteSource src(region);
    doc_ids->resize(head.point_count);
    RETURN_IF_ERROR(pfor_decode(&src, head.point_count, doc_ids->data()));
    if (!src.eof()) {
        return corrupted("trailing bytes between the doc id block and the leaf tail");
    }
    if (head.value_mode == LeafValueMode::kRaw) {
        return Status::OK();
    }

    // kAllEqual is one run over the leaf, kRle one per value: in both the first
    // code of a run is absolute and the rest are deltas off it.
    for (const LeafValueRun& run : runs) {
        uint64_t doc_id = (*doc_ids)[run.first_point];
        for (uint32_t i = 1; i < run.count; ++i) {
            doc_id += (*doc_ids)[run.first_point + i];
            if (doc_id > std::numeric_limits<uint32_t>::max()) {
                return corrupted("doc id deltas overflow a 32-bit row id");
            }
            (*doc_ids)[run.first_point + i] = static_cast<uint32_t>(doc_id);
        }
    }
    return Status::OK();
}

} // namespace

Status decode_leaf_block(Slice block, uint32_t bytes_per_dim, uint32_t expected_point_count,
                         DecodedLeafBlock* out) {
    DORIS_CHECK(out != nullptr);
    DORIS_CHECK_GT(bytes_per_dim, 0U);
    out->clear();

    ByteSource src(block);
    LeafHead head;
    RETURN_IF_ERROR(decode_leaf_head(&src, bytes_per_dim, expected_point_count, &head));

    uint32_t docid_block_offset = 0;
    size_t docid_block_end = 0;
    RETURN_IF_ERROR(decode_leaf_tail(block, &docid_block_offset, &docid_block_end));

    RETURN_IF_ERROR(read_value_area(&src, head, &out->runs));
    // The two ends of the block must agree on where the values stop. Without this
    // a damaged offset would let the doc id decoder reinterpret value bytes.
    if (src.position() != docid_block_offset) {
        return corrupted("docid_block_offset disagrees with the end of the value area");
    }
    RETURN_IF_ERROR(decode_doc_id_block(
            block.subslice(docid_block_offset, docid_block_end - docid_block_offset), head,
            out->runs, &out->doc_ids));

    out->value_mode = head.value_mode;
    out->common_prefix = head.common_prefix;
    out->suffix_width = head.suffix_width;
    // Written LAST, so a decode that failed anywhere above leaves a leaf that
    // reports zero points rather than one holding half-decoded scratch.
    out->point_count = head.point_count;
    return Status::OK();
}

Status decode_leaf_doc_ids(Slice block, uint32_t bytes_per_dim, uint32_t expected_point_count,
                           std::vector<uint32_t>* doc_ids) {
    DORIS_CHECK(doc_ids != nullptr);
    DORIS_CHECK_GT(bytes_per_dim, 0U);

    ByteSource src(block);
    LeafHead head;
    RETURN_IF_ERROR(decode_leaf_head(&src, bytes_per_dim, expected_point_count, &head));

    uint32_t docid_block_offset = 0;
    size_t docid_block_end = 0;
    RETURN_IF_ERROR(decode_leaf_tail(block, &docid_block_offset, &docid_block_end));

    std::vector<LeafValueRun> runs;
    RETURN_IF_ERROR(skip_value_area(&src, head, &runs));
    if (src.position() != docid_block_offset) {
        return corrupted("docid_block_offset disagrees with the end of the value area");
    }
    return decode_doc_id_block(
            block.subslice(docid_block_offset, docid_block_end - docid_block_offset), head, runs,
            doc_ids);
}

} // namespace doris::snii::bkd
