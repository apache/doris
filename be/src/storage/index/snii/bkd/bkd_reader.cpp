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

#include "storage/index/snii/bkd/bkd_reader.h"

#include <algorithm>
#include <cstring>
#include <string_view>
#include <utility>

#include "common/check.h"
#include "roaring/roaring.hh"

namespace doris::snii::bkd {

namespace {

// LSD radix sort over 32-bit doc ids: four 8-bit passes, counting sort each.
//
// std::sort is the wrong tool here and was measured to be: sorting 1M doc ids
// with it costs more than the roaring insertion it was meant to cheapen (the
// wide-range case went from 0.886x to 1.588x against the baseline). Radix is
// O(n) with a 256-entry histogram per pass, which is what makes "sort, then
// insert ascending" cheaper than inserting in leaf order.
void radix_sort_u32(std::vector<uint32_t>* values, std::vector<uint32_t>* scratch) {
    const size_t n = values->size();
    if (n < 2) {
        return;
    }
    scratch->resize(n);
    uint32_t* src = values->data();
    uint32_t* dst = scratch->data();
    for (int shift = 0; shift < 32; shift += 8) {
        size_t count[257] = {};
        for (size_t i = 0; i < n; ++i) {
            ++count[((src[i] >> shift) & 0xFFU) + 1];
        }
        // A pass whose key byte is constant reorders nothing; skipping it also
        // keeps the src/dst parity correct without an extra copy.
        bool uniform = false;
        for (size_t b = 1; b < 257; ++b) {
            if (count[b] == n) {
                uniform = true;
                break;
            }
        }
        if (uniform) {
            continue;
        }
        for (size_t b = 1; b < 257; ++b) {
            count[b] += count[b - 1];
        }
        for (size_t i = 0; i < n; ++i) {
            dst[count[(src[i] >> shift) & 0xFFU]++] = src[i];
        }
        std::swap(src, dst);
    }
    // Odd number of executed passes leaves the result in the scratch buffer.
    if (src != values->data()) {
        std::memcpy(values->data(), src, n * sizeof(uint32_t));
    }
}

// The BkdSections extents come from the container's named-file table, i.e. from
// disk. Damage there is reported, never asserted (design 8) -- and it is caught
// BEFORE a length is handed to a read, so a corrupt one cannot drive a
// multi-gigabyte allocation on the way to failing.
Status corrupted(std::string_view what) {
    return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("bkd_reader: {}", what);
}

Status check_extent(uint64_t offset, uint64_t length, uint64_t file_size, std::string_view name) {
    // Written as "length first, then offset against what is left" so neither
    // comparison can overflow the way offset + length would.
    if (length > file_size || offset > file_size - length) {
        return corrupted(name);
    }
    return Status::OK();
}

// ---------------------------------------------------------------------------
// Split-value search (design 7.2)
// ---------------------------------------------------------------------------
//
// The split array replaces the old recursive descent entirely: routing a value to
// a leaf in one dimension is exactly a binary search over an ordered fixed-width
// array, with no per-level VLong / VInt / prefix decode to pay.
//
// Both searches are over an array that is NON-decreasing, not strictly
// increasing -- one value repeated across several leaves makes consecutive
// splits equal -- so the two bounds genuinely differ and neither may be
// substituted for the other.

// Number of split values strictly less than `key`, i.e. the index of the first
// split >= key.
uint32_t split_lower_bound(Slice splits, uint32_t width, const uint8_t* key) {
    uint32_t low = 0;
    auto high = static_cast<uint32_t>(splits.size() / width);
    while (low < high) {
        const uint32_t mid = low + (high - low) / 2;
        if (std::memcmp(splits.data() + static_cast<size_t>(mid) * width, key, width) < 0) {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    return low;
}

// Number of split values <= `key`, i.e. the index of the first split > key.
uint32_t split_upper_bound(Slice splits, uint32_t width, const uint8_t* key) {
    uint32_t low = 0;
    auto high = static_cast<uint32_t>(splits.size() / width);
    while (low < high) {
        const uint32_t mid = low + (high - low) / 2;
        if (std::memcmp(splits.data() + static_cast<size_t>(mid) * width, key, width) <= 0) {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    return low;
}

// The whole zero-IO half of design 7.2: narrows a range down to the contiguous
// run of leaves [*first, *last] that can hold a match, from nothing but the
// resident header and split array. Returns false when nothing can match, in
// which case not a single leaf is touched.
//
// `block` must be non-empty -- the empty index is answered by the caller.
bool locate_leaf_window(const BkdIndexBlockReader& block, Slice lower, bool lower_inclusive,
                        Slice upper, bool upper_inclusive, uint32_t* first, uint32_t* last) {
    const uint32_t width = block.header().bytes_per_dim;

    // Global-bounds fast reject. Every indexed value lies in
    // [min_value, max_value], so if the extreme value on one side already fails
    // its bound, nothing in the index can satisfy it.
    if (!upper.empty()) {
        const int order = std::memcmp(block.min_value().data(), upper.data(), width);
        if (upper_inclusive ? order > 0 : order >= 0) {
            return false;
        }
    }
    if (!lower.empty()) {
        const int order = std::memcmp(block.max_value().data(), lower.data(), width);
        if (lower_inclusive ? order < 0 : order <= 0) {
            return false;
        }
    }

    // Leaf i spans values [split(i-1), split(i)] inclusive at BOTH ends
    // (split(-1) is min_value, split(leaf_count-1) is max_value): the builder
    // makes split(i) the FIRST value of leaf i+1, and one value repeated across
    // leaves makes the last value of leaf i equal split(i) too.
    //
    //   first = the lowest leaf whose MAXIMUM can still satisfy the lower bound.
    //           Leaf j's maximum is at most split(j), so a leaf whose split(j)
    //           already fails the bound is skipped outright.
    //   last  = the highest leaf whose MINIMUM can still satisfy the upper bound.
    //           Leaf j's minimum is exactly split(j-1), so the COUNT of splits
    //           still satisfying the bound is that leaf's index.
    const Slice splits = block.split_values();
    *first = 0;
    if (!lower.empty()) {
        *first = lower_inclusive ? split_lower_bound(splits, width, lower.data())
                                 : split_upper_bound(splits, width, lower.data());
    }
    *last = block.leaf_count() - 1;
    if (!upper.empty()) {
        *last = upper_inclusive ? split_upper_bound(splits, width, upper.data())
                                : split_lower_bound(splits, width, upper.data());
    }
    // An interval whose lower bound sits above its upper one is empty by
    // definition -- a legal query (a planner fusing `a > 30 AND a < 10` produces
    // one), and one the global reject cannot catch when both bounds lie inside
    // [min_value, max_value].
    return *first <= *last;
}

// ---------------------------------------------------------------------------
// Boundary-leaf value filter
// ---------------------------------------------------------------------------

enum class BoundSide { kLower, kUpper };

// One side of the range predicate, specialized to ONE decoded leaf.
//
// Every value in a leaf is common_prefix ++ suffix, and the prefix is the same
// for all of them, so its comparison against the bound is done ONCE here; per run
// only the suffix is left. The boundary scan therefore never reassembles a whole
// value, and when the prefix alone already decides (the usual case for a narrow
// range over a wide type) the per-run cost is a single branch.
//
// An EMPTY `bound` is the unbounded side (design 7.1): satisfied_by() is then
// always true, which is also how the two boundary leaves of a multi-leaf range
// each test only the one bound that can still exclude something.
class LeafValueBound {
public:
    LeafValueBound(const DecodedLeafBlock& leaf, Slice bound, bool inclusive, BoundSide side)
            : bounded_(!bound.empty()), inclusive_(inclusive), side_(side) {
        if (!bounded_) {
            return;
        }
        const size_t prefix_length = leaf.common_prefix.size();
        // A zero-length prefix decides nothing, and memcmp over zero bytes would
        // be handed the null data() of an empty Slice.
        prefix_order_ = prefix_length == 0 ? 0
                                           : std::memcmp(leaf.common_prefix.data(), bound.data(),
                                                         prefix_length);
        bound_suffix_ = bound.data() + prefix_length;
        suffix_width_ = leaf.suffix_width;
    }

    bool satisfied_by(const LeafValueRun& run) const {
        if (!bounded_) {
            return true;
        }
        const int order = compare(run);
        if (side_ == BoundSide::kLower) {
            return inclusive_ ? order >= 0 : order > 0;
        }
        return inclusive_ ? order <= 0 : order < 0;
    }

private:
    // Sign of (run value) - (bound), as an unsigned byte-wise comparison (INV-1).
    int compare(const LeafValueRun& run) const {
        if (prefix_order_ != 0) {
            return prefix_order_;
        }
        // kAllEqual: the prefix IS the whole value, so equal prefixes mean equal
        // values and there is no suffix to look at.
        if (suffix_width_ == 0) {
            return 0;
        }
        return std::memcmp(run.suffix.data(), bound_suffix_, suffix_width_);
    }

    const bool bounded_;
    const bool inclusive_;
    const BoundSide side_;
    int prefix_order_ = 0;
    const uint8_t* bound_suffix_ = nullptr;
    uint32_t suffix_width_ = 0;
};

} // namespace

BkdReader::BkdReader(io::FileReader* file, const BkdSections& sections)
        : file_(file), sections_(sections) {}

Status BkdReader::open(io::FileReader* file, const BkdSections& sections,
                       std::unique_ptr<BkdReader>* out) {
    DORIS_CHECK(file != nullptr);
    DORIS_CHECK(out != nullptr);

    const uint64_t file_size = file->size();
    RETURN_IF_ERROR(check_extent(sections.index_offset, sections.index_length, file_size,
                                 "the bkd_index extent does not fit the file"));
    RETURN_IF_ERROR(check_extent(sections.data_offset, sections.data_length, file_size,
                                 "the bkd_data extent does not fit the file"));

    // bkd_index is the HOT sub-file: read in full once, kept resident, never read
    // again (design 5.1). A zero length falls through to the framer, which
    // reports it as damage.
    std::vector<uint8_t> index_bytes;
    index_bytes.resize(static_cast<size_t>(sections.index_length));
    RETURN_IF_ERROR(file->read_into(sections.index_offset, index_bytes.data(), index_bytes.size()));

    auto reader = std::unique_ptr<BkdReader>(new BkdReader(file, sections));
    // Runs the ENTIRE structural validation, including bounding the leaf offsets
    // against the bkd_data length passed here -- which is what lets read_leaf()
    // below compute a block extent without re-checking anything (design 8.2).
    RETURN_IF_ERROR(
            BkdIndexBlockReader::open(Slice(index_bytes), sections.data_length, &reader->block_));
    // Published only once everything is valid, so a failed open leaves the
    // caller's unique_ptr untouched.
    *out = std::move(reader);
    return Status::OK();
}

Status BkdReader::range(Slice lower, bool lower_inclusive, Slice upper, bool upper_inclusive,
                        roaring::Roaring* hits) const {
    BkdQueryScratch scratch;
    return range(lower, lower_inclusive, upper, upper_inclusive, hits, &scratch);
}

Status BkdReader::range(Slice lower, bool lower_inclusive, Slice upper, bool upper_inclusive,
                        roaring::Roaring* hits, BkdQueryScratch* scratch) const {
    DORIS_CHECK(hits != nullptr);
    DORIS_CHECK(scratch != nullptr);
    const uint32_t width = block_.header().bytes_per_dim;
    // Bounds come from the caller's KeyCoder for header().field_type, so a wrong
    // width is a programming error, not damage. Empty is the unbounded side.
    DORIS_CHECK(lower.empty() || lower.size() == width);
    DORIS_CHECK(upper.empty() || upper.size() == width);

    // Whatever the caller's bitmap held is not part of this answer.
    *hits = roaring::Roaring();

    // The empty index (design 5.3 / 10.4): an empty result, NOT an error for the
    // adapter to translate, and no I/O.
    if (block_.empty()) {
        return Status::OK();
    }

    uint32_t first = 0;
    uint32_t last = 0;
    // Answered entirely from the resident bkd_index. A range that cannot match --
    // outside the global bounds, or an empty interval -- therefore costs zero
    // positioned reads (design 7.2).
    if (!locate_leaf_window(block_, lower, lower_inclusive, upper, upper_inclusive, &first,
                            &last)) {
        return Status::OK();
    }

    if (first == last) {
        return scan_boundary_leaf(first, lower, lower_inclusive, upper, upper_inclusive, hits,
                                  scratch);
    }

    // first < last, so split(first) already satisfies the lower bound and
    // split(last - 1) already satisfies the upper one. Everything in leaf `first`
    // is therefore at most split(first) <= upper, and everything in leaf `last` is
    // at least split(last-1) >= lower: each boundary leaf only has to test the one
    // bound on its own side.
    RETURN_IF_ERROR(
            scan_boundary_leaf(first, lower, lower_inclusive, Slice(), true, hits, scratch));
    // Leaves strictly between them are bounded by those same two splits on both
    // sides, so they are whole-leaf hits: doc ids only, values never decoded.
    // Interior leaves are whole-leaf hits. Their doc ids are gathered, SORTED,
    // and inserted once.
    //
    // The sort is the point, not the batching. Leaves are ordered by VALUE, so
    // consecutive leaves carry unrelated doc ids and the insertion sequence is
    // effectively random. Roaring pays far more for that than for an ascending
    // run: measured on this benchmark, inserting 1M doc ids in leaf order costs
    // ~63 ms while inserting the same ids ascending costs ~5.5 ms. Batching
    // alone does not recover it -- an earlier attempt that gathered without
    // sorting changed nothing.
    // Flushed in bounded chunks, NOT accumulated across the whole range.
    //
    // Gathering every interior leaf's doc ids into one vector makes the
    // allocation a function of the RANGE, and nothing in the format bounds
    // leaf_count * points_per_leaf: a crafted index of a few tens of KB, whose
    // leaves each declare the legal maximum count and encode as kAllEqual (zero
    // bytes per point), drives billions of doc ids here. The per-leaf ceiling in
    // bkd_index_block does not compose into an aggregate one.
    //
    // A chunk still sorts in large batches, which is where the win is -- the
    // cost being avoided is random-order insertion, not the call count.
    constexpr size_t kMaxGatheredDocIds = 1U << 20;
    // The boundary-leaf decode holds Slices INTO scratch->leaf_bytes, which the
    // interior loop is about to overwrite and may reallocate. Cleared so nothing
    // can later read a struct that still looks populated but points at freed
    // bytes.
    scratch->decoded.clear();
    scratch->gathered.clear();
    const auto flush = [&] {
        if (scratch->gathered.empty()) {
            return;
        }
        radix_sort_u32(&scratch->gathered, &scratch->radix_scratch);
        hits->addMany(scratch->gathered.size(), scratch->gathered.data());
        scratch->gathered.clear();
    };
    for (uint32_t leaf = first + 1; leaf < last; ++leaf) {
        RETURN_IF_ERROR(read_leaf(leaf, &scratch->leaf_bytes));
        RETURN_IF_ERROR(decode_leaf_doc_ids(Slice(scratch->leaf_bytes),
                                            block_.header().bytes_per_dim, block_.leaf(leaf).count,
                                            &scratch->doc_ids));
        scratch->gathered.insert(scratch->gathered.end(), scratch->doc_ids.begin(),
                                 scratch->doc_ids.end());
        if (scratch->gathered.size() >= kMaxGatheredDocIds) {
            flush();
        }
    }
    flush();
    return scan_boundary_leaf(last, Slice(), true, upper, upper_inclusive, hits, scratch);
}

Status BkdReader::lookup_many(const std::vector<Slice>& values, roaring::Roaring* hits) const {
    BkdQueryScratch scratch;
    return lookup_many(values, hits, &scratch);
}

Status BkdReader::lookup_many(const std::vector<Slice>& values, roaring::Roaring* hits,
                              BkdQueryScratch* scratch) const {
    DORIS_CHECK(hits != nullptr);
    DORIS_CHECK(scratch != nullptr);
    const uint32_t width = block_.header().bytes_per_dim;

    *hits = roaring::Roaring();
    if (values.empty() || block_.empty()) {
        return Status::OK();
    }
    for (const Slice& value : values) {
        // Same width contract as range(): these come from the caller's KeyCoder.
        DORIS_CHECK_EQ(value.size(), static_cast<size_t>(width));
    }

    // Ordered HERE rather than demanded of the caller.
    //
    // This used to be a caller invariant, justified by "the caller holds the set
    // and knows its order for free". That premise is wrong for the caller this
    // exists to serve: InListPredicateBase iterates a HybridSet backed by
    // phmap::flat_hash_set, which yields HASH order. It was also enforced with a
    // bare glog DCHECK, which is a no-op under NDEBUG -- so in a release build an
    // unsorted list silently lost rows instead of failing, because the watermark
    // below skips every leaf under the high-water mark.
    //
    // Sorting N probe values is negligible against the leaf reads they cause, and
    // it makes the entry point correct for any caller. Duplicates are dropped
    // because two equal values would locate the same window twice.
    scratch->probes.assign(values.begin(), values.end());
    std::sort(scratch->probes.begin(), scratch->probes.end(),
              [width](const Slice& a, const Slice& b) {
                  return std::memcmp(a.data(), b.data(), width) < 0;
              });
    scratch->probes.erase(std::unique(scratch->probes.begin(), scratch->probes.end(),
                                      [width](const Slice& a, const Slice& b) {
                                          return std::memcmp(a.data(), b.data(), width) == 0;
                                      }),
                          scratch->probes.end());
    const std::vector<Slice>& ordered = scratch->probes;

    // Every leaf that at least one value can live in, ascending and unique.
    // Windows are non-decreasing because the values are, so the watermark alone
    // deduplicates them -- a value repeated across leaves widens its own window,
    // and two values in one leaf produce overlapping windows that collapse here.
    std::vector<uint32_t> leaves;
    uint32_t watermark = 0;
    for (const Slice& value : ordered) {
        uint32_t first = 0;
        uint32_t last = 0;
        if (!locate_leaf_window(block_, value, true, value, true, &first, &last)) {
            continue;
        }
        for (uint32_t leaf = std::max(first, watermark); leaf <= last; ++leaf) {
            leaves.push_back(leaf);
        }
        watermark = std::max(watermark, last + 1);
    }

    std::vector<uint8_t> value_buffer(width);
    for (const uint32_t leaf_index : leaves) {
        RETURN_IF_ERROR(read_leaf(leaf_index, &scratch->leaf_bytes));
        RETURN_IF_ERROR(decode_leaf_block(Slice(scratch->leaf_bytes), width,
                                          block_.leaf(leaf_index).count, &scratch->decoded));
        const DecodedLeafBlock& leaf = scratch->decoded;
        std::memcpy(value_buffer.data(), leaf.common_prefix.data(), leaf.common_prefix.size());

        // Runs, not points: a run is one distinct value, so one binary search
        // over the query set answers for all of its doc ids at once. Both
        // sequences ascend, but the search is kept per run rather than turned
        // into a linear merge because a leaf typically holds far more runs than
        // the query holds values.
        for (const LeafValueRun& run : leaf.runs) {
            std::memcpy(value_buffer.data() + leaf.common_prefix.size(), run.suffix.data(),
                        run.suffix.size());
            // `ordered`, never `values`: the caller's vector is in whatever order
            // it was handed to us, and a binary search over it is meaningless.
            const auto found = std::lower_bound(
                    ordered.begin(), ordered.end(), value_buffer,
                    [width](const Slice& candidate, const std::vector<uint8_t>& target) {
                        return std::memcmp(candidate.data(), target.data(), width) < 0;
                    });
            if (found != ordered.end() &&
                std::memcmp(found->data(), value_buffer.data(), width) == 0) {
                hits->addMany(run.count, leaf.doc_ids.data() + run.first_point);
            }
        }
    }
    return Status::OK();
}

Status BkdReader::estimate_cardinality(Slice lower, bool lower_inclusive, Slice upper,
                                       bool upper_inclusive, uint64_t* out) const {
    DORIS_CHECK(out != nullptr);
    const uint32_t width = block_.header().bytes_per_dim;
    DORIS_CHECK(lower.empty() || lower.size() == width);
    DORIS_CHECK(upper.empty() || upper.size() == width);

    *out = 0;
    if (block_.empty()) {
        return Status::OK();
    }
    uint32_t first = 0;
    uint32_t last = 0;
    if (!locate_leaf_window(block_, lower, lower_inclusive, upper, upper_inclusive, &first,
                            &last)) {
        return Status::OK();
    }

    // A boundary leaf is only GUESSED at when the bound actually cuts into it.
    // Halving it unconditionally would be wrong in the common case: an unbounded
    // interval has no partial leaf at all, yet the outermost leaves are still
    // "first" and "last", and halving them would under-count a full scan by a
    // whole leaf on each side.
    //
    // leaf i spans [leaf_min(i), leaf_max(i)] where leaf_min(i) is split(i-1) --
    // exactly leaf i's first value, since the builder makes split(i) the first
    // value of leaf i+1 -- and leaf_max(i) is split(i), which is an UPPER bound
    // on leaf i's real maximum. Using it errs toward calling a leaf partial, so
    // the estimate stays conservative rather than optimistic.
    const Slice splits = block_.split_values();
    const auto leaf_min = [&](uint32_t i) {
        return i == 0 ? block_.min_value()
                      : Slice(splits.data() + static_cast<size_t>(i - 1) * width, width);
    };
    const auto leaf_max = [&](uint32_t i) {
        return i + 1 == block_.leaf_count()
                       ? block_.max_value()
                       : Slice(splits.data() + static_cast<size_t>(i) * width, width);
    };

    bool first_whole = lower.empty();
    if (!first_whole) {
        const int order = std::memcmp(leaf_min(first).data(), lower.data(), width);
        first_whole = lower_inclusive ? order >= 0 : order > 0;
    }
    bool last_whole = upper.empty();
    if (!last_whole) {
        const int order = std::memcmp(leaf_max(last).data(), upper.data(), width);
        last_whole = upper_inclusive ? order <= 0 : order < 0;
    }

    // The interior is EXACT: those leaves lie wholly inside the interval, so
    // their recorded counts are the answer, not a guess.
    uint64_t estimate = 0;
    for (uint32_t leaf = first + 1; leaf + 1 <= last; ++leaf) {
        estimate += block_.leaf(leaf).count;
    }
    if (first == last) {
        const uint32_t count = block_.leaf(first).count;
        estimate += (first_whole && last_whole) ? count : count / 2;
    } else {
        estimate += first_whole ? block_.leaf(first).count : block_.leaf(first).count / 2;
        estimate += last_whole ? block_.leaf(last).count : block_.leaf(last).count / 2;
    }
    *out = estimate;
    return Status::OK();
}

Status BkdReader::read_leaf(uint32_t index, std::vector<uint8_t>* buffer) const {
    const LeafRef leaf = block_.leaf(index);
    // open() established that leaf offsets strictly increase and that the last one
    // is within data_length, so the subtraction cannot wrap and the extent cannot
    // leave the sub-file (design 8.2). A last leaf starting exactly at the end of
    // bkd_data yields an empty block, which the leaf decoder rejects as damage.
    const uint64_t end = (index + 1 < block_.leaf_count()) ? block_.leaf(index + 1).offset
                                                           : sections_.data_length;
    const size_t length = static_cast<size_t>(end - leaf.offset);
    buffer->resize(length);
    // One stateless positioned read per leaf -- no cursor, hence no clone() and no
    // synchronization between concurrent queries (design 9).
    return file_->read_into(sections_.data_offset + leaf.offset, buffer->data(), length);
}

Status BkdReader::scan_boundary_leaf(uint32_t index, Slice lower, bool lower_inclusive, Slice upper,
                                     bool upper_inclusive, roaring::Roaring* hits,
                                     BkdQueryScratch* scratch) const {
    RETURN_IF_ERROR(read_leaf(index, &scratch->leaf_bytes));
    RETURN_IF_ERROR(decode_leaf_block(Slice(scratch->leaf_bytes), block_.header().bytes_per_dim,
                                      block_.leaf(index).count, &scratch->decoded));
    const DecodedLeafBlock& leaf = scratch->decoded;

    const LeafValueBound lower_bound(leaf, lower, lower_inclusive, BoundSide::kLower);
    const LeafValueBound upper_bound(leaf, upper, upper_inclusive, BoundSide::kUpper);
    // Runs, not points: a run of equal values is judged ONCE and then accepted or
    // skipped whole, which is what makes a leaf of a heavily repeated value cost a
    // handful of comparisons instead of one per point.
    for (const LeafValueRun& run : leaf.runs) {
        if (!lower_bound.satisfied_by(run)) {
            continue;
        }
        // Runs ascend, so the first one past the upper bound ends the leaf: the
        // early exit the old implementation had, kept.
        if (!upper_bound.satisfied_by(run)) {
            break;
        }
        hits->addMany(run.count, leaf.doc_ids.data() + run.first_point);
    }
    return Status::OK();
}

} // namespace doris::snii::bkd
