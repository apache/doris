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

#include "storage/index/snii/bkd/bkd_builder.h"

#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <limits>
#include <string>
#include <utility>
#include <vector>

#include "common/cast_set.h"
#include "common/check.h"
#include "storage/index/snii/bkd/bkd_format.h"
#include "storage/index/snii/bkd/bkd_index_block.h"
#include "storage/index/snii/bkd/leaf_codec.h"
#include "storage/index/snii/bkd/point_merger.h"
#include "storage/index/snii/bkd/point_run.h"
#include "storage/index/snii/bkd/point_sorter.h"
#include "storage/index/snii/writer/temp_dir.h"
#include "storage/types.h"

namespace doris::snii::bkd {

namespace {

// Points the record buffer allocates for on its first growth. Large enough that a
// small column does not walk the geometric ladder from one record, small enough that
// a column with a handful of points does not hold a page it will never fill.
constexpr size_t kInitialRecordBufferPoints = 1024;

// The Phase 1 point stream: the builder's own resident run, already sorted in place,
// handed out in leaf-sized windows. It copies nothing -- every block is a view into
// the record buffer -- which is why sorting has to be in place (design 6.2): a
// second buffer would double the bound build_buffer_bytes is supposed to set.
//
// Phase 2's k-way merge over spilled runs implements this same interface, so the
// leaf-cutting loop in write_index() stays untouched.
class ResidentPointSource final : public PointSource {
public:
    ResidentPointSource(Slice records, size_t record_size)
            : records_(records), record_size_(record_size) {
        DORIS_CHECK_GT(record_size, 0UL);
        DORIS_CHECK_EQ(records.size() % record_size, 0UL);
    }

    Status next_block(uint32_t max_points, Slice* records) override {
        DORIS_CHECK(records != nullptr);
        DORIS_CHECK_GT(max_points, 0U);
        const size_t wanted = std::min(records_.size() - consumed_,
                                       static_cast<size_t>(max_points) * record_size_);
        *records = Slice(records_.data() + consumed_, wanted);
        consumed_ += wanted;
        return Status::OK();
    }

private:
    const Slice records_;
    const size_t record_size_;
    size_t consumed_ = 0;
};

} // namespace

BkdBuilder::BkdBuilder(const BkdBuilderOptions& options)
        : options_(options),
          record_size_(static_cast<size_t>(options.bytes_per_dim) + kPointDocIdBytes),
          max_points_(static_cast<size_t>(options.build_buffer_bytes / record_size_)),
          reservation_(options.reporter == nullptr ? writer::MemoryReporter::Reservation()
                                                   : options.reporter->make_reservation()) {}

Status BkdBuilder::create(const BkdBuilderOptions& options, std::unique_ptr<BkdBuilder>* out) {
    DORIS_CHECK(out != nullptr);
    // Design 6.1: everything the builder needs is settled BEFORE the object exists,
    // so there is no constructed-but-invalid state for later code to defend against.
    // These options come from Doris's own writer layer in this same process -- they
    // are internal invariants, not untrusted bytes, hence DORIS_CHECK (design 8).
    DORIS_CHECK_GT(options.bytes_per_dim, 0U);
    // INV-2. The membership test for "is this field type indexable at all" belongs to
    // encode_bkd_index_block, which owns the on-disk field_type vocabulary and asserts
    // it on the header this builder hands over in finish().
    DORIS_CHECK_EQ(static_cast<size_t>(options.bytes_per_dim), field_type_size(options.field_type));
    DORIS_CHECK_GT(options.points_per_leaf, 0U);
    // A buffer that cannot hold one record would make every add() fail.
    DORIS_CHECK_GE(options.build_buffer_bytes,
                   static_cast<uint64_t>(options.bytes_per_dim) + kPointDocIdBytes);
    *out = std::unique_ptr<BkdBuilder>(new BkdBuilder(options));
    return Status::OK();
}

Status BkdBuilder::add(uint32_t doc_id, Slice sortable_value) {
    DORIS_CHECK(!finished_);
    // DORIS_CHECK rather than DCHECK even though this is the per-row path:
    // bytes_per_dim bytes are copied out of this Slice below, so a short value would
    // read past its end in a release build too.
    DORIS_CHECK_EQ(sortable_value.size(), static_cast<size_t>(options_.bytes_per_dim));

    if (records_.size() / record_size_ == max_points_) {
        // The ceiling is a spill trigger, not a refusal (design 6.2). The old
        // implementation had no offline sort at all and simply grew until the
        // process died; here the resident footprint stays flat and the excess
        // goes to a run.
        RETURN_IF_ERROR(spill_current_run());
    }
    const size_t point_count = records_.size() / record_size_;
    RETURN_IF_ERROR(reserve_points(point_count + 1));

    // doc_count is counted HERE (design 6.1), never pushed in from outside. Doris
    // appends in ascending row order and an array column repeats one row's doc id
    // consecutively, so a doc id that differs from the previous one starts a new
    // document. That ordering is what makes the running counter exact; it is a
    // per-point property, hence DCHECK.
    //
    // "First point ever" is doc_count_ == 0, NOT an empty resident buffer: a
    // spill empties that buffer mid-stream, and testing it here would restart the
    // run of equal doc ids and count one document twice.
    DCHECK(doc_count_ == 0 || doc_id >= last_doc_id_);
    if (doc_count_ == 0 || doc_id != last_doc_id_) {
        ++doc_count_;
    }
    last_doc_id_ = doc_id;

    records_.insert(records_.end(), sortable_value.data(),
                    sortable_value.data() + sortable_value.size());
    // BIG-endian doc id tail: the memcmp of the whole record is then exactly
    // (value, doc_id) order, which is what point_sorter sorts by and what
    // leaf_codec's "doc ids ascend inside a run" relies on (design 6.2).
    for (uint32_t i = 0; i < kPointDocIdBytes; ++i) {
        records_.push_back(static_cast<uint8_t>(doc_id >> (8 * (kPointDocIdBytes - 1 - i))));
    }
    return Status::OK();
}

Status BkdBuilder::finish(io::FileWriter* data_out, ByteSink* index_out, BkdStats* stats) {
    DORIS_CHECK(data_out != nullptr);
    DORIS_CHECK(index_out != nullptr);
    DORIS_CHECK(stats != nullptr);
    DORIS_CHECK(!finished_);
    finished_ = true;

    Status status;
    if (run_paths_.empty()) {
        // FAST PATH: everything stayed resident. One in-place pass over the run.
        // The whole record is the key, so this single sort establishes
        // (value, doc_id) order without a separate tie-break (design 6.3).
        point_sorter::sort(records_.data(), records_.size() / record_size_,
                           static_cast<uint32_t>(record_size_));
        // Scoped so the source -- which is nothing but a view into records_ -- is
        // gone before the buffer it views is.
        ResidentPointSource source(Slice(records_), record_size_);
        status = write_index(&source, data_out, index_out, stats);
        release_records();
        return status;
    }

    // MERGE PATH. The residual becomes one more run rather than a special case, so
    // the merge sees a uniform set of inputs and the leaf-cutting loop below still
    // cannot tell the two build modes apart (design 6.2).
    status = spill_current_run();
    if (status.ok()) {
        // The resident buffer is dead weight from here on: the merge's own
        // footprint is (runs x per-run window + one leaf block), and holding the
        // old buffer on top of it would breach the very bound this path exists to
        // keep.
        release_records();

        // Fan-in the resident allowance can actually window: every cursor gets at
        // least kMinMergeCursorRecords. Above this many runs a single merge would
        // fall back to one record per cursor and hold run_count records --
        // total_points / max_points_, a footprint that GROWS with the input --
        // so the runs are folded in groups first. The leaf block sits on top of
        // the cursor windows because a leaf has to be materialized contiguously
        // no matter how the points arrived.
        const size_t fan_in = std::max<size_t>(2, max_points_ / kMinMergeCursorRecords);
        status = fold_runs_to_fan_in(fan_in, stats);
        if (status.ok()) {
            const size_t per_run = records_per_cursor(run_paths_.size());
            ++stats->merge_passes;
            std::unique_ptr<MergingPointSource> source;
            status = MergingPointSource::create(run_paths_, static_cast<uint32_t>(record_size_),
                                                options_.points_per_leaf,
                                                static_cast<uint32_t>(per_run), &source);
            if (status.ok()) {
                // Measured from the cursors that were actually opened, not
                // recomputed from what they were asked for.
                stats->peak_merge_buffer_bytes = std::max<uint64_t>(
                        stats->peak_merge_buffer_bytes, source->resident_buffer_bytes());
                status = write_index(source.get(), data_out, index_out, stats);
            }
        }
    }
    release_records();
    remove_runs();
    if (status.ok()) {
        stats->built_with_spill = true;
    }
    return status;
}

Status BkdBuilder::spill_current_run() {
    point_sorter::sort(records_.data(), records_.size() / record_size_,
                       static_cast<uint32_t>(record_size_));

    // pid plus a process-wide counter: two builders running concurrently in one BE
    // must not collide, and a stale file from a previous process must not be
    // mistaken for one of ours.
    static std::atomic<uint64_t> sequence {0};
    const std::string path = writer::resolve_temp_dir() + "/snii_bkd_" +
                             std::to_string(::getpid()) + "_" +
                             std::to_string(sequence.fetch_add(1)) + ".run";

    PointRunWriter run;
    RETURN_IF_ERROR(run.open(path));
    // Recorded BEFORE the first write: a run that fails halfway still has to be
    // unlinked, and remove_runs() can only clean up what it knows about.
    run_paths_.push_back(path);
    if (!records_.empty()) {
        RETURN_IF_ERROR(run.append(Slice(records_)));
    }
    RETURN_IF_ERROR(run.close());

    // Keep the capacity: the ceiling is meant to hold the footprint flat, not to
    // make it oscillate between empty and full.
    records_.clear();
    return Status::OK();
}

size_t BkdBuilder::records_per_cursor(size_t run_count) const {
    DORIS_CHECK_GT(run_count, 0U);
    // run_count x this <= max_points_ by construction, except at the degenerate
    // floor of one record per cursor -- which is reached only when max_points_ is
    // smaller than the fan-in itself. The overshoot there is a CONSTANT couple of
    // records, not the run_count-proportional growth the fold exists to remove.
    return std::max<size_t>(1, max_points_ / run_count);
}

Status BkdBuilder::merge_group_into_run(const std::vector<std::string>& group,
                                        uint32_t cursor_records, BkdStats* stats,
                                        std::string* out) {
    DORIS_CHECK_GE(group.size(), 2U);
    static std::atomic<uint64_t> sequence {0};
    const std::string path = writer::resolve_temp_dir() + "/snii_bkd_fold_" +
                             std::to_string(::getpid()) + "_" +
                             std::to_string(sequence.fetch_add(1)) + ".run";
    // Handed back BEFORE a byte is written: a fold that fails halfway still has
    // to leave a path its caller can register for removal.
    *out = path;

    std::unique_ptr<MergingPointSource> source;
    RETURN_IF_ERROR(MergingPointSource::create(group, static_cast<uint32_t>(record_size_),
                                               options_.points_per_leaf, cursor_records, &source));
    // Measured from the cursors this fold actually opened.
    stats->peak_merge_buffer_bytes =
            std::max<uint64_t>(stats->peak_merge_buffer_bytes, source->resident_buffer_bytes());
    PointRunWriter run;
    RETURN_IF_ERROR(run.open(path));
    while (true) {
        Slice records;
        RETURN_IF_ERROR(source->next_block(options_.points_per_leaf, &records));
        if (records.empty()) {
            break;
        }
        RETURN_IF_ERROR(run.append(records));
    }
    return run.close();
}

Status BkdBuilder::fold_runs_to_fan_in(size_t fan_in, BkdStats* stats) {
    DORIS_CHECK_GE(fan_in, 2U);
    while (run_paths_.size() > fan_in) {
        const size_t per_run = records_per_cursor(fan_in);
        std::vector<std::string> folded;
        Status status;
        size_t begin = 0;
        for (; begin < run_paths_.size(); begin += fan_in) {
            const size_t end = std::min(begin + fan_in, run_paths_.size());
            if (end - begin == 1) {
                // An odd tail carries forward untouched; rewriting it would cost
                // a full copy to achieve nothing.
                folded.push_back(run_paths_[begin]);
                continue;
            }
            const std::vector<std::string> group(run_paths_.begin() + cast_set<ptrdiff_t>(begin),
                                                 run_paths_.begin() + cast_set<ptrdiff_t>(end));
            std::string merged;
            status = merge_group_into_run(group, static_cast<uint32_t>(per_run), stats, &merged);
            if (!merged.empty()) {
                folded.push_back(merged);
            }
            if (!status.ok()) {
                break;
            }
            // Unlinked only once its replacement is complete, so disk holds one
            // extra copy of a GROUP at a time -- never one extra copy per pass.
            for (const std::string& path : group) {
                ::unlink(path.c_str());
            }
        }
        if (!status.ok()) {
            // Everything from `begin` on is still on disk and unmerged. It must
            // stay in run_paths_ or remove_runs() would leak it.
            folded.insert(folded.end(), run_paths_.begin() + cast_set<ptrdiff_t>(begin),
                          run_paths_.end());
            run_paths_ = std::move(folded);
            return status;
        }
        run_paths_ = std::move(folded);
        ++stats->merge_passes;
    }
    return Status::OK();
}

void BkdBuilder::remove_runs() {
    for (const std::string& path : run_paths_) {
        ::unlink(path.c_str());
    }
    run_paths_.clear();
}

BkdBuilder::~BkdBuilder() {
    // An abandoned build (an error between add() and finish(), or a caller that
    // simply drops the builder) must not leave runs in the temp dir.
    remove_runs();
}

Status BkdBuilder::reserve_points(size_t point_count) {
    const size_t needed = point_count * record_size_;
    if (needed <= records_.capacity()) {
        return Status::OK();
    }
    // Geometric growth, then clamped to the configured ceiling: the buffer must not
    // overshoot build_buffer_bytes even transiently, or the bound design 6.2 promises
    // would only hold for the logical size and not for the RSS.
    size_t target = std::max(needed, records_.capacity() * 2);
    target = std::max(target, kInitialRecordBufferPoints * record_size_);
    target = std::min(target, max_points_ * record_size_);
    // add() rejects a point beyond max_points_ before calling here, so the clamp
    // above can never land below what this point needs.
    DCHECK_GE(target, needed);

    if (options_.reporter != nullptr) {
        // Charges the new buffer WHILE the old one is still charged, which is exactly
        // the transient double residency of a vector growth. Only after the physical
        // move succeeds does the old charge go away.
        writer::MemoryReporter::Reservation replacement;
        RETURN_IF_ERROR(reservation_.prepare_replacement(target, &replacement));
        records_.reserve(target);
        reservation_ = std::move(replacement);
        return Status::OK();
    }
    records_.reserve(target);
    return Status::OK();
}

Status BkdBuilder::write_index(PointSource* source, io::FileWriter* data_out, ByteSink* index_out,
                               BkdStats* stats) {
    // bytes_written() is the offset truth (io/file_writer.h), so the leaf directory is
    // anchored to wherever this build's bkd_data begins inside the container.
    const uint64_t data_start = data_out->bytes_written();
    const size_t index_start = index_out->size();
    const size_t bytes_per_dim = options_.bytes_per_dim;

    std::vector<LeafRef> leaves;
    std::vector<uint8_t> min_value;
    std::vector<uint8_t> max_value;
    std::vector<uint8_t> split_values;
    // Reused across leaves: one buffer for the whole build instead of one per leaf.
    ByteSink leaf_block;
    uint64_t leaf_offset = 0;
    uint64_t point_count = 0;

    // Design 6.4: cut every points_per_leaf points, let the last leaf keep the
    // remainder, and do NOT round the leaf count up to a power of two -- an ordered
    // split array has no complete-binary-tree requirement, so the configured capacity
    // is the real capacity instead of an upper bound that repeated halving dilutes.
    while (true) {
        Slice block;
        RETURN_IF_ERROR(source->next_block(options_.points_per_leaf, &block));
        if (block.empty()) {
            break;
        }
        DCHECK_EQ(block.size() % record_size_, 0UL);
        const uint32_t count = static_cast<uint32_t>(block.size() / record_size_);
        const uint8_t* first_value = block.data();
        const uint8_t* last_value = block.data() + (count - 1) * record_size_;

        if (leaves.empty()) {
            min_value.assign(first_value, first_value + bytes_per_dim);
        } else {
            // The boundary between leaf i and leaf i + 1 is leaf i + 1's FIRST value,
            // i.e. leaf i + 1 covers [split(i), split(i + 1)).
            split_values.insert(split_values.end(), first_value, first_value + bytes_per_dim);
        }
        // The stream is ordered, so whichever leaf turns out to be last leaves the
        // global maximum behind.
        max_value.assign(last_value, last_value + bytes_per_dim);

        leaf_block.clear();
        encode_leaf_block(block, options_.bytes_per_dim, &leaf_block);
        RETURN_IF_ERROR(data_out->append(leaf_block.view()));
        leaves.push_back(LeafRef {.offset = leaf_offset, .count = count});
        leaf_offset += leaf_block.size();
        point_count += count;
    }
    DORIS_CHECK_LE(leaves.size(), static_cast<size_t>(std::numeric_limits<uint32_t>::max()));

    BkdIndexHeader header;
    header.format_version = kFormatVersion;
    // Phase 1 finishes entirely in RAM, so index_flags::kBuiltWithSpill stays clear.
    header.flags = 0;
    header.bytes_per_dim = options_.bytes_per_dim;
    header.field_type = options_.field_type;
    header.point_count = point_count;
    header.doc_count = doc_count_;
    // 0 leaves is the empty index (design 5.3): header only, zero-length bkd_data.
    header.leaf_count = static_cast<uint32_t>(leaves.size());
    header.points_per_leaf = options_.points_per_leaf;
    encode_bkd_index_block(header, Slice(min_value), Slice(max_value), Slice(split_values), leaves,
                           index_out);

    stats->point_count = point_count;
    stats->doc_count = doc_count_;
    stats->leaf_count = header.leaf_count;
    stats->index_bytes = index_out->size() - index_start;
    stats->data_bytes = data_out->bytes_written() - data_start;
    stats->built_with_spill = false;
    return Status::OK();
}

void BkdBuilder::release_records() {
    std::vector<uint8_t>().swap(records_);
    reservation_.reset();
}

} // namespace doris::snii::bkd
