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
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/bkd/bkd_types.h"
#include "storage/index/snii/bkd/point_source.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_sink.h"
#include "storage/index/snii/io/file_writer.h"
#include "storage/index/snii/writer/memory_reporter.h"

// Builds sorted BKD leaves and writes bkd_data and bkd_index.
namespace doris::snii::bkd {

// One-shot builder: create, add points, then finish. Points spill to sorted runs when the resident buffer reaches build_buffer_bytes.
class BkdBuilder {
public:
    // The ONLY way to obtain a builder. Options are checked BEFORE the object
    // exists, so there is no such thing as a constructed-but-invalid builder -- the
    // old bkd_writer instead threw from its constructor and left docs_seen_
    // uninitialized for the caller to fill in from outside.
    static Status create(const BkdBuilderOptions& options, std::unique_ptr<BkdBuilder>* out);

    ~BkdBuilder();

    BkdBuilder(const BkdBuilder&) = delete;
    BkdBuilder& operator=(const BkdBuilder&) = delete;

    // Appends a KeyCoder-encoded value and its ascending doc ID; NULL rows use the separate bitmap. Counts distinct consecutive doc IDs and returns MEM_LIMIT_EXCEEDED if memory growth is refused.
    Status add(uint32_t doc_id, Slice sortable_value);

    // Orders the points, cuts leaves, APPENDS the leaf blocks to `data_out` and the
    // framed bkd_index section to `index_out`, and reports what was written.
    //
    // Leaf offsets are relative to the START of this build's bkd_data, i.e. to
    // data_out->bytes_written() on entry, so the writer may already carry other
    // sub-files of the same container. `data_out` is NOT finalized here: it belongs
    // to the container writer, which keeps appending after this returns.
    //
    // Consumes the builder: it releases its point buffer here and calling add() or
    // finish() again is a caller bug (DORIS_CHECK). No Directory, no IndexOutput --
    // a FileWriter and a ByteSink are the whole output surface (D2).
    Status finish(io::FileWriter* data_out, ByteSink* index_out, BkdStats* stats);

private:
    explicit BkdBuilder(const BkdBuilderOptions& options);

    // Grows the record buffer to hold `point_count` points, pre-charging the
    // MemoryReporter before the allocation happens.
    Status reserve_points(size_t point_count);

    // Sorts the resident buffer and writes it out as one more run, then empties
    // the buffer while KEEPING its capacity -- the point of the ceiling is that
    // the resident footprint stays flat, not that it oscillates.
    Status spill_current_run();

    // Unlinks every run written so far. Called on every exit from finish() and
    // again from the destructor, so an abandoned build leaves nothing behind.
    void remove_runs();

    // Runs a merge that a single pass can window, given the resident ceiling.
    // Above it, folds run_paths_ in groups of that many until the remainder
    // fits, replacing the group with its merged output and unlinking the inputs
    // as it goes -- so disk holds one extra copy of the data at most, never one
    // per pass. Records what it did in *stats.
    Status fold_runs_to_fan_in(size_t fan_in, BkdStats* stats);

    // Merges `group` into one new run file and appends its path to *out. The
    // inputs are NOT unlinked here; the caller owns that, because a failure
    // partway through still has to leave every path it knows about removable.
    Status merge_group_into_run(const std::vector<std::string>& group, uint32_t records_per_cursor,
                                BkdStats* stats, std::string* out);

    // Cursor-window bytes one merge over `run_count` runs would hold, and the
    // per-cursor record window that produces it. Single-sourced so the bound
    // reported in BkdStats cannot drift from the one actually configured.
    size_t records_per_cursor(size_t run_count) const;

    // Writes leaves from either a resident or merged ordered point stream.
    Status write_index(PointSource* source, io::FileWriter* data_out, ByteSink* index_out,
                       BkdStats* stats);

    // Drops the point buffer and its memory charge. Called once the points have been
    // consumed, so a builder awaiting destruction holds nothing.
    void release_records();

    const BkdBuilderOptions options_;
    // Whole-record byte order is (value, doc_id) order.
    const size_t record_size_;
    // build_buffer_bytes expressed in whole records -- the resident ceiling add()
    // enforces. Rounded DOWN, so the buffer never exceeds the configured bound.
    const size_t max_points_;

    // point_count * record_size_ bytes of [value][doc_id big-endian] records, in
    // insertion order until finish() sorts them in place. This IS the point count:
    // no second counter can drift away from it.
    std::vector<uint8_t> records_;
    // Charge held against options_.reporter for records_.capacity(). Default (owner
    // null) when no reporter was supplied, which is legal off-Doris.
    writer::MemoryReporter::Reservation reservation_;

    // Paths of the runs spilled so far, in spill order. Empty means the build
    // never crossed the ceiling and finish() takes the resident fast path.
    std::vector<std::string> run_paths_;

    uint32_t doc_count_ = 0;
    uint32_t last_doc_id_ = 0;
    bool finished_ = false;
};

} // namespace doris::snii::bkd
