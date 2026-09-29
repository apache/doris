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

#include "storage/index/snii/bkd/point_merger.h"

#include <algorithm>
#include <cstring>

#include "common/check.h"

namespace doris::snii::bkd {

MergingPointSource::MergingPointSource(uint32_t record_size, uint32_t block_records)
        : record_size_(record_size), block_records_(block_records) {}

Status MergingPointSource::create(const std::vector<std::string>& run_paths, uint32_t record_size,
                                  uint32_t block_records, uint32_t buffer_records_per_run,
                                  std::unique_ptr<MergingPointSource>* out) {
    DORIS_CHECK(out != nullptr);
    DORIS_CHECK_GT(record_size, 0U);
    DORIS_CHECK_GT(block_records, 0U);
    DORIS_CHECK_GT(buffer_records_per_run, 0U);

    std::unique_ptr<MergingPointSource> source(new MergingPointSource(record_size, block_records));
    for (const std::string& path : run_paths) {
        auto reader = std::make_unique<PointRunReader>();
        RETURN_IF_ERROR(reader->open(path, record_size, buffer_records_per_run));
        // An empty run never enters the heap. Doing that here rather than at pop
        // time is what keeps the heap invariant "every member has a current
        // record", so sorts_after never has to ask whether one exists.
        if (!reader->exhausted()) {
            source->heap_.push_back(static_cast<uint32_t>(source->readers_.size()));
        }
        source->readers_.push_back(std::move(reader));
    }

    MergingPointSource* raw = source.get();
    std::make_heap(raw->heap_.begin(), raw->heap_.end(),
                   [raw](uint32_t a, uint32_t b) { return raw->sorts_after(a, b); });
    *out = std::move(source);
    return Status::OK();
}

bool MergingPointSource::sorts_after(uint32_t a, uint32_t b) const {
    const Slice left = readers_[a]->current();
    const Slice right = readers_[b]->current();
    // Fixed width, so one memcmp IS the (value, doc_id) order. Ties are records
    // that are byte-identical, and which copy is emitted first cannot be
    // observed -- so no tiebreak on the run index is needed for determinism of
    // the OUTPUT, only for that of the heap, which nothing depends on.
    return std::memcmp(left.data(), right.data(), record_size_) > 0;
}

Status MergingPointSource::next_block(uint32_t max_points, Slice* records) {
    DORIS_CHECK(records != nullptr);
    DORIS_CHECK_GT(max_points, 0U);

    const auto after = [this](uint32_t a, uint32_t b) { return sorts_after(a, b); };
    const size_t wanted = std::min<size_t>(max_points, block_records_);
    block_.clear();
    block_.reserve(wanted * record_size_);

    while (block_.size() / record_size_ < wanted && !heap_.empty()) {
        std::pop_heap(heap_.begin(), heap_.end(), after);
        const uint32_t run = heap_.back();
        const Slice record = readers_[run]->current();
        block_.insert(block_.end(), record.data(), record.data() + record.size());

        RETURN_IF_ERROR(readers_[run]->advance());
        if (readers_[run]->exhausted()) {
            heap_.pop_back();
        } else {
            // The cursor moved, so this run's key changed; re-establishing the
            // heap from the back is what push_heap is for.
            std::push_heap(heap_.begin(), heap_.end(), after);
        }
    }

    *records = Slice(block_.data(), block_.size());
    return Status::OK();
}

} // namespace doris::snii::bkd
