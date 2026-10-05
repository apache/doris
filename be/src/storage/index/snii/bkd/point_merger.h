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
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/bkd/point_run.h"
#include "storage/index/snii/bkd/point_source.h"
#include "storage/index/snii/common/slice.h"

// The spilling half of design 6.2: a k-way merge over spilled runs, presented as
// an ordinary PointSource so the leaf-cutting loop cannot tell the two build
// modes apart.
namespace doris::snii::bkd {

class MergingPointSource final : public PointSource {
public:
    // Opens one cursor per run. The caller keeps ownership of the files,
    // including their removal -- this type never unlinks anything, so a failed
    // build leaves the runs where a human can look at them.
    //
    // Resident footprint is (runs x buffer_records_per_run + block_records)
    // records, which is what keeps the merge bounded independently of how much
    // was spilled.
    static Status create(const std::vector<std::string>& run_paths, uint32_t record_size,
                         uint32_t block_records, uint32_t buffer_records_per_run,
                         std::unique_ptr<MergingPointSource>* out);

    ~MergingPointSource() override = default;

    Status next_block(uint32_t max_points, Slice* records) override;

    // Summed over every open cursor: the merge's actual resident footprint,
    // excluding the leaf block which is accounted separately. This is a
    // MEASUREMENT -- recomputing it from create()'s arguments would only restate
    // the request, which is exactly what the bound needs to be checked against.
    uint64_t resident_buffer_bytes() const {
        uint64_t total = 0;
        for (const auto& reader : readers_) {
            total += reader->resident_buffer_bytes();
        }
        return total;
    }

private:
    MergingPointSource(uint32_t record_size, uint32_t block_records);

    // True when the record under cursor `a` sorts AFTER the one under `b`, which
    // is what std::push_heap/pop_heap need to behave as a min-heap. Records are
    // fixed width, so one memcmp IS the (value, doc_id) order.
    bool sorts_after(uint32_t a, uint32_t b) const;

    uint32_t record_size_;
    uint32_t block_records_;
    std::vector<std::unique_ptr<PointRunReader>> readers_;
    // Indices into readers_, kept as a min-heap over the record each one is
    // positioned on. An exhausted reader is simply absent.
    std::vector<uint32_t> heap_;
    // The contiguous view handed out by next_block, valid until the next call.
    std::vector<uint8_t> block_;
};

} // namespace doris::snii::bkd
