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
#include <roaring/roaring.hh>
#include <vector>

#include "io/cache/cache_block_aware_prefetch_remote_reader.h"
#include "storage/segment/common.h"

namespace doris {
class StorageReadOptions;
}

namespace doris::segment_v2 {

class OrdinalIndexReader;

enum class FileAccessRangeBuildMethod : int { FROM_ROWIDS = 0, ALL_DATA_PAGES = 1 };

struct SegmentCacheBlockPrefetchParams {
    io::CacheBlockPrefetchPolicy policy;
    const StorageReadOptions& read_options;
};

// Builds file access ranges from segment rowids and an ordinal index.
//
// The segment layer knows rowids, while CacheBlockAwarePrefetchRemoteReader only
// understands file ranges. This helper owns the ordinal-index walk that bridges
// the two representations:
// - add_rowids()/finish_by_rowids() consumes selected rowids and emits each
//   touched data page as a FileAccessRange.
// - build_all_data_ranges() emits every data page for full-segment readers such
//   as compaction.
//
// The builder deliberately returns page file ranges, not file-cache block ids.
// A data page may be larger than a file-cache block or may straddle multiple
// blocks. CacheBlockAwarePrefetchRemoteReader expands each [offset, offset+size)
// range into every covered file-cache block and deduplicates them there. Prefetch
// progress is triggered by the file offset of the page being read, not by row
// ordinal, so this helper keeps rowid handling entirely inside the segment layer.
class SegmentFileAccessRangeBuilder {
public:
    SegmentFileAccessRangeBuilder(OrdinalIndexReader* ordinal_index, bool is_forward);

    void reset();
    void add_rowids(const rowid_t* rowids, uint32_t num);
    std::vector<io::FileAccessRange> finish_by_rowids();
    std::vector<io::FileAccessRange> build_all_data_ranges();

    static void add_rowids_from_bitmap(const roaring::Roaring& row_bitmap,
                                       const std::vector<SegmentFileAccessRangeBuilder*>& builders);

private:
    void _append_page_access_range(int page_index);
    void _reverse_if_backward();

    OrdinalIndexReader* _ordinal_index = nullptr;
    bool _is_forward = true;
    std::vector<io::FileAccessRange> _access_ranges;

    int _page_idx = 0;
    int _last_page_idx = -1;
};

} // namespace doris::segment_v2
