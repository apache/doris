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

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

#include "olap/rowset/rowset_fwd.h"
#include "olap/rowset/segment_v2/row_ranges.h"
#include "olap/segment_loader.h"
#include "olap/tablet.h"
#include "vec/exec/scan/new_olap_scanner.h"

namespace doris {

namespace pipeline {
class OlapScanLocalState;
}

namespace vectorized {
class VScanner;
}

using VScannerSPtr = std::shared_ptr<vectorized::VScanner>;

class ParallelScannerBuilder {
public:
    ParallelScannerBuilder(pipeline::OlapScanLocalState* parent,
                           const std::vector<TabletWithVersion>& tablets,
                           const std::shared_ptr<RuntimeProfile>& profile,
                           const std::vector<OlapScanRange*>& key_ranges, RuntimeState* state,
                           int64_t limit, bool is_dup_mow_key, bool is_preaggregation)
            : _parent(parent),
              _scanner_profile(profile),
              _state(state),
              _limit(limit),
              _is_dup_mow_key(is_dup_mow_key),
              _is_preaggregation(is_preaggregation),
              _tablets(tablets.cbegin(), tablets.cend()),
              _key_ranges(key_ranges.cbegin(), key_ranges.cend()) {}

    Status build_scanners(std::list<VScannerSPtr>& scanners);

    void set_max_scanners_count(size_t count) { _max_scanners_count = count; }

    void set_min_rows_per_scanner(int64_t size) { _min_rows_per_scanner = size; }

private:
    Status _load();

    Status _build_scanners_by_rowid(std::list<VScannerSPtr>& scanners);

    std::shared_ptr<vectorized::NewOlapScanner> _build_scanner(
            BaseTabletSPtr tablet, int64_t version, const std::vector<OlapScanRange*>& key_ranges,
            TabletReader::ReadSource&& read_source);

    pipeline::OlapScanLocalState* _parent;

    /// Max scanners count limit to build
    size_t _max_scanners_count {16};

    /// Min rows per scanner
    size_t _min_rows_per_scanner {2 * 1024 * 1024};

    size_t _total_rows {};

    size_t _rows_per_scanner {_min_rows_per_scanner};

    std::map<RowsetId, SegmentCacheHandle> _segment_cache_handles;

    std::shared_ptr<RuntimeProfile> _scanner_profile;
    RuntimeState* _state;
    int64_t _limit;
    bool _is_dup_mow_key;
    bool _is_preaggregation;
    std::vector<TabletWithVersion> _tablets;
    std::vector<OlapScanRange*> _key_ranges;
    std::unordered_map<int64_t, TabletReader::ReadSource> _all_read_sources;
};

} // namespace doris
