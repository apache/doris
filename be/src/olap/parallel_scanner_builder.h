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
#include "vec/exec/scan/olap_scanner.h"

namespace doris {

namespace pipeline {
class OlapScanLocalState;
}

namespace vectorized {
class Scanner;
}

using ScannerSPtr = std::shared_ptr<vectorized::Scanner>;

class ParallelScannerBuilder {
public:
    ParallelScannerBuilder(pipeline::OlapScanLocalState* parent,
                           const std::vector<TabletWithVersion>& tablets,
                           std::vector<TabletReader::ReadSource>& read_sources,
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
              _key_ranges(key_ranges.cbegin(), key_ranges.cend()),
              _read_sources(read_sources) {}

    Status build_scanners(std::list<ScannerSPtr>& scanners);

    void set_max_scanners_count(size_t count) { _max_scanners_count = count; }

    void set_min_rows_per_scanner(int64_t size) { _min_rows_per_scanner = size; }

private:
    Status _load();

    Status _build_scanners_by_rowid(std::list<ScannerSPtr>& scanners);

    std::shared_ptr<vectorized::OlapScanner> _build_scanner(
            BaseTabletSPtr tablet, int64_t version, const std::vector<OlapScanRange*>& key_ranges,
            TabletReader::ReadSource&& read_source);

    pipeline::OlapScanLocalState* _parent;

    /// Max scanners count limit to build
    size_t _max_scanners_count {16};

    /// Min rows per scanner
    size_t _min_rows_per_scanner {2 * 1024 * 1024};

    size_t _total_rows {};

    size_t _rows_per_scanner {_min_rows_per_scanner};

    std::map<RowsetId, std::vector<size_t>> _all_segments_rows;

    std::shared_ptr<RuntimeProfile> _scanner_profile;
    RuntimeState* _state;
    int64_t _limit;
    bool _is_dup_mow_key;
    // The flag of preagg's meaning is whether return pre agg data(or partial agg data)
    // PreAgg ON: The storage layer returns partially aggregated data without additional processing. (Fast data reading)
    // for example, if a table is select userid,count(*) from base table.
    // And the user send a query like select userid,count(*) from base table group by userid.
    // then the storage layer do not need do aggregation, it could just return the partial agg data, because the compute layer will do aggregation.
    // PreAgg OFF: The storage layer must complete pre-aggregation and return fully aggregated data. (Slow data reading)
    bool _is_preaggregation;
    std::vector<TabletWithVersion> _tablets;
    std::vector<OlapScanRange*> _key_ranges;
    std::unordered_map<int64_t, TabletReader::ReadSource> _all_read_sources;
    std::vector<TabletReader::ReadSource>& _read_sources;
};

} // namespace doris
