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

#include "common/config.h"
#include "runtime/client_cache.h"
#include "runtime/runtime_state.h"

namespace doris::vectorized {

/*
 * Multiple scanners within a scan node share a split source.
 * Each scanner call `get_next` to get the next scan range. A fast scanner will immediately obtain
 * the next scan range, so there is no situation of data skewing.
 */
class SplitSourceConnector {
public:
    SplitSourceConnector() = default;
    virtual ~SplitSourceConnector() = default;

    /**
     * Get the next scan range. has_next should be to true to fetch the next scan range.
     * @param has_next whether exists the next scan range
     * @param range the obtained next scan range
     */
    virtual Status get_next(bool* has_next, TFileRangeDesc* range) = 0;

    virtual int num_scan_ranges() = 0;

    virtual TFileScanRangeParams* get_params() = 0;
};

/**
 * The file splits are already assigned in `TFileScanRange.ranges`. Scan node has need to
 * fetch the scan ranges from frontend.
 *
 * In cases where the number of files is small, the splits are directly transmitted to backend.
 */
class LocalSplitSourceConnector : public SplitSourceConnector {
private:
    std::mutex _range_lock;
    std::vector<TScanRangeParams> _scan_ranges;
    int _scan_index = 0;
    int _range_index = 0;

public:
    LocalSplitSourceConnector(const std::vector<TScanRangeParams>& scan_ranges)
            : _scan_ranges(scan_ranges) {}

    Status get_next(bool* has_next, TFileRangeDesc* range) override;

    int num_scan_ranges() override { return _scan_ranges.size(); }

    TFileScanRangeParams* get_params() override {
        if (_scan_ranges.size() > 0 &&
            _scan_ranges[0].scan_range.ext_scan_range.file_scan_range.__isset.params) {
            // for compatibility.
            return &_scan_ranges[0].scan_range.ext_scan_range.file_scan_range.params;
        }
        LOG(FATAL) << "Unreachable, params is got by file_scan_range_params_map";
    }
};

/**
 * The file splits are lazily generated in frontend, and saved as a split source in frontend.
 * The scan node needs to fetch file splits from the frontend service. Each split source is identified by
 * a unique ID, and the ID is stored in `TFileScanRange.split_source.split_source_id`
 *
 * In the case of a large number of files, backend can scan data while obtaining splits information.
 */
class RemoteSplitSourceConnector : public SplitSourceConnector {
private:
    std::mutex _range_lock;
    RuntimeState* _state;
    RuntimeProfile::Counter* _get_split_timer;
    int64 _split_source_id;
    int _num_splits;

    std::vector<TScanRangeLocations> _scan_ranges;
    bool _last_batch = false;
    int _scan_index = 0;
    int _range_index = 0;

public:
    RemoteSplitSourceConnector(RuntimeState* state, RuntimeProfile::Counter* get_split_timer,
                               int64 split_source_id, int num_splits)
            : _state(state),
              _get_split_timer(get_split_timer),
              _split_source_id(split_source_id),
              _num_splits(num_splits) {}

    Status get_next(bool* has_next, TFileRangeDesc* range) override;

    /*
     * Remote split source is fetched in batch mode, and the splits are generated while scanning,
     * so the number of scan ranges may not be accurate.
     */
    int num_scan_ranges() override { return _num_splits; }

    TFileScanRangeParams* get_params() override {
        LOG(FATAL) << "Unreachable, params is got by file_scan_range_params_map";
    }
};

} // namespace doris::vectorized
