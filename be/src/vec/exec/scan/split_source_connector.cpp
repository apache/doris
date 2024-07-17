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

#include "vec/exec/scan/split_source_connector.h"

#include "runtime/exec_env.h"
#include "runtime/query_context.h"

namespace doris::vectorized {

using apache::thrift::transport::TTransportException;

Status LocalSplitSourceConnector::get_next(bool* has_next, TFileRangeDesc* range) {
    std::lock_guard<std::mutex> l(_range_lock);
    *has_next = false;
    if (_scan_index < _scan_ranges.size()) {
        auto& ranges = _scan_ranges[_scan_index].scan_range.ext_scan_range.file_scan_range.ranges;
        if (_range_index < ranges.size()) {
            *has_next = true;
            *range = ranges[_range_index++];
            if (_range_index == ranges.size()) {
                _scan_index++;
                _range_index = 0;
            }
        }
    }
    return Status::OK();
}

Status RemoteSplitSourceConnector::get_next(bool* has_next, TFileRangeDesc* range) {
    std::lock_guard<std::mutex> l(_range_lock);
    *has_next = false;
    if (_scan_index == _scan_ranges.size() && !_last_batch) {
        SCOPED_TIMER(_get_split_timer);
        Status coord_status;
        FrontendServiceConnection coord(_state->exec_env()->frontend_client_cache(),
                                        _state->get_query_ctx()->coord_addr, &coord_status);
        RETURN_IF_ERROR(coord_status);
        TFetchSplitBatchRequest request;
        request.__set_split_source_id(_split_source_id);
        request.__set_max_num_splits(config::remote_split_source_batch_size);
        TFetchSplitBatchResult result;
        try {
            coord->fetchSplitBatch(result, request);
        } catch (std::exception& e) {
            return Status::IOError<false>("Failed to get batch of split source: {}", e.what());
        }
        _last_batch = result.splits.empty();
        _scan_ranges = result.splits;
        _scan_index = 0;
        _range_index = 0;
    }
    if (_scan_index < _scan_ranges.size()) {
        auto& ranges = _scan_ranges[_scan_index].scan_range.ext_scan_range.file_scan_range.ranges;
        if (_range_index < ranges.size()) {
            *has_next = true;
            *range = ranges[_range_index++];
            if (_range_index == ranges.size()) {
                _scan_index++;
                _range_index = 0;
            }
        }
    }
    return Status::OK();
}

} // namespace doris::vectorized
