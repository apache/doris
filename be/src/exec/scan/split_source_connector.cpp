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

#include "exec/scan/split_source_connector.h"

#include <gen_cpp/Status_types.h>

#include "runtime/exec_env.h"
#include "runtime/query_context.h"

namespace doris {

using apache::thrift::transport::TTransportException;

Status LocalSplitSourceConnector::get_next(bool* has_next, TFileRangeDesc* range,
                                           const std::atomic_bool* /*scanner_stop*/) {
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

Status RemoteSplitSourceConnector::get_next(bool* has_next, TFileRangeDesc* range,
                                            const std::atomic_bool* scanner_stop) {
    std::lock_guard<std::mutex> l(_range_lock);
    *has_next = false;
    if (_scan_index == _scan_ranges.size() && !_last_batch) {
        SCOPED_TIMER(_get_split_timer);
        TFetchSplitBatchRequest request;
        request.__set_split_source_id(_split_source_id);
        request.__set_max_num_splits(config::remote_split_source_batch_size);
        TFetchSplitBatchResult result;
        do {
            RETURN_IF_CANCELLED(_state);
            // A local LIMIT stops this scanner without cancelling the query. Do not keep a scan
            // worker in the retry loop while another branch of the same query remains active.
            if (scanner_stop->load(std::memory_order_acquire)) {
                return Status::OK();
            }
            Status coord_status;
            // Each FE call is bounded by the split-fetch wait interval. Release the client between
            // retries so a slow listing does not reserve a connection for the whole query.
            FrontendServiceConnection coord(_state->exec_env()->frontend_client_cache(),
                                            _state->get_query_ctx()->coord_addr, &coord_status);
            RETURN_IF_ERROR(coord_status);
            try {
                coord->fetchSplitBatch(result, request);
                if (result.__isset.status && result.status.status_code == TStatusCode::INCOMPLETE) {
                    // One Hive partition may have supplied the first range while another is still
                    // listing files. FE's bounded wait is not EOF or a query failure in that case.
                    continue;
                }
                if (result.__isset.status && result.status.status_code != TStatusCode::OK) {
                    return Status::IOError<false>("Failed to get batch of split source: {}",
                                                  result.status.error_msgs.empty()
                                                          ? "unknown error"
                                                          : result.status.error_msgs[0]);
                }
            } catch (std::exception& e) {
                return Status::IOError<false>("Failed to get batch of split source: {}", e.what());
            }
            break;
        } while (true);
        _last_batch = result.splits.empty();
        _merge_ranges<TScanRangeLocations>(_scan_ranges, result.splits);
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

} // namespace doris
