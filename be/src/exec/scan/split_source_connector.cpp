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

#include "runtime/exec_env.h"
#include "runtime/query_context.h"

namespace doris {

using apache::thrift::transport::TTransportException;

Status SplitSourceConnector::get_next_split(bool* has_next, FileScanSplit* split) {
    DORIS_CHECK(has_next != nullptr);
    DORIS_CHECK(split != nullptr);
    std::unique_lock lock(_split_lock);
    while (true) {
        *has_next = false;
        if (_stopped) {
            return Status::OK();
        }
        if (!_generated_splits.empty()) {
            *split = std::move(_generated_splits.front());
            _generated_splits.pop_front();
            *has_next = true;
            return Status::OK();
        }
        if (_source_exhausted) {
            if (_active_source_splits.empty()) {
                return Status::OK();
            }
            _split_ready.wait(lock);
            continue;
        }
        if (_source_claim_in_progress) {
            _split_ready.wait(lock);
            continue;
        }

        TFileRangeDesc range;
        bool has_source = false;
        // Record the in-flight claim before dropping the queue lock, so another scanner cannot
        // report raw EOS before this claim becomes an active source. The potentially blocking
        // remote RPC must not hold the queue lock because a footer producer may publish children
        // while that fetch is in flight.
        _source_claim_in_progress = true;
        lock.unlock();
        const auto source_status = get_next(&has_source, &range);
        lock.lock();
        _source_claim_in_progress = false;
        if (source_status.ok() && !has_source) {
            // Both local exhaustion and an empty final remote batch are terminal. Remember EOS so
            // parent waiters do not wake each other into repeated empty source fetches.
            _source_exhausted = true;
        }
        _split_ready.notify_all();
        RETURN_IF_ERROR(source_status);
        if (_stopped) {
            return Status::OK();
        }
        if (has_source) {
            // A scanner reuses its output envelope across files. Reset child-only shared range and
            // context state before installing a raw FE range, or materialization can read the
            // previous child's file instead of this source.
            *split = {};
            split->range = std::move(range);
            split->is_source_split = true;
            split->source_split_id = _next_source_split_id++;
            split->source_progress = std::make_shared<SourceSplitProgress>();
            _active_source_splits.insert(split->source_split_id);
            *has_next = true;
            return Status::OK();
        }
        if (_active_source_splits.empty()) {
            return Status::OK();
        }
        _split_ready.wait(lock);
    }
}

Status SplitSourceConnector::finish_source_split(const FileScanSplit& source_split,
                                                 std::vector<FileScanSplit> generated_splits) {
    if (!source_split.is_source_split || source_split.source_split_id == 0) {
        return Status::InvalidArgument("Only an active source split can publish generated splits");
    }
    {
        std::lock_guard lock(_split_lock);
        if (_active_source_splits.erase(source_split.source_split_id) != 1) {
            return Status::InvalidArgument("Source split {} is not active",
                                           source_split.source_split_id);
        }
        if (!_stopped) {
            if (!generated_splits.empty()) {
                DORIS_CHECK(source_split.source_progress != nullptr);
                source_split.source_progress->reset_for_children(generated_splits.size());
            }
            for (auto& split : generated_splits) {
                split.is_source_split = false;
                split.source_split_id = 0;
                split.source_progress = source_split.source_progress;
                _generated_splits.push_back(std::move(split));
            }
        }
    }
    _split_ready.notify_all();
    return Status::OK();
}

void SplitSourceConnector::stop() {
    {
        std::lock_guard lock(_split_lock);
        _stopped = true;
        // Cancellation must release queued descriptors and footer contexts immediately; source
        // producers finishing later will only retire their reservation without publishing work.
        _generated_splits.clear();
    }
    _split_ready.notify_all();
}

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
        // No need to set timeout because on FE side, there is a max fetch time
        FrontendServiceConnection coord(_state->exec_env()->frontend_client_cache(),
                                        _state->get_query_ctx()->coord_addr, &coord_status);
        RETURN_IF_ERROR(coord_status);
        TFetchSplitBatchRequest request;
        request.__set_split_source_id(_split_source_id);
        request.__set_max_num_splits(config::remote_split_source_batch_size);
        TFetchSplitBatchResult result;
        try {
            coord->fetchSplitBatch(result, request);
            if (result.__isset.status && result.status.status_code != TStatusCode::OK) {
                return Status::IOError<false>("Failed to get batch of split source: {}",
                                              result.status.error_msgs.empty()
                                                      ? "unknown error"
                                                      : result.status.error_msgs[0]);
            }
        } catch (std::exception& e) {
            return Status::IOError<false>("Failed to get batch of split source: {}", e.what());
        }
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
