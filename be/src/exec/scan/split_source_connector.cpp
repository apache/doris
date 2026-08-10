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

#include "io/cache/cached_remote_file_reader.h"
#include "runtime/exec_env.h"
#include "runtime/query_context.h"

namespace doris {

using apache::thrift::transport::TTransportException;

Status SplitSourceConnector::get_next_split(bool* has_next, FileScanSplitTask* task) {
    DORIS_CHECK(task != nullptr);
    task->context.reset();
    RETURN_IF_ERROR(get_next(has_next, &task->range));
    if (*has_next) {
        std::lock_guard lock(_scan_range_lock);
        _mark_parent_claimed(task->range);
    }
    return Status::OK();
}

bool SplitSourceConnector::_take_generated_split(FileScanSplitTask* task) {
    if (_generated_splits.empty()) {
        return false;
    }
    *task = std::move(_generated_splits.front());
    _generated_splits.pop_front();
    return true;
}

void SplitSourceConnector::_mark_parent_claimed(const TFileRangeDesc& range) {
    if (range.__isset.is_file_parent && range.is_file_parent) {
        ++_active_file_parents;
    }
}

Status SplitSourceConnector::finish_file_parent(std::vector<FileScanSplitTask> children) {
    std::lock_guard lock(_scan_range_lock);
    if (_active_file_parents == 0) {
        return Status::InternalError("No active file parent task to finish");
    }
    for (auto& child : children) {
        _generated_splits.push_back(std::move(child));
    }
    --_active_file_parents;
    _range_ready.notify_all();
    return Status::OK();
}

std::shared_ptr<io::FileScannerV2ReaderLocalCache>
SplitSourceConnector::get_or_create_reader_local_cache(
        size_t capacity, std::shared_ptr<MemTrackerLimiter> query_mem_tracker) {
    std::lock_guard lock(_scan_range_lock);
    if (_reader_local_cache == nullptr) {
        _reader_local_cache = std::make_shared<io::FileScannerV2ReaderLocalCache>(
                capacity, std::move(query_mem_tracker));
    }
    return _reader_local_cache;
}

Status LocalSplitSourceConnector::get_next(bool* has_next, TFileRangeDesc* range) {
    std::lock_guard<std::mutex> l(_scan_range_lock);
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

Status LocalSplitSourceConnector::get_next_split(bool* has_next, FileScanSplitTask* task) {
    DORIS_CHECK(has_next != nullptr && task != nullptr);
    std::unique_lock lock(_scan_range_lock);
    while (true) {
        if (_take_generated_split(task)) {
            *has_next = true;
            return Status::OK();
        }
        if (_scan_index < _scan_ranges.size()) {
            auto& ranges =
                    _scan_ranges[_scan_index].scan_range.ext_scan_range.file_scan_range.ranges;
            if (_range_index < ranges.size()) {
                task->range = ranges[_range_index++];
                task->context.reset();
                _mark_parent_claimed(task->range);
                if (_range_index == ranges.size()) {
                    ++_scan_index;
                    _range_index = 0;
                }
                *has_next = true;
                return Status::OK();
            }
        }
        if (_active_file_parents == 0) {
            *has_next = false;
            return Status::OK();
        }
        _range_ready.wait(
                lock, [this] { return !_generated_splits.empty() || _active_file_parents == 0; });
    }
}

Status RemoteSplitSourceConnector::get_next(bool* has_next, TFileRangeDesc* range) {
    std::lock_guard<std::mutex> l(_scan_range_lock);
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

Status RemoteSplitSourceConnector::get_next_split(bool* has_next, FileScanSplitTask* task) {
    DORIS_CHECK(has_next != nullptr && task != nullptr);
    std::unique_lock lock(_scan_range_lock);
    while (true) {
        if (_take_generated_split(task)) {
            *has_next = true;
            return Status::OK();
        }
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
            auto& ranges =
                    _scan_ranges[_scan_index].scan_range.ext_scan_range.file_scan_range.ranges;
            if (_range_index < ranges.size()) {
                task->range = ranges[_range_index++];
                task->context.reset();
                _mark_parent_claimed(task->range);
                if (_range_index == ranges.size()) {
                    ++_scan_index;
                    _range_index = 0;
                }
                *has_next = true;
                return Status::OK();
            }
        }
        if (!_last_batch) {
            continue;
        }
        if (_active_file_parents == 0) {
            *has_next = false;
            return Status::OK();
        }
        _range_ready.wait(
                lock, [this] { return !_generated_splits.empty() || _active_file_parents == 0; });
    }
}

} // namespace doris
