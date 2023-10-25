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

#include "pipeline/exec/file_scan_operator.h"

#include <fmt/format.h>

#include <memory>

#include "olap/storage_engine.h"
#include "olap/tablet_manager.h"
#include "pipeline/exec/olap_scan_operator.h"
#include "pipeline/exec/scan_operator.h"
#include "vec/exec/format/format_common.h"
#include "vec/exec/scan/vfile_scanner.h"

namespace doris::pipeline {

Status FileScanLocalState::_init_scanners(std::list<vectorized::VScannerSPtr>* scanners) {
    if (_scan_ranges.empty()) {
        Base::_eos_dependency->set_ready_for_read();
        return Status::OK();
    }

    auto& p = _parent->cast<FileScanOperatorX>();
    size_t shard_num =
            std::min<size_t>(config::doris_scanner_thread_pool_thread_num, _scan_ranges.size());
    _kv_cache.reset(new vectorized::ShardedKVCache(shard_num));
    for (auto& scan_range : _scan_ranges) {
        std::unique_ptr<vectorized::VFileScanner> scanner = vectorized::VFileScanner::create_unique(
                state(), this, p._limit_per_scanner,
                scan_range.scan_range.ext_scan_range.file_scan_range, _scanner_profile.get(),
                _kv_cache.get());
        RETURN_IF_ERROR(
                scanner->prepare(_conjuncts, &_colname_to_value_range, &_colname_to_slot_id));
        scanners->push_back(std::move(scanner));
    }
    return Status::OK();
}

void FileScanLocalState::set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) {
    int max_scanners = config::doris_scanner_thread_pool_thread_num;
    if (scan_ranges.size() <= max_scanners) {
        _scan_ranges = scan_ranges;
    } else {
        // There is no need for the number of scanners to exceed the number of threads in thread pool.
        _scan_ranges.clear();
        auto range_iter = scan_ranges.begin();
        for (int i = 0; i < max_scanners && range_iter != scan_ranges.end(); ++i, ++range_iter) {
            _scan_ranges.push_back(*range_iter);
        }
        for (int i = 0; range_iter != scan_ranges.end(); ++i, ++range_iter) {
            if (i == max_scanners) {
                i = 0;
            }
            auto& ranges = _scan_ranges[i].scan_range.ext_scan_range.file_scan_range.ranges;
            auto& merged_ranges = range_iter->scan_range.ext_scan_range.file_scan_range.ranges;
            ranges.insert(ranges.end(), merged_ranges.begin(), merged_ranges.end());
        }
        _scan_ranges.shrink_to_fit();
        LOG(INFO) << "Merge " << scan_ranges.size() << " scan ranges to " << _scan_ranges.size();
    }
    if (scan_ranges.size() > 0 &&
        scan_ranges[0].scan_range.ext_scan_range.file_scan_range.__isset.params) {
        // for compatibility.
        // in new implement, the tuple id is set in prepare phase
        _output_tuple_id =
                scan_ranges[0].scan_range.ext_scan_range.file_scan_range.params.dest_tuple_id;
    }
}

Status FileScanLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(ScanLocalState<FileScanLocalState>::init(state, info));
    auto& p = _parent->cast<FileScanOperatorX>();
    _output_tuple_id = p._output_tuple_id;
    return Status::OK();
}

Status FileScanLocalState::_process_conjuncts() {
    RETURN_IF_ERROR(ScanLocalState<FileScanLocalState>::_process_conjuncts());
    if (Base::_eos_dependency->read_blocked_by() == nullptr) {
        return Status::OK();
    }
    // TODO: Push conjuncts down to reader.
    return Status::OK();
}

Status FileScanOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ScanOperatorX<FileScanLocalState>::prepare(state));
    if (state->get_query_ctx() != nullptr &&
        state->get_query_ctx()->file_scan_range_params_map.contains(node_id())) {
        TFileScanRangeParams& params =
                state->get_query_ctx()->file_scan_range_params_map[node_id()];
        _output_tuple_id = params.dest_tuple_id;
    }
    return Status::OK();
}

} // namespace doris::pipeline
