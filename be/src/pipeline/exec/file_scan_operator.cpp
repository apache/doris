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
    if (_split_source->num_scan_ranges() == 0) {
        _eos = true;
        return Status::OK();
    }

    auto& p = _parent->cast<FileScanOperatorX>();
    size_t shard_num = std::min<size_t>(
            config::doris_scanner_thread_pool_thread_num / state()->query_parallel_instance_num(),
            _max_scanners);
    shard_num = std::max(shard_num, (size_t)1);
    _kv_cache.reset(new vectorized::ShardedKVCache(shard_num));
    for (int i = 0; i < _max_scanners; ++i) {
        std::unique_ptr<vectorized::VFileScanner> scanner = vectorized::VFileScanner::create_unique(
                state(), this, p._limit, _split_source, _scanner_profile.get(), _kv_cache.get(),
                &_colname_to_value_range, &p._colname_to_slot_id);
        RETURN_IF_ERROR(scanner->prepare(state(), _conjuncts));
        scanners->push_back(std::move(scanner));
    }
    return Status::OK();
}

std::string FileScanLocalState::name_suffix() const {
    return fmt::format(" (id={}. nereids_id={}. table name = {})",
                       std::to_string(_parent->node_id()), std::to_string(_parent->nereids_id()),
                       _parent->cast<FileScanOperatorX>()._table_name);
}

void FileScanLocalState::set_scan_ranges(RuntimeState* state,
                                         const std::vector<TScanRangeParams>& scan_ranges) {
    _max_scanners =
            config::doris_scanner_thread_pool_thread_num / state->query_parallel_instance_num();
    _max_scanners = std::max(std::max(_max_scanners, state->parallel_scan_max_scanners_count()), 1);
    // For select * from table limit 10; should just use one thread.
    if (should_run_serial()) {
        _max_scanners = 1;
    }
    if (scan_ranges.size() == 1) {
        auto scan_range = scan_ranges[0].scan_range.ext_scan_range.file_scan_range;
        if (scan_range.__isset.split_source) {
            auto split_source = scan_range.split_source;
            RuntimeProfile::Counter* get_split_timer = ADD_TIMER(_runtime_profile, "GetSplitTime");
            _split_source = std::make_shared<vectorized::RemoteSplitSourceConnector>(
                    state, get_split_timer, split_source.split_source_id, split_source.num_splits,
                    _max_scanners);
        }
    }
    if (_split_source == nullptr) {
        _split_source =
                std::make_shared<vectorized::LocalSplitSourceConnector>(scan_ranges, _max_scanners);
    }
    _max_scanners = std::min(_max_scanners, _split_source->num_scan_ranges());
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
    SCOPED_TIMER(_init_timer);
    auto& p = _parent->cast<FileScanOperatorX>();
    _output_tuple_id = p._output_tuple_id;
    return Status::OK();
}

Status FileScanLocalState::_process_conjuncts(RuntimeState* state) {
    RETURN_IF_ERROR(ScanLocalState<FileScanLocalState>::_process_conjuncts(state));
    if (Base::_eos) {
        return Status::OK();
    }
    // TODO: Push conjuncts down to reader.
    return Status::OK();
}

Status FileScanOperatorX::open(RuntimeState* state) {
    RETURN_IF_ERROR(ScanOperatorX<FileScanLocalState>::open(state));
    if (state->get_query_ctx() != nullptr &&
        state->get_query_ctx()->file_scan_range_params_map.contains(node_id())) {
        TFileScanRangeParams& params =
                state->get_query_ctx()->file_scan_range_params_map[node_id()];
        _output_tuple_id = params.dest_tuple_id;
    }
    return Status::OK();
}

} // namespace doris::pipeline
