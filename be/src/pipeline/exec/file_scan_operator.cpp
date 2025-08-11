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
#include "vec/exec/scan/file_scanner.h"
#include "vec/exec/scan/scanner_context.h"

namespace doris::pipeline {
#include "common/compile_check_begin.h"
Status FileScanLocalState::_init_scanners(std::list<vectorized::ScannerSPtr>* scanners) {
    if (_split_source->num_scan_ranges() == 0) {
        _eos = true;
        return Status::OK();
    }

    auto& id_file_map = state()->get_id_file_map();
    if (id_file_map != nullptr) {
        id_file_map->set_external_scan_params(state()->get_query_ctx(), _max_scanners);
    }

    auto& p = _parent->cast<FileScanOperatorX>();
    // There's only one scan range for each backend in batch split mode. Each backend only starts up one ScanNode instance.
    uint32_t shard_num = std::min(vectorized::ScannerScheduler::get_remote_scan_thread_num() /
                                          p.query_parallel_instance_num(),
                                  _max_scanners);
    shard_num = std::max(shard_num, 1U);
    _kv_cache.reset(new vectorized::ShardedKVCache(shard_num));
    for (int i = 0; i < _max_scanners; ++i) {
        std::unique_ptr<vectorized::FileScanner> scanner = vectorized::FileScanner::create_unique(
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
    auto& p = _parent->cast<FileScanOperatorX>();

    auto calc_max_scanners = [&](int parallel_instance_num) -> int {
        int max_scanners =
                vectorized::ScannerScheduler::get_remote_scan_thread_num() / parallel_instance_num;
        if (should_run_serial()) {
            max_scanners = 1;
        }
        return max_scanners;
    };

    if (scan_ranges.size() == 1) {
        auto scan_range = scan_ranges[0].scan_range.ext_scan_range.file_scan_range;
        if (scan_range.__isset.split_source) {
            p._batch_split_mode = true;
            auto split_source = scan_range.split_source;
            RuntimeProfile::Counter* get_split_timer = ADD_TIMER(custom_profile(), "GetSplitTime");

            _max_scanners = calc_max_scanners(p.query_parallel_instance_num());
            _split_source = std::make_shared<vectorized::RemoteSplitSourceConnector>(
                    state, get_split_timer, split_source.split_source_id, split_source.num_splits,
                    _max_scanners);
        }
    }

    if (!p._batch_split_mode) {
        _max_scanners = calc_max_scanners(p.query_parallel_instance_num());
        if (_split_source == nullptr) {
            _split_source = std::make_shared<vectorized::LocalSplitSourceConnector>(scan_ranges,
                                                                                    _max_scanners);
        }
        // currently the total number of splits in the bach split mode cannot be accurately obtained,
        // so we don't do it in the batch split mode.
        _max_scanners = std::min(_max_scanners, _split_source->num_scan_ranges());
    }

    if (!scan_ranges.empty() &&
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
