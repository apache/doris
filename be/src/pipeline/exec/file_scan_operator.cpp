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
} // namespace doris::pipeline
