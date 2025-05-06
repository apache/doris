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

#include "pipeline/exec/doris_adbc_scan_operator.h"

#include "common/object_pool.h"
#include "vec/exec/scan/doris_adbc_scanner.h"

namespace doris::pipeline {
#include "common/compile_check_begin.h"
std::string DorisAdbcScanLocalState::name_suffix() const {
    return fmt::format(" (id={}. table name = {})", std::to_string(_parent->node_id()),
                       _parent->cast<DorisAdbcScanOperatorX>()._table_name);
}

Status DorisAdbcScanLocalState::_init_scanners(std::list<vectorized::ScannerSPtr>* scanners) {
    if (_scan_ranges.empty()) {
        _eos = true;
        _scan_dependency->set_ready();
        return Status::OK();
    }

    auto& p = Base::_parent->cast<DorisAdbcScanOperatorX>();
    for (auto& doris_arrow_scan_range : _scan_ranges) {
        // Collect the information from scan range to scanner
        std::shared_ptr<vectorized::NewDorisAdbcScanner> scanner = vectorized::NewDorisAdbcScanner::create_shared(
                _state, this, p._limit, p._tuple_id, doris_arrow_scan_range->uri_str,
                doris_arrow_scan_range->ticket,
                _state->runtime_profile());

        RETURN_IF_ERROR(scanner->prepare(_state, Base::_conjuncts));
        scanners->push_back(scanner);
    }

    return Status::OK();
}

void DorisAdbcScanLocalState::set_scan_ranges(RuntimeState* state,
                                       const std::vector<TScanRangeParams>& scan_ranges) {
    for (auto& doris_arrow_scan_range : scan_ranges) {
        DCHECK(doris_arrow_scan_range.scan_range.__isset.doris_arrow_scan_range);
        _scan_ranges.emplace_back(new TDorisArrowScanRange(doris_arrow_scan_range.scan_range.doris_arrow_scan_range));
    }
}

DorisAdbcScanOperatorX::DorisAdbcScanOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                                 const DescriptorTbl& descs, int parallel_tasks)
        : ScanOperatorX<DorisAdbcScanLocalState>(pool, tnode, operator_id, descs, parallel_tasks),
          _table_name(tnode.doris_adbc_scan_node.table_name),
          _tuple_id(tnode.doris_adbc_scan_node.tuple_id) {
    _output_tuple_id = tnode.doris_adbc_scan_node.tuple_id;
}

} // namespace doris::pipeline
