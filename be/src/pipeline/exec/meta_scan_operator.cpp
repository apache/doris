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

#include "pipeline/exec/meta_scan_operator.h"

#include "vec/exec/scan/vmeta_scanner.h"

namespace doris::pipeline {

Status MetaScanLocalState::_init_scanners(std::list<vectorized::VScannerSPtr>* scanners) {
    if (Base::_eos_dependency->read_blocked_by() == nullptr) {
        return Status::OK();
    }

    auto& p = _parent->cast<MetaScanOperatorX>();

    for (auto& scan_range : _scan_ranges) {
        std::shared_ptr<vectorized::VMetaScanner> scanner = vectorized::VMetaScanner::create_shared(
                state(), this, p._tuple_id, scan_range, p._limit_per_scanner, profile(),
                p._user_identity);
        RETURN_IF_ERROR(scanner->prepare(state(), _conjuncts));
        scanners->push_back(scanner);
    }

    return Status::OK();
}

void MetaScanLocalState::set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) {
    _scan_ranges = scan_ranges;
}

Status MetaScanLocalState::_process_conjuncts() {
    return Status::OK();
}

MetaScanOperatorX::MetaScanOperatorX(ObjectPool* pool, const TPlanNode& tnode,
                                     const DescriptorTbl& descs)
        : ScanOperatorX<MetaScanLocalState>(pool, tnode, descs),
          _tuple_id(tnode.meta_scan_node.tuple_id) {
    _output_tuple_id = _tuple_id;
    if (tnode.meta_scan_node.__isset.current_user_ident) {
        _user_identity = tnode.meta_scan_node.current_user_ident;
    }
}

} // namespace doris::pipeline
