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

#include "vec/exec/scan/union_scanner.h"

namespace doris::vectorized {

Status UnionScanner::init() {
    for (auto& scanner : _olap_scanners) {
        RETURN_IF_ERROR(scanner->init());
    }
    _is_init = true;
    return Status::OK();
}

Status UnionScanner::prepare(RuntimeState* state, const VExprContextSPtrs& conjuncts) {
    RETURN_IF_ERROR(VScanner::prepare(state, conjuncts));
    for (auto& scanner : _olap_scanners) {
        RETURN_IF_ERROR(scanner->prepare(state, conjuncts));
    }
    return Status::OK();
}

Status UnionScanner::open(RuntimeState* state) {
    RETURN_IF_ERROR(_olap_scanners[0]->open(state));
    return Status::OK();
}

Status UnionScanner::close(RuntimeState* state) {
    if (_scanner_cursor != _olap_scanners.size()) {
        RETURN_IF_ERROR(_olap_scanners[_scanner_cursor]->close(state));
    }

    return Status::OK();
}

void UnionScanner::set_compound_filters(const std::vector<TCondition>& compound_filters) {
    for (auto& scanner : _olap_scanners) {
        scanner->set_compound_filters(compound_filters);
    }
}

TabletStorageType UnionScanner::get_storage_type() {
    return _olap_scanners[0]->get_storage_type();
}

Status UnionScanner::_get_block_impl(RuntimeState* state, Block* block, bool* eos) {
    if (_scanner_cursor == _olap_scanners.size()) [[unlikely]] {
        *eos = true;
        return Status::OK();
    }

    do {
        auto& scanner = _olap_scanners[_scanner_cursor];
        bool inner_eos = false;
        *eos = false;
        RETURN_IF_ERROR(scanner->get_block(state, block, &inner_eos));

        if (inner_eos) {
            scanner->_collect_profile_before_close();
            RETURN_IF_ERROR(scanner->close(state));
            _scanner_cursor++;
            if (_scanner_cursor != _olap_scanners.size()) {
                RETURN_IF_ERROR(_olap_scanners[_scanner_cursor]->open(state));
            } else {
                break;
            }
        }
    } while (block->empty());
    return Status::OK();
}

void UnionScanner::_collect_profile_before_close() {
    if (_scanner_cursor != _olap_scanners.size()) {
        _olap_scanners[_scanner_cursor]->_collect_profile_before_close();
    }
}

} // namespace doris::vectorized