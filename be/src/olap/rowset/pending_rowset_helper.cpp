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

#include "olap/rowset/pending_rowset_helper.h"

#include "olap/olap_common.h"

namespace doris {

PendingRowsetGuard::~PendingRowsetGuard() {
    if (_pending_rowset_set) {
        _pending_rowset_set->remove(_rowset_id);
    }
}

PendingRowsetGuard::PendingRowsetGuard(const RowsetId& rowset_id, PendingRowsetSet* set)
        : _rowset_id(rowset_id), _pending_rowset_set(set) {}

PendingRowsetGuard::PendingRowsetGuard(PendingRowsetGuard&& other) noexcept {
    CHECK(!_pending_rowset_set ||
          (_rowset_id == other._rowset_id && _pending_rowset_set == other._pending_rowset_set))
            << _rowset_id << ' ' << other._rowset_id << ' ' << _pending_rowset_set << ' '
            << other._pending_rowset_set;
    _rowset_id = other._rowset_id;
    _pending_rowset_set = other._pending_rowset_set;
    other._pending_rowset_set = nullptr;
}

PendingRowsetGuard& PendingRowsetGuard::operator=(PendingRowsetGuard&& other) noexcept {
    CHECK(!_pending_rowset_set ||
          (_rowset_id == other._rowset_id && _pending_rowset_set == other._pending_rowset_set))
            << _rowset_id << ' ' << other._rowset_id << ' ' << _pending_rowset_set << ' '
            << other._pending_rowset_set;
    _rowset_id = other._rowset_id;
    _pending_rowset_set = other._pending_rowset_set;
    other._pending_rowset_set = nullptr;
    return *this;
}

void PendingRowsetGuard::drop() {
    if (_pending_rowset_set) {
        _pending_rowset_set->remove(_rowset_id);
    }
    _pending_rowset_set = nullptr;
    _rowset_id = RowsetId {};
}

bool PendingRowsetSet::contains(const RowsetId& rowset_id) {
    std::lock_guard lock(_mtx);
    return _set.contains(rowset_id);
}

PendingRowsetGuard PendingRowsetSet::add(const RowsetId& rowset_id) {
    {
        std::lock_guard lock(_mtx);
        _set.insert(rowset_id);
    }
    return PendingRowsetGuard {rowset_id, this};
}

void PendingRowsetSet::remove(const RowsetId& rowset_id) {
    std::lock_guard lock(_mtx);
    _set.erase(rowset_id);
}

} // namespace doris
