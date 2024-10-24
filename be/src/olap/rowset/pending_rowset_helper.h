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

#pragma once

#include <mutex>

#include "olap/olap_common.h"

namespace doris {

class PendingRowsetSet;

class [[nodiscard]] PendingRowsetGuard {
public:
    PendingRowsetGuard() = default;

    // Remove guarded rowset id from `PendingRowsetSet` if it's initialized
    ~PendingRowsetGuard();

    PendingRowsetGuard(const PendingRowsetGuard&) = delete;
    PendingRowsetGuard& operator=(const PendingRowsetGuard&) = delete;

    PendingRowsetGuard(PendingRowsetGuard&& other) noexcept;
    PendingRowsetGuard& operator=(PendingRowsetGuard&& other) noexcept;

    // Remove guarded rowset id from `PendingRowsetSet` if it's initialized and reset the guard to
    // uninitialized state. This be used to manually release the pending rowset, ensure that the
    // guarded rowset is indeed no longer in use.
    void drop();

private:
    friend class PendingRowsetSet;
    explicit PendingRowsetGuard(const RowsetId& rowset_id, PendingRowsetSet* set);

    RowsetId _rowset_id;
    PendingRowsetSet* _pending_rowset_set = nullptr;
};

// Pending rowsets refer to those rowsets that are under construction, and have not been added to
// the tablet. BE storage engine will skip these pending rowsets during garbage collection.
class PendingRowsetSet {
public:
    bool contains(const RowsetId& rowset_id);

    // Developers MUST add the rowset id to `PendingRowsetSet` before writing first row data to a
    // local rowset, and hold returned `PendingRowsetGuard` during rowset construction before
    // adding the rowset to tablet. rowset id will be removed from `PendingRowsetSet` eventually
    // when `PendingRowsetGuard` is destroyed.
    PendingRowsetGuard add(const RowsetId& rowset_id);

private:
    friend class PendingRowsetGuard;
    void remove(const RowsetId& rowset_id);

    // TODO(plat1ko): Use high concurrency data structure
    std::mutex _mtx;
    RowsetIdUnorderedSet _set;
};

} // namespace doris
