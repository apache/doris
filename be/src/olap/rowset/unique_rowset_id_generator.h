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

#include <atomic>

#include "olap/rowset/rowset_id_generator.h"
#include "util/spinlock.h"
#include "util/uid_util.h"

namespace doris {

class UniqueRowsetIdGenerator : public RowsetIdGenerator {
public:
    UniqueRowsetIdGenerator(const UniqueId& backend_uid);
    ~UniqueRowsetIdGenerator();

    RowsetId next_id() override;

    bool id_in_use(const RowsetId& rowset_id) const override;

    void release_id(const RowsetId& rowset_id) override;

private:
    mutable SpinLock _lock;
    const UniqueId _backend_uid;
    const int64_t _version = 2; // modify it when create new version id generator
    // A monotonically increasing integer generator,
    // This integer will be part of a rowset id.
    std::atomic<int64_t> _inc_id;
    // Save the high part of rowset ids generated since last process startup.
    // Therefore, we cannot strictly rely on _valid_rowset_id_hi
    // to determine whether the rowset id is being used.
    // But to use id_in_use() and release_id() to check it.
    std::unordered_set<int64_t> _valid_rowset_id_hi;

    DISALLOW_COPY_AND_ASSIGN(UniqueRowsetIdGenerator);
}; // UniqueRowsetIdGenerator

} // namespace doris
